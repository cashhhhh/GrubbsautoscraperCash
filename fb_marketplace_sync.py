#!/usr/bin/env python3
"""
fb_marketplace_sync.py
Scrapes used inventory from a DealerOn dealer website and syncs it
to a Facebook Product Catalog (automotive type) via the Meta Graph API.

Usage:
    python fb_marketplace_sync.py              # full run: scrape + upload
    python fb_marketplace_sync.py --csv-only   # scrape + save CSV, skip upload

Setup:
    pip install -r requirements.txt
    playwright install chromium
    cp .env.example .env  # then fill in your credentials
"""

import argparse
import asyncio
import csv
import io
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Optional

import requests
from dotenv import load_dotenv
from playwright.async_api import async_playwright
from playwright.async_api import TimeoutError as PWTimeout

load_dotenv()

# ──────────────────────────────────────────────────────────────────────────────
# Config  (override any of these in .env)
# ──────────────────────────────────────────────────────────────────────────────
# Comma-separated list of RSS feed URLs to pull from.
# Used + CPO gives a good mix: cheap daily drivers AND expensive certified INFINITIs.
# Add rss-newinventory.aspx here if you ever want brand-new cars too.
_rss_urls_env = os.getenv(
    "RSS_URLS",
    "https://www.infinitiofsanantonio.com/rss-usedinventory.aspx,"
    "https://www.infinitiofsanantonio.com/rss-certifiedinventory.aspx",
)
RSS_URLS = [u.strip() for u in _rss_urls_env.split(",") if u.strip()]
DEALER_BASE_URL          = os.getenv("DEALER_BASE_URL", "https://www.infinitiofsanantonio.com")
DEALER_ADDR1             = os.getenv("DEALER_ADDR1",    "11911 IH 10 West")
DEALER_CITY              = os.getenv("DEALER_CITY",     "San Antonio")
DEALER_REGION            = os.getenv("DEALER_REGION",   "Texas")
DEALER_POSTAL_CODE       = os.getenv("DEALER_POSTAL_CODE", "78230")
DEALER_COUNTRY           = os.getenv("DEALER_COUNTRY",  "United States")
FB_ACCESS_TOKEN          = os.getenv("FB_ACCESS_TOKEN", "")
FB_CATALOG_ID            = os.getenv("FB_CATALOG_ID",   "")
FB_API_VERSION           = os.getenv("FB_API_VERSION",  "v21.0")
BATCH_SIZE               = int(os.getenv("BATCH_SIZE",               "50"))
PRICE_SCRAPE_CONCURRENCY = int(os.getenv("PRICE_SCRAPE_CONCURRENCY", "5"))
PRICE_SCRAPE_TIMEOUT_MS  = int(os.getenv("PRICE_SCRAPE_TIMEOUT_MS",  "30000"))
CSV_OUTPUT_PATH          = os.getenv("CSV_OUTPUT_PATH", "inventory_feed.csv")

# DealerOn price selectors — tried in order, first match wins
PRICE_SELECTORS = [
    "[data-vehicle-pricing-final-price]",
    ".final-price",
    ".vehicle-price",
    ".price-block .price",
    ".pricing .price",
    ".asking-price",
    ".internet-price",
    "span.price",
    "[class*='price'][class*='final']",
    "[class*='final'][class*='price']",
    "[class*='internet'][class*='price']",
]


# ──────────────────────────────────────────────────────────────────────────────
# Data model
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class Vehicle:
    vin:            str
    title:          str
    link:           str
    stock_number:   str
    mileage:        str          # numeric string, miles
    exterior_color: str
    image_url:      str
    year:           str = ""
    make:           str = ""
    model:          str = ""
    trim:           str = ""
    description:    str = ""
    price:          Optional[str] = None   # "24995 USD"
    condition:      str = "used"           # "new" or "used"
    body_style:     str = ""               # inferred; see _infer_body_style()


# ──────────────────────────────────────────────────────────────────────────────
# Step 1 — Parse RSS feed
# ──────────────────────────────────────────────────────────────────────────────
def _parse_rss_feed(url: str, condition: str = "used") -> list[Vehicle]:
    """Fetch one RSS feed URL and return a list of Vehicles."""
    resp = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()

    raw = resp.text.lstrip("\ufeff").strip()
    root = ET.fromstring(raw)

    vehicles: list[Vehicle] = []
    for item in root.findall(".//item"):

        def txt(tag: str) -> str:
            el = item.find(tag)
            if el is None:
                return ""
            return (el.text or "").strip()

        title            = txt("title")
        link             = txt("link")
        description_html = txt("description")

        # ── VIN ─────────────────────────────────────────────────────────────
        vin = ""
        m = re.search(r"VIN#[:\s]+([A-HJ-NPR-Z0-9]{17})", description_html, re.I)
        if m:
            vin = m.group(1)
        if not vin:
            m = re.search(r"([A-HJ-NPR-Z0-9]{17})$", link.rstrip("/"))
            if m:
                vin = m.group(1)
        if not vin:
            continue  # can't identify the vehicle

        # ── Stock number ─────────────────────────────────────────────────────
        m = re.search(r"Stock#[:\s]+(\S+)", description_html, re.I)
        stock_number = m.group(1) if m else vin

        # ── Mileage ──────────────────────────────────────────────────────────
        m = re.search(r"([\d,]+)\s*(?:Miles|mi\.?)\b", description_html, re.I)
        mileage = m.group(1).replace(",", "") if m else "0"

        # ── Exterior color ───────────────────────────────────────────────────
        m = re.search(r"Exterior Color[:\s]+([^<\n,]+)", description_html, re.I)
        exterior_color = m.group(1).strip() if m else ""

        # ── Price from RSS description ────────────────────────────────────────
        rss_price: Optional[str] = None
        _price_pats = [
            r"(?:Sale|Internet|Our|Asking|Final|List|MSRP|Retail)\s*Price[:\s]*\$?\s*([\d]{2,3},?[\d]{3})",
            r"Price[:\s]*\$\s*([\d]{2,3},?[\d]{3})",
            r"\$\s*([\d]{2,3},[\d]{3})",
        ]
        for _pat in _price_pats:
            _pm = re.search(_pat, description_html, re.I)
            if _pm:
                _val = int(_pm.group(1).replace(",", ""))
                if 500 < _val < 500_000:
                    rss_price = f"{_val} USD"
                    break

        # ── Image URL ────────────────────────────────────────────────────────
        m = re.search(r'src=["\']([^"\']+inventoryphotos[^"\']+)["\']', description_html, re.I)
        if m:
            raw_path = m.group(1)
            # Upgrade thumbnail path to full-size image
            full_path = re.sub(r"/thumbs/(\d+\.jpg)$", r"/\1", raw_path)
            image_url = full_path if full_path.startswith("http") else DEALER_BASE_URL + full_path
        else:
            image_url = f"{DEALER_BASE_URL}/inventoryphotos/27380/{vin}/ip/1.jpg"

        # ── Year / Make / Model / Trim from title ────────────────────────────
        title_clean = re.sub(r"\s+", " ", title).strip()
        m = re.match(r"(\d{4})\s+(\S+)\s+(\S+)\s*(.*)", title_clean)
        year = make = model = trim = ""
        if m:
            year  = m.group(1)
            make  = m.group(2)
            model = m.group(3)
            trim  = m.group(4).strip()

        description = (
            f"{title_clean}. "
            f"Stock #{stock_number}. VIN: {vin}. "
            f"Mileage: {int(mileage):,} miles. "
            f"Exterior: {exterior_color}. "
            f"Used vehicle available at Grubbs INFINITI of San Antonio. "
            f"View full details at {link}"
        )

        vehicles.append(Vehicle(
            vin=vin,
            title=title_clean,
            link=link,
            stock_number=stock_number,
            mileage=mileage,
            exterior_color=exterior_color,
            image_url=image_url,
            year=year,
            make=make,
            model=model,
            trim=trim,
            description=description,
            price=rss_price,
            condition=condition,
        ))

    return vehicles


def fetch_rss() -> list[Vehicle]:
    """Fetch all configured RSS feeds and return deduplicated vehicles."""
    seen_vins: set[str] = set()
    all_vehicles: list[Vehicle] = []
    for url in RSS_URLS:
        label = url.split("/")[-1]   # e.g. rss-usedinventory.aspx
        try:
            condition = "new" if "newinventory" in url else "used"
            batch = _parse_rss_feed(url, condition=condition)
            new = [v for v in batch if v.vin not in seen_vins]
            seen_vins.update(v.vin for v in new)
            all_vehicles.extend(new)
            print(f"[RSS] {label}: {len(batch)} vehicles ({len(new)} new after dedup)", flush=True)
        except Exception as exc:
            print(f"[RSS] WARN — could not fetch {url}: {exc}", flush=True)

    rss_prices = sum(1 for v in all_vehicles if v.price)
    print(f"[RSS] Total: {len(all_vehicles)} vehicles ({rss_prices} with price in feed)", flush=True)
    return all_vehicles


# ──────────────────────────────────────────────────────────────────────────────
# Step 2 — Scrape prices with Playwright
# ──────────────────────────────────────────────────────────────────────────────
def _parse_price_val(text: str) -> Optional[str]:
    """Extract a plausible vehicle price from a string. Returns 'NNNNN USD' or None."""
    m = re.search(r"\$?\s*([\d]{1,3}(?:,[\d]{3})+|[\d]{4,6})", text)
    if m:
        val = int(m.group(1).replace(",", ""))
        # Exclude model-year values (1900-2035) which appear everywhere on VDP pages
        if 1900 <= val <= 2035:
            return None
        if 2_500 < val < 500_000:
            return f"{val} USD"
    return None


async def _price_from_json_ld(page) -> Optional[str]:
    """Try to extract price from JSON-LD structured data embedded in the page."""
    scripts = await page.query_selector_all('script[type="application/ld+json"]')
    for script in scripts:
        try:
            content = await script.inner_text()
            data = json.loads(content)
            # data may be a list or a single object
            items = data if isinstance(data, list) else [data]
            for item in items:
                # Vehicle or Product schema
                offers = item.get("offers") or item.get("Offers")
                if offers:
                    if isinstance(offers, list):
                        offers = offers[0]
                    price_raw = str(offers.get("price", ""))
                    result = _parse_price_val(price_raw)
                    if result:
                        return result
                # Sometimes price is directly on the item
                for key in ("price", "Price", "salePrice", "offerPrice"):
                    if key in item:
                        result = _parse_price_val(str(item[key]))
                        if result:
                            return result
        except Exception:
            continue
    return None


async def _price_from_js(page) -> Optional[str]:
    """Try to read price from common dealer JS globals."""
    snippets = [
        "window.digitalData?.product?.[0]?.productInfo?.price?.basePrice",
        "window.vehicleData?.price",
        "window.vehicle?.price",
        "window.pageData?.vehicle?.price",
        "window.inventory?.price",
        "window.ddl?.vehicle?.price",
    ]
    for snippet in snippets:
        try:
            val = await page.evaluate(f"(() => {{ try {{ return {snippet}; }} catch(e) {{ return null; }} }})()")
            if val:
                result = _parse_price_val(str(val))
                if result:
                    return result
        except Exception:
            continue
    return None


async def _scrape_one(page, vehicle: Vehicle) -> Optional[str]:
    """Return price as '24995 USD', or None if not found."""
    try:
        await page.goto(
            vehicle.link,
            wait_until="load",
            timeout=PRICE_SCRAPE_TIMEOUT_MS,
        )
        # Give AJAX pricing calls time to return after the page fires "load"
        await page.wait_for_timeout(3000)

        # 1 — JSON-LD structured data (most reliable)
        result = await _price_from_json_ld(page)
        if result:
            return result

        # 2 — JS globals set by the dealer platform
        result = await _price_from_js(page)
        if result:
            return result

        # 3 — CSS selectors
        for selector in PRICE_SELECTORS:
            try:
                el = await page.query_selector(selector)
                if el:
                    raw_text = await el.inner_text()
                    result = _parse_price_val(raw_text)
                    if result:
                        return result
            except Exception:
                continue

        # 4 — Last resort: scan full page text near the word "price"
        body = await page.inner_text("body")
        for price_section in re.finditer(r"(?i)price.{0,200}", body):
            result = _parse_price_val(price_section.group())
            if result:
                return result

    except Exception as exc:
        print(f"    [WARN] {vehicle.vin}: {exc}", flush=True)

    return None


async def scrape_prices(vehicles: list[Vehicle], debug: bool = False) -> list[Vehicle]:
    """Scrape VDP pages for vehicles that still need a price."""
    need_scrape = [v for v in vehicles if not v.price]
    rss_found   = len(vehicles) - len(need_scrape)
    if rss_found:
        print(f"[Playwright] {rss_found}/{len(vehicles)} prices already in RSS — skipping those.", flush=True)
    if not need_scrape:
        return vehicles

    sem = asyncio.Semaphore(PRICE_SCRAPE_CONCURRENCY)
    debug_saved = False

    async def run_one(ctx, v: Vehicle):
        nonlocal debug_saved
        async with sem:
            page = await ctx.new_page()
            try:
                v.price = await _scrape_one(page, v)
                status = v.price if v.price else "not found"
                print(f"    {v.vin[:10]}  {v.title[:42]:42s}  {status}", flush=True)
                if debug and not v.price and not debug_saved:
                    html = await page.content()
                    with open("debug_vdp.html", "w", encoding="utf-8") as fh:
                        fh.write(html)
                    print(f"    [DEBUG] Saved page HTML → debug_vdp.html ({v.link})", flush=True)
                    debug_saved = True
            finally:
                await page.close()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
        )
        print(
            f"\n[Playwright] Scraping {len(need_scrape)} VDPs without RSS price "
            f"(concurrency={PRICE_SCRAPE_CONCURRENCY})...",
            flush=True,
        )
        await asyncio.gather(*[run_one(ctx, v) for v in need_scrape])
        await browser.close()

    found = sum(1 for v in vehicles if v.price)
    print(f"\n[Playwright] Prices found: {found}/{len(vehicles)}", flush=True)
    return vehicles


# ──────────────────────────────────────────────────────────────────────────────
# Step 3 — Build Facebook automotive XML feed
# ──────────────────────────────────────────────────────────────────────────────
def _price_to_decimal_str(price: Optional[str]) -> str:
    """Convert '24995 USD' → '24995.00 USD' as required by the feed format."""
    if not price:
        return "0.00 USD"
    m = re.match(r"(\d+)\s+([A-Z]+)", price)
    if m:
        return f"{int(m.group(1)):.2f} {m.group(2)}"
    return price


def _infer_body_style(make: str, model: str, trim: str) -> str:
    """
    Infer Facebook body_style enum from make/model/trim.
    Accepted values: CONVERTIBLE, COUPE, CROSSOVER, HATCHBACK,
                     MINIVAN, SEDAN, SUV, TRUCK, VAN, WAGON, OTHER
    """
    text = f"{make} {model} {trim}".upper()

    # Trucks
    if any(x in text for x in [
        "F-150", "F150", "F-250", "F250", "F-350", "F350",
        "SILVERADO", "SIERRA", " RAM ", "TUNDRA", "TACOMA",
        "COLORADO", "CANYON", "FRONTIER", "RANGER", "RIDGELINE",
        "TITAN", "AVALANCHE", "DAKOTA",
    ]):
        return "TRUCK"

    # Minivans
    if any(x in text for x in [
        "SIENNA", "ODYSSEY", "PACIFICA", "CARAVAN", "QUEST",
        "SEDONA", "TOWN & COUNTRY", "TOWN AND COUNTRY",
    ]):
        return "MINIVAN"

    # Convertibles
    if any(x in text for x in ["CONVERT", "CABRIOLET", "ROADSTER", "SPYDER"]):
        return "CONVERTIBLE"

    # Coupes
    if any(x in text for x in [
        " Q60", "MUSTANG", "CAMARO", "CHALLENGER", "CORVETTE",
        "370Z", "350Z", "86", "BRZ", "RC ", " TT ", "M4", "M2",
    ]):
        return "COUPE"

    # Wagons
    if any(x in text for x in ["WAGON", "ALLROAD", "SPORTBACK", " A4 AVANT", "OUTBACK"]):
        return "WAGON"

    # Hatchbacks
    if any(x in text for x in [
        " GOLF", " POLO", "HATCHBACK", "5-DOOR", "3-DOOR",
        "FOCUS ST", "FOCUS SE 5", "IMPREZA HATCH",
    ]):
        return "HATCHBACK"

    # SUVs / Crossovers — check after trucks/vans to avoid false matches
    if any(x in text for x in [
        "QX", "EXPLORER", "EXPEDITION", "NAVIGATOR", "ESCALADE",
        "SUBURBAN", "TAHOE", "YUKON", "TRAVERSE", "PILOT", "PASSPORT",
        "PATHFINDER", "ARMADA", "HIGHLANDER", "4RUNNER", "SEQUOIA",
        "LAND CRUISER", "MDX", "RDX", "ACADIA", "ENCLAVE", "ENVISION",
        "ATLAS", "TIGUAN", "TOUAREG", "Q7", "Q5", "Q3", "SQ5",
        "X1", "X3", "X5", "X6", "X7", "GLC", "GLE", "GLS", "ML",
        "RX", "GX", "LX", "NX", "UX", "CX-5", "CX-7", "CX-9",
        "CR-V", "HR-V", "PILOT", "ROGUE", "MURANO", "XTERRA",
        "SANTA FE", "TUCSON", "PALISADE", "TELLURIDE", "SPORTAGE",
        "SORENTO", "SOUL", "EQUINOX", "TRAX", "BLAZER", "TRAILBLAZER",
        "COMPASS", "RENEGADE", "WRANGLER", "GRAND CHEROKEE", "CHEROKEE",
        "EDGE", "ESCAPE", "FLEX", "TERRAIN", "VUE", "CAPTIVA",
        "4RUNNER", "FJ CRUISER", "RAV4", "VENZA", "CROSSOVER",
        "GRAND VITARA", "VITARA", "OUTLANDER", "ECLIPSE CROSS",
        "FORESTER", "CROSSTREK", "ASCENT", "BAJA",
        " EX35", " FX", " JX", " QX",
    ]):
        return "SUV"

    # Default to SEDAN for everything else
    return "SEDAN"


def build_xml_feed(vehicles: list[Vehicle]) -> bytes:
    """Return a Facebook automotive XML feed as UTF-8 bytes."""
    root = ET.Element("listings")
    ET.SubElement(root, "title").text = "Grubbs INFINITI of San Antonio"

    for v in vehicles:
        listing = ET.SubElement(root, "listing")

        ET.SubElement(listing, "vehicle_id").text  = v.vin
        ET.SubElement(listing, "title").text        = v.title
        ET.SubElement(listing, "description").text  = v.description
        ET.SubElement(listing, "url").text           = v.link

        img = ET.SubElement(listing, "image")
        ET.SubElement(img, "url").text = v.image_url

        ET.SubElement(listing, "price").text = _price_to_decimal_str(v.price)

        mil = ET.SubElement(listing, "mileage")
        ET.SubElement(mil, "unit").text  = "MI"
        ET.SubElement(mil, "value").text = v.mileage if v.mileage else "0"

        body = v.body_style or _infer_body_style(v.make, v.model, v.trim)
        ET.SubElement(listing, "body_style").text        = body
        ET.SubElement(listing, "state_of_vehicle").text  = v.condition.upper()
        ET.SubElement(listing, "make").text              = v.make
        ET.SubElement(listing, "model").text             = v.model
        if v.year:
            ET.SubElement(listing, "year").text = v.year
        if v.trim:
            ET.SubElement(listing, "trim").text = v.trim
        if v.exterior_color:
            ET.SubElement(listing, "exterior_color").text = v.exterior_color

        addr = ET.SubElement(listing, "address")
        addr.set("format", "simple")
        for name, value in [
            ("addr1",       DEALER_ADDR1),
            ("city",        DEALER_CITY),
            ("region",      DEALER_REGION),
            ("postal_code", DEALER_POSTAL_CODE),
            ("country",     DEALER_COUNTRY),
        ]:
            c = ET.SubElement(addr, "component")
            c.set("name", name)
            c.text = value

    buf = io.BytesIO()
    ET.ElementTree(root).write(buf, encoding="utf-8", xml_declaration=True)
    return buf.getvalue()


# ──────────────────────────────────────────────────────────────────────────────
# Step 4a — Save CSV backup (human-readable) + XML feed file
# ──────────────────────────────────────────────────────────────────────────────
CSV_FIELDS = [
    "vehicle_id", "title", "description", "price",
    "url", "image_url", "year", "make", "model", "trim",
    "mileage", "exterior_color", "state_of_vehicle",
]

def save_csv(vehicles: list[Vehicle], path: str = CSV_OUTPUT_PATH) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for v in vehicles:
            writer.writerow({
                "vehicle_id":      v.vin,
                "title":           v.title,
                "description":     v.description,
                "price":           _price_to_decimal_str(v.price),
                "url":             v.link,
                "image_url":       v.image_url,
                "year":            v.year,
                "make":            v.make,
                "model":           v.model,
                "trim":            v.trim,
                "mileage":         v.mileage,
                "exterior_color":  v.exterior_color,
                "state_of_vehicle": v.condition.upper(),
            })
    print(f"[CSV] Saved {len(vehicles)} vehicles → {path}", flush=True)


# ──────────────────────────────────────────────────────────────────────────────
# Step 4b — Upload to Facebook Catalog API
# ──────────────────────────────────────────────────────────────────────────────
def resolve_catalog_id() -> str:
    """
    Return FB_CATALOG_ID from env, or auto-discover the first automotive
    catalog accessible to the token via the Graph API.
    """
    if FB_CATALOG_ID and FB_CATALOG_ID != "your_catalog_id_here":
        return FB_CATALOG_ID

    if not FB_ACCESS_TOKEN:
        return ""

    print("[FB] FB_CATALOG_ID not set — querying Graph API to find your catalog…", flush=True)

    base = f"https://graph.facebook.com/{FB_API_VERSION}"
    token_param = f"access_token={FB_ACCESS_TOKEN}"

    # Try: businesses the token can see → their owned catalogs
    try:
        r = requests.get(
            f"{base}/me/businesses?fields=id,name,owned_product_catalogs{{id,name}}&{token_param}",
            timeout=15,
        )
        data = r.json()
        if "error" in data:
            print(f"[FB] businesses API error: {data['error'].get('message')}", flush=True)
        else:
            bizzes = data.get("data", [])
            print(f"[FB] Found {len(bizzes)} business(es) on this token.", flush=True)
            for biz in bizzes:
                cats = (biz.get("owned_product_catalogs") or {}).get("data", [])
                print(f"[FB]   Biz '{biz.get('name')}' → {len(cats)} catalog(s)", flush=True)
                if cats:
                    cat = cats[0]
                    print(f"[FB] Auto-detected catalog: {cat['name']} (id={cat['id']})", flush=True)
                    print(f"[FB] Tip: set FB_CATALOG_ID={cat['id']} in .env to skip this lookup.", flush=True)
                    return cat["id"]
    except Exception as exc:
        print(f"[FB] businesses API exception: {exc}", flush=True)

    # Fallback: catalogs directly on the token (system-user tokens)
    try:
        r = requests.get(
            f"{base}/me/product_catalogs?fields=id,name&{token_param}",
            timeout=15,
        )
        data = r.json()
        if "error" in data:
            print(f"[FB] product_catalogs API error: {data['error'].get('message')}", flush=True)
        else:
            cats = data.get("data", [])
            print(f"[FB] product_catalogs fallback: found {len(cats)} catalog(s).", flush=True)
            if cats:
                cat = cats[0]
                print(f"[FB] Auto-detected catalog: {cat['name']} (id={cat['id']})", flush=True)
                print(f"[FB] Tip: set FB_CATALOG_ID={cat['id']} in .env to skip this lookup.", flush=True)
                return cat["id"]
    except Exception as exc:
        print(f"[FB] product_catalogs API exception: {exc}", flush=True)

    print("[FB] Could not auto-detect catalog ID. Set FB_CATALOG_ID in .env manually.", flush=True)
    return ""


def check_catalog_type(catalog_id: str) -> None:
    """Query and print the catalog's vertical/type so the user can confirm it's automotive."""
    try:
        r = requests.get(
            f"https://graph.facebook.com/{FB_API_VERSION}/{catalog_id}",
            params={"fields": "id,name,vertical", "access_token": FB_ACCESS_TOKEN},
            timeout=15,
        )
        data = r.json()
        if "error" in data:
            print(f"  [FB] Could not read catalog info: {data['error'].get('message')}", flush=True)
        else:
            vertical = data.get("vertical", "unknown")
            name     = data.get("name", "unknown")
            print(f"  [FB] Catalog: \"{name}\" | vertical={vertical}", flush=True)
            if vertical.lower() not in ("vehicles", "automotive"):
                print(
                    f"  [FB] WARNING — catalog vertical is '{vertical}', not 'vehicles'.\n"
                    f"  [FB] Go to Commerce Manager → delete this catalog → create a new one\n"
                    f"  [FB] and choose 'Vehicles' as the catalog type, then update FB_CATALOG_ID in .env.",
                    flush=True,
                )
    except Exception as exc:
        print(f"  [FB] catalog type check failed: {exc}", flush=True)


def _get_or_create_feed(catalog_id: str) -> str:
    """Return the ID of the first product feed for this catalog, creating one if needed."""
    base = f"https://graph.facebook.com/{FB_API_VERSION}"
    r = requests.get(
        f"{base}/{catalog_id}/product_feeds",
        params={"access_token": FB_ACCESS_TOKEN, "fields": "id,name"},
        timeout=15,
    )
    data = r.json()
    if "error" in data:
        raise RuntimeError(data["error"].get("message"))
    feeds = data.get("data", [])
    if feeds:
        feed = feeds[0]
        print(f"  [FB] Using existing feed: '{feed['name']}' (id={feed['id']})", flush=True)
        return feed["id"]
    # Create a new feed
    r = requests.post(
        f"{base}/{catalog_id}/product_feeds",
        data={"name": "Grubbs INFINITI Inventory", "access_token": FB_ACCESS_TOKEN},
        timeout=15,
    )
    result = r.json()
    if "error" in result:
        raise RuntimeError(result["error"].get("message"))
    feed_id = result["id"]
    print(f"  [FB] Created new feed (id={feed_id})", flush=True)
    return feed_id


def upload_to_facebook(vehicles: list[Vehicle]) -> bool:
    """
    Upload vehicles to the Facebook Product Catalog via CSV feed upload.
    Uses the product_feeds / uploads API which accepts full automotive field names.
    """
    if not FB_ACCESS_TOKEN:
        print(
            "\n[FB] FB_ACCESS_TOKEN not set — skipping upload.\n"
            "     Fill it in .env and re-run, or use --csv-only to just export the feed.",
            flush=True,
        )
        return False

    catalog_id = resolve_catalog_id()
    if not catalog_id:
        print(
            "\n[FB] Could not determine catalog ID — skipping upload.\n"
            "     Set FB_CATALOG_ID in .env and re-run.",
            flush=True,
        )
        return False

    check_catalog_type(catalog_id)

    try:
        feed_id = _get_or_create_feed(catalog_id)
    except Exception as exc:
        print(f"  [FB] Could not get/create feed: {exc}", flush=True)
        return False

    # Build XML feed in the format Facebook's template specifies
    xml_bytes = build_xml_feed(vehicles)

    # Upload the XML to the feed
    endpoint = f"https://graph.facebook.com/{FB_API_VERSION}/{feed_id}/uploads"
    resp = requests.post(
        endpoint,
        files={"file": ("inventory.xml", xml_bytes, "text/xml")},
        data={"access_token": FB_ACCESS_TOKEN},
        timeout=120,
    )
    result = resp.json()

    if "error" in result:
        print(f"  [FB] Upload ERROR: {result['error']}", flush=True)
        return False

    upload_id = result.get("id", "unknown")
    print(f"  [FB] Feed upload accepted — upload_id={upload_id}", flush=True)
    print(f"  [FB] Items will appear in Commerce Manager within a few minutes.", flush=True)
    return True


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Sync dealer inventory to Facebook Marketplace.")
    parser.add_argument(
        "--csv-only",
        action="store_true",
        help="Scrape inventory and save CSV, but do not upload to Facebook.",
    )
    parser.add_argument(
        "--no-price-scrape",
        action="store_true",
        help="Skip Playwright price scraping (prices will be blank).",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Save HTML of the first VDP that fails price scraping to debug_vdp.html.",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("  Facebook Marketplace Inventory Sync")
    print("  Grubbs INFINITI of San Antonio")
    print("=" * 60)

    # 1 — Fetch RSS
    print("\n[1/4] Fetching inventory from DealerOn RSS feed…")
    vehicles = fetch_rss()
    if not vehicles:
        print("No vehicles found. Exiting.")
        sys.exit(1)

    # 2 — Scrape prices
    if args.no_price_scrape:
        print("\n[2/4] Skipping price scraping (--no-price-scrape).")
    else:
        print("\n[2/4] Scraping vehicle prices with Playwright…")
        vehicles = asyncio.run(scrape_prices(vehicles, debug=args.debug))

    # 3 — CSV backup (always)
    print("\n[3/4] Saving CSV backup…")
    save_csv(vehicles)

    # 4 — Facebook upload
    if args.csv_only:
        print("\n[4/4] Skipping Facebook upload (--csv-only).")
        print(f"\nFeed saved to: {CSV_OUTPUT_PATH}")
        print("You can upload this CSV manually via Business Manager → Catalogs → Data Sources.")
    else:
        print("\n[4/4] Uploading to Facebook Catalog…")
        ok = upload_to_facebook(vehicles)
        if ok:
            print("\nAll done — inventory is live in your Facebook catalog!")
        else:
            print("\nDone with some errors — check output above.")
            sys.exit(1)


if __name__ == "__main__":
    main()
