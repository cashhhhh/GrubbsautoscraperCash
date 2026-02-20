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
import base64
import csv
import io
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass, field
from typing import Optional

import subprocess

import requests
from dotenv import load_dotenv
from playwright.async_api import async_playwright
from playwright.async_api import TimeoutError as PWTimeout


def _ensure_chromium() -> None:
    """Install the Chromium browser binary if it isn't already present.

    Render's build step only runs ``pip install``, so the browser is installed
    here on first run instead.  Subsequent runs are a no-op because Playwright
    skips re-downloading an already-present binary.
    """
    subprocess.run(
        [sys.executable, "-m", "playwright", "install", "chromium"],
        check=False,        # don't crash if already installed
        capture_output=True,
    )


_ensure_chromium()

load_dotenv()

# ──────────────────────────────────────────────────────────────────────────────
# Config  (override any of these in .env)
# ──────────────────────────────────────────────────────────────────────────────
# Comma-separated list of RSS feed URLs to pull from.
# Used + CPO gives a good mix: cheap daily drivers AND expensive certified INFINITIs.
# Add rss-newinventory.aspx here if you ever want brand-new cars too.
_rss_urls_env = os.getenv(
    "RSS_URLS",
    "https://www.infinitiofsanantonio.com/rss-usedinventory.aspx"
    "?Dealership=Grubbs+INFINITI+of+San+Antonio",
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
PRICE_SCRAPE_CONCURRENCY = int(os.getenv("PRICE_SCRAPE_CONCURRENCY", "3"))
PRICE_SCRAPE_TIMEOUT_MS  = int(os.getenv("PRICE_SCRAPE_TIMEOUT_MS",  "60000"))
MAX_SCRAPE_ATTEMPTS      = int(os.getenv("MAX_SCRAPE_ATTEMPTS",       "3"))
CSV_OUTPUT_PATH          = os.getenv("CSV_OUTPUT_PATH", "inventory_feed.csv")
# Only process RSS URLs whose domain contains this string.
# Guards against accidentally pulling partner / other-store feeds.
SA_DOMAIN_FILTER         = os.getenv("SA_DOMAIN_FILTER", "infinitiofsanantonio.com")
# DealerOn inventory JSON API — returns all vehicles + prices with no browser needed
DEALERON_DEALER_ID       = os.getenv("DEALERON_DEALER_ID", "27380")
DEALERON_PAGE_ID         = os.getenv("DEALERON_PAGE_ID",   "2854470")
DEALERON_API_URL         = (
    "https://www.infinitiofsanantonio.com"
    "/api/vhcliaa/vehicle-pages/cosmos/srp/vehicles"
    "/{dealer_id}/{page_id}"
    "?host=www.infinitiofsanantonio.com"
    "&Dealership=Grubbs+INFINITI+of+San+Antonio"
    "&pageSize=200"
)

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

        # ── SA store filter — skip vehicles whose detail page is on another store ──
        if SA_DOMAIN_FILTER and SA_DOMAIN_FILTER.lower() not in link.lower():
            continue

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
        if SA_DOMAIN_FILTER and SA_DOMAIN_FILTER.lower() not in url.lower():
            print(f"[RSS] SKIP {url} — not San Antonio store (SA_DOMAIN_FILTER={SA_DOMAIN_FILTER})", flush=True)
            continue
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


def fetch_vehicles_from_api() -> list[Vehicle]:
    """
    Build the vehicle list directly from the DealerOn inventory API.
    Replaces RSS as the primary vehicle source so VINs always match what
    the price API returns (RSS can include unlisted/wholesale inventory
    that never appears in the search-page API).

    Each DisplayCard.VehicleCard contains:
      VehicleVin, VehicleName, VehicleYear/Make/Model/Trim, Mileage,
      ExteriorColorLabel, VehicleDetailUrl, VehicleImageModel.PhotoList,
      VehiclePriceLibrary (base64), VehicleType ("used"/"new"), etc.
    """
    vehicles: list[Vehicle] = []
    seen_vins: set[str] = set()
    base_url = DEALERON_API_URL.format(
        dealer_id=DEALERON_DEALER_ID,
        page_id=DEALERON_PAGE_ID,
    )
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    page_num    = 1
    total_pages = 1

    while page_num <= total_pages:
        url = base_url if page_num == 1 else f"{base_url}&pn={page_num}"
        print(f"  [API] Fetching vehicles page {page_num}/{total_pages}: {url}", flush=True)
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            print(f"  [API] ERROR on page {page_num}: {exc}", flush=True)
            break

        if page_num == 1:
            paging = (data.get("Paging") or {}).get("PaginationDataModel") or {}
            total_pages = paging.get("TotalPages") or 1

        cards = data.get("DisplayCards") or []
        found_this_page = 0

        for card in cards:
            if card.get("IsAdCard"):
                continue
            v = card.get("VehicleCard") or {}
            vin = (v.get("VehicleVin") or "").strip().upper()
            if not vin or vin in seen_vins:
                continue

            # ── Price (base64 VehiclePriceLibrary) ──────────────────────────
            price: Optional[str] = None
            price_lib = v.get("VehiclePriceLibrary") or ""
            if price_lib:
                try:
                    decoded = base64.b64decode(price_lib).decode("utf-8")
                    m = re.search(r"Selling Price:([\d.]+)", decoded)
                    if m:
                        val = float(m.group(1))
                        if 2_500 < val < 500_000:
                            price = f"{int(val)} USD"
                except Exception:
                    pass

            # ── VDP URL ─────────────────────────────────────────────────────
            link = v.get("VehicleDetailUrl") or ""
            if not link:
                img_model = v.get("VehicleImageModel") or {}
                link = img_model.get("VehicleDetailUrl") or ""

            # ── Full-size first photo ───────────────────────────────────────
            image_url = ""
            img_model  = v.get("VehicleImageModel") or {}
            carousel   = img_model.get("VehicleImageCarouselModel") or {}
            photo_list = carousel.get("PhotoList") or []
            if photo_list:
                image_url = DEALER_BASE_URL.rstrip("/") + photo_list[0]
            else:
                thumb = img_model.get("VehiclePhotoSrc") or ""
                if thumb:
                    image_url = DEALER_BASE_URL.rstrip("/") + re.sub(r"/thumbs/(\d+\.jpg)$", r"/\1", thumb)

            # ── Mileage (strip non-digits) ──────────────────────────────────
            mileage = re.sub(r"[^\d]", "", v.get("Mileage") or "0") or "0"

            # ── Description ─────────────────────────────────────────────────
            desc_parts = [p for p in [v.get("VehicleBodyStyle"), v.get("VehicleEngine")] if p]
            description = ", ".join(desc_parts)

            vehicle = Vehicle(
                vin=vin,
                title=(v.get("VehicleName") or
                       f"{v.get('VehicleYear','')} {v.get('VehicleMake','')} {v.get('VehicleModel','')}".strip()),
                link=link,
                stock_number=v.get("VehicleStockNumber") or vin,
                mileage=mileage,
                exterior_color=v.get("ExteriorColorLabel") or "",
                image_url=image_url,
                year=str(v.get("VehicleYear") or ""),
                make=v.get("VehicleMake") or "",
                model=v.get("VehicleModel") or "",
                trim=v.get("VehicleTrim") or "",
                description=description,
                price=price,
                condition=(v.get("VehicleType") or "used").lower(),
                body_style=v.get("VehicleBodyStyle") or "",
            )
            vehicles.append(vehicle)
            seen_vins.add(vin)
            found_this_page += 1

        priced = sum(1 for veh in vehicles if veh.price)
        print(f"  [API] Page {page_num}: {found_this_page} vehicles added "
              f"(total {len(vehicles)}, {priced} with price)", flush=True)
        if found_this_page == 0:
            break   # Only ads or empty — stop paging
        page_num += 1

    return vehicles


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


def _fetch_prices_from_api() -> dict[str, str]:
    """
    Hit the DealerOn inventory JSON API directly — no browser required.
    Returns {VIN: 'NNNNN USD'} for every vehicle found across all pages.

    Response shape:
      DisplayCards[]
        .IsAdCard  (bool) — skip these
        .VehicleCard
          .VehicleVin          — 17-char VIN
          .VehiclePriceLibrary — base64: "Selling Price:15995.0;..."
    """
    vin_prices: dict[str, str] = {}
    base_url = DEALERON_API_URL.format(
        dealer_id=DEALERON_DEALER_ID,
        page_id=DEALERON_PAGE_ID,
    )
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    page_num    = 1
    total_pages = 1
    total_count = 0

    while page_num <= total_pages:
        url = base_url if page_num == 1 else f"{base_url}&pn={page_num}"
        print(f"  [API] Fetching inventory page {page_num}/{total_pages}: {url}", flush=True)
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            print(f"  [API] ERROR on page {page_num}: {exc}", flush=True)
            break

        # Only trust TotalPages from page 1 — later pages can report stale/different values
        if page_num == 1:
            paging = (data.get("Paging") or {}).get("PaginationDataModel") or {}
            total_pages = paging.get("TotalPages") or 1
            total_count = paging.get("TotalCount") or 0

        cards = data.get("DisplayCards") or []
        found_this_page = 0
        vehicle_cards_seen = 0

        for card in cards:
            if card.get("IsAdCard"):
                continue
            v = card.get("VehicleCard") or {}
            vin = (v.get("VehicleVin") or "").strip().upper()
            if not vin:
                continue
            vehicle_cards_seen += 1
            if vin in vin_prices:
                continue

            # Primary: VehiclePriceLibrary is base64 "Selling Price:15995.0;..."
            price_lib = v.get("VehiclePriceLibrary") or ""
            if price_lib:
                try:
                    decoded = base64.b64decode(price_lib).decode("utf-8")
                    m = re.search(r"Selling Price:([\d.]+)", decoded)
                    if m:
                        val = float(m.group(1))
                        if 2_500 < val < 500_000:
                            vin_prices[vin] = f"{int(val)} USD"
                            found_this_page += 1
                            continue
                except Exception:
                    pass

            # Fallback: parse vehiclePricingHighlightAmount in nested HTML
            try:
                buy_html = (
                    v.get("WasabiVehiclePricingPanelViewModel", {})
                     .get("PriceStakViewModel", {})
                     .get("PriceStakTabsModel", {})
                     .get("BuyContent", "")
                ) or ""
                if buy_html:
                    m = re.search(r"vehiclePricingHighlightAmount[^>]*>\$?([\d,]+)", buy_html)
                    if m:
                        val = int(m.group(1).replace(",", ""))
                        if 2_500 < val < 500_000:
                            vin_prices[vin] = f"{val} USD"
                            found_this_page += 1
                            continue
            except Exception:
                pass

        print(f"  [API] Page {page_num}: {found_this_page} new prices "
              f"({vehicle_cards_seen} vehicle cards, running total {len(vin_prices)}/{total_count})",
              flush=True)
        if vehicle_cards_seen == 0:
            break   # Page returned only ads or was empty — stop paging
        page_num += 1

    return vin_prices


async def scrape_prices(
    vehicles: list[Vehicle],
    debug: bool = False,
    skip_vins: set[str] | None = None,
) -> list[Vehicle]:
    """Scrape VDP pages for vehicles that still need a price."""
    skip_vins = skip_vins or set()
    need_scrape = [v for v in vehicles if not v.price and v.vin not in skip_vins]
    rss_found   = sum(1 for v in vehicles if v.price)
    exhausted   = sum(1 for v in vehicles if not v.price and v.vin in skip_vins)
    if rss_found:
        print(f"[Playwright] {rss_found}/{len(vehicles)} prices already in RSS — skipping those.", flush=True)
    if exhausted:
        print(f"[Playwright] {exhausted} vehicles skipped — hit max scrape attempts ({MAX_SCRAPE_ATTEMPTS}x). "
              f"Delete their entry from the DB or set MAX_SCRAPE_ATTEMPTS higher to retry.", flush=True)
    if not need_scrape:
        return vehicles

    # Hit the DealerOn JSON API directly — no browser, no timeouts
    print(f"\n[API] Fetching prices for {len(need_scrape)} vehicles…", flush=True)
    vin_price_map = _fetch_prices_from_api()
    print(f"[API] {len(vin_price_map)} prices retrieved from inventory API", flush=True)

    # Match prices back to vehicles by VIN
    for v in need_scrape:
        price = vin_price_map.get(v.vin)
        if price:
            v.price = price
        status = v.price if v.price else "not found"
        print(f"    {v.vin[:10]}  {v.title[:42]:42s}  {status}", flush=True)

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

    t_start = time.time()
    success = True

    # 1 — Fetch inventory (API first; RSS fallback)
    print("\n[1/4] Fetching inventory from DealerOn API…")
    vehicles = fetch_vehicles_from_api()
    if not vehicles:
        print("[API] No vehicles from API — falling back to RSS feed…")
        vehicles = fetch_rss()
    if not vehicles:
        print("No vehicles found. Exiting.")
        sys.exit(1)

    # 2 — Scrape prices
    _attempts: dict[str, int] = {}
    _skip_vins: set[str] = set()
    if args.no_price_scrape:
        print("\n[2/4] Skipping price scraping (--no-price-scrape).")
    else:
        print("\n[2/4] Scraping vehicle prices with Playwright…")
        # Load past scrape-attempt counts from DB so we don't retry hopeless VINs
        try:
            import db as _db
            _db.init_db()
            _attempts = _db.get_scrape_attempts([v.vin for v in vehicles])
            _skip_vins = {vin for vin, cnt in _attempts.items() if cnt >= MAX_SCRAPE_ATTEMPTS}
        except Exception:
            pass
        vehicles = asyncio.run(scrape_prices(vehicles, debug=args.debug, skip_vins=_skip_vins))

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
        success = ok
        if ok:
            print("\nAll done — inventory is live in your Facebook catalog!")
        else:
            print("\nDone with some errors — check output above.")

    # 5 — Persist to dashboard DB
    try:
        import db as _db
        _db.init_db()
        vehicle_dicts = [asdict(v) for v in vehicles]
        _db.upsert_vehicles(vehicle_dicts)
        priced_count = sum(1 for v in vehicles if v.price)

        # Update scrape-attempt counters:
        #   - vehicles that just got priced → reset to 0
        #   - vehicles that were attempted but still have no price → increment
        #   - vehicles that were skipped (in _skip_vins) → leave unchanged
        try:
            _attempt_updates: dict[str, int] = {}
            _attempted_this_run: set[str] = set()
            if not args.no_price_scrape:
                _attempted_this_run = {v.vin for v in vehicles if v.vin not in _skip_vins}
            for v in vehicles:
                if v.vin not in _attempted_this_run:
                    continue
                if v.price:
                    _attempt_updates[v.vin] = 0          # success — reset
                else:
                    prev = _attempts.get(v.vin, 0)
                    _attempt_updates[v.vin] = prev + 1   # failure — increment
            if _attempt_updates:
                _db.update_scrape_attempts(_attempt_updates)
                failed_new = sum(1 for c in _attempt_updates.values() if c >= MAX_SCRAPE_ATTEMPTS)
                if failed_new:
                    print(f"[DB] {failed_new} vehicles now at max attempts and will be skipped next run.", flush=True)
        except Exception:
            pass   # attempt tracking is best-effort

        _db.record_sync_run({
            "vehicles_found":    len(vehicles),
            "vehicles_priced":   priced_count,
            "vehicles_uploaded": len(vehicles) if (not args.csv_only and success) else 0,
            "duration_seconds":  time.time() - t_start,
            "success":           success,
        })
        print(f"\n[DB] Saved {len(vehicles)} vehicles to dashboard database.", flush=True)
    except Exception as exc:
        print(f"\n[DB] WARNING — could not write to dashboard DB: {exc}", flush=True)

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
