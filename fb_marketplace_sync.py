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
RSS_URL                  = os.getenv("RSS_URL",         "https://www.infinitiofsanantonio.com/rss-usedinventory.aspx")
DEALER_BASE_URL          = os.getenv("DEALER_BASE_URL", "https://www.infinitiofsanantonio.com")
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


# ──────────────────────────────────────────────────────────────────────────────
# Step 1 — Parse RSS feed
# ──────────────────────────────────────────────────────────────────────────────
def fetch_rss() -> list[Vehicle]:
    """Fetch and parse the DealerOn used-inventory RSS feed."""
    resp = requests.get(RSS_URL, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()

    # Strip any BOM / leading whitespace that would break ET
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
        ))

    rss_prices = sum(1 for v in vehicles if v.price)
    print(f"[RSS] Parsed {len(vehicles)} vehicles ({rss_prices} with price in feed)", flush=True)
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
# Step 3 — Build Facebook catalog items
# ──────────────────────────────────────────────────────────────────────────────
def _fb_item(v: Vehicle) -> dict:
    """Return the data dict for one vehicle in the FB catalog format."""
    price_str = v.price if v.price else "0 USD"

    return {
        "id":             v.vin,
        "title":          v.title,
        "description":    v.description,
        "availability":   "in stock",
        "condition":      "used",
        "price":          price_str,
        "link":           v.link,
        "image_link":     v.image_url,
        "brand":          v.make,
        # Automotive-specific fields
        "year":           int(v.year) if v.year and v.year.isdigit() else None,
        "make":           v.make,
        "model":          v.model,
        "trim":           v.trim,
        "mileage":        {"value": int(v.mileage) if v.mileage.isdigit() else 0, "unit": "MI"},
        "exterior_color": v.exterior_color,
        "vin":            v.vin,
        "vehicle_type":   "car_truck",
        "state_of_vehicle": "used",
    }


# ──────────────────────────────────────────────────────────────────────────────
# Step 4a — Save CSV backup
# ──────────────────────────────────────────────────────────────────────────────
CSV_FIELDS = [
    "id", "title", "description", "availability", "condition",
    "price", "link", "image_link", "brand",
    "year", "make", "model", "trim",
    "mileage", "exterior_color", "vin",
    "vehicle_type", "state_of_vehicle",
]

def save_csv(vehicles: list[Vehicle], path: str = CSV_OUTPUT_PATH) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for v in vehicles:
            item = _fb_item(v)
            item["mileage"] = item["mileage"]["value"]     # flatten for CSV
            row = {k: (item.get(k) or "") for k in CSV_FIELDS}
            writer.writerow(row)
    print(f"[CSV] Saved {len(vehicles)} vehicles → {path}", flush=True)


# ──────────────────────────────────────────────────────────────────────────────
# Step 4b — Upload to Facebook Catalog API
# ──────────────────────────────────────────────────────────────────────────────
def upload_to_facebook(vehicles: list[Vehicle]) -> bool:
    """
    POST vehicles to the Facebook Product Catalog batch endpoint.
    Returns True on full success, False if any batch had errors.

    Facebook docs:
      POST /{catalog_id}/batch
      https://developers.facebook.com/docs/marketing-api/catalog/reference/
    """
    if not FB_ACCESS_TOKEN or not FB_CATALOG_ID:
        print(
            "\n[FB] FB_ACCESS_TOKEN or FB_CATALOG_ID not set — skipping upload.\n"
            "     Fill them in .env and re-run, or use --csv-only to just export the feed.",
            flush=True,
        )
        return False

    endpoint = f"https://graph.facebook.com/{FB_API_VERSION}/{FB_CATALOG_ID}/batch"
    all_ok = True

    for batch_num, i in enumerate(range(0, len(vehicles), BATCH_SIZE), start=1):
        chunk = vehicles[i : i + BATCH_SIZE]

        requests_payload = [
            {
                "method":      "UPDATE",   # creates or updates
                "retailer_id": v.vin,
                "data":        _fb_item(v),
            }
            for v in chunk
        ]

        resp = requests.post(
            endpoint,
            data={
                "requests":     json.dumps(requests_payload),
                "access_token": FB_ACCESS_TOKEN,
            },
            timeout=60,
        )
        result = resp.json()

        if "error" in result:
            print(f"  [FB] Batch {batch_num} ERROR: {result['error']}", flush=True)
            all_ok = False
        else:
            handles = result.get("handles", [])
            print(
                f"  [FB] Batch {batch_num}: {len(chunk)} items submitted, "
                f"{len(handles)} handle(s) returned",
                flush=True,
            )

        time.sleep(0.5)   # respect rate limits

    return all_ok


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
