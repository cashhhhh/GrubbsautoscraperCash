#!/usr/bin/env python3
"""
check_setup.py
Validates that all credentials in .env are correct before running
the full inventory sync.

Usage:
    python check_setup.py
"""

import json
import os
import sys

import requests
from dotenv import load_dotenv

load_dotenv()

FB_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN", "")
FB_CATALOG_ID   = os.getenv("FB_CATALOG_ID",   "")
FB_APP_ID       = os.getenv("FB_APP_ID",        "")
FB_APP_SECRET   = os.getenv("FB_APP_SECRET",    "")
FB_API_VERSION  = os.getenv("FB_API_VERSION",   "v21.0")
RSS_URL         = os.getenv("RSS_URL", ""https://www.infinitiofsanantonio.com/searchused.aspx?Dealership=Grubbs%20INFINITI%20of%20San%20Antonio")

BASE = f"https://graph.facebook.com/{FB_API_VERSION}"

OK   = "\033[92m✓\033[0m"
FAIL = "\033[91m✗\033[0m"
WARN = "\033[93m!\033[0m"


def check(label: str, passed: bool, detail: str = "") -> bool:
    icon = OK if passed else FAIL
    print(f"  {icon}  {label}" + (f"  →  {detail}" if detail else ""))
    return passed


def main() -> None:
    print("\n── Facebook Marketplace Sync — Setup Check ──\n")
    all_ok = True

    # ── 1. Environment variables ──────────────────────────────────────────────
    print("1. Environment variables (.env)")
    all_ok &= check("FB_APP_ID set",       bool(FB_APP_ID),       FB_APP_ID[:12] + "…" if FB_APP_ID else "MISSING")
    all_ok &= check("FB_APP_SECRET set",   bool(FB_APP_SECRET),   "set" if FB_APP_SECRET else "MISSING")
    all_ok &= check("FB_ACCESS_TOKEN set", bool(FB_ACCESS_TOKEN), "set" if FB_ACCESS_TOKEN else "MISSING")
    all_ok &= check("FB_CATALOG_ID set",   bool(FB_CATALOG_ID),   FB_CATALOG_ID if FB_CATALOG_ID else "MISSING")

    if not FB_ACCESS_TOKEN:
        print("\n  ⚠  Cannot continue checks without FB_ACCESS_TOKEN.\n")
        sys.exit(1)

    # ── 2. Token validity & identity ─────────────────────────────────────────
    print("\n2. Access token")
    resp = requests.get(f"{BASE}/me", params={"access_token": FB_ACCESS_TOKEN, "fields": "id,name"}, timeout=10)
    data = resp.json()
    if "error" in data:
        check("Token valid", False, data["error"].get("message", "unknown error"))
        all_ok = False
        print("\n  ⚠  Token is invalid. Re-generate via Business Manager → System Users.\n")
        sys.exit(1)
    else:
        check("Token valid", True, f"ID={data.get('id')}  name={data.get('name','—')}")

    # ── 3. Token permissions / debug info ────────────────────────────────────
    print("\n3. Token permissions")
    debug = requests.get(
        f"{BASE}/debug_token",
        params={
            "input_token":  FB_ACCESS_TOKEN,
            "access_token": f"{FB_APP_ID}|{FB_APP_SECRET}" if FB_APP_ID and FB_APP_SECRET else FB_ACCESS_TOKEN,
        },
        timeout=10,
    ).json().get("data", {})

    scopes = debug.get("scopes", [])
    needed = {"catalog_management", "business_management"}
    for perm in needed:
        check(f"Permission: {perm}", perm in scopes, "granted" if perm in scopes else "MISSING — re-generate token with this scope")
        if perm not in scopes:
            all_ok = False

    expires = debug.get("expires_at", 0)
    if expires == 0:
        check("Token expiry", True, "never (System User token — good)")
    else:
        import datetime
        exp_dt = datetime.datetime.fromtimestamp(expires)
        check("Token expiry", True, str(exp_dt))

    # ── 4. Catalog access ─────────────────────────────────────────────────────
    if FB_CATALOG_ID:
        print("\n4. Catalog")
        cat_resp = requests.get(
            f"{BASE}/{FB_CATALOG_ID}",
            params={"access_token": FB_ACCESS_TOKEN, "fields": "id,name,vertical"},
            timeout=10,
        ).json()
        if "error" in cat_resp:
            check("Catalog accessible", False, cat_resp["error"].get("message", "unknown"))
            all_ok = False
        else:
            check("Catalog accessible", True,
                  f"name='{cat_resp.get('name')}' type={cat_resp.get('vertical','—')}")
            if cat_resp.get("vertical") != "vehicles":
                print(f"  {WARN}  Catalog vertical is '{cat_resp.get('vertical')}' — should be 'vehicles' for automotive listings.")
    else:
        print(f"\n4. Catalog\n  {WARN}  FB_CATALOG_ID not set — skipping catalog check.")

    # ── 5. RSS feed reachable ─────────────────────────────────────────────────
    print("\n5. Dealer RSS feed")
    try:
        rss = requests.head(RSS_URL, timeout=10)
        check("RSS feed reachable", rss.status_code < 400, f"HTTP {rss.status_code}")
    except Exception as e:
        check("RSS feed reachable", False, str(e))
        all_ok = False

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    if all_ok:
        print("  All checks passed — run python fb_marketplace_sync.py to sync inventory.\n")
    else:
        print("  Some checks failed — fix the issues above before running the sync.\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
