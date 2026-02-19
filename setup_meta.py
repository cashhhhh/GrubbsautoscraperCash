#!/usr/bin/env python3
"""
setup_meta.py
One-shot script that uses your temporary user access token to:
  1. Find your Business Manager ID
  2. Create an Automotive catalog
  3. Create a System User (Admin)
  4. Generate a non-expiring System User token
  5. Assign the catalog to the system user
  6. Write FB_ACCESS_TOKEN and FB_CATALOG_ID back into .env

Run this ONCE on your local machine:
    pip install requests python-dotenv
    python setup_meta.py
"""

import os
import re
import sys

import requests
from dotenv import load_dotenv, set_key

load_dotenv()

APP_ID       = os.getenv("FB_APP_ID", "899738769223393")
APP_SECRET   = os.getenv("FB_APP_SECRET", "")
USER_TOKEN   = os.getenv("FB_ACCESS_TOKEN", "")
API_VERSION  = os.getenv("FB_API_VERSION", "v21.0")
BASE         = f"https://graph.facebook.com/{API_VERSION}"
ENV_FILE     = os.path.join(os.path.dirname(__file__), ".env")

CATALOG_NAME = "Grubbs INFINITI Used Inventory"
SYS_USER_NAME = "inventory_sync_bot"


def api(method: str, path: str, **kwargs) -> dict:
    url = f"{BASE}/{path.lstrip('/')}"
    params = kwargs.pop("params", {})
    params["access_token"] = USER_TOKEN
    resp = requests.request(method, url, params=params, timeout=20, **kwargs)
    data = resp.json()
    if "error" in data:
        print(f"\n  ERROR calling {path}:")
        print(f"  {data['error'].get('message')}")
        sys.exit(1)
    return data


def main() -> None:
    global USER_TOKEN

    print("\n── Meta Setup Script ──\n")

    if not USER_TOKEN or USER_TOKEN == "your_system_user_access_token_here":
        print("ERROR: FB_ACCESS_TOKEN not set in .env")
        print("Generate one at developers.facebook.com/tools/explorer")
        sys.exit(1)

    # ── 1. Identify user ──────────────────────────────────────────────────────
    me = api("GET", "/me", params={"fields": "id,name"})
    print(f"  Logged in as: {me['name']} (id={me['id']})")

    # ── 2. Find Business Manager ──────────────────────────────────────────────
    biz_resp = api("GET", "/me/businesses", params={"fields": "id,name"})
    businesses = biz_resp.get("data", [])
    if not businesses:
        print("\n  ERROR: No Business Manager found for this account.")
        print("  Create one at business.facebook.com first.")
        sys.exit(1)

    if len(businesses) == 1:
        biz = businesses[0]
    else:
        print("\n  Multiple Business Managers found:")
        for i, b in enumerate(businesses):
            print(f"    [{i}] {b['name']}  (id={b['id']})")
        idx = int(input("  Enter number to use: ").strip())
        biz = businesses[idx]

    BIZ_ID = biz["id"]
    print(f"  Using Business Manager: {biz['name']} (id={BIZ_ID})")

    # ── 3. Create Automotive catalog ──────────────────────────────────────────
    print(f"\n  Creating catalog '{CATALOG_NAME}'...")
    cat = api("POST", f"/{BIZ_ID}/owned_product_catalogs",
              data={"name": CATALOG_NAME, "vertical": "vehicles"})
    CATALOG_ID = cat["id"]
    print(f"  Catalog created: id={CATALOG_ID}")

    # ── 4. Create System User ─────────────────────────────────────────────────
    print(f"\n  Creating system user '{SYS_USER_NAME}'...")
    su = api("POST", f"/{BIZ_ID}/system_users",
             data={"name": SYS_USER_NAME, "role": "ADMIN"})
    SU_ID = su["id"]
    print(f"  System user created: id={SU_ID}")

    # ── 5. Generate non-expiring System User token ────────────────────────────
    print("\n  Generating System User access token...")
    tok = api("POST", f"/{BIZ_ID}/access_token",  # noqa: S106  (not a hardcoded secret)
              params={
                  "scope": "catalog_management,business_management,ads_management",
                  "appsecret_proof": _appsecret_proof(),
              },
              data={
                  "system_user_id": SU_ID,
                  "app_id": APP_ID,
              })
    SU_TOKEN = tok["access_token"]
    print(f"  System User token generated (length={len(SU_TOKEN)})")

    # ── 6. Assign catalog to system user ──────────────────────────────────────
    print("\n  Assigning catalog to system user...")
    api("POST", f"/{CATALOG_ID}/assigned_users",
        data={"user": SU_ID, "tasks": '["MANAGE"]'})
    print("  Catalog assigned.")

    # ── 7. Write values back to .env ──────────────────────────────────────────
    print(f"\n  Writing FB_CATALOG_ID and FB_ACCESS_TOKEN to {ENV_FILE} ...")
    set_key(ENV_FILE, "FB_CATALOG_ID", CATALOG_ID)
    set_key(ENV_FILE, "FB_ACCESS_TOKEN", SU_TOKEN)
    print("  Done.")

    print("\n── Setup complete ──────────────────────────────────────────")
    print(f"  Catalog ID : {CATALOG_ID}")
    print(f"  Token      : {SU_TOKEN[:30]}…")
    print("\n  Next: python check_setup.py  →  python fb_marketplace_sync.py\n")


def _appsecret_proof() -> str:
    import hashlib, hmac
    if not APP_SECRET:
        return ""
    return hmac.new(APP_SECRET.encode(), USER_TOKEN.encode(), hashlib.sha256).hexdigest()


if __name__ == "__main__":
    main()
