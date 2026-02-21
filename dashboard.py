#!/usr/bin/env python3
"""
dashboard.py — Production web dashboard for the Grubbs INFINITI FB Marketplace sync.

Start:
    python dashboard.py                 # runs on port 8000
    DASHBOARD_PORT=9000 python dashboard.py
"""

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import secrets

import requests
import uvicorn
from dotenv import load_dotenv
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Path as FPath, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel
from starlette.middleware.sessions import SessionMiddleware

import db

load_dotenv()

FB_ACCESS_TOKEN  = os.getenv("FB_ACCESS_TOKEN", "")
FB_CATALOG_ID    = os.getenv("FB_CATALOG_ID", "")
FB_API_VERSION   = os.getenv("FB_API_VERSION", "v21.0")
_ENV_ADDENDUM    = int(os.getenv("ADDENDUM_AMOUNT", "0"))   # env fallback only
DASHBOARD_PORT   = int(os.getenv("PORT", os.getenv("DASHBOARD_PORT", "8000")))

_TEMPLATE = Path(__file__).parent / "templates" / "dashboard.html"

db.init_db()

_SECRET_KEY = os.getenv("SECRET_KEY") or secrets.token_hex(32)

app = FastAPI(title="Grubbs INFINITI — Marketplace Dashboard", docs_url=None, redoc_url=None)
app.add_middleware(SessionMiddleware, secret_key=_SECRET_KEY, session_cookie="grubbs_session", max_age=86400 * 7)

_LOGIN_TEMPLATE = Path(__file__).parent / "templates" / "login.html"


# ─────────────────────────────────────────────────────────────────────────────
# Auth helpers
# ─────────────────────────────────────────────────────────────────────────────
def _current_user(request: Request) -> dict:
    """FastAPI dependency — returns session user or raises 401."""
    username = request.session.get("username")
    if not username:
        raise HTTPException(status_code=401, detail="Not authenticated")
    user = db.get_user(username)
    if not user:
        request.session.clear()
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


def _admin_user(user: dict = Depends(_current_user)) -> dict:
    """FastAPI dependency — requires admin flag."""
    if not user["is_admin"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _effective_addendum() -> int:
    """Addendum from DB settings, falling back to env var."""
    s = db.get_all_settings(_ENV_ADDENDUM)
    return s["addendum_amount"]


def _enrich_vehicle(v: dict, addendum: int) -> dict:
    """Compute derived fields for a single vehicle dict."""
    ep = v.get("price_override") if v.get("price_override") is not None else v.get("price_dollars")
    ea = v.get("addendum_override") if v.get("addendum_override") is not None else addendum
    v["effective_price"]     = ep
    v["effective_addendum"]  = ea
    v["price_with_addendum"] = (ep + ea) if ep is not None else None
    v["price_overridden"]    = v.get("price_override") is not None
    v["addendum_overridden"] = v.get("addendum_override") is not None
    v["price_ok"]            = ep is not None

    # % to market value
    mv = v.get("market_value")
    if ep and mv and mv > 0:
        v["pct_to_market"] = round((ep - mv) / mv * 100, 1)
    else:
        v["pct_to_market"] = None

    return v


# ─────────────────────────────────────────────────────────────────────────────
# HTML shell
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    if request.session.get("username"):
        return RedirectResponse("/", status_code=302)
    return _LOGIN_TEMPLATE.read_text(encoding="utf-8")


class LoginPayload(BaseModel):
    username: str
    password: str


@app.post("/api/login")
def api_login(payload: LoginPayload, request: Request):
    user = db.verify_password(payload.username, payload.password)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid username or password")
    request.session["username"] = user["username"]
    request.session["is_admin"] = bool(user["is_admin"])
    return {"username": user["username"], "is_admin": bool(user["is_admin"])}


@app.post("/api/logout")
def api_logout(request: Request):
    request.session.clear()
    return {"ok": True}


@app.get("/api/me")
def api_me(user: dict = Depends(_current_user)):
    return {"username": user["username"], "is_admin": bool(user["is_admin"])}


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    if not request.session.get("username"):
        return RedirectResponse("/login", status_code=302)
    return _TEMPLATE.read_text(encoding="utf-8")


# ─────────────────────────────────────────────────────────────────────────────
# User management (admin only)
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/users")
def api_list_users(_admin: dict = Depends(_admin_user)):
    return {"users": db.list_users()}


class CreateUserPayload(BaseModel):
    username: str
    password: str
    is_admin: bool = False


@app.post("/api/users")
def api_create_user(payload: CreateUserPayload, _admin: dict = Depends(_admin_user)):
    if not payload.username.strip():
        raise HTTPException(400, "Username cannot be blank")
    if len(payload.password) < 6:
        raise HTTPException(400, "Password must be at least 6 characters")
    ok = db.create_user(payload.username.strip(), payload.password, payload.is_admin)
    if not ok:
        raise HTTPException(409, f"Username '{payload.username}' already exists")
    return {"ok": True, "users": db.list_users()}


@app.delete("/api/users/{username}")
def api_delete_user(username: str, request: Request, admin: dict = Depends(_admin_user)):
    if username.lower() == admin["username"].lower():
        raise HTTPException(400, "Cannot delete your own account")
    ok = db.delete_user(username)
    if not ok:
        raise HTTPException(404, f"User '{username}' not found")
    return {"ok": True, "users": db.list_users()}


class ChangePasswordPayload(BaseModel):
    new_password: str


@app.post("/api/users/{username}/password")
def api_change_password(username: str, payload: ChangePasswordPayload, admin: dict = Depends(_admin_user)):
    if len(payload.new_password) < 6:
        raise HTTPException(400, "Password must be at least 6 characters")
    ok = db.change_password(username, payload.new_password)
    if not ok:
        raise HTTPException(404, f"User '{username}' not found")
    return {"ok": True}


# ─────────────────────────────────────────────────────────────────────────────
# Settings
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/settings")
def api_get_settings(_: dict = Depends(_current_user)):
    return db.get_all_settings(_ENV_ADDENDUM)


class SettingsPayload(BaseModel):
    addendum_amount:          Optional[int] = None
    marketcheck_api_key:      Optional[str] = None
    marketcheck_api_secret:   Optional[str] = None
    dealer_zip:               Optional[str] = None
    market_radius:            Optional[int] = None


@app.post("/api/settings")
def api_save_settings(payload: SettingsPayload, _: dict = Depends(_current_user)):
    if payload.addendum_amount is not None:
        if payload.addendum_amount < 0:
            raise HTTPException(400, "addendum_amount must be >= 0")
        db.set_setting("addendum_amount", str(payload.addendum_amount))
    if payload.marketcheck_api_key is not None:
        db.set_setting("marketcheck_api_key", payload.marketcheck_api_key.strip())
    if payload.marketcheck_api_secret is not None:
        db.set_setting("marketcheck_api_secret", payload.marketcheck_api_secret.strip())
    if payload.dealer_zip is not None:
        db.set_setting("dealer_zip", payload.dealer_zip.strip())
    if payload.market_radius is not None:
        db.set_setting("market_radius", str(max(10, min(500, payload.market_radius))))
    return db.get_all_settings(_ENV_ADDENDUM)


# ─────────────────────────────────────────────────────────────────────────────
# Vehicles
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/vehicles")
def api_vehicles(
    make:        str  = Query(default=""),
    condition:   str  = Query(default=""),
    body_style:  str  = Query(default=""),
    year:        str  = Query(default=""),
    search:      str  = Query(default=""),
    active_only: bool = Query(default=True),
    _: dict = Depends(_current_user),
):
    addendum = _effective_addendum()
    vehicles = db.get_vehicles(
        make=make, condition=condition, body_style=body_style,
        year=year, search=search, active_only=active_only,
    )
    return {"vehicles": [_enrich_vehicle(v, addendum) for v in vehicles],
            "count": len(vehicles),
            "addendum_amount": addendum}


@app.get("/api/summary")
def api_summary(_: dict = Depends(_current_user)):
    return db.get_summary(_effective_addendum())


class VehicleUpdatePayload(BaseModel):
    price_override:    Optional[int]   = None
    clear_price:       bool             = False   # set True to remove override
    addendum_override: Optional[int]   = None
    clear_addendum:    bool             = False
    market_value:      Optional[int]   = None
    clear_market_value: bool            = False
    notes:             Optional[str]   = None


@app.post("/api/vehicle/{vin}/update")
def api_update_vehicle(vin: str, payload: VehicleUpdatePayload, _: dict = Depends(_current_user)):
    fields: dict = {}
    if payload.clear_price:
        fields["price_override"] = None
    elif payload.price_override is not None:
        fields["price_override"] = payload.price_override

    if payload.clear_addendum:
        fields["addendum_override"] = None
    elif payload.addendum_override is not None:
        fields["addendum_override"] = payload.addendum_override

    if payload.clear_market_value:
        fields["market_value"] = None
    elif payload.market_value is not None:
        fields["market_value"] = payload.market_value

    if payload.notes is not None:
        fields["notes"] = payload.notes

    if not db.update_vehicle_fields(vin, fields):
        raise HTTPException(404, f"VIN {vin} not found")

    # Return enriched vehicle
    rows = db.get_vehicles(search=vin, active_only=False)
    if not rows:
        raise HTTPException(404, f"VIN {vin} not found")
    return _enrich_vehicle(rows[0], _effective_addendum())


@app.get("/api/comparable/{vin}")
def api_comparable(vin: str, _: dict = Depends(_current_user)):
    comps = db.get_comparable_vehicles(vin)
    addendum = _effective_addendum()
    enriched = []
    for c in comps:
        ep = c.get("effective_price")
        enriched.append({**c, "addendum_amount": addendum,
                         "price_with_addendum": (ep + addendum) if ep else None})
    return {"comparables": enriched, "count": len(enriched)}


# ─────────────────────────────────────────────────────────────────────────────
# Cox Auto report import
# ─────────────────────────────────────────────────────────────────────────────
class CoxImportPayload(BaseModel):
    raw_text: str


@app.post("/api/cox-import")
def api_cox_import(payload: CoxImportPayload, _: dict = Depends(_current_user)):
    if not payload.raw_text.strip():
        raise HTTPException(400, "raw_text is empty")
    records = db.parse_cox_report(payload.raw_text)
    if not records:
        raise HTTPException(400, "No VINs found in report text — make sure you pasted the full Cox report")
    result = db.cox_import(records)
    return {**result, "parsed": len(records)}


# ─────────────────────────────────────────────────────────────────────────────
# Deals (saved pencil history)
# ─────────────────────────────────────────────────────────────────────────────
class SaveDealPayload(BaseModel):
    vin:             str
    customer_name:   str           = ""
    base_price:      Optional[int] = None
    addendum_amount: int           = 0
    tax_rate:        float         = 0
    doc_fee:         int           = 0
    down_payment:    int           = 0
    apr:             float         = 0
    term_months:     int           = 72
    out_the_door:    Optional[int] = None
    amount_financed: Optional[int] = None
    monthly_payment: Optional[float] = None
    gross:           Optional[int] = None
    notes:           str           = ""


@app.post("/api/deals")
def api_save_deal(payload: SaveDealPayload, user: dict = Depends(_current_user)):
    deal_id = db.save_deal({**payload.dict(), "created_by": user["username"]})
    return {"ok": True, "id": deal_id}


@app.get("/api/deals")
def api_get_deals(
    vin:   str = Query(default=""),
    limit: int = Query(default=50),
    _: dict = Depends(_current_user),
):
    return {"deals": db.get_deals(vin=vin, limit=limit)}


# ─────────────────────────────────────────────────────────────────────────────
# Market comps (MarketCheck API)
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/market-comps/{vin}")
def api_market_comps(vin: str = FPath(...), _: dict = Depends(_current_user)):
    settings  = db.get_all_settings(_ENV_ADDENDUM)
    api_key   = settings.get("marketcheck_api_key", "")
    dealer_zip = settings.get("dealer_zip", "")
    radius    = settings.get("market_radius", 150)

    if not api_key:
        raise HTTPException(400, "MarketCheck API key not configured — add it in Settings")
    if not dealer_zip:
        raise HTTPException(400, "Dealer ZIP not configured — add it in Settings")

    rows = db.get_vehicles(search=vin, active_only=False)
    if not rows:
        raise HTTPException(404, f"VIN {vin} not found")
    v = rows[0]

    make  = v.get("make", "")
    model = v.get("model", "")
    our_price = v.get("price_override") if v.get("price_override") is not None else v.get("price_dollars")

    if not make or not model:
        raise HTTPException(400, "Vehicle is missing make/model data")

    # ── Cache check (same make/model/zip/radius share one daily call) ─────────
    cache_key = f"{make.lower()}|{model.lower()}|{dealer_zip}|{radius}"
    cached = db.get_comps_cache(cache_key)
    if cached is not None:
        # Re-compute price diffs against this vehicle's current price
        for c in cached:
            p = c.get("price")
            c["beats_us"]   = (p < our_price) if (p and our_price) else None
            c["price_diff"] = (p - our_price) if (p and our_price) else None
        return {
            "listings":  cached,
            "count":     len(cached),
            "our_price": our_price,
            "cached":    True,
            "search":    {"make": make, "model": model, "zip": dealer_zip, "radius": radius},
        }

    try:
        resp = requests.get(
            "https://api.marketcheck.com/v2/search/car/active",
            params={
                "api_key":    api_key,
                "make":       make,
                "model":      model,
                "car_type":   "used",
                "zip":        dealer_zip,
                "radius":     radius,
                "rows":       30,
                "sort_by":    "price",
                "sort_order": "asc",
            },
            timeout=15,
        )
        data = resp.json()
    except Exception as exc:
        raise HTTPException(502, f"MarketCheck request failed: {exc}")

    if resp.status_code != 200:
        msg = data.get("message") or data.get("error", {}).get("message", "") or resp.text[:200]
        raise HTTPException(502, f"MarketCheck ({resp.status_code}): {msg}")

    if "error" in data:
        raise HTTPException(502, f"MarketCheck: {data['error'].get('message', 'Unknown error')}")

    listings = data.get("listings", [])
    enriched = []
    for lst in listings:
        price  = lst.get("price")
        dealer = lst.get("dealer") or {}
        enriched.append({
            "vin":            lst.get("vin", ""),
            "heading":        lst.get("heading", ""),
            "price":          price,
            "miles":          lst.get("miles"),
            "trim":           lst.get("trim", ""),
            "year":           lst.get("year"),
            "exterior_color": lst.get("exterior_color", ""),
            "dealer_name":    dealer.get("name", ""),
            "dealer_city":    dealer.get("city", ""),
            "dealer_state":   dealer.get("state", ""),
            "dom":            lst.get("dom"),
            "vdp_url":        lst.get("vdp_url", ""),
            "beats_us":       (price < our_price) if (price and our_price) else None,
            "price_diff":     (price - our_price) if (price and our_price) else None,
        })

    db.set_comps_cache(cache_key, enriched)

    return {
        "listings":  enriched,
        "count":     len(enriched),
        "our_price": our_price,
        "cached":    False,
        "search":    {"make": make, "model": model, "zip": dealer_zip, "radius": radius},
    }


# ─────────────────────────────────────────────────────────────────────────────
# VIN Decode  (MarketCheck — cached permanently)
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/vin-decode/{vin}")
def api_vin_decode(vin: str = FPath(...), _: dict = Depends(_current_user)):
    cached = db.get_vin_cache(vin)
    if cached and cached["specs"]:
        return {"specs": cached["specs"], "cached": True}

    settings = db.get_all_settings(_ENV_ADDENDUM)
    api_key  = settings.get("marketcheck_api_key", "")
    if not api_key:
        raise HTTPException(400, "MarketCheck API key not configured — add it in Settings")

    try:
        resp = requests.get(
            f"https://api.marketcheck.com/v2/decode/car/{vin}/specs",
            params={"api_key": api_key},
            timeout=15,
        )
        data = resp.json()
    except Exception as exc:
        raise HTTPException(502, f"MarketCheck VIN decode failed: {exc}")

    if resp.status_code != 200:
        msg = data.get("message") or data.get("error", {}).get("message", "") or resp.text[:200]
        raise HTTPException(502, f"MarketCheck ({resp.status_code}): {msg}")

    specs = data if isinstance(data, list) else data.get("specs", [])
    db.set_vin_cache(vin, specs=specs)
    return {"specs": specs, "cached": False}


# ─────────────────────────────────────────────────────────────────────────────
# Window Sticker  (MarketCheck — cached permanently)
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/window-sticker/{vin}")
def api_window_sticker(vin: str = FPath(...), _: dict = Depends(_current_user)):
    cached = db.get_vin_cache(vin)
    if cached and cached["sticker_url"]:
        return {"sticker_url": cached["sticker_url"], "cached": True}

    settings = db.get_all_settings(_ENV_ADDENDUM)
    api_key  = settings.get("marketcheck_api_key", "")
    if not api_key:
        raise HTTPException(400, "MarketCheck API key not configured — add it in Settings")

    try:
        resp = requests.get(
            f"https://api.marketcheck.com/v2/sticker/car/{vin}",
            params={"api_key": api_key},
            timeout=15,
        )
        data = resp.json()
    except Exception as exc:
        raise HTTPException(502, f"MarketCheck sticker request failed: {exc}")

    if resp.status_code != 200:
        msg = data.get("message") or data.get("error", {}).get("message", "") or resp.text[:200]
        raise HTTPException(502, f"MarketCheck ({resp.status_code}): {msg}")

    sticker_url = data.get("url") or data.get("sticker_url", "")
    if sticker_url:
        db.set_vin_cache(vin, sticker_url=sticker_url)
    return {"sticker_url": sticker_url, "cached": False}


# ─────────────────────────────────────────────────────────────────────────────
# Sync history
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/sync-runs")
def api_sync_runs(limit: int = Query(default=25), _: dict = Depends(_current_user)):
    return {"runs": db.get_sync_runs(limit)}


@app.get("/api/makes")
def api_makes(_: dict = Depends(_current_user)):
    return {"makes": db.get_makes()}


@app.get("/api/years")
def api_years(_: dict = Depends(_current_user)):
    return {"years": db.get_years()}


# ─────────────────────────────────────────────────────────────────────────────
# Sync trigger
# ─────────────────────────────────────────────────────────────────────────────
_sync: dict = {"running": False, "last_message": "Never run from dashboard", "started_at": None}


@app.get("/api/sync-status")
def api_sync_status(_: dict = Depends(_current_user)):
    return _sync


@app.post("/api/trigger-sync")
def api_trigger_sync(background_tasks: BackgroundTasks, _: dict = Depends(_current_user)):
    if _sync["running"]:
        return JSONResponse({"error": "Sync already in progress"}, status_code=409)
    background_tasks.add_task(_run_sync)
    return {"status": "started", "message": "Sync started — refresh in ~60 s"}


def _run_sync():
    _sync["running"]      = True
    _sync["started_at"]   = datetime.utcnow().isoformat(timespec="seconds")
    _sync["last_message"] = "Running …"
    try:
        result = subprocess.run(
            [sys.executable, str(Path(__file__).parent / "fb_marketplace_sync.py")],
            capture_output=True, text=True, timeout=600,
            cwd=str(Path(__file__).parent),
        )
        if result.returncode == 0:
            _sync["last_message"] = f"Completed at {datetime.utcnow().strftime('%H:%M:%S UTC')}"
        else:
            tail = (result.stderr or result.stdout or "")[-400:].strip()
            _sync["last_message"] = f"Error (rc={result.returncode}): {tail}"
    except subprocess.TimeoutExpired:
        _sync["last_message"] = "Timed out after 10 minutes"
    except Exception as exc:
        _sync["last_message"] = f"Exception: {exc}"
    finally:
        _sync["running"] = False


# ─────────────────────────────────────────────────────────────────────────────
# Facebook stats refresh
# ─────────────────────────────────────────────────────────────────────────────
_fb_stats: dict = {"running": False, "last_message": "Never fetched", "fetched_at": None}


@app.get("/api/fb-stats-status")
def api_fb_stats_status(_: dict = Depends(_current_user)):
    return _fb_stats


@app.post("/api/refresh-fb-stats")
def api_refresh_fb_stats(background_tasks: BackgroundTasks, _: dict = Depends(_current_user)):
    if not FB_ACCESS_TOKEN:
        raise HTTPException(400, "FB_ACCESS_TOKEN not configured in .env")
    if not FB_CATALOG_ID:
        raise HTTPException(400, "FB_CATALOG_ID not configured in .env")
    if _fb_stats["running"]:
        return JSONResponse({"error": "Stats refresh already running"}, status_code=409)
    background_tasks.add_task(_fetch_fb_stats)
    return {"status": "started"}


def _fetch_fb_stats():
    _fb_stats["running"]      = True
    _fb_stats["last_message"] = "Fetching catalog items …"
    base = f"https://graph.facebook.com/{FB_API_VERSION}"
    try:
        all_items: list[dict] = []
        url = f"{base}/{FB_CATALOG_ID}/vehicles"
        params = {"access_token": FB_ACCESS_TOKEN,
                  "fields": "id,vehicle_id,retailer_id,title", "limit": 200}
        while url:
            resp = requests.get(url, params=params, timeout=30)
            data = resp.json()
            if "error" in data:
                _fb_stats["last_message"] = f"API error: {data['error'].get('message')}"
                return
            all_items.extend(data.get("data", []))
            url    = data.get("paging", {}).get("next")
            params = {}

        _fb_stats["last_message"] = f"Got {len(all_items)} items. Fetching insights …"
        stats: list[dict] = []
        for item in all_items:
            item_id = item.get("id")
            vin     = item.get("vehicle_id") or item.get("retailer_id") or item_id
            if not item_id or not vin:
                continue
            ins = requests.get(f"{base}/{item_id}/insights",
                               params={"access_token": FB_ACCESS_TOKEN,
                                       "fields": "impressions,link_clicks,saves"},
                               timeout=15).json()
            if "error" not in ins:
                d = (ins.get("data") or [{}])[0]
                stats.append({"vin": vin,
                               "impressions": int(d.get("impressions",  0) or 0),
                               "clicks":      int(d.get("link_clicks",  0) or 0),
                               "saves":       int(d.get("saves",        0) or 0)})
        if stats:
            db.upsert_vehicle_stats(stats)
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        _fb_stats["last_message"] = f"Updated {len(stats)}/{len(all_items)} vehicles at {now}"
        _fb_stats["fetched_at"]   = now
    except Exception as exc:
        _fb_stats["last_message"] = f"Exception: {exc}"
    finally:
        _fb_stats["running"] = False


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"  Dashboard → http://localhost:{DASHBOARD_PORT}")
    print(f"  Addendum  → ${_effective_addendum():,} (from {'DB' if db.get_setting('addendum_amount') else 'env'})")
    print(f"  DB        → {os.path.abspath(os.getenv('DB_PATH', 'inventory.db'))}")
    uvicorn.run("dashboard:app", host="0.0.0.0", port=DASHBOARD_PORT, reload=False)
