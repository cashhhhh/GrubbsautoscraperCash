#!/usr/bin/env python3
"""
dashboard.py — Production web dashboard for the Grubbs INFINITI FB Marketplace sync.

Start:
    python dashboard.py                 # runs on port 8000
    DASHBOARD_PORT=9000 python dashboard.py
"""

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
import uvicorn
from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, HTTPException, Path as FPath, Query
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

import db

load_dotenv()

FB_ACCESS_TOKEN  = os.getenv("FB_ACCESS_TOKEN", "")
FB_CATALOG_ID    = os.getenv("FB_CATALOG_ID", "")
FB_API_VERSION   = os.getenv("FB_API_VERSION", "v21.0")
_ENV_ADDENDUM    = int(os.getenv("ADDENDUM_AMOUNT", "0"))   # env fallback only
DASHBOARD_PORT   = int(os.getenv("PORT", os.getenv("DASHBOARD_PORT", "8000")))

_TEMPLATE = Path(__file__).parent / "templates" / "dashboard.html"

db.init_db()

app = FastAPI(title="Grubbs INFINITI — Marketplace Dashboard", docs_url=None, redoc_url=None)


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
@app.get("/", response_class=HTMLResponse)
def index():
    return _TEMPLATE.read_text(encoding="utf-8")


# ─────────────────────────────────────────────────────────────────────────────
# Settings
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/settings")
def api_get_settings():
    return db.get_all_settings(_ENV_ADDENDUM)


class SettingsPayload(BaseModel):
    addendum_amount: Optional[int] = None


@app.post("/api/settings")
def api_save_settings(payload: SettingsPayload):
    if payload.addendum_amount is not None:
        if payload.addendum_amount < 0:
            raise HTTPException(400, "addendum_amount must be >= 0")
        db.set_setting("addendum_amount", str(payload.addendum_amount))
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
def api_summary():
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
def api_update_vehicle(vin: str, payload: VehicleUpdatePayload):
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
def api_comparable(vin: str):
    comps = db.get_comparable_vehicles(vin)
    addendum = _effective_addendum()
    enriched = []
    for c in comps:
        ep = c.get("effective_price")
        enriched.append({**c, "addendum_amount": addendum,
                         "price_with_addendum": (ep + addendum) if ep else None})
    return {"comparables": enriched, "count": len(enriched)}


# ─────────────────────────────────────────────────────────────────────────────
# Sync history
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/sync-runs")
def api_sync_runs(limit: int = Query(default=25)):
    return {"runs": db.get_sync_runs(limit)}


@app.get("/api/makes")
def api_makes():
    return {"makes": db.get_makes()}


@app.get("/api/years")
def api_years():
    return {"years": db.get_years()}


# ─────────────────────────────────────────────────────────────────────────────
# Sync trigger
# ─────────────────────────────────────────────────────────────────────────────
_sync: dict = {"running": False, "last_message": "Never run from dashboard", "started_at": None}


@app.get("/api/sync-status")
def api_sync_status():
    return _sync


@app.post("/api/trigger-sync")
def api_trigger_sync(background_tasks: BackgroundTasks):
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
def api_fb_stats_status():
    return _fb_stats


@app.post("/api/refresh-fb-stats")
def api_refresh_fb_stats(background_tasks: BackgroundTasks):
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
