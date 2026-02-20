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
import time
from datetime import datetime
from pathlib import Path

import requests
import uvicorn
from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse

import db

load_dotenv()

FB_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN", "")
FB_CATALOG_ID   = os.getenv("FB_CATALOG_ID", "")
FB_API_VERSION  = os.getenv("FB_API_VERSION", "v21.0")
ADDENDUM_AMOUNT = int(os.getenv("ADDENDUM_AMOUNT", "0"))
DASHBOARD_PORT  = int(os.getenv("DASHBOARD_PORT", "8000"))

_TEMPLATE = Path(__file__).parent / "templates" / "dashboard.html"

# Initialise DB on startup
db.init_db()

app = FastAPI(title="Grubbs INFINITI — Marketplace Dashboard", docs_url=None, redoc_url=None)


# ─────────────────────────────────────────────────────────────────────────────
# HTML shell
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
def index():
    return _TEMPLATE.read_text(encoding="utf-8")


# ─────────────────────────────────────────────────────────────────────────────
# REST API
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/summary")
def api_summary():
    return db.get_summary(ADDENDUM_AMOUNT)


@app.get("/api/vehicles")
def api_vehicles(
    make:        str  = Query(default=""),
    condition:   str  = Query(default=""),
    body_style:  str  = Query(default=""),
    year:        str  = Query(default=""),
    search:      str  = Query(default=""),
    active_only: bool = Query(default=True),
):
    vehicles = db.get_vehicles(
        make=make,
        condition=condition,
        body_style=body_style,
        year=year,
        search=search,
        active_only=active_only,
    )
    for v in vehicles:
        p = v.get("price_dollars")
        v["addendum_amount"]     = ADDENDUM_AMOUNT
        v["price_with_addendum"] = (p + ADDENDUM_AMOUNT) if p is not None else None
        v["price_ok"]            = p is not None
    return {"vehicles": vehicles, "count": len(vehicles)}


@app.get("/api/sync-runs")
def api_sync_runs(limit: int = Query(default=25)):
    return {"runs": db.get_sync_runs(limit)}


@app.get("/api/makes")
def api_makes():
    return {"makes": db.get_makes()}


@app.get("/api/years")
def api_years():
    return {"years": db.get_years()}


# ── Sync trigger ─────────────────────────────────────────────────────────────
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
    _sync["running"]    = True
    _sync["started_at"] = datetime.utcnow().isoformat(timespec="seconds")
    _sync["last_message"] = "Running …"
    try:
        result = subprocess.run(
            [sys.executable, str(Path(__file__).parent / "fb_marketplace_sync.py")],
            capture_output=True,
            text=True,
            timeout=600,
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


# ── Facebook stats refresh ───────────────────────────────────────────────────
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
    """Pull vehicle-level insights from the Meta Graph API and store them."""
    _fb_stats["running"] = True
    _fb_stats["last_message"] = "Fetching catalog items …"
    base = f"https://graph.facebook.com/{FB_API_VERSION}"

    try:
        # ── 1. List all vehicles in the catalog ──────────────────────────────
        all_items: list[dict] = []
        url = f"{base}/{FB_CATALOG_ID}/vehicles"
        params = {
            "access_token": FB_ACCESS_TOKEN,
            "fields": "id,vehicle_id,retailer_id,title",
            "limit": 200,
        }
        while url:
            resp = requests.get(url, params=params, timeout=30)
            data = resp.json()
            if "error" in data:
                _fb_stats["last_message"] = f"API error: {data['error'].get('message')}"
                return
            all_items.extend(data.get("data", []))
            next_page = data.get("paging", {}).get("next")
            url    = next_page if next_page else None
            params = {}          # next page URL already has all params

        _fb_stats["last_message"] = f"Got {len(all_items)} catalog items. Fetching insights …"

        # ── 2. Fetch insights for each item ──────────────────────────────────
        stats: list[dict] = []
        for item in all_items:
            item_id = item.get("id")
            # vehicle_id = VIN we set in the feed; fallback to retailer_id
            vin = item.get("vehicle_id") or item.get("retailer_id") or item_id
            if not item_id or not vin:
                continue

            ins_resp = requests.get(
                f"{base}/{item_id}/insights",
                params={
                    "access_token": FB_ACCESS_TOKEN,
                    "fields": "impressions,link_clicks,saves",
                },
                timeout=15,
            )
            ins = ins_resp.json()
            if "error" not in ins:
                d = ins.get("data", [{}])
                d = d[0] if d else {}
                stats.append({
                    "vin":         vin,
                    "impressions": int(d.get("impressions",  0) or 0),
                    "clicks":      int(d.get("link_clicks",  0) or 0),
                    "saves":       int(d.get("saves",        0) or 0),
                })

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
    print(f"  Addendum  → ${ADDENDUM_AMOUNT:,}")
    print(f"  DB        → {os.path.abspath(os.getenv('DB_PATH', 'inventory.db'))}")
    uvicorn.run("dashboard:app", host="0.0.0.0", port=DASHBOARD_PORT, reload=False)
