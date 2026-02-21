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
from datetime import datetime, timedelta
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

    # Days on lot (timezone-aware)
    try:
        from datetime import timezone as _tz
        fs = datetime.fromisoformat(v.get("first_seen", ""))
        if fs.tzinfo is None:
            fs = fs.replace(tzinfo=_tz.utc)
        v["days_on_lot"] = max(0, (datetime.now(_tz.utc) - fs).days)
    except Exception:
        v["days_on_lot"] = 0

    # Inventory intelligence flags
    body = (v.get("body_style") or "").lower()
    text_blob = " ".join([str(v.get("title") or ""), str(v.get("trim") or ""), str(v.get("model") or "")]).lower()
    three_row_terms = ("3rd row", "third row", "7-pass", "7 pass", "8-pass", "captain chairs", "3-row")
    v["is_three_row_suv"] = ("suv" in body) and any(t in text_blob for t in three_row_terms)

    # Price war: we're priced above market value
    v["price_war_alert"] = bool(ep and mv and mv > 0 and ep > mv)

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
    smtp_host:                Optional[str] = None
    smtp_port:                Optional[int] = None
    smtp_user:                Optional[str] = None
    smtp_pass:                Optional[str] = None
    digest_email:             Optional[str] = None


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
    if payload.smtp_host is not None:
        db.set_setting("smtp_host", payload.smtp_host.strip())
    if payload.smtp_port is not None:
        db.set_setting("smtp_port", str(max(1, min(65535, payload.smtp_port))))
    if payload.smtp_user is not None:
        db.set_setting("smtp_user", payload.smtp_user.strip())
    if payload.smtp_pass is not None:
        db.set_setting("smtp_pass", payload.smtp_pass)
    if payload.digest_email is not None:
        db.set_setting("digest_email", payload.digest_email.strip())
    return db.get_all_settings(_ENV_ADDENDUM)


# ─────────────────────────────────────────────────────────────────────────────
# Weekly Email Digest
# ─────────────────────────────────────────────────────────────────────────────
@app.post("/api/send-digest")
def api_send_digest(_: dict = Depends(_current_user)):
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    s = db.get_all_settings(_ENV_ADDENDUM)
    smtp_host    = s.get("smtp_host", "").strip()
    smtp_port    = int(s.get("smtp_port") or 587)
    smtp_user    = s.get("smtp_user", "").strip()
    smtp_pass    = s.get("smtp_pass", "")
    digest_email = s.get("digest_email", "").strip()

    if not smtp_host or not smtp_user or not digest_email:
        raise HTTPException(400, "Configure SMTP host, username, and recipient email in Settings first.")

    addendum = _effective_addendum()
    vehicles = db.get_vehicles(active_only=True)
    enriched = [_enrich_vehicle(v, addendum) for v in vehicles]

    total     = len(enriched)
    priced    = sum(1 for v in enriched if v.get("price_ok"))
    top_fb    = sorted(enriched, key=lambda v: v.get("fb_clicks", 0), reverse=True)[:5]
    aging     = [v for v in enriched if v.get("days_on_lot", 0) >= 60]
    aging     = sorted(aging, key=lambda v: v.get("days_on_lot", 0), reverse=True)[:10]
    no_price  = [v for v in enriched if not v.get("price_ok")]
    avg_price = (sum(v["effective_price"] for v in enriched if v.get("price_ok")) // priced) if priced else 0

    def veh_name(v):
        return f"{v.get('year','')} {v.get('make','')} {v.get('model','')}".strip()

    def money(n):
        return f"${n:,.0f}" if n else "—"

    rows_top = "".join(
        f"<tr><td>{veh_name(v)}</td><td style='text-align:right'>{v.get('fb_clicks',0)}</td>"
        f"<td style='text-align:right'>{v.get('fb_impressions',0):,}</td>"
        f"<td style='text-align:right'>{money(v.get('effective_price'))}</td></tr>"
        for v in top_fb
    )
    rows_aging = "".join(
        f"<tr><td>{veh_name(v)}</td><td style='text-align:right;color:#dc2626'>{v.get('days_on_lot',0)} days</td>"
        f"<td style='text-align:right'>{money(v.get('effective_price'))}</td>"
        f"<td style='text-align:right'>{v.get('stock_number','—')}</td></tr>"
        for v in aging
    ) or "<tr><td colspan='4' style='color:#6b7280'>None — great job!</td></tr>"

    date_str = datetime.utcnow().strftime("%B %d, %Y")
    html_body = f"""
<!doctype html><html><head><meta charset="utf-8"/>
<style>
body{{font-family:Arial,sans-serif;background:#f1f5f9;color:#0f172a;margin:0;padding:0}}
.wrap{{max-width:640px;margin:24px auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,.08)}}
.hd{{background:#0f172a;padding:24px 28px;color:#fff}}
.hd h1{{margin:0;font-size:20px;font-weight:800}}
.hd p{{margin:4px 0 0;font-size:12px;color:#94a3b8}}
.stats{{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;padding:20px 28px}}
.stat{{background:#f8fafc;border-radius:8px;padding:14px;text-align:center}}
.stat .n{{font-size:28px;font-weight:800;color:#1877F2}}
.stat .l{{font-size:11px;color:#64748b;text-transform:uppercase;font-weight:600;margin-top:2px}}
.section{{padding:0 28px 20px}}
.section h2{{font-size:14px;font-weight:700;color:#0f172a;margin:0 0 10px;border-bottom:1px solid #e2e8f0;padding-bottom:8px}}
table{{width:100%;border-collapse:collapse;font-size:13px}}
th{{text-align:left;font-weight:600;color:#64748b;padding:6px 0;font-size:11px;text-transform:uppercase}}
td{{padding:7px 0;border-bottom:1px solid #f1f5f9;vertical-align:top}}
.foot{{padding:16px 28px;text-align:center;font-size:11px;color:#94a3b8;border-top:1px solid #f1f5f9}}
</style></head><body>
<div class="wrap">
  <div class="hd"><h1>Grubbs INFINITI — Weekly Digest</h1><p>{date_str}</p></div>
  <div class="stats">
    <div class="stat"><div class="n">{total}</div><div class="l">Active</div></div>
    <div class="stat"><div class="n">{priced}</div><div class="l">Priced</div></div>
    <div class="stat"><div class="n">{money(avg_price)}</div><div class="l">Avg Price</div></div>
  </div>
  <div class="section">
    <h2>🔥 Top FB Performers (Clicks)</h2>
    <table>
      <tr><th>Vehicle</th><th style="text-align:right">Clicks</th><th style="text-align:right">Views</th><th style="text-align:right">Price</th></tr>
      {rows_top}
    </table>
  </div>
  <div class="section">
    <h2>⏰ Aging Units (60+ Days)</h2>
    <table>
      <tr><th>Vehicle</th><th style="text-align:right">Days</th><th style="text-align:right">Price</th><th style="text-align:right">Stock</th></tr>
      {rows_aging}
    </table>
  </div>
  {'<div class="section"><h2>⚠️ No Price Set</h2><p style="font-size:13px;color:#64748b">' + ", ".join(veh_name(v) for v in no_price[:10]) + ('…' if len(no_price) > 10 else '') + '</p></div>' if no_price else ''}
  <div class="foot">Sent from Grubbs INFINITI FB Marketplace Dashboard · {date_str}</div>
</div></body></html>"""

    recipients = [e.strip() for e in digest_email.split(",") if e.strip()]
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Weekly Inventory Digest — {date_str}"
    msg["From"]    = smtp_user
    msg["To"]      = ", ".join(recipients)
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=15) as server:
            server.ehlo()
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, recipients, msg.as_string())
    except Exception as exc:
        raise HTTPException(500, f"SMTP error: {exc}")

    return {"message": f"Digest sent to {', '.join(recipients)}"}


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
    three_row:   bool = Query(default=False),
    _: dict = Depends(_current_user),
):
    addendum = _effective_addendum()
    vehicles = db.get_vehicles(
        make=make, condition=condition, body_style=body_style,
        year=year, search=search, active_only=active_only,
        three_row=three_row,
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


@app.get("/api/deal-history")
def api_deal_history(limit: int = Query(default=100), _: dict = Depends(_current_user)):
    return {"deals": db.get_deal_history(limit)}


@app.post("/api/weekly-digest")
def api_weekly_digest(_: dict = Depends(_current_user)):
    summary = db.get_summary(_effective_addendum())
    vehicles = [
        _enrich_vehicle(v, _effective_addendum())
        for v in db.get_vehicles(active_only=True)
    ]
    stale_units = sorted(
        [v for v in vehicles if (v.get("days_on_lot") or 0) >= 45],
        key=lambda x: x.get("days_on_lot") or 0,
        reverse=True,
    )[:10]
    top_units = sorted(vehicles, key=lambda x: x.get("fb_clicks") or 0, reverse=True)[:10]

    digest = {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds"),
        "window_start": (datetime.utcnow() - timedelta(days=7)).date().isoformat(),
        "window_end": datetime.utcnow().date().isoformat(),
        "kpis": {
            "active_listings": summary.get("total_active", 0),
            "fb_clicks": summary.get("total_clicks", 0),
            "fb_impressions": summary.get("total_impressions", 0),
            "fb_saves": summary.get("total_saves", 0),
        },
        "top_performers": [
            {"vin": v.get("vin"), "title": v.get("title"), "clicks": v.get("fb_clicks", 0), "impressions": v.get("fb_impressions", 0)}
            for v in top_units
        ],
        "most_aged": [
            {"vin": v.get("vin"), "title": v.get("title"), "days_on_lot": v.get("days_on_lot", 0), "price": v.get("effective_price")}
            for v in stale_units
        ],
    }
    return {"ok": True, "digest": digest}




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
# Market Value lookup — median from MarketCheck comps (reuses comps cache)
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/market-value/{vin}")
def api_market_value(vin: str = FPath(...), _: dict = Depends(_current_user)):
    import statistics

    settings   = db.get_all_settings(_ENV_ADDENDUM)
    api_key    = settings.get("marketcheck_api_key", "")
    dealer_zip = settings.get("dealer_zip", "")
    radius     = settings.get("market_radius", 150)

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
    if not make or not model:
        raise HTTPException(400, "Vehicle is missing make/model data")

    # ── Reuse the same comps cache as /api/market-comps ───────────────────────
    cache_key = f"{make.lower()}|{model.lower()}|{dealer_zip}|{radius}"
    cached = db.get_comps_cache(cache_key)
    if cached is None:
        # Fetch fresh comps and store in cache
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

        our_price = v.get("price_override") if v.get("price_override") is not None else v.get("price_dollars")
        listings = data.get("listings", [])
        cached = []
        for lst in listings:
            price = lst.get("price")
            dealer = lst.get("dealer") or {}
            cached.append({
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
        db.set_comps_cache(cache_key, cached)

    prices = [c["price"] for c in cached if c.get("price") and c["price"] > 0]
    if not prices:
        raise HTTPException(404, detail="No comparable listings found for this vehicle")

    median_price = round(statistics.median(prices))
    return {
        "market_value": median_price,
        "comp_count":   len(prices),
        "make":         make,
        "model":        model,
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
