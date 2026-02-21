"""
db.py — SQLite persistence layer for the FB Marketplace Dashboard.

Tables
------
  vehicles       – every vehicle ever seen; is_active=0 if dropped from feed
  sync_runs      – one row per fb_marketplace_sync.py execution
  vehicle_stats  – FB impressions/clicks/saves, one row per (vin, date)
  settings       – key/value store for dashboard config (addendum, etc.)
"""

import json
import os
import re
import sqlite3
from datetime import datetime, timezone

import bcrypt as _bcrypt


def _hash_password(password: str) -> str:
    return _bcrypt.hashpw(password.encode(), _bcrypt.gensalt()).decode()


def _verify_password(password: str, hashed: str) -> bool:
    return _bcrypt.checkpw(password.encode(), hashed.encode())

DB_PATH = os.getenv("DB_PATH", "inventory.db")


# ─────────────────────────────────────────────────────────────────────────────
# Connection helper
# ─────────────────────────────────────────────────────────────────────────────
def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


# ─────────────────────────────────────────────────────────────────────────────
# Schema
# ─────────────────────────────────────────────────────────────────────────────
def init_db() -> None:
    with _conn() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS vehicles (
            vin              TEXT PRIMARY KEY,
            title            TEXT,
            stock_number     TEXT,
            year             INTEGER,
            make             TEXT,
            model            TEXT,
            trim             TEXT,
            condition        TEXT    DEFAULT 'used',
            body_style       TEXT    DEFAULT '',
            mileage          INTEGER DEFAULT 0,
            exterior_color   TEXT    DEFAULT '',
            price_dollars    INTEGER,
            image_url        TEXT    DEFAULT '',
            link             TEXT    DEFAULT '',
            first_seen       TEXT    NOT NULL,
            last_seen        TEXT    NOT NULL,
            is_active        INTEGER DEFAULT 1,
            price_override   INTEGER,
            addendum_override INTEGER,
            market_value     INTEGER,
            notes            TEXT    DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS sync_runs (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at              TEXT NOT NULL,
            vehicles_found      INTEGER DEFAULT 0,
            vehicles_priced     INTEGER DEFAULT 0,
            vehicles_uploaded   INTEGER DEFAULT 0,
            duration_seconds    REAL    DEFAULT 0,
            success             INTEGER DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS vehicle_stats (
            vin          TEXT NOT NULL,
            stat_date    TEXT NOT NULL,
            impressions  INTEGER DEFAULT 0,
            clicks       INTEGER DEFAULT 0,
            saves        INTEGER DEFAULT 0,
            PRIMARY KEY (vin, stat_date)
        );

        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS users (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            username      TEXT UNIQUE NOT NULL COLLATE NOCASE,
            password_hash TEXT NOT NULL,
            is_admin      INTEGER DEFAULT 0,
            created_at    TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS deals (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            vin             TEXT NOT NULL,
            created_at      TEXT NOT NULL,
            created_by      TEXT DEFAULT '',
            customer_name   TEXT DEFAULT '',
            base_price      INTEGER,
            addendum_amount INTEGER DEFAULT 0,
            tax_rate        REAL DEFAULT 0,
            doc_fee         INTEGER DEFAULT 0,
            down_payment    INTEGER DEFAULT 0,
            apr             REAL DEFAULT 0,
            term_months     INTEGER DEFAULT 72,
            out_the_door    INTEGER,
            amount_financed INTEGER,
            monthly_payment REAL,
            gross           INTEGER,
            notes           TEXT DEFAULT ''
        );

        CREATE INDEX IF NOT EXISTS idx_vs_vin   ON vehicle_stats(vin);
        CREATE INDEX IF NOT EXISTS idx_deals_vin ON deals(vin);
        CREATE INDEX IF NOT EXISTS idx_vs_date  ON vehicle_stats(stat_date);
        CREATE INDEX IF NOT EXISTS idx_v_make   ON vehicles(make);
        CREATE INDEX IF NOT EXISTS idx_v_active ON vehicles(is_active);

        CREATE TABLE IF NOT EXISTS comps_cache (
            cache_key   TEXT PRIMARY KEY,
            data        TEXT NOT NULL,
            fetched_at  TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS vin_cache (
            vin         TEXT PRIMARY KEY,
            specs_json  TEXT NOT NULL DEFAULT '[]',
            sticker_url TEXT NOT NULL DEFAULT '',
            fetched_at  TEXT NOT NULL
        );
        """)

    # Migrate existing DBs that are missing the new columns
    _migrate()
    _seed_admin()


def _migrate() -> None:
    """Add new columns to existing databases without breaking anything."""
    migrations = [
        "ALTER TABLE vehicles ADD COLUMN price_override       INTEGER",
        "ALTER TABLE vehicles ADD COLUMN addendum_override    INTEGER",
        "ALTER TABLE vehicles ADD COLUMN market_value         INTEGER",
        "ALTER TABLE vehicles ADD COLUMN notes                TEXT DEFAULT ''",
        "ALTER TABLE vehicles ADD COLUMN price_scrape_attempts INTEGER DEFAULT 0",
        "ALTER TABLE vehicles ADD COLUMN cost                 INTEGER",
        "ALTER TABLE vehicles ADD COLUMN pack                 INTEGER DEFAULT 0",
        "ALTER TABLE vehicles ADD COLUMN cox_adj_cost_to_market REAL",
        "ALTER TABLE vehicles ADD COLUMN cox_report_date      TEXT",
    ]
    with _conn() as c:
        for sql in migrations:
            try:
                c.execute(sql)
            except sqlite3.OperationalError:
                pass  # column already exists — that's fine


# ─────────────────────────────────────────────────────────────────────────────
# Users
# ─────────────────────────────────────────────────────────────────────────────
def _seed_admin() -> None:
    """Create the default admin accounts if they don't exist yet."""
    if not get_user("Cash"):
        create_user("Cash", "Cash1345", is_admin=True)
    if not get_user("JT"):
        create_user("JT", "Test1234$", is_admin=True)


def get_user(username: str) -> dict | None:
    with _conn() as c:
        row = c.execute(
            "SELECT id, username, password_hash, is_admin, created_at FROM users WHERE username=? COLLATE NOCASE",
            (username,),
        ).fetchone()
    return dict(row) if row else None


def verify_password(username: str, password: str) -> dict | None:
    """Return the user dict if credentials are valid, else None."""
    user = get_user(username)
    if user and _verify_password(password, user["password_hash"]):
        return user
    return None


def create_user(username: str, password: str, is_admin: bool = False) -> bool:
    """Hash *password* and insert a new user. Returns False if username taken."""
    hashed = _hash_password(password)
    now = datetime.utcnow().isoformat(timespec="seconds")
    try:
        with _conn() as c:
            c.execute(
                "INSERT INTO users (username, password_hash, is_admin, created_at) VALUES (?,?,?,?)",
                (username, hashed, 1 if is_admin else 0, now),
            )
        return True
    except sqlite3.IntegrityError:
        return False  # username already exists


def delete_user(username: str) -> bool:
    with _conn() as c:
        cur = c.execute("DELETE FROM users WHERE username=? COLLATE NOCASE", (username,))
    return cur.rowcount > 0


def list_users() -> list[dict]:
    with _conn() as c:
        rows = c.execute(
            "SELECT id, username, is_admin, created_at FROM users ORDER BY id"
        ).fetchall()
    return [dict(r) for r in rows]


def change_password(username: str, new_password: str) -> bool:
    hashed = _hash_password(new_password)
    with _conn() as c:
        cur = c.execute(
            "UPDATE users SET password_hash=? WHERE username=? COLLATE NOCASE",
            (hashed, username),
        )
    return cur.rowcount > 0


# ─────────────────────────────────────────────────────────────────────────────
# Settings
# ─────────────────────────────────────────────────────────────────────────────
def get_setting(key: str, default: str = "") -> str:
    with _conn() as c:
        row = c.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(key: str, value: str) -> None:
    with _conn() as c:
        c.execute("""
            INSERT INTO settings (key, value) VALUES (?,?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """, (key, value))


def get_all_settings(env_addendum: int = 0) -> dict:
    """Return all dashboard settings, falling back to env vars."""
    raw = get_setting("addendum_amount", str(env_addendum))
    try:
        addendum = int(raw)
    except (ValueError, TypeError):
        addendum = env_addendum
    try:
        radius = int(get_setting("market_radius", "150"))
    except (ValueError, TypeError):
        radius = 150
    return {
        "addendum_amount": addendum,
        "marketcheck_api_key":    get_setting("marketcheck_api_key", ""),
        "marketcheck_api_secret": get_setting("marketcheck_api_secret", ""),
        "dealer_zip":    get_setting("dealer_zip", ""),
        "market_radius": radius,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Price-scrape attempt tracking
# ─────────────────────────────────────────────────────────────────────────────
def get_scrape_attempts(vins: list[str]) -> dict[str, int]:
    """Return {vin: price_scrape_attempts} for the given VINs."""
    if not vins:
        return {}
    placeholders = ",".join("?" * len(vins))
    with _conn() as c:
        rows = c.execute(
            f"SELECT vin, COALESCE(price_scrape_attempts,0) FROM vehicles WHERE vin IN ({placeholders})",
            vins,
        ).fetchall()
    return {r[0]: r[1] for r in rows}


def update_scrape_attempts(updates: dict[str, int]) -> None:
    """Bulk-update price_scrape_attempts. Pass {vin: new_count}."""
    if not updates:
        return
    with _conn() as c:
        for vin, count in updates.items():
            c.execute(
                "UPDATE vehicles SET price_scrape_attempts=? WHERE vin=?",
                (max(0, count), vin),
            )


# ─────────────────────────────────────────────────────────────────────────────
# Cox Auto report import
# ─────────────────────────────────────────────────────────────────────────────
def parse_cox_report(text: str) -> list[dict]:
    """
    Parse raw Cox Auto inventory report text.
    Splits on 'VIN:' boundaries and extracts VIN, internet price,
    Adj Cost To Market %, and report date per vehicle block.
    """
    records: list[dict] = []
    blocks = re.split(r"VIN:", text)
    for block in blocks[1:]:
        lines = block.split("\n")
        vin = lines[0].strip()
        if not vin or len(vin) != 17:
            continue

        price_m = re.search(r"\$(\d{1,3}(?:,\d{3})+|\d{4,6})", block)
        price   = int(price_m.group(1).replace(",", "")) if price_m else None

        adj_m = re.search(r"Adj Cost To Market:(\d+)%", block)
        adj   = int(adj_m.group(1)) if adj_m else None

        date_m = re.search(r"(\d{2}/\d{2}/\d{4})", block)
        date   = date_m.group(1) if date_m else None

        records.append({"vin": vin, "internet_price": price,
                         "adj_cost_to_market": adj, "report_date": date})
    return records


def cox_import(records: list[dict]) -> dict:
    """
    Update vehicles matched by VIN from a parsed Cox report.
    Derives market_value = internet_price / (adj_cost_to_market / 100)
    when both values are present.
    Returns {"updated": N, "skipped": M}.
    """
    updated = 0
    skipped = 0
    with _conn() as c:
        for r in records:
            vin = r.get("vin", "")
            if not vin or len(vin) != 17:
                skipped += 1
                continue
            row = c.execute("SELECT vin FROM vehicles WHERE vin=?", (vin,)).fetchone()
            if not row:
                skipped += 1
                continue

            fields: dict = {}
            adj   = r.get("adj_cost_to_market")
            price = r.get("internet_price")
            if adj is not None:
                fields["cox_adj_cost_to_market"] = adj
            if r.get("report_date"):
                fields["cox_report_date"] = r["report_date"]
            if price and adj and adj > 0:
                fields["market_value"] = round(price / (adj / 100))

            if fields:
                set_clause = ", ".join(f"{k}=?" for k in fields)
                c.execute(f"UPDATE vehicles SET {set_clause} WHERE vin=?",
                          [*fields.values(), vin])
                updated += 1
            else:
                skipped += 1
    return {"updated": updated, "skipped": skipped}


# ─────────────────────────────────────────────────────────────────────────────
# Vehicles
# ─────────────────────────────────────────────────────────────────────────────
def upsert_vehicles(vehicle_rows: list[dict]) -> None:
    """
    Upsert vehicles from a sync run.
    Marks vehicles no longer in the feed as inactive.
    Does NOT overwrite user-set fields (price_override, addendum_override,
    market_value, notes).
    """
    now = datetime.utcnow().isoformat(timespec="seconds")

    with _conn() as c:
        c.execute("UPDATE vehicles SET is_active = 0 WHERE is_active = 1")

        for v in vehicle_rows:
            vin = v.get("vin", "")
            if not vin:
                continue

            price_dollars: int | None = None
            if v.get("price"):
                m = re.match(r"(\d+)", str(v["price"]))
                if m:
                    price_dollars = int(m.group(1))

            row = c.execute("SELECT first_seen FROM vehicles WHERE vin = ?", (vin,)).fetchone()
            first_seen = row["first_seen"] if row else now

            year = v.get("year")
            try:
                year = int(year) if year else None
            except (TypeError, ValueError):
                year = None

            c.execute("""
                INSERT INTO vehicles
                    (vin, title, stock_number, year, make, model, trim,
                     condition, body_style, mileage, exterior_color,
                     price_dollars, image_url, link, first_seen, last_seen, is_active)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)
                ON CONFLICT(vin) DO UPDATE SET
                    title          = excluded.title,
                    stock_number   = excluded.stock_number,
                    year           = excluded.year,
                    make           = excluded.make,
                    model          = excluded.model,
                    trim           = excluded.trim,
                    condition      = excluded.condition,
                    body_style     = excluded.body_style,
                    mileage        = excluded.mileage,
                    exterior_color = excluded.exterior_color,
                    price_dollars  = excluded.price_dollars,
                    image_url      = excluded.image_url,
                    link           = excluded.link,
                    last_seen      = excluded.last_seen,
                    is_active      = 1
            """, (
                vin, v.get("title",""), v.get("stock_number", vin),
                year, v.get("make",""), v.get("model",""), v.get("trim",""),
                v.get("condition","used"), v.get("body_style",""),
                int(v.get("mileage") or 0), v.get("exterior_color",""),
                price_dollars, v.get("image_url",""), v.get("link",""),
                first_seen, now,
            ))


def update_vehicle_fields(vin: str, fields: dict) -> bool:
    """
    Update user-editable fields on a vehicle.
    Accepted keys: price_override, addendum_override, market_value, notes
    Pass None to clear a numeric override.
    Returns True if a row was updated.
    """
    allowed = {"price_override", "addendum_override", "market_value", "notes", "cost", "pack"}
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return False
    set_clause = ", ".join(f"{k}=?" for k in updates)
    params = list(updates.values()) + [vin]
    with _conn() as c:
        cur = c.execute(f"UPDATE vehicles SET {set_clause} WHERE vin=?", params)
    return cur.rowcount > 0


def record_sync_run(run: dict) -> None:
    with _conn() as c:
        c.execute("""
            INSERT INTO sync_runs
                (run_at, vehicles_found, vehicles_priced,
                 vehicles_uploaded, duration_seconds, success)
            VALUES (?,?,?,?,?,?)
        """, (
            run.get("run_at", datetime.utcnow().isoformat(timespec="seconds")),
            run.get("vehicles_found", 0),
            run.get("vehicles_priced", 0),
            run.get("vehicles_uploaded", 0),
            round(run.get("duration_seconds", 0), 2),
            1 if run.get("success") else 0,
        ))


def upsert_vehicle_stats(stats: list[dict]) -> None:
    today = datetime.utcnow().strftime("%Y-%m-%d")
    with _conn() as c:
        for s in stats:
            c.execute("""
                INSERT INTO vehicle_stats (vin, stat_date, impressions, clicks, saves)
                VALUES (?,?,?,?,?)
                ON CONFLICT(vin, stat_date) DO UPDATE SET
                    impressions=excluded.impressions,
                    clicks=excluded.clicks,
                    saves=excluded.saves
            """, (
                s["vin"], today,
                int(s.get("impressions", 0)),
                int(s.get("clicks", 0)),
                int(s.get("saves", 0)),
            ))


# ─────────────────────────────────────────────────────────────────────────────
# Queries
# ─────────────────────────────────────────────────────────────────────────────
def get_vehicles(
    make: str = "",
    condition: str = "",
    body_style: str = "",
    year: str = "",
    search: str = "",
    active_only: bool = True,
    three_row_only: bool = False,
) -> list[dict]:
    filters: list[str] = []
    params:  list      = []

    if active_only:
        filters.append("v.is_active = 1")
    if make:
        filters.append("LOWER(v.make) = LOWER(?)")
        params.append(make)
    if condition:
        filters.append("LOWER(v.condition) = LOWER(?)")
        params.append(condition)
    if body_style:
        filters.append("LOWER(v.body_style) = LOWER(?)")
        params.append(body_style)
    if year:
        try:
            filters.append("v.year = ?")
            params.append(int(year))
        except ValueError:
            pass
    if search:
        filters.append("(v.title LIKE ? OR v.vin LIKE ? OR v.stock_number LIKE ? OR v.model LIKE ?)")
        s = f"%{search}%"
        params.extend([s, s, s, s])
    if three_row_only:
        filters.append("LOWER(v.body_style) LIKE '%suv%'")
        filters.append("(LOWER(v.title) LIKE '%3rd row%' OR LOWER(v.title) LIKE '%third row%' OR LOWER(v.title) LIKE '%7-pass%' OR LOWER(v.title) LIKE '%8-pass%' OR LOWER(v.title) LIKE '%3-row%' OR LOWER(v.trim) LIKE '%3rd row%' OR LOWER(v.trim) LIKE '%third row%' OR LOWER(v.trim) LIKE '%7-pass%' OR LOWER(v.trim) LIKE '%8-pass%' OR LOWER(v.trim) LIKE '%3-row%')")

    where = "WHERE " + " AND ".join(filters) if filters else ""

    sql = f"""
        SELECT
            v.*,
            COALESCE(s.impressions, 0) AS fb_impressions,
            COALESCE(s.clicks,      0) AS fb_clicks,
            COALESCE(s.saves,       0) AS fb_saves,
            s.stat_date                AS stats_date
        FROM   vehicles v
        LEFT JOIN (
            SELECT vin, impressions, clicks, saves, stat_date
            FROM   vehicle_stats
            WHERE  stat_date = (SELECT MAX(stat_date) FROM vehicle_stats)
        ) s ON s.vin = v.vin
        {where}
        ORDER BY v.make, v.year DESC, v.model
    """
    with _conn() as c:
        rows = c.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def get_comparable_vehicles(vin: str, limit: int = 8) -> list[dict]:
    """
    Return active vehicles with the same make+model within ±4 model years,
    excluding the vehicle itself. Used for market comparison and duplicate detection.
    """
    with _conn() as c:
        target = c.execute("SELECT make, model, year, trim FROM vehicles WHERE vin=?", (vin,)).fetchone()
        if not target:
            return []
        rows = c.execute("""
            SELECT
                vin, title, year, make, model, trim, condition, body_style,
                mileage, exterior_color, image_url, link,
                COALESCE(price_override, price_dollars) AS effective_price,
                price_override, price_dollars, market_value, is_active
            FROM vehicles
            WHERE make = ?
              AND model = ?
              AND vin != ?
              AND is_active = 1
              AND (? IS NULL OR ABS(year - ?) <= 4)
            ORDER BY ABS(year - COALESCE(?,0)) ASC, year DESC
            LIMIT ?
        """, (
            target["make"], target["model"], vin,
            target["year"], target["year"],
            target["year"], limit,
        )).fetchall()
    results = []
    for r in rows:
        d = dict(r)
        # Flag near-duplicates: same year + trim
        d["is_near_duplicate"] = (
            r["year"] == target["year"] and
            (r["trim"] or "").lower() == (target["trim"] or "").lower()
        )
        results.append(d)
    return results


def get_summary(addendum: int = 0) -> dict:
    with _conn() as c:
        total   = c.execute("SELECT COUNT(*) FROM vehicles WHERE is_active=1").fetchone()[0]
        priced  = c.execute("SELECT COUNT(*) FROM vehicles WHERE is_active=1 AND (price_override IS NOT NULL OR price_dollars IS NOT NULL)").fetchone()[0]
        avg_row = c.execute("""
            SELECT AVG(COALESCE(price_override, price_dollars))
            FROM vehicles
            WHERE is_active=1 AND (price_override IS NOT NULL OR price_dollars IS NOT NULL)
        """).fetchone()
        avg_p   = round(avg_row[0] or 0)

        makes_rows  = c.execute("SELECT make, COUNT(*) cnt FROM vehicles WHERE is_active=1 AND make!='' GROUP BY make ORDER BY cnt DESC").fetchall()
        bodies_rows = c.execute("SELECT body_style, COUNT(*) cnt FROM vehicles WHERE is_active=1 AND body_style!='' GROUP BY body_style ORDER BY cnt DESC").fetchall()
        years_rows  = c.execute("SELECT year, COUNT(*) cnt FROM vehicles WHERE is_active=1 AND year IS NOT NULL GROUP BY year ORDER BY year DESC").fetchall()
        with_mv     = c.execute("SELECT COUNT(*) FROM vehicles WHERE is_active=1 AND market_value IS NOT NULL").fetchone()[0]

        last_run  = c.execute("SELECT run_at, success, vehicles_found, vehicles_uploaded FROM sync_runs ORDER BY id DESC LIMIT 1").fetchone()
        stats_s   = c.execute("""
            SELECT COALESCE(SUM(clicks),0), COALESCE(SUM(impressions),0), COALESCE(SUM(saves),0)
            FROM vehicle_stats
            WHERE stat_date = (SELECT MAX(stat_date) FROM vehicle_stats)
        """).fetchone()
        stats_date = c.execute("SELECT MAX(stat_date) FROM vehicle_stats").fetchone()[0]

    return {
        "total_active":            total,
        "total_priced":            priced,
        "total_no_price":          total - priced,
        "avg_price":               avg_p,
        "avg_price_with_addendum": avg_p + addendum,
        "addendum_amount":         addendum,
        "vehicles_with_market_value": with_mv,
        "total_clicks":            stats_s[0] if stats_s else 0,
        "total_impressions":       stats_s[1] if stats_s else 0,
        "total_saves":             stats_s[2] if stats_s else 0,
        "stats_date":              stats_date,
        "makes_breakdown":         {r["make"]: r["cnt"]       for r in makes_rows},
        "body_breakdown":          {r["body_style"]: r["cnt"] for r in bodies_rows},
        "years_breakdown":         {str(r["year"]): r["cnt"]  for r in years_rows},
        "last_sync_at":            last_run["run_at"]            if last_run else None,
        "last_sync_ok":            bool(last_run["success"])     if last_run else None,
        "last_sync_count":         last_run["vehicles_found"]    if last_run else 0,
        "last_sync_uploaded":      last_run["vehicles_uploaded"] if last_run else 0,
    }


def get_sync_runs(limit: int = 20) -> list[dict]:
    with _conn() as c:
        rows = c.execute("SELECT * FROM sync_runs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    return [dict(r) for r in rows]


def get_makes() -> list[str]:
    with _conn() as c:
        rows = c.execute("SELECT DISTINCT make FROM vehicles WHERE is_active=1 AND make!='' ORDER BY make").fetchall()
    return [r["make"] for r in rows]



def get_deal_history(limit: int = 100) -> list[dict]:
    """Best-effort sold/deal history based on units no longer active in feed."""
    with _conn() as c:
        rows = c.execute("""
            SELECT
                vin, stock_number, year, make, model, trim,
                COALESCE(price_override, price_dollars) AS sold_price,
                first_seen, last_seen,
                CAST((julianday(last_seen) - julianday(first_seen)) AS INTEGER) AS days_listed
            FROM vehicles
            WHERE is_active = 0
            ORDER BY last_seen DESC
            LIMIT ?
        """, (limit,)).fetchall()

        bench_rows = c.execute("""
            SELECT make, model,
                   ROUND(AVG(CAST((julianday(last_seen) - julianday(first_seen)) AS INTEGER)), 1) AS avg_days,
                   ROUND(AVG(COALESCE(price_override, price_dollars)), 0) AS avg_price
            FROM vehicles
            WHERE is_active = 0
              AND first_seen IS NOT NULL
              AND last_seen IS NOT NULL
            GROUP BY make, model
        """).fetchall()

    benchmarks = {(r["make"], r["model"]): {"avg_days": r["avg_days"], "avg_price": r["avg_price"]} for r in bench_rows}

    out = []
    for r in rows:
        d = dict(r)
        d["days_listed"] = max(0, int(d.get("days_listed") or 0))
        d["benchmark"] = benchmarks.get((d.get("make"), d.get("model")), {"avg_days": None, "avg_price": None})
        out.append(d)
    return out

def get_years() -> list[int]:
    with _conn() as c:
        rows = c.execute("SELECT DISTINCT year FROM vehicles WHERE is_active=1 AND year IS NOT NULL ORDER BY year DESC").fetchall()
    return [r["year"] for r in rows]


# ─────────────────────────────────────────────────────────────────────────────
# Deals (saved pencil history)
# ─────────────────────────────────────────────────────────────────────────────
def save_deal(deal: dict) -> int:
    """Insert a saved pencil deal. Returns the new row id."""
    now = datetime.utcnow().isoformat(timespec="seconds")
    with _conn() as c:
        cur = c.execute("""
            INSERT INTO deals
                (vin, created_at, created_by, customer_name,
                 base_price, addendum_amount, tax_rate, doc_fee,
                 down_payment, apr, term_months,
                 out_the_door, amount_financed, monthly_payment, gross, notes)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            deal.get("vin", ""),
            now,
            deal.get("created_by", ""),
            deal.get("customer_name", ""),
            deal.get("base_price"),
            deal.get("addendum_amount", 0),
            deal.get("tax_rate", 0),
            deal.get("doc_fee", 0),
            deal.get("down_payment", 0),
            deal.get("apr", 0),
            deal.get("term_months", 72),
            deal.get("out_the_door"),
            deal.get("amount_financed"),
            deal.get("monthly_payment"),
            deal.get("gross"),
            deal.get("notes", ""),
        ))
    return cur.lastrowid


# ─────────────────────────────────────────────────────────────────────────────
# MarketCheck cache helpers
# ─────────────────────────────────────────────────────────────────────────────
_COMPS_TTL_HOURS = 24


def get_comps_cache(cache_key: str) -> list | None:
    """Return cached listings if fresh (< 24 h old), else None."""
    with _conn() as c:
        row = c.execute(
            "SELECT data, fetched_at FROM comps_cache WHERE cache_key=?",
            (cache_key,)
        ).fetchone()
    if not row:
        return None
    fetched = datetime.fromisoformat(row["fetched_at"])
    age_h = (datetime.now(timezone.utc).replace(tzinfo=None) - fetched).total_seconds() / 3600
    if age_h > _COMPS_TTL_HOURS:
        return None
    return json.loads(row["data"])


def set_comps_cache(cache_key: str, listings: list) -> None:
    now = datetime.utcnow().isoformat()
    with _conn() as c:
        c.execute(
            """INSERT INTO comps_cache (cache_key, data, fetched_at) VALUES (?,?,?)
               ON CONFLICT(cache_key) DO UPDATE SET data=excluded.data, fetched_at=excluded.fetched_at""",
            (cache_key, json.dumps(listings), now)
        )


def get_vin_cache(vin: str) -> dict | None:
    """Return cached VIN decode + sticker data, or None if not found."""
    with _conn() as c:
        row = c.execute(
            "SELECT specs_json, sticker_url FROM vin_cache WHERE vin=?",
            (vin,)
        ).fetchone()
    if not row:
        return None
    return {"specs": json.loads(row["specs_json"]), "sticker_url": row["sticker_url"]}


def set_vin_cache(vin: str, specs: list | None = None, sticker_url: str | None = None) -> None:
    """Upsert VIN cache; only updates fields that are provided."""
    with _conn() as c:
        existing = c.execute(
            "SELECT specs_json, sticker_url FROM vin_cache WHERE vin=?", (vin,)
        ).fetchone()
        now = datetime.utcnow().isoformat()
        if existing:
            new_specs = json.dumps(specs) if specs is not None else existing["specs_json"]
            new_sticker = sticker_url if sticker_url is not None else existing["sticker_url"]
            c.execute(
                "UPDATE vin_cache SET specs_json=?, sticker_url=?, fetched_at=? WHERE vin=?",
                (new_specs, new_sticker, now, vin)
            )
        else:
            c.execute(
                "INSERT INTO vin_cache (vin, specs_json, sticker_url, fetched_at) VALUES (?,?,?,?)",
                (vin, json.dumps(specs or []), sticker_url or "", now)
            )


def get_deals(vin: str = "", limit: int = 50) -> list[dict]:
    """Return saved deals, optionally filtered by VIN, newest first."""
    with _conn() as c:
        if vin:
            rows = c.execute("""
                SELECT d.*, v.year, v.make, v.model, v.trim, v.stock_number
                FROM deals d
                LEFT JOIN vehicles v ON v.vin = d.vin
                WHERE d.vin = ?
                ORDER BY d.id DESC LIMIT ?
            """, (vin, limit)).fetchall()
        else:
            rows = c.execute("""
                SELECT d.*, v.year, v.make, v.model, v.trim, v.stock_number
                FROM deals d
                LEFT JOIN vehicles v ON v.vin = d.vin
                ORDER BY d.id DESC LIMIT ?
            """, (limit,)).fetchall()
    return [dict(r) for r in rows]
