"""
db.py — SQLite persistence layer for the FB Marketplace Dashboard.

Tables
------
  vehicles       – every vehicle ever seen; is_active=0 if dropped from feed
  sync_runs      – one row per fb_marketplace_sync.py execution
  vehicle_stats  – FB impressions/clicks/saves, one row per (vin, date)
"""

import os
import re
import sqlite3
from datetime import datetime

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
            condition        TEXT  DEFAULT 'used',
            body_style       TEXT  DEFAULT '',
            mileage          INTEGER DEFAULT 0,
            exterior_color   TEXT  DEFAULT '',
            price_dollars    INTEGER,        -- NULL = no price found
            image_url        TEXT  DEFAULT '',
            link             TEXT  DEFAULT '',
            first_seen       TEXT  NOT NULL,
            last_seen        TEXT  NOT NULL,
            is_active        INTEGER DEFAULT 1
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
            stat_date    TEXT NOT NULL,   -- YYYY-MM-DD
            impressions  INTEGER DEFAULT 0,
            clicks       INTEGER DEFAULT 0,
            saves        INTEGER DEFAULT 0,
            PRIMARY KEY (vin, stat_date)
        );

        CREATE INDEX IF NOT EXISTS idx_vs_vin  ON vehicle_stats(vin);
        CREATE INDEX IF NOT EXISTS idx_vs_date ON vehicle_stats(stat_date);
        CREATE INDEX IF NOT EXISTS idx_v_make  ON vehicles(make);
        CREATE INDEX IF NOT EXISTS idx_v_active ON vehicles(is_active);
        """)


# ─────────────────────────────────────────────────────────────────────────────
# Vehicles
# ─────────────────────────────────────────────────────────────────────────────
def upsert_vehicles(vehicle_rows: list[dict]) -> None:
    """
    Upsert vehicles from a sync run.
    All previously-active vehicles not in this batch get is_active=0.
    """
    now = datetime.utcnow().isoformat(timespec="seconds")
    active_vins = {v["vin"] for v in vehicle_rows if v.get("vin")}

    with _conn() as c:
        # Mark everything inactive; the loop below re-activates current ones
        c.execute("UPDATE vehicles SET is_active = 0 WHERE is_active = 1")

        for v in vehicle_rows:
            vin = v.get("vin", "")
            if not vin:
                continue

            # Parse price string like "24995 USD" → 24995
            price_dollars: int | None = None
            if v.get("price"):
                m = re.match(r"(\d+)", str(v["price"]))
                if m:
                    price_dollars = int(m.group(1))

            # Preserve original first_seen date
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
                vin,
                v.get("title", ""),
                v.get("stock_number", vin),
                year,
                v.get("make", ""),
                v.get("model", ""),
                v.get("trim", ""),
                v.get("condition", "used"),
                v.get("body_style", ""),
                int(v.get("mileage") or 0),
                v.get("exterior_color", ""),
                price_dollars,
                v.get("image_url", ""),
                v.get("link", ""),
                first_seen,
                now,
            ))


def record_sync_run(run: dict) -> None:
    """Append one sync_runs row."""
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
    """
    Upsert Facebook insight stats.
    stats = list of {vin, impressions, clicks, saves}
    """
    today = datetime.utcnow().strftime("%Y-%m-%d")
    with _conn() as c:
        for s in stats:
            c.execute("""
                INSERT INTO vehicle_stats (vin, stat_date, impressions, clicks, saves)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(vin, stat_date) DO UPDATE SET
                    impressions = excluded.impressions,
                    clicks      = excluded.clicks,
                    saves       = excluded.saves
            """, (
                s["vin"],
                today,
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
) -> list[dict]:
    """Return vehicles joined with their latest FB stats."""
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


def get_summary(addendum: int = 0) -> dict:
    with _conn() as c:
        total   = c.execute("SELECT COUNT(*) FROM vehicles WHERE is_active=1").fetchone()[0]
        priced  = c.execute("SELECT COUNT(*) FROM vehicles WHERE is_active=1 AND price_dollars IS NOT NULL").fetchone()[0]
        avg_row = c.execute("SELECT AVG(price_dollars) FROM vehicles WHERE is_active=1 AND price_dollars IS NOT NULL").fetchone()
        avg_p   = round(avg_row[0] or 0)

        makes_rows  = c.execute("SELECT make, COUNT(*) cnt FROM vehicles WHERE is_active=1 AND make!='' GROUP BY make ORDER BY cnt DESC").fetchall()
        bodies_rows = c.execute("SELECT body_style, COUNT(*) cnt FROM vehicles WHERE is_active=1 AND body_style!='' GROUP BY body_style ORDER BY cnt DESC").fetchall()
        years_rows  = c.execute("SELECT year, COUNT(*) cnt FROM vehicles WHERE is_active=1 AND year IS NOT NULL GROUP BY year ORDER BY year DESC").fetchall()

        last_run = c.execute("SELECT run_at, success, vehicles_found, vehicles_uploaded FROM sync_runs ORDER BY id DESC LIMIT 1").fetchone()
        stats_s  = c.execute("""
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
        "total_clicks":            stats_s[0] if stats_s else 0,
        "total_impressions":       stats_s[1] if stats_s else 0,
        "total_saves":             stats_s[2] if stats_s else 0,
        "stats_date":              stats_date,
        "makes_breakdown":         {r["make"]: r["cnt"]       for r in makes_rows},
        "body_breakdown":          {r["body_style"]: r["cnt"] for r in bodies_rows},
        "years_breakdown":         {str(r["year"]): r["cnt"]  for r in years_rows},
        "last_sync_at":            last_run["run_at"]          if last_run else None,
        "last_sync_ok":            bool(last_run["success"])   if last_run else None,
        "last_sync_count":         last_run["vehicles_found"]  if last_run else 0,
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


def get_years() -> list[int]:
    with _conn() as c:
        rows = c.execute("SELECT DISTINCT year FROM vehicles WHERE is_active=1 AND year IS NOT NULL ORDER BY year DESC").fetchall()
    return [r["year"] for r in rows]
