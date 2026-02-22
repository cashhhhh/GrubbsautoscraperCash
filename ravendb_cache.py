"""
RavenDB persistent cache for MarketCheck API responses.
Falls back to SQLite if RavenDB is unavailable.
"""

import json
import os
from datetime import datetime, timezone
from typing import Optional

try:
    from ravendb import DocumentStore
    RAVENDB_AVAILABLE = True
except ImportError:
    RAVENDB_AVAILABLE = False
    DocumentStore = None  # type: ignore


_COMPS_TTL_HOURS = 24
_store: Optional[object] = None


def init_ravendb() -> bool:
    """Initialize RavenDB connection. Returns True if successful."""
    global _store

    if not RAVENDB_AVAILABLE:
        print("[RavenDB] pyravendb not installed, using SQLite fallback")
        return False

    try:
        server_url = os.getenv(
            "RAVENDB_URL",
            "https://a.free.cashmcccombs.ravendb.cloud"
        )
        username = os.getenv("RAVENDB_USERNAME", "Cashmccombs@gmail.com")
        password = os.getenv("RAVENDB_PASSWORD", "Cash1345")
        database = os.getenv("RAVENDB_DATABASE", "cashdashboard")

        _store = DocumentStore(urls=[server_url], database=database)

        # Authentication
        _store.conventions.disable_topology_updates = True
        _store.auth_options.certificate = os.getenv("RAVENDB_CERT_PATH")
        _store.auth_options.username = username
        _store.auth_options.password = password

        _store.initialize()

        # Test connection
        with _store.open_session() as session:
            session.query(object_type=object).first()

        print(f"[RavenDB] Connected to {server_url}/{database}")
        return True
    except Exception as e:
        print(f"[RavenDB] Connection failed: {e}. Using SQLite fallback.")
        _store = None
        return False


def get_comps_cache(cache_key: str, fallback_fn=None) -> Optional[list]:
    """
    Get cached listings from RavenDB (with 24h TTL).
    Falls back to SQLite function if provided and RavenDB unavailable.
    """
    if _store:
        try:
            with _store.open_session() as session:
                doc = session.query(object_type=dict).where_equals("id", cache_key).first()

                if not doc:
                    return None

                fetched = datetime.fromisoformat(doc.get("fetched_at", ""))
                age_h = (datetime.now(timezone.utc).replace(tzinfo=None) - fetched).total_seconds() / 3600

                if age_h > _COMPS_TTL_HOURS:
                    return None

                return doc.get("data", [])
        except Exception as e:
            print(f"[RavenDB] get_comps_cache error: {e}")

    # Fallback to SQLite
    if fallback_fn:
        return fallback_fn(cache_key)
    return None


def set_comps_cache(cache_key: str, listings: list, fallback_fn=None) -> None:
    """
    Cache listings in RavenDB.
    Falls back to SQLite function if provided and RavenDB unavailable.
    """
    if _store:
        try:
            with _store.open_session() as session:
                doc = {
                    "id": cache_key,
                    "data": listings,
                    "fetched_at": datetime.utcnow().isoformat(),
                    "@metadata": {"@collection": "CompsCache"}
                }
                session.store(doc)
                session.save_changes()
            return
        except Exception as e:
            print(f"[RavenDB] set_comps_cache error: {e}")

    # Fallback to SQLite
    if fallback_fn:
        fallback_fn(cache_key, listings)


def get_vin_cache(vin: str, fallback_fn=None) -> Optional[dict]:
    """
    Get cached VIN specs from RavenDB.
    Falls back to SQLite function if provided and RavenDB unavailable.
    """
    if _store:
        try:
            with _store.open_session() as session:
                doc = session.query(object_type=dict).where_equals("id", f"vin_{vin}").first()

                if not doc:
                    return None

                return {
                    "specs": doc.get("specs", []),
                    "sticker_url": doc.get("sticker_url", "")
                }
        except Exception as e:
            print(f"[RavenDB] get_vin_cache error: {e}")

    # Fallback to SQLite
    if fallback_fn:
        return fallback_fn(vin)
    return None


def set_vin_cache(vin: str, specs: Optional[list] = None, sticker_url: Optional[str] = None, fallback_fn=None) -> None:
    """
    Cache VIN specs in RavenDB.
    Falls back to SQLite function if provided and RavenDB unavailable.
    """
    if _store:
        try:
            with _store.open_session() as session:
                doc_id = f"vin_{vin}"

                # Try to get existing doc
                doc = session.query(object_type=dict).where_equals("id", doc_id).first()

                if doc:
                    if specs is not None:
                        doc["specs"] = specs
                    if sticker_url is not None:
                        doc["sticker_url"] = sticker_url
                else:
                    doc = {
                        "id": doc_id,
                        "vin": vin,
                        "specs": specs or [],
                        "sticker_url": sticker_url or "",
                        "fetched_at": datetime.utcnow().isoformat(),
                        "@metadata": {"@collection": "VinCache"}
                    }

                session.store(doc)
                session.save_changes()
            return
        except Exception as e:
            print(f"[RavenDB] set_vin_cache error: {e}")

    # Fallback to SQLite
    if fallback_fn:
        fallback_fn(vin, specs, sticker_url)
