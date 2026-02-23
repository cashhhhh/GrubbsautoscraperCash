"""
RavenDB persistent cache for MarketCheck API responses.
Falls back to SQLite if RavenDB is unavailable.
"""

import importlib
import os
from datetime import datetime, timezone
from typing import Optional

DocumentStore = None


_COMPS_TTL_HOURS = 24
_store: Optional[object] = None


def init_ravendb() -> bool:
    """Initialize RavenDB connection. Returns True if successful."""
    global _store

    document_store_cls = _load_document_store()

    if not document_store_cls:
        print("[RavenDB] Python client not importable; using SQLite fallback")
        print("           Ensure build installs: pyravendb==5.0.0.5")
        return False

    server_url = os.getenv(
            "RAVENDB_URL",
            "https://a.free.cashmcccombs.ravendb.cloud"
        ).strip()
    database = os.getenv("RAVENDB_DATABASE", "cashdashboard").strip()

    try:
        _store = document_store_cls(urls=[server_url], database=database)

        # Authentication
        _store.conventions.disable_topology_updates = True
        cert_path = os.getenv("RAVENDB_CERT_PATH", "").strip()
        username = os.getenv("RAVENDB_USERNAME", "Cashmccombs@gmail.com").strip()
        password = os.getenv("RAVENDB_PASSWORD", "Cash1345").strip()

        if cert_path:
            _store.auth_options.certificate = cert_path
        if username and password:
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


def _load_document_store():
    """Load RavenDB DocumentStore lazily so environments can install dependencies later."""
    global DocumentStore

    if DocumentStore:
        return DocumentStore

    module_candidates = (
        "ravendb",
        "pyravendb",
        "ravendb.documents.store.document_store",
        "pyravendb.store.document_store",
    )

    for module_name in module_candidates:
        try:
            module = importlib.import_module(module_name)
            document_store_cls = getattr(module, "DocumentStore", None)
            if document_store_cls:
                DocumentStore = document_store_cls
                return DocumentStore
        except Exception:
            continue

    DocumentStore = None
    return DocumentStore


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
