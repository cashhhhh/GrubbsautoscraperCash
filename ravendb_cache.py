"""
RavenDB persistent cache for MarketCheck API responses.
Falls back to SQLite if RavenDB is unavailable.
"""

import importlib
import os
from datetime import datetime, timezone
from typing import Optional

import requests

DocumentStore = None


_COMPS_TTL_HOURS = 24
_store: Optional[object] = None


def init_ravendb() -> bool:
    """Initialize RavenDB connection. Returns True if successful."""
    global _store

    server_url = os.getenv(
        "RAVENDB_URL",
        "https://a.free.cashmcccombs.ravendb.cloud"
    ).strip()
    database = os.getenv("RAVENDB_DATABASE", "cashdashboard").strip()
    cert_path = os.getenv("RAVENDB_CERT_PATH", "").strip()
    username = os.getenv("RAVENDB_USERNAME", "Cashmccombs@gmail.com").strip()
    password = os.getenv("RAVENDB_PASSWORD", "Cash1345").strip()

    document_store_cls = _load_document_store()
    if document_store_cls:
        try:
            _store = document_store_cls(urls=[server_url], database=database)
            _store.conventions.disable_topology_updates = True

            if cert_path:
                _store.auth_options.certificate = cert_path
            if username and password:
                _store.auth_options.username = username
                _store.auth_options.password = password

            _store.initialize()
            with _store.open_session() as session:
                session.query(object_type=object).first()

            print(f"[RavenDB] Connected via python client to {server_url}/{database}")
            return True
        except Exception as e:
            print(f"[RavenDB] Python client connection failed: {e}")

    # HTTP API fallback if python client is missing/broken.
    http_store = {
        "kind": "http",
        "server_url": server_url.rstrip("/"),
        "database": database,
        "username": username,
        "password": password,
        "cert_path": cert_path,
    }

    if _probe_http_store(http_store):
        _store = http_store
        print(f"[RavenDB] Connected via HTTP API to {server_url}/{database}")
        return True

    _store = None
    print("[RavenDB] Connection unavailable. Using SQLite fallback.")
    return False


def _probe_http_store(http_store: dict) -> bool:
    try:
        response = requests.get(
            _docs_url(http_store),
            params={"start": 0, "pageSize": 1},
            timeout=5,
            auth=_http_auth(http_store),
            verify=_http_verify(http_store),
        )
        return response.status_code < 400
    except Exception as e:
        print(f"[RavenDB] HTTP probe failed: {e}")
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


def _is_http_store() -> bool:
    return isinstance(_store, dict) and _store.get("kind") == "http"


def _docs_url(http_store: dict) -> str:
    return f"{http_store['server_url']}/databases/{http_store['database']}/docs"


def _http_auth(http_store: dict):
    if http_store.get("username") and http_store.get("password"):
        return (http_store["username"], http_store["password"])
    return None


def _http_verify(http_store: dict):
    if http_store.get("cert_path"):
        return http_store["cert_path"]
    return True


def _http_get_doc(doc_id: str) -> Optional[dict]:
    if not _is_http_store():
        return None

    try:
        response = requests.get(
            _docs_url(_store),
            params={"id": doc_id},
            timeout=8,
            auth=_http_auth(_store),
            verify=_http_verify(_store),
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        payload = response.json()

        if "Results" in payload and payload["Results"]:
            return payload["Results"][0]
        return None
    except Exception as e:
        print(f"[RavenDB] HTTP get error: {e}")
        return None


def _http_put_doc(doc_id: str, doc: dict) -> bool:
    if not _is_http_store():
        return False

    try:
        response = requests.put(
            _docs_url(_store),
            params={"id": doc_id},
            json=doc,
            timeout=8,
            auth=_http_auth(_store),
            verify=_http_verify(_store),
        )
        response.raise_for_status()
        return True
    except Exception as e:
        print(f"[RavenDB] HTTP put error: {e}")
        return False


def get_comps_cache(cache_key: str, fallback_fn=None) -> Optional[list]:
    """
    Get cached listings from RavenDB (with 24h TTL).
    Falls back to SQLite function if provided and RavenDB unavailable.
    """
    if _store and not _is_http_store():
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

    if _is_http_store():
        doc = _http_get_doc(cache_key)
        if doc:
            try:
                fetched = datetime.fromisoformat(doc.get("fetched_at", ""))
                age_h = (datetime.now(timezone.utc).replace(tzinfo=None) - fetched).total_seconds() / 3600
                if age_h <= _COMPS_TTL_HOURS:
                    return doc.get("data", [])
            except Exception:
                return None

    # Fallback to SQLite
    if fallback_fn:
        return fallback_fn(cache_key)
    return None


def set_comps_cache(cache_key: str, listings: list, fallback_fn=None) -> None:
    """
    Cache listings in RavenDB.
    Falls back to SQLite function if provided and RavenDB unavailable.
    """
    if _store and not _is_http_store():
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

    if _is_http_store():
        doc = {
            "id": cache_key,
            "data": listings,
            "fetched_at": datetime.utcnow().isoformat(),
            "@metadata": {"@collection": "CompsCache"}
        }
        if _http_put_doc(cache_key, doc):
            return

    # Fallback to SQLite
    if fallback_fn:
        fallback_fn(cache_key, listings)


def get_vin_cache(vin: str, fallback_fn=None) -> Optional[dict]:
    """
    Get cached VIN specs from RavenDB.
    Falls back to SQLite function if provided and RavenDB unavailable.
    """
    if _store and not _is_http_store():
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

    if _is_http_store():
        doc = _http_get_doc(f"vin_{vin}")
        if doc:
            return {
                "specs": doc.get("specs", []),
                "sticker_url": doc.get("sticker_url", "")
            }

    # Fallback to SQLite
    if fallback_fn:
        return fallback_fn(vin)
    return None


def set_vin_cache(vin: str, specs: Optional[list] = None, sticker_url: Optional[str] = None, fallback_fn=None) -> None:
    """
    Cache VIN specs in RavenDB.
    Falls back to SQLite function if provided and RavenDB unavailable.
    """
    if _store and not _is_http_store():
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

    if _is_http_store():
        doc_id = f"vin_{vin}"
        existing = _http_get_doc(doc_id) or {}
        if not existing:
            existing = {
                "id": doc_id,
                "vin": vin,
                "fetched_at": datetime.utcnow().isoformat(),
                "@metadata": {"@collection": "VinCache"}
            }
        if specs is not None:
            existing["specs"] = specs
        elif "specs" not in existing:
            existing["specs"] = []

        if sticker_url is not None:
            existing["sticker_url"] = sticker_url
        elif "sticker_url" not in existing:
            existing["sticker_url"] = ""

        if _http_put_doc(doc_id, existing):
            return

    # Fallback to SQLite
    if fallback_fn:
        fallback_fn(vin, specs, sticker_url)
