"""Infohash -> content id conversion where stream IDs enter the app.

The engine plays a *content id* with ``getstream?id=``. Its own catalogue
search and many published playlists (``getstream?infohash=``) hand out
*infohashes* instead: the same 40-hex shape, a different key, and
``getstream?id=<infohash>`` answers "failed to load content". Everything
downstream (DB rows, playback, relay, checks, Acexy, playlists, tuner) keys on
content ids, so infohashes are converted once, on import, and never stored.

Successful conversions are remembered in a small JSON file next to the
database so a re-scrape costs no engine calls. Failures are not persisted; an
"unknown to the engine" answer is only remembered in memory for a short while.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import tempfile
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterable, Optional

import httpx
from sqlalchemy.engine import make_url

from app.services.engine_client import (
    LOOKUP_TIMEOUT, EngineClient, EngineUnavailableError, engine_url_from_settings,
)

logger = logging.getLogger(__name__)

CACHE_FILE_NAME = "infohash_content_ids.json"
MAX_CACHE_ENTRIES = 20_000
NEGATIVE_TTL_SECONDS = 600.0
RESOLVE_CONCURRENCY = 4

# Marks a parsed record whose id is an infohash still to be converted.
ID_KIND_KEY = "id_kind"
INFOHASH = "infohash"

_HEX40 = re.compile(r"[0-9a-f]{40}")


def normalize_infohash(value: str) -> Optional[str]:
    candidate = (value or "").strip().lower()
    return candidate if _HEX40.fullmatch(candidate) else None


def is_infohash_record(metadata: Optional[dict]) -> bool:
    return isinstance(metadata, dict) and metadata.get(ID_KIND_KEY) == INFOHASH


class InfohashNotResolvedError(RuntimeError):
    """The engine answered but has no content id for this infohash."""


class EngineNotConfiguredError(EngineUnavailableError):
    """No playback engine URL is saved, so nothing can be converted."""


def describe_failure(exc: Exception) -> str:
    """User-facing reason; never echoes engine hosts or transport details."""
    prefix = "Could not convert this infohash to a content ID"
    if isinstance(exc, (InfohashNotResolvedError, EngineNotConfiguredError)):
        return f"{prefix}: {exc}"
    return f"{prefix}: the AceStream engine could not be reached"


class InfohashCache:
    """Bounded, persistent infohash -> content id map (oldest evicted first).

    Writes go to a temp file in the same directory and are swapped in with
    ``os.replace``, so a crash never leaves a half-written file. A missing,
    unreadable or corrupt file starts the cache empty; an unwritable one keeps
    the entries in memory only.
    """

    def __init__(self, path: Path, max_entries: int = MAX_CACHE_ENTRIES):
        self.path = Path(path)
        self.max_entries = max_entries
        self._entries: "OrderedDict[str, str]" = OrderedDict()
        self._loaded = False
        self._lock = threading.Lock()

    def _load(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        try:
            raw = self.path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return
        except OSError as exc:
            logger.warning("Infohash cache %s is unreadable; starting empty: %s", self.path, exc)
            return
        try:
            data = json.loads(raw)
            entries = data["entries"] if isinstance(data, dict) else None
            if not isinstance(entries, dict):
                raise ValueError("no 'entries' object")
        except (ValueError, KeyError, TypeError) as exc:
            logger.warning("Infohash cache %s is corrupt; starting empty: %s", self.path, exc)
            return
        for infohash, content_id in entries.items():
            key, value = normalize_infohash(str(infohash)), normalize_infohash(str(content_id))
            if key and value:
                self._entries[key] = value
        self._trim()

    def _trim(self) -> None:
        while len(self._entries) > self.max_entries:
            self._entries.popitem(last=False)

    def get_many(self, infohashes: Iterable[str]) -> Dict[str, str]:
        with self._lock:
            self._load()
            return {ih: self._entries[ih] for ih in infohashes if ih in self._entries}

    def get(self, infohash: str) -> Optional[str]:
        return self.get_many([infohash]).get(infohash)

    def put_many(self, mapping: Dict[str, str]) -> None:
        if not mapping:
            return
        with self._lock:
            self._load()
            for infohash, content_id in mapping.items():
                self._entries.pop(infohash, None)
                self._entries[infohash] = content_id
            self._trim()
            self._save()

    def _save(self) -> None:
        payload = json.dumps({"version": 1, "entries": self._entries}, separators=(",", ":"))
        tmp_name = None
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                "w", encoding="utf-8", dir=self.path.parent, prefix=".infohash_", suffix=".tmp", delete=False,
            ) as handle:
                tmp_name = handle.name
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, self.path)
            tmp_name = None
        except OSError as exc:
            logger.warning("Could not write infohash cache %s; keeping it in memory: %s", self.path, exc)
        finally:
            if tmp_name:
                try:
                    os.unlink(tmp_name)
                except OSError:
                    pass


def cache_path() -> Path:
    """The cache lives beside the SQLite database (``/app/config`` in Docker)."""
    from app.config.settings import get_settings
    from app.utils.path import config_dir

    try:
        url = make_url(get_settings().DATABASE_URL)
        if url.drivername.startswith("sqlite") and url.database and url.database != ":memory:":
            return Path(url.database).resolve().parent / CACHE_FILE_NAME
    except Exception:  # pragma: no cover - malformed URL falls back below
        pass
    return config_dir() / CACHE_FILE_NAME


_caches: Dict[Path, InfohashCache] = {}
_caches_lock = threading.Lock()
_negative: Dict[str, float] = {}
_negative_lock = threading.Lock()


def get_cache() -> InfohashCache:
    path = cache_path()
    with _caches_lock:
        cache = _caches.get(path)
        if cache is None:
            cache = _caches[path] = InfohashCache(path)
        return cache


def _recently_unknown(infohash: str) -> bool:
    with _negative_lock:
        expiry = _negative.get(infohash)
        if expiry is None:
            return False
        if expiry <= time.monotonic():
            del _negative[infohash]
            return False
        return True


def _remember_unknown(infohash: str) -> None:
    with _negative_lock:
        if len(_negative) >= MAX_CACHE_ENTRIES:
            _negative.clear()
        _negative[infohash] = time.monotonic() + NEGATIVE_TTL_SECONDS


def reset_negative_cache() -> None:
    with _negative_lock:
        _negative.clear()


@dataclass
class ResolutionOutcome:
    resolved: Dict[str, str] = field(default_factory=dict)
    unknown: set = field(default_factory=set)
    unavailable: set = field(default_factory=set)
    no_engine: set = field(default_factory=set)
    engine_calls: int = 0

    @property
    def unresolved(self) -> int:
        return len(self.unknown) + len(self.unavailable) + len(self.no_engine)

    @property
    def only_definitive_failures(self) -> bool:
        """True when every failure is the engine saying the hash is unknown."""
        return not self.unavailable and not self.no_engine


class ContentIdResolver:
    """Cache first, then the playback engine's ``get_content_id`` lookup."""

    def __init__(
        self,
        engine_url: Optional[str],
        cache: Optional[InfohashCache] = None,
        *,
        concurrency: int = RESOLVE_CONCURRENCY,
        timeout: httpx.Timeout = LOOKUP_TIMEOUT,
        client_factory: Optional[Callable[[str], EngineClient]] = None,
    ):
        self.engine_url = engine_url
        self.cache = cache or get_cache()
        self.concurrency = max(1, concurrency)
        self.timeout = timeout
        self.client_factory = client_factory or EngineClient

    @classmethod
    def from_settings(cls, db) -> "ContentIdResolver":
        from app.repositories.settings_repository import SettingsRepository

        try:
            engine_url = engine_url_from_settings(SettingsRepository(db))
        except EngineUnavailableError:
            engine_url = None
        return cls(engine_url)

    def resolve_one(self, infohash: str) -> str:
        """Blocking single lookup for the add-channel endpoints.

        Raises :class:`InfohashNotResolvedError` (engine knows no content id),
        :class:`EngineNotConfiguredError` or :class:`EngineUnavailableError`.
        """
        key = normalize_infohash(infohash)
        if key is None:
            raise InfohashNotResolvedError("An infohash is 40 hexadecimal characters")
        cached = self.cache.get(key)
        if cached:
            return cached
        if not self.engine_url:
            raise EngineNotConfiguredError("No AceStream engine is configured to convert the infohash")
        with self.client_factory(self.engine_url) as client:
            content_id = client.get_content_id(key, timeout=self.timeout)
        if not content_id:
            _remember_unknown(key)
            raise InfohashNotResolvedError("The engine has no content ID for this infohash")
        self.cache.put_many({key: content_id})
        return content_id

    async def resolve_many(self, infohashes: Iterable[str]) -> ResolutionOutcome:
        """Resolve a batch with bounded concurrency; one cache write at the end.

        After the first "engine unreachable" answer the remaining lookups are
        not attempted: every one of them would wait out its own timeout.
        """
        outcome = ResolutionOutcome()
        pending = sorted({key for key in (normalize_infohash(i) for i in infohashes) if key})
        if not pending:
            return outcome
        outcome.resolved.update(await asyncio.to_thread(self.cache.get_many, pending))
        pending = [ih for ih in pending if ih not in outcome.resolved]
        for infohash in [ih for ih in pending if _recently_unknown(ih)]:
            outcome.unknown.add(infohash)
        pending = [ih for ih in pending if ih not in outcome.unknown]
        if not pending:
            return outcome
        if not self.engine_url:
            outcome.no_engine.update(pending)
            return outcome

        fresh: Dict[str, str] = {}
        engine_down = asyncio.Event()
        semaphore = asyncio.Semaphore(self.concurrency)
        client = self.client_factory(self.engine_url)

        async def lookup(infohash: str) -> None:
            async with semaphore:
                if engine_down.is_set():
                    outcome.unavailable.add(infohash)
                    return
                outcome.engine_calls += 1
                try:
                    content_id = await asyncio.to_thread(client.get_content_id, infohash, self.timeout)
                except EngineUnavailableError:
                    engine_down.set()
                    outcome.unavailable.add(infohash)
                    return
                if content_id:
                    fresh[infohash] = content_id
                else:
                    _remember_unknown(infohash)
                    outcome.unknown.add(infohash)

        try:
            await asyncio.gather(*(lookup(ih) for ih in pending))
        finally:
            await asyncio.to_thread(client.close)
        outcome.resolved.update(fresh)
        if fresh:
            await asyncio.to_thread(self.cache.put_many, fresh)
        if outcome.unavailable:
            logger.warning(
                "Engine unreachable while converting infohashes: %s unresolved", len(outcome.unavailable),
            )
        return outcome
