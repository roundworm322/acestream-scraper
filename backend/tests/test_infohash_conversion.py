"""Infohash -> content id conversion at ingestion.

Engine evidence (native 3.2.11): ``getstream?id=<infohash>`` answers
"failed to load content", while ``/server/api?method=get_content_id`` maps the
infohash to a content id that ``getstream?id=`` plays.
"""
import asyncio
import json
import os
import threading
import time

import httpx
import pytest

from app.models.models import AcestreamChannel, TVChannel
from app.repositories.settings_repository import SettingsRepository
from app.services import content_id_resolver as resolver_module
from app.services.content_id_resolver import (
    ContentIdResolver, EngineNotConfiguredError, InfohashCache, InfohashNotResolvedError,
)
from app.services.engine_client import EngineClient, EngineUnavailableError
from app.services.m3u_service import M3UService
from app.services.scraper_service import ScraperService

IH1 = "42832fbf9a1d3bdcca043e09951d6769fbbe04e7"
CID1 = "e72d03fb9694164317260f684470be9ab781ed95"
IH2 = "1111111111111111111111111111111111111111"
CID2 = "2222222222222222222222222222222222222222"
IH_UNKNOWN = "3333333333333333333333333333333333333333"
DIRECT = "4444444444444444444444444444444444444444"
ENGINE = "http://engine:6878"


class FakeEngine:
    """Answers ``get_content_id`` like the native engine and counts calls."""

    def __init__(self, mapping=None, *, down=False, delay=0.0):
        self.mapping = dict(mapping or {})
        self.down = down
        self.delay = delay
        self.calls = []
        self.active = 0
        self.max_active = 0
        self._lock = threading.Lock()

    def handler(self, request):
        assert request.url.path == "/server/api", "conversion must never open a playback session"
        params = request.url.params
        assert params["method"] == "get_content_id" and params["api_version"] == "3"
        with self._lock:
            self.calls.append(params["infohash"])
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            if self.delay:
                time.sleep(self.delay)
            if self.down:
                raise httpx.ConnectError("connection refused")
            content_id = self.mapping.get(params["infohash"])
            if content_id:
                return httpx.Response(200, json={"result": {"content_id": content_id}})
            return httpx.Response(200, json={"result": None, "error": "unknown infohash"})
        finally:
            with self._lock:
                self.active -= 1

    def client(self, url=ENGINE):
        client = EngineClient(url, client=httpx.Client(transport=httpx.MockTransport(self.handler)))
        client._owns_client = True
        return client


@pytest.fixture(autouse=True)
def isolated_cache(tmp_path, monkeypatch):
    path = tmp_path / "config" / resolver_module.CACHE_FILE_NAME
    monkeypatch.setattr(resolver_module, "cache_path", lambda: path)
    resolver_module._caches.clear()
    resolver_module.reset_negative_cache()
    yield path
    resolver_module._caches.clear()
    resolver_module.reset_negative_cache()


def use_engine(monkeypatch, engine):
    monkeypatch.setattr(resolver_module, "EngineClient", engine.client)


def set_engine_url(db_session, url=ENGINE):
    SettingsRepository(db_session).set_setting(SettingsRepository.ACE_ENGINE_URL, url)


# --- engine helper -----------------------------------------------------------

def _engine_client(handler):
    return EngineClient(ENGINE, client=httpx.Client(transport=httpx.MockTransport(handler)))


def test_get_content_id_uses_server_api_and_lowercases():
    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        return httpx.Response(200, json={"result": {"content_id": CID1.upper()}})

    assert _engine_client(handler).get_content_id(IH1) == CID1
    assert seen["url"] == f"{ENGINE}/server/api?api_version=3&method=get_content_id&infohash={IH1}"


@pytest.mark.parametrize("payload", [
    {"result": None, "error": "unknown infohash"},
    {"result": {}},
    {"result": {"content_id": "not-a-hash"}},
    {"result": {"content_id": None}},
    ["unexpected"],
])
def test_get_content_id_returns_none_when_engine_knows_no_content_id(payload):
    assert _engine_client(lambda r: httpx.Response(200, json=payload)).get_content_id(IH1) is None


@pytest.mark.parametrize("make", [
    lambda r: httpx.Response(503, text="busy"),
    lambda r: httpx.Response(200, text="<html>"),
    lambda r: (_ for _ in ()).throw(httpx.ConnectError("down")),
    lambda r: (_ for _ in ()).throw(httpx.ReadTimeout("slow")),
])
def test_get_content_id_unreachable_engine_raises(make):
    with pytest.raises(EngineUnavailableError):
        _engine_client(make).get_content_id(IH1)


# --- cache -------------------------------------------------------------------

def test_cache_hit_miss_and_persistence(tmp_path):
    path = tmp_path / "cache.json"
    cache = InfohashCache(path)
    assert cache.get(IH1) is None
    cache.put_many({IH1: CID1})
    assert cache.get(IH1) == CID1
    assert InfohashCache(path).get(IH1) == CID1
    assert json.loads(path.read_text())["entries"] == {IH1: CID1}


def test_cache_corrupt_file_starts_empty_and_is_rewritten(tmp_path, caplog):
    path = tmp_path / "cache.json"
    path.write_text("{not json")
    cache = InfohashCache(path)
    assert cache.get(IH1) is None
    assert "corrupt" in caplog.text
    cache.put_many({IH1: CID1})
    assert InfohashCache(path).get(IH1) == CID1


def test_cache_ignores_malformed_entries(tmp_path):
    path = tmp_path / "cache.json"
    path.write_text(json.dumps({"version": 1, "entries": {IH1: CID1, "x": CID2, IH2: "nope"}}))
    cache = InfohashCache(path)
    assert cache.get_many([IH1, IH2, "x"]) == {IH1: CID1}


def test_cache_write_is_atomic(tmp_path, monkeypatch):
    path = tmp_path / "cache.json"
    InfohashCache(path).put_many({IH1: CID1})

    def failing_replace(src, dst):
        raise OSError("disk full")

    monkeypatch.setattr(resolver_module.os, "replace", failing_replace)
    cache = InfohashCache(path)
    cache.put_many({IH2: CID2})
    # The old file is intact, no temp file is left behind, memory keeps both.
    assert json.loads(path.read_text())["entries"] == {IH1: CID1}
    assert [p.name for p in tmp_path.iterdir()] == ["cache.json"]
    assert cache.get_many([IH1, IH2]) == {IH1: CID1, IH2: CID2}


def test_cache_unwritable_directory_keeps_memory(tmp_path):
    blocker = tmp_path / "file"
    blocker.write_text("")
    cache = InfohashCache(blocker / "cache.json")
    cache.put_many({IH1: CID1})
    assert cache.get(IH1) == CID1


def test_cache_is_bounded_and_keeps_the_most_recent(tmp_path):
    path = tmp_path / "cache.json"
    cache = InfohashCache(path, max_entries=3)
    keys = [f"{n:040x}" for n in range(5)]
    for key in keys:
        cache.put_many({key: CID1})
    assert list(cache.get_many(keys)) == keys[2:]
    assert list(json.loads(path.read_text())["entries"]) == keys[2:]
    assert list(InfohashCache(path, max_entries=2).get_many(keys)) == keys[3:]


def test_cache_concurrent_writers_lose_nothing(tmp_path):
    cache = InfohashCache(tmp_path / "cache.json")
    keys = [f"{n:040x}" for n in range(200)]

    def write(chunk):
        for key in chunk:
            cache.put_many({key: CID1})

    threads = [threading.Thread(target=write, args=(keys[i::4],)) for i in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(InfohashCache(tmp_path / "cache.json").get_many(keys)) == 200


# --- resolver ----------------------------------------------------------------

def test_resolve_many_uses_cache_then_engine_and_counts_failures(isolated_cache):
    engine = FakeEngine({IH1: CID1})
    resolver_module.get_cache().put_many({IH2: CID2})
    resolver = ContentIdResolver(ENGINE, client_factory=engine.client)
    outcome = asyncio.run(resolver.resolve_many([IH1, IH2.upper(), IH_UNKNOWN, IH1]))
    assert outcome.resolved == {IH1: CID1, IH2: CID2}
    assert outcome.unknown == {IH_UNKNOWN}
    assert sorted(engine.calls) == sorted([IH1, IH_UNKNOWN])
    assert json.loads(isolated_cache.read_text())["entries"] == {IH2: CID2, IH1: CID1}

    # Failures are not persisted; the unknown answer is remembered briefly in memory.
    again = asyncio.run(resolver.resolve_many([IH1, IH_UNKNOWN]))
    assert again.resolved == {IH1: CID1} and again.unknown == {IH_UNKNOWN}
    assert len(engine.calls) == 2


def test_resolve_many_stops_calling_an_unreachable_engine():
    engine = FakeEngine(down=True)
    resolver = ContentIdResolver(ENGINE, client_factory=engine.client, concurrency=1)
    hashes = [f"{n:040x}" for n in range(50)]
    outcome = asyncio.run(resolver.resolve_many(hashes))
    assert outcome.unavailable == set(hashes)
    assert len(engine.calls) == 1


def test_resolve_many_without_engine_uses_cache_only():
    resolver_module.get_cache().put_many({IH1: CID1})
    outcome = asyncio.run(ContentIdResolver(None).resolve_many([IH1, IH2]))
    assert outcome.resolved == {IH1: CID1}
    assert outcome.no_engine == {IH2}


def test_large_batch_is_bounded_and_second_pass_is_cached():
    hashes = [f"{n:040x}" for n in range(1, 2001)]
    engine = FakeEngine({ih: ih[::-1] for ih in hashes}, delay=0.001)
    resolver = ContentIdResolver(ENGINE, client_factory=engine.client, concurrency=4)
    started = time.monotonic()
    outcome = asyncio.run(resolver.resolve_many(hashes))
    assert len(outcome.resolved) == 2000 and outcome.unresolved == 0
    assert engine.max_active <= 4
    assert time.monotonic() - started < 60
    engine.calls.clear()
    second = asyncio.run(ContentIdResolver(ENGINE, client_factory=engine.client).resolve_many(hashes))
    assert len(second.resolved) == 2000 and engine.calls == [] and second.engine_calls == 0


def test_resolve_one_errors():
    with pytest.raises(EngineNotConfiguredError):
        ContentIdResolver(None).resolve_one(IH1)
    with pytest.raises(InfohashNotResolvedError):
        ContentIdResolver(ENGINE, client_factory=FakeEngine().client).resolve_one(IH1)
    with pytest.raises(InfohashNotResolvedError):
        ContentIdResolver(ENGINE, client_factory=FakeEngine().client).resolve_one("not-a-hash")
    with pytest.raises(EngineUnavailableError):
        ContentIdResolver(ENGINE, client_factory=FakeEngine(down=True).client).resolve_one(IH1)


# --- M3U parser --------------------------------------------------------------

def test_m3u_tags_infohash_entries_and_keeps_id_entries_unchanged():
    content = f"""#EXTM3U
#EXTINF:-1 tvg-id="one",One
http://127.0.0.1:6878/ace/getstream?infohash={IH1}
#EXTINF:-1,Two
http://127.0.0.1:6878/ace/getstream?pid=7&amp;infohash={IH2.upper()}&format=json
#EXTINF:-1,Three
http://host:6878/ace/manifest.m3u8?infohash={IH_UNKNOWN}
#EXTINF:-1,Direct
acestream://{DIRECT}
#EXTINF:-1,Content id link
http://127.0.0.1:6878/ace/getstream?id={CID1}
#EXTINF:-1,Plain HTTP
http://example.com/live.m3u8
"""
    channels = M3UService().extract_channels_from_content(content)
    by_name = {name: (channel_id, metadata) for channel_id, name, metadata in channels}
    assert by_name["One"] == (IH1, {"tvg_id": "one", "id_kind": "infohash"})
    assert by_name["Two"] == (IH2, {"id_kind": "infohash"})
    assert by_name["Three"] == (IH_UNKNOWN, {"id_kind": "infohash"})
    assert by_name["Direct"] == (DIRECT, {})
    assert by_name["Content id link"] == (CID1, {})
    assert by_name["Plain HTTP"][0].startswith("url:")


def test_m3u_ignores_infohash_that_is_not_an_engine_parameter():
    content = f"#EXTM3U\n#EXTINF:-1,Other\nhttp://example.com/watch?hash={IH1}\n"
    channel_id, _name, metadata = M3UService().extract_channels_from_content(content)[0]
    assert channel_id.startswith("url:") and "id_kind" not in metadata


def test_bare_ids_rule_is_unchanged():
    from app.models.url_types import RegularURL
    from app.scrapers.http import HTTPScraper

    scraper = HTTPScraper(RegularURL("https://example.com/page"))
    assert scraper.extract_bare_ids(f"Link http://host/ace/getstream?infohash={IH1}") == []


# --- scrape flow -------------------------------------------------------------

class _FakeScraper:
    def __init__(self, source_url, content):
        from app.models.url_types import RegularURL

        self.url_obj = RegularURL(source_url)
        self.content = content
        self.statuses = []

    async def scrape(self):
        return M3UService().extract_channels_from_content(self.content), "OK"

    async def update_url_status(self, url, status, error=None):
        self.statuses.append(status)


def _scrape(db_session, monkeypatch, source_url, content):
    scraper = _FakeScraper(source_url, content)
    monkeypatch.setattr("app.services.scraper_service.create_scraper_for_url", lambda _u, _t: scraper)
    channels, status = asyncio.run(ScraperService(db_session).scrape_url(source_url, "regular"))
    return channels, status, scraper


def _ids(db_session, source_url):
    rows = db_session.query(AcestreamChannel).filter(AcestreamChannel.source_url == source_url).all()
    return {row.id for row in rows}


MIXED = f"""#EXTM3U
#EXTINF:-1,One
http://127.0.0.1:6878/ace/getstream?infohash={IH1}
#EXTINF:-1,Two
http://127.0.0.1:6878/ace/getstream?infohash={IH2}
#EXTINF:-1,Direct
acestream://{DIRECT}
"""


def test_mixed_source_persists_content_ids(db_session, monkeypatch):
    source = "https://example.com/mixed.m3u"
    set_engine_url(db_session)
    use_engine(monkeypatch, FakeEngine({IH1: CID1, IH2: CID2}))
    channels, status, _ = _scrape(db_session, monkeypatch, source, MIXED)
    assert status == "OK"
    assert {c.channel_id for c in channels} == {CID1, CID2, DIRECT}
    assert _ids(db_session, source) == {CID1, CID2, DIRECT}
    assert db_session.query(AcestreamChannel).filter(AcestreamChannel.id.in_([IH1, IH2])).count() == 0


def test_unresolved_entries_are_skipped_and_counted(db_session, monkeypatch):
    source = "https://example.com/partial.m3u"
    set_engine_url(db_session)
    use_engine(monkeypatch, FakeEngine({IH1: CID1}))
    channels, status, scraper = _scrape(db_session, monkeypatch, source, MIXED)
    assert status == "OK — 2 imported, 1 infohash unresolved (1 unknown to the engine)"
    assert scraper.statuses == [status]
    assert _ids(db_session, source) == {CID1, DIRECT}


def test_pruning_uses_resolved_ids(db_session, monkeypatch):
    source = "https://example.com/prune.m3u"
    db_session.add_all([
        AcestreamChannel(id=CID1, name="One", source_url=source),
        AcestreamChannel(id="5" * 40, name="Gone", source_url=source),
        AcestreamChannel(id=IH1, name="Legacy raw infohash", source_url=source),
    ])
    db_session.commit()
    set_engine_url(db_session)
    use_engine(monkeypatch, FakeEngine({IH1: CID1, IH2: CID2}))
    _scrape(db_session, monkeypatch, source, MIXED)
    assert _ids(db_session, source) == {CID1, CID2, DIRECT}


def test_engine_outage_preserves_stored_channels(db_session, monkeypatch):
    source = "https://example.com/outage.m3u"
    content = f"#EXTM3U\n#EXTINF:-1,One\nhttp://127.0.0.1:6878/ace/getstream?infohash={IH1}\n"
    db_session.add_all([
        AcestreamChannel(id=CID1, name="One", source_url=source),
        AcestreamChannel(id=CID2, name="Two", source_url=source),
    ])
    db_session.commit()
    set_engine_url(db_session)
    use_engine(monkeypatch, FakeEngine(down=True))
    channels, status, scraper = _scrape(db_session, monkeypatch, source, content)
    assert channels == []
    assert status.startswith("Error: AceStream engine unreachable")
    assert scraper.statuses == [status]
    assert _ids(db_session, source) == {CID1, CID2}


def test_partial_outage_imports_but_does_not_prune(db_session, monkeypatch):
    source = "https://example.com/partial-outage.m3u"
    db_session.add(AcestreamChannel(id=CID2, name="Two", source_url=source))
    db_session.commit()
    resolver_module.get_cache().put_many({IH1: CID1})
    set_engine_url(db_session)
    use_engine(monkeypatch, FakeEngine(down=True))
    channels, status, _ = _scrape(db_session, monkeypatch, source, MIXED)
    assert {c.channel_id for c in channels} == {CID1, DIRECT}
    assert "1 engine unreachable" in status and status.startswith("OK")
    assert _ids(db_session, source) == {CID1, CID2, DIRECT}


def test_no_engine_skips_infohash_entries_but_imports_the_rest(db_session, monkeypatch):
    source = "https://example.com/no-engine.m3u"
    set_engine_url(db_session, "")
    engine = FakeEngine({IH1: CID1})
    use_engine(monkeypatch, engine)
    channels, status, _ = _scrape(db_session, monkeypatch, source, MIXED)
    assert [c.channel_id for c in channels] == [DIRECT]
    assert status == "OK — 1 imported, 2 infohashes unresolved (2 no engine configured)"
    assert engine.calls == []


def test_no_engine_and_only_infohash_entries_is_an_error(db_session, monkeypatch):
    source = "https://example.com/only-infohash.m3u"
    set_engine_url(db_session, "")
    content = f"#EXTM3U\n#EXTINF:-1,One\nhttp://127.0.0.1:6878/ace/getstream?infohash={IH1}\n"
    channels, status, _ = _scrape(db_session, monkeypatch, source, content)
    assert channels == [] and status.startswith("Error:") and "engine URL in Settings" in status


def test_duplicates_after_conversion_are_stored_once(db_session, monkeypatch):
    source = "https://example.com/dupes.m3u"
    content = f"""#EXTM3U
#EXTINF:-1,Direct first
acestream://{CID1}
#EXTINF:-1,Same content via infohash
http://127.0.0.1:6878/ace/getstream?infohash={IH1}
#EXTINF:-1,Another infohash, same content
http://127.0.0.1:6878/ace/getstream?infohash={IH2}
#EXTINF:-1,Repeated infohash
http://127.0.0.1:6878/ace/getstream?infohash={IH1.upper()}
"""
    set_engine_url(db_session)
    use_engine(monkeypatch, FakeEngine({IH1: CID1, IH2: CID1}))
    channels, status, _ = _scrape(db_session, monkeypatch, source, content)
    assert status == "OK"
    assert [c.channel_id for c in channels] == [CID1]
    assert db_session.get(AcestreamChannel, CID1).name == "Direct first"


def test_converted_id_already_assigned_elsewhere_keeps_its_tv_channel(db_session, monkeypatch):
    other = "https://example.com/other.m3u"
    tv = TVChannel(name="Existing TV", is_active=True)
    db_session.add(tv)
    db_session.commit()
    db_session.add(AcestreamChannel(id=CID1, name="One", source_url=other, tv_channel_id=tv.id))
    db_session.commit()
    source = "https://example.com/second.m3u"
    content = f"#EXTM3U\n#EXTINF:-1,One again\nhttp://127.0.0.1:6878/ace/getstream?infohash={IH1}\n"
    set_engine_url(db_session)
    use_engine(monkeypatch, FakeEngine({IH1: CID1}))
    _scrape(db_session, monkeypatch, source, content)
    db_session.expire_all()
    rows = db_session.query(AcestreamChannel).filter(AcestreamChannel.id == CID1).all()
    assert len(rows) == 1 and rows[0].tv_channel_id == tv.id


def test_second_scrape_is_served_from_cache(db_session, monkeypatch, isolated_cache):
    source = "https://example.com/cached.m3u"
    set_engine_url(db_session)
    engine = FakeEngine({IH1: CID1, IH2: CID2})
    use_engine(monkeypatch, engine)
    _scrape(db_session, monkeypatch, source, MIXED)
    assert len(engine.calls) == 2
    resolver_module._caches.clear()  # a restart: only the file remains
    _scrape(db_session, monkeypatch, source, MIXED)
    assert len(engine.calls) == 2
    assert isolated_cache.exists()


# --- add endpoints -----------------------------------------------------------

@pytest.fixture
def engine_for_api(db_session, monkeypatch):
    set_engine_url(db_session)
    engine = FakeEngine({IH1: CID1})
    use_engine(monkeypatch, engine)
    return engine


def test_create_with_infohash_stores_content_id(client, db_session, engine_for_api):
    response = client.post("/api/v1/acestream-channels/", json={"id": IH1, "name": "From search", "id_type": "infohash"})
    assert response.status_code == 201, response.text
    assert response.json()["id"] == CID1
    assert db_session.get(AcestreamChannel, CID1) is not None
    assert db_session.get(AcestreamChannel, IH1) is None


def test_create_with_unknown_infohash_stores_nothing(client, db_session, engine_for_api):
    response = client.post("/api/v1/acestream-channels/", json={"id": IH2, "name": "Unknown", "id_type": "infohash"})
    assert response.status_code == 422
    error = response.json()["error"]
    assert error["code"] == "INFOHASH_UNRESOLVED"
    assert error["message"].startswith("Could not convert this infohash to a content ID")
    assert db_session.query(AcestreamChannel).count() == 0


def test_create_with_infohash_engine_down(client, db_session, monkeypatch):
    set_engine_url(db_session)
    use_engine(monkeypatch, FakeEngine(down=True))
    response = client.post("/api/v1/acestream-channels/", json={"id": IH1, "name": "x", "id_type": "infohash"})
    assert response.status_code == 502
    assert response.json()["error"]["code"] == "ENGINE_UNAVAILABLE"
    assert "engine:6878" not in response.text
    assert db_session.query(AcestreamChannel).count() == 0


def test_create_with_infohash_and_no_engine(client, db_session):
    set_engine_url(db_session, "")
    response = client.post("/api/v1/acestream-channels/", json={"id": IH1, "name": "x", "id_type": "infohash"})
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "ENGINE_NOT_CONFIGURED"
    assert db_session.query(AcestreamChannel).count() == 0


def test_create_with_infohash_of_existing_channel_updates_it(client, db_session, engine_for_api):
    db_session.add(AcestreamChannel(id=CID1, name="Old", source_url="https://example.com/a.m3u"))
    db_session.commit()
    response = client.post("/api/v1/acestream-channels/", json={"id": IH1, "name": "New", "id_type": "infohash"})
    assert response.status_code == 201
    db_session.expire_all()
    assert db_session.get(AcestreamChannel, CID1).name == "New"
    assert db_session.query(AcestreamChannel).count() == 1


def test_create_without_flag_keeps_the_id(client, db_session, engine_for_api):
    response = client.post("/api/v1/acestream-channels/", json={"id": DIRECT, "name": "Direct"})
    assert response.status_code == 201 and response.json()["id"] == DIRECT
    assert engine_for_api.calls == []


def test_search_add_endpoints_convert_infohashes(client, db_session, engine_for_api):
    response = client.post("/api/v1/search/add", json={"id": IH1, "name": "One", "url": "Acestream Search", "id_type": "infohash"})
    assert response.status_code == 201
    assert response.json()["channel"]["id"] == CID1

    response = client.post("/api/v1/search/add", json={"id": IH2, "name": "Two", "url": "Acestream Search", "id_type": "infohash"})
    assert response.status_code == 422

    response = client.post("/api/v1/search/add_multiple", json={"channels": [
        {"id": IH1, "name": "One", "url": "Acestream Search", "id_type": "infohash"},
        {"id": IH2, "name": "Two", "url": "Acestream Search", "id_type": "infohash"},
    ]})
    data = response.json()
    assert data["updated_count"] == 1 and data["failed_count"] == 1
    assert "Could not convert this infohash" in data["results"][1]["error"]
    assert db_session.get(AcestreamChannel, IH2) is None
    assert db_session.get(AcestreamChannel, IH1) is None


def test_search_add_without_engine_stores_nothing(client, db_session):
    set_engine_url(db_session, "")
    response = client.post("/api/v1/search/add", json={"id": IH1, "name": "One", "url": "Acestream Search", "id_type": "infohash"})
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "ENGINE_NOT_CONFIGURED"
    response = client.post("/api/v1/search/add_multiple", json={"channels": [
        {"id": IH1, "name": "One", "url": "Acestream Search", "id_type": "infohash"},
    ]})
    assert response.json()["failed_count"] == 1
    assert "No AceStream engine is configured" in response.json()["results"][0]["error"]
    assert db_session.query(AcestreamChannel).count() == 0


def test_large_playlist_scrape_is_bounded_and_rescrape_uses_the_cache(db_session, monkeypatch):
    source = "https://example.com/large.m3u"
    hashes = [f"{n:040x}" for n in range(1, 2001)]
    content = "#EXTM3U\n" + "".join(
        f"#EXTINF:-1,Channel {n}\nhttp://127.0.0.1:6878/ace/getstream?infohash={ih}\n" for n, ih in enumerate(hashes)
    )
    set_engine_url(db_session)
    engine = FakeEngine({ih: ih[::-1] for ih in hashes}, delay=0.001)
    use_engine(monkeypatch, engine)
    started = time.monotonic()
    channels, status, _ = _scrape(db_session, monkeypatch, source, content)
    assert status == "OK" and len(channels) == 2000
    assert engine.max_active <= resolver_module.RESOLVE_CONCURRENCY
    assert time.monotonic() - started < 120
    assert len(engine.calls) == 2000
    engine.calls.clear()
    channels, status, _ = _scrape(db_session, monkeypatch, source, content)
    assert status == "OK" and len(channels) == 2000 and engine.calls == []
    assert _ids(db_session, source) == {ih[::-1] for ih in hashes}
