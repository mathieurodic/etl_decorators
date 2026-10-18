"""Tests for `etl_decorators.download`.

Run with pytest (the module docstring of the original scratch test served as
the specification for these tests).
"""
from __future__ import annotations

import gc
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from etl_decorators.download import Progress, download, _local_path

DATA = (300_000).to_bytes(300_000, "big")  # deterministic pseudo-random body
LATIN = "h\u00e9llo\nw\u00f6rld\n".encode("latin-1")
BIG = bytes(range(256)) * (4 * 1024 * 1024 // 256)  # 4 MiB, delivered fast (backpressure tests)


class _Hits:
    """Records (path, Range header) of every request the test server got."""

    def __init__(self) -> None:
        self._list: list[tuple[str, str | None]] = []
        self._lock = threading.Lock()

    def add(self, path: str, rng: str | None) -> None:
        with self._lock:
            self._list.append((path, rng))

    def __len__(self) -> int:
        with self._lock:
            return len(self._list)

    @property
    def last(self) -> tuple[str, str | None]:
        with self._lock:
            return self._list[-1]

    def clear(self) -> None:
        with self._lock:
            self._list.clear()


def make_server(hits: _Hits) -> ThreadingHTTPServer:
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):  # silence the test server
            pass

        def do_GET(self):
            rng = self.headers.get("Range")
            hits.add(self.path, rng)
            if self.path == "/big":  # a large body delivered as fast as possible
                self.send_response(200)
                self.send_header("ETag", '"abc"')
                self.send_header("Content-Type", "application/zip")
                self.send_header("Content-Length", str(len(BIG)))
                self.end_headers()
                try:
                    self.wfile.write(BIG)
                except OSError:  # the client closed: stop writing
                    pass
                return
            if self.path == "/slow":  # trickles DATA so a test can cancel it mid-flight
                self.send_response(200)
                self.send_header("ETag", '"abc"')
                self.send_header("Content-Type", "application/zip")
                self.send_header("Content-Length", str(len(DATA)))
                self.end_headers()
                try:
                    for i in range(0, len(DATA), 1000):
                        self.wfile.write(DATA[i:i + 1000])
                        self.wfile.flush()
                        time.sleep(0.01)  # ~100 KiB/s: a 300 KiB body takes ~3 s
                except OSError:  # the client closed: stop trickling
                    pass
                return
            start = int(rng[6:-1]) if rng else 0
            body = LATIN if self.path == "/latin" else DATA[start:]
            self.send_response(206 if rng else 200)
            self.send_header("ETag", '"abc"')
            self.send_header(
                "Content-Type",
                {
                    "/page": 'Text/HTML; charset="UTF-8"',
                    "/latin": "text/plain; charset=ISO-8859-1",
                }.get(self.path, "application/zip"),
            )
            if rng:
                self.send_header("Content-Range", f"bytes {start}-{len(DATA)-1}/{len(DATA)}")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


@pytest.fixture()
def env(tmp_path: Path):
    """A running test HTTP server + a download() bound to a temp cache dir."""
    hits = _Hits()
    srv = make_server(hits)
    port = srv.server_port

    def U(p: str) -> str:
        return f"http://127.0.0.1:{port}{p}"

    D = download(str(tmp_path))
    try:
        yield tmp_path, D, U, hits, f"127.0.0.1+{port}"
    finally:
        srv.shutdown()
        srv.server_close()
        hits.clear()


# ---------------------------------------------------------------- pure path mapping


@pytest.mark.parametrize(
    "url, expected",
    [
        ("https://example.org/directory/thing.zip", "https/example.org/directory/thing.zip"),
        ("http://example.org", "http/example.org"),
        ("http://example.org:8080/x", "http/example.org+8080/x"),
        # credentials are not used; '.', '.INDEX' and '.DOWNLOAD' are escaped
        ("https://u:p@example.org/a/../.INDEX/x.DOWNLOAD",
         "https/example.org/a/%2E./%2EINDEX/x%2EDOWNLOAD"),
    ],
)
def test_local_path_mapping(url: str, expected: str):
    base = Path("my/favorite/path")
    assert _local_path(base, url) == base / expected


def test_local_path_escapes_download_suffix_in_query():
    base = Path("my/favorite/path")
    assert _local_path(base, "https://example.org/p?q=1.DOWNLOAD").name.endswith("%2EDOWNLOAD")


def test_local_path_rejects_relative_urls():
    with pytest.raises(ValueError):
        _local_path(Path("b"), "not-a-url")


# ---------------------------------------------------------------- fresh download + cache


def test_fresh_download_layout_and_content_type(env):
    T, D, U, hits, host = env
    s = D(U("/dir/thing.zip"))
    assert s.content_type == "application/zip"
    assert s.read() == DATA
    f = T / "http" / host / "dir" / "thing.zip"
    assert f.is_file()
    assert f.with_name(f.name + ".DOWNLOAD").is_file()


def test_cached_download_makes_no_request(env):
    T, D, U, hits, host = env
    D(U("/dir/thing.zip")).read()
    n = len(hits)
    s = D(U("/dir/thing.zip"))
    assert s.content_type == "application/zip"
    assert s.read() == DATA
    assert len(hits) == n  # served entirely from the disk cache


def test_decorator_usage_and_direct_call_agree(env):
    T, D, U, hits, host = env

    @download(str(T))
    def url_of(path: str) -> str:
        return U(path)

    assert url_of("/dir/thing.zip").read() == D(U("/dir/thing.zip")).read()


def test_decorated_function_must_return_str(env):
    T, D, U, hits, host = env

    @download(str(T))
    def bad() -> int:
        return 42

    with pytest.raises(TypeError):
        bad()


def test_download_rejects_non_str_non_callable():
    with pytest.raises(TypeError):
        download("x")(123)


def test_resume_after_partial_download(env):
    T, D, U, hits, host = env
    D(U("/dir/thing.zip")).read()
    f = T / "http" / host / "dir" / "thing.zip"
    m = f.with_name(f.name + ".DOWNLOAD")
    f.write_bytes(DATA[:100_000])
    meta = json.loads(m.read_text(encoding="utf-8"))
    meta.update(downloaded=100_000, complete=False)
    m.write_text(json.dumps(meta), encoding="utf-8")

    s = D(U("/dir/thing.zip"))
    assert s.content_type == "application/zip"
    assert s.read() == DATA
    assert hits.last[1] == "bytes=100000-"  # resumed where the cache stopped


def test_content_type_and_charset_are_read_only(env):
    T, D, U, hits, host = env
    s = D(U("/dir/thing.zip"))
    with pytest.raises(AttributeError):
        s.content_type = "x"
    with pytest.raises(AttributeError):
        s.charset = "x"


# ---------------------------------------------------------------- promote: file first, then child


def test_promote_file_to_directory(env):
    T, D, U, hits, host = env
    assert D(U("/mammals")).read() == DATA
    assert (T / "http" / host / "mammals").is_file()

    assert D(U("/mammals/cats")).read() == DATA
    mam = T / "http" / host / "mammals"
    assert mam.is_dir()
    assert (mam / ".INDEX").is_file()
    assert (mam / ".INDEX.DOWNLOAD").is_file()
    assert (mam / "cats").is_file()

    # the original URL is now served from the .INDEX file, without a request
    n = len(hits)
    s = D(U("/mammals"))
    assert s.read() == DATA
    assert s.content_type == "application/zip"
    assert len(hits) == n


def test_promote_child_first_then_page(env):
    T, D, U, hits, host = env
    assert D(U("/birds/owls")).read() == DATA
    assert D(U("/birds")).read() == DATA
    bd = T / "http" / host / "birds"
    assert (bd / ".INDEX").is_file()
    assert (bd / "owls").is_file()
    assert not (bd / "birds").exists()

    n = len(hits)
    assert D(U("/birds")).read() == DATA
    assert len(hits) == n


# ---------------------------------------------------------------- content_type / charset


def test_content_type_charset_split_fresh_and_cached(env):
    T, D, U, hits, host = env
    s = D(U("/page"))
    assert (s.content_type, s.charset) == ("text/html", "utf-8")
    s.read()

    n = len(hits)
    s = D(U("/page"))
    assert (s.content_type, s.charset) == ("text/html", "utf-8")
    assert s.read() == DATA
    assert len(hits) == n

    s = D(U("/dir/thing.zip"))
    assert (s.content_type, s.charset) == ("application/zip", None)
    s.read()


# ---------------------------------------------------------------- as_text_stream


def test_as_text_stream_uses_server_charset(env):
    T, D, U, hits, host = env
    t = D(U("/latin")).as_text_stream()
    assert list(t) == ["h\u00e9llo\n", "w\u00f6rld\n"]
    t.close()


def test_as_text_stream_cached(env):
    T, D, U, hits, host = env
    first = D(U("/latin")).as_text_stream()
    assert first.read() == "h\u00e9llo\nw\u00f6rld\n"  # drain to EOF so the cache is complete
    first.close()
    n = len(hits)
    t = D(U("/latin")).as_text_stream()
    assert t.read() == "h\u00e9llo\nw\u00f6rld\n"
    t.close()
    assert len(hits) == n


def test_as_text_stream_encoding_override(env):
    T, D, U, hits, host = env
    t = D(U("/latin")).as_text_stream(encoding="ascii", errors="replace")
    assert t.read() == "h\ufffdllo\nw\ufffdrld\n"
    t.close()


def test_as_text_stream_utf8_on_binary_body(env):
    T, D, U, hits, host = env
    # /page declares charset=utf-8 but serves random bytes: it just decodes
    t = D(U("/page")).as_text_stream(errors="replace")
    assert t.read(5)
    t.close()


# ---------------------------------------------------------------- on_progress

def test_on_progress_reports_monotonic_bytes_on_fresh_download(env):
    T, D, U, hits, host = env
    total = len(DATA)
    seen: list[tuple[int, int | None]] = []
    D = download(str(T), on_progress=lambda n, t: seen.append((n, t)))
    assert D(U("/dir/thing.zip")).read() == DATA
    assert seen, "on_progress was never called"
    assert seen[-1] == (total, total)  # ends at the full size with the known total
    counts = [n for n, _ in seen]
    assert counts == sorted(counts)  # monotonic
    assert all(t == total for _, t in seen)  # Content-Length is forwarded


def test_on_progress_reports_replay_from_cache(env):
    T, D, U, hits, host = env
    D(U("/dir/thing.zip")).read()  # populate the cache first
    seen: list[tuple[int, int | None]] = []
    D2 = download(str(T), on_progress=lambda n, t: seen.append((n, t)))
    assert D2(U("/dir/thing.zip")).read() == DATA
    assert seen[-1] == (len(DATA), len(DATA))  # cached replay also reports progress


def test_on_progress_reports_replay_on_resume(env):
    T, D, U, hits, host = env
    D(U("/dir/thing.zip")).read()
    f = T / "http" / host / "dir" / "thing.zip"
    m = f.with_name(f.name + ".DOWNLOAD")
    f.write_bytes(DATA[:100_000])
    meta = json.loads(m.read_text(encoding="utf-8"))
    meta.update(downloaded=100_000, complete=False)
    m.write_text(json.dumps(meta), encoding="utf-8")

    seen: list[tuple[int, int | None]] = []
    D2 = download(str(T), on_progress=lambda n, t: seen.append((n, t)))
    assert D2(U("/dir/thing.zip")).read() == DATA
    assert seen[-1] == (len(DATA), len(DATA))  # the replayed prefix and the tail are both counted


def test_on_progress_none_is_the_default(tmp_path):
    # the callback is optional; a plain download()/read() still works
    assert download(str(tmp_path)).on_progress is None


# ---------------------------------------------------------------- polled progress

def test_progress_before_start_reports_zero_and_does_not_start_the_download(env):
    T, D, U, hits, host = env
    s = D(U("/dir/thing.zip"))
    assert s.progress == Progress(0, None, False)
    assert len(hits) == 0  # polling must not trigger the fetch


def test_progress_after_a_full_read_is_complete(env):
    T, D, U, hits, host = env
    s = D(U("/dir/thing.zip"))
    assert s.read() == DATA
    assert s.progress == Progress(len(DATA), len(DATA), True)


def test_progress_reports_complete_when_the_reader_stops_early_but_stays_open(env):
    T, D, U, hits, host = env
    s = D(U("/dir/thing.zip"))
    assert s.read(10)  # start the fetch, then stop reading (without closing the stream)
    deadline = time.monotonic() + 5  # the writer keeps going while the stream is alive
    while not s.progress.complete and time.monotonic() < deadline:
        time.sleep(0.01)
    p = s.progress
    assert p.complete is True       # polling is the only completion signal here
    assert p.downloaded == len(DATA)
    assert p.total == len(DATA)
    s.close()


def test_progress_from_cache_reports_complete(env):
    T, D, U, hits, host = env
    D(U("/dir/thing.zip")).read()  # populate the cache first
    s = download(str(T))(U("/dir/thing.zip"))
    assert s.read() == DATA
    assert s.progress == Progress(len(DATA), len(DATA), True)


def test_progress_on_resume_ends_complete(env):
    T, D, U, hits, host = env
    D(U("/dir/thing.zip")).read()
    f = T / "http" / host / "dir" / "thing.zip"
    m = f.with_name(f.name + ".DOWNLOAD")
    f.write_bytes(DATA[:100_000])
    meta = json.loads(m.read_text(encoding="utf-8"))
    meta.update(downloaded=100_000, complete=False)
    m.write_text(json.dumps(meta), encoding="utf-8")

    s = download(str(T))(U("/dir/thing.zip"))
    assert s.read() == DATA
    assert s.progress == Progress(len(DATA), len(DATA), True)


# ---------------------------------------------------------------- cancellation
#
# Closing the stream (or dropping it) cancels the background fetch at the next chunk
# boundary. The bytes already on disk stay there with their sidecar, so a later call
# resumes. These tests use the trickling "/slow" endpoint so cancellation is observable.


def _wait_for_worker(url: str, timeout: float = 10.0) -> None:
    """Block until the background thread for `url` has exited (close/GC cancelled it)."""
    name = f"download:{url}"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not any(t.name == name and t.is_alive() for t in threading.enumerate()):
            return
        time.sleep(0.02)
    raise AssertionError(f"worker {name!r} did not stop")


def test_close_cancels_the_download(env):
    T, D, U, hits, host = env
    url = U("/slow")
    s = D(url)
    assert len(s.read(10_000)) == 10_000  # start the fetch, read a little
    s.close()  # ...then abandon it: the worker must stop
    _wait_for_worker(url)

    p = s.progress
    assert p.complete is False, "the download kept running after close()"
    assert p.downloaded < len(DATA)

    f = T / "http" / host / "slow"
    assert f.stat().st_size == p.downloaded  # partial file matches the reported progress
    assert f.with_name(f.name + ".DOWNLOAD").is_file()  # sidecar kept, so it can resume


def test_closed_download_resumes_from_where_it_stopped(env):
    T, D, U, hits, host = env
    url = U("/slow")
    s = D(url)
    s.read(10_000)
    s.close()
    _wait_for_worker(url)
    n = s.progress.downloaded
    assert 0 < n < len(DATA)

    hits.clear()
    fresh = download(str(T))(url)
    assert fresh.read() == DATA  # resumed to completion
    assert hits.last[1] == f"bytes={n}-"  # ...exactly where the cancelled one stopped
    assert fresh.progress == Progress(len(DATA), len(DATA), True)


def test_garbage_collection_cancels_the_download(env):
    T, D, U, hits, host = env
    url = U("/slow")
    s = D(url)
    s.read(10_000)  # start the fetch, then drop the only strong reference on purpose
    del s
    gc.collect()  # the worker holds only a weakref, so the stream can now be collected
    _wait_for_worker(url)

    f = T / "http" / host / "slow"
    assert 0 < f.stat().st_size < len(DATA), "the download kept running after the GC"


# ---------------------------------------------------------------- backpressure


def _wait_until(pred, timeout: float = 5.0) -> bool:
    """Wait until `pred()` is true, or `timeout` elapses."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(0.01)
    return False


def test_slow_reader_applies_backpressure(env):
    T, D, U, hits, host = env
    url = U("/big")
    s = download(str(T), max_buffer=256 * 1024)(url)
    assert len(s.read(1)) == 1  # start the fetch, then stop reading
    # The fast server has 4 MiB to send; the worker must fill the bound and then block,
    # not slurp the whole body into memory ahead of a reader that is not reading.
    assert _wait_until(lambda: s.progress.downloaded >= 256 * 1024), "worker never filled the buffer"
    time.sleep(0.2)  # give it time to overrun, if it were going to
    assert s.progress.downloaded < len(BIG), "the whole body was buffered despite a lagging reader"
    assert s._buffered <= s._max_buffer, "the read-ahead buffer exceeded its bound"
    assert s.read() == BIG[1:]  # draining unblocks the worker and yields the rest
    assert s.progress == Progress(len(BIG), len(BIG), True)


def test_close_unblocks_a_writer_waiting_on_backpressure(env):
    T, D, U, hits, host = env
    url = U("/big")
    s = download(str(T), max_buffer=256 * 1024)(url)
    s.read(1)
    # Buffer fills up to the bound and the worker then blocks in feed(); one byte was consumed
    # by the read above, so the steady state is `max_buffer - 1`, not exactly `max_buffer`.
    assert _wait_until(lambda: s._buffered >= 256 * 1024 - 8192), "worker never filled the buffer"
    s.close()  # the worker is blocked in feed(): close must wake and stop it
    _wait_for_worker(url)


# ---------------------------------------------------------------- failures


def test_download_error_surfaces_via_content_type(tmp_path):
    # port 9 (discard) on 127.0.0.1: connection refused, quickly
    bad = download(str(tmp_path), timeout=1)("http://127.0.0.1:9/x")
    with pytest.raises(OSError):
        bad.content_type
