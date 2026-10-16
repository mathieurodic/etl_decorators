"""Tests for `etl_decorators.download`.

Run with pytest (the module docstring of the original scratch test served as
the specification for these tests).
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from etl_decorators.download import download, _local_path

DATA = (300_000).to_bytes(300_000, "big")  # deterministic pseudo-random body
LATIN = "h\u00e9llo\nw\u00f6rld\n".encode("latin-1")


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
    D(U("/latin")).as_text_stream().close()
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


# ---------------------------------------------------------------- failures


def test_download_error_surfaces_via_content_type(tmp_path):
    # port 9 (discard) on 127.0.0.1: connection refused, quickly
    bad = download(str(tmp_path), timeout=1)("http://127.0.0.1:9/x")
    with pytest.raises(OSError):
        bad.content_type
