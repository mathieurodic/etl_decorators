"""`download` decorator: stream a URL to the caller while caching it on disk, resumably.

    from download import download

    @download("my/favorite/path")
    def url_of(name):               # any function / method returning a str
        return f"http://example.org/{name}"

    stream = url_of("file.bin")     # -> DownloadStream (a readable binary io object)
    data = stream.read()

    stream = download("my/favorite/path")("http://example.org")   # direct use on a string
    stream.content_type                                           # e.g. "text/html" (or None)
    stream.charset                                       # e.g. "utf-8"     (or None)

Layout on disk (path "my/favorite/path"):
    https://example.org/directory/thing.zip  ->  my/favorite/path/https/example.org/directory/thing.zip
    http://example.org                       ->  my/favorite/path/http/example.org
    http://example.org:8080/x                ->  my/favorite/path/http/example.org+8080/x
    <data file>.DOWNLOAD                     JSON metadata next to the data (url, etag,
                                             last_modified, content_type, total,
                                             downloaded, complete)

Progress can be observed three ways, all fed by the same events: `progress_bar=True` draws a bar on
stderr, `on_progress(downloaded, total)` calls your function with the running byte count, and
`stream.progress` polls `Progress(downloaded, total, complete)` from any thread without blocking.

Reading while downloading is the intended use of the stream: the worker buffers at most `max_buffer`
bytes (8 MiB by default) ahead of the reader and then blocks, so a slow reader applies backpressure
rather than pulling a whole archive into memory.

Lifetime: closing the stream, or letting it be garbage-collected, cancels the background download at
the next chunk boundary. The bytes already written stay on disk with their sidecar, so downloading
the same URL again resumes where it stopped.

Name clashes: if "http://h/mammals" is cached as a file and "http://h/mammals/cats" arrives,
"mammals" is turned into a directory and the first page moves to "mammals/.INDEX" (its
metadata to "mammals/.INDEX.DOWNLOAD"). Whenever a URL's path is a directory, its content
is the ".INDEX" file inside. Names starting with "." or ending in ".DOWNLOAD" are escaped,
so no URL can collide with these reserved names.
"""
from __future__ import annotations

import collections
import functools
import hashlib
import io
import json
import os
import sys
import threading
import time
import uuid
import urllib.error
import weakref
from email.message import Message
import urllib.request
from pathlib import Path
from typing import Callable, NamedTuple, Optional
from urllib.parse import quote, urlsplit

__all__ = ["download", "DownloadStream", "Progress"]

CHUNK_SIZE = 64 * 1024
DEFAULT_MAX_BUFFER = 8 * 1024 * 1024  # bounded read-ahead; the writer blocks past this (backpressure)
META_SUFFIX = ".DOWNLOAD"
INDEX_NAME = ".INDEX"  # content of a URL whose path is a directory (other URLs live below it)

_locks: dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()


def _lock_for(path: Path) -> threading.Lock:
    """One lock per destination file, so two downloads of the same URL never interleave."""
    key = str(path.resolve())
    with _locks_guard:
        return _locks.setdefault(key, threading.Lock())


class Progress(NamedTuple):
    """Snapshot of a download's progress, returned by `DownloadStream.progress`."""
    downloaded: int        # bytes written for this URL (replayed plus freshly fetched)
    total: Optional[int]   # expected size, or None when the server sent no Content-Length
    complete: bool         # True once the whole file is on disk, even if the reader stopped early


class _ProgressState:
    """Thread-safe progress of one download: the source of truth behind `on_progress`.

    The worker thread writes it; any thread reads `snapshot()`. `complete` turns true once
    the whole file is on disk, whether or not the reader drained the stream.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._downloaded = 0
        self._total: Optional[int] = None
        self._complete = False

    def set(self, downloaded: int, total: Optional[int]) -> None:
        with self._lock:
            self._downloaded = downloaded
            self._total = total

    def set_downloaded(self, downloaded: int) -> None:
        with self._lock:
            self._downloaded = downloaded

    def set_complete(self) -> None:
        with self._lock:
            self._complete = True

    def snapshot(self) -> Progress:
        with self._lock:
            return Progress(self._downloaded, self._total, self._complete)


# --------------------------------------------------------------------------- stream


class DownloadStream(io.RawIOBase):
    """Double-ended stream: the worker thread calls feed()/finish(); the user just reads.

    Reading blocks until data is available, EOF, or the download failed (in which case
    the error is raised once the already-received bytes have been consumed).
    The download itself starts lazily, on the first read (or an explicit start()).
    """

    def __init__(self, starter: Callable[["DownloadStream"], None],
                 max_buffer: int = DEFAULT_MAX_BUFFER):
        super().__init__()
        self._starter = starter
        self._start_lock = threading.Lock()
        self._started = False
        self._cond = threading.Condition()
        self._chunks: collections.deque[bytes] = collections.deque()
        self._buffered = 0                # bytes currently queued ahead of the reader
        self._max_buffer = max(1, max_buffer)  # feed() blocks once this much is queued
        self._eof = False
        self._error: Optional[BaseException] = None
        self._content_type: Optional[str] = None
        self._charset: Optional[str] = None
        self._headers_known = False
        self._headers_ready = threading.Event()
        self._progress = _ProgressState()
        self._cancelled = threading.Event()
        # The worker holds only a weak reference to us, so we can be collected mid-download.
        # When that happens (or on close()) the finalizer stops the fetch; io.IOBase.__del__
        # also calls close(). Either way the partial file and its sidecar stay resumable.
        weakref.finalize(self, self._cancelled.set)

    # ---- writer side
    def _set_content_type(self, raw_header: Optional[str]) -> None:
        """`raw_header` is the full Content-Type header, e.g. 'text/html; charset=UTF-8'."""
        self._content_type, self._charset = _parse_content_type(raw_header)
        self._headers_known = True
        self._headers_ready.set()

    def feed(self, data: bytes) -> None:
        chunk = bytes(data)
        with self._cond:
            # Backpressure: once `max_buffer` bytes are queued ahead of the reader, the worker
            # blocks here instead of reading the network unboundedly, so a slow reader slows the
            # download rather than buffering the whole file in memory. A chunk larger than the
            # bound is still accepted while the buffer is empty, to avoid a stall.
            while (not self.closed and self._buffered > 0
                   and self._buffered + len(chunk) > self._max_buffer):
                self._cond.wait()
            if self.closed:
                return  # reader left; the fetch is being cancelled, drop remaining chunks
            self._chunks.append(chunk)
            self._buffered += len(chunk)
            self._cond.notify_all()

    def finish(self, error: Optional[BaseException] = None) -> None:
        with self._cond:
            self._eof = True
            self._error = error
            self._cond.notify_all()
        self._headers_ready.set()  # don't leave content_type waiters hanging on failure

    # ---- reader side
    def _wait_headers(self) -> None:
        self.start()
        self._headers_ready.wait()
        if not self._headers_known and self._error is not None:
            raise self._error  # the download failed before any header arrived

    @property
    def content_type(self) -> Optional[str]:
        """Media type from the Content-Type header, lower-cased, e.g. "text/html".

        Read-only. Starts the download if needed and blocks until the server's headers
        are known (or the metadata of the cached copy has been read). None if the server
        sent no Content-Type; raises the download error if it failed before any header.
        """
        self._wait_headers()
        return self._content_type

    @property
    def charset(self) -> Optional[str]:
        """Charset from the Content-Type header, lower-cased, e.g. "utf-8", or None.

        (Not the HTTP "Content-Encoding" header, which is about compression.)
        Read-only; blocks like `content_type`.
        """
        self._wait_headers()
        return self._charset

    @property
    def progress(self) -> Progress:
        """Current `Progress` (downloaded, total, complete), safe to poll from any thread.

        Unlike `content_type`/`charset` this never blocks and never starts the download:
        before the first read it reports `(0, None, False)`. `total` stays None until the
        server's Content-Length is known (and forever if it sends none). `complete` turns
        true once the whole file is on disk — including when the reader stopped early, in
        which case polling is the only completion signal. Read-only.
        """
        return self._progress.snapshot()

    def as_text_stream(self, encoding: Optional[str] = None, errors: str = "strict",
                       newline: Optional[str] = None) -> io.TextIOWrapper:
        """This stream decoded as text (line iteration, readline, read(n chars)...).

        `encoding` defaults to the server's `charset`, or "utf-8" if it sent none.
        Blocks until the headers are known, like `charset`. Use this instead of the
        stream itself from then on; closing the text stream closes this one.
        An unknown charset raises LookupError: pass `encoding=` to override it.
        """
        if encoding is None:
            encoding = self.charset or "utf-8"
        return io.TextIOWrapper(io.BufferedReader(self), encoding=encoding,
                                errors=errors, newline=newline)

    def start(self) -> None:
        with self._start_lock:
            if self._started:
                return
            self._started = True
        self._starter(self)

    def readable(self) -> bool:
        return True

    def readinto(self, b) -> int:
        self.start()
        view = memoryview(b).cast("B")
        with self._cond:
            while not self._chunks and not self._eof and not self.closed:
                self._cond.wait()
            if self._chunks:
                head = self._chunks.popleft()
                n = min(len(view), len(head))
                view[:n] = head[:n]
                if n < len(head):
                    self._chunks.appendleft(head[n:])
                self._buffered -= n
                self._cond.notify_all()  # room freed: unblock a writer waiting on backpressure
                return n
            if self._error is not None:
                raise self._error
            return 0  # EOF (or closed)

    def close(self) -> None:
        # Closing the reader abandons the download: the worker stops at the next chunk
        # boundary, leaving a partial file and its sidecar, so a later call resumes it.
        self._cancelled.set()
        with self._cond:
            self._chunks.clear()
            self._buffered = 0
            self._cond.notify_all()  # wakes blocked readers and a writer waiting on backpressure
        super().close()


# --------------------------------------------------------------------------- disk helpers


# ':' is legal in file names everywhere except Windows, and "host:8080" reads much better
_SAFE_IN_NAME = "" if os.name == "nt" else ":"


def _parse_content_type(raw: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """'Text/HTML; charset="UTF-8"' -> ('text/html', 'utf-8');  None/'' -> (None, None)."""
    if not raw:
        return None, None
    msg = Message()
    msg["Content-Type"] = raw
    return msg.get_content_type(), msg.get_content_charset()


def _finish_name(name: str) -> str:
    """Make an already percent-encoded name safe, and keep the mapping injective.

    Since quote() always escapes '%' itself, the names produced below ('%2E...') can
    never come from a different URL, and no URL can produce '.INDEX' or '*.DOWNLOAD'.
    """
    if name.startswith("."):  # also '.' and '..'; reserves '.INDEX' for ourselves
        name = "%2E" + name[1:]
    if name.endswith(META_SUFFIX):  # reserved for metadata files
        name = name[: -len(META_SUFFIX)] + "%2E" + META_SUFFIX[1:]
    if len(name) > 200:  # keep names legal on common filesystems
        name = name[:150] + "-" + hashlib.sha256(name.encode()).hexdigest()[:32]
    return name


def _segment(text: str) -> str:
    """One URL component -> one safe file-system name."""
    return _finish_name(quote(text, safe=_SAFE_IN_NAME))  # no '/', '\\', NUL ... inside


def _local_path(base: Path, url: str) -> Path:
    """https://example.org/directory/thing.zip -> <base>/https/example.org/directory/thing.zip

    http://example.org -> <base>/http/example.org;  with a port: .../example.org+8080
    ('+' is legal in file names everywhere and can't occur in a host name).
    The query string (if any) is appended, percent-encoded, to the last name.
    Credentials in the URL are never used for the path.
    """
    parts = urlsplit(url)
    if not parts.scheme or not parts.hostname:
        raise ValueError(f"not an absolute URL: {url!r}")
    host = _segment(parts.hostname) + (f"+{parts.port}" if parts.port else "")
    names = [host] + [_segment(s) for s in parts.path.split("/") if s]
    if parts.query:
        names[-1] = _finish_name(names[-1] + quote("?" + parts.query, safe=_SAFE_IN_NAME))
    return base.joinpath(_segment(parts.scheme), *names)


def _promote(path: Path) -> None:
    """A cached file must become a directory, because another URL lives below it:
    move it (and its metadata) to <path>/.INDEX."""
    with _lock_for(path):  # waits for a download that is still writing to it
        if not path.is_file():
            return  # somebody else promoted it meanwhile
        meta = path.with_name(path.name + META_SUFFIX)
        if not meta.is_file():
            raise OSError(f"{path} is in the way and is not a cached download; not touching it")
        tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex}.PROMOTING")
        os.rename(path, tmp)
        path.mkdir()
        os.rename(tmp, path / INDEX_NAME)
        os.rename(meta, path / (INDEX_NAME + META_SUFFIX))


def _make_room(url: str, logical: Path) -> None:
    """Create the parent directories, promoting cached files that are in the way."""
    try:
        for parent in logical.parents:  # at most one of them can be a file
            if parent.is_file():
                _promote(parent)
        logical.parent.mkdir(parents=True, exist_ok=True)
    except (FileExistsError, NotADirectoryError) as e:
        raise OSError(f"cannot store {url!r} at {logical}: {e}") from e


def _load_meta(path: Path) -> Optional[dict]:
    try:
        with open(path, encoding="utf-8") as f:
            meta = json.load(f)
        return meta if isinstance(meta, dict) else None
    except (OSError, ValueError):
        return None


def _save_meta(path: Path, meta: dict) -> None:
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(meta, f)
    os.replace(tmp, path)  # atomic: a crash never leaves half-written metadata


def _replay(path: Path, stream: DownloadStream, nbytes: int,
            on_progress: Optional[Callable[[int, Optional[int]], None]] = None,
            cancel: Optional[threading.Event] = None) -> bool:
    """Push the first `nbytes` bytes already on disk into the stream.

    Returns False if `cancel` was set part-way through, so the caller stops."""
    remaining = nbytes
    done = 0
    with open(path, "rb") as f:
        while remaining > 0:
            if cancel is not None and cancel.is_set():
                return False
            chunk = f.read(min(CHUNK_SIZE, remaining))
            if not chunk:
                raise IOError(f"{path} is shorter than its metadata claims")
            stream.feed(chunk)
            remaining -= len(chunk)
            done += len(chunk)
            stream._progress.set_downloaded(done)
            if on_progress is not None:
                on_progress(done, nbytes)
    return True


# --------------------------------------------------------------------------- worker


class _Abandoned(Exception):
    """Raised when the reader vanished (closed or collected) and the stream is gone."""


class _StreamProxy:
    """Forwards to a `DownloadStream` held by weak reference.

    The worker thread keeps one of these instead of the stream itself, so the stream stays
    collectible: a strong reference in the thread would pin it for the whole download. Each
    forwarded call briefly resolves the weak reference; once the stream is gone, the proxy
    raises `_Abandoned`, which stops the fetch and leaves the partial file resumable.
    """

    def __init__(self, ref: "weakref.ref[DownloadStream]"):
        self._ref = ref

    def __getattr__(self, name: str):
        stream = self._ref()
        if stream is None:
            raise _Abandoned()
        return getattr(stream, name)


class _Progress:
    """Progress bar on stderr for one download. Uses tqdm when it is installed,
    otherwise a minimal built-in bar. `initial` counts bytes already on disk (resume)."""

    def __init__(self, url: str, total: Optional[int], initial: int):
        self.desc = url.rstrip("/").rsplit("/", 1)[-1] or url
        self.total, self.n, self._last = total, initial, 0.0
        try:
            from tqdm import tqdm
        except ImportError:
            self._bar = None
            self._draw()
        else:
            self._bar = tqdm(total=total, initial=initial, desc=self.desc, unit="B",
                             unit_scale=True, unit_divisor=1024, file=sys.stderr)

    def _draw(self, final: bool = False) -> None:
        now = time.monotonic()
        if not final and now - self._last < 0.1:  # throttle redraws
            return
        self._last = now
        if self.total:
            frac = min(self.n / self.total, 1.0)
            fill = int(frac * 30)
            line = f"{self.desc[:30]:<30} |{'#' * fill}{'-' * (30 - fill)}| {frac:4.0%} " \
                   f"{self.n / 1048576:.1f}/{self.total / 1048576:.1f} MiB"
        else:
            line = f"{self.desc[:30]:<30} {self.n / 1048576:.1f} MiB"
        sys.stderr.write("\r" + line + ("\n" if final else ""))
        sys.stderr.flush()

    def update(self, nbytes: int) -> None:
        self.n += nbytes
        if self._bar is not None:
            self._bar.update(nbytes)
        else:
            self._draw()

    def close(self) -> None:
        if self._bar is not None:
            self._bar.close()
        else:
            self._draw(final=True)


def _fetch(url: str, logical: Path, stream: DownloadStream, timeout: float,
           progress_bar: bool = False,
           on_progress: Optional[Callable[[int, Optional[int]], None]] = None,
           cancel: Optional[threading.Event] = None) -> None:
    _make_room(url, logical)
    # If other URLs live below this one, its content is the '.INDEX' file inside.
    dest = logical / INDEX_NAME if logical.is_dir() else logical
    meta_path = dest.with_name(dest.name + META_SUFFIX)

    meta = _load_meta(meta_path)
    if meta is not None and meta.get("url") != url:
        meta = None
    have = 0
    if meta is not None and dest.exists():
        have = min(int(meta.get("downloaded", 0)), dest.stat().st_size)
        if have < int(meta.get("downloaded", 0)):
            meta, have = None, 0  # file lost bytes the metadata promised: start over

    # 1. Already complete: serve from disk, no network.
    if meta is not None and meta.get("complete") and have == int(meta.get("downloaded", -1)):
        stream._set_content_type(meta.get("content_type"))
        stream._progress.set(have, int(meta.get("total", have)))
        if not _replay(dest, stream, have, on_progress, cancel):
            return
        stream._progress.set_complete()
        return

    # 2. Try to resume (needs a validator so we know the remote content didn't change).
    validator = meta and (meta.get("etag") or meta.get("last_modified"))
    for attempt in (0, 1):
        headers = {}
        offset = 0
        if meta is not None and have > 0 and validator and attempt == 0:
            offset = have
            headers["Range"] = f"bytes={offset}-"
            headers["If-Range"] = meta.get("etag") or meta.get("last_modified")
        try:
            resp = urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=timeout)
        except urllib.error.HTTPError as e:
            if e.code == 416 and offset:  # our range is past the end
                if meta.get("total") == offset:  # ...because we already have everything
                    meta["complete"] = True
                    _save_meta(meta_path, meta)
                    stream._set_content_type(meta.get("content_type"))
                    stream._progress.set(offset, offset)
                    if not _replay(dest, stream, offset, on_progress, cancel):
                        return
                    stream._progress.set_complete()
                    return
                continue  # otherwise retry from scratch
            raise
        break

    with resp:
        status = getattr(resp, "status", 200)
        length = resp.headers.get("Content-Length")
        resumed = bool(offset) and status == 206
        content_type = resp.headers.get("Content-Type") or (
            (meta or {}).get("content_type") if resumed else None)
        stream._set_content_type(content_type)
        if resumed:
            mode = "ab"
            cr = resp.headers.get("Content-Range", "")  # "bytes 100-199/200"
            total = int(cr.rsplit("/", 1)[-1]) if cr.rsplit("/", 1)[-1].isdigit() else (
                offset + int(length) if length else None)
            with open(dest, "r+b") as f:
                f.truncate(offset)  # drop any bytes written after the last metadata update
            # the reader sees the whole file, not just the tail
            if not _replay(dest, stream, offset, on_progress, cancel):
                return
        else:  # fresh download, or the server ignored/refused the range
            offset = 0
            mode = "wb"
            total = int(length) if length else None

        # both branches have set `offset` and `total` by now; publish them (resume replays
        # from 0, so `downloaded` will climb from `offset` to `total` below)
        stream._progress.set(offset, total)

        meta = {
            "url": url,
            "etag": resp.headers.get("ETag"),
            "last_modified": resp.headers.get("Last-Modified"),
            "content_type": content_type,
            "total": total,
            "downloaded": offset,
            "complete": False,
        }
        _save_meta(meta_path, meta)

        bar = _Progress(url, total, offset) if progress_bar else None
        try:
            with open(dest, mode) as f:
                while True:
                    if cancel is not None and cancel.is_set():
                        _save_meta(meta_path, meta)  # keep `downloaded` in sync with the file
                        return  # partial file + sidecar stay resumable
                    chunk = resp.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    f.write(chunk)
                    f.flush()
                    meta["downloaded"] += len(chunk)
                    _save_meta(meta_path, meta)  # file first, metadata second
                    stream.feed(chunk)
                    if bar:
                        bar.update(len(chunk))
                    stream._progress.set(meta["downloaded"], total)
                    if on_progress is not None:
                        on_progress(meta["downloaded"], total)
        finally:
            if bar:
                bar.close()

    if meta["total"] is not None and meta["downloaded"] != meta["total"]:
        raise IOError(f"incomplete download: {meta['downloaded']}/{meta['total']} bytes (resumable)")
    meta["complete"] = True
    if meta["total"] is None:
        meta["total"] = meta["downloaded"]
    _save_meta(meta_path, meta)
    stream._progress.set(meta["downloaded"], meta["total"])
    stream._progress.set_complete()


# --------------------------------------------------------------------------- public API


class _Download:
    def __init__(self, path: str, timeout: float, progress_bar: bool = False,
                 on_progress: Optional[Callable[[int, Optional[int]], None]] = None,
                 max_buffer: int = DEFAULT_MAX_BUFFER):
        self.base = Path(path)
        self.timeout = timeout
        self.progress_bar = progress_bar
        self.on_progress = on_progress
        self.max_buffer = max_buffer

    def _open(self, url: str) -> DownloadStream:
        dest = _local_path(self.base, url)

        def run(stream: DownloadStream) -> None:
            # Hold only a weak reference so the stream can be collected mid-download: a strong
            # reference in the worker would pin the object for the whole download, and its
            # death (which must cancel the fetch) could never happen.
            ref = weakref.ref(stream)
            cancel = stream._cancelled
            proxy = _StreamProxy(ref)

            def work() -> None:
                try:
                    with _lock_for(dest):
                        _fetch(url, dest, proxy, self.timeout, self.progress_bar,
                               self.on_progress, cancel)
                except _Abandoned:
                    pass  # the reader is gone; the partial file and sidecar stay resumable
                except BaseException as e:  # surfaced to the reader
                    if (alive := ref()) is not None:
                        alive.finish(e)
                else:
                    if (alive := ref()) is not None:
                        alive.finish()

            threading.Thread(target=work, name=f"download:{url}", daemon=True).start()

        return DownloadStream(run, self.max_buffer)

    def __call__(self, target):
        if isinstance(target, str):  # applied directly to a string
            return self._open(target)
        if callable(target):  # decorating a function / method returning a string
            @functools.wraps(target)
            def wrapper(*args, **kwargs):
                url = target(*args, **kwargs)
                if not isinstance(url, str):
                    raise TypeError(f"{target.__name__} must return a str, got {type(url).__name__}")
                return self._open(url)

            return wrapper
        raise TypeError("expected a string or a callable returning a string")


def download(path: str = "download", progress_bar: bool = False, *, timeout: float = 30.0,
             on_progress: Optional[Callable[[int, Optional[int]], None]] = None,
             max_buffer: int = DEFAULT_MAX_BUFFER):
    """download("some/dir") -> something you can apply to a URL string or decorate a
    str-returning callable with. Either way you get back a readable DownloadStream.
    Also works as a bare `@download`.

    progress_bar: show a progress bar on stderr while bytes are actually being downloaded
    (not when the file is served from the cache). Uses tqdm if installed, else a built-in bar.

    on_progress(downloaded, total): called with the running byte count as data arrives (and as a
    resumed or cached file is replayed to the reader). `total` is the expected size in bytes, or
    None when the server sent no Content-Length. Use it for logging, metrics or a custom progress
    display; `progress_bar` is a built-in consumer of the same events. The callback runs on the
    download thread, so it must be quick and must not read from the stream.

    max_buffer: how many bytes the worker may buffer ahead of the reader (default 8 MiB). A reader
    that lags applies backpressure — the worker blocks in feed() rather than reading the network
    unboundedly — so parsing an archive as it arrives never buffers the whole file in memory.

    The returned stream also exposes a polled view: `stream.progress` returns a `Progress`
    (downloaded, total, complete) snapshot from any thread without blocking, and `complete`
    becomes true once the file is on disk even if the reader stopped early.

    Lifetime: the download runs on a daemon thread that holds only a weak reference to the
    stream. Closing the stream, or letting it be garbage-collected, cancels the download at
    the next chunk boundary; the bytes already written (and their sidecar) stay on disk, so a
    later `download()` of the same URL resumes from there.
    """
    if callable(path):  # bare @download
        return _Download("download", timeout, progress_bar, on_progress, max_buffer)(path)
    return _Download(path, timeout, progress_bar, on_progress, max_buffer)
