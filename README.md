# ETL-decorators

A collection of small, focused decorators used in ETL / data applications.

## Installation

Base package (no optional dependencies):

```bash
pip install etl-decorators
```

### Optional dependencies

Some subpackages have optional (extra) dependencies. Each such subpackage
documents its extra in its section below.

Most functionality is organized in subpackages with **optional dependencies**.
Where feasible, optional dependencies are imported lazily (only when a decorator
is executed) so that importing `etl_decorators` doesn’t pull heavy dependencies.

Each subpackage’s README documents its own dependency behavior.

## Decorators

## All-in-one example ("kitchen sink")

Below is a **single** (illustrative) snippet that uses *every* decorator (or
decorator-like helper) exposed by this project at least once.

It’s intentionally compact and mixes unrelated concerns (parsing, streaming,
caching, ORM helpers, LLM calls, …) to serve as a quick tour.

> Note: some parts require optional dependencies / services:
> - `etl-decorators[csv|html|xml|redis|sqlalchemy|templating|llms]`
> - a running Redis for `RedisCache`
> - LLM credentials for real `LLM` calls

```python
from __future__ import annotations

import io
import sqlalchemy as sa
from pydantic import BaseModel
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from etl_decorators.csv import parse_csv
from etl_decorators.download import download
from etl_decorators.html import parse_html
from etl_decorators.json import parse_json, parse_jsonl
from etl_decorators.logging import log
from etl_decorators.llms import LLM
from etl_decorators.redis import RedisCache
from etl_decorators.resilience import retry, timeout
from etl_decorators.sqlalchemy import JSON, as_model, field, materialized_property
from etl_decorators.streams import batch, dedupe, keep, transform
from etl_decorators.templating import template
from etl_decorators.xml import parse_xml


# -----------------
# Parsing decorators
# -----------------


@parse_csv
def extract_csv() -> str:
    return "id;name\n1;Ada\n1;Ada\n2;Bob\n"


@parse_json
def extract_json() -> str:
    return '{"items": [{"id": 1}, {"id": 2}]}'


@parse_jsonl
def extract_jsonl() -> io.StringIO:
    return io.StringIO('{"id": 1}\n{"id": 2}\n')


@parse_html(extract="h1")
def extract_html_title() -> str:
    return "<html><body><h1>Hello</h1></body></html>"


@parse_xml(extract="//item", extract_as_collection=True)
def extract_xml_items() -> str:
    return "<root><item>a</item><item>b</item></root>"


# ------------------
# Download decorator
# ------------------


@download("my/favorite/path")
def url_of(name: str) -> str:
    return f"https://example.org/files/{name}"


def fetch_remote() -> None:
    stream = url_of("data.bin")  # DownloadStream: readable binary io object
    _ = stream.read()
    _type = stream.content_type  # e.g. "application/octet-stream" (or None)


# ------------------
# Stream decorators
# ------------------


@transform(lambda row: {**row, "name": row["name"].upper()})
@dedupe(key=lambda row: row["id"])
@keep(lambda row: row["id"] > 0)
@batch(size=2)
def stream_rows():
    # parse_csv yields dict rows; these stream decorators stay lazy
    yield from extract_csv()


# ------------------
# Resilience + logging
# ------------------


@log(level="INFO", with_arguments=True, with_result=False)
@retry(retry_on=RuntimeError, max_attempts=3, interval=0.2)
@timeout(seconds=5)
def load_batch(rows: list[dict]) -> int:
    # pretend to load rows somewhere
    return len(rows)


# ------------------
# Redis caching
# ------------------


cache = RedisCache(url="redis://localhost:6379/0", prefix="etl_decorators.demo")


@cache(read_ttl=60, write_ttl=60, serialization="json")
def cached_reference_data() -> dict:
    # expensive lookup
    return {"countries": ["FR", "US"]}


# ------------------
# SQLAlchemy helpers
# ------------------


class Base(DeclarativeBase):
    pass


@as_model(Base)
class Document:
    # `field(...)` lets you pass mapped_column kwargs / defaults
    external_id: str = field(unique=True, index=True)
    title: str
    meta: JSON = field(default_factory=lambda: {"source": "demo"})


class Summary(BaseModel):
    summary: str


llm = LLM(model="openai/gpt-4o-mini")


class Article(Base):
    __tablename__ = "articles"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(sa.String)

    # decorator stacking order (bottom-up):
    # template -> llm -> materialized_property
    @materialized_property
    @llm(return_type=Summary)
    @template
    def summary(self) -> str:
        return "Return JSON with a short summary for: {{ self.title }}"


def main() -> None:
    # Parsing outputs
    _title_tag = extract_html_title()  # bs4.Tag | None
    _items = extract_xml_items()  # list[str]
    _payload = extract_json()
    _events = list(extract_jsonl())

    # Download with disk cache (resumable)
    fetch_remote()

    # Stream processing
    for rows in stream_rows():
        load_batch(rows)

    # Redis memoization
    _ref = cached_reference_data()

    # SQLAlchemy materialization demo (SQLite)
    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        a = Article(title="Hello world")
        session.add(a)
        session.flush()

        # first access triggers compute + flush
        _ = a.summary


if __name__ == "__main__":
    main()
```

### Parsing

#### `etl_decorators.csv`

- Install: `pip install etl-decorators[csv]`
- User documentation: [`etl_decorators.csv.parse_csv`](doc/parse_csv.md)
- Developer notes / code layout: [`src/etl_decorators/csv/README.md`](src/etl_decorators/csv/README.md)
- Tests: [`tests/csv/`](tests/csv/)

#### `etl_decorators.html`

- Install: `pip install etl-decorators[html]`
- User documentation: [`etl_decorators.html.parse_html`](doc/parse_html.md)
- Developer notes / code layout: [`src/etl_decorators/html/README.md`](src/etl_decorators/html/README.md)
- Tests: [`tests/html/`](tests/html/)

#### `etl_decorators.json`

- User documentation:
  - [`etl_decorators.json.parse_json`](doc/parse_json.md)
  - [`etl_decorators.json.parse_jsonl`](doc/parse_jsonl.md)
- Developer notes / code layout: [`src/etl_decorators/json/README.md`](src/etl_decorators/json/README.md)
- Tests: [`tests/json/`](tests/json/)

#### `etl_decorators.xml`

- Install: `pip install etl-decorators[xml]`
- User documentation: [`etl_decorators.xml.parse_xml`](doc/parse_xml.md)
- Developer notes / code layout: [`src/etl_decorators/xml/README.md`](src/etl_decorators/xml/README.md)
- Tests: [`tests/xml/`](tests/xml/)

### Downloading

#### `etl_decorators.download`

- Install: none required (pure standard library; `tqdm` is used for the optional
  progress bar if installed)
- User documentation: none yet — see the module docstring in
  [`src/etl_decorators/download.py`](src/etl_decorators/download.py)
- Tests: [`tests/test_download.py`](tests/test_download.py)

`download` streams a URL to the caller while caching it on disk, resumably:

```python
from etl_decorators.download import download

@download("my/favorite/path")
def url_of(name):               # any function / method returning a str
    return f"http://example.org/{name}"

stream = url_of("file.bin")     # -> DownloadStream (a readable binary io object)
data = stream.read()

stream = download("my/favorite/path")("http://example.org")   # direct use on a string
stream.content_type             # e.g. "text/html" (or None)
stream.charset                  # e.g. "utf-8"     (or None)
```

Key points:

- The returned [`DownloadStream`](src/etl_decorators/download.py:69) is a readable
  binary io object; the download starts lazily on the first read and reading blocks
  until data is available. `as_text_stream()` decodes it (using the server's charset
  by default) for line iteration / text reads.
- Each URL is cached on disk under the given directory
  (e.g. `https://example.org/directory/thing.zip` →
  `my/favorite/path/https/example.org/directory/thing.zip`), with JSON metadata in a
  sibling `.DOWNLOAD` file (url, etag, last_modified, content_type, total,
  downloaded, complete).
- Interrupted downloads resume automatically (via `Range` / `If-Range`, when the
  server provides an ETag or Last-Modified validator); completed downloads are
  served from the cache with no network access.
- `progress_bar=True` shows a progress bar on stderr (tqdm if installed, else a
  built-in bar); `timeout=` sets the network timeout (default 30 s). Also works as a
  bare `@download` (default cache directory `"download"`).

### Caching

#### `etl_decorators.redis`

- Install: `pip install etl-decorators[redis]`
- User documentation: [`etl_decorators.redis.RedisCache`](doc/cache_redis.md)
- Developer notes / code layout: [`src/etl_decorators/redis/README.md`](src/etl_decorators/redis/README.md)
- Tests: [`tests/redis/`](tests/redis/)

#### `etl_decorators.sqlalchemy`

- Install: `pip install etl-decorators[sqlalchemy]`
- User documentation:
  - [`etl_decorators.sqlalchemy.materialized_property`](doc/materialized_property.md)
  - [`etl_decorators.sqlalchemy.as_model`](doc/as_model.md)
- Developer notes / code layout: [`src/etl_decorators/sqlalchemy/README.md`](src/etl_decorators/sqlalchemy/README.md)
- Tests: [`tests/sqlalchemy/`](tests/sqlalchemy/)

### LLM

#### `etl_decorators.llms`

- Install: `pip install etl-decorators[llms]`
- User documentation: [`etl_decorators.llms.LLM`](doc/llm.md)
- Developer notes / code layout: [`src/etl_decorators/llms/README.md`](src/etl_decorators/llms/README.md)
- Tests: [`tests/llms/`](tests/llms/); LLM integration tests are environment-gated: they require model + API key [environment variables](./.env.example), and are expected to be skipped in typical local/CI setups without credentials

### Resilience

#### `etl_decorators.resilience`

- User documentation:
  - [`etl_decorators.resilience.retry`](doc/retry.md)
  - [`etl_decorators.resilience.timeout`](doc/timeout.md)
- Developer notes / code layout: [`src/etl_decorators/resilience/README.md`](src/etl_decorators/resilience/README.md)
- Tests: [`tests/resilience/`](tests/resilience/)

### Streams

#### `etl_decorators.streams`

- User documentation:
  - [`etl_decorators.streams.batch`](doc/batch.md)
  - [`etl_decorators.streams.transform`](doc/transform.md)
  - [`etl_decorators.streams.keep`](doc/keep.md)
  - [`etl_decorators.streams.dedupe`](doc/dedupe.md)
- Developer notes / code layout: [`src/etl_decorators/streams/README.md`](src/etl_decorators/streams/README.md)
- Tests: [`tests/streams/`](tests/streams/)

### Logging

#### `etl_decorators.logging`

- User documentation: [`etl_decorators.logging.log`](doc/log.md)
- Developer notes / code layout: [`src/etl_decorators/logging/README.md`](src/etl_decorators/logging/README.md)
- Tests: [`tests/logging/`](tests/logging/)

### Templating

#### `etl_decorators.templating`

- Install: `pip install etl-decorators[templating]`
- User documentation: [`etl_decorators.templating.template`](doc/template.md)
- Developer notes / code layout: [`src/etl_decorators/templating/README.md`](src/etl_decorators/templating/README.md)
- Tests: [`tests/templating/`](tests/templating/)

## Internal utilities

### `etl_decorators._base`

- Shared decorator plumbing: [`src/etl_decorators/_base/README.md`](src/etl_decorators/_base/README.md)
