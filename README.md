# TikTok End-to-End Crawler

<div align="center">

**Config-driven, event-driven scraper pipeline for TikTok data**

[![Python](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Pydantic](https://img.shields.io/badge/pydantic-v2-e92063.svg)](https://docs.pydantic.dev/latest/)
[![Docker](https://img.shields.io/badge/docker-ready-2496ED.svg)](https://www.docker.com/)
[![Tests](https://img.shields.io/badge/tests-47_passed-green.svg)]()

</div>

---

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [CLI Usage](#cli-usage)
- [Configuration](#configuration)
- [Module Reference](#module-reference)
- [Data Schema](#data-schema)
- [Output Drivers](#output-drivers)
- [Testing](#testing)
- [Docker & Deployment](#docker--deployment)
- [Extending](#extending)
- [Troubleshooting](#troubleshooting)

---

## Overview

This project implements a **production-grade, config-driven scraper pipeline** for TikTok data using the [tikwm.com](https://www.tikwm.com) proxy API.

### Operation Modes

| Mode | `--type` | Endpoint | Description |
|------|----------|----------|-------------|
| `scrape` | `search` | `POST /api/feed/search` | Search by keyword -> JSON stdout/file |
| `scrape` | `user-posts` | `POST /api/user/posts` | Fetch posts by username -> JSON stdout/file |
| `full` | `search` | `POST /api/feed/search` | Crawl + publish to output driver (Kafka/ES/file/std) |
| `full` | `user-posts` | `POST /api/user/posts` | Crawl + publish to output driver |

### Design Principles

| Principle | Implementation |
|-----------|---------------|
| **Zero hardcoding** | `Pydantic BaseSettings` — env vars, YAML, or constructor overrides |
| **Open/Closed** | `Controllers(ABC)` + `OutputDriver(ABC)` + `InputDriver(ABC)` — extend without touching the engine |
| **Factory pattern** | `OutputDriverFactory` / `InputDriverFactory` — destination resolved at runtime |
| **Dependency injection** | Cookies, settings, driver config flow through constructor chain — no global mutation |
| **OOP-first** | Abstract base classes, typed interfaces throughout |
| **Fully async** | `asyncio` for HTTP; Kafka driver uses running event loop (no private loops) |
| **Memory-efficient** | Scrape loop uses O(1) counter; only accumulates list when `--output-file` is set |
| **Single serialization** | `model_dump_json()` -> string passed directly to drivers (no double encode) |

---

## Project Structure

```
tiktok-end-to-end-crawler/
├── Dockerfile                         # Docker image (python:3.11-slim)
├── config.yaml                        # YAML configuration
├── README.md
├── requirements.txt                   # -> source/requirements.txt
├── search_post.txt                    # Reference cURL: POST /api/feed/search
├── search_post_account.txt            # Reference cURL: POST /api/user/posts
│
└── source/
    ├── main.py                        # CLI entry point (argparse)
    ├── requirements.txt               # Python dependencies
    ├── .gitignore
    ├── .dockerignore
    │
    ├── controllers/
    │   ├── __init__.py                #   Controllers(ABC) — main loop, input, output, exception handling
    │   └── tiktok/
    │       ├── __init__.py            #   TikTokControllers — API client lifecycle, job helpers
    │       ├── search_post.py         #   TikTokSearchPost — keyword search handler
    │       └── user_posts.py          #   TikTokUserPosts — user-posts handler (NEW)
    │
    ├── exception/
    │   ├── __init__.py
    │   └── exception.py               #   Custom exceptions + MessageException patterns
    │
    ├── helpers/
    │   ├── __init__.py
    │   ├── input/
    │   │   ├── __init__.py            #   Input facade
    │   │   └── driver/
    │   │       ├── __init__.py        #   InputDriver(ABC)
    │   │       ├── std.py             #   StdInputDriver (keyword / JSON file)
    │   │       └── factory/__init__.py
    │   └── output/
    │       ├── __init__.py            #   Output facade
    │       └── driver/
    │           ├── __init__.py        #   OutputDriver(ABC)
    │           ├── kafka.py           #   KafkaOutputDriver (uses running event loop)
    │           ├── elasticsearch.py   #   ElasticsearchOutputDriver (config-driven)
    │           ├── file.py            #   FileOutputDriver (cached handles, actionable errors)
    │           ├── std.py             #   StdOutputDriver
    │           └── factory/__init__.py#   OutputDriverFactory (registry pattern)
    │
    ├── library/
    │   ├── __init__.py
    │   ├── config.py                  #   Pydantic v2 BaseSettings (env + YAML, absolute-path resolution)
    │   ├── schemas.py                 #   Pydantic v2 models (timezone-aware timestamps)
    │   └── tiktok_api.py             #   TikTokAPI — async HTTP client (cookies via constructor)
    │
    ├── deployment/
    │   ├── 01-configmap.yaml
    │   └── 02-deployment.yaml
    │
    └── tests/
        ├── conftest.py                #   Shared fixtures
        ├── test_config.py             #   5 tests
        ├── test_schemas.py            #   14 tests
        ├── test_tiktok_api.py         #   8 tests
        ├── test_output_drivers.py     #   6 tests
        ├── test_controllers.py        #   6 tests
        └── test_user_posts.py         #   8 tests (NEW)
```

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     source/main.py                            │
│                   CLI Entry Point                             │
│                  argparse -> dispatch                         │
└────────────┬──────────────────────────────┬──────────────────┘
             │                              │
   ┌─────────▼──────────┐         ┌─────────▼─────────────────┐
   │  MODE: scrape       │         │  MODE: full                │
   │  (no output driver) │         │  (with output driver)       │
   │                     │         │                             │
   │  TikTokSearchPost   │         │  TikTokSearchPost           │
   │  TikTokUserPosts    │         │  TikTokUserPosts            │
   │    .scrape_to_json()│         │    .handler()               │
   │         │           │         │       │                     │
   │         ▼           │         │       ▼                     │
   │   TikTokAPI         │         │   TikTokAPI                 │
   │   (httpx.AsyncClient)│        │   (httpx.AsyncClient)       │
   │         │           │         │       │                     │
   │         ▼           │         │       ▼                     │
   │   list[dict]        │         │   model_dump_json() -> str  │
   │   -> stdout / file  │         │       │                     │
   └─────────────────────┘         │       ▼                     │
                                   │   Output Driver             │
                                   │   (kafka | es | file | std) │
                                   └─────────────────────────────┘
```

### Data Flow

```
TikTokAPI.search_posts("keyword")  /  TikTokAPI.get_user_posts("@user")
        │
        ▼
TikTokSearchRequest / TikTokUserPostsRequest (form-encoded)
        │
        ▼
httpx.AsyncClient -> POST https://www.tikwm.com/api/feed/search   (search)
                    POST https://www.tikwm.com/api/user/posts     (user-posts)
        │
        ▼
TikTokSearchResponse
  └─ data
       └─ videos[]  -> TikTokPost (per post)
            │
            ├── [scrape mode] -> model_dump(mode="json") -> dict -> stdout / file.json
            │
            └── [full mode]   -> model_dump_json() -> str (single serialization)
                                      │
                                      ├── KafkaOutputDriver -> topic (running event loop)
                                      ├── ElasticsearchOutputDriver -> index (config-driven)
                                      ├── FileOutputDriver -> file (cached handles)
                                      └── StdOutputDriver -> print
```

### Cookies Flow

```
CLI --cookies "cf_clearance=..."
  │
  ▼
kwargs dict (main.py)
  │
  ▼
self.args["cookies"] (Controllers.__init__)
  │
  ▼
_ensure_api() -> TikTokAPI(settings, cookies=...)  (constructor injection)
  │
  ▼
TikTokAPI.start() -> cookie_source (override or config fallback)
```

No global mutation. Multiple controller instances with different cookies coexist safely.

---

## Quick Start

### Prerequisites

| Dependency | Required For |
|-----------|-------------|
| Python 3.11+ | Runtime |
| Apache Kafka 2.8+ | `full` mode with `-d kafka` |
| Elasticsearch 8.x | `full` mode with `-d elasticsearch` |
| Docker | Containerised deployment |

### 1. Install

```bash
git clone <repo-url>
cd tiktok-end-to-end-crawler

python -m venv .venv
source .venv/bin/activate       # Linux/macOS
# .venv\Scripts\activate        # Windows

pip install -r source/requirements.txt
```

### 2. Configure

Edit `config.yaml` at the repository root, or set env vars:

```bash
export TIKTOK_KAFKA__BOOTSTRAP_SERVERS="broker1:9092,broker2:9092"
export TIKTOK_CRAWLER__RATE_LIMIT_RPS="10"
```

### 3. Run

```bash
cd source/

# === Keyword search ===
python main.py crawler --mode scrape --keyword "persib"
python main.py crawler --mode scrape --keyword "viral" --count 20 --max-pages 3
python main.py crawler --mode scrape --keyword "persib" -o results.json --pretty

# === User-posts (requires Cloudflare cookies) ===
python main.py crawler --mode scrape --type user-posts --unique-id "@persib" \
    --cookies "cf_clearance=XXX; current_language=en" --count 10

# === Full pipeline ===
python main.py crawler --mode full --keyword "persib" -d kafka -o tiktok.posts.raw
python main.py crawler --mode full --keyword "persib" -d elasticsearch -o tiktok_posts
python main.py crawler --mode full --keyword "persib" -d file -o ./output.json
python main.py crawler --mode full --keyword "persib" -d std
```

---

## CLI Usage

### Full argument reference

```
usage: main.py [-h] [-c CONFIG] [-s SOURCE] [-d DESTINATION] [-i INPUT]
               [-o OUTPUT] [--bootstrap-servers ...] [--elasticsearch-hosts ...]
               {crawler} ...

crawler options:
  --mode {scrape,full}     scrape: JSON only | full: crawl + output driver
  --type {search,user-posts}  search: keyword | user-posts: by username (default: search)
  --keyword KEYWORD        Search keyword (for --type search)
  --unique-id UNIQUE_ID    TikTok username (for --type user-posts, e.g. @persib)
  --count COUNT            Results per page (default: 12, max: 50)
  --max-pages MAX_PAGES    Pages to crawl (default: 1)
  --hd {0,1}               HD quality (default: 1)
  --cookies COOKIES        Cookie string for authenticated endpoints
  -o, --output PATH        Save JSON to file (scrape) or destination name (full)
  --pretty                 Pretty-print JSON output
  --log-level LEVEL        DEBUG | INFO | WARNING | ERROR (default: INFO)
```

### Examples

```bash
# Minimal
python main.py crawler --mode scrape --keyword "persib"

# Paginated + saved
python main.py crawler --mode scrape --keyword "viral" --max-pages 5 --count 30 -o out.json

# Pipe to jq
python main.py crawler --mode scrape --keyword "persib" | jq '.[] | {id: .video_id, likes: .digg_count}'

# User posts
python main.py crawler --mode scrape --type user-posts --unique-id "@persib" \
    --cookies "cf_clearance=XXX" -o user_posts.json

# Full pipeline: Kafka
python main.py crawler --mode full --keyword "persib" -d kafka -o my_topic \
    --bootstrap-servers kafka1:9092,kafka2:9092

# Full pipeline: Elasticsearch
python main.py crawler --mode full --keyword "persib" -d elasticsearch -o tiktok_posts \
    --elasticsearch-hosts http://es:9200

# Full pipeline: user-posts -> Kafka
python main.py crawler --mode full --type user-posts --unique-id "@persib" \
    --cookies "cf_clearance=XXX" -d kafka -o tiktok.user.posts
```

---

## Configuration

### Priority (highest first)

```
1. CLI arguments                  (--keyword, --cookies, --bootstrap-servers, etc.)
2. Environment variables          (TIKTOK_ prefix)
3. config.yaml                    (project root — resolved by file location + CWD)
4. .env file
5. Code defaults                  (library/config.py)
```

### Environment Variables

All prefixed with `TIKTOK_`. Nested settings use `__` (double underscore).

#### Kafka

| Variable | Default |
|----------|---------|
| `TIKTOK_KAFKA__BOOTSTRAP_SERVERS` | `localhost:9092` |
| `TIKTOK_KAFKA__TOPIC` | `tiktok.posts.raw` |
| `TIKTOK_KAFKA__CLIENT_ID` | `tiktok-crawler` |
| `TIKTOK_KAFKA__ACKS` | `all` |
| `TIKTOK_KAFKA__COMPRESSION_TYPE` | `gzip` |

#### Elasticsearch

| Variable | Default |
|----------|---------|
| `TIKTOK_ELASTICSEARCH__HOSTS` | `["http://localhost:9200"]` |
| `TIKTOK_ELASTICSEARCH__INDEX_NAME` | `tiktok_posts` |
| `TIKTOK_ELASTICSEARCH__REQUEST_TIMEOUT` | `30` |
| `TIKTOK_ELASTICSEARCH__MAX_RETRIES` | `3` |

#### Crawler

| Variable | Default |
|----------|---------|
| `TIKTOK_CRAWLER__BASE_URL` | `https://www.tikwm.com` |
| `TIKTOK_CRAWLER__RATE_LIMIT_RPS` | `5.0` |
| `TIKTOK_CRAWLER__REQUEST_TIMEOUT` | `30.0` |
| `TIKTOK_CRAWLER__MAX_RETRIES` | `3` (minimum 1, guarded) |
| `TIKTOK_CRAWLER__COOKIES` | — |
| `TIKTOK_CRAWLER__PROXY_URL` | — |
| `TIKTOK_CRAWLER__USER_POSTS_ENDPOINT` | `/api/user/posts` |

---

## Module Reference

### `source/main.py` — CLI Entry Point

```python
from controllers.tiktok.search_post import TikTokSearchPost
from controllers.tiktok.user_posts import TikTokUserPosts

# Scrape search
ctl = TikTokSearchPost(keyword="persib", count=20, max_pages=3)
posts = await ctl.scrape_to_json({"keyword": "persib"})

# Scrape user posts
ctl = TikTokUserPosts(unique_id="@persib", count=10, cookies="cf_clearance=...")
posts = await ctl.scrape_to_json({"unique_id": "persib"})

# Full pipeline
ctl = TikTokSearchPost(keyword="persib", destination="kafka", output="my_topic",
                       bootstrap_servers="kafka:9092")
await ctl.handler({"keyword": "persib"})
```

### `source/library/tiktok_api.py` — TikTokAPI

```python
from library.tiktok_api import TikTokAPI
from library.config import settings

# Cookies via constructor (preferred) — no global mutation
api = TikTokAPI(settings.crawler, cookies="cf_clearance=XXX; current_language=en")

async with api:
    # Keyword search
    async for event in api.search_posts("persib", max_pages=3, count=20):
        print(event.payload.title)

    # User posts
    async for event in api.get_user_posts("@persib", max_pages=3):
        print(event.payload.stats.play_count)

    # Single post (best-effort via search)
    event = await api.fetch_post("7123456789012345678")
```

### `source/library/config.py` — Settings

```python
from library.config import settings, Settings

print(settings.kafka.bootstrap_servers)
print(settings.crawler.rate_limit_rps)

# Custom per-instance
custom = Settings(kafka={"bootstrap_servers": "prod:9092"})
```

### `source/library/schemas.py` — Data Models

```python
from library.schemas import (
    TikTokSearchRequest, TikTokUserPostsRequest, TikTokPost, KafkaEvent
)

# Search request
req = TikTokSearchRequest(keywords="persib", count=20)
print(req.to_form_data())  # -> {"keywords": "persib", "count": "20", ...}

# User-posts request
req = TikTokUserPostsRequest(unique_id="@persib")  # @ auto-normalized
print(req.to_form_data())  # -> {"unique_id": "@persib", ...}

# Parse API response
post = TikTokPost.from_api_response({"video_id": "123", "title": "Hello"})

# Create event (timezone-aware timestamp)
event = KafkaEvent(payload=post, metadata={"query": "persib"})
print(event.model_dump_json())
```

### `source/controllers/` — Business Logic

```python
from controllers.tiktok.search_post import TikTokSearchPost
from controllers.tiktok.user_posts import TikTokUserPosts

# Scrape search
ctl = TikTokSearchPost(keyword="persib")
posts = await ctl.scrape_to_json({"keyword": "persib"})

# Scrape user posts
ctl = TikTokUserPosts(unique_id="@persib", cookies="cf_clearance=...")
posts = await ctl.scrape_to_json({"unique_id": "persib"})

# Full pipeline
ctl = TikTokSearchPost(keyword="persib", destination="kafka", output="topic",
                       bootstrap_servers="kafka:9092")
await ctl.handler({"keyword": "persib"})
```

### `source/helpers/output/` — Output Drivers

```python
from helpers.output.driver.factory import OutputDriverFactory

driver = OutputDriverFactory.create_output_driver(
    destination="kafka", output="my_topic",
    bootstrap_servers="kafka:9092",
)
driver.put('{"video_id": "123"}')   # synchronous wrapper around running event loop
driver.close()

# Available: kafka | elasticsearch | file | std
```

---

## Data Schema

### TikTokSearchRequest

| Field | Type | Required | Default | Constraints |
|-------|------|----------|---------|-------------|
| `keywords` | `str` | yes | — | 1–500 chars |
| `count` | `int` | no | `12` | 1–50 |
| `cursor` | `int` | no | `0` | >= 0 |
| `web` | `int` | no | `1` | 0/1 |
| `hd` | `int` | no | `1` | 0/1 |

### TikTokUserPostsRequest

| Field | Type | Required | Default | Constraints |
|-------|------|----------|---------|-------------|
| `unique_id` | `str` | yes | — | 1–255 chars, `@` auto-normalized |
| `count` | `int` | no | `12` | 1–50 |
| `cursor` | `int` | no | `0` | >= 0 |
| `web` | `int` | no | `1` | 0/1 |
| `hd` | `int` | no | `1` | 0/1 |

### TikTokPost

```
TikTokPost
 ├── id: str                     (alias="video_id")
 ├── title: str
 ├── description: str?           (alias="desc")
 ├── create_time: int?           (Unix timestamp)
 ├── hashtags: list[str]
 ├── is_ad: bool
 ├── author: TikTokAuthor
 │    ├── id: str                (alias="author_id")
 │    ├── unique_id: str
 │    └── nickname: str
 ├── music: TikTokMusic           (coerces string -> object)
 │    ├── id: str                (alias="music_id")
 │    └── title: str
 ├── video: TikTokVideo
 │    ├── id: str                (alias="video_id")
 │    ├── duration: int
 │    └── play_url: str?         (alias="play")
 └── stats: TikTokStats
      ├── digg_count: int
      ├── share_count: int
      ├── comment_count: int
      └── play_count: int
```

### API Response Envelope

```json
{
  "code": 0,
  "msg": "success",
  "data": {
    "videos": [ /* TikTokPost... */ ],
    "cursor": 12,
    "hasMore": true
  }
}
```

### KafkaEvent

| Field | Type | Description |
|-------|------|-------------|
| `event_id` | `str` | UUID4 hex (auto-generated) |
| `event_type` | `str` | `"tiktok.post.scraped"` |
| `source` | `str` | `"tiktok-crawler"` |
| `timestamp` | `datetime` | **UTC-aware** (`timezone.utc`) |
| `payload` | `TikTokPost` | The scraped post |
| `metadata` | `dict` | e.g. `{"query": "persib"}` |

---

## Output Drivers

| Driver | `-d` value | Requires | Notes |
|--------|-----------|----------|-------|
| `StdOutputDriver` | `std` | Nothing | Prints to stdout |
| `FileOutputDriver` | `file` | `-o <path>` | Caches file handles; actionable errors |
| `KafkaOutputDriver` | `kafka` | `-o <topic>` `--bootstrap-servers` | Uses **running** event loop; no private loop |
| `ElasticsearchOutputDriver` | `elasticsearch` | `-o <index>` `--elasticsearch-hosts` | Config-driven timeouts/retries |

### Adding a new driver

```python
# source/helpers/output/driver/redis.py
from helpers.output.driver import OutputDriver

class RedisOutputDriver(OutputDriver):
    name = "redis"

    def __init__(self, *args, host="localhost", port=6379, key="tiktok", **kwargs):
        super().__init__(*args, **kwargs)
        import redis
        self._client = redis.Redis(host=host, port=port)
        self._key = key

    def put(self, output: str, **kwargs):
        self._client.rpush(self._key, output)

    def close(self):
        self._client.close()
```

Register in `factory/__init__.py`:

```python
_DRIVERS = {
    "kafka": KafkaOutputDriver,
    "elasticsearch": ElasticsearchOutputDriver,
    "file": FileOutputDriver,
    "std": StdOutputDriver,
    "redis": RedisOutputDriver,   # <- add here
}
```

---

## Testing

```bash
cd source/

# All tests
python -m pytest tests/ -v

# Specific module
python -m pytest tests/test_tiktok_api.py -v
python -m pytest tests/test_output_drivers.py -v
python -m pytest tests/test_user_posts.py -v

# With coverage
pip install pytest-cov
python -m pytest tests/ --cov=. --cov-report=term-missing
```

```
47 tests across 6 modules (all external services mocked)
────────────────────────────────────────────────────────
├── test_config.py            5 tests   — settings defaults, env override
├── test_schemas.py          14 tests   — validation, coercion, aliases, timezone
├── test_tiktok_api.py        8 tests   — HTTP mock, pagination, cursor, headers
├── test_output_drivers.py    6 tests   — factory, std, file drivers
├── test_controllers.py       6 tests   — scrape_to_json, handler + output
└── test_user_posts.py        8 tests   — schema, controller, unique_id normalization
```

---

## Docker & Deployment

### Build

```bash
docker build -t tiktok-crawler .
```

### Run

```bash
# Scrape mode
docker run --rm tiktok-crawler crawler --mode scrape --keyword "persib" --count 5

# Full mode with Kafka
docker run --rm \
  -e TIKTOK_KAFKA__BOOTSTRAP_SERVERS=kafka:9092 \
  tiktok-crawler \
  crawler --mode full --keyword "persib" -d kafka -o tiktok.posts.raw

# User posts with cookies
docker run --rm \
  -e TIKTOK_CRAWLER__COOKIES="cf_clearance=XXX; current_language=en" \
  tiktok-crawler \
  crawler --mode scrape --type user-posts --unique-id "@persib"
```

### Kubernetes

```bash
kubectl apply -f source/deployment/
kubectl logs -l kind=tiktok-crawler --tail=50
```

---

## Extending

### Adding a new crawler platform

```python
# source/controllers/instagram/__init__.py
from controllers import Controllers

class InstagramControllers(Controllers):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
```

```python
# source/controllers/instagram/search_post.py
from controllers.instagram import InstagramControllers

class InstagramSearchPost(InstagramControllers):
    async def handler(self, job: dict):
        data = await self.fetch_instagram(job["keyword"])
        self.send_output(data)
```

### Programmatic usage

```python
import asyncio
from library.tiktok_api import TikTokAPI
from library.config import settings
from helpers.output.driver.kafka import KafkaOutputDriver

async def custom_pipeline():
    driver = KafkaOutputDriver(
        topic="tiktok.raw",
        bootstrap_servers=settings.kafka.bootstrap_servers,
    )
    async with TikTokAPI(settings.crawler, cookies="cf_clearance=...") as api:
        async for event in api.search_posts("viral", max_pages=10):
            driver.put(event.payload.model_dump_json())
    driver.close()

asyncio.run(custom_pipeline())
```

---

## Troubleshooting

| Symptom | Cause | Solution |
|---------|-------|----------|
| `OutputDriverNotRecognizeException` | Unknown `-d` value | Use: `kafka`, `elasticsearch`, `file`, or `std` |
| `RuntimeError: no running event loop` (Kafka) | Driver used outside `asyncio.run()` | Wrap in `async def` + `asyncio.run()` |
| `RuntimeError: FileOutputDriver has no output path` | Missing `-o` in full+file mode | Add `-o <path>` |
| `ValueError: unique_id is required` | Empty `--unique-id` | Provide `--unique-id @<username>` |
| 403 on `/api/user/posts` | Missing/expired Cloudflare cookies | Get fresh `cf_clearance` from browser, pass via `--cookies` |
| `ConnectionRefusedError` (Kafka) | Broker unreachable | Check `--bootstrap-servers` |
| `ConnectionError` (ES) | ES node down | Verify `--elasticsearch-hosts` |
| `ValidationError` | API response format changed | Update field aliases in `library/schemas.py` |
| `ImportError: PyYAML` | Missing `pydantic-settings[yaml]` | `pip install pydantic-settings[yaml]` |
| `UnicodeEncodeError` (Windows) | CP1252 terminal | Use `-o <file>` instead of stdout |
| Config silently ignored | CWD mismatch | Run from `source/` directory, or set env vars |

### Health check

```bash
cd source && python main.py crawler --mode scrape --keyword "test" --count 1 --log-level DEBUG
```

---

<div align="center">

Built with Python, Pydantic v2, AIOKafka, Elasticsearch & httpx

</div>
