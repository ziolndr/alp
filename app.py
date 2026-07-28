#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import gzip
import hashlib
import html
import json
import math
import mimetypes
import os
import re
import sqlite3
import ssl
import sys
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, deque
from datetime import datetime, timezone
from html.parser import HTMLParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterable

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
STATIC_DIR = BASE_DIR / "static"
DB_PATH = Path(os.getenv("PURPOSE_FIELD_DB", str(DATA_DIR / "purpose_field.sqlite3")))
SOURCE_MANIFEST = Path(os.getenv("PURPOSE_FIELD_SOURCES", str(DATA_DIR / "sources.json")))
CUSTOM_SOURCE_MANIFEST = DATA_DIR / "custom_sources.json"

PRODUCT_VERSION = "1.0.1"
PRODUCT_ARCHITECTURE = "ecosystem-field"

ARBITER_EMBED_URL = os.getenv(
    "ARBITER_EMBED_URL",
    "https://api.arbiter.traut.ai/public/embed",
).strip()
ARBITER_API_KEY = os.getenv("ARBITER_API_KEY", "").strip()
PURPOSE_FIELD_API_KEY = os.getenv("PURPOSE_FIELD_API_KEY", "").strip()
HOST = os.getenv("PURPOSE_FIELD_HOST", "127.0.0.1").strip()
PORT = int(os.getenv("PURPOSE_FIELD_PORT", "8844"))
VECTOR_DIM = int(os.getenv("PURPOSE_FIELD_VECTOR_DIM", "72"))
HTTP_TIMEOUT = int(os.getenv("PURPOSE_FIELD_HTTP_TIMEOUT", "35"))
EMBED_TIMEOUT = int(os.getenv("PURPOSE_FIELD_EMBED_TIMEOUT", "60"))
EMBED_BATCH_SIZE = max(1, int(os.getenv("PURPOSE_FIELD_EMBED_BATCH_SIZE", "16")))
EMBED_WORKERS = max(1, int(os.getenv("PURPOSE_FIELD_EMBED_WORKERS", "4")))
CRAWL_DELAY = max(0.0, float(os.getenv("PURPOSE_FIELD_CRAWL_DELAY", "0.10")))
MAX_PAGE_BYTES = int(os.getenv("PURPOSE_FIELD_MAX_PAGE_BYTES", str(4 * 1024 * 1024)))
INSECURE_SSL = os.getenv("PURPOSE_FIELD_INSECURE_SSL", "0").lower() in {"1", "true", "yes"}

DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

_DB_LOCK = threading.RLock()
_BUILD_LOCK = threading.Lock()
_BUILD_THREAD: threading.Thread | None = None

MODE_PREFIXES = {
    "person": (
        "Match the whole support field to this person's actual life, preferences, strengths, "
        "communication, barriers, culture, location, schedule, dignity, agency, and goals. "
    ),
    "goal": (
        "Find concrete services, programs, benefits, opportunities, and community resources "
        "that can advance this goal, including prerequisites, access routes, and practical constraints. "
    ),
    "plan": (
        "From a person-centered planning and IPP or ISP perspective, identify supports that can become "
        "specific actions, referrals, measurements, and next steps without replacing human judgment. "
    ),
    "agency": (
        "From A Living Purpose's operational perspective, identify relevant programs, partners, regulations, "
        "funding pathways, provider resources, workforce tools, and community opportunities. "
    ),
}

PERSPECTIVE_PREFIXES = {
    "person": (
        "Prioritize what the person is likely to accept, use, value, and sustain. "
        "Protect choice, dignity, control, belonging, and self-advocacy. "
    ),
    "family": (
        "Prioritize safety, stability, continuity, practical access, communication, and the person's own choices. "
    ),
    "case_manager": (
        "Prioritize actionable referrals, eligibility, coordination, documented goals, generic resources, "
        "transportation, funding, and measurable next steps. "
    ),
    "director": (
        "Prioritize agency capability, partnerships, compliance, service development, staff execution, "
        "program coverage, and durable operational value. "
    ),
}


class ApiError(Exception):
    def __init__(self, status: int, detail: Any):
        super().__init__(str(detail))
        self.status = status
        self.detail = detail


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def compact_space(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def slug(value: str) -> str:
    value = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return value or "item"


def db() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH, timeout=60)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA synchronous=NORMAL")
    return connection


def init_db() -> None:
    with _DB_LOCK, db() as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS sources (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                organization TEXT NOT NULL,
                category TEXT NOT NULL,
                url TEXT NOT NULL,
                description TEXT NOT NULL,
                tags_json TEXT NOT NULL,
                max_pages INTEGER NOT NULL,
                include_json TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'registered',
                page_count INTEGER NOT NULL DEFAULT 0,
                record_count INTEGER NOT NULL DEFAULT 0,
                last_built_at TEXT,
                last_error TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS records (
                id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL,
                category TEXT NOT NULL,
                record_type TEXT NOT NULL,
                title TEXT NOT NULL,
                organization TEXT NOT NULL,
                url TEXT NOT NULL,
                text TEXT NOT NULL,
                tags_json TEXT NOT NULL,
                vector_json TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                embedding_source TEXT NOT NULL,
                fetched_at TEXT NOT NULL,
                FOREIGN KEY(source_id) REFERENCES sources(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_records_source ON records(source_id);
            CREATE INDEX IF NOT EXISTS idx_records_category ON records(category);
            CREATE INDEX IF NOT EXISTS idx_records_url ON records(url);

            CREATE TABLE IF NOT EXISTS embedding_cache (
                content_hash TEXT PRIMARY KEY,
                vector_json TEXT NOT NULL,
                embedding_source TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS state (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )


def set_state(key: str, value: Any) -> None:
    with _DB_LOCK, db() as connection:
        connection.execute(
            """
            INSERT INTO state(key, value_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value_json=excluded.value_json,
                updated_at=excluded.updated_at
            """,
            (key, json.dumps(value, ensure_ascii=False), utc_now()),
        )


def get_state(key: str, default: Any = None) -> Any:
    with db() as connection:
        row = connection.execute("SELECT value_json FROM state WHERE key = ?", (key,)).fetchone()
    if not row:
        return default
    try:
        return json.loads(row["value_json"])
    except Exception:
        return default


def load_source_manifest() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for path in (SOURCE_MANIFEST, CUSTOM_SOURCE_MANIFEST):
        if not path.exists():
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        entries = payload.get("sources", payload if isinstance(payload, list) else [])
        if isinstance(entries, list):
            items.extend(item for item in entries if isinstance(item, dict))

    seen: set[str] = set()
    normalized: list[dict[str, Any]] = []
    for item in items:
        source_id = compact_space(str(item.get("id") or slug(str(item.get("title") or item.get("url") or "source"))))
        if source_id in seen:
            continue
        seen.add(source_id)
        normalized.append(
            {
                "id": source_id,
                "title": compact_space(str(item.get("title") or item.get("organization") or source_id)),
                "organization": compact_space(str(item.get("organization") or item.get("title") or source_id)),
                "category": compact_space(str(item.get("category") or "Community Resources")),
                "url": compact_space(str(item.get("url") or "")),
                "description": compact_space(str(item.get("description") or "")),
                "tags": [compact_space(str(tag)) for tag in item.get("tags", []) if compact_space(str(tag))],
                "max_pages": max(1, min(30, int(item.get("max_pages", 4)))),
                "include": [compact_space(str(term)).lower() for term in item.get("include", []) if compact_space(str(term))],
            }
        )
    return normalized


def sync_sources() -> int:
    sources = load_source_manifest()
    now = utc_now()
    with _DB_LOCK, db() as connection:
        for source in sources:
            connection.execute(
                """
                INSERT INTO sources(
                    id, title, organization, category, url, description,
                    tags_json, max_pages, include_json, status, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'registered', ?)
                ON CONFLICT(id) DO UPDATE SET
                    title=excluded.title,
                    organization=excluded.organization,
                    category=excluded.category,
                    url=excluded.url,
                    description=excluded.description,
                    tags_json=excluded.tags_json,
                    max_pages=excluded.max_pages,
                    include_json=excluded.include_json,
                    updated_at=excluded.updated_at
                """,
                (
                    source["id"],
                    source["title"],
                    source["organization"],
                    source["category"],
                    source["url"],
                    source["description"],
                    json.dumps(source["tags"], ensure_ascii=False),
                    source["max_pages"],
                    json.dumps(source["include"], ensure_ascii=False),
                    now,
                ),
            )
    return len(sources)


def normalize_vector(value: Any) -> list[float] | None:
    if not isinstance(value, list) or not value or isinstance(value[0], (list, dict)):
        return None
    try:
        vector = [float(item) for item in value]
    except (TypeError, ValueError):
        return None
    if len(vector) != VECTOR_DIM or not all(math.isfinite(item) for item in vector):
        return None
    norm = math.sqrt(sum(item * item for item in vector))
    if norm <= 0:
        return None
    return [item / norm for item in vector]


def extract_vectors(payload: Any) -> list[list[float]]:
    """Accept common embedding response shapes without binding the field to one wrapper."""
    candidates: list[Any] = [payload]
    if isinstance(payload, dict):
        for key in (
            "embeddings", "vectors", "embedding", "vector", "data",
            "result", "results", "output", "items", "values",
        ):
            if key in payload:
                candidates.append(payload[key])

    for candidate in candidates:
        direct = normalize_vector(candidate)
        if direct:
            return [direct]

        if isinstance(candidate, dict):
            for key in ("embedding", "vector", "values"):
                direct = normalize_vector(candidate.get(key))
                if direct:
                    return [direct]

        if isinstance(candidate, list):
            vectors: list[list[float]] = []
            for item in candidate:
                found = None
                if isinstance(item, dict):
                    for key in ("embedding", "vector", "values"):
                        found = normalize_vector(item.get(key))
                        if found:
                            break
                else:
                    found = normalize_vector(item)
                if found:
                    vectors.append(found)
            if vectors:
                return vectors
    return []


class Embedder:
    BATCH_PAYLOADS = (
        lambda texts: {"texts": texts},
        lambda texts: {"input": texts},
        lambda texts: {"inputs": texts},
        lambda texts: {"text": texts},
        lambda texts: {"sentences": texts},
    )
    SINGLE_PAYLOADS = (
        lambda text: {"text": text},
        lambda text: {"input": text},
        lambda text: {"query": text},
        lambda text: {"texts": [text]},
        lambda text: {"inputs": [text]},
    )

    def __init__(self) -> None:
        self._batch_strategy: int | None = None
        self._single_strategy: int | None = None
        self._strategy_lock = threading.Lock()

    def headers(self) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": f"A-Living-Purpose-Purpose-Field/{PRODUCT_VERSION}",
        }
        if ARBITER_API_KEY:
            headers["Authorization"] = f"Bearer {ARBITER_API_KEY}"
            headers["X-API-Key"] = ARBITER_API_KEY
        return headers

    def request_json(self, payload: dict[str, Any]) -> Any:
        request = urllib.request.Request(
            ARBITER_EMBED_URL,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers=self.headers(),
            method="POST",
        )
        context = ssl._create_unverified_context() if INSECURE_SSL else ssl.create_default_context()
        try:
            with urllib.request.urlopen(request, timeout=EMBED_TIMEOUT, context=context) as response:
                raw = response.read(MAX_PAGE_BYTES).decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            raw = exc.read(2000).decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code}: {raw[:800]}") from exc
        except Exception as exc:
            raise RuntimeError(str(exc)) from exc
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"ARBITER returned non-JSON: {raw[:800]}") from exc

    def embed_batch_remote(self, texts: list[str]) -> list[list[float]]:
        order = list(range(len(self.BATCH_PAYLOADS)))
        if self._batch_strategy is not None:
            order.remove(self._batch_strategy)
            order.insert(0, self._batch_strategy)
        errors: list[str] = []
        for index in order:
            try:
                vectors = extract_vectors(self.request_json(self.BATCH_PAYLOADS[index](texts)))
                if len(vectors) == len(texts):
                    with self._strategy_lock:
                        self._batch_strategy = index
                    return vectors
                errors.append(f"payload {index}: {len(vectors)} vectors for {len(texts)} texts")
            except Exception as exc:
                errors.append(f"payload {index}: {exc}")
        raise RuntimeError("; ".join(errors[-5:]))

    def embed_single_remote(self, text: str) -> list[float]:
        order = list(range(len(self.SINGLE_PAYLOADS)))
        if self._single_strategy is not None:
            order.remove(self._single_strategy)
            order.insert(0, self._single_strategy)
        errors: list[str] = []
        for index in order:
            try:
                vectors = extract_vectors(self.request_json(self.SINGLE_PAYLOADS[index](text)))
                if len(vectors) == 1:
                    with self._strategy_lock:
                        self._single_strategy = index
                    return vectors[0]
                errors.append(f"payload {index}: {len(vectors)} vectors")
            except Exception as exc:
                errors.append(f"payload {index}: {exc}")
        raise RuntimeError("; ".join(errors[-5:]))

    def embed_remote(self, texts: list[str]) -> list[list[float]]:
        if len(texts) == 1:
            try:
                return self.embed_batch_remote(texts)
            except Exception:
                return [self.embed_single_remote(texts[0])]
        try:
            return self.embed_batch_remote(texts)
        except Exception:
            with concurrent.futures.ThreadPoolExecutor(max_workers=EMBED_WORKERS) as executor:
                return list(executor.map(self.embed_single_remote, texts))

    def embed_many(self, texts: list[str]) -> tuple[list[list[float]], str]:
        if not texts:
            return [], "none"

        hashes = [hashlib.sha256(text.encode("utf-8")).hexdigest() for text in texts]
        results: list[list[float] | None] = [None] * len(texts)
        sources: list[str | None] = [None] * len(texts)

        with db() as connection:
            for index, content_hash in enumerate(hashes):
                row = connection.execute(
                    "SELECT vector_json, embedding_source FROM embedding_cache WHERE content_hash = ?",
                    (content_hash,),
                ).fetchone()
                if row:
                    try:
                        vector = normalize_vector(json.loads(row["vector_json"]))
                    except Exception:
                        vector = None
                    if vector:
                        results[index] = vector
                        sources[index] = row["embedding_source"]

        missing = [index for index, value in enumerate(results) if value is None]
        for start in range(0, len(missing), EMBED_BATCH_SIZE):
            indices = missing[start : start + EMBED_BATCH_SIZE]
            batch_texts = [texts[index] for index in indices]
            try:
                batch_vectors = self.embed_remote(batch_texts)
            except Exception as exc:
                raise ApiError(
                    503,
                    {
                        "error": "ARBITER embedding unavailable",
                        "endpoint": ARBITER_EMBED_URL,
                        "reason": str(exc)[:1600],
                        "expected_dimensions": VECTOR_DIM,
                    },
                ) from exc
            if len(batch_vectors) != len(indices):
                raise ApiError(502, f"ARBITER returned {len(batch_vectors)} vectors for {len(indices)} texts")
            now = utc_now()
            with _DB_LOCK, db() as connection:
                for index, vector in zip(indices, batch_vectors):
                    normalized = normalize_vector(vector)
                    if not normalized:
                        raise ApiError(502, f"ARBITER response was not a valid {VECTOR_DIM}D vector")
                    results[index] = normalized
                    sources[index] = "arbiter"
                    connection.execute(
                        """
                        INSERT INTO embedding_cache(content_hash, vector_json, embedding_source, created_at)
                        VALUES (?, ?, 'arbiter', ?)
                        ON CONFLICT(content_hash) DO UPDATE SET
                            vector_json=excluded.vector_json,
                            embedding_source=excluded.embedding_source
                        """,
                        (hashes[index], json.dumps(normalized), now),
                    )

        return [value for value in results if value is not None], ",".join(sorted(set(item for item in sources if item))) or "arbiter"


EMBEDDER = Embedder()


class PageExtractor(HTMLParser):
    BLOCK_TAGS = {"h1", "h2", "h3", "h4", "p", "li", "dt", "dd", "td", "th", "blockquote"}
    SKIP_TAGS = {"script", "style", "svg", "noscript", "template", "form", "button", "iframe"}
    LANDMARK_SKIP = {"nav", "footer"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.title_parts: list[str] = []
        self.blocks: list[tuple[str, str]] = []
        self.links: list[str] = []
        self._tag_stack: list[str] = []
        self._skip_depth = 0
        self._current_tag: str | None = None
        self._buffer: list[str] = []
        self._current_heading = ""
        self._in_title = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        self._tag_stack.append(tag)
        if tag in self.SKIP_TAGS or tag in self.LANDMARK_SKIP:
            self._skip_depth += 1
        if tag == "title":
            self._in_title = True
        if self._skip_depth == 0 and tag in self.BLOCK_TAGS:
            self._flush()
            self._current_tag = tag
            self._buffer = []
        if self._skip_depth == 0 and tag == "a":
            href = dict(attrs).get("href")
            if href:
                self.links.append(href)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if self._skip_depth == 0 and self._current_tag == tag:
            self._flush()
        if tag == "title":
            self._in_title = False
        if tag in self.SKIP_TAGS or tag in self.LANDMARK_SKIP:
            self._skip_depth = max(0, self._skip_depth - 1)
        if self._tag_stack:
            self._tag_stack.pop()

    def handle_data(self, data: str) -> None:
        if self._skip_depth:
            return
        value = compact_space(data)
        if not value:
            return
        if self._in_title:
            self.title_parts.append(value)
        if self._current_tag:
            self._buffer.append(value)

    def _flush(self) -> None:
        if not self._current_tag:
            return
        text = compact_space(" ".join(self._buffer))
        tag = self._current_tag
        self._current_tag = None
        self._buffer = []
        if len(text) < 2:
            return
        if tag.startswith("h"):
            self._current_heading = text
            self.blocks.append((text, "heading"))
        else:
            self.blocks.append((text, self._current_heading))

    def close(self) -> None:
        self._flush()
        super().close()


def fetch_url(url: str) -> tuple[str, bytes, str]:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; A Living Purpose Purpose Field/1.0; +https://livingpurposeils.org/)",
            "Accept": "text/html,application/xhtml+xml,text/plain;q=0.9,*/*;q=0.2",
            "Accept-Encoding": "gzip",
        },
    )
    context = ssl._create_unverified_context() if INSECURE_SSL else ssl.create_default_context()
    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT, context=context) as response:
            final_url = response.geturl()
            content_type = response.headers.get_content_type()
            encoding = response.headers.get("Content-Encoding", "")
            raw = response.read(MAX_PAGE_BYTES + 1)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"HTTP {exc.code}") from exc
    except Exception as exc:
        raise RuntimeError(str(exc)) from exc

    if len(raw) > MAX_PAGE_BYTES:
        raise RuntimeError(f"page exceeded {MAX_PAGE_BYTES} bytes")
    if encoding.lower() == "gzip":
        raw = gzip.decompress(raw)
    return final_url, raw, content_type


def decode_page(raw: bytes) -> str:
    for encoding in ("utf-8", "utf-8-sig", "windows-1252", "latin-1"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def canonical_url(base_url: str, href: str) -> str | None:
    href = compact_space(href)
    if not href or href.startswith(("#", "mailto:", "tel:", "javascript:", "data:")):
        return None
    joined = urllib.parse.urljoin(base_url, href)
    parsed = urllib.parse.urlparse(joined)
    if parsed.scheme not in {"http", "https"}:
        return None
    path = re.sub(r"/+", "/", parsed.path or "/")
    blocked_extensions = (
        ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".zip", ".doc", ".docx",
        ".xls", ".xlsx", ".ppt", ".pptx", ".mp4", ".mp3", ".avi", ".mov",
    )
    if path.lower().endswith(blocked_extensions):
        return None
    return urllib.parse.urlunparse((parsed.scheme, parsed.netloc.lower(), path, "", "", ""))


def allowed_link(seed_url: str, candidate: str, include_terms: list[str]) -> bool:
    seed = urllib.parse.urlparse(seed_url)
    parsed = urllib.parse.urlparse(candidate)
    if parsed.netloc.lower().removeprefix("www.") != seed.netloc.lower().removeprefix("www."):
        return False
    lowered = (parsed.path + " " + parsed.query).lower()
    blocked = (
        "/wp-admin", "/login", "/privacy", "/terms", "/donate", "/news", "/blog",
        "/events", "/calendar", "/cart", "/checkout", "/search", "/tag/", "/author/",
    )
    if any(term in lowered for term in blocked):
        return False
    if not include_terms:
        return True
    return any(term in lowered for term in include_terms)


def parse_html_page(url: str, raw: bytes) -> dict[str, Any]:
    parser = PageExtractor()
    parser.feed(decode_page(raw))
    parser.close()
    title = compact_space(" ".join(parser.title_parts))
    if not title:
        title = urllib.parse.urlparse(url).path.strip("/").split("/")[-1].replace("-", " ").title() or url
    seen: set[str] = set()
    blocks: list[tuple[str, str]] = []
    for text, heading in parser.blocks:
        key = text.lower()
        if key in seen or len(text) < 12:
            continue
        seen.add(key)
        blocks.append((text, heading))
    return {"url": url, "title": title, "blocks": blocks, "links": parser.links}


def crawl_source(source: dict[str, Any], metadata_only: bool = False) -> tuple[list[dict[str, Any]], list[str]]:
    if metadata_only or not source["url"]:
        return [], []

    queue: deque[str] = deque([source["url"]])
    visited: set[str] = set()
    pages: list[dict[str, Any]] = []
    errors: list[str] = []

    while queue and len(pages) < source["max_pages"]:
        requested_url = queue.popleft()
        canonical = canonical_url(source["url"], requested_url)
        if not canonical or canonical in visited:
            continue
        visited.add(canonical)
        try:
            final_url, raw, content_type = fetch_url(canonical)
            if content_type not in {"text/html", "application/xhtml+xml", "text/plain"}:
                errors.append(f"{canonical}: unsupported {content_type}")
                continue
            if content_type == "text/plain":
                page = {
                    "url": final_url,
                    "title": urllib.parse.urlparse(final_url).path.split("/")[-1] or source["title"],
                    "blocks": [(compact_space(decode_page(raw)), "")],
                    "links": [],
                }
            else:
                page = parse_html_page(final_url, raw)
            if page["blocks"]:
                pages.append(page)
            for href in page["links"]:
                linked = canonical_url(final_url, href)
                if linked and linked not in visited and allowed_link(source["url"], linked, source["include"]):
                    queue.append(linked)
            if CRAWL_DELAY:
                time.sleep(CRAWL_DELAY)
        except Exception as exc:
            errors.append(f"{canonical}: {exc}")

    return pages, errors


def chunk_page(source: dict[str, Any], page: dict[str, Any]) -> list[dict[str, Any]]:
    prefix = (
        f"Organization: {source['organization']}. "
        f"Source: {source['title']}. "
        f"Category: {source['category']}. "
        f"Program summary: {source['description']} "
        f"Signals: {', '.join(source['tags'])}."
    )
    blocks = page["blocks"]
    chunks: list[dict[str, Any]] = []
    current: list[str] = []
    current_heading = ""
    current_len = 0

    def flush() -> None:
        nonlocal current, current_len
        if not current:
            return
        body = compact_space(" ".join(current))
        text = compact_space(f"{prefix} Page: {page['title']}. {body}")
        chunks.append(
            {
                "title": current_heading or page["title"] or source["title"],
                "url": page["url"],
                "text": text,
                "record_type": "page-section",
            }
        )
        current = []
        current_len = 0

    for block_text, heading in blocks:
        if heading == "heading":
            if current_len > 850:
                flush()
            current_heading = block_text
            current.append(block_text)
            current_len += len(block_text)
            continue
        if current_len + len(block_text) > 1750 and current:
            flush()
        if heading and heading != "heading" and (not current or current_heading != heading):
            current_heading = heading
            current.append(heading)
            current_len += len(heading)
        current.append(block_text)
        current_len += len(block_text)
    flush()
    return chunks[:20]


def metadata_record(source: dict[str, Any]) -> dict[str, Any]:
    text = compact_space(
        f"Organization: {source['organization']}. "
        f"Program or resource: {source['title']}. "
        f"Category: {source['category']}. "
        f"{source['description']} "
        f"Signals: {', '.join(source['tags'])}. "
        f"Official source: {source['url']}."
    )
    return {
        "title": source["title"],
        "url": source["url"],
        "text": text,
        "record_type": "source-profile",
    }


def make_record_id(source_id: str, url: str, index: int, text: str) -> str:
    digest = hashlib.sha256(f"{source_id}\n{url}\n{index}\n{text[:240]}".encode("utf-8")).hexdigest()[:24]
    return f"{source_id}:{digest}"


def update_source_status(source_id: str, **updates: Any) -> None:
    allowed = {
        "status", "page_count", "record_count", "last_built_at", "last_error",
    }
    fields = [(key, value) for key, value in updates.items() if key in allowed]
    if not fields:
        return
    sql = "UPDATE sources SET " + ", ".join(f"{key} = ?" for key, _ in fields) + ", updated_at = ? WHERE id = ?"
    params = [value for _, value in fields] + [utc_now(), source_id]
    with _DB_LOCK, db() as connection:
        connection.execute(sql, params)


def build_one_source(source: dict[str, Any], metadata_only: bool = False) -> dict[str, Any]:
    source_id = source["id"]
    update_source_status(source_id, status="building", last_error=None)
    pages, errors = crawl_source(source, metadata_only=metadata_only)

    raw_records: list[dict[str, Any]] = [metadata_record(source)]
    for page in pages:
        raw_records.extend(chunk_page(source, page))

    deduped: list[dict[str, Any]] = []
    seen_hashes: set[str] = set()
    for record in raw_records:
        content_hash = hashlib.sha256(record["text"].encode("utf-8")).hexdigest()
        if content_hash in seen_hashes:
            continue
        seen_hashes.add(content_hash)
        record["content_hash"] = content_hash
        deduped.append(record)

    vectors, embedding_source = EMBEDDER.embed_many([record["text"] for record in deduped])
    if len(vectors) != len(deduped):
        raise ApiError(502, f"Embedding count mismatch for {source_id}")

    now = utc_now()
    with _DB_LOCK, db() as connection:
        connection.execute("DELETE FROM records WHERE source_id = ?", (source_id,))
        for index, (record, vector) in enumerate(zip(deduped, vectors)):
            connection.execute(
                """
                INSERT INTO records(
                    id, source_id, category, record_type, title, organization, url,
                    text, tags_json, vector_json, content_hash, embedding_source, fetched_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    make_record_id(source_id, record["url"], index, record["text"]),
                    source_id,
                    source["category"],
                    record["record_type"],
                    record["title"],
                    source["organization"],
                    record["url"],
                    record["text"],
                    json.dumps(source["tags"], ensure_ascii=False),
                    json.dumps(vector),
                    record["content_hash"],
                    embedding_source,
                    now,
                ),
            )

    status = "ready" if pages or metadata_only else "metadata-only"
    error_text = "\n".join(errors[-8:]) if errors else None
    update_source_status(
        source_id,
        status=status,
        page_count=len(pages),
        record_count=len(deduped),
        last_built_at=now,
        last_error=error_text,
    )
    return {
        "source_id": source_id,
        "status": status,
        "pages": len(pages),
        "records": len(deduped),
        "errors": errors,
        "embedding_source": embedding_source,
    }


def build_field(
    reset: bool = False,
    metadata_only: bool = False,
    selected_ids: set[str] | None = None,
) -> dict[str, Any]:
    if not _BUILD_LOCK.acquire(blocking=False):
        raise ApiError(409, "A field build is already running")

    started = time.time()
    try:
        init_db()
        source_count = sync_sources()
        sources = load_source_manifest()
        if selected_ids:
            sources = [source for source in sources if source["id"] in selected_ids]
        if reset:
            with _DB_LOCK, db() as connection:
                if selected_ids:
                    placeholders = ",".join("?" for _ in selected_ids)
                    connection.execute(f"DELETE FROM records WHERE source_id IN ({placeholders})", list(selected_ids))
                else:
                    connection.execute("DELETE FROM records")
                    connection.execute("DELETE FROM embedding_cache")
        set_state(
            "build",
            {
                "running": True,
                "started_at": utc_now(),
                "completed_sources": 0,
                "total_sources": len(sources),
                "current_source": None,
                "errors": 0,
                "metadata_only": metadata_only,
            },
        )

        completed: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        for index, source in enumerate(sources, 1):
            set_state(
                "build",
                {
                    "running": True,
                    "started_at": get_state("build", {}).get("started_at"),
                    "completed_sources": index - 1,
                    "total_sources": len(sources),
                    "current_source": source["title"],
                    "current_source_id": source["id"],
                    "errors": len(failed),
                    "metadata_only": metadata_only,
                },
            )
            print(f"[{index:02d}/{len(sources):02d}] {source['organization']} · {source['title']}", flush=True)
            try:
                result = build_one_source(source, metadata_only=metadata_only)
                completed.append(result)
                print(
                    f"  READY · {result['pages']} pages · {result['records']} records · {result['embedding_source']}",
                    flush=True,
                )
            except Exception as exc:
                detail = exc.detail if isinstance(exc, ApiError) else str(exc)
                update_source_status(source["id"], status="failed", last_error=str(detail)[:3000])
                failed.append({"source_id": source["id"], "error": detail})
                print(f"  FAILED · {detail}", flush=True)

        with db() as connection:
            record_count = connection.execute("SELECT COUNT(*) AS n FROM records").fetchone()["n"]
            category_count = connection.execute("SELECT COUNT(DISTINCT category) AS n FROM records").fetchone()["n"]
        finished_at = utc_now()
        state = {
            "running": False,
            "started_at": get_state("build", {}).get("started_at"),
            "finished_at": finished_at,
            "elapsed_seconds": round(time.time() - started, 2),
            "completed_sources": len(completed),
            "failed_sources": len(failed),
            "total_sources": len(sources),
            "current_source": None,
            "records": record_count,
            "categories": category_count,
            "metadata_only": metadata_only,
            "failures": failed[:20],
        }
        set_state("build", state)
        return {
            "ok": not failed,
            "registered_sources": source_count,
            "built_sources": len(completed),
            "failed_sources": len(failed),
            "records": record_count,
            "categories": category_count,
            "elapsed_seconds": round(time.time() - started, 2),
            "failures": failed,
        }
    finally:
        _BUILD_LOCK.release()


def dot(left: list[float], right: list[float]) -> float:
    return sum(a * b for a, b in zip(left, right))


def excerpt(text: str, query: str, limit: int = 520) -> str:
    clean = compact_space(text)
    if len(clean) <= limit:
        return clean
    tokens = [token.lower() for token in re.findall(r"[a-zA-Z0-9']+", query) if len(token) > 3]
    lowered = clean.lower()
    positions = [lowered.find(token) for token in tokens if lowered.find(token) >= 0]
    center = min(positions) if positions else 0
    start = max(0, center - limit // 4)
    end = min(len(clean), start + limit)
    if end - start < limit:
        start = max(0, end - limit)
    return ("…" if start else "") + clean[start:end].strip() + ("…" if end < len(clean) else "")


def search_field(
    query: str,
    mode: str = "person",
    perspective: str = "person",
    categories: list[str] | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    started = time.perf_counter()
    query = compact_space(query)
    if len(query) < 3:
        raise ApiError(400, "Enter a fuller description of the person, goal, situation, or agency need.")
    mode = mode if mode in MODE_PREFIXES else "person"
    perspective = perspective if perspective in PERSPECTIVE_PREFIXES else "person"
    full_query = MODE_PREFIXES[mode] + PERSPECTIVE_PREFIXES[perspective] + query
    vectors, embedding_source = EMBEDDER.embed_many([full_query])
    query_vector = vectors[0]

    params: list[Any] = []
    sql = """
        SELECT r.*, s.description AS source_description, s.status AS source_status
        FROM records r
        JOIN sources s ON s.id = r.source_id
    """
    if categories:
        placeholders = ",".join("?" for _ in categories)
        sql += f" WHERE r.category IN ({placeholders})"
        params.extend(categories)

    scored: list[tuple[float, sqlite3.Row]] = []
    with db() as connection:
        for row in connection.execute(sql, params):
            try:
                vector = json.loads(row["vector_json"])
            except Exception:
                continue
            if len(vector) != VECTOR_DIM:
                continue
            scored.append((dot(query_vector, vector), row))

    scored.sort(key=lambda item: (-item[0], item[1]["organization"].lower(), item[1]["title"].lower()))
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for score, row in scored:
        key = (row["source_id"], row["url"])
        if key not in grouped:
            grouped[key] = {
                "id": row["id"],
                "source_id": row["source_id"],
                "category": row["category"],
                "record_type": row["record_type"],
                "title": row["title"],
                "organization": row["organization"],
                "url": row["url"],
                "description": row["source_description"],
                "excerpt": excerpt(row["text"], query),
                "tags": json.loads(row["tags_json"]),
                "score": score,
                "supporting_sections": 1,
                "source_status": row["source_status"],
            }
        else:
            grouped[key]["supporting_sections"] += 1

    results = list(grouped.values())
    for result in results:
        result["score"] = round(
            result["score"] + min(0.012, math.log1p(result["supporting_sections"]) * 0.003),
            6,
        )
    results.sort(key=lambda item: (-item["score"], item["organization"].lower(), item["title"].lower()))
    results = results[: max(1, min(100, limit))]

    facets = Counter(item["category"] for item in grouped.values())
    return {
        "ok": True,
        "query": query,
        "mode": mode,
        "perspective": perspective,
        "embedding_source": embedding_source,
        "vector_dimensions": VECTOR_DIM,
        "candidate_records": len(scored),
        "candidate_pages": len(grouped),
        "elapsed_ms": round((time.perf_counter() - started) * 1000, 2),
        "results": results,
        "facets": [{"category": key, "count": value} for key, value in sorted(facets.items())],
    }


def field_stats() -> dict[str, Any]:
    sync_sources()
    with db() as connection:
        source_total = connection.execute("SELECT COUNT(*) AS n FROM sources").fetchone()["n"]
        ready_sources = connection.execute(
            "SELECT COUNT(*) AS n FROM sources WHERE status IN ('ready','metadata-only')"
        ).fetchone()["n"]
        records = connection.execute("SELECT COUNT(*) AS n FROM records").fetchone()["n"]
        pages = connection.execute("SELECT COALESCE(SUM(page_count),0) AS n FROM sources").fetchone()["n"]
        category_rows = connection.execute(
            """
            SELECT category, COUNT(*) AS records, COUNT(DISTINCT source_id) AS sources
            FROM records
            GROUP BY category
            ORDER BY records DESC, category
            """
        ).fetchall()
        registered_category_rows = connection.execute(
            """
            SELECT category, COUNT(*) AS sources
            FROM sources
            GROUP BY category
            ORDER BY sources DESC, category
            """
        ).fetchall()
    categories = [
        {"category": row["category"], "records": row["records"], "sources": row["sources"]}
        for row in category_rows
    ]
    if not categories:
        categories = [
            {"category": row["category"], "records": 0, "sources": row["sources"]}
            for row in registered_category_rows
        ]
    return {
        "ok": True,
        "service": "A Living Purpose — Purpose Field",
        "version": PRODUCT_VERSION,
        "architecture": PRODUCT_ARCHITECTURE,
        "database": str(DB_PATH),
        "sources": source_total,
        "ready_sources": ready_sources,
        "pages": pages,
        "records": records,
        "categories": categories,
        "category_count": len(categories),
        "vector_dimensions": VECTOR_DIM,
        "arbiter_endpoint": ARBITER_EMBED_URL,
        "build": get_state("build", {"running": False}),
    }


def list_sources() -> list[dict[str, Any]]:
    sync_sources()
    with db() as connection:
        rows = connection.execute(
            """
            SELECT id, title, organization, category, url, description, tags_json,
                   status, page_count, record_count, last_built_at, last_error
            FROM sources
            ORDER BY category, organization, title
            """
        ).fetchall()
    return [
        {
            "id": row["id"],
            "title": row["title"],
            "organization": row["organization"],
            "category": row["category"],
            "url": row["url"],
            "description": row["description"],
            "tags": json.loads(row["tags_json"]),
            "status": row["status"],
            "pages": row["page_count"],
            "records": row["record_count"],
            "last_built_at": row["last_built_at"],
            "last_error": row["last_error"],
        }
        for row in rows
    ]


def add_custom_source(payload: dict[str, Any]) -> dict[str, Any]:
    url = compact_space(str(payload.get("url") or ""))
    title = compact_space(str(payload.get("title") or ""))
    organization = compact_space(str(payload.get("organization") or title))
    category = compact_space(str(payload.get("category") or "Community Resources"))
    description = compact_space(str(payload.get("description") or ""))
    if not url.startswith(("http://", "https://")):
        raise ApiError(400, "A complete http or https URL is required.")
    if not title or not organization or not description:
        raise ApiError(400, "title, organization, and description are required.")
    source = {
        "id": compact_space(str(payload.get("id") or f"custom-{slug(organization)}-{hashlib.sha256(url.encode()).hexdigest()[:8]}")),
        "title": title,
        "organization": organization,
        "category": category,
        "url": url,
        "description": description,
        "tags": [compact_space(str(item)) for item in payload.get("tags", []) if compact_space(str(item))],
        "max_pages": max(1, min(30, int(payload.get("max_pages", 5)))),
        "include": [compact_space(str(item)).lower() for item in payload.get("include", []) if compact_space(str(item))],
    }
    existing = {"sources": []}
    if CUSTOM_SOURCE_MANIFEST.exists():
        existing = json.loads(CUSTOM_SOURCE_MANIFEST.read_text(encoding="utf-8"))
        if not isinstance(existing.get("sources"), list):
            existing = {"sources": []}
    existing["sources"] = [item for item in existing["sources"] if item.get("id") != source["id"]]
    existing["sources"].append(source)
    CUSTOM_SOURCE_MANIFEST.write_text(json.dumps(existing, indent=2, ensure_ascii=False), encoding="utf-8")
    sync_sources()
    return source


def require_admin(headers: Any) -> None:
    if not PURPOSE_FIELD_API_KEY:
        return
    supplied = headers.get("X-Purpose-Field-Key", "")
    if supplied != PURPOSE_FIELD_API_KEY:
        raise ApiError(401, "Missing or invalid X-Purpose-Field-Key")


def start_background_build(
    reset: bool = False,
    metadata_only: bool = False,
    selected_ids: set[str] | None = None,
) -> dict[str, Any]:
    global _BUILD_THREAD
    if _BUILD_THREAD and _BUILD_THREAD.is_alive():
        return {"ok": True, "started": False, "build": get_state("build", {})}

    def runner() -> None:
        try:
            build_field(reset=reset, metadata_only=metadata_only, selected_ids=selected_ids)
        except Exception as exc:
            set_state(
                "build",
                {
                    "running": False,
                    "finished_at": utc_now(),
                    "fatal_error": exc.detail if isinstance(exc, ApiError) else str(exc),
                },
            )
            traceback.print_exc()

    _BUILD_THREAD = threading.Thread(target=runner, name="purpose-field-build", daemon=True)
    _BUILD_THREAD.start()
    return {"ok": True, "started": True, "build": get_state("build", {})}


class Handler(BaseHTTPRequestHandler):
    server_version = f"PurposeField/{PRODUCT_VERSION}"

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stdout.write(f"{self.address_string()} - {fmt % args}\n")
        sys.stdout.flush()

    def send_json(self, status: int, payload: Any) -> None:
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)

    def read_json(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        if length <= 0 or length > 2_000_000:
            raise ApiError(400, "Invalid request body")
        raw = self.rfile.read(length)
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ApiError(400, "Invalid JSON") from exc
        if not isinstance(payload, dict):
            raise ApiError(400, "JSON body must be an object")
        return payload

    def serve_static(self, relative: str) -> None:
        relative = relative.lstrip("/") or "index.html"
        path = (STATIC_DIR / relative).resolve()
        if STATIC_DIR.resolve() not in path.parents and path != STATIC_DIR.resolve():
            raise ApiError(404, "Not found")
        if not path.exists() or not path.is_file():
            raise ApiError(404, "Not found")
        raw = path.read_bytes()
        content_type = mimetypes.guess_type(str(path))[0] or "application/octet-stream"
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self) -> None:
        try:
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path
            if path == "/health":
                stats = field_stats()
                stats["ok"] = True
                self.send_json(200, stats)
                return
            if path == "/api/stats":
                self.send_json(200, field_stats())
                return
            if path == "/api/sources":
                self.send_json(200, {"ok": True, "sources": list_sources()})
                return
            if path == "/api/build/status":
                self.send_json(200, {"ok": True, "build": get_state("build", {"running": False})})
                return
            if path == "/":
                self.serve_static("index.html")
                return
            if path.startswith("/static/"):
                self.serve_static(path[len("/static/"):])
                return
            self.serve_static(path)
        except ApiError as exc:
            self.send_json(exc.status, {"ok": False, "detail": exc.detail})
        except Exception as exc:
            self.send_json(500, {"ok": False, "detail": str(exc)})

    def do_POST(self) -> None:
        try:
            path = urllib.parse.urlparse(self.path).path
            payload = self.read_json()
            if path == "/api/search":
                categories = payload.get("categories")
                if categories is not None and not isinstance(categories, list):
                    raise ApiError(400, "categories must be an array")
                result = search_field(
                    query=str(payload.get("query") or ""),
                    mode=str(payload.get("mode") or "person"),
                    perspective=str(payload.get("perspective") or "person"),
                    categories=[str(item) for item in categories] if categories else None,
                    limit=int(payload.get("limit", 20)),
                )
                self.send_json(200, result)
                return
            if path == "/api/build":
                require_admin(self.headers)
                self.send_json(
                    202,
                    start_background_build(
                        reset=bool(payload.get("reset", False)),
                        metadata_only=bool(payload.get("metadata_only", False)),
                        selected_ids=(
                            {str(item) for item in payload.get("source_ids", [])}
                            if isinstance(payload.get("source_ids"), list) and payload.get("source_ids")
                            else None
                        ),
                    ),
                )
                return
            if path == "/api/sources/add":
                require_admin(self.headers)
                source = add_custom_source(payload)
                build = start_background_build(
                    reset=False,
                    metadata_only=False,
                    selected_ids={source["id"]},
                )
                self.send_json(202, {"ok": True, "source": source, "build": build})
                return
            raise ApiError(404, "Not found")
        except ApiError as exc:
            self.send_json(exc.status, {"ok": False, "detail": exc.detail})
        except Exception as exc:
            self.send_json(500, {"ok": False, "detail": str(exc)})


def run_server() -> None:
    init_db()
    source_count = sync_sources()
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print("A LIVING PURPOSE — ARBITER PURPOSE FIELD")
    print("────────────────────────────────────────────────────────")
    print(f"app:      http://{HOST}:{PORT}")
    print(f"arbiter:  {ARBITER_EMBED_URL}")
    print(f"sources:  {source_count}")
    print(f"database: {DB_PATH}")
    print()
    print("READY", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def verify() -> int:
    init_db()
    registered = sync_sources()
    print("A LIVING PURPOSE — PURPOSE FIELD VERIFY")
    print("────────────────────────────────────────────────────────")
    print(f"registered sources  {registered:,}")
    print(f"ARBITER endpoint    {ARBITER_EMBED_URL}")
    print(f"vector dimensions   {VECTOR_DIM}")
    try:
        vectors, source = EMBEDDER.embed_many(["A person wants to live independently and find meaningful community work."])
        print(f"ARBITER probe       PASS · {len(vectors[0])}D · {source}")
    except Exception as exc:
        detail = exc.detail if isinstance(exc, ApiError) else str(exc)
        print(f"ARBITER probe       FAIL · {detail}")
        return 1

    stats = field_stats()
    print(f"field records       {stats['records']:,}")
    print(f"field pages         {stats['pages']:,}")
    print(f"field categories    {stats['category_count']:,}")
    if stats["records"] == 0:
        print("search              NOT RUN · build the field first")
        return 2

    result = search_field(
        "An adult wants to live in their own apartment, learn budgeting, use public transportation, and find a calm volunteer role.",
        mode="person",
        perspective="person",
        limit=5,
    )
    print(f"search              PASS · {len(result['results'])} results")
    for index, item in enumerate(result["results"][:5], 1):
        print(f"  {index:02d} {item['score']:.3f} · {item['category']} · {item['organization']} · {item['title']}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="A Living Purpose ARBITER Purpose Field")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("serve")
    build_parser = subparsers.add_parser("build")
    build_parser.add_argument("--reset", action="store_true")
    build_parser.add_argument("--metadata-only", action="store_true")
    build_parser.add_argument("--source", action="append", default=[])

    subparsers.add_parser("verify")
    subparsers.add_parser("probe")

    args = parser.parse_args()
    command = args.command or "serve"

    if command == "serve":
        run_server()
        return 0
    if command == "build":
        result = build_field(
            reset=args.reset,
            metadata_only=args.metadata_only,
            selected_ids=set(args.source) if args.source else None,
        )
        print()
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result["ok"] else 1
    if command == "verify":
        return verify()
    if command == "probe":
        init_db()
        vectors, source = EMBEDDER.embed_many(["purpose field endpoint probe"])
        print(json.dumps({"ok": True, "endpoint": ARBITER_EMBED_URL, "dimensions": len(vectors[0]), "source": source}, indent=2))
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
