import html
import json
import logging
import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Dict, List, Optional

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("pull_rss")

RSS_URL = "https://www.reddit.com/r/DungeonCourtroom/new/.rss"
USER_AGENT = "DungeonCourtroomPipeline/0.1 (+https://www.youtube.com/@DungeonCourtroom)"

CASE_TITLE_PREFIX = "Case Submission:"

EXCLUDE_TITLE_SUBSTRINGS = [
    "Welcome to Dungeon Courtroom",
    "Start Here:",
    "Submit Your D&D Case",
    "Template Inside",
]

ATOM_NS = {"a": "http://www.w3.org/2005/Atom"}
STATE_PATH = "state/seen.json"
OUT_PATH = "out/latest_case.json"

# Treat these tags as line breaks before stripping all tags
_BREAK_TAG_RE = re.compile(r"(?is)<\s*(br\s*/?|/p\s*|/div\s*|/li\s*)>")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_seen() -> Dict[str, bool]:
    try:
        if not os.path.exists(STATE_PATH):
            return {}
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        log.exception("Failed to read %s", STATE_PATH)
        return {}


def save_seen(seen: Dict[str, bool]) -> None:
    try:
        d = os.path.dirname(STATE_PATH)
        if d:
            os.makedirs(d, exist_ok=True)
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(seen, f, indent=2, sort_keys=True)
    except Exception:
        log.exception("Failed to write %s", STATE_PATH)
        raise


def fetch_rss() -> str:
    try:
        r = requests.get(RSS_URL, headers={"User-Agent": USER_AGENT}, timeout=25)
        r.raise_for_status()
        return r.text
    except Exception:
        log.exception("RSS fetch failed")
        raise


def strip_tags_and_decode_keep_newlines(s: str) -> str:
    """
    Convert some HTML structure into newlines, strip tags, decode entities,
    and normalize whitespace while keeping paragraph breaks.
    """
    s = s or ""

    # Insert newlines for common break-ish tags BEFORE removing tags
    s = _BREAK_TAG_RE.sub("\n", s)

    # Strip remaining tags
    s = re.sub(r"(?is)<[^>]+>", " ", s)

    # Decode entities; reddit can double-escape
    cur = s
    for _ in range(3):
        nxt = html.unescape(cur)
        if nxt == cur:
            break
        cur = nxt

    # Normalize: keep newlines, collapse spaces per line
    cur = cur.replace("\r", "")
    lines = []
    for line in cur.split("\n"):
        line = re.sub(r"[ \t]+", " ", line).strip()
        if line:
            lines.append(line)

    # Collapse multiple blank lines (already removed), join with newline
    return "\n".join(lines).strip()


def normalize_reddit_rss_text(text: str) -> str:
    """
    Strip boilerplate like:
      'submitted by /u/name'
      '[link]' / '[comments]'
    Works even when those appear on separate lines.
    """
    t = (text or "").strip()
    if not t:
        return ""

    # Remove "submitted by /u/..." anywhere (line-based or inline)
    t = re.sub(r"(?im)^\s*submitted by\s*/u/\S+.*$", "", t).strip()
    t = re.sub(r"(?i)\s*submitted by\s*/u/\S+.*$", "", t).strip()

    # Remove bare [link] / [comments] lines
    t = re.sub(r"(?im)^\s*\[(link|comments)\]\s*$", "", t).strip()

    # Collapse excessive blank lines after removals
    lines = [ln.strip() for ln in t.splitlines() if ln.strip()]
    return "\n".join(lines).strip()


def build_case_text(title: str, content_text: str) -> str:
    """
    Prefer body content if it looks substantive; otherwise fall back.

    - If content has enough non-whitespace => use it
    - Else if title starts with "Case Submission:" => use title suffix
    - Else => use full title (so normal questions work)
    """
    ct = normalize_reddit_rss_text(content_text)
    if len(re.sub(r"\s+", "", ct)) >= 20:
        return ct

    t = (title or "").strip()
    if t.lower().startswith(CASE_TITLE_PREFIX.lower()):
        return t[len(CASE_TITLE_PREFIX) :].strip()

    return t


def _parse_published_iso(s: str) -> datetime:
    try:
        # RSS uses ISO like 2026-02-27T03:57:52+00:00
        return datetime.fromisoformat((s or "").strip())
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)


def parse_entries(xml_text: str) -> List[dict]:
    try:
        root = ET.fromstring(xml_text)
        out: List[dict] = []

        for entry in root.findall("a:entry", ATOM_NS):
            post_id = (entry.findtext("a:id", default="", namespaces=ATOM_NS) or "").strip()
            title = (entry.findtext("a:title", default="", namespaces=ATOM_NS) or "").strip()
            author = (entry.findtext("a:author/a:name", default="", namespaces=ATOM_NS) or "").strip()
            published = (entry.findtext("a:published", default="", namespaces=ATOM_NS) or "").strip()

            # Prefer rel="alternate" for permalink
            url = ""
            for link_el in entry.findall("a:link", ATOM_NS):
                rel = (link_el.get("rel") or "").lower()
                href = (link_el.get("href") or "").strip()
                if rel == "alternate" and href:
                    url = href
                    break
            if not url:
                link_el = entry.find("a:link", ATOM_NS)
                url = (link_el.get("href") if link_el is not None else "") or ""

            content_html = (entry.findtext("a:content", default="", namespaces=ATOM_NS) or "")
            content_text = strip_tags_and_decode_keep_newlines(content_html)
            content_text = normalize_reddit_rss_text(content_text)

            out.append(
                {
                    "post_id": post_id,
                    "title": title,
                    "author": author,
                    "published": published,
                    "url": url,
                    "content_text": content_text,
                    "case_text": build_case_text(title, content_text),
                }
            )

        # Deterministic: newest-first
        out.sort(key=lambda e: _parse_published_iso(e.get("published", "")), reverse=True)
        return out

    except Exception:
        log.exception("RSS parse failed")
        raise


def _is_excluded_title(title: str) -> bool:
    t = (title or "").strip().lower()
    return any(s.lower() in t for s in EXCLUDE_TITLE_SUBSTRINGS)


def pick_next_case(entries: List[dict], seen: Dict[str, bool]) -> Optional[dict]:
    """
    Two-pass selection:
      Pass 1: Prefer explicit Case Submission posts.
      Pass 2: Otherwise accept any new post (except excluded pinned/info).
    """
    def is_seen(post_id: str) -> bool:
        return bool(seen.get(post_id, False))

    # Pass 1 (preferred)
    for e in entries:
        title = (e.get("title") or "").strip()
        post_id = (e.get("post_id") or "").strip()
        if not post_id or is_seen(post_id) or _is_excluded_title(title):
            continue
        if CASE_TITLE_PREFIX.lower() in title.lower():
            return e

    # Pass 2 (fallback)
    for e in entries:
        title = (e.get("title") or "").strip()
        post_id = (e.get("post_id") or "").strip()
        if not post_id or is_seen(post_id) or _is_excluded_title(title):
            continue
        return e

    return None


def write_latest(case: dict) -> None:
    try:
        d = os.path.dirname(OUT_PATH)
        if d:
            os.makedirs(d, exist_ok=True)
        payload = {"ingested_utc": utc_now_iso(), **case}
        with open(OUT_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        log.info("Wrote %s", OUT_PATH)
    except Exception:
        log.exception("Failed to write %s", OUT_PATH)
        raise


def main() -> int:
    try:
        seen = load_seen()
        xml_text = fetch_rss()
        entries = parse_entries(xml_text)
        case = pick_next_case(entries, seen)

        # Do NOT overwrite latest_case.json if nothing new exists
        if not case:
            log.info("No new case found; leaving %s unchanged.", OUT_PATH)
            return 0

        write_latest(case)
        seen[case["post_id"]] = True
        save_seen(seen)
        log.info("Marked seen: %s", case["post_id"])
        return 0

    except Exception:
        log.exception("Fatal error in main()")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
