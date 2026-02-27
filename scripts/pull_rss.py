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
CASE_TITLE_PREFIX = "Case Submission:"  # RSS has no flair; rely on this marker

ATOM_NS = {"a": "http://www.w3.org/2005/Atom"}
STATE_PATH = "state/seen.json"
OUT_PATH = "out/latest_case.json"


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
        os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
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


def strip_tags_and_decode(s: str) -> str:
    """
    1) Remove HTML tags
    2) Decode HTML entities (Reddit RSS can double-escape, so decode until stable)
    3) Normalize whitespace
    """
    s = re.sub(r"<[^>]+>", " ", s or "")

    cur = s
    for _ in range(3):  # cap loops: prevents weird edge cases
        nxt = html.unescape(cur)
        if nxt == cur:
            break
        cur = nxt

    cur = re.sub(r"\s+", " ", cur).strip()
    return cur


def normalize_reddit_rss_text(text: str) -> str:
    """
    Strip boilerplate like:
      '... submitted by /u/name [link] [comments]'
    """
    t = (text or "").strip()
    t = re.sub(r"\s*submitted by\s*/u/\S+.*$", "", t, flags=re.IGNORECASE).strip()
    t = re.sub(r"\s*\[(link|comments)\]\s*$", "", t, flags=re.IGNORECASE).strip()
    return t


def build_case_text(title: str, content_text: str) -> str:
    """
    Prefer cleaned content; fallback to the title minus prefix.
    """
    ct = normalize_reddit_rss_text(content_text).strip()
    if len(ct) >= 20:
        return ct

    t = (title or "").strip()
    if t.lower().startswith(CASE_TITLE_PREFIX.lower()):
        return t[len(CASE_TITLE_PREFIX) :].strip()

    return ct


def parse_entries(xml_text: str) -> List[dict]:
    try:
        root = ET.fromstring(xml_text)
        out: List[dict] = []

        for entry in root.findall("a:entry", ATOM_NS):
            post_id = (entry.findtext("a:id", default="", namespaces=ATOM_NS) or "").strip()
            title = (entry.findtext("a:title", default="", namespaces=ATOM_NS) or "").strip()
            author = (entry.findtext("a:author/a:name", default="", namespaces=ATOM_NS) or "").strip()
            published = (entry.findtext("a:published", default="", namespaces=ATOM_NS) or "").strip()

            # Prefer rel="alternate" for the post permalink
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
            content_text = strip_tags_and_decode(content_html)
            content_text = normalize_reddit_rss_text(content_text)
            case_text = build_case_text(title, content_text)

            out.append(
                {
                    "post_id": post_id,
                    "title": title,
                    "author": author,
                    "published": published,
                    "url": url,
                    "content_text": content_text,
                    "case_text": case_text,
                }
            )

        return out
    except Exception:
        log.exception("RSS parse failed")
        raise


def pick_next_case(entries: List[dict], seen: Dict[str, bool]) -> Optional[dict]:
    for e in entries:
        if CASE_TITLE_PREFIX.lower() not in (e.get("title") or "").lower():
            continue

        post_id = (e.get("post_id") or "").strip()
        if not post_id:
            continue
        if seen.get(post_id, False):
            continue

        return e

    return None


def write_latest(case: dict) -> None:
    try:
        os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
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

        # IMPORTANT: do NOT overwrite latest_case.json when nothing new exists
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
