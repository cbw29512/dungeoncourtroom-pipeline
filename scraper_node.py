"""
scraper_node.py
Question sources in priority order:
  1. r/DungeonCourtroom (your subreddit — community submissions)
  2. r/DnD, r/DMAcademy, r/dndnext (backup Reddit sources)
  3. StackExchange RPG RSS (final fallback)

No PRAW needed — uses Reddit's free JSON API (no credentials required).
Deduplicates against docket_history.json.
"""

import requests
import xml.etree.ElementTree as ET
import json
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DIR     = Path(__file__).parent
HISTORY_FILE = BASE_DIR / "docket_history.json"

REDDIT_SOURCES = [
    {
        "name":     "r/DungeonCourtroom",
        "url":      "https://www.reddit.com/r/DungeonCourtroom/new.json?limit=25",
        "priority": 1,
        "own_sub":  True,
    },
    {
        "name":     "r/DnD",
        "url":      "https://www.reddit.com/r/DnD/search.json?q=rules+question&sort=top&t=week&limit=25",
        "priority": 2,
        "own_sub":  False,
    },
    {
        "name":     "r/DMAcademy",
        "url":      "https://www.reddit.com/r/DMAcademy/search.json?q=rules&sort=top&t=week&limit=25",
        "priority": 3,
        "own_sub":  False,
    },
    {
        "name":     "r/dndnext",
        "url":      "https://www.reddit.com/r/dndnext/search.json?q=RAW+rules&sort=top&t=week&limit=25",
        "priority": 4,
        "own_sub":  False,
    },
]

STACKEXCHANGE_URL = "https://rpg.stackexchange.com/feeds/tag?tagnames=dnd-5e-2014&sort=newest"

HEADERS = {
    "User-Agent": "DungeonCourtroom/1.0 (automated D&D comedy channel; contact divclass01@gmail.com)"
}

MIN_UPVOTES = 5

SKIP_KEYWORDS = [
    "homebrew", "my dm", "my player", "personal", "looking for",
    "recommend", "art", "[closed]", "[duplicate]", "deleted", "removed",
    "new to", "beginner", "just started"
]

GOOD_KEYWORDS = [
    "can", "does", "do", "is ", "are ", "could", "would", "should",
    "when", "how", "what", "which", "whether", "?", " vs ", " or "
]


def load_history() -> set:
    if not HISTORY_FILE.exists():
        return set()
    try:
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            return set(json.load(f))
    except Exception:
        return set()


def save_to_history(new_titles: list):
    history = load_history()
    history.update(new_titles)
    with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(sorted(list(history)), f, indent=4)


def _is_good_question(title: str, upvotes: int = 0, own_sub: bool = False) -> bool:
    t = title.lower()
    for kw in SKIP_KEYWORDS:
        if kw in t:
            return False
    if not own_sub and upvotes < MIN_UPVOTES:
        return False
    if not any(kw in t for kw in GOOD_KEYWORDS):
        return False
    return True


def _fetch_reddit(source: dict, history: set) -> list:
    results = []
    try:
        resp = requests.get(source["url"], headers=HEADERS, timeout=15)
        resp.raise_for_status()
        posts = resp.json().get("data", {}).get("children", [])
        for post in posts:
            p = post.get("data", {})
            title   = p.get("title", "").strip()
            upvotes = p.get("ups", 0)
            link    = "https://reddit.com" + p.get("permalink", "")
            context = p.get("selftext", "")[:500].strip()
            if not title or title in history:
                continue
            if not _is_good_question(title, upvotes, source["own_sub"]):
                continue
            results.append({"title": title, "link": link,
                            "source": source["name"], "context": context})
        logging.info(f"📜 SCRAPER: {source['name']} → {len(results)} new")
    except Exception as e:
        logging.warning(f"📜 SCRAPER: {source['name']} failed: {e}")
    return results


def _fetch_stackexchange(history: set) -> list:
    results = []
    try:
        resp = requests.get(STACKEXCHANGE_URL, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        ns = {'atom': 'http://www.w3.org/2005/Atom'}
        for entry in root.findall('atom:entry', ns):
            title_el = entry.find('atom:title', ns)
            link_el  = entry.find('atom:link', ns)
            if title_el is None:
                continue
            title = title_el.text.replace("dnd-5e-2014 - ", "").strip()
            link  = link_el.get('href', '') if link_el is not None else ''
            if title in history:
                continue
            if not _is_good_question(title, upvotes=99):
                continue
            results.append({"title": title, "link": link,
                            "source": "StackExchange RPG", "context": ""})
        logging.info(f"📜 SCRAPER: StackExchange → {len(results)} new")
    except Exception as e:
        logging.warning(f"📜 SCRAPER: StackExchange failed: {e}")
    return results


def fetch_dnd_questions(max_results: int = 10) -> list:
    """
    Returns up to max_results fresh D&D questions.
    GAP 2 FIX: does NOT save to history here — caller saves after successful episode.
    Each returned dict includes a 'title' key used for dedup by the caller.
    """
    logging.info("📜 SCRAPER: Fetching questions from all sources...")
    history = load_history()
    collected = []
    seen_titles = []

    for source in REDDIT_SOURCES:
        if len(collected) >= max_results:
            break
        for q in _fetch_reddit(source, history):
            if q["title"] not in seen_titles and len(collected) < max_results:
                collected.append(q)
                seen_titles.append(q["title"])

    if len(collected) < max_results:
        for q in _fetch_stackexchange(history):
            if q["title"] not in seen_titles and len(collected) < max_results:
                collected.append(q)
                seen_titles.append(q["title"])

    # GAP 2 FIX: history is saved by main.py AFTER successful episode completion
    # NOT here at scrape time — so failed episodes can be retried

    logging.info(f"✅ SCRAPER: {len(collected)} questions ready.")
    for q in collected:
        logging.info(f"   [{q['source']}] {q['title'][:70]}")
    return collected


if __name__ == "__main__":
    questions = fetch_dnd_questions(max_results=5)
    for q in questions:
        print(f"\n[{q['source']}] {q['title']}")
