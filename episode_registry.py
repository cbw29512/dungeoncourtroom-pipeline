"""
episode_registry.py
Tracks every episode produced — number, question, source, status, YouTube ID.
Never loses episode data. Append-only JSON log.
"""

import json
import logging
from pathlib import Path
from datetime import datetime

BASE_DIR       = Path(__file__).parent
REGISTRY_FILE  = BASE_DIR / "episode_registry.json"

def _load_registry() -> list:
    if not REGISTRY_FILE.exists():
        return []
    try:
        with open(REGISTRY_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return []


def _save_registry(registry: list):
    """Atomic write — temp file then rename. NEW GAP 1 FIX."""
    tmp = REGISTRY_FILE.with_suffix(".tmp")
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(registry, f, indent=4, ensure_ascii=False)
    tmp.replace(REGISTRY_FILE)


def get_next_episode_number() -> int:
    """Returns the next episode number (1-based, never reuses)."""
    registry = _load_registry()
    if not registry:
        return 1
    return max(ep["episode_number"] for ep in registry) + 1


def register_episode(question: str, source: str, episode_id: str,
                     plaintiff_title: str = "", defendant_title: str = "") -> int:
    """
    Register a new episode at the start of production.
    GAP 4 FIX: stores monster names so registry is queryable by character.
    Returns the assigned episode number.
    """
    registry = _load_registry()
    ep_num = get_next_episode_number()

    entry = {
        "episode_number":   ep_num,
        "episode_id":       episode_id,
        "question":         question,
        "source":           source,
        "plaintiff":        plaintiff_title,   # GAP 4 FIX
        "defendant":        defendant_title,   # GAP 4 FIX
        "ruling_outcome":   None,              # Set after script generation
        "status":           "rendering",
        "youtube_id":       None,
        "youtube_url":      None,
        "created_at":       datetime.now().isoformat(),
        "completed_at":     None,
    }

    registry.append(entry)
    _save_registry(registry)
    logging.info(f"📋 REGISTRY: Episode #{ep_num}: {question[:60]}")
    return ep_num


def update_ruling_outcome(episode_id: str, outcome: str):
    """Store the ruling outcome after script generation."""
    registry = _load_registry()
    for ep in registry:
        if ep["episode_id"] == episode_id:
            ep["ruling_outcome"] = outcome
            break
    _save_registry(registry)


def update_episode_cast(episode_id: str,
                        plaintiff_title: str, defendant_title: str):
    """Store plaintiff and defendant titles after casting. NEW BUG 2 FIX."""
    registry = _load_registry()
    for ep in registry:
        if ep["episode_id"] == episode_id:
            ep["plaintiff"] = plaintiff_title
            ep["defendant"] = defendant_title
            break
    _save_registry(registry)


def update_episode_status(episode_id: str, status: str,
                          youtube_id: str = None):
    """Update status and optionally the YouTube video ID."""
    registry = _load_registry()
    for ep in registry:
        if ep["episode_id"] == episode_id:
            ep["status"] = status
            if youtube_id:
                ep["youtube_id"]  = youtube_id
                ep["youtube_url"] = f"https://youtu.be/{youtube_id}"
            if status in ("complete", "failed"):
                ep["completed_at"] = datetime.now().isoformat()
            break
    _save_registry(registry)


def get_episode_summary() -> str:
    """Print-friendly summary of all episodes."""
    registry = _load_registry()
    if not registry:
        return "No episodes registered yet."

    lines = [f"\n{'='*65}",
             "  DUNGEON COURTROOM — EPISODE REGISTRY",
             f"{'='*65}"]

    for ep in sorted(registry, key=lambda x: x["episode_number"]):
        yt          = ep.get("youtube_url") or "Not uploaded"
        plaintiff   = ep.get("plaintiff", "")
        defendant   = ep.get("defendant", "")
        outcome     = ep.get("ruling_outcome", "")
        status_icon = {"complete": "[OK]", "rendering": "[..]",
                       "failed":   "[!!]"}.get(ep["status"], "[?]")

        outcome_str = {"raw":  "RAW wins", "cool": "Cool wins",
                       "draw": "DRAW"}.get(outcome, "")

        lines.append(
            f"\n  Ep #{ep['episode_number']:03d} {status_icon} [{ep['status'].upper()}]"
            f"\n    Q:  {ep['question'][:65]}"
        )
        if plaintiff and defendant:
            lines.append(
                f"    VS: {plaintiff}  vs  {defendant}"
                + (f"  [{outcome_str}]" if outcome_str else "")
            )
        lines.append(
            f"    YT: {yt}"
            f"\n    On: {ep['created_at'][:10]}"
        )

    lines.append(f"\n{'='*65}")
    lines.append(
        f"  Total: {len(registry)} | "
        f"Complete: {sum(1 for e in registry if e['status']=='complete')} | "
        f"Failed: {sum(1 for e in registry if e['status']=='failed')}"
    )
    lines.append(f"{'='*65}\n")
    return "\n".join(lines)


if __name__ == "__main__":
    print(get_episode_summary())
