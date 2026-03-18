"""
monster_roster.py
Manages the permanent monster roster for DungeonCourtroom.

Every monster character that appears on the show gets saved here forever.
When the same monster type is cast again, the existing character is reused —
same name, same portrait path, same personality, same voice.

Same-type matchups (e.g. Goblin vs Goblin) are allowed but the two
characters must have contrasting names and personalities.

roster.json structure:
{
  "monsters": {
    "Goblin": [
      {
        "name": "Zarnak",
        "type": "Goblin",
        "gender": "male",
        "personality": "manic, over-caffeinated, waves arms constantly",
        "portrait_path": "assets/images/roster_Goblin_Zarnak.png",
        "role_history": ["plaintiff", "defendant", "plaintiff"],
        "episode_history": [1, 4, 9],
        "appearances": 3,
        "created_episode": 1
      },
      {
        "name": "Squishwick",
        "type": "Goblin",
        ...
      }
    ]
  }
}
"""

import json
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DIR     = Path(__file__).parent
ROSTER_FILE  = BASE_DIR / "monster_roster.json"

# In-memory cache — loaded once per process, flushed on every save
# Minor issue fix: avoids repeated disk reads per episode
_ROSTER_CACHE: dict | None = None


# ── Roster I/O ────────────────────────────────────────────────────────────────

def _load_roster() -> dict:
    global _ROSTER_CACHE
    if _ROSTER_CACHE is not None:
        return _ROSTER_CACHE
    if not ROSTER_FILE.exists():
        _ROSTER_CACHE = {"monsters": {}}
        return _ROSTER_CACHE
    try:
        with open(ROSTER_FILE, 'r', encoding='utf-8') as f:
            _ROSTER_CACHE = json.load(f)
            return _ROSTER_CACHE
    except Exception:
        _ROSTER_CACHE = {"monsters": {}}
        return _ROSTER_CACHE


def _save_roster(roster: dict):
    global _ROSTER_CACHE
    # Atomic write: write to temp then rename (minor issue fix)
    tmp = ROSTER_FILE.with_suffix(".tmp")
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(roster, f, indent=2, ensure_ascii=False)
    tmp.replace(ROSTER_FILE)
    _ROSTER_CACHE = roster


# ── Roster lookup ─────────────────────────────────────────────────────────────

def get_portrait_path(monster_type: str, name: str) -> str:
    """
    Return the cached portrait path for a character, or "" if not set.
    GAP 1: used by vision_node to skip regeneration for returning monsters.
    """
    roster = _load_roster()
    for char in roster["monsters"].get(monster_type, []):
        if char["name"].lower() == name.lower():
            return char.get("portrait_path", "")
    return ""


def get_existing_character(monster_type: str,
                           exclude_names: list = None) -> dict | None:
    """
    Return an existing roster character of this monster type.
    exclude_names: names already cast this episode (for same-type matchups).
    Returns None if no character exists yet for this type.
    """
    roster = _load_roster()
    chars  = roster["monsters"].get(monster_type, [])

    if not chars:
        return None

    exclude = [n.lower() for n in (exclude_names or [])]

    # Filter out already-cast characters (same-type matchup protection)
    available = [c for c in chars if c["name"].lower() not in exclude]

    if not available:
        # All characters of this type are already cast — return any
        # (shouldn't happen with 2-party system but safety net)
        available = chars

    # Pick the one with fewest appearances for variety
    available.sort(key=lambda c: c.get("appearances", 0))
    return available[0]


def register_character(monster_type: str, name: str, gender: str,
                       personality: str, portrait_path: str,
                       role: str, episode_num: int) -> dict:
    """
    Add a brand new character to the roster.
    Returns the new character dict.
    """
    roster = _load_roster()

    if monster_type not in roster["monsters"]:
        roster["monsters"][monster_type] = []

    character = {
        "name":            name,
        "type":            monster_type,
        "gender":          gender,
        "personality":     personality,
        "portrait_path":   portrait_path,
        "role_history":    [role],
        "episode_history": [episode_num],
        "appearances":     1,
        "created_episode": episode_num,
    }

    roster["monsters"][monster_type].append(character)
    _save_roster(roster)
    logging.info(f"📋 ROSTER: New character saved — {name} the {monster_type}")
    return character


def record_appearance(monster_type: str, name: str,
                      role: str, episode_num: int):
    """Record that an existing character appeared in a new episode."""
    roster = _load_roster()
    chars  = roster["monsters"].get(monster_type, [])

    for char in chars:
        if char["name"].lower() == name.lower():
            char["appearances"]     = char.get("appearances", 0) + 1
            char["role_history"]    = char.get("role_history", []) + [role]
            char["episode_history"] = char.get("episode_history", []) + [episode_num]
            break

    _save_roster(roster)


def update_portrait_path(monster_type: str, name: str, portrait_path: str):
    """Update portrait path after generation."""
    roster = _load_roster()
    for char in roster["monsters"].get(monster_type, []):
        if char["name"].lower() == name.lower():
            char["portrait_path"] = portrait_path
            break
    _save_roster(roster)


def get_roster_summary() -> str:
    """Human-readable roster summary."""
    roster = _load_roster()
    monsters = roster.get("monsters", {})
    if not monsters:
        return "Roster is empty — no recurring characters yet."

    total_chars = sum(len(v) for v in monsters.values())
    lines = [
        f"\n{'='*55}",
        f"  DUNGEON COURTROOM — MONSTER ROSTER ({total_chars} characters)",
        f"{'='*55}",
    ]
    for mtype, chars in sorted(monsters.items()):
        lines.append(f"\n  {mtype.upper()}:")
        for c in chars:
            eps = c.get("episode_history", [])
            ep_str = ", ".join(f"#{e}" for e in eps[:5])
            if len(eps) > 5:
                ep_str += f" +{len(eps)-5} more"
            lines.append(
                f"    {c['name']} ({c['gender']}) — {c.get('appearances',1)} appearance(s)"
                f"\n      Episodes: {ep_str}"
                f"\n      Personality: {c.get('personality','?')[:60]}"
            )
    lines.append(f"\n{'='*55}\n")
    return "\n".join(lines)


if __name__ == "__main__":
    print(get_roster_summary())
