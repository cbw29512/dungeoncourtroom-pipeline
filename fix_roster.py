"""
fix_roster.py
One-time migration:
  1. Rename "Lexicon Veximus" -> "Veximus" (drop the English word "Lexicon")
  2. Rename "Zephyrax the Chaotic" -> "Zephyrax" (drop "the Chaotic")
  3. Clear episode-dir portrait paths so they get recopied to assets/images/ on next run
Run once from the videogenerator folder:  python fix_roster.py
"""
import json, shutil
from pathlib import Path

ROSTER_FILE = Path(__file__).parent / "monster_roster.json"

with open(ROSTER_FILE, 'r', encoding='utf-8') as f:
    roster = json.load(f)

changes = [
    ("Cambion",          "Lexicon Veximus",    "Veximus"),
    ("Young Blue Dragon","Zephyrax the Chaotic","Zephyrax"),
]

for monster_type, old_name, new_name in changes:
    chars = roster["monsters"].get(monster_type, [])
    for c in chars:
        if c["name"].lower() == old_name.lower():
            print(f"Renaming: {c['name']} -> {new_name}")
            c["name"] = new_name
            # Clear portrait path so it gets regenerated fresh with stable name
            c["portrait_path"] = ""

tmp = ROSTER_FILE.with_suffix(".tmp")
with open(tmp, 'w', encoding='utf-8') as f:
    json.dump(roster, f, indent=2, ensure_ascii=False)
tmp.replace(ROSTER_FILE)
print("Done. Roster saved.")
print("Next run will regenerate portraits for renamed characters.")
