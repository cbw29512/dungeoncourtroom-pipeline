"""
script_node.py
Generates the full episode script using Ollama (qwen2.5:14b).

CAST STRUCTURE:
  FIXED (never change):
    - judge   : Judge Aldric Bonecrusher — randomly weighted ruling
    - bailiff : Bailiff Crunch — announces, introduces, reacts

  VARIABLE (changes every episode — SRD monsters):
    - plaintiff : RULES LAWYER — always cites the Player's Handbook and Dungeon Master's Guide, argues the rules as written
    - defendant : RULE OF COOL — never cites books, argues fun/story/memorable moments

RULING WEIGHTS (random per episode, decided before ruling act):
  60% — Rules as Written  (plaintiff wins)
  30% — Rule of Cool      (defendant wins)
  10% — DRAW              (judge asks the AUDIENCE to decide)

MEMORY MODEL:
  Every Ollama call receives the full CASE FILE:
    - question, source, context
    - full cast with names, types, roles
    - ALL panels already written
  The ruling act sees every argument made and rules consistently.
"""

import random
import requests
import json
import logging
from pathlib import Path
from typing import Optional

from monster_roster import (get_existing_character, register_character,
                             record_appearance)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

OLLAMA_MODEL = "qwen2.5:14b"

# ── Fixed cast — NEVER changes ────────────────────────────────────────────────
# ── Argument angles — randomly selected each episode ──────────────────────────
# These force different arguments even for the same question across episodes.
# Each episode picks one plaintiff angle and one defendant angle at random.
PLAINTIFF_ANGLES = [
    "Focus on the MECHANICAL consequences — what breaks in the rules system if this is allowed.",
    "Focus on PRECEDENT — if this is allowed, every player will try to abuse it.",
    "Focus on INTENT — the designers wrote this rule for a reason. What was that reason?",
    "Focus on the OATH itself — what does the sacred text of the oath actually say, word for word?",
    "Focus on GAME BALANCE — this would make the class trivially easy to play.",
    "Focus on CONSISTENCY — the same rule applies to every other class, why not this one?",
]

DEFENDANT_ANGLES = [
    "Focus on the STORY — what narrative does each outcome create? Which is more memorable?",
    "Focus on the PLAYER — what does the player feel in each outcome? Which creates joy?",
    "Focus on REAL TABLES — what do actual DMs at real tables actually do? Survey says: fun wins.",
    "Focus on the CHARACTER — what would THIS specific Paladin actually do? Rules ignore character.",
    "Focus on the SPIRIT of the class — Paladins exist to be heroic, not lawyerly.",
    "Focus on the DM — the DM is the final authority, not a rulebook. Always.",
]

import random as _random

def _pick_argument_angles() -> tuple:
    """Pick random distinct argument angles for this episode."""
    p_angle = _random.choice(PLAINTIFF_ANGLES)
    d_angle = _random.choice(DEFENDANT_ANGLES)
    return p_angle, d_angle


FIXED_CAST = {
    "judge": {
        "display_name": "JUDGE ALDRIC BONECRUSHER",
        "title":        "The Honorable Judge Aldric Bonecrusher",
        "desc":         "Ancient lich judge. Dry wit. Has seen every rules argument in existence. Done with everyone. Delivers crushing verdicts.",
        "voice":        "en_US-bryce-medium.onnx",
    },
    "bailiff": {
        "display_name": "BAILIFF CRUNCH",
        "title":        "Bailiff Crunch",
        "desc":         "Ogre bailiff. Enormously enthusiastic. Completely loyal. Occasionally misunderstands everything. Loves his job more than life itself.",
        "voice":        "en_US-lessac-medium.onnx",
    },
}

# ── SRD 5.1 monster pool — all copyright-safe ────────────────────────────────
SRD_MONSTERS = [
    "Goblin","Hobgoblin","Bugbear","Kobold","Orc","Gnoll",
    "Lizardfolk","Merfolk","Harpy","Kenku","Yuan-ti Pureblood",
    "Skeleton","Zombie","Ghoul","Ghost","Wight","Wraith",
    "Specter","Banshee","Vampire Spawn",
    "Owlbear","Basilisk","Cockatrice","Manticore","Griffon",
    "Hippogriff","Peryton","Wyvern","Chimera",
    "Displacer Beast","Rust Monster","Gelatinous Cube",
    "Ogre","Troll","Hill Giant","Stone Giant","Fire Giant",
    "Frost Giant","Cloud Giant",
    "Young Red Dragon","Young Blue Dragon","Young Green Dragon",
    "Young White Dragon","Young Black Dragon",
    "Air Elemental","Fire Elemental","Water Elemental","Earth Elemental",
    "Gargoyle","Mimic","Minotaur","Centaur","Satyr",
    "Medusa","Naga","Hydra","Ettin",
    "Imp","Quasit","Cambion","Barbed Devil","Chain Devil",
    "Dryad","Sprite","Green Hag","Sea Hag",
]

# ── Ruling outcome — rolled once per episode ──────────────────────────────────
RULING_OUTCOMES = [
    ("raw",  0.60),   # Rules as Written — plaintiff wins
    ("cool", 0.30),   # Rule of Cool     — defendant wins
    ("draw", 0.10),   # Hung court       — audience decides
]

def _roll_ruling() -> str:
    roll = random.random()
    cumulative = 0.0
    for outcome, weight in RULING_OUTCOMES:
        cumulative += weight
        if roll < cumulative:
            return outcome
    return "raw"


# ── Ruling instructions per outcome ──────────────────────────────────────────
RULING_INSTRUCTIONS = {
    "raw": """\
THE RULING — RULES AS WRITTEN WINS (plaintiff wins this one):
Panel 1: Judge silences both parties. Briefly acknowledges BOTH made legitimate points.
  The judge is NEUTRAL and fair — credit where due before ruling.
Panel 2: Judge explains why, despite the fun argument, the rules as written are clear here.
  Reference the specific rule that applies. Not dismissive of fun — just honest about the text.
Panel 3: Judge rules IN FAVOR of {plaintiff_name}. One mic-drop line. Gavel slam.
Panel 4: Bailiff reacts with enormous enthusiasm and completely misunderstands what just happened.
Speakers: judge (1-3), bailiff (4).""",

    "cool": """\
THE RULING — RULE OF COOL WINS (defendant wins this one):
Panel 1: Judge silences both parties. Briefly acknowledges BOTH made legitimate points.
  The judge is NEUTRAL and fair — credit where due before ruling.
Panel 2: Judge explains why, despite correct rules citations, the spirit of the game matters more here.
  Not dismissive of rules — just honest that D&D is a game, not a legal document.
Panel 3: Judge rules IN FAVOR of {defendant_name}. One mic-drop line. Gavel slam.
Panel 4: Bailiff reacts with enormous enthusiasm and completely misunderstands what just happened.
Speakers: judge (1-3), bailiff (4).""",

    "draw": """\
THE RULING — HUNG COURT (draw — audience decides):
Panel 1: Judge slams gavel. "Both arguments are simultaneously correct and an affront to my intelligence."
Panel 2: Judge explains that {plaintiff_name} made a valid rules-as-written case AND {defendant_name} made a valid Rule of Cool case.
  The court cannot decide — this is unprecedented.
Panel 3: Judge points DIRECTLY AT THE AUDIENCE (break the fourth wall).
  "This court is deadlocked. YOU — watching this — must decide. Comment your verdict below."
  Make it dramatic and funny.
Panel 4: Bailiff enthusiastically asks the audience to like and subscribe, missing the gravity entirely.
Speakers: judge (1-3), bailiff (4).""",
}


# ── Act instructions ──────────────────────────────────────────────────────────
ACT_INSTRUCTIONS = {
    "casting": """\
You are the casting director for DUNGEON COURTROOM, a D&D comedy comic series.
Choose two SRD monsters that would be THEMATICALLY FUNNY or RELEVANT as plaintiff and defendant.
Give each a funny unique D&D-style name.

CASE QUESTION: "{question}"
AVAILABLE MONSTERS: {monster_list}

CHARACTER ROLES — this is permanent and must match the monster's nature:
- PLAINTIFF = the RULES LAWYER. Pick a monster whose nature suits rigid rule-following
  (e.g. a Lich suits death rules, a Kobold suits trap rules, a Devil suits contract rules).
- DEFENDANT = the RULE OF COOL advocate. Pick a monster whose nature suits chaos/fun/creativity
  (e.g. a Mimic suits deception rules, a Dragon suits "because I said so" rules).

Names should be funny: "Zarnak", "Mrrg", "Blobbus", "Squishwick", "Grumph", "Vex", etc.

Return ONLY this JSON:
{{
  "plaintiff_type": "monster type from the list",
  "plaintiff_name": "funny name",
  "defendant_type": "monster type from the list",
  "defendant_name": "funny name"
}}""",

    "opening": """\
Write the OPENING: exactly 3 panels.

Panel 1: Bailiff calls court to order — loud, dramatic, over-the-top enthusiastic.
Panel 2: Bailiff formally announces the full case:
  "The case of {plaintiff_name} the {plaintiff_type} versus {defendant_name} the {defendant_type}!
   The question before this court: {question}!
   Presiding: The Honorable Judge Aldric Bonecrusher!"
  Use the EXACT names and types. Bailiff is genuinely thrilled about this.
Panel 3: Judge calls order. IMPORTANT — the judge is a NEUTRAL ARBITRATOR.
  One dry, world-weary line acknowledging the question WITHOUT taking sides.
  The judge finds both parties equally exhausting. No opinion on the outcome.
  Just barely suppressed ancient disdain for everyone in the room.
  Bad example: "Another pointless debate" (too generic)
  Good example: "Another day, another Paladin creating a paperwork nightmare."

Speakers: bailiff (panels 1-2), judge (panel 3).""",

    "prosecution": """\
Write the PROSECUTION: exactly 5 panels. Build ONE escalating argument, not 5 separate points.

{plaintiff_name} the {plaintiff_type} is the PLAINTIFF — a RULES LAWYER.
CHARACTER: Monster nature flavors HOW they argue:
  Devil = contract lawyer | Kobold = manic energy | Cambion = cold superiority
  Lich = ancient authority | Dragon = arrogant entitlement | default = rigid pedantry
VOICE: Starts confident. Gets more frustrated and personally offended as panels progress.

THIS EPISODE'S ARGUMENT ANGLE: {plaintiff_angle}
Build ALL 5 panels around this angle. This is what makes this episode feel different.

ARGUMENT STRUCTURE:
  Panel 1: Establish position through your angle. Be specific to: {question}
  Panel 2: Cite a specific rulebook + FAKE plausible page number. Quote it as if it ends debate.
  Panel 3: Through your angle, destroy the "but fun!" defense before it's made. Dismissive.
  Panel 4: Consequences. What breaks if the rule is ignored? Make it sound catastrophic.
  Panel 5: Close. Devastating. Self-satisfied. Sit down like you've already won.

RULES:
  - Full book names: "the Player's Handbook", "the Dungeon Master's Guide" — NEVER PHB/DMG/XGE
  - Panels must chain together — ONE argument, escalating
  - Max 30 words per panel text. Action max 8 words.

Speaker: plaintiff only. 5 panels.""",

    "defense": """\
Write the DEFENSE: exactly 5 panels. Directly dismantle the prosecution's argument.

{defendant_name} the {defendant_type} is the DEFENDANT — a RULE OF COOL advocate.
CHARACTER: Never cites a rulebook. Ever.
  Dragon = arrogant royalty | Mimic = mocks plaintiff's words back wrong
  Green Dragon = dripping sarcasm | default = relaxed, amused dismissal
VOICE: Starts relaxed and unbothered. Gets more passionate as panels progress.

THIS EPISODE'S ARGUMENT ANGLE: {defendant_angle}
Build ALL 5 panels around this angle while countering what {plaintiff_name} argued.

ARGUMENT STRUCTURE — reference STORY SO FAR:
  Panel 1: Dismiss the prosecution's angle with one line through YOUR angle.
  Panel 2: Take the EXACT rulebook quote {plaintiff_name} used — reframe it to support YOUR side.
  Panel 3: "Imagine you're at the table..." — paint a vivid scene through your angle.
  Panel 4: The consequences {plaintiff_name} described? Explain why that outcome is WORSE.
  Panel 5: Close. Passionate. The table WOULD cheer. Sit back like this was never a contest.

RULES:
  - Never cite a rulebook. Story and feeling only.
  - Panels must chain together — ONE argument
  - Max 30 words per panel text. Action max 8 words.

Speaker: defendant only. 5 panels.""",

    "dream": """\
Write the DREAM SEQUENCE: exactly 2 panels.

The judge zones out mid-session and daydreams about D&D lore.
THE DREAM MUST BE DIRECTLY ABOUT THIS SPECIFIC QUESTION: {question}
Not a general D&D fact — something that feels like apocryphal lore ABOUT THIS EXACT RULING.

Make it absurd but lore-accurate-sounding. Examples of tone:
  "In 1987, Gary Gygax ruled that a Paladin who lied to protect the innocent..."
  "In the original Greyhawk setting, there was a famous Paladin case known as..."
  "The 3rd Edition errata specifically addressed the question of Paladins and deception..."

Each panel should feel like a lost footnote from D&D history, directly relevant.
Speaker: narrator only. Emotion: dreaming. 2 panels.""",


    "crossexam": """\
Write CROSS EXAMINATION: exactly 4 panels. Most heated moment — they go personal.

{plaintiff_name} and {defendant_name} argue DIRECTLY at each other. Not to the judge.
React to SPECIFIC things each said earlier (see STORY SO FAR).

STRUCTURE — strictly plaintiff, defendant, plaintiff, defendant:
  Panel 1 ({plaintiff_name}): Attack a SPECIFIC thing {defendant_name} said. Quote it back twisted.
    OFFENDED that feelings were cited as evidence. Personal.
  Panel 2 ({defendant_name}): Throw it back. Mock the citation. Ask something unanswerable.
    Smug. Finds this entire argument hilarious.
  Panel 3 ({plaintiff_name}): Escalate. Accuse {defendant_name} of not caring about the game.
    Becoming slightly unhinged. This is their life's work.
  Panel 4 ({defendant_name}): Short. Devastating comeback. Completely unbothered.

RULES:
  - Must reference SPECIFIC earlier dialogue — no generic statements
  - Max 25 words per panel. Action max 8 words.

Speakers: plaintiff and defendant only. 4 panels.""",

    # Ruling text is set dynamically based on the rolled outcome
    "ruling": None,
}
# NOTE: "casting" is handled separately by _select_monsters() — not in this loop


def find_ollama_port() -> Optional[int]:
    """Scan common Ollama ports and return the first one that responds."""
    for port in [11434, 4242, 11435, 8080]:
        try:
            r = requests.get(f"http://localhost:{port}/api/tags", timeout=3)
            if r.status_code == 200:
                logging.info(f"✅ OLLAMA: Found on port {port}")
                return port
        except Exception:
            continue
    return None


def _scrub_acronyms(text: str) -> str:
    """
    Replace D&D acronyms with full names regardless of what Ollama produces.
    Applied to every panel text before it enters the pipeline.
    """
    replacements = [
        # Ordered longest-first to avoid partial replacements
        ("Xanathar's Guide to Everything", "Xanathar's Guide to Everything"),  # already fine
        ("Tasha's Cauldron of Everything", "Tasha's Cauldron of Everything"),  # already fine
        ("Player's Handbook",              "Player's Handbook"),               # already fine
        ("Dungeon Master's Guide",         "Dungeon Master's Guide"),          # already fine
        ("Monster Manual",                 "Monster Manual"),                  # already fine
        # Acronyms to replace
        ("XGtE",  "Xanathar's Guide to Everything"),
        ("TCoE",  "Tasha's Cauldron of Everything"),
        (" XGE ", " Xanathar's Guide to Everything "),
        (" TCE ", " Tasha's Cauldron of Everything "),
        ("XGE ",  "Xanathar's Guide to Everything "),
        ("TCE ",  "Tasha's Cauldron of Everything "),
        (" XGE",  " Xanathar's Guide to Everything"),
        (" TCE",  " Tasha's Cauldron of Everything"),
        (" PHB ", " the Player's Handbook "),
        (" DMG ", " the Dungeon Master's Guide "),
        (" MM ",  " the Monster Manual "),
        ("PHB ",  "the Player's Handbook "),
        ("DMG ",  "the Dungeon Master's Guide "),
        ("MM ",   "the Monster Manual "),
        (" PHB",  " the Player's Handbook"),
        (" DMG",  " the Dungeon Master's Guide"),
        (" RAW ", " the rules as written "),
        (" RAI ", " the rules as intended "),
        ("RAW ",  "the rules as written "),
        ("RAI ",  "the rules as intended "),
        (" RAW",  " the rules as written"),
        (" RAI",  " the rules as intended"),
    ]
    for acronym, full in replacements:
        text = text.replace(acronym, full)
    return text


def _scrub_panels(panels: list) -> list:
    """Apply acronym scrub + remove panels with empty text (prevents TTS crash)."""
    clean = []
    for panel in panels:
        if "text" in panel:
            panel["text"] = _scrub_acronyms(panel["text"].strip())
        if "action" in panel:
            panel["action"] = _scrub_acronyms(panel["action"].strip())
        # Skip panels with no speakable text — edge-tts crashes on empty strings
        if panel.get("text", "").strip():
            clean.append(panel)
        else:
            logging.warning(f"   [scrub] Dropped empty-text panel: {panel.get('speaker','?')}")
    return clean


def _call_ollama(port: int, prompt: str, act_name: str,
                 max_tokens: int = 1500) -> Optional[dict]:
    url = f"http://localhost:{port}/api/generate"
    payload = {
        "model":  OLLAMA_MODEL,
        "prompt": prompt,
        "format": "json",
        "stream": False,
        "options": {"temperature": 0.82, "num_predict": max_tokens, "num_ctx": 8192},
    }
    try:
        r = requests.post(url, json=payload, timeout=180)
        r.raise_for_status()
        return json.loads(r.json().get("response", "").strip())
    except json.JSONDecodeError as e:
        logging.warning(f"⚠️  [{act_name}] JSON decode failed: {e}")
        return None
    except Exception as e:
        logging.warning(f"⚠️  [{act_name}] Ollama call failed: {e}")
        return None


def _unwrap_panels(data, act_name: str) -> list:
    if isinstance(data, dict):
        for key in ("panels","script","lines","dialogue"):
            if key in data and isinstance(data[key], list):
                data = data[key]
                break
        else:
            if "speaker" in data:
                data = [data]
            else:
                logging.warning(f"⚠️  [{act_name}] Unexpected shape: {list(data.keys())}")
                return []
    if not isinstance(data, list):
        return []

    valid_emotions = {"neutral","angry","smug","shocked","tired","excited","dreaming"}
    results = []
    for p in data:
        if not isinstance(p, dict):
            continue
        if not all(k in p for k in ("speaker","emotion","text","action")):
            continue
        p["speaker"] = str(p["speaker"]).lower().strip()
        p["emotion"] = str(p["emotion"]).lower().strip()
        if p["emotion"] not in valid_emotions:
            p["emotion"] = "neutral"
        p["text"]   = str(p["text"]).strip()
        p["action"] = str(p["action"]).strip()
        if p["text"]:
            results.append(p)
    return results


def _normalise_speaker(panel: dict, vc: dict) -> dict:
    sp = panel["speaker"].lower().strip()
    pk = vc["plaintiff"]["key"]
    dk = vc["defendant"]["key"]
    pn = vc["plaintiff"]["name"].lower()
    dn = vc["defendant"]["name"].lower()
    pt = vc["plaintiff"]["type"].lower()
    dt = vc["defendant"]["type"].lower()

    if sp in ("plaintiff", pk, pn, pt, pn.split()[0]):
        panel["speaker"] = pk
    elif sp in ("defendant", dk, dn, dt, dn.split()[0]):
        panel["speaker"] = dk
    elif sp not in ("judge","bailiff","narrator"):
        panel["speaker"] = "judge"
    return panel


def _build_case_file(state: dict) -> str:
    vc = state.get("variable_cast", {})
    p  = vc.get("plaintiff", {})
    d  = vc.get("defendant", {})

    lines = [
        "=" * 60,
        "DUNGEON COURTROOM — FULL CASE FILE",
        "=" * 60,
        f"CASE QUESTION: {state['question']}",
        f"SOURCE: {state['source']}",
    ]
    if state.get("context"):
        lines.append(f"CONTEXT: {state['context']}")

    lines += [
        "",
        "FIXED CAST (same every episode):",
        "  JUDGE ALDRIC BONECRUSHER: Ancient lich. Dry wit. Done with everyone.",
        "  BAILIFF CRUNCH: Ogre. Enthusiastic. Misunderstands things.",
        "",
        "THIS EPISODE'S CAST:",
    ]
    if p:
        lines.append(f"  PLAINTIFF: {p['name']} the {p['type']} (speaker key: \"{p['key']}\")")
        lines.append(f"    Role: RULES LAWYER — argues rules as written, cites full rulebook names")
        lines.append(f"    Position: argues the strict interpretation does NOT allow the fun option")
    if d:
        lines.append(f"  DEFENDANT: {d['name']} the {d['type']} (speaker key: \"{d['key']}\")")
        lines.append(f"    Role: RULE OF COOL — never cites books, argues fun/story/memorable moments")
        lines.append(f"    Position: argues the Rule of Cool DOES allow / supports the fun option")

    # Add ruling outcome hint for ruling act
    outcome = state.get("ruling_outcome")
    if outcome:
        outcome_desc = {
            "raw":  "RULES AS WRITTEN wins this episode (plaintiff wins)",
            "cool": "RULE OF COOL wins this episode (defendant wins)",
            "draw": "HUNG COURT — draw, audience decides",
        }.get(outcome, "")
        lines.append(f"\nRULING OUTCOME (already decided): {outcome_desc}")

    prior = state.get("panels", [])
    if prior:
        name_map = {
            "judge":   "JUDGE BONECRUSHER",
            "bailiff": "BAILIFF CRUNCH",
            "narrator":"NARRATOR",
            p.get("key","plaintiff"): f"{p.get('name','?')} THE {p.get('type','?').upper()}",
            d.get("key","defendant"): f"{d.get('name','?')} THE {d.get('type','?').upper()}",
        }
        lines += [
            "",
            f"STORY SO FAR ({len(prior)} panels):",
            "DO NOT repeat dialogue. Ruling MUST reference specific arguments below by name.",
            "",
        ]
        for i, panel in enumerate(prior, 1):
            name = name_map.get(panel["speaker"], panel["speaker"].upper())
            lines.append(f"  [{i:02d}] {name} ({panel['emotion']}): "
                         f"\"{panel['text']}\" — {panel['action']}")
    else:
        lines.append("\nSTORY SO FAR: No panels written yet.")

    lines += ["", "=" * 60, "YOUR TASK FOR THIS ACT:"]
    return "\n".join(lines)


def _generate_new_character(port: int, monster_type: str, role: str,
                             question: str, existing_name: str = None) -> dict:
    """Ask Ollama to create a new character. Returns name/gender/personality dict."""
    role_desc = {
        "plaintiff": "a rules lawyer who obsessively cites rulebooks and hates fun",
        "defendant": "a Rule of Cool advocate who never cites books and loves chaos",
    }.get(role, "a courtroom character")

    contrast = ""
    if existing_name:
        contrast = (f"\nIMPORTANT: A {monster_type} named '{existing_name}' already exists. "
                    f"This new character MUST have an obviously contrasting name and personality.")

    prompt = f"""Create a unique recurring character for DUNGEON COURTROOM, a D&D comedy show.
Monster type: {monster_type}
Role: {role_desc}
Case: {question[:80]}{contrast}

Give them a funny fantasy monster name, a gender, and a one-sentence personality.

NAME RULES — the name must:
- Sound like a fantasy monster name (e.g. Zarnak, Grumble, Vexis, Thrax, Snorgle, Blarg)
- NOT be an English word or dictionary term (e.g. NOT "Lexicon", "Codex", "Axiom", "Chaos")
- NOT describe their job or personality literally
- Be 1-2 words maximum, easy to say aloud

Return ONLY:
{{"name": "fantasy monster name", "gender": "male or female", "personality": "one sentence"}}"""

    data = _call_ollama(port, prompt, "char_gen", max_tokens=150)
    if isinstance(data, dict) and all(k in data for k in ("name","gender","personality")):
        name = data["name"].strip()
        # Reject names that are plain English dictionary words
        english_words = {
            "lexicon","codex","axiom","chaos","order","law","rule","judge","scale",
            "balance","truth","honor","glory","valor","justice","wrath","rage",
            "void","shadow","flame","frost","storm","thunder","arcane","mystic",
        }
        if name.lower().split()[0] in english_words:
            import random as _r
            fallback = ["Zarnak","Grumble","Vexis","Thrax","Snorgle","Blarg",
                        "Mrrg","Squishwick","Fangrot","Durble","Krix","Wobble"]
            name = _r.choice(fallback)
        return {
            "name":        name,
            "gender":      data["gender"].strip().lower(),
            "personality": data["personality"].strip(),
        }

    import random as _r
    p_names = ["Zarnak","Fangrot","Krix","Rulwick","Durble","Grimthorn"]
    d_names = ["Blobbus","Squishwick","Mrrg","Wobble","Snorgle","Thrax"]
    return {
        "name":        _r.choice(p_names if role=="plaintiff" else d_names),
        "gender":      _r.choice(["male","female"]),
        "personality": ("Obsessively cites page numbers, physically uncomfortable when rules are bent."
                        if role=="plaintiff" else
                        "Cannot name a single rulebook, has never stopped smiling."),
    }


def _select_monsters(port: int, question: str, episode_num: int) -> dict:
    """
    Select plaintiff and defendant using the persistent monster roster.
    Reuses existing characters when the same monster type is cast again.
    Creates new characters and saves them forever when a new type appears.
    """
    # Step 1: Pick monster types thematically
    monster_list = ", ".join(SRD_MONSTERS)
    type_prompt = f"""You are casting director for DUNGEON COURTROOM, a D&D comedy show.
Pick two SRD monster types that are thematically funny for this case.

CASE: "{question}"
AVAILABLE: {monster_list}

PLAINTIFF = Rules Lawyer (suits rigid rule-following — Devils, Liches, Kobolds work well)
DEFENDANT = Rule of Cool (suits chaos/creativity — Mimics, Dragons, Gremlins work well)

Return ONLY: {{"plaintiff_type": "type", "defendant_type": "type"}}"""

    logging.info("   [casting] Selecting monster types...")
    type_data = _call_ollama(port, type_prompt, "type_sel", max_tokens=80)

    srd_map = {m.lower(): m for m in SRD_MONSTERS}
    pt = "Kobold"
    dt = "Mimic"
    if isinstance(type_data, dict):
        raw_pt = type_data.get("plaintiff_type","").strip().lower()
        raw_dt = type_data.get("defendant_type","").strip().lower()
        if raw_pt in srd_map: pt = srd_map[raw_pt]
        if raw_dt in srd_map: dt = srd_map[raw_dt]

    logging.info(f"   [casting] Types chosen: {pt} vs {dt}")
    same_type = (pt.lower() == dt.lower())

    # Step 2: Get or create plaintiff
    p_existing = get_existing_character(pt, exclude_names=[])
    if p_existing:
        logging.info(f"   [casting] RETURNING {p_existing['name']} the {pt} "
                     f"({p_existing.get('appearances',1)} prior appearances)")
        record_appearance(pt, p_existing["name"], "plaintiff", episode_num)
        p_char = p_existing
    else:
        logging.info(f"   [casting] NEW character for {pt}...")
        new_p  = _generate_new_character(port, pt, "plaintiff", question)
        p_char = register_character(
            monster_type=pt, name=new_p["name"], gender=new_p["gender"],
            personality=new_p["personality"], portrait_path="",
            role="plaintiff", episode_num=episode_num,
        )
        p_char.update(new_p)

    # Step 3: Get or create defendant
    # Same-type matchup: exclude plaintiff name so we get a contrasting character
    exclude_d = [p_char["name"]] if same_type else []
    d_existing = get_existing_character(dt, exclude_names=exclude_d)
    if d_existing:
        logging.info(f"   [casting] RETURNING {d_existing['name']} the {dt} "
                     f"({d_existing.get('appearances',1)} prior appearances)")
        record_appearance(dt, d_existing["name"], "defendant", episode_num)
        d_char = d_existing
    else:
        contrast = p_char["name"] if same_type else None
        logging.info(f"   [casting] NEW character for {dt}"
                     + (f" (contrasting with {contrast})" if contrast else "") + "...")
        new_d  = _generate_new_character(port, dt, "defendant", question,
                                          existing_name=contrast)
        d_char = register_character(
            monster_type=dt, name=new_d["name"], gender=new_d["gender"],
            personality=new_d["personality"], portrait_path="",
            role="defendant", episode_num=episode_num,
        )
        d_char.update(new_d)

    logging.info(f"   [casting] CAST LOCKED:")
    logging.info(f"     P: {p_char['name']} the {pt} — {p_char.get('personality','')[:55]}")
    logging.info(f"     D: {d_char['name']} the {dt} — {d_char.get('personality','')[:55]}")

    return {
        "plaintiff": {
            "key": "plaintiff", "name": p_char["name"], "type": pt,
            "gender": p_char.get("gender","unknown"),
            "personality": p_char.get("personality",""),
            "display_name": f"{p_char['name'].upper()} THE {pt.upper()}",
            "title": f"{p_char['name']} the {pt} — Plaintiff",
            "voice": "en_US-joe-medium.onnx",
        },
        "defendant": {
            "key": "defendant", "name": d_char["name"], "type": dt,
            "gender": d_char.get("gender","unknown"),
            "personality": d_char.get("personality",""),
            "display_name": f"{d_char['name'].upper()} THE {dt.upper()}",
            "title": f"{d_char['name']} the {dt} — Defendant",
            "voice": "en_GB-southern_english_female-low.onnx",
        },
    }


def generate_episode_script(state: dict) -> list:
    """
    Generate full episode script. Updates state throughout.
    Rolls ruling outcome at start — baked into the ruling act prompt.
    """
    port = find_ollama_port()
    if port is None:
        logging.error("❌ SCRIPT: Ollama not found. Using fallback.")
        vc = _default_variable_cast()
        state["variable_cast"]  = vc
        state["ruling_outcome"] = _roll_ruling()
        panels = _fallback_script(state["question"], vc, state["ruling_outcome"])
        state["panels"] = panels
        return panels

    question    = state["question"]
    episode_num = state.get("episode_number", 1)
    logging.info(f"🧠 SCRIPT: '{question[:65]}'")

    # Roll ruling outcome NOW — before any generation
    outcome = _roll_ruling()
    state["ruling_outcome"] = outcome
    logging.info(f"   [ruling] Outcome rolled: {outcome.upper()} "
                 f"({'Rules as Written' if outcome=='raw' else 'Rule of Cool' if outcome=='cool' else 'DRAW - audience decides'})")

    # Select monsters via roster system
    vc = _select_monsters(port, question, episode_num)
    state["variable_cast"] = vc
    if "panels" not in state:
        state["panels"] = []

    p   = vc["plaintiff"]
    d   = vc["defendant"]
    # Pick random argument angles — forces variety across episodes on same question
    p_angle, d_angle = _pick_argument_angles()
    logging.info(f"   [angles] P: {p_angle[:60]}...")
    logging.info(f"   [angles] D: {d_angle[:60]}...")

    fmt = {
        "question":        question,
        "plaintiff_name":  p["name"],
        "plaintiff_type":  p["type"],
        "defendant_name":  d["name"],
        "defendant_type":  d["type"],
        "plaintiff_angle": p_angle,
        "defendant_angle": d_angle,
    }

    for act_name in ACT_INSTRUCTIONS:
        if act_name == "casting":
            continue

        # Get act instructions — ruling is dynamic based on outcome
        if act_name == "ruling":
            instructions = RULING_INSTRUCTIONS[outcome].format(**fmt)
        else:
            instructions = ACT_INSTRUCTIONS[act_name].format(**fmt)

        case_file = _build_case_file(state)
        prompt = f"""{case_file}
{instructions}

TONE: Absurdist D&D courtroom comedy. Characters take this VERY seriously.
WRITING RULES:
  - Text max 30 words per panel (crossexam: 25). Action max 8 words.
  - Dialogue must be followable by a complete newcomer to D&D
  - Each act is ONE flowing argument — panels build on each other
  - NEVER use acronyms: PHB, DMG, MM, XGE, TCE, RAW, RAI
  - Always say the FULL book name: "the Player's Handbook", "the Monster Manual"
EMOTION RULES — do NOT default to neutral:
  - plaintiff citing rules: smug. plaintiff challenged: angry or shocked.
  - defendant mocking rules: smug. defendant arguing: excited.
  - judge neutral observation: tired. judge losing patience: angry.
  - bailiff: excited or shocked. narrator: dreaming.
  - "neutral" only for calm factual statements.
Speaker keys: "judge", "bailiff", "narrator", "{p['key']}", "{d['key']}"

Return ONLY: {{"panels": [{{"speaker":"...","emotion":"...","text":"...","action":"..."}}]}}"""

        logging.info(f"   [{act_name}] Calling Ollama "
                     f"(context: {len(state['panels'])} prior panels)...")

        data   = _call_ollama(port, prompt, act_name)
        panels = _unwrap_panels(data, act_name) if data is not None else []
        panels = [_normalise_speaker(pnl, vc) for pnl in panels]

        # Per-act minimum panel counts — short responses use fallback
        ACT_MIN = {"opening": 3, "prosecution": 4, "defense": 4,
                   "dream": 2, "crossexam": 3, "ruling": 3}
        min_required = ACT_MIN.get(act_name, 2)

        if len(panels) >= min_required:
            panels = _scrub_panels(panels)
            state["panels"].extend(panels)
            logging.info(f"   [{act_name}] {len(panels)} panels "
                         f"(total: {len(state['panels'])})")
        else:
            if panels:
                logging.warning(f"   [{act_name}] Only {len(panels)} panels "
                                f"(min {min_required}) — using fallback")
            else:
                logging.warning(f"   [{act_name}] Failed — using fallback act")
            fb = _scrub_panels(_fallback_act(act_name, question, vc, outcome))
            state["panels"].extend(fb)
            logging.info(f"   [{act_name}] Fallback: {len(fb)} panels")

    total = len(state["panels"])
    if total < 8:
        logging.warning(f"⚠️  SCRIPT: Only {total} panels — full fallback.")
        panels = _fallback_script(question, vc, outcome)
        state["panels"] = panels
        return panels

    logging.info(f"✅ SCRIPT: {total} panels | Ruling: {outcome.upper()}")
    return state["panels"]


# ── Fallbacks ─────────────────────────────────────────────────────────────────

def _default_variable_cast() -> dict:
    return {
        "plaintiff": {
            "key":"plaintiff","name":"Zarnak","type":"Kobold",
            "display_name":"ZARNAK THE KOBOLD",
            "title":"Zarnak the Kobold — Plaintiff",
            "voice":"en_US-joe-medium.onnx",
        },
        "defendant": {
            "key":"defendant","name":"Blobbus","type":"Mimic",
            "display_name":"BLOBBUS THE MIMIC",
            "title":"Blobbus the Mimic — Defendant",
            "voice":"en_GB-southern_english_female-low.onnx",
        },
    }


def _fallback_act(act_name: str, question: str,
                  vc: dict, outcome: str = "raw") -> list:
    p  = vc["plaintiff"]
    d  = vc["defendant"]
    pk = p["key"]
    dk = d["key"]
    pn = f"{p['name']} the {p['type']}"
    dn = f"{d['name']} the {d['type']}"

    fallbacks = {
        "opening": [
            {"speaker":"bailiff","emotion":"excited",
             "text":"ALL RISE! The Dungeon Courtroom is now in session!",
             "action":"Bailiff slams gavel on podium"},
            {"speaker":"bailiff","emotion":"excited",
             "text":f"The case of {pn} versus {dn}! Question: {question[:40]}! Presiding: The Honorable Judge Aldric Bonecrusher!",
             "action":"Bailiff gestures grandly at everyone"},
            {"speaker":"judge","emotion":"tired",
             "text":"Be seated. Let us get through this with minimal suffering.",
             "action":"Judge glares at both parties"},
        ],
        "prosecution": [
            {"speaker":pk,"emotion":"smug",
             "text":f"Player's Handbook page 194. Dungeon Master's Guide chapter 8. The rules are CLEAR, Your Honor.",
             "action":f"{pn} stabs finger at rulebook"},
            {"speaker":pk,"emotion":"angry",
             "text":"This is not a matter of opinion. It is a matter of the rules as written!",
             "action":f"{pn} waves rulebook furiously"},
            {"speaker":pk,"emotion":"smug",
             "text":"Xanathar's Guide page 77 explicitly addresses this. EXPLICITLY.",
             "action":f"{pn} underlines text with a tiny claw"},
            {"speaker":pk,"emotion":"neutral",
             "text":"The rules exist for a reason. That reason is to be FOLLOWED.",
             "action":f"{pn} adjusts glasses primly"},
            {"speaker":pk,"emotion":"smug",
             "text":"I rest my case. The defense has nothing but feelings.",
             "action":f"{pn} sits down with great satisfaction"},
        ],
        "defense": [
            {"speaker":dk,"emotion":"excited",
             "text":"Would this moment make everyone at the table cheer? YES. Then it is correct.",
             "action":f"{dn} spreads arms wide"},
            {"speaker":dk,"emotion":"smug",
             "text":"Page numbers are for people who forgot why they play D&D.",
             "action":f"{dn} waves dismissively at the rulebook"},
            {"speaker":dk,"emotion":"excited",
             "text":"The best rule is the one that creates the best story. Period.",
             "action":f"{dn} pounds table enthusiastically"},
            {"speaker":dk,"emotion":"neutral",
             "text":"My opponent cited three books. I cite one thing: is it FUN?",
             "action":f"{dn} leans forward with a grin"},
            {"speaker":dk,"emotion":"smug",
             "text":"Rule of Cool has never ended a friendship. Following the rules as written strictly has ended several.",
             "action":f"{dn} gestures knowingly at audience"},
        ],
        "dream": [
            {"speaker":"narrator","emotion":"dreaming",
             "text":"Meanwhile the Judge recalls the Great Rules Schism of 1991...",
             "action":"Dreamlike mist swirls around judge"},
            {"speaker":"narrator","emotion":"dreaming",
             "text":"...when a single ambiguous spell description split an entire gaming group for a decade.",
             "action":"Ancient tome glows with eerie light"},
        ],
        "crossexam": [
            {"speaker":pk,"emotion":"angry",
             "text":f"You cited FEELINGS, {d['name']}! This is a court of LAW!",
             "action":f"{pn} points accusingly"},
            {"speaker":dk,"emotion":"smug",
             "text":f"And you cited a book, {p['name']}. Has a book ever made you laugh?",
             "action":f"{dn} tilts head with a smirk"},
            {"speaker":pk,"emotion":"shocked",
             "text":"That is — I — the Player's Handbook has made me feel many things!",
             "action":f"{pn} clutches rulebook protectively"},
            {"speaker":dk,"emotion":"excited",
             "text":"Name ONE time following the rules strictly created a story worth telling. ONE time!",
             "action":f"{dn} slams both hands on table"},
        ],
        "ruling": _fallback_ruling(pn, dn, outcome),
    }
    return fallbacks.get(act_name, [])


def _fallback_ruling(pn: str, dn: str, outcome: str) -> list:
    if outcome == "raw":
        return [
            {"speaker":"judge","emotion":"angry",
             "text":"SILENCE. I have heard enough from both of you.",
             "action":"Judge slams gavel, cracks the bench"},
            {"speaker":"judge","emotion":"tired",
             "text":f"{dn} argued fun. Admirable. Wrong.",
             "action":"Judge waves hand dismissively"},
            {"speaker":"judge","emotion":"tired",
             "text":f"The rules are the rules. {pn} wins. The rules as written stand. Court adjourned.",
             "action":"Judge points sternly at audience"},
            {"speaker":"bailiff","emotion":"excited",
             "text":"ALL RISE! The rules have been upheld! Great ruling everyone!",
             "action":"Bailiff pumps fist with zero understanding"},
        ]
    elif outcome == "cool":
        return [
            {"speaker":"judge","emotion":"angry",
             "text":"SILENCE. I have heard enough from both of you.",
             "action":"Judge slams gavel, cracks the bench"},
            {"speaker":"judge","emotion":"tired",
             "text":f"{pn} cited page numbers. Correct but soulless.",
             "action":"Judge steeples skeletal fingers"},
            {"speaker":"judge","emotion":"tired",
             "text":f"D&D is a game. {dn} wins. Rule of Cool stands. Court adjourned.",
             "action":"Judge points at audience with a rare slight smile"},
            {"speaker":"bailiff","emotion":"excited",
             "text":"ALL RISE! Fun is officially legal! I think! What happened?",
             "action":"Bailiff cheers while looking confused"},
        ]
    else:  # draw
        return [
            {"speaker":"judge","emotion":"angry",
             "text":"SILENCE. Both arguments are correct. Both are infuriating.",
             "action":"Judge slams gavel twice"},
            {"speaker":"judge","emotion":"tired",
             "text":f"{pn} cited the rules. {dn} cited the vibe. Both have a point. This court is deadlocked.",
             "action":"Judge holds head in skeletal hands"},
            {"speaker":"judge","emotion":"neutral",
             "text":"YOU — watching this — must decide. Comment your verdict. The court awaits.",
             "action":"Judge points directly at the camera"},
            {"speaker":"bailiff","emotion":"excited",
             "text":"LIKE AND SUBSCRIBE to cast your vote! I think that's how it works!",
             "action":"Bailiff waves at camera enthusiastically"},
        ]


def _fallback_script(question: str, vc: dict, outcome: str) -> list:
    logging.info("⚠️  SCRIPT: Using complete fallback script.")
    result = []
    for act in ACT_INSTRUCTIONS:
        if act == "casting":
            continue
        result.extend(_fallback_act(act, question, vc, outcome))
    return result


if __name__ == "__main__":
    test_state = {
        "question": "Can a Paladin lie to protect innocent people without losing their oath?",
        "source":   "manual test",
        "context":  "",
        "panels":   [],
    }
    panels = generate_episode_script(test_state)
    vc = test_state.get("variable_cast", {})
    outcome = test_state.get("ruling_outcome", "?")
    print(f"\n{len(panels)} panels | Ruling: {outcome.upper()}")
    print(f"Plaintiff: {vc.get('plaintiff',{}).get('title','?')}")
    print(f"Defendant: {vc.get('defendant',{}).get('title','?')}\n")
    for i, p in enumerate(panels, 1):
        print(f"[{i:02d}] {p['speaker'].upper()} ({p['emotion']}): {p['text']}")
