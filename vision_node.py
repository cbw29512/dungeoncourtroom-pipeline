"""
vision_node.py
Two responsibilities:
  1. generate_portrait()   - Dreamshaper-8 for any character.
                             Fixed cast (judge/bailiff) cached forever.
                             Variable cast (plaintiff/defendant monsters)
                             generated fresh every episode.
  2. render_comic_panel()  - Full 1920x1080 comic panel with:
                             - Character zone left (themed per speaker)
                             - Speech bubble top-right
                             - Action zone bottom-right
                             - Full nameplate with title
                             - Emotion FX
"""

import logging
import math
import textwrap
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

from monster_roster import update_portrait_path as _roster_update_portrait

# Use getLogger instead of basicConfig so main.py's handler is not overridden
logger = logging.getLogger(__name__)

BASE_DIR   = Path(__file__).parent
ASSETS_DIR = BASE_DIR / "assets"
IMAGES_DIR = ASSETS_DIR / "images"
IMAGES_DIR.mkdir(parents=True, exist_ok=True)

PANEL_W, PANEL_H = 1920, 1080
CHAR_ZONE_W      = 520
CONTENT_X        = CHAR_ZONE_W + 30
CONTENT_W        = PANEL_W - CONTENT_X - 30

# ── Fixed cast themes (never change) ─────────────────────────────────────────
FIXED_THEMES = {
    "judge": {
        "bg": (8, 4, 2), "accent": (180, 30, 20), "nameplate": (25, 8, 6),
        "portrait_prompt": (
            "D&D death knight undead warrior, full black plate armor, "
            "glowing red eyes visible through helmet visor, "
            "skeletal face partially visible under helmet, "
            "tattered black cape billowing behind, "
            "one hand raised holding a bone gavel, "
            "dark energy crackling around gauntlets, "
            "imposing armored undead judge, front facing, "
            "cell shaded comic book art, thick black outlines, "
            "flat bold colors, white background, solo, no shadow",
            # negative
            "living human, flesh skin, warm colors, blonde hair, "
            "friendly expression, bright colors, no armor, "
            "multiple characters, background, scenery, "
            "realistic photo, 3d render, blur, watermark"
        ),
    },
    "bailiff": {
        "bg": (8, 30, 45), "accent": (60, 160, 200), "nameplate": (12, 45, 68),
        "portrait_prompt": (
            "masterpiece, official D&D character art, large friendly ogre in brown "
            "bailiff uniform with a bronze badge, holding a tiny gavel comically small "
            "in his huge hand, big wide happy grin, simple good-natured expression, "
            "cell shaded comic book illustration, thick black outlines, flat bold colors, "
            "full body portrait, front facing, pure white background",
            "background, scenery, realistic, photo, 3d, shadow, blur, watermark, grey, gradient"
        ),
    },
    "narrator": {
        "bg": (15, 18, 45), "accent": (120, 140, 255), "nameplate": (25, 28, 70),
        "portrait_prompt": None,  # Narrator gets cosmic background, no portrait
    },
}

# Theme colors for variable cast
PLAINTIFF_THEME = {"bg": (10, 40, 10),  "accent": (80, 200, 60),  "nameplate": (15, 60, 15)}
DEFENDANT_THEME = {"bg": (28, 8, 38),   "accent": (180, 60, 220), "nameplate": (42, 12, 58)}



# ── SRD monster physical descriptions — keyed to exact SD prompt keywords ─────
# Each entry matches the official Monster Manual illustration style.
# Used so SD generates a recognizable creature, not a generic fantasy figure.
SRD_MONSTER_DESC = {
    "Kobold":        "small reptilian humanoid, scaly red-brown skin, tiny horns, long snout, "
                     "large orange eyes, thin tail, bony claws, hunched posture, rat-like",
    "Goblin":        "small green-skinned humanoid, oversized ears, flat nose, beady yellow eyes, "
                     "wide mouth with sharp teeth, wiry limbs, hunched",
    "Hobgoblin":     "militaristic orange-skinned humanoid, flat wide nose, red-orange skin, "
                     "fierce dark eyes, strong jaw, soldier-like build",
    "Bugbear":       "large hairy goblinoid, brown fur covering body, wedge-shaped head, "
                     "small beady eyes, wide flat nose, massive powerful arms",
    "Orc":           "large muscular green-grey humanoid, prominent lower tusks jutting upward, "
                     "flat wide nose, deep-set red eyes, brutish heavy brow",
    "Gnoll":         "hyena-headed humanoid, spotted brown fur, mane of coarse hair, "
                     "powerful hyena jaws with sharp teeth, hunched spine, laughing expression",
    "Lizardfolk":    "bipedal reptilian humanoid, green-grey scaly skin, long snout, "
                     "lidded yellow eyes, thick tail, clawed feet and hands",
    "Kenku":         "raven-headed humanoid, jet black feathers, crow beak, "
                     "glossy black eyes, wings as arms with clawed hands, taloned feet",
    "Minotaur":      "massive muscular bull-headed humanoid, large curved horns, "
                     "dark brown fur on upper body, hooves, powerfully built",
    "Medusa":        "beautiful woman from waist up with snakes for hair, "
                     "writhing living snakes growing from scalp, cold eyes",
    "Harpy":         "woman's upper body with large vulture wings instead of arms, "
                     "taloned bird legs, wild hair, sharp clawed hands",
    "Ogre":          "massive lumbering humanoid, grey-green warty skin, "
                     "huge flat nose, jutting lower jaw, dull sunken eyes, fat and muscular",
    "Troll":         "tall thin gangly green humanoid, rubbery green skin, "
                     "long rubbery arms reaching to knees, clawed hands, hunched, "
                     "small beady eyes, wide flat nose, toothy grin, scraggly hair",
    "Owlbear":       "massive creature with bear body and owl head, "
                     "brown feathers on head and shoulders, yellow owl eyes, sharp beak, "
                     "bear claws, feathered and furred hybrid",
    "Basilisk":      "eight-legged stocky lizard, dull green scaly hide, "
                     "eight legs splayed wide, heavy armored body, "
                     "glowing pale green eyes, wide toad-like mouth",
    "Manticore":     "lion body with human male face, large mane, "
                     "bat wings, scorpion tail covered in spikes",
    "Gelatinous Cube": "perfectly transparent cube of clear slime, "
                       "objects visible trapped inside, glistening wet surface",
    "Displacer Beast": "panther-like body, six legs, two long tentacles from shoulders "
                       "with spiky ends, blue-black fur, eerie green eyes",
    "Rust Monster":  "insect-like creature, antennae, segmented tan and brown body, "
                     "four legs, feathery antennae tips, armadillo-like shell",
    "Young Red Dragon": "wingless young red dragon, crimson red scales, "
                        "two large curved horns, reptilian head, "
                        "powerful forelimbs, smoke curling from nostrils",
    "Young Blue Dragon": "young blue dragon, sapphire blue scales, "
                         "single large lightning-bolt shaped horn on snout, "
                         "frilled neck, crackling electricity around body",
    "Young Green Dragon": "young green dragon, forest green scales, "
                          "swept-back ridged horns, long elegant neck, "
                          "frill along spine, chlorine green mist",
    "Young White Dragon": "young white dragon, pale icy white scales, "
                          "crown of crystalline spines, cold blue eyes, "
                          "frost breath misting from mouth",
    "Young Black Dragon": "young black dragon, glossy black scales with grey highlights, "
                          "swept-back curved horns, acid dripping from jaws, "
                          "sunken hollow eye sockets, skeletal face",
    "Ghoul":         "emaciated undead humanoid, grey corpse skin pulled tight, "
                     "sharp elongated fingers with claws, white glowing eyes, "
                     "lipless mouth showing rotting teeth, hunched",
    "Skeleton":      "articulated human skeleton, yellowed bones, "
                     "empty dark eye sockets with pinprick lights, "
                     "jaw hanging slightly open",
    "Zombie":        "shambling rotting corpse humanoid, decomposed grey-green flesh, "
                     "blank white eyes, torn clothing, arms outstretched",
    "Wight":         "undead warrior with withered grey skin, sunken dark eyes "
                     "with cold white pinpoints, ancient armor, gaunt face",
    "Vampire Spawn": "pale undead humanoid, pallid white skin, red gleaming eyes, "
                     "elongated fangs, elegant but predatory bearing",
    "Imp":           "tiny red-skinned devil, small curved horns, bat wings, "
                     "barbed tail, glowing yellow eyes, impish grin",
    "Quasit":        "tiny green-skinned demon, large bat-like ears, "
                     "small curved horns, barbed tail, wide yellow eyes",
    "Cambion":       "half-devil humanoid, red skin, small curved horns on forehead, "
                     "leathery bat wings, solid black eyes, elegant but sinister",
    "Barbed Devil":  "tall red devil covered in sharp barbs and spines, "
                     "curved horns, barbed tail, burning eyes, muscular",
    "Gargoyle":      "stone grey winged humanoid statue, angular chiseled features, "
                     "horns, bat wings, clawed hands and feet, crouching predatory pose",
    "Griffon":       "eagle head and wings, lion body and hindquarters, "
                     "yellow eagle eyes, golden feathers on head, tawny lion fur",
    "Chimera":       "three heads: lion, goat, dragon — lion forebody, goat horns, "
                     "dragon tail, bat wings, fire breathing",
    "Mimic":         "wooden treasure chest with a wide toothy maw open, "
                     "large googly eyes on lid, pseudopod arms, sticky surface",
    "Centaur":       "upper human torso merging into horse body, "
                     "horse ears, wild hair, strong arms, galloping stance",
    "Satyr":         "human upper body with goat legs below waist, "
                     "small curved horns, pointed ears, holding pan pipes",
    "Ettin":         "massive two-headed giant, two distinct ugly heads on one body, "
                     "grey-brown warty skin, club-like arms, brutish",
    "Hydra":         "massive multi-headed serpentine body, nine long necks "
                     "each ending in a fanged dragon-like head, green-grey scales",
    "Dryad":         "beautiful female nature spirit, hair made of leaves and vines, "
                     "green-tinged skin, elfin features, flowing natural robes",
    "Sprite":        "tiny winged fey, insect wings, small elfin face, "
                     "green clothing, mischievous expression",
    "Green Hag":     "old withered woman with green warty skin, wild tangled hair, "
                     "long hooked nose, sharp claws, twisted hunched posture",
    "Hill Giant":    "enormous fat-muscled giant, crude animal skins, "
                     "lumpy skin, matted hair, dull expression, club in hand",
    "Fire Giant":    "massive giant with orange-red skin, black armor, "
                     "fiery red hair and beard, orange glowing eyes",
    "Frost Giant":   "enormous pale blue-white skinned giant, "
                     "icy blue eyes, white fur clothing, frost-rimed beard",
    "Naga":          "serpentine body from waist down, human face and upper torso, "
                     "scaled snake body coiling, hood like a cobra",
}

def _get_srd_desc(monster_type: str) -> str:
    """Return SRD-accurate physical description for SD prompt, or generic fallback."""
    return SRD_MONSTER_DESC.get(monster_type, f"{monster_type.lower()} creature")


def _get_theme(speaker: str, variable_cast: dict = None) -> dict:
    if speaker in FIXED_THEMES:
        return FIXED_THEMES[speaker]
    if variable_cast:
        if speaker == variable_cast.get("plaintiff", {}).get("key"):
            return PLAINTIFF_THEME
        if speaker == variable_cast.get("defendant", {}).get("key"):
            return DEFENDANT_THEME
    return {"bg": (30, 30, 30), "accent": (150, 150, 150), "nameplate": (45, 45, 45)}


def _get_display_name(speaker: str, variable_cast: dict = None) -> str:
    """Get the full display name + title for a speaker's nameplate."""
    fixed_names = {
        "judge":    "JUDGE ALDRIC BONECRUSHER",
        "bailiff":  "BAILIFF CRUNCH",
        "narrator": "-- LORE SCROLL --",
    }
    if speaker in fixed_names:
        return fixed_names[speaker]
    if variable_cast:
        p = variable_cast.get("plaintiff", {})
        d = variable_cast.get("defendant", {})
        if speaker == p.get("key"):
            return p.get("display_name", "PLAINTIFF")
        if speaker == d.get("key"):
            return d.get("display_name", "DEFENDANT")
    return speaker.upper()


def _get_portrait_subtitle(speaker: str, variable_cast: dict = None) -> str:
    """Second line of nameplate — role/title."""
    fixed_subtitles = {
        "judge":    "PRESIDING JUDGE",
        "bailiff":  "COURT BAILIFF",
        "narrator": "",
    }
    if speaker in fixed_subtitles:
        return fixed_subtitles[speaker]
    if variable_cast:
        p = variable_cast.get("plaintiff", {})
        d = variable_cast.get("defendant", {})
        if speaker == p.get("key"):
            return "PLAINTIFF"
        if speaker == d.get("key"):
            return "DEFENDANT"
    return ""


# ── Font loader ───────────────────────────────────────────────────────────────

def _load_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    candidates = [
        Path("C:/Windows/Fonts/impact.ttf"),
        Path("C:/Windows/Fonts/ariblk.ttf"),
        Path("C:/Windows/Fonts/arialbd.ttf") if bold else Path("C:/Windows/Fonts/arial.ttf"),
        Path("C:/Windows/Fonts/arial.ttf"),
    ]
    for p in candidates:
        if p.exists():
            try:
                return ImageFont.truetype(str(p), size)
            except Exception:
                continue
    return ImageFont.load_default()


def _sanitize_text(text: str) -> str:
    """Replace Unicode characters Impact/Arial can't render — prevents □ boxes."""
    replacements = {
        '’': "'",    # right single quote
        '‘': "'",    # left single quote
        '“': '"',    # left double quote
        '”': '"',    # right double quote
        '—': ' - ',  # em dash
        '–': '-',    # en dash
        '…': '...',  # ellipsis
        'é': 'e',    'è': 'e',  'ê': 'e',
        'à': 'a',    'â': 'a',
        'ô': 'o',    'ö': 'o',
        'ü': 'u',    'û': 'u',
        'ç': 'c',
        '−': '-',    # minus sign
        '·': '.',    # middle dot
        ' ': ' ',    # non-breaking space
    }
    for char, replacement in replacements.items():
        text = text.replace(char, replacement)
    # Final safety — strip any remaining non-ASCII
    return text.encode('ascii', 'replace').decode('ascii').replace('?', "'")


# ── Background removal ────────────────────────────────────────────────────────

def _remove_background(img: Image.Image, threshold: int = 225) -> Image.Image:
    img = img.convert("RGBA")
    data = img.getdata()
    new_data = []
    for r, g, b, a in data:
        brightness = (r + g + b) / 3
        is_grey    = abs(r-g) < 25 and abs(g-b) < 25 and abs(r-b) < 25
        if brightness > threshold and is_grey:
            new_data.append((r, g, b, 0))
        elif brightness > threshold - 20 and is_grey:
            alpha = int(((brightness-(threshold-20))/20)*255)
            new_data.append((r, g, b, 255-alpha))
        else:
            new_data.append((r, g, b, a))
    img.putdata(new_data)
    return img


# ── Portrait generation ───────────────────────────────────────────────────────

def _make_monster_prompt(monster_type: str, role: str) -> tuple:
    """Generate a Dreamshaper-8 prompt for any SRD monster.
    Kept under ~72 words so CLIP (77 token limit) doesn't truncate the
    isolation/style keywords that prevent shadows and multiple characters."""
    role_desc = {
        "plaintiff": "in ill-fitting suit and tie, holding rulebook, pointing",
        "defendant": "in formal robe, arms crossed defiantly",
    }.get(role, "in courtroom, looking important")

    # Mimic — pick a random object it's disguised as, with eyes and teeth showing
    if monster_type.lower() == "mimic":
        import random as _r
        mimic_forms = [
            "treasure chest",
            "wooden door",
            "leather armchair",
            "stone throne",
            "bookcase",
            "podium",
            "wardrobe",
        ]
        mimic_form = _r.choice(mimic_forms)
        prompt = (
            f"masterpiece, D&D character art, Mimic monster disguised as a {mimic_form}, "
            f"mouth open showing rows of sharp teeth, large googly eyes visible, "
            f"tongue lolling out, clearly a monster not furniture, "
            f"cell shaded comic book illustration, thick black outlines, flat bold colors, "
            f"flat lighting, no drop shadow, no cast shadow, no ambient shadow, "
            f"full body, front facing, pure white background, "
            f"isolated on white, no text, no logos, original design"
        )
        negative = (
            "human, person, humanoid, "
            "multiple characters, character sheet, reference sheet, "
            "realistic chest, plain furniture, no monster features, "
            "drop shadow, cast shadow, background, scenery, photo, blur, "
            "grey background, gradient, vignette, copyrighted"
        )
        return prompt, negative

    # Use SRD-accurate physical description so SD generates a recognizable creature
    srd_physical = _get_srd_desc(monster_type)

    # IMPORTANT: Put isolation/style keywords FIRST — CLIP truncates at 77 tokens
    # and reads left-to-right, so white background + single character must come
    # before the SRD description which can be long.
    prompt = (
        f"masterpiece, solo, single character, white background, "
        f"full body, front facing, flat lighting, no shadow, "
        f"D&D comic art, {monster_type}, {srd_physical}, "
        f"{role_desc}, "
        f"cell shaded, thick outlines, flat colors, isolated on white"
    )
    negative = (
        "multiple characters, character sheet, reference sheet, pose sheet, "
        "character lineup, many figures, group, crowd, two characters, "
        "human, humanoid, person, man, woman, elf, human face, human skin, "
        "text, watermark, logo, "
        "drop shadow, cast shadow, shadow, "
        "background, scenery, realistic, photo, 3d render, blur, "
        "grey background, gradient, vignette, copyrighted"
    )
    return prompt, negative


def generate_portrait(speaker: str, variable_cast: dict = None,
                      episode_dir: Path = None) -> str:
    """
    Generate a portrait for a speaker.
    Fixed cast (judge/bailiff): cached forever in assets/images/
    Variable cast (monsters): generated fresh, saved to episode_dir/
    Returns path to PNG.
    """
    # Fixed cast — cache forever
    if speaker in FIXED_THEMES and speaker != "narrator":
        cache_path = IMAGES_DIR / f"{speaker}_portrait.png"
        if cache_path.exists():
            logging.info(f"   Portrait cached: {speaker}")
            return str(cache_path)
        prompt_data = FIXED_THEMES[speaker]["portrait_prompt"]
        if prompt_data:
            return _generate_sd_portrait(prompt_data[0], prompt_data[1],
                                         cache_path, speaker)
        return _make_placeholder(speaker, cache_path)

    if speaker == "narrator":
        cache_path = IMAGES_DIR / "narrator_portrait.png"
        return _make_narrator_bg(cache_path)

    # Variable cast — check roster cache first (GAP 1 FIX)
    if variable_cast and episode_dir:
        p = variable_cast.get("plaintiff", {})
        d = variable_cast.get("defendant", {})

        if speaker == p.get("key"):
            monster_type = p["type"]
            monster_name = p["name"]
            role         = "plaintiff"
        elif speaker == d.get("key"):
            monster_type = d["type"]
            monster_name = d["name"]
            role         = "defendant"
        else:
            return _make_placeholder(speaker, episode_dir / f"portrait_{speaker}.png")

        # GAP 1 FIX: if roster has a valid cached portrait, reuse it
        cached = variable_cast.get(role, {}).get("portrait_path", "")
        if cached and Path(cached).exists():
            logging.info(f"   Portrait reused from roster: {monster_name} the {monster_type}")
            return cached

        # Generate fresh portrait for this episode
        out_path = episode_dir / f"portrait_{role}_{monster_type.replace(' ','_')}.png"
        prompt, negative = _make_monster_prompt(monster_type, role)
        portrait_path = _generate_sd_portrait(prompt, negative, out_path, speaker)

        # BUG 3 FIX: update roster with monster_name and monster_type in scope
        _roster_update_portrait(monster_type, monster_name, portrait_path)
        return portrait_path

    return _make_placeholder(speaker, IMAGES_DIR / f"{speaker}_portrait.png")


def _generate_sd_portrait(prompt: str, negative: str,
                           out_path: Path, speaker: str) -> str:
    """Run Dreamshaper-8 to generate a portrait."""
    try:
        import torch
        from diffusers import StableDiffusionPipeline

        logging.info(f"   Generating portrait: {speaker}...")
        pipe = StableDiffusionPipeline.from_pretrained(
            "Lykon/dreamshaper-8",
            torch_dtype=torch.float16,
            safety_checker=None,
        ).to("cuda")
        pipe.enable_attention_slicing()

        image = pipe(
            prompt, negative_prompt=negative,
            num_inference_steps=40, guidance_scale=8.0,
            width=512, height=768,
        ).images[0]

        del pipe
        torch.cuda.empty_cache()

        image = _remove_background(image)
        image.save(out_path)
        logging.info(f"   Portrait saved: {out_path.name}")
        return str(out_path)

    except Exception as e:
        logging.error(f"   Portrait generation failed for {speaker}: {e}")
        return _make_placeholder(speaker, out_path)


def _make_placeholder(speaker: str, out_path: Path) -> str:
    if out_path.exists():
        return str(out_path)
    colors = {
        "judge":    (80, 20, 20),  "bailiff":  (20, 60, 80),
        "narrator": (20, 20, 60),  "plaintiff":(20, 80, 20),
        "defendant":(60, 20, 80),
    }
    color = colors.get(speaker, (60, 60, 60))
    img   = Image.new("RGBA", (512, 768), (0, 0, 0, 0))
    draw  = ImageDraw.Draw(img)
    draw.ellipse([156, 60, 356, 260], fill=color+(255,))
    draw.rectangle([100, 280, 412, 720], fill=color+(255,))
    draw.text((256, 490), speaker[0].upper(), fill=(255,255,255,200),
              font=_load_font(140, bold=True), anchor="mm")
    img.save(out_path)
    return str(out_path)


def _make_narrator_bg(out_path: Path) -> str:
    if out_path.exists():
        return str(out_path)
    import random
    img  = Image.new("RGBA", (512, 768), (0,0,0,0))
    draw = ImageDraw.Draw(img)

    # Cosmic gradient background
    for y in range(768):
        t = y/768
        draw.line([(0,y),(512,y)],
                  fill=(int(15+t*10), int(18+t*12), int(45+t*30), 255))

    # Stars
    rng = random.Random(99)
    for _ in range(120):
        x, y = rng.randint(0,512), rng.randint(0,768)
        r = rng.randint(1,4)
        b = rng.randint(180,255)
        draw.ellipse([x-r,y-r,x+r,y+r], fill=(b,b,b,255))

    # Draw a D20 shape using polygons — no font needed
    cx, cy, size = 256, 384, 110
    gold = (200, 160, 40, 220)
    outline = (240, 200, 80, 255)

    # Outer icosahedron silhouette (20-sided approximation with 10 points)
    outer = []
    for i in range(10):
        a = (i / 10) * math.pi * 2 - math.pi/2
        r = size if i % 2 == 0 else size * 0.62
        outer.append((cx + r * math.cos(a), cy + r * math.sin(a)))
    draw.polygon(outer, fill=gold, outline=outline)

    # Inner pentagon
    inner = []
    for i in range(5):
        a = (i / 5) * math.pi * 2 - math.pi/2
        inner.append((cx + size*0.35 * math.cos(a), cy + size*0.35 * math.sin(a)))
    draw.polygon(inner, fill=(150, 120, 30, 180), outline=outline)

    # "20" label using font (small, centered, safe fallback)
    font = _load_font(52, bold=True)
    draw.text((cx, cy), "20", fill=(255, 240, 180, 255), font=font, anchor="mm")

    img.save(out_path)
    return str(out_path)


# ── Panel rendering helpers ───────────────────────────────────────────────────

def _draw_halftone(draw, x1, y1, x2, y2, color, spacing=22):
    for y in range(y1, y2, spacing):
        offset = (spacing//2) if ((y-y1)//spacing)%2==1 else 0
        for x in range(x1+offset, x2, spacing):
            r = spacing//4
            draw.ellipse([x-r,y-r,x+r,y+r], fill=color)


def _draw_anger_rays(draw, ox, oy):
    import random
    rng = random.Random(42)
    for _ in range(18):
        angle  = math.atan2(PANEL_H//2-oy, CONTENT_X-ox)
        spread = rng.uniform(-0.5, 0.5)
        a      = angle + spread
        length = rng.randint(180, 450)
        ex     = int(ox + math.cos(a)*length)
        ey     = int(oy + math.sin(a)*length)
        draw.line([(ox,oy),(ex,ey)], fill=(200,30,20), width=rng.randint(1,3))


def _draw_shock_burst(draw, cx, cy, radius=200):
    for i in range(16):
        a  = (i/16)*math.pi*2
        x1 = cx + int(math.cos(a)*radius*0.5)
        y1 = cy + int(math.sin(a)*radius*0.5)
        x2 = cx + int(math.cos(a)*radius)
        y2 = cy + int(math.sin(a)*radius)
        draw.line([(x1,y1),(x2,y2)], fill=(240,220,30), width=8)
    r = int(radius*0.4)
    draw.ellipse([cx-r,cy-r,cx+r,cy+r], fill=(255,240,60))


def _draw_dream_clouds(draw, x1, y1, x2, y2):
    import random
    rng = random.Random(7)
    for _ in range(12):
        cx = rng.randint(x1,x2)
        cy = rng.randint(y1,y2)
        for dr in [80,60,45]:
            draw.ellipse([cx-dr,cy-dr//2,cx+dr,cy+dr//2],
                         fill=(200,215,255), outline=(180,195,240), width=2)


def _draw_speech_bubble(draw, text, bx, by, bw, bh, emotion):
    is_dream = emotion == "dreaming"
    is_shout = emotion in ("angry","excited")
    padding  = 50
    radius   = 35
    size     = min(bw, bh)   # used for spike proportions
    bg       = (215,230,255) if is_dream else (252,248,235)
    border   = (10,5,2)
    bwidth   = 5 if is_shout else 4

    if is_shout:
        # Proper shout bubble: rounded rect with outward spikes on the border
        # Draw spikes first (behind bubble), then the filled rounded rect on top
        spike_pts = []
        steps = 32
        for i in range(steps):
            a   = (i / steps) * math.pi * 2
            jag = size * 0.08 if i % 2 == 0 else 0
            rx  = (bw / 2 + jag) * math.cos(a)
            ry  = (bh / 2 + jag) * math.sin(a)
            spike_pts.append((bx + bw//2 + rx, by + bh//2 + ry))
        draw.polygon(spike_pts, fill=border, outline=border)
        draw.rounded_rectangle([bx+6, by+6, bx+bw-6, by+bh-6],
                                radius=radius, fill=bg, outline=border, width=bwidth)
    else:
        draw.rounded_rectangle([bx,by,bx+bw,by+bh], radius=radius,
                                fill=bg, outline=border, width=bwidth)

    tail_bx = bx + 80
    tail_by = by + bh
    if is_dream:
        for i in range(5):
            cr = max(4, 18-i*3)
            cx = tail_bx + i*14
            cy = tail_by + i*20
            draw.ellipse([cx-cr,cy-cr,cx+cr,cy+cr],
                         fill=bg, outline=border, width=2)
    else:
        tail_pts = [(tail_bx,tail_by-4),(tail_bx+55,tail_by-4),(tail_bx+8,tail_by+65)]
        draw.polygon(tail_pts, fill=bg)
        draw.line([(tail_bx+3,tail_by-2),(tail_bx+52,tail_by-2)], fill=bg, width=8)

    text      = _sanitize_text(text)
    font_size = 46
    font      = _load_font(font_size, bold=True)
    usable_w  = bw - padding*2
    char_w    = max(1, usable_w//(font_size*0.55))
    lines     = textwrap.wrap(text, width=int(char_w))
    if len(lines) > 4:
        font_size = 38
        font      = _load_font(font_size, bold=True)
        char_w    = max(1, usable_w//(font_size*0.55))
        lines     = textwrap.wrap(text, width=int(char_w))

    line_h  = int(font_size*1.35)
    total_h = len(lines)*line_h
    start_y = by + (bh-total_h)//2
    for i, line in enumerate(lines):
        draw.text((bx+bw//2, start_y+i*line_h), line,
                  fill=(10,5,2), font=font, anchor="mm")


def _draw_action_zone(draw, action, x, y, w, h, theme):
    action = _sanitize_text(action)
    accent = theme["accent"]
    bg     = tuple(max(0,c-20) for c in theme["bg"])
    draw.rectangle([x,y,x+w,y+h], fill=bg, outline=accent, width=4)
    pad = 12
    draw.rectangle([x+pad,y+pad,x+w-pad,y+h-pad],
                   fill=tuple(min(c+15,255) for c in bg), outline=accent, width=2)
    lh = 36
    draw.rectangle([x,y,x+w,y+lh], fill=accent, outline=(10,5,2), width=2)
    draw.text((x+w//2,y+lh//2), "[ ACTION ]",
              fill=(10,5,2), font=_load_font(22,bold=True), anchor="mm")
    if action:
        font    = _load_font(34,bold=True)
        wrapped = textwrap.wrap(action.upper(), width=28)
        lh2     = 42
        total   = len(wrapped)*lh2
        sy      = y+lh+(h-lh-total)//2
        for i, line in enumerate(wrapped):
            draw.text((x+w//2,sy+i*lh2), line,
                      fill=(220,210,180), font=font, anchor="mm")


def _draw_nameplate(draw, display_name, subtitle, theme, x, y, w):
    """Two-line nameplate: name on top, role/title below."""
    h      = 80
    accent = theme["accent"]
    np_bg  = theme["nameplate"]

    draw.rectangle([x+4,y+4,x+w+4,y+h+4], fill=(0,0,0))
    draw.rectangle([x,y,x+w,y+h], fill=np_bg, outline=accent, width=3)
    draw.rectangle([x,y,x+w,y+5], fill=accent)

    name_font = _load_font(28, bold=True)
    sub_font  = _load_font(20)

    draw.text((x+w//2, y+28), display_name,
              fill=(235,225,200), font=name_font, anchor="mm")
    if subtitle:
        draw.text((x+w//2, y+58), subtitle,
                  fill=accent, font=sub_font, anchor="mm")


# ── Main panel renderer ───────────────────────────────────────────────────────

def render_comic_panel(speaker: str, emotion: str, text: str, action: str,
                       portrait_path: str, episode_dir: Path,
                       panel_index: int, episode_num: int = 0,
                       variable_cast: dict = None) -> str:
    """Render a full 1920x1080 comic panel. Returns path to PNG."""
    out_path  = episode_dir / f"panel_{panel_index:03d}_{speaker}.png"
    theme     = _get_theme(speaker, variable_cast)
    is_dream  = emotion == "dreaming"

    img  = Image.new("RGB", (PANEL_W, PANEL_H), theme["bg"])
    draw = ImageDraw.Draw(img)

    # Character zone gradient
    for x in range(0, CHAR_ZONE_W, 4):
        t     = x/CHAR_ZONE_W
        shade = tuple(int(c*(0.6+0.4*t)) for c in theme["bg"])
        draw.line([(x,0),(x,PANEL_H)], fill=shade, width=2)

    # Content zone
    if is_dream:
        draw.rectangle([CHAR_ZONE_W,0,PANEL_W,PANEL_H], fill=(185,200,240))
        _draw_dream_clouds(draw, CHAR_ZONE_W, 0, PANEL_W, PANEL_H)
    else:
        draw.rectangle([CHAR_ZONE_W,0,PANEL_W,PANEL_H], fill=(242,232,195))
        _draw_halftone(draw, CHAR_ZONE_W, 0, PANEL_W, PANEL_H,
                       color=(225,215,178), spacing=22)

    # Divider
    draw.rectangle([CHAR_ZONE_W,0,CHAR_ZONE_W+8,PANEL_H], fill=theme["accent"])
    draw.rectangle([CHAR_ZONE_W+8,0,CHAR_ZONE_W+12,PANEL_H], fill=(10,5,2))

    # Emotion FX
    cx, cy = CHAR_ZONE_W//2, PANEL_H//2
    if emotion == "shocked":
        _draw_shock_burst(draw, cx, cy-50, radius=220)
    elif emotion == "angry":
        _draw_anger_rays(draw, CHAR_ZONE_W-20, PANEL_H//2)

    # Portrait — leave room for nameplate at bottom (90px) plus padding
    NAMEPLATE_H = 90
    port_w = CHAR_ZONE_W - 20
    port_h = PANEL_H - NAMEPLATE_H - 20   # 20px gap above nameplate

    if portrait_path and Path(portrait_path).exists():
        try:
            portrait = Image.open(portrait_path).convert("RGBA")
            portrait = _remove_background(portrait)
            portrait = portrait.resize((port_w, port_h), Image.LANCZOS)
            img.paste(portrait, (10, 10), portrait)
        except Exception as e:
            logging.warning(f"   Portrait paste failed: {e}")
            draw.rectangle([10, 10, CHAR_ZONE_W-10, PANEL_H-NAMEPLATE_H-10],
                           fill=(50, 50, 50))
    else:
        # No portrait — draw a coloured silhouette placeholder
        draw.rectangle([10, 10, CHAR_ZONE_W-10, PANEL_H-NAMEPLATE_H-10],
                       fill=tuple(min(c+40, 255) for c in theme["bg"]))

    # Nameplate
    display_name = _get_display_name(speaker, variable_cast)
    subtitle     = _get_portrait_subtitle(speaker, variable_cast)
    _draw_nameplate(draw, display_name, subtitle, theme,
                    x=0, y=PANEL_H-90, w=CHAR_ZONE_W)

    # Speech bubble
    margin   = 28
    bubble_x = CONTENT_X + margin
    bubble_w = CONTENT_W - margin*2
    bubble_y = margin
    bubble_h = int(PANEL_H*0.58)
    _draw_speech_bubble(draw, text, bubble_x, bubble_y, bubble_w, bubble_h, emotion)

    # Action zone
    action_y = bubble_y + bubble_h + margin
    action_h = PANEL_H - action_y - margin
    _draw_action_zone(draw, action, bubble_x, action_y,
                      bubble_w, action_h, theme)

    # Watermark
    draw.text((PANEL_W-20, 16), "DUNGEON COURTROOM",
              fill=theme["accent"], font=_load_font(24,bold=True), anchor="rt")
    if episode_num > 0:
        draw.text((PANEL_W-20, 44), f"EP #{episode_num:03d}  |  PANEL {panel_index}",
                  fill=tuple(c//2 for c in theme["accent"]),
                  font=_load_font(20), anchor="rt")

    # Border
    bw = 10
    draw.rectangle([0,0,PANEL_W-1,PANEL_H-1], outline=(8,4,2), width=bw)
    draw.rectangle([bw+3,bw+3,PANEL_W-bw-4,PANEL_H-bw-4],
                   outline=(8,4,2), width=2)

    img.save(out_path, quality=95)
    logging.info(f"   Panel {panel_index:03d}: {speaker} [{emotion}] -> {out_path.name}")
    return str(out_path)


if __name__ == "__main__":
    import tempfile
    test_dir = Path(tempfile.mkdtemp())
    vc = {
        "plaintiff": {"key":"plaintiff","name":"Zarnak","type":"Goblin",
                      "display_name":"ZARNAK THE GOBLIN","title":"Zarnak the Goblin — Plaintiff"},
        "defendant": {"key":"defendant","name":"Mrrg","type":"Troll",
                      "display_name":"MRRG THE TROLL","title":"Mrrg the Troll — Defendant"},
    }
    portrait = generate_portrait("judge")
    out = render_comic_panel(
        speaker="judge", emotion="tired",
        text="The case of Zarnak versus Mrrg. This better be worth my undeath.",
        action="Judge glares at both parties",
        portrait_path=portrait,
        episode_dir=test_dir,
        panel_index=1, episode_num=1,
        variable_cast=vc,
    )
    print(f"Test panel: {out}")
