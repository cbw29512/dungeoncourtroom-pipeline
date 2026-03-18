"""
audio_node.py
TTS pipeline: Kokoro (local CPU) → edge-tts (network fallback)
SFX: gavel bang, table slam, book thud, jury gasp — synthesized in pure Python.

SFX triggers:
  gavel_bang  — any action containing "gavel"
  table_slam  — slams table/desk/fist/hands/podium
  book_thud   — slams/waves/throws rulebook
  jury_gasp   — any panel with emotion == "shocked"

Voices:
  judge     → en-GB-RyanNeural    -18%   (deep British, very slow, ancient authority)
  bailiff   → en-US-BrianNeural   +8%    (booming, jovial)
  narrator  → en-GB-RyanNeural    -20%   (slow, mysterious)
  plaintiff → en-US-RogerNeural   +20%   (uptight, fast, slightly whiny)
  defendant → en-AU-WilliamNeural -8%    (chill Australian, laid back)
"""

import asyncio
import logging
import math
import random
import struct
import subprocess
import wave
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DIR  = Path(__file__).parent
PIPER_DIR = BASE_DIR / "assets" / "voices" / "piper"
PIPER_EXE = PIPER_DIR / "piper.exe"
SFX_DIR   = BASE_DIR / "assets" / "sfx"

# ── Voice maps ────────────────────────────────────────────────────────────────

EDGE_VOICES = {
    "judge":    "en-GB-RyanNeural",
    "bailiff":  "en-US-BrianNeural",
    "narrator": "en-GB-RyanNeural",
}
EDGE_RATES = {
    "judge":    "-18%",
    "bailiff":  "+8%",
    "narrator": "-20%",
}
PIPER_VOICES = {
    "judge":    "en_US-bryce-medium.onnx",
    "bailiff":  "en_US-lessac-medium.onnx",
    "narrator": "en_US-bryce-medium.onnx",
}
KOKORO_VOICES = {
    "judge":    "am_adam",
    "bailiff":  "am_michael",
    "narrator": "bm_daniel",
}
KOKORO_SPEEDS = {
    "judge":    0.82,
    "bailiff":  1.05,
    "narrator": 0.78,
}


# ── SFX synthesis ─────────────────────────────────────────────────────────────

def _write_wav(path: Path, frames: list, rate: int = 44100):
    with wave.open(str(path), 'w') as wf:
        wf.setnchannels(1); wf.setsampwidth(2); wf.setframerate(rate)
        wf.writeframes(struct.pack(f'<{len(frames)}h', *frames))


def _ensure_sfx() -> dict:
    """Generate all SFX WAV files if not cached. Pure Python, zero dependencies."""
    SFX_DIR.mkdir(parents=True, exist_ok=True)
    sfx = {}

    # Gavel bang — real recording preferred, synthesized fallback
    # To use real sound: rename the gavel MP3 to gavel_source.mp3
    # and place it in assets/sfx/
    p = SFX_DIR / "gavel_bang.wav"
    if not p.exists():
        real_mp3  = SFX_DIR / "gavel_source.mp3"
        converted = False
        if real_mp3.exists():
            import subprocess as _sp
            result = _sp.run(
                ["ffmpeg", "-y", "-i", str(real_mp3),
                 "-ss", "0.895", "-t", "1.108",
                 "-ar", "44100", "-ac", "1", "-acodec", "pcm_s16le",
                 str(p)],
                capture_output=True, timeout=15
            )
            converted = (result.returncode == 0 and p.exists())
            if converted:
                logging.info("🔊 SFX: gavel_bang.wav from real recording")
        if not converted:
            # Three-layer synthesis: crack + wood resonance + thud
            rate, frames, rng = 44100, [], random.Random(99)
            for i in range(int(rate * 0.55)):
                t     = i / rate
                crack = math.sin(2 * math.pi * 1800 * t) * math.exp(-28 * t)
                wood  = math.sin(2 * math.pi * 650  * t) * math.exp(-18 * t)
                thud  = math.sin(2 * math.pi * 120  * t) * math.exp(-8  * t)
                noise = rng.uniform(-1, 1) * math.exp(-200 * t)
                env   = math.exp(-6 * t)
                frames.append(int(env * (0.25*crack + 0.40*wood + 0.30*thud + 0.05*noise) * 31000))
            _write_wav(p, frames)
            logging.info("🔊 SFX: gavel_bang.wav synthesized")
    sfx["gavel"] = p

    # Table / fist slam — heavy dull impact
    p = SFX_DIR / "table_slam.wav"
    if not p.exists():
        rate, frames, rng = 44100, [], random.Random(7)
        for i in range(int(rate * 0.45)):
            t     = i / rate
            thud  = math.sin(2 * math.pi * 90  * t)
            body  = math.sin(2 * math.pi * 220 * t) * math.exp(-30 * t)
            noise = rng.uniform(-0.08, 0.08)
            frames.append(int(math.exp(-14*t) * (0.7*thud + 0.25*body + noise) * 26000))
        _write_wav(p, frames)
        logging.info("🔊 SFX: table_slam.wav generated")
    sfx["table"] = p

    # Book thud — papery slap + resonant surface
    p = SFX_DIR / "book_thud.wav"
    if not p.exists():
        rate, frames, rng = 44100, [], random.Random(13)
        for i in range(int(rate * 0.35)):
            t    = i / rate
            slap = rng.uniform(-1, 1) * math.exp(-50 * t)
            thud = math.sin(2 * math.pi * 160 * t) * math.exp(-18 * t)
            frames.append(int((0.4*slap + 0.6*thud) * 22000))
        _write_wav(p, frames)
        logging.info("🔊 SFX: book_thud.wav generated")
    sfx["book"] = p

    # Jury gasp — crowd breath swell
    p = SFX_DIR / "jury_gasp.wav"
    if not p.exists():
        rate, frames, rng = 44100, [], random.Random(42)
        for i in range(int(rate * 0.9)):
            t   = i / rate
            env = (t/0.08) if t < 0.08 else (1.0 if t < 0.3 else math.exp(-3.5*(t-0.3)))
            noise  = rng.uniform(-1, 1)
            shaped = noise * (0.6 + 0.4 * math.sin(2 * math.pi * 320 * t))
            frames.append(int(shaped * env * 18000))
        _write_wav(p, frames)
        logging.info("🔊 SFX: jury_gasp.wav generated")
    sfx["gasp"] = p

    return sfx


def _get_sfx_for_panel(panel: dict, sfx: dict) -> tuple:
    """Return (sfx_path, volume) for this panel, or (None, 0)."""
    action  = panel.get("action", "").lower()
    emotion = panel.get("emotion", "").lower()

    if "gavel" in action:
        return sfx["gavel"], 0.65

    if any(kw in action for kw in (
        "slams table", "pounds table", "slams both hands", "slams fist",
        "bangs fist",  "slaps table",  "slams desk",  "pounds desk",
        "slams hand",  "fist on",      "hands on table", "pounds podium",
        "slams podium", "slams both",
    )):
        return sfx["table"], 0.55

    if any(kw in action for kw in (
        "slams book", "drops book", "slaps book",
        "waves rulebook furiously", "throws rulebook",
        "pounds rulebook", "slams rulebook",
    )):
        return sfx["book"], 0.45

    if emotion == "shocked":
        return sfx["gasp"], 0.45

    return None, 0


def _mix_sfx(speech_path: Path, sfx_path: Path, out_path: Path,
              sfx_volume: float = 0.55) -> bool:
    """Mix SFX at the start of a speech WAV. Returns True on success."""
    try:
        with wave.open(str(speech_path), 'rb') as sp:
            rate = sp.getframerate(); n_ch = sp.getnchannels()
            speech_raw = sp.readframes(sp.getnframes())
        with wave.open(str(sfx_path), 'rb') as sf:
            sfx_raw = sf.readframes(sf.getnframes())

        speech_s = list(struct.unpack(f'<{len(speech_raw)//2}h', speech_raw))
        sfx_s    = list(struct.unpack(f'<{len(sfx_raw)//2}h', sfx_raw))
        mixed    = [max(-32768, min(32767,
                        s + (int(sfx_s[i] * sfx_volume) if i < len(sfx_s) else 0)))
                    for i, s in enumerate(speech_s)]

        with wave.open(str(out_path), 'w') as wf:
            wf.setnchannels(n_ch); wf.setsampwidth(2); wf.setframerate(rate)
            wf.writeframes(struct.pack(f'<{len(mixed)}h', *mixed))
        return True
    except Exception as e:
        logging.warning(f"⚠️  SFX: mix failed: {e}")
        return False


# ── Voice map builder ─────────────────────────────────────────────────────────

def _piper_available() -> bool:
    return PIPER_EXE.exists()


def _kokoro_available() -> bool:
    try:
        import kokoro  # noqa
        return True
    except ImportError:
        return False


def _build_voice_maps(variable_cast: dict = None) -> dict:
    """Build complete voice config for this episode. Never mutates globals."""
    kokoro = dict(KOKORO_VOICES)
    speeds = dict(KOKORO_SPEEDS)
    edge   = dict(EDGE_VOICES)
    rates  = dict(EDGE_RATES)
    piper  = dict(PIPER_VOICES)

    if variable_cast:
        p  = variable_cast.get("plaintiff", {})
        d  = variable_cast.get("defendant", {})
        pk = p.get("key", "plaintiff")
        dk = d.get("key", "defendant")

        # Plaintiff: rules lawyer — uptight, fast, slightly whiny
        kokoro[pk] = "am_eric";  speeds[pk] = 1.22
        edge[pk]   = "en-US-RogerNeural";   rates[pk] = "+20%"
        piper[pk]  = p.get("voice", "en_US-joe-medium.onnx")

        # Defendant: rule of cool — chill, warm, unhurried Australian
        kokoro[dk] = "am_onyx";  speeds[dk] = 0.90
        edge[dk]   = "en-AU-WilliamNeural"; rates[dk] = "-8%"
        piper[dk]  = d.get("voice", "en_GB-southern_english_female-low.onnx")

    return {"kokoro": kokoro, "speeds": speeds,
            "edge": edge, "rates": rates, "piper": piper}


# ── TTS backends ──────────────────────────────────────────────────────────────

# Cache Kokoro availability — test once per process, skip if broken
_KOKORO_OK: bool | None = None

def _generate_kokoro(text: str, speaker: str, out_path: Path,
                     vm: dict) -> bool:
    """Kokoro TTS — CPU only, zero VRAM. Caches availability to avoid repeated failures."""
    global _KOKORO_OK
    if _KOKORO_OK is False:
        return False  # Already confirmed broken — skip immediately
    try:
        from kokoro import KPipeline
        import soundfile as sf
        import numpy as np

        voice = vm["kokoro"].get(speaker, "am_adam")
        speed = vm["speeds"].get(speaker, 1.0)
        lang  = "b" if voice.startswith("b") else "a"

        pipeline = KPipeline(lang_code=lang)
        chunks   = []
        for result in pipeline(text, voice=voice, speed=speed):
            # Handle both old API (returns tensor) and new API (returns (gs, ps, audio))
            if isinstance(result, tuple):
                audio_part = result[2]  # new API: (graphemes, phonemes, audio)
            else:
                audio_part = result     # old API: tensor directly
            if hasattr(audio_part, 'numpy'):
                audio_part = audio_part.numpy()
            if audio_part is not None and len(audio_part) > 0:
                chunks.append(np.asarray(audio_part, dtype=np.float32))

        if not chunks:
            _KOKORO_OK = False
            return False

        audio = np.concatenate(chunks)
        sf.write(str(out_path), audio, 24000)
        _KOKORO_OK = True
        return out_path.exists()
    except Exception as e:
        logging.warning(f"⚠️  AUDIO: Kokoro failed for {speaker}: {e}")
        _KOKORO_OK = False  # Mark as broken — skip for rest of episode
        return False


async def _edge_tts_async(text: str, voice: str, rate: str,
                           out_path: Path) -> bool:
    """
    edge-tts saves MP3 regardless of extension.
    Convert to PCM WAV via ffmpeg so SFX mixing works.
    """
    try:
        import edge_tts
        mp3_path = out_path.with_suffix(".mp3")
        comm = edge_tts.Communicate(text, voice, rate=rate)
        await comm.save(str(mp3_path))
        if not mp3_path.exists():
            return False
        result = subprocess.run(
            ["ffmpeg", "-y", "-i", str(mp3_path),
             "-ar", "44100", "-ac", "1", "-acodec", "pcm_s16le", str(out_path)],
            capture_output=True, timeout=30
        )
        mp3_path.unlink(missing_ok=True)
        return out_path.exists() and result.returncode == 0
    except Exception as e:
        logging.error(f"❌ AUDIO: edge-tts failed: {e}")
        return False


def generate_panel_audio(text: str, speaker: str, out_path: Path,
                          voice_maps: dict = None) -> bool:
    """
    Generate TTS for one panel.
    Priority: Kokoro (local CPU) → Piper (local) → edge-tts (network)
    """
    speaker = speaker.lower().strip()
    vm = voice_maps or _build_voice_maps()

    # 1. Kokoro
    if _kokoro_available():
        if _generate_kokoro(text, speaker, out_path, vm):
            logging.info(f"🔊 AUDIO [Kokoro]: {speaker} → {out_path.name}")
            return True
        logging.warning(f"⚠️  AUDIO: Kokoro failed, falling back...")

    # 2. Piper
    if _piper_available():
        model_name = vm["piper"].get(speaker) or "en_US-bryce-medium.onnx"
        model_path = PIPER_DIR / model_name
        if model_path.exists():
            try:
                result = subprocess.run(
                    [str(PIPER_EXE), "--model", str(model_path),
                     "--output_file", str(out_path)],
                    input=text, text=True, capture_output=True, timeout=30,
                )
                if result.returncode == 0 and out_path.exists():
                    logging.info(f"🔊 AUDIO [Piper]: {speaker} → {out_path.name}")
                    return True
            except Exception as e:
                logging.warning(f"⚠️  AUDIO: Piper failed: {e}")

    # 3. edge-tts
    voice   = vm["edge"].get(speaker) or "en-GB-RyanNeural"
    rate    = vm["rates"].get(speaker, "+0%")
    success = asyncio.run(_edge_tts_async(text, voice, rate, out_path))
    if success:
        logging.info(f"🔊 AUDIO [edge-tts]: {speaker} → {out_path.name}")
    else:
        logging.error(f"❌ AUDIO: All TTS methods failed for {out_path.name}")
    return success


def generate_episode_audio(panels: list, episode_dir: Path,
                           variable_cast: dict = None) -> list:
    """
    Generate TTS + SFX for all panels.
    Returns list of audio file paths parallel to panels list.
    """
    vm  = _build_voice_maps(variable_cast)
    sfx = _ensure_sfx()

    audio_paths = []
    for i, panel in enumerate(panels):
        speaker = panel.get("speaker", "judge")
        text    = panel.get("text", "")

        if not text.strip():
            audio_paths.append(None)
            continue

        out_path  = episode_dir / f"audio_{i:03d}_{speaker}.wav"
        speech_ok = generate_panel_audio(text, speaker, out_path, voice_maps=vm)

        if speech_ok:
            sfx_path, sfx_vol = _get_sfx_for_panel(panel, sfx)
            if sfx_path and sfx_path.exists():
                mixed = episode_dir / f"audio_{i:03d}_{speaker}_sfx.wav"
                if _mix_sfx(out_path, sfx_path, mixed, sfx_volume=sfx_vol):
                    out_path.unlink(missing_ok=True)
                    mixed.rename(out_path)
                    logging.info(f"   + {sfx_path.stem} mixed into panel {i+1}")

        audio_paths.append(str(out_path) if speech_ok else None)

    logging.info(f"✅ AUDIO: {sum(1 for p in audio_paths if p)} / {len(panels)} files.")
    return audio_paths


if __name__ == "__main__":
    import tempfile
    test_dir = Path(tempfile.mkdtemp())
    sfx = _ensure_sfx()
    print(f"Kokoro: {_kokoro_available()} | Piper: {_piper_available()}")
    print("SFX files:")
    for name, path in sfx.items():
        print(f"  {name:12} {path.stat().st_size:,} bytes")

    vm = _build_voice_maps({
        "plaintiff": {"key": "plaintiff"},
        "defendant": {"key": "defendant"},
    })
    tests = [
        {"speaker":"judge",    "emotion":"angry",   "text":"Silence.",
         "action":"Judge slams gavel on bench"},
        {"speaker":"plaintiff","emotion":"smug",    "text":"Player's Handbook page 194.",
         "action":"Slams rulebook on table"},
        {"speaker":"defendant","emotion":"shocked", "text":"That is outrageous!",
         "action":"Stands in disbelief"},
        {"speaker":"bailiff",  "emotion":"excited", "text":"ALL RISE!",
         "action":"Bangs gavel on podium"},
    ]
    for p in tests:
        out      = test_dir / f"test_{p['speaker']}.wav"
        ok       = generate_panel_audio(p["text"], p["speaker"], out, voice_maps=vm)
        sfx_p, _ = _get_sfx_for_panel(p, sfx)
        print(f"{'OK' if ok else 'FAIL'}  {p['speaker']:12} sfx={sfx_p.stem if sfx_p else 'none':15} | {p['action']}")
