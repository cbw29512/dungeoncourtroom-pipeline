"""
main.py — DungeonCourtroom Master Pipeline Orchestrator

CAST STRUCTURE:
  Fixed (same every episode): judge, bailiff
  Variable (changes every episode): plaintiff monster, defendant monster
  episode_state.json is the single source of truth throughout.

Usage:
  python main.py              # Fresh question from sources, full run
  python main.py --test       # Hardcoded question, no upload
  python main.py --question "Can a Bard inspire themselves?"
  python main.py --no-upload  # Generate but skip YouTube
  python main.py --daemon     # Continuous production loop
  python main.py --list       # Show episode registry
  python main.py --roster     # Show monster roster
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(Path(__file__).parent / "pipeline.log", encoding='utf-8'),
    ]
)

BASE_DIR     = Path(__file__).parent
EPISODES_DIR = BASE_DIR / "episodes"
EPISODES_DIR.mkdir(exist_ok=True)

from scraper_node     import fetch_dnd_questions, save_to_history
from script_node      import generate_episode_script, find_ollama_port, FIXED_CAST
from vision_node      import generate_portrait, render_comic_panel
from audio_node       import generate_episode_audio
from render_node      import assemble_episode
from publisher_node   import upload_episode
from episode_registry import (register_episode, update_episode_status,
                               update_ruling_outcome, update_episode_cast,
                               get_episode_summary, get_next_episode_number)


# ── Episode state helpers ─────────────────────────────────────────────────────

def _state_path(episode_dir: Path) -> Path:
    return episode_dir / "episode_state.json"


def _save_state(state: dict, episode_dir: Path):
    # Atomic write — temp file then rename
    tmp = _state_path(episode_dir).with_suffix(".tmp")
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2, ensure_ascii=False)
    tmp.replace(_state_path(episode_dir))


def _init_state(episode_num, episode_id, question, source, context) -> dict:
    return {
        "episode_number": episode_num,
        "episode_id":     episode_id,
        "question":       question,
        "source":         source,
        "context":        context,
        "fixed_cast":     {k: {"display_name": v["display_name"],
                               "title": v["title"]}
                           for k, v in FIXED_CAST.items()},
        "variable_cast":  {},
        "panels":         [],
        "image_paths":    [],
        "audio_paths":    [],
        "output_file":    None,
        "youtube_id":     None,
        "status":         "started",
        "created_at":     datetime.now().isoformat(),
        "steps_complete": [],
    }


def _mark_step(state, step, episode_dir):
    if step not in state["steps_complete"]:
        state["steps_complete"].append(step)
    _save_state(state, episode_dir)
    logging.info(f"   [state] step '{step}' saved.")


# ── Environment check ─────────────────────────────────────────────────────────

def check_environment() -> bool:
    logging.info("=" * 60)
    logging.info("  DUNGEON COURTROOM -- PRE-FLIGHT CHECK")
    logging.info("=" * 60)
    ok = True

    port = find_ollama_port()
    if port:
        logging.info(f"  [OK] Ollama on port {port}")
    else:
        logging.warning("  [!!] Ollama not found -- fallback script will be used")

    piper = BASE_DIR / "assets" / "voices" / "piper" / "piper.exe"
    if piper.exists():
        logging.info("  [OK] Piper TTS")
    else:
        logging.warning("  [!!] Piper TTS not found")
        try:
            import edge_tts
            logging.info("  [OK] edge-tts fallback available")
        except ImportError:
            logging.error("  [NO] edge-tts missing -- run: pip install edge-tts")
            ok = False

    for pkg in ["moviepy", "PIL", "requests", "diffusers"]:
        mod = "PIL.Image" if pkg == "PIL" else pkg
        try:
            __import__(mod)
            logging.info(f"  [OK] {pkg}")
        except ImportError:
            logging.error(f"  [NO] {pkg} missing")
            ok = False

    if (BASE_DIR / "client_secrets.json").exists():
        logging.info("  [OK] YouTube client_secrets.json")
    else:
        logging.warning("  [!!] YouTube credentials missing -- upload will be skipped")

    try:
        import torch
        if torch.cuda.is_available():
            name = torch.cuda.get_device_name(0)
            vram = torch.cuda.get_device_properties(0).total_memory / 1e9
            logging.info(f"  [OK] GPU: {name} ({vram:.1f} GB VRAM)")
        else:
            logging.warning("  [!!] CUDA not available")
    except ImportError:
        logging.warning("  [!!] torch not found")

    logging.info(f"  [OK] Next episode: #{get_next_episode_number():03d}")
    logging.info("=" * 60)
    return ok


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run_episode(question: str, source: str = "manual",
                context: str = "", upload: bool = True) -> bool:

    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    episode_id  = f"ep_{timestamp}"
    episode_dir = EPISODES_DIR / episode_id
    episode_dir.mkdir(parents=True, exist_ok=True)

    # GAP 4 FIX: monster names added after script step below
    # Register with empty cast first — updated after casting
    episode_num = register_episode(question, source, episode_id)
    state       = _init_state(episode_num, episode_id, question, source, context)
    _save_state(state, episode_dir)

    logging.info("")
    logging.info("=" * 60)
    logging.info(f"  EPISODE #{episode_num:03d}")
    logging.info(f"  Q: {question[:65]}")
    logging.info(f"  Source: {source}")
    logging.info(f"  State: {_state_path(episode_dir)}")
    logging.info("=" * 60)

    try:
        # ── Step 1: Script + monster casting ──────────────────────────────
        logging.info("\n[1/6] Generating script (casting + all acts)...")
        state["status"] = "scripting"
        _save_state(state, episode_dir)

        panels = generate_episode_script(state)

        if not panels:
            raise RuntimeError("No panels generated.")

        vc      = state.get("variable_cast", {})
        p       = vc.get("plaintiff", {})
        d       = vc.get("defendant", {})
        outcome = state.get("ruling_outcome", "raw")

        logging.info(f"  -> {len(panels)} panels")
        logging.info(f"  -> Plaintiff: {p.get('title','?')}")
        logging.info(f"  -> Defendant: {d.get('title','?')}")
        logging.info(f"  -> Ruling outcome: {outcome.upper()}")

        # Update registry with cast and ruling now that we have them
        update_ruling_outcome(episode_id, outcome)
        update_episode_cast(episode_id,
                            p.get("title", ""),
                            d.get("title", ""))

        _mark_step(state, "script", episode_dir)

        # ── Step 2: Portraits ──────────────────────────────────────────────
        logging.info("\n[2/6] Generating portraits...")
        state["status"] = "portraits"
        _save_state(state, episode_dir)

        portrait_cache = {}
        all_speakers   = set(panel["speaker"] for panel in panels)

        for sp in all_speakers:
            portrait_cache[sp] = generate_portrait(
                speaker=sp,
                variable_cast=vc,
                episode_dir=episode_dir,
            )

        # GAP 1 FIX: write fresh portrait paths back into variable_cast
        # so scraper_node can reuse them next time this monster appears
        for role_key in ("plaintiff", "defendant"):
            sp = vc.get(role_key, {}).get("key")
            if sp and sp in portrait_cache:
                vc[role_key]["portrait_path"] = portrait_cache[sp]
        state["variable_cast"]  = vc
        state["portrait_cache"] = portrait_cache
        _mark_step(state, "portraits", episode_dir)
        logging.info(f"  -> {len(portrait_cache)} portraits ready")

        # ── Step 3: Render comic panels ────────────────────────────────────
        logging.info("\n[3/6] Rendering comic panels...")
        state["status"] = "rendering_panels"
        _save_state(state, episode_dir)

        image_paths = []
        for i, panel in enumerate(panels):
            sp   = panel["speaker"]
            port = portrait_cache.get(sp) or portrait_cache.get("judge", "")
            path = render_comic_panel(
                speaker=sp,
                emotion=panel.get("emotion", "neutral"),
                text=panel.get("text", ""),
                action=panel.get("action", ""),
                portrait_path=port,
                episode_dir=episode_dir,
                panel_index=i+1,
                episode_num=episode_num,
                variable_cast=vc,
            )
            image_paths.append(path)

        state["image_paths"] = image_paths
        _mark_step(state, "panels", episode_dir)
        logging.info(f"  -> {len(image_paths)} panels rendered")

        # ── Step 4: Audio ──────────────────────────────────────────────────
        logging.info("\n[4/6] Generating audio...")
        state["status"] = "audio"
        _save_state(state, episode_dir)

        audio_paths = generate_episode_audio(panels, episode_dir, variable_cast=vc)
        state["audio_paths"] = audio_paths
        _mark_step(state, "audio", episode_dir)
        logging.info(f"  -> {sum(1 for ap in audio_paths if ap)}/{len(panels)} files")

        # ── Step 5: Assemble MP4 ───────────────────────────────────────────
        output_path = EPISODES_DIR / f"DungeonCourtroom_EP{episode_num:03d}.mp4"
        logging.info(f"\n[5/6] Assembling -> {output_path.name}...")
        state["status"] = "rendering_video"
        _save_state(state, episode_dir)

        success = assemble_episode(
            panels=panels,
            panel_image_paths=image_paths,
            audio_paths=audio_paths,
            episode_dir=episode_dir,
            output_path=output_path,
            episode_num=episode_num,
            question=question,
            source=source,
            plaintiff_title=p.get("title", ""),
            defendant_title=d.get("title", ""),
        )
        if not success:
            raise RuntimeError("Video assembly failed.")

        state["output_file"] = str(output_path)
        _mark_step(state, "video", episode_dir)

        # ── Step 6: Upload ─────────────────────────────────────────────────
        youtube_id = None
        if upload and (BASE_DIR / "client_secrets.json").exists():
            logging.info("\n[6/6] Uploading to YouTube...")
            state["status"] = "uploading"
            _save_state(state, episode_dir)

            p_title  = p.get("title", "")
            d_title  = d.get("title", "")

            # GAP 3 FIX: YouTube title includes both monster names
            yt_title = (
                f"Ep #{episode_num:03d}: {p_title} vs {d_title}"
                f" | {question[:45]}"
            )

            youtube_id = upload_episode(
                video_path=output_path,
                episode_title=yt_title,
                question=question,
                plaintiff_title=p_title,      # BUG 4 FIX: pass cast to publisher
                defendant_title=d_title,
                ruling_outcome=outcome,
            )
            if youtube_id:
                state["youtube_id"] = youtube_id
                logging.info(f"  -> https://youtu.be/{youtube_id}")
            _mark_step(state, "upload", episode_dir)
        else:
            logging.info("\n[6/6] Upload skipped.")

        # ── Finalise ───────────────────────────────────────────────────────
        state["status"]       = "complete"
        state["completed_at"] = datetime.now().isoformat()
        _save_state(state, episode_dir)
        update_episode_status(episode_id, "complete", youtube_id)

        # GAP 2 FIX: save question to history ONLY after successful completion
        # BUG 5: history saving now happens in scraper_node.py after successful episode completion

        # Clean temp audio (keep panels + state for reference)
        for f in episode_dir.glob("audio_*.wav"):
            try: f.unlink()
            except Exception: pass

        logging.info("")
        logging.info("=" * 60)
        logging.info(f"  EPISODE #{episode_num:03d} COMPLETE")
        logging.info(f"  File:    {output_path.name}")
        logging.info(f"  Panels:  {len(panels)}")
        logging.info(f"  Ruling:  {outcome.upper()}")
        logging.info(f"  State:   {_state_path(episode_dir).name}")
        if youtube_id:
            logging.info(f"  YouTube: https://youtu.be/{youtube_id}")
        logging.info("=" * 60)
        return True

    except Exception as e:
        import traceback
        logging.error(f"Pipeline error:\n{traceback.format_exc()}")
        state["status"] = "failed"
        state["error"]  = str(e)
        _save_state(state, episode_dir)
        update_episode_status(episode_id, "failed")
        # GAP 2 FIX: do NOT save to history on failure — allow retry
        return False


# ── Daemon ────────────────────────────────────────────────────────────────────

def run_daemon(upload: bool = True):
    logging.info("DAEMON MODE: Starting...")
    while True:
        try:
            questions = fetch_dnd_questions(max_results=5)
            if not questions:
                logging.info("No new questions. Sleeping 1 hour...")
                time.sleep(3600)
                continue
            for q in questions:
                run_episode(q["title"], source=q["source"],
                            context=q.get("context", ""), upload=upload)
                time.sleep(60)
        except KeyboardInterrupt:
            logging.info("Daemon stopped.")
            break
        except Exception as e:
            logging.error(f"Daemon error: {e}")
            time.sleep(300)


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="DungeonCourtroom Pipeline")
    parser.add_argument("--test",      action="store_true",
                        help="Run with hardcoded test question, no upload")
    parser.add_argument("--no-upload", action="store_true",
                        help="Generate but skip YouTube upload")
    parser.add_argument("--question",  type=str, default=None,
                        help="Use a specific question")
    parser.add_argument("--daemon",    action="store_true",
                        help="Run continuous production loop")
    parser.add_argument("--list",      action="store_true",  # GAP: was missing
                        help="Show episode registry and exit")
    parser.add_argument("--roster",    action="store_true",
                        help="Show monster roster and exit")
    args = parser.parse_args()

    if args.list:
        print(get_episode_summary())
        return

    if args.roster:
        from monster_roster import get_roster_summary
        print(get_roster_summary())
        return

    if not check_environment():
        sys.exit(1)

    upload = not args.test and not args.no_upload

    if args.daemon:
        run_daemon(upload=upload)
    elif args.test or args.question:
        q = args.question or \
            "Can a Paladin lie to protect innocent people without losing their oath?"
        run_episode(q, source="manual test", upload=upload)
    else:
        questions = fetch_dnd_questions(max_results=1)
        if questions:
            q = questions[0]
            run_episode(q["title"], source=q["source"],
                        context=q.get("context", ""), upload=upload)
        else:
            run_episode(
                "Can a Paladin lie to protect innocent people without losing their oath?",
                source="fallback", upload=upload
            )


if __name__ == "__main__":
    main()
