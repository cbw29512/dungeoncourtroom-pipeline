"""
render_node.py
Assembles comic panel images + audio into final 1920x1080 MP4.
Title card + episode panels + end card.
No emoji — uses text-safe characters only.
"""

import logging
import textwrap
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

PANEL_W, PANEL_H       = 1920, 1080
DEFAULT_PANEL_DURATION = 4.0
MIN_PANEL_DURATION     = 2.5
CROSSFADE_DUR          = 0.12   # 3 frames at 24fps — smooth dissolve, feels instant


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


def render_title_card(episode_num: int, question: str,
                      source: str, episode_dir: Path,
                      plaintiff_title: str = "",
                      defendant_title: str = "") -> str:
    """Render the opening title card with cast names. No emoji — pure text."""
    out_path = episode_dir / "panel_000_title.png"

    img  = Image.new("RGB", (PANEL_W, PANEL_H), (12, 6, 2))
    draw = ImageDraw.Draw(img)

    for y in range(0, PANEL_H, 35):
        draw.line([(0, y), (PANEL_W, y)], fill=(20, 12, 6), width=1)

    for i, color in [(14, (140, 100, 25)), (22, (190, 145, 35)), (30, (140, 100, 25))]:
        draw.rectangle([i, i, PANEL_W-i, PANEL_H-i], outline=color, width=3)

    gavel_font = _load_font(80, bold=True)
    draw.text((PANEL_W//2, 100), "* COURT IS IN SESSION *",
              fill=(150, 110, 30), font=gavel_font, anchor="mm")

    draw.line([(120, 160), (PANEL_W-120, 160)], fill=(180, 135, 35), width=4)

    title_font = _load_font(180, bold=True)
    draw.text((PANEL_W//2, 290), "DUNGEON",
              fill=(195, 150, 35), font=title_font, anchor="mm")
    draw.text((PANEL_W//2 + 5, 455), "COURTROOM",
              fill=(80, 15, 10), font=title_font, anchor="mm")
    draw.text((PANEL_W//2, 450), "COURTROOM",
              fill=(210, 35, 25), font=title_font, anchor="mm")

    draw.line([(120, 530), (PANEL_W-120, 530)], fill=(180, 135, 35), width=4)

    ep_font = _load_font(52, bold=True)
    draw.text((PANEL_W//2, 585), f"EPISODE #{episode_num:03d}",
              fill=(235, 225, 195), font=ep_font, anchor="mm")

    # GAP 3 FIX: show plaintiff vs defendant on title card
    if plaintiff_title and defendant_title:
        vs_font = _load_font(38, bold=True)
        draw.text((PANEL_W//2, 640),
                  f"{plaintiff_title}  vs  {defendant_title}",
                  fill=(195, 150, 35), font=vs_font, anchor="mm")
        q_y_start = 700
    else:
        q_y_start = 640

    q_font  = _load_font(38)
    wrapped = textwrap.wrap(f'"{question}"', width=70)
    for i, line in enumerate(wrapped[:3]):
        draw.text((PANEL_W//2, q_y_start + i*50), line,
                  fill=(175, 160, 125), font=q_font, anchor="mm")

    src_font = _load_font(26)
    draw.text((PANEL_W//2, PANEL_H - 95),
              f"SOURCE: {source.upper()}",
              fill=(100, 80, 30), font=src_font, anchor="mm")

    tag_font = _load_font(30, bold=True)
    draw.text((PANEL_W//2, PANEL_H - 52),
              "WHERE D&D'S GREATEST DEBATES GO TO COURT",
              fill=(130, 100, 30), font=tag_font, anchor="mm")

    draw.rectangle([0, 0, PANEL_W-1, PANEL_H-1], outline=(8, 4, 2), width=12)
    img.save(out_path)
    return str(out_path)


def render_end_card(episode_num: int, episode_dir: Path) -> str:
    """Render the end card."""
    out_path = episode_dir / "panel_999_end.png"

    img  = Image.new("RGB", (PANEL_W, PANEL_H), (12, 6, 2))
    draw = ImageDraw.Draw(img)

    for y in range(0, PANEL_H, 35):
        draw.line([(0, y), (PANEL_W, y)], fill=(20, 12, 6), width=1)

    for i, color in [(14, (140, 100, 25)), (22, (190, 145, 35))]:
        draw.rectangle([i, i, PANEL_W-i, PANEL_H-i], outline=color, width=3)

    draw.line([(120, 220), (PANEL_W-120, 220)], fill=(180, 135, 35), width=4)

    main_font = _load_font(150, bold=True)
    draw.text((PANEL_W//2 + 6, 366), "COURT ADJOURNED",
              fill=(70, 10, 5), font=main_font, anchor="mm")
    draw.text((PANEL_W//2, 360), "COURT ADJOURNED",
              fill=(195, 150, 35), font=main_font, anchor="mm")

    draw.line([(120, 460), (PANEL_W-120, 460)], fill=(180, 135, 35), width=4)

    sub_font = _load_font(62)
    draw.text((PANEL_W//2, 550), "Subscribe for weekly rulings",
              fill=(235, 225, 195), font=sub_font, anchor="mm")

    like_font = _load_font(48)
    draw.text((PANEL_W//2, 640),
              "LIKE  *  COMMENT  *  SHARE YOUR VERDICT BELOW",
              fill=(175, 160, 125), font=like_font, anchor="mm")

    ep_font = _load_font(36, bold=True)
    draw.text((PANEL_W//2, 750),
              f"Episode #{episode_num:03d} -- Dungeon Courtroom",
              fill=(130, 100, 30), font=ep_font, anchor="mm")

    draw.text((PANEL_W//2, PANEL_H - 60),
              "THE HONORABLE JUDGE ALDRIC BONECRUSHER PRESIDING",
              fill=(90, 70, 25), font=_load_font(28, bold=True), anchor="mm")

    draw.rectangle([0, 0, PANEL_W-1, PANEL_H-1], outline=(8, 4, 2), width=12)
    img.save(out_path)
    return str(out_path)


def _get_audio_duration(audio_path: str) -> float:
    """Get duration without opening a MoviePy clip (avoids handle leaks)."""
    try:
        import subprocess
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-show_entries", "format=duration",
             "-of", "csv=p=0", audio_path],
            capture_output=True, text=True, timeout=10
        )
        dur = float(result.stdout.strip())
        return max(dur + 0.4, MIN_PANEL_DURATION)
    except Exception:
        return DEFAULT_PANEL_DURATION


def assemble_episode(panels: list, panel_image_paths: list,
                     audio_paths: list, episode_dir: Path,
                     output_path: Path, episode_num: int,
                     question: str, source: str,
                     plaintiff_title: str = "",
                     defendant_title: str = "") -> bool:
    """
    Assemble all panels into final MP4 with smooth crossfade transitions.
    No black flicker between panels — each dissolves directly into the next.
    Returns True on success.
    """
    try:
        from moviepy.editor import (
            ImageClip, AudioFileClip, concatenate_videoclips
        )

        logging.info("🎬 RENDER: Starting assembly...")
        clips = []

        # ── Title card ────────────────────────────────────────────────────
        title_img  = render_title_card(episode_num, question, source, episode_dir,
                                        plaintiff_title, defendant_title)
        title_clip = (ImageClip(title_img)
                      .set_duration(4.5)
                      .fadeout(CROSSFADE_DUR))
        clips.append(title_clip)

        # ── Episode panels ────────────────────────────────────────────────
        total = len(panels)
        for i, (panel, img_path, audio_path) in enumerate(
                zip(panels, panel_image_paths, audio_paths)):

            logging.info(f"🎬 RENDER: Panel {i+1}/{total}...")

            if not img_path or not Path(img_path).exists():
                logging.warning(f"   Skipping panel {i} — image missing")
                continue

            if audio_path and Path(audio_path).exists():
                audio_clip = AudioFileClip(audio_path)
                duration   = max(audio_clip.duration + 0.4, MIN_PANEL_DURATION)
                img_clip   = (ImageClip(img_path)
                              .set_duration(duration)
                              .set_audio(audio_clip))
                img_clip._audio_to_close = audio_clip
            else:
                img_clip = ImageClip(img_path).set_duration(DEFAULT_PANEL_DURATION)

            # Apply crossfade on both ends of every panel
            # fadein overlaps with previous clip's fadeout — no black frame
            img_clip = img_clip.fadein(CROSSFADE_DUR)
            # Only fadeout if not the last panel (end card does its own fadein)
            if i < total - 1:
                img_clip = img_clip.fadeout(CROSSFADE_DUR)

            clips.append(img_clip)

        # ── End card ──────────────────────────────────────────────────────
        end_img  = render_end_card(episode_num, episode_dir)
        end_clip = (ImageClip(end_img)
                    .set_duration(6.0)
                    .fadein(CROSSFADE_DUR))
        clips.append(end_clip)

        if len(clips) < 3:
            logging.error("❌ RENDER: Not enough clips.")
            return False

        # Overlap clips by CROSSFADE_DUR so fades blend instead of going black
        logging.info(f"🎬 RENDER: Concatenating {len(clips)} clips with crossfades...")
        final = concatenate_videoclips(clips,
                                       padding=-CROSSFADE_DUR,
                                       method="compose")

        logging.info(f"🎬 RENDER: Writing {output_path.name}...")
        final.write_videofile(
            str(output_path),
            fps=24,
            codec="libx264",
            audio_codec="aac",
            bitrate="12000k",
            audio_bitrate="192k",
            preset="medium",
            logger=None,
            threads=4,
        )
        final.close()

        # Explicitly close all audio file handles
        for clip in clips:
            try:
                ac = getattr(clip, '_audio_to_close', None)
                if ac:
                    ac.close()
                clip.close()
            except Exception:
                pass

        size_mb = output_path.stat().st_size / (1024 * 1024)
        duration_s = sum(
            _get_audio_duration(ap) if ap and Path(ap).exists() else DEFAULT_PANEL_DURATION
            for ap in audio_paths
        )
        logging.info(f"✅ RENDER: {output_path.name} | {size_mb:.1f} MB | ~{duration_s/60:.1f} min")
        return True

    except Exception as e:
        import traceback
        logging.error(f"❌ RENDER FAILED:\n{traceback.format_exc()}")
        return False
