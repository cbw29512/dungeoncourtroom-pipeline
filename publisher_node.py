"""
publisher_node.py
Uploads finished episodes to YouTube via Data API v3.
OAuth2 desktop flow — authenticates once, stores token.
Uploads as Unlisted for manual review before publishing.
"""

import os
import pickle
import logging
import random
import time
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DIR            = Path(__file__).parent
CLIENT_SECRETS_FILE = BASE_DIR / "client_secrets.json"
TOKEN_FILE          = BASE_DIR / "youtube_token.pickle"
SCOPES              = ["https://www.googleapis.com/auth/youtube.upload"]
RETRIABLE_STATUSES  = {500, 502, 503, 504}
MAX_RETRIES         = 5

def _build_description(question: str, plaintiff_title: str = "",
                        defendant_title: str = "", ruling_outcome: str = "") -> str:
    """Build YouTube description with actual episode cast. BUG 4 FIX."""
    outcome_line = {
        "raw":  "RULING: Rules as Written prevailed.",
        "cool": "RULING: Rule of Cool prevailed.",
        "draw": "RULING: HUNG COURT -- you decide! Comment your verdict below.",
    }.get(ruling_outcome, "")

    cast_line = ""
    if plaintiff_title and defendant_title:
        cast_line = f"This week: {plaintiff_title} vs {defendant_title}.\n\n"

    return f"""DUNGEON COURTROOM

{cast_line}{question}

Real D&D questions. Real arguments. Questionable rulings.
Presiding: The Honorable Judge Aldric Bonecrusher.

{outcome_line}

New episodes weekly!
Like if you agree with the ruling.
Comment YOUR ruling below.
Subscribe for weekly courtroom drama.

#DungeonsAndDragons #DnD #DungeonCourtroom #DnD5e #TabletopRPG #RulesLawyer #Comedy
"""

BASE_TAGS = [
    "D&D", "Dungeons and Dragons", "DnD 5e", "DnD Rules", "Rules Lawyer",
    "Dungeon Courtroom", "DnD Comedy", "Tabletop RPG", "RPG", "DnD Memes",
    "DnD Humor", "Monster Comedy", "DnD Rules Debate",
    "Lich", "DnD Judge", "DnD Questions", "DnD 5e Rules", "Courtroom Drama"
]


def _build_tags(plaintiff_title: str = "", defendant_title: str = "") -> list:
    """Build tags including actual monster types from this episode."""
    tags = list(BASE_TAGS)
    # Extract monster type from title like "Zarnak the Goblin — Plaintiff"
    for title in (plaintiff_title, defendant_title):
        if " the " in title:
            monster_type = title.split(" the ")[1].split(" —")[0].strip()
            if monster_type and monster_type not in tags:
                tags.append(monster_type)
    return tags[:30]  # YouTube max 500 chars total tag length


def _get_youtube_service():
    """Authenticate and return the YouTube API service object."""
    from googleapiclient.discovery import build
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request

    if not CLIENT_SECRETS_FILE.exists():
        raise FileNotFoundError(
            f"client_secrets.json not found at {CLIENT_SECRETS_FILE}\n"
            "Download it from Google Cloud Console → APIs & Services → Credentials."
        )

    creds = None
    if TOKEN_FILE.exists():
        with open(TOKEN_FILE, 'rb') as f:
            creds = pickle.load(f)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            logging.info("🔑 PUBLISHER: Refreshing expired token...")
            creds.refresh(Request())
        else:
            logging.info("🔑 PUBLISHER: Starting OAuth flow — browser will open...")
            flow = InstalledAppFlow.from_client_secrets_file(str(CLIENT_SECRETS_FILE), SCOPES)
            creds = flow.run_local_server(port=0)

        with open(TOKEN_FILE, 'wb') as f:
            pickle.dump(creds, f)
        logging.info("✅ PUBLISHER: Token saved.")

    return build('youtube', 'v3', credentials=creds)


def upload_episode(video_path: Path, episode_title: str, question: str,
                   plaintiff_title: str = "", defendant_title: str = "",
                   ruling_outcome: str = "") -> str | None:
    """
    Upload a video file to YouTube.
    Returns the YouTube video ID on success, None on failure.
    Video is uploaded as Unlisted for manual review.
    """
    from googleapiclient.http import MediaFileUpload
    from googleapiclient.errors import HttpError

    if not video_path.exists():
        logging.error(f"❌ PUBLISHER: Video file not found: {video_path}")
        return None

    logging.info(f"📤 PUBLISHER: Uploading '{episode_title}' to YouTube...")

    youtube = _get_youtube_service()

    body = {
        "snippet": {
            "title":           f"{episode_title} | Dungeon Courtroom",
            "description":     _build_description(question, plaintiff_title,
                                                   defendant_title, ruling_outcome),
            "tags":            _build_tags(plaintiff_title, defendant_title),
            "categoryId":      "24",
            "defaultLanguage": "en",
        },
        "status": {
            "privacyStatus": "unlisted",   # Manual review before going public
            "selfDeclaredMadeForKids": False,
        }
    }

    media = MediaFileUpload(str(video_path), chunksize=-1, resumable=True, mimetype="video/mp4")
    request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)

    response = None
    retry = 0

    while response is None:
        try:
            status, response = request.next_chunk()
            if status:
                pct = int(status.progress() * 100)
                logging.info(f"📤 PUBLISHER: Upload progress: {pct}%")
            if response and "id" in response:
                video_id = response["id"]
                logging.info(f"✅ PUBLISHER: Uploaded → https://youtu.be/{video_id} (Unlisted)")
                return video_id

        except HttpError as e:
            if e.resp.status in RETRIABLE_STATUSES:
                retry += 1
                if retry > MAX_RETRIES:
                    logging.error(f"❌ PUBLISHER: Max retries exceeded.")
                    return None
                wait = random.uniform(1, 2 ** retry)
                logging.warning(f"⚠️  PUBLISHER: HTTP {e.resp.status}, retrying in {wait:.1f}s...")
                time.sleep(wait)
            else:
                logging.error(f"❌ PUBLISHER: Non-retriable HTTP error: {e}")
                return None
        except Exception as e:
            logging.error(f"❌ PUBLISHER: Upload failed: {e}")
            return None

    return None


if __name__ == "__main__":
    # Test auth only — doesn't upload anything
    try:
        svc = _get_youtube_service()
        logging.info("✅ PUBLISHER: Auth test successful. YouTube service ready.")
    except Exception as e:
        logging.error(f"❌ PUBLISHER: Auth test failed: {e}")
