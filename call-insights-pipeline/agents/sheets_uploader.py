#!/usr/bin/env python3
"""
Agent 4: Google Sheets Uploader
────────────────────────────────
Reads transcribed_calls.json, checks what's already in the Google Sheet,
and appends only NEW rows. Uses gspread + Google Sheets API.

This agent is designed to be run after Agent 2 (Deepgram Transcriber).
It auto-appends new transcripts to the "CX call conversations" Google Sheet.

Usage:
  ./venv/bin/python agents/sheets_uploader.py
"""

import os
import sys
import json
import logging
import time
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("SheetsUploader")

# Config
SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "1TVfdnmAx9qagVnt55k9r1DCzmg7c1TIb5_ghdwub58s")
SHEET_TAB = os.getenv("GOOGLE_SHEET_TAB", "Transcripts")
INPUT_FILE = BASE_DIR / "output" / "transcribed_calls.json"

# Column order matching the Google Sheet
COLUMNS = [
    "call_sid", "date", "start_time", "end_time", "from_number", "to_number",
    "exotel_number", "direction", "status", "duration_seconds",
    "conversation_duration_seconds", "duration_minutes", "app_name",
    "from_circle", "to_circle", "disconnected_by", "group_name",
    "num_speakers", "transcription_status", "recording_url", "transcript"
]


def load_transcribed_calls():
    """Load transcribed calls from Agent 2 output."""
    if not INPUT_FILE.exists():
        logger.error(f"No transcribed calls found at {INPUT_FILE}")
        logger.error("Run Agent 2 first: ./venv/bin/python run_pipeline.py --agent 2")
        return []

    with open(INPUT_FILE) as f:
        calls = json.load(f)

    # Only include successful transcriptions
    success = []
    for call in calls:
        if call.get("transcription_status") != "success":
            continue
        td = call.get("transcript_data") or {}
        success.append({
            "call_sid": call.get("call_sid", ""),
            "date": call.get("date", ""),
            "start_time": call.get("start_time", ""),
            "end_time": call.get("end_time", ""),
            "from_number": str(call.get("from_number", "")),
            "to_number": str(call.get("to_number", "")),
            "exotel_number": str(call.get("exotel_number", "")),
            "direction": call.get("direction", ""),
            "status": call.get("status", ""),
            "duration_seconds": str(call.get("duration_seconds", "")),
            "conversation_duration_seconds": str(call.get("conversation_duration_seconds", "")),
            "duration_minutes": str(call.get("duration_minutes", "")),
            "app_name": call.get("app_name", ""),
            "from_circle": call.get("from_circle", ""),
            "to_circle": call.get("to_circle", ""),
            "disconnected_by": call.get("disconnected_by", ""),
            "group_name": call.get("group_name", ""),
            "num_speakers": str(td.get("num_speakers", 0)),
            "transcription_status": "success",
            "recording_url": call.get("recording_url", ""),
            "transcript": td.get("transcript", ""),
        })

    logger.info(f"Loaded {len(success)} successful transcriptions")
    return success


def get_existing_sids_from_sheet():
    """Get SIDs already in the Google Sheet using gspread."""
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        logger.error("gspread not installed. Run: pip install gspread google-auth")
        return set()

    creds_file = BASE_DIR / "config" / "google_credentials.json"
    if not creds_file.exists():
        logger.warning(f"No credentials file at {creds_file}")
        logger.info("Will upload all rows (can't check for duplicates)")
        return set()

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(str(creds_file), scopes=scopes)
    gc = gspread.authorize(creds)

    sheet = gc.open_by_key(SHEET_ID)
    worksheet = sheet.worksheet(SHEET_TAB)

    # Get all values in column A (call_sid)
    existing_sids = set()
    col_a = worksheet.col_values(1)
    for sid in col_a[1:]:  # Skip header
        if sid:
            existing_sids.add(sid)

    logger.info(f"Found {len(existing_sids)} existing SIDs in Google Sheet")
    return existing_sids


def upload_to_sheet_via_gspread(rows):
    """Upload rows using gspread (requires service account credentials)."""
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        logger.error("gspread not installed")
        return False

    creds_file = BASE_DIR / "config" / "google_credentials.json"
    if not creds_file.exists():
        logger.error(f"Credentials file not found: {creds_file}")
        return False

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(str(creds_file), scopes=scopes)
    gc = gspread.authorize(creds)

    sheet = gc.open_by_key(SHEET_ID)
    worksheet = sheet.worksheet(SHEET_TAB)

    # Convert to list of lists
    values = []
    for row in rows:
        values.append([row.get(col, "") for col in COLUMNS])

    # Append in batches of 500 (Google Sheets API limit)
    BATCH_SIZE = 500
    total_uploaded = 0

    for i in range(0, len(values), BATCH_SIZE):
        batch = values[i:i + BATCH_SIZE]
        worksheet.append_rows(batch, value_input_option="RAW")
        total_uploaded += len(batch)
        logger.info(f"Uploaded {total_uploaded}/{len(values)} rows")
        time.sleep(1)  # Rate limit

    return True


def upload_to_sheet_via_csv(rows):
    """Fallback: Save CSV that can be manually imported to Google Sheets."""
    import csv

    output = BASE_DIR / "output" / "for_google_sheets.csv"
    with open(output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    logger.info(f"Saved CSV for manual import: {output}")
    logger.info(f"You can import this to Google Sheets manually")
    return True


def run():
    """Main entry point."""
    logger.info("=" * 60)
    logger.info("AGENT 4: Google Sheets Uploader — Starting")
    logger.info("=" * 60)

    # 1. Load transcribed calls
    calls = load_transcribed_calls()
    if not calls:
        return []

    # 2. Check existing SIDs in sheet
    existing_sids = get_existing_sids_from_sheet()

    # 3. Filter to only new calls
    new_calls = [c for c in calls if c["call_sid"] not in existing_sids]
    logger.info(f"New calls to upload: {len(new_calls)} (skipping {len(calls) - len(new_calls)} already in sheet)")

    if not new_calls:
        logger.info("No new calls to upload!")
        return []

    # Sort by date
    new_calls.sort(key=lambda r: r.get("start_time", ""))

    # 4. Try gspread first, fallback to CSV
    try:
        success = upload_to_sheet_via_gspread(new_calls)
        if success:
            logger.info(f"✅ Successfully uploaded {len(new_calls)} rows to Google Sheet")
    except Exception as e:
        logger.warning(f"gspread upload failed: {e}")
        logger.info("Falling back to CSV export...")
        upload_to_sheet_via_csv(new_calls)

    return new_calls


if __name__ == "__main__":
    results = run()
    print(f"\n✅ Agent 4 complete: {len(results)} rows uploaded")
