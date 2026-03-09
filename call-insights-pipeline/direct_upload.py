#!/usr/bin/env python3
"""
Direct Google Sheets Upload via API (no service account needed)
Uses an API key approach with the sheets API directly.

This script uploads all_transcripts_combined.csv to a Google Sheet.
It handles grid expansion and batch uploads automatically.

Usage:
  ./venv/bin/python direct_upload.py
"""

import csv
import json
import time
import sys
import os
import urllib.request
import urllib.error
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# Config
SHEET_ID = "1CI5CGV0M7a298pfEndL1yTVhDduLZAK_fRJxhkjWRhE"
SHEET_TAB = "Sheet1"
CSV_FILE = BASE_DIR / "output" / "all_transcripts_combined.csv"

COLUMNS = [
    "call_sid", "date", "start_time", "end_time", "from_number", "to_number",
    "exotel_number", "direction", "status", "duration_seconds",
    "conversation_duration_seconds", "duration_minutes", "app_name",
    "from_circle", "to_circle", "disconnected_by", "group_name",
    "num_speakers", "transcription_status", "recording_url", "transcript"
]


def upload_with_gspread():
    """Upload using gspread (requires service account)."""
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        print("❌ gspread not installed. Run: pip install gspread google-auth")
        return False

    creds_file = BASE_DIR / "config" / "google_credentials.json"
    if not creds_file.exists():
        print(f"❌ Credentials not found: {creds_file}")
        return False

    # Read CSV
    print(f"📖 Reading {CSV_FILE}...")
    with open(CSV_FILE, encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = list(reader)
    print(f"   {len(rows)} rows loaded")

    # Auth
    print("🔐 Authenticating...")
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(str(creds_file), scopes=scopes)
    gc = gspread.authorize(creds)

    # Open sheet
    sheet = gc.open_by_key(SHEET_ID)
    try:
        worksheet = sheet.worksheet(SHEET_TAB)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sheet.add_worksheet(title=SHEET_TAB, rows=len(rows) + 10, cols=len(header))

    # Expand rows if needed
    current_rows = worksheet.row_count
    needed = len(rows) + 1  # +1 for header
    if current_rows < needed:
        print(f"📐 Expanding sheet from {current_rows} to {needed} rows...")
        worksheet.resize(rows=needed, cols=len(header))

    # Clear and write header
    print("🧹 Clearing existing data...")
    worksheet.clear()
    worksheet.append_row(header, value_input_option="RAW")

    # Upload in batches
    BATCH_SIZE = 500
    total = len(rows)
    uploaded = 0

    print(f"📤 Uploading {total} rows in batches of {BATCH_SIZE}...")
    for i in range(0, total, BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        try:
            worksheet.append_rows(batch, value_input_option="RAW")
            uploaded += len(batch)
            pct = (uploaded / total) * 100
            print(f"   ✅ {uploaded}/{total} ({pct:.0f}%)")
        except Exception as e:
            print(f"   ❌ Error at batch {i//BATCH_SIZE}: {e}")
            print("   Retrying in 5 seconds...")
            time.sleep(5)
            try:
                worksheet.append_rows(batch, value_input_option="RAW")
                uploaded += len(batch)
                pct = (uploaded / total) * 100
                print(f"   ✅ {uploaded}/{total} ({pct:.0f}%) [retry OK]")
            except Exception as e2:
                print(f"   ❌ Retry failed: {e2}")
                print(f"   Uploaded {uploaded} rows so far. Run again to continue.")
                return False
        time.sleep(1)  # Rate limit

    print(f"\n🎉 Done! {uploaded} rows uploaded to '{SHEET_TAB}'")
    print(f"   View: https://docs.google.com/spreadsheets/d/{SHEET_ID}")
    return True


def upload_with_oauth():
    """Upload using OAuth2 (browser-based auth, no service account needed)."""
    try:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials as OAuthCreds
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build
    except ImportError:
        print("❌ Missing packages. Run:")
        print("   pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client")
        return False

    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
    token_file = BASE_DIR / "config" / "token.json"
    oauth_creds_file = BASE_DIR / "config" / "oauth_credentials.json"

    creds = None
    if token_file.exists():
        creds = OAuthCreds.from_authorized_user_file(str(token_file), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not oauth_creds_file.exists():
                print(f"❌ OAuth credentials not found: {oauth_creds_file}")
                print("Download from Google Cloud Console → APIs & Services → Credentials → OAuth 2.0 Client IDs")
                return False
            flow = InstalledAppFlow.from_client_secrets_file(str(oauth_creds_file), SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_file, "w") as f:
            f.write(creds.to_json())

    service = build("sheets", "v4", credentials=creds)

    # Read CSV
    print(f"📖 Reading {CSV_FILE}...")
    with open(CSV_FILE, encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = list(reader)
    print(f"   {len(rows)} rows loaded")

    all_values = [header] + rows

    # Clear existing
    print("🧹 Clearing sheet...")
    service.spreadsheets().values().clear(
        spreadsheetId=SHEET_ID,
        range=f"{SHEET_TAB}!A:U"
    ).execute()

    # Upload in batches
    BATCH_SIZE = 1000
    total = len(all_values)
    uploaded = 0

    print(f"📤 Uploading {total} rows in batches of {BATCH_SIZE}...")
    for i in range(0, total, BATCH_SIZE):
        batch = all_values[i:i + BATCH_SIZE]
        body = {"values": batch}
        start_row = i + 1
        range_str = f"{SHEET_TAB}!A{start_row}"

        service.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=range_str,
            valueInputOption="RAW",
            body=body,
        ).execute()

        uploaded += len(batch)
        pct = (uploaded / total) * 100
        print(f"   ✅ {uploaded}/{total} ({pct:.0f}%)")
        time.sleep(0.5)

    print(f"\n🎉 Done! {uploaded} rows uploaded")
    print(f"   View: https://docs.google.com/spreadsheets/d/{SHEET_ID}")
    return True


def main():
    if not CSV_FILE.exists():
        print(f"❌ CSV not found: {CSV_FILE}")
        print("Run the pipeline first to generate transcripts.")
        sys.exit(1)

    print("=" * 60)
    print("  GOOGLE SHEETS UPLOADER")
    print("=" * 60)
    print(f"  CSV: {CSV_FILE}")
    print(f"  Sheet: {SHEET_ID}")
    print(f"  Tab: {SHEET_TAB}")
    print()

    # Try gspread first (service account)
    creds_file = BASE_DIR / "config" / "google_credentials.json"
    if creds_file.exists():
        print("🔑 Found service account credentials, using gspread...")
        if upload_with_gspread():
            return
        print("gspread failed, trying OAuth...")

    # Try OAuth
    oauth_file = BASE_DIR / "config" / "oauth_credentials.json"
    if oauth_file.exists() or (BASE_DIR / "config" / "token.json").exists():
        print("🔑 Found OAuth credentials, using google-api-python-client...")
        if upload_with_oauth():
            return

    # No credentials available
    print()
    print("=" * 60)
    print("  NO GOOGLE CREDENTIALS FOUND")
    print("=" * 60)
    print()
    print("You need ONE of these options:")
    print()
    print("OPTION A: Service Account (recommended for automation)")
    print("  1. Go to https://console.cloud.google.com/")
    print("  2. Create/select a project")
    print("  3. Enable 'Google Sheets API' and 'Google Drive API'")
    print("  4. Go to IAM & Admin → Service Accounts → Create")
    print("  5. Create a key (JSON) → download it")
    print(f"  6. Save as: {creds_file}")
    print(f"  7. Share the Google Sheet with the service account email (Editor)")
    print(f"  8. Run this script again")
    print()
    print("OPTION B: OAuth (quick, browser-based)")
    print("  1. Go to https://console.cloud.google.com/")
    print("  2. APIs & Services → Credentials → Create OAuth Client ID")
    print("  3. Application type: Desktop app")
    print("  4. Download JSON")
    print(f"  5. Save as: {oauth_file}")
    print(f"  6. Run this script again (browser will open for auth)")
    print()
    print("OPTION C: Manual import")
    print(f"  1. Open: https://docs.google.com/spreadsheets/d/{SHEET_ID}")
    print(f"  2. File → Import → Upload → select: {CSV_FILE}")
    print(f"  3. Choose 'Replace current sheet'")


if __name__ == "__main__":
    main()
