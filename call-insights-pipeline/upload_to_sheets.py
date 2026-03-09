#!/usr/bin/env python3
"""
Quick uploader: Push all_transcripts_combined.csv → Google Sheets (Transcripts tab)
Uses Google Sheets API with a service account.

Setup (one-time):
  1. Go to https://console.cloud.google.com/
  2. Create a project (or use existing)
  3. Enable "Google Sheets API" and "Google Drive API"
  4. Create a Service Account → download JSON key
  5. Save it as: config/google_credentials.json
  6. Share your Google Sheet with the service account email (Editor access)

Usage:
  ./venv/bin/python upload_to_sheets.py
"""

import csv
import time
import sys
import os
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "1TVfdnmAx9qagVnt55k9r1DCzmg7c1TIb5_ghdwub58s")
SHEET_TAB = os.getenv("GOOGLE_SHEET_TAB", "Transcripts")
CSV_FILE = BASE_DIR / "output" / "all_transcripts_combined.csv"
CREDS_FILE = BASE_DIR / "config" / "google_credentials.json"


def upload():
    # Check prerequisites
    if not CSV_FILE.exists():
        print(f"❌ CSV not found: {CSV_FILE}")
        sys.exit(1)

    if not CREDS_FILE.exists():
        print(f"❌ Credentials not found: {CREDS_FILE}")
        print()
        print("Setup instructions:")
        print("  1. Go to https://console.cloud.google.com/")
        print("  2. Enable 'Google Sheets API' and 'Google Drive API'")
        print("  3. Create a Service Account → download JSON key")
        print(f"  4. Save it as: {CREDS_FILE}")
        print(f"  5. Share your Google Sheet with the service account email (Editor access)")
        sys.exit(1)

    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        print("❌ Missing packages. Install with:")
        print("  pip install gspread google-auth")
        sys.exit(1)

    # Read CSV
    print(f"📖 Reading {CSV_FILE}...")
    with open(CSV_FILE, encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = list(reader)
    print(f"   {len(rows)} rows loaded")

    # Auth
    print("🔐 Authenticating with Google...")
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(str(CREDS_FILE), scopes=scopes)
    gc = gspread.authorize(creds)

    # Open sheet
    print(f"📊 Opening sheet: {SHEET_ID} → {SHEET_TAB}")
    sheet = gc.open_by_key(SHEET_ID)
    worksheet = sheet.worksheet(SHEET_TAB)

    # Clear existing data
    print("🧹 Clearing existing data...")
    worksheet.clear()

    # Write header
    worksheet.append_row(header, value_input_option="RAW")

    # Upload in batches
    BATCH_SIZE = 500
    total = len(rows)
    uploaded = 0

    print(f"📤 Uploading {total} rows in batches of {BATCH_SIZE}...")
    for i in range(0, total, BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        worksheet.append_rows(batch, value_input_option="RAW")
        uploaded += len(batch)
        pct = (uploaded / total) * 100
        print(f"   ✅ {uploaded}/{total} ({pct:.0f}%)")
        time.sleep(1)  # Rate limit

    print(f"\n🎉 Done! {uploaded} rows uploaded to '{SHEET_TAB}' tab")
    print(f"   View: https://docs.google.com/spreadsheets/d/{SHEET_ID}")


if __name__ == "__main__":
    upload()
