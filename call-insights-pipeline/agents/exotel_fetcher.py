"""
Agent 1: Exotel Call Fetcher (CSV Mode)
────────────────────────────────────────
Reads from Exotel CSV export, filters to inbound calls >2min with recordings.
Outputs filtered_calls.json for Agent 2.
"""

import os
import sys
import csv
import json
import logging
from collections import Counter
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(BASE_DIR / "logs" / "exotel_fetcher.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("ExotelFetcher")

MIN_DURATION = int(os.getenv("MIN_CALL_DURATION_SECONDS", "120"))

# Input CSV — look in multiple places
CSV_CANDIDATES = [
    BASE_DIR / "input" / "calls.csv",
    BASE_DIR / "calls.csv",
]
# Also check for any CSV in input/
INPUT_DIR = BASE_DIR / "input"

OUTPUT_FILE = BASE_DIR / "output" / "filtered_calls.json"
STATS_FILE = BASE_DIR / "output" / "fetch_stats.json"


def find_csv():
    """Find the input CSV file."""
    for p in CSV_CANDIDATES:
        if p.exists():
            return p
    if INPUT_DIR.exists():
        csvs = list(INPUT_DIR.glob("*.csv"))
        if csvs:
            return csvs[0]
    logger.error("No input CSV found! Place your Exotel CSV export in input/calls.csv")
    logger.error(f"Searched: {[str(p) for p in CSV_CANDIDATES]}")
    sys.exit(1)


def run():
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    (BASE_DIR / "logs").mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("AGENT 1: Exotel CSV Loader — Starting")
    logger.info("=" * 60)

    csv_path = find_csv()
    logger.info(f"Reading: {csv_path}")

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    logger.info(f"Total rows in CSV: {len(rows)}")
    logger.info(f"Columns: {list(rows[0].keys()) if rows else 'EMPTY'}")

    # Stats
    dirs = Counter(r.get("Direction", "") for r in rows)
    statuses = Counter(r.get("Status", "") for r in rows)
    dates = Counter(r.get("StartTime", "")[:10] for r in rows)

    logger.info(f"Direction breakdown: {dict(dirs)}")
    logger.info(f"Status breakdown:    {dict(statuses)}")
    logger.info(f"Date breakdown:      {dict(sorted(dates.items()))}")

    # Filter: inbound + ConversationDuration >= 120s + has recording URL
    filtered = []
    for r in rows:
        if r.get("Direction", "").lower() != "inbound":
            continue

        # Use ConversationDuration (actual talk time) if available, else Duration
        conv_dur = int(r.get("ConversationDuration") or 0)
        dur = int(r.get("Duration") or 0)
        effective_duration = conv_dur if conv_dur > 0 else dur

        if effective_duration < MIN_DURATION:
            continue

        rec_url = (r.get("RecordingUrls") or "").strip()
        if not rec_url or not rec_url.startswith("http"):
            continue

        filtered.append({
            "call_sid": r.get("Id", ""),
            "direction": r.get("Direction", ""),
            "status": r.get("Status", ""),
            "duration_seconds": dur,
            "conversation_duration_seconds": conv_dur,
            "duration_minutes": round(effective_duration / 60, 2),
            "start_time": r.get("StartTime", ""),
            "end_time": r.get("EndTime", ""),
            "from_number": r.get("From", ""),
            "to_number": r.get("To", ""),
            "exotel_number": r.get("ExotelNumber", ""),
            "recording_url": rec_url,
            "has_recording": True,
            "app_name": r.get("AppName", ""),
            "app_id": r.get("AppID", ""),
            "from_circle": r.get("FromCircle", ""),
            "to_circle": r.get("ToCircle", ""),
            "disconnected_by": r.get("DisconnectedBy", ""),
            "group_name": r.get("GroupName", ""),
        })

    # Sort newest first
    filtered.sort(key=lambda c: c.get("start_time", ""), reverse=True)

    with open(OUTPUT_FILE, "w") as f:
        json.dump(filtered, f, indent=2, default=str)

    total_min = sum(c["duration_minutes"] for c in filtered)
    date_breakdown = Counter(c["start_time"][:10] for c in filtered)

    logger.info(f"\n{'='*60}")
    logger.info(f"FILTER RESULTS:")
    logger.info(f"  Total CSV rows:              {len(rows)}")
    logger.info(f"  Inbound + >2min + recording: {len(filtered)}")
    logger.info(f"  Total audio minutes:         {total_min:.0f}")
    logger.info(f"  Day breakdown:")
    for d, count in sorted(date_breakdown.items()):
        logger.info(f"    {d}: {count} calls")
    logger.info(f"\n  Saved → {OUTPUT_FILE}")
    logger.info(f"{'='*60}")

    stats = {
        "total_rows": len(rows),
        "filtered": len(filtered),
        "total_audio_minutes": round(total_min),
        "day_breakdown": dict(sorted(date_breakdown.items())),
    }
    with open(STATS_FILE, "w") as f:
        json.dump(stats, f, indent=2)

    return filtered


if __name__ == "__main__":
    calls = run()
    print(f"\n✅ Agent 1: {len(calls)} inbound calls >2min with recordings ready")
