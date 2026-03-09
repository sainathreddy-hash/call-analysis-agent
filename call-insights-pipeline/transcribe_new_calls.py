"""
Transcribe new calls from CSV that are not already in transcribed_calls.json.
Reads CSV, filters out existing call SIDs, transcribes via Deepgram, merges output.
"""

import os
import sys
import json
import csv
import asyncio
import logging
import time
from pathlib import Path
from typing import Optional

import aiohttp
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("NewCallTranscriber")

DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY")
EXOTEL_API_KEY = os.getenv("EXOTEL_API_KEY")
EXOTEL_API_TOKEN = os.getenv("EXOTEL_API_TOKEN")
CONCURRENCY = 80
DEEPGRAM_URL = "https://api.deepgram.com/v1/listen"

CSV_FILE = BASE_DIR.parent / "6148e397126840f6133dbec8c21ce0c1.csv"
EXISTING_FILE = BASE_DIR / "output" / "transcribed_calls.json"
OUTPUT_FILE = BASE_DIR / "output" / "transcribed_calls.json"
CHECKPOINT_FILE = BASE_DIR / "output" / "new_transcriptions_checkpoint.json"


def load_existing_sids():
    """Load call SIDs already transcribed."""
    if not EXISTING_FILE.exists():
        return set()
    data = json.loads(EXISTING_FILE.read_text())
    return set(c.get("call_sid", "") for c in data)


def parse_csv():
    """Parse CSV and convert to pipeline format, filtering out existing calls."""
    existing = load_existing_sids()
    logger.info(f"Existing transcribed calls: {len(existing)}")

    # Load checkpoint if exists (partially completed runs)
    checkpoint_sids = set()
    checkpoint_calls = []
    if CHECKPOINT_FILE.exists():
        checkpoint_calls = json.loads(CHECKPOINT_FILE.read_text())
        checkpoint_sids = set(c.get("call_sid", "") for c in checkpoint_calls)
        logger.info(f"Checkpoint: {len(checkpoint_calls)} already transcribed in previous run")

    calls = []
    with open(CSV_FILE, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            sid = row.get("Id", "").strip()
            if not sid or sid in existing or sid in checkpoint_sids:
                continue

            rec_url = row.get("RecordingUrls", "").strip()
            conv_dur = int(row.get("ConversationDuration", "0") or "0")

            # Skip calls without recordings or very short conversations
            if not rec_url or conv_dur < 10:
                continue

            calls.append({
                "call_sid": sid,
                "direction": row.get("Direction", ""),
                "from_number": row.get("From", ""),
                "to_number": row.get("To", ""),
                "status": row.get("Status", ""),
                "start_time": row.get("StartTime", ""),
                "end_time": row.get("EndTime", ""),
                "duration": int(row.get("Duration", "0") or "0"),
                "conversation_duration": conv_dur,
                "recording_url": rec_url,
                "has_recording": True,
                "group_name": row.get("GroupName", ""),
                "from_circle": row.get("FromCircle", ""),
                "to_circle": row.get("ToCircle", ""),
                "disconnected_by": row.get("DisconnectedBy", ""),
                "leg1_status": row.get("Leg1Status", ""),
                "leg2_status": row.get("Leg2Status", ""),
                "app_name": row.get("AppName", ""),
            })

    logger.info(f"New calls to transcribe: {len(calls)}")
    return calls, checkpoint_calls


class Transcriber:
    def __init__(self):
        self.semaphore = asyncio.Semaphore(CONCURRENCY)
        self.session = None
        self.stats = {"total": 0, "success": 0, "failed": 0, "skipped": 0}
        self.lock = asyncio.Lock()

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            headers={"Authorization": f"Token {DEEPGRAM_API_KEY}", "Content-Type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=120, connect=15),
        )
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()

    async def transcribe(self, call, idx, total):
        async with self.semaphore:
            sid = call["call_sid"]
            url = call["recording_url"]

            try:
                payload = {"url": url}
                params = {
                    "model": "nova-3",
                    "language": "hi",
                    "detect_language": "true",
                    "smart_format": "true",
                    "diarize": "true",
                    "punctuate": "true",
                    "paragraphs": "true",
                    "utterances": "true",
                    "filler_words": "false",
                }

                for attempt in range(3):
                    try:
                        async with self.session.post(DEEPGRAM_URL, json=payload, params=params) as resp:
                            if resp.status == 429:
                                await asyncio.sleep(2 * (attempt + 1))
                                continue
                            if resp.status != 200:
                                body = await resp.text()
                                if attempt < 2:
                                    await asyncio.sleep(1)
                                    continue
                                raise RuntimeError(f"Deepgram {resp.status}: {body[:200]}")
                            result = await resp.json()
                            break
                    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                        if attempt < 2:
                            await asyncio.sleep(1)
                            continue
                        raise
                else:
                    raise RuntimeError("All retries failed")

                # Parse response
                transcript_data = self._parse(result)
                async with self.lock:
                    self.stats["success"] += 1
                    done = self.stats["success"] + self.stats["failed"]
                    if done % 50 == 0 or done == total:
                        logger.info(f"Progress: {done}/{total} ({self.stats['success']} ok, {self.stats['failed']} fail)")

                return {**call, "transcript_data": transcript_data, "transcription_status": "success"}

            except Exception as e:
                async with self.lock:
                    self.stats["failed"] += 1
                if idx < 5 or self.stats["failed"] < 10:
                    logger.error(f"[{idx}/{total}] {sid}: {e}")
                return {**call, "transcript_data": None, "transcription_status": f"error: {str(e)[:100]}"}

    def _parse(self, result):
        try:
            channels = result.get("results", {}).get("channels", [])
            if not channels:
                return {"transcript": "", "speakers": [], "utterances": [], "language_detected": "unknown"}

            alt = channels[0].get("alternatives", [{}])[0]
            transcript = alt.get("transcript", "")
            paragraphs_data = alt.get("paragraphs", {}).get("paragraphs", [])

            utterances = []
            for para in paragraphs_data:
                speaker = para.get("speaker", 0)
                for sent in para.get("sentences", []):
                    utterances.append({
                        "speaker": f"Speaker {speaker}",
                        "text": sent.get("text", ""),
                        "start": sent.get("start", 0),
                        "end": sent.get("end", 0),
                    })

            if not utterances:
                for utt in result.get("results", {}).get("utterances", []):
                    utterances.append({
                        "speaker": f"Speaker {utt.get('speaker', 0)}",
                        "text": utt.get("transcript", ""),
                        "start": utt.get("start", 0),
                        "end": utt.get("end", 0),
                    })

            full_transcript = "\n".join(f"[{u['speaker']}]: {u['text']}" for u in utterances) if utterances else transcript
            detected_lang = channels[0].get("detected_language", "unknown")
            unique_speakers = list(set(u["speaker"] for u in utterances))

            return {
                "transcript": full_transcript,
                "transcript_raw": transcript,
                "speakers": unique_speakers,
                "num_speakers": len(unique_speakers),
                "utterances": utterances,
                "language_detected": detected_lang,
                "word_count": len(transcript.split()) if transcript else 0,
            }
        except Exception as e:
            return {"transcript": "", "error": str(e)}


async def main():
    calls, checkpoint_calls = parse_csv()
    if not calls:
        logger.info("No new calls to transcribe!")
        return

    total = len(calls)
    logger.info(f"Starting transcription of {total} calls with {CONCURRENCY} concurrent workers...")
    start = time.time()

    async with Transcriber() as t:
        # Process in batches of 500 with checkpointing
        BATCH = 500
        all_results = list(checkpoint_calls)  # Start with checkpoint

        for batch_start in range(0, total, BATCH):
            batch = calls[batch_start:batch_start + BATCH]
            tasks = [t.transcribe(c, batch_start + i, total) for i, c in enumerate(batch)]
            results = await asyncio.gather(*tasks)
            all_results.extend(results)

            # Checkpoint after each batch
            with open(CHECKPOINT_FILE, 'w') as f:
                json.dump(all_results, f)
            elapsed = time.time() - start
            logger.info(f"Batch done. Total: {len(all_results)}, Elapsed: {elapsed:.0f}s")

    elapsed = time.time() - start
    logger.info(f"Transcription done in {elapsed:.0f}s — {t.stats}")

    # Merge with existing
    logger.info("Merging with existing transcribed_calls.json...")
    existing = json.loads(EXISTING_FILE.read_text()) if EXISTING_FILE.exists() else []
    merged = existing + all_results
    logger.info(f"Merged: {len(existing)} existing + {len(all_results)} new = {len(merged)} total")

    with open(OUTPUT_FILE, 'w') as f:
        json.dump(merged, f)
    logger.info(f"Saved to {OUTPUT_FILE}")

    # Clean checkpoint
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()


if __name__ == "__main__":
    asyncio.run(main())
