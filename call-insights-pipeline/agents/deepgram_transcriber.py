"""
Agent 2: Deepgram Transcriber
──────────────────────────────
Reads filtered_calls.json from Agent 1, transcribes all recordings
using Deepgram's pre-recorded API with 100 concurrent workers.

Each call takes ~2-3 seconds to transcribe, so we run 100 in parallel
for massive throughput.

Output: transcribed_calls.json (call metadata + full transcript + speakers)
"""

import os
import sys
import json
import asyncio
import logging
import time
from pathlib import Path
from typing import Optional

import aiohttp
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# ─── Setup ────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(BASE_DIR / "logs" / "deepgram_transcriber.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("DeepgramTranscriber")

# ─── Config ───────────────────────────────────────────────────────────────────
DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY")
EXOTEL_API_KEY = os.getenv("EXOTEL_API_KEY")
EXOTEL_API_TOKEN = os.getenv("EXOTEL_API_TOKEN")
CONCURRENCY = int(os.getenv("CONCURRENCY_LIMIT", "100"))

INPUT_FILE = BASE_DIR / "output" / "filtered_calls.json"
OUTPUT_FILE = BASE_DIR / "output" / "transcribed_calls.json"
STATS_FILE = BASE_DIR / "output" / "transcription_stats.json"

DEEPGRAM_URL = "https://api.deepgram.com/v1/listen"


def validate_config():
    if not DEEPGRAM_API_KEY:
        logger.error("DEEPGRAM_API_KEY not set")
        sys.exit(1)
    if not INPUT_FILE.exists():
        logger.error(f"Input file not found: {INPUT_FILE}")
        logger.error("Run Agent 1 (exotel_fetcher.py) first!")
        sys.exit(1)
    logger.info(f"Config OK — Concurrency={CONCURRENCY}")


class DeepgramTranscriber:
    """Async Deepgram transcription engine with concurrency control."""

    def __init__(self):
        self.semaphore = asyncio.Semaphore(CONCURRENCY)
        self.session: Optional[aiohttp.ClientSession] = None
        self.exotel_session: Optional[aiohttp.ClientSession] = None
        self.stats = {
            "total_calls": 0,
            "transcribed": 0,
            "failed": 0,
            "skipped_no_recording": 0,
            "total_audio_minutes": 0,
            "total_transcription_time_seconds": 0,
            "errors": [],
        }
        self.progress_lock = asyncio.Lock()

    async def __aenter__(self):
        # Deepgram session
        self.session = aiohttp.ClientSession(
            headers={
                "Authorization": f"Token {DEEPGRAM_API_KEY}",
                "Content-Type": "application/json",
            },
            timeout=aiohttp.ClientTimeout(total=120, connect=15),
        )
        # Exotel session for downloading recordings
        if EXOTEL_API_KEY and EXOTEL_API_TOKEN:
            self.exotel_session = aiohttp.ClientSession(
                auth=aiohttp.BasicAuth(EXOTEL_API_KEY, EXOTEL_API_TOKEN),
                timeout=aiohttp.ClientTimeout(total=120, connect=15),
            )
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()
        if self.exotel_session:
            await self.exotel_session.close()

    def _build_recording_url(self, call: dict) -> Optional[str]:
        """Build the full recording URL from Exotel call data."""
        rec_url = call.get("recording_url", "")
        if not rec_url:
            return None

        # Exotel recording URLs can be:
        # 1. Full URL: https://...
        # 2. Relative path: /v1/Accounts/SID/Recordings/...
        if rec_url.startswith("http"):
            return rec_url

        subdomain = os.getenv("EXOTEL_SUBDOMAIN", "api.exotel.com")
        sid = os.getenv("EXOTEL_SID")
        # Build full URL
        if rec_url.startswith("/"):
            return f"https://{subdomain}{rec_url}"
        else:
            return f"https://{subdomain}/v1/Accounts/{sid}/Recordings/{rec_url}"

    async def _download_recording(self, url: str) -> Optional[bytes]:
        """Download recording audio from Exotel (needs auth)."""
        session = self.exotel_session or self.session
        try:
            # Try with .mp3 extension if not present
            urls_to_try = [url]
            if not url.endswith(('.mp3', '.wav')):
                urls_to_try.append(f"{url}.mp3")

            for try_url in urls_to_try:
                async with session.get(try_url, allow_redirects=True) as resp:
                    if resp.status == 200:
                        content_type = resp.headers.get("Content-Type", "")
                        if "audio" in content_type or "octet" in content_type or resp.content_length:
                            data = await resp.read()
                            if len(data) > 1000:  # Sanity check: not an error page
                                return data
                    elif resp.status in (301, 302):
                        redirect_url = resp.headers.get("Location", "")
                        if redirect_url:
                            async with session.get(redirect_url) as rresp:
                                if rresp.status == 200:
                                    return await rresp.read()
            return None
        except Exception as e:
            logger.error(f"Failed to download recording {url}: {e}")
            return None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
    )
    async def _transcribe_url(self, audio_url: str) -> dict:
        """Send audio URL to Deepgram for transcription."""
        payload = {"url": audio_url}
        params = {
            "model": "nova-3",           # Best available Deepgram model
            "language": "hi",            # Hindi (primary for Indian calls)
            "detect_language": "true",   # Auto-detect if not Hindi
            "smart_format": "true",
            "diarize": "true",           # Speaker separation
            "punctuate": "true",
            "paragraphs": "true",
            "utterances": "true",        # Get per-utterance timestamps
            "filler_words": "false",
        }

        async with self.session.post(DEEPGRAM_URL, json=payload, params=params) as resp:
            if resp.status == 401:
                raise RuntimeError("Deepgram auth failed — check DEEPGRAM_API_KEY")
            if resp.status == 402:
                raise RuntimeError("Deepgram quota exceeded — check billing")
            if resp.status == 429:
                await asyncio.sleep(2)
                raise aiohttp.ClientError("Rate limited")
            if resp.status != 200:
                body = await resp.text()
                raise RuntimeError(f"Deepgram error {resp.status}: {body[:300]}")
            return await resp.json()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
    )
    async def _transcribe_audio_bytes(self, audio_data: bytes) -> dict:
        """Send raw audio bytes to Deepgram for transcription."""
        params = {
            "model": "nova-3",           # Best available Deepgram model
            "language": "hi",
            "detect_language": "true",
            "smart_format": "true",
            "diarize": "true",
            "punctuate": "true",
            "paragraphs": "true",
            "utterances": "true",
            "filler_words": "false",
        }

        headers = {
            "Authorization": f"Token {DEEPGRAM_API_KEY}",
            "Content-Type": "audio/mp3",
        }

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=120)
        ) as temp_session:
            async with temp_session.post(
                DEEPGRAM_URL, data=audio_data, params=params, headers=headers
            ) as resp:
                if resp.status == 401:
                    raise RuntimeError("Deepgram auth failed")
                if resp.status == 429:
                    await asyncio.sleep(2)
                    raise aiohttp.ClientError("Rate limited")
                if resp.status != 200:
                    body = await resp.text()
                    raise RuntimeError(f"Deepgram error {resp.status}: {body[:300]}")
                return await resp.json()

    def _parse_deepgram_response(self, result: dict) -> dict:
        """Parse Deepgram response into clean transcript data."""
        try:
            channels = result.get("results", {}).get("channels", [])
            if not channels:
                return {"transcript": "", "speakers": [], "utterances": [], "language": "unknown"}

            alt = channels[0].get("alternatives", [{}])[0]
            transcript = alt.get("transcript", "")
            paragraphs_data = alt.get("paragraphs", {}).get("paragraphs", [])

            # Build speaker-labeled transcript
            utterances = []
            for para in paragraphs_data:
                speaker = para.get("speaker", 0)
                sentences = para.get("sentences", [])
                for sent in sentences:
                    utterances.append({
                        "speaker": f"Speaker {speaker}",
                        "text": sent.get("text", ""),
                        "start": sent.get("start", 0),
                        "end": sent.get("end", 0),
                    })

            # Also try utterances from top-level
            if not utterances:
                for utt in result.get("results", {}).get("utterances", []):
                    utterances.append({
                        "speaker": f"Speaker {utt.get('speaker', 0)}",
                        "text": utt.get("transcript", ""),
                        "start": utt.get("start", 0),
                        "end": utt.get("end", 0),
                    })

            # Build full speaker-labeled transcript text
            full_transcript_lines = []
            for u in utterances:
                full_transcript_lines.append(f"[{u['speaker']}]: {u['text']}")
            full_transcript = "\n".join(full_transcript_lines) if full_transcript_lines else transcript

            # Detect language
            detected_lang = (
                result.get("results", {})
                .get("channels", [{}])[0]
                .get("detected_language", "unknown")
            )

            # Count unique speakers
            unique_speakers = list(set(u["speaker"] for u in utterances)) if utterances else []

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
            logger.error(f"Error parsing Deepgram response: {e}")
            return {"transcript": "", "error": str(e)}

    async def transcribe_call(self, call: dict, index: int, total: int) -> dict:
        """Transcribe a single call using the semaphore for concurrency control."""
        async with self.semaphore:
            call_sid = call.get("call_sid", "unknown")
            recording_url = self._build_recording_url(call)

            if not recording_url:
                async with self.progress_lock:
                    self.stats["skipped_no_recording"] += 1
                logger.warning(f"[{index}/{total}] {call_sid}: No recording URL — skipping")
                return {**call, "transcript_data": None, "transcription_status": "no_recording"}

            start_time = time.time()
            logger.info(f"[{index}/{total}] {call_sid}: Transcribing ({call.get('duration_minutes', '?')} min)...")

            try:
                # Strategy 1: Try sending URL directly to Deepgram
                try:
                    result = await self._transcribe_url(recording_url)
                except Exception as url_err:
                    logger.info(f"[{index}/{total}] {call_sid}: URL method failed ({url_err}), trying download...")

                    # Strategy 2: Download audio from Exotel, then send bytes to Deepgram
                    audio_data = await self._download_recording(recording_url)
                    if not audio_data:
                        raise RuntimeError(f"Could not download recording from {recording_url}")
                    result = await self._transcribe_audio_bytes(audio_data)

                elapsed = time.time() - start_time
                transcript_data = self._parse_deepgram_response(result)

                async with self.progress_lock:
                    self.stats["transcribed"] += 1
                    self.stats["total_audio_minutes"] += call.get("duration_minutes", 0)
                    self.stats["total_transcription_time_seconds"] += elapsed

                logger.info(
                    f"[{index}/{total}] {call_sid}: ✅ Done in {elapsed:.1f}s "
                    f"({transcript_data.get('word_count', 0)} words, "
                    f"{transcript_data.get('num_speakers', 0)} speakers)"
                )

                return {
                    **call,
                    "transcript_data": transcript_data,
                    "transcription_status": "success",
                    "transcription_time_seconds": round(elapsed, 2),
                }

            except Exception as e:
                elapsed = time.time() - start_time
                async with self.progress_lock:
                    self.stats["failed"] += 1
                    self.stats["errors"].append(f"{call_sid}: {str(e)}")

                logger.error(f"[{index}/{total}] {call_sid}: ❌ Failed after {elapsed:.1f}s — {e}")
                return {
                    **call,
                    "transcript_data": None,
                    "transcription_status": "failed",
                    "transcription_error": str(e),
                }

    async def transcribe_all(self, calls: list[dict]) -> list[dict]:
        """Transcribe all calls with 100 concurrent workers."""
        self.stats["total_calls"] = len(calls)
        logger.info(f"Starting transcription of {len(calls)} calls with {CONCURRENCY} concurrent workers")

        # Create tasks for all calls
        tasks = [
            self.transcribe_call(call, i + 1, len(calls))
            for i, call in enumerate(calls)
        ]

        # Run all concurrently (semaphore limits to CONCURRENCY at a time)
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Handle any exceptions that weren't caught
        processed = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Unexpected error for call {i}: {result}")
                self.stats["failed"] += 1
                processed.append({
                    **calls[i],
                    "transcript_data": None,
                    "transcription_status": "error",
                    "transcription_error": str(result),
                })
            else:
                processed.append(result)

        return processed


async def run():
    """Main entry point for Agent 2."""
    validate_config()

    # Ensure output dirs exist
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    (BASE_DIR / "logs").mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("AGENT 2: Deepgram Transcriber — Starting")
    logger.info("=" * 60)

    # Load calls from Agent 1
    with open(INPUT_FILE, "r") as f:
        calls = json.load(f)
    logger.info(f"Loaded {len(calls)} calls from Agent 1")

    if not calls:
        logger.warning("No calls to transcribe. Exiting.")
        return []

    overall_start = time.time()

    async with DeepgramTranscriber() as transcriber:
        results = await transcriber.transcribe_all(calls)

        overall_elapsed = time.time() - overall_start

        # Save results
        with open(OUTPUT_FILE, "w") as f:
            json.dump(results, f, indent=2, default=str)
        logger.info(f"Saved {len(results)} transcribed calls to {OUTPUT_FILE}")

        # Save stats
        transcriber.stats["total_wall_time_seconds"] = round(overall_elapsed, 2)
        with open(STATS_FILE, "w") as f:
            json.dump(transcriber.stats, f, indent=2)

        # Summary
        logger.info(f"\n{'='*60}")
        logger.info(f"TRANSCRIPTION SUMMARY")
        logger.info(f"  Total calls:       {transcriber.stats['total_calls']}")
        logger.info(f"  Transcribed:       {transcriber.stats['transcribed']}")
        logger.info(f"  Failed:            {transcriber.stats['failed']}")
        logger.info(f"  No recording:      {transcriber.stats['skipped_no_recording']}")
        logger.info(f"  Audio minutes:     {transcriber.stats['total_audio_minutes']:.1f}")
        logger.info(f"  Wall time:         {overall_elapsed:.1f}s")
        logger.info(f"  Avg per call:      {transcriber.stats['total_transcription_time_seconds']/max(transcriber.stats['transcribed'],1):.1f}s")
        logger.info(f"{'='*60}")

    return results


if __name__ == "__main__":
    results = asyncio.run(run())
    success = sum(1 for r in results if r.get("transcription_status") == "success")
    print(f"\n✅ Agent 2 complete: {success}/{len(results)} calls transcribed")
