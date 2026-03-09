"""
Agent 3: Call Analyzer (Deep Insights Engine)
──────────────────────────────────────────────
Reads transcribed_calls.json from Agent 2, analyzes each transcript
using Claude for deep word-by-word insights.

Produces a comprehensive CSV with:
- Call metadata (SID, duration, from/to, timestamps)
- Full transcript
- Sentiment analysis (caller & agent)
- Key issues / topics discussed
- Agent performance score
- Red flags & escalation triggers
- Customer satisfaction indicators
- Action items identified
- Language / tone breakdown
"""

import os
import sys
import json
import asyncio
import logging
import time
import csv
from pathlib import Path
from typing import Optional
from datetime import datetime

import anthropic
from dotenv import load_dotenv

# ─── Setup ────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(BASE_DIR / "logs" / "call_analyzer.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("CallAnalyzer")

# ─── Config ───────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
ANALYSIS_CONCURRENCY = 10  # Claude API rate limits are stricter than Deepgram

INPUT_FILE = BASE_DIR / "output" / "transcribed_calls.json"
OUTPUT_CSV = BASE_DIR / "output" / "call_insights.csv"
OUTPUT_JSON = BASE_DIR / "output" / "call_insights.json"
STATS_FILE = BASE_DIR / "output" / "analysis_stats.json"


ANALYSIS_PROMPT = """You are an expert call center quality analyst. Analyze the following customer service call transcript with extreme depth and precision. Examine every single word, pause, and interaction pattern.

<call_metadata>
Call SID: {call_sid}
Direction: {direction}
Duration: {duration_minutes} minutes
From: {from_number}
To: {to_number}
Start Time: {start_time}
Number of Speakers: {num_speakers}
Language: {language}
</call_metadata>

<transcript>
{transcript}
</transcript>

Provide your analysis in EXACTLY this JSON format (no markdown, no code blocks, just raw JSON):
{{
  "overall_sentiment": "positive|negative|neutral|mixed",
  "caller_sentiment": "positive|negative|neutral|frustrated|angry|confused|satisfied|anxious",
  "agent_sentiment": "professional|empathetic|dismissive|helpful|rushed|patient|robotic",
  "caller_tone_details": "2-3 sentence description of caller's emotional journey through the call",
  "agent_tone_details": "2-3 sentence description of agent's communication style and tone shifts",
  "primary_topic": "main reason for the call in 5-10 words",
  "all_topics_discussed": ["topic1", "topic2", "topic3"],
  "key_issues_identified": ["issue1", "issue2"],
  "customer_complaint": "exact complaint if any, or 'None'",
  "resolution_status": "resolved|partially_resolved|unresolved|escalated|transferred",
  "resolution_details": "what was the outcome in 1-2 sentences",
  "agent_performance_score": 0,
  "agent_performance_breakdown": {{
    "greeting_professionalism": 0,
    "active_listening": 0,
    "problem_understanding": 0,
    "solution_quality": 0,
    "empathy_shown": 0,
    "call_control": 0,
    "closing_quality": 0
  }},
  "agent_strengths": ["strength1", "strength2"],
  "agent_improvement_areas": ["area1", "area2"],
  "red_flags": ["any concerning patterns, policy violations, or escalation triggers"],
  "customer_satisfaction_estimate": 0,
  "action_items": ["follow-up actions needed"],
  "hold_time_mentioned": false,
  "transfer_occurred": false,
  "callback_promised": false,
  "product_service_mentioned": ["product or service names discussed"],
  "competitor_mentioned": ["any competitor names mentioned"],
  "word_level_insights": "Deep analysis of specific words/phrases that reveal customer intent, hidden frustrations, or agent effectiveness. Quote specific parts of the transcript.",
  "call_category": "complaint|inquiry|support|billing|technical|feedback|cancellation|onboarding|other",
  "urgency_level": "low|medium|high|critical",
  "summary": "3-5 sentence executive summary of the entire call"
}}

IMPORTANT: All score fields (agent_performance_score, customer_satisfaction_estimate, and all breakdown scores) must be integers from 1 to 10. Return ONLY valid JSON."""


def validate_config():
    if not ANTHROPIC_API_KEY:
        logger.error("ANTHROPIC_API_KEY not set")
        sys.exit(1)
    if not INPUT_FILE.exists():
        logger.error(f"Input file not found: {INPUT_FILE}")
        logger.error("Run Agent 2 (deepgram_transcriber.py) first!")
        sys.exit(1)
    logger.info(f"Config OK — Analysis concurrency={ANALYSIS_CONCURRENCY}")


class CallAnalyzer:
    """Analyzes call transcripts using Claude for deep insights."""

    def __init__(self):
        self.client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
        self.semaphore = asyncio.Semaphore(ANALYSIS_CONCURRENCY)
        self.stats = {
            "total_calls": 0,
            "analyzed": 0,
            "failed": 0,
            "skipped_no_transcript": 0,
            "errors": [],
        }
        self.lock = asyncio.Lock()

    async def analyze_call(self, call: dict, index: int, total: int) -> dict:
        """Analyze a single call transcript."""
        async with self.semaphore:
            call_sid = call.get("call_sid", "unknown")
            transcript_data = call.get("transcript_data")

            if not transcript_data or not transcript_data.get("transcript"):
                async with self.lock:
                    self.stats["skipped_no_transcript"] += 1
                logger.warning(f"[{index}/{total}] {call_sid}: No transcript — skipping analysis")
                return {**call, "analysis": None, "analysis_status": "no_transcript"}

            transcript = transcript_data.get("transcript", "")

            # Skip very short transcripts (likely noise)
            if len(transcript.split()) < 10:
                async with self.lock:
                    self.stats["skipped_no_transcript"] += 1
                logger.warning(f"[{index}/{total}] {call_sid}: Transcript too short ({len(transcript.split())} words)")
                return {**call, "analysis": None, "analysis_status": "too_short"}

            prompt = ANALYSIS_PROMPT.format(
                call_sid=call_sid,
                direction=call.get("direction", "unknown"),
                duration_minutes=call.get("duration_minutes", 0),
                from_number=call.get("from_number", ""),
                to_number=call.get("to_number", ""),
                start_time=call.get("start_time", ""),
                num_speakers=transcript_data.get("num_speakers", 0),
                language=transcript_data.get("language_detected", "unknown"),
                transcript=transcript[:15000],  # Cap to avoid token overflow
            )

            start_time = time.time()
            logger.info(f"[{index}/{total}] {call_sid}: Analyzing ({len(transcript.split())} words)...")

            try:
                # Call Claude with retry logic
                for attempt in range(3):
                    try:
                        response = await self.client.messages.create(
                            model="claude-sonnet-4-5-20250929",
                            max_tokens=4096,
                            messages=[{"role": "user", "content": prompt}],
                        )
                        break
                    except anthropic.RateLimitError:
                        wait_time = (attempt + 1) * 5
                        logger.warning(f"[{index}/{total}] Rate limited, waiting {wait_time}s...")
                        await asyncio.sleep(wait_time)
                    except anthropic.APIConnectionError as e:
                        if attempt == 2:
                            raise
                        await asyncio.sleep(2)

                # Parse JSON response
                raw_text = response.content[0].text.strip()

                # Clean up potential markdown wrapping
                if raw_text.startswith("```"):
                    raw_text = raw_text.split("\n", 1)[1] if "\n" in raw_text else raw_text[3:]
                if raw_text.endswith("```"):
                    raw_text = raw_text[:-3]
                if raw_text.startswith("json"):
                    raw_text = raw_text[4:]
                raw_text = raw_text.strip()

                analysis = json.loads(raw_text)
                elapsed = time.time() - start_time

                async with self.lock:
                    self.stats["analyzed"] += 1

                logger.info(
                    f"[{index}/{total}] {call_sid}: ✅ Analyzed in {elapsed:.1f}s "
                    f"(sentiment={analysis.get('overall_sentiment')}, "
                    f"score={analysis.get('agent_performance_score')}/10)"
                )

                return {**call, "analysis": analysis, "analysis_status": "success"}

            except json.JSONDecodeError as e:
                elapsed = time.time() - start_time
                async with self.lock:
                    self.stats["failed"] += 1
                    self.stats["errors"].append(f"{call_sid}: JSON parse error — {e}")
                logger.error(f"[{index}/{total}] {call_sid}: ❌ JSON parse error — {e}")
                # Return raw text as fallback
                return {
                    **call,
                    "analysis": {"raw_response": raw_text[:2000], "parse_error": str(e)},
                    "analysis_status": "parse_error",
                }

            except Exception as e:
                elapsed = time.time() - start_time
                async with self.lock:
                    self.stats["failed"] += 1
                    self.stats["errors"].append(f"{call_sid}: {str(e)}")
                logger.error(f"[{index}/{total}] {call_sid}: ❌ Failed — {e}")
                return {**call, "analysis": None, "analysis_status": "failed", "analysis_error": str(e)}

    async def analyze_all(self, calls: list[dict]) -> list[dict]:
        """Analyze all calls with controlled concurrency."""
        self.stats["total_calls"] = len(calls)
        logger.info(f"Starting analysis of {len(calls)} calls with {ANALYSIS_CONCURRENCY} concurrent workers")

        tasks = [
            self.analyze_call(call, i + 1, len(calls))
            for i, call in enumerate(calls)
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        processed = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Unexpected error for call {i}: {result}")
                self.stats["failed"] += 1
                processed.append({**calls[i], "analysis": None, "analysis_status": "error"})
            else:
                processed.append(result)

        return processed


def generate_csv(results: list[dict], output_path: Path):
    """Generate comprehensive CSV from analyzed calls."""
    CSV_COLUMNS = [
        "call_sid", "direction", "status", "duration_seconds", "duration_minutes",
        "start_time", "end_time", "from_number", "to_number",
        "recording_url", "transcription_status", "transcription_time_seconds",
        "language_detected", "num_speakers", "word_count",
        "full_transcript",
        "analysis_status",
        "overall_sentiment", "caller_sentiment", "agent_sentiment",
        "caller_tone_details", "agent_tone_details",
        "primary_topic", "all_topics_discussed", "key_issues_identified",
        "customer_complaint", "resolution_status", "resolution_details",
        "agent_performance_score",
        "greeting_professionalism", "active_listening", "problem_understanding",
        "solution_quality", "empathy_shown", "call_control", "closing_quality",
        "agent_strengths", "agent_improvement_areas",
        "red_flags", "customer_satisfaction_estimate",
        "action_items", "hold_time_mentioned", "transfer_occurred", "callback_promised",
        "product_service_mentioned", "competitor_mentioned",
        "word_level_insights", "call_category", "urgency_level", "summary",
    ]

    rows = []
    for r in results:
        analysis = r.get("analysis") or {}
        transcript_data = r.get("transcript_data") or {}
        breakdown = analysis.get("agent_performance_breakdown", {})

        row = {
            # Call metadata
            "call_sid": r.get("call_sid", ""),
            "direction": r.get("direction", ""),
            "status": r.get("status", ""),
            "duration_seconds": r.get("duration_seconds", 0),
            "duration_minutes": r.get("duration_minutes", 0),
            "start_time": r.get("start_time", ""),
            "end_time": r.get("end_time", ""),
            "from_number": r.get("from_number", ""),
            "to_number": r.get("to_number", ""),
            "recording_url": r.get("recording_url", ""),
            # Transcription
            "transcription_status": r.get("transcription_status", ""),
            "transcription_time_seconds": r.get("transcription_time_seconds", ""),
            "language_detected": transcript_data.get("language_detected", ""),
            "num_speakers": transcript_data.get("num_speakers", 0),
            "word_count": transcript_data.get("word_count", 0),
            "full_transcript": transcript_data.get("transcript", ""),
            # Analysis
            "analysis_status": r.get("analysis_status", ""),
            "overall_sentiment": analysis.get("overall_sentiment", ""),
            "caller_sentiment": analysis.get("caller_sentiment", ""),
            "agent_sentiment": analysis.get("agent_sentiment", ""),
            "caller_tone_details": analysis.get("caller_tone_details", ""),
            "agent_tone_details": analysis.get("agent_tone_details", ""),
            "primary_topic": analysis.get("primary_topic", ""),
            "all_topics_discussed": "; ".join(analysis.get("all_topics_discussed", [])),
            "key_issues_identified": "; ".join(analysis.get("key_issues_identified", [])),
            "customer_complaint": analysis.get("customer_complaint", ""),
            "resolution_status": analysis.get("resolution_status", ""),
            "resolution_details": analysis.get("resolution_details", ""),
            "agent_performance_score": analysis.get("agent_performance_score", ""),
            "greeting_professionalism": breakdown.get("greeting_professionalism", ""),
            "active_listening": breakdown.get("active_listening", ""),
            "problem_understanding": breakdown.get("problem_understanding", ""),
            "solution_quality": breakdown.get("solution_quality", ""),
            "empathy_shown": breakdown.get("empathy_shown", ""),
            "call_control": breakdown.get("call_control", ""),
            "closing_quality": breakdown.get("closing_quality", ""),
            "agent_strengths": "; ".join(analysis.get("agent_strengths", [])),
            "agent_improvement_areas": "; ".join(analysis.get("agent_improvement_areas", [])),
            "red_flags": "; ".join(analysis.get("red_flags", [])),
            "customer_satisfaction_estimate": analysis.get("customer_satisfaction_estimate", ""),
            "action_items": "; ".join(analysis.get("action_items", [])),
            "hold_time_mentioned": analysis.get("hold_time_mentioned", ""),
            "transfer_occurred": analysis.get("transfer_occurred", ""),
            "callback_promised": analysis.get("callback_promised", ""),
            "product_service_mentioned": "; ".join(analysis.get("product_service_mentioned", [])),
            "competitor_mentioned": "; ".join(analysis.get("competitor_mentioned", [])),
            "word_level_insights": analysis.get("word_level_insights", ""),
            "call_category": analysis.get("call_category", ""),
            "urgency_level": analysis.get("urgency_level", ""),
            "summary": analysis.get("summary", ""),
        }
        rows.append(row)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    logger.info(f"CSV written: {output_path} ({len(rows)} rows, {len(CSV_COLUMNS)} columns)")


async def run():
    """Main entry point for Agent 3."""
    validate_config()

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    (BASE_DIR / "logs").mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("AGENT 3: Call Analyzer — Starting")
    logger.info("=" * 60)

    # Load transcribed calls from Agent 2
    with open(INPUT_FILE, "r") as f:
        calls = json.load(f)

    # Only analyze successfully transcribed calls
    transcribed = [c for c in calls if c.get("transcription_status") == "success"]
    logger.info(f"Loaded {len(calls)} calls, {len(transcribed)} with successful transcripts")

    if not transcribed:
        logger.warning("No transcribed calls to analyze. Exiting.")
        return []

    overall_start = time.time()

    analyzer = CallAnalyzer()
    results = await analyzer.analyze_all(transcribed)

    overall_elapsed = time.time() - overall_start

    # Save full JSON results
    with open(OUTPUT_JSON, "w") as f:
        json.dump(results, f, indent=2, default=str)
    logger.info(f"Full results saved to {OUTPUT_JSON}")

    # Generate CSV
    generate_csv(results, OUTPUT_CSV)

    # Save stats
    analyzer.stats["total_wall_time_seconds"] = round(overall_elapsed, 2)
    with open(STATS_FILE, "w") as f:
        json.dump(analyzer.stats, f, indent=2)

    # Summary
    analyzed = [r for r in results if r.get("analysis_status") == "success"]
    logger.info(f"\n{'='*60}")
    logger.info(f"ANALYSIS SUMMARY")
    logger.info(f"  Total calls:     {analyzer.stats['total_calls']}")
    logger.info(f"  Analyzed:        {analyzer.stats['analyzed']}")
    logger.info(f"  Failed:          {analyzer.stats['failed']}")
    logger.info(f"  Skipped:         {analyzer.stats['skipped_no_transcript']}")
    logger.info(f"  Wall time:       {overall_elapsed:.1f}s")
    if analyzed:
        avg_score = sum(a["analysis"].get("agent_performance_score", 0) for a in analyzed) / len(analyzed)
        logger.info(f"  Avg agent score: {avg_score:.1f}/10")
        sentiments = {}
        for a in analyzed:
            s = a["analysis"].get("overall_sentiment", "unknown")
            sentiments[s] = sentiments.get(s, 0) + 1
        logger.info(f"  Sentiments:      {sentiments}")
    logger.info(f"{'='*60}")
    logger.info(f"\n📊 CSV output: {OUTPUT_CSV}")

    return results


if __name__ == "__main__":
    results = asyncio.run(run())
    success = sum(1 for r in results if r.get("analysis_status") == "success")
    print(f"\n✅ Agent 3 complete: {success}/{len(results)} calls analyzed")
    print(f"📊 CSV: {OUTPUT_CSV}")
