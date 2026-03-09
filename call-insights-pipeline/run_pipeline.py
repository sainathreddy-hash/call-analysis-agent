#!/usr/bin/env python3
"""
Call Insights Pipeline — Orchestrator
══════════════════════════════════════
Runs all 3 agents in sequence:

  Agent 1: Exotel Fetcher    → Fetches inbound calls >2min from last 3 days
  Agent 2: Deepgram Transcriber → Transcribes all calls (100 concurrent workers)
  Agent 3: Call Analyzer      → Deep AI analysis → CSV output

Usage:
  python run_pipeline.py              # Run full pipeline
  python run_pipeline.py --agent 1    # Run only Agent 1
  python run_pipeline.py --agent 2    # Run only Agent 2
  python run_pipeline.py --agent 3    # Run only Agent 3
  python run_pipeline.py --from 2     # Resume from Agent 2
"""

import sys
import os
import asyncio
import argparse
import time
import json
import logging
from pathlib import Path
from datetime import datetime

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent))

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(BASE_DIR / "logs" / "pipeline.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("Pipeline")


def print_banner():
    print("""
╔══════════════════════════════════════════════════════════════╗
║           CALL INSIGHTS PIPELINE v2.0                       ║
║                                                              ║
║  Agent 1: Exotel Fetcher (inbound connected calls)           ║
║  Agent 2: Deepgram Transcriber (nova-3, 100 workers)         ║
║  Agent 3: Call Analyzer (AI deep insights → CSV)             ║
║  Agent 4: Google Sheets Uploader (auto-append)               ║
╚══════════════════════════════════════════════════════════════╝
""")


def check_prerequisites():
    """Verify all required packages and env vars."""
    errors = []

    # Check env vars
    required_vars = {
        "Agent 1": ["EXOTEL_API_KEY", "EXOTEL_API_TOKEN", "EXOTEL_SID"],
        "Agent 2": ["DEEPGRAM_API_KEY"],
        "Agent 3": ["ANTHROPIC_API_KEY"],
    }

    for agent, vars_list in required_vars.items():
        for var in vars_list:
            if not os.getenv(var):
                errors.append(f"  {agent}: Missing {var}")

    if errors:
        print("\n⚠️  Missing environment variables:")
        for e in errors:
            print(e)
        print("\nSet them in .env file or export them.")
        return False

    # Check packages
    try:
        import aiohttp
        import anthropic
    except ImportError as e:
        print(f"\n⚠️  Missing package: {e}")
        print("Run: pip install -r requirements.txt")
        return False

    print("✅ All prerequisites checked")
    return True


async def run_agent_1():
    """Run Exotel fetcher (sync, reads CSV)."""
    from agents.exotel_fetcher import run as run_fetcher
    return run_fetcher()


async def run_agent_2():
    """Run Deepgram transcriber."""
    from agents.deepgram_transcriber import run as run_transcriber
    return await run_transcriber()


async def run_agent_3():
    """Run Call analyzer."""
    from agents.call_analyzer import run as run_analyzer
    return await run_analyzer()


async def run_agent_4():
    """Run Google Sheets uploader."""
    from agents.sheets_uploader import run as run_uploader
    return run_uploader()


async def run_full_pipeline(start_from: int = 1, only_agent: int = None):
    """Run the full pipeline or specific agents."""
    overall_start = time.time()
    results = {}

    agents = {
        1: ("Exotel Fetcher", run_agent_1),
        2: ("Deepgram Transcriber", run_agent_2),
        3: ("Call Analyzer", run_agent_3),
        4: ("Google Sheets Uploader", run_agent_4),
    }

    # Determine which agents to run
    if only_agent:
        to_run = [only_agent]
    else:
        to_run = [a for a in agents if a >= start_from]

    for agent_num in to_run:
        name, func = agents[agent_num]
        print(f"\n{'='*60}")
        print(f"🚀 Starting Agent {agent_num}: {name}")
        print(f"{'='*60}\n")

        agent_start = time.time()
        try:
            result = await func()
            agent_elapsed = time.time() - agent_start
            count = len(result) if result else 0
            results[agent_num] = {"status": "success", "count": count, "time": round(agent_elapsed, 1)}
            print(f"\n✅ Agent {agent_num} ({name}) completed in {agent_elapsed:.1f}s — {count} items")
        except Exception as e:
            agent_elapsed = time.time() - agent_start
            results[agent_num] = {"status": "failed", "error": str(e), "time": round(agent_elapsed, 1)}
            logger.error(f"Agent {agent_num} ({name}) FAILED: {e}", exc_info=True)
            print(f"\n❌ Agent {agent_num} ({name}) FAILED after {agent_elapsed:.1f}s: {e}")

            if not only_agent:
                print("Pipeline halted due to agent failure.")
                break

    # Final summary
    overall_elapsed = time.time() - overall_start
    print(f"\n{'='*60}")
    print(f"📋 PIPELINE SUMMARY")
    print(f"{'='*60}")
    for num, res in results.items():
        name = agents[num][0]
        status_icon = "✅" if res["status"] == "success" else "❌"
        print(f"  {status_icon} Agent {num} ({name}): {res['status']} — {res.get('count', 'N/A')} items in {res['time']}s")
    print(f"\n  Total wall time: {overall_elapsed:.1f}s ({overall_elapsed/60:.1f} minutes)")

    # Check for output CSV
    csv_path = BASE_DIR / "output" / "call_insights.csv"
    if csv_path.exists():
        import csv as csv_mod
        with open(csv_path) as f:
            reader = csv_mod.reader(f)
            row_count = sum(1 for _ in reader) - 1  # subtract header
        print(f"\n  📊 Output CSV: {csv_path}")
        print(f"     Rows: {row_count}")
    print(f"{'='*60}")

    # Save pipeline run metadata
    run_meta = {
        "run_timestamp": datetime.now().isoformat(),
        "agents_run": list(results.keys()),
        "results": results,
        "total_time_seconds": round(overall_elapsed, 2),
    }
    with open(BASE_DIR / "output" / "pipeline_run.json", "w") as f:
        json.dump(run_meta, f, indent=2)


def main():
    parser = argparse.ArgumentParser(description="Call Insights Pipeline")
    parser.add_argument("--agent", type=int, choices=[1, 2, 3, 4], help="Run only a specific agent")
    parser.add_argument("--from", type=int, choices=[1, 2, 3, 4], default=1, dest="start_from",
                        help="Resume pipeline from a specific agent")
    parser.add_argument("--skip-checks", action="store_true", help="Skip prerequisite checks")
    args = parser.parse_args()

    print_banner()

    # Ensure directories exist
    (BASE_DIR / "output").mkdir(exist_ok=True)
    (BASE_DIR / "logs").mkdir(exist_ok=True)

    if not args.skip_checks:
        if not check_prerequisites():
            sys.exit(1)

    asyncio.run(run_full_pipeline(start_from=args.start_from, only_agent=args.agent))


if __name__ == "__main__":
    main()
