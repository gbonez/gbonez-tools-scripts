"""Daily Metrograph updater for all movie users."""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone


ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from models import init_db
from services.movie_service import run_daily_movie_user_update_cycle


def log_event(message: str, **details) -> None:
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "message": message,
    }
    if details:
        payload["details"] = details
    print(json.dumps(payload), flush=True)


def run_daily_movies_job() -> dict:
    started_at = time.perf_counter()
    log_event("Starting daily per-user Metrograph update job")
    init_db()
    try:
        results = run_daily_movie_user_update_cycle()
        summary = {
            "users_processed": len(results),
            "texts_sent": sum(1 for result in results if result.get("text_sent")),
            "users_with_new_watchlist_films": sum(1 for result in results if result.get("new_watchlist_films", 0) > 0),
        }
        payload = {
            "results": results,
            "summary": summary,
            "duration_seconds": round(time.perf_counter() - started_at, 2),
        }
        log_event("Finished daily per-user Metrograph update job", **summary, duration_seconds=payload["duration_seconds"])
        return payload
    except Exception as error:
        log_event("Daily per-user Metrograph update job failed", error=str(error))
        raise


def main() -> None:
    try:
        result = run_daily_movies_job()
        print(json.dumps(result, indent=2), flush=True)
    except Exception as error:
        print(
            json.dumps(
                {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "message": "Movies cron entrypoint exiting with failure",
                    "error": str(error),
                }
            ),
            flush=True,
        )
        raise


if __name__ == "__main__":
    main()