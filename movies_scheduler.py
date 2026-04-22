"""External scheduler for the movies section."""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

MAIN_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(MAIN_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(MAIN_REPO_ROOT))

from models import SessionLocal, init_db
from services.movie_service import run_movie_refresh_pipeline

MY_PHONE_NUMBER = os.getenv("MY_PHONE_NUMBER", "+15132268634")
SELFPING_API_KEY = os.getenv("SELFPING_API_KEY")
SELFPING_ENDPOINT = "https://www.selfping.com/api/sms"


def log_event(message: str, **details) -> None:
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "message": message,
    }
    if details:
        payload["details"] = details
    print(json.dumps(payload), flush=True)


def format_watchlist_alert(movies: list[dict]) -> str:
    lines = [
        "🚨🎬Metrograph Watchlist Alert 🎬🚨",
        "",
        "New showtimes added for movies on your watchlist:",
        "",
    ]

    for index, movie in enumerate(movies[:5], start=1):
        suffix = " (SPECIAL EVENT)" if movie.get("special_event") else ""
        lines.append(f"{index}. {movie['title']} by {movie['director']}{suffix}")

    lines.extend([
        "",
        "See more at www.gbonez.org/movies",
    ])
    return "\n".join(lines)


def send_watchlist_text(message: str) -> None:
    if not message:
        log_event("Skipping SMS send because message body is empty")
        return

    if not SELFPING_API_KEY:
        raise RuntimeError("SELFPING_API_KEY is required to send watchlist texts.")
    if not SELFPING_ENDPOINT:
        raise RuntimeError("SELFPING_ENDPOINT is required to send watchlist texts.")

    log_event("Sending watchlist SMS", endpoint=SELFPING_ENDPOINT, to=MY_PHONE_NUMBER)
    response = requests.post(
        SELFPING_ENDPOINT,
        headers={
            "Authorization": f"Bearer {SELFPING_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "to": MY_PHONE_NUMBER,
            "message": message,
        },
        timeout=8,
    )
    response.raise_for_status()
    log_event("Watchlist SMS sent successfully", status_code=response.status_code)


def run_movies_job() -> dict:
    started_at = time.perf_counter()
    log_event("Starting movies cron job")
    init_db()
    db = SessionLocal()
    try:
        log_event("Running movie refresh pipeline")
        result = run_movie_refresh_pipeline(db)
        new_movies = [movie for movie in result.get("new_watchlist_films", []) if movie.get("title")]

        log_event(
            "Movie refresh pipeline completed",
            updated_movies=result.get("updated_movies"),
            new_watchlist_movies=len(new_movies),
            schedule_updated_at=result.get("schedule_updated_at"),
        )

        if new_movies:
            log_event(
                "New watchlist movies found",
                count=len(new_movies),
                titles=[movie.get("title") for movie in new_movies[:5]],
            )
            send_watchlist_text(format_watchlist_alert(new_movies[:5]))
        else:
            log_event("No new watchlist movies found; SMS not sent")

        result["text_sent"] = bool(new_movies)
        result["texted_movies"] = new_movies[:5]
        result["duration_seconds"] = round(time.perf_counter() - started_at, 2)
        log_event(
            "Movies cron job finished",
            text_sent=result["text_sent"],
            texted_movies=[movie.get("title") for movie in result["texted_movies"]],
            duration_seconds=result["duration_seconds"],
        )
        return result
    except Exception as error:
        log_event("Movies cron job failed", error=str(error))
        raise
    finally:
        db.close()
        log_event("Database session closed")


def main() -> None:
    try:
        result = run_movies_job()
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