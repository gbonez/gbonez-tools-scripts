"""External scheduler for the movies section."""

from __future__ import annotations

import os
import json
import sys
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
        return

    if not SELFPING_API_KEY:
        raise RuntimeError("SELFPING_API_KEY is required to send watchlist texts.")
    if not SELFPING_ENDPOINT:
        raise RuntimeError("SELFPING_ENDPOINT is required to send watchlist texts.")

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


def run_movies_job() -> dict:
    init_db()
    db = SessionLocal()
    try:
        result = run_movie_refresh_pipeline(db)
        new_movies = [movie for movie in result.get("new_watchlist_films", []) if movie.get("title")]

        if new_movies:
            send_watchlist_text(format_watchlist_alert(new_movies[:5]))

        result["text_sent"] = bool(new_movies)
        result["texted_movies"] = new_movies[:5]
        return result
    finally:
        db.close()


def main() -> None:
    result = run_movies_job()
    print(json.dumps(result, indent=2), flush=True)


if __name__ == "__main__":
    main()