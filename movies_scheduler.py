"""External scheduler for the movies section."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

import requests

from models import MovieUser, SessionLocal, init_db
from movie_service import run_movie_refresh_pipeline

MY_PHONE_NUMBER = os.getenv("MY_PHONE_NUMBER", "+15132268634")
SELFPING_API_KEY = os.getenv("SELFPING_API_KEY")
SELFPING_ENDPOINT = "https://www.selfping.com/api/sms"
MOVIES_API_BASE_URL = os.getenv("MOVIES_API_BASE_URL", "https://subway-tracker-production.up.railway.app").rstrip("/")
MOVIES_CRON_MODE = os.getenv("MOVIES_CRON_MODE", "watchlist-alert")


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


def queue_all_movie_user_updates() -> dict:
    started_at = time.perf_counter()
    log_event("Starting all-movie-user sync queue job", api_base_url=MOVIES_API_BASE_URL)
    init_db()
    db = SessionLocal()
    try:
        users = db.query(MovieUser).order_by(MovieUser.username.asc()).all()
        log_event("Loaded movie users for sync queue job", count=len(users))

        results = []
        for index, user in enumerate(users, start=1):
            endpoint = f"{MOVIES_API_BASE_URL}/movies/users/{requests.utils.quote(user.username)}/letterboxd-sync"
            log_event(
                "Queueing movie user sync",
                username=user.username,
                position=index,
                total=len(users),
                endpoint=endpoint,
            )

            try:
                response = requests.post(endpoint, timeout=20)
                payload = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}

                if response.status_code in {200, 202}:
                    status = "queued"
                elif response.status_code == 409:
                    status = "already-running"
                else:
                    status = "failed"

                result = {
                    "username": user.username,
                    "status_code": response.status_code,
                    "status": status,
                    "detail": payload.get("detail") or payload.get("message"),
                }
                results.append(result)
                log_event("Movie user sync queue response", **result)
            except Exception as error:
                result = {
                    "username": user.username,
                    "status_code": None,
                    "status": "error",
                    "detail": str(error),
                }
                results.append(result)
                log_event("Movie user sync queue request failed", **result)

        summary = {
            "queued": sum(1 for result in results if result["status"] == "queued"),
            "already_running": sum(1 for result in results if result["status"] == "already-running"),
            "failed": sum(1 for result in results if result["status"] in {"failed", "error"}),
        }
        payload = {
            "results": results,
            "summary": summary,
            "duration_seconds": round(time.perf_counter() - started_at, 2),
        }
        log_event("Finished all-movie-user sync queue job", **summary, duration_seconds=payload["duration_seconds"])
        return payload
    finally:
        db.close()
        log_event("Database session closed for all-movie-user sync queue job")


def send_daily_calendar_update_texts() -> dict:
    started_at = time.perf_counter()
    log_event("Starting daily calendar update text job", api_base_url=MOVIES_API_BASE_URL)
    init_db()
    db = SessionLocal()
    try:
        users = db.query(MovieUser).order_by(MovieUser.username.asc()).all()
        log_event("Loaded movie users for daily calendar update texts", count=len(users))

        results = []
        for index, user in enumerate(users, start=1):
            endpoint = f"{MOVIES_API_BASE_URL}/movies/text/send-custom"
            payload = {
                "recipient": user.username,
                "message_kind": "calendar-update",
                "message_body": "",
                "setup_username": "",
            }
            log_event(
                "Sending daily calendar update text",
                username=user.username,
                position=index,
                total=len(users),
                endpoint=endpoint,
            )

            try:
                response = requests.post(endpoint, json=payload, timeout=20)
                response_payload = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}

                if response.status_code in {200, 202}:
                    status = "sent"
                elif response.status_code == 400:
                    status = "skipped"
                else:
                    status = "failed"

                result = {
                    "username": user.username,
                    "status_code": response.status_code,
                    "status": status,
                    "detail": response_payload.get("detail") or response_payload.get("message"),
                }
                results.append(result)
                log_event("Daily calendar update text response", **result)
            except Exception as error:
                result = {
                    "username": user.username,
                    "status_code": None,
                    "status": "error",
                    "detail": str(error),
                }
                results.append(result)
                log_event("Daily calendar update text request failed", **result)

        summary = {
            "sent": sum(1 for result in results if result["status"] == "sent"),
            "skipped": sum(1 for result in results if result["status"] == "skipped"),
            "failed": sum(1 for result in results if result["status"] in {"failed", "error"}),
        }
        payload = {
            "results": results,
            "summary": summary,
            "duration_seconds": round(time.perf_counter() - started_at, 2),
        }
        log_event("Finished daily calendar update text job", **summary, duration_seconds=payload["duration_seconds"])
        return payload
    finally:
        db.close()
        log_event("Database session closed for daily calendar update text job")


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
        if MOVIES_CRON_MODE == "queue-all-users":
            result = queue_all_movie_user_updates()
        elif MOVIES_CRON_MODE == "daily-calendar-texts":
            result = send_daily_calendar_update_texts()
        else:
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