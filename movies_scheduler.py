"""Daily Metrograph updater for all movie users."""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone

import requests


ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from models import init_db


def log_event(message: str, **details) -> None:
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "message": message,
    }
    if details:
        payload["details"] = details
    print(json.dumps(payload), flush=True)


def _has_root_movie_app() -> bool:
    return os.path.isdir(os.path.join(ROOT_DIR, "services")) and os.path.exists(os.path.join(ROOT_DIR, "main.py"))


def _build_daily_refresh_url() -> str:
    explicit_url = (os.getenv("MOVIES_DAILY_SYNC_URL") or "").strip()
    if explicit_url:
        return explicit_url

    base_url = (
        os.getenv("MOVIES_API_BASE_URL")
        or os.getenv("APP_BASE_URL")
        or (f"https://{os.getenv('RAILWAY_PUBLIC_DOMAIN')}" if os.getenv("RAILWAY_PUBLIC_DOMAIN") else "")
        or "https://subway-tracker-production.up.railway.app"
    ).strip()
    return f"{base_url.rstrip('/')}/movies/daily-refresh"


def _run_daily_job_via_http() -> dict:
    daily_refresh_url = _build_daily_refresh_url()
    log_event("Root movie app not present in cron container; delegating daily refresh over HTTP", daily_refresh_url=daily_refresh_url)

    response = requests.post(daily_refresh_url, timeout=60 * 60)
    try:
        payload = response.json()
    except ValueError as error:
        raise RuntimeError(
            f"Daily refresh request to {daily_refresh_url} returned a non-JSON response with status {response.status_code}."
        ) from error

    if response.status_code >= 400:
        raise RuntimeError(
            payload.get("detail")
            or payload.get("error")
            or f"Daily refresh request to {daily_refresh_url} failed with status {response.status_code}."
        )

    return payload


def _run_daily_job_from_repo() -> dict:
    from services.movie_service import run_daily_movie_user_update_cycle

    init_db()
    results = run_daily_movie_user_update_cycle()
    summary = {
        "users_processed": len(results),
        "texts_sent": sum(1 for result in results if result.get("text_sent")),
        "users_with_new_watchlist_films": sum(1 for result in results if result.get("new_watchlist_films", 0) > 0),
    }
    return {
        "results": results,
        "summary": summary,
    }


def run_daily_movies_job() -> dict:
    started_at = time.perf_counter()
    log_event("Starting daily per-user Metrograph update job")
    try:
        payload = _run_daily_job_from_repo() if _has_root_movie_app() else _run_daily_job_via_http()
        payload["duration_seconds"] = round(time.perf_counter() - started_at, 2)
        summary = payload.get("summary") or {}
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