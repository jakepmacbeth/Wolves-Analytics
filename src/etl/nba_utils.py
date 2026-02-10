from __future__ import annotations

import time
from typing import Any, Callable, Optional

def call_with_retries(
    call_fn: Callable[[], Any],
    max_retries: int = 3,
    backoff_seconds: Optional[list[int]] = None,
    max_total_wait_seconds: int = 600,  # cap at 4 minutes per call
) -> Any:
    """
    Retry NBA API calls with long backoff (throttle-friendly),
    but cap total time spent on a single call so backfills keep moving.
    """
    if backoff_seconds is None:
        backoff_seconds = [120, 200, 250]

    start = time.time()

    for attempt in range(max_retries):
        try:
            return call_fn()
        except Exception as e:
            if attempt == max_retries - 1:
                raise

            wait = backoff_seconds[min(attempt, len(backoff_seconds) - 1)]

            # If we'd exceed the cap, give up so caller can log + skip
            elapsed = time.time() - start
            if elapsed + wait > max_total_wait_seconds:
                raise

            print(
                f"API call failed ({type(e).__name__}: {e}). "
                f"Retrying in {wait}s (attempt {attempt + 1}/{max_retries})..."
            )
            time.sleep(wait)

    raise RuntimeError("Unexpected retry fallthrough")

