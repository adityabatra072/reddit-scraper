"""Covered-range routing shared by the sequential and parallel backfill drivers.

``covered_range`` records, per (subreddit, kind), the [since, until) window
already fully fetched. ``backfill_kind_auto`` consults it so that repeat runs
fetch only the genuinely missing part of the requested window:

  * nothing covered yet -> one full backfill of the whole request.
  * request's ``since`` is older than what's covered -> an "older gap"
    backfill for [since, covered_since), under its own cursor namespace so it
    can never collide with the original backfill's cursors. This is the case
    that was once missed entirely: a subreddit first scraped with a narrow
    ``since`` (e.g. the default "2y") and later re-run with a wider one
    (e.g. "all") needs its older history actually fetched, not just assumed
    complete because the original run finished.
  * request's ``before`` is newer than what's covered -> a cheap tail
    catch-up for [covered_until, before).

Windows are frozen for the duration of a (possibly multi-run) backfill: the
first attempt records the exact (since, before) it is about to fetch, and
every resume reuses that recorded window verbatim, ignoring whatever the
current run resolved from config. Both bounds must be frozen. ``before``
drifts on its own (it is usually "now"), and ``since`` can be edited in
config while a backfill is still incomplete; either change reshapes the
window that saved resume cursors point into, which silently skips data.

The covered range only ever grows to match what has actually been proven
fetched; a failed or interrupted fetch leaves it untouched so the next run
retries exactly the missing part.
"""
from __future__ import annotations

import time
from collections.abc import Callable

from .cache import Cache

# fetch_window(since, before, cursor_prefix) -> (new_rows, completed)
FetchWindow = Callable[[int, int, str], tuple[int, bool]]
# catchup(since, before) -> new_rows
Catchup = Callable[[int, int], int]


def _run_frozen_window(cache: Cache, subreddit: str, since: int, before: int,
                       prefix: str, fetch_window: FetchWindow) -> tuple[int, bool, int, int]:
    """Run (or resume) one backfill whose window is frozen under ``prefix``.

    Starting a brand-new window first deletes any cursors already stored
    under ``prefix``: they necessarily belong to a *different* window (a
    version of this scraper that predates window freezing, or an earlier,
    already-completed gap fetch) and a cursor that is "done" for that window
    is not done for this one.

    Returns ``(new_rows, completed, frozen_since, frozen_before)`` so the
    caller records exactly the window that was actually fetched, not the one
    it asked for.
    """
    now = int(time.time())
    frozen = cache.get_target_range(subreddit, prefix)
    if frozen is None:
        cache.delete_cursors(subreddit, prefix)
        frozen = (since, before)
        cache.set_target_range(subreddit, prefix, since, before, now)
    f_since, f_before = frozen
    new_rows, ok = fetch_window(f_since, f_before, prefix)
    return new_rows, ok, f_since, f_before


def backfill_kind_auto(cache: Cache, subreddit: str, kind: str, since: int,
                       before: int, fetch_window: FetchWindow,
                       catchup: Catchup) -> int:
    """Cover [since, before) for (subreddit, kind), fetching only the missing part.

    ``fetch_window`` does the driver-specific heavy lifting (sharded threads
    or a single sequential cursor) for one frozen window, namespacing its
    resume cursors under the given prefix. ``catchup`` fetches a newer tail
    with a single ascending cursor; a tail needs no frozen window because an
    ascending cursor is always resumed against itself, so a later ``before``
    can only extend it, never reshape it.
    """
    covered = cache.get_covered_range(subreddit, kind)
    total = 0
    now = int(time.time())

    if covered is None:
        new_rows, ok, f_since, f_before = _run_frozen_window(
            cache, subreddit, since, before, kind, fetch_window)
        total += new_rows
        if not ok:
            return total
        cache.set_covered_range(subreddit, kind, f_since, f_before, now)
        cache.clear_target_range(subreddit, kind, now)
        # fall through: if the request is wider than the window frozen on the
        # first attempt (config changed mid-backfill), the gap/tail handling
        # below picks up the difference in this same run.
        covered = (f_since, f_before)

    cov_since, cov_until = covered

    if since < cov_since:
        older_prefix = f"{kind}_older"
        new_rows, ok, f_since, _ = _run_frozen_window(
            cache, subreddit, since, cov_since, older_prefix, fetch_window)
        total += new_rows
        if ok:
            cov_since = min(cov_since, f_since)
            cache.set_covered_range(subreddit, kind, cov_since, cov_until, now)
            cache.clear_target_range(subreddit, older_prefix, now)

    if before > cov_until:
        new_rows = catchup(cov_until, before)
        total += new_rows
        cache.set_covered_range(subreddit, kind, cov_since, before, now)

    return total
