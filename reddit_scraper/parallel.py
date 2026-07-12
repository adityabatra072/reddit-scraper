"""Parallel, time-sharded backfill driver.

The single-worker driver in ``backfill.py`` paginates one (subreddit, kind)
cursor sequentially. That is correct but slow for huge subs (r/Accounting alone
is ~1.2M comments = ~12k sequential requests at 100 items/page).

Here we split a [since, before) window into ``workers`` equal time-slices and
process each slice on its own thread with its own ``ArcticClient`` (so request
pacing is per-worker and they truly run concurrently). Arctic Shift tolerates
~10-20 concurrent workers under its ~2000 req/min ceiling.

Resumability within one sharded backfill: each shard has its own cursor
stored under a namespaced ``<prefix>#<i>`` key in the ``fetch_cursor`` table,
so a killed run resumes each slice independently. SQLite ids dedup, so any
overlap at slice boundaries is harmless.

Resumability across separate runs is where this gets subtle. A shard's cursor
key is only meaningful for the exact ``[since, before)`` that produced it —
``before`` is usually "now", which is different on every run, so re-sharding
and reusing old per-shard cursors against the new geometry silently drops
whatever falls between a new shard's start and the stale cursor's value. And
a subreddit's *first* backfill may use a narrower ``since`` than a later run
(e.g. the config default "2y" today, "all" next month) — treating "the old
shards all finished" as "everything is covered" then skips the entire older
history the narrower run never fetched in the first place.

``_backfill_kind_auto`` is the entry point that avoids both problems: it
tracks the exact ``[since, until)`` range already proven fetched
(``Cache.get_covered_range``) and only ever re-shards the genuinely missing
part — an older gap (if ``since`` widened) via its own namespaced shard
cursors, or a newer tail (if ``before`` advanced, i.e. normal re-runs of an
active subreddit) via a cheap non-sharded catch-up cursor. Every call site in
this codebase should go through it rather than calling
``backfill_kind_parallel`` directly.
"""
from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from tqdm import tqdm

from .arctic_client import ArcticClient, ArcticShiftError
from .backfill import _paginate
from .cache import Cache
from .config import Config
from .logging_util import get_logger
from .reconstruct import reconstruct_subreddit
from .writer import JsonlWriter

log = get_logger()


def _make_shards(since: int, before: int, n: int) -> list[tuple[int, int]]:
    """Split [since, before) into n contiguous [start, end) epoch slices."""
    if n <= 1 or before <= since:
        return [(since, before)]
    span = before - since
    step = span // n
    shards = []
    start = since
    for i in range(n):
        end = before if i == n - 1 else start + step
        shards.append((start, end))
        start = end
    return shards


def _run_shard(cfg: Config, kind: str, subreddit: str, shard_prefix: str, shard_idx: int,
               start: int, end: int, cache: Cache, writer: JsonlWriter, counter: dict,
               lock: threading.Lock, pbar: tqdm) -> int:
    """Paginate one time-slice for one kind. Own client => concurrent pacing.

    Called only against a ``[start, end)`` whose ``end`` was frozen by
    ``get_or_set_target_before`` for the duration of this particular sharded
    backfill (see ``backfill_kind_parallel``). So a shard marked done here
    really is done for good — there is no reshaped window it could need
    resuming against.

    ``shard_prefix`` namespaces the cursor key so that a later, separate
    sharded backfill over a *different* range (e.g. an older gap discovered
    after widening ``since``) never collides with — or gets mistaken for —
    this one's shard cursors.
    """
    cursor_key = f"{shard_prefix}#{shard_idx}"
    saved_start, done = cache.get_cursor(subreddit, cursor_key, start)
    if done:
        return 0
    client = ArcticClient(cfg, use_http_cache=False)  # avoid sqlite-cache write contention
    if kind == "posts":
        fetch = lambda a, b: client.search_posts(subreddit, a, b)
        upsert, write = cache.upsert_posts, writer.write_posts
    else:
        fetch = lambda a, b: client.search_comments(subreddit, a, b)
        upsert, write = cache.upsert_comments, writer.write_comments

    new_total = 0
    try:
        # resume from saved cursor but never before this shard's own start
        from_after = max(saved_start, start)
        for page, last_created in _paginate(fetch, subreddit, from_after, end, cfg.page_limit):
            fetched = int(time.time())
            ids = [r["id"] for r in page]
            already = cache.existing_ids(kind, ids)
            fresh = [r for r in page if r["id"] not in already]
            upsert(page, subreddit, fetched)
            write(fresh)
            cache.set_cursor(subreddit, cursor_key, int(last_created), False, fetched)
            new_total += len(fresh)
            with lock:
                counter[kind] += len(page)
                pbar.update(len(page))
        cache.set_cursor(subreddit, cursor_key, end, True, int(time.time()))
    finally:
        client.close()
    return new_total


def backfill_kind_parallel(cfg: Config, cache: Cache, writer: JsonlWriter,
                           subreddit: str, kind: str, since: int, before: int,
                           workers: int, shard_prefix: str | None = None) -> tuple[int, bool]:
    """Run all shards for one kind concurrently, covering ``[since, before)``.

    Only call this for a range that has never been fully completed before —
    callers own tracking that via ``covered_range`` (see ``_backfill_kind_auto``).
    Shard boundaries are a pure function of ``(since, before, workers)``; two
    calls with different ``before`` values (e.g. two separate runs, each using
    "now") produce *different* shard geometries but would write to the *same*
    per-shard cursor keys if reusing ``kind`` as the prefix. Resuming those
    cursors against a reshaped window silently drops whatever lies between a
    new shard's start and the old cursor's value — a real, silent data-loss
    bug (the one this module exists to avoid). ``shard_prefix`` (default
    ``kind``) namespaces the cursor keys so a distinct call for a distinct
    range never collides with another's.

    A shard that exhausts its retries does not abort the run: the exception is
    caught, logged, and the remaining shards still complete. Each shard's cursor
    is persisted as it goes, so a failed slice is resumed on the next run rather
    than lost. Returns ``(new_rows, all_shards_succeeded)``.
    """
    prefix = shard_prefix or kind
    shards = _make_shards(since, before, workers)
    counter = {kind: 0}
    lock = threading.Lock()
    pbar = tqdm(desc=f"{subreddit} {kind} x{len(shards)}", unit=kind[:4], leave=False)
    new_total = 0
    failures = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {
            ex.submit(_run_shard, cfg, kind, subreddit, prefix, i, s, e, cache, writer,
                      counter, lock, pbar): i
            for i, (s, e) in enumerate(shards)
        }
        for f in as_completed(futures):
            idx = futures[f]
            try:
                new_total += f.result()
            except ArcticShiftError as e:
                failures += 1
                log.warning("[backfill] r/%s %s shard #%d failed (will resume next run): %s",
                            subreddit, kind, idx, e)
    pbar.close()
    if failures:
        log.warning("[backfill] r/%s %s: %d/%d shards failed; re-run to finish them.",
                    subreddit, kind, failures, len(shards))
    return new_total, failures == 0


def _catchup_kind(cfg: Config, cache: Cache, writer: JsonlWriter, subreddit: str,
                  kind: str, catchup_since: int, before: int) -> int:
    """Sequentially fetch a tail beyond an already-covered range.

    Once a sharded backfill has covered up to some point, new activity since
    then is a small delta relative to the whole history — even for a busy
    subreddit, "posts/comments made since I last scraped" is nowhere near
    "every post/comment ever". A single non-sharded cursor (``{kind}_tail``)
    is fast enough for that delta and, critically, is always resumed against
    itself rather than against a reshaped shard split, so it can't silently
    skip newly-created items the way re-sharding a shifted ``[since, now)``
    window can (see ``backfill_kind_parallel``'s docstring).
    """
    client = ArcticClient(cfg)
    if kind == "posts":
        fetch = lambda a, b: client.search_posts(subreddit, a, b)
        upsert, write = cache.upsert_posts, writer.write_posts
    else:
        fetch = lambda a, b: client.search_comments(subreddit, a, b)
        upsert, write = cache.upsert_comments, writer.write_comments

    cursor_key = f"{kind}_tail"
    saved_start, _ = cache.get_cursor(subreddit, cursor_key, catchup_since)
    start = max(saved_start, catchup_since)

    new_total = 0
    pbar = tqdm(desc=f"{subreddit} {kind} (catch-up)", unit=kind[:4], leave=False)
    try:
        for page, last_created in _paginate(fetch, subreddit, start, before, cfg.page_limit):
            fetched = int(time.time())
            ids = [r["id"] for r in page]
            already = cache.existing_ids(kind, ids)
            fresh = [r for r in page if r["id"] not in already]
            upsert(page, subreddit, fetched)
            write(fresh)
            cache.set_cursor(subreddit, cursor_key, int(last_created), False, fetched)
            new_total += len(fresh)
            pbar.update(len(page))
        cache.set_cursor(subreddit, cursor_key, before, True, int(time.time()))
    finally:
        pbar.close()
        client.close()
    return new_total


def _backfill_kind_auto(cfg: Config, cache: Cache, writer: JsonlWriter, subreddit: str,
                        kind: str, since: int, before: int, workers: int) -> int:
    """Cover ``[since, before)`` for (subreddit, kind), doing only the missing part.

    ``covered_range`` records the ``[since, until)`` already fully fetched.
    Three cases, all handled without ever re-sharding a window whose bounds
    have shifted from a previous run (the bug this module exists to avoid):

      * nothing covered yet -> one sharded backfill of the whole request.
      * request's ``since`` is older than what's covered -> an *older-gap*
        sharded backfill for ``[since, covered_since)`` first (its own
        namespaced shard cursors, so they can't collide with the original
        backfill's). This is the case that was previously missed entirely:
        a subreddit first scraped with a narrow ``since`` (e.g. the default
        "2y") and later re-run with a wider one (e.g. "all") needs its older
        history actually fetched, not just assumed complete because the
        original shards were "done".
      * request's ``before`` is newer than what's covered -> a cheap,
        non-sharded tail catch-up for ``[covered_until, before)``.

    The covered range only ever grows to match what has actually been proven
    fetched; a failed older-gap or tail fetch leaves it untouched so the next
    run retries exactly the missing part.
    """
    covered = cache.get_covered_range(subreddit, kind)
    total = 0
    now = int(time.time())

    if covered is None:
        frozen_before = cache.get_or_set_target_before(subreddit, kind, before, now)
        new_rows, ok = backfill_kind_parallel(cfg, cache, writer, subreddit, kind,
                                              since, frozen_before, workers)
        total += new_rows
        if ok:
            cache.set_covered_range(subreddit, kind, since, frozen_before, now)
            cache.clear_target_before(subreddit, kind, now)
        return total

    cov_since, cov_until = covered

    if since < cov_since:
        older_prefix = f"{kind}_older"
        frozen_gap_end = cache.get_or_set_target_before(subreddit, older_prefix, cov_since, now)
        new_rows, ok = backfill_kind_parallel(cfg, cache, writer, subreddit, kind,
                                              since, frozen_gap_end, workers,
                                              shard_prefix=older_prefix)
        total += new_rows
        if ok:
            cov_since = min(cov_since, since)
            cache.set_covered_range(subreddit, kind, cov_since, cov_until, now)
            cache.clear_target_before(subreddit, older_prefix, now)

    if before > cov_until:
        new_rows = _catchup_kind(cfg, cache, writer, subreddit, kind, cov_until, before)
        total += new_rows
        cache.set_covered_range(subreddit, kind, cov_since, before, now)

    return total


def backfill_subreddit_parallel(cfg: Config, cache: Cache, subreddit: str,
                                since: int, before: int, workers: int) -> dict:
    writer = JsonlWriter(cfg, subreddit)
    result: dict = {"posts": 0, "comments": 0, "threads": 0}
    if cfg.subreddit_meta:
        from .backfill import backfill_meta
        client = ArcticClient(cfg)
        try:
            result["meta"] = 1 if backfill_meta(client, cache, subreddit) else 0
        finally:
            client.close()
    if cfg.fetch_posts:
        result["posts"] = _backfill_kind_auto(cfg, cache, writer, subreddit,
                                              "posts", since, before, workers)
    if cfg.fetch_comments:
        result["comments"] = _backfill_kind_auto(cfg, cache, writer, subreddit,
                                                 "comments", since, before, workers)
    # threads are rebuilt offline from the flat comments (free)
    if cfg.reconstruct_threads and cfg.fetch_comments:
        rec = reconstruct_subreddit(cfg, cache, subreddit)
        result["threads"] = rec["posts_with_threads"]
    return result
