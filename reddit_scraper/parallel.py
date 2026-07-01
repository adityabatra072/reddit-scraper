"""Parallel, time-sharded backfill driver.

The single-worker driver in ``backfill.py`` paginates one (subreddit, kind)
cursor sequentially. That is correct but slow for huge subs (r/Accounting alone
is ~1.2M comments = ~12k sequential requests at 100 items/page).

Here we split the [since, before] window into ``workers`` equal time-slices and
process each slice on its own thread with its own ``ArcticClient`` (so request
pacing is per-worker and they truly run concurrently). Arctic Shift tolerates
~10-20 concurrent workers under its ~2000 req/min ceiling.

Resumability: each shard has its own cursor stored under ``kind#<i>`` in the
``fetch_cursor`` table, so a killed run resumes each slice independently. SQLite
ids dedup, so any overlap at slice boundaries is harmless.

Correctness vs. sequential: identical data. Sharding only changes the ORDER in
which items are fetched, not which items. Every comment/post in the window is
covered by exactly one slice.
"""
from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from tqdm import tqdm

from .arctic_client import ArcticClient
from .backfill import _paginate
from .cache import Cache
from .config import Config
from .reconstruct import reconstruct_subreddit
from .writer import JsonlWriter


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


def _run_shard(cfg: Config, kind: str, subreddit: str, shard_idx: int, start: int,
               end: int, cache: Cache, writer: JsonlWriter, counter: dict,
               lock: threading.Lock, pbar: tqdm) -> int:
    """Paginate one time-slice for one kind. Own client => concurrent pacing."""
    cursor_key = f"{kind}#{shard_idx}"
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
                           workers: int) -> int:
    shards = _make_shards(since, before, workers)
    counter = {kind: 0}
    lock = threading.Lock()
    pbar = tqdm(desc=f"{subreddit} {kind} x{len(shards)}", unit=kind[:4], leave=False)
    new_total = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [
            ex.submit(_run_shard, cfg, kind, subreddit, i, s, e, cache, writer,
                      counter, lock, pbar)
            for i, (s, e) in enumerate(shards)
        ]
        for f in as_completed(futures):
            new_total += f.result()
    pbar.close()
    return new_total


def backfill_subreddit_parallel(cfg: Config, cache: Cache, subreddit: str,
                                since: int, before: int, workers: int) -> dict:
    writer = JsonlWriter(cfg, subreddit)
    result: dict = {"posts": 0, "comments": 0, "threads": 0}
    if cfg.fetch_posts:
        result["posts"] = backfill_kind_parallel(cfg, cache, writer, subreddit,
                                                 "posts", since, before, workers)
    if cfg.fetch_comments:
        result["comments"] = backfill_kind_parallel(cfg, cache, writer, subreddit,
                                                    "comments", since, before, workers)
    # threads are rebuilt offline from the flat comments (free)
    if cfg.reconstruct_threads and cfg.fetch_comments:
        rec = reconstruct_subreddit(cfg, cache, subreddit)
        result["threads"] = rec["posts_with_threads"]
    return result
