"""Stage 1 — archive backfill via Arctic Shift (sequential, single worker).

For each subreddit, over the configured [since, until] window:
  1. paginate all posts (sort=asc, created_utc cursor) -> SQLite + JSONL
  2. paginate all comments the same way -> SQLite + JSONL
  3. (optional) fetch the full nested comment tree per post -> SQLite + JSONL

Everything is resumable: the (subreddit, kind) cursor records the last
created_utc processed, and SQLite ids dedup so re-runs are cheap and JSONL gets
only genuinely-new records appended.

For large subreddits prefer the parallel driver (``parallel.py``); this module
is the simple path used when workers <= 1.
"""
from __future__ import annotations

import time

from tqdm import tqdm

from .arctic_client import ArcticClient
from .cache import Cache
from .config import Config
from .writer import JsonlWriter


def _paginate(fetch, subreddit: str, start_after: int, before: int, page_limit: int):
    """Yield pages (lists) of items ordered ascending by created_utc.

    ``fetch(after, before)`` must return a list sorted ascending. Advances the
    cursor to the last item's created_utc each page; stops on a short page.
    Guards against a stall where every item on a page shares the boundary
    timestamp by nudging the cursor forward by 1 second.
    """
    after = start_after
    while True:
        items = fetch(after, before)
        if not items:
            return
        yield items, items[-1]["created_utc"]
        last = int(items[-1]["created_utc"])
        if len(items) < page_limit:
            return
        next_after = last
        if next_after <= after:
            next_after = after + 1
        after = next_after


def _backfill_kind(cfg: Config, client: ArcticClient, cache: Cache, writer: JsonlWriter,
                   subreddit: str, kind: str, since: int, before: int) -> int:
    """Generic post/comment backfill paginating ascending by created_utc.

    This cursor tracks one absolute ``created_utc`` value (unlike the sharded
    parallel driver, which splits the window and so must not blindly resume a
    "done" cursor against a reshaped window — see ``parallel.py``). Here that
    hazard doesn't exist: resuming past a "done" cursor is always safe because
    it always means "fetch anything newer than last time", regardless of what
    ``before`` resolved to on this run. So a finished window is only ever
    skipped when there is provably nothing new to fetch (``before`` hasn't
    moved past the saved cursor) — a repeat run of an active subreddit still
    picks up whatever was posted since last time, automatically.
    """
    if kind == "posts":
        fetch = lambda a, b: client.search_posts(subreddit, a, b)
        upsert, write, unit = cache.upsert_posts, writer.write_posts, "post"
    else:
        fetch = lambda a, b: client.search_comments(subreddit, a, b)
        upsert, write, unit = cache.upsert_comments, writer.write_comments, "cmt"

    start, done = cache.get_cursor(subreddit, kind, since)
    if done and before <= start:
        return 0

    new_total = 0
    pbar = tqdm(desc=f"{subreddit} {kind}", unit=unit, leave=False)
    for page, last_created in _paginate(fetch, subreddit, start, before, cfg.page_limit):
        fetched = int(time.time())
        ids = [r["id"] for r in page]
        already = cache.existing_ids(kind, ids)
        fresh = [r for r in page if r["id"] not in already]
        upsert(page, subreddit, fetched)
        write(fresh)
        cache.set_cursor(subreddit, kind, int(last_created), False, fetched)
        new_total += len(fresh)
        pbar.update(len(page))
    cache.set_cursor(subreddit, kind, before, True, int(time.time()))
    pbar.close()
    return new_total


def backfill_meta(client: ArcticClient, cache: Cache, subreddit: str) -> bool:
    """Fetch and store one-shot subreddit metadata (subscribers, created, etc.).

    Returns True if metadata was found and saved.
    """
    meta = client.subreddit_meta(subreddit)
    if not meta:
        return False
    cache.save_subreddit_meta(subreddit, meta, int(time.time()))
    return True


def backfill_trees(cfg: Config, client: ArcticClient, cache: Cache, writer: JsonlWriter,
                   subreddit: str) -> int:
    """Fetch full nested comment trees for posts that don't yet have one."""
    missing = cache.post_ids_missing_tree(subreddit)
    if not missing:
        return 0
    count = 0
    for pid in tqdm(missing, desc=f"{subreddit} trees", unit="tree", leave=False):
        tree = client.comment_tree(pid)
        fetched = int(time.time())
        cache.save_comment_tree(pid, subreddit, tree, fetched)
        writer.write_tree(pid, tree)
        count += 1
    return count


def backfill_subreddit(cfg: Config, client: ArcticClient, cache: Cache, subreddit: str,
                       since: int, before: int) -> dict:
    writer = JsonlWriter(cfg, subreddit)
    result: dict = {}
    if cfg.subreddit_meta:
        result["meta"] = 1 if backfill_meta(client, cache, subreddit) else 0
    if cfg.fetch_posts:
        result["posts"] = _backfill_kind(cfg, client, cache, writer, subreddit,
                                         "posts", since, before)
    if cfg.fetch_comments:
        result["comments"] = _backfill_kind(cfg, client, cache, writer, subreddit,
                                            "comments", since, before)
    if cfg.api_trees:
        result["trees"] = backfill_trees(cfg, client, cache, writer, subreddit)
    return result
