"""Offline comment-tree reconstruction from the flat comments table.

The subreddit-wide ``comments/search`` sweep already captures every comment with
its ``link_id`` (parent post, ``t3_...``) and ``parent_id`` (``t1_...`` for a
reply, ``t3_...`` for a top-level comment). That is enough to rebuild the full
nested thread for each post with zero extra network calls - far cheaper than the
per-post tree API endpoint (~1.5s each).

Output: ``<data_dir>/<subreddit>/threads.jsonl`` - one line per post::

    {"link_id": "1abcde", "tree": [ {<comment>, "replies": [ ... ]}, ... ]}

Top-level comments are ordered by ``created_utc``; replies likewise.
Orphans (parent missing from the scrape) are attached at top level so nothing is
dropped.
"""
from __future__ import annotations

import json

from .cache import Cache
from .config import Config
from .logging_util import get_logger

log = get_logger()


def _strip(fullname: str | None) -> str | None:
    """'t1_abc' / 't3_abc' -> 'abc'."""
    if not fullname:
        return None
    return fullname.split("_", 1)[1] if "_" in fullname else fullname


def reconstruct_subreddit(cfg: Config, cache: Cache, subreddit: str) -> dict:
    """Rebuild nested threads for every post in ``subreddit`` from flat comments.

    Writes ``threads.jsonl`` and returns {posts_with_threads, comments_placed}.
    """
    # Stream comments ordered by link_id so all comments for one post are
    # contiguous; build + flush one post's tree at a time. This keeps only a
    # single post's comments in memory (huge subs have 1M+ comments - loading
    # them all at once OOMs a small box).
    rows = cache.iter_comments_for_threads(subreddit)

    out_path = cfg.sub_dir(subreddit) / "threads.jsonl"
    placed = 0
    posts_done = 0
    current_link: str | None = None
    group: list[dict] = []

    def flush(f, link_id: str, comments: list[dict]) -> tuple[int, int]:
        post_id = _strip(link_id)
        if not post_id or not comments:
            return 0, 0
        tree, n = _build_tree(comments)
        f.write(json.dumps({"link_id": post_id, "tree": tree}, ensure_ascii=False))
        f.write("\n")
        return 1, n

    with out_path.open("w", encoding="utf-8") as f:
        for row in rows:
            link_id = row["link_id"]
            if link_id != current_link and current_link is not None:
                p, n = flush(f, current_link, group)
                posts_done += p
                placed += n
                group = []
            current_link = link_id
            group.append(json.loads(row["archive_json"]))
        # final group
        p, n = flush(f, current_link, group) if current_link else (0, 0)
        posts_done += p
        placed += n
    return {"posts_with_threads": posts_done, "comments_placed": placed}


def _build_tree(comments: list[dict]) -> tuple[list[dict], int]:
    """Turn a flat list of comments (one post) into a nested tree.

    Each node is the original comment dict with an added ``replies`` list.
    Returns (top_level_nodes, total_comments_placed).
    """
    nodes: dict[str, dict] = {}
    for c in comments:
        node = dict(c)
        node["replies"] = []
        nodes[c["id"]] = node

    roots: list[dict] = []
    for c in comments:
        node = nodes[c["id"]]
        parent_id = _strip(c.get("parent_id"))
        parent_kind = (c.get("parent_id") or "").split("_", 1)[0]
        if parent_kind == "t1" and parent_id in nodes:
            nodes[parent_id]["replies"].append(node)
        else:
            # top-level (parent is the post) or orphaned reply -> attach at root
            roots.append(node)

    def sort_rec(items: list[dict]) -> None:
        items.sort(key=lambda n: n.get("created_utc") or 0)
        for it in items:
            if it["replies"]:
                sort_rec(it["replies"])

    sort_rec(roots)
    return roots, len(comments)


def reconstruct_all(cfg: Config, cache: Cache, subreddits: list[str]) -> dict:
    totals = {"posts_with_threads": 0, "comments_placed": 0}
    for s in subreddits:
        r = reconstruct_subreddit(cfg, cache, s)
        totals["posts_with_threads"] += r["posts_with_threads"]
        totals["comments_placed"] += r["comments_placed"]
        log.info("[threads] r/%s: %d threads, %d comments placed",
                 s, r["posts_with_threads"], r["comments_placed"])
    return totals
