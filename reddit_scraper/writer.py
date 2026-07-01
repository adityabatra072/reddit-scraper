"""Append-only JSONL writers (+ optional CSV export) for full-fidelity output.

One file per subreddit per kind under ``<data_dir>/<subreddit>/``:
  posts.jsonl, comments.jsonl, comment_trees.jsonl, threads.jsonl

The SQLite cache guarantees dedup; the JSONL files are the raw, complete dump.
We serialize appends with a lock so concurrent worker threads never interleave
lines. JSONL writing can be disabled in config (sqlite is always authoritative);
CSV export is a separate post-hoc step (see ``export_csv``).
"""
from __future__ import annotations

import csv
import json
import threading
from pathlib import Path

from .config import Config


class JsonlWriter:
    def __init__(self, cfg: Config, subreddit: str):
        self.enabled = cfg.write_jsonl
        self.dir = cfg.sub_dir(subreddit)
        self.posts_path = self.dir / "posts.jsonl"
        self.comments_path = self.dir / "comments.jsonl"
        self.trees_path = self.dir / "comment_trees.jsonl"
        # Serialize appends so concurrent worker threads never interleave lines.
        self._lock = threading.Lock()

    def _append(self, path: Path, records: list[dict]) -> None:
        if not self.enabled or not records:
            return
        with self._lock, path.open("a", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r, ensure_ascii=False))
                f.write("\n")

    def write_posts(self, records: list[dict]) -> None:
        self._append(self.posts_path, records)

    def write_comments(self, records: list[dict]) -> None:
        self._append(self.comments_path, records)

    def write_tree(self, link_id: str, tree: object) -> None:
        if not self.enabled:
            return
        with self._lock, self.trees_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps({"link_id": link_id, "tree": tree}, ensure_ascii=False))
            f.write("\n")


# --- CSV export -------------------------------------------------------------
_POST_COLS = ["id", "subreddit", "created_utc", "author", "title", "permalink",
              "url", "score", "num_comments", "is_self",
              "live_score", "live_num_comments", "live_refreshed_at"]
_COMMENT_COLS = ["id", "link_id", "parent_id", "subreddit", "created_utc",
                 "author", "score", "body"]


def export_csv(cfg: Config, cache, subreddit: str) -> dict:
    """Flatten the SQLite rows for one subreddit into posts.csv / comments.csv.

    Only the common scalar columns are written (the full nested JSON stays in
    the .jsonl / archive_json). Easy to open in Excel or load with pandas.
    """
    out = {"posts": 0, "comments": 0}
    sub_dir = cfg.sub_dir(subreddit)
    for table, cols in (("posts", _POST_COLS), ("comments", _COMMENT_COLS)):
        path = sub_dir / f"{table}.csv"
        n = 0
        with path.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
            w.writeheader()
            for row in cache.iter_rows(table, subreddit):
                w.writerow({c: row[c] for c in cols})
                n += 1
        out[table] = n
    return out
