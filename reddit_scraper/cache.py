"""SQLite cache / source-of-truth for scraped Reddit data.

Every post and comment is keyed by its base36 id, so re-running the scraper
never inserts a datapoint twice (idempotent). The ``fetch_cursor`` table lets a
killed run resume from the last ``created_utc`` it processed per (subreddit, kind).

Tables
------
posts          : one row per submission, full archive JSON + live-refreshed counts
comments       : one row per comment (flat), full archive JSON
comment_trees  : one row per submission, the nested tree JSON from /comments/tree
fetch_cursor   : resume bookmarks per (subreddit, kind)
"""
from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path

from .config import Config

SCHEMA = """
CREATE TABLE IF NOT EXISTS posts (
    id              TEXT PRIMARY KEY,
    subreddit       TEXT NOT NULL,
    created_utc     INTEGER NOT NULL,
    author          TEXT,
    title           TEXT,
    permalink       TEXT,
    url             TEXT,
    score           INTEGER,
    num_comments    INTEGER,
    is_self         INTEGER,
    archive_json    TEXT NOT NULL,
    live_score          INTEGER,
    live_num_comments   INTEGER,
    live_refreshed_at   INTEGER,
    fetched_at      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_posts_sub_created ON posts(subreddit, created_utc);

CREATE TABLE IF NOT EXISTS comments (
    id              TEXT PRIMARY KEY,
    link_id         TEXT,
    parent_id       TEXT,
    subreddit       TEXT NOT NULL,
    created_utc     INTEGER NOT NULL,
    author          TEXT,
    score           INTEGER,
    body            TEXT,
    archive_json    TEXT NOT NULL,
    fetched_at      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_comments_sub_created ON comments(subreddit, created_utc);
CREATE INDEX IF NOT EXISTS idx_comments_link ON comments(link_id);

CREATE TABLE IF NOT EXISTS comment_trees (
    link_id         TEXT PRIMARY KEY,
    subreddit       TEXT NOT NULL,
    tree_json       TEXT NOT NULL,
    fetched_at      INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS fetch_cursor (
    subreddit       TEXT NOT NULL,
    kind            TEXT NOT NULL,        -- 'posts' | 'comments' | 'posts#<i>' ...
    last_created_utc INTEGER NOT NULL,
    done            INTEGER NOT NULL DEFAULT 0,
    updated_at      INTEGER NOT NULL,
    PRIMARY KEY (subreddit, kind)
);

CREATE TABLE IF NOT EXISTS subreddit_meta (
    subreddit       TEXT PRIMARY KEY,
    subscribers     INTEGER,
    created_utc     INTEGER,
    public_description TEXT,
    meta_json       TEXT NOT NULL,
    fetched_at      INTEGER NOT NULL
);

-- The [since, until) range fully fetched for (subreddit, kind). See
-- Cache.get_covered_range for why this must be an explicit range rather than
-- something inferred from fetch_cursor's per-shard "done" flags.
CREATE TABLE IF NOT EXISTS covered_range (
    subreddit       TEXT NOT NULL,
    kind            TEXT NOT NULL,        -- 'posts' | 'comments'
    since           INTEGER NOT NULL,
    until           INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL,
    PRIMARY KEY (subreddit, kind)
);
"""


class Cache:
    def __init__(self, cfg: Config, db_path: Path | None = None):
        self.cfg = cfg
        self.db_path = Path(db_path or cfg.db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        # check_same_thread=False + an explicit lock lets multiple fetch threads
        # share one connection safely. DB writes are fast relative to network,
        # so a single serialized writer is not the bottleneck.
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA busy_timeout=30000;")
        self.conn.executescript(SCHEMA)
        self.conn.commit()
        self._lock = threading.Lock()

    # --- writes -------------------------------------------------------------
    def upsert_posts(self, rows: Iterable[dict], subreddit: str, fetched_at: int) -> int:
        """Insert posts; existing ids are left untouched (archive is immutable).

        Returns the number of newly inserted rows.
        """
        sql = """
        INSERT INTO posts (id, subreddit, created_utc, author, title, permalink,
                           url, score, num_comments, is_self, archive_json, fetched_at)
        VALUES (:id, :subreddit, :created_utc, :author, :title, :permalink,
                :url, :score, :num_comments, :is_self, :archive_json, :fetched_at)
        ON CONFLICT(id) DO NOTHING;
        """
        params = []
        for r in rows:
            params.append({
                "id": r.get("id"),
                "subreddit": r.get("subreddit") or subreddit,
                "created_utc": int(r.get("created_utc") or 0),
                "author": r.get("author"),
                "title": r.get("title"),
                "permalink": r.get("permalink"),
                "url": r.get("url"),
                "score": r.get("score"),
                "num_comments": r.get("num_comments"),
                "is_self": 1 if r.get("is_self") else 0,
                "archive_json": json.dumps(r, ensure_ascii=False),
                "fetched_at": fetched_at,
            })
        if not params:
            return 0
        with self._lock:
            before = self.conn.total_changes
            self.conn.executemany(sql, params)
            self.conn.commit()
            return self.conn.total_changes - before

    def upsert_comments(self, rows: Iterable[dict], subreddit: str, fetched_at: int) -> int:
        sql = """
        INSERT INTO comments (id, link_id, parent_id, subreddit, created_utc,
                              author, score, body, archive_json, fetched_at)
        VALUES (:id, :link_id, :parent_id, :subreddit, :created_utc,
                :author, :score, :body, :archive_json, :fetched_at)
        ON CONFLICT(id) DO NOTHING;
        """
        params = []
        for r in rows:
            params.append({
                "id": r.get("id"),
                "link_id": r.get("link_id"),
                "parent_id": r.get("parent_id"),
                "subreddit": r.get("subreddit") or subreddit,
                "created_utc": int(r.get("created_utc") or 0),
                "author": r.get("author"),
                "score": r.get("score"),
                "body": r.get("body"),
                "archive_json": json.dumps(r, ensure_ascii=False),
                "fetched_at": fetched_at,
            })
        if not params:
            return 0
        with self._lock:
            before = self.conn.total_changes
            self.conn.executemany(sql, params)
            self.conn.commit()
            return self.conn.total_changes - before

    def save_comment_tree(self, link_id: str, subreddit: str, tree: object,
                          fetched_at: int) -> None:
        with self._lock:
            self.conn.execute(
                """INSERT INTO comment_trees (link_id, subreddit, tree_json, fetched_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(link_id) DO UPDATE SET tree_json=excluded.tree_json,
                                                      fetched_at=excluded.fetched_at;""",
                (link_id, subreddit, json.dumps(tree, ensure_ascii=False), fetched_at),
            )
            self.conn.commit()

    def has_comment_tree(self, link_id: str) -> bool:
        with self._lock:
            cur = self.conn.execute("SELECT 1 FROM comment_trees WHERE link_id=?", (link_id,))
            return cur.fetchone() is not None

    def save_subreddit_meta(self, subreddit: str, meta: dict, fetched_at: int) -> None:
        with self._lock:
            self.conn.execute(
                """INSERT INTO subreddit_meta
                       (subreddit, subscribers, created_utc, public_description,
                        meta_json, fetched_at)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(subreddit) DO UPDATE SET
                       subscribers=excluded.subscribers,
                       created_utc=excluded.created_utc,
                       public_description=excluded.public_description,
                       meta_json=excluded.meta_json,
                       fetched_at=excluded.fetched_at;""",
                (
                    subreddit,
                    meta.get("subscribers"),
                    int(meta["created_utc"]) if meta.get("created_utc") else None,
                    meta.get("public_description"),
                    json.dumps(meta, ensure_ascii=False),
                    fetched_at,
                ),
            )
            self.conn.commit()

    def update_live_counts(self, post_id: str, score: int | None,
                           num_comments: int | None, refreshed_at: int) -> None:
        with self._lock:
            self.conn.execute(
                """UPDATE posts SET live_score=?, live_num_comments=?, live_refreshed_at=?
                   WHERE id=?;""",
                (score, num_comments, refreshed_at, post_id),
            )
            self.conn.commit()

    # --- cursor -------------------------------------------------------------
    def get_cursor(self, subreddit: str, kind: str, default: int) -> tuple[int, bool]:
        with self._lock:
            cur = self.conn.execute(
                "SELECT last_created_utc, done FROM fetch_cursor WHERE subreddit=? AND kind=?",
                (subreddit, kind),
            )
            row = cur.fetchone()
        if row is None:
            return default, False
        return int(row["last_created_utc"]), bool(row["done"])

    def set_cursor(self, subreddit: str, kind: str, last_created_utc: int,
                   done: bool, updated_at: int) -> None:
        with self._lock:
            self.conn.execute(
                """INSERT INTO fetch_cursor (subreddit, kind, last_created_utc, done, updated_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(subreddit, kind) DO UPDATE SET
                       last_created_utc=excluded.last_created_utc,
                       done=excluded.done,
                       updated_at=excluded.updated_at;""",
                (subreddit, kind, last_created_utc, 1 if done else 0, updated_at),
            )
            self.conn.commit()

    def get_covered_range(self, subreddit: str, kind: str) -> tuple[int, int] | None:
        """Return the ``(since, until)`` range fully covered for (subreddit, kind).

        Deliberately tracked as an explicit range rather than inferred from
        per-shard cursors. Two bugs motivated this:

        1. Shard boundaries are a pure function of ``[since, until)``; ``until``
           is usually "now", which is different on every run. Resuming an old
           per-shard cursor (``{kind}#i``) against a freshly re-sharded window
           silently drops whatever falls between a new shard's start and the
           stale cursor's value.
        2. Treating "every shard cursor is marked done" as "the window is
           fully covered" (an earlier version of this migration did exactly
           that) ignores ``since`` entirely. A subreddit first scraped with
           the default ``since: 2y`` and later re-run with ``since: all`` would
           still report every old shard as "done" — even though the entire
           multi-year history before the original ``since`` was never fetched.
           There is no reliable way to recover the original ``since`` from
           shard cursors alone (they only record each shard's end, not its
           start), so this class does not attempt to migrate old shard-cursor
           data into a covered range at all: an unrecorded range is always
           treated as *not* covered, and callers fall back to a full sharded
           backfill (safe and idempotent — re-fetching a range that happens to
           overlap already-stored ids is a wasted request, never a duplicate row).
        """
        with self._lock:
            row = self.conn.execute(
                "SELECT since, until FROM covered_range WHERE subreddit=? AND kind=?",
                (subreddit, kind),
            ).fetchone()
        return (int(row["since"]), int(row["until"])) if row else None

    def set_covered_range(self, subreddit: str, kind: str, since: int, until: int,
                          updated_at: int) -> None:
        with self._lock:
            self.conn.execute(
                """INSERT INTO covered_range (subreddit, kind, since, until, updated_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(subreddit, kind) DO UPDATE SET
                       since=excluded.since, until=excluded.until,
                       updated_at=excluded.updated_at;""",
                (subreddit, kind, since, until, updated_at),
            )
            self.conn.commit()

    def get_or_set_target_before(self, subreddit: str, kind: str, before: int,
                                 updated_at: int) -> int:
        """Freeze the ``before`` of an in-progress sharded backfill.

        A large subreddit's initial backfill can span multiple runs if some
        shards fail. Each run re-resolves ``before`` (typically "now"), which
        would otherwise reshape the shard split between attempts — the same
        drift hazard described in ``get_covered_range``, just triggered by a
        failed/resumed initial sync rather than a repeat incremental one.
        Freezing the first attempt's ``before`` and reusing it on every resume
        (until the range is recorded as fully covered) keeps the shard
        geometry — and therefore every shard's cursor — self-consistent.
        """
        existing, done = self.get_cursor(subreddit, f"{kind}_target", default=-1)
        if not done and existing >= 0:
            return existing
        self.set_cursor(subreddit, f"{kind}_target", before, False, updated_at)
        return before

    def clear_target_before(self, subreddit: str, kind: str, updated_at: int) -> None:
        self.set_cursor(subreddit, f"{kind}_target", 0, True, updated_at)

    # --- reads --------------------------------------------------------------
    def posts_needing_live_refresh(self, since_created_utc: int,
                                   subreddits: list[str] | None = None) -> list[sqlite3.Row]:
        with self._lock:
            if subreddits:
                ph = ",".join("?" * len(subreddits))
                cur = self.conn.execute(
                    f"""SELECT id, subreddit, permalink, created_utc FROM posts
                        WHERE created_utc >= ? AND subreddit IN ({ph})
                        ORDER BY created_utc DESC;""",
                    (since_created_utc, *subreddits),
                )
            else:
                cur = self.conn.execute(
                    """SELECT id, subreddit, permalink, created_utc FROM posts
                       WHERE created_utc >= ? ORDER BY created_utc DESC;""",
                    (since_created_utc,),
                )
            return cur.fetchall()

    def existing_ids(self, table: str, ids: list[str]) -> set[str]:
        """Return the subset of ``ids`` already present in ``table``."""
        if not ids:
            return set()
        assert table in ("posts", "comments"), table
        out: set[str] = set()
        with self._lock:
            # chunk to stay under SQLite's variable limit
            for i in range(0, len(ids), 500):
                chunk = ids[i:i + 500]
                q = f"SELECT id FROM {table} WHERE id IN ({','.join('?' * len(chunk))})"
                out.update(r["id"] for r in self.conn.execute(q, chunk).fetchall())
        return out

    def post_ids_missing_tree(self, subreddit: str) -> list[str]:
        with self._lock:
            cur = self.conn.execute(
                """SELECT p.id FROM posts p
                   LEFT JOIN comment_trees t ON t.link_id = p.id
                   WHERE p.subreddit = ? AND t.link_id IS NULL
                   ORDER BY p.created_utc ASC;""",
                (subreddit,),
            )
            return [r["id"] for r in cur.fetchall()]

    def stats(self, subreddit: str | None = None) -> dict:
        where = "WHERE subreddit=?" if subreddit else ""
        args = (subreddit,) if subreddit else ()
        def _count(table: str) -> int:
            return self.conn.execute(
                f"SELECT COUNT(*) c FROM {table} {where}", args).fetchone()["c"]

        with self._lock:
            return {
                "posts": _count("posts"),
                "comments": _count("comments"),
                "comment_trees": _count("comment_trees"),
            }

    def iter_rows(self, table: str, subreddit: str):
        """Yield sqlite3.Row for one subreddit's posts/comments (for CSV export).

        Reads the full result set under the lock, then yields — so we never hold
        an open cursor on the shared connection while the caller does other work.
        """
        assert table in ("posts", "comments")
        with self._lock:
            rows = self.conn.execute(
                f"SELECT * FROM {table} WHERE subreddit=? ORDER BY created_utc ASC",
                (subreddit,),
            ).fetchall()
        yield from rows

    def iter_comments_for_threads(self, subreddit: str):
        """Yield (link_id, archive_json) rows for one subreddit, grouped by post.

        Ordered by ``link_id`` then ``created_utc`` so all comments for a post
        are contiguous and chronological — the shape ``reconstruct`` needs to
        build one thread at a time without loading the whole sub into memory.

        Uses a dedicated short-lived read connection rather than the shared
        one, so it can stream lazily (huge subs have 1M+ comments — a single
        ``fetchall`` would OOM a small box) without holding a cursor on the
        connection the writer threads use.
        """
        ro = sqlite3.connect(self.db_path)
        ro.row_factory = sqlite3.Row
        try:
            cur = ro.execute(
                """SELECT link_id, archive_json
                   FROM comments WHERE subreddit = ?
                   ORDER BY link_id ASC, created_utc ASC""",
                (subreddit,),
            )
            yield from cur
        finally:
            ro.close()

    def close(self) -> None:
        self.conn.close()


@contextmanager
def open_cache(cfg: Config) -> Iterator[Cache]:
    c = Cache(cfg)
    try:
        yield c
    finally:
        c.close()
