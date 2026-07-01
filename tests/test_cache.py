"""Tests for the SQLite cache: dedup, cursors, stats, metadata."""
from __future__ import annotations

import pytest

from reddit_scraper.cache import Cache
from reddit_scraper.config import load


@pytest.fixture
def cache(tmp_path):
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(
        f"subreddits: [test]\n"
        f"output:\n  db_path: '{(tmp_path / 'scraper.db').as_posix()}'\n"
        f"  data_dir: '{(tmp_path / 'data').as_posix()}'\n",
        encoding="utf-8",
    )
    cfg = load(cfg_path)
    c = Cache(cfg)
    yield c
    c.close()


def _post(pid, created=100):
    return {"id": pid, "created_utc": created, "title": f"t{pid}",
            "score": 1, "num_comments": 0}


def _comment(cid, link="t3_p1", parent="t3_p1", created=100):
    return {"id": cid, "link_id": link, "parent_id": parent,
            "created_utc": created, "body": "hi"}


def test_upsert_posts_returns_new_count(cache):
    assert cache.upsert_posts([_post("p1"), _post("p2")], "test", 0) == 2


def test_upsert_posts_dedups(cache):
    cache.upsert_posts([_post("p1")], "test", 0)
    # re-inserting the same id inserts nothing (archive is immutable)
    assert cache.upsert_posts([_post("p1"), _post("p2")], "test", 0) == 1


def test_existing_ids(cache):
    cache.upsert_posts([_post("p1"), _post("p2")], "test", 0)
    assert cache.existing_ids("posts", ["p1", "p3"]) == {"p1"}


def test_existing_ids_empty(cache):
    assert cache.existing_ids("posts", []) == set()


def test_cursor_roundtrip(cache):
    start, done = cache.get_cursor("test", "posts", default=42)
    assert (start, done) == (42, False)  # unset -> default
    cache.set_cursor("test", "posts", 500, True, 0)
    assert cache.get_cursor("test", "posts", default=42) == (500, True)


def test_stats(cache):
    cache.upsert_posts([_post("p1")], "test", 0)
    cache.upsert_comments([_comment("c1")], "test", 0)
    s = cache.stats()
    assert s["posts"] == 1 and s["comments"] == 1


def test_save_subreddit_meta(cache):
    meta = {"display_name": "test", "subscribers": 1234,
            "created_utc": 1600000000, "public_description": "desc"}
    cache.save_subreddit_meta("test", meta, 0)
    row = cache.conn.execute(
        "SELECT subscribers, public_description FROM subreddit_meta WHERE subreddit='test'"
    ).fetchone()
    assert row["subscribers"] == 1234
    assert row["public_description"] == "desc"


def test_iter_comments_for_threads_grouped(cache):
    cache.upsert_comments(
        [_comment("c1", link="t3_a"), _comment("c2", link="t3_b"),
         _comment("c3", link="t3_a")],
        "test", 0,
    )
    rows = list(cache.iter_comments_for_threads("test"))
    link_ids = [r["link_id"] for r in rows]
    # grouped by link_id (all t3_a together, then t3_b)
    assert link_ids == sorted(link_ids)
    assert len(rows) == 3
