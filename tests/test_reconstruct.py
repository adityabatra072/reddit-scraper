"""Tests for offline comment-tree reconstruction."""
from __future__ import annotations

from reddit_scraper.reconstruct import _build_tree, _strip


def test_strip_fullnames():
    assert _strip("t1_abc") == "abc"
    assert _strip("t3_xyz") == "xyz"
    assert _strip("abc") == "abc"
    assert _strip(None) is None


def _c(cid, parent, created):
    return {"id": cid, "parent_id": parent, "created_utc": created}


def test_flat_top_level_comments():
    comments = [
        _c("a", "t3_post", 100),
        _c("b", "t3_post", 200),
    ]
    roots, placed = _build_tree(comments)
    assert placed == 2
    assert [n["id"] for n in roots] == ["a", "b"]
    assert all(n["replies"] == [] for n in roots)


def test_nested_replies_attach():
    comments = [
        _c("a", "t3_post", 100),
        _c("b", "t1_a", 150),
        _c("c", "t1_b", 175),
    ]
    roots, placed = _build_tree(comments)
    assert placed == 3
    assert len(roots) == 1
    assert roots[0]["id"] == "a"
    assert roots[0]["replies"][0]["id"] == "b"
    assert roots[0]["replies"][0]["replies"][0]["id"] == "c"


def test_replies_sorted_by_created():
    comments = [
        _c("a", "t3_post", 100),
        _c("late", "t1_a", 300),
        _c("early", "t1_a", 200),
    ]
    roots, _ = _build_tree(comments)
    assert [n["id"] for n in roots[0]["replies"]] == ["early", "late"]


def test_orphan_reply_attaches_at_root():
    # parent 'z' was never scraped -> the reply must not be dropped
    comments = [
        _c("a", "t3_post", 100),
        _c("orphan", "t1_z", 150),
    ]
    roots, placed = _build_tree(comments)
    assert placed == 2
    assert {n["id"] for n in roots} == {"a", "orphan"}


def test_top_level_sorted_by_created():
    comments = [
        _c("second", "t3_post", 200),
        _c("first", "t3_post", 100),
    ]
    roots, _ = _build_tree(comments)
    assert [n["id"] for n in roots] == ["first", "second"]
