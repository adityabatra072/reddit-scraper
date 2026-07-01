"""Tests for time-shard splitting."""
from __future__ import annotations

from reddit_scraper.parallel import _make_shards


def test_single_worker_one_shard():
    assert _make_shards(0, 100, 1) == [(0, 100)]


def test_zero_or_negative_workers():
    assert _make_shards(0, 100, 0) == [(0, 100)]


def test_shards_are_contiguous_and_cover_window():
    shards = _make_shards(0, 100, 4)
    assert len(shards) == 4
    assert shards[0][0] == 0
    assert shards[-1][1] == 100
    # each end is the next start — no gaps, no overlap
    for (_, a_end), (b_start, _) in zip(shards, shards[1:], strict=False):
        assert a_end == b_start


def test_last_shard_absorbs_remainder():
    # 100 / 3 = 33 step; last shard must reach 100 exactly
    shards = _make_shards(0, 100, 3)
    assert shards == [(0, 33), (33, 66), (66, 100)]


def test_empty_window():
    assert _make_shards(100, 100, 4) == [(100, 100)]


def test_inverted_window():
    assert _make_shards(200, 100, 4) == [(200, 100)]
