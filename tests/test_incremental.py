"""Tests for the covered-range tracking that prevents two real data-loss bugs.

Background: the parallel backfill splits [since, before) into N shards and
gives each a cursor keyed by shard index. ``before`` is usually "now", which
is different on every run. Re-sharding a *different* [since, before) window
and blindly resuming old shard-indexed cursors against the new shards
silently drops whatever falls between a new shard's start and the stale
cursor's value.

A second, related bug: a subreddit's first backfill might use a narrower
``since`` than a later run (e.g. config default "2y" today, "all" next
month). Treating "the old shards all finished" as "everything is covered"
then silently skips the entire older history the narrower run never
fetched — this is *not* a resume-drift issue, it's a genuine gap that must
be filled with its own backfill.

These tests pin down the fix: an explicit ``covered_range`` per (subreddit,
kind) that later runs consult to fetch only the genuinely missing older gap
and/or newer tail, never re-sharding an already-covered range.
"""
from __future__ import annotations

import pytest

from reddit_scraper import parallel
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


# --- covered_range tracking --------------------------------------------------

def test_covered_range_unset_returns_none(cache):
    assert cache.get_covered_range("test", "posts") is None


def test_covered_range_roundtrip(cache):
    cache.set_covered_range("test", "posts", 100, 500, 0)
    assert cache.get_covered_range("test", "posts") == (100, 500)


def test_covered_range_independent_per_kind(cache):
    cache.set_covered_range("test", "posts", 100, 500, 0)
    assert cache.get_covered_range("test", "comments") is None


def test_covered_range_update_overwrites(cache):
    cache.set_covered_range("test", "posts", 100, 500, 0)
    cache.set_covered_range("test", "posts", 50, 900, 1)
    assert cache.get_covered_range("test", "posts") == (50, 900)


# --- frozen target for multi-run sharded backfills ---------------------------

def test_target_before_frozen_across_calls(cache):
    first = cache.get_or_set_target_before("test", "posts", 1000, 0)
    # a later call in the same run (or a resumed run) passes a different
    # "now" -- must keep returning the originally frozen value.
    second = cache.get_or_set_target_before("test", "posts", 9999, 0)
    assert first == second == 1000


def test_target_before_resets_after_clear(cache):
    cache.get_or_set_target_before("test", "posts", 1000, 0)
    cache.clear_target_before("test", "posts", 0)
    fresh = cache.get_or_set_target_before("test", "posts", 5000, 0)
    assert fresh == 5000


def test_target_before_namespaced_independently(cache):
    cache.get_or_set_target_before("test", "posts", 1000, 0)
    # a differently-prefixed target (e.g. the "older gap" backfill) must not
    # collide with the main one.
    fresh = cache.get_or_set_target_before("test", "posts_older", 42, 0)
    assert fresh == 42


# --- _backfill_kind_auto routing --------------------------------------------
# These stub out the network-hitting functions to isolate the pure routing
# decision, covering all three cases: nothing covered yet -> full sharded
# backfill; since widened -> older-gap backfill; before advanced -> tail
# catch-up; both at once -> both, independently.

def test_auto_full_backfill_when_nothing_covered(cache, monkeypatch):
    calls = []

    def fake_sharded(cfg, cache_, writer, subreddit, kind, since, before, workers,
                     shard_prefix=None):
        calls.append((since, before, shard_prefix))
        return 7, True

    monkeypatch.setattr(parallel, "backfill_kind_parallel", fake_sharded)
    result = parallel._backfill_kind_auto(
        cfg=None, cache=cache, writer=None, subreddit="test", kind="posts",
        since=0, before=1000, workers=4,
    )
    assert result == 7
    assert calls == [(0, 1000, None)]
    assert cache.get_covered_range("test", "posts") == (0, 1000)


def test_auto_freezes_target_across_resumed_runs_on_failure(cache, monkeypatch):
    """A shard failure must not mark the range covered, and a resumed run
    with a later resolved 'now' must reuse the original frozen target."""
    seen_befores = []

    def fake_sharded(cfg, cache_, writer, subreddit, kind, since, before, workers,
                     shard_prefix=None):
        seen_befores.append(before)
        return 0, False  # simulate: a shard failed, window not fully covered

    monkeypatch.setattr(parallel, "backfill_kind_parallel", fake_sharded)
    parallel._backfill_kind_auto(None, cache, None, "test", "posts", 0, 1000, 4)
    # second run, resolved "now" has moved on to 2000
    parallel._backfill_kind_auto(None, cache, None, "test", "posts", 0, 2000, 4)
    assert seen_befores == [1000, 1000]  # frozen, not reshaped to 2000
    assert cache.get_covered_range("test", "posts") is None  # never marked covered


def test_auto_tail_catchup_when_before_advances(cache, monkeypatch):
    cache.set_covered_range("test", "posts", 0, 1000, 0)
    calls = []

    def fake_catchup(cfg, cache_, writer, subreddit, kind, catchup_since, before):
        calls.append((catchup_since, before))
        return 3

    monkeypatch.setattr(parallel, "_catchup_kind", fake_catchup)
    result = parallel._backfill_kind_auto(
        cfg=None, cache=cache, writer=None, subreddit="test", kind="posts",
        since=0, before=2000, workers=4,
    )
    assert result == 3
    assert calls == [(1000, 2000)]
    assert cache.get_covered_range("test", "posts") == (0, 2000)


def test_auto_noop_when_range_already_fully_covered(cache, monkeypatch):
    cache.set_covered_range("test", "posts", 0, 1000, 0)

    def boom(*a, **k):
        raise AssertionError("should not be called")

    monkeypatch.setattr(parallel, "backfill_kind_parallel", boom)
    monkeypatch.setattr(parallel, "_catchup_kind", boom)
    result = parallel._backfill_kind_auto(
        cfg=None, cache=cache, writer=None, subreddit="test", kind="posts",
        since=0, before=1000, workers=4,
    )
    assert result == 0


def test_auto_older_gap_backfill_when_since_widens(cache, monkeypatch):
    """The core regression test: a subreddit first scraped with a narrow
    'since' (e.g. default 2y) and later re-run with a wider one (e.g. 'all')
    must actually fetch the older history, not silently skip it."""
    cache.set_covered_range("test", "posts", 500, 1000, 0)
    calls = []

    def fake_sharded(cfg, cache_, writer, subreddit, kind, since, before, workers,
                     shard_prefix=None):
        calls.append((since, before, shard_prefix))
        return 5, True

    monkeypatch.setattr(parallel, "backfill_kind_parallel", fake_sharded)
    result = parallel._backfill_kind_auto(
        cfg=None, cache=cache, writer=None, subreddit="test", kind="posts",
        since=0, before=1000, workers=4,  # since=0 is older than covered since=500
    )
    assert result == 5
    assert calls == [(0, 500, "posts_older")]  # namespaced, distinct from "posts"
    assert cache.get_covered_range("test", "posts") == (0, 1000)


def test_auto_older_gap_and_tail_both_when_window_widens_both_sides(cache, monkeypatch):
    cache.set_covered_range("test", "posts", 500, 1000, 0)
    sharded_calls, catchup_calls = [], []

    def fake_sharded(cfg, cache_, writer, subreddit, kind, since, before, workers,
                     shard_prefix=None):
        sharded_calls.append((since, before, shard_prefix))
        return 2, True

    def fake_catchup(cfg, cache_, writer, subreddit, kind, catchup_since, before):
        catchup_calls.append((catchup_since, before))
        return 4

    monkeypatch.setattr(parallel, "backfill_kind_parallel", fake_sharded)
    monkeypatch.setattr(parallel, "_catchup_kind", fake_catchup)
    result = parallel._backfill_kind_auto(
        cfg=None, cache=cache, writer=None, subreddit="test", kind="posts",
        since=0, before=2000, workers=4,
    )
    assert result == 6
    assert sharded_calls == [(0, 500, "posts_older")]
    assert catchup_calls == [(1000, 2000)]
    assert cache.get_covered_range("test", "posts") == (0, 2000)


def test_auto_older_gap_failure_does_not_mark_covered(cache, monkeypatch):
    cache.set_covered_range("test", "posts", 500, 1000, 0)

    def fake_sharded(cfg, cache_, writer, subreddit, kind, since, before, workers,
                     shard_prefix=None):
        return 0, False  # older-gap shard failed

    monkeypatch.setattr(parallel, "backfill_kind_parallel", fake_sharded)
    parallel._backfill_kind_auto(
        cfg=None, cache=cache, writer=None, subreddit="test", kind="posts",
        since=0, before=1000, workers=4,
    )
    # covered range's 'since' must stay at 500 -- the gap was NOT proven filled
    assert cache.get_covered_range("test", "posts") == (500, 1000)
