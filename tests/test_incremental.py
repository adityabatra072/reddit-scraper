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
fetched. This is *not* a resume-drift issue; it's a genuine gap that must
be filled with its own backfill.

These tests pin down the fix: an explicit ``covered_range`` per (subreddit,
kind) that later runs consult to fetch only the genuinely missing older gap
and/or newer tail, never re-fetching an already-covered range against a
reshaped window. The routing lives in ``coverage.backfill_kind_auto`` and is
shared by both the parallel and the sequential drivers.
"""
from __future__ import annotations

import pytest

from reddit_scraper import backfill, coverage, parallel
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


# --- frozen target window for multi-run backfills ----------------------------

def test_target_range_unset_returns_none(cache):
    assert cache.get_target_range("test", "posts") is None


def test_target_range_frozen_across_calls(cache):
    cache.set_target_range("test", "posts", 10, 1000, 0)
    # a resumed run resolves a different window from config; the frozen one
    # must keep winning until it is cleared.
    assert cache.get_target_range("test", "posts") == (10, 1000)


def test_target_range_resets_after_clear(cache):
    cache.set_target_range("test", "posts", 10, 1000, 0)
    cache.clear_target_range("test", "posts", 0)
    assert cache.get_target_range("test", "posts") is None


def test_target_range_namespaced_independently(cache):
    cache.set_target_range("test", "posts", 10, 1000, 0)
    # a differently-prefixed target (e.g. the "older gap" backfill) must not
    # collide with the main one.
    assert cache.get_target_range("test", "posts_older") is None


# --- stale-cursor cleanup -----------------------------------------------------

def test_delete_cursors_removes_prefix_and_shards(cache):
    cache.set_cursor("test", "posts", 500, True, 0)
    cache.set_cursor("test", "posts#0", 100, True, 0)
    cache.set_cursor("test", "posts#7", 900, False, 0)
    cache.delete_cursors("test", "posts")
    assert cache.get_cursor("test", "posts", -1) == (-1, False)
    assert cache.get_cursor("test", "posts#0", -1) == (-1, False)
    assert cache.get_cursor("test", "posts#7", -1) == (-1, False)


def test_delete_cursors_spares_sibling_namespaces(cache):
    cache.set_cursor("test", "posts_tail", 500, False, 0)
    cache.set_cursor("test", "posts_older#0", 100, True, 0)
    cache.delete_cursors("test", "posts")
    assert cache.get_cursor("test", "posts_tail", -1) == (500, False)
    assert cache.get_cursor("test", "posts_older#0", -1) == (100, True)


# --- backfill_kind_auto routing -----------------------------------------------
# These stub out the two driver callbacks to isolate the pure routing
# decision, covering all cases: nothing covered yet -> full backfill; since
# widened -> older-gap backfill; before advanced -> tail catch-up; both at
# once -> both, independently.

def _auto(cache, since, before, fetch_window, catchup=None):
    return coverage.backfill_kind_auto(
        cache, "test", "posts", since, before, fetch_window,
        catchup or (lambda s, b: 0))


def test_auto_full_backfill_when_nothing_covered(cache):
    calls = []

    def fetch_window(since, before, prefix):
        calls.append((since, before, prefix))
        return 7, True

    assert _auto(cache, 0, 1000, fetch_window) == 7
    assert calls == [(0, 1000, "posts")]
    assert cache.get_covered_range("test", "posts") == (0, 1000)


def test_auto_freezes_window_across_resumed_runs_on_failure(cache):
    """A failure must not mark the range covered, and a resumed run with a
    later resolved 'now' (or an edited 'since') must reuse the original
    frozen window rather than reshaping it."""
    seen = []

    def fetch_window(since, before, prefix):
        seen.append((since, before))
        return 0, False  # simulate: window not fully covered

    _auto(cache, 100, 1000, fetch_window)
    # second run: "now" moved to 2000 and since was edited to 0
    _auto(cache, 0, 2000, fetch_window)
    assert seen == [(100, 1000), (100, 1000)]  # frozen, not reshaped
    assert cache.get_covered_range("test", "posts") is None  # never marked covered


def test_auto_new_window_clears_stale_cursors(cache):
    """Cursors left under the same prefix by a different (pre-freeze) window
    must be dropped before a new window starts, or a stale 'done' shard would
    silently skip its slice of the new window."""
    cache.set_cursor("test", "posts#0", 999, True, 0)

    def fetch_window(since, before, prefix):
        # by the time the driver runs, the stale shard cursor must be gone
        assert cache.get_cursor("test", "posts#0", -1) == (-1, False)
        return 1, True

    assert _auto(cache, 0, 1000, fetch_window) == 1


def test_auto_tail_catchup_when_before_advances(cache):
    cache.set_covered_range("test", "posts", 0, 1000, 0)
    calls = []

    def catchup(catchup_since, before):
        calls.append((catchup_since, before))
        return 3

    def boom(*a):
        raise AssertionError("no sharded backfill expected")

    assert _auto(cache, 0, 2000, boom, catchup) == 3
    assert calls == [(1000, 2000)]
    assert cache.get_covered_range("test", "posts") == (0, 2000)


def test_auto_noop_when_range_already_fully_covered(cache):
    cache.set_covered_range("test", "posts", 0, 1000, 0)

    def boom(*a):
        raise AssertionError("should not be called")

    assert _auto(cache, 0, 1000, boom, boom) == 0


def test_auto_older_gap_backfill_when_since_widens(cache):
    """The core regression test: a subreddit first scraped with a narrow
    'since' (e.g. default 2y) and later re-run with a wider one (e.g. 'all')
    must actually fetch the older history, not silently skip it."""
    cache.set_covered_range("test", "posts", 500, 1000, 0)
    calls = []

    def fetch_window(since, before, prefix):
        calls.append((since, before, prefix))
        return 5, True

    assert _auto(cache, 0, 1000, fetch_window) == 5
    assert calls == [(0, 500, "posts_older")]  # namespaced, distinct from "posts"
    assert cache.get_covered_range("test", "posts") == (0, 1000)


def test_auto_older_gap_and_tail_both_when_window_widens_both_sides(cache):
    cache.set_covered_range("test", "posts", 500, 1000, 0)
    window_calls, catchup_calls = [], []

    def fetch_window(since, before, prefix):
        window_calls.append((since, before, prefix))
        return 2, True

    def catchup(catchup_since, before):
        catchup_calls.append((catchup_since, before))
        return 4

    assert _auto(cache, 0, 2000, fetch_window, catchup) == 6
    assert window_calls == [(0, 500, "posts_older")]
    assert catchup_calls == [(1000, 2000)]
    assert cache.get_covered_range("test", "posts") == (0, 2000)


def test_auto_older_gap_failure_does_not_mark_covered(cache):
    cache.set_covered_range("test", "posts", 500, 1000, 0)

    def fetch_window(since, before, prefix):
        return 0, False  # older-gap fetch failed

    _auto(cache, 0, 1000, fetch_window)
    # covered range's 'since' must stay at 500 -- the gap was NOT proven filled
    assert cache.get_covered_range("test", "posts") == (500, 1000)


def test_auto_second_older_gap_reuses_prefix_safely(cache):
    """Two successive since-widenings reuse the same 'posts_older' prefix.
    The second gap is a different window, so the first gap's leftover done
    cursors must not be mistaken for progress on it."""
    cache.set_covered_range("test", "posts", 500, 1000, 0)
    calls = []

    def fetch_window(since, before, prefix):
        calls.append((since, before, prefix))
        return 1, True

    _auto(cache, 200, 1000, fetch_window)   # first widening: [200, 500)
    _auto(cache, 0, 1000, fetch_window)     # second widening: [0, 200)
    assert calls == [(200, 500, "posts_older"), (0, 200, "posts_older")]
    assert cache.get_covered_range("test", "posts") == (0, 1000)


# --- drivers actually route through the shared logic --------------------------

def test_parallel_driver_uses_shared_routing(cache, monkeypatch):
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
    assert calls == [(0, 1000, "posts")]
    assert cache.get_covered_range("test", "posts") == (0, 1000)


def test_sequential_driver_fetches_older_gap_when_since_widens(cache, monkeypatch):
    """Regression for the workers=1 path: widening 'since' must fetch the
    older history through the sequential driver too, not only the parallel one."""
    cache.set_covered_range("test", "posts", 500, 1000, 0)
    calls = []

    def fake_fetch_window(cfg, client, cache_, writer, subreddit, kind, since,
                          before, cursor_key, desc):
        calls.append((since, before, cursor_key))
        return 5

    monkeypatch.setattr(backfill, "_fetch_window", fake_fetch_window)
    result = backfill._backfill_kind(
        cfg=None, client=None, cache=cache, writer=None, subreddit="test",
        kind="posts", since=0, before=1000,
    )
    assert result == 5
    assert calls == [(0, 500, "posts_older")]
    assert cache.get_covered_range("test", "posts") == (0, 1000)
