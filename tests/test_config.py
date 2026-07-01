"""Tests for config loading, date parsing, and CLI override precedence."""
from __future__ import annotations

import time

import pytest

from reddit_scraper.config import REDDIT_EPOCH, _deep_get, load, parse_timespec


# --- parse_timespec ---------------------------------------------------------
def test_parse_epoch_int():
    assert parse_timespec(1704067200, is_since=True) == 1704067200


def test_parse_epoch_string():
    assert parse_timespec("1704067200", is_since=True) == 1704067200


def test_parse_absolute_date():
    # 2024-01-31 UTC midnight
    assert parse_timespec("2024-01-31", is_since=True) == 1706659200


def test_parse_relative_days():
    now = int(time.time())
    got = parse_timespec("90d", is_since=True)
    assert abs((now - got) - 90 * 24 * 3600) < 5  # within a few seconds


def test_parse_relative_units():
    now = int(time.time())
    assert abs((now - parse_timespec("2y", is_since=True)) - 2 * 365 * 24 * 3600) < 5
    assert abs((now - parse_timespec("12h", is_since=True)) - 12 * 3600) < 5


def test_parse_now():
    assert abs(parse_timespec("now", is_since=False) - int(time.time())) < 5


def test_parse_all_since_clamps_to_reddit_epoch():
    assert parse_timespec("all", is_since=True) == REDDIT_EPOCH


def test_parse_all_until_is_now():
    assert abs(parse_timespec("all", is_since=False) - int(time.time())) < 5


def test_parse_none_defaults():
    assert parse_timespec(None, is_since=True) == REDDIT_EPOCH
    assert abs(parse_timespec(None, is_since=False) - int(time.time())) < 5


def test_parse_bad_date_raises():
    with pytest.raises(SystemExit):
        parse_timespec("not-a-date", is_since=True)


# --- _deep_get --------------------------------------------------------------
def test_deep_get_nested():
    d = {"a": {"b": {"c": 42}}}
    assert _deep_get(d, "a", "b", "c") == 42


def test_deep_get_missing_returns_default():
    d = {"a": {"b": {}}}
    assert _deep_get(d, "a", "b", "c", default="x") == "x"
    assert _deep_get(d, "z", default=None) is None


def test_deep_get_non_dict_midway():
    d = {"a": 5}
    assert _deep_get(d, "a", "b", default="fallback") == "fallback"


# --- load + overrides -------------------------------------------------------
def _write_yaml(tmp_path, text):
    p = tmp_path / "config.yaml"
    p.write_text(text, encoding="utf-8")
    return p


def test_load_basic(tmp_path):
    p = _write_yaml(tmp_path, "subreddits:\n  - python\nsince: '90d'\nuntil: now\n")
    cfg = load(p)
    assert cfg.subreddits == ["python"]
    assert cfg.until > cfg.since


def test_load_comma_separated_string(tmp_path):
    p = _write_yaml(tmp_path, "subreddits: 'python, rust, golang'\n")
    cfg = load(p)
    assert cfg.subreddits == ["python", "rust", "golang"]


def test_cli_override_wins(tmp_path):
    p = _write_yaml(tmp_path, "subreddits:\n  - python\nworkers: 3\n")
    cfg = load(p, {"subreddits": ["rust"], "workers": 12})
    assert cfg.subreddits == ["rust"]
    assert cfg.workers == 12


def test_none_override_ignored(tmp_path):
    p = _write_yaml(tmp_path, "subreddits:\n  - python\nworkers: 3\n")
    cfg = load(p, {"workers": None})
    assert cfg.workers == 3


def test_no_subreddits_raises(tmp_path):
    p = _write_yaml(tmp_path, "since: '90d'\n")
    with pytest.raises(SystemExit):
        load(p)


def test_until_before_since_raises(tmp_path):
    p = _write_yaml(tmp_path, "subreddits:\n  - x\nsince: now\nuntil: '2020-01-01'\n")
    with pytest.raises(SystemExit):
        load(p)


def test_missing_config_path_raises(tmp_path):
    with pytest.raises(SystemExit):
        load(tmp_path / "does_not_exist.yaml")


def test_fetch_toggles(tmp_path):
    p = _write_yaml(tmp_path, "subreddits: [x]\nfetch:\n  posts: false\n  comments: true\n")
    cfg = load(p)
    assert cfg.fetch_posts is False
    assert cfg.fetch_comments is True
