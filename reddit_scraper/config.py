"""Configuration for the general Reddit scraper.

Unlike a hardcoded module, all knobs live in a YAML file (``config.yaml`` by
default) so a non-programmer can drive the whole scraper without touching code.
This module:

  * loads that YAML,
  * resolves human date specs ("2y", "2024-01-31", "now", "all") to epoch secs,
  * lets the CLI override any field,
  * exposes a single ``Config`` object every other module reads from.

There is intentionally no module-level mutable state: ``load()`` returns a
``Config`` instance that is threaded through the pipeline. This makes it trivial
to scrape with different settings in the same process (e.g. tests).
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError as e:  # pragma: no cover
    raise SystemExit(
        "PyYAML is required. Install dependencies with:\n"
        "    pip install -r requirements.txt"
    ) from e


# --- constants not worth exposing in YAML -----------------------------------
ARCTIC_BASE = "https://arctic-shift.photon-reddit.com/api"
# Reddit's public launch; "since: all" clamps here so we never send year-0 cursors.
REDDIT_EPOCH = 1119225600  # 2005-06-23

USER_AGENT = "reddit-scraper/1.0 (research; +https://github.com) python-requests"


# --- date parsing -----------------------------------------------------------
_REL_RE = re.compile(r"^\s*(\d+)\s*([yYmMdDhH])\s*$")
_SECONDS_PER = {"y": 365 * 24 * 3600, "m": 30 * 24 * 3600,
                "d": 24 * 3600, "h": 3600}


def now_epoch() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def parse_timespec(spec: Any, *, is_since: bool) -> int:
    """Turn a human date spec into an epoch-second integer (UTC).

    Accepts: relative ("2y","18m","90d","12h"), absolute "YYYY-MM-DD",
    epoch int/str, "now", and (for since) "all".
    """
    if spec is None:
        return REDDIT_EPOCH if is_since else now_epoch()
    if isinstance(spec, (int, float)):
        return int(spec)
    s = str(spec).strip().lower()
    if s == "now":
        return now_epoch()
    if s == "all":
        # only sensible as a start; as an `until` it's meaningless -> now.
        return REDDIT_EPOCH if is_since else now_epoch()
    if s.isdigit():
        return int(s)
    m = _REL_RE.match(s)
    if m:
        qty, unit = int(m.group(1)), m.group(2).lower()
        return now_epoch() - qty * _SECONDS_PER[unit]
    # absolute date
    try:
        dt = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except ValueError as e:
        raise SystemExit(
            f"Cannot understand date '{spec}'. Use e.g. '2y', '90d', "
            f"'2024-01-31', an epoch number, 'now', or 'all'."
        ) from e


@dataclass
class Config:
    subreddits: list[str]
    since: int
    until: int

    # fetch toggles
    fetch_posts: bool = True
    fetch_comments: bool = True
    reconstruct_threads: bool = True
    api_trees: bool = False
    subreddit_meta: bool = False

    # live refresh
    live_enabled: bool = False
    live_window_days: int = 30
    live_min_delay: float = 2.0
    live_max_delay: float = 5.0
    live_headless: bool = True
    live_use_proxies: bool = False
    live_limit: int | None = None

    # performance
    workers: int = 1

    # output
    data_dir: Path = Path("./data")
    db_path: Path = Path("./scraper.db")
    write_jsonl: bool = True
    write_csv: bool = False

    # network
    http_cache: bool = True
    page_limit: int = 100
    request_timeout: int = 45
    min_request_interval: float = 0.25
    ratelimit_floor: int = 20
    max_retries: int = 9
    backoff_base: float = 2
    backoff_cap: float = 60
    tree_limit: int = 25000
    tree_breadth: int = 100000
    tree_depth: int = 100000

    # constants (mirrored for convenience so modules import one object)
    arctic_base: str = ARCTIC_BASE
    user_agent: str = USER_AGENT

    # derived paths
    http_cache_path: Path = field(default=Path("./http_cache"))

    # --- helpers ------------------------------------------------------------
    def sub_dir(self, subreddit: str) -> Path:
        d = self.data_dir / subreddit
        d.mkdir(parents=True, exist_ok=True)
        return d

    @property
    def proxyscrape_url(self) -> str:
        return ("https://api.proxyscrape.com/v4/free-proxy-list/get"
                "?request=display_proxies&proxy_format=protocolipport&format=text")


def _deep_get(d: dict, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def load(yaml_path: str | Path | None = None, overrides: dict | None = None) -> Config:
    """Load YAML config, apply CLI overrides, return a resolved ``Config``.

    ``overrides`` is a flat dict of already-resolved values (from argparse);
    only non-None entries replace the YAML-derived value.
    """
    path = Path(yaml_path or os.environ.get("REDDIT_SCRAPER_CONFIG", "config.yaml"))
    raw: dict = {}
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
    elif yaml_path is not None:
        raise SystemExit(f"Config file not found: {path}")

    ov = overrides or {}

    def pick(cli_key, *yaml_keys, default):
        if ov.get(cli_key) is not None:
            return ov[cli_key]
        v = _deep_get(raw, *yaml_keys)
        return v if v is not None else default

    subreddits = pick("subreddits", "subreddits", default=[])
    if isinstance(subreddits, str):
        subreddits = [s.strip() for s in subreddits.split(",") if s.strip()]
    if not subreddits:
        raise SystemExit(
            "No subreddits configured. Add them under 'subreddits:' in "
            "config.yaml or pass --subreddits sub1 sub2 ..."
        )

    since = parse_timespec(pick("since", "since", default="2y"), is_since=True)
    until = parse_timespec(pick("until", "until", default="now"), is_since=False)
    if until <= since:
        raise SystemExit(f"'until' ({until}) must be after 'since' ({since}).")

    data_dir = Path(pick("data_dir", "output", "data_dir", default="./data"))
    db_path = Path(pick("db_path", "output", "db_path", default="./scraper.db"))

    cfg = Config(
        subreddits=list(subreddits),
        since=since,
        until=until,
        fetch_posts=bool(pick("fetch_posts", "fetch", "posts", default=True)),
        fetch_comments=bool(pick("fetch_comments", "fetch", "comments", default=True)),
        reconstruct_threads=bool(
            pick("reconstruct_threads", "fetch", "reconstruct_threads", default=True)),
        api_trees=bool(pick("api_trees", "fetch", "api_trees", default=False)),
        subreddit_meta=bool(pick("subreddit_meta", "fetch", "subreddit_meta", default=False)),
        live_enabled=bool(pick("live_enabled", "live_refresh", "enabled", default=False)),
        live_window_days=int(pick("live_window_days", "live_refresh", "window_days", default=30)),
        live_min_delay=float(_deep_get(raw, "live_refresh", "min_delay", default=2.0)),
        live_max_delay=float(_deep_get(raw, "live_refresh", "max_delay", default=5.0)),
        live_headless=bool(pick("live_headless", "live_refresh", "headless", default=True)),
        live_use_proxies=bool(
            pick("live_use_proxies", "live_refresh", "use_proxies", default=False)),
        live_limit=pick("live_limit", "live_refresh", "limit", default=None),
        workers=int(pick("workers", "workers", default=1)),
        data_dir=data_dir,
        db_path=db_path,
        write_jsonl=bool(pick("write_jsonl", "output", "formats", "jsonl", default=True)),
        write_csv=bool(pick("write_csv", "output", "formats", "csv", default=False)),
        http_cache=bool(pick("http_cache", "network", "http_cache", default=True)),
        page_limit=int(_deep_get(raw, "network", "page_limit", default=100)),
        request_timeout=int(_deep_get(raw, "network", "request_timeout", default=45)),
        min_request_interval=float(_deep_get(raw, "network", "min_request_interval", default=0.25)),
        ratelimit_floor=int(_deep_get(raw, "network", "ratelimit_floor", default=20)),
        max_retries=int(_deep_get(raw, "network", "max_retries", default=9)),
        backoff_base=float(_deep_get(raw, "network", "backoff_base", default=2)),
        backoff_cap=float(_deep_get(raw, "network", "backoff_cap", default=60)),
        tree_limit=int(_deep_get(raw, "network", "tree_limit", default=25000)),
        tree_breadth=int(_deep_get(raw, "network", "tree_breadth", default=100000)),
        tree_depth=int(_deep_get(raw, "network", "tree_depth", default=100000)),
    )
    # http cache sits next to the db (no extension; requests-cache adds .sqlite)
    cfg.http_cache_path = db_path.parent / "http_cache"
    return cfg
