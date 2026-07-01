"""CLI entry point for the general Reddit scraper.

Everything is driven by config.yaml; every field can be overridden on the CLI.

Examples
--------
  # Run exactly what's in config.yaml:
  python -m reddit_scraper

  # Scrape different subs / window without touching the file:
  python -m reddit_scraper --subreddits python rust golang --since 1y

  # Posts only, last 90 days, write CSV too:
  python -m reddit_scraper --subreddits news --since 90d --no-comments --csv

  # Just rebuild threads from already-downloaded comments:
  python -m reddit_scraper --stage threads

  # Live-refresh recent post scores (needs playwright):
  python -m reddit_scraper --stage refresh --live

  # Show what's in the database:
  python -m reddit_scraper --stage stats

  # Export everything already scraped to CSV:
  python -m reddit_scraper --stage export
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone

from . import config as cfgmod
from .logging_util import get_logger, setup_logging

log = get_logger()


def _bool_pair(ap, name, dest, help_on):
    """Add --flag / --no-flag that resolve to True / False (default: unset=None)."""
    ap.add_argument(f"--{name}", dest=dest, action="store_true", default=None, help=help_on)
    ap.add_argument(f"--no-{name}", dest=dest, action="store_false", default=None)


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="reddit_scraper",
        description="General no-auth Reddit scraper (Arctic Shift archive). "
                    "Config lives in config.yaml; any flag below overrides it.",
    )
    ap.add_argument("--config", help="path to YAML config (default: config.yaml)")
    ap.add_argument("--stage", default="all",
                    choices=["all", "backfill", "threads", "refresh", "stats", "export"],
                    help="all=backfill+threads(+refresh if --live); "
                         "or run one stage. stats=show DB counts. export=write CSV.")

    ap.add_argument("--subreddits", nargs="*", help="subreddits to scrape (space separated)")
    ap.add_argument("--since", help="start of window: 2y / 90d / 2024-01-31 / epoch / all")
    ap.add_argument("--until", help="end of window: now / 2025-01-01 / epoch")
    ap.add_argument("--workers", type=int, help="parallel time-sharded workers (e.g. 10)")

    # fetch toggles
    _bool_pair(ap, "posts", "fetch_posts", "fetch submissions")
    _bool_pair(ap, "comments", "fetch_comments", "fetch comments")
    _bool_pair(ap, "threads", "reconstruct_threads", "rebuild nested threads offline")
    ap.add_argument("--api-trees", dest="api_trees", action="store_true", default=None,
                    help="also fetch nested trees from the API per post (slow)")

    # output
    ap.add_argument("--csv", dest="write_csv", action="store_true", default=None,
                    help="also export flattened CSV")
    _bool_pair(ap, "jsonl", "write_jsonl", "write JSONL dumps")
    ap.add_argument("--data-dir", dest="data_dir", help="output directory")
    ap.add_argument("--db", dest="db_path", help="SQLite database path")
    ap.add_argument("--no-http-cache", dest="http_cache", action="store_false", default=None,
                    help="disable persistent HTTP cache")

    # live refresh
    ap.add_argument("--live", dest="live_enabled", action="store_true", default=None,
                    help="enable live score refresh (browser)")
    ap.add_argument("--no-headless", dest="live_headless", action="store_false", default=None,
                    help="show the browser during live refresh")
    ap.add_argument("--proxies", dest="live_use_proxies", action="store_true", default=None,
                    help="rotate free proxies for live refresh")
    ap.add_argument("--refresh-limit", dest="live_limit", type=int,
                    help="cap posts refreshed (testing)")
    ap.add_argument("--incremental", action="store_true",
                    help="resume finished subs to catch newly-created items")

    # verbosity
    ap.add_argument("-v", "--verbose", action="store_true",
                    help="verbose (DEBUG) logging")
    ap.add_argument("-q", "--quiet", action="store_true",
                    help="quiet: warnings and errors only")
    return ap


def _fmt(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%d")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    setup_logging(1 if args.verbose else (-1 if args.quiet else 0))
    _control = ("config", "stage", "incremental", "verbose", "quiet")
    overrides = {k: v for k, v in vars(args).items()
                 if k not in _control and v is not None}
    cfg = cfgmod.load(args.config, overrides)

    from .cache import Cache
    cache = Cache(cfg)
    subs = cfg.subreddits

    if args.stage == "stats":
        log.info("=== overall === %s", cache.stats())
        for s in subs:
            log.info("r/%s: %s", s, cache.stats(s))
        cache.close()
        return 0

    if args.stage == "export":
        from .writer import export_csv
        for s in subs:
            res = export_csv(cfg, cache, s)
            log.info("[export] r/%s: %d posts.csv rows, %d comments.csv rows",
                     s, res["posts"], res["comments"])
        cache.close()
        return 0

    log.info("Subreddits: %s", ", ".join("r/" + s for s in subs))
    log.info("Window:     %s -> %s  (%d days) | workers=%d",
             _fmt(cfg.since), _fmt(cfg.until),
             (cfg.until - cfg.since) // 86400, cfg.workers)

    # --- backfill ----------------------------------------------------------
    if args.stage in ("backfill", "all"):
        if cfg.workers > 1:
            from .parallel import backfill_subreddit_parallel
            for s in subs:
                res = backfill_subreddit_parallel(cfg, cache, s, cfg.since, cfg.until,
                                                  cfg.workers, args.incremental)
                log.info("[backfill x%d] r/%s: +%d posts, +%d comments, %d threads",
                         cfg.workers, s, res.get("posts", 0),
                         res.get("comments", 0), res.get("threads", 0))
        else:
            from .arctic_client import ArcticClient
            client = ArcticClient(cfg)
            try:
                for s in subs:
                    res = backfill_subreddit(cfg, client, cache, s, cfg.since, cfg.until,
                                             args.incremental)
                    log.info("[backfill] r/%s: %s", s,
                             ", ".join(f"+{v} {k}" for k, v in res.items()))
            finally:
                client.close()

    # --- threads (sequential path / explicit stage) ------------------------
    # In parallel mode threads are already rebuilt per-sub by the driver.
    if cfg.reconstruct_threads and (
        args.stage == "threads" or (args.stage in ("backfill", "all") and cfg.workers <= 1)
    ):
        from .reconstruct import reconstruct_all
        totals = reconstruct_all(cfg, cache, subs)
        log.info("[threads] total: %s", totals)

    # --- live refresh ------------------------------------------------------
    if args.stage == "refresh" or (args.stage == "all" and cfg.live_enabled):
        from .live_refresh import refresh_recent
        res = refresh_recent(cfg, cache, subs)
        log.info("[refresh] %s", res)

    # --- optional CSV export ----------------------------------------------
    if cfg.write_csv and args.stage in ("backfill", "all"):
        from .writer import export_csv
        for s in subs:
            res = export_csv(cfg, cache, s)
            log.info("[export] r/%s: %d posts.csv rows, %d comments.csv rows",
                     s, res["posts"], res["comments"])

    log.info("=== final stats === %s", cache.stats())
    cache.close()
    return 0


# imported lazily above to keep --stats fast
from .backfill import backfill_subreddit  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
