# reddit-scraper

A general-purpose, no-auth Reddit scraper built on the [Arctic Shift](https://arctic-shift.photon-reddit.com) archive. Point it at any subreddit and it will backfill every post and comment over a date range you choose - no Reddit account, API key, or OAuth app required.

Everything is driven by a single `config.yaml`, and every setting can be overridden from the command line.

## Features

- **No authentication** - uses the public Arctic Shift archive, which mirrors Reddit in near real-time.
- **Full backfill** - paginates every post and comment in a subreddit over any date window (`2y`, `90d`, an absolute date, or `all` the way back to 2005).
- **Offline thread reconstruction** - rebuilds nested reply trees from the flat comment dump with zero extra network calls.
- **Parallel** - time-shards the date window across worker threads for ~8x faster scraping on large subs.
- **Resumable & idempotent** - SQLite is the source of truth; a killed run picks up exactly where it left off, and re-runs never duplicate data.
- **Incremental re-runs** - the scraper tracks exactly which date range it has already covered per subreddit, so repeat runs fetch only what's new (or the older history, if you widen `since`) instead of starting over.
- **Persistent HTTP cache** - identical queries are never re-issued across runs.
- **Multiple output formats** - SQLite (always), JSONL (full fidelity), and optional flattened CSV.
- **Optional live refresh** - opens a real browser (Playwright) to read up-to-the-minute score/comment counts for recent posts, since archived counts can be stale.

## Installation

Requires Python 3.10+.

```bash
git clone https://github.com/adityabatra072/reddit-scraper.git
cd reddit-scraper
pip install -e .            # installs the `reddit-scraper` command
```

The optional `--live` score refresh feature needs Playwright and a Chromium browser:

```bash
pip install -e ".[live]"
playwright install chromium
```

## Quick start

Edit `config.yaml` to list the subreddits and date range you want, then run:

```bash
reddit-scraper            # or: python -m reddit_scraper
```

Output lands in `./data/<subreddit>/` (JSONL) and `./scraper.db` (SQLite).

Add `-v` for verbose (DEBUG) logging or `-q` to show warnings and errors only.

## Usage

Everything in `config.yaml` can be overridden on the command line:

```bash
# Run exactly what's in config.yaml
python -m reddit_scraper

# Scrape different subs / window without editing the file
python -m reddit_scraper --subreddits python rust golang --since 1y

# Posts only, last 90 days, also write CSV
python -m reddit_scraper --subreddits news --since 90d --no-comments --csv

# Just rebuild threads from already-downloaded comments
python -m reddit_scraper --stage threads

# Live-refresh recent post scores (needs playwright)
python -m reddit_scraper --stage refresh --live

# Show what's in the database
python -m reddit_scraper --stage stats

# Export everything already scraped to CSV
python -m reddit_scraper --stage export
```

Run `python -m reddit_scraper --help` for the full flag list.

### Stages

| Stage | What it does |
|-------|--------------|
| `all` (default) | backfill + thread reconstruction (+ live refresh if enabled) |
| `backfill` | download posts and comments only |
| `threads` | rebuild nested reply trees from already-downloaded comments |
| `refresh` | live-refresh recent post scores via a browser |
| `stats` | print row counts in the database |
| `export` | write flattened `posts.csv` / `comments.csv` |

## Configuration

`config.yaml` is fully commented. Key sections:

- **`subreddits`** - list of subreddits to scrape.
- **`since` / `until`** - the post/comment creation window. Accepts relative ages (`2y`, `18m`, `90d`, `12h`), absolute dates (`2024-01-31`), epoch seconds, `now`, and `all`.
- **`fetch`** - toggle posts, comments, offline thread reconstruction, per-post API trees, and subreddit metadata independently.
- **`live_refresh`** - optional browser-based score refresh for recent posts.
- **`workers`** - number of concurrent time-sharded workers (Arctic Shift tolerates 10–20).
- **`output`** - output directory, database path, and which formats (JSONL / CSV) to write.
- **`network`** - pacing, retry, and rate-limit tuning (sane defaults; rarely need changing).

## Output

For each subreddit, under `<data_dir>/<subreddit>/`:

| File | Contents |
|------|----------|
| `posts.jsonl` | one JSON object per submission (full archive fidelity) |
| `comments.jsonl` | one JSON object per comment (flat) |
| `threads.jsonl` | one line per post: the reconstructed nested reply tree |
| `comment_trees.jsonl` | per-post trees from the API (only if `api_trees` is enabled) |
| `posts.csv` / `comments.csv` | flattened scalar columns (only if CSV output is enabled) |

`scraper.db` (SQLite) is always written and is the authoritative, deduplicated store. Every post/comment is keyed by its Reddit id, so it is safe to re-run at any time.

## Architecture

The pipeline is a set of small, single-responsibility modules under `reddit_scraper/`:

| Module | Role |
|--------|------|
| `config.py` | loads YAML, resolves date specs, applies CLI overrides into one `Config` object |
| `arctic_client.py` | HTTP client for the Arctic Shift API (caching, pacing, rate-limit-aware retries) |
| `cache.py` | SQLite source-of-truth (posts, comments, trees, resume cursors, covered ranges) |
| `backfill.py` | sequential single-worker backfill |
| `parallel.py` | time-sharded parallel backfill driver |
| `coverage.py` | covered-range routing shared by both drivers (fetch only what's missing) |
| `reconstruct.py` | offline nested-thread reconstruction from flat comments |
| `writer.py` | append-only JSONL writers and CSV export |
| `live_refresh.py` | Playwright-based live score refresh |
| `logging_util.py` | tqdm-aware logging setup |
| `__main__.py` | CLI entry point and stage orchestration |

## Development

```bash
pip install -e ".[dev]"   # editable install + pytest, ruff, playwright
pytest                    # run the (offline) test suite
ruff check .              # lint
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for details. CI runs lint and tests across Python 3.10–3.13.

## Notes & etiquette

- This tool reads a public third-party archive. Please be respectful of Arctic Shift's infrastructure - the default pacing and worker counts stay well within its published limits. Don't crank the workers into the hundreds.
- Scraped data is subject to Reddit's content policy and the rights of the original authors. Use it responsibly.

## License

MIT - see [LICENSE](LICENSE).
