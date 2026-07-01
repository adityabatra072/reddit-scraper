# Contributing

Thanks for your interest in improving reddit-scraper. Contributions of all kinds are welcome — bug reports, features, docs, and tests.

## Development setup

```bash
git clone https://github.com/adityabatra072/reddit-scraper.git
cd reddit-scraper
python -m venv .venv && source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -e ".[dev]"
```

This installs the package in editable mode along with the dev tools (`pytest`, `ruff`, `playwright`).

## Running tests

```bash
pytest
```

The test suite is offline and fast — it covers date parsing, config override precedence, time-shard splitting, thread reconstruction, and the SQLite cache. No network access is required.

## Linting

```bash
ruff check .
```

CI runs both `ruff check` and `pytest` across Python 3.10–3.13; please make sure both pass locally before opening a PR.

## Guidelines

- Keep modules small and single-responsibility, matching the existing layout (see the Architecture table in the README).
- Anything user-facing should be driven by `config.yaml` with a matching CLI override.
- Progress goes through `tqdm`; everything else uses the package logger (`reddit_scraper.logging_util`), not `print`.
- Add tests for new pure-function logic.
- Please be considerate of Arctic Shift's infrastructure — don't add defaults that hammer the API beyond its published limits.

## Reporting bugs

Open an issue with the command you ran, the config, and the full output. If it's a scraping/network issue, include the subreddit and date window.
