"""General no-auth Reddit scraper built on the Arctic Shift archive.

Driven entirely by config.yaml; see README.md. Public API:

    from reddit_scraper import config
    cfg = config.load("config.yaml")
"""
from __future__ import annotations

__version__ = "1.0.0"
