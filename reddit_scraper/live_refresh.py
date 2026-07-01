"""Stage 2 — live refresh of upvote/comment counts via a real browser.

Arctic Shift stores ``score``/``num_comments`` shortly after a post is created,
so recent posts can have stale counts. Reddit's no-auth ``.json`` endpoints are
hard-blocked (HTTP 403), but the normal post page renders in a real browser and
the ``<shreddit-post>`` element exposes live ``score`` and ``comment-count``
attributes. We read those and update the ``live_*`` columns in SQLite.

Only posts created within ``live_window_days`` are refreshed. Paced with jitter;
an optional free-proxy rotation can be enabled if the IP gets throttled.
"""
from __future__ import annotations

import random
import time

from .cache import Cache
from .config import Config, now_epoch


def _load_proxies(cfg: Config) -> list[str]:
    import requests
    try:
        r = requests.get(cfg.proxyscrape_url, timeout=30)
        r.raise_for_status()
        return [ln.strip() for ln in r.text.splitlines() if ln.strip().startswith("http")]
    except Exception:
        return []


def refresh_recent(cfg: Config, cache: Cache, subreddits: list[str] | None = None) -> dict:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return {"error": "playwright not installed; run: pip install playwright && playwright install chromium"}

    cutoff = now_epoch() - cfg.live_window_days * 24 * 3600
    posts = cache.posts_needing_live_refresh(cutoff, subreddits)
    if cfg.live_limit:
        posts = posts[:cfg.live_limit]
    if not posts:
        return {"refreshed": 0, "failed": 0, "total": 0}

    proxies = _load_proxies(cfg) if cfg.live_use_proxies else []
    proxy_idx = 0
    refreshed = failed = 0
    headless = cfg.live_headless

    with sync_playwright() as p:
        launch_kwargs: dict = {"headless": headless}
        if proxies:
            launch_kwargs["proxy"] = {"server": proxies[0]}
        browser = p.chromium.launch(**launch_kwargs)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
        )
        page = context.new_page()

        for row in posts:
            permalink = row["permalink"]
            if not permalink:
                failed += 1
                continue
            url = "https://www.reddit.com" + permalink
            try:
                page.goto(url, timeout=45000, wait_until="domcontentloaded")
                page.wait_for_selector("shreddit-post", timeout=15000)
                data = page.evaluate(
                    """() => {
                        const el = document.querySelector('shreddit-post');
                        if (!el) return null;
                        return {
                            score: el.getAttribute('score'),
                            comments: el.getAttribute('comment-count'),
                        };
                    }"""
                )
                if data and data.get("score") is not None:
                    score = int(data["score"]) if str(data["score"]).lstrip("-").isdigit() else None
                    nc = int(data["comments"]) if data.get("comments") and str(data["comments"]).isdigit() else None
                    cache.update_live_counts(row["id"], score, nc, now_epoch())
                    refreshed += 1
                else:
                    failed += 1
            except Exception:
                failed += 1
                # On repeated failure with proxies enabled, rotate to next proxy.
                if proxies:
                    proxy_idx = (proxy_idx + 1) % len(proxies)
                    try:
                        context.close(); browser.close()
                    except Exception:
                        pass
                    browser = p.chromium.launch(headless=headless,
                                                proxy={"server": proxies[proxy_idx]})
                    context = browser.new_context()
                    page = context.new_page()
            time.sleep(random.uniform(cfg.live_min_delay, cfg.live_max_delay))

        context.close()
        browser.close()

    return {"refreshed": refreshed, "failed": failed, "total": len(posts)}
