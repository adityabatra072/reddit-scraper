"""HTTP client for the Arctic Shift Reddit archive API.

Arctic Shift (https://arctic-shift.photon-reddit.com) mirrors Reddit posts and
comments in near real-time, requires no auth/API key, and returns full objects.

This client adds:
  * a persistent on-disk HTTP cache (requests-cache) so an identical query is
    never re-issued across runs;
  * polite pacing + reading of ``x-ratelimit-remaining`` to back off proactively;
  * exponential backoff retry on 429/422/5xx/network errors and DB "warm-up" misses.

Docs: https://github.com/ArthurHeitmann/arctic_shift/tree/master/api
Pagination model: results are ordered by ``created_utc``; with ``sort=asc`` feed
the last item's ``created_utc`` as the next request's ``after`` until a short page.
"""
from __future__ import annotations

import time
from typing import Any

import requests

try:
    import requests_cache
except ImportError:  # optional, but listed in requirements
    requests_cache = None

from .config import Config


class ArcticShiftError(RuntimeError):
    pass


class ArcticClient:
    def __init__(self, cfg: Config, use_http_cache: bool | None = None):
        self.cfg = cfg
        use_cache = cfg.http_cache if use_http_cache is None else use_http_cache
        if use_cache and requests_cache is not None:
            # GET-only cache; never cache errors. 7-day expiry is plenty since
            # the archive for past data is immutable.
            self.session = requests_cache.CachedSession(
                cache_name=str(cfg.http_cache_path),
                backend="sqlite",
                expire_after=7 * 24 * 3600,
                allowable_codes=(200,),
                allowable_methods=("GET",),
                stale_if_error=True,
            )
        else:
            self.session = requests.Session()
        self.session.headers.update({"User-Agent": cfg.user_agent})
        self._last_request = 0.0

    # --- core ---------------------------------------------------------------
    def _get(self, endpoint: str, params: dict[str, Any]) -> dict:
        cfg = self.cfg
        url = f"{cfg.arctic_base}/{endpoint.lstrip('/')}"
        last_err: Exception | None = None
        for attempt in range(cfg.max_retries):
            self._pace()
            try:
                resp = self.session.get(url, params=params, timeout=cfg.request_timeout)
            except requests.RequestException as e:
                last_err = e
                self._sleep_backoff(attempt)
                continue

            self._respect_ratelimit(resp)

            if resp.status_code == 200:
                try:
                    payload = resp.json()
                except ValueError as e:
                    last_err = e
                    self._sleep_backoff(attempt)
                    continue
                if isinstance(payload, dict) and payload.get("error"):
                    err = str(payload["error"])
                    if self._is_transient(err):
                        # server-side query timeout / "slow down" -> retryable
                        last_err = ArcticShiftError(f"{endpoint}: {err}")
                        self._sleep_backoff(attempt)
                        continue
                    # genuinely bad query (e.g. invalid field): not retryable
                    raise ArcticShiftError(f"{endpoint}: {err} (params={params})")
                return payload
            # Arctic Shift uses 422 for a server query timeout with a JSON body
            # like {"error":"Timeout. Maybe slow down a bit"} — that is transient
            # and must be retried, not treated as fatal.
            if resp.status_code in (429, 422) or resp.status_code >= 500:
                last_err = ArcticShiftError(f"HTTP {resp.status_code} on {endpoint}: {resp.text[:150]}")
                self._sleep_backoff(attempt)
                continue
            raise ArcticShiftError(f"HTTP {resp.status_code} on {endpoint}: {resp.text[:200]}")

        raise ArcticShiftError(f"giving up on {endpoint} after {cfg.max_retries} retries: {last_err}")

    @staticmethod
    def _is_transient(error_text: str) -> bool:
        t = error_text.lower()
        return any(k in t for k in ("timeout", "slow down", "try again", "warming up", "busy"))

    def _pace(self) -> None:
        elapsed = time.monotonic() - self._last_request
        if elapsed < self.cfg.min_request_interval:
            time.sleep(self.cfg.min_request_interval - elapsed)
        self._last_request = time.monotonic()

    def _respect_ratelimit(self, resp: requests.Response) -> None:
        remaining = resp.headers.get("x-ratelimit-remaining")
        reset = resp.headers.get("x-ratelimit-reset")
        if remaining is None:
            return
        try:
            if float(remaining) <= self.cfg.ratelimit_floor:
                wait = float(reset) + 1 if reset else 10
                time.sleep(min(wait, 60))
        except (TypeError, ValueError):
            pass

    def _sleep_backoff(self, attempt: int) -> None:
        delay = self.cfg.backoff_base * (2 ** attempt)
        time.sleep(min(delay, self.cfg.backoff_cap))

    # --- endpoints ----------------------------------------------------------
    def search_posts(self, subreddit: str, after: int, before: int,
                     limit: int | None = None, sort: str = "asc") -> list[dict]:
        payload = self._get("posts/search", {
            "subreddit": subreddit,
            "after": after,
            "before": before,
            "limit": limit or self.cfg.page_limit,
            "sort": sort,
        })
        return payload.get("data") or []

    def search_comments(self, subreddit: str, after: int, before: int,
                        limit: int | None = None, sort: str = "asc") -> list[dict]:
        payload = self._get("comments/search", {
            "subreddit": subreddit,
            "after": after,
            "before": before,
            "limit": limit or self.cfg.page_limit,
            "sort": sort,
        })
        return payload.get("data") or []

    def comment_tree(self, link_id: str) -> Any:
        """Full nested comment tree for a post.

        Defaults collapse the tree (limit=50, start_breadth/depth=4), so we pass
        a large limit and high breadth/depth to return every comment fully
        expanded with its nested ``replies`` structure intact.
        """
        params: dict[str, Any] = {
            "link_id": link_id,
            "limit": self.cfg.tree_limit,
            "start_breadth": self.cfg.tree_breadth,
            "start_depth": self.cfg.tree_depth,
        }
        payload = self._get("comments/tree", params)
        return payload.get("data")

    def subreddit_meta(self, subreddit: str) -> dict | None:
        payload = self._get("subreddits/search", {
            "subreddit": subreddit,
            "fields": "display_name,subscribers,created_utc,public_description",
        })
        data = payload.get("data") or []
        return data[0] if data else None

    def close(self) -> None:
        self.session.close()
