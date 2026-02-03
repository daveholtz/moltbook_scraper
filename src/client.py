"""Moltbook API client."""

import random
import time
from typing import Optional, Callable

import requests


class RateLimitError(Exception):
    """Raised when rate limit is exceeded and retries are exhausted."""
    pass


class MoltbookClient:
    """Client for interacting with the Moltbook API."""

    BASE_URL = "https://www.moltbook.com/api/v1"

    def __init__(
        self,
        api_key: str,
        max_retries: int = 3,
        base_delay: float = 1.0,
        on_request: Optional[Callable[[str], None]] = None,
    ):
        self.api_key = api_key
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.on_request = on_request
        self.request_count = 0
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        })

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make a request with retry logic for rate limiting and server errors."""
        # Set default timeout if not provided
        if "timeout" not in kwargs:
            kwargs["timeout"] = 30

        last_error = None
        for attempt in range(self.max_retries + 1):
            self.request_count += 1
            if self.on_request:
                self.on_request(url)

            try:
                response = self.session.request(method, url, **kwargs)
            except requests.exceptions.Timeout as e:
                last_error = e
                if attempt < self.max_retries:
                    delay = self.base_delay * (2 ** attempt)
                    time.sleep(delay)
                    continue
                raise

            # Retry on rate limit (429)
            if response.status_code == 429:
                if attempt < self.max_retries:
                    delay = self.base_delay * (2 ** attempt)
                    time.sleep(delay)
                    continue
                else:
                    raise RateLimitError(
                        f"Rate limited after {self.max_retries} retries (429)"
                    )

            # Retry on server errors (5xx)
            if response.status_code >= 500:
                if attempt < self.max_retries:
                    delay = self.base_delay * (2 ** attempt)
                    time.sleep(delay)
                    continue
                # On final attempt, return the response so raise_for_status can handle it
                return response

            return response

        # Should not reach here
        raise RateLimitError("Max retries exceeded")

    def fetch_submolts(
        self,
        on_page: Optional[Callable[[int, int], None]] = None,
    ) -> list[dict]:
        """Fetch all submolts, paginating through all pages.

        Args:
            on_page: Optional callback called with (page_num, submolts_so_far)
        """
        all_submolts = []
        offset = 0
        page = 0
        while True:
            params = {"offset": offset} if offset > 0 else {}
            response = self._request("GET", f"{self.BASE_URL}/submolts", params=params)
            response.raise_for_status()
            data = response.json()
            submolts = data.get("submolts", [])
            if not submolts:
                break
            all_submolts.extend(submolts)
            page += 1
            if on_page:
                on_page(page, len(all_submolts))
            # If we got fewer than 100 (the apparent page size), we're done
            if len(submolts) < 100:
                break
            offset += len(submolts)
        return all_submolts

    def fetch_submolts_streaming(
        self,
        on_page: Callable[[int, list[dict]], None],
        target_count: int = 0,
        max_stale_pages: int = 20,
    ) -> int:
        """Fetch all submolts, calling on_page with each batch for immediate saving.

        The API is non-deterministic, so we keep fetching until we hit target_count
        unique submolts, or until we've seen max_stale_pages consecutive pages with
        no new submolts.

        Args:
            on_page: Callback called with (page_num, submolts_list) for each page.
            target_count: Target number of unique submolts to collect. If 0, uses
                pagination signals (less reliable with flaky API).
            max_stale_pages: Stop after this many consecutive pages with <10% new submolts.

        Returns:
            Total number of unique submolts fetched.
        """
        total = 0
        offset = 0
        page = 0
        page_size = 100
        seen_names = set()  # Track unique submolts
        stale_pages = 0  # Count consecutive pages with mostly duplicates
        consecutive_errors = 0
        max_consecutive_errors = 10  # Give up after 10 consecutive failures

        while True:
            params = {"offset": offset} if offset > 0 else {}
            try:
                response = self._request("GET", f"{self.BASE_URL}/submolts", params=params)
                response.raise_for_status()
                data = response.json()
                submolts = data.get("submolts", [])
                consecutive_errors = 0  # Reset on success
            except Exception:
                consecutive_errors += 1
                if consecutive_errors >= max_consecutive_errors:
                    break  # Too many consecutive failures
                # Try a different offset
                offset = random.randint(0, max(target_count, 20000))
                time.sleep(self.base_delay * consecutive_errors)
                continue

            if not submolts:
                # Try a different offset range if we got empty results
                if target_count > 0 and total < target_count:
                    offset = (offset + 1000) % (target_count * 2)
                    stale_pages += 1
                    if stale_pages >= max_stale_pages:
                        break
                    continue
                break

            # Filter to only new submolts
            new_submolts = [s for s in submolts if s.get("name") not in seen_names]

            if new_submolts:
                page += 1
                on_page(page, new_submolts)
                total += len(new_submolts)
                for s in new_submolts:
                    seen_names.add(s.get("name"))
                stale_pages = 0  # Reset stale counter
            else:
                stale_pages += 1

            # Check if we've hit target
            if target_count > 0 and total >= target_count:
                break

            # Check if we're stuck (too many stale pages)
            if stale_pages >= max_stale_pages:
                break

            # Advance offset - jump around if we're seeing lots of duplicates
            if len(new_submolts) < len(submolts) * 0.5:
                # Mostly duplicates - try a different offset range
                offset = random.randint(0, max(target_count, 20000))
            else:
                offset += page_size

        return total

    def fetch_posts(self, offset: int = 0, limit: int = 100) -> list[dict]:
        """Fetch a page of posts from the API."""
        params = {"limit": limit}
        if offset > 0:
            params["offset"] = offset
        response = self._request("GET", f"{self.BASE_URL}/posts", params=params)
        response.raise_for_status()
        data = response.json()
        return data["posts"]

    def fetch_all_posts(
        self,
        on_page: Optional[Callable[[int, int], None]] = None,
        limit: int = 100,
    ) -> list[dict]:
        """Fetch all posts, paginating through all pages.

        Args:
            on_page: Optional callback called with (page_num, posts_so_far)
            limit: Number of posts per page (default 100)
        """
        all_posts = []
        offset = 0
        page = 0
        while True:
            params = {"limit": limit}
            if offset > 0:
                params["offset"] = offset
            response = self._request("GET", f"{self.BASE_URL}/posts", params=params)
            response.raise_for_status()
            data = response.json()
            posts = data.get("posts", [])
            if not posts:
                break
            all_posts.extend(posts)
            page += 1
            if on_page:
                on_page(page, len(all_posts))
            if len(posts) < limit:
                break
            offset += limit
        return all_posts

    def fetch_posts_streaming(
        self,
        on_page: Callable[[int, list[dict]], None],
        target_count: int = 0,
        max_stale_pages: int = 20,
    ) -> int:
        """Fetch all posts, calling on_page with each batch for immediate saving.

        The API is non-deterministic, so we keep fetching until we hit target_count
        unique posts, or until we've seen max_stale_pages consecutive pages with
        no new posts.

        Args:
            on_page: Callback called with (page_num, posts_list) for each page.
            target_count: Target number of unique posts to collect.
            max_stale_pages: Stop after this many consecutive pages with <10% new posts.

        Returns:
            Total number of unique posts fetched.
        """
        total_posts = 0
        offset = 0
        page = 0
        page_size = 100  # Posts now support 100 per page
        seen_ids = set()  # Track unique posts
        stale_pages = 0
        consecutive_errors = 0
        max_consecutive_errors = 10

        while True:
            params = {"limit": page_size}
            if offset > 0:
                params["offset"] = offset
            try:
                response = self._request("GET", f"{self.BASE_URL}/posts", params=params)
                response.raise_for_status()
            except Exception:
                consecutive_errors += 1
                if consecutive_errors >= max_consecutive_errors:
                    break
                offset = random.randint(0, max(target_count, 100000))
                time.sleep(self.base_delay * consecutive_errors)
                continue
            consecutive_errors = 0
            data = response.json()
            posts = data.get("posts", [])

            if not posts:
                if target_count > 0 and total_posts < target_count:
                    offset = (offset + 500) % (target_count * 2)
                    stale_pages += 1
                    if stale_pages >= max_stale_pages:
                        break
                    continue
                break

            # Filter to only new posts
            new_posts = [p for p in posts if p.get("id") not in seen_ids]

            if new_posts:
                page += 1
                on_page(page, new_posts)
                total_posts += len(new_posts)
                for p in new_posts:
                    seen_ids.add(p.get("id"))
                stale_pages = 0
            else:
                stale_pages += 1

            # Check if we've hit target
            if target_count > 0 and total_posts >= target_count:
                break

            # Check if we're stuck
            if stale_pages >= max_stale_pages:
                break

            # Advance offset - jump around if seeing lots of duplicates
            if len(new_posts) < len(posts) * 0.5:
                offset = random.randint(0, max(target_count, 100000))
            else:
                offset += page_size

        return total_posts

    def fetch_agent_profile(self, name: str) -> Optional[dict]:
        """Fetch an agent's profile by name. Returns None if not found."""
        response = self._request(
            "GET",
            f"{self.BASE_URL}/agents/profile",
            params={"name": name}
        )
        response.raise_for_status()
        data = response.json()
        if not data.get("success", False):
            return None
        return data["agent"]

    def fetch_platform_stats(self, max_retries: int = 10) -> dict:
        """Fetch platform-wide statistics from moltbook.

        Retries until all key values are non-zero (API is flaky).

        Args:
            max_retries: Maximum retries to get non-zero values.

        Returns:
            Dict with keys: agents, submolts, posts, comments
        """
        for attempt in range(max_retries):
            response = self._request("GET", f"{self.BASE_URL}/stats")
            response.raise_for_status()
            data = response.json()
            stats = {
                "agents": data.get("agents", 0),
                "submolts": data.get("submolts", 0),
                "posts": data.get("posts", 0),
                "comments": data.get("comments", 0),
            }
            # Check if all values are non-zero
            if all(v > 0 for v in stats.values()):
                return stats
            # Wait before retry
            if attempt < max_retries - 1:
                time.sleep(self.base_delay * (attempt + 1))

        # Return best effort if we couldn't get all non-zero
        return stats

    def fetch_submolt_moderators(self, submolt_name: str) -> list[dict]:
        """Fetch moderators for a submolt. Returns empty list if not found or error."""
        try:
            response = self._request(
                "GET",
                f"{self.BASE_URL}/submolts/{submolt_name}/moderators"
            )
            if response.status_code == 404:
                return []
            response.raise_for_status()
            data = response.json()
            return data.get("moderators", [])
        except Exception:
            return []

    def fetch_post_with_comments(self, post_id: str) -> Optional[dict]:
        """Fetch a post with its comments. Returns None if not found."""
        response = self._request(
            "GET",
            f"{self.BASE_URL}/posts/{post_id}"
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        data = response.json()
        if not data.get("success", False):
            return None
        return {
            "post": data.get("post"),
            "comments": data.get("comments", []),
        }
