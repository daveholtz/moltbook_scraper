"""Moltbook API client."""

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
        """Make a request with retry logic for rate limiting."""
        for attempt in range(self.max_retries + 1):
            self.request_count += 1
            if self.on_request:
                self.on_request(url)

            response = self.session.request(method, url, **kwargs)

            if response.status_code == 429:
                if attempt < self.max_retries:
                    delay = self.base_delay * (2 ** attempt)
                    time.sleep(delay)
                    continue
                else:
                    raise RateLimitError(
                        f"Rate limited after {self.max_retries} retries (429)"
                    )

            return response

        # Should not reach here
        raise RateLimitError("Max retries exceeded")

    def fetch_submolts(self) -> list[dict]:
        """Fetch all submolts from the API."""
        response = self._request("GET", f"{self.BASE_URL}/submolts")
        response.raise_for_status()
        data = response.json()
        return data["submolts"]

    def fetch_posts(self, offset: int = 0) -> list[dict]:
        """Fetch a page of posts from the API."""
        params = {}
        if offset > 0:
            params["offset"] = offset
        response = self._request("GET", f"{self.BASE_URL}/posts", params=params)
        response.raise_for_status()
        data = response.json()
        return data["posts"]

    def fetch_all_posts(
        self,
        on_page: Optional[Callable[[int, int], None]] = None,
    ) -> list[dict]:
        """Fetch all posts, paginating through all pages.

        Args:
            on_page: Optional callback called with (page_num, posts_so_far)
        """
        all_posts = []
        offset = 0
        page = 0
        while True:
            params = {"offset": offset} if offset > 0 else {}
            response = self._request("GET", f"{self.BASE_URL}/posts", params=params)
            response.raise_for_status()
            data = response.json()
            all_posts.extend(data["posts"])
            page += 1
            if on_page:
                on_page(page, len(all_posts))
            if not data.get("has_more", False):
                break
            offset = data.get("next_offset", offset + 25)
        return all_posts

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
