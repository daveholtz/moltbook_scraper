"""Scraper orchestration for Moltbook."""

from typing import Optional, Callable

from src.client import MoltbookClient
from src.database import Database


class Scraper:
    """Orchestrates scraping of Moltbook data."""

    def __init__(
        self,
        client: MoltbookClient,
        db: Database,
        on_progress: Optional[Callable[[str], None]] = None,
    ):
        self.client = client
        self.db = db
        self.on_progress = on_progress

    def _log(self, message: str):
        """Log progress message."""
        if self.on_progress:
            self.on_progress(message)

    def scrape_submolts(self):
        """Fetch and store all submolts."""
        self._log("Fetching submolts...")
        submolts = self.client.fetch_submolts()
        for submolt in submolts:
            self.db.upsert_submolt(submolt)
        self._log(f"Stored {len(submolts)} submolts")

    def scrape_posts(self):
        """Fetch and store all posts, extracting author info."""
        self._log("Fetching all posts...")

        def on_page(page_num, total):
            self._log(f"  Page {page_num}: {total} posts so far")

        posts = self.client.fetch_all_posts(on_page=on_page)
        for post in posts:
            self.db.upsert_post(post)
            if post.get("author"):
                self.db.upsert_agent(post["author"])
        self._log(f"Stored {len(posts)} posts")

    def scrape_posts_incremental(self) -> int:
        """Fetch only new posts, stopping when we hit known posts.

        Returns:
            Number of new posts found.
        """
        self._log("Fetching new posts (incremental)...")
        new_count = 0
        offset = 0

        while True:
            posts = self.client.fetch_posts(offset=offset)
            if not posts:
                break

            found_known = False
            for post in posts:
                # Check if we already have this post
                if self.db.get_post(post["id"]) is not None:
                    found_known = True
                    continue

                # New post - store it
                self.db.upsert_post(post)
                if post.get("author"):
                    self.db.upsert_agent(post["author"])
                new_count += 1

            if found_known:
                # We hit known posts, stop pagination
                break

            offset += 25

        self._log(f"Found {new_count} new posts")
        return new_count

    def enrich_agents(self):
        """Fetch full profiles for all known agents."""
        agents = self.db.get_all_agent_names()
        self._log(f"Enriching {len(agents)} agent profiles...")

        for i, name in enumerate(agents):
            profile = self.client.fetch_agent_profile(name)
            if profile:
                self.db.upsert_agent(profile)
            if (i + 1) % 10 == 0:
                self._log(f"  Enriched {i + 1}/{len(agents)} agents")

        self._log(f"Enriched {len(agents)} agents")

    def full_scrape(self):
        """Run a complete scrape of all data."""
        self._log("Starting full scrape...")
        self.scrape_submolts()
        self.scrape_posts()
        self.enrich_agents()
        self._log("Full scrape complete")
