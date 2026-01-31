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

        def on_page(page_num, total):
            self._log(f"  Page {page_num}: {total} submolts so far")

        submolts = self.client.fetch_submolts(on_page=on_page)
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

        success_count = 0
        error_count = 0
        for i, name in enumerate(agents):
            try:
                profile = self.client.fetch_agent_profile(name)
                if profile:
                    self.db.upsert_agent(profile)
                    success_count += 1
            except Exception as e:
                error_count += 1
                # Don't log every error, just track count
            if (i + 1) % 100 == 0:
                self._log(f"  Progress: {i + 1}/{len(agents)} ({success_count} enriched, {error_count} errors)")

        self._log(f"Enriched {success_count} agents ({error_count} errors)")

    def scrape_comments(self, only_missing: bool = False):
        """Fetch comments for posts.

        Args:
            only_missing: If True, only fetch comments for posts without any comments.
                         If False, fetch for all posts (updates existing).
        """
        if only_missing:
            post_ids = self.db.get_post_ids_without_comments()
            self._log(f"Fetching comments for {len(post_ids)} posts (missing only)...")
        else:
            post_ids = self.db.get_all_post_ids()
            self._log(f"Fetching comments for {len(post_ids)} posts...")

        total_comments = 0
        error_count = 0
        for i, post_id in enumerate(post_ids):
            try:
                result = self.client.fetch_post_with_comments(post_id)
                if result and result.get("comments"):
                    comments = result["comments"]
                    self._store_comments_recursive(comments, post_id)
                    total_comments += self._count_comments_recursive(comments)
                    self.db.commit()
            except Exception as e:
                error_count += 1

            if (i + 1) % 50 == 0:
                self._log(f"  Progress: {i + 1}/{len(post_ids)} posts ({total_comments} comments, {error_count} errors)")

        self._log(f"Stored {total_comments} comments ({error_count} errors)")

    def _store_comments_recursive(self, comments: list, post_id: str):
        """Store comments and their nested replies."""
        for comment in comments:
            self.db.upsert_comment(comment, post_id)
            if comment.get("author"):
                self.db.upsert_agent(comment["author"])
            if comment.get("replies"):
                self._store_comments_recursive(comment["replies"], post_id)

    def _count_comments_recursive(self, comments: list) -> int:
        """Count total comments including nested replies."""
        count = len(comments)
        for comment in comments:
            if comment.get("replies"):
                count += self._count_comments_recursive(comment["replies"])
        return count

    def create_snapshots(self):
        """Create daily snapshots of post, comment, and agent metrics."""
        self._log("Creating snapshots...")

        # Post snapshots
        cursor = self.db.conn.execute("SELECT id, upvotes, downvotes, comment_count FROM posts")
        post_count = 0
        for row in cursor.fetchall():
            self.db.save_post_snapshot(row[0], row[1], row[2], row[3])
            post_count += 1
        self._log(f"  Created {post_count} post snapshots")

        # Comment snapshots
        cursor = self.db.conn.execute("SELECT id, upvotes, downvotes FROM comments")
        comment_count = 0
        for row in cursor.fetchall():
            self.db.save_comment_snapshot(row[0], row[1], row[2])
            comment_count += 1
        self._log(f"  Created {comment_count} comment snapshots")

        # Agent snapshots
        cursor = self.db.conn.execute("SELECT name, karma, follower_count, following_count FROM agents WHERE karma IS NOT NULL")
        agent_count = 0
        for row in cursor.fetchall():
            self.db.save_agent_snapshot(row[0], row[1], row[2], row[3])
            agent_count += 1
        self._log(f"  Created {agent_count} agent snapshots")

        self.db.commit()
        self._log("Snapshots complete")

    def full_scrape(self):
        """Run a complete scrape of all data."""
        self._log("Starting full scrape...")
        self.scrape_submolts()
        self.scrape_posts()
        self.scrape_comments()
        self.enrich_agents()
        self.create_snapshots()
        self._log("Full scrape complete")
