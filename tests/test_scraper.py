"""Tests for scraper orchestration."""

import tempfile
import os
import responses
import pytest
from src.scraper import Scraper
from src.client import MoltbookClient
from src.database import Database


class TestScraperFullScrape:
    """Tests for full scrape functionality."""

    @responses.activate
    def test_full_scrape_stores_submolts(self):
        """Full scrape should fetch and store all submolts."""
        # Mock submolts endpoint
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/submolts",
            json={
                "success": True,
                "submolts": [
                    {
                        "id": "sub1",
                        "name": "general",
                        "display_name": "General",
                        "subscriber_count": 100,
                    }
                ],
            },
            status=200,
        )
        # Empty page signals end of submolt pagination
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/submolts",
            json={"success": True, "submolts": []},
            status=200,
        )
        # Mock posts endpoint (empty for this test)
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/posts",
            json={"success": True, "posts": [], "has_more": False},
            status=200,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            db = Database(db_path)
            client = MoltbookClient(api_key="test_key")
            scraper = Scraper(client, db)

            scraper.scrape_submolts()

            submolt = db.get_submolt("general")
            assert submolt is not None
            assert submolt["subscriber_count"] == 100

    @responses.activate
    def test_full_scrape_stores_posts_and_extracts_authors(self):
        """Full scrape should store posts and extract author info."""
        # Mock posts endpoint with cursor pagination
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/posts",
            json={
                "success": True,
                "posts": [
                    {
                        "id": "post1",
                        "title": "Hello",
                        "content": "World",
                        "upvotes": 10,
                        "author": {
                            "id": "a1",
                            "name": "testbot",
                            "karma": 42,
                        },
                        "submolt": {"name": "general"},
                    }
                ],
                "has_more": False,
                "next_cursor": None,
            },
            status=200,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            db = Database(db_path)
            client = MoltbookClient(api_key="test_key")
            scraper = Scraper(client, db)

            scraper.scrape_posts()

            # Post should be stored
            post = db.get_post("post1")
            assert post is not None
            assert post["title"] == "Hello"

            # Author should be extracted and stored
            agent = db.get_agent("testbot")
            assert agent is not None
            assert agent["karma"] == 42

            # Submolt should be extracted and stored as a stub
            submolt = db.get_submolt("general")
            assert submolt is not None


class TestScraperProgress:
    """Tests for progress reporting."""

    @responses.activate
    def test_scraper_calls_progress_callback(self):
        """Scraper should call progress callback during scrape."""
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/submolts",
            json={"success": True, "submolts": [{"name": "test", "id": "1"}]},
            status=200,
        )
        # Empty page signals end of submolt pagination
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/submolts",
            json={"success": True, "submolts": []},
            status=200,
        )

        progress_calls = []

        def on_progress(msg):
            progress_calls.append(msg)

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            db = Database(db_path)
            client = MoltbookClient(api_key="test_key")
            scraper = Scraper(client, db, on_progress=on_progress)

            scraper.scrape_submolts()

            assert len(progress_calls) > 0
            assert any("submolt" in msg.lower() for msg in progress_calls)


class TestScraperAgentEnrichment:
    """Tests for agent profile enrichment."""

    @responses.activate
    def test_enrich_agents_fetches_full_profiles(self):
        """Scraper should fetch full profiles for agents."""
        # Mock agent profile endpoint
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/agents/profile",
            json={
                "success": True,
                "agent": {
                    "id": "a1",
                    "name": "testbot",
                    "karma": 100,
                    "follower_count": 50,
                    "following_count": 25,
                    "owner": {"x_handle": "testuser"},
                },
            },
            status=200,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            db = Database(db_path)
            # Pre-insert a basic agent
            db.upsert_agent({"name": "testbot", "karma": 10})

            client = MoltbookClient(api_key="test_key")
            scraper = Scraper(client, db)

            scraper.enrich_agents()

            agent = db.get_agent("testbot")
            assert agent["karma"] == 100  # Updated from API
            assert agent["follower_count"] == 50


class TestScraperSubmoltEnrichment:
    """Tests for submolt detail enrichment."""

    @responses.activate
    def test_enrich_submolts_fetches_full_details(self):
        """Scraper should fetch full details for submolts and extract creators."""
        # Mock submolt detail endpoint
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/submolts/general",
            json={
                "success": True,
                "submolt": {
                    "id": "sub1",
                    "name": "general",
                    "display_name": "General",
                    "description": "The town square",
                    "subscriber_count": 500,
                    "post_count": 5000,
                    "is_nsfw": False,
                    "is_private": False,
                    "creator_id": "c1",
                    "created_by": {
                        "id": "c1",
                        "name": "creator_bot",
                        "karma": 100,
                        "followerCount": 50,
                    },
                    "created_at": "2026-01-27T18:01:09.076Z",
                },
            },
            status=200,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            db = Database(db_path)
            # Pre-insert a minimal submolt
            db.upsert_submolt({"name": "general", "subscriber_count": 100})
            db.commit()

            client = MoltbookClient(api_key="test_key")
            scraper = Scraper(client, db)

            scraper.enrich_submolts()

            submolt = db.get_submolt("general")
            assert submolt["subscriber_count"] == 500  # Updated from API
            assert submolt["post_count"] == 5000
            assert submolt["creator_id"] == "c1"

            # Creator agent should be extracted and stored
            agent = db.get_agent("creator_bot")
            assert agent is not None
            assert agent["follower_count"] == 50


class TestScraperIncremental:
    """Tests for incremental scraping."""

    @responses.activate
    def test_incremental_scrape_skips_known_posts(self):
        """Incremental scrape should stop when hitting known posts."""
        # First call returns new and old posts with cursor pagination
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/posts",
            json={
                "success": True,
                "posts": [
                    {"id": "new_post", "title": "New"},
                    {"id": "old_post", "title": "Old"},
                ],
                "has_more": True,
                "next_cursor": "some_cursor",
            },
            status=200,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            db = Database(db_path)
            # Pre-insert old post
            db.upsert_post({"id": "old_post", "title": "Old"})

            client = MoltbookClient(api_key="test_key")
            scraper = Scraper(client, db)

            new_count = scraper.scrape_posts_incremental()

            # Should have found 1 new post
            assert new_count == 1
            # New post should be stored
            assert db.get_post("new_post") is not None
