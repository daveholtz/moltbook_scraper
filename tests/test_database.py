"""Tests for database operations."""

import sqlite3
import tempfile
import os
import pytest
from src.database import Database


class TestDatabaseSchema:
    """Tests for database schema creation."""

    def test_creates_agents_table(self):
        """Database should create agents table on init."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            db = Database(db_path)

            # Check table exists
            conn = sqlite3.connect(db_path)
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='agents'"
            )
            assert cursor.fetchone() is not None
            conn.close()

    def test_creates_posts_table(self):
        """Database should create posts table on init."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            db = Database(db_path)

            conn = sqlite3.connect(db_path)
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='posts'"
            )
            assert cursor.fetchone() is not None
            conn.close()

    def test_creates_submolts_table(self):
        """Database should create submolts table on init."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            db = Database(db_path)

            conn = sqlite3.connect(db_path)
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='submolts'"
            )
            assert cursor.fetchone() is not None
            conn.close()


class TestDatabaseUpsertAgent:
    """Tests for upserting agents."""

    def test_upsert_agent_inserts_new_agent(self):
        """Database should insert a new agent."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            db = Database(db_path)

            agent = {
                "id": "agent123",
                "name": "TestBot",
                "description": "A test bot",
                "karma": 42,
                "is_claimed": True,
                "follower_count": 10,
                "following_count": 5,
            }
            db.upsert_agent(agent)

            result = db.get_agent("TestBot")
            assert result is not None
            assert result["name"] == "TestBot"
            assert result["karma"] == 42

    def test_upsert_agent_updates_existing_agent(self):
        """Database should update an existing agent's fields."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            db = Database(db_path)

            agent = {
                "id": "agent123",
                "name": "TestBot",
                "karma": 10,
            }
            db.upsert_agent(agent)

            # Update karma
            agent["karma"] = 50
            db.upsert_agent(agent)

            result = db.get_agent("TestBot")
            assert result["karma"] == 50


class TestDatabaseUpsertPost:
    """Tests for upserting posts."""

    def test_upsert_post_inserts_new_post(self):
        """Database should insert a new post."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            db = Database(db_path)

            post = {
                "id": "post123",
                "title": "Test Post",
                "content": "Hello world",
                "author": {"name": "testbot"},
                "submolt": {"name": "general"},
                "upvotes": 10,
                "downvotes": 1,
                "comment_count": 5,
            }
            db.upsert_post(post)

            result = db.get_post("post123")
            assert result is not None
            assert result["title"] == "Test Post"
            assert result["upvotes"] == 10


class TestDatabaseUpsertSubmolt:
    """Tests for upserting submolts."""

    def test_upsert_submolt_inserts_new_submolt(self):
        """Database should insert a new submolt."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            db = Database(db_path)

            submolt = {
                "id": "sub123",
                "name": "general",
                "display_name": "General",
                "description": "The town square",
                "subscriber_count": 100,
                "created_by": {"name": "admin"},
            }
            db.upsert_submolt(submolt)

            result = db.get_submolt("general")
            assert result is not None
            assert result["display_name"] == "General"
            assert result["subscriber_count"] == 100
