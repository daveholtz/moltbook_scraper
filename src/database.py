"""Database operations for Moltbook scraper."""

import sqlite3
import json
from datetime import datetime
from typing import Optional


class Database:
    """SQLite database for storing scraped Moltbook data."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()

    def _create_tables(self):
        """Create database tables if they don't exist."""
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS agents (
                name TEXT PRIMARY KEY,
                id TEXT,
                description TEXT,
                karma INTEGER,
                is_claimed BOOLEAN,
                follower_count INTEGER,
                following_count INTEGER,
                avatar_url TEXT,
                owner_json TEXT,
                metadata_json TEXT,
                created_at TEXT,
                first_seen_at TEXT DEFAULT CURRENT_TIMESTAMP,
                last_updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS posts (
                id TEXT PRIMARY KEY,
                title TEXT,
                content TEXT,
                url TEXT,
                author_name TEXT,
                submolt_name TEXT,
                upvotes INTEGER,
                downvotes INTEGER,
                comment_count INTEGER,
                is_pinned BOOLEAN,
                created_at TEXT,
                first_seen_at TEXT DEFAULT CURRENT_TIMESTAMP,
                last_updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS submolts (
                name TEXT PRIMARY KEY,
                id TEXT,
                display_name TEXT,
                description TEXT,
                subscriber_count INTEGER,
                avatar_url TEXT,
                banner_url TEXT,
                created_by_name TEXT,
                metadata_json TEXT,
                created_at TEXT,
                last_activity_at TEXT,
                first_seen_at TEXT DEFAULT CURRENT_TIMESTAMP,
                last_updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS agent_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_name TEXT,
                scraped_at TEXT DEFAULT CURRENT_TIMESTAMP,
                karma INTEGER,
                follower_count INTEGER,
                following_count INTEGER
            );

            CREATE TABLE IF NOT EXISTS scrape_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT,
                completed_at TEXT,
                agents_scraped INTEGER,
                posts_scraped INTEGER,
                submolts_scraped INTEGER,
                status TEXT
            );
        """)
        self.conn.commit()

    def upsert_agent(self, agent: dict):
        """Insert or update an agent."""
        now = datetime.utcnow().isoformat()
        owner_json = json.dumps(agent.get("owner")) if agent.get("owner") else None

        self.conn.execute("""
            INSERT INTO agents (name, id, description, karma, is_claimed,
                              follower_count, following_count, avatar_url,
                              owner_json, created_at, last_updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                id = excluded.id,
                description = excluded.description,
                karma = excluded.karma,
                is_claimed = excluded.is_claimed,
                follower_count = excluded.follower_count,
                following_count = excluded.following_count,
                avatar_url = excluded.avatar_url,
                owner_json = excluded.owner_json,
                last_updated_at = excluded.last_updated_at
        """, (
            agent["name"],
            agent.get("id"),
            agent.get("description"),
            agent.get("karma"),
            agent.get("is_claimed"),
            agent.get("follower_count"),
            agent.get("following_count"),
            agent.get("avatar_url"),
            owner_json,
            agent.get("created_at"),
            now,
        ))
        self.conn.commit()

    def get_agent(self, name: str) -> Optional[dict]:
        """Get an agent by name."""
        cursor = self.conn.execute(
            "SELECT * FROM agents WHERE name = ?", (name,)
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return dict(row)

    def upsert_post(self, post: dict):
        """Insert or update a post."""
        now = datetime.utcnow().isoformat()
        author_name = post.get("author", {}).get("name") if post.get("author") else None
        submolt_name = post.get("submolt", {}).get("name") if post.get("submolt") else None

        self.conn.execute("""
            INSERT INTO posts (id, title, content, url, author_name, submolt_name,
                             upvotes, downvotes, comment_count, is_pinned,
                             created_at, last_updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                title = excluded.title,
                content = excluded.content,
                url = excluded.url,
                upvotes = excluded.upvotes,
                downvotes = excluded.downvotes,
                comment_count = excluded.comment_count,
                is_pinned = excluded.is_pinned,
                last_updated_at = excluded.last_updated_at
        """, (
            post["id"],
            post.get("title"),
            post.get("content"),
            post.get("url"),
            author_name,
            submolt_name,
            post.get("upvotes"),
            post.get("downvotes"),
            post.get("comment_count"),
            post.get("is_pinned"),
            post.get("created_at"),
            now,
        ))
        self.conn.commit()

    def get_post(self, post_id: str) -> Optional[dict]:
        """Get a post by ID."""
        cursor = self.conn.execute(
            "SELECT * FROM posts WHERE id = ?", (post_id,)
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return dict(row)

    def upsert_submolt(self, submolt: dict):
        """Insert or update a submolt."""
        now = datetime.utcnow().isoformat()
        created_by_name = submolt.get("created_by", {}).get("name") if submolt.get("created_by") else None

        self.conn.execute("""
            INSERT INTO submolts (name, id, display_name, description,
                                subscriber_count, avatar_url, banner_url,
                                created_by_name, created_at, last_activity_at,
                                last_updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                id = excluded.id,
                display_name = excluded.display_name,
                description = excluded.description,
                subscriber_count = excluded.subscriber_count,
                avatar_url = excluded.avatar_url,
                banner_url = excluded.banner_url,
                last_activity_at = excluded.last_activity_at,
                last_updated_at = excluded.last_updated_at
        """, (
            submolt["name"],
            submolt.get("id"),
            submolt.get("display_name"),
            submolt.get("description"),
            submolt.get("subscriber_count"),
            submolt.get("avatar_url"),
            submolt.get("banner_url"),
            created_by_name,
            submolt.get("created_at"),
            submolt.get("last_activity_at"),
            now,
        ))
        self.conn.commit()

    def get_submolt(self, name: str) -> Optional[dict]:
        """Get a submolt by name."""
        cursor = self.conn.execute(
            "SELECT * FROM submolts WHERE name = ?", (name,)
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return dict(row)

    def get_all_agent_names(self) -> list[str]:
        """Get all agent names in the database."""
        cursor = self.conn.execute("SELECT name FROM agents")
        return [row[0] for row in cursor.fetchall()]

    def close(self):
        """Close the database connection."""
        self.conn.close()
