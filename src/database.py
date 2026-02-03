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
                scrape_run_id INTEGER,
                agent_id TEXT,
                description TEXT,
                karma INTEGER,
                is_claimed BOOLEAN,
                follower_count INTEGER,
                following_count INTEGER,
                avatar_url TEXT,
                owner_json TEXT,
                metadata_json TEXT,
                created_at TEXT
            );

            CREATE TABLE IF NOT EXISTS scrape_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT,
                completed_at TEXT,
                agents_scraped INTEGER,
                posts_scraped INTEGER,
                comments_scraped INTEGER,
                submolts_scraped INTEGER,
                status TEXT
            );

            CREATE TABLE IF NOT EXISTS comments (
                id TEXT PRIMARY KEY,
                post_id TEXT,
                parent_id TEXT,
                content TEXT,
                author_name TEXT,
                upvotes INTEGER,
                downvotes INTEGER,
                created_at TEXT,
                first_seen_at TEXT DEFAULT CURRENT_TIMESTAMP,
                last_updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS post_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id TEXT,
                scraped_at TEXT DEFAULT CURRENT_TIMESTAMP,
                scrape_run_id INTEGER,
                title TEXT,
                content TEXT,
                url TEXT,
                author_name TEXT,
                submolt_name TEXT,
                upvotes INTEGER,
                downvotes INTEGER,
                comment_count INTEGER,
                is_pinned BOOLEAN,
                created_at TEXT
            );

            CREATE TABLE IF NOT EXISTS comment_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                comment_id TEXT,
                scraped_at TEXT DEFAULT CURRENT_TIMESTAMP,
                scrape_run_id INTEGER,
                post_id TEXT,
                parent_id TEXT,
                content TEXT,
                author_name TEXT,
                upvotes INTEGER,
                downvotes INTEGER,
                created_at TEXT
            );

            CREATE TABLE IF NOT EXISTS submolt_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                submolt_name TEXT,
                scraped_at TEXT DEFAULT CURRENT_TIMESTAMP,
                scrape_run_id INTEGER,
                submolt_id TEXT,
                display_name TEXT,
                description TEXT,
                subscriber_count INTEGER,
                avatar_url TEXT,
                banner_url TEXT,
                created_by_name TEXT,
                created_at TEXT,
                last_activity_at TEXT
            );

            CREATE TABLE IF NOT EXISTS moderators (
                submolt_name TEXT,
                agent_name TEXT,
                role TEXT,
                first_seen_at TEXT DEFAULT CURRENT_TIMESTAMP,
                last_updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (submolt_name, agent_name)
            );

            CREATE TABLE IF NOT EXISTS moderator_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                submolt_name TEXT,
                agent_name TEXT,
                role TEXT,
                scraped_at TEXT DEFAULT CURRENT_TIMESTAMP,
                scrape_run_id INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_comments_post_id ON comments(post_id);
            CREATE INDEX IF NOT EXISTS idx_comments_parent_id ON comments(parent_id);
            CREATE INDEX IF NOT EXISTS idx_posts_submolt_name ON posts(submolt_name);
            CREATE INDEX IF NOT EXISTS idx_posts_author_name ON posts(author_name);
            CREATE INDEX IF NOT EXISTS idx_post_snapshots_post_id ON post_snapshots(post_id);
            CREATE INDEX IF NOT EXISTS idx_agent_snapshots_agent_name ON agent_snapshots(agent_name);
            CREATE INDEX IF NOT EXISTS idx_comment_snapshots_comment_id ON comment_snapshots(comment_id);
            CREATE INDEX IF NOT EXISTS idx_submolt_snapshots_submolt_name ON submolt_snapshots(submolt_name);
            CREATE INDEX IF NOT EXISTS idx_moderator_snapshots_submolt ON moderator_snapshots(submolt_name);
            CREATE INDEX IF NOT EXISTS idx_moderator_snapshots_agent ON moderator_snapshots(agent_name);
            CREATE INDEX IF NOT EXISTS idx_moderator_snapshots_run_id ON moderator_snapshots(scrape_run_id);

            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY,
                applied_at TEXT DEFAULT CURRENT_TIMESTAMP,
                description TEXT
            );
        """)
        self.conn.commit()

    def upsert_agent(self, agent: dict):
        """Insert or update an agent.

        Uses COALESCE for enrichment-only fields (karma, is_claimed, follower_count,
        following_count, owner_json, metadata_json) to avoid overwriting with NULL
        when partial updates come from posts/comments (which only have id and name).
        """
        now = datetime.utcnow().isoformat()
        owner_json = json.dumps(agent.get("owner")) if agent.get("owner") else None
        metadata_json = json.dumps(agent.get("metadata")) if agent.get("metadata") else None

        self.conn.execute("""
            INSERT INTO agents (name, id, description, karma, is_claimed,
                              follower_count, following_count, avatar_url,
                              owner_json, metadata_json, created_at, last_updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                id = COALESCE(excluded.id, agents.id),
                description = COALESCE(excluded.description, agents.description),
                karma = COALESCE(excluded.karma, agents.karma),
                is_claimed = COALESCE(excluded.is_claimed, agents.is_claimed),
                follower_count = COALESCE(excluded.follower_count, agents.follower_count),
                following_count = COALESCE(excluded.following_count, agents.following_count),
                avatar_url = COALESCE(excluded.avatar_url, agents.avatar_url),
                owner_json = COALESCE(excluded.owner_json, agents.owner_json),
                metadata_json = COALESCE(excluded.metadata_json, agents.metadata_json),
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
            metadata_json,
            agent.get("created_at"),
            now,
        ))
        # Don't commit here - let caller batch commits

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
        # Don't commit here - let caller batch commits

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
        metadata_json = json.dumps(submolt.get("metadata")) if submolt.get("metadata") else None

        self.conn.execute("""
            INSERT INTO submolts (name, id, display_name, description,
                                subscriber_count, avatar_url, banner_url,
                                created_by_name, metadata_json, created_at, last_activity_at,
                                last_updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                id = excluded.id,
                display_name = excluded.display_name,
                description = excluded.description,
                subscriber_count = excluded.subscriber_count,
                avatar_url = excluded.avatar_url,
                banner_url = excluded.banner_url,
                metadata_json = COALESCE(excluded.metadata_json, submolts.metadata_json),
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
            metadata_json,
            submolt.get("created_at"),
            submolt.get("last_activity_at"),
            now,
        ))
        # Don't commit here - let caller batch commits

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

    def get_all_post_ids(self) -> list[str]:
        """Get all post IDs in the database."""
        cursor = self.conn.execute("SELECT id FROM posts")
        return [row[0] for row in cursor.fetchall()]

    def get_comment_count(self) -> int:
        """Get total number of comments in the database."""
        return self.conn.execute("SELECT COUNT(*) FROM comments").fetchone()[0]

    def get_post_ids_without_comments(self) -> list[str]:
        """Get post IDs that don't have any comments fetched yet.

        Returns posts sorted by comment_count DESC to prioritize posts with comments.
        """
        cursor = self.conn.execute("""
            SELECT id FROM posts
            WHERE id NOT IN (SELECT DISTINCT post_id FROM comments)
            ORDER BY comment_count DESC
        """)
        return [row[0] for row in cursor.fetchall()]

    def upsert_comment(self, comment: dict, post_id: str):
        """Insert or update a comment."""
        now = datetime.utcnow().isoformat()
        author_name = comment.get("author", {}).get("name") if comment.get("author") else None

        self.conn.execute("""
            INSERT INTO comments (id, post_id, parent_id, content, author_name,
                                upvotes, downvotes, created_at, last_updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                content = excluded.content,
                upvotes = excluded.upvotes,
                downvotes = excluded.downvotes,
                last_updated_at = excluded.last_updated_at
        """, (
            comment["id"],
            post_id,
            comment.get("parent_id"),
            comment.get("content"),
            author_name,
            comment.get("upvotes"),
            comment.get("downvotes"),
            comment.get("created_at"),
            now,
        ))
        # Don't commit here - let caller batch commits

    def save_post_snapshot(self, post: dict, scrape_run_id: int = None):
        """Save a snapshot of all post data."""
        self.conn.execute("""
            INSERT INTO post_snapshots (
                post_id, scrape_run_id, title, content, url, author_name, submolt_name,
                upvotes, downvotes, comment_count, is_pinned, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            post["id"],
            scrape_run_id,
            post.get("title"),
            post.get("content"),
            post.get("url"),
            post.get("author_name"),
            post.get("submolt_name"),
            post.get("upvotes"),
            post.get("downvotes"),
            post.get("comment_count"),
            post.get("is_pinned"),
            post.get("created_at"),
        ))

    def save_agent_snapshot(self, agent: dict, scrape_run_id: int = None):
        """Save a snapshot of all agent data."""
        self.conn.execute("""
            INSERT INTO agent_snapshots (
                agent_name, scrape_run_id, agent_id, description, karma, is_claimed,
                follower_count, following_count, avatar_url,
                owner_json, metadata_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            agent["name"],
            scrape_run_id,
            agent.get("id"),
            agent.get("description"),
            agent.get("karma"),
            agent.get("is_claimed"),
            agent.get("follower_count"),
            agent.get("following_count"),
            agent.get("avatar_url"),
            agent.get("owner_json"),
            agent.get("metadata_json"),
            agent.get("created_at"),
        ))

    def save_comment_snapshot(self, comment: dict, scrape_run_id: int = None):
        """Save a snapshot of all comment data."""
        self.conn.execute("""
            INSERT INTO comment_snapshots (
                comment_id, scrape_run_id, post_id, parent_id, content,
                author_name, upvotes, downvotes, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            comment["id"],
            scrape_run_id,
            comment.get("post_id"),
            comment.get("parent_id"),
            comment.get("content"),
            comment.get("author_name"),
            comment.get("upvotes"),
            comment.get("downvotes"),
            comment.get("created_at"),
        ))

    def save_submolt_snapshot(self, submolt: dict, scrape_run_id: int = None):
        """Save a snapshot of all submolt data."""
        self.conn.execute("""
            INSERT INTO submolt_snapshots (
                submolt_name, scrape_run_id, submolt_id, display_name, description,
                subscriber_count, avatar_url, banner_url,
                created_by_name, created_at, last_activity_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            submolt["name"],
            scrape_run_id,
            submolt.get("id"),
            submolt.get("display_name"),
            submolt.get("description"),
            submolt.get("subscriber_count"),
            submolt.get("avatar_url"),
            submolt.get("banner_url"),
            submolt.get("created_by_name"),
            submolt.get("created_at"),
            submolt.get("last_activity_at"),
        ))

    def upsert_moderator(self, submolt_name: str, agent_name: str, role: str = None):
        """Insert or update a moderator relationship."""
        now = datetime.utcnow().isoformat()
        self.conn.execute("""
            INSERT INTO moderators (submolt_name, agent_name, role, last_updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(submolt_name, agent_name) DO UPDATE SET
                role = COALESCE(excluded.role, moderators.role),
                last_updated_at = excluded.last_updated_at
        """, (submolt_name, agent_name, role, now))

    def save_moderator_snapshot(self, submolt_name: str, agent_name: str, role: str, scrape_run_id: int = None):
        """Save a snapshot of a moderator relationship."""
        self.conn.execute("""
            INSERT INTO moderator_snapshots (submolt_name, agent_name, role, scrape_run_id)
            VALUES (?, ?, ?, ?)
        """, (submolt_name, agent_name, role, scrape_run_id))

    def get_all_submolt_names(self) -> list[str]:
        """Get all submolt names in the database."""
        cursor = self.conn.execute("SELECT name FROM submolts")
        return [row[0] for row in cursor.fetchall()]

    def start_scrape_run(self) -> int:
        """Record the start of a scrape run. Returns the run ID."""
        now = datetime.utcnow().isoformat()
        cursor = self.conn.execute("""
            INSERT INTO scrape_runs (started_at, status)
            VALUES (?, 'running')
        """, (now,))
        self.conn.commit()
        return cursor.lastrowid

    def complete_scrape_run(self, run_id: int, posts: int, agents: int, comments: int, submolts: int, status: str = 'completed'):
        """Record the completion of a scrape run."""
        now = datetime.utcnow().isoformat()
        self.conn.execute("""
            UPDATE scrape_runs
            SET completed_at = ?, posts_scraped = ?, agents_scraped = ?, comments_scraped = ?, submolts_scraped = ?, status = ?
            WHERE id = ?
        """, (now, posts, agents, comments, submolts, status, run_id))
        self.conn.commit()

    def get_latest_snapshot_counts(self) -> dict:
        """Get entity counts from the most recent completed scrape for validation.

        Returns:
            Dict with keys: submolts, posts, comments, agents
            Each value is the count of entities in the latest completed scrape run.
        """
        counts = {"submolts": 0, "posts": 0, "comments": 0, "agents": 0}

        # Get the most recent completed scrape run
        result = self.conn.execute("""
            SELECT id FROM scrape_runs
            WHERE status = 'completed'
            ORDER BY completed_at DESC
            LIMIT 1
        """).fetchone()

        if not result:
            return counts

        run_id = result[0]

        # Count entities from that specific run
        result = self.conn.execute(
            "SELECT COUNT(*) FROM submolt_snapshots WHERE scrape_run_id = ?", (run_id,)
        ).fetchone()
        counts["submolts"] = result[0] if result else 0

        result = self.conn.execute(
            "SELECT COUNT(*) FROM post_snapshots WHERE scrape_run_id = ?", (run_id,)
        ).fetchone()
        counts["posts"] = result[0] if result else 0

        result = self.conn.execute(
            "SELECT COUNT(*) FROM comment_snapshots WHERE scrape_run_id = ?", (run_id,)
        ).fetchone()
        counts["comments"] = result[0] if result else 0

        result = self.conn.execute(
            "SELECT COUNT(*) FROM agent_snapshots WHERE scrape_run_id = ?", (run_id,)
        ).fetchone()
        counts["agents"] = result[0] if result else 0

        return counts

    def commit(self):
        """Commit pending changes."""
        self.conn.commit()

    def close(self):
        """Close the database connection."""
        self.conn.close()
