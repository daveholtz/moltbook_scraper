"""Command-line interface for Moltbook scraper."""

import argparse
import os
import sys
from datetime import datetime
from dotenv import load_dotenv

from src.client import MoltbookClient
from src.database import Database
from src.scraper import Scraper


def log(message: str):
    """Print a timestamped log message."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}")


def main():
    """Main entry point for the CLI."""
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Moltbook scraper - archive moltbook.com for research"
    )
    parser.add_argument(
        "command",
        choices=["full", "incremental", "submolts", "posts", "comments", "enrich", "snapshots", "status"],
        help="Scrape command to run",
    )
    parser.add_argument(
        "--db",
        default="moltbook.db",
        help="Path to SQLite database (default: moltbook.db)",
    )
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Suppress progress messages",
    )
    parser.add_argument(
        "--only-missing",
        action="store_true",
        help="For comments: only fetch for posts without comments yet",
    )
    args = parser.parse_args()

    api_key = os.getenv("MOLTBOOK_API_KEY")
    if not api_key and args.command != "status":
        print("Error: MOLTBOOK_API_KEY not set in environment or .env file")
        sys.exit(1)

    db = Database(args.db)

    if args.command == "status":
        # Show database stats (no API needed)
        conn = db.conn
        agents = conn.execute("SELECT COUNT(*) FROM agents").fetchone()[0]
        posts = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
        submolts = conn.execute("SELECT COUNT(*) FROM submolts").fetchone()[0]
        comments = conn.execute("SELECT COUNT(*) FROM comments").fetchone()[0]

        # Snapshot counts
        agent_snaps = conn.execute("SELECT COUNT(*) FROM agent_snapshots").fetchone()[0]
        post_snaps = conn.execute("SELECT COUNT(*) FROM post_snapshots").fetchone()[0]
        comment_snaps = conn.execute("SELECT COUNT(*) FROM comment_snapshots").fetchone()[0]

        # Get latest activity
        latest_post = conn.execute(
            "SELECT created_at FROM posts ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        latest_agent = conn.execute(
            "SELECT last_updated_at FROM agents ORDER BY last_updated_at DESC LIMIT 1"
        ).fetchone()

        print(f"Database: {args.db}")
        print(f"  Agents:   {agents:,}")
        print(f"  Posts:    {posts:,}")
        print(f"  Submolts: {submolts:,}")
        print(f"  Comments: {comments:,}")
        print(f"  Snapshots: {agent_snaps:,} agent, {post_snaps:,} post, {comment_snaps:,} comment")
        if latest_post:
            print(f"  Latest post: {latest_post[0]}")
        if latest_agent:
            print(f"  Latest agent update: {latest_agent[0]}")
        db.close()
        return

    # Create client and scraper with progress callback
    progress_fn = None if args.quiet else log
    client = MoltbookClient(api_key=api_key)
    scraper = Scraper(client, db, on_progress=progress_fn)

    try:
        if args.command == "full":
            log("Starting full scrape...")
            scraper.full_scrape()
            log("Full scrape complete.")

        elif args.command == "incremental":
            log("Starting incremental scrape...")
            new_posts = scraper.scrape_posts_incremental()
            log(f"Incremental scrape complete. Found {new_posts} new posts.")

        elif args.command == "submolts":
            log("Scraping submolts...")
            scraper.scrape_submolts()
            log("Submolts scrape complete.")

        elif args.command == "posts":
            log("Scraping all posts...")
            scraper.scrape_posts()
            log("Posts scrape complete.")

        elif args.command == "enrich":
            log("Enriching agent profiles...")
            scraper.enrich_agents()
            log("Agent enrichment complete.")

        elif args.command == "comments":
            log("Scraping comments...")
            scraper.scrape_comments(only_missing=args.only_missing)
            log("Comments scrape complete.")

        elif args.command == "snapshots":
            log("Creating snapshots...")
            scraper.create_snapshots()
            log("Snapshots complete.")

    except KeyboardInterrupt:
        log("Interrupted by user.")
        sys.exit(1)
    except Exception as e:
        log(f"Error: {e}")
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
