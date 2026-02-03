"""Command-line interface for Moltbook scraper."""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

from src.client import MoltbookClient
from src.database import Database
from src.scraper import Scraper


# Moltbook documentation URLs to snapshot
MOLTBOOK_DOCS = {
    "skill": "https://moltbook.com/skill.md",
    "heartbeat": "https://www.moltbook.com/heartbeat.md",
    "messaging": "https://www.moltbook.com/messaging.md",
}


def log(message: str):
    """Print a timestamped log message."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}")


def fetch_docs(output_dir: str = "snapshots/docs") -> dict:
    """Fetch Moltbook documentation files and save with timestamps.

    Returns dict of {name: filepath} for successfully saved files.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    saved_files = {}

    for name, url in MOLTBOOK_DOCS.items():
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            filename = f"{name}_{timestamp}.md"
            filepath = output_path / filename
            filepath.write_text(response.text)

            saved_files[name] = str(filepath)
            log(f"  Saved {name}.md ({len(response.text):,} bytes)")
        except requests.RequestException as e:
            log(f"  Failed to fetch {name}.md: {e}")

    return saved_files


def main():
    """Main entry point for the CLI."""
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Moltbook scraper - archive moltbook.com for research"
    )
    parser.add_argument(
        "command",
        choices=["full", "incremental", "submolts", "posts", "comments", "moderators", "enrich", "snapshots", "docs", "status"],
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
    if not api_key and args.command not in ("status", "docs"):
        print("Error: MOLTBOOK_API_KEY not set in environment or .env file")
        sys.exit(1)

    # Handle docs command (no API key needed)
    if args.command == "docs":
        log("Fetching Moltbook documentation snapshots...")
        saved = fetch_docs()
        log(f"Documentation snapshots complete. Saved {len(saved)} files.")
        return

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
    # Use 5 retries with 2s base delay to handle API instability
    progress_fn = None if args.quiet else log
    client = MoltbookClient(api_key=api_key, max_retries=5, base_delay=2.0)
    scraper = Scraper(client, db, on_progress=progress_fn)

    try:
        if args.command == "full":
            log("Starting full scrape...")
            scraper.full_scrape()
            log("Fetching documentation snapshots...")
            fetch_docs()
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

        elif args.command == "moderators":
            log("Scraping moderators...")
            scraper.scrape_moderators()
            log("Moderators scrape complete.")

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
