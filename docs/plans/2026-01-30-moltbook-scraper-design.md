# Moltbook Scraper Design

**Date:** 2026-01-30
**Status:** Approved

## Overview

Build a scraper to archive moltbook.com - a social network for AI agents ("the front page of the agent internet"). The goal is research/archival: capture complete platform state daily and track evolution over time.

## Platform Stats (as of design date)

- 37,017 AI agents
- 3,139 submolts (communities)
- 6,038 posts
- 58,560 comments

## Technical Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Language | Python | Familiar, good for data work |
| Storage | SQLite | Portable, easy analysis with pandas |
| Auth | API with registered agent | Clean JSON vs messy HTML scraping |
| Rate limit | 100 req/min | ~65 min for full scrape, acceptable |

## Data Model

### Core Entities

- **Agents** - name, description, karma, avatar, metadata
- **Posts** - id, title, content/url, author, submolt, votes, timestamps
- **Comments** - id, content, author, post_id, parent_id (threading), votes
- **Submolts** - name, display_name, description, avatar, banner, settings

### Relationships

- **Moderators** - agent → submolt (with role)

*Note: The API does not expose follows or subscription relationships - only counts. See Known Limitations.*

### Historical Tracking

- **Agent Snapshots** - daily karma, follower count, post count per agent
- **Scrape Runs** - metadata about each scrape execution

## Storage Schema

```sql
-- Core entities
agents (
    name TEXT PRIMARY KEY,
    description TEXT,
    avatar_url TEXT,
    karma INTEGER,
    metadata JSON,
    first_seen_at TIMESTAMP,
    last_updated_at TIMESTAMP
)

posts (
    id TEXT PRIMARY KEY,
    title TEXT,
    content TEXT,
    url TEXT,
    author_name TEXT REFERENCES agents,
    submolt_name TEXT REFERENCES submolts,
    upvotes INTEGER,
    downvotes INTEGER,
    is_pinned BOOLEAN,
    created_at TIMESTAMP,
    first_seen_at TIMESTAMP,
    last_updated_at TIMESTAMP
)

comments (
    id TEXT PRIMARY KEY,
    content TEXT,
    author_name TEXT REFERENCES agents,
    post_id TEXT REFERENCES posts,
    parent_id TEXT,
    upvotes INTEGER,
    downvotes INTEGER,
    created_at TIMESTAMP,
    first_seen_at TIMESTAMP,
    last_updated_at TIMESTAMP
)

submolts (
    name TEXT PRIMARY KEY,
    display_name TEXT,
    description TEXT,
    avatar_url TEXT,
    banner_url TEXT,
    metadata JSON,
    created_at TIMESTAMP,
    first_seen_at TIMESTAMP,
    last_updated_at TIMESTAMP
)

-- Relationships (API only provides moderators; follows/subscriptions are counts only)
moderators (
    agent_name TEXT REFERENCES agents,
    submolt_name TEXT REFERENCES submolts,
    role TEXT,
    first_seen_at TIMESTAMP,
    PRIMARY KEY (agent_name, submolt_name)
)

-- Historical tracking
agent_snapshots (
    agent_name TEXT REFERENCES agents,
    scraped_at TIMESTAMP,
    karma INTEGER,
    follower_count INTEGER,
    following_count INTEGER,
    -- post_count not available from API; can be derived from posts table if needed
    PRIMARY KEY (agent_name, scraped_at)
)

scrape_runs (
    id INTEGER PRIMARY KEY,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    agents_scraped INTEGER,
    posts_scraped INTEGER,
    comments_scraped INTEGER,
    status TEXT
)
```

## Scraping Strategy

### Initial Full Scrape

1. Fetch all submolts → store
2. Fetch all agents → store + create initial snapshots
3. Fetch all posts (sorted by new) → store
4. For each post, fetch comments → store
5. For each submolt, fetch moderators → store relationships

### Daily Incremental Scrape

1. Fetch posts sorted by "new" until we hit already-seen posts
2. Fetch comments for new posts + refresh recent posts (last 7 days)
3. Refresh agent profiles that posted/commented recently
4. Check for new submolts
5. Weekly: full refresh of all agent snapshots

### Error Handling

- Rate limit (429): exponential backoff
- Transient failures (5xx, network): retry with backoff
- Permanent failures (404, 403): log and skip
- Track all scrape runs in database

## Code Structure

```
moltbook_scraper/
├── src/
│   ├── __init__.py
│   ├── client.py          # API wrapper with rate limiting
│   ├── database.py        # SQLite operations
│   ├── models.py          # Dataclasses
│   ├── scraper.py         # Orchestration
│   └── cli.py             # Command-line interface
├── tests/
├── moltbook.db            # SQLite database (gitignored)
├── .env                   # API key (gitignored)
├── requirements.txt
├── CLAUDE.md
└── README.md
```

## API Endpoints Used

| Method | Endpoint | Purpose |
|--------|----------|---------|
| GET | `/submolts` | List all submolts |
| GET | `/submolts/{name}` | Submolt details |
| GET | `/submolts/{name}/moderators` | Moderator list |
| GET | `/agents/profile?name=X` | Agent profile |
| GET | `/posts` | List posts (paginated) |
| GET | `/posts/{id}` | Post details |
| GET | `/posts/{id}/comments` | Comments for post |
| GET | `/search` | Search functionality |

Base URL: `https://www.moltbook.com/api/v1`

## Ethical Constraints

**Observer only - no participation:**
- No posting, commenting, voting
- No following agents
- No creating submolts
- Scraper agent exists solely for data collection

## Known Limitations (Discovered During Implementation)

- **Follows/Subscriptions**: API only returns counts, not actual relationships
- **Comment Cap**: API returns max 1000 comments per post
- **Agent Discovery**: No `/agents` endpoint; agents discovered via posts/comments only
- **Post Count**: API does not return post count on agent profiles

## Security Considerations

- API key stored in `.env`, never committed
- API key has write permissions - protect it
- ToS reviewed: no explicit scraping restrictions
- Public data from AI agents (low privacy concern)

## Next Steps

1. Register scraper agent on Moltbook (requires Twitter verification)
2. Implement client.py with rate limiting
3. Implement database.py with schema
4. Implement scraper.py orchestration
5. Add CLI interface
6. Run initial full scrape
7. Set up daily cron job
