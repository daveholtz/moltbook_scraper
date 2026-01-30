"""Tests for MoltbookClient API wrapper."""

import time
import responses
import pytest
from src.client import MoltbookClient


class TestMoltbookClientFetchSubmolts:
    """Tests for fetching submolts."""

    @responses.activate
    def test_fetch_submolts_returns_list_of_submolts(self):
        """Client should return list of submolt dicts from API."""
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/submolts",
            json={
                "success": True,
                "submolts": [
                    {
                        "id": "abc123",
                        "name": "general",
                        "display_name": "General",
                        "description": "The town square",
                        "subscriber_count": 100,
                        "created_at": "2026-01-27T18:01:09.076047+00:00",
                    }
                ],
                "count": 1,
            },
            status=200,
        )

        client = MoltbookClient(api_key="test_key")
        submolts = client.fetch_submolts()

        assert len(submolts) == 1
        assert submolts[0]["name"] == "general"
        assert submolts[0]["subscriber_count"] == 100


class TestMoltbookClientFetchPosts:
    """Tests for fetching posts."""

    @responses.activate
    def test_fetch_posts_returns_list_of_posts(self):
        """Client should return posts from the API."""
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/posts",
            json={
                "success": True,
                "posts": [
                    {
                        "id": "post123",
                        "title": "Test post",
                        "content": "Hello world",
                        "upvotes": 10,
                        "downvotes": 1,
                        "comment_count": 5,
                        "created_at": "2026-01-30T05:39:05.821605+00:00",
                        "author": {"id": "author1", "name": "testbot"},
                        "submolt": {"id": "sub1", "name": "general"},
                    }
                ],
                "count": 1,
                "has_more": False,
                "next_offset": 25,
            },
            status=200,
        )

        client = MoltbookClient(api_key="test_key")
        posts = client.fetch_posts()

        assert len(posts) == 1
        assert posts[0]["title"] == "Test post"
        assert posts[0]["author"]["name"] == "testbot"

    @responses.activate
    def test_fetch_all_posts_paginates_until_no_more(self):
        """Client should paginate through all posts when has_more is True."""
        # First page
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/posts",
            json={
                "success": True,
                "posts": [{"id": "post1", "title": "First"}],
                "has_more": True,
                "next_offset": 25,
            },
            status=200,
        )
        # Second page
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/posts",
            json={
                "success": True,
                "posts": [{"id": "post2", "title": "Second"}],
                "has_more": False,
                "next_offset": 50,
            },
            status=200,
        )

        client = MoltbookClient(api_key="test_key")
        posts = client.fetch_all_posts()

        assert len(posts) == 2
        assert posts[0]["title"] == "First"
        assert posts[1]["title"] == "Second"


class TestMoltbookClientFetchAgentProfile:
    """Tests for fetching agent profiles."""

    @responses.activate
    def test_fetch_agent_profile_returns_agent_with_owner(self):
        """Client should return agent profile including owner Twitter info."""
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/agents/profile",
            json={
                "success": True,
                "agent": {
                    "id": "agent123",
                    "name": "Clawd",
                    "description": "Personal assistant",
                    "karma": 42,
                    "is_claimed": True,
                    "follower_count": 10,
                    "following_count": 5,
                    "owner": {
                        "x_handle": "testuser",
                        "x_name": "Test User",
                    },
                },
                "recentPosts": [],
            },
            status=200,
        )

        client = MoltbookClient(api_key="test_key")
        agent = client.fetch_agent_profile("Clawd")

        assert agent["name"] == "Clawd"
        assert agent["karma"] == 42
        assert agent["owner"]["x_handle"] == "testuser"

    @responses.activate
    def test_fetch_agent_profile_returns_none_when_not_found(self):
        """Client should return None when agent not found."""
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/agents/profile",
            json={
                "success": False,
                "error": "Bot not found",
            },
            status=200,
        )

        client = MoltbookClient(api_key="test_key")
        agent = client.fetch_agent_profile("nonexistent")

        assert agent is None


class TestMoltbookClientRateLimiting:
    """Tests for rate limiting behavior."""

    @responses.activate
    def test_retries_on_429_with_backoff(self):
        """Client should retry with exponential backoff on 429."""
        # First request returns 429
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/submolts",
            json={"error": "Rate limited"},
            status=429,
        )
        # Second request succeeds
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/submolts",
            json={"success": True, "submolts": [{"name": "test"}]},
            status=200,
        )

        client = MoltbookClient(api_key="test_key")
        submolts = client.fetch_submolts()

        assert len(submolts) == 1
        assert len(responses.calls) == 2  # Two requests made

    @responses.activate
    def test_gives_up_after_max_retries(self):
        """Client should give up after max retries on persistent 429."""
        # All requests return 429
        for _ in range(5):
            responses.add(
                responses.GET,
                "https://www.moltbook.com/api/v1/submolts",
                json={"error": "Rate limited"},
                status=429,
            )

        client = MoltbookClient(api_key="test_key", max_retries=3)

        with pytest.raises(Exception) as exc_info:
            client.fetch_submolts()

        assert "429" in str(exc_info.value) or "rate" in str(exc_info.value).lower()

    def test_tracks_request_count(self):
        """Client should track number of requests made."""
        client = MoltbookClient(api_key="test_key")
        assert client.request_count == 0
