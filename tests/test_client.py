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
        # Empty page signals end of pagination
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/submolts",
            json={"success": True, "submolts": []},
            status=200,
        )

        client = MoltbookClient(api_key="test_key")
        submolts = client.fetch_submolts()

        assert len(submolts) == 1
        assert submolts[0]["name"] == "general"
        assert submolts[0]["subscriber_count"] == 100

    @responses.activate
    def test_fetch_submolts_paginates_by_page(self):
        """Client should paginate submolts using page param until empty."""
        # Page 1
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/submolts",
            json={
                "success": True,
                "submolts": [{"name": f"sub{i}", "id": str(i)} for i in range(20)],
            },
            status=200,
        )
        # Page 2
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/submolts",
            json={
                "success": True,
                "submolts": [{"name": "sub20", "id": "20"}],
            },
            status=200,
        )
        # Page 3 - empty (signals end)
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/submolts",
            json={
                "success": True,
                "submolts": [],
            },
            status=200,
        )

        client = MoltbookClient(api_key="test_key")
        submolts = client.fetch_submolts()

        assert len(submolts) == 21
        # Verify page params were used
        assert "page=1" in responses.calls[0].request.url
        assert "page=2" in responses.calls[1].request.url
        assert "page=3" in responses.calls[2].request.url


class TestMoltbookClientFetchPosts:
    """Tests for fetching posts."""

    @responses.activate
    def test_fetch_posts_returns_dict_with_pagination(self):
        """Client should return dict with posts, has_more, next_cursor."""
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
                "has_more": True,
                "next_cursor": "abc123cursor",
            },
            status=200,
        )

        client = MoltbookClient(api_key="test_key")
        result = client.fetch_posts()

        assert isinstance(result, dict)
        assert len(result["posts"]) == 1
        assert result["posts"][0]["title"] == "Test post"
        assert result["has_more"] is True
        assert result["next_cursor"] == "abc123cursor"

    @responses.activate
    def test_fetch_all_posts_follows_cursor(self):
        """Client should follow cursor until has_more is false."""
        # First page - has more
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/posts",
            json={
                "success": True,
                "posts": [
                    {"id": "post1", "title": "First"},
                    {"id": "post2", "title": "Second"},
                ],
                "has_more": True,
                "next_cursor": "cursor_page2",
            },
            status=200,
        )
        # Second page - no more
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/posts",
            json={
                "success": True,
                "posts": [{"id": "post3", "title": "Third"}],
                "has_more": False,
                "next_cursor": None,
            },
            status=200,
        )

        client = MoltbookClient(api_key="test_key")
        posts = client.fetch_all_posts(limit=2)

        assert len(posts) == 3
        assert posts[0]["title"] == "First"
        assert posts[2]["title"] == "Third"
        # Verify cursor was passed in second request
        assert "cursor=cursor_page2" in responses.calls[1].request.url


class TestMoltbookClientFetchSubmoltDetail:
    """Tests for fetching submolt details."""

    @responses.activate
    def test_fetch_submolt_detail_returns_submolt(self):
        """Client should return full submolt details."""
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/submolts/general",
            json={
                "success": True,
                "submolt": {
                    "id": "sub123",
                    "name": "general",
                    "display_name": "General",
                    "description": "The town square",
                    "subscriber_count": 100,
                    "post_count": 5000,
                    "is_nsfw": False,
                    "is_private": False,
                    "creator_id": "creator1",
                    "created_by": {
                        "id": "creator1",
                        "name": "testbot",
                    },
                    "created_at": "2026-01-27T18:01:09.076Z",
                },
            },
            status=200,
        )

        client = MoltbookClient(api_key="test_key")
        submolt = client.fetch_submolt_detail("general")

        assert submolt is not None
        assert submolt["name"] == "general"
        assert submolt["post_count"] == 5000
        assert submolt["created_by"]["name"] == "testbot"

    @responses.activate
    def test_fetch_submolt_detail_returns_none_for_404(self):
        """Client should return None when submolt not found."""
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/submolts/nonexistent",
            json={"error": "Not found"},
            status=404,
        )

        client = MoltbookClient(api_key="test_key")
        submolt = client.fetch_submolt_detail("nonexistent")

        assert submolt is None


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


class TestMoltbookClientFetchPostWithComments:
    """Tests for fetching a single post with comments."""

    @responses.activate
    def test_fetch_post_with_comments_uses_separate_endpoint(self):
        """Client should fetch comments from /posts/{id}/comments."""
        # Mock post endpoint
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/posts/post123",
            json={
                "success": True,
                "post": {"id": "post123", "title": "Test"},
            },
            status=200,
        )
        # Mock comments endpoint
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/posts/post123/comments",
            json={
                "success": True,
                "comments": [
                    {"id": "c1", "content": "Hello", "author": {"name": "bot1"}},
                    {"id": "c2", "content": "World", "author": {"name": "bot2"}},
                ],
            },
            status=200,
        )

        client = MoltbookClient(api_key="test_key")
        result = client.fetch_post_with_comments("post123")

        assert result is not None
        assert result["post"]["id"] == "post123"
        assert len(result["comments"]) == 2

    @responses.activate
    def test_fetch_post_with_comments_returns_none_for_404(self):
        """Client should return None when post not found."""
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/posts/missing",
            json={"error": "Not found"},
            status=404,
        )

        client = MoltbookClient(api_key="test_key")
        result = client.fetch_post_with_comments("missing")

        assert result is None

    @responses.activate
    def test_fetch_post_with_comments_returns_empty_on_comments_error(self):
        """Client should return post with empty comments if comments endpoint fails."""
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/posts/post123",
            json={
                "success": True,
                "post": {"id": "post123", "title": "Test"},
            },
            status=200,
        )
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/posts/post123/comments",
            json={"error": "Server error"},
            status=500,
        )

        client = MoltbookClient(api_key="test_key")
        result = client.fetch_post_with_comments("post123")

        assert result is not None
        assert result["post"]["id"] == "post123"
        assert result["comments"] == []


class TestMoltbookClientFetchPlatformStats:
    """Tests for fetching platform statistics."""

    @responses.activate
    def test_fetch_platform_stats_parses_new_key_format(self):
        """Client should parse totalAgents/totalPosts/etc key format."""
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/stats",
            json={
                "totalAgents": 2844466,
                "totalPosts": 1560225,
                "totalComments": 12347655,
                "totalSubmolts": 18320,
            },
            status=200,
        )

        client = MoltbookClient(api_key="test_key")
        stats = client.fetch_platform_stats()

        assert stats["agents"] == 2844466
        assert stats["posts"] == 1560225
        assert stats["comments"] == 12347655
        assert stats["submolts"] == 18320


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
        # Empty page signals end of pagination
        responses.add(
            responses.GET,
            "https://www.moltbook.com/api/v1/submolts",
            json={"success": True, "submolts": []},
            status=200,
        )

        client = MoltbookClient(api_key="test_key")
        submolts = client.fetch_submolts()

        assert len(submolts) == 1
        assert len(responses.calls) == 3  # 429 + success + empty terminal page

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
