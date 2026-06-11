"""Tests for library/config.py — Pydantic BaseSettings."""

from __future__ import annotations

import pytest

from library.config import (
    ElasticsearchSettings,
    KafkaSettings,
    Settings,
    TikTokCrawlerSettings,
)


class TestKafkaSettings:
    def test_defaults(self) -> None:
        ks = KafkaSettings()
        assert ks.bootstrap_servers == "localhost:9092"
        assert ks.topic == "tiktok.posts.raw"

    def test_override_via_init(self) -> None:
        ks = KafkaSettings(bootstrap_servers="kafka:29092", topic="custom.topic")
        assert ks.bootstrap_servers == "kafka:29092"
        assert ks.topic == "custom.topic"


class TestElasticsearchSettings:
    def test_defaults(self) -> None:
        es = ElasticsearchSettings()
        assert es.hosts == ["http://localhost:9200"]
        assert es.index_name == "tiktok_posts"
        assert es.chunk_size == 500


class TestTikTokCrawlerSettings:
    def test_defaults(self) -> None:
        cs = TikTokCrawlerSettings()
        assert cs.base_url == "https://www.tikwm.com"
        assert cs.rate_limit_rps == 5.0

    def test_count_bounds(self) -> None:
        with pytest.raises(Exception):
            TikTokCrawlerSettings(default_count=0)
        with pytest.raises(Exception):
            TikTokCrawlerSettings(default_count=100)


class TestRootSettings:
    def test_nested_settings_created(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("library.config.Path.exists", lambda _self: False)
        settings = Settings()
        assert isinstance(settings.kafka, KafkaSettings)
        assert isinstance(settings.elasticsearch, ElasticsearchSettings)
        assert isinstance(settings.crawler, TikTokCrawlerSettings)
