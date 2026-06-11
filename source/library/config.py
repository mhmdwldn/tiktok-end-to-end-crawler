"""
Configuration module for the TikTok scraper pipeline.

All settings are loaded via Pydantic BaseSettings, supporting:
  - Environment variables (prefixed with TIKTOK_)
  - YAML configuration file
  - Direct initialisation overrides

Zero hardcoded values — every tunable is defined here.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic_settings.sources import YamlConfigSettingsSource


class KafkaSettings(BaseSettings):
    """Apache Kafka connection and producer configuration."""

    bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Comma-separated list of Kafka broker addresses",
    )
    topic: str = Field(
        default="tiktok.posts.raw",
        description="Default Kafka topic for scraped TikTok posts",
    )
    client_id: str = Field(
        default="tiktok-crawler",
        description="Kafka client identifier",
    )
    acks: str = Field(
        default="all",
        description="Producer acknowledgment level: 0, 1, or 'all'",
    )
    compression_type: Optional[str] = Field(
        default="gzip",
        description="Compression codec: gzip, snappy, lz4, zstd, or None",
    )
    max_request_size: int = Field(
        default=1_048_576,
        description="Maximum request size in bytes (default 1 MB)",
    )
    linger_ms: int = Field(
        default=10,
        description="Artificial delay in ms to batch outgoing messages",
    )
    request_timeout_ms: int = Field(
        default=30_000,
        description="Kafka producer request timeout in ms",
    )


class ElasticsearchSettings(BaseSettings):
    """Elasticsearch connection and indexing configuration."""

    hosts: list[str] = Field(
        default=["http://localhost:9200"],
        description="List of Elasticsearch node URLs",
    )
    index_name: str = Field(
        default="tiktok_posts",
        description="Target Elasticsearch index",
    )
    api_key: Optional[str] = Field(
        default=None,
        description="Optional API key for Elasticsearch authentication",
    )
    username: Optional[str] = Field(
        default=None,
        description="Basic-auth username",
    )
    password: Optional[str] = Field(
        default=None,
        description="Basic-auth password",
    )
    request_timeout: int = Field(
        default=30,
        description="ES client request timeout in seconds",
    )
    max_retries: int = Field(
        default=3,
        description="Number of retries on transient failures",
    )
    chunk_size: int = Field(
        default=500,
        description="Number of documents per bulk indexing request",
    )


class CrawlerSettings(BaseSettings):
    """Generic crawler HTTP configuration."""

    request_timeout: float = Field(
        default=30.0,
        description="HTTP request timeout in seconds",
    )
    max_retries: int = Field(
        default=3,
        description="Maximum retry attempts on transient HTTP errors",
    )
    retry_backoff: float = Field(
        default=2.0,
        description="Exponential backoff multiplier for retries",
    )
    user_agent: str = Field(
        default=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/149.0.0.0 Safari/537.36"
        ),
        description="Default User-Agent header for HTTP requests",
    )
    rate_limit_rps: float = Field(
        default=5.0,
        description="Maximum requests per second (per crawler instance)",
    )
    proxy_url: Optional[str] = Field(
        default=None,
        description="Optional HTTP/SOCKS proxy URL",
    )


class TikTokCrawlerSettings(CrawlerSettings):
    """TikTok-specific crawler configuration."""

    base_url: str = Field(
        default="https://www.tikwm.com",
        description="Base URL for the TikTok API proxy",
    )
    search_endpoint: str = Field(
        default="/api/feed/search",
        description="Search feed endpoint path",
    )
    user_posts_endpoint: str = Field(
        default="/api/user/posts",
        description="User-posts endpoint — fetch posts by unique_id",
    )
    user_story_endpoint: str = Field(
        default="/api/user/story",
        description="User-story endpoint — fetch stories by unique_id",
    )
    cookies: str = Field(
        default="",
        description="Optional cookie string for authenticated endpoints "
                    "(e.g. cf_clearance=...; current_language=en)",
    )
    default_count: int = Field(
        default=12,
        ge=1,
        le=50,
        description="Default number of results per request",
    )


class Settings(BaseSettings):
    """Root settings aggregating all sub-configurations."""

    model_config = SettingsConfigDict(
        env_prefix="TIKTOK_",
        env_nested_delimiter="__",
        yaml_file="../config.yaml",
        yaml_config_section="tiktok_crawler",
        case_sensitive=False,
    )

    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    elasticsearch: ElasticsearchSettings = Field(default_factory=ElasticsearchSettings)
    crawler: TikTokCrawlerSettings = Field(default_factory=TikTokCrawlerSettings)

    environment: str = Field(default="development")
    log_level: str = Field(default="INFO")

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings,
        env_settings,
        dotenv_settings,
        file_secret_settings,
    ):
        """Customise the settings source priority.

        Priority (highest first):
          1. Constructor / init kwargs
          2. Environment variables
          3. YAML config file (if present)
          4. .env / dotenv files
          5. File secrets
        """
        yaml_path = cls._resolve_yaml_path(settings_cls)
        sources = [
            init_settings,
            env_settings,
        ]
        if yaml_path and yaml_path.exists():
            section = settings_cls.model_config.get("yaml_config_section")
            sources.append(
                YamlConfigSettingsSource(
                    settings_cls,
                    yaml_file=str(yaml_path),
                    yaml_config_section=section,
                )
            )
        sources.extend([dotenv_settings, file_secret_settings])
        return tuple(sources)

    @staticmethod
    def _resolve_yaml_path(settings_cls: type[BaseSettings]) -> Path | None:
        """Resolve the YAML config path — checks multiple locations."""
        yaml_file = settings_cls.model_config.get("yaml_file", "../config.yaml")
        candidates = [
            Path(yaml_file),                          # relative to CWD
            Path(__file__).resolve().parent.parent.parent / "config.yaml",  # project root
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return candidates[0]  # return primary path even if missing (will be skipped)


# Singleton settings instance — import this throughout the application.
settings = Settings()
