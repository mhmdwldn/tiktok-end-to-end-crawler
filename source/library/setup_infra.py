#!/usr/bin/env python3
"""
Infrastructure setup — create Kafka topics & Elasticsearch indices.

Usage:
    python library/setup_infra.py                  # create default topic + index
    python library/setup_infra.py --dry-run        # show what would be created
    python library/setup_infra.py --delete         # delete and recreate

Reads configuration from ``config.yaml`` (or env vars) via ``library.config.settings``.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

import requests
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError

from library.config import settings

logger = logging.getLogger("setup_infra")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)


# ============================================================================
# Kafka
# ============================================================================

def create_kafka_topic(
    topic: str | None = None,
    num_partitions: int = 3,
    replication_factor: int = 1,
    dry_run: bool = False,
    delete_first: bool = False,
) -> bool:
    """Create a Kafka topic.

    Args:
        topic: Topic name (defaults to ``settings.kafka.topic``).
        num_partitions: Number of partitions.
        replication_factor: Replication factor.
        dry_run: If True, only print what would be done.
        delete_first: If True, delete the topic before recreating.

    Returns:
        True on success.
    """
    topic = topic or settings.kafka.topic
    broker = settings.kafka.bootstrap_servers

    logger.info("Connecting to Kafka broker: %s", broker)

    try:
        admin = KafkaAdminClient(bootstrap_servers=broker, client_id="setup-infra")
    except KafkaError as e:
        logger.error("Failed to connect to Kafka: %s", e)
        return False

    # Check existing topics
    existing = admin.list_topics()
    logger.info("Existing topics: %s", existing)

    if topic in existing:
        if delete_first:
            logger.info("Deleting existing topic: %s", topic)
            if not dry_run:
                admin.delete_topics([topic])
                time.sleep(1)
        else:
            logger.info("Topic '%s' already exists — skipping", topic)
            admin.close()
            return True

    if dry_run:
        logger.info("[DRY-RUN] Would create topic: %s (partitions=%d, rf=%d)",
                     topic, num_partitions, replication_factor)
        admin.close()
        return True

    try:
        new_topic = NewTopic(
            name=topic,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )
        admin.create_topics([new_topic])
        logger.info("[OK] Kafka topic created: %s (partitions=%d)", topic, num_partitions)
    except TopicAlreadyExistsError:
        logger.info("Topic '%s' already exists", topic)
    except KafkaError as e:
        logger.error("Failed to create topic: %s", e)
        admin.close()
        return False

    admin.close()
    return True


# ============================================================================
# Elasticsearch
# ============================================================================

ES_INDEX_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
    },
    "mappings": {
        "properties": {
            "video_id": {"type": "keyword"},
            "title": {"type": "text", "analyzer": "standard"},
            "desc": {"type": "text"},
            "create_time": {"type": "date", "format": "epoch_second"},
            "author": {
                "properties": {
                    "author_id": {"type": "keyword"},
                    "unique_id": {"type": "keyword"},
                    "nickname": {"type": "text"},
                }
            },
            "music": {
                "properties": {
                    "music_id": {"type": "keyword"},
                    "title": {"type": "text"},
                    "duration": {"type": "integer"},
                }
            },
            "stats": {
                "properties": {
                    "digg_count": {"type": "long"},
                    "share_count": {"type": "long"},
                    "comment_count": {"type": "long"},
                    "play_count": {"type": "long"},
                }
            },
            "hashtags": {"type": "keyword"},
            "is_ad": {"type": "boolean"},
        }
    },
}


def _es_url(path: str = "") -> str:
    """Build a full Elasticsearch REST URL."""
    host = settings.elasticsearch.hosts[0].rstrip("/")
    return f"{host}/{path}"


def _es_request(method: str, path: str, json_body: dict | None = None, timeout: int = 10) -> requests.Response:
    """Make a request to Elasticsearch REST API."""
    url = _es_url(path)
    headers = {"Content-Type": "application/json"}
    return requests.request(method, url, json=json_body, headers=headers, timeout=timeout)


def create_elasticsearch_index(
    index: str | None = None,
    dry_run: bool = False,
    delete_first: bool = False,
) -> bool:
    """Create an Elasticsearch index with optimised mappings (REST API).

    Args:
        index: Index name (defaults to ``settings.elasticsearch.index_name``).
        dry_run: If True, only print what would be done.
        delete_first: If True, delete the index before recreating.

    Returns:
        True on success.
    """
    index = index or settings.elasticsearch.index_name

    logger.info("Connecting to Elasticsearch: %s", settings.elasticsearch.hosts)

    try:
        resp = _es_request("GET", "")
        info = resp.json()
        logger.info("ES cluster: %s (version %s)", info["cluster_name"], info["version"]["number"])
    except Exception as e:
        logger.error("Failed to connect to Elasticsearch: %s", e)
        return False

    # Check existing
    exists = _es_request("HEAD", index)
    if exists.status_code == 200:
        if delete_first:
            logger.info("Deleting existing index: %s", index)
            if not dry_run:
                _es_request("DELETE", index)
        else:
            logger.info("Index '%s' already exists -- skipping", index)
            return True

    if dry_run:
        logger.info("[DRY-RUN] Would create index: %s", index)
        return True

    try:
        resp = _es_request("PUT", index, json_body=ES_INDEX_MAPPING)
        if resp.status_code in (200, 201):
            logger.info("[OK] Elasticsearch index created: %s", index)
        else:
            logger.error("Failed to create index: %s", resp.text)
            return False
    except Exception as e:
        logger.error("Failed to create index: %s", e)
        return False

    return True


# ============================================================================
# Health check
# ============================================================================

def health_check() -> dict[str, str]:
    """Quick connectivity check for Kafka + Elasticsearch."""
    status: dict[str, str] = {"kafka": "...", "elasticsearch": "..."}

    # Kafka
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=settings.kafka.bootstrap_servers,
            client_id="health-check",
            request_timeout_ms=5000,
        )
        topics = admin.list_topics()
        admin.close()
        status["kafka"] = f"[OK] connected ({len(topics)} topics)"
    except Exception as e:
        status["kafka"] = f"[FAIL] {e}"

    # Elasticsearch
    try:
        resp = _es_request("GET", "", timeout=5)
        info = resp.json()
        status["elasticsearch"] = f"[OK] connected (v{info['version']['number']})"
    except Exception as e:
        status["elasticsearch"] = f"[FAIL] {e}"

    return status


# ============================================================================
# CLI
# ============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create Kafka topics & Elasticsearch indices")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    parser.add_argument("--delete", action="store_true", help="Delete and recreate")
    parser.add_argument("--topic", type=str, default=None, help="Kafka topic name")
    parser.add_argument("--index", type=str, default=None, help="ES index name")
    parser.add_argument("--health", action="store_true", help="Only health check")
    args = parser.parse_args()

    if args.health:
        report = health_check()
        for svc, stat in report.items():
            print(f"  {svc}: {stat}")
        sys.exit(0)

    print("=" * 60)
    print("TikTok Crawler — Infrastructure Setup")
    print("=" * 60)

    # Health check first
    report = health_check()
    for svc, stat in report.items():
        print(f"  {svc}: {stat}")

    if "[FAIL]" in report["kafka"] or "[FAIL]" in report["elasticsearch"]:
        logger.error("One or more services are unreachable. Aborting.")
        sys.exit(1)

    print()

    # Create Kafka topic
    ok_kafka = create_kafka_topic(
        topic=args.topic,
        dry_run=args.dry_run,
        delete_first=args.delete,
    )

    # Create ES index
    ok_es = create_elasticsearch_index(
        index=args.index,
        dry_run=args.dry_run,
        delete_first=args.delete,
    )

    print()
    if ok_kafka and ok_es:
        print("[OK] All infrastructure ready.")
    else:
        print("⚠ Some steps failed — check logs above.")
        sys.exit(1)