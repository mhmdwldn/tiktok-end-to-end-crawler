"""Elasticsearch output driver — uses REST API for broad version compatibility."""

import json
import logging
from typing import Any, Optional

import requests
from helpers.output.driver import OutputDriver

logger = logging.getLogger(__name__)


class ElasticsearchOutputDriver(OutputDriver):
    """Index documents into Elasticsearch via REST API.

    Uses ``requests`` directly (not the ES client) to avoid version
    mismatches between the client library and the cluster.

    Errors are logged but do **not** raise — the pipeline continues
    on best-effort.
    """

    name = "elasticsearch"

    def __init__(
        self,
        index_name: str,
        hosts: list[str] | str,
        *args,
        request_timeout: int = 30,
        max_retries: int = 3,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.index_name = index_name
        if isinstance(hosts, str):
            hosts = [hosts]
        self._base_url = hosts[0].rstrip("/")
        self._request_timeout = request_timeout
        self._max_retries = max_retries
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

    # ------------------------------------------------------------------
    # OutputDriver interface
    # ------------------------------------------------------------------

    def put(self, output: str, **kwargs):
        """Index *output* (JSON string or dict) into Elasticsearch."""
        index = kwargs.get("index", self.index_name)
        doc_id = kwargs.get("doc_id")

        if isinstance(output, str):
            try:
                doc = json.loads(output)
            except json.JSONDecodeError:
                logger.error("Invalid JSON for ES indexing")
                return
        elif isinstance(output, dict):
            doc = output
        else:
            logger.error("Unsupported output type for ES: %s", type(output))
            return

        url = f"{self._base_url}/{index}/_doc"
        if doc_id:
            url = f"{url}/{doc_id}"

        for attempt in range(1, self._max_retries + 1):
            try:
                resp = self._session.post(url, json=doc, timeout=self._request_timeout)
                if resp.status_code in (200, 201):
                    logger.debug("Indexed doc_id=%s into index=%s", doc_id, index)
                    return
                else:
                    logger.error("ES indexing error (status=%d): %s", resp.status_code, resp.text[:200])
                    return
            except requests.RequestException as e:
                if attempt < self._max_retries:
                    logger.warning("ES attempt %d/%d failed: %s — retrying", attempt, self._max_retries, e)
                else:
                    logger.error("ES indexing failed after %d attempts: %s", self._max_retries, e)

    def close(self):
        """Close the HTTP session."""
        self._session.close()

    # ------------------------------------------------------------------
    # Bulk helper (optional)
    # ------------------------------------------------------------------

    def bulk_put(self, docs: list[dict], index: str | None = None):
        """Bulk-index a list of documents in a single _bulk request."""
        index = index or self.index_name
        if not docs:
            return

        lines = []
        for doc in docs:
            action = {"index": {"_index": index}}
            if "video_id" in doc:
                action["index"]["_id"] = doc["video_id"]
            lines.append(json.dumps(action, ensure_ascii=False))
            lines.append(json.dumps(doc, ensure_ascii=False, default=str))

        body = "\n".join(lines) + "\n"
        url = f"{self._base_url}/_bulk"

        try:
            resp = self._session.post(
                url, data=body,
                headers={"Content-Type": "application/x-ndjson"},
                timeout=self._request_timeout,
            )
            if resp.status_code == 200:
                result = resp.json()
                if result.get("errors"):
                    logger.warning("Bulk index had errors: %d docs", len(docs))
                else:
                    logger.info("Bulk-indexed %d docs into %s", len(docs), index)
            else:
                logger.error("Bulk index failed (status=%d): %s", resp.status_code, resp.text[:200])
        except requests.RequestException as e:
            logger.error("Bulk index request failed: %s", e)