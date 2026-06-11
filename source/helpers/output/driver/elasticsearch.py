"""Elasticsearch output driver."""

import json
import logging
from typing import Any, Optional

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import (
    ConnectionError as ESConnectionError,
    ConnectionTimeout as ESConnectionTimeout,
    TransportError,
)
from helpers.output.driver import OutputDriver

logger = logging.getLogger(__name__)


class ElasticsearchOutputDriver(OutputDriver):
    """Index documents into Elasticsearch.

    Errors are logged but do **not** raise — the pipeline continues
    on best-effort.  For strict delivery guarantees, wrap calls in
    application-level retry / dead-letter logic.
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
        self._hosts = hosts
        self._request_timeout = request_timeout
        self._max_retries = max_retries
        self._client: Optional[Elasticsearch] = None
        self._ensure_client()

    # ------------------------------------------------------------------
    # OutputDriver interface
    # ------------------------------------------------------------------

    def put(self, output: str, **kwargs):
        """Index *output* (JSON string or dict) into Elasticsearch."""
        self._ensure_client()

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

        try:
            self._client.index(index=index, id=doc_id, document=doc)
            logger.debug("Indexed doc_id=%s into index=%s", doc_id, index)
        except (ESConnectionError, ESConnectionTimeout, TransportError) as e:
            logger.error("ES indexing error for index=%s: %s", index, e)

    def close(self):
        """Close the Elasticsearch client."""
        if self._client is not None:
            self._client.close()
            self._client = None

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _ensure_client(self):
        """Lazily create the Elasticsearch client from stored config."""
        if self._client is not None:
            return
        self._client = Elasticsearch(
            hosts=self._hosts,
            request_timeout=self._request_timeout,
            max_retries=self._max_retries,
            retry_on_timeout=True,
        )
        logger.info(
            "ElasticsearchOutputDriver connected to %s (timeout=%ds, retries=%d)",
            self._hosts, self._request_timeout, self._max_retries,
        )