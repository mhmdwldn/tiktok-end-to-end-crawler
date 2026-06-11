"""Kafka output driver — AIOKafka-based (uses the running event loop)."""

import asyncio
import logging
from typing import Optional

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError, KafkaTimeoutError
from helpers.output.driver import OutputDriver

logger = logging.getLogger(__name__)


class KafkaOutputDriver(OutputDriver):
    """Publish messages to an Apache Kafka topic via AIOKafka.

    Uses the **currently running** event loop (``asyncio.get_running_loop()``)
    rather than creating a private loop, so the driver must be used from
    within an async context (e.g. inside ``asyncio.run()``).
    """

    name = "kafka"

    def __init__(
        self,
        topic: str,
        bootstrap_servers: str,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.topic = topic
        self._bootstrap_servers = bootstrap_servers
        self._producer: Optional[AIOKafkaProducer] = None

    # ------------------------------------------------------------------
    # OutputDriver interface
    # ------------------------------------------------------------------

    def put(self, output: str, **kwargs):
        """Send *output* (bytes or str) to the Kafka topic.

        Schedules the async send on the running event loop and waits for
        the result (synchronous wrapper).
        """
        self._ensure_producer()
        topic = kwargs.get("topic", self.topic)

        if isinstance(output, str):
            output = output.encode("utf-8")

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.error("KafkaOutputDriver.put() must be called from within an async context")
            return

        future = asyncio.run_coroutine_threadsafe(
            self._send(topic, output), loop
        )
        try:
            future.result(timeout=30)
        except KafkaTimeoutError:
            logger.error("Kafka timeout sending to topic=%s", topic)
        except KafkaError as err:
            logger.error("Kafka error on topic=%s: %s", topic, err)

    def close(self):
        """Stop the underlying producer (must be called from event-loop thread)."""
        if self._producer is None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running loop — cannot cleanly stop; the OS will close sockets
            self._producer = None
            return

        future = asyncio.run_coroutine_threadsafe(
            self._producer.stop(), loop
        )
        try:
            future.result(timeout=10)
        except Exception:
            pass
        self._producer = None

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _ensure_producer(self):
        """Lazily create and start the AIOKafkaProducer on the running loop."""
        if self._producer is not None:
            return

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError(
                "KafkaOutputDriver requires a running event loop. "
                "Ensure you are inside asyncio.run() or an async function."
            )

        producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            max_request_size=1_048_576,
            request_timeout_ms=60_000,
        )

        async def _start():
            await producer.start()

        future = asyncio.run_coroutine_threadsafe(_start(), loop)
        try:
            future.result(timeout=30)
        except Exception as e:
            logger.error("Failed to start Kafka producer: %s", e)
            raise

        self._producer = producer
        logger.info("KafkaOutputDriver connected to %s", self._bootstrap_servers)

    async def _send(self, topic: str, value: bytes):
        """Async send with flush."""
        assert self._producer is not None
        await self._producer.send_and_wait(topic, value)