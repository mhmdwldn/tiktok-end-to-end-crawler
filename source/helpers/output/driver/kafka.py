"""Kafka output driver — AIOKafka-based, uses running event loop."""

import asyncio
import logging
import threading
from typing import Optional

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError, KafkaTimeoutError
from helpers.output.driver import OutputDriver

logger = logging.getLogger(__name__)


class KafkaOutputDriver(OutputDriver):
    """Publish messages to an Apache Kafka topic via AIOKafka.

    The async producer runs on a **dedicated background thread** with its
    own event loop, so the synchronous ``put()`` interface never deadlocks
    with the caller's event loop.
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
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._ready = threading.Event()
        self._start_error: Optional[str] = None
        self._start_background()

    # ------------------------------------------------------------------
    # OutputDriver interface
    # ------------------------------------------------------------------

    def put(self, output: str, **kwargs):
        """Send *output* to the Kafka topic (thread-safe, synchronous)."""
        topic = kwargs.get("topic", self.topic)

        if isinstance(output, str):
            output = output.encode("utf-8")

        if not self._ready.wait(timeout=30):
            logger.error("Kafka producer not ready — dropping message for topic=%s", topic)
            return

        if self._loop is None or self._producer is None:
            logger.error("Kafka producer not available for topic=%s", topic)
            return

        future = asyncio.run_coroutine_threadsafe(
            self._send(topic, output), self._loop
        )
        try:
            future.result(timeout=30)
        except KafkaTimeoutError:
            logger.error("Kafka timeout sending to topic=%s", topic)
        except KafkaError as err:
            logger.error("Kafka error on topic=%s: %s", topic, err)

    def close(self):
        """Stop the background producer thread."""
        if self._loop is not None and self._producer is not None:
            future = asyncio.run_coroutine_threadsafe(
                self._producer.stop(), self._loop
            )
            try:
                future.result(timeout=10)
            except Exception:
                pass
            self._producer = None
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._loop = None
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=5)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _start_background(self):
        """Start the Kafka producer on a dedicated background thread."""

        def _run_loop():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop

            async def _start():
                producer = AIOKafkaProducer(
                    bootstrap_servers=self._bootstrap_servers,
                    max_request_size=1_048_576,
                    request_timeout_ms=60_000,
                )
                await producer.start()
                self._producer = producer
                self._ready.set()
                logger.info("KafkaOutputDriver connected to %s", self._bootstrap_servers)

            try:
                loop.run_until_complete(_start())
                # Keep the loop running to process future put() calls
                loop.run_forever()
            except Exception as e:
                self._start_error = str(e)
                logger.error("Kafka background loop error: %s", e)
                self._ready.set()  # unblock waiters
            finally:
                if self._producer is not None:
                    try:
                        loop.run_until_complete(self._producer.stop())
                    except Exception:
                        pass
                loop.close()

        self._thread = threading.Thread(target=_run_loop, daemon=True, name="kafka-producer")
        self._thread.start()

    async def _send(self, topic: str, value: bytes):
        """Async send with wait — runs on the background loop."""
        assert self._producer is not None
        await self._producer.send_and_wait(topic, value)