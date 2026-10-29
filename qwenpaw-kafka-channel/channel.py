"""Kafka-backed QwenPaw channel.

The channel owns the Kafka consumer loop instead of using ChannelManager's
in-memory ingress queue.  This lets it commit a Kafka offset only after the
Agent turn has returned.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import ssl
import time
import uuid
from collections import OrderedDict
from pathlib import Path
from typing import Any

from qwenpaw.app.channels.base import (
    BaseChannel,
    OnReplySent,
    OutgoingContentPart,
    ProcessHandler,
)
from qwenpaw.app.channels.renderer import ChannelDisplayConfig
from qwenpaw.schemas import ContentType, TextContent

logger = logging.getLogger(__name__)


def _value(config: Any, name: str, default: Any = None) -> Any:
    if isinstance(config, dict):
        return config.get(name, default)
    return getattr(config, name, default)


def _text(config: Any, name: str, default: str = "") -> str:
    value = _value(config, name, default)
    return str(value if value is not None else default).strip()


def _integer(config: Any, name: str, default: int) -> int:
    try:
        return int(_value(config, name, default))
    except (TypeError, ValueError):
        return default


def _boolean(config: Any, name: str, default: bool) -> bool:
    """读取页面开关，同时兼容布尔值、数字和常见字符串。"""
    value = _value(config, name, default)
    if isinstance(value, bool):
        return value
    if value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "on"}:
            return True
        if normalized in {"false", "0", "no", "off"}:
            return False
    return default


class KafkaChannel(BaseChannel):
    """Consume Kafka records, invoke an Agent, and publish its replies."""

    channel = "kafka"
    # Kafka offsets are committed by this channel after Agent processing.
    uses_manager_queue = False

    def __init__(
        self,
        process: ProcessHandler,
        *,
        enabled: bool,
        input_bootstrap_servers: str,
        input_topic: str,
        qwenpaw_consumer_group: str,
        qwenpaw_consumer_client_id: str = "qwenpaw-kafka-consumer",
        auto_offset_reset: str = "latest",
        input_security_protocol: str = "PLAINTEXT",
        input_sasl_mechanism: str = "PLAIN",
        input_sasl_username: str = "",
        input_sasl_password: str = "",
        input_ssl_cafile: str = "",
        input_ssl_certfile: str = "",
        input_ssl_keyfile: str = "",
        output_bootstrap_servers: str = "",
        output_topic: str = "",
        qwenpaw_producer_client_id: str = "qwenpaw-kafka-producer",
        output_security_protocol: str = "PLAINTEXT",
        output_sasl_mechanism: str = "PLAIN",
        output_sasl_username: str = "",
        output_sasl_password: str = "",
        output_ssl_cafile: str = "",
        output_ssl_certfile: str = "",
        output_ssl_keyfile: str = "",
        text_field: str = "text",
        reply_topic_field: str = "reply_to",
        sender_id_field: str = "sender_id",
        session_id_field: str = "session_id",
        message_id_field: str = "message_id",
        correlation_id_field: str = "correlation_id",
        max_poll_interval_ms: int = 1_800_000,
        max_concurrent_messages: int = 5,
        raw_input_output: bool = True,
        bot_prefix: str = "",
        on_reply_sent: OnReplySent = None,
        display_config: ChannelDisplayConfig | None = None,
        no_text_debounce: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            process,
            on_reply_sent=on_reply_sent,
            display_config=display_config,
            no_text_debounce=no_text_debounce,
        )
        self.enabled = enabled
        self.input_bootstrap_servers = input_bootstrap_servers
        self.input_topic = input_topic
        self.group_id = qwenpaw_consumer_group
        self.qwenpaw_consumer_client_id = (
            qwenpaw_consumer_client_id or "qwenpaw-kafka-consumer"
        )
        self.auto_offset_reset = auto_offset_reset
        self.input_security_protocol = input_security_protocol.upper()
        self.input_sasl_mechanism = input_sasl_mechanism.upper()
        self.input_sasl_username = input_sasl_username
        self.input_sasl_password = input_sasl_password
        self.input_ssl_cafile = input_ssl_cafile
        self.input_ssl_certfile = input_ssl_certfile
        self.input_ssl_keyfile = input_ssl_keyfile
        self.output_bootstrap_servers = output_bootstrap_servers
        self.output_topic = output_topic
        self.qwenpaw_producer_client_id = (
            qwenpaw_producer_client_id or "qwenpaw-kafka-producer"
        )
        self.output_security_protocol = output_security_protocol.upper()
        self.output_sasl_mechanism = output_sasl_mechanism.upper()
        self.output_sasl_username = output_sasl_username
        self.output_sasl_password = output_sasl_password
        self.output_ssl_cafile = output_ssl_cafile
        self.output_ssl_certfile = output_ssl_certfile
        self.output_ssl_keyfile = output_ssl_keyfile
        self.text_field = text_field or "text"
        self.reply_topic_field = reply_topic_field or "reply_to"
        self.sender_id_field = sender_id_field or "sender_id"
        self.session_id_field = session_id_field or "session_id"
        self.message_id_field = message_id_field or "message_id"
        self.correlation_id_field = correlation_id_field or "correlation_id"
        self.max_poll_interval_ms = max(60_000, max_poll_interval_ms)
        # Bound the number of Agent turns that may run at the same time.  The
        # offset tracker below still commits each partition only through its
        # continuously completed prefix, so an earlier slow request cannot be
        # acknowledged merely because a later request finished first.
        self.max_concurrent_messages = max(1, min(100, max_concurrent_messages))
        # 开启时不解析业务 JSON，也不包装响应 JSON：Kafka value 原文进入
        # Agent，Agent 最终回答原文进入下游 Kafka。
        self.raw_input_output = raw_input_output
        self.bot_prefix = bot_prefix

        self._consumer: Any = None
        self._producer: Any = None
        self._consume_task: asyncio.Task[None] | None = None
        self._connected = False
        self._commit_lock = asyncio.Lock()
        # (topic, partition) -> ordered mapping of fetched offset -> completed.
        # OrderedDict is used instead of assuming numeric offsets are adjacent,
        # because compacted Kafka topics may contain offset gaps.
        self._partition_progress: dict[
            tuple[str, int], OrderedDict[int, bool]
        ] = {}
        # Avoid a duplicate Agent call when processing succeeded but the
        # following Kafka offset commit temporarily failed.
        self._seen_ids: OrderedDict[str, float] = OrderedDict()
        self._seen_limit = 10_000

    @classmethod
    def from_config(
        cls,
        process: ProcessHandler,
        config: Any,
        on_reply_sent: OnReplySent = None,
        display_config: ChannelDisplayConfig | None = None,
        no_text_debounce: bool = True,
        workspace_dir: Path | None = None,
        **kwargs: Any,
    ) -> KafkaChannel:
        del workspace_dir, kwargs
        legacy_bootstrap_servers = _text(config, "bootstrap_servers")
        legacy_client_id = _text(config, "client_id", "qwenpaw-kafka")
        legacy_security_protocol = _text(config, "security_protocol", "PLAINTEXT")
        legacy_sasl_mechanism = _text(config, "sasl_mechanism", "PLAIN")
        legacy_sasl_username = _text(config, "sasl_username")
        legacy_sasl_password = _text(config, "sasl_password")
        legacy_ssl_cafile = _text(config, "ssl_cafile")
        legacy_ssl_certfile = _text(config, "ssl_certfile")
        legacy_ssl_keyfile = _text(config, "ssl_keyfile")
        # 1.2.x named endpoints from QwenPaw's internal Kafka role. 1.3.x
        # names them from the external role shown in the form: producer_* is
        # request/input and consumer_* is response/output. The new group key
        # acts as an unambiguous migration marker for already-saved configs.
        external_role_names = _value(config, "qwenpaw_consumer_group") is not None
        input_prefix = "producer" if external_role_names else "consumer"
        output_prefix = "consumer" if external_role_names else "producer"

        def endpoint_text(prefix: str, suffix: str, default: str = "") -> str:
            return _text(config, f"{prefix}_{suffix}", default)

        return cls(
            process,
            enabled=bool(_value(config, "enabled", False)),
            input_bootstrap_servers=endpoint_text(
                input_prefix, "bootstrap_servers", legacy_bootstrap_servers
            ),
            input_topic=_text(config, "input_topic"),
            qwenpaw_consumer_group=_text(
                config,
                "qwenpaw_consumer_group",
                _text(
                    config,
                    "consumer_group_id",
                    _text(config, "group_id", "qwenpaw-agent"),
                ),
            ),
            qwenpaw_consumer_client_id=_text(
                config,
                "qwenpaw_consumer_client_id",
                _text(
                    config,
                    "consumer_client_id",
                    f"{legacy_client_id}-consumer",
                ),
            ),
            auto_offset_reset=_text(config, "auto_offset_reset", "latest"),
            input_security_protocol=endpoint_text(
                input_prefix, "security_protocol", legacy_security_protocol
            ),
            input_sasl_mechanism=endpoint_text(
                input_prefix, "sasl_mechanism", legacy_sasl_mechanism
            ),
            input_sasl_username=endpoint_text(
                input_prefix, "sasl_username", legacy_sasl_username
            ),
            input_sasl_password=endpoint_text(
                input_prefix, "sasl_password", legacy_sasl_password
            ),
            input_ssl_cafile=endpoint_text(
                input_prefix, "ssl_cafile", legacy_ssl_cafile
            ),
            input_ssl_certfile=endpoint_text(
                input_prefix, "ssl_certfile", legacy_ssl_certfile
            ),
            input_ssl_keyfile=endpoint_text(
                input_prefix, "ssl_keyfile", legacy_ssl_keyfile
            ),
            output_bootstrap_servers=endpoint_text(
                output_prefix, "bootstrap_servers", legacy_bootstrap_servers
            ),
            output_topic=_text(config, "output_topic"),
            qwenpaw_producer_client_id=_text(
                config,
                "qwenpaw_producer_client_id",
                _text(
                    config,
                    "producer_client_id",
                    f"{legacy_client_id}-producer",
                ),
            ),
            output_security_protocol=endpoint_text(
                output_prefix, "security_protocol", legacy_security_protocol
            ),
            output_sasl_mechanism=endpoint_text(
                output_prefix, "sasl_mechanism", legacy_sasl_mechanism
            ),
            output_sasl_username=endpoint_text(
                output_prefix, "sasl_username", legacy_sasl_username
            ),
            output_sasl_password=endpoint_text(
                output_prefix, "sasl_password", legacy_sasl_password
            ),
            output_ssl_cafile=endpoint_text(
                output_prefix, "ssl_cafile", legacy_ssl_cafile
            ),
            output_ssl_certfile=endpoint_text(
                output_prefix, "ssl_certfile", legacy_ssl_certfile
            ),
            output_ssl_keyfile=endpoint_text(
                output_prefix, "ssl_keyfile", legacy_ssl_keyfile
            ),
            text_field=_text(config, "text_field", "text"),
            reply_topic_field=_text(config, "reply_topic_field", "reply_to"),
            sender_id_field=_text(config, "sender_id_field", "sender_id"),
            session_id_field=_text(config, "session_id_field", "session_id"),
            message_id_field=_text(config, "message_id_field", "message_id"),
            correlation_id_field=_text(
                config, "correlation_id_field", "correlation_id"
            ),
            max_poll_interval_ms=_integer(config, "max_poll_interval_ms", 1_800_000),
            max_concurrent_messages=_integer(
                config, "max_concurrent_messages", 5
            ),
            raw_input_output=_boolean(config, "raw_input_output", True),
            bot_prefix=_text(config, "bot_prefix"),
            on_reply_sent=on_reply_sent,
            display_config=display_config or ChannelDisplayConfig.from_config(config),
            no_text_debounce=no_text_debounce,
        )

    @classmethod
    def from_env(
        cls,
        process: ProcessHandler,
        on_reply_sent: OnReplySent = None,
    ) -> KafkaChannel:
        config = {
            "enabled": os.getenv("KAFKA_CHANNEL_ENABLED", "0") == "1",
            "consumer_bootstrap_servers": os.getenv(
                "KAFKA_CONSUMER_BOOTSTRAP_SERVERS",
                os.getenv("KAFKA_BOOTSTRAP_SERVERS", ""),
            ),
            "input_topic": os.getenv("KAFKA_INPUT_TOPIC", ""),
            "qwenpaw_consumer_group": os.getenv(
                "KAFKA_QWENPAW_CONSUMER_GROUP",
                os.getenv("KAFKA_GROUP_ID", "qwenpaw-agent"),
            ),
            "qwenpaw_consumer_client_id": os.getenv(
                "KAFKA_QWENPAW_CONSUMER_CLIENT_ID",
                os.getenv("KAFKA_CONSUMER_CLIENT_ID", "qwenpaw-kafka-consumer"),
            ),
            "auto_offset_reset": os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest"),
            "consumer_security_protocol": os.getenv(
                "KAFKA_CONSUMER_SECURITY_PROTOCOL",
                os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
            ),
            "consumer_sasl_mechanism": os.getenv(
                "KAFKA_CONSUMER_SASL_MECHANISM",
                os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
            ),
            "consumer_sasl_username": os.getenv(
                "KAFKA_CONSUMER_SASL_USERNAME",
                os.getenv("KAFKA_SASL_USERNAME", ""),
            ),
            "consumer_sasl_password": os.getenv(
                "KAFKA_CONSUMER_SASL_PASSWORD",
                os.getenv("KAFKA_SASL_PASSWORD", ""),
            ),
            "consumer_ssl_cafile": os.getenv(
                "KAFKA_CONSUMER_SSL_CAFILE", os.getenv("KAFKA_SSL_CAFILE", "")
            ),
            "consumer_ssl_certfile": os.getenv(
                "KAFKA_CONSUMER_SSL_CERTFILE",
                os.getenv("KAFKA_SSL_CERTFILE", ""),
            ),
            "consumer_ssl_keyfile": os.getenv(
                "KAFKA_CONSUMER_SSL_KEYFILE", os.getenv("KAFKA_SSL_KEYFILE", "")
            ),
            "producer_bootstrap_servers": os.getenv(
                "KAFKA_PRODUCER_BOOTSTRAP_SERVERS",
                os.getenv("KAFKA_BOOTSTRAP_SERVERS", ""),
            ),
            "output_topic": os.getenv("KAFKA_OUTPUT_TOPIC", ""),
            "qwenpaw_producer_client_id": os.getenv(
                "KAFKA_QWENPAW_PRODUCER_CLIENT_ID",
                os.getenv("KAFKA_PRODUCER_CLIENT_ID", "qwenpaw-kafka-producer"),
            ),
            "producer_security_protocol": os.getenv(
                "KAFKA_PRODUCER_SECURITY_PROTOCOL",
                os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
            ),
            "producer_sasl_mechanism": os.getenv(
                "KAFKA_PRODUCER_SASL_MECHANISM",
                os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
            ),
            "producer_sasl_username": os.getenv(
                "KAFKA_PRODUCER_SASL_USERNAME",
                os.getenv("KAFKA_SASL_USERNAME", ""),
            ),
            "producer_sasl_password": os.getenv(
                "KAFKA_PRODUCER_SASL_PASSWORD",
                os.getenv("KAFKA_SASL_PASSWORD", ""),
            ),
            "producer_ssl_cafile": os.getenv(
                "KAFKA_PRODUCER_SSL_CAFILE", os.getenv("KAFKA_SSL_CAFILE", "")
            ),
            "producer_ssl_certfile": os.getenv(
                "KAFKA_PRODUCER_SSL_CERTFILE",
                os.getenv("KAFKA_SSL_CERTFILE", ""),
            ),
            "producer_ssl_keyfile": os.getenv(
                "KAFKA_PRODUCER_SSL_KEYFILE", os.getenv("KAFKA_SSL_KEYFILE", "")
            ),
            "raw_input_output": os.getenv("KAFKA_RAW_INPUT_OUTPUT", "1"),
            "max_concurrent_messages": os.getenv(
                "KAFKA_MAX_CONCURRENT_MESSAGES", "5"
            ),
        }
        return cls.from_config(process, config, on_reply_sent=on_reply_sent)

    def _validate_config(self) -> None:
        missing = [
            name
            for name, value in (
                ("input_bootstrap_servers", self.input_bootstrap_servers),
                ("input_topic", self.input_topic),
                ("qwenpaw_consumer_group", self.group_id),
                ("output_bootstrap_servers", self.output_bootstrap_servers),
                ("output_topic", self.output_topic),
            )
            if not value
        ]
        if missing:
            raise ValueError("missing required Kafka settings: " + ", ".join(missing))
        if self.auto_offset_reset not in {"latest", "earliest"}:
            raise ValueError("auto_offset_reset must be latest or earliest")
        supported_protocols = {"PLAINTEXT", "SASL_PLAINTEXT", "SSL", "SASL_SSL"}
        for role, protocol, username, password in (
            (
                "input",
                self.input_security_protocol,
                self.input_sasl_username,
                self.input_sasl_password,
            ),
            (
                "output",
                self.output_security_protocol,
                self.output_sasl_username,
                self.output_sasl_password,
            ),
        ):
            if protocol not in supported_protocols:
                raise ValueError(f"unsupported {role}_security_protocol: {protocol}")
            if protocol.startswith("SASL_") and (not username or not password):
                raise ValueError(f"{role} SASL username and password are required")

    @staticmethod
    def _ssl_context(
        security_protocol: str,
        ssl_cafile: str,
        ssl_certfile: str,
        ssl_keyfile: str,
    ) -> ssl.SSLContext | None:
        if security_protocol not in {"SSL", "SASL_SSL"}:
            return None
        context = ssl.create_default_context(cafile=ssl_cafile or None)
        if ssl_certfile:
            context.load_cert_chain(
                certfile=ssl_certfile,
                keyfile=ssl_keyfile or None,
            )
        return context

    def _connection_kwargs(
        self,
        *,
        bootstrap_servers: str,
        security_protocol: str,
        sasl_mechanism: str,
        sasl_username: str,
        sasl_password: str,
        ssl_cafile: str,
        ssl_certfile: str,
        ssl_keyfile: str,
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "bootstrap_servers": [
                item.strip() for item in bootstrap_servers.split(",") if item.strip()
            ],
            "security_protocol": security_protocol,
        }
        ssl_context = self._ssl_context(
            security_protocol, ssl_cafile, ssl_certfile, ssl_keyfile
        )
        if ssl_context is not None:
            kwargs["ssl_context"] = ssl_context
        if security_protocol.startswith("SASL_"):
            kwargs.update(
                sasl_mechanism=sasl_mechanism,
                sasl_plain_username=sasl_username,
                sasl_plain_password=sasl_password,
            )
        return kwargs

    def _consumer_connection_kwargs(self) -> dict[str, Any]:
        return self._connection_kwargs(
            bootstrap_servers=self.input_bootstrap_servers,
            security_protocol=self.input_security_protocol,
            sasl_mechanism=self.input_sasl_mechanism,
            sasl_username=self.input_sasl_username,
            sasl_password=self.input_sasl_password,
            ssl_cafile=self.input_ssl_cafile,
            ssl_certfile=self.input_ssl_certfile,
            ssl_keyfile=self.input_ssl_keyfile,
        )

    def _producer_connection_kwargs(self) -> dict[str, Any]:
        return self._connection_kwargs(
            bootstrap_servers=self.output_bootstrap_servers,
            security_protocol=self.output_security_protocol,
            sasl_mechanism=self.output_sasl_mechanism,
            sasl_username=self.output_sasl_username,
            sasl_password=self.output_sasl_password,
            ssl_cafile=self.output_ssl_cafile,
            ssl_certfile=self.output_ssl_certfile,
            ssl_keyfile=self.output_ssl_keyfile,
        )

    async def start(self) -> None:
        if not self.enabled:
            return
        if self._consume_task and not self._consume_task.done():
            return
        self._validate_config()
        try:
            from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
        except ImportError as exc:
            raise RuntimeError(
                "aiokafka is not installed; reinstall the Kafka plugin dependencies"
            ) from exc

        self._producer = AIOKafkaProducer(
            **self._producer_connection_kwargs(),
            client_id=self.qwenpaw_producer_client_id,
            acks="all",
            enable_idempotence=True,
        )
        self._consumer = AIOKafkaConsumer(
            self.input_topic,
            **self._consumer_connection_kwargs(),
            group_id=self.group_id,
            client_id=self.qwenpaw_consumer_client_id,
            enable_auto_commit=False,
            auto_offset_reset=self.auto_offset_reset,
            max_poll_interval_ms=self.max_poll_interval_ms,
        )

        try:
            await self._producer.start()
            await self._consumer.start()
        except Exception:
            await self._close_clients()
            raise
        self._connected = True
        self._consume_task = asyncio.create_task(
            self._consume_loop(), name=f"kafka-channel-{self.group_id}"
        )
        logger.info(
            "Kafka channel started: input=%s producer_side_brokers=%s "
            "output=%s consumer_side_brokers=%s group=%s",
            self.input_topic,
            self.input_bootstrap_servers,
            self.output_topic,
            self.output_bootstrap_servers,
            self.group_id,
        )

    async def stop(self) -> None:
        self._connected = False
        if self._consume_task:
            self._consume_task.cancel()
            try:
                await self._consume_task
            except asyncio.CancelledError:
                pass
            self._consume_task = None
        await self._close_clients()
        logger.info("Kafka channel stopped")

    async def _close_clients(self) -> None:
        consumer, producer = self._consumer, self._producer
        self._consumer = None
        self._producer = None
        if consumer is not None:
            try:
                await consumer.stop()
            except Exception:
                logger.exception("Failed to stop Kafka consumer")
        if producer is not None:
            try:
                await producer.stop()
            except Exception:
                logger.exception("Failed to stop Kafka producer")

    async def _consume_loop(self) -> None:
        workers: set[asyncio.Task[None]] = set()
        next_record_task: asyncio.Task[Any] | None = None
        try:
            iterator = self._consumer.__aiter__()
            while True:
                # Keep at most one pending Kafka read, and stop reading when all
                # Agent concurrency slots are occupied. This provides bounded
                # backpressure rather than buffering an unlimited number of records.
                if (
                    next_record_task is None
                    and len(workers) < self.max_concurrent_messages
                ):
                    next_record_task = asyncio.create_task(iterator.__anext__())

                waiting: set[asyncio.Task[Any]] = set(workers)
                if next_record_task is not None:
                    waiting.add(next_record_task)
                done, _ = await asyncio.wait(
                    waiting, return_when=asyncio.FIRST_COMPLETED
                )

                # Observe completed workers first. Any Agent/publish/commit error
                # stops the loop without acknowledging that failed record.
                completed_workers = [task for task in done if task in workers]
                for task in completed_workers:
                    workers.remove(task)
                    task.result()

                if next_record_task is not None and next_record_task in done:
                    try:
                        record = next_record_task.result()
                    except StopAsyncIteration:
                        next_record_task = None
                        break
                    next_record_task = None
                    self._register_record(record)
                    worker = asyncio.create_task(
                        self._process_record(record),
                        name=(
                            f"kafka-record-{record.topic}-{record.partition}-"
                            f"{record.offset}"
                        ),
                    )
                    workers.add(worker)

            if workers:
                await asyncio.gather(*workers)
        except asyncio.CancelledError:
            raise
        except Exception:
            self._connected = False
            logger.exception("Kafka consumer loop stopped unexpectedly")
        finally:
            pending = list(workers)
            if next_record_task is not None:
                pending.append(next_record_task)
            for task in pending:
                if not task.done():
                    task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

    async def _process_record(self, record: Any) -> None:
        """Run one Kafka record through the Agent and mark it complete."""
        try:
            native = self._record_to_native(record)
        except Exception:
            # Poison records cannot become valid through retry. Log and mark them
            # complete so one malformed record does not permanently block a partition.
            logger.exception(
                "Invalid Kafka record at %s[%s] offset=%s; skipping",
                record.topic,
                record.partition,
                record.offset,
            )
            await self._mark_record_complete(record)
            return

        message_id = native["meta"]["request_message_id"]
        if message_id in self._seen_ids:
            logger.warning("Skipping duplicate Kafka message_id=%s", message_id)
            await self._mark_record_complete(record)
            return

        # BaseChannel runs the Agent and publishes the completed reply. An
        # exception deliberately leaves this offset uncommitted for retry.
        await self._consume_one_request(native)
        self._remember(message_id)
        await self._mark_record_complete(record)

    def _register_record(self, record: Any) -> None:
        """Record fetch order before starting concurrent processing."""
        key = (str(record.topic), int(record.partition))
        progress = self._partition_progress.setdefault(key, OrderedDict())
        progress.setdefault(int(record.offset), False)

    async def _mark_record_complete(self, record: Any) -> None:
        """Commit only the continuously completed prefix of one partition."""
        from aiokafka import TopicPartition

        key = (str(record.topic), int(record.partition))
        offset = int(record.offset)
        async with self._commit_lock:
            progress = self._partition_progress.setdefault(key, OrderedDict())
            progress.setdefault(offset, False)
            progress[offset] = True

            completed_prefix: list[int] = []
            for pending_offset, completed in progress.items():
                if not completed:
                    break
                completed_prefix.append(pending_offset)
            if not completed_prefix:
                return

            commit_offset = completed_prefix[-1] + 1
            tp = TopicPartition(*key)
            await self._consumer.commit({tp: commit_offset})
            for completed_offset in completed_prefix:
                progress.pop(completed_offset, None)

    async def _commit_record(self, record: Any) -> None:
        """Backward-compatible alias for callers from earlier plugin versions."""
        self._register_record(record)
        await self._mark_record_complete(record)

    def _remember(self, message_id: str) -> None:
        self._seen_ids[message_id] = time.monotonic()
        self._seen_ids.move_to_end(message_id)
        while len(self._seen_ids) > self._seen_limit:
            self._seen_ids.popitem(last=False)

    @staticmethod
    def _decode_optional(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return str(value)

    def _record_to_native(self, record: Any) -> dict[str, Any]:
        raw_value = self._decode_optional(record.value)
        if not raw_value.strip():
            raise ValueError("Kafka message value is empty")

        key = self._decode_optional(getattr(record, "key", None))
        fallback_id = f"{record.topic}:{record.partition}:{record.offset}"

        if self.raw_input_output:
            # 原样模式只把 Kafka record value 解码为字符串，不做 JSON 提取或
            # strip；路由固定使用默认下游 Topic。每条消息使用独立的随机会话，
            # 避免继承任何历史上下文。其余字段仅供频道内部 offset 幂等和
            # Kafka key 使用，不会写入下游消息正文。
            sender_id = key or "kafka-user"
            session_id = f"kafka:raw:{uuid.uuid4()}"
            return {
                "channel_id": self.channel,
                "sender_id": sender_id,
                "session_id": session_id,
                "content_parts": [
                    TextContent(type=ContentType.TEXT, text=raw_value),
                ],
                "meta": {
                    "request_message_id": fallback_id,
                    "correlation_id": fallback_id,
                    "reply_topic": "",
                    "sender_id": sender_id,
                    "session_id": session_id,
                    "input_topic": record.topic,
                    "partition": record.partition,
                    "offset": record.offset,
                    "kafka_key": key,
                    "kafka_headers": {
                        self._decode_optional(k): self._decode_optional(v)
                        for k, v in (getattr(record, "headers", None) or [])
                    },
                },
            }

        raw = raw_value.strip()

        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError:
            decoded = {self.text_field: raw}
        if isinstance(decoded, str):
            decoded = {self.text_field: decoded}
        if not isinstance(decoded, dict):
            raise TypeError("Kafka message must be a JSON object or text")

        text = decoded.get(self.text_field)
        if text is None:
            raise ValueError(f"Kafka JSON message has no {self.text_field!r} field")
        if not isinstance(text, str):
            text = json.dumps(text, ensure_ascii=False, separators=(",", ":"))

        sender_id = str(decoded.get(self.sender_id_field) or key or "kafka-user")
        supplied_session_id = str(decoded.get(self.session_id_field) or "").strip()
        session_id = supplied_session_id or f"kafka:{sender_id}"
        if not session_id.startswith("kafka:"):
            session_id = f"kafka:{session_id}"

        message_id = str(decoded.get(self.message_id_field) or fallback_id)
        correlation_id = str(decoded.get(self.correlation_id_field) or message_id)
        reply_topic = str(decoded.get(self.reply_topic_field) or "").strip()
        headers = {
            self._decode_optional(k): self._decode_optional(v)
            for k, v in (getattr(record, "headers", None) or [])
        }

        meta = {
            "request_message_id": message_id,
            "correlation_id": correlation_id,
            "reply_topic": reply_topic,
            "sender_id": sender_id,
            "session_id": session_id,
            "input_topic": record.topic,
            "partition": record.partition,
            "offset": record.offset,
            "kafka_key": key,
            "kafka_headers": headers,
        }
        custom_metadata = decoded.get("metadata")
        if isinstance(custom_metadata, dict):
            # Keep routing and identity fields authoritative.
            meta = {**custom_metadata, **meta}

        return {
            "channel_id": self.channel,
            "sender_id": sender_id,
            "session_id": session_id,
            "content_parts": [TextContent(type=ContentType.TEXT, text=text)],
            "meta": meta,
        }

    def build_agent_request_from_native(self, native_payload: Any) -> Any:
        payload = native_payload if isinstance(native_payload, dict) else {}
        sender_id = str(payload.get("sender_id") or "kafka-user")
        meta = dict(payload.get("meta") or {})
        session_id = str(
            payload.get("session_id") or meta.get("session_id") or f"kafka:{sender_id}"
        )
        request = self.build_agent_request_from_user_content(
            channel_id=self.channel,
            sender_id=sender_id,
            session_id=session_id,
            content_parts=payload.get("content_parts") or [],
            channel_meta=meta,
        )
        request.channel_meta = meta
        return request

    def resolve_session_id(
        self, sender_id: str, channel_meta: dict[str, Any] | None = None
    ) -> str:
        meta = channel_meta or {}
        return str(meta.get("session_id") or f"kafka:{sender_id}")

    def get_to_handle_from_request(self, request: Any) -> str:
        meta = getattr(request, "channel_meta", None) or {}
        return str(meta.get("reply_topic") or self.output_topic)

    @staticmethod
    def _event_type(event: Any) -> str:
        value = getattr(event, "type", None)
        if value is None:
            return "message"
        return str(value.value if hasattr(value, "value") else value)

    @staticmethod
    def _parts_to_text(parts: list[OutgoingContentPart]) -> str:
        rendered: list[str] = []
        for part in parts:
            part_type = getattr(part, "type", None)
            if part_type == ContentType.TEXT and getattr(part, "text", None):
                rendered.append(str(part.text))
            elif part_type == ContentType.REFUSAL and getattr(part, "refusal", None):
                rendered.append(str(part.refusal))
            elif part_type == ContentType.IMAGE and getattr(part, "image_url", None):
                rendered.append(f"[Image: {part.image_url}]")
            elif part_type == ContentType.VIDEO and getattr(part, "video_url", None):
                rendered.append(f"[Video: {part.video_url}]")
            elif part_type == ContentType.FILE and (
                getattr(part, "file_url", None) or getattr(part, "file_id", None)
            ):
                rendered.append(f"[File: {part.file_url or part.file_id}]")
            elif part_type == ContentType.AUDIO:
                rendered.append("[Audio]")
        return "\n".join(rendered).strip()

    async def on_event_content(
        self,
        request: Any,
        to_handle: str,
        event: Any,
        send_meta: dict[str, Any],
    ) -> bool:
        """Collect visible tool output into the single response envelope."""
        del request, to_handle
        if getattr(event, "type", None) != ContentType.DATA:
            return False
        if not self._display_config.show_tool_results:
            return False
        data = getattr(event, "data", None) or {}
        if not isinstance(data, dict) or "output" not in data:
            return False
        body = self._format_stream_tool_output_body(event)
        if body:
            send_meta.setdefault("_kafka_details", []).append(
                {"type": "tool_result", "text": body}
            )
        return True

    async def on_event_message_completed(
        self,
        request: Any,
        to_handle: str,
        event: Any,
        send_meta: dict[str, Any],
    ) -> None:
        """Buffer completed events and publish once when the turn finishes."""
        del request, to_handle
        text = self._parts_to_text(self._message_to_content_parts(event))
        if not text:
            return
        event_type = self._event_type(event)
        if event_type == "reasoning":
            send_meta.setdefault("_kafka_reasoning", []).append(text)
        elif event_type == "message":
            send_meta.setdefault("_kafka_answers", []).append(text)
        else:
            send_meta.setdefault("_kafka_details", []).append(
                {"type": event_type, "text": text}
            )

    async def _send_model_fallback_notice(
        self,
        to_handle: str,
        event: Any,
        send_meta: dict[str, Any],
    ) -> None:
        """Keep model fallback notices inside the aggregated JSON."""
        del to_handle
        for fallback in self._model_fallback_events(event):
            send_meta.setdefault("_kafka_details", []).append(
                {
                    "type": "model_fallback",
                    "text": self._format_model_fallback_notice(fallback),
                }
            )

    async def _on_process_completed(
        self,
        request: Any,
        to_handle: str,
        send_meta: dict[str, Any],
    ) -> None:
        """Publish exactly one Kafka record for one completed Agent turn."""
        del request, to_handle
        reasoning = "\n\n".join(send_meta.pop("_kafka_reasoning", []))
        answer = "\n\n".join(send_meta.pop("_kafka_answers", []))
        details = send_meta.pop("_kafka_details", [])
        await self._publish_response(send_meta, reasoning, answer, details)

    def _render_output_topic(self, meta: dict[str, Any]) -> str:
        dynamic = str(meta.get("reply_topic") or "").strip()
        template = dynamic or self.output_topic
        values = {
            "sender_id": str(meta.get("sender_id") or ""),
            "session_id": str(meta.get("session_id") or ""),
            "correlation_id": str(meta.get("correlation_id") or ""),
            "message_id": str(meta.get("request_message_id") or ""),
        }
        try:
            topic = template.format(**values)
        except (KeyError, ValueError) as exc:
            raise ValueError(
                f"invalid Kafka output topic template: {template}"
            ) from exc
        if not topic:
            raise ValueError("Kafka output topic resolved to an empty value")
        return topic

    async def _publish_response(
        self,
        meta: dict[str, Any] | None,
        reasoning: str,
        answer: str,
        details: list[dict[str, str]] | None = None,
    ) -> None:
        if self._producer is None:
            raise RuntimeError("Kafka producer is not connected")
        route = dict(meta or {})
        topic = self._render_output_topic(route)
        key_value = str(route.get("correlation_id") or route.get("sender_id") or "")
        if self.raw_input_output:
            await self._producer.send_and_wait(
                topic,
                answer.encode("utf-8"),
                key=key_value.encode("utf-8") if key_value else None,
                headers=[("content-type", b"text/plain; charset=utf-8")],
            )
            return

        envelope = {
            "type": "agent.response",
            "response_id": str(uuid.uuid4()),
            "message_id": route.get("request_message_id", ""),
            "correlation_id": route.get("correlation_id", ""),
            "sender_id": route.get("sender_id", ""),
            "session_id": route.get("session_id", ""),
            "reasoning": reasoning,
            "answer": answer,
            "details": list(details or []),
            "timestamp_ms": int(time.time() * 1000),
        }
        await self._producer.send_and_wait(
            topic,
            json.dumps(envelope, ensure_ascii=False).encode("utf-8"),
            key=key_value.encode("utf-8") if key_value else None,
            headers=[("content-type", b"application/json")],
        )

    async def send(
        self,
        to_handle: str,
        text: str,
        meta: dict[str, Any] | None = None,
    ) -> None:
        """Publish non-turn messages (errors, denials) as one response JSON."""
        del to_handle
        await self._publish_response(meta, "", text, [])

    async def send_media(
        self,
        to_handle: str,
        part: OutgoingContentPart,
        meta: dict[str, Any] | None = None,
    ) -> None:
        part_type = str(getattr(part, "type", "file"))
        url = (
            getattr(part, "image_url", "")
            or getattr(part, "video_url", "")
            or getattr(part, "file_url", "")
            or getattr(part, "file_id", "")
        )
        await self.send(to_handle, f"[{part_type}] {url}".strip(), meta)

    async def health_check(self) -> dict[str, Any]:
        if not self.enabled:
            return {
                "channel": self.channel,
                "status": "disabled",
                "detail": "Kafka channel is disabled.",
            }
        task_ok = self._consume_task is not None and not self._consume_task.done()
        if self._connected and task_ok:
            return {
                "channel": self.channel,
                "status": "healthy",
                "detail": (
                    f"Kafka consumer is running for {self.input_topic} "
                    f"with group {self.group_id}."
                ),
            }
        return {
            "channel": self.channel,
            "status": "unhealthy",
            "detail": "Kafka consumer is not running.",
        }
