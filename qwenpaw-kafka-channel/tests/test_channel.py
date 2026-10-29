"""Unit tests that do not require a running Kafka broker."""

import asyncio
import importlib.util
import json
import sys
import unittest
from collections import namedtuple
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace

PLUGIN_DIR = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "qwenpaw_kafka_channel_test_channel",
    PLUGIN_DIR / "channel.py",
)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)
KafkaChannel = MODULE.KafkaChannel
FakeTopicPartition = namedtuple("FakeTopicPartition", "topic partition")


def make_channel() -> KafkaChannel:
    channel = object.__new__(KafkaChannel)
    channel.channel = "kafka"
    channel.text_field = "text"
    channel.reply_topic_field = "reply_to"
    channel.sender_id_field = "sender_id"
    channel.session_id_field = "session_id"
    channel.message_id_field = "message_id"
    channel.correlation_id_field = "correlation_id"
    channel.output_topic = "responses.{sender_id}"
    channel.publish_to_downstream = True
    channel.raw_input_output = False
    return channel


def make_split_cluster_channel() -> KafkaChannel:
    channel = make_channel()
    channel.input_bootstrap_servers = "input-1:9092,input-2:9092"
    channel.input_security_protocol = "SASL_PLAINTEXT"
    channel.input_sasl_mechanism = "SCRAM-SHA-256"
    channel.input_sasl_username = "input-user"
    channel.input_sasl_password = "input-password"
    channel.input_ssl_cafile = ""
    channel.input_ssl_certfile = ""
    channel.input_ssl_keyfile = ""
    channel.output_bootstrap_servers = "output-1:9093"
    channel.output_security_protocol = "PLAINTEXT"
    channel.output_sasl_mechanism = "PLAIN"
    channel.output_sasl_username = ""
    channel.output_sasl_password = ""
    channel.output_ssl_cafile = ""
    channel.output_ssl_certfile = ""
    channel.output_ssl_keyfile = ""
    return channel


class KafkaChannelParsingTests(unittest.TestCase):
    def test_raw_mode_passes_the_record_value_directly_to_agent(self):
        channel = make_channel()
        channel.raw_input_output = True
        record = SimpleNamespace(
            topic="requests",
            partition=1,
            offset=8,
            key=b"raw-user",
            headers=[],
            value=b"  who are you?  ",
        )

        native = channel._record_to_native(record)

        self.assertEqual(native["content_parts"][0].text, "  who are you?  ")
        self.assertEqual(native["meta"]["reply_topic"], "")
        self.assertEqual(native["meta"]["request_message_id"], "requests:1:8")
        self.assertTrue(native["session_id"].startswith("kafka:raw:"))

        second = channel._record_to_native(record)
        self.assertNotEqual(native["session_id"], second["session_id"])

    def test_json_record_is_mapped_to_native_payload(self):
        channel = make_channel()
        record = SimpleNamespace(
            topic="requests",
            partition=2,
            offset=9,
            key=b"fallback-user",
            headers=[("trace", b"abc")],
            value=json.dumps(
                {
                    "message_id": "m-1",
                    "correlation_id": "c-1",
                    "sender_id": "user-7",
                    "session_id": "ticket-9",
                    "reply_to": "dynamic.responses",
                    "text": "hello",
                }
            ).encode(),
        )

        native = channel._record_to_native(record)

        self.assertEqual(native["sender_id"], "user-7")
        self.assertEqual(native["session_id"], "kafka:ticket-9")
        self.assertEqual(native["meta"]["request_message_id"], "m-1")
        self.assertEqual(native["meta"]["correlation_id"], "c-1")
        self.assertEqual(native["meta"]["reply_topic"], "dynamic.responses")
        self.assertEqual(native["content_parts"][0].text, "hello")

    def test_plain_text_uses_kafka_position_as_message_id(self):
        channel = make_channel()
        record = SimpleNamespace(
            topic="requests",
            partition=0,
            offset=3,
            key=b"device-1",
            headers=[],
            value=b"plain request",
        )

        native = channel._record_to_native(record)

        self.assertEqual(native["sender_id"], "device-1")
        self.assertEqual(native["session_id"], "kafka:device-1")
        self.assertEqual(native["meta"]["request_message_id"], "requests:0:3")
        self.assertEqual(native["content_parts"][0].text, "plain request")

    def test_output_topic_supports_placeholders_and_dynamic_override(self):
        channel = make_channel()
        self.assertEqual(
            channel._render_output_topic({"sender_id": "user-1"}),
            "responses.user-1",
        )
        self.assertEqual(
            channel._render_output_topic(
                {"reply_topic": "special.responses", "sender_id": "user-1"}
            ),
            "special.responses",
        )


class FakeProducer:
    def __init__(self):
        self.calls = []

    async def send_and_wait(self, topic, value, **kwargs):
        self.calls.append((topic, value, kwargs))


class FakeConsumer:
    def __init__(self):
        self.commits = []

    async def commit(self, offsets):
        self.commits.append(offsets)


class FakeStartedConsumer:
    instances = []

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.started = False
        self.stopped = False
        self.__class__.instances.append(self)

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration


class KafkaChannelSendingTests(unittest.IsolatedAsyncioTestCase):
    async def test_disabled_downstream_does_not_publish(self):
        channel = make_channel()
        channel.publish_to_downstream = False
        channel._producer = None

        await channel.send("ignored", "agent reply", {"sender_id": "user-1"})

        self.assertIsNone(channel._producer)

    async def test_raw_mode_publishes_only_the_final_answer(self):
        channel = make_channel()
        channel.raw_input_output = True
        channel._producer = FakeProducer()

        await channel.send("ignored", "我是 QwenPaw", {"sender_id": "user-1"})

        topic, value, kwargs = channel._producer.calls[0]
        self.assertEqual(topic, "responses.user-1")
        self.assertEqual(value, "我是 QwenPaw".encode("utf-8"))
        self.assertEqual(
            kwargs["headers"],
            [("content-type", b"text/plain; charset=utf-8")],
        )

    async def test_send_publishes_correlated_json_envelope(self):
        channel = make_channel()
        channel._producer = FakeProducer()

        await channel.send(
            "ignored",
            "agent reply",
            {
                "request_message_id": "m-1",
                "correlation_id": "c-1",
                "sender_id": "user-1",
                "session_id": "kafka:s-1",
            },
        )

        self.assertEqual(len(channel._producer.calls), 1)
        topic, value, kwargs = channel._producer.calls[0]
        envelope = json.loads(value)
        self.assertEqual(topic, "responses.user-1")
        self.assertEqual(envelope["message_id"], "m-1")
        self.assertEqual(envelope["correlation_id"], "c-1")
        self.assertEqual(envelope["reasoning"], "")
        self.assertEqual(envelope["answer"], "agent reply")
        self.assertEqual(envelope["details"], [])
        self.assertEqual(kwargs["key"], b"c-1")

    async def test_reasoning_and_answer_are_published_as_one_record(self):
        channel = make_channel()
        channel._producer = FakeProducer()
        channel._message_to_content_parts = lambda event: [
            MODULE.TextContent(type=MODULE.ContentType.TEXT, text=event.text)
        ]
        meta = {
            "request_message_id": "m-2",
            "correlation_id": "c-2",
            "sender_id": "user-2",
            "session_id": "kafka:s-2",
        }

        await channel.on_event_message_completed(
            None,
            "ignored",
            SimpleNamespace(type="reasoning", text="internal reasoning"),
            meta,
        )
        await channel.on_event_message_completed(
            None,
            "ignored",
            SimpleNamespace(type="message", text="final answer"),
            meta,
        )
        await channel._on_process_completed(None, "ignored", meta)

        self.assertEqual(len(channel._producer.calls), 1)
        _, value, _ = channel._producer.calls[0]
        envelope = json.loads(value)
        self.assertEqual(envelope["reasoning"], "internal reasoning")
        self.assertEqual(envelope["answer"], "final answer")


class KafkaChannelLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_disabled_downstream_starts_only_the_upstream_consumer(self):
        FakeStartedConsumer.instances.clear()

        def fail_if_producer_is_created(*args, **kwargs):
            raise AssertionError("downstream producer must not be created")

        channel = KafkaChannel.from_config(
            lambda request: None,
            {
                "enabled": True,
                "producer_bootstrap_servers": "request:9092",
                "input_topic": "requests",
                "qwenpaw_consumer_group": "agent-group",
                "publish_to_downstream": False,
            },
        )
        fake_aiokafka = SimpleNamespace(
            AIOKafkaConsumer=FakeStartedConsumer,
            AIOKafkaProducer=fail_if_producer_is_created,
        )

        with patch.dict(sys.modules, {"aiokafka": fake_aiokafka}):
            await channel.start()
            await asyncio.sleep(0)
            await channel.stop()

        self.assertEqual(len(FakeStartedConsumer.instances), 1)
        self.assertTrue(FakeStartedConsumer.instances[0].started)
        self.assertTrue(FakeStartedConsumer.instances[0].stopped)
        self.assertIsNone(channel._producer)


class KafkaConcurrentCommitTests(unittest.IsolatedAsyncioTestCase):
    async def test_later_completion_does_not_commit_past_earlier_record(self):
        channel = make_channel()
        channel._consumer = FakeConsumer()
        channel._commit_lock = asyncio.Lock()
        channel._partition_progress = {}
        first = SimpleNamespace(topic="requests", partition=0, offset=10)
        second = SimpleNamespace(topic="requests", partition=0, offset=11)
        channel._register_record(first)
        channel._register_record(second)

        fake_aiokafka = SimpleNamespace(TopicPartition=FakeTopicPartition)
        with patch.dict(sys.modules, {"aiokafka": fake_aiokafka}):
            await channel._mark_record_complete(second)
            self.assertEqual(channel._consumer.commits, [])
            await channel._mark_record_complete(first)
        self.assertEqual(len(channel._consumer.commits), 1)
        offsets = channel._consumer.commits[0]
        self.assertEqual(next(iter(offsets.values())), 12)

    async def test_different_partitions_can_commit_independently(self):
        channel = make_channel()
        channel._consumer = FakeConsumer()
        channel._commit_lock = asyncio.Lock()
        channel._partition_progress = {}
        partition_zero = SimpleNamespace(topic="requests", partition=0, offset=5)
        partition_one = SimpleNamespace(topic="requests", partition=1, offset=20)
        channel._register_record(partition_zero)
        channel._register_record(partition_one)

        fake_aiokafka = SimpleNamespace(TopicPartition=FakeTopicPartition)
        with patch.dict(sys.modules, {"aiokafka": fake_aiokafka}):
            await channel._mark_record_complete(partition_one)

        self.assertEqual(len(channel._consumer.commits), 1)
        offsets = channel._consumer.commits[0]
        topic_partition, committed_offset = next(iter(offsets.items()))
        self.assertEqual(topic_partition.partition, 1)
        self.assertEqual(committed_offset, 21)


class KafkaSplitClusterConfigTests(unittest.TestCase):
    def test_consumer_and_producer_use_independent_connections(self):
        channel = make_split_cluster_channel()

        consumer = channel._consumer_connection_kwargs()
        producer = channel._producer_connection_kwargs()

        self.assertEqual(
            consumer["bootstrap_servers"], ["input-1:9092", "input-2:9092"]
        )
        self.assertEqual(consumer["security_protocol"], "SASL_PLAINTEXT")
        self.assertEqual(consumer["sasl_plain_username"], "input-user")
        self.assertEqual(producer["bootstrap_servers"], ["output-1:9093"])
        self.assertEqual(producer["security_protocol"], "PLAINTEXT")
        self.assertNotIn("sasl_plain_username", producer)

    def test_external_role_config_maps_producer_to_input(self):
        channel = KafkaChannel.from_config(
            lambda request: None,
            {
                "enabled": True,
                "producer_bootstrap_servers": "request:9092",
                "input_topic": "requests",
                "qwenpaw_consumer_group": "agent-group",
                "consumer_bootstrap_servers": "response:9092",
                "output_topic": "responses",
            },
        )

        self.assertEqual(channel.input_bootstrap_servers, "request:9092")
        self.assertEqual(channel.output_bootstrap_servers, "response:9092")
        self.assertEqual(channel.group_id, "agent-group")
        self.assertTrue(channel.raw_input_output)
        self.assertTrue(channel.publish_to_downstream)
        self.assertEqual(channel.max_concurrent_messages, 5)

    def test_downstream_can_be_disabled_without_output_config(self):
        channel = KafkaChannel.from_config(
            lambda request: None,
            {
                "enabled": True,
                "producer_bootstrap_servers": "request:9092",
                "input_topic": "requests",
                "qwenpaw_consumer_group": "agent-group",
                "publish_to_downstream": False,
            },
        )

        channel._validate_config()
        self.assertFalse(channel.publish_to_downstream)
        self.assertEqual(channel.output_bootstrap_servers, "")
        self.assertEqual(channel.output_topic, "")

    def test_max_concurrency_can_be_configured(self):
        channel = KafkaChannel.from_config(
            lambda request: None,
            {
                "enabled": True,
                "producer_bootstrap_servers": "request:9092",
                "input_topic": "requests",
                "qwenpaw_consumer_group": "agent-group",
                "consumer_bootstrap_servers": "response:9092",
                "output_topic": "responses",
                "max_concurrent_messages": 8,
            },
        )

        self.assertEqual(channel.max_concurrent_messages, 8)

    def test_legacy_internal_role_config_is_migrated_safely(self):
        channel = KafkaChannel.from_config(
            lambda request: None,
            {
                "enabled": True,
                "consumer_bootstrap_servers": "legacy-request:9092",
                "input_topic": "requests",
                "consumer_group_id": "legacy-group",
                "producer_bootstrap_servers": "legacy-response:9092",
                "output_topic": "responses",
            },
        )

        self.assertEqual(channel.input_bootstrap_servers, "legacy-request:9092")
        self.assertEqual(channel.output_bootstrap_servers, "legacy-response:9092")
        self.assertEqual(channel.group_id, "legacy-group")


if __name__ == "__main__":
    unittest.main()
