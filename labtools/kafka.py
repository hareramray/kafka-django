"""Finite Kafka operations. Lessons also show the underlying APIs directly.

Never swallow failed deliveries or silently commit a partially processed batch.
All consumer clients are owned by their caller, except collect(), which closes its own.
"""
from collections.abc import Iterable, Mapping
import json
import os
from pathlib import Path
import re
from time import monotonic, sleep
from uuid import uuid4

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env", override=False)


def bootstrap_servers():
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def client_config(extra=None):
    config = {"bootstrap.servers": bootstrap_servers(), "client.id": "python-workshop",
              "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")}
    for env, setting in {
        "KAFKA_SASL_MECHANISM": "sasl.mechanism",
        "KAFKA_SASL_USERNAME": "sasl.username",
        "KAFKA_SASL_PASSWORD": "sasl.password",
        "KAFKA_SSL_CA_LOCATION": "ssl.ca.location",
        "KAFKA_SSL_CERTIFICATE_LOCATION": "ssl.certificate.location",
        "KAFKA_SSL_KEY_LOCATION": "ssl.key.location",
    }.items():
        if os.getenv(env):
            config[setting] = os.environ[env]
    config.update(extra or {})
    return config


def require_broker(timeout=10):
    """Fail with a useful setup message before running a lesson."""
    try:
        metadata = AdminClient(client_config()).list_topics(timeout=timeout)
    except KafkaException as exc:
        raise RuntimeError(
            "Kafka is unreachable. Start Docker Desktop (Linux containers), run "
            "'docker compose up -d --wait', then check .env and advertised listeners."
        ) from exc
    if not metadata.brokers:
        raise RuntimeError("Kafka returned no brokers.")
    return metadata


def new_topic(label, partitions=3, config=None, replication_factor=1):
    """Create a fresh topic, so Restart Kernel + Run All has independent data."""
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._-]{0,100}", label):
        raise ValueError("Use a short alphanumeric topic label, with dots, dashes or underscores.")
    if partitions < 1 or replication_factor < 1:
        raise ValueError("Partitions and replication factor must be positive.")
    name = f"lab.{label}.{uuid4().hex[:10]}"
    admin = AdminClient(client_config())
    futures = admin.create_topics([NewTopic(name, num_partitions=partitions,
                               replication_factor=replication_factor, config=config or {})],
                               request_timeout=15, operation_timeout=10)
    futures[name].result(timeout=20)
    deadline = monotonic() + 15
    while monotonic() < deadline:
        meta = admin.list_topics(topic=name, timeout=5).topics.get(name)
        if meta and not meta.error and len(meta.partitions) == partitions and all(
            part.leader >= 0 for part in meta.partitions.values()
        ):
            return name
        sleep(0.2)
    raise TimeoutError(f"Topic {name} was created but leaders are not ready yet.")


def producer(**overrides):
    config = {"enable.idempotence": True, "acks": "all", "delivery.timeout.ms": 10000}
    config.update(overrides)
    return Producer(client_config(config))


def consumer(group_id, **overrides):
    config = {"group.id": group_id, "enable.auto.commit": False,
              "enable.auto.offset.store": False, "auto.offset.reset": "earliest",
              "allow.auto.create.topics": False}
    config.update(overrides)
    return Consumer(client_config(config))


def publish(topic, values: Iterable[Mapping], key_field=None):
    """Publish JSON and return broker delivery reports; an enqueue is not an ACK."""
    p = producer()
    delivered, failures = [], []

    def report(error, message):
        if error:
            failures.append(str(error))
        else:
            delivered.append({"topic": message.topic(), "partition": message.partition(),
                              "offset": message.offset()})

    try:
        for value in values:
            payload = json.dumps(value, allow_nan=False).encode("utf-8")
            key = str(value[key_field]).encode("utf-8") if key_field else None
            deadline = monotonic() + 15
            while True:
                try:
                    p.produce(topic, key=key, value=payload, on_delivery=report)
                    break
                except BufferError:
                    if monotonic() >= deadline:
                        raise TimeoutError("Producer queue is still full after 15 seconds.")
                    p.poll(0.1)
            p.poll(0)
    finally:
        remaining = p.flush(15)
    if failures or remaining:
        raise RuntimeError(f"Publish failed: {len(failures)} delivery error(s), {remaining} pending. {failures[:3]}")
    return delivered


def poll_records(c, *, count, timeout=20):
    """Read exactly count messages or raise. Does not commit or close c."""
    if count < 0 or timeout <= 0:
        raise ValueError("count must be nonnegative and timeout must be positive")
    messages = []
    deadline = monotonic() + timeout
    while len(messages) < count and monotonic() < deadline:
        message = c.poll(min(0.5, max(0, deadline - monotonic())))
        if message is None:
            continue
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                continue
            raise KafkaException(message.error())
        messages.append(message)
    if len(messages) != count:
        raise TimeoutError(f"Expected {count} records, received {len(messages)} in {timeout}s. "
                           "Check topic, group offsets, broker health and transaction isolation.")
    return messages


def collect(topic, *, count, group_id=None, timeout=20, config=None, commit=False):
    """Convenience reader. commit=True is for inspections, not external side effects."""
    c = consumer(group_id or f"lab.reader.{uuid4().hex}", **(config or {}))
    try:
        c.subscribe([topic])
        messages = poll_records(c, count=count, timeout=timeout)
        if commit:
            # commit(message) commits only that message's partition at offset+1.
            for message in messages:
                c.commit(message=message, asynchronous=False)
        return messages
    finally:
        c.close()


def decode(message):
    return None if message.value() is None else json.loads(message.value().decode("utf-8"))
