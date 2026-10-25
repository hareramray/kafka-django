"""The database/Kafka boundary, with explicit failure windows for the lessons."""
import hashlib
import json
import re
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation
from uuid import UUID, uuid4

from django.conf import settings
from django.db import transaction
from django.utils import timezone
from labtools.kafka import producer
from .models import InboxEvent, Order, OrderProjection, OutboxEvent


def validate_input(customer, total):
    if not isinstance(customer, str) or not customer.strip() or len(customer.strip()) > 120:
        raise ValueError("customer must contain 1-120 characters")
    if not isinstance(total, str):
        raise ValueError('total must be a decimal string, for example "19.95"')
    try:
        amount = Decimal(total)
        if not amount.is_finite() or not Decimal("0") < amount <= Decimal("99999999.99"):
            raise ValueError("total must be positive and at most 99999999.99")
        if amount != amount.quantize(Decimal("0.01")):
            raise ValueError("total may have at most two decimal places")
    except InvalidOperation as exc:
        raise ValueError("total is not a valid money amount") from exc
    return customer.strip(), amount.quantize(Decimal("0.01"))


def validate_topic(topic):
    if not isinstance(topic, str) or len(topic) > 249 or not re.fullmatch(r"lab\.[A-Za-z0-9._-]+", topic):
        raise ValueError("this workshop only publishes to topics beginning with lab.")
    return topic


def order_event(order):
    return {
        "event_id": str(uuid4()),
        "event_type": "order.created",
        "schema_version": 1,
        "occurred_at": timezone.now().isoformat(),
        "data": {"order_id": str(order.id), "customer": order.customer, "total": str(order.total)},
    }


def validate_event(event):
    if not isinstance(event, dict):
        raise ValueError("event must be a JSON object")
    try:
        if event["event_type"] != "order.created" or type(event["schema_version"]) is not int or event["schema_version"] != 1:
            raise ValueError("unsupported event type or schema version")
        UUID(event["event_id"])
        timestamp = datetime.fromisoformat(event["occurred_at"])
        if timestamp.tzinfo is None:
            raise ValueError("occurred_at must contain a timezone")
        data = event["data"]
        UUID(data["order_id"])
        validate_input(data["customer"], data["total"])
    except (KeyError, TypeError, AttributeError) as exc:
        raise ValueError("event is missing required fields or contains invalid field types") from exc
    return event


def publish_event(event, topic):
    """Return only after the broker acknowledges delivery, or raise."""
    validate_event(event)
    validate_topic(topic)
    kafka_producer = producer()
    errors = []
    delivered = []

    def delivery(error, message):
        if error:
            errors.append(str(error))
        else:
            delivered.append((message.partition(), message.offset()))

    kafka_producer.produce(
        topic, key=event["data"]["order_id"].encode(),
        value=json.dumps(event, sort_keys=True).encode(), on_delivery=delivery,
    )
    pending = kafka_producer.flush(15)
    if pending or errors or not delivered:
        raise RuntimeError(f"Kafka delivery not acknowledged: pending={pending}, errors={errors}")
    return delivered[0]


def create_order_with_outbox(customer, total, *, topic=None):
    customer, total = validate_input(customer, total)
    topic = validate_topic(topic or settings.KAFKA_ORDERS_TOPIC)
    with transaction.atomic():
        order = Order.objects.create(customer=customer, total=total)
        event = order_event(order)
        OutboxEvent.objects.create(event_id=event["event_id"], order=order, topic=topic, payload=event)
    return order, event


def create_order_after_commit(customer, total, *, topic=None):
    """Teaching contrast: a callback is not durable storage for an event."""
    customer, total = validate_input(customer, total)
    topic = validate_topic(topic or settings.KAFKA_ORDERS_TOPIC)
    with transaction.atomic():
        order = Order.objects.create(customer=customer, total=total)
        event = order_event(order)
        transaction.on_commit(lambda: publish_event(event, topic))
    return order, event


def publish_pending(*, limit=100, timeout=30, topic=None):
    """Single-publisher SQLite lab implementation; acknowledge, then mark sent.

    The time budget is checked BETWEEN records; each delivery can take 15 seconds.
    A process crash after delivery but before update deliberately permits replay.
    """
    if limit < 1 or timeout <= 0:
        raise ValueError("limit and timeout must be positive")
    rows = OutboxEvent.objects.filter(published_at__isnull=True).order_by("created_at", "event_id")
    if topic is not None:
        rows = rows.filter(topic=validate_topic(topic))
    ids = list(rows.values_list("event_id", flat=True)[:limit])
    deadline = time.monotonic() + timeout
    sent = 0
    for event_id in ids:
        if time.monotonic() >= deadline:
            break
        row = OutboxEvent.objects.get(event_id=event_id)
        if row.published_at is not None:
            continue
        publish_event(row.payload, row.topic)
        OutboxEvent.objects.filter(event_id=event_id, published_at__isnull=True).update(published_at=timezone.now())
        sent += 1
    return sent


def apply_event(event):
    """Atomically persist both the deduplication receipt and the projection."""
    validate_event(event)
    fingerprint = hashlib.sha256(json.dumps(event, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    data = event["data"]
    with transaction.atomic():
        receipt, created = InboxEvent.objects.get_or_create(
            event_id=event["event_id"], defaults={"payload_hash": fingerprint},
        )
        if not created:
            if receipt.payload_hash != fingerprint:
                raise ValueError("an existing event_id was reused with different content")
            return False
        OrderProjection.objects.create(
            order_id=data["order_id"], customer=data["customer"], total=Decimal(data["total"]),
            source_event_id=event["event_id"],
        )
    return True
