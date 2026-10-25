import copy
import json
from decimal import Decimal
from unittest.mock import MagicMock, patch
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import transaction
from django.test import TestCase, TransactionTestCase, override_settings
from .models import InboxEvent, Order, OrderProjection, OutboxEvent
from .services import apply_event, create_order_after_commit, create_order_with_outbox, publish_pending, validate_event


class OutboxAndProjectionTests(TestCase):
    def setUp(self):
        self.order, self.event = create_order_with_outbox("Ada", "19.95")

    def test_order_and_outbox_roll_back_together(self):
        before = (Order.objects.count(), OutboxEvent.objects.count())
        with self.assertRaises(RuntimeError):
            with transaction.atomic():
                create_order_with_outbox("Rollback", "3.00")
                raise RuntimeError("simulated failure")
        self.assertEqual(before, (Order.objects.count(), OutboxEvent.objects.count()))

    def test_outbox_insert_failure_rolls_back_order(self):
        with patch("orders.services.OutboxEvent.objects.create", side_effect=RuntimeError("disk full")):
            with self.assertRaises(RuntimeError):
                create_order_with_outbox("Not saved", "1.00")
        self.assertEqual(Order.objects.count(), 1)

    def test_ack_failure_leaves_event_pending(self):
        with patch("orders.services.publish_event", side_effect=RuntimeError("no ack")):
            with self.assertRaises(RuntimeError):
                publish_pending()
        self.assertIsNone(OutboxEvent.objects.get().published_at)

    def test_ack_success_marks_event_and_next_batch_does_not_republish(self):
        with patch("orders.services.publish_event") as send:
            self.assertEqual(publish_pending(), 1)
            self.assertEqual(publish_pending(), 0)
            send.assert_called_once()
        self.assertIsNotNone(OutboxEvent.objects.get().published_at)

    def test_duplicate_has_one_durable_effect(self):
        self.assertTrue(apply_event(self.event))
        self.assertFalse(apply_event(copy.deepcopy(self.event)))
        self.assertEqual(InboxEvent.objects.count(), 1)
        self.assertEqual(OrderProjection.objects.get().total, Decimal("19.95"))

    def test_same_id_changed_payload_is_rejected(self):
        apply_event(self.event)
        changed = copy.deepcopy(self.event)
        changed["data"]["total"] = "29.95"
        with self.assertRaises(ValueError):
            apply_event(changed)
        self.assertEqual(OrderProjection.objects.get().total, Decimal("19.95"))

    def test_failed_projection_does_not_keep_receipt(self):
        with patch("orders.services.OrderProjection.objects.create", side_effect=RuntimeError("disk full")):
            with self.assertRaises(RuntimeError):
                apply_event(self.event)
        self.assertFalse(InboxEvent.objects.exists())

    def test_schema_validation_precedes_writes(self):
        for field, value in [("schema_version", 2), ("event_id", "bad-id"), ("data", None)]:
            invalid = copy.deepcopy(self.event)
            invalid[field] = value
            with self.assertRaises(ValueError):
                apply_event(invalid)
        self.assertFalse(InboxEvent.objects.exists())

    def test_input_rejects_non_finite_negative_and_float_money(self):
        for amount in ["NaN", "Infinity", "-1", "0", "1.001", 1.5, "100000000.00"]:
            with self.assertRaises(ValueError):
                create_order_with_outbox("Invalid", amount)

    @override_settings(KAFKA_ORDERS_TOPIC="lab.test.api")
    def test_http_api_enqueues_without_contacting_kafka(self):
        with patch("orders.services.publish_event") as send:
            response = self.client.post("/orders/", {"customer": "Grace", "total": "2.50"}, content_type="application/json")
            self.assertEqual(response.status_code, 201)
            send.assert_not_called()
        row = OutboxEvent.objects.get(event_id=response.json()["event_id"])
        self.assertEqual(row.topic, "lab.test.api")
        self.assertEqual(self.client.get(f'/projections/{response.json()["order_id"]}/').status_code, 404)
        apply_event(row.payload)
        self.assertEqual(self.client.get(f'/projections/{response.json()["order_id"]}/').status_code, 200)

    def test_http_api_rejects_bad_payload(self):
        for body in [[], {"customer": "", "total": "1"}, {"customer": "Ada", "total": 2.5}]:
            response = self.client.post("/orders/", body, content_type="application/json")
            self.assertEqual(response.status_code, 400)

    def test_consumer_commits_after_projection_and_closes(self):
        message = MagicMock()
        message.error.return_value = None
        message.value.return_value = json.dumps(self.event).encode()
        client = MagicMock()
        client.poll.return_value = message

        def inspect_commit(**kwargs):
            self.assertTrue(InboxEvent.objects.filter(event_id=self.event["event_id"]).exists())
            self.assertTrue(OrderProjection.objects.filter(order_id=self.order.id).exists())
            self.assertIs(kwargs["message"], message)
            self.assertFalse(kwargs["asynchronous"])

        client.commit.side_effect = inspect_commit
        with patch("orders.management.commands.consume_orders.consumer", return_value=client):
            call_command("consume_orders", topic="lab.test.consumer", max_messages=1, timeout=1)
        client.commit.assert_called_once()
        client.close.assert_called_once()

    def test_poison_record_does_not_commit_and_closes(self):
        message = MagicMock()
        message.error.return_value = None
        message.value.return_value = b'{"schema_version":999}'
        client = MagicMock()
        client.poll.return_value = message
        with patch("orders.management.commands.consume_orders.consumer", return_value=client):
            with self.assertRaises(CommandError):
                call_command("consume_orders", topic="lab.test.consumer", max_messages=1, timeout=1)
        client.commit.assert_not_called()
        client.close.assert_called_once()
        self.assertFalse(InboxEvent.objects.exists())

    def test_database_failure_does_not_commit_source_offset(self):
        message = MagicMock()
        message.error.return_value = None
        message.value.return_value = json.dumps(self.event).encode()
        client = MagicMock()
        client.poll.return_value = message
        with patch("orders.management.commands.consume_orders.consumer", return_value=client):
            with patch("orders.services.OrderProjection.objects.create", side_effect=RuntimeError("database unavailable")):
                with self.assertRaises(RuntimeError):
                    call_command("consume_orders", topic="lab.test.consumer", max_messages=1, timeout=1)
        client.commit.assert_not_called()
        client.close.assert_called_once()
        self.assertFalse(InboxEvent.objects.exists())


class OnCommitTests(TransactionTestCase):
    def test_callback_runs_only_after_commit(self):
        with patch("orders.services.publish_event") as send:
            with transaction.atomic():
                create_order_after_commit("Deferred", "1.00")
                send.assert_not_called()
            send.assert_called_once()

    def test_rollback_discards_callback(self):
        with patch("orders.services.publish_event") as send:
            with self.assertRaises(RuntimeError):
                with transaction.atomic():
                    create_order_after_commit("Rolled back", "1.00")
                    raise RuntimeError("rollback")
            send.assert_not_called()
        self.assertFalse(Order.objects.exists())

    def test_callback_failure_does_not_undo_committed_order(self):
        with patch("orders.services.publish_event", side_effect=RuntimeError("broker down")):
            with self.assertRaises(RuntimeError):
                create_order_after_commit("Committed", "1.00")
        self.assertEqual(Order.objects.count(), 1)
        self.assertFalse(OutboxEvent.objects.exists())
