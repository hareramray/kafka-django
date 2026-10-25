import uuid
from django.db import models


class Order(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    customer = models.CharField(max_length=120)
    total = models.DecimalField(max_digits=10, decimal_places=2)
    created_at = models.DateTimeField(auto_now_add=True)


class OutboxEvent(models.Model):
    event_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    order = models.ForeignKey(Order, on_delete=models.PROTECT)
    topic = models.CharField(max_length=249)
    payload = models.JSONField()
    created_at = models.DateTimeField(auto_now_add=True)
    published_at = models.DateTimeField(null=True, blank=True, db_index=True)


class InboxEvent(models.Model):
    event_id = models.UUIDField(primary_key=True)
    payload_hash = models.CharField(max_length=64)
    received_at = models.DateTimeField(auto_now_add=True)


class OrderProjection(models.Model):
    order_id = models.UUIDField(primary_key=True)
    customer = models.CharField(max_length=120)
    total = models.DecimalField(max_digits=10, decimal_places=2)
    source_event_id = models.UUIDField(unique=True)
    updated_at = models.DateTimeField(auto_now=True)
