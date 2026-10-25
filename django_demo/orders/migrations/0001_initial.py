import uuid
import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True
    dependencies = []
    operations = [
        migrations.CreateModel(name="Order", fields=[
            ("id", models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
            ("customer", models.CharField(max_length=120)),
            ("total", models.DecimalField(decimal_places=2, max_digits=10)),
            ("created_at", models.DateTimeField(auto_now_add=True)),
        ]),
        migrations.CreateModel(name="InboxEvent", fields=[
            ("event_id", models.UUIDField(primary_key=True, serialize=False)),
            ("payload_hash", models.CharField(max_length=64)),
            ("received_at", models.DateTimeField(auto_now_add=True)),
        ]),
        migrations.CreateModel(name="OrderProjection", fields=[
            ("order_id", models.UUIDField(primary_key=True, serialize=False)),
            ("customer", models.CharField(max_length=120)),
            ("total", models.DecimalField(decimal_places=2, max_digits=10)),
            ("source_event_id", models.UUIDField(unique=True)),
            ("updated_at", models.DateTimeField(auto_now=True)),
        ]),
        migrations.CreateModel(name="OutboxEvent", fields=[
            ("event_id", models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
            ("topic", models.CharField(max_length=249)),
            ("payload", models.JSONField()),
            ("created_at", models.DateTimeField(auto_now_add=True)),
            ("published_at", models.DateTimeField(blank=True, db_index=True, null=True)),
            ("order", models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to="orders.order")),
        ]),
    ]
