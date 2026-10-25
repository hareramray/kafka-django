# Django order pipeline

This localhost teaching app supports notebooks 11–14. The notebooks create separate databases
under `.lab-data/`; commands use `django_demo/db.sqlite3` unless `DJANGO_DB_PATH` is set.

From the repository root, with the workshop Python environment active:

```text
python django_demo/manage.py migrate
python django_demo/manage.py test orders
python django_demo/manage.py runserver 127.0.0.1:8000 --noreload
```

Create the Kafka destination explicitly and set `KAFKA_ORDERS_TOPIC` before starting a server.
The topic must begin with `lab.`. Default: `lab.django.orders` (not automatically created).

| Route | Behavior |
| --- | --- |
| `GET /health/` | Checks the app responds; does not check Kafka readiness. |
| `POST /orders/` | Atomically creates an order and an outbox event. |
| `POST /orders/on-commit/` | Demonstrates a callback after database commit and its lost-event window. |
| `GET /projections/<order UUID>/` | Reads the materialized view; returns 404 until projected. |

Both POST routes accept `Content-Type: application/json` and a body like
`{"customer": "Ada", "total": "19.95"}`. Money is a positive decimal string with at most
two decimal places. The API returns order and event UUIDs. This demo has no HTTP request
idempotency key: retrying an accepted POST creates another order.

Worker commands are separate from the web process and always finite:

```text
python django_demo/manage.py publish_outbox --topic lab.django.orders --max-messages 100 --timeout 30
python django_demo/manage.py consume_orders --topic lab.django.orders --group lab.django.projections --max-messages 100 --timeout 30
```

The publisher marks rows sent only after Kafka acknowledgement. Its timeout is checked between
records; an individual delivery may take another 15 seconds. Run one publisher at a time with
this SQLite implementation. A crash after acknowledgement and before marking permits duplicate
delivery. The consumer writes its deduplication receipt and projection in one database transaction,
then commits the Kafka offset. It rejects a reused event ID with changed content. Invalid events
stop consumption without committing their offsets; notebook 08 covers a DLQ alternative.

The notebooks use Django's HTTP test client to exercise real routing/views without starting a
server. They install `%%django_sync` to run their visible synchronous Python cells on one dedicated
thread, preserving Django's async safety in Jupyter kernels that keep an event loop running even
after `%autoawait off`. Restart the kernel between Django notebooks.
If you launch a separate server or worker, use the same `DJANGO_DB_PATH` to see the same records.

These settings and unauthenticated, CSRF-exempt JSON endpoints are for localhost teaching. A
deployment needs an appropriate security model, request idempotency, durable infrastructure,
publisher coordination, event/receipt retention, observability, and recovery procedures.
