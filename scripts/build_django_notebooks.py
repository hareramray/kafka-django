"""Regenerate Django workshop notebooks. Run from the repository root."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from labtools.notebook_builder import BOOTSTRAP, code, md, write_notebook


DJANGO_SETUP = '''
import os
import django
from django.conf import settings

if settings.configured:
    raise RuntimeError("Restart the kernel before running this notebook again; Django settings are process-wide.")
DATA_DIR = ROOT / ".lab-data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / f"django-{LESSON}-{RUN}.sqlite3"
os.environ["DJANGO_DB_PATH"] = str(DB_PATH)
os.environ["DJANGO_SETTINGS_MODULE"] = "config.settings"
sys.path.insert(0, str(ROOT / "django_demo"))
django.setup()
from django.core.management import call_command
from django.db import transaction
from django.test import Client, override_settings
from django.utils import timezone
from decimal import Decimal
from unittest.mock import patch
from orders.models import Order, OutboxEvent, InboxEvent, OrderProjection
from orders.services import (
    order_event, validate_event, publish_event, apply_event,
    create_order_after_commit, create_order_with_outbox, publish_pending,
)
call_command("migrate", verbosity=0)
http = Client()
print("Isolated database:", DB_PATH.name)
'''


def setup(lesson):
    return code(f'LESSON = "{lesson}"\n' + DJANGO_SETUP)


def save(name, cells):
    # Jupyter kernels can retain an asyncio loop even after %autoawait off.
    # Keep lesson code visible while moving synchronous ORM work to one owner thread.
    support_note = md('''
    ### Jupyter and synchronous Django code

    Jupyter kernels may keep an event loop running even with `%autoawait off`. The next cell
    installs `%%django_sync`: it executes the visible Python body on one dedicated synchronous
    thread and waits for completion. All Django code cells below use that thread, so ORM
    connection ownership and transaction boundaries remain consistent. Variables stay in the
    notebook namespace, exceptions remain visible, and Django's async safety stays enabled.
    The magic adds no business logic. In a terminal, run the same Python bodies normally.
    ''')
    support_code = code('''
    %autoawait off
    sys.path.insert(0, str(ROOT / "django_demo"))
    from notebook_support import install_sync_magic
    DJANGO_EXECUTOR = install_sync_magic(get_ipython())
    print("Synchronous Django cell executor installed")
    ''')
    for cell in cells[2:]:
        if cell["cell_type"] == "code":
            cell["source"] = "%%django_sync\n" + cell["source"]
    cells[2:2] = [support_note, support_code]
    write_notebook(ROOT / "notebooks" / name, cells)


def main():
    save("11_django_http_and_on_commit.ipynb", [
        md('''
        # 11 · Django requests and Kafka events

        **Level:** intermediate · **Time:** 45–60 minutes · **Needs:** running Kafka, notebooks 01–04.

        Build an HTTP request → database → Kafka flow. Observe exactly when a database transaction
        commits, then reproduce the gap between committing an order and publishing its event.
        You will use the included Django application's real URL routing, views, and ORM through
        Django's HTTP test client; starting a web server is optional.

        **Run from top to bottom in a fresh kernel.** Each run creates its own database and topic.
        Do not run the Django notebooks in one shared kernel: Django settings are process-wide.
        ''') ,
        code(BOOTSTRAP),
        md('''
        ## Step 1 — Start Django inside Jupyter

        `%%django_sync` runs ORM operations without an active event loop on the database thread.
        We do not disable Django's async safety protection. Migrations create four tables:
        the original order, a durable outgoing event, an incoming event receipt, and a read model.
        This notebook initially uses only `Order`.
        ''') ,
        setup("11"),
        code('''
        from django.apps import apps
        print([model.__name__ for model in apps.get_models()])
        assert http.get("/health/").json()["status"] == "ok"
        '''),
        md('''
        ## Step 2 — Inspect the write boundary

        The service creates the order inside `transaction.atomic()`, then registers a function
        with `transaction.on_commit()`. The function runs after the outermost database transaction
        commits. It cannot roll that committed transaction back.
        ''') ,
        code('''
        import inspect
        print(inspect.getsource(create_order_after_commit))
        '''),
        md('''
        ## Step 3 — Prepare the destination explicitly

        Topic auto-creation is disabled in this workshop. One partition makes this first
        demonstration easy to inspect; later services partition by order ID.
        ''') ,
        code('''
        require_broker()
        topic = new_topic("django-on-commit", partitions=1)
        print(topic)
        '''),
        md('''
        ## Step 4 — Make a real Django HTTP request

        A money amount is a string to avoid binary floating-point surprises. A UUID identifies
        the order; another UUID identifies the event. `201` means the view returned successfully.
        This teaching endpoint waits for its Kafka delivery callback, so broker latency affects
        request latency. The outbox endpoint in notebook 13 will remove that dependency.
        ''') ,
        code('''
        with override_settings(KAFKA_ORDERS_TOPIC=topic):
            response = http.post("/orders/on-commit/", {"customer": "Ada", "total": "49.90"}, content_type="application/json")
        assert response.status_code == 201, response.content
        result = response.json()
        print(result)
        assert Order.objects.filter(id=result["order_id"]).exists()
        '''),
        md('''
        ## Step 5 — Inspect what Kafka received

        The message key is the order ID. The value is a versioned event envelope. The event
        timestamp is UTC, and the money value remains a decimal string after serialization.
        ''') ,
        code('''
        message = collect(topic, count=1)[0]
        event = decode(message)
        assert event["event_id"] == result["event_id"]
        assert message.key().decode() == result["order_id"]
        assert event["data"]["total"] == "49.90"
        print(event)
        '''),
        md('''
        ## Step 6 — Reject invalid HTTP input before writing

        A JSON float is rejected by the API contract. Validate before opening the transaction;
        a rejected request should neither create an order nor publish a message.
        ''') ,
        code('''
        before = Order.objects.count()
        with override_settings(KAFKA_ORDERS_TOPIC=topic):
            bad = http.post("/orders/on-commit/", {"customer": "Ada", "total": 49.9}, content_type="application/json")
        assert bad.status_code == 400
        assert Order.objects.count() == before
        print(bad.json())
        '''),
        md('''
        ## Step 7 — Observe deferred callbacks

        This cell deliberately replaces only the network send with a spy. The Django database
        transaction is real. The callback stays pending until the surrounding transaction exits.
        ''') ,
        code('''
        with patch("orders.services.publish_event") as send:
            with transaction.atomic():
                deferred_order, _ = create_order_after_commit("Deferred", "5.00", topic=topic)
                assert send.call_count == 0
                print("Inside the transaction: callback has not run")
            assert send.call_count == 1
            print("After commit: callback ran once")
        '''),
        md('''
        ## Step 8 — Rollback removes the callback too

        The intentional exception below is caught so Run All continues. Neither the order nor
        its callback survives the rollback. This prevents an event describing an order that never existed.
        ''') ,
        code('''
        before = Order.objects.count()
        with patch("orders.services.publish_event") as send:
            try:
                with transaction.atomic():
                    create_order_after_commit("Rolled back", "6.00", topic=topic)
                    raise RuntimeError("simulated request failure")
            except RuntimeError:
                pass
            send.assert_not_called()
        assert Order.objects.count() == before
        print("Rollback removed both the order and the callback")
        '''),
        md('''
        ## Step 9 — Reproduce the lost-event window

        Inject a broker failure after the database commits. This is a deterministic failure
        simulation: we do not stop your broker. The order survives, but no durable outgoing
        record exists. A process crash before the callback runs creates the same gap.
        ''') ,
        code('''
        before = Order.objects.count()
        with patch("orders.services.publish_event", side_effect=RuntimeError("simulated broker outage")):
            try:
                create_order_after_commit("Committed but unpublished", "7.00", topic=topic)
            except RuntimeError as exc:
                print(exc)
        assert Order.objects.count() == before + 1
        assert OutboxEvent.objects.count() == 0
        print("The database committed. There is no durable retry record.")
        '''),
        md('''
        ## Step 10 — Optional live HTTP demonstration

        In a separate terminal at the project root, first migrate the default database, then
        start the development server bound to `127.0.0.1`:

        ```text
        python django_demo/manage.py migrate
        python django_demo/manage.py runserver 127.0.0.1:8000 --noreload
        ```

        The terminal server uses its own database unless you set `DJANGO_DB_PATH` to this
        notebook's printed path. Set `KAFKA_ORDERS_TOPIC` to the printed topic before starting
        the server, then POST the same JSON with your preferred HTTP client. Stop with Ctrl+C.
        The demo endpoints are unauthenticated and CSRF-exempt for local teaching; they are not
        a deployment template. HTTP request retries also need a separate request-idempotency design.
        ''') ,
        md('''
        ## Practice and explain-back

        1. Send `"1.001"`, `"NaN"`, and an empty customer. Predict each response before running it.
        2. Move the intentional exception after the transaction block. Does the order remain?
        3. Why does `on_commit()` prevent premature publication but not guarantee publication?
        4. Draw the crash points between INSERT, COMMIT, PRODUCE, and acknowledgement.

        **Solution hints:** invalid money/customer returns 400; exceptions after COMMIT cannot undo
        it; a Python callback lives in process memory. Notebook 13 stores retryable intent in the
        same database transaction. Notebook 12 first makes receiving duplicates safe.

        Reference: [Django transaction callbacks](https://docs.djangoproject.com/en/5.2/topics/db/transactions/#performing-actions-after-commit)
        and [IPython async context](https://docs.djangoproject.com/en/5.2/topics/async/#async-safety).
        ''') ,
    ])

    save("12_django_idempotent_consumer.ipynb", [
        md('''
        # 12 · A durable Django consumer and safe replay

        **Level:** advanced · **Time:** 50–70 minutes · **Needs:** running Kafka, notebook 11.

        Build a materialized read model, simulate a crash after a database commit but before
        an offset commit, and prove that replay changes the database only once. Then run the
        same logic as a bounded Django management command and handle a poison event explicitly.

        Restart the kernel, then Run All. A fresh SQLite database and topic isolate this run.
        ''') ,
        code(BOOTSTRAP),
        setup("12"),
        md('''
        ## Step 1 — Create one event

        The source order and the projection represent different responsibilities even though
        this compact workshop keeps their tables in one SQLite database. In a deployed service
        they may live in separate databases. The consumer needs only the event's contents.
        ''') ,
        code('''
        require_broker()
        topic = new_topic("django-consumer", partitions=1)
        group = f"lab.django-consumer.{RUN}"
        order = Order.objects.create(customer="Grace", total=Decimal("12.50"))
        event = order_event(order)
        publish_event(event, topic)
        print("Event:", event["event_id"], "group:", group)
        '''),
        md('''
        ## Step 2 — Inspect atomic deduplication

        `InboxEvent.event_id` is a primary key. The receipt and the projection are committed in
        one database transaction. If the projection write fails, the receipt rolls back too.
        An existing receipt means the event was already applied. Reusing an ID with different
        content is rejected rather than silently accepted.
        ''') ,
        code('''
        import inspect
        print(inspect.getsource(apply_event))
        '''),
        md('''
        ## Step 3 — Process a record, then omit the offset commit

        Closing this consumer without committing simulates the Kafka-visible result of a
        process crash after the database commit. Automatic offset commits are disabled.
        No background thread can acknowledge this record on our behalf.
        ''') ,
        code('''
        first = consumer(group)
        try:
            first.subscribe([topic])
            original_message = poll_records(first, count=1)[0]
            assert apply_event(decode(original_message)) is True
            original_offset = original_message.offset()
            # Deliberately no first.commit(...).
        finally:
            first.close()
        assert InboxEvent.objects.count() == 1
        assert OrderProjection.objects.count() == 1
        print("Database committed; Kafka offset was not committed")
        '''),
        md('''
        ## Step 4 — Restart with the SAME group

        The group has no committed offset, so `earliest` selects the record again. The second
        application sees the durable receipt and skips the side effect. Only then does it
        commit the next offset. Kafka offsets identify positions, not business event IDs.
        ''') ,
        code('''
        restarted = consumer(group)
        try:
            restarted.subscribe([topic])
            replayed = poll_records(restarted, count=1)[0]
            assert replayed.offset() == original_offset
            assert apply_event(decode(replayed)) is False
            restarted.commit(message=replayed, asynchronous=False)
            from confluent_kafka import TopicPartition
            committed = restarted.committed([TopicPartition(topic, 0)], timeout=10)[0].offset
            assert committed == replayed.offset() + 1
        finally:
            restarted.close()
        assert InboxEvent.objects.count() == OrderProjection.objects.count() == 1
        print("Replay was harmless; committed next offset:", committed)
        '''),
        md('''
        ## Step 5 — Query the read model through HTTP

        Reading the projection does not call Kafka. This is eventual consistency: the write
        request may finish before its read model appears. A 404 can mean 'not projected yet'.
        ''') ,
        code('''
        projection = http.get(f"/projections/{order.id}/")
        assert projection.status_code == 200
        assert projection.json()["total"] == "12.50"
        print(projection.json())
        '''),
        md('''
        ## Step 6 — Run the same consumer as a separate command

        Create two more events, then launch a real child process with the same database path.
        The command exits after two messages or 20 seconds. There are no worker threads inside
        Django's web server or `AppConfig.ready()`.
        ''') ,
        code('''
        more_events = []
        for customer, total in [("Linus", "8.00"), ("Margaret", "13.00")]:
            source = Order.objects.create(customer=customer, total=Decimal(total))
            more_events.append(order_event(source))
            publish_event(more_events[-1], topic)
        import subprocess
        completed = subprocess.run(
            [sys.executable, str(ROOT / "django_demo" / "manage.py"), "consume_orders",
             "--topic", topic, "--group", group, "--max-messages", "2", "--timeout", "20"],
            cwd=ROOT, env=os.environ.copy(), text=True, capture_output=True, timeout=45, check=True,
        )
        print(completed.stdout)
        assert OrderProjection.objects.count() == 3
        '''),
        md('''
        ## Step 7 — Observe a duplicate arriving at a NEW Kafka offset

        A duplicate business event can be a completely new Kafka record. This is common when
        an outbox publisher crashes after delivery but before marking a row sent. Producer
        idempotence does not identify a second application-level publish as the same event.
        ''') ,
        code('''
        publish_event(event, topic)
        call_command("consume_orders", topic=topic, group=group, max_messages=1, timeout=20)
        assert OrderProjection.objects.count() == 3
        assert InboxEvent.objects.count() == 3
        print("Duplicate event at another offset left the projection unchanged")
        '''),
        md('''
        ## Step 8 — Make poison handling explicit

        Send an unsupported schema version. This consumer uses a **fail-stop policy**: it
        reports topic/partition/offset, closes, and does not commit the bad record. That preserves
        evidence and blocks subsequent progress on this partition until an operator repairs,
        quarantines, or deliberately skips it. Notebook 08 demonstrates a dead-letter alternative.
        ''') ,
        code('''
        import copy
        poison = copy.deepcopy(event)
        poison["event_id"] = str(uuid4())
        poison["schema_version"] = 999
        publish(topic, [poison])  # Generic publisher deliberately bypasses the domain validator.
        from django.core.management.base import CommandError
        try:
            call_command("consume_orders", topic=topic, group=group, max_messages=1, timeout=20)
        except CommandError as exc:
            print("Expected stop:", exc)
        else:
            raise AssertionError("The poison record should stop this consumer")
        assert InboxEvent.objects.count() == 3
        '''),
        md('''
        ## Step 9 — Verify the poison record remains unacknowledged

        A separate consumer using the same group receives the poison record again. Inspecting
        it does not advance the committed offset. In a real incident, avoid repeated retries
        without a plan: decide who owns schema repair and where rejected events are retained.
        ''') ,
        code('''
        diagnostic = consumer(group)
        try:
            diagnostic.subscribe([topic])
            stuck = poll_records(diagnostic, count=1)[0]
            assert decode(stuck)["schema_version"] == 999
            previous_commit = diagnostic.committed([TopicPartition(topic, 0)], timeout=10)[0].offset
            assert previous_commit == stuck.offset()
            print("Committed next offset:", previous_commit, "poison offset:", stuck.offset())
        finally:
            diagnostic.close()
        '''),
        md('''
        ## Step 10 — Prove receipts cannot outlive failed side effects

        Inject a database-side error after receipt creation. The surrounding `atomic()` block
        rolls back both operations, so a later retry is still allowed to apply the event.
        ''') ,
        code('''
        source = Order.objects.create(customer="Retry safely", total=Decimal("4.00"))
        retryable = order_event(source)
        with patch("orders.services.OrderProjection.objects.create", side_effect=RuntimeError("simulated database failure")):
            try:
                apply_event(retryable)
            except RuntimeError:
                pass
        assert not InboxEvent.objects.filter(event_id=retryable["event_id"]).exists()
        assert apply_event(retryable) is True
        print("Receipt rolled back; retry applied successfully")
        '''),
        md('''
        ## Practice and solution hints

        1. Explain why committing Kafka before updating the database can lose a side effect.
        2. Replay the topic into a new group. Why do existing receipt rows still suppress duplicates?
        3. Design receipt retention. How long might historical events be replayed?
        4. Extend the consumer with a DLQ. Which acknowledgement must succeed before source commit?

        **Hints:** database first + Kafka commit second creates a replay window, handled by durable
        deduplication. A new group changes Kafka offsets, not database history. Receipt retention
        must cover replay requirements. A DLQ publish must be acknowledged before committing the
        source, and DLQ consumers must tolerate duplicates too. This is at-least-once delivery with
        idempotent database effects, not a distributed exactly-once transaction.
        ''') ,
    ])

    save("13_django_transactional_outbox.ipynb", [
        md('''
        # 13 · Transactional outbox: close the database-to-Kafka gap

        **Level:** advanced · **Time:** 50–70 minutes · **Needs:** running Kafka, notebooks 11–12.

        Store an order and its event intent in one database transaction. Publish later, keep a
        failed event pending, and recreate the inevitable publish-before-mark-sent duplicate.
        Then combine the outbox with the durable consumer receipt from notebook 12.

        Start with a fresh kernel. This notebook uses a new database and Kafka topic.
        ''') ,
        code(BOOTSTRAP),
        setup("13"),
        md('''
        ## Step 1 — Examine the atomic write

        `Order` is business state. `OutboxEvent` is the durable intent to publish that change.
        Both use the same database transaction and connection. A database commit preserves
        both; a rollback preserves neither. The request does not contact Kafka.
        ''') ,
        code('''
        import inspect
        print(inspect.getsource(create_order_with_outbox))
        require_broker()
        topic = new_topic("django-outbox", partitions=1)
        '''),
        md('''
        ## Step 2 — Accept a request without sending a message

        A spy verifies that the HTTP endpoint never invokes the network publisher. Returning
        201 means the order and pending event are durable; it does not mean a consumer has
        projected the order yet.
        ''') ,
        code('''
        with override_settings(KAFKA_ORDERS_TOPIC=topic), patch("orders.services.publish_event") as send:
            response = http.post("/orders/", {"customer": "Outbox learner", "total": "25.00"}, content_type="application/json")
            send.assert_not_called()
        assert response.status_code == 201, response.content
        result = response.json()
        pending = OutboxEvent.objects.get(event_id=result["event_id"])
        assert pending.published_at is None
        assert http.get(f'/projections/{result["order_id"]}/').status_code == 404
        print(result)
        '''),
        md('''
        ## Step 3 — Prove atomic rollback

        Introduce an error in an outer transaction after creating an order and outbox row.
        Neither should survive. The test uses actual database writes and a caught exception.
        ''') ,
        code('''
        before = (Order.objects.count(), OutboxEvent.objects.count())
        try:
            with transaction.atomic():
                create_order_with_outbox("Will roll back", "2.00", topic=topic)
                raise RuntimeError("request failed before commit")
        except RuntimeError:
            pass
        assert before == (Order.objects.count(), OutboxEvent.objects.count())
        print("Order and event intent rolled back together")
        '''),
        md('''
        ## Step 4 — Inspect the publisher's ordering

        The publisher reads pending rows, sends one event, waits for its delivery acknowledgement,
        and only then sets `published_at`. It runs a finite snapshot. The time budget is checked
        between rows; an individual send can take up to 15 seconds beyond that budget.
        ''') ,
        code('''
        print(inspect.getsource(publish_pending))
        '''),
        md('''
        ## Step 5 — Failure must leave the event pending

        Inject a delivery error. A publisher must not mark an event as published merely because
        `produce()` accepted it into a local buffer. The callback/flush result establishes delivery.
        ''') ,
        code('''
        with patch("orders.services.publish_event", side_effect=RuntimeError("simulated delivery failure")):
            try:
                publish_pending(topic=topic)
            except RuntimeError as exc:
                print(exc)
        pending.refresh_from_db()
        assert pending.published_at is None
        print("Pending event is available for retry")
        '''),
        md('''
        ## Step 6 — Retry against the real broker

        The management command uses the same service. It exits when the bounded snapshot is
        drained; schedule or supervise it separately in a real system. For this SQLite lesson,
        run exactly one outbox publisher at a time.
        ''') ,
        code('''
        call_command("publish_outbox", topic=topic, max_messages=10, timeout=20)
        pending.refresh_from_db()
        assert pending.published_at is not None
        first_message = collect(topic, count=1)[0]
        assert decode(first_message)["event_id"] == result["event_id"]
        print("Published at:", pending.published_at)
        '''),
        md('''
        ## Step 7 — Marked rows are skipped

        Calling the publisher again must not send another event for already marked rows.
        This assertion inspects the returned count and verifies that the network function
        was not called; it does not rely on a short Kafka polling timeout to prove absence.
        ''') ,
        code('''
        with patch("orders.services.publish_event") as send:
            assert publish_pending(topic=topic) == 0
            send.assert_not_called()
        print("No pending rows; no publish attempted")
        '''),
        md('''
        ## Step 8 — Recreate a crash AFTER delivery, BEFORE marking

        Manually publish a second pending event, then deliberately leave its row unmarked.
        This models a process dying immediately after a successful acknowledgement. The
        replacement publisher cannot know delivery succeeded, so it correctly retries.
        ''') ,
        code('''
        second_order, second_event = create_order_with_outbox("Crash window", "31.50", topic=topic)
        publish_event(second_event, topic)
        second_row = OutboxEvent.objects.get(event_id=second_event["event_id"])
        assert second_row.published_at is None
        assert publish_pending(topic=topic) == 1
        print("One business event was delivered twice")
        '''),
        md('''
        ## Step 9 — Observe three records but two distinct event IDs

        A fresh group reads the whole topic from the beginning. The repeated event keeps its
        original ID and payload. Creating a new event ID on retry would defeat deduplication.
        ''') ,
        code('''
        records = collect(topic, count=3)
        event_ids = [decode(record)["event_id"] for record in records]
        assert len(event_ids) == 3
        assert len(set(event_ids)) == 2
        assert event_ids.count(second_event["event_id"]) == 2
        print(event_ids)
        '''),
        md('''
        ## Step 10 — Apply all deliveries safely

        The outbox is an at-least-once publication mechanism. Combine it with durable consumer
        receipts to make database effects idempotent. The inbox and projection share one database
        transaction, while Kafka commits remain a separate operation.
        ''') ,
        code('''
        effects = [apply_event(decode(record)) for record in records]
        assert effects.count(True) == 2 and effects.count(False) == 1
        assert OrderProjection.objects.count() == InboxEvent.objects.count() == 2
        print("Applied flags:", effects)
        '''),
        md('''
        ## Scaling discussion and exercises

        1. Add `attempt_count` and `last_error` fields. Keep failures pending and expose their age.
        2. Why is setting `published_at` before sending unsafe? Explain the lost-message window.
        3. Why doesn't a Kafka transaction make SQLite and Kafka one atomic transaction?
        4. Propose a PostgreSQL design for multiple publishers, preserving per-order ordering.

        **Solution hints:** acknowledgement-before-marking favors duplicates over silent loss;
        Kafka transactions do not enlist this SQLite database. Multiple publishers need an explicit
        claim/lease or locking protocol and partition/order policy. SQLite's `select_for_update()`
        does not provide the row-locking design you would need. This sample deliberately supports
        one publisher. A production system may use CDC for the outbox; operating a connector,
        schema evolution, retention, monitoring, and recovery are additional responsibilities.

        Pending rows and receipts need retention policies. Never purge receipts while events that
        rely on them can still be replayed. HTTP request idempotency is separate from event deduplication.
        ''') ,
    ])

    save("14_capstone_order_pipeline.ipynb", [
        md('''
        # 14 · Capstone: demonstrate an order pipeline and its recovery

        **Level:** advanced/expert practice · **Time:** 60–90 minutes · **Needs:** notebooks 11–13 and Kafka.

        Demonstrate the complete path: HTTP request → order + outbox → Kafka → consumer receipt +
        projection → HTTP read. Prove eventual consistency, simulate a publisher crash, run workers
        as separate processes, and verify business invariants. Use the checklist at the end as
        a presentation script. Passing this lab is a starting point for production design, not
        a claim that the local single-broker system is production-ready.

        Restart the kernel and Run All. Topics, group IDs, orders, and database are isolated per run.
        ''') ,
        code(BOOTSTRAP),
        setup("14"),
        md('''
        ## Step 1 — Set up a three-partition event stream

        Keys are order IDs, so all records for one order use one partition with a stable partition
        count. Different orders can be consumed in parallel; no global order is promised.
        This capstone still runs one consumer process to keep the demonstration easy to observe.
        ''') ,
        code('''
        require_broker()
        topic = new_topic("capstone-orders", partitions=3)
        group = f"lab.capstone.{RUN}"
        print("Topic:", topic, "Consumer group:", group)
        '''),
        md('''
        ## Step 2 — Receive five HTTP requests

        The database commits order state and outgoing intent together. Kafka is not needed
        inside these requests. Keep the returned IDs: they connect the HTTP, outbox, Kafka,
        receipt, and projection views of the same work.
        ''') ,
        code('''
        accepted = []
        with override_settings(KAFKA_ORDERS_TOPIC=topic):
            for customer, total in [("Ada", "10.00"), ("Grace", "20.25"), ("Linus", "5.50"), ("Margaret", "30.00"), ("Guido", "14.25")]:
                response = http.post("/orders/", {"customer": customer, "total": total}, content_type="application/json")
                assert response.status_code == 201, response.content
                accepted.append(response.json())
        assert Order.objects.count() == OutboxEvent.objects.count() == 5
        assert OutboxEvent.objects.filter(published_at__isnull=True).count() == 5
        print(accepted)
        '''),
        md('''
        ## Step 3 — Observe eventual consistency

        The write model exists while all read models are absent. This is expected pipeline
        delay. A UI can show 'processing' until a projection appears; it must not equate request
        acceptance with completion of every downstream side effect.
        ''') ,
        code('''
        initial_statuses = [http.get(f'/projections/{item["order_id"]}/').status_code for item in accepted]
        assert initial_statuses == [404] * 5
        print("Before workers:", initial_statuses)
        '''),
        md('''
        ## Step 4 — Simulate an interrupted publisher

        Deliver one pending event successfully, then leave its row pending. This is the same
        externally visible state as a process crash between the Kafka acknowledgement and
        the database marker update. It is intentional and will create one duplicate.
        ''') ,
        code('''
        interrupted = OutboxEvent.objects.order_by("created_at", "event_id").first()
        publish_event(interrupted.payload, interrupted.topic)
        assert interrupted.published_at is None
        print("Delivered but unmarked:", interrupted.event_id)
        '''),
        md('''
        ## Step 5 — Run a replacement outbox publisher process

        `subprocess.run()` inherits `DJANGO_DB_PATH` set above. The process uses the same local
        database, explicit topic filter, and Python environment as the notebook. Each worker
        is bounded, and failures propagate through `check=True`.
        ''') ,
        code('''
        import subprocess

        def worker(command, *arguments):
            completed = subprocess.run(
                [sys.executable, str(ROOT / "django_demo" / "manage.py"), command, *arguments],
                cwd=ROOT, env=os.environ.copy(), text=True, capture_output=True,
                timeout=120, check=True,
            )
            print(completed.stdout.strip())
            return completed

        worker("publish_outbox", "--topic", topic, "--max-messages", "10", "--timeout", "60")
        assert OutboxEvent.objects.filter(published_at__isnull=True).count() == 0
        '''),
        md('''
        ## Step 6 — Inspect the event log before applying it

        A fresh inspection group reads six Kafka records: five events, one delivered twice.
        Independent groups do not steal messages from each other. Partition/offset identifies
        a log record; event ID identifies the business fact that must be applied once.
        ''') ,
        code('''
        messages = collect(topic, count=6)
        inspected_ids = [decode(message)["event_id"] for message in messages]
        assert len(set(inspected_ids)) == 5
        for message in messages:
            value = decode(message)
            print(message.partition(), message.offset(), value["event_id"], value["data"]["customer"])
        '''),
        md('''
        ## Step 7 — Run the projection consumer process

        Each message is validated, then its receipt and projection are committed together.
        Only after that database transaction succeeds does the consumer acknowledge the Kafka
        position. The repeated event is a successful no-op and can safely be acknowledged.
        ''') ,
        code('''
        worker("consume_orders", "--topic", topic, "--group", group, "--max-messages", "6", "--timeout", "40")
        assert InboxEvent.objects.count() == 5
        assert OrderProjection.objects.count() == 5
        '''),
        md('''
        ## Step 8 — Verify business results through HTTP

        Validate more than process exit codes: every accepted order should have one projection,
        the event IDs should match, and exact-decimal totals should agree.
        ''') ,
        code('''
        projected = []
        for item in accepted:
            response = http.get(f'/projections/{item["order_id"]}/')
            assert response.status_code == 200
            body = response.json()
            assert body["source_event_id"] == item["event_id"]
            assert Decimal(body["total"]) == Decimal(item["total"])
            projected.append(body)
        expected_total = sum((Decimal(item["total"]) for item in accepted), Decimal("0.00"))
        actual_total = sum((row.total for row in OrderProjection.objects.all()), Decimal("0.00"))
        assert expected_total == actual_total == Decimal("80.00")
        print("Five projections; total:", actual_total)
        '''),
        md('''
        ## Step 9 — Rebuild or replay with a new group

        A new group starts from the beginning, but the existing receipt table still remembers
        each event. All six deliveries should be duplicates. To rebuild a projection into a
        genuinely new store, use a separate database (including fresh receipts); do not clear
        receipts while a live consumer is using them.
        ''') ,
        code('''
        worker("consume_orders", "--topic", topic, "--group", f"lab.capstone-replay.{RUN}", "--max-messages", "6", "--timeout", "40")
        assert InboxEvent.objects.count() == OrderProjection.objects.count() == 5
        print("Replay preserved every business invariant")
        '''),
        md('''
        ## Step 10 — Calculate operational indicators

        Inspect backlog and oldest pending age in the database. Kafka group lag is a different
        measurement: notebook 10 covers log offsets and consumer positions. Low outbox backlog
        does not prove that downstream consumers are healthy.
        ''') ,
        code('''
        pending_rows = OutboxEvent.objects.filter(published_at__isnull=True)
        oldest = pending_rows.order_by("created_at").first()
        oldest_age_seconds = (timezone.now() - oldest.created_at).total_seconds() if oldest else 0
        print({"outbox_pending": pending_rows.count(), "oldest_pending_seconds": oldest_age_seconds,
               "orders": Order.objects.count(), "projected_orders": OrderProjection.objects.count()})
        assert pending_rows.count() == 0
        '''),
        md('''
        ## Your live demonstration script

        1. Explain topics, partitions, keys, offsets, and group IDs using the six inspected records.
        2. Pause before starting workers and show the five 404 projection responses.
        3. Explain the deliberately unmarked event and predict the number of Kafka records.
        4. Start each worker and show its bounded output.
        5. Show six deliveries becoming five database effects, then replay without changing totals.
        6. Name the remaining boundaries: HTTP request retries, broker replication, authentication,
           schemas, retention, poison records, and monitoring.

        ## Expert extension challenges

        - Add `order.cancelled` with explicit allowed transitions and per-order sequence numbers.
        - Partition work across two consumer processes and verify exclusive assignments.
        - Move to PostgreSQL and design publisher ownership, retries, and ordering under concurrency.
        - Add a DLQ and a reviewed redrive operation that preserves event IDs.
        - Measure p50/p95 end-to-end delay and alarm on oldest pending outbox age.

        **Solution direction:** write the invariants and crash matrix first. A retry must keep
        event identity; a transition must reject stale versions; concurrent publisher ownership
        must expire safely. Use notebook 15 to explore broker replication separately. Keep this
        SQLite, single-publisher, unauthenticated local application scoped to teaching.
        '''),
    ])


if __name__ == "__main__":
    main()
