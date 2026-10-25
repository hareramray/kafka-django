"""Regenerate the advanced Kafka workshop notebooks (07-10)."""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from labtools.notebook_builder import BOOTSTRAP, code, md, write_notebook


def build_transactions():
    cells = [
        md("""
        # 07 · Idempotence and Kafka transactions

        **Level:** advanced · **Time:** 50–65 minutes · **Prerequisites:** notebooks 01–06.

        You will distinguish producer retries from application duplicates, inspect aborted
        transactions, and atomically publish output **and** commit input offsets.
        Start Kafka with `docker compose up -d --wait` from the repository root.
        Restart the kernel, then run cells from top to bottom. Each run creates fresh topics.

        Keep these three boundaries separate: a producer's retry, a Kafka transaction,
        and an external database transaction. They solve different failure cases.
        """),
        code(BOOTSTRAP),
        code("""
        import json
        from confluent_kafka import KafkaException, TopicPartition

        require_broker()
        duplicate_topic = new_topic("07.duplicates", partitions=1)
        visibility_topic = new_topic("07.visibility", partitions=1)
        source_topic = new_topic("07.input", partitions=1)
        output_topic = new_topic("07.output", partitions=1)
        """),
        md("""
        ## 1. An idempotent producer does not deduplicate your business events

        Our producer helper enables `enable.idempotence=True` and `acks=all`.
        Send the same event twice deliberately. Both calls are legitimate new sends;
        Kafka cannot infer that the second application call was a mistake.
        """),
        code("""
        event = {"event_id": "payment-001", "amount_paise": 12500}
        publish(duplicate_topic, [event, event], key_field="event_id")
        duplicates = collect(duplicate_topic, count=2)
        print([(m.offset(), decode(m)) for m in duplicates])
        assert len({decode(m)["event_id"] for m in duplicates}) == 1
        assert len(duplicates) == 2
        """),
        md("""
        **Observe:** two offsets, one business ID. Producer idempotence handles eligible
        protocol retries; an event ID plus durable application deduplication handles repeated
        application submissions. Do not use an in-memory set as a production deduplication store.
        """),
        md("""
        ## 2. Publish one aborted and one committed transaction

        A `transactional.id` identifies a logical writer. In a deployed service it must remain
        stable across that writer's restart, while distinct concurrent writers need distinct IDs.
        The random ID below is appropriate for this isolated notebook run. Reusing an ID from a
        second producer can fence the first writer.
        """),
        code("""
        tx = producer(**{"transactional.id": f"lab-07-{uuid4().hex}",
                         "transaction.timeout.ms": 30000})
        tx.init_transactions(20)

        def enqueue_checked(p, topic, values):
            errors = []
            def delivered(err, message):
                if err is not None:
                    errors.append(str(err))
            for value in values:
                p.produce(topic, key=value["event_id"],
                          value=json.dumps(value).encode(), on_delivery=delivered)
                p.poll(0)
            remaining = p.flush(15)
            if remaining or errors:
                raise RuntimeError(f"Unconfirmed delivery: pending={remaining}, errors={errors}")
        """),
        code("""
        tx.begin_transaction()
        try:
            enqueue_checked(tx, visibility_topic,
                            [{"event_id": "aborted", "status": "do-not-use"}])
            # Flush first so this record reaches the log before we abort it.
            tx.abort_transaction(15)
        except Exception:
            try:
                tx.abort_transaction(15)
            except KafkaException:
                pass
            raise
        print("First transaction aborted.")
        """),
        code("""
        tx.begin_transaction()
        try:
            enqueue_checked(tx, visibility_topic,
                            [{"event_id": "committed-1", "status": "ready"},
                             {"event_id": "committed-2", "status": "ready"}])
            tx.commit_transaction(15)
        except Exception:
            try:
                tx.abort_transaction(15)
            except KafkaException:
                pass
            raise
        print("Second transaction committed.")
        """),
        md("""
        ## 3. Compare the two isolation levels

        `read_uncommitted` may return aborted records. `read_committed` hides them and
        waits behind an open transaction's visibility boundary. Transaction control records
        also consume offsets, so offsets need not be consecutive in the returned application records.
        """),
        code("""
        raw = collect(visibility_topic, count=3,
                      config={"isolation.level": "read_uncommitted"})
        committed = collect(visibility_topic, count=2,
                            config={"isolation.level": "read_committed"})
        print("Raw:", [(m.offset(), decode(m)["event_id"]) for m in raw])
        print("Committed:", [(m.offset(), decode(m)["event_id"]) for m in committed])
        assert {decode(m)["event_id"] for m in committed} == {"committed-1", "committed-2"}
        """),
        md("""
        ## 4. Consume → transform → produce as one Kafka transaction

        Publishing output and then separately committing source offsets leaves a crash gap.
        `send_offsets_to_transaction` joins the source offsets to the output transaction.
        We will abort once, restart the consumer, and then commit the replayed input.
        """),
        code("""
        publish(source_topic, [
            {"event_id": "a", "amount_paise": 10000},
            {"event_id": "b", "amount_paise": 20000},
        ], key_field="event_id")
        pipeline_group = f"lab-07-pipeline-{uuid4().hex}"

        def transform_transaction(c, messages, *, abort):
            tx.begin_transaction()
            try:
                transformed = [{**decode(m), "tax_paise": decode(m)["amount_paise"] // 10}
                               for m in messages]
                enqueue_checked(tx, output_topic, transformed)
                # Offsets mean the NEXT record to consume, independently for each partition.
                next_offsets = {}
                for m in messages:
                    k = (m.topic(), m.partition())
                    next_offsets[k] = max(next_offsets.get(k, 0), m.offset() + 1)
                offsets = [TopicPartition(t, p, o) for (t, p), o in next_offsets.items()]
                tx.send_offsets_to_transaction(offsets, c.consumer_group_metadata(), 15)
                if abort:
                    tx.abort_transaction(15)
                else:
                    tx.commit_transaction(15)
            except Exception:
                try:
                    tx.abort_transaction(15)
                except KafkaException:
                    pass
                raise
        """),
        code("""
        first = consumer(pipeline_group, **{"isolation.level": "read_committed"})
        try:
            first.subscribe([source_topic])
            attempt_one = poll_records(first, count=2, timeout=20)
            transform_transaction(first, attempt_one, abort=True)
            print("Aborted input offsets:", [m.offset() for m in attempt_one])
        finally:
            first.close()
        """),
        md("""
        The aborted transaction advanced neither the committed source offset nor the visible
        output. The previous consumer's in-memory position did advance; closing it and using
        the same group makes the restart behavior explicit.
        """),
        code("""
        retry = consumer(pipeline_group, **{"isolation.level": "read_committed"})
        try:
            retry.subscribe([source_topic])
            attempt_two = poll_records(retry, count=2, timeout=20)
            assert [m.offset() for m in attempt_one] == [m.offset() for m in attempt_two]
            transform_transaction(retry, attempt_two, abort=False)
            offsets_after = retry.committed([TopicPartition(source_topic, 0)], timeout=10)
            assert offsets_after[0].offset == 2
            print("Committed next input offset:", offsets_after[0].offset)
        finally:
            retry.close()
        """),
        code("""
        visible_output = collect(output_topic, count=2,
                                 config={"isolation.level": "read_committed"})
        visible_values = [decode(m) for m in visible_output]
        print(visible_values)
        assert {v["event_id"]: v["tax_paise"] for v in visible_values} == {"a": 1000, "b": 2000}
        all_output = collect(output_topic, count=4,
                             config={"isolation.level": "read_uncommitted"})
        assert len(all_output) == 4
        print("Four physically sent output records; two transactionally visible results.")
        """),
        md("""
        ## 5. State the guarantee precisely

        This demonstrates atomic Kafka output plus Kafka consumer offsets for the input batch.
        It does **not** atomically include a Django database update, HTTP call, email, or a
        duplicate already present in the input topic. Use an outbox and durable idempotent
        handling for database integration (notebooks 12–14).

        In production inspect `KafkaException.args[0]`: a retriable error may permit retrying
        the operation; `txn_requires_abort()` requires aborting and replaying the input;
        fatal/fencing errors require stopping that writer. A commit timeout must not trigger a
        blind new business send. This teaching code stops on errors instead of implementing a
        deployment-specific recovery loop. Keep processing within `max.poll.interval.ms` and
        the transaction timeout, and handle rebalances before committing stale ownership.
        """),
        md("""
        ## Practice

        1. Add a third input event and update the assertions.
        2. Deliberately send event `a` twice to the input. Does a Kafka transaction remove it?
        3. Explain why an HTTP call inside `transform_transaction` is not rolled back by abort.
        4. Open a transaction, publish, and compare the two isolation levels before aborting.
           Use a short bounded poll for the committed reader; always abort in `finally`.

        **Solution hints:** the source offset becomes 3; business duplicates remain legitimate
        source records; external services are outside Kafka's transaction coordinator; a
        committed reader can wait for an earlier transaction to resolve.
        """),
        md("""
        ## Presenter checkpoint and references

        Ask a learner to explain all three observations: application duplicates survived,
        aborted output was hidden, and aborted source offsets replayed.

        Reference APIs: [Confluent Python transactions](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#transactional-api),
        [official consume/produce example](https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/eos_transactions.py).
        """),
    ]
    write_notebook(ROOT / "notebooks/07_idempotence_and_transactions.ipynb", cells)


def build_retries():
    cells = [
        md("""
        # 08 · Retries, poison records, and a dead-letter queue

        **Level:** advanced · **Time:** 45–60 minutes · **Prerequisites:** notebooks 03–06.

        You will distinguish invalid input from a temporary dependency failure, retry a small
        bounded number of times, route a permanent failure to a dead-letter topic (DLQ), and
        acknowledge the source **only after** the output is durably acknowledged by Kafka.

        Start Kafka, restart the kernel, then run every cell in order. This notebook injects
        failures intentionally; the expected failures are caught and explained.
        """),
        code(BOOTSTRAP),
        code("""
        import json
        import time
        from collections import Counter
        from datetime import datetime, timezone
        from confluent_kafka import TopicPartition

        require_broker()
        source_topic = new_topic("08.orders", partitions=2)
        accepted_topic = new_topic("08.accepted", partitions=2)
        dlq_topic = new_topic("08.dlq", partitions=2)
        replay_topic = new_topic("08.replay", partitions=2)
        """),
        md("""
        ## 1. Prepare four distinct failure cases

        Money is integer paise to keep arithmetic exact. `temporary` will succeed on its third
        attempt; `poison` never succeeds; `invalid` must not be retried because retrying cannot
        change its invalid data. The fault names are teaching controls, not event schema fields.
        """),
        code("""
        examples = [
            {"event_id": "normal", "customer_id": "c1", "amount_paise": 5000},
            {"event_id": "temporary", "customer_id": "c2", "amount_paise": 7000},
            {"event_id": "invalid", "customer_id": "c3", "amount_paise": -10},
            {"event_id": "poison", "customer_id": "c4", "amount_paise": 9000},
        ]
        publish(source_topic, examples, key_field="customer_id")
        """),
        code("""
        class TemporaryDependencyError(Exception):
            pass

        def validate_order(value):
            if not isinstance(value, dict):
                raise ValueError("Order must be an object")
            for name in ("event_id", "customer_id"):
                if not isinstance(value.get(name), str) or not value[name]:
                    raise ValueError(f"{name} must be a nonempty string")
            amount = value.get("amount_paise")
            if type(amount) is not int or amount <= 0:
                raise ValueError("amount_paise must be a positive integer")
            return value
        """),
        md("""
        ## 2. Make the retry policy explicit

        Only temporary failures get a retry. Three attempts means the initial call plus at
        most two retries. We use very short delays for a live demo. Real policies need a time
        budget, jitter, observability, and a decision about ordering while a partition is blocked.
        """),
        code("""
        attempts = Counter()

        def reserve_inventory(order):
            event_id = order["event_id"]
            attempts[event_id] += 1
            if event_id == "poison" or (event_id == "temporary" and attempts[event_id] < 3):
                raise TemporaryDependencyError("Injected inventory service failure")
            return {**order, "status": "reserved"}

        def reserve_with_retry(order, max_attempts=3):
            for attempt in range(1, max_attempts + 1):
                try:
                    return reserve_inventory(order)
                except TemporaryDependencyError:
                    if attempt == max_attempts:
                        raise
                    time.sleep(0.05 * 2 ** (attempt - 1))
        """),
        md("""
        ## 3. Preserve enough context to investigate a DLQ record

        The envelope records the original topic, partition, offset, key, raw bytes, error class,
        attempts, and a timestamp. Base64 preserves even input that is not valid UTF-8/JSON.
        Store only data you are permitted to retain; a DLQ is still a data store with access and
        retention requirements.
        """),
        code("""
        import base64

        def dead_letter(message, error, attempt_count):
            raw = message.value()
            return {
                "dlq_id": f"{message.topic()}:{message.partition()}:{message.offset()}",
                "source": {"topic": message.topic(), "partition": message.partition(),
                           "offset": message.offset()},
                "key_base64": (base64.b64encode(message.key()).decode()
                               if message.key() is not None else None),
                "value_base64": (base64.b64encode(raw).decode() if raw is not None else None),
                "error_type": type(error).__name__, "error": str(error),
                "attempts": attempt_count,
                "failed_at": datetime.now(timezone.utc).isoformat(),
            }
        """),
        md("""
        ## 4. Publish the outcome before committing the source

        `publish` checks delivery callbacks and a bounded flush; it raises if delivery is not
        confirmed. If either output publish or source commit fails, this worker stops. It does
        not continue and commit a later offset from the same partition over a failed record.
        Processing one record at a time makes this rule visible.
        """),
        code("""
        worker_group = f"lab-08-worker-{uuid4().hex}"
        c = consumer(worker_group)
        outcomes = []
        try:
            c.subscribe([source_topic])
            for _ in range(len(examples)):
                message = poll_records(c, count=1, timeout=20)[0]
                value = None
                try:
                    value = validate_order(decode(message))
                    result = reserve_with_retry(value)
                except (ValueError, TypeError, UnicodeDecodeError, TemporaryDependencyError) as error:
                    event_id = value.get("event_id", "") if isinstance(value, dict) else ""
                    envelope = dead_letter(message, error, attempts[event_id])
                    publish(dlq_topic, [envelope], key_field="dlq_id")
                    destination = "dlq"
                else:
                    publish(accepted_topic, [result], key_field="event_id")
                    destination = "accepted"
                # Only reached after the output acknowledgement succeeds.
                c.commit(message=message, asynchronous=False)
                outcomes.append((message.partition(), message.offset(), destination))
        finally:
            c.close()
        print(outcomes)
        print("Dependency attempts:", dict(attempts))
        assert attempts == Counter(normal=1, temporary=3, poison=3)
        """),
        code("""
        accepted = [decode(m) for m in collect(accepted_topic, count=2)]
        failures = [decode(m) for m in collect(dlq_topic, count=2)]
        assert {x["event_id"] for x in accepted} == {"normal", "temporary"}
        assert {x["error_type"] for x in failures} == {"ValueError", "TemporaryDependencyError"}
        for failure in failures:
            print(failure["source"], failure["error_type"], "attempts:", failure["attempts"])
        """),
        md("""
        **Observe:** invalid data bypassed the dependency, a transient fault recovered, and a
        repeatedly failing valid record reached the DLQ. Moving it aside allows later source
        records to progress, but can violate the business order of related events. Decide whether
        blocking the partition, retry topics, or a per-entity pause is appropriate for your domain.
        """),
        md("""
        ## 5. Prove that an unacknowledged DLQ write must not advance the source

        Use a separate one-record topic and inject failure at the publish boundary. There is
        deliberately no source commit in the exception branch. A restarted consumer in the same
        group must receive the same input offset again.
        """),
        code("""
        failure_topic = new_topic("08.publish-failure", partitions=1)
        publish(failure_topic, [{"event_id": "retry-me", "amount_paise": -1}])
        failure_group = f"lab-08-failure-{uuid4().hex}"
        before = consumer(failure_group)
        try:
            before.subscribe([failure_topic])
            original = poll_records(before, count=1, timeout=20)[0]
            try:
                raise RuntimeError("Injected: DLQ delivery was not acknowledged")
                # A commit here would lose the only durable copy of the failure outcome.
            except RuntimeError as error:
                print(error)
        finally:
            before.close()

        after = consumer(failure_group)
        try:
            after.subscribe([failure_topic])
            replayed = poll_records(after, count=1, timeout=20)[0]
            assert replayed.offset() == original.offset()
            print("Replayed source offset:", replayed.offset())
        finally:
            after.close()
        """),
        md("""
        ## 6. Repair and replay deliberately

        A DLQ is not a retry scheduler. Inspect a record, correct the cause, and send the approved
        repair to a replay topic. We preserve the business event ID and add repair provenance.
        A second replay may still produce duplicates, so the downstream service must deduplicate
        durably or define a versioned correction event. This cell handles only the invalid amount.
        """),
        code("""
        invalid_failure = next(x for x in failures if x["error_type"] == "ValueError")
        repaired = json.loads(base64.b64decode(invalid_failure["value_base64"]))
        repaired["amount_paise"] = 1000
        repaired["repair_of"] = invalid_failure["dlq_id"]
        validate_order(repaired)
        publish(replay_topic, [repaired], key_field="event_id")
        repaired_read = decode(collect(replay_topic, count=1)[0])
        assert repaired_read["event_id"] == "invalid"
        assert repaired_read["amount_paise"] == 1000
        print(repaired_read)
        """),
        md("""
        ## 7. Discuss the remaining crash gap

        If output delivery succeeds and the process crashes before the source commit, the
        source replays and the output may be published twice. This is **at-least-once** routing.
        A Kafka transaction can join the output and source offsets (notebook 07). A database
        side effect instead needs a durable event-ID record in the same database transaction
        as that effect (notebook 12). This notebook makes no exactly-once claim.

        Inline retries also consume the consumer's polling time budget. Long delays can trigger
        rebalances. Retrying on separate topics permits progress but adds ordering, scheduling,
        and operational decisions; sleeping for minutes in a consumer is not a robust scheduler.
        """),
        md("""
        ## Practice

        1. Add malformed UTF-8 bytes to the input and confirm the DLQ preserves the bytes.
        2. Make `temporary` fail on four attempts. Predict the outcome with three allowed attempts.
        3. Inject a failure immediately after output acknowledgement, then restart the group.
        4. Track DLQ counts by error class and expose the age of the oldest unresolved failure.

        **Solution hints:** use raw `producer.produce` for malformed bytes; it reaches the DLQ;
        a post-publish crash can duplicate output; alert on age and rate, not just a nonempty topic.
        """),
        md("""
        ## Presenter checkpoint and reference

        Ask: which exact statement makes a source offset safe to commit? The intended answer
        is that processing or the chosen durable failure handoff has succeeded, with duplicate
        handling for the gap before the commit.

        [Python consumer delivery semantics](https://docs.confluent.io/kafka-clients/python/current/overview.html#delivery-guarantees).
        """),
    ]
    write_notebook(ROOT / "notebooks/08_retries_and_dead_letter_queue.ipynb", cells)


def build_windows():
    cells = [
        md("""
        # 09 · Event-time windows, late events, and replayable state

        **Level:** advanced · **Time:** 50–65 minutes · **Prerequisites:** notebooks 02–06.

        Build a small Python stream processor that sums purchases in ten-second tumbling
        windows. Observe out-of-order arrival, a watermark, an event that arrives too late,
        final results, and rebuilding state by replaying the source.

        This is an explicit Python teaching implementation, not the Java Kafka Streams API.
        It uses one source partition so the event sequence is deterministic. Start Kafka,
        restart the kernel, and run top to bottom.
        """),
        code(BOOTSTRAP),
        code("""
        from collections import defaultdict

        require_broker()
        source_topic = new_topic("09.purchases", partitions=1)
        totals_topic = new_topic("09.window-totals", partitions=1,
                                 config={"cleanup.policy": "compact"})
        late_topic = new_topic("09.late", partitions=1)
        """),
        md("""
        ## 1. Arrival order and event time are different

        `event_second` is a timestamp measured from the beginning of our fictional session.
        Windows are `[0,10)`, `[10,20)`, and so on: the right endpoint is excluded.
        Kafka preserves order within this partition, but producers can send event timestamps
        out of order. Offsets tell us arrival order, not when the business event happened.
        """),
        code("""
        purchases = [
            {"event_id": "e1", "customer_id": "c1", "event_second": 2, "amount_paise": 10},
            {"event_id": "e2", "customer_id": "c1", "event_second": 7, "amount_paise": 20},
            {"event_id": "e3", "customer_id": "c1", "event_second": 12, "amount_paise": 30},
            {"event_id": "e4", "customer_id": "c2", "event_second": 6, "amount_paise": 7},
            {"event_id": "e5", "customer_id": "c1", "event_second": 25, "amount_paise": 40},
            {"event_id": "e6", "customer_id": "c1", "event_second": 1, "amount_paise": 999},
            {"event_id": "e7", "customer_id": "c2", "event_second": 36, "amount_paise": 50},
        ]
        publish(source_topic, purchases, key_field="customer_id")
        print([(x["event_id"], x["event_second"]) for x in purchases])
        """),
        md("""
        ## 2. Define the watermark policy before writing code

        Our watermark is `maximum event time observed − 5 seconds`. Finalize a window when
        its end is less than or equal to that watermark. A new event for an already finalized
        window is late and goes to a side topic. This is our policy, not a guarantee that all
        earlier events really arrived. A future-dated event can advance the watermark too far.

        Event `e4` is out of order but its window remains open. Event `e6` arrives after the
        watermark has closed its window. Predict which one will contribute to the result.
        """),
        code("""
        WINDOW_SECONDS = 10
        ALLOWED_LATENESS = 5

        def window_start(second):
            return (second // WINDOW_SECONDS) * WINDOW_SECONDS

        assert [window_start(t) for t in [0, 9, 10, 19, 20]] == [0, 0, 10, 10, 20]
        """),
        md("""
        ## 3. Implement the state machine

        Each open `(customer, window)` key holds a sum and count. Finalization emits a stable
        key and removes that state. A hard cap prevents accidental unbounded key cardinality;
        a real processor needs a suitable persistent state store, backpressure, and monitoring.
        """),
        code("""
        class WindowTotals:
            def __init__(self, max_open_keys=1000):
                self.max_seen = None
                self.watermark = float("-inf")
                self.state = {}
                self.max_open_keys = max_open_keys

            def emit_closed(self, watermark):
                results = []
                ready = [k for k in self.state if k[1] + WINDOW_SECONDS <= watermark]
                for customer, start in sorted(ready):
                    total, count = self.state.pop((customer, start))
                    results.append({"window_id": f"{customer}:{start}",
                                    "customer_id": customer, "window_start": start,
                                    "window_end": start + WINDOW_SECONDS,
                                    "total_paise": total, "event_count": count})
                return results

            def process(self, event):
                second = event["event_second"]
                if type(second) is not int or second < 0:
                    raise ValueError("event_second must be a nonnegative integer")
                if type(event["amount_paise"]) is not int or event["amount_paise"] <= 0:
                    raise ValueError("amount_paise must be a positive integer")
                start = window_start(second)
                self.max_seen = second if self.max_seen is None else max(self.max_seen, second)
                self.watermark = self.max_seen - ALLOWED_LATENESS
                closed = self.emit_closed(self.watermark)
                if start + WINDOW_SECONDS <= self.watermark:
                    late = {**event, "watermark": self.watermark,
                            "reason": "window_already_finalized"}
                    return closed, late
                key = (event["customer_id"], start)
                if key not in self.state and len(self.state) >= self.max_open_keys:
                    raise RuntimeError("Open-window state limit reached; stop and investigate")
                total, count = self.state.get(key, (0, 0))
                self.state[key] = (total + event["amount_paise"], count + 1)
                return closed, None

            def finish_finite_sample(self):
                # Legal only because our finite input batch has an explicit end.
                return self.emit_closed(float("inf"))
        """),
        md("""
        ## 4. Inspect the algorithm before connecting it to the consumer

        The trace shows each watermark, the remaining open keys, and finalized output.
        An idle live stream has no explicit end: a silent source does not automatically advance
        this event-time watermark. Do not call the finite-sample finish operation on a live stream.
        """),
        code("""
        preview = WindowTotals()
        preview_results, preview_late = [], []
        for event in purchases:
            closed, late = preview.process(event)
            preview_results.extend(closed)
            if late:
                preview_late.append(late)
            print(event["event_id"], "watermark=", preview.watermark,
                  "open_keys=", len(preview.state), "closed=", len(closed),
                  "late=", late is not None)
        preview_results.extend(preview.finish_finite_sample())
        assert [x["event_id"] for x in preview_late] == ["e6"]
        assert preview.state == {}
        """),
        md("""
        ## 5. Run the processor against actual Kafka records

        For this lab we deliberately leave source offsets uncommitted and rebuild from the
        complete retained input on restart. Committing an offset while all window state exists
        only in memory would lose that state after a crash. This full-replay design has increasing
        recovery cost and needs source retention long enough for a complete reconstruction.

        Each final result uses `window_id` as its Kafka key. Consumers should overwrite the
        previous result for that key, rather than add the same total again. Compaction is
        asynchronous; duplicate versions can exist in the log until cleaning occurs.
        """),
        code("""
        rebuild_group = f"lab-09-rebuild-{uuid4().hex}"

        def run_finite_job(destination, late_destination):
            processor = WindowTotals()
            reader = consumer(rebuild_group)
            emitted, late_count = [], 0
            try:
                reader.subscribe([source_topic])
                for _ in range(len(purchases)):
                    message = poll_records(reader, count=1, timeout=20)[0]
                    closed, late = processor.process(decode(message))
                    if closed:
                        publish(destination, closed, key_field="window_id")
                        emitted.extend(closed)
                    if late:
                        publish(late_destination, [late], key_field="event_id")
                        late_count += 1
                final = processor.finish_finite_sample()
                publish(destination, final, key_field="window_id")
                emitted.extend(final)
                # Intentionally no source commit: there is no durable state checkpoint.
            finally:
                reader.close()
            return emitted, late_count

        emitted, late_count = run_finite_job(totals_topic, late_topic)
        print("Finalized windows:", len(emitted), "late events:", late_count)
        """),
        code("""
        totals = [decode(m) for m in collect(totals_topic, count=5)]
        late_events = [decode(m) for m in collect(late_topic, count=1)]
        actual = {(x["customer_id"], x["window_start"]): x["total_paise"] for x in totals}
        expected = {("c1", 0): 30, ("c2", 0): 7, ("c1", 10): 30,
                    ("c1", 20): 40, ("c2", 30): 50}
        assert actual == expected
        assert late_events[0]["event_id"] == "e6"
        assert sum(x["event_count"] for x in totals) == 6
        print(actual)
        """),
        md("""
        **Observe:** six purchases contributed to five customer windows. The value 999 never
        enters a final total because its window was already finalized. A different lateness
        policy can produce different results from the same arrival sequence.
        """),
        md("""
        ## 6. Rebuild from the same group and compare the results

        The group has no committed offset, so `earliest` selects the beginning again. We use
        fresh output topics to make the two executions easy to compare. A real rebuild could
        write versioned output and atomically switch readers after verification.
        """),
        code("""
        rebuilt_topic = new_topic("09.rebuilt-totals", partitions=1,
                                  config={"cleanup.policy": "compact"})
        rebuilt_late_topic = new_topic("09.rebuilt-late", partitions=1)
        rebuilt, rebuilt_late_count = run_finite_job(rebuilt_topic, rebuilt_late_topic)
        observed_rebuild = [decode(m) for m in collect(rebuilt_topic, count=5)]
        by_id = lambda values: {x["window_id"]: x for x in values}
        assert by_id(observed_rebuild) == by_id(totals)
        assert rebuilt_late_count == late_count == 1
        print("Replayed input reconstructed identical window results.")
        """),
        md("""
        ## 7. From this demonstration to a durable stream processor

        Incremental recovery requires coordinating source offsets, window state, and output.
        A checkpoint file written separately from an offset commit leaves a crash gap. An
        output transaction alone does not preserve arbitrary in-memory Python state. Possible
        designs include a transactional database holding state and source positions, or a
        streaming runtime with a changelog/state-store recovery model.

        With multiple input partitions, using a single global maximum timestamp can close
        windows prematurely when another partition lags. Real watermarks usually account for
        per-partition progress and an explicit idle-partition policy. Validate future timestamps,
        handle duplicate event IDs, and budget state cardinality before deploying.
        """),
        md("""
        ## Practice

        1. Raise allowed lateness to 30 seconds and rerun the preview. Is `e6` still late?
        2. Move `e6` before `e5` and explain the changed result.
        3. Duplicate `e1`. Does this processor deduplicate it?
        4. Create more than 1,000 distinct open customer windows and observe the state cap.

        **Solution hints:** the first window stays open longer; arrival order affects finalization;
        this processor counts duplicate input again; the cap intentionally fails instead of
        silently using unlimited memory. A production deduplication store also needs retention.
        """),
        md("""
        ## Presenter checkpoint and references

        Ask the learner to explain event time, arrival order, a watermark, finalization, and why
        committing offsets without durable window state is unsafe.

        [Kafka Streams core concepts](https://kafka.apache.org/41/streams/core-concepts/)
        provides terminology; [Kafka log compaction](https://kafka.apache.org/41/design/design/#log-compaction)
        explains keyed retention. The code above is our Python implementation of the stated policy.
        """),
    ]
    write_notebook(ROOT / "notebooks/09_event_time_windows_and_state.ipynb", cells)


def build_operations():
    cells = [
        md("""
        # 10 · Inspect, measure, and operate Kafka

        **Level:** advanced · **Time:** 55–70 minutes · **Prerequisites:** notebooks 01–09.

        Inspect real topic metadata and configuration, measure committed consumer lag, compare
        batching and compression, and rebuild a keyed view with a tombstone. Finish by mapping
        the local lab to authenticated production connections and realistic failure drills.

        Start Kafka, restart the kernel, and run top to bottom. This is a **single-broker local
        lab**: it cannot demonstrate replica failover or durable operation after broker loss.
        """),
        code(BOOTSTRAP),
        code("""
        import json
        import os
        import time
        from confluent_kafka import KafkaError, KafkaException, TopicPartition
        from confluent_kafka.admin import AdminClient, ConfigResource, ResourceType

        require_broker()
        admin = AdminClient(client_config())
        observed_topic = new_topic("10.observe", partitions=3)
        """),
        md("""
        ## 1. Inspect leaders, replicas, and ISR

        A partition's leader serves its writes. Replicas hold copies, and ISR is the set of
        replicas currently considered in sync. One local replica explains why `acks=all` here
        still means just one broker acknowledged the record.
        """),
        code("""
        metadata = admin.list_topics(topic=observed_topic, timeout=10)
        for partition, info in sorted(metadata.topics[observed_topic].partitions.items()):
            print({"partition": partition, "leader": info.leader,
                   "replicas": info.replicas, "isr": info.isrs})
        assert len(metadata.topics[observed_topic].partitions) == 3
        """),
        code("""
        resource = ConfigResource(ResourceType.TOPIC, observed_topic)
        entries = admin.describe_configs([resource], request_timeout=10)[resource].result(timeout=15)
        for name in ["cleanup.policy", "retention.ms", "segment.bytes", "min.insync.replicas"]:
            entry = entries[name]
            print(name, "=", entry.value, "| source:", entry.source)
        """),
        md("""
        ## 2. Measure committed lag independently from the current position

        For an ordinary nontransactional topic, a useful approximation is
        `log-end offset − committed next offset`, per partition. A consumer's current position
        may be ahead of its committed offset. A negative committed offset is a sentinel for
        “no committed offset”, not a real negative record number.

        This is offset lag, not necessarily the exact count of application records: compaction,
        retention, and transaction control records can create gaps. Transactional visibility also
        has a last-stable-offset boundary. Lag in seconds needs event-age measurements too.
        """),
        code("""
        lag_topic = new_topic("10.lag", partitions=1)
        publish(lag_topic, [{"event_id": str(i)} for i in range(10)])
        lag_group = f"lab-10-lag-{uuid4().hex}"
        lag_reader = consumer(lag_group)
        try:
            lag_reader.subscribe([lag_topic])
            first_four = poll_records(lag_reader, count=4, timeout=20)
            lag_reader.commit(message=first_four[-1], asynchronous=False)
            next_two = poll_records(lag_reader, count=2, timeout=10)
            partition = TopicPartition(lag_topic, 0)
            low, high = lag_reader.get_watermark_offsets(partition, timeout=10, cached=False)
            committed_offset = lag_reader.committed([partition], timeout=10)[0].offset
            position = lag_reader.position([partition])[0].offset
            print({"low": low, "high": high, "committed": committed_offset,
                   "position": position, "committed_lag": high - committed_offset})
            assert (low, high, committed_offset, position) == (0, 10, 4, 6)
        finally:
            lag_reader.close()
        """),
        code("""
        remaining = collect(lag_topic, count=6, group_id=lag_group, commit=True)
        assert [decode(m)["event_id"] for m in remaining] == [str(i) for i in range(4, 10)]
        print("Restart began at the committed offset 4, replaying the two uncommitted reads.")
        """),
        md("""
        ## 3. Benchmark delivered records, not enqueue calls

        `produce()` normally enqueues work locally. Timing only that call measures queueing,
        not delivery. The small experiment below includes flush and successful delivery reports.
        We keep `acks=all` and idempotence enabled in every variant so reliability settings do
        not confound the comparison.

        Connection warm-up, CPU, Docker resource limits, message size, and noise dominate small
        runs. This is a measurement exercise, not a production sizing result or a promise that
        compression always helps. Compression effectiveness also depends on payload entropy.
        """),
        code("""
        N = 500
        payloads = [json.dumps({"event_id": str(i), "category": "demo", "body": "abc" * 100}).encode()
                    for i in range(N)]

        def benchmark(label, overrides):
            topic = new_topic(f"10.bench.{label}", partitions=3)
            p = producer(**overrides)
            # Warm metadata, so the timed region is less dominated by topic discovery.
            p.list_topics(topic=topic, timeout=10)
            delivered, errors = [], []
            def report(error, message):
                if error:
                    errors.append(str(error))
                else:
                    delivered.append(message.offset())
            started = time.perf_counter()
            try:
                for i, payload in enumerate(payloads):
                    p.produce(topic, key=str(i % 10), value=payload, on_delivery=report)
                    p.poll(0)
            finally:
                pending = p.flush(15)
            elapsed = time.perf_counter() - started
            assert not errors and pending == 0 and len(delivered) == N, (errors, pending)
            return {"variant": label, "records": N, "seconds": round(elapsed, 4),
                    "delivered_per_second": round(N / elapsed),
                    "application_bytes": sum(map(len, payloads))}
        """),
        code("""
        measurements = [
            benchmark("immediate", {"linger.ms": 0, "compression.type": "none"}),
            benchmark("batched", {"linger.ms": 20, "compression.type": "none"}),
            benchmark("batched-gzip", {"linger.ms": 20, "compression.type": "gzip"}),
        ]
        for row in measurements:
            print(row)
        """),
        md("""
        **Interpretation:** `application_bytes` is the uncompressed payload size, not measured
        network bytes. Batching can increase throughput while adding latency; gzip spends CPU
        to reduce suitable payloads. Repeat variants in shuffled order, separate warm-up, record
        latency percentiles and resource use, and include a consumer before making decisions.
        """),
        md("""
        ## 4. Build a keyed view and delete a key with a tombstone

        On a compacted topic, the key identifies the entity. A tombstone is a **null Kafka value**,
        not the JSON string `"null"`. Compaction eventually removes obsolete versions; it does
        not deduplicate records immediately or promise one record per key in a fresh read.
        """),
        code("""
        compacted_topic = new_topic("10.customer-view", partitions=1,
                                    config={"cleanup.policy": "compact"})
        compact_producer = producer()
        compact_errors = []
        def compact_report(error, message):
            if error:
                compact_errors.append(str(error))
        changes = [
            ("c1", {"name": "Asha", "tier": "basic"}),
            ("c1", {"name": "Asha", "tier": "gold"}),
            ("c2", {"name": "Ravi", "tier": "basic"}),
            ("c2", None),
        ]
        for key, value in changes:
            compact_producer.produce(compacted_topic, key=key,
                value=None if value is None else json.dumps(value).encode(),
                on_delivery=compact_report)
            compact_producer.poll(0)
        assert compact_producer.flush(15) == 0 and not compact_errors
        """),
        code("""
        # Capture an offset boundary, not an expected number of retained records.
        # Compaction may have removed older values while preserving their offset gaps.
        snapshot_reader = consumer(f"lab-10-snapshot-{uuid4().hex}",
                                   **{"enable.partition.eof": True})
        changes_read = []
        try:
            partition = TopicPartition(compacted_topic, 0)
            low, snapshot_end = snapshot_reader.get_watermark_offsets(partition, timeout=10)
            snapshot_reader.assign([TopicPartition(compacted_topic, 0, low)])
            reached_end = low >= snapshot_end
            deadline = time.monotonic() + 20
            while not reached_end and time.monotonic() < deadline:
                message = snapshot_reader.poll(0.5)
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        reached_end = message.offset() >= snapshot_end
                        continue
                    raise KafkaException(message.error())
                if message.offset() < snapshot_end:
                    changes_read.append(message)
                reached_end = message.offset() + 1 >= snapshot_end
            if not reached_end:
                raise TimeoutError("Did not reach the captured compacted-topic end offset.")
        finally:
            snapshot_reader.close()

        view = {}
        for message in changes_read:
            key = message.key().decode()
            if message.value() is None:
                view.pop(key, None)
            else:
                view[key] = decode(message)
        assert view == {"c1": {"name": "Asha", "tier": "gold"}}
        print("Retained changes read:", len(changes_read), "| captured end offset:", snapshot_end)
        print("Materialized view:", view)
        """),
        md("""
        The read stops at the end offset captured before polling. It can read all four records,
        or fewer if cleaning has already removed obsolete versions; both reconstruct the same
        current keyed view in this isolated example. An EOF event also lets the scan finish when
        offsets have gaps. This bound is not database-style snapshot isolation against concurrent
        writers or cleaning. Do not depend on a fixed cleaning delay or an exact record count. Tombstones
        can eventually expire; a slow or rebuilding consumer must respect the deletion-retention
        window. Combining compaction with time deletion changes rebuilding assumptions too.
        """),
        md("""
        ## 5. Inspect connection security without printing credentials

        The bundled Docker broker uses loopback plaintext for local learning. Authentication,
        TLS, and authorization must be configured on the broker or managed service as well as
        on clients. Client settings alone cannot secure a plaintext broker.

        The helper reads `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_MECHANISM`,
        `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`, and `KAFKA_SSL_CA_LOCATION` from
        the environment or local `.env`. Keep secret values out of notebook cells and outputs.
        """),
        code("""
        connection = client_config()
        print("Security protocol:", connection.get("security.protocol", "PLAINTEXT"))
        print("SASL mechanism configured:", "sasl.mechanism" in connection)
        print("Username configured:", bool(connection.get("sasl.username")))
        print("Password configured:", bool(connection.get("sasl.password")))
        print("Custom CA configured:", bool(connection.get("ssl.ca.location")))
        # Do not display the full configuration dictionary: it may contain secrets.
        """),
        md("""
        For an authorized SASL/SCRAM cluster, obtain the bootstrap address, CA requirements,
        and credentials from its administrator. Set `KAFKA_SECURITY_PROTOCOL=SASL_SSL` and
        the required mechanism, for example `SCRAM-SHA-512`, in your private environment.
        Then restart the kernel and run the preflight. Keep certificate verification enabled.
        Topic creation and transactions may need additional ACLs; a production application
        identity often should not have general administration permissions.

        This section is a configuration walkthrough, not an authenticated-broker exercise.
        Do not point these topic-creating notebooks at a shared production cluster.
        """),
        md("""
        ## 6. An operations drill you can explain and repeat

        In a terminal, `docker compose stop kafka` makes the local broker unavailable. Run
        `require_broker(timeout=3)` in a separate scratch cell and observe the bounded failure.
        Restart with `docker compose start kafka`, wait for health, then retry preflight. Do not
        execute this stop command while other notebooks or Django workers are using the broker.

        This is an outage-and-recovery drill, **not replica failover**. A real high-availability
        drill needs multiple brokers/controllers, replication and minimum-ISR settings, a measured
        leader election, and verification of writes before and after failure. Increasing the
        replication factor to 3 on this one-broker lab cannot create that capacity.
        """),
        md("""
        ## Practice

        1. Consume five records but commit only two; calculate both position lag and committed lag.
        2. Replace the repetitive benchmark body with random bytes and compare gzip again.
        3. Publish another `c1` update after the tombstone for `c2`; rebuild the keyed view.
        4. Write an alert specification for stalled progress, growing event age, delivery errors,
           disk pressure, unavailable partitions, and under-replicated partitions.

        **Solution hints:** on ten records the two lags are 5 and 8; incompressible data can cost
        CPU with little bandwidth saving; replay applies the latest value by key; each alert needs
        a threshold, duration, owner, and useful remediation steps.
        """),
        md("""
        ## Presenter checkpoint and references

        Explain why low lag does not prove correct business processing, why `acks=all` cannot
        compensate for a single replica, and why compaction is not immediate deduplication.

        [Python AdminClient and offsets](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html),
        [Kafka monitoring](https://kafka.apache.org/41/operations/monitoring/),
        [Kafka log compaction](https://kafka.apache.org/41/design/design/#log-compaction),
        [Kafka security](https://kafka.apache.org/41/security/overview/).
        """),
    ]
    write_notebook(ROOT / "notebooks/10_operations_performance_and_compaction.ipynb", cells)


if __name__ == "__main__":
    build_transactions()
    build_retries()
    build_windows()
    build_operations()
