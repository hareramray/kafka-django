"""Authoring source for notebooks 00-06. Run: python -m scripts.build_foundation_notebooks."""
from pathlib import Path
from labtools.notebook_builder import BOOTSTRAP, code, md, write_notebook

OUT = Path(__file__).resolve().parents[1] / "notebooks"


def save(name, cells):
    write_notebook(OUT / name, cells)


save("00_start_here.ipynb", [
md("""
# 00 · Start here: your Kafka practice environment
**Level:** beginner · **Time:** 20–30 minutes · **Prerequisites:** basic Python, a terminal.

You will check your Python kernel, inspect the workshop, and verify a Kafka connection.
The running example throughout the course is an online shop: orders become events, and
independent consumers turn those events into useful views.

Run one cell at a time with **Shift+Enter**. Read its output before continuing. Use
**Kernel → Restart Kernel and Run All Cells** to repeat a lesson. Every lesson creates
fresh topic names; a fresh group reads retained records independently. Do not run
several copies of the same Django notebook against the same database concurrently.
"""),
md(r"""
## 1. Install once, from the project folder
Install Python 3.11–3.13 and Docker Desktop with Linux containers. Start Docker Desktop.
The commands below are **terminal commands**, not Python cells.

**Windows PowerShell** (activation is unnecessary):
```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
Copy-Item .env.example .env
docker compose up -d --wait
.\.venv\Scripts\python.exe -m ipykernel install --sys-prefix --name kafka-workshop --display-name "Python (Kafka Workshop)"
.\.venv\Scripts\python.exe -m jupyter lab
```
**macOS/Linux:** use `.venv/bin/python` in place of `.\.venv\Scripts\python.exe`,
and `cp .env.example .env`. Choose **Python (Kafka Workshop)** in the notebook kernel menu.
The initial image/package downloads require internet access. Python connects to
`localhost:9092`; containers use `kafka:19092` internally.
"""),
code(BOOTSTRAP),
md("""## 2. Confirm the kernel
`sys.executable` should point inside this project's `.venv`. If an import fails, choose
the correct kernel; installing into an unrelated terminal Python will not repair it."""),
code("""
import platform
from importlib.metadata import version
print("Python:", platform.python_version())
print("Kernel executable:", sys.executable)
for package in ["confluent-kafka", "Django", "jupyterlab", "jsonschema"]:
    print(f"{package}: {version(package)}")
assert sys.version_info >= (3, 11)
"""),
md("""
## 3. Learn the five nouns
| Term | In this workshop |
|---|---|
| Producer | Python/Django code sending an order event |
| Topic | Named log of related events |
| Partition | Ordered portion of a topic; unit of consumer parallelism |
| Offset | A record's position within **one** partition |
| Consumer group | Collaborating consumers with their own saved progress |

A Kafka read does not remove a record. Retention/compaction decides when data is removed.
Two groups can process the same events independently. Ordering is per partition, not global.
"""),
code("""
lesson_files = sorted((ROOT / "notebooks").glob("*.ipynb"))
for path in lesson_files:
    print(path.name)
print("Notebook count:", len(lesson_files))
"""),
md("""## 4. Check Docker without starting or stopping anything
Expected: a running, healthy `kafka` service. This diagnostic still works when Docker
is absent: it explains the next step instead of claiming that Kafka is running."""),
code("""
import shutil
import subprocess
if shutil.which("docker"):
    check = subprocess.run(["docker", "compose", "ps"], cwd=ROOT,
                           capture_output=True, text=True, timeout=20)
    print(check.stdout or check.stderr)
else:
    print("Docker CLI is not available. Install/start Docker Desktop, then reopen your terminal.")
"""),
md("""## 5. Ask Kafka for metadata
Expected: broker `1` and its advertised address. This notebook catches the connection
error for diagnosis. All subsequent Kafka labs require a successful connection."""),
code("""
BROKER_READY = False
try:
    metadata = require_broker(timeout=5)
    BROKER_READY = True
    print("Brokers:", {key: str(value) for key, value in metadata.brokers.items()})
except RuntimeError as error:
    print(error)
print("Ready for notebook 01:", BROKER_READY)
"""),
md("""
## 6. Choose a learning route
* **First working demo:** 00 → 01 → 02 → 03 → 04 (about 3 hours).
* **Application developer:** add 05 → 06 → 08 → 11 → 12 → 13 → 14.
* **Advanced practice:** complete 07 → 09 → 10 → 15 as well.

Allow roughly 12–16 hours for all guided labs, plus time for exercises. A notebook is
a starting point for practice, not a certification of production expertise. Kafka Connect,
Schema Registry deployment, cross-region replication and full cluster security are extension topics.
"""),
md("""
## Exercise
Explain why two partitions may both contain offset `0`. Then explain what happens if
two independent consumer groups subscribe to the same topic.

<details><summary>Solution hints</summary>
Offsets are scoped to a topic-partition. Each group has independent committed offsets,
so both groups can read the same retained events. Within a traditional consumer group,
one partition is assigned to at most one active member at a time.
</details>

## Troubleshooting and cleanup
* Connection refused: `docker compose ps`, then `docker compose logs --tail 80 kafka`.
* Kafka connects then fails: check advertised listeners; host clients need `localhost:9092`.
* Import error: check the printed kernel path and reinstall requirements into that interpreter.
* A polling timeout: check the expected count, group offsets, transaction isolation and broker health.
* Stop while keeping records: `docker compose down`.
* Delete **only this workshop's** broker data: `docker compose down -v` (deliberately destructive).

See [the Apache quickstart](https://kafka.apache.org/41/getting-started/quickstart/)
and [the Python client guide](https://docs.confluent.io/kafka-clients/python/current/overview.html).
Next: **01 — first producer and consumer**.
"""),
])

save("01_first_producer_consumer.ipynb", [
md("""
# 01 · Send your first event and read it back
**Level:** beginner · **Time:** 30–40 minutes · **Prerequisite:** notebook 00 connects to Kafka.

You will create a topic, serialize a Python dictionary, send it, inspect its delivery report,
and consume it twice with independent groups. All loops have deadlines.
"""), code(BOOTSTRAP), code("metadata = require_broker()\nprint('Connected to', len(metadata.brokers), 'broker(s)')"),
md("""## 1. Create a fresh topic
We start with one partition so the sequence is easy to see. Topic creation uses Kafka's
Admin API. Inspect `labtools/kafka.py` later; for now focus on the event lifecycle."""),
code("topic = new_topic('first-event', partitions=1)\nprint(topic)"),
md("""## 2. Build an event
An event describes something that happened. An `event_id` identifies this occurrence;
an `order_id` identifies the business entity. They serve different purposes."""),
code("""
from datetime import datetime, timezone
event = {
    "event_id": str(uuid4()), "event_type": "order.created", "schema_version": 1,
    "occurred_at": datetime.now(timezone.utc).isoformat(),
    "order_id": "order-1001", "customer_id": "customer-42", "total": "49.90",
}
event
"""),
md("""## 3. Serialize to bytes
Kafka stores bytes. JSON is our initial wire format. Money is a decimal string because
binary floating point can introduce rounding errors."""),
code("""
import json
payload = json.dumps(event, allow_nan=False).encode("utf-8")
assert json.loads(payload.decode("utf-8")) == event
print(type(payload).__name__, len(payload), "bytes")
"""),
md("""## 4. Produce and wait for acknowledgement
`produce()` queues locally. `poll()` and `flush()` serve callbacks; `flush()` returning
zero only means the queue drained. We also inspect callback errors before claiming success."""),
code("""
p = producer()
delivery_reports = []
def delivered(error, message):
    delivery_reports.append({"error": str(error) if error else None,
        "partition": message.partition(), "offset": message.offset()})
p.produce(topic, key=event["order_id"].encode(), value=payload, on_delivery=delivered)
p.poll(0)
remaining = p.flush(15)
assert remaining == 0, f"{remaining} records still queued"
assert len(delivery_reports) == 1 and delivery_reports[0]["error"] is None, delivery_reports
delivery_reports
"""),
md("""## 5. Consume with the underlying API
`earliest` applies only when this group has no usable saved offset. A fresh group is
essential to this demonstration. Close the consumer in `finally`, even when an assertion fails."""),
code("""
c = consumer(f"lab.first.{RUN}")
try:
    c.subscribe([topic])
    messages = poll_records(c, count=1, timeout=20)
    message = messages[0]
    received = decode(message)
    print("topic / partition / offset:", message.topic(), message.partition(), message.offset())
    print("key:", message.key().decode())
    assert received == event
    # The successful print/validation is our processing step for this introductory lab.
    c.commit(message=message, asynchronous=False)
finally:
    c.close()
received
"""),
md("""## 6. Read again with an independent group
The first read did not delete the event. This time use the small `collect` helper, which
subscribes, polls with a deadline and always closes its own consumer."""),
code("""
again = collect(topic, count=1, group_id=f"lab.audit.{RUN}")
assert decode(again[0]) == event
print("An independent group read the same event at offset", again[0].offset())
"""),
md("""## 7. A short batch
These are three new occurrences. We reuse the topic to show that the log appends."""),
code("""
batch = [{**event, "event_id": str(uuid4()), "order_id": f"order-{i}"} for i in range(1002, 1005)]
reports = publish(topic, batch, key_field="order_id")
print(reports)
all_messages = collect(topic, count=4)
assert [decode(m)["order_id"] for m in all_messages] == ["order-1001", "order-1002", "order-1003", "order-1004"]
assert [m.offset() for m in all_messages] == sorted(m.offset() for m in all_messages)
"""),
md("""
## Expected observation
Four events exist on one partition. The first consumer's group saved its progress;
the independent audit group still read from the beginning. A production handler must
complete its durable side effect before committing, a topic we revisit in Django.

## Exercise
Send an `order.cancelled` event for `order-1001`. Read all five events with a new group.
Which field stays the same, and which must change?
<details><summary>Solution hint</summary>
Reuse `order_id`, create a new `event_id`, change `event_type`, and publish with
`key_field='order_id'`. Collect five records using a new group. Do not assume a topic is a mutable table.
</details>

## Demonstrate aloud
Point out: dictionary → bytes → producer queue → broker acknowledgement → stored record → consumer.
Next: **02 — partitions, keys and ordering**.
"""),
])

save("02_partitions_keys_and_ordering.ipynb", [
md("""
# 02 · Partitions, keys and ordering
**Level:** beginner · **Time:** 40 minutes · **Prerequisite:** 01.
You will inspect partition metadata and test per-key order. You will also see why
adding partitions can change where future keyed events go.
"""), code(BOOTSTRAP), code("require_broker()\ntopic = new_topic('partitioning', partitions=3)\nprint(topic)"),
md("""## 1. Inspect the partition leaders
The one-broker setup has one replica per partition. Three partitions on one broker
increase logical parallelism; they do not provide broker-failure redundancy."""),
code("""
from confluent_kafka.admin import AdminClient
admin = AdminClient(client_config())
info = admin.list_topics(topic=topic, timeout=10).topics[topic]
for number, part in info.partitions.items():
    print({"partition": number, "leader": part.leader, "replicas": part.replicas, "isr": part.isrs})
assert len(info.partitions) == 3
"""),
md("""## 2. Key by the entity whose order matters
A stable nonempty key uses the client's partitioner to select a partition. Different keys
may collide on the same partition. This exercise does not assume a particular hash algorithm."""),
code("""
events = [{"event_id": str(uuid4()), "customer_id": customer, "sequence": sequence}
          for sequence in range(4) for customer in ["alice", "bob", "carol", "dev"]]
publish(topic, events, key_field="customer_id")
messages = collect(topic, count=len(events))
[(m.key().decode(), m.partition(), m.offset(), decode(m)["sequence"]) for m in messages]
"""),
md("""## 3. Test the invariant, not a lucky output order
The consumer can interleave records from different partitions. There is no single
global offset. For each customer, a single producer sends sequence 0, 1, 2, 3 in order."""),
code("""
from collections import defaultdict
by_customer = defaultdict(list)
for m in messages:
    by_customer[m.key().decode()].append((m.partition(), decode(m)["sequence"]))
for customer, rows in by_customer.items():
    assert len({partition for partition, _ in rows}) == 1
    assert [sequence for _, sequence in rows] == [0, 1, 2, 3]
    print(customer, rows)
"""),
md("""## 4. Explicit routing
Use explicit partitions to demonstrate their independent offsets. Most business producers
should rely on a deliberate key strategy instead of hard-coding partition numbers."""),
code("""
import json
explicit = new_topic("explicit-routing", partitions=3)
p = producer()
errors = []
def report(error, message):
    if error:
        errors.append(str(error))
for partition in range(3):
    for sequence in range(2):
        p.produce(explicit, partition=partition,
                  value=json.dumps({"partition_requested": partition, "sequence": sequence}).encode(),
                  on_delivery=report)
assert p.flush(15) == 0 and not errors, errors
rows = collect(explicit, count=6)
for partition in range(3):
    offsets = [m.offset() for m in rows if m.partition() == partition]
    assert offsets == [0, 1]
    print("Partition", partition, "has offsets", offsets)
"""),
md("""## 5. Observe a different partition count
Create a second topic with six partitions. Which keys move depends on hashing; no
particular key is guaranteed to move. Existing records never move merely because partitions are added."""),
code("""
larger = new_topic("partitioning-six", partitions=6)
publish(larger, events, key_field="customer_id")
larger_rows = collect(larger, count=len(events))
old_map = {m.key().decode(): m.partition() for m in messages}
new_map = {m.key().decode(): m.partition() for m in larger_rows}
for key in sorted(old_map):
    print(key, "3 partitions:", old_map[key], "6 partitions:", new_map[key])
"""),
md("""
## Expected observation
Each nonempty key stays on one partition for a fixed topic partition count and partitioner.
Every partition has its own offset sequence. Multiple producers or a partition-count
change require additional business sequencing if strict entity order is essential.

## Exercise
Choose a key for (a) a bank account ledger, (b) shipment tracking, and (c) click analytics.
Then modify the dataset so 95% of records use one customer key. Explain the bottleneck.
<details><summary>Solution hints</summary>
Account ID, shipment ID, and a carefully chosen user/session key are possible choices.
One hot key stays on one partition, so simply adding group members does not split its load.
Salting a key spreads load but gives up straightforward per-entity ordering.
</details>
Next: **03 — delivery acknowledgements and producer failures**.
"""),
])

save("03_reliable_producers.ipynb", [
md("""
# 03 · Reliable producers: acknowledgements, errors and batching
**Level:** beginner → intermediate · **Time:** 40 minutes · **Prerequisite:** 02.
Distinguish local enqueue, successful delivery and business success. Handle malformed input,
observe an actual broker delivery failure, and attach trace metadata.
"""), code(BOOTSTRAP), code("require_broker()\ntopic = new_topic('producer-reliability', partitions=1)"),
md("""## 1. Name your guarantees
Our producer explicitly enables idempotence and `acks=all`. Idempotence protects against
duplicates caused by protocol retries within the producer session. Sending the same business
event twice is still two sends. `acks=all` acknowledges the in-sync replica set; with one
replica it is not protection against losing that broker's disk."""),
code("""
import json
delivery_log = []
p = producer(**{"linger.ms": 10, "compression.type": "gzip"})
def report(error, message):
    delivery_log.append({"error": str(error) if error else None,
                         "partition": message.partition(), "offset": message.offset()})
for sequence in range(10):
    event = {"event_id": str(uuid4()), "order_id": "order-batched", "sequence": sequence}
    p.produce(topic, key=b"order-batched", value=json.dumps(event).encode(),
              headers=[("trace-id", RUN.encode()), ("content-type", b"application/json")],
              on_delivery=report)
    p.poll(0)
print("Queued ten calls; final delivery status follows in the next cell.")
"""),
md("""## 2. Drain and inspect both signals
Batching amortizes network overhead. Flushing after every record would defeat much of
the batching. A full local queue raises `BufferError`; poll with a deadline and retry the
enqueue, while leaving delivery retries to the client."""),
code("""
pending = p.flush(15)
failures = [entry for entry in delivery_log if entry["error"]]
assert pending == 0 and not failures, (pending, failures)
assert len(delivery_log) == 10
delivery_log
"""),
md("""## 3. Read trace headers
Headers are metadata; Kafka does not validate their meaning. Avoid putting passwords
or sensitive personal data into trace metadata."""),
code("""
messages = collect(topic, count=10)
print(messages[0].headers())
assert dict(messages[0].headers())["trace-id"] == RUN.encode()
assert [decode(m)["sequence"] for m in messages] == list(range(10))
"""),
md("""## 4. Fail before enqueueing bad JSON
Validate and serialize before calling the producer. This error is local; it has no
delivery report because the producer never received bytes."""),
code("""
from decimal import Decimal
try:
    json.dumps({"total": Decimal("19.99")})
except TypeError as error:
    print("Expected serialization failure:", error)
fixed = {"total": str(Decimal("19.99"))}
assert json.loads(json.dumps(fixed)) == {"total": "19.99"}
"""),
md("""## 5. Trigger a real delivery failure without stopping the broker
Create an isolated topic with `min.insync.replicas=2`, although it has only one replica.
An `acks=all` producer cannot satisfy this requirement. The failure is deliberately bounded.
This demonstrates the configuration constraint; notebook 15 tests actual replica failure."""),
code("""
strict_topic = new_topic("insufficient-isr", partitions=1,
                         config={"min.insync.replicas": "2"})
bad_reports = []
strict_producer = producer(**{"delivery.timeout.ms": 4000, "request.timeout.ms": 1000})
def failed_delivery(error, message):
    bad_reports.append(str(error) if error else None)
strict_producer.produce(strict_topic, value=b'{"example":"cannot-ack"}', on_delivery=failed_delivery)
pending = strict_producer.flush(8)
assert pending == 0 and len(bad_reports) == 1 and bad_reports[0] is not None, bad_reports
print("Expected delivery error:", bad_reports[0])
"""),
md("""## 6. Business duplicates still need business handling
A network retry and a second application `produce()` call are different things."""),
code("""
duplicate_topic = new_topic("application-duplicates", partitions=1)
same_event = {"event_id": str(uuid4()), "order_id": "order-duplicate", "total": "12.00"}
publish(duplicate_topic, [same_event, same_event], key_field="order_id")
duplicates = collect(duplicate_topic, count=2)
assert decode(duplicates[0])["event_id"] == decode(duplicates[1])["event_id"]
assert duplicates[0].offset() != duplicates[1].offset()
print("Two sends created two records. Django lab 12 will deduplicate their effects.")
"""),
md("""
## Exercise
Predict the result if you only check `flush() == 0` in step 5. Add a callback that counts
successes and failures separately. What should an HTTP endpoint report if its database
commit succeeded but its Kafka delivery failed?
<details><summary>Solution hints</summary>
The queue can drain with failures. Inspect both pending count and delivery errors.
A DB commit cannot be undone by a later Kafka failure; use an outbox and return the
stored order's status. A blind HTTP retry can create another order without request idempotency.
</details>
Next: **04 — offsets, commits and replay**.
"""),
])

save("04_offsets_commits_and_replay.ipynb", [
md("""
# 04 · Offsets, commits, crash windows and replay
**Level:** intermediate · **Time:** 45 minutes · **Prerequisite:** 03.
You will restart a consumer under the same group, prove that commits mean the **next**
offset, and reproduce both duplicate processing and skipped work using real Kafka offsets.
One partition keeps the experiment unambiguous; production commits are per partition.
"""), code(BOOTSTRAP), code("require_broker()\ntopic = new_topic('offsets', partitions=1)\npublish(topic, [{'number': i} for i in range(5)])"),
md("""## 1. Process two records, then commit
Auto commit and auto offset storage are disabled. Commit the message only after the
handler succeeds. A consumer's fetch position can be ahead of its durable committed offset."""),
code("""
from confluent_kafka import TopicPartition
group = f"lab.offsets.{RUN}"
c = consumer(group)
try:
    c.subscribe([topic])
    first = poll_records(c, count=2)
    for m in first:
        print("Processed", decode(m))
        c.commit(message=m, asynchronous=False)
    saved = c.committed([TopicPartition(topic, 0)], timeout=10)[0].offset
    print("Committed next offset:", saved)
    assert saved == first[-1].offset() + 1 == 2
finally:
    c.close()
"""),
md("""## 2. Restart the same group
Despite `auto.offset.reset=earliest`, a valid committed offset takes priority."""),
code("""
c = consumer(group)
try:
    c.subscribe([topic])
    rest = poll_records(c, count=3)
    assert [decode(m)["number"] for m in rest] == [2, 3, 4]
    for m in rest:
        c.commit(message=m, asynchronous=False)
    print("Resumed at", rest[0].offset())
finally:
    c.close()
"""),
md("""## 3. Reproduce the at-least-once crash window
The business operation below is a list append. Close without committing to model a
crash **after** the side effect but **before** the offset commit. Closing is deterministic
here because auto commit is disabled; it is not a process-kill test."""),
code("""
crash_group = f"lab.crash.{RUN}"
side_effects = []
c = consumer(crash_group)
try:
    c.subscribe([topic])
    m = poll_records(c, count=1)[0]
    side_effects.append(decode(m)["number"])
    print("Side effect completed; intentionally omit commit.")
finally:
    c.close()
c = consumer(crash_group)
try:
    c.subscribe([topic])
    m = poll_records(c, count=1)[0]
    side_effects.append(decode(m)["number"])
    c.commit(message=m, asynchronous=False)
finally:
    c.close()
assert side_effects == [0, 0]
print("Duplicate effect:", side_effects)
"""),
md("""## 4. Reproduce the at-most-once crash window
Commit first, then omit the business operation. Restarting skips that operation.
This is why moving the commit earlier is not a general fix for duplicate effects."""),
code("""
skip_group = f"lab.skip.{RUN}"
c = consumer(skip_group)
try:
    c.subscribe([topic])
    skipped = poll_records(c, count=1)[0]
    c.commit(message=skipped, asynchronous=False)
    # Deliberately no business operation after the commit.
finally:
    c.close()
c = consumer(skip_group)
try:
    c.subscribe([topic])
    next_message = poll_records(c, count=1)[0]
    assert decode(next_message)["number"] == 1
    print("Record 0's operation was skipped; restart received", decode(next_message))
finally:
    c.close()
"""),
md("""## 5. Replay explicitly with manual assignment
`assign()` bypasses group subscription assignment. Use a separate replay group and do
not reset an active production group's offsets. `seek()` changes the current position,
not the persisted committed offset."""),
code("""
c = consumer(f"lab.replay.{RUN}")
try:
    c.assign([TopicPartition(topic, 0, 0)])
    replay = poll_records(c, count=5)
    assert [decode(m)["number"] for m in replay] == list(range(5))
    c.seek(TopicPartition(topic, 0, 3))
    tail = poll_records(c, count=2)
    assert [decode(m)["number"] for m in tail] == [3, 4]
    print("Replay tail:", [decode(m) for m in tail])
finally:
    c.close()
"""),
md("""
## Exercise
Replace the list side effect with a dictionary keyed by event ID. Why is this insufficient
when the process restarts? What two writes must be in the same database transaction?
<details><summary>Solution hints</summary>
An in-memory dictionary disappears. Persist a unique processed-event ID and the business
effect atomically, then commit Kafka. On replay the unique ID suppresses the repeated effect.
Notebook 12 implements this in Django. Never commit a later message on a partition while
an earlier message on that partition remains unsuccessfully processed.
</details>
Next: **05 — groups and rebalancing**.
"""),
])

save("05_consumer_groups_and_rebalancing.ipynb", [
md("""
# 05 · Consumer groups, parallelism and rebalancing
**Level:** intermediate · **Time:** 45 minutes · **Prerequisite:** 04.
Start two real consumers in one group, inspect ownership, stop a member, and observe
the remaining member take over. This lesson uses the classic group protocol and eager
`range` assignor explicitly; cooperative and the newer consumer protocol differ.
"""), code(BOOTSTRAP), code("require_broker()\ntopic = new_topic('groups', partitions=3)"),
md("""## 1. Capture rebalance callbacks
We poll both consumers from one notebook thread for a small bounded demonstration.
In services, give each worker its own Consumer and polling loop. Callbacks show assignment
changes; the default client action applies the assignment after the callback returns."""),
code("""
from time import monotonic
events = []
def assigned(label):
    def callback(c, partitions):
        events.append((label, "assigned", [p.partition for p in partitions]))
    return callback
def revoked(label):
    def callback(c, partitions):
        events.append((label, "revoked", [p.partition for p in partitions]))
    return callback
group = f"lab.team.{RUN}"
group_config = {"group.protocol": "classic", "partition.assignment.strategy": "range",
                "session.timeout.ms": 6000, "heartbeat.interval.ms": 2000}
"""),
md("""## 2. Wait for stable assignments before producing
The polling here is not processing business events: the fresh topic is still empty.
The assertions require all three partitions to be split without overlap."""),
code("""
c1 = consumer(group, **group_config)
c2 = consumer(group, **group_config)
try:
    c1.subscribe([topic], on_assign=assigned("A"), on_revoke=revoked("A"))
    c2.subscribe([topic], on_assign=assigned("B"), on_revoke=revoked("B"))
    deadline = monotonic() + 30
    while monotonic() < deadline:
        c1.poll(0.1)
        c2.poll(0.1)
        a = {p.partition for p in c1.assignment()}
        b = {p.partition for p in c2.assignment()}
        if a and b and not a & b and a | b == {0, 1, 2}:
            break
    else:
        raise TimeoutError("Group did not settle; inspect broker/group logs.")
    print("A owns", sorted(a), "B owns", sorted(b))

    # Produce to every partition so both members definitely have work.
    import json
    p = producer()
    delivery_errors = []
    def report(error, message):
        if error:
            delivery_errors.append(str(error))
    for partition in range(3):
        for number in range(3):
            p.produce(topic, partition=partition,
                      value=json.dumps({"id": f"{partition}-{number}"}).encode(), on_delivery=report)
    assert p.flush(15) == 0 and not delivery_errors, delivery_errors
    seen = {"A": [], "B": []}
    deadline = monotonic() + 20
    while sum(map(len, seen.values())) < 9 and monotonic() < deadline:
        for label, c in [("A", c1), ("B", c2)]:
            m = c.poll(0.1)
            if m is None:
                continue
            if m.error():
                raise RuntimeError(m.error())
            seen[label].append((m.partition(), decode(m)["id"]))
            c.commit(message=m, asynchronous=False)
    ids = [event_id for rows in seen.values() for _, event_id in rows]
    assert len(ids) == len(set(ids)) == 9, seen
    print("Work by member:", seen)

    # Graceful departure triggers reassignment; no processing is in flight.
    c2.close()
    c2 = None
    deadline = monotonic() + 30
    while monotonic() < deadline:
        c1.poll(0.2)
        if {p.partition for p in c1.assignment()} == {0, 1, 2}:
            break
    else:
        raise TimeoutError("Remaining member did not acquire all partitions.")
    print("After B leaves, A owns:", [p.partition for p in c1.assignment()])
finally:
    c1.close()
    if c2 is not None:
        c2.close()
"""),
md("""## 3. Inspect the timeline
There can be more than one assignment callback during startup. A rebalance changes
ownership; it does not erase stored offsets. This clean run has no duplicate work,
but failures between processing and commit can still cause duplicates."""),
code("for entry in events:\n    print(entry)"),
md("""## 4. An independent group sees the complete stream
This is useful for analytics and audit consumers that must not share work with each other."""),
code("""
audit = collect(topic, count=9, group_id=f"lab.group-audit.{RUN}")
assert {decode(m)["id"] for m in audit} == {f"{partition}-{number}" for partition in range(3) for number in range(3)}
print("Audit group read", len(audit), "records")
"""),
md("""
## 5. Reason about scaling
| Partitions | Active group members | Maximum members with assignments |
|---:|---:|---:|
| 3 | 1 | 1 |
| 3 | 2 | 2 |
| 3 | 5 | 3 |

Increasing members beyond partition count leaves members idle in a traditional consumer
group. Slow processing can exceed `max.poll.interval.ms`, trigger reassignment and make
old work unsafe to commit. Increasing that timeout hides symptoms if the underlying
work is unbounded. Monitor lag, handler duration and rebalance frequency together.

## Exercise
Change the topic to two partitions and add a third consumer. Poll all three and print
their assignments. Then stop the member owning a partition. What changes?
<details><summary>Solution hints</summary>
One member has no partitions. Close the chosen consumer, continue polling the others,
and wait with a deadline for the two partitions to be reassigned. Keep `try/finally`
around all clients. Do not copy the polling-only assignment loop onto a nonempty topic:
it would discard fetched records without processing them.
</details>
Next: **06 — event contracts and schema evolution**.
"""),
])

save("06_event_contracts_and_schema_evolution.ipynb", [
md("""
# 06 · Event contracts and schema evolution
**Level:** intermediate · **Time:** 45 minutes · **Prerequisite:** 03–05.
Define a JSON Schema, reject invalid messages, and test a compatible field addition.
This lab validates locally. It does **not** deploy a Schema Registry or use its wire format.
Kafka itself does not enforce this schema. The Django event contract is introduced in lab 11.
"""), code(BOOTSTRAP),
md("""## 1. Write the contract before the producer
Use a stable event ID, explicit type/version, UTC timestamp, and exact money representation.
Allowing unknown fields gives old readers room to accept additive changes."""),
code(r"""
from jsonschema import Draft202012Validator, FormatChecker
schema_v1 = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": ["event_id", "event_type", "schema_version", "occurred_at", "order_id", "total"],
    "properties": {
        "event_id": {"type": "string", "format": "uuid"},
        "event_type": {"const": "order.created"},
        "schema_version": {"const": 1},
        "occurred_at": {"type": "string", "format": "date-time"},
        "order_id": {"type": "string", "minLength": 1},
        "total": {"type": "string", "pattern": r"^[0-9]+\.[0-9]{2}$"},
    },
    "additionalProperties": True,
}
Draft202012Validator.check_schema(schema_v1)
validator_v1 = Draft202012Validator(schema_v1, format_checker=FormatChecker())
"""),
code("""
from datetime import datetime, timezone
good = {"event_id": str(uuid4()), "event_type": "order.created", "schema_version": 1,
        "occurred_at": datetime.now(timezone.utc).isoformat(), "order_id": "order-contract", "total": "25.00"}
validator_v1.validate(good)
print("Valid event:", good)
"""),
md("""## 2. Fail early with useful errors
An integer or float total is not the agreed wire representation. Schema validation is
separate from business validation such as an upper limit or a permitted currency."""),
code("""
bad = {**good, "event_id": "not-a-uuid", "total": 25.0}
errors = sorted(validator_v1.iter_errors(bad), key=lambda error: str(error.path))
for error in errors:
    print(list(error.path), error.message)
assert len(errors) == 2
"""),
md("""## 3. Add an optional field
Here the optional currency has a documented default of USD. JSON Schema's `default`
is an annotation: it does not mutate the event. Consumers must supply the default.
This compatible addition stays on schema version 1; a breaking major version needs a migration."""),
code("""
from copy import deepcopy
schema_v1_1 = deepcopy(schema_v1)
schema_v1_1["properties"]["currency"] = {"type": "string", "enum": ["USD", "INR", "EUR"], "default": "USD"}
validator_new = Draft202012Validator(schema_v1_1, format_checker=FormatChecker())
new_event = {**good, "event_id": str(uuid4()), "currency": "INR"}
# Old reader accepts a new writer; new reader accepts an old writer.
validator_v1.validate(new_event)
validator_new.validate(good)
assert "currency" not in good
assert good.get("currency", "USD") == "USD"
print("Both sample compatibility directions pass.")
"""),
md("""## 4. Show a breaking change
Renaming `total` to `amount` removes a required field. Testing two examples is useful
but not a formal proof of compatibility for all values. Registry compatibility rules
depend on the schema format, policy and reader/writer semantics."""),
code("""
breaking = {key: value for key, value in good.items() if key != "total"}
breaking["amount"] = "25.00"
assert not validator_v1.is_valid(breaking)
print([error.message for error in validator_v1.iter_errors(breaking)])
"""),
md("""## 5. Validate both sides of a real Kafka exchange
Validate producer input, then validate the bytes decoded by the consumer. A producer
outside your codebase might send an incompatible event, so consumer validation matters too."""),
code("""
require_broker()
topic = new_topic("contracts", partitions=1)
for event in [good, new_event]:
    validator_new.validate(event)
publish(topic, [good, new_event], key_field="order_id")
received = [decode(m) for m in collect(topic, count=2)]
for event in received:
    validator_new.validate(event)
    print(event["order_id"], event["total"], event.get("currency", "USD"))
"""),
md("""
## Exercise
Make `currency` required and rerun the old-writer validation. Then design a migration
from `total` to `amount`: consider dual fields, a new event type/topic, and rollout order.
<details><summary>Solution hints</summary>
Requiring currency breaks old events lacking it. During migration, old producers may
still run and retained events may be replayed. Update readers first, test historical
fixtures, then update writers. Remove legacy fields only after a deliberate compatibility window.
</details>

## Extension: Schema Registry
A registry stores schemas and applies configured compatibility checks. Its serializers
typically include a schema identifier in a defined wire format; bare JSON in this lesson
is not automatically compatible with those serializers. Add a registry as a separate exercise.
See [Confluent's serialization APIs](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html).
Next: **07 — Kafka transactions**, or **08 — retries and dead-letter topics**.
"""),
])
