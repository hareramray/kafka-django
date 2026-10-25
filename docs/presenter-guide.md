# Live demonstration guide

Run each planned notebook once before a session, using the same laptop and Docker stack.
Open Jupyter from the repository root, select the workshop kernel, and increase editor
font size. Keep a terminal ready with `docker compose ps`. Do not run a failure drill
while someone else is using that broker. Restart the kernel between lessons.

## Session 1 — first events (60–90 minutes)

Use 00, 01 and 02. Ask the audience to predict whether a second consumer can read an
already consumed record. Show delivery metadata, the bytes on the wire and two groups
reading the same event. Then show three partitions each containing offset zero.

Success: attendees can explain producer, topic, partition, offset and group, and can
write a producer and consumer themselves. Give the order-cancellation exercise as practice.

## Session 2 — reliability (90 minutes)

Use 03, 04 and 05. Trigger the insufficient-ISR delivery error and point out that a
drained producer queue is not proof of success. Reproduce the two crash windows around
an offset commit. End by showing partition reassignment after one group member closes.

Ask: “Where is the business side effect durable?” and “Which offset is the next one?”
Success: attendees can identify why early commit loses work and late commit permits
duplicates, without promising that retries alone solve either problem.

## Session 3 — contracts and error handling (90 minutes)

Use 06 and 08. Change the total from a decimal string to a number and watch validation
fail. Introduce a compatible optional field. Inject one transient and one permanent
error, inspect the DLQ envelope, repair the permanent error and replay it.

Ask what happens if the process crashes after the DLQ acknowledgement but before the
source commit. Success: attendees explain duplicate DLQ delivery and why replay requires
a deliberate idempotency policy.

## Session 4 — Django application (2–3 hours)

Use 11–14. The notebooks use Django's HTTP test client; no live server is required.
Show the actual service and model code, then the database rows before and after each step.
First show the `on_commit` lost-event gap, then the outbox solution. Publish the same
event twice and show two Kafka records but one projection effect.

The key explanation is the sequence of boundaries: database transaction, broker ACK,
publisher status update, consumer database transaction, and Kafka offset commit.
Each gap has a distinct failure mode. Request retries are a separate concern: the demo
API deliberately does not implement HTTP request idempotency keys.

## Session 5 — advanced processing (2 hours)

Use 07, 09 and 10. Show committed versus aborted output and offset transactions. Then
feed an out-of-order event into a window, explain the watermark, and compare position
lag with committed lag. Treat throughput results as laptop measurements, not capacity promises.

Success: attendees explain the scope of Kafka exactly-once processing and the additional
checkpoint/recovery design needed for durable stateful Python applications.

## Session 6 — optional failure lab (60–90 minutes)

Start the independent cluster using [replication.md](replication.md). Inspect ISR=3,
identify the leader, enable the explicit failure flag, and watch a new leader appear.
Contrast a topic requiring two ISR members with one requiring three while one broker
is unavailable. Restore the broker and inspect ISR recovery.

This stack combines broker/controller roles: stopping two nodes loses controller quorum.
Do not describe that as a clean test of only the topic's minimum ISR setting.

## When a demo fails

Read the first failing assertion and its expected record count. Check broker health,
the selected topic/group, and whether the kernel has stale state. Restarting a kernel
creates new topics; it does not erase old records. Keep output when investigating and
avoid claiming an expected failure was observed if the broker never connected.

## Practical assessment

Ask learners to build a shipment projection from a fresh topic. Require validation,
per-shipment ordering, one durable effect under duplicate delivery, bounded worker
shutdown, useful lag measurement and a documented failure/recovery sequence. Have them
demonstrate each property by injecting a failure or changing the input, not by slides alone.
