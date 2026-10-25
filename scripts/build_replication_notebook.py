"""Generate the optional, separate-cluster replication workshop notebook."""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from labtools.notebook_builder import BOOTSTRAP, code, md, write_notebook


cells = [
    md("""
    # 15 · Replication, leader failover, and durability

    **Level:** expert operations · **Time:** 45-60 minutes

    **Prerequisites:** notebooks 01-05 and 07; Docker Desktop; this workshop's optional
    three-broker cluster. This notebook is excluded from the normal single-broker run.
    Python still runs on your host. Read `docs/replication.md` before the experiment.

    **By the end you will:** inspect replicas and ISR, stop a partition leader, observe
    election, distinguish `acks=all` from `min.insync.replicas`, and verify recovery.
    These are real broker operations. The failure experiment requires an explicit
    flag in Step 2; the default runs healthy-cluster checks only.
    """),
    md("""
    ## Step 1 · Start an independent cluster

    Run these commands in a terminal **from the repository root**:

    ```powershell
    docker compose -p kafka-expert -f compose.cluster.yaml up -d --wait
    docker compose -p kafka-expert -f compose.cluster.yaml ps
    ```

    Expect three healthy services. Their host ports are 19092, 29092, and 39092,
    so the beginner broker on 9092 can remain running. Named volumes preserve logs
    across restart. First startup can take a few minutes.
    """),
    code(BOOTSTRAP),
    md("""
    ## Step 2 · Select the experiment

    Leave `RUN_FAILOVER = False` for a healthy-cluster walkthrough. Set it to `True`
    to let Step 9 temporarily stop **one service in the kafka-expert project**.
    A `finally` block restores it. If the kernel or host dies, run the recovery
    command in Step 11 manually. Restart the kernel before repeating a full demo.

    We construct an explicit local configuration, so the credentials or bootstrap
    servers from a different workshop `.env` cannot redirect the experiment.
    """),
    code("""
    import json
    import shutil
    import subprocess
    import time
    from confluent_kafka import Producer, Consumer, TopicPartition, KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, ResourceType

    RUN_FAILOVER = False
    CLUSTER = "localhost:19092,localhost:29092,localhost:39092"
    LOCAL = {"bootstrap.servers": CLUSTER, "security.protocol": "PLAINTEXT"}
    compose = ["docker", "compose", "-p", "kafka-expert", "-f",
               str(ROOT / "compose.cluster.yaml")]
    cluster_admin = AdminClient(LOCAL)
    metadata = cluster_admin.list_topics(timeout=10)
    assert set(metadata.brokers) == {1, 2, 3}, "Start all three expert brokers first."
    print({node_id: f"{node.host}:{node.port}"
           for node_id, node in metadata.brokers.items()})
    """),
    md("""
    ## Step 3 · Separate the two kinds of redundancy

    A **partition leader** handles writes for that partition. Its **replicas** are
    assigned copies; its **ISR** is the subset currently sufficiently caught up.
    A **KRaft controller quorum** maintains cluster metadata, including leadership.

    This learning cluster runs broker and controller roles together on three nodes.
    Stopping one leaves two controllers, enough for a majority. Stopping two would
    lose that majority as well as replicas, which would confound an ISR experiment.
    This is one laptop, so it does not test rack, disk, or regional independence.
    """),
    code("""
    # Fresh topic names isolate this complete run from earlier demonstrations.
    available_topic = f"lab.replication.available.{RUN}"
    strict_topic = f"lab.replication.strict.{RUN}"
    creations = cluster_admin.create_topics([
        NewTopic(available_topic, num_partitions=1, replication_factor=3,
                 config={"min.insync.replicas": "2"}),
        NewTopic(strict_topic, num_partitions=1, replication_factor=3,
                 config={"min.insync.replicas": "3"}),
    ], request_timeout=15)
    for name, future in creations.items():
        future.result(timeout=20)
        print("Created", name)
    """),
    md("""
    ## Step 4 · Inspect actual leaders and ISR

    Both topics have one partition and three replicas. They differ only in minimum
    ISR: one permits writes with two synchronized replicas; the other requires all
    three. Leadership and replica ordering are assigned by Kafka, so do not hardcode
    a leader ID. Wait for readiness instead of assuming topic creation means all
    replicas have already joined the ISR.
    """),
    code("""
    def partition_state(topic):
        meta = cluster_admin.list_topics(topic=topic, timeout=3)
        topic_meta = meta.topics.get(topic)
        if topic_meta is None or topic_meta.error:
            raise RuntimeError(f"Topic not ready: {topic}")
        partition = topic_meta.partitions[0]
        return {"leader": partition.leader,
                "replicas": list(partition.replicas), "isr": list(partition.isrs)}

    def wait_for_isr(topic, expected, *, previous_leader=None, timeout=50):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            try:
                last = partition_state(topic)
                leader_ok = (last["leader"] >= 0 and
                             last["leader"] != previous_leader)
                if len(last["isr"]) == expected and leader_ok:
                    return last
            except (KafkaException, RuntimeError) as exc:
                last = str(exc)
            time.sleep(0.5)
        raise TimeoutError(f"ISR did not reach {expected}: {topic}, last={last}")

    initial = wait_for_isr(available_topic, 3)
    strict_initial = wait_for_isr(strict_topic, 3)
    assert set(initial["replicas"]) == {1, 2, 3}
    print("Available topic:", initial)
    print("Strict topic:", strict_initial)
    """),
    code("""
    resources = [ConfigResource(ResourceType.TOPIC, name)
                 for name in (available_topic, strict_topic)]
    described = cluster_admin.describe_configs(resources, request_timeout=10)
    for resource, future in described.items():
        configs = future.result(timeout=15)
        print(resource.name, "min.insync.replicas =",
              configs["min.insync.replicas"].value)
    """),
    md("""
    ## Step 5 · Use delivery callbacks as evidence

    `produce()` queues locally. Only a successful delivery callback tells us Kafka
    acknowledged the record. With `acks=all`, all current ISR replicas acknowledge;
    it does not mean every configured replica is always available. Minimum ISR sets
    how small the ISR may become before those writes are rejected.

    Idempotence handles retries within this producer session. It cannot identify
    two deliberate application calls as the same business event. A timed-out record
    may have an uncertain outcome, so never infer "definitely absent" from timeout.
    """),
    code("""
    reliable = Producer({**LOCAL, "acks": "all", "enable.idempotence": True,
                         "delivery.timeout.ms": 15000})

    def send_acknowledged(p, topic, event, timeout=20):
        outcomes = []
        p.produce(topic, partition=0, key=event["event_id"].encode(),
                  value=json.dumps(event).encode(),
                  on_delivery=lambda error, message: outcomes.append(
                      (error, message.partition(), message.offset())))
        remaining = p.flush(timeout)
        if remaining or not outcomes:
            raise TimeoutError("Delivery unfinished; outcome is uncertain.")
        error, partition, offset = outcomes[0]
        if error is not None:
            raise KafkaException(error)
        return {"event_id": event["event_id"], "partition": partition, "offset": offset}

    acknowledged = []
    for number in range(3):
        event = {"event_id": f"{RUN}-before-{number}", "phase": "before"}
        acknowledged.append(send_acknowledged(reliable, available_topic, event))
    print(acknowledged)
    """),
    md("""
    **Expected:** three acknowledged records at offsets 0, 1, and 2 in this new,
    nontransactional partition. These are known successful writes we will read after
    recovery. The stricter topic also accepts writes while all three replicas are in ISR.
    """),
    code("""
    strict_success = send_acknowledged(
        reliable, strict_topic,
        {"event_id": f"{RUN}-strict-healthy", "phase": "healthy"})
    print("Strict topic while all three are healthy:", strict_success)
    """),
    md("""
    ## Step 6 · Predict before stopping anything

    Write down your prediction:

    1. Will the available topic's leader ID change if its service stops?
    2. Will the topic with minimum ISR 2 accept a new write after election?
    3. Will the topic with minimum ISR 3 accept one while only two ISR remain?
    4. Does a failed producer acknowledgment prove a record never entered a log?

    The next cell displays the exact service and scoped Docker command. Service
    names map to broker IDs in `compose.cluster.yaml`; they are not container names
    guessed from another project.
    """),
    code("""
    leader_before = partition_state(available_topic)["leader"]
    assert leader_before in {1, 2, 3}
    stopped_service = f"broker{leader_before}"
    print("Current leader:", leader_before)
    print("Stop command:", " ".join(compose + ["stop", "--timeout", "5", stopped_service]))
    print("Failover enabled:", RUN_FAILOVER)
    """),
    md("""
    ## Step 7 · Define a bounded failure experiment

    The function stops only that one service. It waits for a different leader and
    two-member ISR, publishes on the available topic, and tries the stricter topic.
    The broker may log expected connection and insufficient-replica errors.

    The strict write uses a short deadline. Expect an error, commonly a timeout
    after retriable insufficient-replica responses. We report the actual result.
    Recovery runs in `finally`, even if an assertion or delivery fails. The source
    below is deliberately visible so you can explain the lifecycle while teaching.
    """),
    code("""
    def failover_experiment():
        if shutil.which("docker") is None:
            raise RuntimeError("Docker CLI is required for the opt-in failover exercise.")
        failure_result = None
        try:
            subprocess.run(compose + ["stop", "--timeout", "5", stopped_service],
                           cwd=ROOT, check=True, timeout=30)
            elected = wait_for_isr(available_topic, 2, previous_leader=leader_before)
            strict_degraded = wait_for_isr(strict_topic, 2)
            print("After stopping the leader:", elected)
            print("Strict topic ISR:", strict_degraded)
            for number in range(3):
                event = {"event_id": f"{RUN}-during-{number}", "phase": "during"}
                acknowledged.append(send_acknowledged(reliable, available_topic, event))

            # No successful write should be acknowledged while minISR=3 and ISR=2.
            strict_producer = Producer({**LOCAL, "acks": "all",
                                       "enable.idempotence": True,
                                       "delivery.timeout.ms": 5000})
            try:
                send_acknowledged(strict_producer, strict_topic,
                                  {"event_id": f"{RUN}-strict-degraded",
                                   "phase": "degraded"}, timeout=10)
            except (KafkaException, TimeoutError) as exc:
                failure_result = str(exc)
                print("Expected strict-topic delivery failure:", failure_result)
            else:
                raise AssertionError("Unexpected acknowledgment: inspect ISR and topic configuration.")
            finally:
                strict_producer.flush(1)
        finally:
            # A kernel/host crash cannot execute finally: manual recovery is below.
            subprocess.run(compose + ["start", stopped_service],
                           cwd=ROOT, check=True, timeout=30)
            restored = wait_for_isr(available_topic, 3)
            wait_for_isr(strict_topic, 3)
            print("Restored all replicas:", restored)
        return failure_result
    """),
    md("""
    ## Step 8 · Pause for your audience

    Explain what the ISR counts mean before executing the next cell. The experiment
    normally takes tens of seconds. A graceful Docker stop triggers leader failover;
    it is not a full test of sudden power loss or storage corruption. Each wait has
    a deadline, so a broken cluster produces an error instead of a hidden infinite loop.
    """),
    md("""
    ## Step 9 · Run the opt-in experiment

    With the default flag this prints a skip message. For the real failure demo,
    set the flag in Step 2 to `True` and run this cell. Do not run this cell twice
    within one run: the application would intentionally append the same event IDs
    again, which producer idempotence does not deduplicate.
    """),
    code("""
    failover_ran = False
    if RUN_FAILOVER:
        strict_error = failover_experiment()
        failover_ran = True
        assert strict_error
    else:
        print("Failover skipped. Set RUN_FAILOVER=True for broker-stop and recovery evidence.")
    """),
    md("""
    ## Step 10 · Verify known acknowledgments after recovery

    A new consumer group reads from the beginning. Manual assignment avoids group
    rebalance delays here; this is a replication test, not a consumer-group test.
    We assert the acknowledged IDs are present exactly once in the successful-write
    topic. We do not make an exact-count assertion about the failed strict write.
    """),
    code("""
    recovered = wait_for_isr(available_topic, 3)
    reader = Consumer({**LOCAL, "group.id": f"lab.replication.verify.{RUN}",
                       "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    try:
        reader.assign([TopicPartition(available_topic, 0, 0)])
        messages = poll_records(reader, count=len(acknowledged), timeout=20)
        observed = [decode(message) for message in messages]
    finally:
        reader.close()
    expected_ids = [item["event_id"] for item in acknowledged]
    observed_ids = [event["event_id"] for event in observed]
    assert observed_ids == expected_ids
    assert len(set(observed_ids)) == len(expected_ids)
    print("Verified acknowledged events:", len(observed_ids))
    print("Failover actually exercised:", failover_ran)
    print("Final ISR:", recovered["isr"])
    """),
    md("""
    **Expected:** 3 events with the default flag, or 6 after the failure experiment.
    All three replicas should be in ISR again. The recovered broker need not become
    leader immediately. A successful run is evidence for this particular experiment,
    not a guarantee against every possible failure.

    ## Step 11 · Recovery and stopping the optional stack

    If interrupted during failure, restore all services from the repository root:

    ```powershell
    docker compose -p kafka-expert -f compose.cluster.yaml start broker1 broker2 broker3
    docker compose -p kafka-expert -f compose.cluster.yaml ps
    ```

    After the class, stop the optional stack while preserving data:

    ```powershell
    docker compose -p kafka-expert -f compose.cluster.yaml down
    ```

    No notebook cell deletes volumes. A fresh full run uses new topic names.
    """),
    md("""
    ## Practice · Explain the tradeoff

    1. Repeat with minimum ISR 1 on a fresh topic. Does this setup prove it survives
       two service losses? Explain the controller-quorum complication.
    2. Add a second partition and inspect which broker leads each partition. Why is
       "the Kafka leader" ambiguous?
    3. Measure elapsed time between stop and the next successful acknowledgment.
       Repeat three full runs and explain why one measurement is not an SLA.
    4. Propose an architecture that isolates broker failures from controller failures.

    <details><summary>Solution hints</summary>

    Minimum ISR 1 permits smaller ISR for `acks=all`, reducing the minimum replicated
    copy requirement. It does not restore a lost controller quorum. Leadership is per
    partition; the active controller is a separate role. Measure with
    `time.perf_counter()` and record Docker shutdown, election, metadata refresh,
    replica catch-up, and producer retry effects. Dedicated controllers on independent
    hosts let you reason about controller and data-plane failures separately.

    </details>
    """),
    md("""
    ## References and next experiments

    The listener layout and combined-role configuration are based on the
    [Apache Kafka 4.1.2 Docker example](https://github.com/apache/kafka/blob/4.1.2/docker/examples/docker-compose-files/cluster/combined/plaintext/docker-compose.yml).
    Review [minimum ISR configuration](https://kafka.apache.org/41/configuration/topic-configs/#min.insync.replicas)
    and [KRaft operations](https://kafka.apache.org/41/operations/kraft/) for the
    durability and quorum behavior discussed above.

    Further projects: dedicated controllers, TLS and per-application ACLs, monitoring
    under-replicated partitions, controlled rolling upgrades, and partition reassignment.
    These require additional infrastructure and are intentionally separate from this
    local three-node failure exercise.
    """),
]


if __name__ == "__main__":
    write_notebook(ROOT / "notebooks" / "15_replication_and_failover.ipynb", cells)
