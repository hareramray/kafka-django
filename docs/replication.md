# Optional three-broker workshop

Notebook `15_replication_and_failover.ipynb` uses its own Docker Compose project. Complete notebooks 01-05 and 07 first. Allow roughly 45-60 minutes. This is a local learning cluster with three combined broker/controller processes, plaintext listeners, and one host. It does not model separate failure domains or production security.

Start from the repository root with Docker Desktop running Linux containers:

```powershell
docker compose -p kafka-expert -f compose.cluster.yaml up -d --wait
docker compose -p kafka-expert -f compose.cluster.yaml ps
```

The first image pull and broker startup may take a few minutes. Plan for at least 4 GB of memory available to Docker for this optional stack; actual requirements depend on your machine. The notebook connects to `localhost:19092,localhost:29092,localhost:39092`. The beginner stack can remain on port 9092. Do not change the normal `.env` bootstrap servers.

The container network uses separate internal listeners so replicas can reach one another; the host listeners advertise addresses the local notebook can resolve. The configuration follows the [Apache Kafka Docker examples](https://github.com/apache/kafka/blob/4.1.2/docker/examples/docker-compose-files/cluster/combined/plaintext/docker-compose.yml). Named volumes are mounted at the writable data path prepared by the [Apache image](https://github.com/apache/kafka/blob/4.1.2/docker/jvm/Dockerfile).

The notebook first verifies a three-broker cluster, creates topics with three replicas, and observes the leader and in-sync replicas (ISR). With `RUN_FAILOVER = False`, it only performs the healthy-cluster portion. For the full exercise, set the flag to `True` and run the cells in order. The experiment stops the current leader's service, waits for a new leader, writes with `min.insync.replicas=2`, observes write failure on a second topic requiring three ISR members, and restores the service in `finally`. `acks=all` requires acknowledgments from the current ISR; `min.insync.replicas` sets the minimum ISR size for accepting such writes. See [Kafka topic configuration](https://kafka.apache.org/41/configuration/topic-configs/#min.insync.replicas).

Stopping two services also removes the majority of this three-controller KRaft quorum. That would mix metadata-quorum loss with ISR loss, so the notebook deliberately tests the stricter topic while two controllers remain alive. The separate roles of controllers and brokers are described in [Kafka KRaft operations](https://kafka.apache.org/41/operations/kraft/).

If the notebook is interrupted or the kernel dies during the failure experiment, restore all three services before continuing:

```powershell
docker compose -p kafka-expert -f compose.cluster.yaml start broker1 broker2 broker3
docker compose -p kafka-expert -f compose.cluster.yaml ps
docker compose -p kafka-expert -f compose.cluster.yaml logs --tail 80 broker1 broker2 broker3
```

Each fresh notebook run creates uniquely named topics. Re-running individual publish cells appends more records; use **Restart Kernel and Run All** for a clean exercise. The notebook records which phases actually ran and does not claim a failover result when the flag is false. Timed-out sends have an uncertain outcome; the failed-write topic is intentionally excluded from exact-count assertions.

To stop this cluster while retaining logs:

```powershell
docker compose -p kafka-expert -f compose.cluster.yaml down
```

Only when you want to erase this optional cluster's local Kafka data, run:

```powershell
docker compose -p kafka-expert -f compose.cluster.yaml down -v
```

This command removes only volumes belonging to the `kafka-expert` project. It is not part of notebook execution. Do not reuse these local credentials-free settings for a remote broker.
