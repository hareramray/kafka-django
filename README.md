# Kafka with Python and Django — practical notebook workshop

**Start with [00_start_here.ipynb](notebooks/00_start_here.ipynb).** This workshop contains
16 Jupyter notebooks, from your first event to transactional processing, a Django order
pipeline, and a three-broker failure exercise. Each lesson includes runnable cells,
expected observations, checks, and exercises with hints. Examples use real Kafka.

You need basic Python knowledge; Django is introduced in notebook 11. Allow about
12–16 hours for guided practice, plus the exercises. Use this for individual learning
or a live demonstration, working through cells one at a time.

## Setup on Windows PowerShell

Install **Python 3.11–3.13** and **Docker Desktop with Linux containers**, then start
Docker Desktop. Run these commands from this folder. No virtualenv activation is needed.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
Copy-Item .env.example .env
docker compose up -d --wait
.\.venv\Scripts\python.exe -m ipykernel install --sys-prefix --name kafka-workshop --display-name "Python (Kafka Workshop)"
.\.venv\Scripts\python.exe -m jupyter lab
```

If `.venv` or `.env` already exists, keep it and skip its creation command. Select the
**Python (Kafka Workshop)** kernel. Open `notebooks/00_start_here.ipynb`.

On macOS/Linux, create the environment with `python3 -m venv .venv`, replace
`.\.venv\Scripts\python.exe` with `.venv/bin/python`, and use `cp .env.example .env`.
The first setup downloads packages and the Kafka image. Give Docker enough memory for
Kafka and your notebook kernel; the optional three-broker exercise uses more resources.

The default stack uses Apache Kafka **4.1.2** in KRaft mode (no ZooKeeper), the
`confluent-kafka` Python client, Django **5.2 LTS**, and SQLite. Kafka runs in Docker;
Python, Jupyter and Django run on the host. The host address is `localhost:9092`, and
containers use `kafka:19092`. The image/version choice follows the
[Apache Kafka 4.1 quickstart](https://kafka.apache.org/41/getting-started/quickstart/).

## Learning path

| Notebook | Level | Practical result |
|---|---|---|
| [00 · Start here](notebooks/00_start_here.ipynb) | Beginner | Check the kernel, Docker and broker connection |
| [01 · First producer and consumer](notebooks/01_first_producer_consumer.ipynb) | Beginner | Send JSON, inspect acknowledgement, consume and replay |
| [02 · Partitions, keys and ordering](notebooks/02_partitions_keys_and_ordering.ipynb) | Beginner | Verify per-key routing and per-partition offsets |
| [03 · Reliable producers](notebooks/03_reliable_producers.ipynb) | Beginner/intermediate | Inspect callbacks, batch messages, provoke a delivery error |
| [04 · Offsets, commits and replay](notebooks/04_offsets_commits_and_replay.ipynb) | Intermediate | Reproduce duplicate work and skipped work after restart |
| [05 · Consumer groups](notebooks/05_consumer_groups_and_rebalancing.ipynb) | Intermediate | Split work, stop a member and observe reassignment |
| [06 · Event contracts](notebooks/06_event_contracts_and_schema_evolution.ipynb) | Intermediate | Validate JSON Schema and test schema evolution |
| [07 · Idempotence and transactions](notebooks/07_idempotence_and_transactions.ipynb) | Advanced | Commit/abort and atomically write output plus input offsets |
| [08 · Retries and DLQ](notebooks/08_retries_and_dead_letter_queue.ipynb) | Intermediate/advanced | Retry transient errors, quarantine poison events, replay a fix |
| [09 · Event-time windows](notebooks/09_event_time_windows_and_state.ipynb) | Advanced | Aggregate a stream with late-event handling and bounded state |
| [10 · Operations and performance](notebooks/10_operations_performance_and_compaction.ipynb) | Advanced | Measure lag/batching and rebuild a compacted keyed view |
| [11 · Django HTTP and on_commit](notebooks/11_django_http_and_on_commit.ipynb) | Django integration | Create an order and expose the DB/Kafka dual-write gap |
| [12 · Django idempotent consumer](notebooks/12_django_idempotent_consumer.ipynb) | Django integration | Persist a projection and deduplicate repeated event effects |
| [13 · Django transactional outbox](notebooks/13_django_transactional_outbox.ipynb) | Advanced | Store order+event atomically and publish acknowledged events |
| [14 · Order-pipeline capstone](notebooks/14_capstone_order_pipeline.ipynb) | Advanced | Demonstrate the whole flow and recover from a duplicate window |
| [15 · Replication and failover](notebooks/15_replication_and_failover.ipynb) | Expert practice, optional | Fail a leader in a real three-broker cluster and restore it |

For a short first session, complete **00–04**. For a Django-focused route, add
**05, 06, 08, 11–14**. Finish the other advanced labs before attempting 15.
Notebook 15 requires [its separate cluster setup](docs/replication.md).

## How to run a lesson

1. Start the broker and open one notebook in a fresh kernel.
2. Read each explanation, predict the output, then run its cell with **Shift+Enter**.
3. Check assertions and explain the result before changing the example.
4. Complete the exercise, then use its solution hints.
5. Use **Restart Kernel and Run All** to reproduce the guided result.

Fresh topic names and groups isolate notebook runs; Kafka data remains until retention
or explicit cleanup. Django notebooks create separate SQLite databases in `.lab-data/`.
Restart the kernel between Django lessons because Django settings are process-wide.
Their visible `%%django_sync` cells run ORM code on one dedicated synchronous thread
so Django's async safety remains enabled even in kernels with a running event loop.
All example polling loops and worker commands have bounds. A timeout is a failure to
investigate, not a successful empty result. Some cells deliberately catch an expected error.

The Django notebooks use Django's HTTP test client to execute actual URLs, views and
database transactions without needing a background web server. For optional browser or
terminal interaction, see [django_demo/README.md](django_demo/README.md). The capstone is:

```text
HTTP POST /orders/
    -> SQLite transaction: Order + OutboxEvent
    -> publish_outbox worker -> Kafka topic (key = order_id)
    -> consume_orders worker
    -> SQLite transaction: InboxEvent + OrderProjection
    -> commit Kafka offset
    -> HTTP GET /projections/<order_id>/
```

## Verification

These checks do not require a running broker:

```powershell
.\.venv\Scripts\python.exe -m scripts.validate_notebooks
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe django_demo/manage.py check
.\.venv\Scripts\python.exe django_demo/manage.py test orders
.\.venv\Scripts\python.exe django_demo/manage.py makemigrations --check --dry-run
```

After starting Kafka and registering the notebook kernel, execute the main course:

```powershell
.\.venv\Scripts\python.exe -m scripts.validate_notebooks --execute
```

Use `--only 01 04` to choose lessons. Execution writes output to `artifacts/executed/`
and leaves the distributed notebooks clean. Notebook 15 is skipped unless you supply
`--include-cluster`; its failure injection additionally requires setting `RUN_FAILOVER=True`
inside that notebook. The start-here notebook can execute without Kafka and reports
the missing prerequisite. See [validation status](docs/validation.md) for checks actually run.

## Troubleshooting and stopping

| Symptom | Action |
|---|---|
| `docker` is not recognized | Install/start Docker Desktop and reopen the terminal |
| Docker daemon unavailable | Wait for Docker Desktop; use Linux containers |
| Connection refused | Run `docker compose ps` and `docker compose logs --tail 80 kafka` |
| Bootstrap connects but later calls fail | Check advertised addresses: host `localhost:9092`, container `kafka:19092` |
| `ModuleNotFoundError` | Check the notebook's `sys.executable` and select the workshop kernel |
| Polling timeout | Check count, topic, committed group offsets and transaction isolation |
| Django `SynchronousOnlyOperation` | Run the setup and magic-registration cells; keep ORM code inside `%%django_sync` cells |
| SQLite locked | Stop duplicate workers; this lab uses one publisher at a time |
| Port already occupied | Stop the conflicting service or update both port mapping and advertised listener |

`docker compose down` stops the main stack and keeps data.
`docker compose down -v` deliberately deletes **this stack's** Kafka volumes.
The optional cluster has separate stop/reset commands in its guide. `.lab-data/` contains
only generated lesson databases; retain them if you want to inspect prior runs.

## Scope and further practice

The one-broker plaintext setup is for local learning. It cannot demonstrate replica
durability; notebook 15 provides an isolated multi-broker experiment. Security in notebook
10 is a client-configuration walkthrough, not a TLS/ACL deployment. Schema validation in
06 is local JSON Schema, not a deployed Schema Registry. Window aggregation in 09 is a
small Python implementation, not the Java Kafka Streams runtime.

Kafka transactions cover Kafka reads/writes/offsets. They do not make arbitrary HTTP or
database effects exactly once. The Django outbox intentionally allows duplicate delivery
and uses database idempotency for repeatable effects. The SQLite publisher is single-worker;
a production multi-worker design needs coordination and stronger operational controls.

Extend the course with Schema Registry, secured listeners and ACLs, Postgres outbox
coordination or CDC, Kafka Connect, load testing, disaster recovery and deployment practices.
See [the presenter guide](docs/presenter-guide.md) for demo sessions and discussion prompts.

Authoring sources are `scripts/build_*_notebooks.py`; the notebooks themselves are ready to
open. Shared helpers are kept small in `labtools/kafka.py`, and lessons use raw Kafka APIs
where the behavior being taught should remain visible.
