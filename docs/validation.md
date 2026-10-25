# Validation status

Checked on **25 October 2026**, on Windows with Python **3.13.15**.
Installed dependencies include confluent-kafka **2.16.0**, Django **5.2.18**,
JupyterLab **4.6.4**, and ipykernel **7.4.0**. The local `.venv` is installed and
the **Python (Kafka Workshop)** kernel is registered inside it.

## Passed without a running Kafka broker

- All 16 notebooks pass the Jupyter notebook schema and Python syntax checks. The
  validator also compiles the actual Python body of custom `%%django_sync` cells.
- 10 shared-helper regression tests cover delivery failures, undrained queues,
  invalid serialization, explicit commits, consumer closure and timeouts.
- 17 Django tests cover atomic rollback, the `on_commit` failure window, outbox
  acknowledgement rules, durable duplicate suppression, HTTP validation, consumer
  commit ordering and no-commit behavior after poison events or database failures.
- Django system checks and migration consistency checks pass.
- The dependency consistency check (`pip check`) passes.
- Notebook 00 executes in a real Jupyter kernel. Its broker diagnostic correctly
  reports that Kafka is unavailable; this is **not** a successful connectivity test.
- Selected Django setup, ORM, rollback and failure-injection cells execute in a real
  notebook kernel against SQLite. This exposed and fixed the Jupyter event-loop issue
  using a dedicated synchronous worker instead of disabling Django's async safety.
- Advanced pure-Python checks cover window aggregation, late-event handling, state
  bounds, retry limits and DLQ raw-byte preservation.
- Both Compose files parse as YAML. The optional cluster has distinct broker IDs,
  loopback host ports and persistent volumes.

## Requires Docker / a real broker

Docker was not available in the authoring environment. The Kafka-dependent notebook
cells, broker startup, transactional processing, consumer rebalances, full Django/Kafka
pipeline, and three-broker failover have **not been executed here**. YAML parsing is not
a substitute for `docker compose config` or starting the containers.

Run the main integration check after Docker Desktop is installed and started:

```powershell
docker compose config
docker compose up -d --wait
.\.venv\Scripts\python.exe -m scripts.validate_notebooks --execute
```

For notebook 15, follow [replication.md](replication.md), start the independent cluster,
and run it interactively. The actual failure injection is explicitly opt-in in the notebook.

Executed notebooks are written under the ignored `artifacts/` directory. Distributed
notebooks have empty outputs and execution counts so that learners generate their own evidence.
