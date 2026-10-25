# Worker implementations

`OutboxWorker` is the shared lifecycle/status interface. Exactly one implementation is registered by `WorkerConfiguration`, selected through `WORKER_TYPE`. The default remains `LISTEN_NOTIFY`.

| Type | Implementation | Primary wakeup mechanism | Idle database work |
|---|---|---|---|
| `POLLING` | `PollingWorker` | Fixed interval, default 1 second | One eligible-event query per interval |
| `LISTEN_NOTIFY` | `ListenNotifyWorker` | PostgreSQL notification after commit | 60-second safety recovery and retry timers |
| `CDC` | `CdcWorker` | Committed changes from PostgreSQL's logical WAL stream | 60-second safety recovery and retry timers |

All three use the same `OutboxRunner` and `OrderProcessor`. Transactions, `FOR UPDATE SKIP LOCKED`, savepoint rollback, automatic attempt limits, permanent failures, and manual recovery are unchanged. The dashboard displays the selected implementation.

## Select a worker

Run from the project directory in PowerShell. Recreating the application preserves its PostgreSQL volume.

```powershell
$env:WORKER_TYPE = 'POLLING'
$env:WORKER_POLL_INTERVAL_MS = '1000'
docker compose up --build -d
```

For notifications:

```powershell
$env:WORKER_TYPE = 'LISTEN_NOTIFY'
docker compose up -d
```

For CDC, enable logical decoding on PostgreSQL and recreate both services:

```powershell
$env:POSTGRES_WAL_LEVEL = 'logical'
$env:WORKER_TYPE = 'CDC'
docker compose up --build -d
```

`wal_level` is a PostgreSQL startup setting. A running database must restart when changing it. Keep `POSTGRES_WAL_LEVEL=logical` while comparing modes after creating a logical replication slot. Polling and LISTEN/NOTIFY also work with that WAL level.

For a local Maven run against PostgreSQL, the application also reads `WORKER_TYPE`. `POSTGRES_WAL_LEVEL` configures the Compose database; it does not reconfigure an external PostgreSQL server.

Invalid worker types fail Spring configuration. CDC startup rejects a server whose `wal_level` is not `logical`.

## Shared coordination

`AbstractOutboxWorker` owns the processing loop, a one-slot wakeup queue, retry timing, pause/resume support, shutdown, and startup recovery. It drains chunks of up to 100 attempts or 500 ms of starting new attempts, continuing immediately while work remains. Each attempt retains its own transaction.

`PollingWorker` uses one processing thread and no extra database connection. Its loop checks eligible rows every configured interval, even when empty. It does not perform a separate next-retry-time query; the next poll finds any newly eligible retry.

`JdbcSignalWorker` shares reconnection and dedicated-connection handling between the notification and CDC implementations. Those implementations use two threads and one dedicated connection outside the three-connection Hikari pool. Shutdown closes the source connection and interrupts both threads. A startup/reconnection check covers work stored while the source was disconnected.

The application-level pause flag is available through the Java interface and `WORKER_ENABLED`, not through dashboard buttons. Resuming explicitly wakes the processing loop. An in-flight database transaction may finish during shutdown or pause.

## LISTEN/NOTIFY

The listener registers `LISTEN outbox_events` and then signals pending-event recovery. pgJDBC's `getNotifications(1000)` waits for network notifications without running an outbox SELECT every second. Statement-level triggers emit notifications after INSERT and changes to `available_at`; rollback delivers none.

Notifications are wakeups rather than a durable event queue. Pending rows remain the durable work record, and retry timers, initial recovery, reconnect recovery, and the safety interval cover missed signals.

## CDC through PostgreSQL WAL

This implementation uses PostgreSQL's built-in `pgoutput` plugin through pgJDBC's logical replication API. It adds no Kafka, Debezium, connector process, or runtime dependency.

On first connection, the worker creates a dedicated publication for `public.outbox (id, status, available_at)` and a persistent logical replication slot if they do not exist. The publication captures INSERT and UPDATE operations. The demo's database role is a superuser; an external role needs logical replication privileges, table access, and permission to create the publication/slot.

The worker requests protocol version 1, reads binary replication frames, and signals processing only on a valid commit frame. It does not infer CDC from NOTIFY or from repeated SQL queries. Rollbacks and writes to unrelated tables do not wake it. `readPending()` checks the replication socket; a 100 ms wait when no data is available prevents busy spinning.

CDC is used as a wakeup source in this demo. The processor still reads and locks the durable outbox row through the shared runner. Published completion updates can produce an additional coalesced wakeup; they do not repeat the order effect or create an endless write loop.

The commit frame's end LSN is marked applied/flushed after the wakeup is queued. pgJDBC sends feedback at its one-second status interval. The slot is reused after reconnect/restart. If the application crashes after acknowledging a wakeup but before processing, startup recovery finds the still-pending row. This is not an atomic commit between a replication offset and an external message broker, nor an exactly-once external-delivery guarantee.

Retries need no new WAL event at the scheduled time: the common processing loop calculates a delay using the database clock. A manual recovery changes `FAILED` to `PENDING`, producing a WAL change that wakes CDC processing.

## CDC slot retention and recovery

Compose permits two replication slots and two WAL senders and sets `max_slot_wal_keep_size=64MB`. This is a retention threshold checked at checkpoints, not a hard limit on total WAL disk usage. A stopped or slow consumer can retain WAL and eventually lose its slot's required WAL when that threshold is exceeded.

Source failures are visible as reconnecting in the dashboard. The common safety scan continues checking pending rows. An invalidated slot is reported; the application does not silently drop an existing slot or overwrite one belonging to a different plugin/database.

Slots and publications persist when switching modes. For this demo's dedicated slot, stop the CDC application before intentionally removing/recreating an inactive slot:

```powershell
docker compose stop app
docker compose exec postgres psql -U outbox -d outbox -c "SELECT pg_drop_replication_slot('outbox_demo_slot');"
docker compose up -d app
```

The worker recreates the slot and recovers remaining pending rows. This cleanup discards the slot's replication position; it does not delete orders or outbox rows. Use a slot dedicated to this demo. Each concurrently running CDC instance needs a different `CDC_SLOT_NAME`.

## Configuration

| Variable | Default | Applies to |
|---|---|---|
| `WORKER_TYPE` | `LISTEN_NOTIFY` | Selects one implementation |
| `WORKER_ENABLED` | `true` | All workers |
| `WORKER_POLL_INTERVAL_MS` | `1000` | Polling interval |
| `POSTGRES_WAL_LEVEL` | `replica` | Compose PostgreSQL; set to `logical` for CDC |
| `CDC_SLOT_NAME` | `outbox_demo_slot` | CDC persistent position |
| `CDC_PUBLICATION_NAME` | `outbox_demo_publication` | CDC publication |

Recovery (60,000 ms), reconnect (2,000 ms), retry (12 seconds), and maximum automatic attempts (2) remain configurable under `outbox.worker` in `application.yml`. Slot/publication identifiers accept 1–63 lowercase letters, digits, or underscores and cannot begin with a digit.

The application's 192 MiB / 0.5 CPU and PostgreSQL's 96 MiB / 0.5 CPU limits are unchanged. CDC adds logical WAL and a replication slot, not another container. Actual memory, CPU, and WAL usage depend on traffic; the small demo limits do not imply a 50,000-RPS processing capacity.

References: [pgJDBC replication](https://jdbc.postgresql.org/documentation/server-prepare/#logical-replication), [PostgreSQL logical replication protocol](https://www.postgresql.org/docs/17/protocol-logical-replication.html), [commit frames](https://www.postgresql.org/docs/17/protocol-logicalrep-message-formats.html), [replication settings](https://www.postgresql.org/docs/17/runtime-config-replication.html).
