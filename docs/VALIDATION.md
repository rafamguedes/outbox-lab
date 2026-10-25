# Validation report

Executed on October 25, 2026, on Windows with JDK 21, Maven 3.9.11, Docker Desktop, and Linux containers.

## Automated tests

The latest Maven run passed **85 tests: 47 unit tests and 38 integration tests**, with zero failures, errors, or skipped tests.

| Test class | Count | Main checks |
|---|---:|---|
| `OrderServiceTest` | 4 | Real JSON contract, matching IDs, insertion order, and failure propagation |
| `OrderProcessorTest` | 5 | Valid events, invalid types/JSON, mismatched order IDs, and simulated failure after UPDATE |
| `OutboxRunnerTest` | 10 | Empty queue, completion order, retry budget, permanent failures, and infrastructure errors |
| `EventRetryServiceTest` | 3 | Selected-event queuing, rejected states, and failure propagation |
| `OutboxIntegrationTest` | 30 | Real PostgreSQL transactions, migrations, locking, recovery, API, both dashboard lists, and manual recovery |
| `PollingWorkerTest` | 5 | Repeated idle polling, pause/resume, multi-chunk draining, infrastructure recovery, and restart |
| `WorkerConfigurationTest` | 9 | Selection of each implementation and rejection of invalid CDC identifiers/intervals |
| `PgOutputCommitTest` | 11 | Commit end LSN, non-commit messages, truncated frames, and unsupported flags |
| `WorkerStrategiesIntegrationTest` | 8 | Real polling and logical WAL streaming with notification triggers disabled |

The unit tests use a real Jackson ObjectMapper except when deliberately simulating a serialization error. Repository mocks check exact IDs, parameters, ordering, and forbidden interactions. They do not prove transaction rollback; integration tests query persisted rows after failures to verify that behavior.

The integration suite runs against a disposable Testcontainers PostgreSQL database. It verifies that an event insert failure leaves no order, a processor failure rolls back the order UPDATE while retaining the retry, and a completion failure rolls back the entire processing transaction. Repeated processing preserves the original order completion timestamp. A concurrent worker processes another row without waiting for a locked event.

The final lifecycle is `PENDING -> RETRY -> FAILED` after two simulated errors. `FAILED` events are excluded from both the automatic lock query and its next-availability query. A real-time test waits for exhaustion, verifies that the worker becomes idle, and then verifies notification-driven manual recovery in under five seconds. Successful events appear in a separate paginated list.

Additional checks reject manual recovery for `PENDING`, `RETRY`, and `PROCESSED`; preserve payloads, IDs, errors, and cumulative counters while queuing; admit exactly one of two concurrent manual requests; preserve `FAILED` after a failed manual attempt; and verify rollback without notification. The integration fixture stops the listener before removing PostgreSQL, so cleanup cannot restart a container through a reconnect attempt.

## Checks against tests that pass with broken code

Eight deliberate defects were applied separately in isolated source copies. The normal suite passed without mutations. Each selected test then failed with an assertion failure, Maven exit code 1, zero test errors, and zero skipped tests. Compilation or environment failures were not accepted as evidence of detection.

| Deliberate defect | Test that rejected it |
|---|---|
| Remove `insertEvent` from order creation | `createsMatchingOrderAndEventWithNormalizedPayload` |
| Remove the payload/order ID comparison | `mismatchedOrderIdHasNoOrderEffect` |
| Remove retry persistence after processing failure | `processorFailureSchedulesRetryAndDoesNotCompleteTheEvent` |
| Use the application clock for event availability | `eventIsImmediatelyAvailableEvenWhenApplicationClockIsAhead` |
| Remove the transition to permanent failure | `exhaustedAttemptsBecomePermanentIncludingFailuresAfterManualRetry` (three parameter cases) |
| Remove the `FAILED` condition from manual requeuing | `manualRetryRejectsPendingRetryingProcessedAndMissingEvents` |
| Select the notification implementation when CDC was requested | `constructsExactlyTheRequestedImplementationWithoutOpeningConnections` |
| Acknowledge the commit's initial LSN instead of its end LSN | `extractsTheTransactionEndLsnWithoutChangingBufferPosition` |

All eight defects were detected. Mutations were never applied to the delivered source. The final baseline suite passed all 85 tests.

These are focused fault checks, not exhaustive mutation coverage or a guarantee that every possible defect is detected. The recovery test also waits for the worker to become idle before making an event pending without a notification, so an earlier wakeup cannot mask a broken periodic recovery mechanism.

## Clock regression

Two immediate-processing assertions exposed an application/database clock difference. Event eligibility now uses PostgreSQL `CURRENT_TIMESTAMP`, retry scheduling uses its `clock_timestamp()`, and the worker reads a delay calculated by PostgreSQL. The regression test inserts an event with an application creation timestamp one day ahead and requires immediate processing; restoring the old application-based availability makes it fail.

## Notifications and recovery

Tests verify notification delivery after commit and no delivery after rollback, startup recovery, timed retries without another order, listener reconnection after terminating its PostgreSQL backend, pause/resume, missed-signal recovery, infrastructure failure recovery, and draining 200 events across multiple processing chunks.

The idle test first requires a notification-triggered order to finish, then checks that the number of outbox lock queries does not increase during a 1.4-second idle window. This rejects the previous one-second worker polling behavior. The configured safety check remains 60 seconds. Dashboard refresh queries are separate from worker queries.

## Worker strategy verification

The default Spring context exposes exactly one `OutboxWorker` bean. Unit tests construct each requested implementation and verify that worker selection does not itself open database connections. Startup rejects CDC on a second, real PostgreSQL container whose WAL level remains `replica`.

Polling and CDC integration tests explicitly disable both outbox notification triggers. They verify polling while idle, pause/resume, timed retry, commit/rollback isolation, ignoring unrelated-table writes, CDC feedback advancement, slot reuse after restart, reconnection after backend termination, permanent failure exhaustion, and manual recovery through WAL. During an uncommitted transaction, the test checks both invisibility from a separate connection and visibility from the transaction's own connection.

Docker runtime smoke tests then switched the existing application through all three configurations, with PostgreSQL using logical WAL:

| Worker | Run ID | Orders created/processed | Attempts per event | Dedicated source sessions |
|---|---|---:|---:|---:|
| CDC | `929131ac` | 2 / 2 | 1 | 1 logical WAL sender |
| Polling | `3337c3ac` | 2 / 2 | 1 | 0 |
| LISTEN/NOTIFY | `15cb7ecd` | 2 / 2 | 1 | 1 notification connection |

The dashboard displayed the correct mode each time. The application was returned to `LISTEN_NOTIFY`. PostgreSQL remains at `wal_level=logical`, and the CDC slot is inactive and retained for reuse. Configuration and slot retention details are in [WORKERS.md](WORKERS.md).

## Existing data and manual simulation

The existing Compose database had 101,105 orders and 101,105 events before migration V4. The same counts and a sampled order ID were preserved after migration. V4 removes only the former batch metadata, renames the active schema, and converts legacy event payloads and statuses. A separate integration test verifies migration from V3 with pending data and unchanged IDs.

The manual script submitted six orders through `POST /orders`, with a simulated failure every third order. All six completed: four in one attempt and two in two attempts. A database query confirmed 101,111 orders, 101,111 events, and zero pending events before the final smoke run.

The normal-retry smoke run (`e90e97f5`) submitted another six orders with the same failure cadence. All six POSTs succeeded, the dashboard returned HTTP 200, and SQL confirmed both records `PROCESSED` for every order, with attempt counts `1, 1, 2, 1, 1, 2`.

The final permanent-failure run (`9ff5babc`) used `Count=3`, `FailureEvery=1`, and `FailureAttempts=2`. All three requests succeeded and all three events became `FAILED` with two attempts. A manual retry was then submitted through the real browser for event `5c9a2e54-e544-434e-9c44-3ca19b2382eb`. It moved to the processed list with three attempts and its order became `PROCESSED`. SQL confirmed that the other two events remained `FAILED` with two attempts and their orders remained `RECEIVED`.

The dashboard was checked in the browser after a full reload: both lists rendered, completed events appeared only in their list, and manual controls appeared only for permanent failures. Two synthetic permanent failures were left available for inspection. The worker remained enabled and waiting for notifications.

Additional script checks rejected `Count=100001` before submission, stopped after the first HTTP failure by default, and attempted both requests with `ContinueOnError`. Both HTTP failure modes returned a nonzero process exit code. No server-side batch endpoint or generator remains.

The final script passed PowerShell syntax parsing and rejected `FailureAttempts=0` and `FailureAttempts=101` before making an HTTP request.

## Docker runtime

The final image built successfully and started with the existing database at Flyway V6. No new schema migration was needed for worker selection; the CDC publication and slot are initialized only when CDC is selected. All six migrations remained valid. The latest image size reported by Docker was **173,275,530 bytes**, approximately 173 MB decimal. Maven, the full JDK, and test dependencies are excluded from the runtime stage.

An idle observation after the worker strategy smoke runs reported approximately **128.4 MiB / 192 MiB** for the application and **19.01 MiB / 96 MiB** for PostgreSQL. Both containers reported zero OOM and OOM-kill events. These measurements are observations rather than a capacity guarantee.

The combined container limits are 288 MiB. This excludes Docker Desktop, its VM, the host, build processes, and Testcontainers. Run builds/tests elsewhere if those development tools exceed the memory available on the deployment machine.

## Reproduce

The later load-generator addition changes no Java or database behavior. Its eight Node.js tests passed, along with four real k6 checks against an isolated HTTP fixture: exactly 40 successful requests at 20 RPS for two seconds; 20 HTTP 400 responses rejected; 10 redirects refused; and 990 of 1,000 scheduled iterations reported as dropped under deliberate VU saturation. Independent fixture counts matched the reports. The full 50,000-RPS application burst was not executed. Details are in [LOAD-TEST.md](LOAD-TEST.md).

From the project directory, with JDK 21 and Docker running:

```powershell
.\mvnw.cmd clean test
docker compose up --build -d
pwsh -File .\scripts\Send-Orders.ps1 -Count 6 -FailureEvery 3
pwsh -File .\scripts\Send-Orders.ps1 -Count 3 -FailureEvery 1 -FailureAttempts 2
docker stats --no-stream
```

To repeat a focused fault check, copy the project into a temporary directory, apply one defect from the table, run its selected test with Maven's `-Dtest=ClassName#methodName`, and inspect the Surefire XML report. Restore the copied source after each check. A valid detection requires a test assertion failure, not a compilation error or missing Docker connection.
