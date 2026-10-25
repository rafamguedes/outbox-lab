# Constant-rate HTTP load test

`scripts/Invoke-LoadTest.ps1` runs a local Grafana k6 scenario targeting **50,000 POST /orders requests per second for 2 seconds** by default. The target is 100,000 requests in total, preserving this demo's per-run request limit. This is a short burst, not a sustained-capacity certification.

The script schedules arrivals independently of response time using k6's `constant-arrival-rate` executor. One iteration sends one POST, with connection reuse and discarded response bodies. There are no HTTP retries, redirect following, per-order tasks accumulated in PowerShell, or new application containers. A final-interval guard prevents an extra scheduler iteration from exceeding the request budget.

## Requirements

- PowerShell 7 or later.
- Grafana k6 on PATH, or an executable supplied through `-K6Path`. Tested with k6 2.3.0.
- The application running and reachable at `BaseUrl`.

Follow the [official k6 installation instructions](https://grafana.com/docs/k6/latest/set-up/install-k6/). On Windows:

```powershell
winget install k6 --source winget
```

The generator runs locally, without a cloud account. The wrapper disables k6's anonymous usage reporting. Node.js is needed only to run the optional script unit tests; it is not needed to generate load.

## Run

From the project directory:

```powershell
docker compose up --build -d
pwsh -File ./scripts/Invoke-LoadTest.ps1 -DryRun
pwsh -File ./scripts/Invoke-LoadTest.ps1 -Rate 50000 -DurationSeconds 2
```

`DryRun` prints the configuration without HTTP requests, report files, or a k6 dependency. To check a small volume first:

```powershell
pwsh -File ./scripts/Invoke-LoadTest.ps1 -Rate 20 -DurationSeconds 2 -PreAllocatedVUs 2 -MaxVUs 2
```

To simulate permanent processing failures in every tenth accepted order:

```powershell
pwsh -File ./scripts/Invoke-LoadTest.ps1 -Rate 50000 -DurationSeconds 2 -FailureEvery 10 -FailureAttempts 2
```

Application processing errors occur asynchronously and are inspected in the dashboard. They do not turn an already accepted HTTP 201 response into an HTTP load-test error.

## Parameters

| Parameter | Default | Meaning |
|---|---:|---|
| `Rate` | 50000 | Target arrivals per second, 1–50,000 |
| `DurationSeconds` | 2 | Active scheduling window; `Rate * DurationSeconds` must be at most 100,000 |
| `BaseUrl` | `http://localhost:8080` | Application root or deployment prefix |
| `PreAllocatedVUs` | 32 | Virtual users allocated before load begins |
| `MaxVUs` | 128 | Virtual-user ceiling; must be at least `PreAllocatedVUs` |
| `TimeoutSeconds` | 3 | Timeout per POST; drain time is limited to this value plus 2 seconds |
| `Quantity` | 1 | Units per order, 1–999 |
| `FailureEvery` | 0 | Select every Nth order for simulated processing errors; 0 disables them |
| `FailureAttempts` | 1 | How many processing attempts fail for each selected order, 1–100 |
| `ProductPrefix` | `Load` | Product prefix, at most 60 characters; run ID and sequence are appended |
| `K6Path` | `k6` | Executable name or absolute path |
| `ReportsDirectory` | `reports/load` in this project | Root for unique run directories |

For a generator on another machine, set `BaseUrl` to an address reachable from that machine. The supplied Compose configuration binds to loopback; this script does not change the application's network exposure.

## Resource limits and interpreting 50,000 RPS

The application still has its existing 192 MiB / 0.5 CPU limit, PostgreSQL 96 MiB / 0.5 CPU, three Hikari connections, and six Tomcat threads. This change does not increase those limits or demonstrate that they sustain 50,000 RPS.

The generator defaults to 32 preallocated VUs and a ceiling of 128 to bound concurrent work on this small-memory setup. If response times exceed the concurrency budget, k6 records `dropped_iterations` rather than building an unbounded queue. At an average 100 ms response time, 50,000 RPS would need roughly 5,000 simultaneous requests, plus capacity for variation and generator overhead. The default 128-VU ceiling cannot maintain that combination of rate and latency.

`PreAllocatedVUs` and `MaxVUs` are configurable up to 10,000, but virtual users consume generator memory and CPU. The generator consumes resources in addition to the two containers. Use measurements from a separate generator with adequate capacity when assessing server throughput; client saturation and server saturation must be distinguished.

## Reports and exit status

Each run writes `reports/load/<run-id>/configuration.json` and `summary.json`. Report directories are ignored by Git and Docker. There is no raw per-request metrics export.

| Field | Interpretation |
|---|---|
| `targetRps` | Requested rate, not measured throughput |
| `plannedRequests` | Rate multiplied by the active window |
| `startedRequests` / `httpRequests` | Measured submissions and HTTP requests |
| `createdOrders` | Responses with exactly HTTP 201 |
| `failedOrUnknownRequests` | Other statuses and transport errors, including ambiguous timeouts |
| `droppedIterations` | Scheduled work that could not start because capacity was unavailable |
| `interruptedRequests` | Requests started without a recorded result before the run ended |
| `submittedRpsOverActiveWindow` | Measured submissions divided by the configured scheduling window |
| `completedRpsIncludingDrain` | k6's measured HTTP completion rate, including final drain time |
| `acceptedRpsIncludingDrain` | HTTP 201 responses divided by actual run time, including drain |
| `latencyMs` | Average, p95, and p99 HTTP latency across responses |
| `passed` | Whether every planned request started and returned 201, with no drops or interruptions |

k6 thresholds and the wrapper both enforce the result. Wrong statuses, dropped work, incomplete volume, interruptions, or failed thresholds produce a nonzero exit. A configured 50,000-RPS target alone cannot produce a passing report.

HTTP acceptance measures durable order/event ingestion, not the speed at which the outbox worker finishes processing. A timeout may occur after a database commit, so the script does not automatically resubmit uncertain POSTs.

## Verification

Run the configuration/report unit tests with Node.js:

```powershell
node --test scripts/tests/load-config.test.mjs
```

Eight tests cover request budgets, VU limits, URLs, payload contracts, failure cadence, actual-rate calculations, and refusal to report success with missing measurements, drops, invalid statuses, interruptions, or failed thresholds.

The real k6 executable was also checked against an isolated loopback HTTP fixture:

| Scenario | Observed result |
|---|---|
| 20 RPS × 2 s, HTTP 201 | Exactly 40 requests accepted; pass |
| 10 RPS × 2 s, HTTP 400 | 20 unexpected responses; nonzero exit |
| 10 RPS × 1 s, HTTP 307 | Exactly 10 HTTP requests, no redirect followed; nonzero exit |
| 1,000 RPS × 1 s, one VU, 100 ms response delay | 10 requests completed, 990 dropped; nonzero exit |

Independent server counters matched the reports, with valid payloads and unique product names. No 50,000-RPS application capacity result is claimed; the full burst is left for manual execution.

References: [constant arrival rate](https://grafana.com/docs/k6/latest/using-k6/scenarios/executors/constant-arrival-rate/), [dropped iterations](https://grafana.com/docs/k6/latest/using-k6/scenarios/concepts/dropped-iterations/), [thresholds](https://grafana.com/docs/k6/latest/using-k6/thresholds/).
