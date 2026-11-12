# Testing and performance

This document lists what the test suite proves about TaskForge, how to run it, and
what the benchmark measured on one machine. Every number below comes from
`SchedulerBenchmarkTest` and can be reproduced with the commands given. It makes no
claims beyond what was measured.

## Running the tests

Requirements: JDK 21+, Maven, and a running Docker daemon. Docker is needed because
every database test uses a throwaway PostgreSQL container (Testcontainers,
`postgres:17-alpine`). No local PostgreSQL is needed or touched.

```bash
mvn test                  # all functional tests (about 1 minute)
mvn test -Pperformance    # only the benchmark (about 3 minutes)
mvn test -Pperformance -Dperf.repetitions=5
```

The benchmark has the `performance` JUnit tag and is excluded from `mvn test`.

### How the database tests are wired

- `support/PostgresIntegrationTest` starts one PostgreSQL container per JVM. Flyway
  applies the real migrations to it. All subclasses share that container and one
  cached Spring context.
- Background workers are disabled in the test context. Tests that need execution
  start their own `JobScheduler`s against the real `DatabaseJobStore`, often
  several at once to stand in for several application instances. They are shut
  down after each test.
- `support/ScriptedJobHandlers` replaces the handler factory. `GENERATE_REPORT`
  jobs follow a per-test script (succeed, fail, hang, throw an `Error`), and every
  execution of every job type is counted. That count is how the tests detect a job
  that ran twice.
- `support/FaultInjectingJobStore` wraps the real store and can fail the next *n*
  claims or result writes, as a lost connection would.

## What is tested

| Area | Test | What it proves |
|---|---|---|
| Concurrent submission + execution | `ConcurrentInMemoryExecutionTest` | 8 threads submit 2,000 jobs to a 4-worker in-memory scheduler. Every job runs exactly once, and never more than 4 at a time. |
| | `ConcurrentExecutionIntegrationTest` | 8 clients submit 400 jobs through `JobService` while 4 workers claim from PostgreSQL. Each job has exactly one execution, `attempt_count = 1` and one attempt row. |
| | | Two scheduler instances share the database and 300 jobs. Both take work, and the claim totals add up to 300 with no duplicates (`FOR UPDATE SKIP LOCKED`). |
| | `JobRepositoryConcurrencyTest` | Two transactions claiming at the same instant get different rows. |
| Retries | `RetryIntegrationTest` | Fail, fail, succeed gives the history FAILED, FAILED, COMPLETED. Gaps between attempt start times (database clock) are at least the 100 ms and 200 ms backoff. |
| | | A job that always fails stops at `maxAttempts`, and no extra attempt starts afterwards. A manual retry runs exactly one more attempt. |
| | | Non-retryable failures use one attempt. Attempts over the job timeout are interrupted and retried. |
| Duplicate requests | `DuplicateRequestIntegrationTest` | 16 concurrent `POST /api/jobs` calls with one `Idempotency-Key`: one `202`, fifteen `200` with `Idempotent-Replayed: true`, one job row, one execution. |
| | | A duplicate that arrives after the job completed gets the completed job back and does not run it again. |
| | `JobSubmissionIdempotencyTest`, `JobControllerTest` | Service-level races, plus key reuse with a different body (`422`). |
| Restarts | `RestartRecoveryIntegrationTest` | **Hard kill.** The real application starts in a separate JVM (`HangingTaskForge`) and claims a job. The process is then killed with `destroyForcibly()`, so no shutdown hooks run. The job is left `RUNNING` with an open attempt. A restart through the production `SchedulerStartup` path recovers it (attempt 1 FAILED with the recovery reason, after backoff) and completes it as attempt 2. |
| | | **Shutdown past the grace period.** The interrupted attempt is recorded and requeued with no delay, and the next instance completes it. |
| | | **Split brain.** An instance is presumed dead and its job recovered and completed elsewhere. When that instance's late result arrives it is discarded and does not overwrite the newer attempt. |
| | `JobRecoveryIntegrationTest` | Recovery of each crash point: no attempt row, open attempt, final attempt, cancellation requested. |
| Worker failures | `WorkerFailureIntegrationTest` | A handler that throws `StackOverflowError` fails its job. The single worker survives and processes the next jobs. |
| | | Injected claim failures: workers back off and carry on, and all 20 jobs complete. |
| | | A result that cannot be written leaves the job `RUNNING`, and other jobs continue. After a restart, recovery completes it. |
| | | **Real connection loss.** `pg_terminate_backend` kills every client connection 10 times during a 300-job run. After recovery and a restart, every job is COMPLETED, and each job's `attempt_count` equals its number of attempt rows, with the last attempt COMPLETED. |
| | `DatabaseBackedSchedulerTest`, `InMemoryReliabilityTest` | Timeouts, cancellation and shutdown at unit level. |

### Delivery guarantee these tests establish

Execution is **at least once**, not exactly once. A job runs again if its process
dies, or loses its connection, after the handler finished but before the result
was committed. The split-brain test shows the same thing. The connection-loss test
prints how often it happened. In three consecutive runs, 0, 10 and 16 of the 300
jobs were orphaned and re-executed. The database record always stays consistent,
but handlers with side effects must be idempotent.

## Performance

### Workload

All measurements go through `JobService` and the real `DatabaseJobStore` with real
PostgreSQL. The HTTP layer is **not** included. Every job is `GENERATE_REPORT`, whose
test handler calls `Thread.sleep(work)` to stand in for I/O-bound work, or does
nothing when `work = 0`. Each scenario starts from empty tables (`TRUNCATE` and
`VACUUM ANALYZE`). A discarded warm-up run comes first, and each scenario runs 3
times.

| Shape | Definition |
|---|---|
| **submit** | *C* client threads call `submitJob` for 2,000 jobs total. No workers run. |
| **drain** | *N* jobs are already queued, then *W* workers start. Throughput = *N* ÷ (last result recorded − workers started). |
| **steady** | One client submits at a fixed rate while *W* workers run. Shows queue wait when the system is not saturated, and what happens when it is. |

### Metrics

All timings use `System.nanoTime()` in the test JVM, recorded by
`performance/TimingJobStore`. Database timestamps mix the host clock with the
container's clock, so they are not used.

- **Queue wait**: from submission (or from workers starting, in a drain) until a worker's claim returns.
- **Execution latency**: from claim returned until the worker starts recording the result. This covers handler time plus hand-off to the handler thread.
- **Claim**: duration of the claim transaction (`UPDATE … FOR UPDATE SKIP LOCKED` plus the attempt insert).
- **End-to-end**: from submission until the result is committed.
- Percentiles are nearest-rank. The tables show medians across repetitions, and throughput also shows the min–max range.

### Results (2026-11-12)

Environment: Windows 11 (amd64), 16 logical processors, OpenJDK 27, max heap 7.9 GB.
PostgreSQL 17.11 (`postgres:17-alpine`) ran in Docker Desktop on the same machine
with default settings. HikariCP pool size was 10 (the default). The raw data is in
`target/performance/results.csv` after a run.

**Submission** (no workers)

| Clients | Submissions/s (min–max) | p50 ms | p95 ms | p99 ms |
|---:|---:|---:|---:|---:|
| 1 | 1,957 (1,885–1,973) | 0.50 | 0.58 | 0.63 |
| 8 | 12,524 (12,158–12,769) | 0.61 | 0.79 | 1.02 |

**Backlog drain**

| Work | Workers | Jobs/s (min–max) | Exec p50 ms | Exec p99 ms | Claim p50 ms | Claim p95 ms |
|---:|---:|---:|---:|---:|---:|---:|
| 0 ms | 1 | 253 (251–265) | 0.03 | 0.05 | 1.86 | 2.13 |
| 0 ms | 4 | 1,028 (1,008–1,035) | 0.02 | 0.05 | 1.69 | 1.90 |
| 0 ms | 8 | 1,753 (1,529–1,792) | 0.02 | 0.05 | 1.91 | 2.29 |
| 0 ms | 16 | 2,118 (2,107–2,128) | 0.02 | 0.05 | 2.11 | 5.38 |
| 20 ms | 1 | 32 (32–32) | 27.14 | 28.54 | 1.62 | 1.79 |
| 20 ms | 4 | 130 (130–130) | 26.29 | 27.33 | 1.97 | 2.19 |
| 20 ms | 8 | 257 (256–257) | 25.97 | 27.10 | 2.08 | 2.50 |
| 20 ms | 16 | 507 (505–510) | 24.96 | 27.54 | 2.08 | 4.31 |

**Steady arrival** (4 workers, 20 ms work)

| Target rate | Achieved jobs/s | Queue wait p50 / p95 / p99 ms | End-to-end p50 / p95 / p99 ms |
|---:|---:|---:|---:|
| 50/s | 50 | 34.5 / 66.3 / 81.3 | 64.1 / 96.9 / 111.9 |
| 150/s | 127 (saturated) | 589 / 1,010 / 1,054 | 618 / 1,039 / 1,084 |

### What the numbers show

1. **Fixed cost per job is about 4 ms of database work on one worker.** With no-op
   jobs a single worker completes 253 jobs/s. Each job needs a claim transaction
   (about 1.9 ms) and a result transaction. Handler hand-off is negligible
   (about 0.03 ms).
2. **I/O-bound jobs scale almost linearly up to 16 workers.** With 20 ms jobs,
   throughput goes 32 → 130 → 257 → 507 jobs/s for 1 → 4 → 8 → 16 workers, and claim
   time stays about 2 ms. The database was not the bottleneck at that size.
3. **No-op jobs stop scaling at 8–16 workers.** Throughput goes 1,753 → 2,118, and
   claim p95 more than doubles (2.3 → 5.4 ms). With a pool of 10 connections, 16
   workers queue for connections. This was not investigated further: pool size and
   PostgreSQL settings were left at defaults.
4. **An idle system adds tens of milliseconds of queue wait.** A worker that finds
   no job sleeps 200 ms (`JobScheduler.databaseWorkerLoop`). At 50 jobs/s, well
   below capacity, the median queue wait is 34 ms and p99 is 81 ms, almost all of
   it polling delay. Notification on submit (for example `LISTEN/NOTIFY`) would
   remove it.
5. **Capacity of 4 workers with 20 ms jobs is about 127–130 jobs/s.** Offered
   150/s, the system processed 127/s and the queue kept growing. Queue wait passed
   one second within the 6-second run and would keep rising for as long as the
   overload lasted. There is no back-pressure on submission in database-backed
   mode.

### Limits of these measurements

- **One machine, with the database on the same host as the application.** There is
  no network latency, and the client, scheduler and PostgreSQL compete for the
  same CPUs. Results on separate hosts will differ, most likely with higher claim
  times.
- **`Thread.sleep` overshoots on this Windows host.** "20 ms" of work measured
  about 25–27 ms of execution latency. That figure includes the overshoot, so
  compare scenarios with each other rather than with the nominal 20 ms.
- **Small runs.** 500–2,000 jobs per run, on a database emptied before each one.
  Long-running behaviour, such as table and index bloat from many finished jobs
  or autovacuum, is not measured.
- **HTTP is not included.** Submission figures are for `JobService.submitJob`.
- **All instances are in one JVM.** Multi-instance behaviour is tested for
  correctness (no duplicate claims) but not benchmarked across processes.
- **Different hardware gives different numbers.** Rerun
  `mvn test -Pperformance` and compare with the CSV. The report header records
  the environment of each run.
