package com.taskforge.performance;

import com.taskforge.dto.SubmitJobRequest;
import com.taskforge.dto.SubmitJobRequest.JobType;
import com.taskforge.performance.TimingJobStore.Timing;
import com.taskforge.scheduler.JobScheduler;
import com.taskforge.service.DatabaseJobStore;
import com.taskforge.service.JobService;
import com.taskforge.support.Await;
import com.taskforge.support.PostgresIntegrationTest;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.ToLongFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;

/*
 * Repeatable throughput and latency measurements against PostgreSQL in
 * a container. Not part of the normal build; run it with
 *
 *     mvn test -Pperformance
 *
 * Optional: -Dperf.repetitions=N (default 3).
 *
 * Three workload shapes, all through JobService (no HTTP layer):
 *
 *   submit  N submissions from C client threads, no workers.
 *   drain   N jobs already queued; W workers start and empty the queue.
 *   steady  Jobs arrive at a fixed rate while W workers run.
 *
 * Every job is GENERATE_REPORT, whose test handler sleeps for the
 * scenario's work time, standing in for I/O-bound work.
 *
 * Results are printed and written to target/performance/.
 */
@Tag("performance")
class SchedulerBenchmarkTest extends PostgresIntegrationTest {

    private static final int REPETITIONS =
            Integer.getInteger("perf.repetitions", 3);

    private static final SubmitJobRequest JOB =
            new SubmitJobRequest("benchmark", JobType.GENERATE_REPORT, 1, 60);

    enum Kind { SUBMIT, DRAIN, STEADY }

    record Scenario(
            String name,
            Kind kind,
            int jobs,
            int workers,
            int clients,
            Duration work,
            double ratePerSecond
    ) {
        static Scenario submit(int jobs, int clients) {
            return new Scenario("submit-" + clients + "c", Kind.SUBMIT,
                    jobs, 0, clients, Duration.ZERO, 0);
        }

        static Scenario drain(int jobs, int workers, int workMs) {
            return new Scenario("drain-" + workMs + "ms-" + workers + "w", Kind.DRAIN,
                    jobs, workers, 0, Duration.ofMillis(workMs), 0);
        }

        static Scenario steady(int jobs, int workers, int workMs, double rate) {
            return new Scenario("steady-" + (int) rate + "ps-" + workMs + "ms-" + workers + "w",
                    Kind.STEADY, jobs, workers, 1, Duration.ofMillis(workMs), rate);
        }
    }

    // Percentiles in milliseconds, plus a throughput in jobs per second.
    record Result(
            Scenario scenario,
            int repetition,
            double throughput,
            double achievedArrivalRate,
            Stats queueWait,
            Stats execution,
            Stats claim,
            Stats endToEnd,
            Stats submit
    ) {
    }

    record Stats(double p50, double p95, double p99, double max) {
        static final Stats NONE = new Stats(Double.NaN, Double.NaN, Double.NaN, Double.NaN);

        static Stats of(long[] nanos) {
            if (nanos.length == 0) {
                return NONE;
            }
            long[] sorted = nanos.clone();
            Arrays.sort(sorted);
            return new Stats(pct(sorted, 50), pct(sorted, 95), pct(sorted, 99),
                    sorted[sorted.length - 1] / 1e6);
        }

        // Nearest-rank percentile.
        private static double pct(long[] sorted, int p) {
            int rank = (int) Math.ceil(p / 100.0 * sorted.length);
            return sorted[Math.max(0, rank - 1)] / 1e6;
        }
    }

    static final List<Scenario> SCENARIOS = List.of(
            Scenario.submit(2000, 1),
            Scenario.submit(2000, 8),
            Scenario.drain(2000, 1, 0),
            Scenario.drain(2000, 4, 0),
            Scenario.drain(2000, 8, 0),
            Scenario.drain(2000, 16, 0),
            Scenario.drain(500, 1, 20),
            Scenario.drain(500, 4, 20),
            Scenario.drain(500, 8, 20),
            Scenario.drain(500, 16, 20),
            Scenario.steady(500, 4, 20, 50),
            Scenario.steady(900, 4, 20, 150)
    );

    @Autowired
    private JobService jobService;

    @Autowired
    private DatabaseJobStore jobStore;

    @Autowired
    private DataSource dataSource;

    @Test
    void measureThroughputAndLatency() throws Exception {
        // Warm up the JIT, connection pool and PostgreSQL caches.
        run(Scenario.drain(500, 4, 0), 0);
        run(Scenario.submit(500, 4), 0);

        List<Result> results = new ArrayList<>();
        for (Scenario scenario : SCENARIOS) {
            for (int rep = 1; rep <= REPETITIONS; rep++) {
                Result result = run(scenario, rep);
                results.add(result);
                System.out.printf(Locale.ROOT, "%-26s rep %d: %8.1f jobs/s%n",
                        scenario.name(), rep, result.throughput());
            }
        }

        writeReport(results);
    }

    private Result run(Scenario scenario, int repetition) throws Exception {
        jdbcTemplate.execute("TRUNCATE job_attempts, jobs");
        jdbcTemplate.execute("VACUUM ANALYZE jobs");
        handlers.reset();
        handlers.setWorkDuration(scenario.work());

        return switch (scenario.kind()) {
            case SUBMIT -> runSubmit(scenario, repetition);
            case DRAIN -> runDrain(scenario, repetition);
            case STEADY -> runSteady(scenario, repetition);
        };
    }

    private Result runSubmit(Scenario scenario, int repetition) throws Exception {
        int perClient = scenario.jobs() / scenario.clients();
        long[] latencies = new long[perClient * scenario.clients()];
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService clients = Executors.newFixedThreadPool(scenario.clients());

        long began;
        long ended;
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int c = 0; c < scenario.clients(); c++) {
                int offset = c * perClient;
                futures.add(clients.submit(() -> {
                    start.await();
                    for (int i = 0; i < perClient; i++) {
                        long t = System.nanoTime();
                        jobService.submitJob(JOB, null);
                        latencies[offset + i] = System.nanoTime() - t;
                    }
                    return null;
                }));
            }
            began = System.nanoTime();
            start.countDown();
            for (Future<?> f : futures) {
                f.get(5, TimeUnit.MINUTES);
            }
            ended = System.nanoTime();
        } finally {
            clients.shutdownNow();
        }

        assertEquals(latencies.length, countJobs("QUEUED"));
        double throughput = latencies.length / ((ended - began) / 1e9);
        return new Result(scenario, repetition, throughput, Double.NaN,
                Stats.NONE, Stats.NONE, Stats.NONE, Stats.NONE, Stats.of(latencies));
    }

    private Result runDrain(Scenario scenario, int repetition) throws Exception {
        Map<String, Long> submitted = new ConcurrentHashMap<>();
        for (int i = 0; i < scenario.jobs(); i++) {
            submitted.put(jobService.submitJob(JOB, null).job().getId(), System.nanoTime());
        }

        TimingJobStore store = new TimingJobStore(jobStore);
        long began = System.nanoTime();
        JobScheduler scheduler = startScheduler(scenario.workers(), store);

        awaitAllFinished(store, scenario);
        scheduler.shutdownGracefully(Duration.ofSeconds(5));

        return measure(scenario, repetition, store, submitted, began, Double.NaN);
    }

    private Result runSteady(Scenario scenario, int repetition) throws Exception {
        TimingJobStore store = new TimingJobStore(jobStore);
        JobScheduler scheduler = startScheduler(scenario.workers(), store);
        Thread.sleep(500); // Let workers reach their idle polling loop.

        Map<String, Long> submitted = new ConcurrentHashMap<>();
        long intervalNanos = (long) (1e9 / scenario.ratePerSecond());
        long began = System.nanoTime();

        for (int i = 0; i < scenario.jobs(); i++) {
            long due = began + i * intervalNanos;
            long wait = due - System.nanoTime();
            if (wait > 0) {
                LockSupport.parkNanos(wait);
            }
            long t = System.nanoTime();
            submitted.put(jobService.submitJob(JOB, null).job().getId(), t);
        }
        double achievedRate = scenario.jobs() / ((System.nanoTime() - began) / 1e9);

        awaitAllFinished(store, scenario);
        scheduler.shutdownGracefully(Duration.ofSeconds(5));

        return measure(scenario, repetition, store, submitted, began, achievedRate);
    }

    private void awaitAllFinished(TimingJobStore store, Scenario scenario)
            throws InterruptedException {
        Await.until(Duration.ofMinutes(5),
                () -> store.timings().values().stream().filter(t -> t.finished != 0).count()
                        == scenario.jobs(),
                () -> scenario.name() + " did not finish");
    }

    private Result measure(
            Scenario scenario,
            int repetition,
            TimingJobStore store,
            Map<String, Long> submitted,
            long began,
            double achievedRate
    ) {
        assertEquals(scenario.jobs(), countJobs("COMPLETED"), "All jobs must complete");
        assertEquals(scenario.jobs(), handlers.executions(), "Each job must run once");

        Map<String, Timing> timings = store.timings();
        long lastFinished = timings.values().stream().mapToLong(t -> t.finished).max().orElseThrow();

        // In a drain the jobs were queued before the clock started, so
        // queue wait counts from when the workers started.
        ToLongFunction<String> readyAt = id -> Math.max(submitted.get(id), began);

        long[] queueWait = submitted.keySet().stream()
                .mapToLong(id -> timings.get(id).claimed - readyAt.applyAsLong(id)).toArray();
        long[] execution = timings.values().stream()
                .mapToLong(t -> t.finishStarted - t.claimed).toArray();
        long[] claim = timings.values().stream()
                .mapToLong(t -> t.claimed - t.claimStarted).toArray();
        long[] endToEnd = submitted.keySet().stream()
                .mapToLong(id -> timings.get(id).finished - submitted.get(id)).toArray();

        double throughput = scenario.jobs() / ((lastFinished - began) / 1e9);
        return new Result(scenario, repetition, throughput, achievedRate,
                Stats.of(queueWait), Stats.of(execution), Stats.of(claim),
                scenario.kind() == Kind.STEADY ? Stats.of(endToEnd) : Stats.NONE,
                Stats.NONE);
    }

    // ---------------------------------------------------------------- report

    private void writeReport(List<Result> results) throws IOException {
        Path dir = Path.of("target", "performance");
        Files.createDirectories(dir);

        StringBuilder csv = new StringBuilder(
                "scenario,kind,jobs,workers,clients,work_ms,target_rate,repetition,"
                        + "throughput_per_s,achieved_arrival_rate,"
                        + "queue_wait_p50_ms,queue_wait_p95_ms,queue_wait_p99_ms,queue_wait_max_ms,"
                        + "execution_p50_ms,execution_p95_ms,execution_p99_ms,execution_max_ms,"
                        + "claim_p50_ms,claim_p95_ms,claim_p99_ms,"
                        + "end_to_end_p50_ms,end_to_end_p95_ms,end_to_end_p99_ms,"
                        + "submit_p50_ms,submit_p95_ms,submit_p99_ms\n");
        for (Result r : results) {
            Scenario s = r.scenario();
            csv.append(String.join(",", List.of(
                    s.name(), s.kind().name(), i(s.jobs()), i(s.workers()), i(s.clients()),
                    i((int) s.work().toMillis()), f(s.ratePerSecond()), i(r.repetition()),
                    f(r.throughput()), f(r.achievedArrivalRate()),
                    f(r.queueWait().p50()), f(r.queueWait().p95()), f(r.queueWait().p99()), f(r.queueWait().max()),
                    f(r.execution().p50()), f(r.execution().p95()), f(r.execution().p99()), f(r.execution().max()),
                    f(r.claim().p50()), f(r.claim().p95()), f(r.claim().p99()),
                    f(r.endToEnd().p50()), f(r.endToEnd().p95()), f(r.endToEnd().p99()),
                    f(r.submit().p50()), f(r.submit().p95()), f(r.submit().p99())
            ))).append('\n');
        }
        Files.writeString(dir.resolve("results.csv"), csv);

        String markdown = markdownSummary(results);
        Files.writeString(dir.resolve("results.md"), markdown);
        System.out.println();
        System.out.println(markdown);
        System.out.println("Raw results: " + dir.resolve("results.csv").toAbsolutePath());
    }

    // Medians across repetitions; throughput also shows the min-max range.
    private String markdownSummary(List<Result> results) {
        Map<Scenario, List<Result>> byScenario = new LinkedHashMap<>();
        results.forEach(r -> byScenario.computeIfAbsent(r.scenario(), k -> new ArrayList<>()).add(r));

        StringBuilder md = new StringBuilder();
        md.append("# TaskForge benchmark results\n\n");
        md.append(environment()).append('\n');
        md.append("Values are medians of ").append(REPETITIONS)
                .append(" repetitions; throughput also shows the min-max range. Latencies in ms.\n\n");

        md.append("## Submission (JobService.submitJob, no workers)\n\n");
        md.append("| Scenario | Jobs | Clients | Submissions/s (min-max) | p50 | p95 | p99 |\n");
        md.append("|---|---:|---:|---:|---:|---:|---:|\n");
        byScenario.forEach((s, rs) -> {
            if (s.kind() != Kind.SUBMIT) return;
            md.append(String.format(Locale.ROOT, "| %s | %d | %d | %s | %.2f | %.2f | %.2f |\n",
                    s.name(), s.jobs(), s.clients(), throughputCell(rs),
                    median(rs, r -> r.submit().p50()), median(rs, r -> r.submit().p95()),
                    median(rs, r -> r.submit().p99())));
        });

        md.append("\n## Backlog drain (jobs queued before workers start)\n\n");
        md.append("| Scenario | Jobs | Workers | Work | Jobs/s (min-max) "
                + "| Exec p50 | Exec p95 | Exec p99 | Claim p50 | Claim p95 |\n");
        md.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        byScenario.forEach((s, rs) -> {
            if (s.kind() != Kind.DRAIN) return;
            md.append(String.format(Locale.ROOT,
                    "| %s | %d | %d | %d ms | %s | %.2f | %.2f | %.2f | %.2f | %.2f |\n",
                    s.name(), s.jobs(), s.workers(), s.work().toMillis(), throughputCell(rs),
                    median(rs, r -> r.execution().p50()), median(rs, r -> r.execution().p95()),
                    median(rs, r -> r.execution().p99()),
                    median(rs, r -> r.claim().p50()), median(rs, r -> r.claim().p95())));
        });

        md.append("\n## Steady arrival (fixed submission rate, workers running)\n\n");
        md.append("| Scenario | Jobs | Workers | Work | Target rate | Achieved rate | Jobs/s "
                + "| Queue wait p50 | p95 | p99 | Exec p50 | p95 | End-to-end p50 | p95 | p99 |\n");
        md.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        byScenario.forEach((s, rs) -> {
            if (s.kind() != Kind.STEADY) return;
            md.append(String.format(Locale.ROOT,
                    "| %s | %d | %d | %d ms | %.0f/s | %.1f/s | %s | %.1f | %.1f | %.1f "
                            + "| %.2f | %.2f | %.1f | %.1f | %.1f |\n",
                    s.name(), s.jobs(), s.workers(), s.work().toMillis(), s.ratePerSecond(),
                    median(rs, Result::achievedArrivalRate), throughputCell(rs),
                    median(rs, r -> r.queueWait().p50()), median(rs, r -> r.queueWait().p95()),
                    median(rs, r -> r.queueWait().p99()),
                    median(rs, r -> r.execution().p50()), median(rs, r -> r.execution().p95()),
                    median(rs, r -> r.endToEnd().p50()), median(rs, r -> r.endToEnd().p95()),
                    median(rs, r -> r.endToEnd().p99())));
        });
        return md.toString();
    }

    private String environment() {
        Runtime rt = Runtime.getRuntime();
        String pool = dataSource instanceof HikariDataSource h
                ? String.valueOf(h.getMaximumPoolSize()) : "unknown";
        return String.join("\n",
                "- Run at: " + Instant.now(),
                "- JVM: " + System.getProperty("java.vm.name") + " "
                        + System.getProperty("java.version"),
                "- OS: " + System.getProperty("os.name") + " " + System.getProperty("os.version")
                        + " (" + System.getProperty("os.arch") + ")",
                "- Available processors: " + rt.availableProcessors()
                        + ", max heap: " + rt.maxMemory() / (1024 * 1024) + " MB",
                "- Database: " + POSTGRES_IMAGE + " in Docker (Testcontainers), default settings",
                "- PostgreSQL server: " + jdbcTemplate.queryForObject("SHOW server_version", String.class),
                "- HikariCP maximum pool size: " + pool,
                "- Repetitions per scenario: " + REPETITIONS,
                "");
    }

    private static String throughputCell(List<Result> rs) {
        double min = rs.stream().mapToDouble(Result::throughput).min().orElse(Double.NaN);
        double max = rs.stream().mapToDouble(Result::throughput).max().orElse(Double.NaN);
        return String.format(Locale.ROOT, "%.0f (%.0f-%.0f)",
                median(rs, Result::throughput), min, max);
    }

    private static double median(List<Result> rs, java.util.function.ToDoubleFunction<Result> f) {
        double[] values = rs.stream().mapToDouble(f).sorted().toArray();
        int n = values.length;
        return n % 2 == 1 ? values[n / 2] : (values[n / 2 - 1] + values[n / 2]) / 2;
    }

    private static String i(int value) {
        return Integer.toString(value);
    }

    private static String f(double value) {
        return Double.isNaN(value) ? "" : String.format(Locale.ROOT, "%.3f", value);
    }
}
