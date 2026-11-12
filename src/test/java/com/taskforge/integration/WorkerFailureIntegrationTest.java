package com.taskforge.integration;

import com.taskforge.dto.SubmitJobRequest;
import com.taskforge.dto.SubmitJobRequest.JobType;
import com.taskforge.scheduler.JobScheduler;
import com.taskforge.service.DatabaseJobStore;
import com.taskforge.service.JobRecoveryService;
import com.taskforge.service.JobService;
import com.taskforge.support.Await;
import com.taskforge.support.FaultInjectingJobStore;
import com.taskforge.support.PostgresIntegrationTest;
import com.taskforge.support.ScriptedJobHandlers.Step;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.taskforge.integration.ConcurrentExecutionIntegrationTest.report;
import static org.junit.jupiter.api.Assertions.*;

/*
 * Failures inside a running instance: handlers that crash, and the
 * database becoming unreachable while workers claim or record jobs.
 */
class WorkerFailureIntegrationTest extends PostgresIntegrationTest {

    @Autowired
    private JobService jobService;

    @Autowired
    private DatabaseJobStore jobStore;

    @Autowired
    private JobRecoveryService recoveryService;

    @BeforeEach
    void emptyDatabase() {
        deleteAllJobs();
    }

    @Test
    void handlerThrowingAnErrorShouldFailItsJobWithoutKillingTheWorker()
            throws Exception {
        handlers.script(Step.ERROR);
        String crashing = submit(new SubmitJobRequest(
                "crashing", JobType.GENERATE_REPORT, 1, null));
        String next = submit(report("after-crash"));
        String last = submit(report("after-crash"));

        // A single worker: later jobs only run if it survived the crash.
        startScheduler(1, jobStore);

        Await.until(Duration.ofSeconds(10),
                () -> "COMPLETED".equals(statusOf(last)),
                "the worker should keep processing after a handler crash");
        assertEquals("FAILED", statusOf(crashing));
        assertEquals("Simulated handler crash", jdbcTemplate.queryForObject(
                "SELECT failure_reason FROM jobs WHERE id = ?", String.class, crashing));
        assertEquals("COMPLETED", statusOf(next));
    }

    @Test
    void workersShouldKeepClaimingAfterTheDatabaseFailsTemporarily()
            throws Exception {
        FaultInjectingJobStore store = new FaultInjectingJobStore(jobStore);
        store.failNextClaims(4);
        for (int i = 0; i < 20; i++) {
            submit(report("claim-outage"));
        }

        startScheduler(2, store);

        Await.until(Duration.ofSeconds(20), () -> countJobs("COMPLETED") == 20,
                () -> countJobs("COMPLETED") + " of 20 completed");
        assertEquals(0, store.pendingInjectedFailures(), "All failures were hit");
        assertEquals(20, handlers.executions());
    }

    @Test
    void resultThatCannotBeRecordedShouldBeRecoveredOnRestart() throws Exception {
        FaultInjectingJobStore store = new FaultInjectingJobStore(jobStore);
        store.failNextFinishes(1);
        String lost = submit(report("lost-result"));
        String other = submit(report("other"));

        JobScheduler first = startScheduler(1, store);
        Await.until(Duration.ofSeconds(10), () -> "COMPLETED".equals(statusOf(other)),
                "the worker should continue after failing to record a result");

        // The job ran, but nothing recorded that; it looks RUNNING forever.
        assertEquals("RUNNING", statusOf(lost));
        first.shutdownGracefully(Duration.ofSeconds(1));

        recoveryService.recoverJobs();
        assertEquals("QUEUED", statusOf(lost));
        skipBackoff();
        startScheduler(1, jobStore);

        Await.until(Duration.ofSeconds(10), () -> "COMPLETED".equals(statusOf(lost)),
                "recovered job should complete");
        assertEquals(2, jdbcTemplate.queryForObject(
                "SELECT attempt_count FROM jobs WHERE id = ?", Integer.class, lost));
        // At-least-once: the lost job's handler ran twice.
        assertEquals(3, handlers.executions());
    }

    /*
     * Real connection loss: PostgreSQL terminates every client connection
     * repeatedly while 300 jobs are processed. Afterwards, recovery and a
     * restart must bring every job to COMPLETED with consistent history.
     */
    @Test
    void noJobShouldBeLostWhenDatabaseConnectionsAreKilledMidWorkload()
            throws Exception {
        int totalJobs = 300;
        handlers.setWorkDuration(Duration.ofMillis(10));
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < totalJobs; i++) {
            ids.add(submit(report("connection-loss")));
        }

        JobScheduler first = startScheduler(4, jobStore);

        int killed = 0;
        for (int round = 0; round < 10; round++) {
            Thread.sleep(100);
            killed += terminateClientConnections();
        }
        assertTrue(killed > 0, "The test must actually kill connections");

        Await.until(Duration.ofSeconds(60), () -> countJobs("QUEUED") == 0,
                () -> countJobs("QUEUED") + " jobs still queued");
        first.shutdownGracefully(Duration.ofSeconds(5));

        // Anything still RUNNING lost its claim or result with its connection.
        int orphaned = countJobs("RUNNING");
        recoveryService.recoverJobs();
        skipBackoff();
        startScheduler(4, jobStore);

        Await.until(Duration.ofSeconds(30), () -> countJobs("COMPLETED") == totalJobs,
                () -> countJobs("COMPLETED") + " of " + totalJobs + " completed");

        // Every job's counter matches its history, and its last attempt succeeded.
        assertEquals(0, jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*) FROM jobs j
                WHERE j.attempt_count <> (SELECT COUNT(*) FROM job_attempts a WHERE a.job_id = j.id)
                   OR NOT EXISTS (SELECT 1 FROM job_attempts a
                                  WHERE a.job_id = j.id
                                    AND a.attempt_number = j.attempt_count
                                    AND a.status = 'COMPLETED')
                """, Integer.class));
        assertTrue(handlers.executions() >= totalJobs);

        System.out.printf(
                "Connection-loss test: %d connections killed, %d jobs orphaned and recovered, "
                        + "%d handler executions for %d jobs%n",
                killed, orphaned, handlers.executions(), totalJobs);
    }

    private int terminateClientConnections() {
        try {
            return jdbcTemplate.queryForObject(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT pg_terminate_backend(pid)
                        FROM pg_stat_activity
                        WHERE datname = current_database()
                          AND pid <> pg_backend_pid()
                          AND backend_type = 'client backend'
                    ) killed
                    """, Integer.class);
        } catch (DataAccessException e) {
            // This connection was itself killed in the previous round.
            return 0;
        }
    }

    private String submit(SubmitJobRequest request) {
        return jobService.submitJob(request, null).job().getId();
    }
}
