package com.taskforge.integration;

import com.taskforge.config.SchedulerStartup;
import com.taskforge.model.JobAttempt;
import com.taskforge.model.JobStatus;
import com.taskforge.repository.JobAttemptRepository;
import com.taskforge.scheduler.JobScheduler;
import com.taskforge.service.DatabaseJobStore;
import com.taskforge.service.JobRecoveryService;
import com.taskforge.service.JobService;
import com.taskforge.support.Await;
import com.taskforge.support.HangingTaskForge;
import com.taskforge.support.PostgresIntegrationTest;
import com.taskforge.support.ScriptedJobHandlers.Step;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.taskforge.integration.ConcurrentExecutionIntegrationTest.report;
import static org.junit.jupiter.api.Assertions.*;

/*
 * What happens to a job when the process running it stops: killed,
 * shut down past its grace period, or presumed dead while still alive.
 */
class RestartRecoveryIntegrationTest extends PostgresIntegrationTest {

    // JobRecoveryService.RECOVERY_FAILURE_REASON
    private static final String RECOVERY_REASON =
            "Execution interrupted by application shutdown or crash";

    @Autowired
    private JobService jobService;

    @Autowired
    private DatabaseJobStore jobStore;

    @Autowired
    private JobRecoveryService recoveryService;

    @Autowired
    private JobAttemptRepository attempts;

    @BeforeEach
    void emptyDatabase() {
        deleteAllJobs();
    }

    @Test
    void jobRunningInAKilledProcessShouldBeRecoveredAndCompletedAfterRestart()
            throws Exception {
        String id = jobService.submitJob(report("crash-test"), null).job().getId();

        // A separate JVM running the real application claims the job,
        // then is killed without any chance to clean up.
        Path log = Path.of("target", "crash-test", "killed-instance.log");
        Process instance = HangingTaskForge.launch(POSTGRES, log);
        try {
            Await.until(Duration.ofSeconds(120),
                    () -> "RUNNING".equals(statusOf(id)) || !instance.isAlive(),
                    "the child instance should claim the job");
            assertTrue(instance.isAlive(), "Child instance exited early; see " + log);
        } finally {
            instance.destroyForcibly();
            assertTrue(instance.waitFor(30, TimeUnit.SECONDS));
        }

        // The kill left the job RUNNING with an open attempt.
        assertEquals("RUNNING", statusOf(id));
        List<JobAttempt> beforeRestart = attempts.findByJobId(id);
        assertEquals(1, beforeRestart.size());
        assertEquals(JobStatus.RUNNING, beforeRestart.get(0).status());

        // Restart through the production startup path: recover, then start workers.
        JobScheduler restarted = createScheduler(1, jobStore, false);
        new SchedulerStartup(recoveryService, restarted).startSchedulerAfterRecovery();

        // Recovery applies the configured 2 s backoff before the retry.
        Await.until(Duration.ofSeconds(15), () -> "COMPLETED".equals(statusOf(id)),
                () -> "job should complete after restart, is " + statusOf(id));

        List<JobAttempt> history = attempts.findByJobId(id);
        assertEquals(2, history.size());
        assertEquals(JobStatus.FAILED, history.get(0).status());
        assertEquals(RECOVERY_REASON, history.get(0).failureReason());
        assertEquals(JobStatus.COMPLETED, history.get(1).status());
        assertEquals(1, handlers.executions(), "Only the restarted instance ran it here");
    }

    @Test
    void jobInterruptedByShutdownShouldBeRequeuedAndRunByTheNextInstance()
            throws Exception {
        handlers.script(Step.HANG);
        String id = jobService.submitJob(report("shutdown-test"), null).job().getId();

        JobScheduler first = startScheduler(1, jobStore);
        assertTrue(handlers.awaitStarted(1, Duration.ofSeconds(5)));

        // Grace period expires; the attempt is recorded as interrupted.
        first.shutdownGracefully(Duration.ofMillis(300));
        assertEquals("QUEUED", statusOf(id));

        startScheduler(1, jobStore);
        Await.until(Duration.ofSeconds(10), () -> "COMPLETED".equals(statusOf(id)),
                "next instance should complete the job");

        List<JobAttempt> history = attempts.findByJobId(id);
        assertEquals(List.of(JobStatus.FAILED, JobStatus.COMPLETED),
                history.stream().map(JobAttempt::status).toList());
        assertEquals(JobScheduler.SHUTDOWN_REASON, history.get(0).failureReason());
    }

    /*
     * Split brain: an instance is presumed dead and its job recovered,
     * but it is still running. The job then executes twice (delivery is
     * at least once), but the late result of the old attempt must be
     * discarded rather than overwrite the newer one.
     */
    @Test
    void lateResultFromAnInstancePresumedDeadShouldBeDiscarded() throws Exception {
        handlers.script(Step.HANG);
        String id = jobService.submitJob(report("split-brain-test"), null).job().getId();

        JobScheduler stale = startScheduler(1, jobStore);
        assertTrue(handlers.awaitStarted(1, Duration.ofSeconds(5)));

        // Another instance starts up and recovers the RUNNING job.
        recoveryService.recoverJobs();
        skipBackoff();
        startScheduler(1, jobStore);
        Await.until(Duration.ofSeconds(10), () -> "COMPLETED".equals(statusOf(id)),
                "recovered job should complete on the new instance");

        // The stale instance's attempt now ends and tries to record itself.
        stale.shutdownGracefully(Duration.ofMillis(100));

        assertEquals("COMPLETED", statusOf(id));
        assertEquals(2, jdbcTemplate.queryForObject(
                "SELECT attempt_count FROM jobs WHERE id = ?", Integer.class, id));
        List<JobAttempt> history = attempts.findByJobId(id);
        assertEquals(List.of(JobStatus.FAILED, JobStatus.COMPLETED),
                history.stream().map(JobAttempt::status).toList());
        assertEquals(RECOVERY_REASON, history.get(0).failureReason());
        assertEquals(2, handlers.executions());
    }
}
