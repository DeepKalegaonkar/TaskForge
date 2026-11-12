package com.taskforge.integration;

import com.taskforge.dto.SubmitJobRequest;
import com.taskforge.dto.SubmitJobRequest.JobType;
import com.taskforge.model.JobAttempt;
import com.taskforge.model.JobStatus;
import com.taskforge.repository.JobAttemptRepository;
import com.taskforge.service.DatabaseJobStore;
import com.taskforge.service.JobService;
import com.taskforge.support.Await;
import com.taskforge.support.PostgresIntegrationTest;
import com.taskforge.support.ScriptedJobHandlers.Step;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.util.List;

import static com.taskforge.support.ScriptedJobHandlers.SCRIPTED_FAILURE;
import static org.junit.jupiter.api.Assertions.*;

/*
 * Retries driven end to end: real workers, PostgreSQL, backoff via
 * available_at. The retry policy is 100 ms doubling (FAST_RETRIES).
 */
class RetryIntegrationTest extends PostgresIntegrationTest {

    @Autowired
    private JobService jobService;

    @Autowired
    private DatabaseJobStore jobStore;

    @Autowired
    private JobAttemptRepository attempts;

    @BeforeEach
    void emptyDatabase() {
        deleteAllJobs();
    }

    @Test
    void transientFailuresShouldBeRetriedWithBackoffUntilTheJobSucceeds()
            throws Exception {
        handlers.script(Step.FAIL, Step.FAIL);
        String id = submit(JobType.GENERATE_REPORT, 3, null);

        startScheduler(1, jobStore);
        awaitStatus(id, "COMPLETED");

        List<JobAttempt> history = attempts.findByJobId(id);
        assertEquals(List.of(JobStatus.FAILED, JobStatus.FAILED, JobStatus.COMPLETED),
                history.stream().map(JobAttempt::status).toList());
        assertEquals(SCRIPTED_FAILURE, history.get(0).failureReason());
        assertEquals(SCRIPTED_FAILURE, history.get(1).failureReason());
        assertNull(history.get(2).failureReason());
        assertEquals(3, handlers.executions());

        // Start times come from the database clock. Each retry must wait
        // at least its backoff: 100 ms, then 200 ms.
        assertTrue(gap(history.get(0), history.get(1)).toMillis() >= 100,
                "First retry came after " + gap(history.get(0), history.get(1)));
        assertTrue(gap(history.get(1), history.get(2)).toMillis() >= 200,
                "Second retry came after " + gap(history.get(1), history.get(2)));
    }

    @Test
    void jobThatKeepsFailingShouldStopAfterMaxAttemptsAndCanBeRetriedManually()
            throws Exception {
        String id = submit(JobType.FAIL_FOR_TESTING, 3, null);

        startScheduler(1, jobStore);
        awaitStatus(id, "FAILED");

        // Longer than the remaining backoff: no fourth attempt may start.
        Thread.sleep(1500);
        assertEquals("FAILED", statusOf(id));
        assertEquals(3, attempts.findByJobId(id).size());
        assertEquals(3, handlers.executions());
        assertTrue(attempts.findByJobId(id).stream()
                .allMatch(a -> a.status() == JobStatus.FAILED));

        // POST /api/jobs/{id}/retry grants exactly one more attempt.
        assertTrue(jobService.retryFailedJob(id).isPresent());
        Await.until(Duration.ofSeconds(10),
                () -> attempts.findByJobId(id).size() == 4 && "FAILED".equals(statusOf(id)),
                "manual retry should run a fourth attempt and fail again");
        Thread.sleep(500);
        assertEquals(4, handlers.executions());
    }

    @Test
    void nonRetryableFailureShouldUseOnlyOneAttempt() throws Exception {
        String id = submit(JobType.FAIL_PERMANENTLY_FOR_TESTING, 5, null);

        startScheduler(1, jobStore);
        awaitStatus(id, "FAILED");
        Thread.sleep(500);

        assertEquals(1, attempts.findByJobId(id).size());
        assertEquals(1, handlers.executions());
    }

    @Test
    void attemptsExceedingTheJobTimeoutShouldBeInterruptedAndRetried()
            throws Exception {
        // SLOW_FOR_TESTING sleeps 15 s; the job allows 1 s per attempt.
        String id = submit(JobType.SLOW_FOR_TESTING, 2, 1);

        startScheduler(1, jobStore);
        awaitStatus(id, "FAILED");

        List<JobAttempt> history = attempts.findByJobId(id);
        assertEquals(2, history.size());
        history.forEach(a -> assertEquals("Timed out after 1 s", a.failureReason()));
    }

    private String submit(JobType type, int maxAttempts, Integer timeoutSeconds) {
        return jobService.submitJob(
                new SubmitJobRequest("retry-test", type, maxAttempts, timeoutSeconds),
                null
        ).job().getId();
    }

    private void awaitStatus(String id, String status) throws InterruptedException {
        Await.until(Duration.ofSeconds(15), () -> status.equals(statusOf(id)),
                () -> "job " + id + " is " + statusOf(id) + ", expected " + status);
    }

    private static Duration gap(JobAttempt earlier, JobAttempt later) {
        return Duration.between(earlier.startedAt(), later.startedAt());
    }
}
