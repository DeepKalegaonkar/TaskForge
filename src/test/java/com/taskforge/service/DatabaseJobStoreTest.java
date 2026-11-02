package com.taskforge.service;

import com.taskforge.model.Job;
import com.taskforge.model.JobAttempt;
import com.taskforge.model.JobStatus;
import com.taskforge.repository.JobAttemptRepository;
import com.taskforge.repository.JobRepository;
import com.taskforge.scheduler.AttemptResult;
import com.taskforge.scheduler.CancellationResult;
import com.taskforge.support.JobRows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class DatabaseJobStoreTest {

    @Autowired
    private DatabaseJobStore jobStore;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobAttemptRepository jobAttemptRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private JobRows rows;

    @BeforeEach
    void setUp() {
        rows = new JobRows(jdbcTemplate);
    }

    @AfterEach
    void cleanUp() {
        rows.deleteAll();
    }

    @Test
    void claimShouldStartTheJobAndRecordTheAttemptTogether() {
        String id = rows.insert(JobStatus.QUEUED, 0, 3);

        Job claimed = jobStore.claimNext().orElseThrow();

        assertEquals(id, claimed.getId());
        assertEquals(JobStatus.RUNNING, claimed.getStatus());
        assertEquals(1, claimed.getAttemptCount());

        List<JobAttempt> attempts = jobAttemptRepository.findByJobId(id);
        assertEquals(1, attempts.size());
        assertEquals(1, attempts.get(0).attemptNumber());
        assertEquals(JobStatus.RUNNING, attempts.get(0).status());
    }

    @Test
    void failedAttemptWithRetriesLeftShouldWaitOutItsBackoff() {
        String id = rows.insert(JobStatus.QUEUED, 0, 3);
        Job job = jobStore.claimNext().orElseThrow();

        job.markFailed("boom");
        Optional<JobStatus> next = jobStore.finishAttempt(
                job, AttemptResult.failed("boom", Duration.ofHours(1))
        );

        assertEquals(Optional.of(JobStatus.QUEUED), next);

        Map<String, Object> row = rows.row(id);
        assertEquals("QUEUED", row.get("status"));
        assertEquals("boom", row.get("failure_reason"));
        assertTrue(((Number) row.get("seconds_until_available")).doubleValue() > 3500,
                "The job must not be claimable before its retry delay");
        assertAttempt(id, 1, JobStatus.FAILED, "boom");

        Job reloaded = jobRepository.findById(id).orElseThrow();
        assertNotNull(reloaded.getNextAttemptAt());
    }

    @Test
    void requeuedJobShouldBeClaimedAsItsNextAttempt() {
        String id = rows.insert(JobStatus.QUEUED, 0, 3);
        Job first = jobStore.claimNext().orElseThrow();
        first.markFailed("boom");
        jobStore.finishAttempt(first, AttemptResult.failed("boom", Duration.ZERO));

        Job second = jobStore.claimNext().orElseThrow();

        assertEquals(id, second.getId());
        assertEquals(2, second.getAttemptCount());
        assertEquals(2, jobAttemptRepository.findByJobId(id).size());
    }

    @Test
    void failureWithoutRetryShouldFailPermanently() {
        String id = rows.insert(JobStatus.QUEUED, 0, 3);
        Job job = jobStore.claimNext().orElseThrow();

        job.markFailed("Invalid input");
        Optional<JobStatus> next = jobStore.finishAttempt(
                job, AttemptResult.failed("Invalid input", null)
        );

        assertEquals(Optional.of(JobStatus.FAILED), next);
        assertEquals("FAILED", rows.row(id).get("status"));
        assertNotNull(rows.row(id).get("completed_at"));
        assertAttempt(id, 1, JobStatus.FAILED, "Invalid input");
    }

    @Test
    void successShouldCompleteTheJobAndItsAttempt() {
        String id = rows.insert(JobStatus.QUEUED, 0, 3);
        Job job = jobStore.claimNext().orElseThrow();

        job.markCompleted();
        jobStore.finishAttempt(job, AttemptResult.completed());

        assertEquals("COMPLETED", rows.row(id).get("status"));
        assertAttempt(id, 1, JobStatus.COMPLETED, null);
    }

    @Test
    void failureAfterCancellationWasRequestedShouldCancelInsteadOfRetrying() {
        String id = rows.insert(JobStatus.QUEUED, 0, 3);
        Job job = jobStore.claimNext().orElseThrow();
        rows.requestCancellation(id);

        job.markFailed("interrupted");
        Optional<JobStatus> next = jobStore.finishAttempt(
                job, AttemptResult.failed("interrupted", Duration.ZERO)
        );

        assertEquals(Optional.of(JobStatus.CANCELLED), next);
        assertEquals("CANCELLED", rows.row(id).get("status"));
    }

    @Test
    void staleWorkerShouldNotOverwriteARecoveredJob() {
        String id = rows.insert(JobStatus.QUEUED, 0, 3);
        Job job = jobStore.claimNext().orElseThrow();

        // Another instance recovered and requeued the job meanwhile.
        jdbcTemplate.update(
                "UPDATE jobs SET status = 'QUEUED' WHERE id = ?", id
        );

        job.markCompleted();
        Optional<JobStatus> next = jobStore.finishAttempt(job, AttemptResult.completed());

        assertTrue(next.isEmpty());
        assertEquals("QUEUED", rows.row(id).get("status"));
        assertAttempt(id, 1, JobStatus.RUNNING, null);
    }

    @Test
    void cancelShouldDependOnTheJobsStatus() {
        String queued = rows.insert(JobStatus.QUEUED, 0, 3);
        String running = rows.insert(JobStatus.RUNNING, 1, 3);
        String completed = rows.insert(JobStatus.COMPLETED, 1, 3);

        assertEquals(CancellationResult.CANCELLED, jobStore.cancel(queued));
        assertEquals("CANCELLED", rows.row(queued).get("status"));

        assertEquals(CancellationResult.CANCELLATION_REQUESTED, jobStore.cancel(running));
        assertEquals("RUNNING", rows.row(running).get("status"));
        assertTrue(jobStore.isCancelRequested(running));

        assertEquals(CancellationResult.NOT_CANCELLABLE, jobStore.cancel(completed));
        assertEquals(CancellationResult.NOT_FOUND, jobStore.cancel("no-such-job"));
    }

    @Test
    void permanentlyFailedJobCanBeRetriedOnce() {
        String failed = rows.insert(JobStatus.FAILED, 3, 3, "boom");
        String completed = rows.insert(JobStatus.COMPLETED, 1, 3);

        assertTrue(jobRepository.retryFailedJob(failed));
        assertFalse(jobRepository.retryFailedJob(completed));

        Map<String, Object> row = rows.row(failed);
        assertEquals("QUEUED", row.get("status"));
        assertEquals(4, row.get("max_attempts"));
        assertEquals(3, row.get("attempt_count"));
    }

    private void assertAttempt(
            String jobId,
            int attemptNumber,
            JobStatus status,
            String failureReason
    ) {
        JobAttempt attempt = jobAttemptRepository.findByJobId(jobId).stream()
                .filter(a -> a.attemptNumber() == attemptNumber)
                .findFirst()
                .orElseThrow();

        assertEquals(status, attempt.status());
        assertEquals(failureReason, attempt.failureReason());
    }
}
