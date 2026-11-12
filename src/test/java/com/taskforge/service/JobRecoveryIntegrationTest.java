package com.taskforge.service;

import com.taskforge.model.Job;
import com.taskforge.model.JobAttempt;
import com.taskforge.model.JobStatus;
import com.taskforge.repository.JobAttemptRepository;
import com.taskforge.repository.JobRepository;
import com.taskforge.support.JobRows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import com.taskforge.support.PostgresIntegrationTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Optional;

import static com.taskforge.service.JobRecoveryService.RECOVERY_FAILURE_REASON;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/*
 * Each test writes the rows a crash at a particular point would leave
 * behind, then runs startup recovery against PostgreSQL.
 */
class JobRecoveryIntegrationTest extends PostgresIntegrationTest {

    @Autowired
    private JobRecoveryService recoveryService;

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
    void crashWithoutAttemptHistoryShouldCreateItAndRequeueWithBackoff() {
        // Jobs claimed before the attempt row joined the claim transaction.
        String id = rows.insert(JobStatus.RUNNING, 1, 3);

        recoveryService.recoverJobs();

        assertJob(id, JobStatus.QUEUED, 1);
        double secondsUntilRetry =
                ((Number) rows.row(id).get("seconds_until_available")).doubleValue();
        assertTrue(secondsUntilRetry > 1 && secondsUntilRetry <= 2,
                "Recovered job should wait the first retry delay (2 s)");

        List<JobAttempt> attempts = jobAttemptRepository.findByJobId(id);
        assertEquals(1, attempts.size());
        assertAttempt(attempts.get(0), 1, JobStatus.FAILED, RECOVERY_FAILURE_REASON);
    }

    @Test
    void crashDuringExecutionShouldCloseRunningAttemptAndRequeue() {
        String id = rows.insert(JobStatus.RUNNING, 2, 3);
        rows.insertAttempt(id, 1, JobStatus.FAILED, "first failure");
        rows.insertAttempt(id, 2, JobStatus.RUNNING, null);

        recoveryService.recoverJobs();

        assertJob(id, JobStatus.QUEUED, 2);
        List<JobAttempt> attempts = jobAttemptRepository.findByJobId(id);
        assertEquals(2, attempts.size());
        assertAttempt(attempts.get(0), 1, JobStatus.FAILED, "first failure");
        assertAttempt(attempts.get(1), 2, JobStatus.FAILED, RECOVERY_FAILURE_REASON);
    }

    @Test
    void crashDuringFinalAttemptShouldFailPermanently() {
        String id = rows.insert(JobStatus.RUNNING, 3, 3);
        rows.insertAttempt(id, 3, JobStatus.RUNNING, null);

        recoveryService.recoverJobs();

        assertJob(id, JobStatus.FAILED, 3);
        assertAttempt(
                jobAttemptRepository.findByJobId(id).get(0),
                3, JobStatus.FAILED, RECOVERY_FAILURE_REASON
        );
    }

    @Test
    void recoveryShouldRespectEachJobsMaxAttempts() {
        String id = rows.insert(JobStatus.RUNNING, 1, 1);
        rows.insertAttempt(id, 1, JobStatus.RUNNING, null);

        recoveryService.recoverJobs();

        assertJob(id, JobStatus.FAILED, 1);
    }

    @Test
    void crashAfterCancellationWasRequestedShouldCancelTheJob() {
        String id = rows.insert(JobStatus.RUNNING, 1, 3);
        rows.insertAttempt(id, 1, JobStatus.RUNNING, null);
        rows.requestCancellation(id);

        recoveryService.recoverJobs();

        assertJob(id, JobStatus.CANCELLED, 1);
        assertAttempt(
                jobAttemptRepository.findByJobId(id).get(0),
                1, JobStatus.FAILED, RECOVERY_FAILURE_REASON
        );
    }

    @Test
    void finishedJobsShouldBeLeftAlone() {
        // FAILED is permanent: a non-retryable failure can leave
        // attempts unused, and recovery must not resurrect the job.
        String permanentlyFailed = rows.insert(JobStatus.FAILED, 1, 3, "Invalid input");
        String exhausted = rows.insert(JobStatus.FAILED, 3, 3, "boom");
        String legacy = rows.insert(JobStatus.FAILED, 0, 3, "before attempt tracking");
        String completed = rows.insert(JobStatus.COMPLETED, 1, 3);

        recoveryService.recoverJobs();

        assertJob(permanentlyFailed, JobStatus.FAILED, 1);
        assertJob(exhausted, JobStatus.FAILED, 3);
        assertJob(legacy, JobStatus.FAILED, 0);
        assertJob(completed, JobStatus.COMPLETED, 1);
    }

    @Test
    void recoveredJobShouldBeClaimedAsTheNextAttempt() {
        String id = rows.insert(JobStatus.RUNNING, 1, 3);
        rows.insertAttempt(id, 1, JobStatus.RUNNING, null);

        recoveryService.recoverJobs();
        rows.makeAvailableNow(id);

        Optional<Job> claimed = jobStore.claimNext();

        assertTrue(claimed.isPresent());
        assertEquals(id, claimed.get().getId());
        assertEquals(2, claimed.get().getAttemptCount());
        assertJob(id, JobStatus.RUNNING, 2);
        assertEquals(2, jobAttemptRepository.findByJobId(id).size());
    }

    @Test
    void runningRecoveryTwiceShouldNotChangeHistoryAgain() {
        String id = rows.insert(JobStatus.RUNNING, 1, 3);

        recoveryService.recoverJobs();
        recoveryService.recoverJobs();

        assertJob(id, JobStatus.QUEUED, 1);
        assertEquals(1, jobAttemptRepository.findByJobId(id).size());
    }

    private void assertJob(String id, JobStatus status, int attemptCount) {
        Job job = jobRepository.findById(id).orElseThrow();
        assertEquals(status, job.getStatus());
        assertEquals(attemptCount, job.getAttemptCount());
    }

    private void assertAttempt(
            JobAttempt attempt,
            int attemptNumber,
            JobStatus status,
            String failureReason
    ) {
        assertEquals(attemptNumber, attempt.attemptNumber());
        assertEquals(status, attempt.status());
        assertEquals(failureReason, attempt.failureReason());
    }
}
