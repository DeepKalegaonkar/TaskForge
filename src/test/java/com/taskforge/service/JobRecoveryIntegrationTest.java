package com.taskforge.service;

import com.taskforge.model.Job;
import com.taskforge.model.JobAttempt;
import com.taskforge.model.JobStatus;
import com.taskforge.repository.JobAttemptRepository;
import com.taskforge.repository.JobRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.taskforge.service.JobRecoveryService.RECOVERY_FAILURE_REASON;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/*
 * Each test writes the rows a crash at a particular point would leave
 * behind, then runs startup recovery against PostgreSQL.
 */
@SpringBootTest
class JobRecoveryIntegrationTest {

    @Autowired
    private JobRecoveryService recoveryService;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobAttemptRepository jobAttemptRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final List<String> createdJobIds = new ArrayList<>();

    @AfterEach
    void cleanUp() {
        for (String id : createdJobIds) {
            jdbcTemplate.update("DELETE FROM job_attempts WHERE job_id = ?", id);
            jdbcTemplate.update("DELETE FROM jobs WHERE id = ?", id);
        }
    }

    @Test
    void crashAfterClaimBeforeAttemptInsertShouldCreateHistoryAndRequeue() {
        String id = insertJob(JobStatus.RUNNING, 1, null);

        recoveryService.recoverJobs();

        assertJob(id, JobStatus.QUEUED, 1);
        List<JobAttempt> attempts = jobAttemptRepository.findByJobId(id);
        assertEquals(1, attempts.size());
        assertAttempt(attempts.get(0), 1, JobStatus.FAILED, RECOVERY_FAILURE_REASON);
    }

    @Test
    void crashDuringExecutionShouldCloseRunningAttemptAndRequeue() {
        String id = insertJob(JobStatus.RUNNING, 2, null);
        insertAttempt(id, 1, JobStatus.FAILED, "first failure");
        insertAttempt(id, 2, JobStatus.RUNNING, null);

        recoveryService.recoverJobs();

        assertJob(id, JobStatus.QUEUED, 2);
        List<JobAttempt> attempts = jobAttemptRepository.findByJobId(id);
        assertEquals(2, attempts.size());
        assertAttempt(attempts.get(0), 1, JobStatus.FAILED, "first failure");
        assertAttempt(attempts.get(1), 2, JobStatus.FAILED, RECOVERY_FAILURE_REASON);
    }

    @Test
    void crashDuringFinalAttemptShouldLeaveJobFailed() {
        String id = insertJob(JobStatus.RUNNING, 3, null);
        insertAttempt(id, 3, JobStatus.RUNNING, null);

        recoveryService.recoverJobs();

        assertJob(id, JobStatus.FAILED, 3);
        assertAttempt(
                jobAttemptRepository.findByJobId(id).get(0),
                3, JobStatus.FAILED, RECOVERY_FAILURE_REASON
        );
    }

    @Test
    void crashDuringRetryDelayShouldRequeueFailedJob() {
        String id = insertJob(JobStatus.FAILED, 1, "handler failed");
        insertAttempt(id, 1, JobStatus.FAILED, "handler failed");

        recoveryService.recoverJobs();

        assertJob(id, JobStatus.QUEUED, 1);
        assertAttempt(
                jobAttemptRepository.findByJobId(id).get(0),
                1, JobStatus.FAILED, "handler failed"
        );
    }

    @Test
    void crashBeforeAttemptResultShouldCloseAttemptWithHandlerReason() {
        String id = insertJob(JobStatus.FAILED, 2, "handler failed");
        insertAttempt(id, 2, JobStatus.RUNNING, null);

        recoveryService.recoverJobs();

        assertJob(id, JobStatus.QUEUED, 2);
        assertAttempt(
                jobAttemptRepository.findByJobId(id).get(0),
                2, JobStatus.FAILED, "handler failed"
        );
    }

    @Test
    void exhaustedAndLegacyFailedJobsShouldStayFailed() {
        String exhausted = insertJob(JobStatus.FAILED, 3, "handler failed");
        String legacy = insertJob(JobStatus.FAILED, 0, "before attempt tracking");

        recoveryService.recoverJobs();

        assertJob(exhausted, JobStatus.FAILED, 3);
        assertJob(legacy, JobStatus.FAILED, 0);
    }

    @Test
    void recoveredJobShouldBeClaimedAsTheNextAttempt() {
        String id = insertJob(JobStatus.RUNNING, 1, null);
        insertAttempt(id, 1, JobStatus.RUNNING, null);

        recoveryService.recoverJobs();

        // The test row is the oldest QUEUED job, so it is claimed first.
        Optional<Job> claimed = jobRepository.claimNextQueuedJob();

        assertTrue(claimed.isPresent());
        assertEquals(id, claimed.get().getId());
        assertEquals(2, claimed.get().getAttemptCount());
        assertJob(id, JobStatus.RUNNING, 2);
    }

    @Test
    void runningRecoveryTwiceShouldNotChangeHistoryAgain() {
        String id = insertJob(JobStatus.RUNNING, 1, null);

        recoveryService.recoverJobs();
        recoveryService.recoverJobs();

        assertJob(id, JobStatus.QUEUED, 1);
        assertEquals(1, jobAttemptRepository.findByJobId(id).size());
    }

    private String insertJob(JobStatus status, int attemptCount, String failureReason) {
        String id = UUID.randomUUID().toString();
        createdJobIds.add(id);

        jdbcTemplate.update(
                """
                INSERT INTO jobs (
                    id, name, job_type, status, created_at,
                    started_at, failure_reason, attempt_count
                )
                VALUES (?, 'recovery-test', 'GENERATE_REPORT', ?,
                        TIMESTAMPTZ '2000-01-01 00:00:00+00',
                        CASE WHEN ? > 0 THEN NOW() END, ?, ?)
                """,
                id, status.name(), attemptCount, failureReason, attemptCount
        );

        return id;
    }

    private void insertAttempt(
            String jobId,
            int attemptNumber,
            JobStatus status,
            String failureReason
    ) {
        jdbcTemplate.update(
                """
                INSERT INTO job_attempts (
                    job_id, attempt_number, status,
                    started_at, completed_at, failure_reason
                )
                VALUES (?, ?, ?, NOW(),
                        CASE WHEN ? = 'RUNNING' THEN NULL ELSE NOW() END, ?)
                """,
                jobId, attemptNumber, status.name(), status.name(), failureReason
        );
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
