package com.taskforge.repository;

import com.taskforge.dto.SubmitJobRequest.JobType;
import com.taskforge.model.Job;
import com.taskforge.model.JobAttempt;
import com.taskforge.model.JobStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
class JobAttemptRepositoryTest {

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
    void shouldCreateAndFinishAttemptHistory() {
        Job job = persistedJob();

        assertTrue(job.markRunning());
        jobAttemptRepository.recordAttemptStarted(job, 1);

        JobAttempt started = jobAttemptRepository.findByJobId(job.getId()).get(0);
        assertEquals(1, started.attemptNumber());
        assertEquals(JobStatus.RUNNING, started.status());
        assertNotNull(started.startedAt());
        assertNull(started.completedAt());

        job.markFailed("handler failed");
        jobAttemptRepository.recordAttemptFinished(job, 1);

        List<JobAttempt> attempts = jobAttemptRepository.findByJobId(job.getId());
        assertEquals(1, attempts.size());
        assertEquals(JobStatus.FAILED, attempts.get(0).status());
        assertEquals("handler failed", attempts.get(0).failureReason());
        assertNotNull(attempts.get(0).completedAt());
    }

    @Test
    void shouldRejectDuplicateAttemptNumber() {
        Job job = persistedJob();
        assertTrue(job.markRunning());

        jobAttemptRepository.recordAttemptStarted(job, 1);

        assertThrows(
                org.springframework.dao.DuplicateKeyException.class,
                () -> jobAttemptRepository.recordAttemptStarted(job, 1)
        );
    }

    @Test
    void finishingMissingAttemptShouldFail() {
        Job job = persistedJob();
        assertTrue(job.markRunning());
        job.markCompleted();

        assertThrows(
                IllegalStateException.class,
                () -> jobAttemptRepository.recordAttemptFinished(job, 1)
        );
    }

    @Test
    void failInterruptedAttemptShouldOnlyTouchRunningAttempts() {
        Job job = persistedJob();
        assertTrue(job.markRunning());
        jobAttemptRepository.recordAttemptStarted(job, 1);

        assertTrue(jobAttemptRepository.failInterruptedAttempt(
                job.getId(), 1, "crashed"
        ));
        assertFalse(jobAttemptRepository.failInterruptedAttempt(
                job.getId(), 1, "crashed again"
        ));

        JobAttempt attempt = jobAttemptRepository.findByJobId(job.getId()).get(0);
        assertEquals(JobStatus.FAILED, attempt.status());
        assertEquals("crashed", attempt.failureReason());
    }

    @Test
    void shouldRejectInvalidAttemptNumberBeforeWriting() {
        Job job = persistedJob();

        assertThrows(
                IllegalArgumentException.class,
                () -> jobAttemptRepository.recordAttemptStarted(job, 0)
        );
        assertFalse(jobAttemptRepository.existsByJobId(job.getId()));
    }

    private Job persistedJob() {
        Job job = new Job("attempt-history-test", JobType.GENERATE_REPORT, () -> {});
        jobRepository.save(job);
        createdJobIds.add(job.getId());
        return job;
    }
}
