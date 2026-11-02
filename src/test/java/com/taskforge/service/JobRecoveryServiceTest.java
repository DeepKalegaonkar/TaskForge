package com.taskforge.service;

import com.taskforge.model.Job;
import com.taskforge.model.JobStatus;
import com.taskforge.repository.JobAttemptRepository;
import com.taskforge.repository.JobRepository;
import com.taskforge.scheduler.AttemptResult;
import com.taskforge.scheduler.JobStore;
import com.taskforge.scheduler.RetryPolicy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static com.taskforge.service.JobRecoveryService.RECOVERY_FAILURE_REASON;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JobRecoveryServiceTest {

    @Mock
    private JobRepository jobRepository;

    @Mock
    private JobAttemptRepository jobAttemptRepository;

    @Mock
    private JobStore jobStore;

    private JobRecoveryService recoveryService;

    @BeforeEach
    void setUp() {
        recoveryService = new JobRecoveryService(
                jobRepository,
                jobAttemptRepository,
                jobStore,
                RetryPolicy.DEFAULT
        );
    }

    @Test
    void shouldCreateMissingHistoryAndRetryWithBackoff() {
        Job job = interruptedJob(2, 3);

        when(jobRepository.findAllRunningJobs()).thenReturn(List.of(job));
        when(jobAttemptRepository.existsByJobIdAndAttemptNumber(job.getId(), 2))
                .thenReturn(false);
        when(jobStore.finishAttempt(any(), any()))
                .thenReturn(Optional.of(JobStatus.QUEUED));

        recoveryService.recoverJobs();

        verify(jobAttemptRepository).recordAttemptStarted(job, 2);
        verify(jobStore).finishAttempt(
                job,
                AttemptResult.failed(RECOVERY_FAILURE_REASON, Duration.ofSeconds(4))
        );
    }

    @Test
    void shouldNotRetryAJobOnItsFinalAttempt() {
        Job job = interruptedJob(3, 3);

        when(jobRepository.findAllRunningJobs()).thenReturn(List.of(job));
        when(jobAttemptRepository.existsByJobIdAndAttemptNumber(job.getId(), 3))
                .thenReturn(true);
        when(jobStore.finishAttempt(any(), any()))
                .thenReturn(Optional.of(JobStatus.FAILED));

        recoveryService.recoverJobs();

        verify(jobAttemptRepository, never()).recordAttemptStarted(job, 3);
        verify(jobStore).finishAttempt(
                job,
                AttemptResult.failed(RECOVERY_FAILURE_REASON, null)
        );
    }

    @Test
    void shouldRejectARunningJobWithoutAnAttempt() {
        Job job = interruptedJob(0, 3);

        when(jobRepository.findAllRunningJobs()).thenReturn(List.of(job));

        assertThrows(IllegalStateException.class, recoveryService::recoverJobs);
    }

    private Job interruptedJob(int attemptCount, int maxAttempts) {
        return Job.builder("Recovery test", () -> {})
                .id("interrupted-job")
                .createdAt(Instant.parse("2026-10-28T09:00:00Z"))
                .status(JobStatus.RUNNING)
                .startedAt(Instant.parse("2026-10-28T09:30:00Z"))
                .attemptCount(attemptCount)
                .maxAttempts(maxAttempts)
                .build();
    }
}
