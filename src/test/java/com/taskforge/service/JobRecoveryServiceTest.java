package com.taskforge.service;

import com.taskforge.model.Job;
import com.taskforge.model.JobStatus;
import com.taskforge.repository.JobAttemptRepository;
import com.taskforge.repository.JobRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JobRecoveryServiceTest {

    @Mock
    private JobRepository jobRepository;

    @Mock
    private JobAttemptRepository jobAttemptRepository;

    @Test
    void shouldCreateMissingHistoryAndRequeueInterruptedJob() {
        Job job = interruptedJob(1);
        JobRecoveryService recoveryService = new JobRecoveryService(
                jobRepository,
                jobAttemptRepository
        );

        when(jobRepository.findAllRunningJobs()).thenReturn(List.of(job));
        when(jobRepository.recoverInterruptedJob(
                job.getId(),
                "Execution interrupted by application shutdown or crash"
        )).thenReturn(true);
        when(jobAttemptRepository.existsByJobIdAndAttemptNumber(
                job.getId(),
                1
        )).thenReturn(false);
        when(jobRepository.requeueFailedJob(job)).thenReturn(true);

        recoveryService.recoverJobs();

        assertEquals(JobStatus.QUEUED, job.getStatus());
        verify(jobAttemptRepository).recordAttemptStarted(job, 1);
        verify(jobAttemptRepository).failInterruptedAttempt(
                job.getId(), 1, JobRecoveryService.RECOVERY_FAILURE_REASON
        );
        verify(jobRepository).requeueFailedJob(job);
    }

    @Test
    void shouldNotDuplicateExistingHistoryForFinalAttempt() {
        Job job = interruptedJob(3);
        JobRecoveryService recoveryService = new JobRecoveryService(
                jobRepository,
                jobAttemptRepository
        );

        when(jobRepository.findAllRunningJobs()).thenReturn(List.of(job));
        when(jobRepository.recoverInterruptedJob(
                job.getId(),
                "Execution interrupted by application shutdown or crash"
        )).thenReturn(true);
        when(jobAttemptRepository.existsByJobIdAndAttemptNumber(
                job.getId(),
                3
        )).thenReturn(true);

        recoveryService.recoverJobs();

        assertEquals(JobStatus.FAILED, job.getStatus());
        verify(jobAttemptRepository, never()).recordAttemptStarted(job, 3);
        verify(jobAttemptRepository).failInterruptedAttempt(
                job.getId(), 3, JobRecoveryService.RECOVERY_FAILURE_REASON
        );
        verify(jobRepository, never()).requeueFailedJob(job);
    }

    @Test
    void shouldSkipJobAlreadyRecoveredByAnotherInstance() {
        Job job = interruptedJob(1);
        JobRecoveryService recoveryService = new JobRecoveryService(
                jobRepository,
                jobAttemptRepository
        );

        when(jobRepository.findAllRunningJobs()).thenReturn(List.of(job));
        when(jobRepository.recoverInterruptedJob(
                job.getId(),
                "Execution interrupted by application shutdown or crash"
        )).thenReturn(false);

        recoveryService.recoverJobs();

        verifyNoInteractions(jobAttemptRepository);
        verify(jobRepository, never()).requeueFailedJob(job);
    }

    private Job interruptedJob(int attemptCount) {
        return new Job(
                "interrupted-job",
                "Recovery test",
                null,
                () -> {},
                Instant.parse("2026-10-28T09:00:00Z"),
                JobStatus.RUNNING,
                Instant.parse("2026-10-28T09:30:00Z"),
                null,
                null,
                attemptCount
        );
    }
}
