package com.taskforge.service;

import com.taskforge.model.Job;
import com.taskforge.model.JobStatus;
import com.taskforge.repository.JobAttemptRepository;
import com.taskforge.repository.JobRepository;
import com.taskforge.scheduler.AttemptResult;
import com.taskforge.scheduler.CancellationResult;
import com.taskforge.scheduler.JobStore;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Service
public class DatabaseJobStore implements JobStore {

    private final JobRepository jobRepository;
    private final JobAttemptRepository jobAttemptRepository;

    public DatabaseJobStore(
            JobRepository jobRepository,
            JobAttemptRepository jobAttemptRepository
    ) {
        this.jobRepository = jobRepository;
        this.jobAttemptRepository = jobAttemptRepository;
    }

    // The attempt row is inserted in the claim transaction, so a crash
    // can no longer leave a RUNNING job without attempt history.
    @Override
    @Transactional
    public Optional<Job> claimNext() {
        Optional<Job> claimed = jobRepository.claimNextQueuedJob();

        claimed.ifPresent(job -> jobAttemptRepository.recordAttemptStarted(
                job,
                job.getAttemptCount()
        ));

        return claimed;
    }

    @Override
    public boolean isCancelRequested(String jobId) {
        return jobRepository.isCancelRequested(jobId);
    }

    @Override
    @Transactional
    public CancellationResult cancel(String jobId) {
        if (jobRepository.cancelQueuedJob(jobId)) {
            return CancellationResult.CANCELLED;
        }

        if (jobRepository.requestCancellation(jobId)) {
            return CancellationResult.CANCELLATION_REQUESTED;
        }

        return jobRepository.existsById(jobId)
                ? CancellationResult.NOT_CANCELLABLE
                : CancellationResult.NOT_FOUND;
    }

    /*
     * Records the attempt and moves the job on in one transaction:
     * COMPLETED, CANCELLED, QUEUED for a retry, or permanently FAILED.
     *
     * The job's status must already reflect the attempt's outcome,
     * since the attempt row is written from it.
     */
    @Override
    @Transactional
    public Optional<JobStatus> finishAttempt(Job job, AttemptResult result) {
        int attemptNumber = job.getAttemptCount();

        Optional<Boolean> cancelRequested =
                jobRepository.lockRunningAttempt(job.getId(), attemptNumber);

        if (cancelRequested.isEmpty()) {
            return Optional.empty();
        }

        JobStatus nextStatus = switch (result.status()) {
            case COMPLETED -> JobStatus.COMPLETED;
            case CANCELLED -> JobStatus.CANCELLED;
            default -> {
                if (cancelRequested.get()) {
                    yield JobStatus.CANCELLED;
                }
                yield result.willRetry() ? JobStatus.QUEUED : JobStatus.FAILED;
            }
        };

        jobAttemptRepository.recordAttemptFinished(job, attemptNumber);

        jobRepository.finishRunningJob(
                job.getId(),
                nextStatus,
                result.failureReason(),
                result.retryDelay()
        );

        return Optional.of(nextStatus);
    }
}
