package com.taskforge.scheduler;

import com.taskforge.model.Job;
import com.taskforge.model.JobStatus;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/*
 * In-memory JobStore that hands out pre-claimed jobs and records
 * every finished attempt, for testing the database-backed scheduler.
 */
class FakeJobStore implements JobStore {

    record FinishedAttempt(
            String jobId,
            int attemptNumber,
            JobStatus jobStatusWhenRecorded,
            AttemptResult result
    ) {
    }

    private final BlockingQueue<Job> claimable = new LinkedBlockingQueue<>();
    private final BlockingQueue<FinishedAttempt> finished = new LinkedBlockingQueue<>();
    private final Set<String> cancelRequested = ConcurrentHashMap.newKeySet();

    void addClaimable(Job job) {
        claimable.add(job);
    }

    // Simulates DELETE /api/jobs/{id} handled by another instance.
    void requestCancellationElsewhere(String jobId) {
        cancelRequested.add(jobId);
    }

    FinishedAttempt awaitFinishedAttempt(long timeout, TimeUnit unit)
            throws InterruptedException {
        FinishedAttempt attempt = finished.poll(timeout, unit);

        if (attempt == null) {
            throw new AssertionError("No attempt finished within " + timeout + " " + unit);
        }
        return attempt;
    }

    boolean hasFinishedAttempts() {
        return !finished.isEmpty();
    }

    @Override
    public Optional<Job> claimNext() {
        return Optional.ofNullable(claimable.poll());
    }

    @Override
    public boolean isCancelRequested(String jobId) {
        return cancelRequested.contains(jobId);
    }

    @Override
    public CancellationResult cancel(String jobId) {
        cancelRequested.add(jobId);
        return CancellationResult.CANCELLATION_REQUESTED;
    }

    @Override
    public Optional<JobStatus> finishAttempt(Job job, AttemptResult result) {
        finished.add(new FinishedAttempt(
                job.getId(), job.getAttemptCount(), job.getStatus(), result
        ));

        return Optional.of(result.willRetry() ? JobStatus.QUEUED : result.status());
    }
}
