package com.taskforge.support;

import com.taskforge.model.Job;
import com.taskforge.model.JobStatus;
import com.taskforge.scheduler.AttemptResult;
import com.taskforge.scheduler.CancellationResult;
import com.taskforge.scheduler.JobStore;
import org.springframework.dao.DataAccessResourceFailureException;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/*
 * Wraps a real JobStore. It counts the jobs claimed through it, which
 * identifies the instance that ran a job, and can fail the next n claims
 * or result writes the way a lost database connection would.
 */
public class FaultInjectingJobStore implements JobStore {

    private final JobStore delegate;
    private final AtomicInteger claims = new AtomicInteger();
    private final AtomicInteger failingClaims = new AtomicInteger();
    private final AtomicInteger failingFinishes = new AtomicInteger();

    public FaultInjectingJobStore(JobStore delegate) {
        this.delegate = delegate;
    }

    public void failNextClaims(int count) {
        failingClaims.set(count);
    }

    public void failNextFinishes(int count) {
        failingFinishes.set(count);
    }

    public int claims() {
        return claims.get();
    }

    public int pendingInjectedFailures() {
        return failingClaims.get() + failingFinishes.get();
    }

    @Override
    public Optional<Job> claimNext() {
        if (failingClaims.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
            throw new DataAccessResourceFailureException("Injected claim failure");
        }

        Optional<Job> claimed = delegate.claimNext();
        claimed.ifPresent(job -> claims.incrementAndGet());
        return claimed;
    }

    @Override
    public boolean isCancelRequested(String jobId) {
        return delegate.isCancelRequested(jobId);
    }

    @Override
    public CancellationResult cancel(String jobId) {
        return delegate.cancel(jobId);
    }

    @Override
    public Optional<JobStatus> finishAttempt(Job job, AttemptResult result) {
        if (failingFinishes.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
            throw new DataAccessResourceFailureException("Injected write failure");
        }
        return delegate.finishAttempt(job, result);
    }
}
