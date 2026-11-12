package com.taskforge.performance;

import com.taskforge.model.Job;
import com.taskforge.model.JobStatus;
import com.taskforge.scheduler.AttemptResult;
import com.taskforge.scheduler.CancellationResult;
import com.taskforge.scheduler.JobStore;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/*
 * Records System.nanoTime() at each point a job passes through the
 * store. All timings come from one monotonic clock in one JVM, so they
 * are not affected by skew between the host and the database container.
 */
class TimingJobStore implements JobStore {

    static final class Timing {
        volatile long claimStarted;
        volatile long claimed;
        volatile long finishStarted;
        volatile long finished;
    }

    private final JobStore delegate;
    private final Map<String, Timing> timings = new ConcurrentHashMap<>();

    TimingJobStore(JobStore delegate) {
        this.delegate = delegate;
    }

    Map<String, Timing> timings() {
        return timings;
    }

    @Override
    public Optional<Job> claimNext() {
        long started = System.nanoTime();
        Optional<Job> claimed = delegate.claimNext();
        long now = System.nanoTime();

        claimed.ifPresent(job -> {
            Timing timing = timings.computeIfAbsent(job.getId(), id -> new Timing());
            timing.claimStarted = started;
            timing.claimed = now;
        });
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
        long started = System.nanoTime();
        Optional<JobStatus> next = delegate.finishAttempt(job, result);

        Timing timing = timings.get(job.getId());
        timing.finishStarted = started;
        timing.finished = System.nanoTime();
        return next;
    }
}
