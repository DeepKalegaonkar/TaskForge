package com.taskforge.scheduler;

import com.taskforge.model.JobStatus;

import java.time.Duration;

/*
 * How one execution attempt ended.
 * retryDelay is null when the job must not run again.
 */
public record AttemptResult(
        JobStatus status,
        String failureReason,
        Duration retryDelay
) {

    public static AttemptResult completed() {
        return new AttemptResult(JobStatus.COMPLETED, null, null);
    }

    public static AttemptResult cancelled() {
        return new AttemptResult(JobStatus.CANCELLED, null, null);
    }

    public static AttemptResult failed(String reason, Duration retryDelay) {
        return new AttemptResult(JobStatus.FAILED, reason, retryDelay);
    }

    public boolean willRetry() {
        return retryDelay != null;
    }
}
