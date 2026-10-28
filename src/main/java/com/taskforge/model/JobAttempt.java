package com.taskforge.model;

import java.time.Instant;

public record JobAttempt(
        String jobId,
        int attemptNumber,
        JobStatus status,
        Instant startedAt,
        Instant completedAt,
        String failureReason
) {
}
