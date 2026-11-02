
package com.taskforge.dto;

import com.taskforge.dto.SubmitJobRequest.JobType;
import com.taskforge.model.Job;
import com.taskforge.model.JobStatus;

import java.time.Instant;

public record JobResponse(
        String id,
        String name,
        JobType type,
        JobStatus status,
        int attemptCount,
        int maxAttempts,
        long timeoutSeconds,
        Instant createdAt,
        Instant startedAt,
        Instant completedAt,
        Instant nextAttemptAt,
        boolean cancelRequested,
        String failureReason,
        String idempotencyKey
) {
    public static JobResponse from(Job job) {
        return new JobResponse(
                job.getId(),
                job.getName(),
                job.getType(),
                job.getStatus(),
                job.getAttemptCount(),
                job.getMaxAttempts(),
                job.getTimeout().toSeconds(),
                job.getCreatedAt(),
                job.getStartedAt(),
                job.getCompletedAt(),
                job.getNextAttemptAt(),
                job.isCancelRequested(),
                job.getFailureReason(),
                job.getIdempotencyKey()
        );
    }
}
