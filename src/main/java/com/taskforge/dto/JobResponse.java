
package com.taskforge.dto;

import com.taskforge.model.Job;
import com.taskforge.model.JobStatus;

import java.time.Instant;

public record JobResponse(
        String id,
        String name,
        JobStatus status,
        Instant createdAt,
        Instant startedAt,
        Instant completedAt,
        String failureReason
) {
    public static JobResponse from(Job job) {
        return new JobResponse(
                job.getId(),
                job.getName(),
                job.getStatus(),
                job.getCreatedAt(),
                job.getStartedAt(),
                job.getCompletedAt(),
                job.getFailureReason()
        );
    }
}
