package com.taskforge.model;

import com.taskforge.dto.SubmitJobRequest.JobType;
import com.taskforge.handler.JobHandler;

import java.time.Instant;
import java.util.UUID;

public class Job {

    private final String id;
    private final String name;
    private final JobType type;
    private final JobHandler handler;
    private final Instant createdAt;

    private volatile JobStatus status;
    private Instant startedAt;
    private Instant completedAt;
    private String failureReason;
    private int attemptCount;

    public Job(String name, JobHandler handler) {
        this(
                UUID.randomUUID().toString(),
                name,
                null,
                handler,
                Instant.now(),
                JobStatus.QUEUED,
                null,
                null,
                null,
                0
        );
    }

    public Job(String name, JobType type, JobHandler handler) {
        this(
                UUID.randomUUID().toString(),
                name,
                type,
                handler,
                Instant.now(),
                JobStatus.QUEUED,
                null,
                null,
                null,
                0
        );
    }

    // Backward-compatible constructor.
    public Job(
            String id,
            String name,
            JobType type,
            JobHandler handler,
            Instant createdAt,
            JobStatus status,
            Instant startedAt,
            Instant completedAt,
            String failureReason
    ) {
        this(
                id,
                name,
                type,
                handler,
                createdAt,
                status,
                startedAt,
                completedAt,
                failureReason,
                0
        );
    }

    // Restores a job, including its persisted attempt count.
    public Job(
            String id,
            String name,
            JobType type,
            JobHandler handler,
            Instant createdAt,
            JobStatus status,
            Instant startedAt,
            Instant completedAt,
            String failureReason,
            int attemptCount
    ) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("Job ID cannot be empty");
        }
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Job name cannot be empty");
        }
        if (handler == null) {
            throw new IllegalArgumentException("Job handler cannot be null");
        }
        if (attemptCount < 0) {
            throw new IllegalArgumentException(
                    "Attempt count cannot be negative"
            );
        }

        this.id = id;
        this.name = name;
        this.type = type;
        this.handler = handler;
        this.createdAt = createdAt;
        this.status = status;
        this.startedAt = startedAt;
        this.completedAt = completedAt;
        this.failureReason = failureReason;
        this.attemptCount = attemptCount;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public JobType getType() {
        return type;
    }

    public JobHandler getHandler() {
        return handler;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public JobStatus getStatus() {
        return status;
    }

    public synchronized Instant getStartedAt() {
        return startedAt;
    }

    public synchronized Instant getCompletedAt() {
        return completedAt;
    }

    public synchronized String getFailureReason() {
        return failureReason;
    }

    public synchronized int getAttemptCount() {
        return attemptCount;
    }

    public synchronized boolean markRunning() {
        if (status != JobStatus.QUEUED) {
            return false;
        }

        status = JobStatus.RUNNING;
        startedAt = Instant.now();
        completedAt = null;
        attemptCount++;

        return true;
    }

    public synchronized boolean markCompleted() {
        if (status != JobStatus.RUNNING) {
            return false;
        }

        status = JobStatus.COMPLETED;
        completedAt = Instant.now();
        failureReason = null;

        return true;
    }

    public synchronized boolean markFailed(String reason) {
        if (status != JobStatus.RUNNING) {
            return false;
        }

        status = JobStatus.FAILED;
        failureReason = reason;
        completedAt = Instant.now();

        return true;
    }

    public synchronized boolean requeueForRetry() {
        if (status != JobStatus.FAILED) {
            return false;
        }

        status = JobStatus.QUEUED;
        startedAt = null;
        completedAt = null;

        return true;
    }

    public synchronized boolean cancel() {
        if (status != JobStatus.QUEUED) {
            return false;
        }

        status = JobStatus.CANCELLED;
        completedAt = Instant.now();

        return true;
    }
}
