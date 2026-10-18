
package com.taskforge.model;

import com.taskforge.handler.JobHandler;

import java.time.Instant;
import java.util.UUID;

public class Job {

    private final String id;
    private final String name;
    private final JobHandler handler;
    private final Instant createdAt;

    private volatile JobStatus status;
    private Instant startedAt;
    private Instant completedAt;
    private String failureReason;

    public Job(String name, JobHandler handler) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException(
                    "Job name cannot be empty"
            );
        }

        if (handler == null) {
            throw new IllegalArgumentException(
                    "Job handler cannot be null"
            );
        }

        this.id = UUID.randomUUID().toString();
        this.name = name;
        this.handler = handler;
        this.createdAt = Instant.now();
        this.status = JobStatus.QUEUED;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
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

    public synchronized boolean markRunning() {
        if (status != JobStatus.QUEUED) {
            return false;
        }

        status = JobStatus.RUNNING;
        startedAt = Instant.now();
        return true;
    }

    public synchronized boolean markCompleted() {
        if (status != JobStatus.RUNNING) {
            return false;
        }

        status = JobStatus.COMPLETED;
        completedAt = Instant.now();
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

    public synchronized boolean cancel() {
        if (status != JobStatus.QUEUED) {
            return false;
        }

        status = JobStatus.CANCELLED;
        completedAt = Instant.now();
        return true;
    }
}
