package com.taskforge.model;

import com.taskforge.dto.SubmitJobRequest.JobType;
import com.taskforge.handler.JobHandler;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

public class Job {

    public static final int DEFAULT_MAX_ATTEMPTS = 3;
    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    private final String id;
    private final String name;
    private final JobType type;
    private final JobHandler handler;
    private final Instant createdAt;
    private final int maxAttempts;
    private final Duration timeout;
    private final String idempotencyKey;

    private volatile JobStatus status;
    private volatile boolean cancelRequested;
    private Instant startedAt;
    private Instant completedAt;
    private Instant nextAttemptAt;
    private String failureReason;
    private int attemptCount;

    public Job(String name, JobHandler handler) {
        this(builder(name, handler));
    }

    public Job(String name, JobType type, JobHandler handler) {
        this(builder(name, handler).type(type));
    }

    private Job(Builder builder) {
        if (builder.id == null || builder.id.isBlank()) {
            throw new IllegalArgumentException("Job ID cannot be empty");
        }
        if (builder.name == null || builder.name.isBlank()) {
            throw new IllegalArgumentException("Job name cannot be empty");
        }
        if (builder.handler == null) {
            throw new IllegalArgumentException("Job handler cannot be null");
        }
        if (builder.attemptCount < 0) {
            throw new IllegalArgumentException(
                    "Attempt count cannot be negative"
            );
        }
        if (builder.maxAttempts < 1) {
            throw new IllegalArgumentException(
                    "Max attempts must be at least 1"
            );
        }
        if (builder.timeout == null
                || builder.timeout.isNegative()
                || builder.timeout.isZero()) {
            throw new IllegalArgumentException("Timeout must be positive");
        }

        this.id = builder.id;
        this.name = builder.name;
        this.type = builder.type;
        this.handler = builder.handler;
        this.createdAt = builder.createdAt;
        this.maxAttempts = builder.maxAttempts;
        this.timeout = builder.timeout;
        this.idempotencyKey = builder.idempotencyKey;
        this.status = builder.status;
        this.cancelRequested = builder.cancelRequested;
        this.startedAt = builder.startedAt;
        this.completedAt = builder.completedAt;
        this.nextAttemptAt = builder.nextAttemptAt;
        this.failureReason = builder.failureReason;
        this.attemptCount = builder.attemptCount;
    }

    public static Builder builder(String name, JobHandler handler) {
        return new Builder(name, handler);
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

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public Duration getTimeout() {
        return timeout;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public JobStatus getStatus() {
        return status;
    }

    public boolean isCancelRequested() {
        return cancelRequested;
    }

    public synchronized Instant getStartedAt() {
        return startedAt;
    }

    public synchronized Instant getCompletedAt() {
        return completedAt;
    }

    // When a QUEUED job becomes eligible to run again after a failure.
    public synchronized Instant getNextAttemptAt() {
        return nextAttemptAt;
    }

    public synchronized String getFailureReason() {
        return failureReason;
    }

    public synchronized int getAttemptCount() {
        return attemptCount;
    }

    public synchronized boolean hasAttemptsRemaining() {
        return attemptCount < maxAttempts;
    }

    public synchronized boolean markRunning() {
        if (status != JobStatus.QUEUED) {
            return false;
        }

        status = JobStatus.RUNNING;
        startedAt = Instant.now();
        completedAt = null;
        nextAttemptAt = null;
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
        return requeueForRetry(Duration.ZERO);
    }

    public synchronized boolean requeueForRetry(Duration delay) {
        if (status != JobStatus.FAILED) {
            return false;
        }

        status = JobStatus.QUEUED;
        startedAt = null;
        completedAt = null;
        nextAttemptAt = Instant.now().plus(delay);

        return true;
    }

    public synchronized boolean cancel() {
        if (status != JobStatus.QUEUED) {
            return false;
        }

        status = JobStatus.CANCELLED;
        completedAt = Instant.now();
        nextAttemptAt = null;

        return true;
    }

    /*
     * Asks a RUNNING job to stop. The scheduler interrupts the handler
     * and records the attempt as CANCELLED.
     */
    public synchronized boolean requestCancellation() {
        if (status != JobStatus.RUNNING) {
            return false;
        }

        cancelRequested = true;
        return true;
    }

    public synchronized boolean markCancelledWhileRunning() {
        if (status != JobStatus.RUNNING) {
            return false;
        }

        status = JobStatus.CANCELLED;
        completedAt = Instant.now();

        return true;
    }

    public static final class Builder {

        private final String name;
        private final JobHandler handler;
        private String id = UUID.randomUUID().toString();
        private JobType type;
        private Instant createdAt = Instant.now();
        private JobStatus status = JobStatus.QUEUED;
        private Instant startedAt;
        private Instant completedAt;
        private Instant nextAttemptAt;
        private String failureReason;
        private int attemptCount;
        private int maxAttempts = DEFAULT_MAX_ATTEMPTS;
        private Duration timeout = DEFAULT_TIMEOUT;
        private String idempotencyKey;
        private boolean cancelRequested;

        private Builder(String name, JobHandler handler) {
            this.name = name;
            this.handler = handler;
        }

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder type(JobType type) {
            this.type = type;
            return this;
        }

        public Builder createdAt(Instant createdAt) {
            this.createdAt = createdAt;
            return this;
        }

        public Builder status(JobStatus status) {
            this.status = status;
            return this;
        }

        public Builder startedAt(Instant startedAt) {
            this.startedAt = startedAt;
            return this;
        }

        public Builder completedAt(Instant completedAt) {
            this.completedAt = completedAt;
            return this;
        }

        public Builder nextAttemptAt(Instant nextAttemptAt) {
            this.nextAttemptAt = nextAttemptAt;
            return this;
        }

        public Builder failureReason(String failureReason) {
            this.failureReason = failureReason;
            return this;
        }

        public Builder attemptCount(int attemptCount) {
            this.attemptCount = attemptCount;
            return this;
        }

        public Builder maxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
            return this;
        }

        public Builder timeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder idempotencyKey(String idempotencyKey) {
            this.idempotencyKey = idempotencyKey;
            return this;
        }

        public Builder cancelRequested(boolean cancelRequested) {
            this.cancelRequested = cancelRequested;
            return this;
        }

        public Job build() {
            return new Job(this);
        }
    }
}
