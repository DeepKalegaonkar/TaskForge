package com.taskforge.scheduler;

import com.taskforge.model.Job;
import com.taskforge.model.JobStatus;

import java.util.Optional;

/*
 * Durable job storage used by a database-backed scheduler.
 * Each method is atomic, so a crash never leaves a job half-updated.
 */
public interface JobStore {

    /*
     * Claims the oldest QUEUED job whose retry delay has passed, marks it
     * RUNNING, increments its attempt count and records the attempt.
     */
    Optional<Job> claimNext();

    boolean isCancelRequested(String jobId);

    /*
     * Cancels a QUEUED job, or flags a RUNNING job so that whichever
     * worker runs it stops and records it as CANCELLED.
     */
    CancellationResult cancel(String jobId);

    /*
     * Records the result of the job's current attempt and moves the job
     * to its next status. Returns empty when the attempt is no longer the
     * job's current RUNNING attempt, e.g. it was already recovered.
     */
    Optional<JobStatus> finishAttempt(Job job, AttemptResult result);
}
