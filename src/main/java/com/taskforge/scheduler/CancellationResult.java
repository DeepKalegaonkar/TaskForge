package com.taskforge.scheduler;

public enum CancellationResult {
    // The job was QUEUED and will never run.
    CANCELLED,
    // The job is RUNNING; its handler is being interrupted.
    CANCELLATION_REQUESTED,
    // The job has already finished.
    NOT_CANCELLABLE,
    NOT_FOUND
}
