package com.taskforge.exception;

/*
 * An Idempotency-Key was reused with a request that differs
 * from the one that created the original job.
 */
public class IdempotencyKeyConflictException extends RuntimeException {

    public IdempotencyKeyConflictException(String idempotencyKey) {
        super("Idempotency key '" + idempotencyKey
                + "' was already used with a different request.");
    }
}
