package com.taskforge.handler;

/*
 * Thrown by a handler when retrying cannot help, such as invalid input.
 * The job is marked FAILED immediately, whatever attempts remain.
 */
public class NonRetryableJobException extends Exception {

    public NonRetryableJobException(String message) {
        super(message);
    }
}
