package com.taskforge.scheduler;

import java.time.Duration;

/*
 * Exponential backoff: the delay after attempt n is
 * initialDelay * multiplier^(n - 1), capped at maxDelay.
 */
public record RetryPolicy(
        Duration initialDelay,
        double multiplier,
        Duration maxDelay
) {

    public static final RetryPolicy DEFAULT = new RetryPolicy(
            Duration.ofSeconds(2),
            2.0,
            Duration.ofMinutes(5)
    );

    public RetryPolicy {
        if (initialDelay == null || initialDelay.isNegative()) {
            throw new IllegalArgumentException(
                    "Initial retry delay cannot be negative"
            );
        }
        if (multiplier < 1.0) {
            throw new IllegalArgumentException(
                    "Retry multiplier must be at least 1"
            );
        }
        if (maxDelay == null || maxDelay.compareTo(initialDelay) < 0) {
            throw new IllegalArgumentException(
                    "Max retry delay cannot be less than the initial delay"
            );
        }
    }

    public Duration delayAfterAttempt(int attemptNumber) {
        if (attemptNumber < 1) {
            throw new IllegalArgumentException(
                    "Attempt number must be at least 1"
            );
        }

        double delayMs = initialDelay.toMillis()
                * Math.pow(multiplier, attemptNumber - 1);

        return delayMs >= maxDelay.toMillis()
                ? maxDelay
                : Duration.ofMillis((long) delayMs);
    }
}
