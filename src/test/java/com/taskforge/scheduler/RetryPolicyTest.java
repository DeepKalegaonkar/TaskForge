package com.taskforge.scheduler;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RetryPolicyTest {

    @Test
    void delayShouldDoubleAfterEachAttempt() {
        RetryPolicy policy = RetryPolicy.DEFAULT;

        assertEquals(Duration.ofSeconds(2), policy.delayAfterAttempt(1));
        assertEquals(Duration.ofSeconds(4), policy.delayAfterAttempt(2));
        assertEquals(Duration.ofSeconds(8), policy.delayAfterAttempt(3));
    }

    @Test
    void delayShouldBeCappedAtTheMaximum() {
        RetryPolicy policy = new RetryPolicy(
                Duration.ofSeconds(1), 3.0, Duration.ofSeconds(10)
        );

        assertEquals(Duration.ofSeconds(9), policy.delayAfterAttempt(3));
        assertEquals(Duration.ofSeconds(10), policy.delayAfterAttempt(4));
        assertEquals(Duration.ofSeconds(10), policy.delayAfterAttempt(60));
    }

    @Test
    void invalidSettingsShouldBeRejected() {
        assertThrows(IllegalArgumentException.class, () -> new RetryPolicy(
                Duration.ofSeconds(-1), 2.0, Duration.ofSeconds(1)
        ));
        assertThrows(IllegalArgumentException.class, () -> new RetryPolicy(
                Duration.ofSeconds(1), 0.5, Duration.ofSeconds(1)
        ));
        assertThrows(IllegalArgumentException.class, () -> new RetryPolicy(
                Duration.ofSeconds(5), 2.0, Duration.ofSeconds(1)
        ));
        assertThrows(IllegalArgumentException.class,
                () -> RetryPolicy.DEFAULT.delayAfterAttempt(0));
    }
}
