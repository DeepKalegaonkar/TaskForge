package com.taskforge.support;

import org.springframework.dao.DataAccessException;

import java.time.Duration;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.fail;

/*
 * Polls a condition until it holds. Database errors while polling are
 * retried, so the condition can be checked during an injected outage.
 */
public final class Await {

    private Await() {
    }

    public static void until(
            Duration timeout,
            BooleanSupplier condition,
            Supplier<String> description
    ) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();

        while (true) {
            try {
                if (condition.getAsBoolean()) {
                    return;
                }
            } catch (DataAccessException e) {
                // Connection killed by the test; try again.
            }

            if (System.nanoTime() > deadline) {
                fail("Timed out after " + timeout + ": " + description.get());
            }
            Thread.sleep(25);
        }
    }

    public static void until(Duration timeout, BooleanSupplier condition, String description)
            throws InterruptedException {
        until(timeout, condition, () -> description);
    }
}
