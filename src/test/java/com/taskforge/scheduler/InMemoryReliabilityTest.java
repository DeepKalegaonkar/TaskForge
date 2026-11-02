package com.taskforge.scheduler;

import com.taskforge.handler.NonRetryableJobException;
import com.taskforge.model.Job;
import com.taskforge.model.JobStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryReliabilityTest {

    private JobScheduler scheduler;

    @AfterEach
    void stopScheduler() {
        if (scheduler != null) {
            scheduler.shutdownGracefully(Duration.ofSeconds(5));
        }
    }

    @Test
    void waitingForARetryShouldNotBlockTheWorker() throws Exception {
        scheduler = scheduler(new RetryPolicy(
                Duration.ofSeconds(1), 2.0, Duration.ofSeconds(1)
        ));
        List<String> executions = new CopyOnWriteArrayList<>();
        CountDownLatch firstFailure = new CountDownLatch(1);

        Job failing = Job.builder("Failing", () -> {
            executions.add("failing");
            firstFailure.countDown();
            throw new RuntimeException("Simulated failure");
        }).maxAttempts(2).build();

        Job other = new Job("Other", () -> executions.add("other"));

        scheduler.submit(failing);
        assertTrue(firstFailure.await(2, TimeUnit.SECONDS));
        scheduler.submit(other);

        scheduler.shutdown();
        assertTrue(scheduler.awaitTermination(5, TimeUnit.SECONDS));

        // With one worker, "other" runs during the failing job's
        // one-second retry delay instead of after it.
        assertEquals(List.of("failing", "other", "failing"), executions);
        assertEquals(JobStatus.FAILED, failing.getStatus());
        assertEquals(2, failing.getAttemptCount());
    }

    @Test
    void jobWaitingForARetryCanBeCancelled() throws Exception {
        scheduler = scheduler(new RetryPolicy(
                Duration.ofSeconds(1), 2.0, Duration.ofSeconds(1)
        ));
        AtomicInteger executions = new AtomicInteger();
        CountDownLatch firstFailure = new CountDownLatch(1);

        Job job = Job.builder("Retrying", () -> {
            executions.incrementAndGet();
            firstFailure.countDown();
            throw new RuntimeException("Simulated failure");
        }).build();

        scheduler.submit(job);
        assertTrue(firstFailure.await(2, TimeUnit.SECONDS));
        waitForStatus(job, JobStatus.QUEUED);

        assertEquals(CancellationResult.CANCELLED, scheduler.cancel(job));

        scheduler.shutdown();
        assertTrue(scheduler.awaitTermination(5, TimeUnit.SECONDS));

        assertEquals(1, executions.get());
        assertEquals(JobStatus.CANCELLED, job.getStatus());
    }

    @Test
    void runningJobCanBeCancelled() throws Exception {
        scheduler = scheduler(RetryPolicy.DEFAULT);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);

        Job job = new Job("Long running", () -> {
            started.countDown();
            try {
                Thread.sleep(10_000);
            } catch (InterruptedException e) {
                interrupted.countDown();
                throw e;
            }
        });

        scheduler.submit(job);
        assertTrue(started.await(2, TimeUnit.SECONDS));

        assertEquals(CancellationResult.CANCELLATION_REQUESTED, scheduler.cancel(job));
        assertTrue(interrupted.await(2, TimeUnit.SECONDS));
        waitForStatus(job, JobStatus.CANCELLED);

        assertEquals(1, job.getAttemptCount());
        assertEquals(CancellationResult.NOT_CANCELLABLE, scheduler.cancel(job));
    }

    @Test
    void jobExceedingItsTimeoutShouldFail() throws Exception {
        scheduler = scheduler(RetryPolicy.DEFAULT);

        Job job = Job.builder("Too slow", () -> Thread.sleep(10_000))
                .timeout(Duration.ofMillis(200))
                .maxAttempts(1)
                .build();

        scheduler.submit(job);
        waitForStatus(job, JobStatus.FAILED);

        assertEquals("Timed out after 200 ms", job.getFailureReason());
    }

    @Test
    void nonRetryableFailureShouldFailAfterOneAttempt() throws Exception {
        scheduler = scheduler(RetryPolicy.DEFAULT);
        AtomicInteger executions = new AtomicInteger();

        Job job = new Job("Bad input", () -> {
            executions.incrementAndGet();
            throw new NonRetryableJobException("Invalid input");
        });

        scheduler.submit(job);
        scheduler.shutdown();
        assertTrue(scheduler.awaitTermination(5, TimeUnit.SECONDS));

        assertEquals(1, executions.get());
        assertEquals(JobStatus.FAILED, job.getStatus());
        assertEquals("Invalid input", job.getFailureReason());
    }

    private static JobScheduler scheduler(RetryPolicy retryPolicy) {
        return new JobScheduler(1, 10, retryPolicy, job -> {});
    }

    private static void waitForStatus(Job job, JobStatus status)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);

        while (job.getStatus() != status) {
            if (System.nanoTime() > deadline) {
                fail("Expected " + status + " but was " + job.getStatus());
            }
            Thread.sleep(20);
        }
    }
}
