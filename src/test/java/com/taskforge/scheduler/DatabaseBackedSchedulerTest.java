package com.taskforge.scheduler;

import com.taskforge.handler.JobHandler;
import com.taskforge.handler.NonRetryableJobException;
import com.taskforge.model.Job;
import com.taskforge.model.JobStatus;
import com.taskforge.scheduler.FakeJobStore.FinishedAttempt;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class DatabaseBackedSchedulerTest {

    private static final RetryPolicy FAST_RETRIES = new RetryPolicy(
            Duration.ofMillis(100), 2.0, Duration.ofSeconds(1)
    );

    private final FakeJobStore store = new FakeJobStore();
    private JobScheduler scheduler;

    @AfterEach
    void stopScheduler() {
        if (scheduler != null) {
            scheduler.shutdownGracefully(Duration.ofSeconds(5));
        }
    }

    @Test
    void claimedJobShouldKeepTheDatabaseAttemptNumber() throws Exception {
        // The store increments attempt_count while claiming,
        // so the scheduler must not increment it a second time.
        Job job = claimedJob(1, 3, Duration.ofSeconds(5), () -> {});
        start(job);

        FinishedAttempt attempt = store.awaitFinishedAttempt(5, TimeUnit.SECONDS);

        assertEquals(1, attempt.attemptNumber());
        assertEquals(JobStatus.COMPLETED, attempt.jobStatusWhenRecorded());
        assertEquals(JobStatus.COMPLETED, attempt.result().status());
        assertFalse(attempt.result().willRetry());
    }

    @Test
    void failedAttemptShouldBeRequeuedWithExponentialBackoff() throws Exception {
        Job job = claimedJob(2, 3, Duration.ofSeconds(5), () -> {
            throw new RuntimeException("Simulated failure");
        });
        start(job);

        FinishedAttempt attempt = store.awaitFinishedAttempt(5, TimeUnit.SECONDS);

        // 100 ms * 2^(2 - 1)
        assertEquals(Duration.ofMillis(200), attempt.result().retryDelay());
        assertEquals(JobStatus.FAILED, attempt.jobStatusWhenRecorded());
        assertEquals("Simulated failure", attempt.result().failureReason());
        assertEquals(JobStatus.QUEUED, job.getStatus());
        assertNotNull(job.getNextAttemptAt());
    }

    @Test
    void finalAttemptShouldFailPermanently() throws Exception {
        Job job = claimedJob(3, 3, Duration.ofSeconds(5), () -> {
            throw new RuntimeException("Simulated failure");
        });
        start(job);

        FinishedAttempt attempt = store.awaitFinishedAttempt(5, TimeUnit.SECONDS);

        assertEquals(JobStatus.FAILED, attempt.result().status());
        assertFalse(attempt.result().willRetry());
        assertEquals(JobStatus.FAILED, job.getStatus());
    }

    @Test
    void nonRetryableFailureShouldNotUseRemainingAttempts() throws Exception {
        Job job = claimedJob(1, 3, Duration.ofSeconds(5), () -> {
            throw new NonRetryableJobException("Invalid input");
        });
        start(job);

        FinishedAttempt attempt = store.awaitFinishedAttempt(5, TimeUnit.SECONDS);

        assertEquals(JobStatus.FAILED, attempt.result().status());
        assertEquals("Invalid input", attempt.result().failureReason());
        assertFalse(attempt.result().willRetry());
    }

    @Test
    void attemptExceedingItsTimeoutShouldBeInterruptedAndRetried()
            throws Exception {
        CountDownLatch handlerInterrupted = new CountDownLatch(1);
        Job job = claimedJob(1, 3, Duration.ofMillis(300), () -> {
            try {
                Thread.sleep(10_000);
            } catch (InterruptedException e) {
                handlerInterrupted.countDown();
                throw e;
            }
        });

        Instant before = Instant.now();
        start(job);

        FinishedAttempt attempt = store.awaitFinishedAttempt(5, TimeUnit.SECONDS);

        assertTrue(Duration.between(before, Instant.now()).toMillis() < 3000);
        assertEquals(JobStatus.FAILED, attempt.result().status());
        assertEquals("Timed out after 300 ms", attempt.result().failureReason());
        assertTrue(attempt.result().willRetry());
        assertTrue(handlerInterrupted.await(2, TimeUnit.SECONDS));
    }

    @Test
    void cancellationRequestedByAnotherInstanceShouldStopTheJob()
            throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch handlerInterrupted = new CountDownLatch(1);
        Job job = claimedJob(1, 3, Duration.ofSeconds(30), () -> {
            started.countDown();
            try {
                Thread.sleep(10_000);
            } catch (InterruptedException e) {
                handlerInterrupted.countDown();
                throw e;
            }
        });
        start(job);

        assertTrue(started.await(2, TimeUnit.SECONDS));
        store.requestCancellationElsewhere(job.getId());

        FinishedAttempt attempt = store.awaitFinishedAttempt(3, TimeUnit.SECONDS);

        assertEquals(JobStatus.CANCELLED, attempt.result().status());
        assertFalse(attempt.result().willRetry());
        assertEquals(JobStatus.CANCELLED, job.getStatus());
        assertTrue(handlerInterrupted.await(2, TimeUnit.SECONDS));
    }

    @Test
    void cancellingAJobRunningHereShouldInterruptItImmediately()
            throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        Job job = claimedJob(1, 3, Duration.ofSeconds(30), () -> {
            started.countDown();
            Thread.sleep(10_000);
        });
        start(job);

        assertTrue(started.await(2, TimeUnit.SECONDS));
        assertEquals(
                CancellationResult.CANCELLATION_REQUESTED,
                scheduler.cancel(job.getId())
        );

        FinishedAttempt attempt = store.awaitFinishedAttempt(1, TimeUnit.SECONDS);

        assertEquals(JobStatus.CANCELLED, attempt.result().status());
        assertEquals(JobStatus.CANCELLED, attempt.jobStatusWhenRecorded());
    }

    @Test
    void gracefulShutdownShouldLetRunningJobsFinish() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        Job job = claimedJob(1, 3, Duration.ofSeconds(30), () -> {
            started.countDown();
            Thread.sleep(500);
        });
        start(job);

        assertTrue(started.await(2, TimeUnit.SECONDS));
        scheduler.shutdownGracefully(Duration.ofSeconds(5));

        FinishedAttempt attempt = store.awaitFinishedAttempt(0, TimeUnit.SECONDS);
        assertEquals(JobStatus.COMPLETED, attempt.result().status());
        assertEquals(0, scheduler.getRunningJobCount());
    }

    @Test
    void shutdownShouldInterruptJobsPastTheGracePeriodAndRequeueThem()
            throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        Job job = claimedJob(1, 3, Duration.ofSeconds(30), () -> {
            started.countDown();
            Thread.sleep(10_000);
        });
        start(job);

        assertTrue(started.await(2, TimeUnit.SECONDS));

        Instant before = Instant.now();
        scheduler.shutdownGracefully(Duration.ofMillis(300));

        assertTrue(Duration.between(before, Instant.now()).toMillis() < 3000);

        FinishedAttempt attempt = store.awaitFinishedAttempt(0, TimeUnit.SECONDS);
        assertEquals(JobStatus.FAILED, attempt.result().status());
        assertEquals(JobScheduler.SHUTDOWN_REASON, attempt.result().failureReason());
        assertEquals(Duration.ZERO, attempt.result().retryDelay());
    }

    @Test
    void shutdownShouldStopClaimingNewJobs() throws Exception {
        scheduler = new JobScheduler(1, store, FAST_RETRIES, Duration.ofSeconds(5), true);
        scheduler.shutdownGracefully(Duration.ofSeconds(5));

        store.addClaimable(claimedJob(1, 3, Duration.ofSeconds(5), () -> {}));
        Thread.sleep(500);

        assertFalse(store.hasFinishedAttempts());
    }

    private void start(Job job) {
        store.addClaimable(job);
        scheduler = new JobScheduler(1, store, FAST_RETRIES, Duration.ofSeconds(5), true);
    }

    private static Job claimedJob(
            int attemptCount,
            int maxAttempts,
            Duration timeout,
            JobHandler handler
    ) {
        return Job.builder("Claimed Job", handler)
                .status(JobStatus.RUNNING)
                .startedAt(Instant.now())
                .attemptCount(attemptCount)
                .maxAttempts(maxAttempts)
                .timeout(timeout)
                .build();
    }
}
