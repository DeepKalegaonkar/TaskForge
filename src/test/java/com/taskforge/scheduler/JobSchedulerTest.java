package com.taskforge.scheduler;

import com.taskforge.model.Job;
import com.taskforge.model.JobStatus;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class JobSchedulerTest {

    @Test
    void submittedJobShouldComplete() throws InterruptedException {
        JobScheduler scheduler = new JobScheduler(2, 10);
        AtomicInteger executions = new AtomicInteger(0);

        try {
            Job job = new Job("Test Job", executions::incrementAndGet);

            scheduler.submit(job);
            scheduler.shutdown();

            assertTrue(
                    scheduler.awaitTermination(5, TimeUnit.SECONDS),
                    "Scheduler should finish within 5 seconds"
            );

            assertEquals(1, executions.get());
            assertEquals(JobStatus.COMPLETED, job.getStatus());
            assertNotNull(job.getStartedAt());
            assertNotNull(job.getCompletedAt());

        } finally {
            scheduler.shutdown();
        }
    }

    @Test
    void queuedJobShouldBeCancelled() throws InterruptedException {
        JobScheduler scheduler = new JobScheduler(1, 10);

        CountDownLatch jobStarted = new CountDownLatch(1);
        CountDownLatch releaseJob = new CountDownLatch(1);
        AtomicInteger executions = new AtomicInteger(0);

        try {
            Job blockingJob = new Job("Blocking Job", () -> {
                jobStarted.countDown();
                releaseJob.await();
            });

            Job queuedJob = new Job("Queued Job", executions::incrementAndGet);

            scheduler.submit(blockingJob);

            assertTrue(
                    jobStarted.await(2, TimeUnit.SECONDS),
                    "Blocking job should start"
            );

            scheduler.submit(queuedJob);

            assertTrue(scheduler.cancel(queuedJob));
            assertEquals(JobStatus.CANCELLED, queuedJob.getStatus());

            releaseJob.countDown();
            scheduler.shutdown();

            assertTrue(scheduler.awaitTermination(5, TimeUnit.SECONDS));
            assertEquals(0, executions.get());
            assertEquals(JobStatus.COMPLETED, blockingJob.getStatus());

        } finally {
            releaseJob.countDown();
            scheduler.shutdown();
        }
    }

    @Test
    void completedJobShouldNotRunAgain() {
        Job job = new Job("Lifecycle Test", () -> {});

        assertTrue(job.markRunning());
        assertTrue(job.markCompleted());

        assertFalse(job.markRunning());
        assertFalse(job.cancel());
        assertEquals(JobStatus.COMPLETED, job.getStatus());
    }

    @Test
    void runningJobShouldNotBeCancelled() {
        Job job = new Job("Running Job", () -> {});

        assertTrue(job.markRunning());

        assertFalse(job.cancel());
        assertEquals(JobStatus.RUNNING, job.getStatus());
    }

    @Test
    void failedHandlerShouldProduceFailedStatus()
            throws InterruptedException {

        JobScheduler scheduler = new JobScheduler(1, 10);
        AtomicInteger executions = new AtomicInteger(0);

        try {
            Job job = new Job("Failing Job", () -> {
                executions.incrementAndGet();
                throw new RuntimeException("Simulated failure");
            });

            scheduler.submit(job);
            scheduler.shutdown();

            // First attempt fails, followed by 2s and 4s retry delays.
            assertTrue(
                    scheduler.awaitTermination(12, TimeUnit.SECONDS),
                    "Scheduler should finish all retry attempts within 12 seconds"
            );

            assertEquals(3, executions.get(),
                    "The handler should run exactly three times");
            assertEquals(JobStatus.FAILED, job.getStatus());
            assertEquals("Simulated failure", job.getFailureReason());
            assertEquals(3, job.getAttemptCount());

        } finally {
            scheduler.shutdown();
        }
    }

    @Test
    void fullQueueShouldRejectNewJobs() throws InterruptedException {
        JobScheduler scheduler = new JobScheduler(1, 1);

        CountDownLatch jobStarted = new CountDownLatch(1);
        CountDownLatch releaseJob = new CountDownLatch(1);

        try {
            Job runningJob = new Job("Running Job", () -> {
                jobStarted.countDown();
                releaseJob.await();
            });

            Job queuedJob = new Job("Queued Job", () -> {});
            Job rejectedJob = new Job("Rejected Job", () -> {});

            scheduler.submit(runningJob);

            assertTrue(jobStarted.await(2, TimeUnit.SECONDS));

            scheduler.submit(queuedJob);
            assertEquals(1, scheduler.getQueueSize());

            assertThrows(
                    java.util.concurrent.RejectedExecutionException.class,
                    () -> scheduler.submit(rejectedJob)
            );

        } finally {
            releaseJob.countDown();
            scheduler.shutdown();
            assertTrue(scheduler.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void shutdownShouldFinishQueuedJobsAndRejectNewJobs()
            throws InterruptedException {

        JobScheduler scheduler = new JobScheduler(1, 5);

        CountDownLatch jobStarted = new CountDownLatch(1);
        CountDownLatch releaseJob = new CountDownLatch(1);

        try {
            Job firstJob = new Job("First Job", () -> {
                jobStarted.countDown();
                releaseJob.await();
            });

            Job secondJob = new Job("Second Job", () -> {});

            scheduler.submit(firstJob);

            assertTrue(
                    jobStarted.await(2, TimeUnit.SECONDS),
                    "First job should start"
            );

            scheduler.submit(secondJob);
            scheduler.shutdown();

            Job lateJob = new Job("Late Job", () -> {});

            assertThrows(
                    java.util.concurrent.RejectedExecutionException.class,
                    () -> scheduler.submit(lateJob)
            );

            releaseJob.countDown();

            assertTrue(
                    scheduler.awaitTermination(5, TimeUnit.SECONDS),
                    "Scheduler should terminate after draining its queue"
            );

            assertEquals(JobStatus.COMPLETED, firstJob.getStatus());
            assertEquals(JobStatus.COMPLETED, secondJob.getStatus());
            assertEquals(JobStatus.QUEUED, lateJob.getStatus());

        } finally {
            releaseJob.countDown();
            scheduler.shutdown();
            assertTrue(scheduler.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void concurrentStartAndCancellationShouldNotConflict()
            throws InterruptedException {

        Job job = new Job("Race Test", () -> {});

        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);

        AtomicInteger successfulStarts = new AtomicInteger();
        AtomicInteger successfulCancellations = new AtomicInteger();

        Thread starter = new Thread(() -> {
            ready.countDown();

            try {
                start.await();

                if (job.markRunning()) {
                    successfulStarts.incrementAndGet();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        Thread canceller = new Thread(() -> {
            ready.countDown();

            try {
                start.await();

                if (job.cancel()) {
                    successfulCancellations.incrementAndGet();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        starter.start();
        canceller.start();

        assertTrue(ready.await(2, TimeUnit.SECONDS));
        start.countDown();

        starter.join();
        canceller.join();

        assertEquals(
                1,
                successfulStarts.get() + successfulCancellations.get(),
                "Exactly one operation must win"
        );

        if (successfulStarts.get() == 1) {
            assertEquals(JobStatus.RUNNING, job.getStatus());
            assertFalse(job.cancel());
        } else {
            assertEquals(JobStatus.CANCELLED, job.getStatus());
            assertFalse(job.markRunning());
        }
    }

    @Test
    void claimedJobShouldKeepTheDatabaseAttemptNumber()
            throws InterruptedException {

        // PostgreSQL increments attempt_count while claiming,
        // so the scheduler must not increment it a second time.
        Job claimed = claimedJob(1, () -> {});
        List<Integer> startedAttempts = new CopyOnWriteArrayList<>();
        List<Integer> finishedAttempts = new CopyOnWriteArrayList<>();
        CountDownLatch finished = new CountDownLatch(1);

        JobScheduler scheduler = new JobScheduler(
                1,
                10,
                job -> {},
                job -> startedAttempts.add(job.getAttemptCount()),
                job -> {
                    finishedAttempts.add(job.getAttemptCount());
                    finished.countDown();
                },
                claimOnce(claimed),
                id -> false
        );

        try {
            assertTrue(finished.await(5, TimeUnit.SECONDS));

            assertEquals(JobStatus.COMPLETED, claimed.getStatus());
            assertEquals(1, claimed.getAttemptCount());
            assertEquals(List.of(1), startedAttempts);
            assertEquals(List.of(1), finishedAttempts);
        } finally {
            scheduler.shutdown();
            assertTrue(scheduler.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void failedClaimedJobShouldBeRequeuedThroughTheDatabase()
            throws InterruptedException {

        Job claimed = claimedJob(1, () -> {
            throw new RuntimeException("Simulated failure");
        });
        AtomicReference<JobStatus> statusAtRetry = new AtomicReference<>();
        AtomicInteger attemptAtRetry = new AtomicInteger();
        CountDownLatch retried = new CountDownLatch(1);

        JobScheduler scheduler = new JobScheduler(
                1,
                10,
                job -> {},
                job -> {},
                job -> {},
                claimOnce(claimed),
                id -> false,
                job -> {
                    statusAtRetry.set(job.getStatus());
                    attemptAtRetry.set(job.getAttemptCount());
                    retried.countDown();
                    return true;
                }
        );

        try {
            // First retry waits 2 seconds.
            assertTrue(retried.await(5, TimeUnit.SECONDS));

            assertEquals(JobStatus.QUEUED, statusAtRetry.get());
            assertEquals(1, attemptAtRetry.get());
        } finally {
            scheduler.shutdown();
            assertTrue(scheduler.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void claimedJobOnFinalAttemptShouldNotBeRetried()
            throws InterruptedException {

        Job claimed = claimedJob(3, () -> {
            throw new RuntimeException("Simulated failure");
        });
        AtomicInteger retries = new AtomicInteger();
        CountDownLatch finished = new CountDownLatch(1);

        JobScheduler scheduler = new JobScheduler(
                1,
                10,
                job -> {},
                job -> {},
                job -> finished.countDown(),
                claimOnce(claimed),
                id -> false,
                job -> {
                    retries.incrementAndGet();
                    return true;
                }
        );

        try {
            assertTrue(finished.await(5, TimeUnit.SECONDS));
        } finally {
            scheduler.shutdown();
            assertTrue(scheduler.awaitTermination(5, TimeUnit.SECONDS));
        }

        assertEquals(JobStatus.FAILED, claimed.getStatus());
        assertEquals(3, claimed.getAttemptCount());
        assertEquals(0, retries.get());
    }

    private static Job claimedJob(int attemptCount, com.taskforge.handler.JobHandler handler) {
        return new Job(
                java.util.UUID.randomUUID().toString(),
                "Claimed Job",
                null,
                handler,
                Instant.now(),
                JobStatus.RUNNING,
                Instant.now(),
                null,
                null,
                attemptCount
        );
    }

    private static java.util.function.Supplier<Optional<Job>> claimOnce(Job job) {
        AtomicBoolean handedOut = new AtomicBoolean(false);
        return () -> handedOut.compareAndSet(false, true)
                ? Optional.of(job)
                : Optional.empty();
    }
}
