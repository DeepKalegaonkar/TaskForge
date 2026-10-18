
package com.taskforge.scheduler;

import com.taskforge.model.Job;
import com.taskforge.model.JobStatus;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class JobSchedulerTest {

    @Test
    void submittedJobShouldComplete() throws InterruptedException {
        JobScheduler scheduler = new JobScheduler(2, 10);
        AtomicInteger executions = new AtomicInteger(0);

        try {
            Job job = new Job(
                    "Test Job",
                    executions::incrementAndGet
            );

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

            Job queuedJob = new Job(
                    "Queued Job",
                    executions::incrementAndGet
            );

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

            assertTrue(
                    scheduler.awaitTermination(5, TimeUnit.SECONDS)
            );

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

        try {
            Job job = new Job("Failing Job", () -> {
                throw new RuntimeException("Simulated failure");
            });

            scheduler.submit(job);
            scheduler.shutdown();

            assertTrue(
                    scheduler.awaitTermination(5, TimeUnit.SECONDS)
            );

            assertEquals(JobStatus.FAILED, job.getStatus());
            assertEquals("Simulated failure", job.getFailureReason());

        } finally {
            scheduler.shutdown();
        }
    }


    @Test
    void fullQueueShouldRejectNewJobs() throws InterruptedException {
        JobScheduler scheduler = new JobScheduler(1, 1);

        java.util.concurrent.CountDownLatch jobStarted =
                new java.util.concurrent.CountDownLatch(1);

        java.util.concurrent.CountDownLatch releaseJob =
                new java.util.concurrent.CountDownLatch(1);

        try {
            Job runningJob = new Job("Running Job", () -> {
                jobStarted.countDown();
                releaseJob.await();
            });

            Job queuedJob = new Job("Queued Job", () -> {});

            Job rejectedJob = new Job("Rejected Job", () -> {});

            scheduler.submit(runningJob);

            assertTrue(jobStarted.await(2, TimeUnit.SECONDS));

            // The worker is busy, so this job occupies the one queue slot.
            scheduler.submit(queuedJob);

            assertEquals(1, scheduler.getQueueSize());

            // The queue is full, so this submission must fail.
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

            // Initiate shutdown while the first job is running
            // and the second job is waiting in the queue.
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

        // Exactly one operation must win.
        assertEquals(
                1,
                successfulStarts.get() + successfulCancellations.get()
        );

        if (successfulStarts.get() == 1) {
            assertEquals(JobStatus.RUNNING, job.getStatus());
            assertFalse(job.cancel());
        } else {
            assertEquals(JobStatus.CANCELLED, job.getStatus());
            assertFalse(job.markRunning());
        }
    }


}
