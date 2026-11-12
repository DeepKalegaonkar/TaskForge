package com.taskforge.scheduler;

import com.taskforge.model.Job;
import com.taskforge.model.JobStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ConcurrentInMemoryExecutionTest {

    private static final int WORKERS = 4;
    private static final int SUBMITTERS = 8;
    private static final int JOBS_PER_SUBMITTER = 250;
    private static final int TOTAL_JOBS = SUBMITTERS * JOBS_PER_SUBMITTER;

    private final JobScheduler scheduler =
            new JobScheduler(WORKERS, TOTAL_JOBS, RetryPolicy.DEFAULT, job -> {});

    @AfterEach
    void stop() {
        scheduler.shutdownGracefully(Duration.ofSeconds(5));
    }

    @Test
    void concurrentSubmissionsShouldEachRunExactlyOnceOnAtMostWorkerCountThreads()
            throws Exception {
        Map<String, AtomicInteger> executionsPerJob = new ConcurrentHashMap<>();
        AtomicInteger running = new AtomicInteger();
        AtomicInteger maxRunning = new AtomicInteger();
        List<Job> jobs = new CopyOnWriteArrayList<>();

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService submitters = Executors.newFixedThreadPool(SUBMITTERS);

        try {
            List<Future<?>> submissions = new java.util.ArrayList<>();
            for (int s = 0; s < SUBMITTERS; s++) {
                submissions.add(submitters.submit(() -> {
                    start.await();
                    for (int i = 0; i < JOBS_PER_SUBMITTER; i++) {
                        String[] id = new String[1];
                        Job job = new Job("concurrent", () -> {
                            executionsPerJob
                                    .computeIfAbsent(id[0], k -> new AtomicInteger())
                                    .incrementAndGet();
                            maxRunning.accumulateAndGet(
                                    running.incrementAndGet(), Math::max
                            );
                            try {
                                Thread.sleep(1);
                            } finally {
                                running.decrementAndGet();
                            }
                        });
                        id[0] = job.getId();
                        jobs.add(job);
                        scheduler.submit(job);
                    }
                    return null;
                }));
            }

            start.countDown();
            for (Future<?> submission : submissions) {
                submission.get(30, TimeUnit.SECONDS);
            }
        } finally {
            submitters.shutdownNow();
        }

        scheduler.shutdown();
        assertTrue(scheduler.awaitTermination(60, TimeUnit.SECONDS));

        assertEquals(TOTAL_JOBS, jobs.size());
        assertTrue(jobs.stream().allMatch(j -> j.getStatus() == JobStatus.COMPLETED),
                "Every submitted job must complete");
        assertEquals(TOTAL_JOBS, executionsPerJob.size(), "Every job must run");
        assertTrue(executionsPerJob.values().stream().allMatch(c -> c.get() == 1),
                "No job may run more than once");
        assertTrue(maxRunning.get() <= WORKERS,
                "At most " + WORKERS + " jobs may run at once, saw " + maxRunning.get());
        assertEquals(WORKERS, maxRunning.get(),
                "All workers should have been busy at some point");
    }
}
