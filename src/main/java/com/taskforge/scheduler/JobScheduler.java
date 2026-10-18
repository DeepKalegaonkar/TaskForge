
package com.taskforge.scheduler;

import com.taskforge.model.Job;
import com.taskforge.model.JobStatus;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class JobScheduler {

    private final BlockingQueue<Job> jobQueue;
    private final ExecutorService workerPool;
    private final AtomicBoolean acceptingJobs = new AtomicBoolean(true);

    public JobScheduler(int workerCount, int queueCapacity) {
        if (workerCount <= 0 || queueCapacity <= 0) {
            throw new IllegalArgumentException(
                    "Worker count and queue capacity must be positive"
            );
        }

        this.jobQueue = new ArrayBlockingQueue<>(queueCapacity);
        this.workerPool = Executors.newFixedThreadPool(workerCount);

        // Start the worker loops.
        for (int i = 0; i < workerCount; i++) {
            workerPool.submit(this::workerLoop);
        }
    }

    public synchronized void submit(Job job) {
        if (job == null) {
            throw new IllegalArgumentException("Job cannot be null");
        }

        if (!acceptingJobs.get()) {
            throw new RejectedExecutionException(
                    "Scheduler is shutting down"
            );
        }

        // Reject the job if the bounded queue is full.
        if (!jobQueue.offer(job)) {
            throw new RejectedExecutionException(
                    "Job queue is full"
            );
        }
    }

    public synchronized boolean cancel(Job job) {
        if (job == null) {
            return false;
        }

        // Cancel only if the job is still queued.
        if (!job.cancel()) {
            return false;
        }

        // Remove it if it is still waiting in the queue.
        jobQueue.remove(job);

        return true;
    }


    private void workerLoop() {
        while (acceptingJobs.get() || !jobQueue.isEmpty()) {
            try {
                Job job = jobQueue.poll(100, TimeUnit.MILLISECONDS);

                if (job != null) {
                    executeJob(job);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void executeJob(Job job) {
        try {
            if (!job.markRunning()) {
                return;
            }

            job.getHandler().execute();
            job.markCompleted();

        } catch (Exception e) {
            if (job.getStatus() == JobStatus.RUNNING) {
                job.markFailed(
                        e.getMessage() == null
                                ? e.getClass().getSimpleName()
                                : e.getMessage()
                );
            }

            System.err.println("Job failed: " + job.getName());
            e.printStackTrace();
        }
    }

    public synchronized void shutdown() {
        acceptingJobs.set(false);
        workerPool.shutdown();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit)
            throws InterruptedException {
        return workerPool.awaitTermination(timeout, unit);
    }

    public int getQueueSize() {
        return jobQueue.size();
    }
}
