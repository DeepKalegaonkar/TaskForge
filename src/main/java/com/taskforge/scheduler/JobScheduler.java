package com.taskforge.scheduler;

import com.taskforge.model.Job;
import com.taskforge.model.JobStatus;

import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class JobScheduler {

    public static final int MAX_ATTEMPTS = 3;
    private static final long INITIAL_RETRY_DELAY_MS = 2000;

    private final BlockingQueue<Job> jobQueue;
    private final ExecutorService workerPool;
    private final int workerCount;

    private final AtomicBoolean acceptingJobs = new AtomicBoolean(true);
    private final AtomicBoolean workersStarted = new AtomicBoolean(false);

    private final Consumer<Job> statusChangeListener;
    private final Consumer<Job> attemptStartedListener;
    private final Consumer<Job> attemptFinishedListener;

    private final Supplier<Optional<Job>> jobClaimer;
    private final Predicate<String> databaseCancellationHandler;
    private final Predicate<Job> databaseRetryHandler;

    public JobScheduler(int workerCount, int queueCapacity) {
        this(workerCount, queueCapacity, job -> {});
    }

    public JobScheduler(
            int workerCount,
            int queueCapacity,
            Consumer<Job> statusChangeListener
    ) {
        this(
                workerCount,
                queueCapacity,
                statusChangeListener,
                job -> {},
                job -> {}
        );
    }

    public JobScheduler(
            int workerCount,
            int queueCapacity,
            Consumer<Job> statusChangeListener,
            Consumer<Job> attemptStartedListener,
            Consumer<Job> attemptFinishedListener
    ) {
        this(
                workerCount,
                queueCapacity,
                statusChangeListener,
                attemptStartedListener,
                attemptFinishedListener,
                null,
                null,
                null
        );
    }

    public JobScheduler(
            int workerCount,
            int queueCapacity,
            Consumer<Job> statusChangeListener,
            Consumer<Job> attemptStartedListener,
            Consumer<Job> attemptFinishedListener,
            Supplier<Optional<Job>> jobClaimer,
            Predicate<String> databaseCancellationHandler
    ) {
        this(
                workerCount,
                queueCapacity,
                statusChangeListener,
                attemptStartedListener,
                attemptFinishedListener,
                jobClaimer,
                databaseCancellationHandler,
                null
        );
    }

    public JobScheduler(
            int workerCount,
            int queueCapacity,
            Consumer<Job> statusChangeListener,
            Consumer<Job> attemptStartedListener,
            Consumer<Job> attemptFinishedListener,
            Supplier<Optional<Job>> jobClaimer,
            Predicate<String> databaseCancellationHandler,
            Predicate<Job> databaseRetryHandler
    ) {
        this(
                workerCount,
                queueCapacity,
                statusChangeListener,
                attemptStartedListener,
                attemptFinishedListener,
                jobClaimer,
                databaseCancellationHandler,
                databaseRetryHandler,
                true
        );
    }

    /**
     * Allows Spring to create the scheduler without starting workers
     * until startup recovery has completed.
     *
     * Existing constructors still start workers immediately.
     */
    public JobScheduler(
            int workerCount,
            int queueCapacity,
            Consumer<Job> statusChangeListener,
            Consumer<Job> attemptStartedListener,
            Consumer<Job> attemptFinishedListener,
            Supplier<Optional<Job>> jobClaimer,
            Predicate<String> databaseCancellationHandler,
            Predicate<Job> databaseRetryHandler,
            boolean startImmediately
    ) {
        if (workerCount <= 0 || queueCapacity <= 0) {
            throw new IllegalArgumentException(
                    "Worker count and queue capacity must be positive"
            );
        }

        if (statusChangeListener == null
                || attemptStartedListener == null
                || attemptFinishedListener == null) {
            throw new IllegalArgumentException(
                    "Listeners cannot be null"
            );
        }

        if ((jobClaimer == null)
                != (databaseCancellationHandler == null)) {
            throw new IllegalArgumentException(
                    "Database claiming and cancellation must be configured together"
            );
        }

        if (databaseRetryHandler != null && jobClaimer == null) {
            throw new IllegalArgumentException(
                    "Database retry handling requires database job claiming"
            );
        }

        this.jobQueue = new ArrayBlockingQueue<>(queueCapacity);
        this.workerPool = Executors.newFixedThreadPool(workerCount);
        this.workerCount = workerCount;

        this.statusChangeListener = statusChangeListener;
        this.attemptStartedListener = attemptStartedListener;
        this.attemptFinishedListener = attemptFinishedListener;

        this.jobClaimer = jobClaimer;
        this.databaseCancellationHandler = databaseCancellationHandler;
        this.databaseRetryHandler = databaseRetryHandler;

        if (startImmediately) {
            start();
        }
    }

    /**
     * Starts worker threads exactly once.
     */
    public void start() {
        if (!acceptingJobs.get()) {
            throw new IllegalStateException(
                    "Cannot start a scheduler that is shutting down"
            );
        }

        if (!workersStarted.compareAndSet(false, true)) {
            return;
        }

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

        if (isDatabaseBacked()) {
            return;
        }

        if (!jobQueue.offer(job)) {
            throw new RejectedExecutionException("Job queue is full");
        }
    }

    public synchronized boolean cancel(String jobId) {
        if (jobId == null || jobId.isBlank()) {
            return false;
        }

        if (isDatabaseBacked()) {
            return databaseCancellationHandler.test(jobId);
        }

        Job queuedJob = jobQueue.stream()
                .filter(job -> job.getId().equals(jobId))
                .findFirst()
                .orElse(null);

        if (queuedJob == null || !queuedJob.cancel()) {
            return false;
        }

        jobQueue.remove(queuedJob);
        notifyStatusChanged(queuedJob);
        return true;
    }

    public synchronized boolean cancel(Job job) {
        return job != null && cancel(job.getId());
    }

    private void workerLoop() {
        if (isDatabaseBacked()) {
            databaseWorkerLoop();
        } else {
            inMemoryWorkerLoop();
        }
    }

    private void inMemoryWorkerLoop() {
        while (acceptingJobs.get() || !jobQueue.isEmpty()) {
            try {
                Job job = jobQueue.poll(100, TimeUnit.MILLISECONDS);

                if (job != null) {
                    executeJob(job, false);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void databaseWorkerLoop() {
        while (acceptingJobs.get()) {
            try {
                Optional<Job> claimedJob = jobClaimer.get();

                if (claimedJob.isPresent()) {
                    executeJob(claimedJob.get(), true);
                } else {
                    TimeUnit.MILLISECONDS.sleep(200);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (RuntimeException e) {
                System.err.println(
                        "Database job claiming failed: " + e.getMessage()
                );

                try {
                    TimeUnit.MILLISECONDS.sleep(1000);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private void executeJob(Job job, boolean alreadyClaimed) {
        if (alreadyClaimed) {
            // PostgreSQL already changed the status to RUNNING and
            // incremented attempt_count while claiming the job.
            if (job.getStatus() != JobStatus.RUNNING) {
                return;
            }
        } else {
            if (!job.markRunning()) {
                return;
            }
        }

        notifyStatusChanged(job);
        notifyAttemptStarted(job);

        try {
            job.getHandler().execute();
            job.markCompleted();
        } catch (Exception e) {
            String reason = e.getMessage() == null
                    ? e.getClass().getSimpleName()
                    : e.getMessage();

            job.markFailed(reason);

            System.err.println(
                    "Job failed: " + job.getName()
                            + " (attempt " + job.getAttemptCount() + ")"
            );
        }

        notifyStatusChanged(job);
        notifyAttemptFinished(job);

        if (job.getStatus() == JobStatus.COMPLETED) {
            return;
        }

        if (job.getStatus() != JobStatus.FAILED) {
            return;
        }

        if (job.getAttemptCount() >= MAX_ATTEMPTS) {
            System.err.println(
                    "Job exhausted all " + MAX_ATTEMPTS
                            + " attempts: " + job.getId()
            );
            return;
        }

        long delay = INITIAL_RETRY_DELAY_MS
                * (1L << (job.getAttemptCount() - 1));

        try {
            System.out.println(
                    "Retrying job " + job.getId()
                            + " after " + delay + " ms"
            );

            TimeUnit.MILLISECONDS.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            System.err.println(
                    "Retry interrupted for job " + job.getId()
            );
            return;
        }

        if (!job.requeueForRetry()) {
            return;
        }

        if (isDatabaseBacked()) {
            if (databaseRetryHandler != null) {
                boolean requeued = databaseRetryHandler.test(job);

                if (!requeued) {
                    System.err.println(
                            "Could not requeue failed job " + job.getId()
                    );
                }
            } else {
                notifyStatusChanged(job);
            }
        } else {
            if (!jobQueue.offer(job)) {
                System.err.println(
                        "Retry queue is full for job " + job.getId()
                );
            }
        }
    }

    private boolean isDatabaseBacked() {
        return jobClaimer != null;
    }

    private void notifyStatusChanged(Job job) {
        try {
            statusChangeListener.accept(job);
        } catch (RuntimeException e) {
            System.err.println(
                    "Could not persist job status for " + job.getId()
            );
            e.printStackTrace();
        }
    }

    private void notifyAttemptStarted(Job job) {
        try {
            attemptStartedListener.accept(job);
        } catch (RuntimeException e) {
            System.err.println(
                    "Could not record attempt start for " + job.getId()
            );
            e.printStackTrace();
        }
    }

    private void notifyAttemptFinished(Job job) {
        try {
            attemptFinishedListener.accept(job);
        } catch (RuntimeException e) {
            System.err.println(
                    "Could not record attempt result for " + job.getId()
            );
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
        return isDatabaseBacked() ? -1 : jobQueue.size();
    }
}
