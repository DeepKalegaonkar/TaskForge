package com.taskforge.scheduler;

import com.taskforge.handler.NonRetryableJobException;
import com.taskforge.model.Job;
import com.taskforge.model.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/*
 * Runs jobs on a fixed pool of workers, either from an in-memory queue
 * or by claiming them from a JobStore.
 *
 * Each handler runs on its own thread so that a worker can enforce the
 * job's timeout and react to cancellation while it waits.
 */
public class JobScheduler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(JobScheduler.class);

    // MDC keys. They appear as fields in structured (JSON) log output.
    static final String MDC_JOB_ID = "jobId";
    static final String MDC_JOB_NAME = "jobName";
    static final String MDC_ATTEMPT = "attempt";

    public static final String SHUTDOWN_REASON =
            "Interrupted by application shutdown";

    private static final Duration DEFAULT_SHUTDOWN_GRACE_PERIOD =
            Duration.ofSeconds(30);
    private static final Duration FORCED_SHUTDOWN_WAIT = Duration.ofSeconds(10);
    private static final long CANCELLATION_POLL_MS = 500;

    private final BlockingQueue<Job> jobQueue;
    private final JobStore jobStore;
    private final RetryPolicy retryPolicy;
    private final Duration shutdownGracePeriod;
    private final Consumer<Job> statusChangeListener;
    private final int workerCount;

    private final ExecutorService workerPool;
    private final ExecutorService handlerPool;
    private final ScheduledExecutorService retryTimer;

    private final Map<String, RunningJob> runningJobs = new ConcurrentHashMap<>();
    private final Map<String, Job> waitingRetries = new ConcurrentHashMap<>();

    private final AtomicBoolean acceptingJobs = new AtomicBoolean(true);
    private final AtomicBoolean workersStarted = new AtomicBoolean(false);
    private volatile boolean forcedShutdown;

    private record RunningJob(Job job, FutureTask<Void> execution) {
    }

    // Internal result of waiting for a handler, before retries are decided.
    private record Outcome(
            JobStatus status,
            String reason,
            boolean retryable,
            boolean retryImmediately
    ) {
        static Outcome completed() {
            return new Outcome(JobStatus.COMPLETED, null, false, false);
        }

        static Outcome cancelled() {
            return new Outcome(JobStatus.CANCELLED, null, false, false);
        }

        static Outcome failed(String reason, boolean retryable) {
            return new Outcome(JobStatus.FAILED, reason, retryable, false);
        }

        static Outcome interrupted() {
            return new Outcome(JobStatus.FAILED, SHUTDOWN_REASON, true, true);
        }
    }

    public JobScheduler(int workerCount, int queueCapacity) {
        this(workerCount, queueCapacity, job -> {});
    }

    public JobScheduler(
            int workerCount,
            int queueCapacity,
            Consumer<Job> statusChangeListener
    ) {
        this(workerCount, queueCapacity, RetryPolicy.DEFAULT, statusChangeListener);
    }

    // In-memory scheduler. Workers start immediately.
    public JobScheduler(
            int workerCount,
            int queueCapacity,
            RetryPolicy retryPolicy,
            Consumer<Job> statusChangeListener
    ) {
        this(
                workerCount,
                validatedQueue(queueCapacity),
                null,
                retryPolicy,
                DEFAULT_SHUTDOWN_GRACE_PERIOD,
                statusChangeListener,
                true
        );
    }

    /*
     * Database-backed scheduler. Spring passes startImmediately = false
     * so that workers start only after startup recovery has completed.
     */
    public JobScheduler(
            int workerCount,
            JobStore jobStore,
            RetryPolicy retryPolicy,
            Duration shutdownGracePeriod,
            boolean startImmediately
    ) {
        this(
                workerCount,
                null,
                requireNonNull(jobStore, "Job store"),
                retryPolicy,
                shutdownGracePeriod,
                job -> {},
                startImmediately
        );
    }

    private JobScheduler(
            int workerCount,
            BlockingQueue<Job> jobQueue,
            JobStore jobStore,
            RetryPolicy retryPolicy,
            Duration shutdownGracePeriod,
            Consumer<Job> statusChangeListener,
            boolean startImmediately
    ) {
        if (workerCount <= 0) {
            throw new IllegalArgumentException(
                    "Worker count and queue capacity must be positive"
            );
        }

        this.workerCount = workerCount;
        this.jobQueue = jobQueue;
        this.jobStore = jobStore;
        this.retryPolicy = requireNonNull(retryPolicy, "Retry policy");
        this.shutdownGracePeriod =
                requireNonNull(shutdownGracePeriod, "Shutdown grace period");
        this.statusChangeListener =
                requireNonNull(statusChangeListener, "Listeners");

        this.workerPool = Executors.newFixedThreadPool(
                workerCount, namedThreads("taskforge-worker", false)
        );
        // Daemon threads: a handler that ignores interruption after a
        // timeout must not keep the JVM alive.
        this.handlerPool = Executors.newCachedThreadPool(
                namedThreads("taskforge-handler", true)
        );
        this.retryTimer = Executors.newSingleThreadScheduledExecutor(
                namedThreads("taskforge-retry", true)
        );

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

        // Workers claim persisted jobs from the store.
        if (isDatabaseBacked()) {
            return;
        }

        if (!jobQueue.offer(job)) {
            throw new RejectedExecutionException("Job queue is full");
        }
    }

    public synchronized CancellationResult cancel(String jobId) {
        if (jobId == null || jobId.isBlank()) {
            return CancellationResult.NOT_FOUND;
        }

        if (isDatabaseBacked()) {
            CancellationResult result = jobStore.cancel(jobId);

            if (result == CancellationResult.CANCELLATION_REQUESTED) {
                interruptIfRunningHere(jobId);
            }
            return result;
        }

        Job queuedJob = jobQueue.stream()
                .filter(job -> job.getId().equals(jobId))
                .findFirst()
                .orElse(null);

        if (queuedJob != null && queuedJob.cancel()) {
            jobQueue.remove(queuedJob);
            notifyStatusChanged(queuedJob);
            return CancellationResult.CANCELLED;
        }

        Job retryingJob = waitingRetries.get(jobId);

        if (retryingJob != null && retryingJob.cancel()) {
            waitingRetries.remove(jobId);
            notifyStatusChanged(retryingJob);
            return CancellationResult.CANCELLED;
        }

        if (interruptIfRunningHere(jobId)) {
            return CancellationResult.CANCELLATION_REQUESTED;
        }

        return CancellationResult.NOT_CANCELLABLE;
    }

    public synchronized CancellationResult cancel(Job job) {
        return job == null
                ? CancellationResult.NOT_FOUND
                : cancel(job.getId());
    }

    private boolean interruptIfRunningHere(String jobId) {
        RunningJob running = runningJobs.get(jobId);

        if (running == null || !running.job().requestCancellation()) {
            return false;
        }

        running.execution().cancel(true);
        return true;
    }

    private void workerLoop() {
        if (isDatabaseBacked()) {
            databaseWorkerLoop();
        } else {
            inMemoryWorkerLoop();
        }
    }

    private void inMemoryWorkerLoop() {
        // After shutdown(), keep draining queued jobs and pending retries.
        while (!forcedShutdown
                && (acceptingJobs.get()
                        || !jobQueue.isEmpty()
                        || !waitingRetries.isEmpty())) {
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
                Optional<Job> claimedJob = jobStore.claimNext();

                if (claimedJob.isPresent()) {
                    executeJob(claimedJob.get(), true);
                } else {
                    TimeUnit.MILLISECONDS.sleep(200);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (RuntimeException e) {
                log.warn("Database job claiming failed; retrying in 1 s: {}",
                        e.getMessage());

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
            // The store already changed the status to RUNNING and
            // incremented attempt_count while claiming the job.
            if (job.getStatus() != JobStatus.RUNNING) {
                return;
            }
        } else {
            if (!job.markRunning()) {
                return;
            }
            notifyStatusChanged(job);
        }

        MDC.put(MDC_JOB_ID, job.getId());
        MDC.put(MDC_JOB_NAME, job.getName());
        MDC.put(MDC_ATTEMPT, String.valueOf(job.getAttemptCount()));
        try {
            log.info("Job started (attempt {} of {})",
                    job.getAttemptCount(), job.getMaxAttempts());
            long startedAt = System.nanoTime();

            Outcome outcome = runAttempt(job);
            finishAttempt(job, outcome,
                    Duration.ofNanos(System.nanoTime() - startedAt));
        } finally {
            MDC.remove(MDC_JOB_ID);
            MDC.remove(MDC_JOB_NAME);
            MDC.remove(MDC_ATTEMPT);
        }
    }

    private Outcome runAttempt(Job job) {
        // Handlers run on another thread; carry the job's log context over.
        Map<String, String> logContext = MDC.getCopyOfContextMap();

        FutureTask<Void> execution = new FutureTask<>(() -> {
            if (logContext != null) {
                MDC.setContextMap(logContext);
            }
            try {
                job.getHandler().execute();
                return null;
            } finally {
                MDC.clear();
            }
        });

        runningJobs.put(job.getId(), new RunningJob(job, execution));

        try {
            handlerPool.execute(execution);
            return awaitOutcome(job, execution);
        } catch (RejectedExecutionException e) {
            return Outcome.interrupted();
        } finally {
            runningJobs.remove(job.getId());
        }
    }

    /*
     * Waits for the handler in short slices so that a timeout or a
     * cancellation requested through the store is noticed promptly.
     */
    private Outcome awaitOutcome(Job job, FutureTask<Void> execution) {
        long deadline = System.nanoTime() + job.getTimeout().toNanos();

        while (true) {
            long remaining = deadline - System.nanoTime();

            if (remaining <= 0) {
                execution.cancel(true);
                return Outcome.failed(
                        "Timed out after " + describe(job.getTimeout()),
                        true
                );
            }

            try {
                execution.get(
                        Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(CANCELLATION_POLL_MS)),
                        TimeUnit.NANOSECONDS
                );
                return Outcome.completed();
            } catch (TimeoutException e) {
                if (cancellationRequestedInStore(job)) {
                    job.requestCancellation();
                    execution.cancel(true);
                    return Outcome.cancelled();
                }
            } catch (CancellationException e) {
                // Interrupted by cancel() or by a forced shutdown.
                return job.isCancelRequested()
                        ? Outcome.cancelled()
                        : Outcome.interrupted();
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                String reason = cause.getMessage() == null
                        ? cause.getClass().getSimpleName()
                        : cause.getMessage();

                return Outcome.failed(
                        reason,
                        !(cause instanceof NonRetryableJobException)
                );
            } catch (InterruptedException e) {
                execution.cancel(true);
                Thread.currentThread().interrupt();
                return Outcome.interrupted();
            }
        }
    }

    private boolean cancellationRequestedInStore(Job job) {
        if (!isDatabaseBacked()) {
            return false;
        }

        try {
            return jobStore.isCancelRequested(job.getId());
        } catch (RuntimeException e) {
            log.warn("Could not check cancellation for job {}: {}",
                    job.getId(), e.getMessage());
            return false;
        }
    }

    private void finishAttempt(Job job, Outcome outcome, Duration elapsed) {
        switch (outcome.status()) {
            case COMPLETED -> job.markCompleted();
            case CANCELLED -> job.markCancelledWhileRunning();
            default -> job.markFailed(outcome.reason());
        }

        Duration retryDelay = retryDelayFor(job, outcome);
        AttemptResult result = new AttemptResult(
                outcome.status(), outcome.reason(), retryDelay
        );

        logOutcome(job, result, elapsed);

        if (isDatabaseBacked()) {
            recordInStore(job, result);
        } else {
            notifyStatusChanged(job);
        }

        if (!result.willRetry()) {
            return;
        }

        if (isDatabaseBacked()) {
            job.requeueForRetry(retryDelay);
            return;
        }

        // Register before the job turns QUEUED, so that cancel()
        // always finds a QUEUED job that is waiting to retry.
        waitingRetries.put(job.getId(), job);
        job.requeueForRetry(retryDelay);
        scheduleInMemoryRetry(job, retryDelay);
    }

    private Duration retryDelayFor(Job job, Outcome outcome) {
        if (outcome.status() != JobStatus.FAILED
                || !outcome.retryable()
                || job.isCancelRequested()
                || !job.hasAttemptsRemaining()) {
            return null;
        }

        return outcome.retryImmediately()
                ? Duration.ZERO
                : retryPolicy.delayAfterAttempt(job.getAttemptCount());
    }

    private void recordInStore(Job job, AttemptResult result) {
        // A worker interrupted during shutdown must still be able to
        // borrow a connection and record the result.
        boolean interrupted = Thread.interrupted();

        try {
            if (jobStore.finishAttempt(job, result).isEmpty()) {
                log.warn("Discarded stale result for job {} attempt {}; "
                                + "the job was already recovered elsewhere",
                        job.getId(), job.getAttemptCount());
            }
        } catch (RuntimeException e) {
            // The job stays RUNNING in the database and is
            // recovered on the next startup.
            log.error("Could not record attempt result for job {}; "
                    + "it stays RUNNING until the next startup recovery",
                    job.getId(), e);
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void scheduleInMemoryRetry(Job job, Duration delay) {
        try {
            retryTimer.schedule(() -> {
                if (waitingRetries.remove(job.getId()) == null
                        || job.getStatus() != JobStatus.QUEUED) {
                    return;
                }

                if (!jobQueue.offer(job)) {
                    log.error("Retry queue is full; dropping retry of job {}",
                            job.getId());
                }
            }, delay.toMillis(), TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            waitingRetries.remove(job.getId());
        }
    }

    private void logOutcome(Job job, AttemptResult result, Duration elapsed) {
        long ms = elapsed.toMillis();

        switch (result.status()) {
            case COMPLETED -> log.info("Job completed in {} ms", ms);
            case CANCELLED -> log.info("Job cancelled after {} ms", ms);
            default -> {
                if (result.willRetry()) {
                    log.warn("Job attempt {} of {} failed after {} ms: {}; "
                                    + "retrying in {} ms",
                            job.getAttemptCount(), job.getMaxAttempts(), ms,
                            result.failureReason(),
                            result.retryDelay().toMillis());
                } else if (!job.hasAttemptsRemaining()) {
                    log.error("Job failed permanently after exhausting "
                                    + "all {} attempts: {}",
                            job.getMaxAttempts(), result.failureReason());
                } else if (!job.isCancelRequested()) {
                    log.error("Job failed permanently (not retryable): {}",
                            result.failureReason());
                }
            }
        }
    }

    private boolean isDatabaseBacked() {
        return jobStore != null;
    }

    private void notifyStatusChanged(Job job) {
        try {
            statusChangeListener.accept(job);
        } catch (RuntimeException e) {
            log.error("Status listener failed for job {}", job.getId(), e);
        }
    }

    /*
     * Stops accepting jobs. Running jobs finish; an in-memory scheduler
     * also drains its queue and pending retries.
     */
    public synchronized void shutdown() {
        acceptingJobs.set(false);
        workerPool.shutdown();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit)
            throws InterruptedException {
        boolean terminated = workerPool.awaitTermination(timeout, unit);

        if (terminated) {
            retryTimer.shutdownNow();
            handlerPool.shutdown();
        }
        return terminated;
    }

    /*
     * Lets running jobs finish within the grace period, then interrupts
     * the rest. Interrupted attempts are recorded as failed and, with a
     * store, requeued immediately for the next start.
     */
    public void shutdownGracefully(Duration gracePeriod) {
        shutdown();

        try {
            if (awaitTermination(gracePeriod.toMillis(), TimeUnit.MILLISECONDS)) {
                return;
            }

            log.warn("Interrupting {} job(s) still running after {}",
                    runningJobs.size(), describe(gracePeriod));

            forcedShutdown = true;
            runningJobs.values()
                    .forEach(running -> running.execution().cancel(true));

            if (!awaitTermination(
                    FORCED_SHUTDOWN_WAIT.toMillis(), TimeUnit.MILLISECONDS
            )) {
                workerPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            workerPool.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            handlerPool.shutdownNow();
            retryTimer.shutdownNow();
        }
    }

    @Override
    public void close() {
        shutdownGracefully(shutdownGracePeriod);
    }

    public int getQueueSize() {
        return isDatabaseBacked() ? -1 : jobQueue.size();
    }

    public int getRunningJobCount() {
        return runningJobs.size();
    }

    public int getWorkerCount() {
        return workerCount;
    }

    // True once start() has run and until shutdown begins.
    public boolean isRunning() {
        return workersStarted.get() && acceptingJobs.get();
    }

    public boolean isShuttingDown() {
        return !acceptingJobs.get();
    }

    private static String describe(Duration duration) {
        return duration.toMillis() < 1000
                ? duration.toMillis() + " ms"
                : duration.toSeconds() + " s";
    }

    private static BlockingQueue<Job> validatedQueue(int queueCapacity) {
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException(
                    "Worker count and queue capacity must be positive"
            );
        }
        return new ArrayBlockingQueue<>(queueCapacity);
    }

    private static <T> T requireNonNull(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " cannot be null");
        }
        return value;
    }

    private static ThreadFactory namedThreads(String prefix, boolean daemon) {
        AtomicInteger counter = new AtomicInteger();

        return runnable -> {
            Thread thread = new Thread(
                    runnable, prefix + "-" + counter.incrementAndGet()
            );
            thread.setDaemon(daemon);
            return thread;
        };
    }
}
