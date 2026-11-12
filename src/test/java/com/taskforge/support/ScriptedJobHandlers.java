package com.taskforge.support;

import com.taskforge.dto.SubmitJobRequest.JobType;
import com.taskforge.handler.JobHandler;
import com.taskforge.handler.JobHandlerFactory;
import org.springframework.context.annotation.Primary;

import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/*
 * Replaces the production handler factory in integration tests.
 *
 * GENERATE_REPORT jobs follow a script: each execution takes the next
 * step (succeed, fail, hang or throw an Error), and once the script is
 * used up every execution succeeds after workDuration. Other job types
 * keep their production behaviour.
 *
 * Every execution of any type is counted, along with the highest number
 * running at the same time, so tests can detect duplicate or excess
 * parallel execution.
 *
 * Deliberately not a @Component: it is added with @Import, so it never
 * leaks into an application context started by a test.
 */
@Primary
public class ScriptedJobHandlers extends JobHandlerFactory {

    public static final String SCRIPTED_FAILURE = "Scripted failure";

    public enum Step { SUCCEED, FAIL, HANG, ERROR }

    private final Queue<Step> script = new ConcurrentLinkedQueue<>();
    private final AtomicInteger executions = new AtomicInteger();
    private final AtomicInteger running = new AtomicInteger();
    private final AtomicInteger maxRunning = new AtomicInteger();
    private final Semaphore started = new Semaphore(0);

    private volatile Step defaultStep = Step.SUCCEED;
    private volatile Duration workDuration = Duration.ZERO;

    @Override
    public JobHandler createHandler(JobType type) {
        JobHandler body = type == JobType.GENERATE_REPORT
                ? this::runNextStep
                : super.createHandler(type);

        return () -> {
            executions.incrementAndGet();
            maxRunning.accumulateAndGet(running.incrementAndGet(), Math::max);
            started.release();
            try {
                body.execute();
            } finally {
                running.decrementAndGet();
            }
        };
    }

    private void runNextStep() throws Exception {
        Step step = script.poll();

        switch (step == null ? defaultStep : step) {
            case FAIL -> throw new RuntimeException(SCRIPTED_FAILURE);
            // An Error, not an Exception: the worst a handler can throw.
            case ERROR -> throw new StackOverflowError("Simulated handler crash");
            case HANG -> Thread.sleep(Duration.ofMinutes(10).toMillis());
            case SUCCEED -> {
                long millis = workDuration.toMillis();
                if (millis > 0) {
                    Thread.sleep(millis);
                }
            }
        }
    }

    public void script(Step... steps) {
        script.addAll(java.util.List.of(steps));
    }

    public void setDefaultStep(Step step) {
        this.defaultStep = step;
    }

    public void setWorkDuration(Duration workDuration) {
        this.workDuration = workDuration;
    }

    public int executions() {
        return executions.get();
    }

    public int maxConcurrentExecutions() {
        return maxRunning.get();
    }

    public boolean awaitStarted(int count, Duration timeout)
            throws InterruptedException {
        return started.tryAcquire(count, timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void reset() {
        script.clear();
        defaultStep = Step.SUCCEED;
        workDuration = Duration.ZERO;
        executions.set(0);
        running.set(0);
        maxRunning.set(0);
        started.drainPermits();
    }
}
