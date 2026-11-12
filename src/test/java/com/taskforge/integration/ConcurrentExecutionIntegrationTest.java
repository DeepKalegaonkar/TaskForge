package com.taskforge.integration;

import com.taskforge.dto.SubmitJobRequest;
import com.taskforge.dto.SubmitJobRequest.JobType;
import com.taskforge.service.DatabaseJobStore;
import com.taskforge.service.JobService;
import com.taskforge.support.Await;
import com.taskforge.support.FaultInjectingJobStore;
import com.taskforge.support.PostgresIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/*
 * Real workers claiming from PostgreSQL while clients submit concurrently.
 * Each job must run exactly once, even with several scheduler instances
 * competing for the same rows.
 */
class ConcurrentExecutionIntegrationTest extends PostgresIntegrationTest {

    @Autowired
    private JobService jobService;

    @Autowired
    private DatabaseJobStore jobStore;

    @BeforeEach
    void emptyDatabase() {
        deleteAllJobs();
    }

    @Test
    void jobsSubmittedConcurrentlyWhileWorkersRunShouldEachRunExactlyOnce()
            throws Exception {
        int workers = 4;
        int clients = 8;
        int jobsPerClient = 50;
        int totalJobs = clients * jobsPerClient;

        handlers.setWorkDuration(Duration.ofMillis(5));
        startScheduler(workers, jobStore);

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(clients);
        try {
            List<Future<?>> submissions = new ArrayList<>();
            for (int c = 0; c < clients; c++) {
                submissions.add(pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < jobsPerClient; i++) {
                        jobService.submitJob(report("concurrent"), null);
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> submission : submissions) {
                submission.get(60, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        Await.until(Duration.ofSeconds(60),
                () -> countJobs("COMPLETED") == totalJobs,
                () -> countJobs("COMPLETED") + " of " + totalJobs + " completed");

        assertExactlyOnce(totalJobs);
        assertTrue(handlers.maxConcurrentExecutions() <= workers,
                "Saw " + handlers.maxConcurrentExecutions()
                        + " concurrent executions with " + workers + " workers");
    }

    @Test
    void twoSchedulerInstancesSharingTheDatabaseShouldNotRunAnyJobTwice()
            throws Exception {
        int totalJobs = 300;
        handlers.setWorkDuration(Duration.ofMillis(5));

        for (int i = 0; i < totalJobs; i++) {
            jobService.submitJob(report("two-instances"), null);
        }

        // Two application instances, two workers each.
        FaultInjectingJobStore instanceA = new FaultInjectingJobStore(jobStore);
        FaultInjectingJobStore instanceB = new FaultInjectingJobStore(jobStore);
        startScheduler(2, instanceA);
        startScheduler(2, instanceB);

        Await.until(Duration.ofSeconds(60),
                () -> countJobs("COMPLETED") == totalJobs,
                () -> countJobs("COMPLETED") + " of " + totalJobs + " completed");

        assertExactlyOnce(totalJobs);
        assertEquals(totalJobs, instanceA.claims() + instanceB.claims());
        assertTrue(instanceA.claims() > 0 && instanceB.claims() > 0,
                "Both instances should have taken work: A=" + instanceA.claims()
                        + " B=" + instanceB.claims());
    }

    private void assertExactlyOnce(int totalJobs) {
        assertEquals(totalJobs, handlers.executions(),
                "Handler executions must equal the number of jobs");
        assertEquals(totalJobs, jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM jobs WHERE status = 'COMPLETED' AND attempt_count = 1",
                Integer.class));
        assertEquals(totalJobs, jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM job_attempts WHERE status = 'COMPLETED' AND attempt_number = 1",
                Integer.class));
        assertEquals(totalJobs, jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM job_attempts", Integer.class),
                "There must be no extra attempt rows");
    }

    static SubmitJobRequest report(String name) {
        return new SubmitJobRequest(name, JobType.GENERATE_REPORT, null, null);
    }
}
