package com.taskforge.service;

import com.taskforge.dto.SubmitJobRequest;
import com.taskforge.dto.SubmitJobRequest.JobType;
import com.taskforge.service.JobService.SubmitResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import com.taskforge.support.PostgresIntegrationTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JobSubmissionIdempotencyTest extends PostgresIntegrationTest {

    private static final int CONCURRENT_REQUESTS = 8;

    @Autowired
    private JobService jobService;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final String key = "idempotency-test-" + UUID.randomUUID();

    @AfterEach
    void cleanUp() {
        jdbcTemplate.update("DELETE FROM jobs WHERE idempotency_key = ?", key);
    }

    @Test
    void concurrentDuplicateSubmissionsShouldCreateOneJob() throws Exception {
        SubmitJobRequest request = new SubmitJobRequest(
                "Idempotency test", JobType.GENERATE_REPORT, null, null
        );

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService clients = Executors.newFixedThreadPool(CONCURRENT_REQUESTS);
        List<Future<SubmitResult>> results = new ArrayList<>();

        try {
            for (int i = 0; i < CONCURRENT_REQUESTS; i++) {
                results.add(clients.submit(() -> {
                    start.await();
                    return jobService.submitJob(request, key);
                }));
            }

            start.countDown();

            List<SubmitResult> submitted = new ArrayList<>();
            for (Future<SubmitResult> result : results) {
                submitted.add(result.get(10, TimeUnit.SECONDS));
            }

            Set<String> jobIds = submitted.stream()
                    .map(result -> result.job().getId())
                    .collect(Collectors.toSet());
            long created = submitted.stream()
                    .filter(result -> !result.replayed())
                    .count();

            assertEquals(1, jobIds.size(), "All requests must get the same job");
            assertEquals(1, created, "Exactly one request creates the job");
            assertEquals(1, jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM jobs WHERE idempotency_key = ?",
                    Integer.class, key
            ));
        } finally {
            clients.shutdownNow();
        }
    }
}
