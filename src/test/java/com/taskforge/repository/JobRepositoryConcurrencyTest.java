
package com.taskforge.repository;

import com.taskforge.model.Job;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
class JobRepositoryConcurrencyTest {

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private org.springframework.jdbc.core.JdbcTemplate jdbcTemplate;

    @BeforeEach
    void cleanQueuedTestJobs() {
        jdbcTemplate.update(
                """
                DELETE FROM jobs
                WHERE name LIKE 'concurrency-test-%'
                """
        );
    }

    @Test
    void concurrentWorkersShouldClaimDifferentJobs() throws Exception {
        String firstId = UUID.randomUUID().toString();
        String secondId = UUID.randomUUID().toString();

        jdbcTemplate.update(
                """
                INSERT INTO jobs (
                    id, name, job_type, status, created_at
                )
                VALUES (?, ?, ?, 'QUEUED', NOW() - INTERVAL '2 seconds')
                """,
                firstId,
                "concurrency-test-first",
                "GENERATE_REPORT"
        );

        jdbcTemplate.update(
                """
                INSERT INTO jobs (
                    id, name, job_type, status, created_at
                )
                VALUES (?, ?, ?, 'QUEUED', NOW() - INTERVAL '1 second')
                """,
                secondId,
                "concurrency-test-second",
                "GENERATE_REPORT"
        );

        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);

        ExecutorService workers = Executors.newFixedThreadPool(2);

        try {
            Future<Optional<Job>> workerOne = workers.submit(() -> {
                ready.countDown();
                start.await();

                return new TransactionTemplate(transactionManager)
                        .execute(status ->
                                jobRepository.claimNextQueuedJob()
                        );
            });

            Future<Optional<Job>> workerTwo = workers.submit(() -> {
                ready.countDown();
                start.await();

                return new TransactionTemplate(transactionManager)
                        .execute(status ->
                                jobRepository.claimNextQueuedJob()
                        );
            });

            assertTrue(ready.await(5, TimeUnit.SECONDS));
            start.countDown();

            Optional<Job> firstClaim =
                    workerOne.get(10, TimeUnit.SECONDS);
            Optional<Job> secondClaim =
                    workerTwo.get(10, TimeUnit.SECONDS);

            assertTrue(firstClaim.isPresent());
            assertTrue(secondClaim.isPresent());

            assertNotEquals(
                    firstClaim.get().getId(),
                    secondClaim.get().getId(),
                    "Two workers must never claim the same job"
            );

            Set<String> claimedIds = new HashSet<>(List.of(
                    firstClaim.get().getId(),
                    secondClaim.get().getId()
            ));

            assertEquals(2, claimedIds.size());

            for (String claimedId : claimedIds) {
                String status = jdbcTemplate.queryForObject(
                        "SELECT status FROM jobs WHERE id = ?",
                        String.class,
                        claimedId
                );

                assertEquals("RUNNING", status);
            }
        } finally {
            workers.shutdownNow();

            jdbcTemplate.update(
                    "DELETE FROM jobs WHERE id IN (?, ?)",
                    firstId,
                    secondId
            );
        }
    }
}
