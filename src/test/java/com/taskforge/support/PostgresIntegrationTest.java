package com.taskforge.support;

import com.taskforge.scheduler.JobScheduler;
import com.taskforge.scheduler.JobStore;
import com.taskforge.scheduler.RetryPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/*
 * Base class for tests that need PostgreSQL.
 *
 * One disposable container is started per JVM and shared by every
 * subclass, so all of them also share one cached Spring context.
 * Flyway applies the real migrations to it on context startup.
 *
 * Workers are disabled in the context (see config/application.properties
 * under src/test/resources). Tests that need execution start their own
 * schedulers with startScheduler(), which are shut down after each test.
 */
@SpringBootTest
@AutoConfigureMockMvc
@Import(ScriptedJobHandlers.class)
public abstract class PostgresIntegrationTest {

    public static final String POSTGRES_IMAGE = "postgres:17-alpine";

    protected static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(POSTGRES_IMAGE);

    // Short backoff so retry tests finish quickly.
    protected static final RetryPolicy FAST_RETRIES = new RetryPolicy(
            Duration.ofMillis(100), 2.0, Duration.ofSeconds(1)
    );

    static {
        POSTGRES.start();
    }

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    protected ScriptedJobHandlers handlers;

    private final List<JobScheduler> schedulers = new ArrayList<>();

    @BeforeEach
    void resetHandlers() {
        handlers.reset();
    }

    @AfterEach
    void stopSchedulers() {
        schedulers.forEach(s -> s.shutdownGracefully(Duration.ofSeconds(2)));
        schedulers.clear();
    }

    /*
     * Starts a database-backed scheduler, as one application instance
     * would. Several can run at once to simulate several instances.
     */
    protected JobScheduler startScheduler(int workers, JobStore store) {
        return createScheduler(workers, store, true);
    }

    protected JobScheduler createScheduler(int workers, JobStore store, boolean start) {
        JobScheduler scheduler = new JobScheduler(
                workers, store, FAST_RETRIES, Duration.ofSeconds(2), start
        );
        schedulers.add(scheduler);
        return scheduler;
    }

    // Makes every waiting job claimable now, as if its backoff had passed.
    protected void skipBackoff() {
        jdbcTemplate.update(
                "UPDATE jobs SET available_at = NOW() WHERE status = 'QUEUED'"
        );
    }

    // Only for tests that own the whole database during the test.
    protected void deleteAllJobs() {
        jdbcTemplate.update("DELETE FROM job_attempts");
        jdbcTemplate.update("DELETE FROM jobs");
    }

    protected String statusOf(String jobId) {
        return jdbcTemplate.queryForObject(
                "SELECT status FROM jobs WHERE id = ?", String.class, jobId
        );
    }

    protected int countJobs(String status) {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM jobs WHERE status = ?", Integer.class, status
        );
    }
}
