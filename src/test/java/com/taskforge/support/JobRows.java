package com.taskforge.support;

import com.taskforge.model.JobStatus;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/*
 * Writes job rows directly, in the state a test needs, and deletes them
 * afterwards. Rows are dated in the year 2000 so that a claim picks them
 * before any other QUEUED job in the database.
 */
public class JobRows {

    private final JdbcTemplate jdbcTemplate;
    private final List<String> createdIds = new ArrayList<>();

    public JobRows(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public String insert(JobStatus status, int attemptCount, int maxAttempts) {
        return insert(status, attemptCount, maxAttempts, null);
    }

    public String insert(
            JobStatus status,
            int attemptCount,
            int maxAttempts,
            String failureReason
    ) {
        String id = UUID.randomUUID().toString();

        jdbcTemplate.update(
                """
                INSERT INTO jobs (
                    id, name, job_type, status, created_at, available_at,
                    started_at, failure_reason, attempt_count, max_attempts
                )
                VALUES (?, 'job-rows-test', 'GENERATE_REPORT', ?,
                        TIMESTAMPTZ '2000-01-01 00:00:00+00'
                            + ? * INTERVAL '1 second',
                        NOW(),
                        CASE WHEN ? > 0 THEN NOW() END, ?, ?, ?)
                """,
                id, status.name(), createdIds.size(),
                attemptCount, failureReason, attemptCount, maxAttempts
        );

        createdIds.add(id);
        return id;
    }

    public void insertAttempt(
            String jobId,
            int attemptNumber,
            JobStatus status,
            String failureReason
    ) {
        jdbcTemplate.update(
                """
                INSERT INTO job_attempts (
                    job_id, attempt_number, status,
                    started_at, completed_at, failure_reason
                )
                VALUES (?, ?, ?, NOW(),
                        CASE WHEN ? = 'RUNNING' THEN NULL ELSE NOW() END, ?)
                """,
                jobId, attemptNumber, status.name(), status.name(), failureReason
        );
    }

    public void requestCancellation(String jobId) {
        jdbcTemplate.update(
                "UPDATE jobs SET cancel_requested = TRUE WHERE id = ?",
                jobId
        );
    }

    // Simulates the retry delay having passed.
    public void makeAvailableNow(String jobId) {
        jdbcTemplate.update(
                "UPDATE jobs SET available_at = NOW() WHERE id = ?",
                jobId
        );
    }

    public Map<String, Object> row(String jobId) {
        return jdbcTemplate.queryForMap(
                """
                SELECT *,
                       EXTRACT(EPOCH FROM (available_at - NOW())) AS seconds_until_available
                FROM jobs
                WHERE id = ?
                """,
                jobId
        );
    }

    public void track(String jobId) {
        createdIds.add(jobId);
    }

    public void deleteAll() {
        for (String id : createdIds) {
            jdbcTemplate.update("DELETE FROM job_attempts WHERE job_id = ?", id);
            jdbcTemplate.update("DELETE FROM jobs WHERE id = ?", id);
        }
        createdIds.clear();
    }
}
