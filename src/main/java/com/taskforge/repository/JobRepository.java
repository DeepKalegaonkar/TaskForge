package com.taskforge.repository;

import com.taskforge.dto.SubmitJobRequest.JobType;
import com.taskforge.handler.JobHandlerFactory;
import com.taskforge.model.Job;
import com.taskforge.model.JobStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public class JobRepository {

    private final JdbcTemplate jdbcTemplate;
    private final JobHandlerFactory handlerFactory;
    private final RowMapper<Job> jobRowMapper;

    public JobRepository(
            JdbcTemplate jdbcTemplate,
            JobHandlerFactory handlerFactory
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.handlerFactory = handlerFactory;

        this.jobRowMapper = (rs, rowNum) -> {
            String id = rs.getString("id");
            String name = rs.getString("name");
            JobType type = JobType.valueOf(rs.getString("job_type"));
            JobStatus status = JobStatus.valueOf(rs.getString("status"));

            Timestamp createdTimestamp = rs.getTimestamp("created_at");
            Timestamp startedTimestamp = rs.getTimestamp("started_at");
            Timestamp completedTimestamp = rs.getTimestamp("completed_at");

            Instant createdAt = createdTimestamp.toInstant();
            Instant startedAt = startedTimestamp != null
                    ? startedTimestamp.toInstant()
                    : null;
            Instant completedAt = completedTimestamp != null
                    ? completedTimestamp.toInstant()
                    : null;

            String failureReason = rs.getString("failure_reason");
            int attemptCount = rs.getInt("attempt_count");

            return new Job(
                    id,
                    name,
                    type,
                    handlerFactory.createHandler(type),
                    createdAt,
                    status,
                    startedAt,
                    completedAt,
                    failureReason,
                    attemptCount
            );
        };
    }

    public Job save(Job job) {
        if (job.getType() == null) {
            throw new IllegalArgumentException(
                    "Job type is required for database persistence"
            );
        }

        jdbcTemplate.update(
                """
                INSERT INTO jobs (
                    id, name, job_type, status, created_at,
                    started_at, completed_at, failure_reason,
                    attempt_count
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    job_type = EXCLUDED.job_type,
                    status = EXCLUDED.status,
                    started_at = EXCLUDED.started_at,
                    completed_at = EXCLUDED.completed_at,
                    failure_reason = EXCLUDED.failure_reason,
                    attempt_count = EXCLUDED.attempt_count
                """,
                job.getId(),
                job.getName(),
                job.getType().name(),
                job.getStatus().name(),
                Timestamp.from(job.getCreatedAt()),
                toTimestamp(job.getStartedAt()),
                toTimestamp(job.getCompletedAt()),
                job.getFailureReason(),
                job.getAttemptCount()
        );

        return job;
    }

    public Optional<Job> findById(String id) {
        return jdbcTemplate.query(
                "SELECT * FROM jobs WHERE id = ?",
                jobRowMapper,
                id
        ).stream().findFirst();
    }

    public List<Job> findAll() {
        return jdbcTemplate.query(
                "SELECT * FROM jobs ORDER BY created_at DESC",
                jobRowMapper
        );
    }

    /*
     * FOR UPDATE makes a second instance recovering at the same time
     * wait, then skip rows whose status has already changed.
     */
    public List<Job> findAllRunningJobs() {
        return jdbcTemplate.query(
                "SELECT * FROM jobs WHERE status = ? FOR UPDATE",
                jobRowMapper,
                JobStatus.RUNNING.name()
        );
    }

    /*
     * FAILED jobs with attempts remaining were waiting out their retry
     * delay when the application stopped. Rows with attempt_count = 0
     * predate attempt tracking and are left alone.
     */
    public List<Job> findFailedJobsAwaitingRetry(int maxAttempts) {
        return jdbcTemplate.query(
                """
                SELECT *
                FROM jobs
                WHERE status = ?
                  AND attempt_count BETWEEN 1 AND ?
                FOR UPDATE
                """,
                jobRowMapper,
                JobStatus.FAILED.name(),
                maxAttempts - 1
        );
    }

    /*
     * Atomically claim one queued job.
     * SKIP LOCKED prevents concurrent workers from claiming the same row.
     * The attempt count is incremented in the same transaction as the claim.
     */
    @Transactional
    public Optional<Job> claimNextQueuedJob() {
        List<String> ids = jdbcTemplate.query(
                """
                SELECT id
                FROM jobs
                WHERE status = ?
                ORDER BY created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """,
                (rs, rowNum) -> rs.getString("id"),
                JobStatus.QUEUED.name()
        );

        if (ids.isEmpty()) {
            return Optional.empty();
        }

        String id = ids.get(0);

        int updated = jdbcTemplate.update(
                """
                UPDATE jobs
                SET status = ?,
                    started_at = NOW(),
                    completed_at = NULL,
                    failure_reason = NULL,
                    attempt_count = attempt_count + 1
                WHERE id = ?
                  AND status = ?
                """,
                JobStatus.RUNNING.name(),
                id,
                JobStatus.QUEUED.name()
        );

        if (updated != 1) {
            return Optional.empty();
        }

        return findById(id);
    }

    /*
     * Cancel only a job that remains queued.
     */
    public boolean cancelQueuedJob(String id) {
        int updated = jdbcTemplate.update(
                """
                UPDATE jobs
                SET status = ?,
                    completed_at = NOW(),
                    failure_reason = NULL
                WHERE id = ?
                  AND status = ?
                """,
                JobStatus.CANCELLED.name(),
                id,
                JobStatus.QUEUED.name()
        );

        return updated == 1;
    }

    public void deleteById(String id) {
        jdbcTemplate.update(
                "DELETE FROM jobs WHERE id = ?",
                id
        );
    }

    public boolean existsById(String id) {
        Boolean exists = jdbcTemplate.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM jobs WHERE id = ?)",
                Boolean.class,
                id
        );

        return Boolean.TRUE.equals(exists);
    }

    private Timestamp toTimestamp(Instant instant) {
        return instant != null ? Timestamp.from(instant) : null;
    }

    /*
     * Requeue a failed job only if the database still contains
     * the expected failed attempt.
     */
    public boolean requeueFailedJob(Job job) {
        if (job == null || job.getStatus() != JobStatus.QUEUED) {
            return false;
        }

        int updated = jdbcTemplate.update(
                """
                UPDATE jobs
                SET status = ?,
                    started_at = NULL,
                    completed_at = NULL,
                    failure_reason = NULL
                WHERE id = ?
                  AND status = ?
                  AND attempt_count = ?
                """,
                JobStatus.QUEUED.name(),
                job.getId(),
                JobStatus.FAILED.name(),
                job.getAttemptCount()
        );

        return updated == 1;
    }

    /*
     * Mark an interrupted RUNNING job as FAILED after a restart.
     */
    public boolean recoverInterruptedJob(
            String jobId,
            String failureReason
    ) {
        int updated = jdbcTemplate.update(
                """
                UPDATE jobs
                SET status = ?,
                    completed_at = NOW(),
                    failure_reason = ?
                WHERE id = ?
                  AND status = ?
                """,
                JobStatus.FAILED.name(),
                failureReason,
                jobId,
                JobStatus.RUNNING.name()
        );

        return updated == 1;
    }
}
