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
import java.time.Duration;
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
            JobType type = JobType.valueOf(rs.getString("job_type"));
            JobStatus status = JobStatus.valueOf(rs.getString("status"));
            int attemptCount = rs.getInt("attempt_count");

            // available_at only matters while a failed job waits to retry.
            Instant nextAttemptAt =
                    status == JobStatus.QUEUED && attemptCount > 0
                            ? toInstant(rs.getTimestamp("available_at"))
                            : null;

            return Job.builder(
                            rs.getString("name"),
                            handlerFactory.createHandler(type)
                    )
                    .id(rs.getString("id"))
                    .type(type)
                    .status(status)
                    .createdAt(toInstant(rs.getTimestamp("created_at")))
                    .startedAt(toInstant(rs.getTimestamp("started_at")))
                    .completedAt(toInstant(rs.getTimestamp("completed_at")))
                    .nextAttemptAt(nextAttemptAt)
                    .failureReason(rs.getString("failure_reason"))
                    .attemptCount(attemptCount)
                    .maxAttempts(rs.getInt("max_attempts"))
                    .timeout(Duration.ofSeconds(rs.getInt("timeout_seconds")))
                    .idempotencyKey(rs.getString("idempotency_key"))
                    .cancelRequested(rs.getBoolean("cancel_requested"))
                    .build();
        };
    }

    /*
     * Inserts a new job. Returns false, without inserting, when another
     * job already uses the same idempotency key.
     */
    public boolean insert(Job job) {
        if (job.getType() == null) {
            throw new IllegalArgumentException(
                    "Job type is required for database persistence"
            );
        }

        int inserted = jdbcTemplate.update(
                """
                INSERT INTO jobs (
                    id, name, job_type, status, created_at,
                    available_at, attempt_count, max_attempts,
                    timeout_seconds, idempotency_key
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (idempotency_key) DO NOTHING
                """,
                job.getId(),
                job.getName(),
                job.getType().name(),
                job.getStatus().name(),
                Timestamp.from(job.getCreatedAt()),
                Timestamp.from(job.getCreatedAt()),
                job.getAttemptCount(),
                job.getMaxAttempts(),
                Math.toIntExact(job.getTimeout().toSeconds()),
                job.getIdempotencyKey()
        );

        return inserted == 1;
    }

    public Optional<Job> findById(String id) {
        return jdbcTemplate.query(
                "SELECT * FROM jobs WHERE id = ?",
                jobRowMapper,
                id
        ).stream().findFirst();
    }

    public Optional<Job> findByIdempotencyKey(String idempotencyKey) {
        return jdbcTemplate.query(
                "SELECT * FROM jobs WHERE idempotency_key = ?",
                jobRowMapper,
                idempotencyKey
        ).stream().findFirst();
    }

    public List<Job> findAll() {
        return jdbcTemplate.query(
                "SELECT * FROM jobs ORDER BY created_at DESC",
                jobRowMapper
        );
    }

    public List<Job> findByStatus(JobStatus status) {
        return jdbcTemplate.query(
                "SELECT * FROM jobs WHERE status = ? ORDER BY created_at DESC",
                jobRowMapper,
                status.name()
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
     * Atomically claim the oldest queued job whose retry delay has passed.
     * SKIP LOCKED prevents concurrent workers from claiming the same row.
     * The attempt count is incremented in the same statement as the claim.
     */
    @Transactional
    public Optional<Job> claimNextQueuedJob() {
        return jdbcTemplate.query(
                """
                UPDATE jobs
                SET status = ?,
                    started_at = NOW(),
                    completed_at = NULL,
                    failure_reason = NULL,
                    attempt_count = attempt_count + 1
                WHERE id = (
                    SELECT id
                    FROM jobs
                    WHERE status = ?
                      AND available_at <= NOW()
                    ORDER BY created_at ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING *
                """,
                jobRowMapper,
                JobStatus.RUNNING.name(),
                JobStatus.QUEUED.name()
        ).stream().findFirst();
    }

    /*
     * Locks the job's row if attemptNumber is still its current RUNNING
     * attempt, and returns whether cancellation was requested.
     * Empty means the attempt is stale and must not change the job.
     */
    public Optional<Boolean> lockRunningAttempt(String id, int attemptNumber) {
        return jdbcTemplate.query(
                """
                SELECT cancel_requested
                FROM jobs
                WHERE id = ?
                  AND status = ?
                  AND attempt_count = ?
                FOR UPDATE
                """,
                (rs, rowNum) -> rs.getBoolean("cancel_requested"),
                id,
                JobStatus.RUNNING.name(),
                attemptNumber
        ).stream().findFirst();
    }

    /*
     * Moves a RUNNING job to the status decided for its attempt.
     * A QUEUED job becomes claimable again after retryDelay.
     */
    public void finishRunningJob(
            String id,
            JobStatus status,
            String failureReason,
            Duration retryDelay
    ) {
        boolean requeued = status == JobStatus.QUEUED;

        jdbcTemplate.update(
                """
                UPDATE jobs
                SET status = ?,
                    started_at = CASE WHEN ? THEN NULL ELSE started_at END,
                    completed_at = CASE WHEN ? THEN NULL ELSE NOW() END,
                    failure_reason = ?,
                    available_at = CASE
                        WHEN ? THEN NOW() + ? * INTERVAL '1 millisecond'
                        ELSE available_at
                    END
                WHERE id = ?
                  AND status = ?
                """,
                status.name(),
                requeued,
                requeued,
                failureReason,
                requeued,
                requeued ? retryDelay.toMillis() : 0,
                id,
                JobStatus.RUNNING.name()
        );
    }

    /*
     * Cancel only a job that remains queued, including one
     * waiting for its next retry.
     */
    public boolean cancelQueuedJob(String id) {
        int updated = jdbcTemplate.update(
                """
                UPDATE jobs
                SET status = ?,
                    completed_at = NOW()
                WHERE id = ?
                  AND status = ?
                """,
                JobStatus.CANCELLED.name(),
                id,
                JobStatus.QUEUED.name()
        );

        return updated == 1;
    }

    public boolean requestCancellation(String id) {
        int updated = jdbcTemplate.update(
                """
                UPDATE jobs
                SET cancel_requested = TRUE
                WHERE id = ?
                  AND status = ?
                """,
                id,
                JobStatus.RUNNING.name()
        );

        return updated == 1;
    }

    public boolean isCancelRequested(String id) {
        return jdbcTemplate.query(
                "SELECT cancel_requested FROM jobs WHERE id = ?",
                (rs, rowNum) -> rs.getBoolean("cancel_requested"),
                id
        ).stream().findFirst().orElse(false);
    }

    /*
     * Gives a permanently FAILED job one more attempt.
     * Its attempt history continues from the last attempt number.
     */
    public boolean retryFailedJob(String id) {
        int updated = jdbcTemplate.update(
                """
                UPDATE jobs
                SET status = ?,
                    started_at = NULL,
                    completed_at = NULL,
                    available_at = NOW(),
                    cancel_requested = FALSE,
                    max_attempts = attempt_count + 1
                WHERE id = ?
                  AND status = ?
                """,
                JobStatus.QUEUED.name(),
                id,
                JobStatus.FAILED.name()
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

    private static Instant toInstant(Timestamp timestamp) {
        return timestamp != null ? timestamp.toInstant() : null;
    }
}
