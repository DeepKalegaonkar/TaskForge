
package com.taskforge.repository;

import com.taskforge.model.Job;
import com.taskforge.model.JobAttempt;
import com.taskforge.model.JobStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

@Repository
public class JobAttemptRepository {

    private final JdbcTemplate jdbcTemplate;

    public JobAttemptRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void recordAttemptStarted(Job job, int attemptNumber) {
        if (attemptNumber < 1) {
            throw new IllegalArgumentException(
                    "Attempt number must be at least 1"
            );
        }

        jdbcTemplate.update(
                """
                INSERT INTO job_attempts (
                    job_id,
                    attempt_number,
                    status,
                    started_at
                )
                VALUES (?, ?, ?, ?)
                """,
                job.getId(),
                attemptNumber,
                JobStatus.RUNNING.name(),
                job.getStartedAt() == null
                        ? Timestamp.from(Instant.now())
                        : Timestamp.from(job.getStartedAt())
        );
    }

    public void recordAttemptFinished(Job job, int attemptNumber) {
        int updated = jdbcTemplate.update(
                """
                UPDATE job_attempts
                SET status = ?,
                    completed_at = ?,
                    failure_reason = ?
                WHERE job_id = ?
                  AND attempt_number = ?
                """,
                job.getStatus().name(),
                job.getCompletedAt() == null
                        ? Timestamp.from(Instant.now())
                        : Timestamp.from(job.getCompletedAt()),
                job.getFailureReason(),
                job.getId(),
                attemptNumber
        );

        if (updated != 1) {
            throw new IllegalStateException(
                    "Could not find execution attempt "
                            + attemptNumber + " for job " + job.getId()
            );
        }
    }

    /**
     * Returns whether a particular attempt exists for a job.
     */
    public boolean existsByJobIdAndAttemptNumber(
            String jobId,
            int attemptNumber
    ) {
        Boolean exists = jdbcTemplate.queryForObject(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM job_attempts
                    WHERE job_id = ?
                      AND attempt_number = ?
                )
                """,
                Boolean.class,
                jobId,
                attemptNumber
        );

        return Boolean.TRUE.equals(exists);
    }

    /**
     * Finishes an interrupted attempt only if it is still RUNNING.
     */
    public boolean failInterruptedAttempt(
            String jobId,
            int attemptNumber,
            String failureReason
    ) {
        int updated = jdbcTemplate.update(
                """
                UPDATE job_attempts
                SET status = ?,
                    completed_at = NOW(),
                    failure_reason = ?
                WHERE job_id = ?
                  AND attempt_number = ?
                  AND status = ?
                """,
                JobStatus.FAILED.name(),
                failureReason,
                jobId,
                attemptNumber,
                JobStatus.RUNNING.name()
        );

        return updated == 1;
    }

    public List<JobAttempt> findByJobId(String jobId) {
        return jdbcTemplate.query(
                """
                SELECT *
                FROM job_attempts
                WHERE job_id = ?
                ORDER BY attempt_number
                """,
                (rs, rowNum) -> {
                    Timestamp startedAt = rs.getTimestamp("started_at");
                    Timestamp completedAt = rs.getTimestamp("completed_at");

                    return new JobAttempt(
                            rs.getString("job_id"),
                            rs.getInt("attempt_number"),
                            JobStatus.valueOf(rs.getString("status")),
                            startedAt != null ? startedAt.toInstant() : null,
                            completedAt != null ? completedAt.toInstant() : null,
                            rs.getString("failure_reason")
                    );
                },
                jobId
        );
    }

    public boolean existsByJobId(String jobId) {
        Boolean exists = jdbcTemplate.queryForObject(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM job_attempts
                    WHERE job_id = ?
                )
                """,
                Boolean.class,
                jobId
        );

        return Boolean.TRUE.equals(exists);
    }
}
