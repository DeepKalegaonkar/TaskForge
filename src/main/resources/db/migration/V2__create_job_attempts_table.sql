CREATE TABLE job_attempts(
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id VARCHAR(36) NOT NULL REFERENCES jobs(id),
    attempt_number INTEGER NOT NULL,
    status VARCHAR(20) NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    failure_reason TEXT,
    CONSTRAINT uq_job_attempt_number UNIQUE (job_id, attempt_number)
);

CREATE INDEX idx_job_attempts_job_id
    ON job_attempts(job_id)