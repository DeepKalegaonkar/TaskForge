-- Per-job retry and timeout settings, retry scheduling,
-- cancellation of running jobs and idempotent submission.
ALTER TABLE jobs
    ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 3,
    ADD COLUMN timeout_seconds INTEGER NOT NULL DEFAULT 30,
    ADD COLUMN available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN idempotency_key VARCHAR(100),
    ADD CONSTRAINT chk_jobs_max_attempts CHECK (max_attempts >= 1),
    ADD CONSTRAINT chk_jobs_timeout_seconds CHECK (timeout_seconds >= 1),
    ADD CONSTRAINT uq_jobs_idempotency_key UNIQUE (idempotency_key);
