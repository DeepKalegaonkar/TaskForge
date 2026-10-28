CREATE INDEX idx_jobs_status_created_at
    ON jobs(status, created_at);
