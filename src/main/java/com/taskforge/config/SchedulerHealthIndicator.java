package com.taskforge.config;

import com.taskforge.scheduler.JobScheduler;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/*
 * Reports whether the worker pool is processing jobs. It is part of the
 * readiness group (see application.properties), so an instance that has
 * not finished startup recovery, or is draining for shutdown, is reported
 * as not ready while its HTTP API may still answer.
 */
@Component("scheduler")
public class SchedulerHealthIndicator implements HealthIndicator {

    private final JobScheduler jobScheduler;

    public SchedulerHealthIndicator(JobScheduler jobScheduler) {
        this.jobScheduler = jobScheduler;
    }

    @Override
    public Health health() {
        Health.Builder builder;

        if (jobScheduler.isRunning()) {
            builder = Health.up();
        } else if (jobScheduler.isShuttingDown()) {
            builder = Health.outOfService().withDetail("reason", "Shutting down");
        } else {
            builder = Health.outOfService()
                    .withDetail("reason", "Workers not started (startup recovery pending or scheduler disabled)");
        }

        return builder
                .withDetail("workers", jobScheduler.getWorkerCount())
                .withDetail("runningJobs", jobScheduler.getRunningJobCount())
                .build();
    }
}
