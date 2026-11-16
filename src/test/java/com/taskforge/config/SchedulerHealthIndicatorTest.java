package com.taskforge.config;

import com.taskforge.scheduler.JobScheduler;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class SchedulerHealthIndicatorTest {

    @Test
    void upWhileWorkersRunThenOutOfServiceDuringShutdown() throws Exception {
        // The in-memory constructor starts its workers immediately.
        JobScheduler scheduler = new JobScheduler(3, 10);
        SchedulerHealthIndicator indicator = new SchedulerHealthIndicator(scheduler);

        Health running = indicator.health();
        assertThat(running.getStatus()).isEqualTo(Status.UP);
        assertThat(running.getDetails())
                .containsEntry("workers", 3)
                .containsEntry("runningJobs", 0);

        scheduler.shutdown();
        try {
            Health stopping = indicator.health();
            assertThat(stopping.getStatus()).isEqualTo(Status.OUT_OF_SERVICE);
            assertThat(stopping.getDetails()).containsEntry("reason", "Shutting down");
        } finally {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
