package com.taskforge.config;

import com.taskforge.scheduler.JobScheduler;
import com.taskforge.service.JobRecoveryService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

// Tests that drive the repositories directly disable this so that
// background workers do not claim their rows.
@Component
@ConditionalOnProperty(
        name = "taskforge.scheduler.enabled",
        havingValue = "true",
        matchIfMissing = true
)
public class SchedulerStartup {

    private final JobRecoveryService jobRecoveryService;
    private final JobScheduler jobScheduler;

    public SchedulerStartup(
            JobRecoveryService jobRecoveryService,
            JobScheduler jobScheduler
    ) {
        this.jobRecoveryService = jobRecoveryService;
        this.jobScheduler = jobScheduler;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void startSchedulerAfterRecovery() {
        // This call returns only after the recovery transaction commits.
        jobRecoveryService.recoverJobs();

        // Workers start only after recovery is complete.
        jobScheduler.start();

        System.out.println(
                "TaskForge scheduler started after recovery."
        );
    }
}
