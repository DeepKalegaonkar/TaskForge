package com.taskforge.config;

import com.taskforge.scheduler.JobScheduler;
import com.taskforge.scheduler.JobStore;
import com.taskforge.scheduler.RetryPolicy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
public class JobExecutionConfig {

    @Bean
    public RetryPolicy retryPolicy(
            @Value("${taskforge.retry.initial-delay:2s}") Duration initialDelay,
            @Value("${taskforge.retry.multiplier:2.0}") double multiplier,
            @Value("${taskforge.retry.max-delay:5m}") Duration maxDelay
    ) {
        return new RetryPolicy(initialDelay, multiplier, maxDelay);
    }

    // Workers start in SchedulerStartup, after recovery. On shutdown,
    // close() lets running jobs finish within the grace period.
    @Bean(destroyMethod = "close")
    public JobScheduler jobScheduler(
            JobStore jobStore,
            RetryPolicy retryPolicy,
            @Value("${taskforge.scheduler.workers:2}") int workers,
            @Value("${taskforge.scheduler.shutdown-grace-period:20s}")
            Duration shutdownGracePeriod
    ) {
        return new JobScheduler(
                workers,
                jobStore,
                retryPolicy,
                shutdownGracePeriod,
                false
        );
    }
}
