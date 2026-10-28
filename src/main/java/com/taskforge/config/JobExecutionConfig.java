package com.taskforge.config;

import com.taskforge.model.JobStatus;
import com.taskforge.repository.JobAttemptRepository;
import com.taskforge.repository.JobRepository;
import com.taskforge.scheduler.JobScheduler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JobExecutionConfig {

    @Bean(destroyMethod = "shutdown")
    public JobScheduler jobScheduler(
            JobRepository jobRepository,
            JobAttemptRepository jobAttemptRepository
    ) {
        return new JobScheduler(
                2,
                100,
                jobRepository::save,

                // Record the actual attempt number.
                job -> jobAttemptRepository.recordAttemptStarted(
                        job,
                        job.getAttemptCount()
                ),

                // Record each completed or failed attempt.
                job -> {
                    if (job.getStatus() == JobStatus.COMPLETED
                            || job.getStatus() == JobStatus.FAILED) {
                        jobAttemptRepository.recordAttemptFinished(
                                job,
                                job.getAttemptCount()
                        );
                    }
                },

                jobRepository::claimNextQueuedJob,
                jobRepository::cancelQueuedJob,
                jobRepository::requeueFailedJob,

                false
        );
    }
}
