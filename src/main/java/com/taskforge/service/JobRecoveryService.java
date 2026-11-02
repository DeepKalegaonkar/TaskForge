package com.taskforge.service;

import com.taskforge.model.Job;
import com.taskforge.model.JobStatus;
import com.taskforge.repository.JobAttemptRepository;
import com.taskforge.repository.JobRepository;
import com.taskforge.scheduler.AttemptResult;
import com.taskforge.scheduler.JobStore;
import com.taskforge.scheduler.RetryPolicy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.Optional;

/*
 * Runs before the workers start. Any job still RUNNING was interrupted
 * by a crash or kill, so its attempt is recorded as failed and the job
 * is retried with the normal backoff if it has attempts left. The
 * backoff stops a job that crashes the process from restarting at once.
 */
@Service
public class JobRecoveryService {

    static final String RECOVERY_FAILURE_REASON =
            "Execution interrupted by application shutdown or crash";

    private final JobRepository jobRepository;
    private final JobAttemptRepository jobAttemptRepository;
    private final JobStore jobStore;
    private final RetryPolicy retryPolicy;

    public JobRecoveryService(
            JobRepository jobRepository,
            JobAttemptRepository jobAttemptRepository,
            JobStore jobStore,
            RetryPolicy retryPolicy
    ) {
        this.jobRepository = jobRepository;
        this.jobAttemptRepository = jobAttemptRepository;
        this.jobStore = jobStore;
        this.retryPolicy = retryPolicy;
    }

    @Transactional
    public void recoverJobs() {
        for (Job job : jobRepository.findAllRunningJobs()) {
            recoverRunningJob(job);
        }

        System.out.println("Startup job recovery completed.");
    }

    private void recoverRunningJob(Job job) {
        int attemptNumber = job.getAttemptCount();

        if (attemptNumber < 1) {
            throw new IllegalStateException(
                    "Invalid attempt count " + attemptNumber
                            + " for interrupted job " + job.getId()
            );
        }

        // Jobs claimed before attempt rows were written in the
        // claim transaction may have no history for this attempt.
        if (!jobAttemptRepository.existsByJobIdAndAttemptNumber(
                job.getId(),
                attemptNumber
        )) {
            jobAttemptRepository.recordAttemptStarted(job, attemptNumber);
        }

        Duration retryDelay = job.hasAttemptsRemaining()
                ? retryPolicy.delayAfterAttempt(attemptNumber)
                : null;

        job.markFailed(RECOVERY_FAILURE_REASON);

        Optional<JobStatus> recoveredStatus = jobStore.finishAttempt(
                job,
                AttemptResult.failed(RECOVERY_FAILURE_REASON, retryDelay)
        );

        recoveredStatus.ifPresent(status -> System.out.println(
                "Recovered job " + job.getId()
                        + "; attempt " + attemptNumber + " of "
                        + job.getMaxAttempts() + " failed. Job is now "
                        + status + "."
        ));
    }
}
