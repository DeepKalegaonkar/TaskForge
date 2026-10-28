package com.taskforge.service;

import com.taskforge.model.Job;
import com.taskforge.repository.JobAttemptRepository;
import com.taskforge.repository.JobRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

import static com.taskforge.scheduler.JobScheduler.MAX_ATTEMPTS;

@Service
public class JobRecoveryService {

    static final String RECOVERY_FAILURE_REASON =
            "Execution interrupted by application shutdown or crash";

    private final JobRepository jobRepository;
    private final JobAttemptRepository jobAttemptRepository;

    public JobRecoveryService(
            JobRepository jobRepository,
            JobAttemptRepository jobAttemptRepository
    ) {
        this.jobRepository = jobRepository;
        this.jobAttemptRepository = jobAttemptRepository;
    }

    @Transactional
    public void recoverJobs() {
        for (Job job : jobRepository.findAllRunningJobs()) {
            recoverRunningJob(job);
        }

        for (Job job : jobRepository.findFailedJobsAwaitingRetry(MAX_ATTEMPTS)) {
            recoverPendingRetry(job);
        }

        System.out.println("Startup job recovery completed.");
    }

    /*
     * The application stopped while the handler was executing,
     * or between claiming the job and recording its attempt.
     */
    private void recoverRunningJob(Job job) {
        int attemptNumber = job.getAttemptCount();

        if (attemptNumber < 1 || attemptNumber > MAX_ATTEMPTS) {
            throw new IllegalStateException(
                    "Invalid attempt count " + attemptNumber
                            + " for interrupted job " + job.getId()
            );
        }

        if (!jobRepository.recoverInterruptedJob(
                job.getId(),
                RECOVERY_FAILURE_REASON
        )) {
            return;
        }

        // Cover the crash window between claiming the job
        // and inserting its attempt-history record.
        if (!jobAttemptRepository.existsByJobIdAndAttemptNumber(
                job.getId(),
                attemptNumber
        )) {
            jobAttemptRepository.recordAttemptStarted(job, attemptNumber);
        }

        jobAttemptRepository.failInterruptedAttempt(
                job.getId(),
                attemptNumber,
                RECOVERY_FAILURE_REASON
        );

        job.markFailed(RECOVERY_FAILURE_REASON);

        if (attemptNumber < MAX_ATTEMPTS) {
            requeue(job, attemptNumber);
        } else {
            System.out.println(
                    "Recovered job " + job.getId()
                            + "; attempt limit reached. "
                            + "Job remains FAILED."
            );
        }
    }

    /*
     * The handler failed and the job was saved as FAILED, but the
     * application stopped before the retry delay ended. The attempt
     * row may still be RUNNING if it stopped before recording the result.
     */
    private void recoverPendingRetry(Job job) {
        int attemptNumber = job.getAttemptCount();

        jobAttemptRepository.failInterruptedAttempt(
                job.getId(),
                attemptNumber,
                job.getFailureReason()
        );

        requeue(job, attemptNumber);
    }

    private void requeue(Job job, int attemptNumber) {
        job.requeueForRetry();

        if (!jobRepository.requeueFailedJob(job)) {
            throw new IllegalStateException(
                    "Could not requeue interrupted job "
                            + job.getId()
                            + " after attempt " + attemptNumber
            );
        }

        System.out.println(
                "Recovered job " + job.getId()
                        + "; attempt " + attemptNumber
                        + " failed and the job was requeued."
        );
    }
}
