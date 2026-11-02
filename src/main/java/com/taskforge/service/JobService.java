package com.taskforge.service;

import com.taskforge.dto.SubmitJobRequest;
import com.taskforge.exception.IdempotencyKeyConflictException;
import com.taskforge.handler.JobHandlerFactory;
import com.taskforge.model.Job;
import com.taskforge.model.JobAttempt;
import com.taskforge.model.JobStatus;
import com.taskforge.repository.JobAttemptRepository;
import com.taskforge.repository.JobRepository;
import com.taskforge.scheduler.CancellationResult;
import com.taskforge.scheduler.JobScheduler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.RejectedExecutionException;

@Service
public class JobService {

    // replayed is true when an earlier request with the same
    // Idempotency-Key already created the job.
    public record SubmitResult(Job job, boolean replayed) {
    }

    private final JobRepository jobRepository;
    private final JobAttemptRepository jobAttemptRepository;
    private final JobScheduler jobScheduler;
    private final JobHandlerFactory handlerFactory;
    private final int defaultMaxAttempts;
    private final Duration defaultTimeout;

    public JobService(
            JobRepository jobRepository,
            JobAttemptRepository jobAttemptRepository,
            JobScheduler jobScheduler,
            JobHandlerFactory handlerFactory,
            @Value("${taskforge.jobs.default-max-attempts:3}") int defaultMaxAttempts,
            @Value("${taskforge.jobs.default-timeout:30s}") Duration defaultTimeout){
        this.jobRepository = jobRepository;
        this.jobAttemptRepository = jobAttemptRepository;
        this.jobScheduler = jobScheduler;
        this.handlerFactory = handlerFactory;
        this.defaultMaxAttempts = defaultMaxAttempts;
        this.defaultTimeout = defaultTimeout;
    }

    public SubmitResult submitJob(SubmitJobRequest request, String idempotencyKey){
        if(request == null){
            throw new IllegalArgumentException("Request cannot be null");
        }

        Job job = Job.builder(
                        request.name(),
                        handlerFactory.createHandler(request.type())
                )
                .type(request.type())
                .maxAttempts(maxAttemptsFor(request))
                .timeout(timeoutFor(request))
                .idempotencyKey(idempotencyKey)
                .build();

        // The unique key makes concurrent duplicates insert at most once.
        if (!jobRepository.insert(job)) {
            return new SubmitResult(existingJobFor(idempotencyKey, request), true);
        }

        try{
            jobScheduler.submit(job);
        } catch(RejectedExecutionException e){
            jobRepository.deleteById(job.getId());
            throw e;
        }
        return new SubmitResult(job, false);
    }

    private Job existingJobFor(String idempotencyKey, SubmitJobRequest request) {
        Job existing = jobRepository.findByIdempotencyKey(idempotencyKey)
                .orElseThrow(() -> new IllegalStateException(
                        "Job for idempotency key " + idempotencyKey
                                + " disappeared"
                ));

        boolean sameRequest = existing.getName().equals(request.name())
                && existing.getType() == request.type()
                && existing.getMaxAttempts() == maxAttemptsFor(request)
                && Objects.equals(existing.getTimeout(), timeoutFor(request));

        if (!sameRequest) {
            throw new IdempotencyKeyConflictException(idempotencyKey);
        }
        return existing;
    }

    private int maxAttemptsFor(SubmitJobRequest request) {
        return request.maxAttempts() != null
                ? request.maxAttempts()
                : defaultMaxAttempts;
    }

    private Duration timeoutFor(SubmitJobRequest request) {
        return request.timeoutSeconds() != null
                ? Duration.ofSeconds(request.timeoutSeconds())
                : defaultTimeout;
    }

    public Optional<Job> getJobById(String id){
        return jobRepository.findById(id);
    }

    public List<Job> getAllJobs(){
        return jobRepository.findAll();
    }

    public List<Job> getJobsByStatus(JobStatus status){
        return jobRepository.findByStatus(status);
    }

    public Optional<List<JobAttempt>> getAttempts(String id){
        if (!jobRepository.existsById(id)) {
            return Optional.empty();
        }

        return Optional.of(jobAttemptRepository.findByJobId(id));
    }

    public CancellationResult cancelJob(String id) {
        return jobScheduler.cancel(id);
    }

    /*
     * Gives a permanently failed job one more attempt.
     * Empty when the job does not exist or is not FAILED.
     */
    public Optional<Job> retryFailedJob(String id) {
        if (!jobRepository.retryFailedJob(id)) {
            return Optional.empty();
        }
        return jobRepository.findById(id);
    }
}
