package com.taskforge.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import com.taskforge.dto.JobResponse;
import com.taskforge.dto.SubmitJobRequest;
import com.taskforge.model.JobAttempt;
import com.taskforge.model.JobStatus;
import com.taskforge.scheduler.CancellationResult;
import com.taskforge.service.JobService;
import com.taskforge.service.JobService.SubmitResult;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@RestController
@RequestMapping("/api/jobs")
public class JobController {

    static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";
    static final String REPLAYED_HEADER = "Idempotent-Replayed";
    private static final int MAX_IDEMPOTENCY_KEY_LENGTH = 100;

    private final JobService jobService;

    public JobController(JobService jobService) {
        this.jobService = jobService;
    }

    @Operation(
            summary = "Check API health",
            description = "Checks whether the TaskForge API is responding."
    )
    @ApiResponse(
            responseCode = "200",
            description = "API is running"
    )

    @GetMapping("/health")
    public String health() {
        return "TaskForge API is running";
    }

    @Operation(
            summary = "Submit a job",
            description = """
                    Submits a predefined job for asynchronous execution.
                    Send an Idempotency-Key header to make retries of this
                    request safe: repeating it returns the original job.
                    """
    )
    @ApiResponses({
            @ApiResponse(responseCode = "202", description = "Job accepted"),
            @ApiResponse(responseCode = "200", description = "Repeated request; the original job is returned"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "409", description = "Scheduler is shutting down"),
            @ApiResponse(responseCode = "422", description = "Idempotency key reused with a different request")
    })

    @PostMapping
    public ResponseEntity<JobResponse> submitJob(
            @Parameter(description = "Client-chosen key, at most 100 characters, identifying this submission")
            @RequestHeader(value = IDEMPOTENCY_KEY_HEADER, required = false)
            String idempotencyKey,
            @Valid @RequestBody SubmitJobRequest request) {

        if (idempotencyKey != null
                && (idempotencyKey.isBlank()
                        || idempotencyKey.length() > MAX_IDEMPOTENCY_KEY_LENGTH)) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Idempotency-Key must be between 1 and "
                            + MAX_IDEMPOTENCY_KEY_LENGTH + " characters."
            );
        }

        SubmitResult result = jobService.submitJob(request, idempotencyKey);
        JobResponse body = JobResponse.from(result.job());

        if (result.replayed()) {
            return ResponseEntity.ok()
                    .header(REPLAYED_HEADER, "true")
                    .body(body);
        }
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(body);
    }

    @Operation(
            summary = "Get job by ID",
            description = "Returns the status and details of a specific job."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Job found"),
            @ApiResponse(responseCode = "404", description = "Job not found")
    })

    @GetMapping("/{id}")
    public JobResponse getJob(@PathVariable String id) {
        return jobService.getJobById(id)
                .map(JobResponse::from)
                .orElseThrow(() -> notFound(id));
    }

    @Operation(
            summary = "Get job execution history",
            description = "Returns every execution attempt of a job, oldest first."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Attempts found"),
            @ApiResponse(responseCode = "404", description = "Job not found")
    })

    @GetMapping("/{id}/attempts")
    public List<JobAttempt> getJobAttempts(@PathVariable String id) {
        return jobService.getAttempts(id)
                .orElseThrow(() -> notFound(id));
    }

    @Operation(
            summary = "List jobs",
            description = """
                    Returns jobs stored in the database, newest first.
                    Use status=FAILED to list permanently failed jobs.
                    """
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Jobs retrieved successfully"),
            @ApiResponse(responseCode = "400", description = "Unknown status")
    })

    @GetMapping
    public List<JobResponse> getAllJobs(
            @RequestParam(required = false) JobStatus status) {
        return (status == null
                ? jobService.getAllJobs()
                : jobService.getJobsByStatus(status))
                .stream()
                .map(JobResponse::from)
                .toList();
    }

    @Operation(
            summary = "Retry a permanently failed job",
            description = """
                    Requeues a FAILED job for one more attempt.
                    Its attempt history continues from the last attempt.
                    """
    )
    @ApiResponses({
            @ApiResponse(responseCode = "202", description = "Job requeued"),
            @ApiResponse(responseCode = "404", description = "Job not found"),
            @ApiResponse(responseCode = "409", description = "Job is not FAILED")
    })

    @PostMapping("/{id}/retry")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public JobResponse retryJob(@PathVariable String id) {
        return jobService.retryFailedJob(id)
                .map(JobResponse::from)
                .orElseThrow(() -> {
                    if (jobService.getJobById(id).isEmpty()) {
                        return notFound(id);
                    }
                    return new ResponseStatusException(
                            HttpStatus.CONFLICT,
                            "Only FAILED jobs can be retried"
                    );
                });
    }

    @Operation(
            summary = "Cancel a job",
            description = """
                    Cancels a queued job immediately, including one waiting
                    to retry. A running job is interrupted and becomes
                    CANCELLED shortly afterwards.
                    """
    )
    @ApiResponses({
            @ApiResponse(responseCode = "204", description = "Job cancelled"),
            @ApiResponse(responseCode = "202", description = "Running job is being cancelled"),
            @ApiResponse(responseCode = "404", description = "Job not found"),
            @ApiResponse(responseCode = "409", description = "Job has already finished")
    })

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> cancelJob(@PathVariable String id) {
        CancellationResult result = jobService.cancelJob(id);

        return switch (result) {
            case CANCELLED -> ResponseEntity.noContent().build();
            case CANCELLATION_REQUESTED -> ResponseEntity.accepted().build();
            case NOT_FOUND -> throw notFound(id);
            case NOT_CANCELLABLE -> throw new ResponseStatusException(
                    HttpStatus.CONFLICT,
                    "Job cannot be cancelled because it has already finished"
            );
        };
    }

    private static ResponseStatusException notFound(String id) {
        return new ResponseStatusException(
                HttpStatus.NOT_FOUND,
                "Job not found: " + id
        );
    }
}
