
package com.taskforge.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import com.taskforge.dto.JobResponse;
import com.taskforge.dto.SubmitJobRequest;
import com.taskforge.handler.JobHandlerFactory;
import com.taskforge.model.Job;
import com.taskforge.service.JobService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final JobService jobService;
    private final JobHandlerFactory handlerFactory;

    public JobController(
            JobService jobService,
            JobHandlerFactory handlerFactory) {
        this.jobService = jobService;
        this.handlerFactory = handlerFactory;
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
            description = "Submits a predefined job for asynchronous execution."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "202", description = "Job accepted"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "409", description = "Job queue unavailable or full")
    })

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public JobResponse submitJob(
            @Valid @RequestBody SubmitJobRequest request) {

        Job job = new Job(
                request.name(),
                handlerFactory.createHandler(request.type())
        );

        return JobResponse.from(jobService.submitJob(job));
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
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND,
                        "Job not found: " + id
                ));
    }

    @Operation(
            summary = "List all jobs",
            description = "Returns all jobs currently stored in memory."
    )
    @ApiResponse(
            responseCode = "200",
            description = "Jobs retrieved successfully"
    )

    @GetMapping
    public List<JobResponse> getAllJobs() {
        return jobService.getAllJobs()
                .stream()
                .map(JobResponse::from)
                .toList();
    }

    @Operation(
            summary = "Cancel a queued job",
            description = "Cancels a job only if it has not started executing."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "204", description = "Job cancelled"),
            @ApiResponse(responseCode = "404", description = "Job not found"),
            @ApiResponse(responseCode = "409", description = "Job cannot be cancelled")
    })

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void cancelJob(@PathVariable String id) {
        boolean cancelled = jobService.cancelJob(id);

        if (!cancelled) {
            if (jobService.getJobById(id).isEmpty()) {
                throw new ResponseStatusException(
                        HttpStatus.NOT_FOUND,
                        "Job not found: " + id
                );
            }

            throw new ResponseStatusException(
                    HttpStatus.CONFLICT,
                    "Job cannot be cancelled because it may already be running or finished"
            );
        }
    }

}
