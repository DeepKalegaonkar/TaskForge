package com.taskforge.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

@Schema(description = "A job to run asynchronously.")
public record SubmitJobRequest(

        @Schema(description = "Human-readable label.", example = "Nightly sales report")
        @NotBlank(message = "Job Name is required.")
        @Size(max = 100, message = "Job name cannot exceed 100 characters.")
        String name,

        @Schema(description = "Which predefined handler runs the job. The *_FOR_TESTING types exist to demonstrate retries, permanent failures, timeouts and cancellation.", example = "GENERATE_REPORT")
        @NotNull(message = "Job Type is required.")
        JobType type,

        // Optional; the configured default is used when omitted.
        @Schema(description = "Total attempts including the first (1-10). Defaults to 3.", example = "3", nullable = true)
        @Min(value = 1, message = "Max attempts must be between 1 and 10.")
        @Max(value = 10, message = "Max attempts must be between 1 and 10.")
        Integer maxAttempts,

        // Optional; the configured default is used when omitted.
        @Schema(description = "Per-attempt timeout in seconds (1-3600). Defaults to 30.", example = "30", nullable = true)
        @Min(value = 1, message = "Timeout must be between 1 and 3600 seconds.")
        @Max(value = 3600, message = "Timeout must be between 1 and 3600 seconds.")
        Integer timeoutSeconds
) {
    public enum JobType {
        GENERATE_REPORT,
        SEND_EMAIL,
        FAIL_FOR_TESTING,
        FAIL_PERMANENTLY_FOR_TESTING,
        SLOW_FOR_TESTING
    }
}
