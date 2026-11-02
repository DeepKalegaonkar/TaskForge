package com.taskforge.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

public record SubmitJobRequest(

        @NotBlank(message = "Job Name is required.")
        @Size(max = 100, message = "Job name cannot exceed 100 characters.")
        String name,

        @NotNull(message = "Job Type is required.")
        JobType type,

        // Optional; the configured default is used when omitted.
        @Min(value = 1, message = "Max attempts must be between 1 and 10.")
        @Max(value = 10, message = "Max attempts must be between 1 and 10.")
        Integer maxAttempts,

        // Optional; the configured default is used when omitted.
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
