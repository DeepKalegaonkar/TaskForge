package com.taskforge.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

public record SubmitJobRequest(

        @NotBlank(message = "Job Name is required.")
        @Size(max = 100, message = "Job name cannot exceed 100 characters.")
        String name,

        @NotNull(message = "Job Type is required.")
        JobType type
) {
    public enum JobType {
        GENERATE_REPORT,
        SEND_EMAIL,
        FAIL_FOR_TESTING
    }
}
