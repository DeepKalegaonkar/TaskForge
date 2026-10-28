package com.taskforge.handler;

import com.taskforge.dto.SubmitJobRequest.JobType;
import org.springframework.stereotype.Component;

@Component
public class JobHandlerFactory {

    public JobHandler createHandler(JobType type) {
        return switch (type) {
            case GENERATE_REPORT -> () -> {
                System.out.println("Generating report...");
                System.out.println("Report generation finished.");
            };

            case SEND_EMAIL -> () -> {
                System.out.println("Sending email...");
                System.out.println("Email sending finished.");
            };

            case FAIL_FOR_TESTING -> () -> {
                System.out.println("Running deliberate failure test...");
                throw new RuntimeException(
                        "Deliberate failure for retry testing"
                );
            };
        };
    }

    public JobHandler createHandler(String type) {
        return createHandler(JobType.valueOf(type));
    }
}
