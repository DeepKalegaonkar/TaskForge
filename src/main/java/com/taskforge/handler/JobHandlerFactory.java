package com.taskforge.handler;

import com.taskforge.dto.SubmitJobRequest.JobType;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class JobHandlerFactory {

    static final Duration SLOW_JOB_DURATION = Duration.ofSeconds(15);

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

            case FAIL_PERMANENTLY_FOR_TESTING -> () -> {
                System.out.println("Running deliberate permanent failure test...");
                throw new NonRetryableJobException(
                        "Deliberate permanent failure for testing"
                );
            };

            // Long enough to exercise timeouts, cancellation and shutdown.
            case SLOW_FOR_TESTING -> () -> {
                System.out.println("Running slow job...");
                Thread.sleep(SLOW_JOB_DURATION.toMillis());
                System.out.println("Slow job finished.");
            };
        };
    }

    public JobHandler createHandler(String type) {
        return createHandler(JobType.valueOf(type));
    }
}
