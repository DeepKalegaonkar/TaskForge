package com.taskforge.handler;

import com.taskforge.dto.SubmitJobRequest.JobType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class JobHandlerFactory {

    private static final Logger log = LoggerFactory.getLogger(JobHandlerFactory.class);

    static final Duration SLOW_JOB_DURATION = Duration.ofSeconds(15);

    public JobHandler createHandler(JobType type) {
        return switch (type) {
            case GENERATE_REPORT -> () -> {
                log.info("Generating report...");
                log.info("Report generation finished.");
            };

            case SEND_EMAIL -> () -> {
                log.info("Sending email...");
                log.info("Email sending finished.");
            };

            case FAIL_FOR_TESTING -> () -> {
                log.info("Running deliberate failure test...");
                throw new RuntimeException(
                        "Deliberate failure for retry testing"
                );
            };

            case FAIL_PERMANENTLY_FOR_TESTING -> () -> {
                log.info("Running deliberate permanent failure test...");
                throw new NonRetryableJobException(
                        "Deliberate permanent failure for testing"
                );
            };

            // Long enough to exercise timeouts, cancellation and shutdown.
            case SLOW_FOR_TESTING -> () -> {
                log.info("Running slow job...");
                Thread.sleep(SLOW_JOB_DURATION.toMillis());
                log.info("Slow job finished.");
            };
        };
    }

    public JobHandler createHandler(String type) {
        return createHandler(JobType.valueOf(type));
    }
}
