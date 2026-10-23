package com.taskforge.handler;

import com.taskforge.dto.SubmitJobRequest.JobType;
import com.taskforge.handler.JobHandler;
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
        };
    }
}
