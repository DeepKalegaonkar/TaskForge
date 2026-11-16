package com.taskforge;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@OpenAPIDefinition(
        info = @Info(
                title = "TaskForge API",
                version = "1.0.0",
                description = """
                        TaskForge is a PostgreSQL-backed background job scheduler.
                        Submit a job and it is executed asynchronously by a pool
                        of workers, with per-job timeouts, retries with
                        exponential backoff, cancellation of queued and running
                        jobs, crash recovery on startup and idempotent
                        submission via the `Idempotency-Key` header.

                        Execution is at least once: a job can run again if its
                        process dies after the handler finished but before the
                        result was committed.

                        Health probes: `/actuator/health/liveness` and
                        `/actuator/health/readiness`.
                        """
        )
)

@SpringBootApplication
public class TaskForgeApplication {
    public static void main(String[] args){
        SpringApplication.run(TaskForgeApplication.class, args);
    }

}