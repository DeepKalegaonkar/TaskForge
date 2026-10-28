package com.taskforge;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@OpenAPIDefinition(
        info = @Info(
                title = "TaskForge API",
                version = "1.0.0",
                description =  """
                        TaskForge is a Java-based background job scheduling
                        system. It supports submitting jobs, monitoring their
                        execution status, and cancelling jobs that are still
                        queued.
                        """
        )
)

@SpringBootApplication
public class TaskForgeApplication {
    public static void main(String[] args){
        SpringApplication.run(TaskForgeApplication.class, args);
    }

}