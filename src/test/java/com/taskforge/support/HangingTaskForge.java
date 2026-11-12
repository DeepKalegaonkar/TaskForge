package com.taskforge.support;

import com.taskforge.TaskForgeApplication;
import com.taskforge.handler.JobHandlerFactory;
import com.taskforge.support.ScriptedJobHandlers.Step;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/*
 * The real TaskForge application, except that every GENERATE_REPORT job
 * hangs. Tests launch it in a separate JVM and kill it mid-job, which
 * leaves the database exactly as a crashed or OOM-killed process would.
 *
 * Not annotated, so component scanning never picks it up; its @Bean
 * method is registered only when it is passed as a source below.
 */
public class HangingTaskForge {

    public static void main(String[] args) {
        new SpringApplicationBuilder(TaskForgeApplication.class, HangingTaskForge.class)
                .run(args);
    }

    @Bean
    @Primary
    JobHandlerFactory hangingHandlers() {
        ScriptedJobHandlers handlers = new ScriptedJobHandlers();
        handlers.setDefaultStep(Step.HANG);
        return handlers;
    }

    // Starts the application in a child JVM against the test database.
    public static Process launch(PostgreSQLContainer<?> postgres, Path log)
            throws IOException {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();

        List<String> command = List.of(
                java,
                "-cp", System.getProperty("java.class.path"),
                HangingTaskForge.class.getName(),
                "--spring.datasource.url=" + postgres.getJdbcUrl(),
                "--spring.datasource.username=" + postgres.getUsername(),
                "--spring.datasource.password=" + postgres.getPassword(),
                // Overrides the test default that disables workers.
                "--taskforge.scheduler.enabled=true",
                "--taskforge.scheduler.workers=1",
                "--spring.main.web-application-type=none"
        );

        File logFile = log.toFile();
        logFile.getParentFile().mkdirs();
        return new ProcessBuilder(command)
                .redirectErrorStream(true)
                .redirectOutput(logFile)
                .start();
    }
}
