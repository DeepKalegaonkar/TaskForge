package com.taskforge.config;

import com.taskforge.support.PostgresIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.matchesPattern;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/*
 * Health probes, API docs and request correlation, as a container
 * orchestrator or an operator would use them.
 */
class OperationalEndpointsTest extends PostgresIntegrationTest {

    private static final String UUID_PATTERN =
            "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";

    @Autowired
    private MockMvc mockMvc;

    @Test
    void livenessIsUp() throws Exception {
        mockMvc.perform(get("/actuator/health/liveness"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"));
    }

    // The test context never starts the workers, which is exactly the
    // state of an instance whose startup recovery has not finished.
    @Test
    void readinessIsOutOfServiceUntilWorkersStart() throws Exception {
        mockMvc.perform(get("/actuator/health/readiness"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value("OUT_OF_SERVICE"))
                .andExpect(jsonPath("$.components.db.status").value("UP"))
                .andExpect(jsonPath("$.components.scheduler.status").value("OUT_OF_SERVICE"))
                .andExpect(jsonPath("$.components.scheduler.details.workers").isNumber());
    }

    @Test
    void openApiDocumentDescribesJobEndpoints() throws Exception {
        mockMvc.perform(get("/v3/api-docs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.info.title").value("TaskForge API"))
                .andExpect(jsonPath("$.paths['/api/jobs'].post").exists())
                .andExpect(jsonPath("$.paths['/api/jobs/{id}/attempts'].get").exists())
                .andExpect(jsonPath("$.components.schemas.SubmitJobRequest.properties.type").exists());
    }

    @Test
    void echoesSafeRequestId() throws Exception {
        mockMvc.perform(get("/api/jobs/health").header("X-Request-Id", "demo-42"))
                .andExpect(header().string("X-Request-Id", "demo-42"));
    }

    @Test
    void generatesRequestIdWhenMissing() throws Exception {
        mockMvc.perform(get("/api/jobs/health"))
                .andExpect(header().string("X-Request-Id", matchesPattern(UUID_PATTERN)));
    }

    @Test
    void replacesUnsafeRequestId() throws Exception {
        mockMvc.perform(get("/api/jobs/health").header("X-Request-Id", "bad id\nforged log line"))
                .andExpect(header().string("X-Request-Id", matchesPattern(UUID_PATTERN)));
    }
}
