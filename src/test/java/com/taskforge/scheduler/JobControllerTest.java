
package com.taskforge.scheduler;

import com.taskforge.dto.SubmitJobRequest;
import com.taskforge.model.JobStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
class JobControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    // Workers are disabled in tests, so submitted jobs would stay QUEUED.
    @AfterEach
    void removeSubmittedJobs() {
        jdbcTemplate.update(
                "DELETE FROM job_attempts WHERE job_id IN "
                        + "(SELECT id FROM jobs WHERE name = 'Test Report')"
        );
        jdbcTemplate.update("DELETE FROM jobs WHERE name = 'Test Report'");
    }

    @Test
    void healthEndpointReturnsOk() throws Exception {
        mockMvc.perform(get("/api/jobs/health"))
                .andExpect(status().isOk())
                .andExpect(content().string("TaskForge API is running"));
    }

    @Test
    void submittingInvalidJobReturnsBadRequest() throws Exception {
        mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "name": "",
                                    "type": "GENERATE_REPORT"
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.message").value("Job Name is required."));
    }

    @Test
    void requestingMissingJobReturnsNotFound() throws Exception {
        mockMvc.perform(get("/api/jobs/nonexistent-job-id"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404))
                .andExpect(jsonPath("$.path")
                        .value("/api/jobs/nonexistent-job-id"));
    }

    @Test
    void submittingValidJobReturnsAccepted() throws Exception {
        mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            {
                                "name": "Test Report",
                                "type": "GENERATE_REPORT"
                            }
                            """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.id").exists())
                .andExpect(jsonPath("$.name").value("Test Report"))
                .andExpect(jsonPath("$.type").value("GENERATE_REPORT"))
                .andExpect(jsonPath("$.attemptCount").value(0))
                .andExpect(jsonPath("$.status").value(
                        org.hamcrest.Matchers.oneOf("QUEUED", "RUNNING", "COMPLETED")
                ));
    }

    @Test
    void submittingUnknownJobTypeReturnsApiError() throws Exception {
        mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            {
                                "name": "Test Report",
                                "type": "NOT_A_TYPE"
                            }
                            """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.message")
                        .value("Request body is missing or malformed."))
                .andExpect(jsonPath("$.path").value("/api/jobs"));
    }

    @Test
    void submittingMalformedJsonReturnsApiError() throws Exception {
        mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{ not json"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400));
    }

    @Test
    void unsupportedMethodKeepsItsStatus() throws Exception {
        mockMvc.perform(put("/api/jobs/some-id"))
                .andExpect(status().isMethodNotAllowed())
                .andExpect(jsonPath("$.status").value(405));
    }

    @Test
    void attemptsOfMissingJobReturnNotFound() throws Exception {
        mockMvc.perform(get("/api/jobs/nonexistent-job-id/attempts"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404));
    }

    @Test
    void attemptsOfNewJobAreEmpty() throws Exception {
        String body = mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            {
                                "name": "Test Report",
                                "type": "GENERATE_REPORT"
                            }
                            """))
                .andExpect(status().isAccepted())
                .andReturn().getResponse().getContentAsString();

        String id = com.jayway.jsonpath.JsonPath.read(body, "$.id");

        mockMvc.perform(get("/api/jobs/" + id + "/attempts"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isEmpty());
    }
}


