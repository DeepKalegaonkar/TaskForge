
package com.taskforge.scheduler;

import com.taskforge.dto.SubmitJobRequest;
import com.taskforge.model.JobStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import com.taskforge.support.PostgresIntegrationTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class JobControllerTest extends PostgresIntegrationTest {

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

    @Test
    void repeatingASubmissionWithTheSameIdempotencyKeyReturnsTheOriginalJob()
            throws Exception {
        String key = java.util.UUID.randomUUID().toString();
        String request = """
                {
                    "name": "Test Report",
                    "type": "GENERATE_REPORT"
                }
                """;

        String first = mockMvc.perform(post("/api/jobs")
                        .header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request))
                .andExpect(status().isAccepted())
                .andExpect(header().doesNotExist("Idempotent-Replayed"))
                .andExpect(jsonPath("$.idempotencyKey").value(key))
                .andReturn().getResponse().getContentAsString();

        String id = com.jayway.jsonpath.JsonPath.read(first, "$.id");

        mockMvc.perform(post("/api/jobs")
                        .header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request))
                .andExpect(status().isOk())
                .andExpect(header().string("Idempotent-Replayed", "true"))
                .andExpect(jsonPath("$.id").value(id));

        Integer jobsWithKey = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM jobs WHERE idempotency_key = ?",
                Integer.class, key
        );
        org.junit.jupiter.api.Assertions.assertEquals(1, jobsWithKey);
    }

    @Test
    void reusingAnIdempotencyKeyForADifferentRequestIsRejected()
            throws Exception {
        String key = java.util.UUID.randomUUID().toString();

        mockMvc.perform(post("/api/jobs")
                        .header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            { "name": "Test Report", "type": "GENERATE_REPORT" }
                            """))
                .andExpect(status().isAccepted());

        mockMvc.perform(post("/api/jobs")
                        .header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            { "name": "Test Report", "type": "SEND_EMAIL" }
                            """))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.status").value(422));
    }

    @Test
    void blankIdempotencyKeyIsRejected() throws Exception {
        mockMvc.perform(post("/api/jobs")
                        .header("Idempotency-Key", " ")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            { "name": "Test Report", "type": "GENERATE_REPORT" }
                            """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400));
    }

    @Test
    void retryAndTimeoutSettingsAreAppliedAndValidated() throws Exception {
        mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            {
                                "name": "Test Report",
                                "type": "GENERATE_REPORT",
                                "maxAttempts": 5,
                                "timeoutSeconds": 120
                            }
                            """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.maxAttempts").value(5))
                .andExpect(jsonPath("$.timeoutSeconds").value(120));

        mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            {
                                "name": "Test Report",
                                "type": "GENERATE_REPORT",
                                "maxAttempts": 0
                            }
                            """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message")
                        .value("Max attempts must be between 1 and 10."));
    }

    @Test
    void defaultsApplyWhenSettingsAreOmitted() throws Exception {
        mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            { "name": "Test Report", "type": "GENERATE_REPORT" }
                            """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.maxAttempts").value(3))
                .andExpect(jsonPath("$.timeoutSeconds").value(30));
    }

    @Test
    void jobsCanBeFilteredByStatus() throws Exception {
        mockMvc.perform(get("/api/jobs").param("status", "FAILED"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[*].status").value(
                        org.hamcrest.Matchers.everyItem(
                                org.hamcrest.Matchers.is("FAILED"))
                ));

        mockMvc.perform(get("/api/jobs").param("status", "NOT_A_STATUS"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message")
                        .value("Invalid value for parameter 'status'."));
    }

    @Test
    void onlyFailedJobsCanBeRetried() throws Exception {
        String body = mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            { "name": "Test Report", "type": "GENERATE_REPORT" }
                            """))
                .andReturn().getResponse().getContentAsString();
        String id = com.jayway.jsonpath.JsonPath.read(body, "$.id");

        mockMvc.perform(post("/api/jobs/" + id + "/retry"))
                .andExpect(status().isConflict());

        mockMvc.perform(post("/api/jobs/nonexistent-job-id/retry"))
                .andExpect(status().isNotFound());
    }

    @Test
    void queuedJobIsCancelledOnlyOnce() throws Exception {
        String body = mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            { "name": "Test Report", "type": "GENERATE_REPORT" }
                            """))
                .andReturn().getResponse().getContentAsString();
        String id = com.jayway.jsonpath.JsonPath.read(body, "$.id");

        mockMvc.perform(delete("/api/jobs/" + id))
                .andExpect(status().isNoContent());

        mockMvc.perform(get("/api/jobs/" + id))
                .andExpect(jsonPath("$.status").value("CANCELLED"));

        mockMvc.perform(delete("/api/jobs/" + id))
                .andExpect(status().isConflict());
    }
}
