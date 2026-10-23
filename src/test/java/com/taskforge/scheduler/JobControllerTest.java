
package com.taskforge.scheduler;

import com.taskforge.dto.SubmitJobRequest;
import com.taskforge.model.JobStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
class JobControllerTest {

    @Autowired
    private MockMvc mockMvc;

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
                .andExpect(jsonPath("$.status").value(
                        org.hamcrest.Matchers.oneOf("QUEUED", "RUNNING", "COMPLETED")
                ));
    }
}


