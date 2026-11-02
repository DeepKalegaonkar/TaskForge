
package com.taskforge.scheduler;

import com.taskforge.controller.JobController;
import com.taskforge.service.JobService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(JobController.class)
class JobControllerCancellationTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private JobService jobService;

    @Test
    void cancellingMissingJobReturnsNotFound() throws Exception {
        when(jobService.cancelJob("missing-id"))
                .thenReturn(CancellationResult.NOT_FOUND);

        mockMvc.perform(delete("/api/jobs/missing-id"))
                .andExpect(status().isNotFound());
    }

    @Test
    void cancellingQueuedJobReturnsNoContent() throws Exception {
        when(jobService.cancelJob("job-123"))
                .thenReturn(CancellationResult.CANCELLED);

        mockMvc.perform(delete("/api/jobs/job-123"))
                .andExpect(status().isNoContent());
    }

    @Test
    void cancellingRunningJobReturnsAccepted() throws Exception {
        when(jobService.cancelJob("job-123"))
                .thenReturn(CancellationResult.CANCELLATION_REQUESTED);

        mockMvc.perform(delete("/api/jobs/job-123"))
                .andExpect(status().isAccepted());
    }

    @Test
    void cancellingFinishedJobReturnsConflict() throws Exception {
        when(jobService.cancelJob("job-123"))
                .thenReturn(CancellationResult.NOT_CANCELLABLE);

        mockMvc.perform(delete("/api/jobs/job-123"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.status").value(409));
    }
}
