
package com.taskforge.scheduler;

import com.taskforge.controller.JobController;
import com.taskforge.service.JobService;
import com.taskforge.handler.JobHandlerFactory;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(JobController.class)
class JobControllerCancellationTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private JobService jobService;

    @MockitoBean
    private JobHandlerFactory jobHandlerFactory;

    @Test
    void cancellingMissingJobReturnsNotFound() throws Exception {
        when(jobService.cancelJob("missing-id")).thenReturn(false);
        when(jobService.getJobById("missing-id"))
                .thenReturn(java.util.Optional.empty());

        mockMvc.perform(delete("/api/jobs/missing-id"))
                .andExpect(status().isNotFound());
    }


    @Test
    void cancellingQueuedJobReturnsNoContent() throws Exception {
        when(jobService.cancelJob("job-123")).thenReturn(true);

        mockMvc.perform(delete("/api/jobs/job-123"))
                .andExpect(status().isNoContent());
    }

}
