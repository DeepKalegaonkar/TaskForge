package com.taskforge.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.service.DatabaseJobStore;
import com.taskforge.support.Await;
import com.taskforge.support.PostgresIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/*
 * Clients that retry a POST (after a timeout, a dropped connection...)
 * send the same Idempotency-Key again. These tests go through the HTTP
 * layer with workers running, and check the job is created and executed
 * once however the duplicates arrive.
 */
class DuplicateRequestIntegrationTest extends PostgresIntegrationTest {

    private static final String BODY = """
            {"name": "duplicate-test", "type": "GENERATE_REPORT"}
            """;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private DatabaseJobStore jobStore;

    private final ObjectMapper json = new ObjectMapper();

    @BeforeEach
    void emptyDatabase() {
        deleteAllJobs();
    }

    @Test
    void concurrentDuplicatePostsShouldCreateAndRunOneJob() throws Exception {
        int duplicates = 16;
        String key = UUID.randomUUID().toString();
        startScheduler(2, jobStore);

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService clients = Executors.newFixedThreadPool(duplicates);
        List<MockHttpServletResponse> responses = new ArrayList<>();
        try {
            List<Future<MockHttpServletResponse>> futures = new ArrayList<>();
            for (int i = 0; i < duplicates; i++) {
                futures.add(clients.submit(() -> {
                    start.await();
                    return post(key);
                }));
            }
            start.countDown();
            for (Future<MockHttpServletResponse> f : futures) {
                responses.add(f.get(30, TimeUnit.SECONDS));
            }
        } finally {
            clients.shutdownNow();
        }

        long created = responses.stream().filter(r -> r.getStatus() == 202).count();
        long replayed = responses.stream()
                .filter(r -> r.getStatus() == 200
                        && "true".equals(r.getHeader("Idempotent-Replayed")))
                .count();
        Set<String> ids = responses.stream().map(this::jobId).collect(Collectors.toSet());

        assertEquals(1, created, "Exactly one request creates the job");
        assertEquals(duplicates - 1, replayed, "Every other request is a replay");
        assertEquals(1, ids.size(), "All responses describe the same job");

        String id = ids.iterator().next();
        Await.until(Duration.ofSeconds(10), () -> "COMPLETED".equals(statusOf(id)),
                "job should complete");
        Thread.sleep(500);
        assertEquals(1, handlers.executions(), "The job must run once");
        assertEquals(1, jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM jobs", Integer.class));
    }

    @Test
    void duplicateArrivingAfterTheJobFinishedShouldReturnItWithoutRunningItAgain()
            throws Exception {
        String key = UUID.randomUUID().toString();
        startScheduler(1, jobStore);

        MockHttpServletResponse first = post(key);
        assertEquals(202, first.getStatus());
        String id = jobId(first);
        Await.until(Duration.ofSeconds(10), () -> "COMPLETED".equals(statusOf(id)),
                "job should complete");

        MockHttpServletResponse late = post(key);

        assertEquals(200, late.getStatus());
        assertEquals("true", late.getHeader("Idempotent-Replayed"));
        assertEquals(id, jobId(late));
        assertEquals("COMPLETED", json.readTree(late.getContentAsString())
                .get("status").asText());
        Thread.sleep(500);
        assertEquals(1, handlers.executions());
    }

    @Test
    void submissionsWithoutAKeyAreIndependent() throws Exception {
        assertNotEquals(jobId(post(null)), jobId(post(null)));
    }

    private MockHttpServletResponse post(String key) throws Exception {
        MockHttpServletRequestBuilder request = MockMvcRequestBuilders.post("/api/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .content(BODY);
        if (key != null) {
            request.header("Idempotency-Key", key);
        }
        return mockMvc.perform(request).andReturn().getResponse();
    }

    private String jobId(MockHttpServletResponse response) {
        try {
            JsonNode body = json.readTree(response.getContentAsString());
            return body.get("id").asText();
        } catch (Exception e) {
            throw new AssertionError("Unexpected response " + response.getStatus(), e);
        }
    }
}
