package com.taskforge.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;
import java.util.regex.Pattern;

/*
 * Gives every request a correlation id and writes one access log line
 * per request. The id is taken from an incoming X-Request-Id header when
 * it looks safe, echoed back in the response, and put in the MDC so that
 * every log line written while handling the request carries it.
 *
 * Actuator requests are not logged: container health probes would
 * otherwise flood the log every few seconds.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RequestLoggingFilter extends OncePerRequestFilter {

    public static final String REQUEST_ID_HEADER = "X-Request-Id";
    static final String MDC_REQUEST_ID = "requestId";

    private static final Logger log = LoggerFactory.getLogger(RequestLoggingFilter.class);
    private static final Pattern SAFE_REQUEST_ID = Pattern.compile("[A-Za-z0-9._-]{1,64}");

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain
    ) throws ServletException, IOException {
        String requestId = requestIdFor(request);
        long startedAt = System.nanoTime();

        MDC.put(MDC_REQUEST_ID, requestId);
        response.setHeader(REQUEST_ID_HEADER, requestId);

        try {
            chain.doFilter(request, response);
        } finally {
            if (!request.getRequestURI().startsWith("/actuator")) {
                log.info("{} {} -> {} in {} ms",
                        request.getMethod(),
                        request.getRequestURI(),
                        response.getStatus(),
                        (System.nanoTime() - startedAt) / 1_000_000);
            }
            MDC.remove(MDC_REQUEST_ID);
        }
    }

    // Rejects client values that could forge or break log lines.
    private static String requestIdFor(HttpServletRequest request) {
        String incoming = request.getHeader(REQUEST_ID_HEADER);

        return incoming != null && SAFE_REQUEST_ID.matcher(incoming).matches()
                ? incoming
                : UUID.randomUUID().toString();
    }
}
