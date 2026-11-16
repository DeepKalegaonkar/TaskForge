# syntax=docker/dockerfile:1

# ---- Build stage ---------------------------------------------------------
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build

# Resolve dependencies in their own layer so that source-only changes
# rebuild quickly. The cache mount keeps ~/.m2 between builds.
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2 mvn -B -q dependency:go-offline

COPY src ./src
# Tests are not run here: the integration tests start PostgreSQL with
# Testcontainers, which needs a Docker daemon. CI runs them as a separate
# job before building this image.
RUN --mount=type=cache,target=/root/.m2 mvn -B -q package -DskipTests \
 && java -Djarmode=tools -jar target/TaskForge-*.jar extract --layers --launcher --destination extracted

# ---- Runtime stage -------------------------------------------------------
FROM eclipse-temurin:21-jre-alpine

RUN addgroup -S taskforge && adduser -S taskforge -G taskforge
WORKDIR /app

# Least- to most-frequently changing, so a code change only replaces the
# small application layer.
COPY --from=build /build/extracted/dependencies/ ./
COPY --from=build /build/extracted/spring-boot-loader/ ./
COPY --from=build /build/extracted/snapshot-dependencies/ ./
COPY --from=build /build/extracted/application/ ./

USER taskforge
EXPOSE 8080

# Size the heap from the container's memory limit rather than the host's.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"

# Ready means: database reachable and workers started after recovery.
HEALTHCHECK --interval=10s --timeout=3s --start-period=40s --retries=3 \
  CMD wget -qO- http://localhost:8080/actuator/health/readiness > /dev/null || exit 1

# Exec form: java is PID 1 and receives SIGTERM directly, which triggers
# the graceful shutdown (in-flight requests and jobs get a grace period).
ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]
