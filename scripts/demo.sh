#!/usr/bin/env bash
#
# Guided tour of TaskForge against a running stack.
#
#   docker compose up --build -d --wait
#   ./scripts/demo.sh            # every scenario (about 1.5 minutes)
#   ./scripts/demo.sh crash      # one scenario
#
# Scenarios: health, submit, idempotency, retries, timeout, cancel, crash.
# Needs only bash and curl. The crash scenario also needs docker compose,
# because it kills the application container with SIGKILL.

set -euo pipefail

BASE_URL="${TASKFORGE_URL:-http://localhost:8080}"
RUN_ID="$(date +%s)-$RANDOM"

bold=$'\e[1m'; dim=$'\e[2m'; green=$'\e[32m'; red=$'\e[31m'; reset=$'\e[0m'
[[ -t 1 ]] || { bold=; dim=; green=; red=; reset=; }

section() { printf '\n%s== %s ==%s\n' "$bold" "$1" "$reset"; }
say()     { printf '%s\n' "$*"; }
show()    { printf '%s$ %s%s\n' "$dim" "$*" "$reset"; }
ok()      { printf '%s  ✔ %s%s\n' "$green" "$*" "$reset"; }
fail()    { printf '%s  ✘ %s%s\n' "$red" "$*" "$reset"; exit 1; }

# Extracts a top-level string or number field from a JSON object.
field() {
  sed -n "s/.*\"$1\":\"\{0,1\}\([^\",}]*\).*/\1/p" | head -n 1
}

# Prints the response body; sets STATUS to the HTTP status code.
request() {
  local method="$1" path="$2"; shift 2
  local out
  out="$(curl -sS -X "$method" "$BASE_URL$path" -w $'\n%{http_code}' "$@")"
  STATUS="${out##*$'\n'}"
  BODY="${out%$'\n'*}"
}

submit() {   # submit <json> [extra curl args...]
  local json="$1"; shift
  show "POST /api/jobs $json $*"
  request POST /api/jobs -H 'Content-Type: application/json' -d "$json" "$@"
  JOB_ID="$(field id <<<"$BODY")"
  say "  HTTP $STATUS  id=$JOB_ID  status=$(field status <<<"$BODY")"
}

job_status() { request GET "/api/jobs/$1"; field status <<<"$BODY"; }

wait_for_status() {   # wait_for_status <id> <status> <timeout-seconds>
  local id="$1" want="$2" deadline=$((SECONDS + $3)) current=""
  while (( SECONDS < deadline )); do
    current="$(job_status "$id")"
    [[ "$current" == "$want" ]] && { ok "job is $want"; return 0; }
    sleep 0.5
  done
  fail "job $id is $current, expected $want within $3 s"
}

print_attempts() {
  request GET "/api/jobs/$1/attempts"
  say "  attempts:"
  # One attempt per line: number, status, failure reason.
  sed 's/},{/}\n{/g' <<<"$BODY" | while read -r attempt; do
    printf '    #%s %-9s %s\n' \
      "$(field attemptNumber <<<"$attempt")" \
      "$(field status <<<"$attempt")" \
      "$(field failureReason <<<"$attempt" | sed 's/^null$//')"
  done
}

wait_until_ready() {
  local deadline=$((SECONDS + ${1:-60}))
  while (( SECONDS < deadline )); do
    if curl -sf "$BASE_URL/actuator/health/readiness" > /dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  if command -v docker > /dev/null; then
    docker compose ps -a >&2 || true
    docker compose logs --tail 40 app >&2 || true
  fi
  fail "TaskForge did not become ready at $BASE_URL"
}

# ---------------------------------------------------------------------------

demo_health() {
  section "Health checks"
  say "Liveness: is the JVM up? Readiness: is the database reachable and"
  say "have the workers started after crash recovery?"
  show "GET /actuator/health/readiness"
  request GET /actuator/health/readiness
  say "  HTTP $STATUS  $BODY"
  [[ "$STATUS" == 200 ]] || fail "not ready"
  ok "ready"
}

demo_submit() {
  section "Submit a job and watch it complete"
  submit '{"name":"Nightly sales report","type":"GENERATE_REPORT"}' \
    -H "X-Request-Id: demo-submit-$RUN_ID"
  [[ "$STATUS" == 202 ]] || fail "expected 202 Accepted"
  wait_for_status "$JOB_ID" COMPLETED 10
  print_attempts "$JOB_ID"
  say "  Log lines for this request carry requestId=demo-submit-$RUN_ID; the"
  say "  worker's lines carry jobId=$JOB_ID (docker compose logs app)."
}

demo_idempotency() {
  section "Idempotent submission"
  say "The same Idempotency-Key twice creates one job; the repeat is a replay."
  local key="demo-$RUN_ID" body='{"name":"Welcome email","type":"SEND_EMAIL"}'
  submit "$body" -H "Idempotency-Key: $key"
  local first="$JOB_ID"
  [[ "$STATUS" == 202 ]] || fail "expected 202 for the first request"
  submit "$body" -H "Idempotency-Key: $key" -D - 2>/dev/null
  [[ "$STATUS" == 200 && "$JOB_ID" == "$first" ]] || fail "expected a 200 replay of $first"
  ok "second request returned the original job (200, Idempotent-Replayed: true)"

  say "Reusing the key with a different body is rejected:"
  submit '{"name":"Different email","type":"SEND_EMAIL"}' -H "Idempotency-Key: $key"
  [[ "$STATUS" == 422 ]] || fail "expected 422"
  ok "422 Unprocessable Entity: $(field message <<<"$BODY")"
}

demo_retries() {
  section "Retries with exponential backoff"
  say "FAIL_FOR_TESTING always throws. With maxAttempts=3 it is retried after"
  say "2 s and 4 s, then marked FAILED."
  submit '{"name":"Flaky import","type":"FAIL_FOR_TESTING","maxAttempts":3}'
  wait_for_status "$JOB_ID" FAILED 20
  print_attempts "$JOB_ID"

  say "A permanently failed job can be retried by hand for one more attempt:"
  show "POST /api/jobs/$JOB_ID/retry"
  request POST "/api/jobs/$JOB_ID/retry"
  say "  HTTP $STATUS  status=$(field status <<<"$BODY")"
  wait_for_status "$JOB_ID" FAILED 10
  print_attempts "$JOB_ID"

  say "A non-retryable failure stops after one attempt even with attempts left:"
  submit '{"name":"Bad input","type":"FAIL_PERMANENTLY_FOR_TESTING","maxAttempts":5}'
  wait_for_status "$JOB_ID" FAILED 10
  print_attempts "$JOB_ID"
}

demo_timeout() {
  section "Per-attempt timeout"
  say "SLOW_FOR_TESTING sleeps 15 s; with timeoutSeconds=2 the worker"
  say "interrupts it."
  submit '{"name":"Slow export","type":"SLOW_FOR_TESTING","timeoutSeconds":2,"maxAttempts":1}'
  wait_for_status "$JOB_ID" FAILED 10
  print_attempts "$JOB_ID"
}

demo_cancel() {
  section "Cancel a running job"
  submit '{"name":"Long rebuild","type":"SLOW_FOR_TESTING"}'
  wait_for_status "$JOB_ID" RUNNING 10
  show "DELETE /api/jobs/$JOB_ID"
  request DELETE "/api/jobs/$JOB_ID"
  say "  HTTP $STATUS (202 = cancellation requested, the handler is interrupted)"
  wait_for_status "$JOB_ID" CANCELLED 10
  print_attempts "$JOB_ID"
}

demo_crash() {
  section "Crash recovery"
  command -v docker > /dev/null || fail "docker is needed for this scenario"
  say "Start a 15 s job, kill the container with SIGKILL while it runs (no"
  say "shutdown hooks), then start it again. Startup recovery records the"
  say "interrupted attempt as FAILED and the job is retried."
  submit '{"name":"Month-end billing","type":"SLOW_FOR_TESTING","timeoutSeconds":60}'
  wait_for_status "$JOB_ID" RUNNING 10

  local container
  container="$(docker compose ps -q app)"
  [[ -n "$container" ]] || fail "no running app container found"

  show "docker compose kill -s SIGKILL app"
  docker compose kill -s SIGKILL app > /dev/null
  # kill returns once the signal is sent, not when the container has
  # exited. Starting before it exits is a no-op, and the stack stays down.
  docker wait "$container" > /dev/null
  ok "container killed while the job was RUNNING (exit code 137)"

  show "docker compose start app"
  docker compose start app
  wait_until_ready 90
  ok "restarted and ready"
  print_attempts "$JOB_ID"

  say "Waiting for attempt 2 (2 s backoff + 15 s of work)..."
  wait_for_status "$JOB_ID" COMPLETED 40
  print_attempts "$JOB_ID"
}

# ---------------------------------------------------------------------------

scenarios=(health submit idempotency retries timeout cancel crash)
selected=("${@:-${scenarios[@]}}")
[[ "${selected[0]}" == all ]] && selected=("${scenarios[@]}")

wait_until_ready 10
for name in "${selected[@]}"; do
  if [[ " ${scenarios[*]} " != *" $name "* ]]; then
    fail "unknown scenario '$name' (choose from: ${scenarios[*]})"
  fi
  "demo_$name"
done

section "Done"
say "Swagger UI:  $BASE_URL/swagger-ui.html"
say "All jobs:    $BASE_URL/api/jobs"
say "Logs:        docker compose logs app"
