# Pipeline Monitoring

This runbook adds the phase runner to an existing Prometheus and Grafana stack.
It uses only checked-in configuration. Applying it restarts or reloads the
operator's monitoring services and recreates the phase-runner container; it
does not add database writes for metrics. Runner startup can settle active
`running` or `paused` rows with no unfinished explicit repair for chains that
are no longer configured, as described below.

The artifacts are:

- [`ops/monitoring/prometheus/phase-runner.yml`](../../ops/monitoring/prometheus/phase-runner.yml)
  — scrape job and rule-file reference;
- [`ops/monitoring/prometheus/phase-runner-alerts.yml`](../../ops/monitoring/prometheus/phase-runner-alerts.yml)
  — paging rules;
- [`ops/monitoring/prometheus/phase-runner-alerts.test.yml`](../../ops/monitoring/prometheus/phase-runner-alerts.test.yml)
  — rule-evaluation fixtures for every checked-in paging rule; and
- [`ops/monitoring/grafana/dashboards/phase-runner.json`](../../ops/monitoring/grafana/dashboards/phase-runner.json)
  — importable dashboard.

## Apply the runner endpoint

Deploy the image built from this change and validate the tracked Compose file
before recreating anything:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml config
```

The tracked configuration binds the container listener to `0.0.0.0:9465` and
publishes it as `127.0.0.1:9465` on the host. The loopback-only host mapping is
useful for a manual check and does not make the endpoint public. Recreate the
runner with the same image and all overlays used by the deployment:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  up -d --no-deps phase-runner

curl -fsS http://127.0.0.1:9465/metrics | \
  grep '^phase_runner_metrics_refresh_success 1$'
```

If `9465` is already assigned, set the Compose-only
`BIGNAME_PHASE_RUNNER_METRICS_PORT` to a free host port. Prometheus does not use
that host port in the recommended container-network setup below; it connects to
the container listener at `phase-runner:9465`.

Before enabling paging, set
`BIGNAME_PHASE_RUNNER_HEARTBEAT_STALE_AFTER_SECS` above the slowest healthy
batch or inter-phase transition measured on this deployment. The checked-in
default is 900 seconds. The runner writes heartbeats between batches, so a
single healthy batch longer than 900 seconds will page at the default. Rebuild
batches during a planned [re-derivation
boundary](../glossary.md#re-derivation-boundary) have historically exceeded
eight minutes. Calibrate the threshold before the full source re-walk, not
after its first false page.

## Connect Prometheus

The scrape artifact expects Prometheus to resolve `phase-runner` on the
application's `bigname_default` Docker network. Add that existing network to the
Prometheus service in the host's monitoring Compose file:

```yaml
services:
  prometheus:
    networks:
      - default
      - bigname

networks:
  bigname:
    external: true
    name: bigname_default
```

Do not expose Prometheus or the metrics listener at the public edge. If the
monitoring stack deliberately uses host networking instead, change the checked
scrape target as it is copied into the host configuration to
`127.0.0.1:9465`.

Merge the `rule_files` and `scrape_configs` entries from
`phase-runner.yml` into the host's Prometheus configuration. Copy or mount
`phase-runner-alerts.yml` beside that configuration so the relative rule-file
path resolves. Keep the job name `bigname-phase-runner`; the dashboard and
alerts select that exact label.

The container-restart rule uses the runner endpoint's process-start gauge. It
does not depend on cAdvisor container labels, which are unavailable from some
cAdvisor and Docker storage-driver combinations. Confirm that the runner target
exports the gauge:

```sh
curl --get --fail --silent --show-error \
  'http://127.0.0.1:9090/api/v1/query' \
  --data-urlencode \
  'query=phase_runner_process_start_timestamp_milliseconds{job="bigname-phase-runner"}'
```

The result must contain one sample per runner target. Prometheus keeps the
target's `job` and `instance` labels stable across container replacements, so
each new process-start value is observable as a change on the same time series.
No cAdvisor or additional scrape configuration is needed for this rule.

Validate the fully assembled host files before reloading Prometheus:

```sh
promtool check rules /path/to/phase-runner-alerts.yml
promtool check config /path/to/prometheus.yml
promtool test rules /path/to/phase-runner-alerts.test.yml
```

Use the monitoring stack's ordinary reload or targeted Compose recreation.
Then check the target and rules without changing application state:

```sh
curl -fsS 'http://127.0.0.1:9090/api/v1/query?query=up%7Bjob%3D%22bigname-phase-runner%22%7D'
curl -fsS 'http://127.0.0.1:9090/api/v1/rules'
```

The first result must contain a sample value of `1`. The second must list the
`bigname-phase-runner` rule group. Route the checked-in `severity=page` label
through the host's existing notification policy; these files do not contain a
receiver, token, or destination.

## Import the Grafana dashboard

Import `phase-runner.json` through Grafana's dashboard import screen and choose
the host Prometheus data source when prompted. For file provisioning, mount the
JSON in the existing dashboard-provisioning directory and use the monitoring
stack's normal Grafana reload or targeted recreation. The stable dashboard UID
is `bigname-phase-runner`, so a later import updates the same dashboard.

## Panels

| Panel | What it means |
| --- | --- |
| Phase lifecycle state | The current `idle`, `running`, `paused`, `completed`, or `failed` row for every chain phase. A value of `1` is the active state. |
| Phase progress | The latest processed block and current target. `-1` means the runner has not recorded that position. |
| Heartbeat age | Seconds since the newest heartbeat for the chain phase, plus the in-process age since the runner loop for each configured chain last crossed a phase or batch boundary. A phase value of `-1` means no database heartbeat exists. Compare both with the configured stale threshold, which defaults to 900 seconds and must exceed the slowest healthy batch or inter-phase transition. |
| Head lag in blocks | Observed provider target minus the phase's processed block. For Live, the target is the provider head observed at the start of its latest batch. The paging rule applies to Live because historical phases can be far behind during an expected rebuild. |
| Verification level | The stored `quick_synced`, `cross_checked`, or `node_checked` result. A value of `1` identifies the recorded level. |
| Repair and reinterpretation state | The active marker and progress for unfinished repair work, plus whether Interpret still needs a repair run because its stored [interpreter content hash](../glossary.md#interpreter-content-hash) differs. Starting the required repair adopts the new hash and clears the requirement gauge; `phase_runner_redo_in_progress` stays at `1` until that work finishes. |
| Exporter health | Whether Prometheus can scrape the runner and whether the latest read of PostgreSQL state succeeded. |

## Alerts

| Alert | Threshold | Plain-language meaning |
| --- | --- | --- |
| `BignamePhaseFailed` | A phase reports `failed` on one rule evaluation. | This intentionally trades pages during retryable transient backoff for guaranteed visibility of terminal errors and crash loops. Use the logs and subsequent state to distinguish them. |
| `BignamePhaseRunnerDown` | The target reports `up=0`, or no `up` series exists for the job, continuously for 2 minutes. | The runner process, metrics listener, or Prometheus target definition stayed unavailable. A successful scrape resets the timer, so this rule does not catch a flapping crash loop. The absent-target branch has only the `job` label because no target exists to supply an `instance`. |
| `BignamePhaseRunnerCapacityPaused` | A phase remains continuously `paused` for 15 minutes. | Storage capacity has stopped pipeline work for the named chain phase. Short capacity waits do not page, while the runner continues refreshing its liveness signals during the wait. |
| `BignamePhaseRunnerContainerRestarting` | The runner's process-start value changes at least 3 times within 10 minutes. | The container is crash-looping, including fresh-deployment failures that happen before a phase failure can be stored. Prometheus must successfully scrape each start that it counts. |
| `BignamePhaseRunnerHeartbeatThresholdMissing` | The configured heartbeat-threshold series is absent for 2 minutes. | The runner image and rules are incompatible, so the age-based alerts cannot be evaluated safely. |
| `BignamePhaseRunnerLoopHeartbeatMissing` | The runner is scrapeable and exports the heartbeat threshold, but its runner-loop heartbeat series is absent for 2 minutes. | The runner image predates the loop-liveness rule, so between-phase stalls cannot be evaluated safely. |
| `BignamePhaseRunnerHeartbeatStale` | An active phase has no database liveness heartbeat, or exceeds `BIGNAME_PHASE_RUNNER_HEARTBEAT_STALE_AFTER_SECS` (900 seconds by default), for 2 minutes. | The runner stopped refreshing phase liveness. Capacity waits keep this heartbeat fresh and instead page through `BignamePhaseRunnerCapacityPaused` after 15 minutes. |
| `BignamePhaseRunnerLoopStale` | The runner loop for a configured chain crosses no phase or batch boundary for the heartbeat threshold, plus 2 minutes. | The process is scrapeable, but work for that chain may be wedged while every phase row rests. |
| `BignamePhaseRunnerHeadLagHigh` | Live lag exceeds 30 blocks and Live is observed running at least once in every 2-minute window for 10 minutes. | The chain is persistently falling behind new blocks; brief completed-state zeroes do not reset the alert, while a failed or resting phase does not keep it active without new running samples. |
| `BignamePhaseRunnerMetricsRefreshStale` | The database read fails, or the last successful refresh becomes older than 60 seconds, for 2 minutes. | The endpoint is reachable but is serving an old view of pipeline state. |

The two hand-detected failure cases from issue #327 map directly to paging: a
terminal phase error triggers `BignamePhaseFailed` while the runner remains
reachable, or `BignamePhaseRunnerDown` if the process exits and remains down
for 2 minutes. Repeated exits with successful scrapes between them trigger
`BignamePhaseRunnerContainerRestarting` reliably for restart periods under
about three minutes, intermittently for periods from roughly three to five
minutes, and not for periods of five minutes or longer. A stalled batch that
crosses the deployment's configured maximum healthy duration triggers
`BignamePhaseRunnerHeartbeatStale`.

A slow crash loop remains unpaged when its restart period is five minutes or
longer, each outage lasts less than two minutes, and no phase failure has yet
been stored; one concrete case is a fresh deployment that is OOM-killed several
minutes into every startup.

A non-Live phase that repeatedly completes batches without advancing its
cursor keeps its heartbeats fresh and is not yet detected; progress-delta
alerting is tracked in [issue #429](https://github.com/ensdomains/bigname/issues/429).

## Removing a configured chain

Before removal, use the normal runner or reviewed recovery procedure to recover
every `failed` phase to `completed` and finish every explicit repair. If either
condition cannot be met, keep the chain configured and escalate to the
phase-runner and storage owners for a separately reviewed decommission cleanup.

Then stop the runner, remove the chain from configuration, and restart it. At
startup, the runner acquires the same per-phase lock used for normal recovery
and changes any `running` or `paused` row with no unfinished explicit repair for
an unconfigured chain to `completed`; it logs the chain and phase it settled and
never starts work for that chain. Failed rows and unfinished repair markers are
deliberately not rewritten, which is why they must be resolved before removal.
Do not update statuses, clear repair markers, or delete rows merely to silence
an alert.

If the chain is configured again later, the runner checks the stored completion
evidence rather than trusting the rewritten `completed` status alone. It resumes
Ingest when the current block or live handoff does not match the target, and
resumes Verify when its final block pair or [verification
level](../glossary.md#verification-level) is missing. Settling an active Ingest
row clears its live handoff but preserves its source cursors, so re-adding the
chain resumes even if an older runner stopped between its formerly separate
summary and cursor writes.

## First-response checks

Keep diagnosis read-only until the failure is understood:

```sh
curl -fsS http://127.0.0.1:9465/metrics | \
  grep -E 'phase_runner_(phase_status|heartbeat_age_seconds|loop_heartbeat_age_seconds|head_lag_blocks)'

docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  logs --tail=200 phase-runner
```

For a failed phase, record the chain, phase, error log, current block, and
target block before choosing a recovery procedure. For stale heartbeats, check
whether block progress is also flat and whether the exporter refresh remains
healthy. For head lag, compare Live progress with its observed provider target
and check provider and database latency. Follow
[`production-docker.md`](production-docker.md#recovery-plays) for recovery; do
not clear phase rows or mark work complete by hand.
