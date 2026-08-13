# Pipeline Monitoring

This runbook adds the phase runner to an existing Prometheus and Grafana stack.
It uses only checked-in configuration. Applying it restarts or reloads the
operator's monitoring services and recreates the phase-runner container; it
does not change phase state or add database writes.

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
lower threshold is not proof that a still-running long batch is stuck.

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
| Heartbeat age | Seconds since the newest heartbeat for the chain phase. `-1` means no heartbeat exists. Compare it with the configured stale threshold, which defaults to 900 seconds and must exceed the slowest healthy batch. |
| Head lag in blocks | Observed provider target minus the phase's processed block. For Live, the target is the provider head observed at the start of its latest batch. The paging rule applies to Live because historical phases can be far behind during an expected rebuild. |
| Verification level | The stored `quick_synced`, `cross_checked`, or `node_checked` result. A value of `1` identifies the recorded level. |
| Repair and reinterpretation state | The active marker and progress for unfinished repair work, plus whether Interpret still needs a repair run because its stored [interpreter content hash](../glossary.md#interpreter-content-hash) differs. Starting the required repair adopts the new hash and clears the requirement gauge; `phase_runner_redo_in_progress` stays at `1` until that work finishes. |
| Exporter health | Whether Prometheus can scrape the runner and whether the latest read of PostgreSQL state succeeded. |

## Alerts

| Alert | Threshold | Plain-language meaning |
| --- | --- | --- |
| `BignamePhaseFailed` | A phase reports `failed` on one rule evaluation. | An error was stored. The runner may be in retry backoff or may have stopped; use the logs and subsequent state to distinguish them. |
| `BignamePhaseRunnerDown` | The target is not scrapeable for 2 minutes. | The runner process or metrics listener is unavailable. This also covers a single-chain terminal error that ends the process immediately after storing failure state. |
| `BignamePhaseRunnerHeartbeatStale` | A running or capacity-paused phase has no heartbeat, or exceeds `BIGNAME_PHASE_RUNNER_HEARTBEAT_STALE_AFTER_SECS` (900 seconds by default), for 2 minutes. | A batch or capacity wait has exceeded the deployment's longest expected heartbeat interval. |
| `BignamePhaseRunnerHeadLagHigh` | Live processing stays more than 30 blocks behind its observed provider target for 10 minutes. | The chain is persistently falling behind new blocks. |
| `BignamePhaseRunnerMetricsRefreshStale` | The database read fails, or the last successful refresh becomes older than 60 seconds, for 2 minutes. | The endpoint is reachable but is serving an old view of pipeline state. |

The two hand-detected failure cases from issue #327 map directly to paging: a
terminal phase error triggers `BignamePhaseFailed` while the runner remains
reachable, or `BignamePhaseRunnerDown` if the process exits; a stalled batch
that crosses the deployment's configured maximum healthy duration triggers
`BignamePhaseRunnerHeartbeatStale`.

## First-response checks

Keep diagnosis read-only until the failure is understood:

```sh
curl -fsS http://127.0.0.1:9465/metrics | \
  grep -E 'phase_runner_(phase_status|heartbeat_age_seconds|head_lag_blocks)'

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
