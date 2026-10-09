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

A directly launched one-shot redo binds an ephemeral loopback listener by
default, so concurrent repairs do not contend for the supervisor port. Its
info-level startup event records the selected address when `RUST_LOG=info`.
That default is not a stable Prometheus target. To apply the checked-in
repair-mode alerts during an operator redo, pass a unique stable
`--metrics-bind-addr` or set
`BIGNAME_PHASE_RUNNER_REDO_METRICS_BIND_ADDR`, then add that address under the
same job name. The redo command deliberately ignores
`BIGNAME_PHASE_RUNNER_METRICS_BIND_ADDR`, which remains the supervised runner's
setting. Remove the temporary target and reload Prometheus immediately when the
redo exits, then confirm it is absent before the two-minute
`BignamePhaseRunnerDown` hold elapses.

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
When planned Ingest, Interpret, and Project one-shot repairs precede a supervisor
restart inside ten minutes, schedule a maintenance silence scoped only to
`alertname="BignamePhaseRunnerContainerRestarting"`, `job="bigname-phase-runner"`,
and the maintained target's exact `instance`;
keep every other alert active and end the silence after the restarted supervisor is healthy.

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
| Heartbeat age | Seconds since the newest heartbeat for the chain phase, plus the in-process age since each supervised or active one-shot repair chain last crossed a phase or batch boundary. A phase value of `-1` means no database heartbeat exists. Compare both with the configured stale threshold, which defaults to 900 seconds and must exceed the slowest healthy batch or inter-phase transition. |
| Head lag in blocks | Observed provider target minus the phase's processed block. For Live, the target is the provider head observed at the start of its latest batch. The paging rule applies to Live because historical phases can be far behind during an expected rebuild. |
| Verification level | The stored `quick_synced`, `cross_checked`, or `node_checked` result. A value of `1` identifies the recorded level. |
| Repair and reinterpretation state | The active marker and progress for unfinished repair work, plus whether Interpret still needs a repair run because its stored [interpreter content hash](../glossary.md#interpreter-content-hash) differs. Starting the required repair adopts the new hash and clears the requirement gauge; `phase_runner_redo_in_progress` stays at `1` until that work finishes. |
| Phase cursor non-progress | Committed [work-bearing batches](../glossary.md#work-bearing-batch) confirmed at the next resume to have left the [durable composite cursor](../glossary.md#durable-composite-cursor) unchanged, and the age of that sequence. Normal, redo, and recompute-flags work remain separate. |
| Exporter health | Whether Prometheus can scrape the runner and whether the latest read of PostgreSQL state succeeded. |
| RPC chain check | `phase_runner_rpc_chain_id{chain,source}` is the chain id each RPC endpoint reported to its latest [RPC chain check](../deployment.md#rpc-chain-check), or `-1` when none could be read; hydration URLs use `source="hydration"`, which they share with a configured source of that key. `phase_runner_rpc_chain_mismatch{chain,source}` is `1` once an Ingest or Live source failed the check during the run; a failed Verify reference shows as a failed Verify phase instead. |

## Ingest RPC traffic

Three counters cover only the JSON-RPC traffic the shared Ingest and Live
engine's providers send to each configured RPC source, including their
periodic RPC chain rechecks and Coinbase's companion RPC. The startup RPC
chain check, Verify's reference provider, source-transport transitions,
Project hydration and API lookups are not counted.

| Counter | What it counts |
| --- | --- |
| `phase_runner_ingest_rpc_requests_total{chain,source,outcome}` | HTTP requests, one per standalone call or batch, including retries. `outcome` is `ok`, or `failed` for a transport error, a non-2xx status or a body that is not JSON. A JSON-RPC error inside a 200 response counts as `ok`. |
| `phase_runner_ingest_rpc_calls_total{chain,source,method}` | JSON-RPC calls, each call of a batch counted once and each retry counted again. This is roughly what a per-call provider bills. |
| `phase_runner_ingest_provider_null_results_total{chain,source,method}` | `null` answers for the receipt (`eth_getTransactionReceipt`) or transaction (`eth_getTransactionByHash`) of a selected log: every `null` answer Ingest accepted, including re-requests. |

A request still in flight when its window fails on another error is abandoned
and not counted.

A rising null count with a flat `phase failed with a retryable error` log means
the re-requests are absorbing the provider's `null` answers. Failed phases
alongside it can mean answers stayed `null` after three re-requests; read the
failure reason. `provider omitted receipt for selected transaction …` or
`provider omitted transaction for selected log …` is that case: lower
`BIGNAME_INGEST_RPC_MAX_IN_FLIGHT` or `BIGNAME_INGEST_RPC_BATCH_SIZE`
([RPC settings](../deployment.md#phase-runner-configuration)). Transport,
routing and reorg-position errors do not show the re-requests ran out;
diagnose them on their own. Lowering the in-flight limit or batch size can
help rate limits and load-related timeouts; it does nothing for a genuine
reorg.

## Served lag

Two gauges measure how far the newest data the API could serve trails the
chain. They have no dashboard panel or paging rule yet; those arrive with the
ops dashboards tracked in Linear TYR-34.

- `phase_runner_served_publication_block{chain}` is an absolute block height:
  the [family marker](../glossary.md#family-marker)'s block while the marker is
  `live`, uses this binary's interpreter content hash, lies on readable
  canonical lineage and is at or below the stored head. The chain must also
  have its Project phase row for the metrics query. Missing or rebuilding
  (`bootstrap_pending`) publication reads `-1`. This gauge does not apply the
  API's [publication lag tolerance](../glossary.md#publication-lag-tolerance), request-specific selected positions or redo
  admission, so a reported publication can still be stale for a request.
  `phase_runner_redo_in_progress` reports active repairs separately. The
  publication gauge is not expected to return to zero.
- `phase_runner_served_lag_blocks{chain}` is the newest observed
  execution-client head minus that publication block. It is the only one of
  the two expected to return to zero. A non-zero value means the newest
  publication is that many blocks behind the newest block the runner has seen.
  Because the API stops serving a chain once its publication trails the stored
  head by more than `BIGNAME_API_PUBLICATION_LAG_TOLERANCE_BLOCKS` (one block by
  default), a value above that tolerance usually means readers are getting
  stale-data errors, not old answers. The Project latency target
  (Linear TYR-36) requires this gauge to return to zero every normal block.

`-1` on either gauge means unavailable, never healthy or caught up. The lag
reads `-1` when either side is missing, and also when the observed head is below
the publication, which the stored heads cannot explain; the runner logs a
warning with both numbers each time that pair changes. Every configured chain is
set to `-1` on both gauges before the initial refresh and before the listener
starts, so a configured chain with no rows shows `-1` from the first scrape.
Each refresh reconciles the whole result: a configured chain the query no longer
returns, for example because its Project row is gone, reads `-1` and keeps its
series; a chain that is neither configured nor returned loses its series. An
alert on the lag must treat `-1` as unservable too, or it goes quiet exactly
when readers get nothing.

The existing `phase_runner_chain_head_block{chain}` is the stored chain head,
not the observed head. Plotting it against the publication block shows how far
Project trails the stored head, which is what the API's fence checks, not the
end-to-end lag this gauge reports.

The observed head is the newer of two stored values: the Live phase's target,
which each Live batch sets to the head it read from the execution client, and
the stored chain head, which Ingest and Live advance as they publish heads. So a
Live batch moves the observed head, and Ingest can move it too, for example
while it catches up after a restart before Live runs again. Two cases make it
briefly inexact: a Live batch that finds no common ancestor with the node stores
the published head as its target, and after a rewind the Live target stays at
the old, higher head until the next Live batch. The metrics code never asks the
execution client itself. Live, Interpret and Project run in turn, so nothing
moves the observed head while a Project batch runs, and the gauge can
under-report until the next Live run. A Project commit reads zero only when its
publication reaches the retained observed head; a batch that started behind
that head still shows the difference after it commits, and a slow batch's real
lag appears once the next Live batch has read the head. When following the
chain normally, each Live batch reads one new block, so the gauge reads 1 until
Project publishes that block and then 0. A value above 1 after a Live batch
means Project fell behind by more than one block, usually because its previous
batch took longer than a block.

Both gauges are refreshed with the others every 5 seconds, and a refresh is
also requested after every Ingest, Live and Project batch has recorded its
progress row, so the value in the runner is current shortly after each of those
commits. Commits that arrive together share one refresh; this is not sampling of
every block. Some transitions send no dedicated refresh request: a failed
Project batch, a rewind, a completed batch whose follow-up confirmation fails
after its progress row committed, and the final redo completion that restores
the Project row after the refresh a redo batch requested. They become visible
on the next successful refresh, periodic or triggered by another commit on any
chain, since every refresh reads all chains. The periodic refresh is the
fallback when no further request arrives. The 5-second refresh alone is slower
than a Base block; on Base the refresh after each commit is what keeps the
value current. Prometheus still samples it only once per scrape
(every 15 seconds in the checked-in configuration), so a lag that lasts less
than a scrape interval may never appear in a graph.

`phase_runner_head_lag_blocks` keeps its meaning: a phase's own target minus
its own progress. A Project batch's target is the head it started with, so that
gauge is not a freshness measure; use `phase_runner_served_lag_blocks` instead.
If Live stops, its target stops moving and the served-lag gauge can read 0
while the chain moves on, so check it together with the Live heartbeat age and
`phase_runner_head_lag_blocks{phase="live"}`.

The build identity is already exported as
`build_info{build_sha, interpreter_content_hash}`, with the value `1` for the
running binary.

## Universal Resolver cutover

Two gauges report, per chain, the
[Universal Resolver cutover](../glossary.md#universal-resolver-cutover) and
where the client-facing Universal Resolver proxy forwards. They have different
sources:

- `phase_runner_universal_resolver_cut_over{chain}` is `1` while the chain's
  deployment profile admits an ENSv2 root registry. This is the cutover the
  name reads apply once the redo republishes. Between a manifest sync and that
  redo, name reads serve the previous publication's admission. No proxy
  upgrade moves the gauge. It changes only with the manifest set.
- `phase_runner_universal_resolver_unadmitted{chain}` is `1` while the
  client-facing proxy, followed through the declared proxies it points at,
  ends at an implementation the `ens_execution` manifest neither lists nor
  declares as a proxy. Project derives this from the proxies' `Upgraded`
  events. It moves no name: the chain stays cut over, and names keep the
  expiry, grace and resolvability they had
  ([Expiry and grace](../api-v1.md#expiry-and-grace)). It means clients on
  chain no longer resolve through the deployment bigname models.

A chain with no admitted root registry and no proxy `Upgraded` reads `0` on
both and is not an alert. A proxy whose implementation is another declared
proxy with no `Upgraded` yet is not unadmitted. Each refresh exports both
gauges for every chain with phase rows.

The runner logs one warning each time a chain's proxies come to end at a new
unadmitted implementation, naming the chain, the proxy, the implementation and
the block of its `Upgraded`, and one info line when that clears.

The `unadmitted` gauge follows the family publication, so during a Project
rebuild, redo or catch-up it describes the block being replayed, and history
can hold implementations the manifest no longer lists. The paging rule therefore waits
for the publication to be current (`phase_runner_served_lag_blocks` between 0
and 30) and for no Project redo to be running
(`phase_runner_redo_in_progress{phase="project"}` at `0`): a redo undoes block
by block while its publication stays readable, so a shallow one can replay a
state the head no longer has. The warning has no such gate. Its block is the
latest `Upgraded` among the proxies on the client-facing proxy's path, which
can be an earlier proxy's repoint rather than the terminal proxy's upgrade, and
not the block being served, so it can be old
for a long-standing state, for example on the first refresh after a restart. Tell
a warning from replayed history by `phase_runner_redo_in_progress` and
`phase_runner_served_lag_blocks` at the time it was logged.

## Project family work

Four metrics describe the family runner's work, and seven its hydration. The runner reports its outcome
to the metrics task, including the committed prefix when a run fails.

- `phase_runner_project_families_seconds{chain}` is the wall time of the newest
  family run.
- `phase_runner_project_family_lag_blocks{chain}` is the distance between the
  committed marker and that run's Project target. Zero requires the target's
  exact hash and a readable marker; an orphaned or same-height mismatched
  marker counts at least one block. This is progress toward the run's fixed
  target, not end-to-end lag to the latest observed execution head.
- `phase_runner_project_family_block_seconds{chain}` records each single-block
  transaction from its first read through commit. Rebuild ranges are excluded
  because one range applies several work blocks in a transaction.
- `phase_runner_project_family_duplicate_anomalies_total{chain}` counts
  conflicting deliveries of one normalized event identity whose position or
  payload disagrees with the delivery retained in canonical order.

[Hydration](../projections.md#follow-only-hydration) runs only on a head block
of `ethereum-mainnet`. Its RPC activity is counted when it happens, including
for a block whose publication then fails; row writes are counted only for
committed blocks. `kind` is `reverse` or `text`.

- `phase_runner_project_hydration_passes_total{chain,result}` counts head
  blocks whose reads were prepared, each under exactly one result: `timed_out`
  (the block's reads spent their 30 seconds, whatever else the pass learned),
  `unserved` (the endpoint did not serve the block) or `served` (neither, a
  head block with nothing to read included). The three results add up to the
  prepared passes. No increase while blocks are published means Project is
  catching up, replaying or rebuilding, or the chain has no new block.
- `phase_runner_project_hydration_rpc_calls_total{chain,kind}` and
  `phase_runner_project_hydration_rpc_failures_total{chain,kind}` count
  Multicall3 aggregates sent and those that failed as a whole. `kind="probe"`
  is the one-call aggregate sent after a failed batch when the endpoint has
  answered nothing at the block yet. It is evidence of whether the endpoint
  serves the block, not proof.
- `phase_runner_project_hydration_selectors_total{chain,kind,outcome}` counts
  selectors by outcome: `observed`, `failed_call`, `deferred` or
  `not_observed`. `not_observed` includes selectors a block had no call left
  for; they keep their place for the next head.
- `phase_runner_project_hydration_writes_total{chain,kind,write}` counts rows
  changed in committed blocks: `value` (the row's observation changed: the
  hydrated value, a cleared one included, or for a reverse tuple the block it
  was observed at) or `schedule` (only its place in the queue, its aggregate
  size limit or its failure count).
- `phase_runner_project_hydration_rpc_seconds{chain}` is the RPC wall time of
  the newest family run that hydrated.
- `phase_runner_project_hydration_head_age_seconds{chain}` is the age of the
  newest hydrated head block, by its timestamp, when its reads began. A large
  value means Project reached the head late; an endpoint that keeps little
  historical state then fails those reads.

A rising `unserved` or `not_observed` count with no `observed` selectors means
the hydration endpoint cannot answer at the blocks Project reaches: check the
endpoint, its state retention against the head age, and the `warn` lines `a
hydration RPC batch failed` and `the hydration endpoint does not serve this
block`, which name the chain, block, kind, selector count and error. No stored
value changes for a block counted as `unserved`. Rising `deferred` with `observed`
selectors means some aggregate cannot be answered although the endpoint
works: the log's error says why (for example a size or gas limit). A steady
small `deferred` rate with matching `schedule` writes is a selector that fails
every time and is tried again, alone, whenever its turn comes. Do not
judge progress by pending text work alone: reverse tuples stay in rotation
after a successful read.

A failed child response waits 7,200 blocks before another hydration read unless
fresh selector evidence resets its delay. A stable pending count during that
wait is expected; the row's original attempt stamp must not advance on every
head. Old eligible work has 63 reserved text slots and a rounded-up quarter of
each kind's call budget. Outer failures remain distinct and follow the split
or unobserved policy above.

The run gauges retain the newest outcome until the next; anomaly and hydration
counters add all reported outcomes. Pending single-block observations are bounded to 65,536
per chain until the metrics task drains them. Cancellation can leave an
in-flight outcome unreported even though a commit completed, so these process
metrics do not replace the durable marker as proof of progress. There is no
second metrics publication transaction. The removed batch-builder row, scope,
stage and step metrics are no longer exported.

Project statements begin with a `/* project:<name> */` comment, so PostgreSQL's
slow log, `pg_stat_activity` and `pg_stat_statements` can identify a slow query.
For example, reverse hydration selection is
`project:families.hydrate.reverse.select`. See the statement's source under
`crates/project/src/families` for the exact identifier.

## Long Project runs

Rebuild and redo use bounded family runs, not one transaction for the complete
history. The default run budget is 256 applied or undone blocks. Older rebuild
work can commit in bounded ranges; follow and replay commit one block at a
time. Each continuation records progress and passes the ordinary phase lock,
heartbeat and capacity checks. `recompute-flags` runs in Interpret and stamps
ordinary downstream Project redo; there is no special Project refresh stage.

Inspect the family marker and repair record alongside phase progress. A
`bootstrap_pending` marker means the rebuild has committed intermediate work
but cannot yet serve. Repair states `undoing`, `replaying` and `rebuilding`
identify the durable operation; `complete` records the accepted target.
Failures leave the committed prefix available for resumption. A malformed
journal is a data-integrity error to investigate, not an instruction to edit
markers or silently discard it. Follow the recovery runbook for operator redo
or rebuild.

## Alerts

| Alert | Threshold | Plain-language meaning |
| --- | --- | --- |
| `BignamePhaseFailed` | A phase reports `failed` on one rule evaluation. | This intentionally trades pages during retryable transient backoff for guaranteed visibility of terminal errors and crash loops. Use the logs and subsequent state to distinguish them. |
| `BignamePhaseRunnerRpcChainMismatch` | `phase_runner_rpc_chain_mismatch` reads `1` on any scrape within 10 minutes. | The named source's RPC endpoint now serves another chain than configured, so that chain stopped. The window keeps the page after the runner exits on it, but only if a scrape saw the gauge first: a single-chain runner usually exits before that, and is caught by `BignamePhaseRunnerDown` and the refusal log instead. A restart refuses to start until the endpoint is fixed. The startup log names the chain, source, expected and observed chain id, the expected genesis hash where the chain pins one, and the observed genesis hash when block 0 was read; an endpoint that could not be read logs the chain, source and error. A chain with no pinned genesis, such as Base, is held to the genesis hash its ingest cursor recorded: at startup the refusal comes once the database is open, or when Ingest, Live or Verify next starts, as an error naming both hashes. That refusal stops the chain without setting this gauge: look for the refusal log, or `BignamePhaseRunnerDown` once a single-chain runner exits. A running runner's mismatch error names the recorded hash as the expected one when the endpoint reports the right chain id but another block 0; a wrong or unreadable chain id is refused before block 0 is read, so that error names chain ids only. |
| `BignameUniversalResolverUnadmitted` | `phase_runner_universal_resolver_unadmitted` reads `1` for 5 minutes while `phase_runner_served_lag_blocks` is between 0 and 30 and no Project redo is running. | The chain's client-facing Universal Resolver ends at an implementation the `ens_execution` manifest does not admit. It moves no name: the chain stays [cut over](#universal-resolver-cutover), and names keep the expiry, grace and resolvability they had. Clients on chain no longer resolve through the deployment bigname models. Nothing is broken in the pipeline and no repair helps. Read the proxy, the implementation and the block from the warning log, then check the `ens_execution` manifest history. If a manifest change removed that implementation, restore it. Otherwise the installed deployment was never admitted, usually because upstream repointed the proxy: pin the deployment the implementation belongs to under `.refs/` and admit it in the `ens_execution` manifest (with the rest of the deployment when upstream redeployed), as for any re-admission ([manifests](../manifests.md#universal_resolver_implementations)). The log's block is the latest `Upgraded` among the proxies on the client-facing chain, the block from which it has ended there, so on a chain of proxies it can be an earlier proxy's repoint rather than the last proxy's upgrade. Either manifest change rotates the interpreter content hash, and the redo it requires reclassifies the proxy, which clears the alert. Neither moves a name. With `BIGNAME_API_PUBLICATION_LAG_TOLERANCE_BLOCKS` above 30, the proxy can drift while the API serves a publication more than 30 blocks behind, and this rule stays quiet. |
| `BignamePhaseRunnerDown` | The target reports `up=0`, or no `up` series exists for the job, continuously for 2 minutes. | The runner process, metrics listener, or Prometheus target definition stayed unavailable. A successful scrape resets the timer, so this rule does not catch a flapping crash loop. The absent-target branch has only the `job` label because no target exists to supply an `instance`. |
| `BignamePhaseRunnerCapacityPaused` | A phase remains continuously `paused` for 15 minutes. | Storage capacity has stopped pipeline work for the named chain phase. Short capacity waits do not page, while the runner continues refreshing its liveness signals during the wait. |
| `BignamePhaseRunnerContainerRestarting` | The runner's process-start value changes at least 3 times within 10 minutes. | The container is crash-looping, including fresh-deployment failures that happen before a phase failure can be stored. Prometheus must successfully scrape each start that it counts. |
| `BignamePhaseRunnerHeartbeatThresholdMissing` | The configured heartbeat-threshold series is absent for 2 minutes. | The runner image and rules are incompatible, so the age-based alerts cannot be evaluated safely. |
| `BignamePhaseRunnerLoopHeartbeatMissing` | The runner is scrapeable and exports the heartbeat threshold, but its runner-loop heartbeat series is absent for 2 minutes. | The runner image predates the loop-liveness rule, so between-phase stalls cannot be evaluated safely. |
| `BignamePhaseRunnerHeartbeatStale` | An active phase has no database liveness heartbeat, or exceeds `BIGNAME_PHASE_RUNNER_HEARTBEAT_STALE_AFTER_SECS` (900 seconds by default), for 2 minutes. | The runner stopped refreshing phase liveness. Capacity waits keep this heartbeat fresh and instead page through `BignamePhaseRunnerCapacityPaused` after 15 minutes. |
| `BignamePhaseRunnerLoopStale` | A supervised or active one-shot repair chain crosses no phase or batch boundary for the heartbeat threshold, plus 2 minutes. | The process is scrapeable, but work for that chain may be wedged while every supervised phase row rests. |
| `BignamePhaseRunnerHeadLagHigh` | Live lag exceeds 30 blocks and Live is observed running at least once in every 2-minute window for 10 minutes. | The chain is persistently falling behind new blocks; brief completed-state zeroes do not reset the alert, while a failed or resting phase does not keep it active without new running samples. |
| `BignamePhaseRunnerMetricsRefreshStale` | The database read fails, or the last successful refresh becomes older than 60 seconds, for 2 minutes. | The endpoint is reachable but is serving an old view of pipeline state. |
| `BignamePhaseRunnerPhaseNonProgress` | Three confirmed unchanged-cursor work batches, or two whose sequence is at least 10 minutes old, remain present for 2 minutes. | The named phase and mode is repeatedly committing work without changing its durable resume position. It does not require the phase to be sampled as `running`; a failed phase remains owned by `BignamePhaseFailed`. |
| `BignamePhaseRunnerProgressMetricsMissing` | Either non-progress metric family is absent from a scrapeable runner that exports the heartbeat threshold for 2 minutes. | The runner image and rules are incompatible, so cursor-progress paging fails closed. |

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

A restart no longer costs Interpret a restore of a chain's whole retained
history: Ethereum mainnet, Ethereum Sepolia and Base all use the
[lookahead loader](../glossary.md#lookahead-loader), which reads only the
history each batch touches. A
runner that is OOM-killed or slow right after every start on such a chain is
paying for batch work, not restart cost. That work has two parts: the
batch's own block range, which `BIGNAME_INTERPRET_BLOCKS_PER_BATCH` bounds; and
the whole retained history of each name, resource and
[ENSv2 state key](../glossary.md#ensv2-state-key) that range touches, which a
smaller batch reduces only by touching fewer of them. Lower the batch size
first. If the cost stays high at one block per batch, remember that both loaders
still load and interpret that block's logs; tell this work apart from restoring
the retained history of the names and ENSv2 state keys it touches, and from
lookahead retries, before choosing a loader override. `BIGNAME_INTERPRET_FORCE_FULL_STATE_LOADER=true` pays the restore
once per start instead of once per batch, for every chain that runner serves.
A chain that logs `interpret chose its prior-state loader` with a full-state
reason, or a runner started with `BIGNAME_INTERPRET_FORCE_FULL_STATE_LOADER=true`,
still restores all retained history on start and after each reorg, with the
memory that implies.

## Phase cursor non-progress response

`phase_runner_phase_batches_since_cursor_advance` counts consecutive successful
work-bearing commits that the next durable resume confirms left the cursor
unchanged. `phase_runner_phase_cursor_stall_age_seconds` measures from the first
such commit. Both gauges use only `chain`, `phase`, and `mode`; `mode` is
`normal`, `redo`, or `recompute_flags`. Normal progress uses
`phase_runner_phase_current_block`; repair work uses
`phase_runner_redo_current_block` and `phase_runner_redo_mode`.

The cursor comparison includes block hashes. Ingest additionally compares its
sorted per-source next block, last processed block, and loaded redo boundary.
Thus a reorg, a hash replacement, or one Ingest source moving resets the
sequence even when the displayed summary block is unchanged. Target/head
movement does not reset it. A single Project boundary replay can reach count
`1` but cannot page. Caught-up Live polls that claim no cursor movement, no-head/empty completions, idle polls,
capacity pauses, and completed Verify revalidation clear or bypass the detector.
An idle poll clears earlier evidence, so pinned commits separated by idle polls
do not accumulate; this is accepted because an idle poll reports no indexing or
repair work. Evidence expires after the configured heartbeat-stale interval
without another successful work-bearing commit.
In-process retries of the same requested repair range retain accumulated evidence across attempt restarts; a different range or a process restart starts fresh.

With the checked-in 15-second rule interval, the two-minute hold pages no later
than 3 minutes after the third confirmed pinned completion. The age path
pages no later than 13 minutes after the second. A single long-running batch
still belongs to the heartbeat alert. Preserve the 13-minute two-batch bound by
configuring `--heartbeat-stale-after-secs` (or
`BIGNAME_PHASE_RUNNER_HEARTBEAT_STALE_AFTER_SECS`) to at least 900 seconds;
a lower expiry can clear that evidence before the age-based rule holds, leaving
the three-batch path as the remaining non-progress page.

When `BignamePhaseRunnerPhaseNonProgress` fires:

1. Record the alert's `chain`, `phase`, and `mode`.
2. Inspect both non-progress gauges, the applicable normal or repair current
   block, the target, recent phase logs, and provider quota/database use.
3. Confirm that the count is increasing or already at threshold while the
   durable cursor remains pinned.
4. If quota or database budget is at risk, stop only the phase runner with the
   exact Compose overlays deployed on the host:

   ```sh
   docker compose --env-file .env.server \
     -f docker-compose.server.yml \
     stop phase-runner
   ```

   Preserve every additional deployed `-f` overlay in the real command.
5. Do not edit `chain_phase_state`, repair fields, or Ingest cursors to silence
   the alert. Capture cursor/target hashes from logs, count, age, image SHA, and
   provider/database evidence before remediation.
6. After deploying the corrective change, resume with the same image and
   overlays:

   ```sh
   docker compose --env-file .env.server \
     -f docker-compose.server.yml \
     up -d phase-runner
   ```

7. Confirm both gauges return to zero and the applicable normal or repair
   cursor advances.

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

If the chain is configured again later, the runner resumes from preserved
cursors and re-runs any Ingest or Verify phase whose completion evidence is
incomplete, rather than trusting the rewritten `completed` status alone. It
resumes Ingest when the current block or live handoff does not match the target,
and resumes Verify unless it has a matching current/target block pair and a
[verification level](../glossary.md#verification-level). Settling an active
Ingest row clears its live handoff but preserves its source cursors, so re-adding
the chain resumes even if an older runner stopped between its formerly separate
summary and cursor writes.

## First-response checks

Keep diagnosis read-only until the failure is understood:

```sh
curl -fsS http://127.0.0.1:9465/metrics | \
  grep -E 'phase_runner_(phase_status|heartbeat_age_seconds|loop_heartbeat_age_seconds|head_lag_blocks|served_lag_blocks|served_publication_block|phase_batches_since_cursor_advance|phase_cursor_stall_age_seconds)'

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

For `BignamePhaseRunnerCapacityPaused`:

1. Find the runner warning `phase paused until storage capacity recovers` and
   read its `breach_reasons` and `free_disk_bytes` fields to identify which
   bound stopped work.
2. If `breach_reasons` contains `database_size`, review actual storage headroom
   before increasing `BIGNAME_PHASE_RUNNER_DATABASE_MAX_BYTES`. Server Compose
   forwards an explicitly configured ceiling; leaving it unset configures
   no ceiling. Empty, malformed or overflowing values are invalid;
   ceiling zero is a limit, not disabled protection. Recreate the runner after
   changing settings because they are read only at startup.
3. For `free_disk`, free space on the filesystem containing the configured
   `BIGNAME_PHASE_RUNNER_WRITABLE_PATH`. Follow the [capacity preflight](production-docker.md#capacity-preflight)
   to verify it is PostgreSQL's actual filesystem and is writable without
   exposing database files. The required server-Compose floor is operator-selected;
   missing/empty values fail rendering, while zero must be rejected operationally.
   The unchanged CLI still accepts zero. Do not derive a production reserve from
   an old host observation or lower the floor merely to clear an alert.
   The guard adds the preceding batch's reserved-write estimate to the floor;
   that estimate starts at zero for a new batch loop and is not a reservation.
   Capacity is checked before batches, not before all startup work or every write.
   Project estimates write bytes from the preceding family run's reported row
   count. Checks occur between bounded runs; they do not reserve the space that
   every block or rebuild range might need.
   Heartbeats and unrelated writes can continue during a pause, so this is not
   an absolute ENOSPC guarantee. A probe permission error is a retryable phase
   failure, not an ordinary capacity breach.
4. Once the capacity check clears, the phase resumes from its stored cursor.
   No phase-state edit or manual phase restart is needed.
