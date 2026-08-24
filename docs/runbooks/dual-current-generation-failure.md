# Mainnet dual-current projection-generation halt

This runbook is for the on-call operator responding when the
[Project phase](../glossary.md#projection-generation)
refuses to publish a Mainnet name because both its ENSv1 and ENSv2
[authority arms](../glossary.md#authority-epoch) still have a
current [surface binding](../glossary.md#surface-name-surface) after a proven,
activated [ENSv1→ENSv2 migration boundary](../glossary.md#migration-boundary)
has been reconciled through the end of the target block
([`crates/project/src/integrity.rs:33-50`](../../crates/project/src/integrity.rs#L33-L50)).
This is a
[projection generation failure](../glossary.md#projection-generation-failure):
Project rolls back the whole attempted publication, and the phase runner then
attempts to write durable evidence. The assertion runs before publication
([`crates/project/src/engine.rs:62-82`](../../crates/project/src/engine.rs#L62-L82)),
and only the phase runner writes the post-rollback audit
([`apps/phase-runner/src/project_phase.rs:143-163`](../../apps/phase-runner/src/project_phase.rs#L143-L163)).

Do not use this play for `dual_current_child_authority`, an ordinary database
error, or a Verify failure. Do not start recovery until the evidence-capture
steps below are complete.

## Failure signature

Condition 1 identifies this exact halt. Conditions 2 and 3 are the normal
durable corroboration; each has a separately handled write-failure exception.
Condition 4 describes the normal API symptom.

1. The phase-runner log has the terminal message
   `chain supervisor stopped after a terminal error`, with the affected
   `chain_id`, `error_kind=DataIntegrity`, and an `error` that begins with this
   exact text, with the bracketed values filled in:

   ```text
   chain [chain-id] name [logical-name-id] holds current bindings on both authority arms after its activated ENSv1→ENSv2 migration boundary; projection generation for block [target-block] is not publishable
   ```

   The assertion constructs that text and attaches the audit evidence at
   [`crates/project/src/integrity.rs:219-234`](../../crates/project/src/integrity.rs#L219-L234).
   A data-integrity error is non-retryable, so the chain supervisor stops rather
   than backing off and retrying
   ([`apps/phase-runner/src/runner.rs:193-232`](../../apps/phase-runner/src/runner.rs#L193-L232));
   the terminal structured log is emitted at
   [`apps/phase-runner/src/supervisor.rs:58-69`](../../apps/phase-runner/src/supervisor.rs#L58-L69).

2. `bigname_phase.chain_phase_state` has `phase_name = 'project'`,
   `phase_status = 'failed'`, and the same text in `last_error`. The failure
   transition sets those fields without replacing the last successfully
   published `current_block_number` or `current_block_hash`
   ([`apps/phase-runner/src/state.rs:370-407`](../../apps/phase-runner/src/state.rs#L370-L407)).

   Exception: after Project returns the invariant error, the runner separately
   records the failed phase. If that write fails, the terminal error appends
   `; additionally failed to record phase failure: [database error]`; the phase
   row can retain its preceding status and `/v2/status` need not yet show the
   direct `stale` mapping below. Capture the terminal error and any audit row,
   file the incident, and hold the chain. Do not edit the phase row or begin a
   redo while its durable state is unknown
   ([`apps/phase-runner/src/runner.rs:417-433`](../../apps/phase-runner/src/runner.rs#L417-L433),
   [`apps/phase-runner/src/error.rs:60-69`](../../apps/phase-runner/src/error.rs#L60-L69)).

3. `bigname_phase.project_generation_failures` has
   `failure_kind = 'dual_current_exact_name_authority'` for the same chain,
   target block, and logical name. The row includes the two binding/resource
   pairs and the activated boundary position
   ([`crates/project/src/integrity.rs:159-194`](../../crates/project/src/integrity.rs#L159-L194)).
   It is append-only, and retrying the same conflict does not add a duplicate
   ([`apps/phase-runner/src/project_failure_audit.rs:6-24`](../../apps/phase-runner/src/project_failure_audit.rs#L6-L24)).

   Exception: if the audit insert itself fails, the terminal error and
   `last_error` keep the exact primary text above and append
   `; additionally failed to record projection generation failure: [database error]`.
   The Project phase still fails with `DataIntegrity`, but a new audit row is
   not guaranteed. Capture the complete secondary database error, query for a
   row without assuming one exists, file the incident, and hold the chain; do
   not infer evidence or begin a redo. The
   runner deliberately preserves the primary error when adding the audit-write
   failure
   ([`apps/phase-runner/src/project_phase.rs:143-163`](../../apps/phase-runner/src/project_phase.rs#L143-L163),
   [`apps/phase-runner/src/error.rs:60-69`](../../apps/phase-runner/src/error.rs#L60-L69)).

4. `GET /v2/status` reports the Mainnet chain (`data.chains["1"]`) as
   `status: "stale"` and keeps `indexed_block` at the most recent successful
   Project publication. A failed Project phase maps directly to `stale`
   ([`apps/api/src/v2/status.rs:175-226`](../../apps/api/src/v2/status.rs#L175-L226)).
   The route does **not** return `last_error`, the failure kind, or the audit
   payload; its response shape contains only aggregate and per-chain readiness
   fields ([`apps/api/src/v2/status.rs:18-42`](../../apps/api/src/v2/status.rs#L18-L42)).

Distinguish this halt from other Project failures by the exact primary message
and, normally, the failed Project row and an audit row with the exact-name
failure kind. Either documented secondary write failure stops at
evidence-preserving escalation. Other Project errors carry no
projection-generation evidence, so the runner does not call the audit writer for them
([`apps/phase-runner/src/project_phase.rs:143-156`](../../apps/phase-runner/src/project_phase.rs#L143-L156)).
The same table also admits `dual_current_child_authority`; that different kind
is outside this runbook and must be escalated to the Project owner
([`schema-v2/baseline/12_project_generation_failures.sql:19-22`](../../schema-v2/baseline/12_project_generation_failures.sql#L19-L22)).

## Stop and capture evidence

Keep the affected chain's long-running phase runner stopped. Before any redo,
restart, deploy, or code change, attach all of the following to the incident:

- the complete terminal log entry, including `chain_id`, `error_kind`, and exact
  error text;
- the phase-runner startup metadata log containing the build SHA and
  [interpreter content hash](../glossary.md#interpreter-content-hash), plus the
  immutable container image ID. The startup log emits those fields at
  [`apps/phase-runner/src/main.rs:73-79`](../../apps/phase-runner/src/main.rs#L73-L79);
- both the Interpret and Project phase rows, including their recorded heads,
  redo ranges, redo modes, `last_error`, and timestamps, plus any active
  [manifest-authority marker](../glossary.md#manifest-authority-marker)
  invalidation token and its reviewed coverage decision;
- every exact-name audit row for the affected chain and logical name, including
  the complete JSON evidence and failure fingerprint;
- the boundary event identity, ENSv1 and ENSv2 binding IDs, resource IDs, block
  hashes, block/transaction/log positions, current lineage states, ENSv1→ENSv2
  migration correlation IDs, and transaction hashes; and
- the raw transaction, receipt, and logs for every captured transaction hash.
  Preserve log order and emitting addresses.

The audit schema makes the chain/target/hash/interpreter/failure/fingerprint key
and the evidence payload durable
([`schema-v2/baseline/12_project_generation_failures.sql:1-25`](../../schema-v2/baseline/12_project_generation_failures.sql#L1-L25)).
Only one conflict is returned by one failed projection generation: the assertion
orders logical names and takes the first one
([`crates/project/src/integrity.rs:192-194`](../../crates/project/src/integrity.rs#L192-L194)).
After that name is repaired, another conflicting name can therefore be the next
failure at the same target. Do not declare the chain clean from one repaired row.

Run the following in a read-only `psql` session against `bigname_phase`. Replace
the sample values; never paste credentials into the incident.

```sql
BEGIN TRANSACTION READ ONLY;

\set chain_id 'ethereum-mainnet'

SELECT chain_id,
       phase_name,
       phase_status,
       current_block_number,
       current_block_hash,
       target_block_number,
       target_block_hash,
       input_content_hash,
       redo_in_progress,
       redo_mode,
       redo_from_block_number,
       redo_to_block_number,
       last_error,
       started_at,
       finished_at,
       updated_at
FROM bigname_phase.chain_phase_state
WHERE chain_id = :'chain_id'
  AND phase_name IN ('interpret', 'project')
ORDER BY phase_name;

SELECT audit.chain_id,
       audit.phase_name,
       audit.generation_token AS active_redo_invalidation_token,
       audit.authority_fingerprint,
       audit.redo_from_block_number,
       audit.redo_to_block_number,
       audit.attested_by,
       audit.attested_at
FROM bigname_phase.manifest_authority_attestations audit
JOIN bigname_phase.chain_phase_state phase
  ON phase.chain_id = audit.chain_id
 AND phase.phase_name = audit.phase_name
 AND phase.redo_in_progress
 AND phase.redo_from_block_number = audit.redo_from_block_number
 AND phase.redo_to_block_number = audit.redo_to_block_number
 AND phase.started_at = audit.attested_at
WHERE audit.chain_id = :'chain_id'
ORDER BY audit.attested_at DESC, audit.generation_token;

SELECT chain_id,
       target_block_number,
       target_block_hash,
       interpreter_content_hash,
       failure_kind,
       failure_fingerprint,
       logical_name_id,
       detected_at,
       jsonb_pretty(evidence) AS evidence
FROM bigname_phase.project_generation_failures
WHERE chain_id = :'chain_id'
  AND failure_kind = 'dual_current_exact_name_authority'
ORDER BY detected_at DESC, logical_name_id;

ROLLBACK;
```

Select the row being investigated by its complete audit key for the remaining
queries. The same semantic conflict can have the same fingerprint at more than
one target, so the fingerprint alone is not a row selector.

```sql
BEGIN TRANSACTION READ ONLY;

\set chain_id 'ethereum-mainnet'
\set target_block 12345678
\set target_hash '0xreplace-with-target-block-hash'
\set interpreter_hash 'keccak256:replace-with-audit-row-hash'
\set failure_fingerprint 'replace-with-64-lowercase-hex-characters'

WITH failure AS (
    SELECT *
    FROM bigname_phase.project_generation_failures
    WHERE chain_id = :'chain_id'
      AND target_block_number = :target_block
      AND target_block_hash = :'target_hash'
      AND interpreter_content_hash = :'interpreter_hash'
      AND failure_kind = 'dual_current_exact_name_authority'
      AND failure_fingerprint = :'failure_fingerprint'
), boundary AS (
    SELECT event.*
    FROM failure
    JOIN bigname_phase.normalized_events event
      ON event.chain_id = failure.chain_id
     AND event.event_identity = failure.evidence #>> '{boundary,event_identity}'
     AND event.block_number =
         (failure.evidence #>> '{boundary,block_number}')::bigint
     AND event.block_hash = failure.evidence #>> '{boundary,block_hash}'
     AND COALESCE(event.transaction_index, -1) =
         (failure.evidence #>> '{boundary,transaction_index}')::bigint
     AND COALESCE(event.log_index, -1) =
         (failure.evidence #>> '{boundary,log_index}')::bigint
)
SELECT failure.chain_id,
       failure.logical_name_id,
       failure.target_block_number,
       failure.target_block_hash,
       failure.evidence #>> '{target,canonicality_state}'
           AS target_canonicality_at_failure,
       target_lineage.canonicality_state AS target_canonicality_now,
       boundary.event_identity AS boundary_event_identity,
       boundary.event_kind AS boundary_event_kind,
       boundary.consumer_visibility AS boundary_visibility,
       boundary.migration_correlation_ids,
       boundary.block_number AS boundary_block_number,
       boundary.block_hash AS boundary_block_hash,
       boundary.transaction_hash AS boundary_transaction_hash,
       boundary.transaction_index AS boundary_transaction_index,
       boundary.log_index AS boundary_log_index,
       boundary.canonicality_state AS boundary_row_canonicality,
       boundary_lineage.canonicality_state AS boundary_lineage_now,
       jsonb_pretty(boundary.after_state) AS boundary_after_state
FROM failure
LEFT JOIN bigname_phase.chain_lineage target_lineage
  ON target_lineage.chain_id = failure.chain_id
 AND target_lineage.block_number = failure.target_block_number
 AND target_lineage.block_hash = failure.target_block_hash
LEFT JOIN boundary ON TRUE
LEFT JOIN bigname_phase.chain_lineage boundary_lineage
  ON boundary_lineage.chain_id = boundary.chain_id
 AND boundary_lineage.block_number = boundary.block_number
 AND boundary_lineage.block_hash = boundary.block_hash;

ROLLBACK;
```

Read the two stable binding IDs named by the audit and compare their current
rows with the captured audit fields. Capture this before any recovery action.
The audit stores the binding ID, resource ID, block number, position, and
canonicality observed at failure, but not the binding's block hash
([`crates/project/src/integrity.rs:159-184`](../../crates/project/src/integrity.rs#L159-L184)).
After a reorg and redo, Interpret can reanchor an orphaned binding under the
same stable ID by replacing its block hash and provenance
([`crates/interpret/src/write/identity.rs:264-310`](../../crates/interpret/src/write/identity.rs#L264-L310)).
The query therefore describes the current row and reports whether its non-fork
fields still match the audit; it does not reconstruct an old witness fork. The
`current_row_open_at_failed_target_time` expression mirrors the assertion's
end-of-target-block cutoff
([`crates/project/src/integrity.rs:46-50`](../../crates/project/src/integrity.rs#L46-L50),
[`crates/project/src/integrity.rs:87-126`](../../crates/project/src/integrity.rs#L87-L126)).

```sql
BEGIN TRANSACTION READ ONLY;

\set chain_id 'ethereum-mainnet'
\set target_block 12345678
\set target_hash '0xreplace-with-target-block-hash'
\set interpreter_hash 'keccak256:replace-with-audit-row-hash'
\set failure_fingerprint 'replace-with-64-lowercase-hex-characters'

WITH failure AS (
    SELECT *
    FROM bigname_phase.project_generation_failures
    WHERE chain_id = :'chain_id'
      AND target_block_number = :target_block
      AND target_block_hash = :'target_hash'
      AND interpreter_content_hash = :'interpreter_hash'
      AND failure_kind = 'dual_current_exact_name_authority'
      AND failure_fingerprint = :'failure_fingerprint'
), target_time AS (
    SELECT lineage.block_timestamp + interval '1 second' AS cutoff
    FROM failure
    JOIN bigname_phase.chain_lineage lineage
      ON lineage.chain_id = failure.chain_id
     AND lineage.block_number = failure.target_block_number
     AND lineage.block_hash = failure.target_block_hash
), witness AS (
    SELECT failure.chain_id, side, payload
    FROM failure
    CROSS JOIN LATERAL (
        VALUES
            ('predecessor'::text, failure.evidence -> 'predecessor'),
            ('successor'::text, failure.evidence -> 'successor')
    ) AS sides(side, payload)
)
SELECT witness.side,
       witness.payload ->> 'surface_binding_id' AS audit_binding_id,
       witness.payload ->> 'resource_id' AS audit_resource_id,
       (witness.payload ->> 'block_number')::bigint AS audit_block_number,
       (witness.payload ->> 'transaction_index')::bigint
           AS audit_transaction_index,
       (witness.payload ->> 'log_index')::bigint AS audit_log_index,
       witness.payload ->> 'canonicality_state'
           AS audit_canonicality_at_failure,
       binding.authority_arm,
       binding.surface_binding_id,
       binding.logical_name_id,
       binding.resource_id,
       binding.binding_kind,
       binding.active_from,
       binding.active_to,
       binding.block_number,
       binding.block_hash,
       binding.provenance,
       binding.canonicality_state AS binding_row_canonicality,
       binding_lineage.canonicality_state AS binding_lineage_now,
       resource.canonicality_state AS resource_row_canonicality,
       resource_lineage.canonicality_state AS resource_lineage_now,
       binding.resource_id = (witness.payload ->> 'resource_id')::uuid
           AND binding.block_number =
               (witness.payload ->> 'block_number')::bigint
           AND COALESCE((binding.provenance ->> 'transaction_index')::bigint, -1) =
               (witness.payload ->> 'transaction_index')::bigint
           AND COALESCE((binding.provenance ->> 'log_index')::bigint, -1) =
               (witness.payload ->> 'log_index')::bigint
           AS current_row_matches_audit_nonfork_fields,
       binding.active_from < target_time.cutoff
           AND (binding.active_to IS NULL OR binding.active_to >= target_time.cutoff)
           AS current_row_open_at_failed_target_time
FROM witness
LEFT JOIN bigname_phase.surface_bindings binding
  ON binding.chain_id = witness.chain_id
 AND binding.surface_binding_id =
     (witness.payload ->> 'surface_binding_id')::uuid
LEFT JOIN bigname_phase.resources resource
  ON resource.chain_id = binding.chain_id
 AND resource.resource_id = binding.resource_id
LEFT JOIN bigname_phase.chain_lineage binding_lineage
  ON binding_lineage.chain_id = binding.chain_id
 AND binding_lineage.block_number = binding.block_number
 AND binding_lineage.block_hash = binding.block_hash
LEFT JOIN bigname_phase.chain_lineage resource_lineage
  ON resource_lineage.chain_id = resource.chain_id
 AND resource_lineage.block_number = resource.block_number
 AND resource_lineage.block_hash = resource.block_hash
CROSS JOIN target_time
ORDER BY witness.side;

ROLLBACK;
```

Finally, capture the complete normalized-event, raw-transaction, receipt, and
raw-log evidence for every implicated transaction. This includes the whole
boundary transaction, not just the boundary position: predecessor cleanup can
occur earlier in that transaction. The boundary is pinned to the block hash in
the audit. Because each binding witness lacks a recorded block hash, the query
intentionally enumerates every raw-log candidate at its recorded block,
transaction, and log position across all retained forks. From those candidates
onward, every transaction, event, receipt, and log join includes `block_hash`.
If one witness has candidates on multiple block hashes, or none, preserve that
result as ambiguity and file-and-hold; do not select a fork by current binding
state.
A [normalized event](../glossary.md#normalized-event) carries the transaction
hash, position, ENSv1→ENSv2 migration correlation IDs, and before/after state
([`schema-v2/baseline/05_normalized_events.sql:19-42`](../../schema-v2/baseline/05_normalized_events.sql#L19-L42));
the immutable raw tables are keyed by chain and block hash as well as transaction
identity
([`schema-v2/baseline/02_raw_facts.sql:1-29`](../../schema-v2/baseline/02_raw_facts.sql#L1-L29),
[`schema-v2/baseline/02_raw_facts.sql:77-107`](../../schema-v2/baseline/02_raw_facts.sql#L77-L107)).

```sql
BEGIN TRANSACTION READ ONLY;

\set chain_id 'ethereum-mainnet'
\set target_block 12345678
\set target_hash '0xreplace-with-target-block-hash'
\set interpreter_hash 'keccak256:replace-with-audit-row-hash'
\set failure_fingerprint 'replace-with-64-lowercase-hex-characters'

WITH failure AS (
    SELECT *
    FROM bigname_phase.project_generation_failures
    WHERE chain_id = :'chain_id'
      AND target_block_number = :target_block
      AND target_block_hash = :'target_hash'
      AND interpreter_content_hash = :'interpreter_hash'
      AND failure_kind = 'dual_current_exact_name_authority'
      AND failure_fingerprint = :'failure_fingerprint'
), witness AS (
    SELECT failure.chain_id,
           failure.logical_name_id,
           side,
           payload
    FROM failure
    CROSS JOIN LATERAL (
        VALUES
            ('predecessor'::text, failure.evidence -> 'predecessor'),
            ('successor'::text, failure.evidence -> 'successor')
    ) AS sides(side, payload)
), evidence_transactions AS (
    SELECT 'boundary'::text AS evidence_source,
           event.chain_id,
           event.block_number,
           event.block_hash,
           event.transaction_hash,
           event.transaction_index
    FROM failure
    JOIN bigname_phase.normalized_events event
      ON event.chain_id = failure.chain_id
     AND event.event_identity = failure.evidence #>> '{boundary,event_identity}'
     AND event.block_number =
         (failure.evidence #>> '{boundary,block_number}')::bigint
     AND event.block_hash = failure.evidence #>> '{boundary,block_hash}'
     AND COALESCE(event.transaction_index, -1) =
         (failure.evidence #>> '{boundary,transaction_index}')::bigint
     AND COALESCE(event.log_index, -1) =
         (failure.evidence #>> '{boundary,log_index}')::bigint
    WHERE event.transaction_hash IS NOT NULL

    UNION ALL

    SELECT witness.side,
           raw_log.chain_id,
           raw_log.block_number,
           raw_log.block_hash,
           raw_log.transaction_hash,
           raw_log.transaction_index
    FROM witness
    JOIN bigname_phase.raw_logs raw_log
      ON raw_log.chain_id = witness.chain_id
     AND raw_log.block_number =
         (witness.payload ->> 'block_number')::bigint
     AND raw_log.transaction_index =
         (witness.payload ->> 'transaction_index')::bigint
     AND raw_log.log_index =
         (witness.payload ->> 'log_index')::bigint
), transactions AS (
    SELECT chain_id,
           block_number,
           block_hash,
           transaction_hash,
           transaction_index,
           array_agg(DISTINCT evidence_source ORDER BY evidence_source)
               AS evidence_sources
    FROM evidence_transactions
    GROUP BY chain_id, block_number, block_hash, transaction_hash,
             transaction_index
)
SELECT evidence_transaction.evidence_sources,
       evidence_transaction.chain_id,
       evidence_transaction.block_number,
       evidence_transaction.block_hash,
       evidence_transaction.transaction_hash,
       evidence_transaction.transaction_index,
       to_jsonb(raw_transaction) AS raw_transaction,
       to_jsonb(raw_receipt) AS raw_receipt,
       COALESCE((
           SELECT jsonb_agg(to_jsonb(event)
                            ORDER BY event.transaction_index,
                                     event.log_index,
                                     event.normalized_event_id)
           FROM bigname_phase.normalized_events event
           WHERE event.chain_id = evidence_transaction.chain_id
             AND event.block_number = evidence_transaction.block_number
             AND event.block_hash = evidence_transaction.block_hash
             AND event.transaction_hash = evidence_transaction.transaction_hash
             AND event.transaction_index = evidence_transaction.transaction_index
       ), '[]'::jsonb) AS normalized_events,
       COALESCE((
           SELECT jsonb_agg(to_jsonb(raw_log)
                            ORDER BY raw_log.log_index)
           FROM bigname_phase.raw_logs raw_log
           WHERE raw_log.chain_id = evidence_transaction.chain_id
             AND raw_log.block_number = evidence_transaction.block_number
             AND raw_log.block_hash = evidence_transaction.block_hash
             AND raw_log.transaction_hash = evidence_transaction.transaction_hash
             AND raw_log.transaction_index = evidence_transaction.transaction_index
       ), '[]'::jsonb) AS raw_logs
FROM transactions evidence_transaction
JOIN bigname_phase.raw_transactions raw_transaction
  ON raw_transaction.chain_id = evidence_transaction.chain_id
 AND raw_transaction.block_number = evidence_transaction.block_number
 AND raw_transaction.block_hash = evidence_transaction.block_hash
 AND raw_transaction.transaction_hash = evidence_transaction.transaction_hash
 AND raw_transaction.transaction_index = evidence_transaction.transaction_index
LEFT JOIN bigname_phase.raw_receipts raw_receipt
  ON raw_receipt.chain_id = evidence_transaction.chain_id
 AND raw_receipt.block_number = evidence_transaction.block_number
 AND raw_receipt.block_hash = evidence_transaction.block_hash
 AND raw_receipt.transaction_hash = evidence_transaction.transaction_hash
 AND raw_receipt.transaction_index = evidence_transaction.transaction_index
ORDER BY evidence_transaction.block_number,
         evidence_transaction.transaction_index;

ROLLBACK;
```

## Classify the cause

First rule out a historical audit row. If the captured target or boundary hash
is now `orphaned`, or Project has since completed beyond that target without the
same `last_error`, retain the row as incident history but do not recover from it.
Audit rows intentionally survive reorgs and later success
([`apps/phase-runner/src/project_failure_audit.rs:6-10`](../../apps/phase-runner/src/project_failure_audit.rs#L6-L10)).
If a reorg or redo already occurred and either binding's current non-fork fields
do not match the audit, or its raw position has zero or multiple fork candidates,
the historical audit is insufficient to reconstruct that binding's old fork.
File the ambiguity and hold rather than classifying from the current row.

For a live failure, use this decision table. If the evidence does not establish
one row unambiguously, file the evidence and hold; do not choose the least costly
recovery by guesswork.

| Cause | Evidence | Classification |
| --- | --- | --- |
| Missed or mis-applied ENSv1→ENSv2 migration interpretation | The activated `MigrationApplied` row does not match the raw transaction, correlation group, exact predecessor cleanup, or successor binding; or it matches but the current interpreter did not close the one selected ENSv1 predecessor and no approved re-derivation is pending. | Indexing defect. Interpret validates a one-to-one activated boundary/transition pair ([`crates/interpret/src/write/identity/transition.rs:11-45`](../../crates/interpret/src/write/identity/transition.rs#L11-L45)) and updates exactly one selected predecessor binding ([`crates/interpret/src/write/identity/transition.rs:141-270`](../../crates/interpret/src/write/identity/transition.rs#L141-L270)). A mismatch means the stored interpretation or that logic is wrong. |
| Invariant is wrong for genuine on-chain behavior | The raw transaction, activated boundary, exact cleanup, successor, both bindings, and current lineage are all internally consistent, and the ENS protocol owner confirms that both bindings are legitimately current after that boundary. | On-chain assumption defect. Do not relabel it as stale data just because a redo would be convenient. The assertion deliberately treats two open arms after this proof as unpublishable ([`crates/project/src/integrity.rs:33-38`](../../crates/project/src/integrity.rs#L33-L38), [`crates/project/src/integrity.rs:127-135`](../../crates/project/src/integrity.rs#L127-L135)). |
| Stale derived state awaiting re-derivation | The current deployed interpreter is already known to produce the correct arm-scoped close, and the Interpret row shows the exact approved redo still pending or interrupted, or the deployment record already requires the affected full [re-derivation boundary](../glossary.md#re-derivation-boundary). | Operationally incomplete re-derivation, not a new protocol claim. A bounded Interpret redo deletes and re-derives its selected event range from [readable](../glossary.md#readable--read-safe) [raw facts](../glossary.md#raw-facts) ([`docs/glossary.md:1165-1171`](../glossary.md#glossary)). |

The ENSv1→ENSv2 migration transition writer is intentionally arm-scoped: it
selects the predecessor by `logical_name_id` and the recorded predecessor arm,
then closes that exact row
([`crates/interpret/src/write/identity/transition.rs:141-161`](../../crates/interpret/src/write/identity/transition.rs#L141-L161),
[`crates/interpret/src/write/identity/transition.rs:259-270`](../../crates/interpret/src/write/identity/transition.rs#L259-L270)).
This is why recovery re-derives evidence instead of editing the open interval.

## Recovery

### Stale derived state: complete the approved redo

1. Follow the production runner controls before selecting a redo: drain public
   traffic, record the exact Compose file set, capture the immutable running
   image as `<recovery-image>`, and stop the long-running phase runner. A missing
   deployment-specific drain procedure is a stop condition
   ([`docs/runbooks/production-docker.md:396-411`](production-docker.md#recovery-plays),
   [`docs/runbooks/production-docker.md:419-434`](production-docker.md#stop-and-escalate-an-interpreter-mismatch)).
2. If an Interpret redo is already recorded, resume **that exact chain and
   persisted range**. Do not create a new range. If the evidence query returns
   an `active_redo_invalidation_token`, require the already-reviewed historical
   fetch or the decision that the
   [watch plan](../glossary.md#watch-plan--watched-tuple) did not widen, and pass
   that exact token as
   `--attest-watch-set-coverage <recorded-active-redo-invalidation-token>`.
   Omit the flag when no active attestation row exists. Never invent,
   substitute, or reuse a token; the runner binds it to the exact active redo
   ([`apps/phase-runner/src/redo_manifest_audit.rs:10-26`](../../apps/phase-runner/src/redo_manifest_audit.rs#L10-L26),
   [`apps/phase-runner/src/redo_manifest_audit.rs:116-145`](../../apps/phase-runner/src/redo_manifest_audit.rs#L116-L145)).

   If Interpret has no marker, Project alone has a persisted redo marker, and
   the captured before/after phase rows and one-shot record prove it is the
   downstream marker from the approved Interpret redo, the process was
   interrupted during the durable Interpret-to-Project handoff.
   Resume only the exact persisted Project range, then continue at step 6:

   ```sh
   BIGNAME_IMAGE=<recovery-image> \
     docker compose --env-file .env.server \
     <compose-files> run --rm --pull never phase-runner \
     phase-runner redo --chain ethereum-mainnet --phase project \
     --from-block <persisted-project-redo-from> \
     --to-block <persisted-project-redo-to>
   ```

   Project redo syntax is documented at
   [`docs/storage.md:431-439`](../storage.md#rainbow-table-preimage-import). Interpret completion
   stamps that Project range atomically, but the runner invokes Project in a
   separate call
   ([`apps/phase-runner/src/redo_state.rs:571-579`](../../apps/phase-runner/src/redo_state.rs#L571-L579),
   [`apps/phase-runner/src/runner_operator_redo.rs:350-363`](../../apps/phase-runner/src/runner_operator_redo.rs#L350-L363)).
   This exact-marker continuation is not a new Project-only repair. If the
   marker's origin or persisted range cannot be proved, file-and-hold.
3. If neither Interpret nor Project has a redo marker, require the incident
   owner to identify the earliest
   affected stored block and approve the range through the recorded Interpret
   head. If that start cannot be established, skip the scoped play and escalate
   to the full [re-derivation boundary](../glossary.md#re-derivation-boundary)
   below. For the command below, `<redo-from>` and `<redo-to>` mean either these
   newly approved bounds or the exact persisted bounds from step 2.
4. Copy every source descriptor for the affected chain exactly from the deployed
   configuration, repeating `--source` once per descriptor, and run the
   documented bounded Interpret redo in the pinned recovery image:

   ```sh
   BIGNAME_IMAGE=<recovery-image> \
     docker compose --env-file .env.server \
     <compose-files> run --rm --pull never phase-runner \
     phase-runner redo --chain ethereum-mainnet --phase interpret \
     --from-block <redo-from> \
     --to-block <redo-to> \
     --source <affected-chain-source> \
     [--source <additional-affected-chain-source> ...] \
     [--attest-watch-set-coverage <recorded-active-redo-invalidation-token>]
   ```

   The source descriptors prove that the persisted raw-data range needed by the
   redo is present; they do not authorize new ingestion. Preserve the deployed
   `BIGNAME_PHASE_RUNNER_HYDRATION_RPC_URLS` value, or its exact
   `--hydration-rpc` equivalent, because the automatic Project cascade may need
   it. The canonical command and descriptor format are documented at
   [`docs/runbooks/production-docker.md:477-496`](production-docker.md#stop-and-escalate-an-interpreter-mismatch).
   If a newly started scoped, unflagged redo is fenced by a
   [manifest-authority marker](../glossary.md#manifest-authority-marker), its
   first error is `ContentHashMismatch`: it demands the full range
   `0..=<redo-to>`, does not name the marker, and prints no token
   ([`apps/phase-runner/src/redo_state.rs:377-386`](../../apps/phase-runner/src/redo_state.rs#L377-L386)).
   That runner error-text gap is tracked in
   [issue #545](https://github.com/ensdomains/bigname/issues/545). The
   invalidation token is already visible in the captured Interpret phase row's
   `input_content_hash` marker value as
   `manifest-authority:<authority-fingerprint>:<invalidation-token>`. Do not use
   it to retry the scoped range. Instead, run the Interpret redo unflagged over
   the demanded full range `0..=<redo-to>`. That full-range attempt reaches the
   manifest-authority fence, stops, and prints the invalidation token
   ([`apps/phase-runner/src/redo_manifest_attestation.rs:70-85`](../../apps/phase-runner/src/redo_manifest_attestation.rs#L70-L85)).
   Complete the mandatory historical fetch for any widened watch plan, or the
   required review proving no widening, then rerun that same full range with
   `--attest-watch-set-coverage <token>`. The production rule and its
   no-invention/no-reuse constraints are at
   [`docs/runbooks/production-docker.md:272-282`](production-docker.md#planned-migration-and-fingerprint-boundary).
   There is no `--arm` option. The redo uses each closing event's recorded arm
   and refuses to guess one when evidence is missing
   ([`crates/adapters/src/schema_v2/seam.rs:71-99`](../../crates/adapters/src/schema_v2/seam.rs#L71-L99)).
5. Let the command's automatic Project cascade run. A successful Interpret redo
   executes the Project redo stamped for the required range
   ([`apps/phase-runner/src/runner_operator_redo.rs:343-363`](../../apps/phase-runner/src/runner_operator_redo.rs#L343-L363)).
6. Require the one-shot command to succeed and both redo markers to be clear,
   then restart the long-running runner under the production procedure with the
   same image and exact Compose file set. Do **not** require Project to be
   `completed` before restart: redo completion restores the lifecycle status
   that preceded the redo, which is `failed` for this incident
   ([`apps/phase-runner/src/redo_completion.rs:9-19`](../../apps/phase-runner/src/redo_completion.rs#L9-L19),
   [`apps/phase-runner/src/redo_state.rs:498-520`](../../apps/phase-runner/src/redo_state.rs#L498-L520)).
7. After restart, require Interpret and Project to report `completed`, confirm
   Project advances past the blocked target, and confirm `/v2/status` is no
   longer stale for this cause. Restore traffic only after the production health
   gates pass.

If the same fingerprint reproduces, stop. Do not widen or repeat the scoped redo.
Escalate to the boundary play, matching the existing runner instruction
([`docs/runbooks/production-docker.md:498-516`](production-docker.md#stop-and-escalate-an-interpreter-mismatch)).

### Indexing defect: file, fix, then re-derive

File a bigname GitHub issue containing the complete evidence bundle and link it
from the active incident and issue
[#494](https://github.com/ensdomains/bigname/issues/494). Name the owning
[source family](../glossary.md#source-family), boundary event identity,
correlation ID, and first affected block. Keep
the chain held at the last successful Project publication until an Interpret or
manifest owner confirms one of these reviewed outcomes:

- the deployed code already contains the correction and the defect is confined
  to a known range: run the scoped Interpret redo above; or
- the correction changes interpretation semantics, source
  [admission](../glossary.md#admission), or the required historical
  fetch: use its approved re-derivation-boundary plan.

If the scoped redo reproduces the conflict, attach the new log and audit result
to the issue and escalate to the boundary plan. Do not keep retrying.

### Wrong invariant: file and hold for a code fix

Do not redo. Replaying an invalid assumption cannot make the publication safe.
File a bigname GitHub issue linked from the incident and #494, tag the Project
and ENS protocol owners, and include the raw transaction sequence plus the
specific upstream behavior that contradicts the assertion. Keep the chain held
until a reviewed code-and-contract change lands with its own re-derivation
decision. There is no operator override or skip path.

### Escalate to the full boundary play

Use the planned full re-derivation procedure when the earliest affected block
cannot be proved, an interpreter content hash changed, source admission or the
[watch plan](../glossary.md#watch-plan--watched-tuple) widened, or the approved scoped redo
reproduces the failure. This
runbook does not authorize an improvised reset. Hand the evidence to the release,
Interpret, manifest, and storage owners and follow
[`docs/runbooks/production-docker.md:31-304`](production-docker.md#planned-migration-and-fingerprint-boundary),
including its fixed image, backup, historical fetch, full Interpret/Project walk,
Verify, and readiness gates.

## Prohibited actions

Never:

- edit `chain_phase_state` to mark Project completed, clear `last_error`, remove
  a redo marker, or advance a recorded head;
- update or delete `surface_bindings`, normalized events, resources, raw facts,
  or any Project projection row;
- delete or alter `project_generation_failures` rows;
- create a new Project-only redo and call the input repaired—the only sanctioned
  Project-only command here completes the exact downstream marker already
  stamped by a proven successful Interpret redo; or
- skip the name, suppress the assertion, or publish a partial projection
  generation.

The audit table is operator diagnostics rather than a product projection
([`schema-v2/baseline/12_project_generation_failures.sql:28-32`](../../schema-v2/baseline/12_project_generation_failures.sql#L28-L32)),
and Project is the sole projection writer
([`docs/glossary.md:1183-1189`](../glossary.md#glossary)). The existing
production recovery rules also prohibit hand-editing identity and normalized
event rows
([`docs/runbooks/production-docker.md:524-525`](production-docker.md#stop-and-escalate-an-interpreter-mismatch)).

## Sepolia carve-out

This exact-name assertion is Mainnet-scoped by the staged
[deployment profile](../glossary.md#deployment-profile):
it requires `deployment_profile = 'mainnet'` and an activated
`migration_authority_transition` proof
([`crates/project/src/integrity.rs:127-135`](../../crates/project/src/integrity.rs#L127-L135)).
That scope is deliberate. Ethereum Sepolia carries distinct ENSv1 and ENSv2
test deployments on the same chain: the pinned ENSv1 registry is
`0x00000000000C2E074eC69A0dFb2997BA6C7d2e1e`
(upstream: .refs/ens_v1/deployments/sepolia/ENSRegistry.json:L2 @ ens_v1@91c966f),
while the pinned ENSv2 RootRegistry and ETHRegistry are
`0x11b5bfbe9078d826b1edbdd1cfc12f5828d9f50c` and
`0x67b728a792e789a8978b30cf1b3b641f19354b43`
(upstream: .refs/ens_v2/contracts/deployments/sepolia/RootRegistry.json:L2 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/deployments/sepolia/ETHRegistry.json:L2 @ ens_v2@ccaeb58).

Genuine Sepolia overlap therefore means that the same logical name has readable
ENSv1 and ENSv2 evidence from those independent deployments **without** an
activated `MigrationApplied` boundary connecting that name. Project leaves that
shape unsupported with `independent_ens_deployments_overlap`; it does not raise
this halt
([`crates/project/src/builders/name_authority.rs:495-503`](../../crates/project/src/builders/name_authority.rs#L495-L503),
[`crates/project/src/builders/name_authority.rs:617-628`](../../crates/project/src/builders/name_authority.rs#L617-L628)).
Do not interpret mere cross-era Sepolia evidence as a missed ENSv1→ENSv2
migration.

An actual unlocked ENSv1→ENSv2 migration transaction is stronger evidence, but
its two entry paths must not be conflated. The registrar-token path reclaims the
token, replaces the ENSv1 registry record, transfers the token to the Graveyard,
and injects the ENSv2 registration
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L111-L119 @ ens_v2@ccaeb58).
The unlocked-wrapped path instead clears the wrapper resolver, unwraps the name
to the Graveyard, and then performs the same ENSv2 injection
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L146-L160 @ ens_v2@ccaeb58).
Only a proven per-name boundary derived from such admitted evidence connects the
two arms. If Sepolia has that proof and an open predecessor, capture and file it
as an indexing or authority-selection defect; do not label it the Mainnet
`dual_current_exact_name_authority` emergency, because the assertion cannot
produce that failure under the Sepolia deployment profile.

## Closure and escalation record

Keep the incident and linked bigname issue updated with:

- the immutable audit row and whether its target/boundary hashes are still
  readable or are now orphaned;
- every boundary, binding, resource, correlation, and transaction identifier;
- the classification decision and the owners who approved it;
- the exact redo range or re-derivation-boundary record, image/build SHA, and
  interpreter content hash; and
- the post-recovery phase rows, new audit query result, and `/v2/status` result.

Close the incident only after the repaired target publishes, the affected chain
advances beyond it, no exact-name failure with new evidence appears on the next
attempt, all required redo markers are clear, and the linked defect or
on-chain-assumption issue records the durable resolution. Leave all audit rows
in place.
