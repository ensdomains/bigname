use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use bigname_storage::{CURRENT_PROJECT_PUBLICATION_JOIN, DEFAULT_RESOLVER_CURRENT_READ_FILTER};
use sqlx::PgPool;

use crate::api_load::{ResolverManifestCoverage, workload::ResolverTarget};

#[derive(Clone, Debug)]
pub(super) struct ResolverCoverage {
    pub(super) resolvers: Vec<ResolverTarget>,
    pub(super) counts: Vec<ResolverManifestCoverage>,
    pub(super) failures: Vec<String>,
}

#[derive(sqlx::FromRow)]
struct ResolverCoverageRow {
    chain_id: String,
    source_family: String,
    manifest_id: i64,
    stored_manifest_version: Option<i64>,
    event_manifest_version: Option<i64>,
    manifest_event_id: Option<i64>,
    resolver_address: Option<String>,
    target_block_number: Option<i64>,
    applicable: bool,
    project_admits_without_stored_active: bool,
    manifest_binding_problem: Option<String>,
    manifest_problem: Option<String>,
    support_status: Option<String>,
    manifest_event_bound: bool,
    api_visible: bool,
}

fn resolver_manifest_coverage_sql() -> String {
    // Match the concrete `exact_declared` arm and the implementation-based
    // `latest_upgrades` arm in crates/project/src/builders/resolver.rs.
    format!(
        r#"
WITH current_projects AS (
    SELECT head.chain_id, project.current_block_number,
           project.current_block_hash
    FROM bigname_phase.chain_heads head
    {CURRENT_PROJECT_PUBLICATION_JOIN}
    WHERE project.input_content_hash = $1
), stored_resolver_manifests AS (
    SELECT manifest.*
    FROM bigname_phase.manifest_versions manifest
    WHERE manifest.source_family IN (
        'ens_v1_resolver_l1', 'ens_v2_resolver_l1',
        'basenames_base_resolver'
    )
), latest_project_manifest_events AS (
    -- Mirror the Project phase's latest-per-manifest selection before applying its
    -- rollout-status and non-null-payload admission filter; see docs/glossary.md#projection.
    SELECT DISTINCT ON (event.source_manifest_id)
           event.source_manifest_id AS manifest_id,
           event.namespace,
           event.chain_id,
           event.source_family,
           event.manifest_version,
           event.after_state ->> 'rollout_status' AS rollout_status,
           event.after_state ->> 'normalizer_version' AS normalizer_version,
           event.after_state -> 'manifest_payload' AS manifest_payload,
           event.normalized_event_id
    FROM bigname_phase.normalized_events event
    LEFT JOIN current_projects current_project
      ON current_project.chain_id = event.chain_id
    LEFT JOIN bigname_phase.chain_lineage event_lineage
      ON event_lineage.chain_id = event.chain_id
     AND event_lineage.block_hash = event.block_hash
     AND event_lineage.block_number = event.block_number
    WHERE event.event_kind = 'SourceManifestUpdated'
      AND event.source_manifest_id IS NOT NULL
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (
          event.block_hash IS NULL
          OR event_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
      )
      AND (
          event.block_number IS NULL
          OR event.block_number <= current_project.current_block_number
      )
    ORDER BY event.source_manifest_id, event.normalized_event_id DESC
), active_families AS (
    SELECT DISTINCT COALESCE(latest.chain_id, manifest.chain_id) AS chain_id,
           COALESCE(latest.source_family, manifest.source_family) AS source_family,
           COALESCE(latest.manifest_id, manifest.manifest_id) AS manifest_id,
           manifest.manifest_version AS stored_manifest_version,
           latest.manifest_version AS event_manifest_version,
           COALESCE(latest.manifest_version, manifest.manifest_version)
               AS manifest_version,
           COALESCE(latest.manifest_payload, manifest.manifest_payload)
               AS manifest_payload,
           latest.normalized_event_id AS manifest_event_id,
           current_project.current_block_number AS target_block_number,
           current_project.current_block_hash AS target_block_hash,
           latest.rollout_status = 'active'
               AND latest.manifest_payload IS NOT NULL
               AND manifest.rollout_status IS DISTINCT FROM 'active'
               AS project_admits_without_stored_active,
           CASE
               WHEN latest.rollout_status = 'active'
                AND latest.manifest_payload IS NOT NULL
                AND manifest.rollout_status IS DISTINCT FROM 'active'
                   THEN 'Project admits the family from its latest manifest event but the stored manifest row is missing/not active'
               WHEN latest.normalized_event_id IS NULL
                   THEN 'no latest canonical SourceManifestUpdated event exists at the current Project head'
               WHEN latest.rollout_status IS DISTINCT FROM 'active'
                   THEN 'the latest projected manifest event is not active'
               WHEN latest.manifest_payload IS NULL
                   THEN 'the latest projected manifest event has no manifest payload'
               WHEN latest.namespace IS DISTINCT FROM manifest.namespace
                 OR latest.chain_id IS DISTINCT FROM manifest.chain_id
                 OR latest.source_family IS DISTINCT FROM manifest.source_family
                   THEN format(
                       'stored active manifest identity diverges from the latest Project event: stored namespace/chain/family=(%s, %s, %s), event=(%s, %s, %s)',
                       manifest.namespace, manifest.chain_id, manifest.source_family,
                       latest.namespace, latest.chain_id, latest.source_family
                   )
               WHEN latest.manifest_version IS DISTINCT FROM manifest.manifest_version
                   THEN 'the stored active version diverges from the latest projected manifest event'
               WHEN latest.normalizer_version IS DISTINCT FROM manifest.normalizer_version
                   THEN 'stored active normalizer_version diverges from the latest projected manifest event'
               WHEN latest.manifest_payload IS DISTINCT FROM manifest.manifest_payload
                   THEN 'stored active payload diverges from the latest projected manifest event'
               ELSE NULL
           END AS manifest_binding_problem,
           COALESCE(latest.source_family, manifest.source_family) = 'ens_v2_resolver_l1'
               AS uses_implementation_admission,
           CASE
               WHEN COALESCE(latest.source_family, manifest.source_family)
                    = 'ens_v2_resolver_l1'
                AND jsonb_typeof(COALESCE(
                    latest.manifest_payload, manifest.manifest_payload
                ) -> 'resolver_implementations') IS DISTINCT FROM 'array'
                   THEN 'resolver_implementations is absent or is not an array'
               WHEN COALESCE(latest.source_family, manifest.source_family)
                    = 'ens_v2_resolver_l1'
                AND jsonb_typeof(COALESCE(
                    latest.manifest_payload, manifest.manifest_payload
                ) -> 'resolver_implementations') = 'array'
                AND EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(COALESCE(
                        latest.manifest_payload, manifest.manifest_payload
                    ) -> 'resolver_implementations') entry
                    WHERE entry ->> 'address' IS NULL
                       OR btrim(entry ->> 'address') = ''
                ) THEN 'a resolver_implementations entry has no address'
               WHEN COALESCE(latest.source_family, manifest.source_family)
                    = 'ens_v2_resolver_l1'
                   THEN NULL
               WHEN jsonb_typeof(COALESCE(
                    latest.manifest_payload, manifest.manifest_payload
                ) -> 'contracts')
                    IS DISTINCT FROM 'array'
                   THEN 'contracts is absent or is not an array'
               WHEN EXISTS (
                   SELECT 1
                   FROM jsonb_array_elements(COALESCE(
                       latest.manifest_payload, manifest.manifest_payload
                   ) -> 'contracts') entry
                   WHERE entry ->> 'address' IS NULL
                      OR btrim(entry ->> 'address') = ''
               ) THEN 'a contract entry has no address'
               WHEN EXISTS (
                   SELECT 1
                   FROM jsonb_array_elements(COALESCE(
                       latest.manifest_payload, manifest.manifest_payload
                   ) -> 'contracts') entry
                   WHERE entry ->> 'start_block' IS NOT NULL
                     AND NOT CASE
                         WHEN jsonb_typeof(entry -> 'start_block') = 'number'
                         THEN (entry ->> 'start_block') ~ '^[0-9]+$'
                          AND (entry ->> 'start_block')::numeric
                              <= 9223372036854775807::numeric
                         ELSE FALSE
                     END
               ) THEN 'a contract entry has an invalid start_block'
               ELSE NULL
           END AS manifest_problem
    FROM stored_resolver_manifests manifest
    FULL OUTER JOIN latest_project_manifest_events latest
      ON latest.manifest_id = manifest.manifest_id
    LEFT JOIN current_projects current_project
      ON current_project.chain_id = COALESCE(latest.chain_id, manifest.chain_id)
    WHERE (
              manifest.manifest_id IS NOT NULL
              OR latest.source_family IN (
                  'ens_v1_resolver_l1', 'ens_v2_resolver_l1',
                  'basenames_base_resolver'
              )
          )
      AND (
              manifest.rollout_status = 'active'
              OR (
                  latest.rollout_status = 'active'
                  AND latest.manifest_payload IS NOT NULL
              )
          )
), declaration_rows AS (
    SELECT active.chain_id,
           active.source_family,
           active.manifest_id,
           active.manifest_version,
           active.manifest_event_id,
           lower(declaration ->> 'address') AS resolver_address,
           active.target_block_number,
           active.target_block_hash,
           CASE
               WHEN active.target_block_number IS NULL THEN FALSE
               WHEN declaration ->> 'start_block' IS NULL THEN TRUE
               WHEN jsonb_typeof(declaration -> 'start_block') = 'number'
               THEN CASE
                   WHEN (declaration ->> 'start_block') ~ '^[0-9]+$'
                   THEN (declaration ->> 'start_block')::numeric
                            <= 9223372036854775807::numeric
                    AND (declaration ->> 'start_block')::numeric
                            <= active.target_block_number::numeric
                   ELSE FALSE
               END
               ELSE FALSE
           END AS applicable,
           CASE
               WHEN declaration ->> 'start_block' IS NULL THEN 0::numeric
               WHEN jsonb_typeof(declaration -> 'start_block') = 'number'
               THEN CASE
                   WHEN (declaration ->> 'start_block') ~ '^[0-9]+$'
                    AND (declaration ->> 'start_block')::numeric
                            <= 9223372036854775807::numeric
                   THEN (declaration ->> 'start_block')::numeric
                   ELSE NULL
               END
               ELSE NULL
           END AS declaration_start_block,
           active.manifest_binding_problem,
           active.manifest_problem
    FROM active_families active
    CROSS JOIN LATERAL jsonb_array_elements(
        CASE
            WHEN jsonb_typeof(active.manifest_payload -> 'contracts') = 'array'
                THEN active.manifest_payload -> 'contracts'
            ELSE '[]'::jsonb
        END
    ) declaration
    WHERE NOT active.uses_implementation_admission
      AND declaration ->> 'address' IS NOT NULL
      AND btrim(declaration ->> 'address') <> ''
), declared AS (
    SELECT chain_id, source_family, manifest_id, manifest_version,
           manifest_event_id,
           resolver_address,
           target_block_number, target_block_hash,
           bool_or(applicable) AS applicable,
           max(declaration_start_block) FILTER (WHERE applicable)
               AS applicable_start_block,
           NULL::bigint AS upgrade_event_id,
           manifest_binding_problem,
           NULL::text AS manifest_problem
    FROM declaration_rows
    GROUP BY chain_id, source_family, manifest_id, manifest_version,
             manifest_event_id,
             resolver_address,
             target_block_number, target_block_hash,
             manifest_binding_problem
), upgrade_ranked AS (
    -- Mirror Project's `upgrade_ranked`/`latest_upgrades` ordering and its
    -- canonical project-event visibility before matching declared implementations.
    SELECT active.chain_id,
           active.source_family,
           active.manifest_id,
           active.manifest_version,
           active.manifest_event_id,
           active.manifest_payload,
           active.target_block_number,
           active.target_block_hash,
           active.manifest_binding_problem,
           active.manifest_problem,
           lower(event.after_state ->> 'proxy_address') AS resolver_address,
           lower(event.after_state ->> 'implementation') AS implementation_address,
           event.normalized_event_id AS upgrade_event_id,
           event.block_number AS upgrade_block_number,
           event.block_hash AS upgrade_block_hash,
           row_number() OVER (
               PARTITION BY active.manifest_id,
                            lower(event.after_state ->> 'proxy_address')
               ORDER BY event.block_number DESC NULLS LAST,
                        event.transaction_index DESC NULLS LAST,
                        event.log_index DESC NULLS LAST,
                        event.normalized_event_id DESC
           ) AS latest_rank
    FROM active_families active
    JOIN bigname_phase.normalized_events event
      ON event.chain_id = active.chain_id
     AND event.source_family = active.source_family
    LEFT JOIN bigname_phase.chain_lineage upgrade_lineage
      ON upgrade_lineage.chain_id = event.chain_id
     AND upgrade_lineage.block_hash = event.block_hash
     AND upgrade_lineage.block_number = event.block_number
    WHERE active.uses_implementation_admission
      AND active.target_block_number IS NOT NULL
      AND event.event_kind = 'Upgraded'
      AND event.consumer_visibility = 'activated'
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND event.after_state ->> 'proxy_address' IS NOT NULL
      AND btrim(event.after_state ->> 'proxy_address') <> ''
      AND (
          (event.block_number IS NULL AND event.block_hash IS NULL)
          OR (
              event.block_number <= active.target_block_number
              AND upgrade_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
          )
      )
), implementation_admitted AS (
    SELECT upgrade.chain_id,
           upgrade.source_family,
           upgrade.manifest_id,
           upgrade.manifest_version,
           upgrade.manifest_event_id,
           upgrade.resolver_address,
           upgrade.target_block_number,
           upgrade.target_block_hash,
           TRUE AS applicable,
           COALESCE(upgrade.upgrade_block_number, 0)::numeric
               AS applicable_start_block,
           upgrade.upgrade_event_id,
           upgrade.upgrade_block_number,
           upgrade.upgrade_block_hash,
           upgrade.manifest_binding_problem,
           upgrade.manifest_problem
    FROM upgrade_ranked upgrade
    WHERE upgrade.latest_rank = 1
      AND EXISTS (
          SELECT 1
          FROM jsonb_array_elements(CASE
              WHEN jsonb_typeof(
                  upgrade.manifest_payload -> 'resolver_implementations'
              ) = 'array'
              THEN upgrade.manifest_payload -> 'resolver_implementations'
              ELSE '[]'::jsonb
          END) declared_implementation
          WHERE lower(declared_implementation ->> 'address') =
                upgrade.implementation_address
      )
), expected AS (
    SELECT chain_id, source_family, manifest_id, manifest_version,
           manifest_event_id,
           NULL::text AS resolver_address,
           target_block_number, target_block_hash, FALSE AS applicable,
           NULL::numeric AS applicable_start_block,
           NULL::bigint AS upgrade_event_id,
           NULL::bigint AS upgrade_block_number,
           NULL::text AS upgrade_block_hash,
           manifest_binding_problem,
           manifest_problem
    FROM active_families
    UNION
    SELECT chain_id, source_family, manifest_id, manifest_version,
           manifest_event_id,
           resolver_address,
           target_block_number, target_block_hash, applicable,
           applicable_start_block,
           upgrade_event_id,
           NULL::bigint AS upgrade_block_number,
           NULL::text AS upgrade_block_hash,
           manifest_binding_problem,
           manifest_problem
    FROM declared
    UNION
    SELECT chain_id, source_family, manifest_id, manifest_version,
           manifest_event_id,
           resolver_address,
           target_block_number, target_block_hash, applicable,
           applicable_start_block,
           upgrade_event_id,
           upgrade_block_number,
           upgrade_block_hash,
           manifest_binding_problem,
           manifest_problem
    FROM implementation_admitted
)
SELECT expected.chain_id,
       expected.source_family,
       expected.manifest_id,
       active.stored_manifest_version,
       active.event_manifest_version,
       expected.manifest_event_id,
       expected.resolver_address,
       expected.target_block_number,
       expected.applicable,
       active.project_admits_without_stored_active,
       expected.manifest_binding_problem,
       expected.manifest_problem,
       candidate.support_status,
       COALESCE(
           candidate.provenance ->> 'manifest_event_id'
               = expected.manifest_event_id::text,
           FALSE
       ) AS manifest_event_bound,
       CASE
           WHEN resolver.resolver_address IS NULL THEN FALSE
           WHEN jsonb_typeof(
                    resolver.chain_positions -> 'target_block_number'
                ) <> 'number' THEN FALSE
           WHEN jsonb_typeof(
                    resolver.chain_positions -> 'target_block_hash'
                ) <> 'string' THEN FALSE
           WHEN (resolver.chain_positions ->> 'target_block_number')
                    !~ '^[0-9]+$' THEN FALSE
           WHEN (resolver.chain_positions ->> 'target_block_number')::numeric
                    > 9223372036854775807::numeric THEN FALSE
           WHEN resolver.manifest_version <> expected.manifest_version THEN FALSE
           WHEN resolver.provenance ->> 'manifest_id'
                    IS DISTINCT FROM expected.manifest_id::text THEN FALSE
           WHEN resolver.provenance ->> 'manifest_event_id'
                    IS DISTINCT FROM expected.manifest_event_id::text THEN FALSE
           WHEN expected.upgrade_event_id IS NOT NULL
            AND resolver.provenance ->> 'upgrade_event_id'
                    IS DISTINCT FROM expected.upgrade_event_id::text THEN FALSE
           WHEN expected.upgrade_block_number IS NOT NULL
            AND jsonb_typeof(resolver.chain_positions -> 'block_number')
                    <> 'number' THEN FALSE
           WHEN expected.upgrade_block_number IS NOT NULL
            AND jsonb_typeof(resolver.chain_positions -> 'block_hash')
                    <> 'string' THEN FALSE
           WHEN expected.upgrade_block_number IS NOT NULL
            AND (resolver.chain_positions ->> 'block_number')
                    !~ '^[0-9]+$' THEN FALSE
           WHEN expected.upgrade_block_number IS NOT NULL
            AND (resolver.chain_positions ->> 'block_number')::numeric
                    > 9223372036854775807::numeric THEN FALSE
           WHEN expected.upgrade_block_number IS NOT NULL
            AND (
                (resolver.chain_positions ->> 'block_number')::numeric
                    <> expected.upgrade_block_number::numeric
                OR resolver.chain_positions ->> 'block_hash'
                    IS DISTINCT FROM expected.upgrade_block_hash
            ) THEN FALSE
           ELSE COALESCE(
               (resolver.chain_positions ->> 'target_block_number')::numeric
                    <= expected.target_block_number::numeric
               AND (resolver.chain_positions ->> 'target_block_number')::numeric
                    >= expected.applicable_start_block
               AND EXISTS (
                   SELECT 1
                   FROM bigname_phase.chain_lineage numbered_lineage
                   WHERE numbered_lineage.chain_id = resolver.chain_id
                     AND numbered_lineage.block_hash =
                         resolver.chain_positions ->> 'target_block_hash'
                     AND numbered_lineage.block_number::numeric =
                         (resolver.chain_positions ->> 'target_block_number')::numeric
               )
               AND (
                   (resolver.chain_positions ->> 'target_block_number')::numeric
                        <> expected.target_block_number::numeric
                   OR resolver.chain_positions ->> 'target_block_hash'
                        = expected.target_block_hash
               ),
               FALSE
           )
       END AS api_visible
FROM expected
JOIN active_families active
  ON active.manifest_id = expected.manifest_id
LEFT JOIN bigname_phase.resolver_current candidate
  ON candidate.chain_id = expected.chain_id
 AND lower(candidate.resolver_address) = expected.resolver_address
LEFT JOIN bigname_phase.resolver_current resolver
  ON resolver.chain_id = expected.chain_id
 AND lower(resolver.resolver_address) = expected.resolver_address
 {DEFAULT_RESOLVER_CURRENT_READ_FILTER}
ORDER BY expected.chain_id, expected.source_family, expected.resolver_address"#
    )
}

pub(super) async fn load(pool: &PgPool) -> Result<ResolverCoverage> {
    let rows: Vec<ResolverCoverageRow> = sqlx::query_as(&resolver_manifest_coverage_sql())
        .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
        .fetch_all(pool)
        .await
        .context("failed to reconcile the resolver corpus with Project and stored manifests")?;
    let mut failures = Vec::new();
    let mut resolvers = Vec::with_capacity(rows.len());
    let mut counts = BTreeMap::<(String, String), (usize, usize, usize)>::new();
    let mut manifest_binding_problems = BTreeSet::new();
    let mut manifest_problems = BTreeSet::new();
    let mut missing_project_heads = BTreeSet::new();

    for row in rows {
        let ResolverCoverageRow {
            chain_id,
            source_family,
            manifest_id,
            stored_manifest_version,
            event_manifest_version,
            manifest_event_id,
            resolver_address,
            target_block_number,
            applicable,
            project_admits_without_stored_active,
            manifest_binding_problem,
            manifest_problem,
            support_status,
            manifest_event_bound,
            api_visible,
        } = row;
        let stored_version_label = stored_manifest_version
            .map_or_else(|| "missing".to_owned(), |version| version.to_string());
        let event_version_label = event_manifest_version
            .map_or_else(|| "missing".to_owned(), |version| version.to_string());
        let count = counts
            .entry((chain_id.clone(), source_family.clone()))
            .or_insert((0, 0, 0));
        if let Some(problem) = manifest_binding_problem
            && manifest_binding_problems.insert((
                manifest_id,
                chain_id.clone(),
                source_family.clone(),
                problem.clone(),
            ))
        {
            if project_admits_without_stored_active {
                failures.push(format!(
                    "Project admits {source_family:?} from its latest manifest event on chain {chain_id:?}, but stored manifest row {manifest_id} is missing/not active (stored version {stored_version_label}, latest event version {event_version_label}); repair manifest/event consistency, rebuild Project, and rerun the gate"
                ));
            } else {
                failures.push(format!(
                    "active stored resolver manifest version {stored_version_label} on chain {chain_id:?} in family {source_family:?} failed projected-event binding against latest Project event version {event_version_label}: {problem}; repair manifest/event consistency, rebuild Project, and rerun the gate"
                ));
            }
        }
        if let Some(problem) = manifest_problem
            && manifest_problems.insert((chain_id.clone(), source_family.clone(), problem.clone()))
        {
            failures.push(format!(
                "active stored resolver manifest on chain {chain_id:?} in family {source_family:?} is malformed: {problem}; restore a validated manifest payload, rebuild Project, and rerun the gate"
            ));
        }
        let Some(resolver_address) = resolver_address else {
            continue;
        };
        count.0 += 1;
        if target_block_number.is_none() {
            if missing_project_heads.insert((chain_id.clone(), source_family.clone())) {
                failures.push(format!(
                    "active resolver manifest chain {chain_id:?} in family {source_family:?} has concrete declarations but no current Project head; complete or rebuild Project for that chain and rerun the gate"
                ));
            }
            continue;
        }
        if !applicable {
            continue;
        }
        count.1 += 1;
        match support_status.as_deref() {
            None => failures.push(format!(
                "active resolver manifest address {resolver_address} on chain {chain_id:?} in family {source_family:?} is missing from resolver_current; rebuild Project from the stored active manifests and rerun the gate"
            )),
            Some(status) if status != "supported" => failures.push(format!(
                "active resolver manifest address {resolver_address} on chain {chain_id:?} in family {source_family:?} is {status:?}, not supported, in resolver_current; rebuild Project from the stored active manifests and rerun the gate"
            )),
            Some(_) if !manifest_event_bound => failures.push(format!(
                "active resolver manifest address {resolver_address} on chain {chain_id:?} in family {source_family:?} for manifest {manifest_id} (stored version {stored_version_label}, latest event version {event_version_label}) does not cite latest projected manifest event {manifest_event_id:?}; rebuild Project from the latest canonical manifest event and rerun the gate"
            )),
            Some(_) if !api_visible => failures.push(format!(
                "active resolver manifest address {resolver_address} on chain {chain_id:?} in family {source_family:?} fails the resolver benchmark's canonical-read or chain-anchor integrity checks at the copy's current Project head; repair or rebuild Project and rerun the gate"
            )),
            Some(_) => {
                count.2 += 1;
                resolvers.push(ResolverTarget {
                    chain_id,
                    source_family,
                    resolver_address,
                });
            }
        }
    }

    for ((chain_id, source_family), (_, _, workload_targets)) in &counts {
        if *workload_targets == 0 {
            failures.push(format!(
                "active resolver manifest on chain {chain_id:?} in family {source_family:?} contributes zero currently applicable, supported, API-visible resolver addresses, so that family's resolver workload cannot be constructed; restore its projected resolver evidence and rerun the gate"
            ));
        }
    }
    let counts = counts
        .into_iter()
        .map(
            |(
                (chain_id, source_family),
                (declared_addresses, applicable_addresses, _workload_targets),
            )| {
                ResolverManifestCoverage {
                    chain_id,
                    source_family,
                    declared_addresses,
                    applicable_addresses,
                    exercised_addresses: 0,
                }
            },
        )
        .collect();
    if resolvers.is_empty() {
        failures.push(
            "active stored resolver-family manifests contribute zero currently applicable, supported, API-visible resolver addresses, so the resolver workload cannot be constructed; restore and project an applicable manifest set, then rerun the gate"
                .to_owned(),
        );
    }
    Ok(ResolverCoverage {
        resolvers,
        counts,
        failures,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coverage_query_binds_the_same_manifest_and_admission_evidence_as_project() {
        let query = resolver_manifest_coverage_sql();
        assert!(query.contains("active.manifest_payload -> 'contracts'"));
        assert!(query.contains("jsonb_typeof(active.manifest_payload -> 'contracts') = 'array'"));
        assert!(query.contains("contracts is absent or is not an array"));
        assert!(query.contains("a contract entry has no address"));
        assert!(query.contains("a contract entry has an invalid start_block"));
        assert!(query.contains("bool_or(applicable) AS applicable"));
        assert!(query.contains("AS applicable_start_block"));
        assert!(query.contains("resolver.manifest_version <> expected.manifest_version"));
        assert!(query.contains("resolver.provenance ->> 'manifest_id'"));
        assert!(query.contains("resolver.provenance ->> 'manifest_event_id'"));
        assert!(query.contains("resolver.provenance ->> 'upgrade_event_id'"));
        assert!(query.contains("event.event_kind = 'SourceManifestUpdated'"));
        assert!(query.contains("event.event_kind = 'Upgraded'"));
        assert!(query.contains("upgrade.manifest_payload -> 'resolver_implementations'"));
        assert!(query.contains("= 'ens_v2_resolver_l1'"));
        assert!(query.contains("SELECT DISTINCT ON (event.source_manifest_id)"));
        assert!(query.contains("FULL OUTER JOIN latest_project_manifest_events"));
        assert!(query.contains("manifest.rollout_status IS DISTINCT FROM 'active'"));
        assert!(query.contains("latest.chain_id IS DISTINCT FROM manifest.chain_id"));
        assert!(query.contains("latest.source_family IS DISTINCT FROM manifest.source_family"));
        assert!(
            query
                .contains("latest.normalizer_version IS DISTINCT FROM manifest.normalizer_version")
        );
        assert!(query.contains("event.block_number AS upgrade_block_number"));
        assert!(query.contains("event.block_hash AS upgrade_block_hash"));
        assert!(query.contains("resolver.chain_positions -> 'block_number'"));
        assert!(query.contains("resolver.chain_positions ->> 'block_hash'"));
        assert!(query.contains("event.consumer_visibility = 'activated'"));
        assert!(query.contains("event.normalized_event_id DESC"));
        assert!(query.contains("current_project.current_block_number AS target_block_number"));
        assert!(query.contains(CURRENT_PROJECT_PUBLICATION_JOIN.trim()));
        assert!(query.contains("project.input_content_hash = $1"));
        assert!(query.contains("resolver.chain_positions -> 'target_block_number'"));
        assert!(query.contains("resolver.chain_positions ->> 'target_block_hash'"));
        assert!(query.contains("END AS applicable"));
        assert!(query.contains("manifest.rollout_status = 'active'"));
        assert!(query.contains("'ens_v1_resolver_l1', 'ens_v2_resolver_l1'"));
        assert!(query.contains("'basenames_base_resolver'"));
        assert!(query.contains(DEFAULT_RESOLVER_CURRENT_READ_FILTER.trim()));
        assert!(query.contains("numbered_lineage.block_number::numeric"));
    }
}
