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
    resolver_address: Option<String>,
    target_block_number: Option<i64>,
    applicable: bool,
    manifest_problem: Option<String>,
    support_status: Option<String>,
    api_visible: bool,
}

fn resolver_manifest_coverage_sql() -> String {
    // Match the concrete-address and start-block selection performed by
    // `exact_declared` in crates/project/src/builders/resolver.rs.
    format!(
        r#"
WITH active_families AS (
    SELECT DISTINCT manifest.chain_id,
           manifest.source_family,
           manifest.manifest_id,
           manifest.manifest_version,
           manifest.manifest_payload,
           current_project.current_block_number AS target_block_number,
           current_project.current_block_hash AS target_block_hash,
           CASE
               WHEN jsonb_typeof(manifest.manifest_payload -> 'contracts')
                    IS DISTINCT FROM 'array'
                   THEN 'contracts is absent or is not an array'
               WHEN EXISTS (
                   SELECT 1
                   FROM jsonb_array_elements(manifest.manifest_payload -> 'contracts') entry
                   WHERE entry ->> 'address' IS NULL
                      OR btrim(entry ->> 'address') = ''
               ) THEN 'a contract entry has no address'
               WHEN EXISTS (
                   SELECT 1
                   FROM jsonb_array_elements(manifest.manifest_payload -> 'contracts') entry
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
    FROM bigname_phase.manifest_versions manifest
    LEFT JOIN (
        SELECT head.chain_id, project.current_block_number,
               project.current_block_hash
        FROM bigname_phase.chain_heads head
        {CURRENT_PROJECT_PUBLICATION_JOIN}
        WHERE project.input_content_hash = $1
    ) current_project
      ON current_project.chain_id = manifest.chain_id
    WHERE manifest.rollout_status = 'active'
      AND manifest.source_family IN (
          'ens_v1_resolver_l1', 'ens_v2_resolver_l1',
          'basenames_base_resolver'
      )
), declaration_rows AS (
    SELECT active.chain_id,
           active.source_family,
           active.manifest_id,
           active.manifest_version,
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
           active.manifest_problem
    FROM active_families active
    CROSS JOIN LATERAL jsonb_array_elements(
        CASE
            WHEN jsonb_typeof(active.manifest_payload -> 'contracts') = 'array'
                THEN active.manifest_payload -> 'contracts'
            ELSE '[]'::jsonb
        END
    ) declaration
    WHERE declaration ->> 'address' IS NOT NULL
      AND btrim(declaration ->> 'address') <> ''
), declared AS (
    SELECT chain_id, source_family, manifest_id, manifest_version,
           resolver_address,
           target_block_number, target_block_hash,
           bool_or(applicable) AS applicable,
           max(declaration_start_block) FILTER (WHERE applicable)
               AS applicable_start_block,
           NULL::text AS manifest_problem
    FROM declaration_rows
    GROUP BY chain_id, source_family, manifest_id, manifest_version,
             resolver_address,
             target_block_number, target_block_hash
), expected AS (
    SELECT chain_id, source_family, manifest_id, manifest_version,
           NULL::text AS resolver_address,
           target_block_number, target_block_hash, FALSE AS applicable,
           NULL::numeric AS applicable_start_block,
           manifest_problem
    FROM active_families
    UNION
    SELECT chain_id, source_family, manifest_id, manifest_version,
           resolver_address,
           target_block_number, target_block_hash, applicable,
           applicable_start_block,
           manifest_problem
    FROM declared
)
SELECT expected.chain_id,
       expected.source_family,
       expected.resolver_address,
       expected.target_block_number,
       expected.applicable,
       expected.manifest_problem,
       candidate.support_status,
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
        .context("failed to compare the resolver corpus with active stored manifests")?;
    let mut failures = Vec::new();
    let mut resolvers = Vec::with_capacity(rows.len());
    let mut counts = BTreeMap::<(String, String), (usize, usize, usize)>::new();
    let mut manifest_problems = BTreeSet::new();
    let mut missing_project_heads = BTreeSet::new();

    for row in rows {
        let ResolverCoverageRow {
            chain_id,
            source_family,
            resolver_address,
            target_block_number,
            applicable,
            manifest_problem,
            support_status,
            api_visible,
        } = row;
        let count = counts
            .entry((chain_id.clone(), source_family.clone()))
            .or_insert((0, 0, 0));
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
            Some(_) if !api_visible => failures.push(format!(
                "active resolver manifest address {resolver_address} on chain {chain_id:?} in family {source_family:?} fails the resolver benchmark's canonical-read or chain-anchor integrity checks at the copy's current Project head; repair or rebuild Project and rerun the gate"
            )),
            Some(_) => resolvers.push(ResolverTarget {
                chain_id,
                source_family,
                resolver_address,
            }),
        }
    }

    let counts = counts
        .into_iter()
        .map(
            |(
                (chain_id, source_family),
                (declared_addresses, applicable_addresses, exercised_addresses),
            )| {
                ResolverManifestCoverage {
                    chain_id,
                    source_family,
                    declared_addresses,
                    applicable_addresses,
                    exercised_addresses,
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
    fn coverage_query_reads_the_same_stored_contract_declarations_as_project() {
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
