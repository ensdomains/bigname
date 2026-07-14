use anyhow::{Context, Result};
use sqlx::{PgPool, Row};

/// Block chunk shared by stored-lineage promotion and read-only completeness inspection when
/// reconciling watched tuples against durable backfill coverage facts.
pub const WATCHED_COVERAGE_VERIFICATION_CHUNK_BLOCKS: i64 = 131_072;

/// A watched (source_family, address) tuple whose required interval within the
/// evaluated block range is not fully contained in any single
/// `backfill_coverage_facts` row (address-scoped or family-scoped).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UncoveredWatchedTuple {
    pub source_family: String,
    pub address: String,
    pub required_from_block: i64,
    pub required_to_block: i64,
}

/// Anti-join the chain's watched-contract tuples (manifest-declared and
/// discovery-edge, the same selection the intake watch set is built from)
/// against durable `backfill_coverage_facts`, restricted to tuples whose
/// active window intersects `[from_block, to_block]` and whose source family
/// produces logs (`log_producing_source_families`). A tuple is covered when
/// its required interval (active window ∩ evaluated range) is fully contained
/// in a single address-scoped fact row for the tuple or a single family-scoped
/// fact row for its family. Returns at most `limit` violations ordered by
/// (source_family, address); never materializes the covered side.
pub async fn find_uncovered_watched_tuples(
    pool: &PgPool,
    chain: &str,
    from_block: i64,
    to_block: i64,
    log_producing_source_families: &[String],
    limit: i64,
) -> Result<Vec<UncoveredWatchedTuple>> {
    if from_block > to_block {
        anyhow::bail!(
            "uncovered watched tuple scan range start {from_block} is after end {to_block}"
        );
    }
    if log_producing_source_families.is_empty() {
        return Ok(Vec::new());
    }

    let rows = sqlx::query(
        r#"
        WITH manifest_watched AS (
            SELECT
                mv.source_family AS source_family,
                LOWER(cia.address) AS address,
                CASE
                    WHEN manifest_range.start_block IS NULL THEN cia.active_from_block_number
                    WHEN cia.active_from_block_number IS NULL THEN manifest_range.start_block
                    ELSE GREATEST(manifest_range.start_block, cia.active_from_block_number)
                END AS active_from_block_number,
                cia.active_to_block_number AS active_to_block_number
            FROM contract_instance_addresses cia
            JOIN manifest_contract_instances mci
              ON mci.contract_instance_id = cia.contract_instance_id
            JOIN manifest_versions mv
              ON mv.manifest_id = mci.manifest_id
             AND mv.chain = cia.chain_id
            LEFT JOIN LATERAL (
                SELECT (entry ->> 'start_block')::BIGINT AS start_block
                FROM jsonb_array_elements(
                    CASE
                        WHEN mci.declaration_kind = 'root' THEN mv.manifest_payload -> 'roots'
                        ELSE mv.manifest_payload -> 'contracts'
                    END
                ) entry
                WHERE (
                        mci.declaration_kind = 'root'
                        AND entry ->> 'name' = mci.declaration_name
                    )
                   OR (
                        mci.declaration_kind = 'contract'
                        AND entry ->> 'role' = mci.declaration_name
                    )
                ORDER BY start_block NULLS LAST
                LIMIT 1
            ) manifest_range ON TRUE
            WHERE cia.chain_id = $1
              AND cia.deactivated_at IS NULL
              AND mv.rollout_status = 'active'
              AND mv.source_family = ANY($4::TEXT[])
        ),
        discovery_watched AS (
            SELECT
                COALESCE(target_mv.source_family, mv.source_family) AS source_family,
                LOWER(cia.address) AS address,
                CASE
                    WHEN de.active_from_block_number IS NULL THEN cia.active_from_block_number
                    WHEN cia.active_from_block_number IS NULL THEN de.active_from_block_number
                    ELSE GREATEST(de.active_from_block_number, cia.active_from_block_number)
                END AS active_from_block_number,
                CASE
                    WHEN de.active_to_block_number IS NULL THEN cia.active_to_block_number
                    WHEN cia.active_to_block_number IS NULL THEN de.active_to_block_number
                    ELSE LEAST(de.active_to_block_number, cia.active_to_block_number)
                END AS active_to_block_number
            FROM contract_instance_addresses cia
            JOIN discovery_edges de
              ON de.chain_id = cia.chain_id
             AND de.to_contract_instance_id = cia.contract_instance_id
             AND de.deactivated_at IS NULL
             AND de.edge_kind <> 'migration'
            JOIN manifest_versions mv
              ON mv.manifest_id = de.source_manifest_id
             AND mv.rollout_status = 'active'
            LEFT JOIN manifest_versions target_mv
              ON target_mv.rollout_status = 'active'
             AND target_mv.namespace = mv.namespace
             AND target_mv.chain = de.chain_id
             AND target_mv.deployment_epoch = mv.deployment_epoch
             AND target_mv.source_family = CASE
                 WHEN de.edge_kind = 'resolver' AND mv.source_family = 'ens_v1_registry_l1'
                     THEN 'ens_v1_resolver_l1'
                 WHEN de.edge_kind = 'resolver' AND mv.source_family = 'ens_v2_registry_l1'
                     THEN 'ens_v2_resolver_l1'
                 WHEN de.edge_kind = 'resolver' AND mv.source_family = 'basenames_base_registry'
                     THEN 'basenames_base_resolver'
                 ELSE NULL
             END
            WHERE cia.chain_id = $1
              AND cia.deactivated_at IS NULL
              AND COALESCE(target_mv.source_family, mv.source_family) = ANY($4::TEXT[])
              AND (
                  de.edge_kind <> 'resolver'
                  OR mv.source_family NOT IN (
                      'ens_v1_registry_l1',
                      'ens_v2_registry_l1',
                      'basenames_base_registry'
                  )
                  OR target_mv.manifest_id IS NOT NULL
              )
              AND (
                  de.active_from_block_number IS NULL
                  OR cia.active_to_block_number IS NULL
                  OR de.active_from_block_number <= cia.active_to_block_number
              )
              AND (
                  cia.active_from_block_number IS NULL
                  OR de.active_to_block_number IS NULL
                  OR cia.active_from_block_number <= de.active_to_block_number
              )
        ),
        required_tuples AS (
            SELECT DISTINCT
                source_family,
                address,
                GREATEST(COALESCE(active_from_block_number, $2::BIGINT), $2::BIGINT)
                    AS required_from_block,
                LEAST(COALESCE(active_to_block_number, $3::BIGINT), $3::BIGINT)
                    AS required_to_block
            FROM (
                SELECT * FROM manifest_watched
                UNION ALL
                SELECT * FROM discovery_watched
            ) watched
            WHERE COALESCE(active_from_block_number, $2::BIGINT) <= $3::BIGINT
              AND COALESCE(active_to_block_number, $3::BIGINT) >= $2::BIGINT
        )
        SELECT
            source_family,
            address,
            required_from_block,
            required_to_block
        FROM required_tuples watched
        WHERE NOT EXISTS (
                SELECT 1
                FROM backfill_coverage_facts fact
                WHERE fact.chain_id = $1
                  AND fact.source_family = watched.source_family
                  AND fact.address = watched.address
                  AND fact.covered_from_block <= watched.required_from_block
                  AND fact.covered_to_block >= watched.required_to_block
            )
          AND NOT EXISTS (
                SELECT 1
                FROM backfill_coverage_facts fact
                WHERE fact.chain_id = $1
                  AND fact.source_family = watched.source_family
                  AND fact.scope = 'family'
                  AND fact.covered_from_block <= watched.required_from_block
                  AND fact.covered_to_block >= watched.required_to_block
            )
        ORDER BY source_family, address, required_from_block
        LIMIT $5
        "#,
    )
    .bind(chain)
    .bind(from_block)
    .bind(to_block)
    .bind(log_producing_source_families)
    .bind(limit)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to scan uncovered watched tuples for chain {chain} over {from_block}..={to_block}")
    })?;

    rows.into_iter()
        .map(|row| {
            Ok(UncoveredWatchedTuple {
                source_family: row
                    .try_get("source_family")
                    .context("missing uncovered tuple source_family")?,
                address: row
                    .try_get("address")
                    .context("missing uncovered tuple address")?,
                required_from_block: row
                    .try_get("required_from_block")
                    .context("missing uncovered tuple required_from_block")?,
                required_to_block: row
                    .try_get("required_to_block")
                    .context("missing uncovered tuple required_to_block")?,
            })
        })
        .collect()
}
