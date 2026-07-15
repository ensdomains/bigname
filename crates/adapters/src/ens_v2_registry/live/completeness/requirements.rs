use std::collections::BTreeMap;

use anyhow::{Context, Result, ensure};
use bigname_manifests::{RequiredWatchedTuple, load_required_watched_tuples};
use sqlx::PgPool;

use crate::ens_v2_registry::constants::{
    DERIVATION_KIND_ENS_V2_REGISTRY_RESOURCE_SURFACE, SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
    SOURCE_FAMILY_ENS_V2_RESOLVER_L1, SOURCE_FAMILY_ENS_V2_ROOT_L1,
};

mod coverage;
pub(super) use coverage::{
    ensure_generation_bound_coverage, ensure_generation_bound_coverage_with_live_selection,
    ensure_newly_required_generation_bound_coverage,
};

pub(in crate::ens_v2_registry) async fn has_authoritative_ens_v2_closure_through(
    pool: &PgPool,
    chain: &str,
    through_block: i64,
) -> Result<bool> {
    ensure!(
        through_block >= 0,
        "ENSv2 authoritative-closure boundary cannot be negative"
    );
    Ok(!load_required_watched_tuples(
        pool,
        chain,
        0,
        through_block,
        &ens_v2_closure_source_families(),
    )
    .await?
    .is_empty())
}

pub(super) fn ens_v2_closure_source_families() -> Vec<String> {
    vec![
        SOURCE_FAMILY_ENS_V2_ROOT_L1.to_owned(),
        SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
    ]
}

/// Source families whose newly admitted historical intervals must be fetched
/// before a live registry sync may publish its discovery epoch. Resolver
/// history participates in this admission delta without becoming part of the
/// registry adapter's retained-history proof or semantic-witness set.
pub(super) fn ens_v2_discovery_history_source_families() -> Vec<String> {
    vec![
        SOURCE_FAMILY_ENS_V2_ROOT_L1.to_owned(),
        SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
        SOURCE_FAMILY_ENS_V2_RESOLVER_L1.to_owned(),
    ]
}

pub(super) fn requirement_intervals_not_covered_by(
    required: &[RequiredWatchedTuple],
    covered: &[RequiredWatchedTuple],
) -> Vec<RequiredWatchedTuple> {
    let mut covered_by_tuple = BTreeMap::<(String, String), Vec<(i64, i64)>>::new();
    for requirement in covered {
        covered_by_tuple
            .entry((
                requirement.source_family.clone(),
                requirement.address.clone(),
            ))
            .or_default()
            .push((
                requirement.required_from_block,
                requirement.required_to_block,
            ));
    }
    for intervals in covered_by_tuple.values_mut() {
        intervals.sort_unstable();
    }

    let mut gaps = Vec::new();
    for requirement in required {
        let key = (
            requirement.source_family.clone(),
            requirement.address.clone(),
        );
        let mut next_required = Some(requirement.required_from_block);
        for &(covered_from, covered_to) in covered_by_tuple.get(&key).into_iter().flatten() {
            let Some(cursor) = next_required else {
                break;
            };
            if covered_to < cursor {
                continue;
            }
            if covered_from > requirement.required_to_block {
                break;
            }
            if covered_from > cursor {
                gaps.push(RequiredWatchedTuple {
                    source_family: requirement.source_family.clone(),
                    address: requirement.address.clone(),
                    required_from_block: cursor,
                    required_to_block: (covered_from - 1).min(requirement.required_to_block),
                });
            }
            if covered_to >= requirement.required_to_block {
                next_required = None;
                break;
            }
            next_required = covered_to.checked_add(1).map(|next| next.max(cursor));
        }
        if let Some(cursor) = next_required
            && cursor <= requirement.required_to_block
        {
            gaps.push(RequiredWatchedTuple {
                source_family: requirement.source_family.clone(),
                address: requirement.address.clone(),
                required_from_block: cursor,
                required_to_block: requirement.required_to_block,
            });
        }
    }
    gaps
}

pub(super) async fn ensure_retained_semantic_witnesses(
    connection: &mut sqlx::PgConnection,
    chain: &str,
    requirements: &[RequiredWatchedTuple],
    through_block: i64,
) -> Result<()> {
    let source_families = requirements
        .iter()
        .map(|requirement| requirement.source_family.clone())
        .collect::<Vec<_>>();
    let addresses = requirements
        .iter()
        .map(|requirement| requirement.address.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let from_blocks = requirements
        .iter()
        .map(|requirement| requirement.required_from_block)
        .collect::<Vec<_>>();
    let to_blocks = requirements
        .iter()
        .map(|requirement| requirement.required_to_block)
        .collect::<Vec<_>>();
    let complete = sqlx::query_scalar::<_, bool>(
        r#"
        WITH required_tuples AS (
            SELECT *
            FROM UNNEST(
                $3::TEXT[],
                $4::TEXT[],
                $5::BIGINT[],
                $6::BIGINT[]
            ) AS watched(
                source_family,
                address,
                required_from_block,
                required_to_block
            )
        ),
        readable_lineage AS (
            SELECT chain_id, block_hash, block_number
            FROM chain_lineage
            WHERE canonicality_state IN (
                'canonical'::canonicality_state,
                'safe'::canonicality_state,
                'finalized'::canonicality_state
            )
        ),
        readable_raw_logs AS (
            SELECT raw.chain_id, raw.block_hash, raw.block_number, raw.log_index
            FROM raw_logs raw
            JOIN readable_lineage lineage
              ON lineage.chain_id = raw.chain_id
             AND lineage.block_hash = raw.block_hash
             AND lineage.block_number = raw.block_number
            WHERE raw.canonicality_state IN (
                'canonical'::canonicality_state,
                'safe'::canonicality_state,
                'finalized'::canonicality_state
            )
        )
        SELECT
            NOT EXISTS (
                SELECT 1
                FROM normalized_events event
                WHERE event.chain_id = $1
                  AND event.derivation_kind = $2
                  AND event.raw_fact_ref ->> 'kind' = 'raw_log'
                  AND event.block_number <= $7
                  AND event.canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
                  AND EXISTS (
                      SELECT 1
                      FROM readable_lineage lineage
                      WHERE lineage.chain_id = event.chain_id
                        AND lineage.block_hash = event.block_hash
                        AND lineage.block_number = event.block_number
                  )
                  AND EXISTS (
                      SELECT 1
                      FROM required_tuples watched
                      WHERE watched.source_family = event.source_family
                        AND watched.address = lower(event.raw_fact_ref ->> 'emitting_address')
                        AND event.block_number BETWEEN watched.required_from_block
                            AND watched.required_to_block
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM readable_raw_logs raw
                      WHERE raw.chain_id = event.chain_id
                        AND raw.block_hash = event.block_hash
                        AND raw.block_number = event.block_number
                        AND raw.log_index = event.log_index
                  )
            )
            AND NOT EXISTS (
                SELECT 1
                FROM discovery_edges edge
                WHERE edge.chain_id = $1
                  AND edge.discovery_source LIKE 'ens_v2_registry_%'
                  AND edge.provenance ->> 'source' = 'raw_log'
                  AND edge.active_from_block_number <= $7
                  AND EXISTS (
                      SELECT 1
                      FROM readable_lineage lineage
                      WHERE lineage.chain_id = edge.chain_id
                        AND lineage.block_hash = edge.provenance ->> 'block_hash'
                        AND lineage.block_number = edge.active_from_block_number
                  )
                  AND EXISTS (
                      SELECT 1
                      FROM required_tuples watched
                      WHERE watched.address = lower(edge.provenance ->> 'from_address')
                        AND edge.active_from_block_number
                            BETWEEN watched.required_from_block AND watched.required_to_block
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM readable_raw_logs raw
                      WHERE raw.chain_id = edge.chain_id
                        AND raw.block_hash = edge.provenance ->> 'block_hash'
                        AND raw.block_number = edge.active_from_block_number
                        AND raw.log_index::TEXT = edge.provenance ->> 'log_index'
                  )
            )
        "#,
    )
    .bind(chain)
    .bind(DERIVATION_KIND_ENS_V2_REGISTRY_RESOURCE_SURFACE)
    .bind(&source_families)
    .bind(&addresses)
    .bind(&from_blocks)
    .bind(&to_blocks)
    .bind(through_block)
    .fetch_one(connection)
    .await
    .with_context(|| {
        format!("failed to verify retained ENSv2 semantic raw-log witnesses for {chain}")
    })?;
    ensure!(
        complete,
        "ENSv2 retained history on {chain} is missing raw-log witnesses for materialized ENSv2 events or discovery through block {through_block}"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn requirement(family: &str, address: &str, from: i64, to: i64) -> RequiredWatchedTuple {
        RequiredWatchedTuple {
            source_family: family.to_owned(),
            address: address.to_owned(),
            required_from_block: from,
            required_to_block: to,
        }
    }

    #[test]
    fn requirement_intervals_treat_same_open_extension_as_pre_sync_coverage() {
        let required = [requirement("registry", "0xa", 0, 20)];
        let covered = [requirement("registry", "0xa", 0, 20)];

        assert!(requirement_intervals_not_covered_by(&required, &covered).is_empty());
    }

    #[test]
    fn requirement_intervals_ignore_resolver_only_epoch_changes() {
        let required = [
            requirement("root", "0x1", 0, 20),
            requirement("registry", "0x2", 0, 20),
        ];

        assert!(requirement_intervals_not_covered_by(&required, &required).is_empty());
    }

    #[test]
    fn requirement_intervals_return_reopened_gap() {
        let required = [
            requirement("registry", "0xa", 0, 10),
            requirement("registry", "0xa", 20, 30),
        ];
        let covered = [requirement("registry", "0xa", 0, 10)];

        assert_eq!(
            requirement_intervals_not_covered_by(&required, &covered),
            vec![requirement("registry", "0xa", 20, 30)]
        );
    }

    #[test]
    fn requirement_intervals_return_earlier_expansion() {
        let required = [requirement("registry", "0xa", 5, 30)];
        let covered = [requirement("registry", "0xa", 10, 30)];

        assert_eq!(
            requirement_intervals_not_covered_by(&required, &covered),
            vec![requirement("registry", "0xa", 5, 9)]
        );
    }

    #[test]
    fn requirement_intervals_merge_overlapping_covered_ranges() {
        let required = [requirement("registry", "0xa", 0, 25)];
        let covered = [
            requirement("registry", "0xa", 0, 10),
            requirement("registry", "0xa", 5, 20),
        ];

        assert_eq!(
            requirement_intervals_not_covered_by(&required, &covered),
            vec![requirement("registry", "0xa", 21, 25)]
        );
    }

    #[test]
    fn requirement_intervals_do_not_cross_family_or_address_identity() {
        let required = [requirement("registry", "0xa", 0, 10)];
        let covered = [
            requirement("root", "0xa", 0, 10),
            requirement("registry", "0xb", 0, 10),
        ];

        assert_eq!(
            requirement_intervals_not_covered_by(&required, &covered),
            required
        );
    }

    #[test]
    fn requirement_interval_subtraction_matches_point_membership_on_small_ranges() {
        let intervals = (0..=5)
            .flat_map(|from| (from..=5).map(move |to| (from, to)))
            .collect::<Vec<_>>();

        for &(required_from, required_to) in &intervals {
            let required = [requirement("registry", "0xa", required_from, required_to)];
            for &(first_from, first_to) in &intervals {
                for &(second_from, second_to) in &intervals {
                    let covered = [
                        requirement("registry", "0xa", first_from, first_to),
                        requirement("registry", "0xa", second_from, second_to),
                    ];
                    let gaps = requirement_intervals_not_covered_by(&required, &covered);

                    for block in required_from..=required_to {
                        let expected_gap = !covered.iter().any(|interval| {
                            block >= interval.required_from_block
                                && block <= interval.required_to_block
                        });
                        let actual_gap = gaps.iter().any(|interval| {
                            block >= interval.required_from_block
                                && block <= interval.required_to_block
                        });
                        assert_eq!(actual_gap, expected_gap, "block {block}, {covered:?}");
                    }
                }
            }
        }
    }
}
