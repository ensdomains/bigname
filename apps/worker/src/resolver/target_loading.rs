use std::collections::BTreeSet;

use anyhow::{Context, Result, ensure};
use bigname_storage::{
    CanonicalityState, PermissionsCurrentRow, SurfaceBindingKind,
    load_permissions_current_for_resolver_scope, normalize_evm_address,
};
use serde_json::Value;
use sqlx::{PgPool, Row, types::time::OffsetDateTime};
use uuid::Uuid;

use super::{
    CANONICAL_STATE_FILTER, EVENT_KIND_ALIAS_CHANGED, EVENT_KIND_RESOLVER_CHANGED,
    SOURCE_FAMILY_BASENAMES_BASE_REGISTRY, SOURCE_FAMILY_BASENAMES_BASE_RESOLVER,
    SOURCE_FAMILY_ENS_V1_REGISTRY_L1, SOURCE_FAMILY_ENS_V1_RESOLVER_L1, ZERO_ADDRESS,
    state_helpers::parse_canonicality_state,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ResolverTarget {
    pub(super) chain_id: String,
    pub(super) resolver_address: String,
    pub(super) profile_source_family: Option<String>,
    pub(super) enumerate_bindings: bool,
}

#[derive(Clone, Debug)]
pub(super) struct CurrentBindingSeed {
    pub(super) chain_id: String,
    pub(super) logical_name_id: String,
    pub(super) canonical_display_name: String,
    pub(super) normalized_name: String,
    pub(super) namehash: String,
    pub(super) resource_id: Uuid,
    pub(super) surface_binding_id: Uuid,
    pub(super) binding_kind: SurfaceBindingKind,
    pub(super) normalized_event_id: i64,
    pub(super) source_family: String,
    pub(super) manifest_version: i64,
    pub(super) source_manifest_id: Option<i64>,
    pub(super) block_number: i64,
    pub(super) block_hash: String,
    pub(super) block_timestamp: Option<OffsetDateTime>,
    pub(super) raw_fact_ref: Value,
    pub(super) canonicality_state: CanonicalityState,
}

#[derive(Clone, Debug)]
pub(super) struct AliasSeed {
    pub(super) chain_id: String,
    pub(super) resolver_address: String,
    pub(super) normalized_event_id: i64,
    pub(super) logical_name_id: Option<String>,
    pub(super) resource_id: Option<Uuid>,
    pub(super) source_family: String,
    pub(super) manifest_version: i64,
    pub(super) source_manifest_id: Option<i64>,
    pub(super) block_number: i64,
    pub(super) block_hash: String,
    pub(super) block_timestamp: Option<OffsetDateTime>,
    pub(super) raw_fact_ref: Value,
    pub(super) canonicality_state: CanonicalityState,
    pub(super) after_state: Value,
}

pub(super) async fn load_target_resolvers_page(
    pool: &PgPool,
    cursor: Option<&[String; 2]>,
    limit: usize,
) -> Result<Vec<ResolverTarget>> {
    ensure!(limit > 0, "resolver target page limit must be positive");
    let cursor_chain_id = cursor.map(|cursor| cursor[0].as_str());
    let cursor_resolver_address = cursor.map(|cursor| cursor[1].as_str());
    let rows = sqlx::query(&target_resolver_page_query())
        .bind(EVENT_KIND_RESOLVER_CHANGED)
        .bind(EVENT_KIND_ALIAS_CHANGED)
        .bind(ZERO_ADDRESS)
        .bind(cursor_chain_id)
        .bind(cursor_resolver_address)
        .bind(i64::try_from(limit).context("resolver target page limit exceeds i64")?)
        .fetch_all(pool)
        .await
        .context("failed to load resolver_current rebuild target page")?;

    rows.into_iter()
        .map(|row| {
            let source_families = row
                .try_get::<Vec<String>, _>("source_families")?
                .into_iter()
                .filter_map(|source_family| {
                    resolver_profile_source_family_for_event_source(&source_family)
                        .map(str::to_owned)
                })
                .collect();
            Ok(ResolverTarget {
                chain_id: row.try_get("chain_id")?,
                resolver_address: normalize_resolver_address(
                    &row.try_get::<String, _>("resolver_address")?,
                ),
                profile_source_family: unique_source_family(source_families),
                enumerate_bindings: false,
            })
        })
        .collect()
}

fn target_resolver_page_query() -> String {
    format!(
        r#"
        WITH current_bindings AS (
            SELECT logical_name_id, resource_id
            FROM surface_bindings
            WHERE active_to IS NULL
              AND canonicality_state {CANONICAL_STATE_FILTER}
        ),
        latest_resolver_events AS (
            SELECT DISTINCT ON (ne.logical_name_id, ne.resource_id)
                ne.logical_name_id,
                ne.resource_id,
                ne.chain_id,
                ne.source_family,
                LOWER(ne.after_state->>'resolver') AS resolver_address
            FROM normalized_events ne
            WHERE ne.event_kind = $1
              AND ne.logical_name_id IS NOT NULL
              AND ne.resource_id IS NOT NULL
              AND ne.chain_id IS NOT NULL
              AND ne.canonicality_state {CANONICAL_STATE_FILTER}
            ORDER BY
                ne.logical_name_id,
                ne.resource_id,
                ne.block_number DESC NULLS LAST,
                ne.log_index DESC NULLS LAST,
                ne.normalized_event_id DESC
        ),
        raw_targets AS (
            SELECT
                lre.chain_id,
                lre.resolver_address,
                lre.source_family
            FROM latest_resolver_events lre
            INNER JOIN current_bindings cb
              ON cb.logical_name_id = lre.logical_name_id
             AND cb.resource_id = lre.resource_id
            WHERE lre.resolver_address IS NOT NULL
              AND lre.resolver_address <> ''
              AND lre.resolver_address <> $3

            UNION ALL

            SELECT
                pc.scope_detail->>'chain_id' AS chain_id,
                LOWER(pc.scope_detail->>'resolver_address') AS resolver_address,
                NULL::TEXT AS source_family
            FROM permissions_current pc
            WHERE pc.scope_kind = 'resolver'
              AND pc.scope_detail->>'chain_id' IS NOT NULL
              AND pc.scope_detail->>'chain_id' <> ''
              AND pc.scope_detail->>'resolver_address' IS NOT NULL
              AND pc.scope_detail->>'resolver_address' <> ''
              AND pc.canonicality_summary ->> 'status' IN (
                  'canonical',
                  'safe',
                  'finalized'
              )

            UNION ALL

            SELECT
                ne.chain_id,
                LOWER(ne.after_state->>'resolver') AS resolver_address,
                ne.source_family
            FROM normalized_events ne
            WHERE ne.event_kind = $2
              AND ne.chain_id IS NOT NULL
              AND ne.after_state->>'resolver' IS NOT NULL
              AND ne.canonicality_state {CANONICAL_STATE_FILTER}
        )
        SELECT
            chain_id,
            resolver_address,
            COALESCE(
                ARRAY_AGG(DISTINCT source_family ORDER BY source_family)
                    FILTER (WHERE source_family IS NOT NULL),
                ARRAY[]::TEXT[]
            ) AS source_families
        FROM raw_targets
        WHERE $4::TEXT IS NULL
           OR (chain_id, resolver_address) > ($4, $5)
        GROUP BY chain_id, resolver_address
        ORDER BY chain_id, resolver_address
        LIMIT $6
        "#
    )
}

fn unique_source_family(source_families: BTreeSet<String>) -> Option<String> {
    let mut source_families = source_families.into_iter();
    let source_family = source_families.next()?;
    source_families.next().is_none().then_some(source_family)
}

fn resolver_profile_source_family_for_event_source(source_family: &str) -> Option<&'static str> {
    match source_family {
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1 | SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => {
            Some(SOURCE_FAMILY_ENS_V1_RESOLVER_L1)
        }
        SOURCE_FAMILY_BASENAMES_BASE_REGISTRY | SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => {
            Some(SOURCE_FAMILY_BASENAMES_BASE_RESOLVER)
        }
        _ => None,
    }
}

pub(super) async fn load_current_bindings(
    pool: &PgPool,
    target: &ResolverTarget,
) -> Result<Vec<CurrentBindingSeed>> {
    let rows = sqlx::query(&format!(
        r#"
        WITH candidate_pairs AS (
            SELECT DISTINCT
                ne.logical_name_id,
                ne.resource_id
            FROM normalized_events ne
            WHERE ne.event_kind = $1
              AND ne.logical_name_id IS NOT NULL
              AND ne.resource_id IS NOT NULL
              AND ne.chain_id = $2
              AND ne.after_state->>'resolver' IS NOT NULL
              AND ne.after_state->>'resolver' <> ''
              AND LOWER(ne.after_state->>'resolver') = $3
              AND ne.canonicality_state {CANONICAL_STATE_FILTER}
        )
        SELECT
            candidate.logical_name_id,
            ns.canonical_display_name,
            ns.normalized_name,
            ns.namehash,
            candidate.resource_id,
            sb.surface_binding_id,
            sb.binding_kind,
            latest.normalized_event_id,
            latest.source_family,
            latest.manifest_version,
            latest.source_manifest_id,
            latest.chain_id,
            latest.block_number,
            latest.block_hash,
            latest.block_timestamp,
            latest.raw_fact_ref,
            latest.canonicality_state
        FROM candidate_pairs candidate
        INNER JOIN surface_bindings sb
          ON sb.logical_name_id = candidate.logical_name_id
         AND sb.resource_id = candidate.resource_id
         AND sb.active_to IS NULL
         AND sb.canonicality_state {CANONICAL_STATE_FILTER}
        INNER JOIN name_surfaces ns
          ON ns.logical_name_id = candidate.logical_name_id
         AND ns.canonicality_state {CANONICAL_STATE_FILTER}
        CROSS JOIN LATERAL (
            SELECT
                ne.normalized_event_id,
                ne.source_family,
                ne.manifest_version,
                ne.source_manifest_id,
                ne.chain_id,
                ne.block_number,
                ne.block_hash,
                rb.block_timestamp,
                ne.raw_fact_ref,
                ne.canonicality_state::TEXT AS canonicality_state,
                LOWER(ne.after_state->>'resolver') AS resolver_address
            FROM normalized_events ne
            LEFT JOIN chain_lineage rb
              ON rb.chain_id = ne.chain_id
             AND rb.block_hash = ne.block_hash
            WHERE ne.event_kind = $1
              AND ne.logical_name_id = candidate.logical_name_id
              AND ne.resource_id = candidate.resource_id
              AND ne.chain_id = $2
              AND ne.canonicality_state {CANONICAL_STATE_FILTER}
            ORDER BY
                ne.block_number DESC NULLS LAST,
                ne.log_index DESC NULLS LAST,
                ne.normalized_event_id DESC
            LIMIT 1
        ) latest
        WHERE latest.resolver_address = $3
        ORDER BY ns.canonical_display_name, candidate.logical_name_id, sb.surface_binding_id
        "#
    ))
    .bind(EVENT_KIND_RESOLVER_CHANGED)
    .bind(&target.chain_id)
    .bind(&target.resolver_address)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load current bindings for resolver {} on chain {}",
            target.resolver_address, target.chain_id
        )
    })?;

    rows.into_iter()
        .map(|row| {
            Ok(CurrentBindingSeed {
                chain_id: row.try_get("chain_id")?,
                logical_name_id: row.try_get("logical_name_id")?,
                canonical_display_name: row.try_get("canonical_display_name")?,
                normalized_name: row.try_get("normalized_name")?,
                namehash: row.try_get("namehash")?,
                resource_id: row.try_get("resource_id")?,
                surface_binding_id: row.try_get("surface_binding_id")?,
                binding_kind: parse_surface_binding_kind(
                    &row.try_get::<String, _>("binding_kind")?,
                )?,
                normalized_event_id: row.try_get("normalized_event_id")?,
                source_family: row.try_get("source_family")?,
                manifest_version: row.try_get("manifest_version")?,
                source_manifest_id: row.try_get("source_manifest_id")?,
                block_number: row.try_get("block_number")?,
                block_hash: row.try_get("block_hash")?,
                block_timestamp: row.try_get("block_timestamp")?,
                raw_fact_ref: row.try_get("raw_fact_ref")?,
                canonicality_state: parse_canonicality_state(
                    &row.try_get::<String, _>("canonicality_state")?,
                )?,
            })
        })
        .collect()
}

pub(super) async fn count_current_binding_candidate_pairs(
    pool: &PgPool,
    target: &ResolverTarget,
    limit: i64,
) -> Result<i64> {
    sqlx::query_scalar(&format!(
        r#"
        WITH candidate_pairs AS (
            SELECT DISTINCT
                ne.logical_name_id,
                ne.resource_id
            FROM normalized_events ne
            WHERE ne.event_kind = $1
              AND ne.logical_name_id IS NOT NULL
              AND ne.resource_id IS NOT NULL
              AND ne.chain_id = $2
              AND ne.after_state->>'resolver' IS NOT NULL
              AND ne.after_state->>'resolver' <> ''
              AND LOWER(ne.after_state->>'resolver') = $3
              AND ne.canonicality_state {CANONICAL_STATE_FILTER}
        )
        SELECT COUNT(*)::BIGINT
        FROM (
            SELECT 1
            FROM candidate_pairs candidate
            INNER JOIN surface_bindings sb
              ON sb.logical_name_id = candidate.logical_name_id
             AND sb.resource_id = candidate.resource_id
             AND sb.active_to IS NULL
             AND sb.canonicality_state {CANONICAL_STATE_FILTER}
            INNER JOIN name_surfaces ns
              ON ns.logical_name_id = candidate.logical_name_id
             AND ns.canonicality_state {CANONICAL_STATE_FILTER}
            CROSS JOIN LATERAL (
                SELECT
                    LOWER(ne.after_state->>'resolver') AS resolver_address
                FROM normalized_events ne
                WHERE ne.event_kind = $1
                  AND ne.logical_name_id = candidate.logical_name_id
                  AND ne.resource_id = candidate.resource_id
                  AND ne.chain_id = $2
                  AND ne.canonicality_state {CANONICAL_STATE_FILTER}
                ORDER BY
                    ne.block_number DESC NULLS LAST,
                    ne.log_index DESC NULLS LAST,
                    ne.normalized_event_id DESC
                LIMIT 1
            ) latest
            WHERE latest.resolver_address = $3
            LIMIT $4
        ) current_pairs
        "#
    ))
    .bind(EVENT_KIND_RESOLVER_CHANGED)
    .bind(&target.chain_id)
    .bind(&target.resolver_address)
    .bind(limit)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to count current binding candidates for resolver {} on chain {}",
            target.resolver_address, target.chain_id
        )
    })
}

pub(super) async fn load_resolver_permissions(
    pool: &PgPool,
    target: &ResolverTarget,
) -> Result<Vec<PermissionsCurrentRow>> {
    let mut rows = load_permissions_current_for_resolver_scope(
        pool,
        &target.chain_id,
        &target.resolver_address,
    )
    .await?;
    rows.sort_by(|left, right| {
        left.subject
            .cmp(&right.subject)
            .then_with(|| left.resource_id.cmp(&right.resource_id))
            .then_with(|| left.manifest_version.cmp(&right.manifest_version))
    });
    Ok(rows)
}

pub(super) async fn load_alias_events(
    pool: &PgPool,
    target: &ResolverTarget,
) -> Result<Vec<AliasSeed>> {
    let rows = sqlx::query(&format!(
        r#"
        SELECT DISTINCT ON (ne.after_state->>'from_dns_encoded_name')
            ne.normalized_event_id,
            ne.logical_name_id,
            ne.resource_id,
            ne.source_family,
            ne.manifest_version,
            ne.source_manifest_id,
            ne.chain_id,
            ne.block_number,
            ne.block_hash,
            rb.block_timestamp,
            ne.raw_fact_ref,
            ne.canonicality_state::TEXT AS canonicality_state,
            ne.after_state,
            LOWER(ne.after_state->>'resolver') AS resolver_address
        FROM normalized_events ne
        LEFT JOIN chain_lineage rb
          ON rb.chain_id = ne.chain_id
         AND rb.block_hash = ne.block_hash
        WHERE ne.event_kind = $1
          AND ne.chain_id = $2
          AND ne.canonicality_state {CANONICAL_STATE_FILTER}
          AND LOWER(ne.after_state->>'resolver') = $3
        ORDER BY
            ne.after_state->>'from_dns_encoded_name',
            ne.block_number DESC NULLS LAST,
            ne.log_index DESC NULLS LAST,
            ne.normalized_event_id DESC
        "#
    ))
    .bind(EVENT_KIND_ALIAS_CHANGED)
    .bind(&target.chain_id)
    .bind(&target.resolver_address)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load AliasChanged events for resolver {} on chain {}",
            target.resolver_address, target.chain_id
        )
    })?;

    rows.into_iter()
        .map(|row| {
            Ok(AliasSeed {
                chain_id: row.try_get("chain_id")?,
                resolver_address: normalize_resolver_address(
                    &row.try_get::<String, _>("resolver_address")?,
                ),
                normalized_event_id: row.try_get("normalized_event_id")?,
                logical_name_id: row.try_get("logical_name_id")?,
                resource_id: row.try_get("resource_id")?,
                source_family: row.try_get("source_family")?,
                manifest_version: row.try_get("manifest_version")?,
                source_manifest_id: row.try_get("source_manifest_id")?,
                block_number: row.try_get("block_number")?,
                block_hash: row.try_get("block_hash")?,
                block_timestamp: row.try_get("block_timestamp")?,
                raw_fact_ref: row.try_get("raw_fact_ref")?,
                canonicality_state: parse_canonicality_state(
                    &row.try_get::<String, _>("canonicality_state")?,
                )?,
                after_state: row.try_get("after_state")?,
            })
        })
        .collect()
}

pub(super) fn normalize_resolver_address(value: &str) -> String {
    normalize_evm_address(value)
}

fn parse_surface_binding_kind(value: &str) -> Result<SurfaceBindingKind> {
    SurfaceBindingKind::parse(value)
}

#[cfg(test)]
mod paging_tests {
    use super::*;

    #[test]
    fn target_resolver_page_query_pushes_cursor_and_limit_into_database() {
        let query = target_resolver_page_query();

        assert!(query.contains("(chain_id, resolver_address) > ($4, $5)"));
        assert!(query.contains("LIMIT $6"));
    }
}
