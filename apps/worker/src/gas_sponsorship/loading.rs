use anyhow::{Context, Result};
use bigname_storage::sql_row;
use sqlx::PgPool;

use super::types::{GlobalFoldEventRow, NameFoldEventRow};

pub(super) const GAS_SPONSORSHIP_NAMESPACE: &str = "ens";
pub(super) const ENTRYPOINT_DERIVATION_KIND: &str = "entrypoint_user_operation";
/// Registrar-derivation registration facts only: the ENSv2 registry also
/// emits `RegistrationRenewed` for the same renewal under its own derivation,
/// and counting both would double every purchased term.
pub(super) const REGISTRATION_DERIVATION_KINDS: &[&str] =
    &["ens_v1_unwrapped_authority", "ens_v2_registrar"];
pub(super) const REGISTRATION_EVENT_KINDS: &[&str] = &[
    "RegistrationGranted",
    "RegistrarNameRegistered",
    "RegistrationRenewed",
];

const CANONICAL_STATE_FILTER: &str = r#"IN (
    'canonical'::canonicality_state,
    'safe'::canonicality_state,
    'finalized'::canonicality_state
)"#;

/// Every name with registration facts (registrar derivations) or sponsored
/// write facts in the namespace.
pub(super) async fn load_target_logical_name_ids(pool: &PgPool) -> Result<Vec<String>> {
    let query = format!(
        r#"
        SELECT DISTINCT logical_name_id
        FROM normalized_events
        WHERE namespace = $1
          AND logical_name_id IS NOT NULL
          AND canonicality_state {CANONICAL_STATE_FILTER}
          AND (
              (derivation_kind = ANY($2::TEXT[]) AND event_kind = ANY($3::TEXT[]))
              OR (derivation_kind = $4 AND event_kind = 'SponsoredNameWriteObserved')
          )
        ORDER BY logical_name_id
        "#
    );
    let rows = sqlx::query(&query)
        .bind(GAS_SPONSORSHIP_NAMESPACE)
        .bind(REGISTRATION_DERIVATION_KINDS)
        .bind(REGISTRATION_EVENT_KINDS)
        .bind(ENTRYPOINT_DERIVATION_KIND)
        .fetch_all(pool)
        .await
        .context("failed to load gas_sponsorship_current target names")?;
    rows.iter()
        .map(|row| sql_row::get(row, "logical_name_id"))
        .collect()
}

/// Registration and sponsored-write facts for one name in canonical chain
/// order.
pub(super) async fn load_name_fold_events(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<Vec<NameFoldEventRow>> {
    let query = format!(
        r#"
        SELECT
            ne.normalized_event_id,
            ne.event_kind,
            ne.chain_id,
            ne.block_number,
            ne.block_hash,
            lineage.block_timestamp,
            ne.manifest_version,
            ne.canonicality_state::TEXT AS canonicality_state,
            ne.before_state,
            ne.after_state
        FROM normalized_events ne
        LEFT JOIN chain_lineage lineage
          ON lineage.chain_id = ne.chain_id
         AND lineage.block_hash = ne.block_hash
        WHERE ne.namespace = $1
          AND ne.logical_name_id = $2
          AND ne.canonicality_state {CANONICAL_STATE_FILTER}
          AND (
              (ne.derivation_kind = ANY($3::TEXT[]) AND ne.event_kind = ANY($4::TEXT[]))
              OR (ne.derivation_kind = $5 AND ne.event_kind = 'SponsoredNameWriteObserved')
          )
        ORDER BY
            ne.block_number NULLS FIRST,
            COALESCE(ne.log_index, 2147483647),
            ne.event_identity
        "#
    );
    sqlx::query_as::<_, NameFoldEventRow>(&query)
        .bind(GAS_SPONSORSHIP_NAMESPACE)
        .bind(logical_name_id)
        .bind(REGISTRATION_DERIVATION_KINDS)
        .bind(REGISTRATION_EVENT_KINDS)
        .bind(ENTRYPOINT_DERIVATION_KIND)
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!("failed to load gas_sponsorship fold events for {logical_name_id}")
        })
}

/// The name's namehash, preferring the canonical name surface, else any
/// sponsored-write node payload.
pub(super) async fn load_name_namehash(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<Option<String>> {
    let query = format!(
        r#"
        SELECT namehash
        FROM name_surfaces
        WHERE logical_name_id = $1
          AND canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY block_number DESC
        LIMIT 1
        "#
    );
    let surface_namehash: Option<String> = sqlx::query_scalar(&query)
        .bind(logical_name_id)
        .fetch_optional(pool)
        .await
        .with_context(|| format!("failed to load name surface namehash for {logical_name_id}"))?;
    if surface_namehash.is_some() {
        return Ok(surface_namehash);
    }

    let write_query = format!(
        r#"
        SELECT after_state->>'node' AS node
        FROM normalized_events
        WHERE namespace = $1
          AND logical_name_id = $2
          AND derivation_kind = $3
          AND event_kind = 'SponsoredNameWriteObserved'
          AND after_state->>'node' IS NOT NULL
          AND canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY normalized_event_id DESC
        LIMIT 1
        "#
    );
    sqlx::query_scalar(&write_query)
        .bind(GAS_SPONSORSHIP_NAMESPACE)
        .bind(logical_name_id)
        .bind(ENTRYPOINT_DERIVATION_KIND)
        .fetch_optional(pool)
        .await
        .with_context(|| format!("failed to load write-event namehash for {logical_name_id}"))
}

/// Namespaces with sponsored-operation facts.
pub(super) async fn load_target_namespaces(pool: &PgPool) -> Result<Vec<String>> {
    let query = format!(
        r#"
        SELECT DISTINCT namespace
        FROM normalized_events
        WHERE derivation_kind = $1
          AND event_kind IN ('SponsoredUserOperationObserved', 'PriceFeedAnswerUpdated')
          AND canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY namespace
        "#
    );
    let rows = sqlx::query(&query)
        .bind(ENTRYPOINT_DERIVATION_KIND)
        .fetch_all(pool)
        .await
        .context("failed to load gas_sponsorship_global_current target namespaces")?;
    rows.iter()
        .map(|row| sql_row::get(row, "namespace"))
        .collect()
}

/// Sponsored-operation and price facts for one namespace in canonical chain
/// order.
pub(super) async fn load_global_fold_events(
    pool: &PgPool,
    namespace: &str,
) -> Result<Vec<GlobalFoldEventRow>> {
    let query = format!(
        r#"
        SELECT
            ne.normalized_event_id,
            ne.event_kind,
            ne.chain_id,
            ne.block_number,
            ne.block_hash,
            lineage.block_timestamp,
            ne.manifest_version,
            ne.canonicality_state::TEXT AS canonicality_state,
            ne.after_state
        FROM normalized_events ne
        LEFT JOIN chain_lineage lineage
          ON lineage.chain_id = ne.chain_id
         AND lineage.block_hash = ne.block_hash
        WHERE ne.namespace = $1
          AND ne.derivation_kind = $2
          AND ne.event_kind IN ('SponsoredUserOperationObserved', 'PriceFeedAnswerUpdated')
          AND ne.canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY
            ne.block_number NULLS FIRST,
            COALESCE(ne.log_index, 2147483647),
            ne.event_identity
        "#
    );
    sqlx::query_as::<_, GlobalFoldEventRow>(&query)
        .bind(namespace)
        .bind(ENTRYPOINT_DERIVATION_KIND)
        .fetch_all(pool)
        .await
        .with_context(|| format!("failed to load global gas_sponsorship events for {namespace}"))
}
