use std::collections::BTreeMap;

use anyhow::{Context, Result};
use bigname_storage::{
    GasSponsorshipCurrentRow, GasSponsorshipGlobalCurrentRow, clear_gas_sponsorship_current,
    clear_gas_sponsorship_global_current, delete_gas_sponsorship_current,
    upsert_gas_sponsorship_current_rows, upsert_gas_sponsorship_global_current_row,
};
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::types::time::OffsetDateTime;

use super::loading::{
    load_global_fold_events, load_name_fold_events, load_name_namehash,
    load_target_logical_name_ids, load_target_namespaces,
};
use super::math::{fold_global_accounting, fold_name_accounting};
use super::types::{
    GasSponsorshipCurrentRebuildSummary, GasSponsorshipGlobalRebuildSummary, GlobalFoldEventRow,
    NameFoldEventRow,
};

const UPSERT_BATCH_SIZE: usize = 500;

pub(super) async fn rebuild_gas_sponsorship_current(
    pool: &PgPool,
    logical_name_id: Option<&str>,
) -> Result<GasSponsorshipCurrentRebuildSummary> {
    match logical_name_id {
        Some(logical_name_id) => rebuild_one_name(pool, logical_name_id).await,
        None => rebuild_all_names(pool).await,
    }
}

async fn rebuild_one_name(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<GasSponsorshipCurrentRebuildSummary> {
    let row = build_name_row(pool, logical_name_id).await?;
    match row {
        Some(row) => {
            let upserted_row_count =
                upsert_gas_sponsorship_current_rows(pool, std::slice::from_ref(&row)).await?;
            Ok(GasSponsorshipCurrentRebuildSummary {
                requested_name_count: 1,
                upserted_row_count,
                deleted_row_count: 0,
            })
        }
        None => {
            let deleted_row_count = delete_gas_sponsorship_current(pool, logical_name_id).await?;
            Ok(GasSponsorshipCurrentRebuildSummary {
                requested_name_count: 1,
                upserted_row_count: 0,
                deleted_row_count,
            })
        }
    }
}

async fn rebuild_all_names(pool: &PgPool) -> Result<GasSponsorshipCurrentRebuildSummary> {
    let target_names = load_target_logical_name_ids(pool).await?;
    let deleted_row_count = clear_gas_sponsorship_current(pool).await?;

    let mut upserted_row_count = 0usize;
    let mut batch = Vec::with_capacity(UPSERT_BATCH_SIZE);
    for logical_name_id in &target_names {
        if let Some(row) = build_name_row(pool, logical_name_id).await? {
            batch.push(row);
        }
        if batch.len() >= UPSERT_BATCH_SIZE {
            upserted_row_count += upsert_gas_sponsorship_current_rows(pool, &batch).await?;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        upserted_row_count += upsert_gas_sponsorship_current_rows(pool, &batch).await?;
    }

    Ok(GasSponsorshipCurrentRebuildSummary {
        requested_name_count: target_names.len(),
        upserted_row_count,
        deleted_row_count,
    })
}

async fn build_name_row(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<Option<GasSponsorshipCurrentRow>> {
    let events = load_name_fold_events(pool, logical_name_id).await?;
    if events.is_empty() {
        return Ok(None);
    }
    let Some(namehash) = load_name_namehash(pool, logical_name_id)
        .await?
        .filter(|namehash| is_namehash_shaped(namehash))
    else {
        // No canonical surface and no node-bearing write (or only a surface
        // whose namehash is not a 32-byte hash): nothing identifies the name
        // on-chain yet, so there is no row to serve.
        return Ok(None);
    };
    let (namespace, normalized_name) = logical_name_id.split_once(':').with_context(|| {
        format!("gas_sponsorship_current key {logical_name_id} must be namespace:name")
    })?;

    let accounting = fold_name_accounting(&events);
    let manifest_version = events
        .iter()
        .map(|event| event.manifest_version)
        .max()
        .unwrap_or(1);

    Ok(Some(GasSponsorshipCurrentRow {
        logical_name_id: logical_name_id.to_owned(),
        namespace: namespace.to_owned(),
        normalized_name: normalized_name.to_owned(),
        namehash: namehash.to_ascii_lowercase(),
        lease_start_at: accounting.lease_start_at,
        registered_seconds_total: accounting.registered_seconds_total,
        earned_updates: accounting.earned_updates,
        spent_updates: accounting.spent_updates,
        last_sponsored_write_at: accounting.last_sponsored_write_at,
        provenance: name_provenance(&events),
        coverage: coverage_value(),
        chain_positions: chain_positions_value(events.iter().map(|event| {
            (
                event.chain_id.as_str(),
                event.block_number,
                event.block_hash.as_deref(),
                event.block_timestamp,
            )
        })),
        canonicality_summary: canonicality_summary_value(
            events
                .iter()
                .map(|event| (event.chain_id.as_str(), event.canonicality_state.as_str())),
        ),
        manifest_version,
        last_recomputed_at: OffsetDateTime::now_utc(),
    }))
}

pub(super) async fn rebuild_gas_sponsorship_global_current(
    pool: &PgPool,
    namespace: Option<&str>,
) -> Result<GasSponsorshipGlobalRebuildSummary> {
    let (namespaces, deleted_row_count) = match namespace {
        Some(namespace) => (vec![namespace.to_owned()], 0),
        None => {
            // A full rebuild also clears rows for namespaces whose facts have
            // vanished (e.g. after reorg repair).
            let target_namespaces = load_target_namespaces(pool).await?;
            let deleted_row_count = clear_gas_sponsorship_global_current(pool).await?;
            (target_namespaces, deleted_row_count)
        }
    };

    let mut upserted_row_count = 0usize;
    for namespace in &namespaces {
        let events = load_global_fold_events(pool, namespace).await?;
        let row = build_global_row(namespace, &events);
        upsert_gas_sponsorship_global_current_row(pool, &row).await?;
        upserted_row_count += 1;
    }

    Ok(GasSponsorshipGlobalRebuildSummary {
        requested_namespace_count: namespaces.len(),
        upserted_row_count,
        deleted_row_count,
    })
}

fn build_global_row(
    namespace: &str,
    events: &[GlobalFoldEventRow],
) -> GasSponsorshipGlobalCurrentRow {
    let accounting = fold_global_accounting(events);
    let manifest_version = events
        .iter()
        .map(|event| event.manifest_version)
        .max()
        .unwrap_or(1);

    GasSponsorshipGlobalCurrentRow {
        namespace: namespace.to_owned(),
        sponsored_op_count: accounting.sponsored_op_count,
        attributed_op_count: accounting.attributed_op_count,
        failed_op_count: accounting.failed_op_count,
        gas_wei_total: accounting.gas_wei_total.to_string(),
        failed_gas_wei_total: accounting.failed_gas_wei_total.to_string(),
        usd_e8_total: accounting.usd_e8_total.to_string(),
        unpriced_wei_total: accounting.unpriced_wei_total.to_string(),
        provenance: global_provenance(events),
        coverage: coverage_value(),
        chain_positions: chain_positions_value(events.iter().map(|event| {
            (
                event.chain_id.as_str(),
                event.block_number,
                event.block_hash.as_deref(),
                event.block_timestamp,
            )
        })),
        canonicality_summary: canonicality_summary_value(
            events
                .iter()
                .map(|event| (event.chain_id.as_str(), event.canonicality_state.as_str())),
        ),
        manifest_version,
        last_recomputed_at: OffsetDateTime::now_utc(),
    }
}

fn name_provenance(events: &[NameFoldEventRow]) -> Value {
    json!({
        "derivation_kind": "gas_sponsorship",
        "normalized_event_ids": events
            .iter()
            .map(|event| event.normalized_event_id)
            .collect::<Vec<_>>(),
    })
}

/// Global inputs grow unboundedly, so provenance carries the span, not every
/// event id.
fn global_provenance(events: &[GlobalFoldEventRow]) -> Value {
    json!({
        "derivation_kind": "gas_sponsorship",
        "normalized_event_count": events.len(),
        "first_normalized_event_id": events.first().map(|event| event.normalized_event_id),
        "last_normalized_event_id": events.iter().map(|event| event.normalized_event_id).max(),
    })
}

fn coverage_value() -> Value {
    json!({
        "status": "partial",
        "exhaustiveness": "not_applicable",
        "source_classes_considered": [
            "ens_gas_sponsorship_l1",
            "ens_v1_registrar_l1",
            "ens_v2_registrar_l1",
        ],
        "enumeration_basis": "gas_sponsorship_lookup",
        "unsupported_reason": null,
    })
}

fn chain_positions_value<'event>(
    positions: impl Iterator<
        Item = (
            &'event str,
            Option<i64>,
            Option<&'event str>,
            Option<OffsetDateTime>,
        ),
    >,
) -> Value {
    let mut latest_by_chain =
        BTreeMap::<String, (Option<i64>, Option<String>, Option<OffsetDateTime>)>::new();
    for (chain_id, block_number, block_hash, block_timestamp) in positions {
        let candidate = (block_number, block_hash.map(str::to_owned), block_timestamp);
        match latest_by_chain.get(chain_id) {
            Some((current_number, _, _)) if *current_number >= block_number => {}
            _ => {
                latest_by_chain.insert(chain_id.to_owned(), candidate);
            }
        }
    }

    Value::Object(
        latest_by_chain
            .into_iter()
            .map(|(chain_id, (block_number, block_hash, block_timestamp))| {
                let slot = chain_slot(&chain_id);
                (
                    slot,
                    json!({
                        "chain_id": chain_id,
                        "block_number": block_number,
                        "block_hash": block_hash,
                        "timestamp": block_timestamp
                            .map(|at| at.unix_timestamp()),
                    }),
                )
            })
            .collect(),
    )
}

fn canonicality_summary_value<'event>(
    states: impl Iterator<Item = (&'event str, &'event str)>,
) -> Value {
    let mut weakest_by_chain = BTreeMap::<String, String>::new();
    let mut weakest_overall: Option<String> = None;
    for (chain_id, state) in states {
        let entry = weakest_by_chain
            .entry(chain_id.to_owned())
            .or_insert_with(|| state.to_owned());
        if canonicality_rank(state) < canonicality_rank(entry) {
            *entry = state.to_owned();
        }
        match &weakest_overall {
            Some(current) if canonicality_rank(state) >= canonicality_rank(current) => {}
            _ => weakest_overall = Some(state.to_owned()),
        }
    }

    json!({
        "status": weakest_overall.unwrap_or_else(|| "canonical".to_owned()),
        "chains": weakest_by_chain,
    })
}

/// Lower is weaker; the fold inputs are already restricted to the canonical
/// branch, so this only distinguishes canonical/safe/finalized.
fn canonicality_rank(state: &str) -> u8 {
    match state {
        "finalized" => 3,
        "safe" => 2,
        "canonical" => 1,
        _ => 0,
    }
}

fn is_namehash_shaped(namehash: &str) -> bool {
    namehash.len() == 66
        && namehash.starts_with("0x")
        && namehash[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn chain_slot(chain_id: &str) -> String {
    match chain_id {
        "ethereum-mainnet" => "ethereum".to_owned(),
        "base-mainnet" => "base".to_owned(),
        _ => chain_id.to_owned(),
    }
}
