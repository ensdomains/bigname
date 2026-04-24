use anyhow::Result;
use bigname_storage::{
    CanonicalityInspection, CanonicalityInspectionStatus, RawFactAuditCounts,
    RawPayloadCacheAuditMetadata,
};
use serde_json::{Value, json};

use super::formatting::{canonicality_state_label, format_timestamp};
use super::{InspectCanonicalityArgs, connect_read_only};

pub(in crate::inspect) async fn inspect_canonicality(args: InspectCanonicalityArgs) -> Result<()> {
    let pool = connect_read_only(&args.database).await?;
    let inspection =
        bigname_storage::inspect_block_canonicality(&pool, &args.chain_id, &args.block_hash)
            .await?;
    let payload_cache_metadata = bigname_storage::list_raw_payload_cache_audit_metadata(
        &pool,
        &args.chain_id,
        &args.block_hash,
    )
    .await?;

    println!(
        "{}",
        render_canonicality_inspection(&inspection, &payload_cache_metadata)
    );
    Ok(())
}

pub(in crate::inspect) fn render_canonicality_inspection(
    inspection: &CanonicalityInspection,
    payload_cache_metadata: &[RawPayloadCacheAuditMetadata],
) -> Value {
    json!({
        "chain_id": inspection.chain_id.as_str(),
        "block_hash": inspection.block_hash.as_str(),
        "status": canonicality_inspection_status_label(inspection.status),
        "lineage_canonicality": inspection.lineage_state.map(canonicality_state_label),
        "parent_hash": inspection.parent_hash.as_deref(),
        "block_number": inspection.block_number,
        "raw_fact_counts": render_raw_fact_counts(&inspection.raw_fact_counts),
        "raw_payload_cache_metadata": render_raw_payload_cache_metadata(payload_cache_metadata),
        "normalized_event_count": inspection.normalized_event_count,
        "states": {
            "observed": inspection.status == CanonicalityInspectionStatus::Observed,
            "canonical": inspection.status == CanonicalityInspectionStatus::Canonical,
            "safe": inspection.status == CanonicalityInspectionStatus::Safe,
            "finalized": inspection.status == CanonicalityInspectionStatus::Finalized,
            "missing": inspection.status == CanonicalityInspectionStatus::Missing,
            "orphaned": inspection.status == CanonicalityInspectionStatus::Orphaned,
        }
    })
}

fn render_raw_fact_counts(counts: &RawFactAuditCounts) -> Value {
    json!({
        "raw_blocks": counts.raw_block_count,
        "raw_code_hashes": counts.raw_code_hash_count,
        "raw_transactions": counts.raw_transaction_count,
        "raw_receipts": counts.raw_receipt_count,
        "raw_logs": counts.raw_log_count,
        "raw_call_snapshots": counts.raw_call_snapshot_count,
        "total": counts.total(),
    })
}

fn render_raw_payload_cache_metadata(metadata: &[RawPayloadCacheAuditMetadata]) -> Value {
    let retained_digest_count = metadata
        .iter()
        .filter(|entry| entry.retained_digest.is_some())
        .count();
    let metadata_only_count = metadata.len() - retained_digest_count;
    let payload_size_bytes_total: i64 = metadata.iter().map(|entry| entry.payload_size_bytes).sum();

    json!({
        "metadata_count": metadata.len(),
        "retained_digest_count": retained_digest_count,
        "metadata_only_count": metadata_only_count,
        "payload_size_bytes_total": payload_size_bytes_total,
        "entries": metadata
            .iter()
            .map(render_raw_payload_cache_metadata_entry)
            .collect::<Vec<_>>(),
    })
}

fn render_raw_payload_cache_metadata_entry(metadata: &RawPayloadCacheAuditMetadata) -> Value {
    let retained_digest_status = if metadata.retained_digest.is_some() {
        "retained"
    } else {
        "metadata_only"
    };

    json!({
        "payload_kind": metadata.payload_kind.as_str(),
        "block_number": metadata.block_number,
        "payload_size_bytes": metadata.payload_size_bytes,
        "content_type": metadata.content_type.as_deref(),
        "content_encoding": metadata.content_encoding.as_deref(),
        "canonicality_state": canonicality_state_label(metadata.canonicality_state),
        "retained_digest_status": retained_digest_status,
        "digest_algorithm": metadata.digest_algorithm.as_deref(),
        "retained_digest": metadata.retained_digest.as_deref(),
        "timestamps": {
            "first_observed_at": format_timestamp(metadata.first_observed_at),
            "last_observed_at": format_timestamp(metadata.last_observed_at),
        },
    })
}

const fn canonicality_inspection_status_label(
    status: CanonicalityInspectionStatus,
) -> &'static str {
    match status {
        CanonicalityInspectionStatus::Missing => "missing",
        CanonicalityInspectionStatus::Observed => "observed",
        CanonicalityInspectionStatus::Canonical => "canonical",
        CanonicalityInspectionStatus::Safe => "safe",
        CanonicalityInspectionStatus::Finalized => "finalized",
        CanonicalityInspectionStatus::Orphaned => "orphaned",
    }
}
