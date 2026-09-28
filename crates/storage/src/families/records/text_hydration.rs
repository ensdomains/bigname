//! Apply a block-pinned F6 text overlay only while its event, version, admission and canonical
//! lineage still match. The retained event columns remain the baseline for every failed check.
use serde_json::{Map, Value, json};

pub(super) const KEY: &str = "canonical_head_multicall_hydration";

pub(super) const COLUMNS: &str = "CASE WHEN value.record_family = 'text'
    AND value.status = 'unsupported'
    AND value.hydrated_value -> 'source_position' = jsonb_build_object(
        'block_number', value.block_number, 'transaction_index', value.transaction_index,
        'log_index', value.log_index, 'event_identity', value.event_identity)
    AND value.hydrated_value -> 'version_position' = COALESCE(partition.version_position, 'null'::jsonb)
    AND value.hydrated_value -> 'admission' = jsonb_build_object(
        'classification', classification.classification,
        'support_status', classification.support_status,
        'unsupported_reason', classification.unsupported_reason,
        'manifest_id', classification.manifest_id)
    AND classification.support_status = 'supported'
    AND value.hydrated_value ->> 'namehash' =
        CASE WHEN value.arm = 'named' THEN surface.namehash ELSE value.node END
    AND EXISTS (SELECT 1 FROM bigname_phase.chain_lineage lineage
        JOIN bigname_phase.project_family_marker marker ON marker.chain_id = lineage.chain_id
        WHERE lineage.chain_id = value.chain_id
          AND lineage.block_number = value.hydrated_at_block
          AND lineage.block_number <= marker.current_block_number
          AND lineage.block_hash = value.hydrated_value ->> 'block_hash'
          AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized'))
    THEN value.hydrated_value || jsonb_build_object('block_number', value.hydrated_at_block)
    END AS text_hydration";

pub(crate) fn apply(entry: &mut Map<String, Value>, payload: &Value) {
    let Some(overlay) = payload.get(KEY) else {
        return;
    };
    let baseline = Value::Object(entry.clone());
    match overlay["status"].as_str() {
        Some("success") => {
            let Some(value) = overlay.get("value") else {
                return;
            };
            entry.insert("status".into(), json!("success"));
            entry.insert("value".into(), value.clone());
        }
        Some("not_found") => {
            entry.insert("status".into(), json!("not_found"));
            entry.remove("value");
        }
        _ => return,
    }
    entry.remove("unsupported_reason");
    entry.insert(
        KEY.into(),
        json!({
            "chain_id": "ethereum-mainnet", "block_number": overlay["block_number"],
            "block_hash": overlay["block_hash"], "source": "multicall_at_canonical_head",
            "baseline": baseline,
        }),
    );
}
