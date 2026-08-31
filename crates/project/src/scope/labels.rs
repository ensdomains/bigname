use std::collections::BTreeSet;

use alloy_primitives::{hex, keccak256};
use serde_json::Value;
use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(super) async fn include_changed_children(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<()> {
    let states = sqlx::query_scalar::<_, Value>(
        "SELECT after_state
         FROM project_changed_events
         WHERE after_state ?| ARRAY[
             'raw_label', 'raw_label_hex', 'raw_labels', 'raw_labels_hex'
         ]",
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to load changed child label observations", error)
    })?;
    let labelhashes = observed_labelhashes(&states)?;
    if labelhashes.is_empty() {
        return Ok(());
    }

    // The changed-event rows are immutable per-observation evidence. The label-preimage row is a
    // shared winner whose provenance may be replaced by another chain before this transaction.
    // Child publication joins that winner without a namespace, so one observation restages
    // matching ENS and Basenames children within this Project chain. Cross-chain propagation has
    // separate lifecycle and readiness requirements.
    sqlx::query(
        "WITH changed_labels AS MATERIALIZED (
             SELECT
                    preimage.labelhash,
                    preimage.raw_label,
                    CASE WHEN preimage.normalized_under_version
                         THEN preimage.decoded_label
                    END AS decoded_label
             FROM unnest($2::text[]) changed(labelhash)
             JOIN label_preimages preimage USING (labelhash)
             WHERE preimage.source_kind <> 'ens_rainbow_import'
         ), edges AS (
             SELECT child.parent_logical_name_id, child.child_logical_name_id
             FROM changed_labels changed
             CROSS JOIN LATERAL (
                 (SELECT child.parent_logical_name_id,
                         child.child_logical_name_id,
                         child.raw_label,
                         child.decoded_label
                  FROM children_current child
                  WHERE child.namespace = 'ens'
                    AND lower(child.labelhash) = changed.labelhash
                    AND child.provenance ->> 'chain_id' = $1
                  OFFSET 0)
                 UNION ALL
                 (SELECT child.parent_logical_name_id,
                         child.child_logical_name_id,
                         child.raw_label,
                         child.decoded_label
                  FROM children_current child
                  WHERE child.namespace = 'basenames'
                    AND lower(child.labelhash) = changed.labelhash
                    AND child.provenance ->> 'chain_id' = $1
                  OFFSET 0)
             ) child
             WHERE child.raw_label IS DISTINCT FROM changed.raw_label
                OR child.decoded_label IS DISTINCT FROM changed.decoded_label
         )
         INSERT INTO project_scope_children
         SELECT parent_logical_name_id FROM edges
         UNION
         SELECT child_logical_name_id FROM edges
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .bind(labelhashes.into_iter().collect::<Vec<_>>())
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to scope newly observed child labels", error)
    })?;
    Ok(())
}

fn observed_labelhashes(states: &[Value]) -> Result<BTreeSet<String>> {
    let mut labelhashes = BTreeSet::new();
    for state in states {
        if let Some(value) = state.get("raw_label")
            && let Some(label) = raw_label_bytes(value)?
        {
            labelhashes.insert(format!("{:#x}", keccak256(label)));
        }
        if let Some(encoded) = state.get("raw_label_hex").and_then(Value::as_str) {
            labelhashes.insert(format!("{:#x}", keccak256(decode_label(encoded)?)));
        }
        if let Some(labels) = state.get("raw_labels").and_then(Value::as_array) {
            for label in labels.iter().filter_map(Value::as_str) {
                labelhashes.insert(format!("{:#x}", keccak256(label.as_bytes())));
            }
        }
        if let Some(labels) = state.get("raw_labels_hex").and_then(Value::as_array) {
            for encoded in labels.iter().filter_map(Value::as_str) {
                labelhashes.insert(format!("{:#x}", keccak256(decode_label(encoded)?)));
            }
        }
    }
    Ok(labelhashes)
}

fn raw_label_bytes(value: &Value) -> Result<Option<Vec<u8>>> {
    if let Some(label) = value.as_str() {
        return Ok(Some(label.as_bytes().to_vec()));
    }
    let Some(encoded) = value
        .as_object()
        .filter(|object| object.get("encoding").and_then(Value::as_str) == Some("hex"))
        .and_then(|object| object.get("bytes"))
        .and_then(Value::as_str)
    else {
        return Ok(None);
    };
    decode_label(encoded).map(Some)
}

fn decode_label(encoded: &str) -> Result<Vec<u8>> {
    hex::decode(encoded).map_err(|error| {
        ProjectError::data_integrity(format!(
            "changed label observation has invalid raw label hex: {error}"
        ))
    })
}
