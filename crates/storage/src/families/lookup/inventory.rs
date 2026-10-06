//! Split the shared inventory result into small metadata and selected keys, and assemble the
//! same logical inventory at read. Record-key and family order is the database's collation.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

use super::{LookupInventoryPublication, LookupRecordEntry};
use crate::IdentityRecordInventoryRow;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LookupInventoryMetadata {
    pub record_version_boundary_key: String,
    pub support_status: String,
    pub unsupported_reason: Option<String>,
    pub provenance: Value,
    /// Fixed classification/getter gaps only. Observed unsupported families come from keys.
    pub declared_unsupported_families: Vec<Value>,
}

impl LookupInventoryPublication {
    pub fn metadata(&self) -> Option<LookupInventoryMetadata> {
        let inventory = self.inventory.as_ref()?;
        let row = &inventory.row;
        let mut provenance = row.provenance.clone();
        if let Some(object) = provenance.as_object_mut() {
            object.remove("record_event_ids");
            object.remove("exact_nonempty_not_found_record_keys");
        }
        if let Some(mirror) = provenance.get_mut("mirror").and_then(Value::as_object_mut) {
            mirror.remove("mirrored_name");
        }
        let declared_unsupported_families = row
            .unsupported_families
            .as_array()
            .into_iter()
            .flatten()
            .filter(|family| {
                family["unsupported_reason"] != "record_family_not_supported_in_phase6_projection"
            })
            .cloned()
            .collect();
        Some(LookupInventoryMetadata {
            record_version_boundary_key: inventory.record_version_boundary_key.clone(),
            support_status: if row.coverage["status"] == "projected" {
                "supported"
            } else {
                "unsupported"
            }
            .into(),
            unsupported_reason: row.coverage["unsupported_reason"]
                .as_str()
                .map(str::to_owned),
            provenance,
            declared_unsupported_families,
        })
    }
}

impl LookupInventoryMetadata {
    /// `records` and `unsupported_families` must be ordered by database collation, as in the
    /// shared composer. Mirrored spelling is supplied from the current identity snapshot.
    pub fn assemble<'a>(
        &self,
        resource_id: Uuid,
        records: impl IntoIterator<Item = (&'a str, &'a LookupRecordEntry)>,
        unsupported_families: &[String],
        mirrored_name: Option<&str>,
        chain_positions: Value,
    ) -> Result<IdentityRecordInventoryRow> {
        let mut provenance = self.provenance.clone();
        let mut event_ids = BTreeSet::new();
        let mut entries = Vec::new();
        let mut selectors = Vec::new();
        let mut absent_keys = Vec::new();
        for (key, record) in records {
            entries.extend(record.entries.iter().cloned());
            selectors.extend(record.selectors.iter().cloned());
            event_ids.extend(record.normalized_event_id);
            if record.zero_address_absent {
                absent_keys.push(json!(key));
            }
        }
        // The compositor sorts record events and link events independently, then concatenates
        // them. Preserve duplicates across those two sets.
        let mut record_events: Vec<Value> = event_ids.into_iter().map(Value::from).collect();
        record_events.extend(
            provenance["record_link_event_ids"]
                .as_array()
                .into_iter()
                .flatten()
                .cloned(),
        );
        provenance["record_event_ids"] = Value::Array(record_events);
        if !absent_keys.is_empty() {
            provenance["exact_nonempty_not_found_record_keys"] = Value::Array(absent_keys);
        }
        if let Some(name) = mirrored_name {
            provenance["mirror"]["mirrored_name"] = json!(name);
        }
        let mut families: Vec<_> = unsupported_families
            .iter()
            .map(|family| {
                json!({
                    "record_family": family,
                    "unsupported_reason": "record_family_not_supported_in_phase6_projection",
                })
            })
            .collect();
        families.extend(self.declared_unsupported_families.iter().cloned());
        Ok(IdentityRecordInventoryRow {
            resource_id,
            record_version_boundary_key: self.record_version_boundary_key.clone(),
            support_status: self.support_status.clone(),
            unsupported_reason: self.unsupported_reason.clone(),
            selectors: Value::Array(selectors),
            entries: Value::Array(entries),
            provenance,
            unsupported_families: Value::Array(families),
            chain_positions,
            last_recomputed_at: OffsetDateTime::UNIX_EPOCH,
        })
    }
}

pub(crate) fn unsupported_families(records: &BTreeMap<String, LookupRecordEntry>) -> Vec<String> {
    records
        .values()
        .filter_map(|record| record.unsupported_family.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}
