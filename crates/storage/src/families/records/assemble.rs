//! The row the record inventory publishes for a resource, built from the records the family read
//! selected (record_inventory.rs, `record_rollups` and the final insert; record_inventory/mirror.rs
//! for the mirrored rows).
use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use serde_json::{Value, json};
use sqlx::{PgPool, types::time::OffsetDateTime};

use super::{
    FamilyPosition, LinkSelection,
    facts::{BlockStamp, ResolverClassification, block_stamps, chain_position, collation_order},
    mirror::MirrorSelection,
    payload,
    rows::RecordCandidate,
    serving::ServingPointer,
};
use crate::{RecordInventoryCurrentRow, record_version_boundary_storage_key};

/// One record the row serves for its key.
#[derive(Clone, Debug)]
pub(crate) struct ServedRecord {
    pub(crate) record_key: String,
    /// The served event's own position.
    pub(crate) position: FamilyPosition,
    pub(crate) normalized_event_id: Option<i64>,
    pub(crate) source_family: String,
    /// The status the family row keeps; `None` for a payload read back from the event.
    pub(crate) stored_status: Option<String>,
    pub(crate) payload: Value,
}

/// The combined version boundary.
#[derive(Clone, Debug)]
pub(crate) struct BoundaryEvent {
    pub(crate) position: FamilyPosition,
    pub(crate) kind: &'static str,
    pub(crate) normalized_event_id: Option<i64>,
}

pub(crate) struct Assembly<'a> {
    pub(crate) chain_id: &'a str,
    pub(crate) pointer: &'a ServingPointer,
    pub(crate) classification: Option<&'a ResolverClassification>,
    pub(crate) eligibility: (bool, Option<String>),
    pub(crate) boundary: Option<BoundaryEvent>,
    pub(crate) served: Vec<ServedRecord>,
    pub(crate) links: Option<&'a LinkSelection>,
    /// Every retained write of the selected record id, eligible or not.
    pub(crate) linked: &'a [RecordCandidate],
    pub(crate) attributed: BTreeSet<i64>,
}

fn change(
    event_id: Option<i64>,
    kind: &str,
    stamps: &BTreeMap<i64, BlockStamp>,
    chain_id: &str,
    block: i64,
) -> Value {
    json!({
        "normalized_event_id": event_id,
        "event_kind": kind,
        "chain_position": chain_position(stamps, chain_id, block),
    })
}

fn coverage(supported: bool, reason: Option<&str>) -> Value {
    if supported {
        json!({"status": "projected", "exhaustiveness": "not_asserted"})
    } else {
        json!({"status": "unsupported", "exhaustiveness": "not_asserted",
               "unsupported_reason": reason})
    }
}

fn row(
    pointer: &ServingPointer,
    boundary: Value,
    fields: [Value; 6],
    supported: bool,
    reason: Option<&str>,
) -> Result<(RecordInventoryCurrentRow, String)> {
    let [
        selectors,
        unsupported_families,
        last_change,
        entries,
        provenance,
        chain_positions,
    ] = fields;
    let key = record_version_boundary_storage_key(&boundary, pointer.resource_id)?;
    Ok((
        RecordInventoryCurrentRow {
            resource_id: pointer.resource_id,
            record_version_boundary: boundary,
            enumeration_basis: json!({"observed_selectors": true,
                "capability_declared_families": true, "globally_enumerable": false}),
            selectors,
            explicit_gaps: json!([]),
            unsupported_families,
            last_change: Some(last_change),
            entries,
            provenance,
            coverage: coverage(supported, reason),
            chain_positions,
            canonicality_summary: json!({"state": "canonical_lineage"}),
            // Not reproduced: the family rows keep no manifest version, and the comparison
            // excludes it.
            manifest_version: 1,
            last_recomputed_at: OffsetDateTime::UNIX_EPOCH,
        },
        key,
    ))
}

pub(crate) async fn assemble(
    pool: &PgPool,
    input: Assembly<'_>,
) -> Result<(RecordInventoryCurrentRow, String)> {
    let Assembly {
        chain_id,
        pointer,
        classification,
        eligibility: (supported, reason),
        boundary,
        served,
        links,
        linked,
        attributed,
    } = input;

    // The link arm's contributing events: every write of the selected record id and the links.
    let contributing: Vec<_> = links
        .map(|links| links.contributing_links().collect())
        .unwrap_or_default();
    let link_ids: BTreeSet<i64> = contributing
        .iter()
        .filter_map(|link| link.normalized_event_id)
        .collect();
    let latest_link = links.and_then(|_| {
        let writes = linked
            .iter()
            .map(|write| (&write.position, write.normalized_event_id, "RecordChanged"));
        let link_events = contributing.iter().map(|link| {
            (
                &link.position,
                link.normalized_event_id,
                "ResolverRecordLinked",
            )
        });
        writes.chain(link_events).max_by(|a, b| a.0.cmp(b.0))
    });
    let latest_record = served.iter().max_by(|a, b| a.position.cmp(&b.position));
    let boundary_block = boundary.as_ref().map_or(pointer.block_number, |boundary| {
        boundary.position.block_number
    });
    let latest_block = [
        Some(pointer.block_number),
        boundary
            .as_ref()
            .map(|boundary| boundary.position.block_number),
        served
            .iter()
            .map(|record| record.position.block_number)
            .max(),
    ]
    .into_iter()
    .flatten()
    .max()
    .unwrap_or(pointer.block_number);
    let mut blocks = vec![pointer.block_number, boundary_block, latest_block];
    blocks.extend(latest_link.map(|(position, _, _)| position.block_number));
    blocks.extend(latest_record.map(|record| record.position.block_number));
    let stamps = block_stamps(pool, chain_id, &blocks).await?;

    // Entries, selectors and families per record key, in the database collation.
    let order = collation_order(
        pool,
        served
            .iter()
            .map(|record| record.record_key.clone())
            .collect(),
    )
    .await?;
    let by_key: BTreeMap<&str, &ServedRecord> = served
        .iter()
        .map(|record| (record.record_key.as_str(), record))
        .collect();
    let (mut entries, mut selectors, mut zero_keys) = (Vec::new(), Vec::new(), Vec::new());
    for key in &order {
        let Some(record) = by_key.get(key.as_str()) else {
            continue;
        };
        let zero_absent = payload::coin60_zero_address_is_absent(
            &record.payload,
            &record.source_family,
            &pointer.source_family,
        );
        if zero_absent {
            zero_keys.push(json!(key));
        }
        entries.extend(payload::entry(
            &record.payload,
            record.stored_status.as_deref(),
            zero_absent,
        ));
        selectors.extend(payload::selector(&record.payload));
    }
    let families: BTreeSet<String> = served
        .iter()
        .filter_map(|record| payload::unsupported_family(&record.payload))
        .collect();
    let mut unsupported_families: Vec<Value> =
        collation_order(pool, families.into_iter().collect())
            .await?
            .into_iter()
            .map(|family| {
                json!({"record_family": family,
                   "unsupported_reason": "record_family_not_supported_in_phase6_projection"})
            })
            .collect();
    if !supported {
        unsupported_families.push(json!({"record_family": "resolver_classification",
                                         "unsupported_reason": reason}));
    }

    let record_ids: BTreeSet<i64> = served
        .iter()
        .filter_map(|record| record.normalized_event_id)
        .collect();
    let boundary_json = json!({
        "logical_name_id": pointer.logical_name_id,
        "resource_id": pointer.resource_id.to_string(),
        "normalized_event_id": boundary.as_ref().and_then(|b| b.normalized_event_id),
        "event_kind": boundary.as_ref()
            .and_then(|b| b.normalized_event_id.map(|_| b.kind)),
        "chain_position": chain_position(&stamps, chain_id, boundary_block),
    });
    let last_change = match (latest_link, latest_record) {
        (Some((position, id, kind)), _) => {
            change(id, kind, &stamps, chain_id, position.block_number)
        }
        (None, Some(record)) => change(
            record.normalized_event_id,
            "RecordChanged",
            &stamps,
            chain_id,
            record.position.block_number,
        ),
        (None, None) => match &boundary {
            Some(boundary) if boundary.normalized_event_id.is_some() => change(
                boundary.normalized_event_id,
                boundary.kind,
                &stamps,
                chain_id,
                boundary_block,
            ),
            _ => change(
                pointer.pointer_event_id,
                "ResolverChanged",
                &stamps,
                chain_id,
                boundary_block,
            ),
        },
    };
    let read_rules = if classification
        .is_some_and(|classification| classification.has_read_feature("ensip19_default_address"))
    {
        json!([{"kind": "ensip19_default_address", "source_record_key": "addr:2147483648"}])
    } else {
        json!([])
    };
    let mut provenance = json!({
        "chain_id": chain_id,
        "logical_name_id": pointer.logical_name_id,
        "resolver_address": pointer.resolver_address,
        "resolver_pointer_event_id": pointer.pointer_event_id,
        "record_event_ids": record_ids.iter().chain(&link_ids).collect::<Vec<_>>(),
        "record_link_event_ids": link_ids,
        "attributed_event_ids": attributed,
        "read_rules": read_rules,
        "coverage": {"status": "projected", "exhaustiveness": "not_asserted"},
    });
    if !zero_keys.is_empty() {
        provenance["exact_nonempty_not_found_record_keys"] = Value::Array(zero_keys);
    }
    let chain_positions = payload::strip_nulls(json!({
        "block_number": latest_block,
        "block_hash": stamps.get(&latest_block).map(|stamp| stamp.block_hash.clone()),
    }));
    row(
        pointer,
        boundary_json,
        [
            Value::Array(selectors),
            Value::Array(unsupported_families),
            last_change,
            Value::Array(entries),
            provenance,
            chain_positions,
        ],
        supported,
        reason.as_deref(),
    )
}

/// A row derived through a mirror takes the mirror back as its resolver and gains
/// `provenance.mirror`; an unsupported one names the mirror as the reason.
pub(crate) fn finish_mirrored(
    row: &mut RecordInventoryCurrentRow,
    mirror: &MirrorSelection,
    serving: &ServingPointer,
) {
    row.provenance["resolver_address"] = json!(serving.resolver_address);
    row.provenance["mirror"] = mirror.provenance(serving);
    if row.coverage.get("status").and_then(Value::as_str) == Some("unsupported") {
        row.coverage["unsupported_reason"] = json!("mirrored_resolver_not_projected");
        if let Value::Array(families) = &mut row.unsupported_families {
            for family in families.iter_mut().filter(|family| {
                family.get("record_family").and_then(Value::as_str)
                    == Some("resolver_classification")
            }) {
                family["unsupported_reason"] = json!("mirrored_resolver_not_projected");
            }
        }
    }
}

/// The row of a mirror pointer whose records cannot be derived: no records, the mirror's reason.
pub(crate) fn unsupported_mirror_row(
    chain_id: &str,
    serving: &ServingPointer,
    mirror: &MirrorSelection,
    stamps: &BTreeMap<i64, BlockStamp>,
) -> Result<(RecordInventoryCurrentRow, String)> {
    let reason = mirror.unsupported_reason();
    let position = chain_position(stamps, chain_id, serving.block_number);
    let boundary = json!({
        "logical_name_id": serving.logical_name_id,
        "resource_id": serving.resource_id.to_string(),
        "normalized_event_id": null,
        "event_kind": null,
        "chain_position": position,
    });
    let provenance = json!({
        "chain_id": chain_id,
        "logical_name_id": serving.logical_name_id,
        "resolver_address": serving.resolver_address,
        "resolver_pointer_event_id": serving.pointer_event_id,
        "record_event_ids": [],
        "record_link_event_ids": [],
        "attributed_event_ids": [],
        "read_rules": [],
        "coverage": {"status": "projected", "exhaustiveness": "not_asserted"},
        "mirror": mirror.provenance(serving),
    });
    let chain_positions = payload::strip_nulls(json!({
        "block_number": serving.block_number,
        "block_hash": stamps.get(&serving.block_number).map(|stamp| stamp.block_hash.clone()),
    }));
    row(
        serving,
        boundary,
        [
            json!([]),
            json!([{"record_family": "resolver_classification", "unsupported_reason": reason}]),
            json!({"normalized_event_id": serving.pointer_event_id,
                   "event_kind": "ResolverChanged", "chain_position": position}),
            json!([]),
            provenance,
            chain_positions,
        ],
        false,
        Some(&reason),
    )
}
