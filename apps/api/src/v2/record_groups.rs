//! The `records` object of name detail and `POST /v1/lookup` `profile=detail`
//! (docs/api-v1-routes.md § `GET /v1/names/{name}`): each record category's `seen_*` key list
//! beside its value map, and the `contenthash` and forward `name` singletons, listed in
//! `seen_singletons` when a write was observed.
//!
//! Encoding: a seen key missing from its value map is not known in this response; a seen key
//! mapped to `null` is set to empty (cleared). A singleton is its value, `null` when cleared (or,
//! left out of `seen_singletons`, never written on an authoritative inventory), and absent when
//! unknown; a seen singleton that is absent is a write whose value is unknown.

use std::collections::{BTreeMap, BTreeSet};

use bigname_storage::{
    AbiContentTypes, AbiContentTypesInput, AbiContentTypesUnavailable, IdentityRecordInventoryRow,
    RecordInventoryCurrentRow, record_version_boundary_storage_key,
};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sqlx::PgPool;
use sqlx::types::Uuid;
use sqlx::types::time::OffsetDateTime;
use tracing::error;

use super::name_record::{string_field, value_to_string};
use super::name_records::RecordAnswer;
use super::name_records_inventory::InventorySections;
use super::name_records_inventory::load_abi_content_types;
use super::support::{ResolutionRecordKey, direct_json_field, record_value_string_from_entry};
use super::{Status, V2Error, V2Result};

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub(crate) struct RecordGroups {
    /// Canonical decimal coin types.
    pub(crate) seen_addresses: Vec<String>,
    pub(crate) addresses: BTreeMap<String, Option<String>>,
    pub(crate) seen_texts: Vec<String>,
    pub(crate) texts: BTreeMap<String, Option<String>>,
    /// ABI content types, or absent with `abi_unsupported_reason` when the index cannot list them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) seen_abis: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) abi_unsupported_reason: Option<String>,
    /// bigname retains no ABI bytes, so every listed content type is unknown here.
    pub(crate) abis: BTreeMap<String, Option<String>>,
    /// `contenthash` and/or `name`: the singletons a write was observed for (indexed) or that
    /// were read (verified).
    pub(crate) seen_singletons: Vec<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present"
    )]
    pub(crate) contenthash: Option<Option<String>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present"
    )]
    pub(crate) name: Option<Option<String>>,
}

/// A present field, `null` included, is `Some`; `default` makes a missing one `None`.
fn present<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Option<String>>, D::Error> {
    Option::<String>::deserialize(deserializer).map(Some)
}

/// Where a record belongs in the grouped object.
enum Slot {
    Address(String),
    Text(String),
    Contenthash,
    Name,
}

impl Slot {
    /// The singleton's name in `seen_singletons`.
    fn singleton(&self) -> Option<&'static str> {
        match self {
            Self::Contenthash => Some("contenthash"),
            Self::Name => Some("name"),
            Self::Address(_) | Self::Text(_) => None,
        }
    }
}

/// The slot of a record key as the inventory stores it. This is not the `keys=` request grammar
/// (`parse_resolution_record_key`): a retained text key can hold whitespace or commas
/// (`text:display name`, `text:a,b`), and it is listed and served like any other.
fn slot(record_key: &str) -> Option<Slot> {
    match record_key.split_once(':') {
        Some(("addr", coin_type)) => {
            bigname_storage::canonical_addr_coin_type(coin_type).map(Slot::Address)
        }
        Some(("text", key)) if !key.is_empty() => Some(Slot::Text(key.to_owned())),
        None if record_key == "avatar" => Some(Slot::Text("avatar".to_owned())),
        None if record_key == "contenthash" => Some(Slot::Contenthash),
        None if record_key == "name" => Some(Slot::Name),
        _ => None,
    }
}

fn item_slot(item: &Value) -> Option<Slot> {
    string_field(item.get("record_key")).and_then(|record_key| slot(&record_key))
}

fn lists_unsupported_family(unsupported_families: &Value, family: &str) -> bool {
    unsupported_families
        .as_array()
        .into_iter()
        .flatten()
        .any(|listed| string_field(listed.get("record_family")).as_deref() == Some(family))
}

/// An empty string is the resolver's unset value.
fn nonempty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

impl RecordGroups {
    fn list(&mut self, slot: &Slot) {
        match slot {
            Slot::Address(coin_type) => self.seen_addresses.push(coin_type.clone()),
            Slot::Text(key) => self.seen_texts.push(key.clone()),
            Slot::Contenthash | Slot::Name => {
                self.seen_singletons
                    .extend(slot.singleton().map(str::to_owned));
            }
        }
    }

    /// `Some(value)` is a served value (`None` inside for a clear).
    fn set(&mut self, slot: Slot, value: Option<String>) {
        match slot {
            Slot::Address(coin_type) => {
                self.addresses.insert(coin_type, value);
            }
            Slot::Text(key) => {
                self.texts.insert(key, value);
            }
            Slot::Contenthash => self.contenthash = Some(value),
            Slot::Name => self.name = Some(value),
        }
    }

    /// A seen record whose value is not known here.
    fn forget(&mut self, slot: Slot) {
        match slot {
            Slot::Address(coin_type) => {
                self.addresses.remove(&coin_type);
            }
            Slot::Text(key) => {
                self.texts.remove(&key);
            }
            Slot::Contenthash => self.contenthash = None,
            Slot::Name => self.name = None,
        }
    }

    fn finish(mut self) -> Self {
        for keys in [
            &mut self.seen_addresses,
            &mut self.seen_texts,
            &mut self.seen_singletons,
        ] {
            let sorted: BTreeSet<String> = keys.drain(..).collect();
            keys.extend(sorted);
        }
        // Canonical decimal coin types, numerically ascending.
        self.seen_addresses
            .sort_by(|left, right| (left.len(), left).cmp(&(right.len(), right)));
        self
    }

    /// The indexed groups of one record inventory row. Keys come from the row's selectors and
    /// entries whatever its coverage, so an unsupported row (an unknown resolver implementation)
    /// still lists what bigname saw written; values come only from a row whose coverage is
    /// authoritative.
    pub(crate) fn indexed(sections: InventorySections<'_>) -> Self {
        let mut groups = Self::default();
        for item in [sections.selectors, sections.entries, sections.explicit_gaps]
            .into_iter()
            .flat_map(|section| section.as_array().into_iter().flatten())
        {
            if let Some(slot) = item_slot(item) {
                groups.list(&slot);
            }
        }
        if sections.authoritative {
            // An authoritative row is complete for the families its resolver holds: a singleton
            // it has no entry for is unset, unless the row lists that family as unsupported or
            // the resolver has no getter for it (a legacy public resolver without contenthash).
            let complete = |family: &str| {
                (!lists_unsupported_family(sections.unsupported_families, family)
                    && bigname_storage::families::records::inventory_resolver_holds_family(
                        sections.provenance,
                        family,
                    ))
                .then_some(None)
            };
            groups.contenthash = complete("contenthash");
            groups.name = complete("name");
            for entry in sections.entries.as_array().into_iter().flatten() {
                let Some(slot) = item_slot(entry) else {
                    continue;
                };
                let value = match string_field(entry.get("status")).as_deref() {
                    // The forward name is served only as a string; retained non-UTF-8 bytes
                    // stay unknown.
                    Some("success") if matches!(slot, Slot::Name) => entry
                        .get("value")
                        .and_then(Value::as_str)
                        .map(|name| nonempty(name.to_owned())),
                    Some("success") => {
                        record_value_string_from_entry(entry, direct_json_field).map(nonempty)
                    }
                    Some("not_found") => Some(None),
                    _ => None,
                };
                match value {
                    Some(value) => groups.set(slot, value),
                    None => groups.forget(slot),
                }
            }
        }
        groups.finish()
    }

    /// The verified groups of one verified lookup: the keys it read, and the value of every key
    /// that answered `ok` (`null` for an empty value) or `not_found` (`null`). A key that answered
    /// `unsupported`, `stale` or `failed` stays listed with no value. The forward name is not read.
    pub(crate) fn verified(
        requested: &[ResolutionRecordKey],
        answers: &BTreeMap<String, RecordAnswer>,
    ) -> Self {
        let mut groups = Self::default();
        for record in requested {
            let Some(slot) = slot(&record.record_key) else {
                continue;
            };
            groups.list(&slot);
            let Some(answer) = answers.get(&record.record_key) else {
                continue;
            };
            match answer.status {
                Status::Ok => {
                    if let Some(value) = answer.value.as_ref().and_then(value_to_string) {
                        groups.set(slot, nonempty(value));
                    }
                }
                Status::NotFound => groups.set(slot, None),
                _ => {}
            }
        }
        groups.finish()
    }

    pub(crate) fn set_abi_content_types(&mut self, answer: AbiContentTypes) {
        (self.seen_abis, self.abi_unsupported_reason) = match answer {
            AbiContentTypes::Observed(content_types) => (Some(content_types), None),
            AbiContentTypes::Unavailable(reason) => (None, Some(reason.as_str().to_owned())),
        };
    }
}

/// The owned identity of the inventory row a `records` object came from, for the request's one
/// batched ABI read.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AbiSource {
    pub(crate) resource_id: Uuid,
    pub(crate) record_version_boundary_key: String,
    pub(crate) provenance: Value,
    pub(crate) chain_positions: Value,
    pub(crate) last_recomputed_at: OffsetDateTime,
}

impl AbiSource {
    pub(crate) fn of_row(row: &RecordInventoryCurrentRow) -> V2Result<Self> {
        let record_version_boundary_key =
            record_version_boundary_storage_key(&row.record_version_boundary, row.resource_id)
                .map_err(|key_error| {
                    error!(service = "api", error = ?key_error, "record inventory boundary key");
                    V2Error::internal_error("failed to load record inventory ABI content types")
                })?;
        Ok(Self {
            resource_id: row.resource_id,
            record_version_boundary_key,
            provenance: row.provenance.clone(),
            chain_positions: row.chain_positions.clone(),
            last_recomputed_at: row.last_recomputed_at,
        })
    }

    pub(crate) fn of_identity_row(row: &IdentityRecordInventoryRow) -> Self {
        Self {
            resource_id: row.resource_id,
            record_version_boundary_key: row.record_version_boundary_key.clone(),
            provenance: row.provenance.clone(),
            chain_positions: row.chain_positions.clone(),
            last_recomputed_at: row.last_recomputed_at,
        }
    }

    /// Content types are listed whatever the row's coverage: like the other key lists they are
    /// what bigname saw written, not a claim about current values.
    pub(crate) fn input(&self) -> AbiContentTypesInput<'_> {
        AbiContentTypesInput {
            authoritative: true,
            resource_id: self.resource_id,
            record_version_boundary_key: &self.record_version_boundary_key,
            provenance: &self.provenance,
            chain_positions: &self.chain_positions,
            last_recomputed_at: self.last_recomputed_at,
        }
    }
}

/// Fills the ABI content types of every `records` object with a source in one batched read; an
/// object without one (a verified read of a name with no inventory row) reports
/// `inventory_not_available`.
pub(crate) async fn fill_abi_content_types(
    pool: &PgPool,
    targets: Vec<(&mut RecordGroups, Option<&AbiSource>)>,
) -> V2Result<()> {
    let inputs = targets
        .iter()
        .filter_map(|(_, source)| source.map(AbiSource::input))
        .collect::<Vec<_>>();
    let mut answers = load_abi_content_types(pool, &inputs).await?.into_iter();
    for (groups, source) in targets {
        let answer = match source {
            Some(_) => answers.next().expect("one ABI answer per inventory source"),
            None => AbiContentTypes::Unavailable(AbiContentTypesUnavailable::InventoryNotAvailable),
        };
        groups.set_abi_content_types(answer);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::v2::support::parse_resolution_record_key;

    /// A current public resolver's classification, which holds every singleton family.
    static MODERN: std::sync::LazyLock<Value> = std::sync::LazyLock::new(|| {
        json!({"abi_observation_classification":
            {"source_family": "ens_v1_resolver_l1", "role": "public_resolver"}})
    });

    fn sections<'a>(
        authoritative: bool,
        selectors: &'a Value,
        entries: &'a Value,
        empty: &'a Value,
    ) -> InventorySections<'a> {
        InventorySections {
            authoritative,
            selectors,
            entries,
            explicit_gaps: empty,
            unsupported_families: empty,
            provenance: &MODERN,
        }
    }

    #[test]
    fn indexed_groups_list_keys_and_serve_values_only_from_authoritative_rows() {
        let selectors = json!([
            {"record_key": "addr:60", "record_family": "addr", "selector_key": "60"},
            {"record_key": "text:url", "record_family": "text", "selector_key": "url"},
            {"record_key": "text:email", "record_family": "text", "selector_key": "email"},
            {"record_key": "contenthash", "record_family": "contenthash"}
        ]);
        let entries = json!([
            {"record_key": "addr:60", "record_family": "addr", "selector_key": "60",
             "status": "success", "value": "0xabc"},
            {"record_key": "text:url", "record_family": "text", "selector_key": "url",
             "status": "not_found"},
            {"record_key": "text:email", "record_family": "text", "selector_key": "email",
             "status": "unsupported"},
            {"record_key": "contenthash", "record_family": "contenthash",
             "status": "success", "value": {"encoding": "hex", "bytes": "0xe301"}},
            {"record_key": "name", "record_family": "name", "status": "success",
             "value": "alice.eth"}
        ]);
        let empty = json!([]);
        let groups = RecordGroups::indexed(sections(true, &selectors, &entries, &empty));
        assert_eq!(
            serde_json::to_value(&groups).expect("serializes"),
            json!({
                "seen_addresses": ["60"], "addresses": {"60": "0xabc"},
                "seen_texts": ["email", "url"], "texts": {"url": null}, "abis": {},
                "seen_singletons": ["contenthash", "name"],
                "contenthash": "0xe301", "name": "alice.eth"
            })
        );
        let unknown = RecordGroups::indexed(sections(false, &selectors, &entries, &empty));
        assert_eq!(
            serde_json::to_value(&unknown).expect("serializes"),
            json!({
                "seen_addresses": ["60"], "addresses": {},
                "seen_texts": ["email", "url"], "texts": {}, "abis": {},
                "seen_singletons": ["contenthash", "name"]
            })
        );
        let round: RecordGroups =
            serde_json::from_value(serde_json::to_value(&groups).expect("serializes"))
                .expect("deserializes");
        assert_eq!(round, groups);
    }

    #[test]
    fn a_cleared_singleton_serializes_as_null() {
        let entries = json!([
            {"record_key": "contenthash", "record_family": "contenthash", "status": "not_found"},
            {"record_key": "name", "record_family": "name", "status": "not_found"}
        ]);
        let empty = json!([]);
        let groups = RecordGroups::indexed(sections(true, &empty, &entries, &empty));
        let value = serde_json::to_value(&groups).expect("serializes");
        assert_eq!(value["contenthash"], Value::Null);
        assert_eq!(value["name"], Value::Null);
        assert!(value.get("contenthash").is_some() && value.get("name").is_some());
        assert_eq!(
            value["seen_singletons"],
            json!(["contenthash", "name"]),
            "{value}"
        );
    }

    #[test]
    fn an_authoritative_row_without_a_singleton_entry_serves_it_unset() {
        let empty = json!([]);
        let groups = RecordGroups::indexed(sections(true, &empty, &empty, &empty));
        let value = serde_json::to_value(&groups).expect("serializes");
        assert_eq!(value["contenthash"], Value::Null, "{value}");
        assert_eq!(value["name"], Value::Null, "{value}");
        assert!(value.get("contenthash").is_some() && value.get("name").is_some());
        assert_eq!(value["seen_singletons"], json!([]), "{value}");

        // A family the row lists as unsupported, or an entry whose value is not known, is unknown.
        let unsupported = json!([{"record_family": "name", "unsupported_reason": "x"}]);
        let entries = json!([
            {"record_key": "contenthash", "record_family": "contenthash", "status": "unsupported"}
        ]);
        let groups = RecordGroups::indexed(InventorySections {
            authoritative: true,
            selectors: &empty,
            entries: &entries,
            explicit_gaps: &empty,
            unsupported_families: &unsupported,
            provenance: &MODERN,
        });
        let value = serde_json::to_value(&groups).expect("serializes");
        // Written, value unknown; and a family the row cannot speak for.
        assert_eq!(value["seen_singletons"], json!(["contenthash"]), "{value}");
        assert!(value.get("contenthash").is_none(), "{value}");
        assert!(value.get("name").is_none(), "{value}");
    }

    #[test]
    fn a_resolver_without_the_contenthash_getter_leaves_it_unknown() {
        let empty = json!([]);
        for role in ["public_resolver_5ffc0143", "public_resolver_1da02271"] {
            let legacy = json!({"abi_observation_classification":
                {"source_family": "ens_v1_resolver_l1", "role": role}});
            let groups = RecordGroups::indexed(InventorySections {
                provenance: &legacy,
                ..sections(true, &empty, &empty, &empty)
            });
            let value = serde_json::to_value(&groups).expect("serializes");
            assert!(value.get("contenthash").is_none(), "{role}: {value}");
            assert_eq!(value["name"], Value::Null, "{role}: {value}");
        }
        // No captured classification: nothing is claimed unset.
        let bare = json!({});
        let groups = RecordGroups::indexed(InventorySections {
            provenance: &bare,
            ..sections(true, &empty, &empty, &empty)
        });
        let value = serde_json::to_value(&groups).expect("serializes");
        assert!(
            value.get("contenthash").is_none() && value.get("name").is_none(),
            "{value}"
        );
    }

    #[test]
    fn retained_text_keys_outside_the_request_grammar_are_listed_and_served() {
        let selectors = json!([
            {"record_key": "text:display name", "record_family": "text",
             "selector_key": "display name"},
            {"record_key": "text:a,b", "record_family": "text", "selector_key": "a,b"}
        ]);
        let entries = json!([
            {"record_key": "text:display name", "record_family": "text",
             "selector_key": "display name", "status": "success", "value": "Alice"},
            {"record_key": "text:a,b", "record_family": "text", "selector_key": "a,b",
             "status": "not_found"}
        ]);
        let empty = json!([]);
        let groups = RecordGroups::indexed(sections(true, &selectors, &entries, &empty));
        let value = serde_json::to_value(&groups).expect("serializes");
        assert_eq!(
            value["seen_texts"],
            json!(["a,b", "display name"]),
            "{value}"
        );
        assert_eq!(
            value["texts"],
            json!({"a,b": null, "display name": "Alice"}),
            "{value}"
        );
        let unknown = RecordGroups::indexed(sections(false, &selectors, &entries, &empty));
        let value = serde_json::to_value(&unknown).expect("serializes");
        assert_eq!(
            value["seen_texts"],
            json!(["a,b", "display name"]),
            "{value}"
        );
        assert_eq!(value["texts"], json!({}), "{value}");
    }

    #[test]
    fn verified_groups_list_every_read_key() {
        let requested = ["addr:60", "avatar", "contenthash", "text:url", "text:email"]
            .map(|key| parse_resolution_record_key(key).expect("key"));
        let answer = |status, value: Option<&str>| RecordAnswer {
            status,
            value: value.map(|value| json!(value)),
            unsupported_reason: None,
            failure_reason: None,
            meta: None,
        };
        let answers = BTreeMap::from([
            ("addr:60".to_owned(), answer(Status::Ok, Some("0xabc"))),
            ("avatar".to_owned(), answer(Status::NotFound, None)),
            ("contenthash".to_owned(), answer(Status::Failed, None)),
            ("text:url".to_owned(), answer(Status::Ok, Some(""))),
        ]);
        let groups = RecordGroups::verified(&requested, &answers);
        assert_eq!(
            serde_json::to_value(&groups).expect("serializes"),
            json!({
                "seen_addresses": ["60"], "addresses": {"60": "0xabc"},
                "seen_texts": ["avatar", "email", "url"],
                "texts": {"avatar": null, "url": null}, "abis": {},
                "seen_singletons": ["contenthash"]
            })
        );
    }
}
