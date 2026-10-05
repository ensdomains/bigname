//! F16, ENSv2 registry entries. `project_ens_v2_entry_owner` keeps, per registry and entry, what
//! the registry's own logs last said about the entry's token: its owner, token id, resource and
//! expiry. `project_ens_v2_registry_parent` keeps the parent registry and label a registry last
//! announced. Both follow the registry contract, not the name: a name whose path was released
//! (an expired or re-pointed ancestor) keeps its entry row, and an entry whose own expiry
//! passed keeps its owner, because the contract burns nothing at expiry. Readers apply the
//! entry's expiry at the block they serve.
//!
//! An entry is the registry's storage slot for a label: the labelhash with its low 32 bits, the
//! version, cleared. The token id and the resource are that id with the entry's token version
//! and role version in those bits.
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/utils/LibLabel.sol:L7-L16 @ ens_v2_sepolia_20261001@07e55a05)
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L638-L640 @ ens_v2_sepolia_20261001@07e55a05)
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L677-L693 @ ens_v2_sepolia_20261001@07e55a05)
//!
//! The adapter also writes registration, release and expiry events that restate a registration
//! when a name's path changes or expires (adapters protocol/v2_registry/topology.rs). Those
//! carry no `sender`, and the grant restated at the TokenResource log has `resource_pending`
//! false; `fact` reads only the events a registry log produced itself.
use alloy_primitives::{hex, keccak256};
use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};

use super::{
    input::BlockEvent,
    reduce::{
        Context, Preload, current, json_number_between, key_of, load_rows, put, raw_lower,
        raw_text, set, text_or_null,
    },
    store::{Row, RowSet},
    tables,
};
use crate::Result;

const FAMILIES: [&str; 2] = ["ens_v2_registry_l1", "ens_v2_root_l1"];

/// What one registry log says about an entry's token.
enum Fact {
    /// `_register` with an owner mints the token to it. A TokenResource log always follows.
    /// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L500-L506 @ ens_v2_sepolia_20261001@07e55a05)
    Registered {
        owner: Option<String>,
    },
    /// `_register` without an owner reserves the label and mints nothing.
    /// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L498-L499 @ ens_v2_sepolia_20261001@07e55a05)
    Reserved,
    /// `unregister` burns the token and sets the entry's expiry to the block time.
    /// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L227-L238 @ ens_v2_sepolia_20261001@07e55a05)
    Unregistered,
    /// `renew` moves the expiry and changes neither the token nor its owner. On an entry
    /// `unregister` left without a token it revives the entry with no owner.
    /// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L243-L258 @ ens_v2_sepolia_20261001@07e55a05)
    Renewed,
    Resource,
    /// A transfer between two accounts; the adapter writes none for a mint or a burn.
    /// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L517-L546 @ ens_v2_sepolia_20261001@07e55a05)
    Transferred {
        to: Option<String>,
    },
    /// A role change burns the token and mints the next token version to the same owner.
    /// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L578-L588 @ ens_v2_sepolia_20261001@07e55a05)
    Regenerated,
}

/// The fact a registry log's own event carries, with the token id it names.
fn fact(event: &BlockEvent) -> Option<(Fact, String)> {
    if !FAMILIES.contains(&event.source_family.as_str()) {
        return None;
    }
    let after = &event.after;
    let source = raw_text(after, "source_event")?;
    let from_caller = after.get("sender").is_some_and(Value::is_string);
    let fact = match (event.event_kind.as_str(), source.as_str()) {
        ("RegistrationGranted", "LabelRegistered")
            if from_caller && after.get("resource_pending") == Some(&Value::Bool(true)) =>
        {
            Fact::Registered {
                owner: raw_lower(after, "registrant"),
            }
        }
        ("RegistrationReserved", "LabelReserved") if from_caller => Fact::Reserved,
        ("RegistrationReleased", "LabelUnregistered")
            if from_caller && after.get("terminal_reason").is_none() =>
        {
            Fact::Unregistered
        }
        ("ExpiryChanged", "ExpiryUpdated") if from_caller => Fact::Renewed,
        ("TokenResourceLinked", "TokenResource") => Fact::Resource,
        ("TokenControlTransferred", "TransferSingle" | "TransferBatch") => Fact::Transferred {
            to: raw_lower(after, "to"),
        },
        ("TokenRegenerated", "TokenRegenerated") => {
            return Some((Fact::Regenerated, token_word(after, "new_token_id")?));
        }
        _ => return None,
    };
    Some((fact, token_word(after, "token_id")?))
}

/// A 32-byte token id as the adapter writes it: `0x` and 64 hex digits, lower-cased.
fn token_word(after: &Value, field: &str) -> Option<String> {
    let word = raw_lower(after, field)?;
    (word.len() == 66
        && word.starts_with("0x")
        && word[2..].bytes().all(|byte| byte.is_ascii_hexdigit()))
    .then_some(word)
}

/// The entry a token id, resource or labelhash belongs to: the id with its version cleared.
fn entry_key(word: &str) -> String {
    format!("{}00000000", &word[..58])
}

fn entry_row_key(chain: &Value, registry: &str, token: &str) -> Row {
    key_of(
        &tables::ENS_V2_ENTRY_OWNER,
        [chain.clone(), json!(registry), json!(entry_key(token))],
    )
}

fn entry_facts(events: &[BlockEvent]) -> Vec<(&BlockEvent, String, Fact, String)> {
    events
        .iter()
        .filter_map(|event| {
            let (fact, token) = fact(event)?;
            Some((event, event.emitting_address()?, fact, token))
        })
        .collect()
}

fn parent_events(events: &[BlockEvent]) -> Vec<(&BlockEvent, String)> {
    events
        .iter()
        .filter(|event| {
            FAMILIES.contains(&event.source_family.as_str())
                && event.event_kind == "ParentChanged"
                && raw_text(&event.after, "source_event").as_deref() == Some("ParentUpdated")
        })
        .filter_map(|event| Some((event, event.emitting_address()?)))
        .collect()
}

fn parent_row_key(chain: &Value, registry: &str) -> Row {
    key_of(
        &tables::ENS_V2_REGISTRY_PARENT,
        [chain.clone(), json!(registry)],
    )
}

/// The entry and registry keys one block's registry events name.
pub(super) fn preload(chain: &Value, events: &[BlockEvent], into: &mut Preload) {
    into.add(
        &tables::ENS_V2_ENTRY_OWNER,
        entry_facts(events)
            .iter()
            .map(|(_, registry, _, token)| entry_row_key(chain, registry, token)),
    );
    into.add(
        &tables::ENS_V2_REGISTRY_PARENT,
        parent_events(events)
            .iter()
            .map(|(_, registry)| parent_row_key(chain, registry)),
    );
}

fn expiry(after: &Value) -> Value {
    json_number_between(after.get("expiry"), u64::MAX)
        .map_or(Value::Null, |number| Value::Number(number.clone()))
}

pub(super) async fn apply(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    events: &[BlockEvent],
    rows: &mut RowSet,
) -> Result<()> {
    let chain = json!(context.chain_id);
    let table = &tables::ENS_V2_ENTRY_OWNER;
    let facts = entry_facts(events);
    let keys = facts
        .iter()
        .map(|(_, registry, _, token)| entry_row_key(&chain, registry, token))
        .collect();
    load_rows(transaction, rows, table, keys).await?;
    for (event, registry, fact, token) in facts {
        let mut row = current(rows, table, &entry_row_key(&chain, &registry, &token));
        let after = &event.after;
        set(&mut row, "token_id", token);
        if let Some(instance) = raw_text(after, "registry_contract_instance_id") {
            set(&mut row, "registry_contract_instance_id", instance);
        }
        let mut owner_change = None;
        match fact {
            Fact::Registered { owner } => {
                owner_change = Some(("registered", owner));
                set(&mut row, "expiry", expiry(after));
            }
            Fact::Reserved => {
                owner_change = Some(("reserved", None));
                set(&mut row, "expiry", expiry(after));
            }
            Fact::Unregistered => {
                set(&mut row, "status", "unregistered");
                set(&mut row, "owner", Value::Null);
                set(&mut row, "owner_position", event.position.to_json());
                set(&mut row, "expiry", json!(context.block.timestamp_seconds));
            }
            Fact::Renewed => {
                set(&mut row, "expiry", expiry(after));
                // A root renewer revived an entry `unregister` left without a token: it is
                // held with no owner, as a reservation is, under the token id the log names.
                // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L243-L258 @ ens_v2_sepolia_20261001@07e55a05)
                if row.get("status").and_then(Value::as_str) == Some("unregistered") {
                    set(&mut row, "status", "reserved");
                    set(&mut row, "upstream_resource", Value::Null);
                    set(&mut row, "resource_id", Value::Null);
                    set(&mut row, "resource_position", Value::Null);
                }
            }
            Fact::Resource => {
                set(
                    &mut row,
                    "upstream_resource",
                    text_or_null(raw_lower(after, "upstream_resource")),
                );
                set(
                    &mut row,
                    "resource_id",
                    text_or_null(event.resource_id.clone()),
                );
                set(&mut row, "resource_position", event.position.to_json());
            }
            Fact::Transferred { to } => {
                set(&mut row, "status", "registered");
                set(&mut row, "owner", text_or_null(to));
                set(&mut row, "owner_position", event.position.to_json());
            }
            // The resource is unchanged; an entry first seen here takes it from the log.
            Fact::Regenerated => {
                if let (Some(resource), Some(resource_id)) =
                    (raw_lower(after, "resource"), event.resource_id.clone())
                {
                    set(&mut row, "upstream_resource", resource);
                    set(&mut row, "resource_id", resource_id);
                }
            }
        }
        // A registration or reservation starts a token whose resource the registry has not
        // announced yet; the entry's earlier resource belongs to the token it replaced.
        if let Some((status, owner)) = owner_change {
            set(&mut row, "status", status);
            set(&mut row, "owner", text_or_null(owner));
            set(&mut row, "owner_position", event.position.to_json());
            set(&mut row, "upstream_resource", Value::Null);
            set(&mut row, "resource_id", Value::Null);
            set(&mut row, "resource_position", Value::Null);
        } else if !row.get("status").is_some_and(Value::is_string) {
            // The first log seen for the entry says nothing about its owner.
            set(&mut row, "status", "unknown");
        }
        put(rows, table, row, event)?;
    }

    let table = &tables::ENS_V2_REGISTRY_PARENT;
    let parents = parent_events(events);
    let keys = parents
        .iter()
        .map(|(_, registry)| parent_row_key(&chain, registry))
        .collect();
    load_rows(transaction, rows, table, keys).await?;
    for (event, registry) in parents {
        let mut row = current(rows, table, &parent_row_key(&chain, &registry));
        let label = raw_lower(&event.after, "raw_label_hex");
        // `findOwner(label)` reads the parent's entry of the label's hash.
        // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L311-L314 @ ens_v2_sepolia_20261001@07e55a05)
        let parent_entry_key = label
            .as_deref()
            .and_then(|label| hex::decode(label).ok())
            .map(|label| entry_key(&format!("{}", keccak256(label))));
        set(
            &mut row,
            "parent",
            text_or_null(raw_lower(&event.after, "parent")),
        );
        set(&mut row, "raw_label_hex", text_or_null(label));
        set(&mut row, "parent_entry_key", text_or_null(parent_entry_key));
        put(rows, table, row, event)?;
    }
    Ok(())
}
