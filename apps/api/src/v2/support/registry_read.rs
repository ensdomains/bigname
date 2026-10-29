//! An ENSv1 name serves the resolver its ENSv1 registry record holds, and that resolver's records,
//! on name detail and the records route. ENSv1 clients read the resolver from the registry
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L137-L141 @ ens_v1@91c966f). The
//! composed row and its record inventory take the resolver from the name's resource pointer,
//! which can lag the node's registry pointer: `project_registry_pointer` keeps the node's latest
//! ENSv1 `ResolverChanged` whichever resource the event carries, and the composed row stores it
//! as `declared_summary.ens_v1_resolver`.
//!
//! The decision is taken once per request, on the row the request's read snapshot served
//! ([`serve_ens_v1_registry_resolver`]), and the record inventory follows it
//! (`load_records_route_inventory`). Only a row that serves a resolver, whose selected arm is
//! `ens_v1` or `ens_v0` and whose served pointer is an ENSv1 one is read this way. A row the
//! cut-over walk withholds or cannot prove, or whose coverage keeps its resolver out of the
//! response, is served as composed. A registry pointer to an address no resolver manifest
//! declares serves that address and its `unsupported` inventory (`resolver_not_declared`), as
//! any name pointing at an undeclared resolver is served. Only an address with no classification
//! row at all keeps the composed answer, because no inventory can be assembled for it.
use bigname_storage::{
    ChainPositions, NameCurrentRow, RecordInventoryCurrentRow, SelectedSnapshot,
    SnapshotSelectionError, families::records::FamilyRecordInventory,
    public_name_fields::ens_v1_resolver,
};
use serde_json::{Value, json};
use sqlx::{PgConnection, PgPool};

use crate::v2::name_record::row_serves_resolver;

/// The ENSv1 pointer families, whose `ResolverChanged` Project also keeps as the node's
/// `project_registry_pointer` row (crates/project/src/families/resolver.rs).
const V1_POINTER_FAMILIES: [&str; 3] = [
    "ens_v1_registry_l1",
    "ens_v1_registrar_l1",
    "ens_v1_wrapper_l1",
];
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// What a name serves through its ENSv1 registry pointer on name detail and the records route.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) enum RegistryRead {
    /// The composed resolver and record inventory.
    #[default]
    Composed,
    /// The registry's pointer is cleared: no resolver and no records.
    Cleared,
    /// The registry names this lower-cased resolver, which has a classification, supported or
    /// not: it and its inventory for the node.
    Resolver(String),
}

/// What `row`'s own fields decide.
#[derive(Debug, Eq, PartialEq)]
enum Decision {
    Composed,
    /// The stored registry pointer is null while the row serves a resolver. It is a clear,
    /// unless bigname observed no pointer write for the node.
    NullPointer,
    /// The stored registry pointer names another resolver.
    Resolver(String),
}

fn decide(row: &NameCurrentRow) -> Decision {
    let arm = row
        .provenance
        .pointer("/authority_selection/authority_arm")
        .and_then(Value::as_str);
    let family = row
        .provenance
        .get("resolver_pointer_source_family")
        .and_then(Value::as_str);
    if !row_serves_resolver(row)
        || !matches!(arm, Some("ens_v1" | "ens_v0"))
        || !family.is_some_and(|family| V1_POINTER_FAMILIES.contains(&family))
        || row.unresolvable_reason().is_some()
        || row.resolution_unsupported_reason().is_some()
    {
        return Decision::Composed;
    }
    let served = row
        .declared_summary
        .pointer("/resolver/address")
        .and_then(Value::as_str)
        .filter(|address| !address.is_empty() && *address != ZERO_ADDRESS)
        .map(str::to_ascii_lowercase);
    let Ok(Some(registry)) = ens_v1_resolver(&row.declared_summary) else {
        return Decision::Composed;
    };
    match (served, registry) {
        (Some(served), Some(registry)) if served == registry.address => Decision::Composed,
        (_, Some(registry)) => Decision::Resolver(registry.address),
        (Some(_), None) => Decision::NullPointer,
        (None, None) => Decision::Composed,
    }
}

pub(super) fn row_chain_id(row: &NameCurrentRow) -> Option<String> {
    ChainPositions::from_value(&row.chain_positions)
        .ok()?
        .as_map()
        .values()
        .map(|position| position.chain_id.clone())
        .next()
}

/// Decides what `row` serves through its registry pointer, on `conn`'s snapshot, and replaces its
/// served resolver with the registry's when that is not the composed one.
pub(crate) async fn serve_ens_v1_registry_resolver(
    conn: &mut PgConnection,
    row: &mut NameCurrentRow,
) -> anyhow::Result<RegistryRead> {
    let decision = decide(row);
    let Some(chain_id) = row_chain_id(row).filter(|_| decision != Decision::Composed) else {
        return Ok(RegistryRead::Composed);
    };
    let read = match decision {
        Decision::Composed => return Ok(RegistryRead::Composed),
        Decision::NullPointer => {
            let pointer = FamilyRecordInventory::load_ens_v1_registry_pointer(
                conn,
                &chain_id,
                &row.namespace,
                &row.namehash,
            )
            .await?;
            // `None` is a node with no observed pointer write: the composed answer stands.
            if pointer != Some(None) {
                return Ok(RegistryRead::Composed);
            }
            RegistryRead::Cleared
        }
        Decision::Resolver(resolver) => {
            if !FamilyRecordInventory::resolver_is_classified(conn, &chain_id, &resolver).await? {
                return Ok(RegistryRead::Composed);
            }
            RegistryRead::Resolver(resolver)
        }
    };
    let address = match &read {
        RegistryRead::Resolver(resolver) => Some(resolver.clone()),
        RegistryRead::Composed | RegistryRead::Cleared => None,
    };
    let latest_event_kind = row
        .declared_summary
        .pointer("/resolver/latest_event_kind")
        .cloned()
        .unwrap_or(Value::Null);
    row.declared_summary["resolver"] = json!({
        "chain_id": address.as_ref().map(|_| chain_id),
        "address": address,
        "latest_event_kind": latest_event_kind,
    });
    Ok(read)
}

/// The record inventory `resolver`, the registry resolver the request decided to serve, holds
/// for `row`'s node at the family publication.
pub(crate) async fn ens_v1_registry_record_inventory(
    pool: &PgPool,
    row: &NameCurrentRow,
    resolver: &str,
    selected_snapshot: &SelectedSnapshot,
) -> Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    let internal = |error: anyhow::Error| {
        SnapshotSelectionError::internal(format!(
            "failed to read the ENSv1 registry resolver records of {}: {error}",
            row.logical_name_id
        ))
    };
    let chain_id = row_chain_id(row).ok_or_else(|| {
        internal(anyhow::anyhow!(
            "composed name row carries no chain position"
        ))
    })?;
    let mut snapshot = bigname_storage::begin_read_snapshot(pool)
        .await
        .map_err(internal)?;
    let inventory = FamilyRecordInventory::load_resolver_node_for_snapshot(
        &mut snapshot,
        &chain_id,
        resolver,
        &row.namespace,
        &row.logical_name_id,
        &row.namehash,
        None,
        &selected_snapshot.chain_positions,
    )
    .await?;
    snapshot
        .commit()
        .await
        .map_err(|error| internal(error.into()))?;
    // The resolver was classified when the request decided to serve it. Losing that between the
    // two reads means a publication moved under the request.
    inventory.map(Some).ok_or_else(|| {
        SnapshotSelectionError::stale(
            "the ENSv1 registry resolver changed while the request read it; retry the request",
        )
    })
}

#[cfg(test)]
#[path = "registry_read_tests.rs"]
mod tests;
