//! API adapters for shared ENSv1 shaping and the registry-child omission rule.
use super::super::{V2Error, V2Result, vocab::Authority};
use super::wrapper_expiry;
pub(crate) use bigname_storage::public_name_fields::EnsV1;
use bigname_storage::{
    AddressNameCurrentEntry, ChildrenCurrentRow, NameCurrentRow,
    wrapper_expiry::{self, WrapperExpiryKey},
};
use serde_json::Value;
use std::collections::BTreeMap;

pub(crate) fn ens_v1(authority: Option<Authority>, summary: &Value) -> V2Result<Option<EnsV1>> {
    bigname_storage::public_name_fields::ens_v1(authority.map(Authority::as_str), summary)
        .map_err(|error| V2Error::internal_error(error.to_string()))
}

/// Reads the pending wrapper expiries of `objects` and serves them: one read per chain for the
/// whole response, on `db`, which must be the snapshot the rows were composed on where the route
/// holds one.
pub(crate) async fn fill_wrapper_expiries<'a>(
    db: impl Into<bigname_storage::ReadDb<'_>>,
    objects: impl IntoIterator<Item = &'a mut EnsV1>,
) -> V2Result<()> {
    let mut objects: Vec<&mut EnsV1> = objects
        .into_iter()
        .filter(|object| object.pending_wrapper_expiry.is_some())
        .collect();
    if objects.is_empty() {
        return Ok(());
    }
    let wanted: BTreeMap<WrapperExpiryKey, bool> = objects
        .iter()
        .filter_map(|object| object.pending_wrapper_expiry.clone())
        .collect();
    let served = wrapper_expiry::load_wrapper_expiries(db, &wanted)
        .await
        .map_err(|error| {
            tracing::error!(service = "api", error = ?error, "failed to read wrapper expiries");
            inconsistent_wrapper_expiry()
        })?;
    for object in &mut objects {
        let Some((key, _)) = object.pending_wrapper_expiry.take() else {
            continue;
        };
        if let Some(word) = served.get(&key).filter(|word| !word.is_null()) {
            let (timestamp, reason) =
                wrapper_expiry(word).ok_or_else(inconsistent_wrapper_expiry)?;
            object.wrapper_expires_at = Some(timestamp);
            object.wrapper_expires_at_reason = reason;
        }
    }
    Ok(())
}

/// One registry child served with no name row: its chain, namespace and node, and its object.
type RegistryChild<'a> = (&'a str, &'a str, &'a str, &'a mut EnsV1);

/// Serves the ENSv1 registry resolver pointer on each registry child's `ens_v1` object: one
/// read per chain, on `db`, the snapshot the rows were read on.
async fn fill_registry_child_resolvers(
    db: impl Into<bigname_storage::ReadDb<'_>>,
    children: &mut [RegistryChild<'_>],
) -> V2Result<()> {
    let mut db = db.into();
    let mut by_chain: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (index, (chain_id, ..)) in children.iter().enumerate() {
        by_chain.entry(chain_id).or_default().push(index);
    }
    for (chain_id, indices) in by_chain {
        let nodes: Vec<(String, String)> = indices
            .iter()
            .map(|&index| {
                let (_, namespace, node, _) = &children[index];
                ((*namespace).to_owned(), node.to_ascii_lowercase())
            })
            .collect();
        let pointers = bigname_storage::families::name::load_ens_v1_resolvers(
            db.reborrow(),
            chain_id,
            &nodes,
        )
        .await
        .map_err(|error| {
            tracing::error!(service = "api", error = ?error, "failed to read registry pointers");
            V2Error::internal_error("failed to read registry resolver pointers")
        })?;
        for (index, key) in indices.into_iter().zip(nodes) {
            let pointer = pointers.get(&key).cloned().unwrap_or(Value::Null);
            children[index].3.resolver = bigname_storage::public_name_fields::ens_v1_resolver(
                &serde_json::json!({ "ens_v1_resolver": pointer }),
            )
            .map_err(|error| V2Error::internal_error(error.to_string()))?;
        }
    }
    Ok(())
}

/// The response-time reads of a children page's `ens_v1` objects, given in page order: the
/// registry resolver pointer of each child with no name row, on the parent's chain, then the
/// wrapper expiries.
pub(crate) async fn fill_children_ens_v1<'a>(
    db: impl Into<bigname_storage::ReadDb<'_>>,
    chain_id: Option<&str>,
    rows: &'a [ChildrenCurrentRow],
    name_rows: &BTreeMap<String, NameCurrentRow>,
    objects: impl IntoIterator<Item = Option<&'a mut EnsV1>>,
) -> V2Result<()> {
    let mut children = Vec::new();
    let mut named = Vec::new();
    for (row, object) in rows.iter().zip(objects) {
        let Some(object) = object else {
            continue;
        };
        match chain_id.filter(|_| !name_rows.contains_key(&row.child_logical_name_id)) {
            Some(chain_id) => children.push((chain_id, &*row.namespace, &*row.namehash, object)),
            None => named.push(object),
        }
    }
    fill_ens_v1(db, children, named).await
}

/// [`fill_children_ens_v1`] for an address-names page: an entry with no name row is a registry
/// child on the chain its provenance names.
pub(crate) async fn fill_address_names_ens_v1<'a>(
    db: impl Into<bigname_storage::ReadDb<'_>>,
    entries: &'a [AddressNameCurrentEntry],
    name_rows: &BTreeMap<String, NameCurrentRow>,
    objects: impl IntoIterator<Item = Option<&'a mut EnsV1>>,
) -> V2Result<()> {
    let mut children = Vec::new();
    let mut named = Vec::new();
    for (entry, object) in entries.iter().zip(objects) {
        let Some(object) = object else {
            continue;
        };
        let chain_id = entry.provenance.get("chain_id").and_then(Value::as_str);
        match chain_id.filter(|_| !name_rows.contains_key(&entry.logical_name_id)) {
            Some(chain_id) => {
                children.push((chain_id, &*entry.namespace, &*entry.namehash, object));
            }
            None => named.push(object),
        }
    }
    fill_ens_v1(db, children, named).await
}

async fn fill_ens_v1(
    db: impl Into<bigname_storage::ReadDb<'_>>,
    mut children: Vec<RegistryChild<'_>>,
    named: Vec<&mut EnsV1>,
) -> V2Result<()> {
    let mut db = db.into();
    fill_registry_child_resolvers(db.reborrow(), &mut children).await?;
    fill_wrapper_expiries(
        db,
        children.into_iter().map(|(.., object)| object).chain(named),
    )
    .await
}

fn inconsistent_wrapper_expiry() -> V2Error {
    V2Error::internal_error("stored wrapper expiry is inconsistent")
}

/// The `ens_v1` object of an ENSv1 registry child with no name row, which serves `authority` from
/// its registry.
///
/// The registry resolver pointer is left out here: the response reads it for the whole page
/// afterwards (`fill_children_ens_v1`, `fill_address_names_ens_v1`).
///
/// A child no label-bearing event named holds only a null expiry. A lease is the BaseRegistrar's
/// `expiries[id]` and token, which a registry `setSubnodeOwner` child never gets
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L142-L147 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f).
///
/// A child whose only name surface is a shadow one (the surface bigname records, but keeps out of
/// name reads, for a name that fails ENSIP-15 normalization) has no name row either, but it can
/// be wrapped or leased. NameWrapper's `setSubnodeOwner` and `setSubnodeRecord` take any label
/// bytes, `_addLabel` checks only the length, and `_wrap` emits `NameWrapped` with that name
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L565-L585 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L596-L630 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L865-L876 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L894-L903 @ ens_v1@91c966f).
/// For a name that fails normalization the wrapper adapter writes a shadow surface instead of a
/// name (crates/adapters/src/schema_v2/protocol/v1/wrapper.rs:133 and :258-267), so its wrapper
/// state, and a `.eth` name's lease, are projected without a composed name. When a NameWrapper or
/// ENSv1 registrar event observed the shadow (`lifecycle_shadow`) the object omits `expires_at`
/// and the wrapper fields rather than claim the child has no lease or wrapper state. A shadow
/// only another observer wrote, such as a resolver `NameChanged`, brings no lifecycle, so that
/// child keeps the null expiry.
pub(crate) fn ens_v1_of_registry_child(
    authority: Option<Authority>,
    lifecycle_shadow: bool,
) -> V2Result<Option<EnsV1>> {
    let ens_v1 = ens_v1(authority, &Value::Object(serde_json::Map::new()))?;
    Ok(ens_v1.map(|ens_v1| {
        if lifecycle_shadow {
            EnsV1 {
                expires_at: None,
                ..ens_v1
            }
        } else {
            ens_v1
        }
    }))
}

/// The `ens_v1` object of a composed name row, for the rows that serve one.
pub(crate) fn ens_v1_of_row(row: Option<&NameCurrentRow>) -> V2Result<Option<EnsV1>> {
    row.map(|row| {
        ens_v1(
            Authority::from_provenance(&row.provenance),
            &row.declared_summary,
        )
    })
    .transpose()
    .map(Option::flatten)
}
