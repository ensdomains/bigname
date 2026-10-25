//! The pointer a resource serves records through, and the outer gate over it: the current F5
//! pointer when it is not a clear (linked_records.rs, `project_record_pointers`), its
//! support from the resolver classification (record_inventory.rs, `pointer_eligibility`).
//!
//! An ENSv1 resource's F5 pointer is read through its node's registry pointer (F4,
//! `project_registry_pointer`) whenever the two name different resolvers. ENSv1 reads the
//! resolver the registry holds for the node at read time
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L137-L141 @ ens_v1@91c966f), and
//! F4 keeps the node's latest ENSv1 `ResolverChanged` whichever resource the event carries. A
//! clear in F4 clears the served pointer too.
use std::collections::BTreeMap;

use anyhow::{Context, Result};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::{
    FamilyPosition, FamilyResourcePointer,
    facts::{ResolverClassification, probe_events},
    is_cleared,
};

/// The ENSv1 pointer families, whose `ResolverChanged` Project also keeps as the node's F4 row
/// (crates/project/src/families/resolver.rs, `V1_POINTER_FAMILIES`).
pub(crate) const V1_POINTER_FAMILIES: [&str; 3] = [
    "ens_v1_registry_l1",
    "ens_v1_registrar_l1",
    "ens_v1_wrapper_l1",
];

/// Whether two stored resolver addresses select the same resolver, a clear matching a clear.
pub(crate) fn same_resolver(left: Option<&str>, right: Option<&str>) -> bool {
    match (is_cleared(left), is_cleared(right)) {
        (false, false) => left
            .unwrap_or_default()
            .eq_ignore_ascii_case(right.unwrap_or_default()),
        (left, right) => left && right,
    }
}

/// The `(namespace, node)` of an F5 row whose current pointer is an ENSv1 pointer, the key of
/// the F4 row it is read through.
pub(crate) fn registry_node(pointer: &FamilyResourcePointer) -> Option<(String, String)> {
    let family = pointer.source_family.as_deref()?;
    if pointer.pointer_position.is_none() || !V1_POINTER_FAMILIES.contains(&family) {
        return None;
    }
    Some((pointer.namespace.clone()?, pointer.namehash.clone()?))
}

/// One node's F4 row.
struct RegistryPointer {
    resolver_address: String,
    source_family: String,
    position: FamilyPosition,
    normalized_event_id: Option<i64>,
}

/// The F4 rows of `nodes` on `chain_id`, keyed by `(namespace, node)`.
async fn registry_pointers(
    conn: &mut PgConnection,
    chain_id: &str,
    nodes: &[(String, String)],
) -> Result<BTreeMap<(String, String), RegistryPointer>> {
    if nodes.is_empty() {
        return Ok(BTreeMap::new());
    }
    let (namespaces, node_keys): (Vec<String>, Vec<String>) = nodes.iter().cloned().unzip();
    let rows = sqlx::query(
        "/* storage:families.records.serving_registry_pointers */
         SELECT pointer.namespace, pointer.node, pointer.resolver_address, pointer.source_family,
                pointer.block_number, pointer.transaction_index, pointer.log_index,
                pointer.event_identity, pointer.normalized_event_id
         FROM bigname_phase.project_registry_pointer pointer
         WHERE pointer.chain_id = $1
           AND (pointer.namespace, pointer.node) IN (SELECT * FROM unnest($2::text[], $3::text[]))",
    )
    .bind(chain_id)
    .bind(&namespaces)
    .bind(&node_keys)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load the registry pointers of ENSv1 resource pointers")?;
    rows.iter()
        .map(|row| {
            Ok((
                (row.try_get("namespace")?, row.try_get("node")?),
                RegistryPointer {
                    resolver_address: row.try_get("resolver_address")?,
                    source_family: row.try_get("source_family")?,
                    position: FamilyPosition::from_row(row)?,
                    normalized_event_id: row.try_get("normalized_event_id")?,
                },
            ))
        })
        .collect()
}

/// One resource's serving pointer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ServingPointer {
    pub(crate) resource_id: Uuid,
    pub(crate) logical_name_id: String,
    pub(crate) namespace: String,
    pub(crate) source_family: String,
    pub(crate) namehash: String,
    pub(crate) resolver_address: String,
    pub(crate) pointer_event_id: Option<i64>,
    pub(crate) block_number: i64,
}

/// The serving pointers of F5 rows, keyed by resource. A row has none when its current pointer,
/// after the registry read above, is a clear or the resource has only version changes. The row
/// keeps neither the pointer event's normalized event id (when a later version change owns the
/// row) nor its logical name, so both are read back from `normalized_events` by the pointer's
/// event identity, for every row in one statement. A pointer event without a logical name serves
/// no records here, as the serving pointer requires one.
pub(crate) async fn serving_pointers<'a>(
    conn: &mut PgConnection,
    pointers: impl IntoIterator<Item = &'a FamilyResourcePointer>,
) -> Result<BTreeMap<Uuid, ServingPointer>> {
    let pointers: Vec<&FamilyResourcePointer> = pointers.into_iter().collect();
    let mut registry = BTreeMap::new();
    for (chain_id, node) in pointers
        .iter()
        .filter_map(|pointer| registry_node(pointer).map(|node| (pointer.chain_id.as_str(), node)))
    {
        registry.entry(chain_id).or_insert_with(Vec::new).push(node);
    }
    let mut registry_rows = BTreeMap::new();
    for (chain_id, nodes) in registry {
        for (node, row) in registry_pointers(conn, chain_id, &nodes).await? {
            registry_rows.insert((chain_id.to_owned(), node), row);
        }
    }
    let current: Vec<(
        &FamilyResourcePointer,
        &FamilyPosition,
        Option<&RegistryPointer>,
    )> = pointers
        .into_iter()
        .filter_map(|pointer| {
            let position = pointer.pointer_position.as_ref()?;
            let read = registry_node(pointer)
                .and_then(|node| registry_rows.get(&(pointer.chain_id.clone(), node)))
                .filter(|row| {
                    !same_resolver(
                        pointer.resolver_address.as_deref(),
                        Some(&row.resolver_address),
                    )
                });
            let resolver = read.map_or(pointer.resolver_address.as_deref(), |row| {
                Some(row.resolver_address.as_str())
            });
            (!is_cleared(resolver)).then_some((pointer, position, read))
        })
        .collect();
    let identities: Vec<String> = current
        .iter()
        .flat_map(|(_, position, read)| {
            std::iter::once(position.event_identity.clone())
                .chain(read.map(|row| row.position.event_identity.clone()))
        })
        .collect();
    let probed = probe_events(conn, &identities).await?;
    let mut out = BTreeMap::new();
    for (pointer, position, read) in current {
        let event = probed.get(&position.event_identity);
        let Some(logical_name_id) = event.and_then(|event| event.logical_name_id.clone()) else {
            continue;
        };
        let (resolver_address, source_family, pointer_event_id, block_number) = match read {
            Some(row) => (
                row.resolver_address.clone(),
                row.source_family.clone(),
                row.normalized_event_id.or(probed
                    .get(&row.position.event_identity)
                    .map(|event| event.normalized_event_id)),
                row.position.block_number,
            ),
            None => (
                pointer.resolver_address.clone().unwrap_or_default(),
                pointer.source_family.clone().unwrap_or_default(),
                pointer
                    .pointer_event_id_if_last()
                    .or(event.map(|event| event.normalized_event_id)),
                position.block_number,
            ),
        };
        out.insert(
            pointer.resource_id,
            ServingPointer {
                resource_id: pointer.resource_id,
                logical_name_id,
                namespace: pointer.namespace.clone().unwrap_or_default(),
                source_family,
                namehash: pointer.namehash.clone().unwrap_or_default(),
                resolver_address,
                pointer_event_id,
                block_number,
            },
        );
    }
    Ok(out)
}

/// Whether the resource's records are served, and the reason when not: the resolver must be
/// classified and supported, and a manifest-declared resolver needs its declaration manifest in
/// the pointer's namespace.
pub(crate) fn family_pointer_eligibility(
    pointer_namespace: &str,
    classification: Option<&ResolverClassification>,
) -> (bool, Option<String>) {
    let Some(classification) = classification else {
        return (false, Some("resolver_classification_missing".to_owned()));
    };
    if !classification.supported() {
        return (
            false,
            Some(
                classification
                    .unsupported_reason
                    .clone()
                    .unwrap_or_else(|| "resolver_classification_missing".to_owned()),
            ),
        );
    }
    if classification.field("basis") == Some("manifest_declared_address")
        && !classification.declared_in(pointer_namespace)
    {
        return (false, Some("resolver_classification_missing".to_owned()));
    }
    (true, None)
}
