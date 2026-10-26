//! The record inventory a resolver holds for one node, read by (resolver, node) instead of through
//! a resource's resolver pointer: `GET /v1/resolvers/{chain_id}/{address}/records`.
//!
//! The record families keep every admitted resolver write for any node, whatever the registry
//! points at (`crates/project/src/families/records.rs`). The resource-keyed read applies the
//! pointer only to choose the resolver and the partitions. This read takes the resolver from the
//! caller and stands in a pointer for it: the queried node and name, and the pointer family of the
//! resolver's own family, so the partitions are the ones the resolver's writes are kept in. The
//! mirror substitution is the resource-keyed read's, and the records are selected by the same
//! function (`inventory_selection::select_plans`), with the same cutoff and winner rules, then
//! assembled the same way.
use std::collections::BTreeSet;

use anyhow::Result;
use sqlx::PgConnection;
use uuid::Uuid;

use crate::families::records::{
    assemble::{self, Assembly, AssemblyReads},
    facts::{ResolverClassification, load_classifications_at},
    inventory_selection::{Plan, Selected, select_plans},
    inventory_types::FamilyRecordInventory,
    is_cleared,
    mirror::{evaluate_family_mirror_at, is_mirror_pointer},
    serving::ServingPointer,
};
use crate::{RecordInventoryCurrentRow, SnapshotSelectionError, families::name::FamilyPublication};

/// The resolver and the node a resolver-anchored read asks about. `resolver_address` and `node`
/// are compared lower-cased, as the record families store them.
struct ResolverNode<'a> {
    resolver_address: &'a str,
    namespace: &'a str,
    logical_name_id: &'a str,
    node: &'a str,
}

impl FamilyRecordInventory {
    /// The inventory `GET /v1/resolvers/{chain_id}/{address}/records` serves: what
    /// `resolver_address` holds for `node`, named `logical_name_id` in `namespace`, read on
    /// `conn` at the family publication of `chain_id`. Only that publication is servable, so any
    /// other selected position is stale, as on the name records route. With `keys`, only those
    /// record keys are read and assembled. `None` when the resolver has no classification.
    #[allow(clippy::too_many_arguments)]
    pub async fn load_resolver_node_for_snapshot(
        conn: &mut PgConnection,
        chain_id: &str,
        resolver_address: &str,
        namespace: &str,
        logical_name_id: &str,
        node: &str,
        keys: Option<&BTreeSet<String>>,
        selected: &crate::ChainPositions,
    ) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
        let internal = |error: anyhow::Error| {
            if crate::families::name::is_publication_unavailable(&error) {
                return SnapshotSelectionError::stale(format!(
                    "record data is unavailable while the families rebuild: {error}"
                ));
            }
            SnapshotSelectionError::internal(format!(
                "failed to assemble the record inventory of resolver {resolver_address} for \
                 {logical_name_id}: {error}"
            ))
        };
        let publication = crate::families::name::servable_publication(conn, chain_id)
            .await
            .map_err(internal)?;
        let at_publication = selected.as_map().values().any(|position| {
            position.chain_id == chain_id
                && position.block_number == publication.block_number
                && position.block_hash == publication.block_hash
        });
        if !at_publication {
            return Err(SnapshotSelectionError::stale(
                "record data is unavailable at the selected historical position",
            ));
        }
        let key = ResolverNode {
            resolver_address,
            namespace,
            logical_name_id,
            node,
        };
        let inventory = node_inventory_at(conn, &publication, &key, keys)
            .await
            .map_err(internal)?;
        Ok(inventory.map(|inventory| {
            let mut row = inventory.row;
            row.chain_positions = serde_json::json!({
                "target_block_number": publication.block_number,
                "target_block_hash": publication.block_hash,
            });
            row
        }))
    }

    /// The resolver the ENSv1 registry holds for `row`'s node, read at request time, when it is
    /// not `served`, the resolver the row's composed resource pointer serves. `None` keeps the
    /// composed answer. `Some(None)` is a registry clear. ENSv1 clients read the resolver from the
    /// registry (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L137-L141 @ ens_v1@91c966f),
    /// and `project_registry_pointer` keeps the node's latest ENSv1 `ResolverChanged` whichever
    /// resource the event carries, so a resource pointer can lag it. Only a row whose selected
    /// arm is `ens_v1` or `ens_v0` and whose served pointer is an ENSv1 one is read this way, and
    /// never a row the cut-over walk withholds or cannot prove.
    pub async fn ens_v1_registry_resolver(
        conn: &mut PgConnection,
        row: &crate::NameCurrentRow,
        served: Option<&str>,
    ) -> Result<Option<Option<String>>> {
        let arm = row
            .provenance
            .pointer("/authority_selection/authority_arm")
            .and_then(serde_json::Value::as_str);
        let family = row
            .provenance
            .get("resolver_pointer_source_family")
            .and_then(serde_json::Value::as_str);
        if !matches!(arm, Some("ens_v1" | "ens_v0"))
            || !family.is_some_and(|family| V1_POINTER_FAMILIES.contains(&family))
            || row.unresolvable_reason().is_some()
            || row.resolution_unsupported_reason().is_some()
        {
            return Ok(None);
        }
        let Some(chain_id) = crate::ChainPositions::from_value(&row.chain_positions)
            .ok()
            .and_then(|positions| {
                positions
                    .as_map()
                    .values()
                    .map(|position| position.chain_id.clone())
                    .next()
            })
        else {
            return Ok(None);
        };
        let registry: Option<Option<String>> = sqlx::query_scalar(
            "/* storage:families.records.ens_v1_registry_resolver */
             SELECT NULLIF(pointer.resolver_address, '')
             FROM bigname_phase.project_registry_pointer pointer
             WHERE pointer.chain_id = $1 AND pointer.namespace = $2 AND pointer.node = $3",
        )
        .bind(&chain_id)
        .bind(&row.namespace)
        .bind(row.namehash.to_ascii_lowercase())
        .fetch_optional(&mut *conn)
        .await?;
        let Some(registry) = registry else {
            return Ok(None);
        };
        let same = match (is_cleared(served), is_cleared(registry.as_deref())) {
            (false, false) => served
                .unwrap_or_default()
                .eq_ignore_ascii_case(registry.as_deref().unwrap_or_default()),
            (served, registry) => served && registry,
        };
        if same {
            return Ok(None);
        }
        Ok(Some(
            registry
                .filter(|address| !is_cleared(Some(address)))
                .map(|address| address.to_ascii_lowercase()),
        ))
    }

    /// The family inventory `resolver_address` holds for `node`, read on `conn`, which the
    /// caller holds in one snapshot, at the current family publication.
    pub async fn load_resolver_node_on(
        conn: &mut PgConnection,
        chain_id: &str,
        resolver_address: &str,
        namespace: &str,
        logical_name_id: &str,
        node: &str,
    ) -> Result<Option<Self>> {
        let publication = crate::families::name::servable_publication(conn, chain_id).await?;
        let key = ResolverNode {
            resolver_address,
            namespace,
            logical_name_id,
            node,
        };
        node_inventory_at(conn, &publication, &key, None).await
    }
}

/// The ENSv1 pointer families, whose `ResolverChanged` Project also keeps as the node's
/// `project_registry_pointer` row (crates/project/src/families/resolver.rs).
const V1_POINTER_FAMILIES: [&str; 3] = [
    "ens_v1_registry_l1",
    "ens_v1_registrar_l1",
    "ens_v1_wrapper_l1",
];

/// The pointer family whose partitions hold a resolver family's writes (`admitted_partitions`).
fn pointer_family(classification: &ResolverClassification) -> &'static str {
    match classification.field("source_family") {
        Some("ens_v1_resolver_l1") => "ens_v1_registry_l1",
        Some("basenames_base_resolver") => "basenames_base_registry",
        Some("ens_v2_resolver_l1") => "ens_v2_registry_l1",
        _ => "",
    }
}

async fn node_inventory_at(
    conn: &mut PgConnection,
    publication: &FamilyPublication,
    key: &ResolverNode<'_>,
    keys: Option<&BTreeSet<String>>,
) -> Result<Option<FamilyRecordInventory>> {
    let chain_id = publication.chain_id.as_str();
    let resolver = key.resolver_address.to_ascii_lowercase();
    let Some(classification) = load_classifications_at(
        conn,
        chain_id,
        std::slice::from_ref(&resolver),
        Some(publication),
    )
    .await?
    .remove(&resolver) else {
        return Ok(None);
    };
    // No pointer event stands behind this pointer: assembly falls back to the publication block
    // where the resource-keyed read uses the pointer's.
    let serving = ServingPointer {
        resource_id: Uuid::nil(),
        logical_name_id: key.logical_name_id.to_owned(),
        namespace: key.namespace.to_owned(),
        source_family: pointer_family(&classification).to_owned(),
        namehash: key.node.to_ascii_lowercase(),
        resolver_address: resolver,
        pointer_event_id: None,
        block_number: publication.block_number,
    };

    // The mirror walk decides a mirror resolver's substituted pointer, or its unsupported row.
    let (plan, classification) = if is_mirror_pointer(&serving, Some(&classification)) {
        let mirror =
            evaluate_family_mirror_at(conn, chain_id, &serving, classification, publication)
                .await?;
        let Some(substituted) = mirror.substituted(&serving) else {
            let reads = AssemblyReads::load(
                conn,
                chain_id,
                BTreeSet::from([serving.block_number]),
                BTreeSet::new(),
            )
            .await?;
            let (row, record_version_boundary_key) =
                assemble::unsupported_mirror_row(chain_id, &serving, &mirror, &reads, false)?;
            return Ok(Some(FamilyRecordInventory {
                row,
                record_version_boundary_key,
                mirrored: true,
            }));
        };
        let address = substituted.resolver_address.to_ascii_lowercase();
        let classification = load_classifications_at(
            conn,
            chain_id,
            std::slice::from_ref(&address),
            Some(publication),
        )
        .await?
        .remove(&address);
        let plan = Plan {
            pointer: substituted,
            link_pointer: serving,
            mirror: Some(mirror),
        };
        (plan, classification)
    } else {
        let plan = Plan {
            pointer: serving.clone(),
            link_pointer: serving,
            mirror: None,
        };
        (plan, Some(classification))
    };

    let (mut selected, _) = select_plans(
        conn,
        publication,
        vec![(plan, classification)],
        |_| keys,
        keys.is_some(),
        None,
    )
    .await?;
    let Selected {
        plan,
        classification,
        eligibility,
        links,
        linked,
        boundary,
        served,
    } = selected.pop().expect("one plan selects one inventory");
    let assembly = Assembly {
        chain_id,
        pointer: &plan.pointer,
        classification: classification.as_ref(),
        eligibility,
        boundary,
        served,
        links: links.as_ref(),
        linked: &linked,
        attributed: None,
    };
    let reads = AssemblyReads::load(
        conn,
        chain_id,
        assembly.blocks().into_iter().collect(),
        assembly.texts().collect(),
    )
    .await?;
    let (mut row, record_version_boundary_key, _) = assemble::assemble(assembly, &reads)?;
    if let Some(mirror) = &plan.mirror {
        assemble::finish_mirrored(&mut row, mirror, &plan.link_pointer);
    }
    Ok(Some(FamilyRecordInventory {
        row,
        record_version_boundary_key,
        mirrored: plan.mirror.is_some(),
    }))
}
