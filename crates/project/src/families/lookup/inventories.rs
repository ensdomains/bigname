use super::super::{
    block::BlockStats,
    input::BlockHeader,
    tables::{LOOKUP_DEPENDENCY, LOOKUP_INVENTORY, LOOKUP_RECORD},
};
use super::replace::{changed, rows_for_resources};
use crate::{ProjectError, Result};
use bigname_storage::families::{
    lookup::{LookupInventoryDependency, compose_lookup_inventories_at},
    name::FamilyPublication,
};
use serde_json::json;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

pub(super) async fn refresh(
    transaction: &mut Transaction<'_, Postgres>,
    publication: &FamilyPublication,
    block: &BlockHeader,
    stats: &mut BlockStats,
) -> Result<()> {
    let chain = &publication.chain_id;
    let mut after: Option<Uuid> = None;
    loop {
        let work: Vec<(Uuid, bool, bool)> = sqlx::query_as("/* project:families.lookup.inventory_page */
            SELECT work.resource_id,
                EXISTS (SELECT 1 FROM project_lookup_name name WHERE name.chain_id=$1
                    AND name.record_serving_resource_id=work.resource_id) AS referenced,
                work.refresh OR NOT EXISTS (SELECT 1 FROM project_lookup_inventory inventory
                    WHERE inventory.chain_id=$1 AND inventory.resource_id=work.resource_id) AS refresh
            FROM pg_temp.bigname_lookup_inventory_work work WHERE ($2::uuid IS NULL OR work.resource_id > $2)
            ORDER BY work.resource_id LIMIT 256")
            .bind(chain).bind(after).fetch_all(&mut **transaction).await
            .map_err(|e| ProjectError::database("failed to page lookup inventories", e))?;
        let Some((last, _, _)) = work.last() else {
            break;
        };
        after = Some(*last);
        let affected: Vec<_> = work
            .iter()
            .filter(|(_, referenced, refresh)| !referenced || *refresh)
            .map(|(id, _, _)| *id)
            .collect();
        let requested: Vec<_> = work
            .iter()
            .filter(|(_, referenced, refresh)| *referenced && *refresh)
            .map(|(id, _, _)| *id)
            .collect();
        if affected.is_empty() {
            continue;
        }
        let fresh = compose_lookup_inventories_at(transaction, publication, &requested)
            .await
            .map_err(|e| {
                ProjectError::data_integrity(format!("failed to compose lookup inventories: {e:#}"))
            })?;
        let mut metadata = Vec::new();
        let mut records = Vec::new();
        let mut dependencies = Vec::new();
        for (resource, selected) in fresh {
            metadata.push(
                json!({"chain_id":chain, "resource_id":resource, "metadata":selected.metadata()}),
            );
            for (key, record) in selected.records {
                records.push(json!({"chain_id":chain, "resource_id":resource, "record_key":key, "payload":record}));
            }
            for dependency in selected.dependencies {
                let (kind, key1, key2, key3) = dependency_key(dependency);
                dependencies.push(json!({"chain_id":chain, "resource_id":resource,
                    "kind":kind, "key1":key1, "key2":key2, "key3":key3}));
            }
        }
        for (table, rows) in [
            (&LOOKUP_INVENTORY, metadata),
            (&LOOKUP_RECORD, records),
            (&LOOKUP_DEPENDENCY, dependencies),
        ] {
            let before = rows_for_resources(transaction, chain, table, &affected).await?;
            changed(transaction, chain, block, table, before, rows, stats).await?;
        }
    }
    Ok(())
}

fn dependency_key(dependency: LookupInventoryDependency) -> (&'static str, String, String, String) {
    use LookupInventoryDependency::*;
    match dependency {
        ResourcePointer { resource_id } => (
            "resource_pointer",
            resource_id.to_string(),
            String::new(),
            String::new(),
        ),
        Identity { logical_name_id } => ("identity", logical_name_id, String::new(), String::new()),
        Classification { resolver_address } => (
            "classification",
            resolver_address,
            String::new(),
            String::new(),
        ),
        RegistryNode { namespace, node } => ("registry_node", namespace, node, String::new()),
        Partition {
            resolver_address,
            arm,
            arm_identity,
        } => ("partition", resolver_address, arm, arm_identity),
        Link {
            resolver_address,
            node,
        } => ("link", resolver_address, node, String::new()),
        RecordId {
            resolver_address,
            record_id,
        } => ("record_id", resolver_address, record_id, String::new()),
    }
}
