use super::{LookupInventoryMetadata, LookupRecordEntry};
use crate::{
    IdentityRecordInventoryRow,
    families::name::{FamilyPublication, rendered::rendered_name_sql},
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use sqlx::PgConnection;
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

pub(super) async fn load(
    conn: &mut PgConnection,
    publication: &FamilyPublication,
    resources: &[Uuid],
) -> Result<BTreeMap<Uuid, IdentityRecordInventoryRow>> {
    let mut out = BTreeMap::new();
    for requested in resources.chunks(256) {
        let metadata: Vec<(Uuid, Option<Value>)> = sqlx::query_as("/* storage:families.lookup.inventories */
            SELECT resource_id, metadata FROM bigname_phase.project_lookup_inventory WHERE chain_id=$1 AND resource_id=ANY($2)")
            .bind(&publication.chain_id).bind(requested).fetch_all(&mut *conn).await?;
        ensure!(
            metadata.len() == requested.len(),
            "published lookup resource has no prepared inventory state"
        );
        let selected: Vec<(Uuid, String, Value)> = sqlx::query_as(
            "/* storage:families.lookup.records */
            SELECT resource_id, record_key, payload FROM bigname_phase.project_lookup_record
            WHERE chain_id=$1 AND resource_id=ANY($2) ORDER BY resource_id, record_key",
        )
        .bind(&publication.chain_id)
        .bind(requested)
        .fetch_all(&mut *conn)
        .await?;
        let mut records: BTreeMap<Uuid, Vec<(String, LookupRecordEntry)>> = BTreeMap::new();
        let mut family_names = BTreeSet::new();
        for (resource, key, payload) in selected {
            let payload: LookupRecordEntry =
                serde_json::from_value(payload).context("invalid stored lookup record")?;
            family_names.extend(payload.unsupported_family.iter().cloned());
            records.entry(resource).or_default().push((key, payload));
        }
        let ordered_families = crate::families::records::facts::collation_order(
            conn,
            family_names.into_iter().collect(),
        )
        .await?;
        let metadata: Vec<(Uuid, LookupInventoryMetadata)> = metadata
            .into_iter()
            .filter_map(|(resource, metadata)| {
                metadata.map(|metadata| {
                    serde_json::from_value(metadata).map(|metadata| (resource, metadata))
                })
            })
            .collect::<std::result::Result<_, _>>()
            .context("invalid stored lookup inventory")?;
        let mirrored = mirrored_names(conn, publication, &metadata).await?;
        for (resource, metadata) in metadata {
            let rows = records.remove(&resource).unwrap_or_default();
            let families: BTreeSet<_> = rows
                .iter()
                .filter_map(|(_, row)| row.unsupported_family.as_ref())
                .collect();
            let families: Vec<_> = ordered_families
                .iter()
                .filter(|family| families.contains(family))
                .cloned()
                .collect();
            out.insert(
                resource,
                metadata.assemble(
                    resource,
                    rows.iter().map(|(key, row)| (key.as_str(), row)),
                    &families,
                    mirrored.get(&resource).map(String::as_str),
                    super::read::positions(publication),
                )?,
            );
        }
    }
    Ok(out)
}

/// Spelling imports change identity outside Project. Resolve every mirror in this resource
/// chunk together rather than copying names into metadata or issuing a query per inventory.
async fn mirrored_names(
    conn: &mut PgConnection,
    publication: &FamilyPublication,
    metadata: &[(Uuid, LookupInventoryMetadata)],
) -> Result<BTreeMap<Uuid, String>> {
    let requested: Vec<_> = metadata
        .iter()
        .filter_map(|(resource, metadata)| {
            metadata
                .provenance
                .pointer("/mirror/mirrored_node")
                .and_then(Value::as_str)
                .map(|node| {
                    json!({"resource_id":resource, "node":node,
                "logical_name_id":metadata.provenance["logical_name_id"]})
                })
        })
        .collect();
    if requested.is_empty() {
        return Ok(BTreeMap::new());
    }
    let rows: Vec<(Uuid, String)> = sqlx::query_as(&format!(
        "/* storage:families.lookup.mirror_names */
        SELECT request.resource_id, {rendered} AS name
        FROM jsonb_to_recordset($2::jsonb) request(resource_id uuid,node text,logical_name_id text)
        JOIN bigname_phase.name_surfaces queried ON queried.logical_name_id=request.logical_name_id
        JOIN bigname_phase.name_surfaces surface ON surface.chain_id=$1
          AND surface.namehash=request.node AND surface.namespace=queried.namespace
        WHERE surface.canonicality_state IN ('canonical','safe','finalized')",
        rendered = rendered_name_sql("surface")
    ))
    .bind(&publication.chain_id)
    .bind(json!(requested))
    .fetch_all(conn)
    .await?;
    ensure!(
        rows.len() == requested.len(),
        "stored mirror lost its retained identity"
    );
    Ok(rows.into_iter().collect())
}
