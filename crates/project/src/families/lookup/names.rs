use super::super::{
    block::BlockStats,
    input::BlockHeader,
    tables::{LOOKUP_NAME, LOOKUP_RELATION},
};
use super::replace::{changed, rows_for_names};
use crate::{ProjectError, Result};
use bigname_storage::families::{lookup::compose_lookup_names_at, name::FamilyPublication};
use serde_json::json;
use sqlx::{Postgres, Transaction};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

/// Compose the lookup names of the name work. With `serving` false, as a composition pass's names
/// chunk writes them, a name's serving resource stays null until the pass's inventories stream
/// composes that resource's inventory and sets it (`serve`), so no names chunk needs an
/// inventory row another chunk may be writing.
pub(super) async fn refresh(
    transaction: &mut Transaction<'_, Postgres>,
    publication: &FamilyPublication,
    block: &BlockHeader,
    serving: bool,
    stats: &mut BlockStats,
) -> Result<()> {
    let chain = &publication.chain_id;
    let mut after = String::new();
    loop {
        let names: Vec<String> = sqlx::query_scalar("/* project:families.lookup.name_page */
            SELECT logical_name_id FROM pg_temp.bigname_lookup_name_work
            WHERE logical_name_id COLLATE \"C\" > $1 ORDER BY logical_name_id COLLATE \"C\" LIMIT 256")
            .bind(&after).fetch_all(&mut **transaction).await
            .map_err(|e| ProjectError::database("failed to page lookup names", e))?;
        let Some(last) = names.last() else {
            break;
        };
        after = last.clone();
        let fresh = compose_lookup_names_at(transaction, publication, &names)
            .await
            .map_err(|e| {
                ProjectError::data_integrity(format!("failed to compose lookup names: {e:#}"))
            })?;
        let before = rows_for_names(transaction, chain, &LOOKUP_NAME, &names).await?;
        let old_resources: BTreeMap<&str, Option<Uuid>> = before
            .iter()
            .filter_map(|row| {
                Some((
                    row["logical_name_id"].as_str()?,
                    row["record_serving_resource_id"]
                        .as_str()
                        .and_then(|v| v.parse().ok()),
                ))
            })
            .collect();
        let mut resources = BTreeSet::new();
        let mut new_names = Vec::new();
        let mut new_relations = Vec::new();
        for (name, selected) in fresh {
            let resource = selected
                .core
                .as_ref()
                .and_then(|core| core.record_serving_resource_id);
            if old_resources.get(name.as_str()).copied().flatten() != resource
                || !old_resources.contains_key(name.as_str())
            {
                resources.extend(old_resources.get(name.as_str()).copied().flatten());
                resources.extend(resource);
            }
            new_names.push(json!({"chain_id":chain, "logical_name_id":name,
                "record_serving_resource_id":resource.filter(|_| serving),
                "supported":selected.core.as_ref().is_some_and(|core| core.coverage["status"] != "unsupported"),
                "core":selected.core}));
            for relation in selected.relations {
                new_relations.push(json!({"chain_id":chain, "logical_name_id":name,
                    "address":relation.address, "relation":relation.relation}));
            }
        }
        let resources: Vec<_> = resources.into_iter().collect();
        sqlx::query("/* project:families.lookup.reference_work */ INSERT INTO pg_temp.bigname_lookup_inventory_work
            SELECT resource, true FROM unnest($1::uuid[]) resource ON CONFLICT (resource_id) DO UPDATE SET refresh=true")
            .bind(resources).execute(&mut **transaction).await
            .map_err(|e| ProjectError::database("failed to retain changed lookup references", e))?;
        changed(
            transaction,
            chain,
            block,
            &LOOKUP_NAME,
            before,
            new_names,
            stats,
        )
        .await?;
        let old_relations = rows_for_names(transaction, chain, &LOOKUP_RELATION, &names).await?;
        changed(
            transaction,
            chain,
            block,
            &LOOKUP_RELATION,
            old_relations,
            new_relations,
            stats,
        )
        .await?;
    }
    Ok(())
}

/// Set the serving resource of each composed lookup name of a composition pass's inventories
/// chunk to the one its core records.
pub(super) async fn serve(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    served: &[(String, Uuid)],
) -> Result<()> {
    let (names, resources): (Vec<&str>, Vec<Uuid>) = served
        .iter()
        .map(|(name, resource)| (name.as_str(), *resource))
        .unzip();
    sqlx::query(
        "/* project:families.lookup.serve */ UPDATE project_lookup_name name
        SET record_serving_resource_id = served.resource
        FROM unnest($2::text[], $3::uuid[]) served (logical_name_id, resource)
        WHERE name.chain_id = $1 AND name.logical_name_id = served.logical_name_id",
    )
    .bind(chain)
    .bind(names)
    .bind(resources)
    .execute(&mut **transaction)
    .await
    .map_err(|e| ProjectError::database("failed to set the lookup serving resources", e))?;
    Ok(())
}
