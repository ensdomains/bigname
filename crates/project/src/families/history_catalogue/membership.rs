//! Exact masks, independent for logical-name and resource history. Existing name rows also
//! retain the selected current-resource evidence, so aliases do not add per-event state.
use std::collections::{BTreeMap, BTreeSet};

use bigname_storage::{
    families::records::CurrentHistoryRelation,
    {historical_history_relations, history_catalogue_contract::relation_mask},
};
use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use super::{
    super::{input::BlockHeader, store, tables::HISTORY_ANCHOR},
    envelopes,
    prepare::Work,
    write,
};
use crate::{ProjectError, Result};

pub(super) async fn refresh(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    block: &BlockHeader,
    work: &Work,
    summary_names: &[String],
    current: &[CurrentHistoryRelation],
) -> Result<(u64, u64)> {
    let before: Vec<Value> = sqlx::query_scalar(
        "/* project:history.membership_before */
SELECT to_jsonb(anchor) FROM (SELECT DISTINCT name FROM unnest($2::text[]) n(name)) wanted
CROSS JOIN LATERAL (SELECT anchor.* FROM project_address_history_anchor anchor
 WHERE anchor.chain_id=$1 AND anchor.anchor_kind=0 AND anchor.anchor_id=wanted.name OFFSET 0) anchor
UNION ALL
SELECT to_jsonb(anchor) FROM (SELECT DISTINCT resource FROM unnest($3::text[]) r(resource)) wanted
CROSS JOIN LATERAL (SELECT anchor.* FROM project_address_history_anchor anchor
 WHERE anchor.chain_id=$1 AND anchor.anchor_kind=1 AND anchor.anchor_id=wanted.resource OFFSET 0) anchor",
    )
    .bind(chain)
    .bind(&work.names)
    .bind(
        work.resources
            .iter()
            .map(Uuid::to_string)
            .collect::<Vec<_>>(),
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| ProjectError::database("failed to read history memberships", e))?;
    let mut fresh: BTreeMap<String, Value> =
        before.iter().map(|row| (key(row), row.clone())).collect();
    let recomposed: BTreeSet<&str> = summary_names.iter().map(String::as_str).collect();
    for row in fresh.values_mut() {
        row["historical_mask"] = json!(0);
        if row["anchor_kind"] == 0 && recomposed.contains(row["anchor_id"].as_str().unwrap_or("")) {
            row["current_mask"] = json!(0);
            row["current_resource_id"] = Value::Null;
        } else if row["anchor_kind"] == 1 {
            row["current_mask"] = json!(0);
        }
    }
    let historical = historical_history_relations(
        transaction,
        chain,
        block.number,
        &work.names,
        &work.resources,
    )
    .await
    .map_err(|e| ProjectError::transient(format!("failed to derive history membership: {e:#}")))?;
    for relation in historical {
        if let Some(name) = relation.logical_name_id {
            let row = entry(
                &mut fresh,
                chain,
                &relation.address,
                &relation.namespace,
                0,
                &name,
            );
            add_mask(row, "historical_mask", relation.relation_mask);
        }
        if let Some(resource) = relation
            .resource_id
            .filter(|resource| work.resources.contains(resource))
        {
            let row = entry(
                &mut fresh,
                chain,
                &relation.address,
                &relation.namespace,
                1,
                &resource.to_string(),
            );
            add_mask(row, "historical_mask", relation.relation_mask);
        }
    }
    for relation in current {
        let row = entry(
            &mut fresh,
            chain,
            &relation.address,
            &relation.namespace,
            0,
            &relation.logical_name_id,
        );
        let mask = relation_mask(relation.relation);
        add_mask(row, "current_mask", mask);
        row["current_resource_id"] = json!(relation.resource_id);
    }
    // Publish name provenance before deriving resource current masks. A resource can be
    // selected by several names, including untouched aliases, whose independent proofs stay.
    let (old_names, old_resources): (Vec<_>, Vec<_>) =
        before.into_iter().partition(|row| row["anchor_kind"] == 0);
    let (names, mut resources): (Vec<_>, Vec<_>) =
        fresh.into_values().partition(|row| row["anchor_kind"] == 0);
    let names = names.into_iter().filter(nonempty).collect();
    let names = envelopes::fill(transaction, chain, 0, names).await?;
    let (mut written, mut undo) =
        write::replace(transaction, chain, block, &HISTORY_ANCHOR, old_names, names).await?;
    let current_resources: Vec<(String, String, Uuid, i16)> = sqlx::query_as(
        "/* project:history.current_resource_memberships */
         SELECT address,namespace,current_resource_id,bit_or(current_mask)
         FROM project_address_history_anchor WHERE chain_id=$1 AND anchor_kind=0
           AND current_resource_id=ANY($2) AND current_mask<>0
         GROUP BY address,namespace,current_resource_id",
    )
    .bind(chain)
    .bind(&work.resources)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| ProjectError::database("failed to derive current history resource masks", e))?;
    let mut by_key: BTreeMap<_, _> = resources.drain(..).map(|row| (key(&row), row)).collect();
    for (address, namespace, resource, mask) in current_resources {
        let row = entry(
            &mut by_key,
            chain,
            &address,
            &namespace,
            1,
            &resource.to_string(),
        );
        row["current_mask"] = json!(mask);
    }
    let fresh = by_key.into_values().filter(nonempty).collect();
    let fresh = envelopes::fill(transaction, chain, 1, fresh).await?;
    let (rows, journal) = write::replace(
        transaction,
        chain,
        block,
        &HISTORY_ANCHOR,
        old_resources,
        fresh,
    )
    .await?;
    written += rows;
    undo += journal;
    Ok((written, undo))
}

fn key(row: &Value) -> String {
    store::key_text(
        &HISTORY_ANCHOR,
        row.as_object().expect("catalogue row is an object"),
    )
}
fn add_mask(row: &mut Value, column: &str, mask: i16) {
    row[column] = json!(row[column].as_i64().unwrap_or(0) | i64::from(mask));
}
fn nonempty(row: &Value) -> bool {
    row["current_mask"].as_i64().unwrap_or(0) != 0
        || row["historical_mask"].as_i64().unwrap_or(0) != 0
}
fn entry<'a>(
    rows: &'a mut BTreeMap<String, Value>,
    chain: &str,
    address: &str,
    namespace: &str,
    kind: i16,
    id: &str,
) -> &'a mut Value {
    let row = json!({"chain_id":chain,"address":address,"namespace":namespace,
        "anchor_kind":kind,"anchor_id":id,"current_mask":0,"historical_mask":0,
        "current_resource_id":null,"first_bucket":null,"last_bucket":null,"bucket_range":"empty",
        "event_mask":0,"key_bloom":"0".repeat(256)});
    rows.entry(key(&row)).or_insert(row)
}
