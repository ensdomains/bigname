//! The ENSv1 name decoding of registrar lifecycle rows (name_authority/stage.rs:147-198),
//! reproduced when a retained row is written and again when a binding candidate for its lease
//! arrives. A row the adapter emitted unnamed is named first through a binding of its resource
//! whose surface namehash is the row's namehash, then through a wrapper candidate that recorded
//! the resource as its wrapped registrar lease at that node, leaving out the transfer that moves
//! the token into the wrapper in the wrap's own transaction. A named row keeps its own name. The
//! result is informational: the read recomputes the two passes from the immutable original.
use std::collections::BTreeMap;

use serde_json::Value;
use sqlx::{Postgres, Transaction};

use super::{
    input::BlockEvent,
    reduce::{Context, in_family, key_of, load_rows},
    store::{Row, RowSet},
    tables,
};
use crate::{ProjectError, Result};

const REGISTRAR: &str = "ens_v1_registrar_l1";

/// The binding candidates of the leases this block's registrar rows and candidates touch, as
/// they stand after the block's own candidates.
pub(crate) struct Candidates {
    rows: Vec<Row>,
    arrived: Vec<String>,
}

fn text(row: &Row, column: &str) -> Option<String> {
    row.get(column).and_then(Value::as_str).map(str::to_owned)
}

fn decodable(event: &BlockEvent) -> bool {
    event.logical_name_id.is_none()
        && event.source_family == REGISTRAR
        && event.event_kind != "RegistrationReserved"
        && event.resource_id.is_some()
}

impl Candidates {
    pub(crate) async fn load(
        transaction: &mut Transaction<'_, Postgres>,
        context: &Context<'_>,
        events: &[BlockEvent],
        rows: &RowSet,
    ) -> Result<Self> {
        let table = &tables::BINDING_CANDIDATE;
        let changed: BTreeMap<String, Option<Row>> = rows
            .changes()
            .into_iter()
            .filter(|change| change.table.name == table.name)
            .map(|change| (change.key.to_owned(), change.after.cloned()))
            .collect();
        let mut arrived = Vec::new();
        for row in changed.values().flatten() {
            arrived.extend(text(row, "resource_id"));
            arrived.extend(text(row, "wrapped_registrar_resource_id"));
        }
        let mut leases: Vec<String> = events
            .iter()
            .filter(|event| decodable(event))
            .filter_map(|event| event.resource_id.clone())
            .chain(arrived.iter().cloned())
            .collect();
        leases.sort();
        leases.dedup();
        if leases.is_empty() {
            return Ok(Self {
                rows: Vec::new(),
                arrived,
            });
        }
        let stored: Vec<Value> = sqlx::query_scalar(
            "/* project:families.decode.lease_candidates */ SELECT to_jsonb(candidate)
             FROM project_binding_candidate candidate
             WHERE candidate.chain_id = $1
               AND (candidate.resource_id::text = ANY($2)
                    OR candidate.wrapped_registrar_resource_id::text = ANY($2))",
        )
        .bind(context.chain_id)
        .bind(&leases)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to read the leases' binding candidates", error)
        })
        .map_err(in_family(tables::LIFECYCLE_EVENT.name))?;
        let mut by_key: BTreeMap<String, Row> = stored
            .into_iter()
            .filter_map(|value| match value {
                Value::Object(row) => Some(row),
                _ => None,
            })
            .map(|row| (super::store::key_text(table, &row), row))
            .collect();
        for (key, after) in changed {
            match after {
                Some(row) => by_key.insert(key, row),
                None => by_key.remove(&key),
            };
        }
        Ok(Self {
            rows: by_key.into_values().collect(),
            arrived,
        })
    }

    /// The decoded name of a retained row.
    pub(crate) fn decode(&self, row: &Row) -> Option<String> {
        if let Some(original) = text(row, "original_logical_name_id") {
            return Some(original);
        }
        let kind = text(row, "event_kind")?;
        if text(row, "source_family").as_deref() != Some(REGISTRAR)
            || kind == "RegistrationReserved"
        {
            return None;
        }
        let resource = text(row, "resource_id")?;
        let namehash = text(row, "namehash")?;
        let lower = |value: Option<String>| value.map(|value| value.to_lowercase());
        let direct = self.rows.iter().filter(|candidate| {
            text(candidate, "resource_id").as_deref() == Some(resource.as_str())
                && lower(text(candidate, "surface_namehash")).as_deref() == Some(namehash.as_str())
        });
        let wrapped = self.rows.iter().filter(|candidate| {
            text(candidate, "wrapped_registrar_resource_id").as_deref() == Some(resource.as_str())
                && lower(text(candidate, "node")).as_deref() == Some(namehash.as_str())
                && !(kind == "TokenControlTransferred"
                    && text(candidate, "transaction_hash") == text(row, "transaction_hash")
                    && lower(text(candidate, "emitting_address")) == text(row, "to_address"))
        });
        let first = |candidates: &mut dyn Iterator<Item = &Row>| {
            candidates
                .filter_map(|candidate| text(candidate, "logical_name_id"))
                .min()
        };
        first(&mut direct.into_iter()).or_else(|| first(&mut wrapped.into_iter()))
    }
}

/// Name the still-unnamed retained rows of every lease a candidate of this block binds or wraps.
pub(crate) async fn redecode(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    candidates: &Candidates,
    rows: &mut RowSet,
) -> Result<()> {
    if candidates.arrived.is_empty() {
        return Ok(());
    }
    let table = &tables::LIFECYCLE_EVENT;
    let keys: Vec<(String, String)> = sqlx::query_as(
        "/* project:families.decode.unnamed_rows */ SELECT state_key, event_identity
         FROM project_lifecycle_event
         WHERE chain_id = $1 AND state_kind = 'resource' AND state_key = ANY($2)
           AND source_family = 'ens_v1_registrar_l1'
           AND original_logical_name_id IS NULL AND decoded_logical_name_id IS NULL",
    )
    .bind(context.chain_id)
    .bind(&candidates.arrived)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to read unnamed lifecycle rows", error))
    .map_err(in_family(table.name))?;
    let keys: Vec<Row> = keys
        .into_iter()
        .map(|(state_key, identity)| {
            key_of(
                table,
                [
                    context.chain_id.into(),
                    "resource".into(),
                    state_key.into(),
                    identity.into(),
                ],
            )
        })
        .collect();
    load_rows(transaction, rows, table, keys.clone()).await?;
    for key in keys {
        let Some(mut row) = rows.get(table, &key).cloned() else {
            continue;
        };
        if row
            .get("decoded_logical_name_id")
            .is_some_and(|value| !value.is_null())
        {
            continue;
        }
        if let Some(name) = candidates.decode(&row) {
            row.insert("decoded_logical_name_id".to_owned(), Value::String(name));
            rows.put(table, row).map_err(in_family(table.name))?;
        }
    }
    Ok(())
}
