//! The ENSv1 name decoding of registrar lifecycle rows (name_authority/stage.rs:147-198),
//! reproduced when a retained row is written and again when a binding candidate for its lease
//! arrives. A row the adapter emitted unnamed is named first through a binding of its resource
//! whose surface namehash is the row's namehash, then through a wrapper candidate that recorded
//! the resource as its wrapped registrar lease at that node, leaving out the transfer that moves
//! the token into the wrapper in the wrap's own transaction. A named row keeps its own name. The
//! result is informational: the read recomputes the two passes from the immutable original.
//!
//! A pass names a row only through one name. Every candidate a pass admits carries the row's
//! namehash (the direct pass compares the candidate's surface namehash, the wrapper pass its
//! recorded node), and a logical name id is `<namespace>:<namehash>`, so two admitted
//! candidates can differ only in namespace. A registrar lease belongs to one namespace, so that
//! does not happen; if it ever did, the pass names nothing rather than picking one, where the
//! served UPDATE would take whichever candidate row it met first.
use std::collections::{BTreeMap, BTreeSet};

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

/// Whether `text` is a hyphenated uuid, the shape every resource id has.
fn is_uuid(text: &str) -> bool {
    text.len() == 36
        && text.char_indices().all(|(index, character)| match index {
            8 | 13 | 18 | 23 => character == '-',
            _ => character.is_ascii_hexdigit(),
        })
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
        // Only a well-formed resource id can name a candidate's uuid columns.
        leases.retain(|lease| is_uuid(lease));
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
               AND (candidate.resource_id = ANY($2::uuid[])
                    OR candidate.wrapped_registrar_resource_id = ANY($2::uuid[]))",
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
        let only = |candidates: &mut dyn Iterator<Item = &Row>| {
            let names: BTreeSet<String> = candidates
                .filter_map(|candidate| text(candidate, "logical_name_id"))
                .collect();
            let count = names.len();
            match count {
                1 => Ok(names.into_iter().next().unwrap_or_default()),
                _ => Err(count),
            }
        };
        match only(&mut direct.into_iter()) {
            Ok(name) => Some(name),
            Err(0) => only(&mut wrapped.into_iter()).ok(),
            Err(_) => None,
        }
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
            row.insert(
                "decoded_logical_name_id".to_owned(),
                Value::String(name.clone()),
            );
            rows.put(table, row.clone())
                .map_err(in_family(table.name))?;
            super::identity::successor_grant(transaction, context, rows, &name, &row).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{Candidates, Row};

    fn row(value: serde_json::Value) -> Row {
        match value {
            serde_json::Value::Object(row) => row,
            _ => unreachable!(),
        }
    }

    fn grant() -> Row {
        row(json!({
            "event_kind": "RegistrationGranted", "source_family": "ens_v1_registrar_l1",
            "resource_id": "lease", "namehash": "0xab", "original_logical_name_id": null,
        }))
    }

    fn direct(name: &str) -> Row {
        row(json!({"logical_name_id": name, "resource_id": "lease", "surface_namehash": "0xAB"}))
    }

    #[test]
    fn a_pass_names_a_row_through_one_name_only() {
        let one = Candidates {
            rows: vec![direct("ens:0xab"), direct("ens:0xab")],
            arrived: Vec::new(),
        };
        assert_eq!(
            one.decode(&grant()).as_deref(),
            Some("ens:0xab"),
            "two candidates of one name agree"
        );

        // Candidates carrying the row's namehash can differ only in namespace; the pass then
        // names nothing, and the wrapper pass is not consulted.
        let wrapper = row(json!({
            "logical_name_id": "ens:0xab", "wrapped_registrar_resource_id": "lease",
            "node": "0xab",
        }));
        let two = Candidates {
            rows: vec![
                direct("ens:0xab"),
                direct("basenames:0xab"),
                wrapper.clone(),
            ],
            arrived: Vec::new(),
        };
        assert_eq!(two.decode(&grant()), None);

        let wrapped_only = Candidates {
            rows: vec![wrapper],
            arrived: Vec::new(),
        };
        assert_eq!(
            wrapped_only.decode(&grant()).as_deref(),
            Some("ens:0xab"),
            "with no direct candidate the wrapper pass names the row"
        );
    }
}
