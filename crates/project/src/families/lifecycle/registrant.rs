//! F13's unmasked registrant of a name, read from the name's retained F2a rows.
use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};

use crate::{
    ProjectError, Result,
    families::{
        input::Position,
        reduce::{Context, in_family, key_of, load_rows, set},
        store::{Row, RowSet},
        tables,
    },
};

/// The registrant a retained row reports to the served registrant block
/// (name_current/build.sql:440-491): a transfer's recipient, a release's prior registrant, a
/// grant's registrant.
fn reported_registrant(row: &Row) -> Option<String> {
    let column = match row.get("event_kind").and_then(Value::as_str)? {
        "TokenControlTransferred" => "to_address",
        "RegistrationReleased" => "before_registrant",
        "RegistrationGranted" => "registrant",
        _ => return None,
    };
    row.get(column)
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase)
}

/// F13's unmasked registrant of every name whose retained rows the block changed: the latest
/// retained row of the name, by position, that reports one. The served block admits and masks
/// those rows at read; the fold keeps the unmasked latest.
pub(super) async fn fold_registrants(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    rows: &mut RowSet,
) -> Result<()> {
    let table = &tables::LIFECYCLE_EVENT;
    let changed: Vec<(String, Option<Row>, Option<Row>)> = rows
        .changes()
        .into_iter()
        .filter(|change| change.table.name == table.name)
        .map(|change| {
            (
                change.key.to_owned(),
                change.before.cloned(),
                change.after.cloned(),
            )
        })
        .collect();
    let decoded = |row: &Option<Row>| {
        row.as_ref()
            .and_then(|row| row.get("decoded_logical_name_id"))
            .and_then(Value::as_str)
            .map(str::to_owned)
    };
    let mut names: Vec<String> = changed
        .iter()
        .flat_map(|(_, before, after)| [decoded(before), decoded(after)])
        .flatten()
        .collect();
    names.sort();
    names.dedup();
    if names.is_empty() {
        return Ok(());
    }
    let stored: Vec<Value> = sqlx::query_scalar(
        "/* project:families.lifecycle.name_registrants */ SELECT to_jsonb(event)
         FROM project_lifecycle_event event
         WHERE event.chain_id = $1 AND event.decoded_logical_name_id = ANY($2)
           AND event.event_kind IN (
               'RegistrationGranted', 'RegistrationReleased', 'TokenControlTransferred'
           )",
    )
    .bind(context.chain_id)
    .bind(&names)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to read names' retained rows", error))
    .map_err(in_family(tables::ADDRESS_NAME_FOLD.name))?;
    let mut current: std::collections::BTreeMap<String, Row> = stored
        .into_iter()
        .filter_map(|value| match value {
            Value::Object(row) => Some(row),
            _ => None,
        })
        .map(|row| (crate::families::store::key_text(table, &row), row))
        .collect();
    for (key, _, after) in changed {
        match after {
            Some(row) => current.insert(key, row),
            None => current.remove(&key),
        };
    }
    let fold = &tables::ADDRESS_NAME_FOLD;
    let chain = json!(context.chain_id);
    let keys = names
        .iter()
        .map(|name| key_of(fold, [chain.clone(), json!(name)]))
        .collect();
    load_rows(transaction, rows, fold, keys).await?;
    for name in names {
        let latest = current
            .values()
            .filter(|row| {
                row.get("decoded_logical_name_id").and_then(Value::as_str) == Some(name.as_str())
            })
            .filter_map(|row| Some((Position::of_row(row)?, reported_registrant(row)?)))
            .max_by(|left, right| left.0.cmp(&right.0));
        let key = key_of(fold, [chain.clone(), json!(name)]);
        let existing = rows.get(fold, &key).cloned();
        let Some((position, registrant)) = latest else {
            // No retained row reports a registrant any more; a fold row keeps no registrant.
            if let Some(mut row) = existing {
                set(&mut row, "registrant", Value::Null);
                set(&mut row, "registrant_position", Value::Null);
                rows.put(fold, row).map_err(in_family(fold.name))?;
            }
            continue;
        };
        let mut row = existing.unwrap_or_else(|| {
            let mut row = key.clone();
            for column in [
                "controller",
                "controller_action",
                "controller_subject",
                "controller_position",
                "token_holder",
                "token_holder_position",
            ] {
                set(&mut row, column, Value::Null);
            }
            position.write_columns(&mut row);
            set(&mut row, "normalized_event_id", Value::Null);
            row
        });
        set(&mut row, "registrant", registrant);
        set(&mut row, "registrant_position", position.to_json());
        rows.put(fold, row).map_err(in_family(fold.name))?;
    }
    Ok(())
}
