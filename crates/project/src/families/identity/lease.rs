//! The lease of a registry-only handoff after the binding: a later registrar grant of the name,
//! once the predecessor's lease was released, replaces it (stage.rs:47-135, the successor
//! lateral).
use serde_json::Value;
use sqlx::{Postgres, Transaction};

use super::text_of;
use crate::{
    ProjectError, Result,
    families::{
        input::Position,
        reduce::{Context, in_family, key_of, load_rows, namehash_of, set},
        store::{self, Row, RowSet},
        tables,
    },
};

/// A registrar grant that names `name` replaces the lease of the name's registry-only ENSv1
/// handoffs it follows (stage.rs:47-135, the successor lateral): a RegistrationGranted of
/// ens_v1_registrar_l1 with registrar authority, on a resource other than the handoff's
/// predecessor, carrying the name's namehash, positioned after the registry-only binding, with a
/// registrar release of the predecessor's resource before it. The latest such grant is the
/// lease. `grant` is the retained lifecycle row, named by its original or decoded name.
pub(crate) async fn successor_grant(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    rows: &mut RowSet,
    name: &str,
    grant: &Row,
) -> Result<()> {
    let text = |column: &str| grant.get(column).and_then(Value::as_str).map(str::to_owned);
    let (Some(resource), Some(position)) = (text("resource_id"), Position::of_row(grant)) else {
        return Ok(());
    };
    if text("source_family").as_deref() != Some("ens_v1_registrar_l1")
        || text("event_kind").as_deref() != Some("RegistrationGranted")
        || !text("authority_kind").is_none_or(|kind| kind.is_empty() || kind == "registrar")
        || text("namehash") != namehash_of(name)
    {
        return Ok(());
    }
    let table = &tables::BINDING_CANDIDATE;
    let stored: Vec<Value> = sqlx::query_scalar(
        "/* project:families.identity.registry_only_candidates */ SELECT to_jsonb(candidate)
         FROM project_binding_candidate candidate
         WHERE candidate.chain_id = $1 AND candidate.logical_name_id = $2
           AND candidate.registry_only AND candidate.authority_arm = 'ens_v1'",
    )
    .bind(context.chain_id)
    .bind(name)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to read registry-only candidates", error))
    .map_err(in_family(table.name))?;
    let stored = rows.overlay_where(
        table,
        store::objects(stored),
        ("logical_name_id", &[name]),
        |row| {
            row.get("registry_only").and_then(Value::as_bool) == Some(true)
                && store::column(row, "authority_arm") == Some("ens_v1")
        },
    );
    let mut keys: Vec<Row> = stored
        .iter()
        .map(|row| key_of(table, [row["surface_binding_id"].clone()]))
        .collect();
    keys.extend(
        rows.changes_in(table)
            .into_iter()
            .filter_map(|change| change.after)
            .filter(|row| row.get("logical_name_id").and_then(Value::as_str) == Some(name))
            .map(|row| key_of(table, [row["surface_binding_id"].clone()])),
    );
    load_rows(transaction, rows, table, keys.clone()).await?;
    // The handoffs this grant moves, as they stood before it: a candidate after the grant that
    // carries one of them over whole (`handoff`) took it from a binding the grant precedes.
    let mut moved: Vec<Value> = Vec::new();
    let mut later: Vec<Row> = Vec::new();
    for key in keys {
        let Some(mut row) = rows.get(table, &key).cloned() else {
            continue;
        };
        let predecessor = text_of(&row, "predecessor_resource_id");
        let registry_only = row.get("registry_only").and_then(Value::as_bool) == Some(true)
            && text_of(&row, "authority_arm") == "ens_v1"
            && !predecessor.is_empty()
            && predecessor != resource;
        if !registry_only {
            continue;
        }
        if Position::of_row(&row).is_none_or(|binding| binding >= position) {
            later.push(row);
            continue;
        }
        let current_successor = text_of(&row, "lease_resource_id") != predecessor;
        let later = row
            .get("lease_position")
            .and_then(Value::as_object)
            .and_then(Position::of_row)
            .is_none_or(|lease| lease < position);
        if (current_successor && !later)
            || !released_before(transaction, context, rows, &predecessor, &position).await?
        {
            continue;
        }
        moved.push(handoff_of(&row));
        set(&mut row, "lease_resource_id", resource.clone());
        set(&mut row, "lease_position", position.to_json());
        rows.put(table, row).map_err(in_family(table.name))?;
    }
    // A registry-only binding after the grant whose handoff is one the grant just moved copied
    // it from a binding before the grant, the grant not yet applied: identity builds a block's
    // candidates before lifecycle reaches its grants, so a grant earlier in the same block comes
    // after the copy. It stands for the same lease, so it moves the same way. Its handoff names
    // that earlier binding's predecessor and lease, which a binding it did not inherit from
    // cannot carry: that one's predecessor would be the binding in between.
    for mut row in later {
        if moved.contains(&handoff_of(&row)) {
            set(&mut row, "lease_resource_id", resource.clone());
            set(&mut row, "lease_position", position.to_json());
            rows.put(table, row).map_err(in_family(table.name))?;
        }
    }
    Ok(())
}

/// The columns `handoff` carries over from a registry-only predecessor that stands for a lease.
fn handoff_of(row: &Row) -> Value {
    Value::Array(
        [
            "predecessor_resource_id",
            "predecessor_position",
            "predecessor_wrapped_registrar_resource_id",
            "predecessor_node",
            "lease_resource_id",
            "lease_position",
        ]
        .into_iter()
        .map(|column| row.get(column).cloned().unwrap_or(Value::Null))
        .collect(),
    )
}

/// Re-read the name's retained registrar grants once an epoch turns an existing candidate
/// registry-only. The served handoff reads the epoch at any position (stage.rs:128-135), so a
/// grant retained before the epoch arrived can already be the lease; `successor_grant` skipped it
/// then, the candidate not being registry-only yet. The grants from `from` on are replayed in
/// canonical order, so the latest qualifying one is the lease. This block's own grants reach
/// `successor_grant` when the lifecycle family runs after identity. Every retained row with an
/// original name carries it as its decoded name too (`decode::Candidates::decode` returns the
/// original first), so the decoded name alone finds them.
pub(super) async fn retained_grants(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    rows: &mut RowSet,
    name: &str,
    from: i64,
) -> Result<()> {
    let stored: Vec<Value> = sqlx::query_scalar(
        "/* project:families.identity.retained_grants */ SELECT to_jsonb(retained)
         FROM project_lifecycle_event retained
         WHERE retained.chain_id = $1 AND retained.source_family = 'ens_v1_registrar_l1'
           AND retained.event_kind = 'RegistrationGranted'
           AND retained.decoded_logical_name_id = $2
           AND retained.block_number >= $3",
    )
    .bind(context.chain_id)
    .bind(name)
    .bind(from)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to read the name's retained grants", error))
    .map_err(in_family(tables::BINDING_CANDIDATE.name))?;
    // Grants an earlier block of a rebuild range retained are not in the table yet.
    let stored = rows.overlay_where(
        &tables::LIFECYCLE_EVENT,
        store::objects(stored),
        ("decoded_logical_name_id", &[name]),
        |row| {
            store::column(row, "source_family") == Some("ens_v1_registrar_l1")
                && store::column(row, "event_kind") == Some("RegistrationGranted")
                && row
                    .get("block_number")
                    .and_then(Value::as_i64)
                    .is_some_and(|block| block >= from)
        },
    );
    let mut grants: Vec<(Position, Row)> = stored
        .into_iter()
        .filter_map(|row| Some((Position::of_row(&row)?, row)))
        .collect();
    grants.sort_by(|left, right| left.0.cmp(&right.0));
    for (_, grant) in grants {
        successor_grant(transaction, context, rows, name, &grant).await?;
    }
    Ok(())
}

/// Whether a registrar RegistrationReleased of `resource` is retained before `position`,
/// counting this block's own retained rows.
async fn released_before(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    rows: &RowSet,
    resource: &str,
    position: &Position,
) -> Result<bool> {
    let is_release = |row: &Row| {
        row.get("state_kind").and_then(Value::as_str) == Some("resource")
            && row.get("state_key").and_then(Value::as_str) == Some(resource)
            && row.get("event_kind").and_then(Value::as_str) == Some("RegistrationReleased")
            && row.get("source_family").and_then(Value::as_str) == Some("ens_v1_registrar_l1")
            && Position::of_row(row).is_some_and(|release| &release < position)
    };
    if rows
        .changes_in(&tables::LIFECYCLE_EVENT)
        .into_iter()
        .filter_map(|change| change.after)
        .any(is_release)
    {
        return Ok(true);
    }
    let stored: Vec<Value> = sqlx::query_scalar(
        "/* project:families.identity.predecessor_releases */ SELECT to_jsonb(release)
         FROM project_lifecycle_event release
         WHERE release.chain_id = $1 AND release.state_kind = 'resource'
           AND release.state_key = $2 AND release.event_kind = 'RegistrationReleased'
           AND release.source_family = 'ens_v1_registrar_l1'
           AND release.block_number <= $3",
    )
    .bind(context.chain_id)
    .bind(resource)
    .bind(position.block_number)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to read predecessor releases", error))
    .map_err(in_family(tables::BINDING_CANDIDATE.name))?;
    let stored = rows.overlay_where(
        &tables::LIFECYCLE_EVENT,
        store::objects(stored),
        ("state_key", &[resource]),
        |row| {
            store::column(row, "state_kind") == Some("resource")
                && store::column(row, "event_kind") == Some("RegistrationReleased")
                && store::column(row, "source_family") == Some("ens_v1_registrar_l1")
                && row
                    .get("block_number")
                    .and_then(Value::as_i64)
                    .is_some_and(|block| block <= position.block_number)
        },
    );
    Ok(stored.iter().any(is_release))
}
