//! F8 and F9, grants and account approvals. A grant row keeps the latest PermissionChanged or
//! RootPermissionChanged of its (resource, subject, scope), revocations included, with the
//! unmasked powers the event carried (permissions.rs, `decoded` and `latest`); wrapper masks,
//! grace, expiry retirement and operator expansion apply at read. The resource's admin aggregate
//! keeps, per holder with a registry or root scope, the admin powers that holder's latest grant
//! carries (resource_summary.rs, `v2_admin_powers`); the read takes their union. An approval row
//! keeps the latest AccountPermissionChanged of its six-part key, an explicit `false` included
//! (account_permissions.rs).
use serde_json::{Map, Value, json};
use sqlx::{Postgres, Transaction};

use super::reduce::in_family;
use super::{
    input::BlockEvent,
    keys,
    reduce::{
        Context, current, json_boolean, key_of, load_rows, put, raw_lower, raw_text, set,
        text_or_null,
    },
    store::RowSet,
    tables,
};
use crate::Result;

/// A decoded grant: its key parts and the event.
struct Grant<'a> {
    resource: &'a str,
    subject: String,
    scope: String,
    event: &'a BlockEvent,
}

fn grant(event: &BlockEvent) -> Option<Grant<'_>> {
    if !matches!(
        event.event_kind.as_str(),
        "PermissionChanged" | "RootPermissionChanged"
    ) {
        return None;
    }
    let after = &event.after;
    let subject = raw_lower(after, "subject").filter(|subject| !subject.trim().is_empty())?;
    after
        .get("effective_powers")
        .filter(|powers| powers.is_array())?;
    let scope = keys::grant_scope(after).filter(|scope| !scope.trim().is_empty())?;
    Some(Grant {
        resource: event.resource_id.as_deref()?,
        subject,
        scope,
        event,
    })
}

fn scope_kind(scope: &Value) -> Value {
    match scope.get("kind").and_then(Value::as_str) {
        Some("root" | "registry_root") => json!("root"),
        Some(kind @ ("registry" | "resource" | "resolver" | "record_manager")) => json!(kind),
        _ => Value::Null,
    }
}

/// The grant's scope, with the record-id selector when its hash is the upstream resource.
fn scope_detail(after: &Value) -> Value {
    let scope = after.get("scope").cloned().unwrap_or(Value::Null);
    let selector = after.get("selector");
    let selected = selector.is_some_and(|selector| {
        selector.get("hash").is_some_and(Value::is_string)
            && raw_text(selector, "hash") == raw_text(after, "upstream_resource")
            && matches!(
                selector.get("kind").and_then(Value::as_str),
                Some("address" | "text" | "abi" | "interface" | "data" | "argument")
            )
    });
    match (scope, selected) {
        (Value::Object(mut scope), true) => {
            scope.insert(
                "resource_selector".into(),
                selector.cloned().unwrap_or(Value::Null),
            );
            Value::Object(scope)
        }
        (scope, _) => scope,
    }
}

fn typed(after: &Value, field: &str, check: fn(&Value) -> bool, fallback: Value) -> Value {
    after
        .get(field)
        .filter(|value| check(value))
        .cloned()
        .unwrap_or(fallback)
}

pub(super) async fn apply(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    events: &[BlockEvent],
    rows: &mut RowSet,
) -> Result<()> {
    let chain = json!(context.chain_id);
    let grants: Vec<Grant<'_>> = events.iter().filter_map(grant).collect();
    let grant_keys = grants
        .iter()
        .map(|grant| {
            key_of(
                &tables::GRANT,
                [
                    chain.clone(),
                    json!(grant.resource),
                    json!(grant.subject),
                    json!(grant.scope),
                ],
            )
        })
        .collect();
    let aggregate_keys = grants
        .iter()
        .map(|grant| {
            key_of(
                &tables::RESOURCE_ADMIN_AGGREGATE,
                [chain.clone(), json!(grant.resource)],
            )
        })
        .collect();
    load_rows(transaction, rows, &tables::GRANT, grant_keys).await?;
    load_rows(
        transaction,
        rows,
        &tables::RESOURCE_ADMIN_AGGREGATE,
        aggregate_keys,
    )
    .await?;
    for grant in &grants {
        apply_grant(rows, &chain, grant)?;
    }

    let approvals: Vec<(Vec<Value>, &BlockEvent)> = events
        .iter()
        .filter_map(|event| approval_key(event).map(|key| (key, event)))
        .collect();
    let approval_keys = approvals
        .iter()
        .map(|(key, _)| {
            key_of(
                &tables::ACCOUNT_APPROVAL,
                std::iter::once(chain.clone()).chain(key.clone()),
            )
        })
        .collect();
    load_rows(transaction, rows, &tables::ACCOUNT_APPROVAL, approval_keys).await?;
    for (key, event) in approvals {
        let table = &tables::ACCOUNT_APPROVAL;
        // The flag as the served `(after_state ->> 'approved')::boolean` reads it. A flag that
        // reads as no boolean (a spelling PostgreSQL rejects, or null) fails every served batch:
        // the cast aborts it, or the NOT NULL column refuses the row. That input cannot coexist
        // with a served batch, so the family keeps nothing for the event rather than guess.
        let Some(flag) = event.after.get("approved").and_then(json_boolean) else {
            continue;
        };
        let mut row = current(
            rows,
            table,
            &key_of(table, std::iter::once(chain.clone()).chain(key)),
        );
        let after = &event.after;
        let scope = after.get("scope").cloned().unwrap_or(Value::Null);
        set(
            &mut row,
            "authority_contract_instance_id",
            text_or_null(raw_text(&scope, "authority_contract_instance_id")),
        );
        set(&mut row, "approved", flag);
        set(
            &mut row,
            "effective_powers",
            after
                .get("effective_powers")
                .cloned()
                .unwrap_or(Value::Null),
        );
        set(
            &mut row,
            "grant_source",
            after.get("grant_source").cloned().unwrap_or(Value::Null),
        );
        set(
            &mut row,
            "revocation_source",
            after
                .get("revocation_source")
                .cloned()
                .unwrap_or(Value::Null),
        );
        set(
            &mut row,
            "inheritance_path",
            after
                .get("inheritance_path")
                .cloned()
                .unwrap_or(Value::Null),
        );
        set(
            &mut row,
            "transfer_behavior",
            after
                .get("transfer_behavior")
                .cloned()
                .unwrap_or(Value::Null),
        );
        put(rows, table, row, event)?;
    }
    Ok(())
}

/// The approval key an AccountPermissionChanged the builder admits addresses.
fn approval_key(event: &BlockEvent) -> Option<Vec<Value>> {
    if event.event_kind != "AccountPermissionChanged" {
        return None;
    }
    let after = &event.after;
    let scope = after.get("scope")?;
    let kind =
        raw_text(scope, "authority_kind").filter(|kind| kind == "registry" || kind == "wrapper")?;
    Some(vec![
        json!(kind),
        json!(raw_lower(scope, "authority_contract")?),
        json!(raw_lower(scope, "owner")?),
        json!(raw_lower(after, "subject")?),
        json!(raw_text(after, "relation_kind")?),
    ])
}

fn apply_grant(rows: &mut RowSet, chain: &Value, grant: &Grant<'_>) -> Result<()> {
    let event = grant.event;
    let after = &event.after;
    let table = &tables::GRANT;
    let mut row = current(
        rows,
        table,
        &key_of(
            table,
            [
                chain.clone(),
                json!(grant.resource),
                json!(grant.subject),
                json!(grant.scope),
            ],
        ),
    );
    let scope = after.get("scope").cloned().unwrap_or(Value::Null);
    let kind = scope_kind(&scope);
    let powers = after
        .get("effective_powers")
        .cloned()
        .unwrap_or(Value::Null);
    set(&mut row, "event_kind", event.event_kind.clone());
    set(&mut row, "scope_kind", kind.clone());
    set(&mut row, "scope_detail", scope_detail(after));
    set(&mut row, "effective_powers", powers.clone());
    set(
        &mut row,
        "grant_source",
        typed(after, "grant_source", Value::is_object, json!({})),
    );
    set(
        &mut row,
        "revocation_source",
        typed(after, "revocation_source", Value::is_object, Value::Null),
    );
    set(
        &mut row,
        "inheritance_path",
        typed(after, "inheritance_path", Value::is_array, json!([])),
    );
    set(
        &mut row,
        "transfer_behavior",
        typed(
            after,
            "transfer_behavior",
            Value::is_object,
            json!({"mode": after.get("transfer_behavior").cloned().unwrap_or(Value::Null)}),
        ),
    );
    // Revoked is a clear: an empty effective-power array (`grant` admits arrays only), the rows
    // the served current read drops (permissions.rs, `jsonb_array_length(masked.effective_powers)
    // > 0`). The revocation source above is provenance only.
    set(
        &mut row,
        "revoked",
        powers.as_array().is_some_and(Vec::is_empty),
    );
    row.entry("registration_position").or_insert(Value::Null);
    put(rows, table, row, event)?;

    // The holder's admin powers under a registry or root scope, kept per holder.
    let admin: Vec<Value> = if matches!(kind.as_str(), Some("registry" | "root")) {
        let mut admin: Vec<&str> = powers
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .filter(|power| power.starts_with("admin_") || *power == "can_transfer_admin")
            .collect();
        admin.sort_unstable();
        admin.dedup();
        admin.into_iter().map(|power| json!(power)).collect()
    } else {
        Vec::new()
    };
    let table = &tables::RESOURCE_ADMIN_AGGREGATE;
    let key = key_of(table, [chain.clone(), json!(grant.resource)]);
    let existing = rows.get(table, &key).cloned();
    let holder = format!("{}|{}", grant.subject, grant.scope);
    let mut holders: Map<String, Value> = existing
        .as_ref()
        .and_then(|row| row.get("admin_powers"))
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let before = holders.clone();
    if admin.is_empty() {
        holders.remove(&holder);
    } else {
        holders.insert(holder, Value::Array(admin));
    }
    if holders == before {
        return Ok(());
    }
    if holders.is_empty() {
        return rows.delete(table, &key).map_err(in_family(table.name));
    }
    let mut row = existing.unwrap_or(key);
    set(&mut row, "admin_powers", Value::Object(holders));
    put(rows, table, row, event)
}
