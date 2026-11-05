//! Test support for the address listing family (F18): the stored rows of a chain
//! (`project_address_listing`) against what `GET /v1/addresses/{address}/names` composes at
//! read for every address that could have one. The route's own pieces answer: the address-index
//! and role candidates (`address_names.rs`), the surface-less registry children
//! (`registry_children.rs`), the page statements with their read filter (`address_names::page`),
//! and the former-owner fold (`former_owners.rs`). Only the chain's candidates are read, so a
//! chain is checked whatever the other chains' publications are. It is compiled for tests only.
//!
//! A stored row equals the route's row in its relations, identity, resource, authority,
//! migration, keys and cited blocks. Its name is the chain-data spelling, so it equals the served
//! name except where a label the stored name leaves as `[labelhash]` is spelled from an imported
//! preimage. The registration lead is checked against the stored rows of its group, in the
//! stored order (`display_key`, then id, bytewise).
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use sqlx::{PgConnection, PgPool, Postgres, QueryBuilder};

use super::{
    address_names::{
        AddressComposer, address_name_candidates, compose_base_chunk, compose_candidate_rows,
    },
    former_owners::{lapsed_owner, served_expiry},
    registry_children::{load_chain_registry_children, registry_child_rows},
};
use crate::{
    AddressNameRelation, AddressNamesCurrentDedupe, AddressNamesCurrentOrder,
    AddressNamesCurrentSort,
    address_names::{
        RowSource, load_address_names_page_entries_from, push_created_at_timestamp_expr,
        push_expires_at_timestamp_expr, push_registered_at_timestamp_expr,
    },
    families::{
        name::{is_publication_unavailable, rendered::bracketed_label, servable_publication},
        topology::{DISPLAY_KEY_BYTES, DISPLAY_KEY_CHARS, MISSING_TIMESTAMP},
    },
};

/// Every address a stored row or a route row can name on the chain: the address index's, the
/// stored rows', and every subject of a live registry-scope grant.
const ADDRESSES: &str = "/* storage:families.records.address_listing_parity_addresses */
    SELECT address FROM bigname_phase.project_address_name_index WHERE chain_id = $1
    UNION SELECT address FROM bigname_phase.project_address_listing WHERE chain_id = $1
    UNION SELECT subject FROM bigname_phase.project_grant
          WHERE chain_id = $1 AND scope_kind = 'registry' AND NOT revoked
    ORDER BY 1";

const STORED: &str = "/* storage:families.records.address_listing_parity_stored */
    SELECT to_jsonb(listing) - 'chain_id' FROM bigname_phase.project_address_listing listing
    WHERE listing.chain_id = $1 ORDER BY listing.address, listing.logical_name_id";

/// The stored rows that still spell a label as `[labelhash]` although a chain-observed preimage
/// spells it: the family step heals a stored name at the block that observes its label.
const UNHEALED_BUT_KNOWN: &str = "/* storage:families.records.address_listing_parity_heal */
    SELECT listing.address || ' ' || listing.logical_name_id || ' ' || label.hash
    FROM bigname_phase.project_address_listing listing
    CROSS JOIN LATERAL unnest(listing.unhealed_labelhashes) AS label(hash)
    WHERE listing.chain_id = $1
      AND EXISTS (SELECT 1 FROM bigname_phase.label_preimages preimage
                  WHERE preimage.labelhash = label.hash
                    AND preimage.source_kind <> 'ens_rainbow_import'
                    AND preimage.decoded_label IS NOT NULL
                    AND preimage.normalized_under_version)
    ORDER BY 1";

/// The columns a route row fixes outright; the name columns and the lead are checked by rule.
const COMPARED: [&str; 20] = [
    "address",
    "logical_name_id",
    "namespace",
    "namehash",
    "resource_id",
    "token_holder",
    "effective_controller",
    "role_holder",
    "listed",
    "former_owner",
    "registry_child",
    "public_authority",
    "migrated",
    "expires_key",
    "registered_key",
    "created_key",
    "former_expires_key",
    "token_holder_block",
    "effective_controller_block",
    "role_holder_block",
];

const AUTHORITIES: [&str; 3] = ["ens_v0", "ens_v1", "ens_v2"];

/// Every page reads the whole list.
const ALL: u64 = 1_000_000;

/// Whether the chain's stored rows were compared: `false` when the readers refuse its
/// publication (`FamilyPublicationUnavailable`), when the route serves nothing to compare with.
pub async fn assert_address_listing_matches_the_route(
    pool: &PgPool,
    chain_id: &str,
) -> Result<bool> {
    let mut snapshot = crate::begin_read_snapshot(pool).await?;
    let publication = match servable_publication(&mut snapshot, chain_id).await {
        Ok(publication) => publication,
        Err(error) if is_publication_unavailable(&error) => return Ok(false),
        Err(error) => return Err(error),
    };
    let addresses: Vec<String> = sqlx::query_scalar(ADDRESSES)
        .bind(chain_id)
        .fetch_all(&mut *snapshot)
        .await?;
    let mut stored: BTreeMap<(String, String), Value> = BTreeMap::new();
    for row in sqlx::query_scalar::<_, Value>(STORED)
        .bind(chain_id)
        .fetch_all(&mut *snapshot)
        .await?
    {
        stored.insert((text(&row, "address"), text(&row, "logical_name_id")), row);
    }
    let mut expected: BTreeMap<(String, String), Expected> = BTreeMap::new();
    for address in &addresses {
        let rows = expected_rows(&mut snapshot, chain_id, publication.block_number, address);
        for (name, row) in rows.await? {
            expected.insert((address.clone(), name), row);
        }
    }
    let unhealed: Vec<String> = sqlx::query_scalar(UNHEALED_BUT_KNOWN)
        .bind(chain_id)
        .fetch_all(&mut *snapshot)
        .await?;
    snapshot.rollback().await?;
    ensure!(
        unhealed.is_empty(),
        "{chain_id} stores address listing names that leave a known label unhealed: {unhealed:?}"
    );

    let stored_keys: BTreeSet<_> = stored.keys().collect();
    let expected_keys: BTreeSet<_> = expected.keys().collect();
    ensure!(
        stored_keys == expected_keys,
        "{chain_id} stores address listing rows for {:?}, the route lists {:?}",
        stored_keys.difference(&expected_keys).collect::<Vec<_>>(),
        expected_keys.difference(&stored_keys).collect::<Vec<_>>(),
    );
    for (key, want) in &expected {
        let row = &stored[key];
        let have: serde_json::Map<String, Value> = COMPARED
            .iter()
            .map(|column| ((*column).to_owned(), row[*column].clone()))
            .collect();
        ensure!(
            Value::Object(have.clone()) == want.columns,
            "{chain_id} stores the address listing row {key:?} as {}, the route composes {}",
            Value::Object(have),
            want.columns
        );
        check_spelling(chain_id, key, row, want)?;
    }
    check_leads(chain_id, &stored)?;
    Ok(true)
}

/// One route row: the compared columns, and the served name and normalized name.
struct Expected {
    columns: Value,
    display_name: String,
    normalized_name: String,
}

#[derive(Default)]
struct Related {
    token_holder: bool,
    effective_controller: bool,
    role_holder: bool,
    registry_child: bool,
    namespace: String,
    namehash: String,
    resource_id: Option<String>,
    display_name: String,
    normalized_name: String,
    former: Option<Option<String>>,
}

/// The rows `address` lists on `chain_id`, published at `published`, by name, as the route
/// composes them.
async fn expected_rows(
    conn: &mut PgConnection,
    chain_id: &str,
    published: i64,
    address: &str,
) -> Result<BTreeMap<String, Expected>> {
    let mut by_chain = address_name_candidates(conn, address, None, true).await?;
    by_chain.retain(|chain, _| chain == chain_id);
    let composer = AddressComposer {
        address,
        include_roles: true,
        with_history_evidence: true,
    };
    let (mut rows, names) = compose_candidate_rows(conn, &composer, &by_chain).await?;
    if let Some((publication, children)) =
        load_chain_registry_children(conn, chain_id, address, None).await?
        && let Value::Array(rows) = &mut rows
    {
        for child in &children {
            rows.extend(registry_child_rows(address, &publication, child));
        }
    }
    let source = RowSource::Composed {
        rows: &rows,
        names: &names,
        parent: None,
    };
    let mut related: BTreeMap<String, Related> = BTreeMap::new();
    for entry in load_address_names_page_entries_from(
        conn,
        source,
        address,
        None,
        None,
        AddressNamesCurrentDedupe::Surface,
        None,
        None,
        None,
        AddressNamesCurrentSort::Name,
        AddressNamesCurrentOrder::Asc,
        None,
        ALL,
    )
    .await?
    .0
    {
        let row = related.entry(entry.logical_name_id.clone()).or_default();
        for relation in &entry.relations {
            match relation {
                AddressNameRelation::TokenHolder => row.token_holder = true,
                AddressNameRelation::EffectiveController => row.effective_controller = true,
                AddressNameRelation::RoleHolder => row.role_holder = true,
            }
        }
        row.registry_child = entry.registry_child;
        row.namespace = entry.namespace;
        row.namehash = entry.namehash;
        row.resource_id = Some(entry.resource_id.to_string());
        row.display_name = entry.canonical_display_name;
        row.normalized_name = entry.normalized_name;
    }
    let listed: BTreeSet<String> = related.keys().cloned().collect();
    let migrated = page_ids(conn, source, address, None, Some(true)).await?;
    let mut authorities: BTreeMap<String, &str> = BTreeMap::new();
    for authority in AUTHORITIES {
        for id in page_ids(conn, source, address, Some(&[authority]), None).await? {
            authorities.insert(id, authority);
        }
    }
    let keys = name_keys(conn, &names).await?;
    let blocks = relation_blocks(&rows, published);

    // The former-owner read: the address's indexed names on the chain whose lapsed owner it is.
    let indexed: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT logical_name_id FROM bigname_phase.project_address_name_index
         WHERE address = $1 AND chain_id = $2 ORDER BY 1",
    )
    .bind(address)
    .bind(chain_id)
    .fetch_all(&mut *conn)
    .await?;
    for chunk in indexed.chunks(super::seams::compose_chunk()) {
        for row in compose_base_chunk(conn, chunk).await?.values() {
            if lapsed_owner(row).as_deref() != Some(address) {
                continue;
            }
            let entry = related.entry(row.logical_name_id.clone()).or_default();
            entry.former = Some(served_expiry(row).map(|expiry| expiry.to_string()));
            if !listed.contains(&row.logical_name_id) {
                entry.namespace = row.namespace.clone();
                entry.namehash = row.namehash.clone();
                entry.resource_id = super::address_names::relation_resource(row);
                entry.display_name = row.canonical_display_name.clone();
                entry.normalized_name = row.normalized_name.clone();
            }
        }
    }
    // A former-only row's keys and authority are the composed name's, which no relation page
    // composes: the former-owner composition gives them.
    let former_only: Vec<String> = related
        .iter()
        .filter(|(id, _)| !listed.contains(*id))
        .map(|(id, _)| id.clone())
        .collect();
    let former_keys = former_only_keys(conn, &former_only).await?;

    let mut out = BTreeMap::new();
    for (id, row) in related {
        let is_listed = listed.contains(&id);
        let key = |column: &str| -> Value {
            keys.get(&id)
                .or_else(|| former_keys.get(&id).map(|(keys, _, _)| keys))
                .and_then(|keys| keys.get(column).cloned())
                .unwrap_or_else(|| json!(MISSING_TIMESTAMP))
        };
        let block = |relation: &str, held: bool| -> Value {
            if !held {
                return Value::Null;
            }
            blocks
                .get(&(id.clone(), relation.to_owned()))
                .copied()
                .flatten()
                .map_or(Value::Null, |block| json!(block))
        };
        let (authority, is_migrated) = if is_listed {
            (
                authorities.get(&id).map(|authority| json!(authority)),
                migrated.contains(&id),
            )
        } else {
            former_keys
                .get(&id)
                .map(|(_, authority, migrated)| (authority.clone().map(Value::String), *migrated))
                .unwrap_or((None, false))
        };
        let former_expires_key = match &row.former {
            Some(Some(expiry)) => serde_json::from_str::<Value>(expiry)?,
            _ => json!(MISSING_TIMESTAMP),
        };
        let columns = json!({
            "address": address,
            "logical_name_id": id,
            "namespace": row.namespace,
            "namehash": row.namehash,
            "resource_id": row.resource_id,
            "token_holder": row.token_holder,
            "effective_controller": row.effective_controller,
            "role_holder": row.role_holder,
            "listed": is_listed,
            "former_owner": row.former.is_some(),
            "registry_child": row.registry_child,
            "public_authority": authority.unwrap_or(Value::Null),
            "migrated": is_migrated,
            "expires_key": key("expires_key"),
            "registered_key": key("registered_key"),
            "created_key": key("created_key"),
            "former_expires_key": former_expires_key,
            "token_holder_block": block("token_holder", row.token_holder),
            "effective_controller_block": block("effective_controller", row.effective_controller),
            "role_holder_block": block("role_holder", row.role_holder),
        });
        out.insert(
            id,
            Expected {
                columns,
                display_name: row.display_name,
                normalized_name: row.normalized_name,
            },
        );
    }
    Ok(out)
}

/// The ids a surface-deduped page lists under `authority` or `is_migrated`.
async fn page_ids(
    conn: &mut PgConnection,
    source: RowSource<'_>,
    address: &str,
    authority: Option<&[&str]>,
    is_migrated: Option<bool>,
) -> Result<BTreeSet<String>> {
    Ok(load_address_names_page_entries_from(
        conn,
        source,
        address,
        None,
        None,
        AddressNamesCurrentDedupe::Surface,
        None,
        authority,
        is_migrated,
        AddressNamesCurrentSort::Name,
        AddressNamesCurrentOrder::Asc,
        None,
        ALL,
    )
    .await?
    .0
    .into_iter()
    .map(|entry| entry.logical_name_id)
    .collect())
}

/// Each composed name's three timestamp keys, by the page statements' own expressions, a missing
/// one as [`MISSING_TIMESTAMP`].
async fn name_keys(
    conn: &mut PgConnection,
    names: &Value,
) -> Result<BTreeMap<String, BTreeMap<String, Value>>> {
    let mut builder = QueryBuilder::<Postgres>::new(
        "SELECT nc.logical_name_id, jsonb_build_object('expires_key', trim_scale(COALESCE(",
    );
    push_expires_at_timestamp_expr(&mut builder);
    builder.push(format!(
        ", {MISSING_TIMESTAMP})), 'registered_key', trim_scale(COALESCE(EXTRACT(EPOCH FROM "
    ));
    push_registered_at_timestamp_expr(&mut builder);
    builder.push(format!(
        "), {MISSING_TIMESTAMP})), 'created_key', trim_scale(COALESCE(EXTRACT(EPOCH FROM "
    ));
    push_created_at_timestamp_expr(&mut builder);
    builder.push(format!(
        "), {MISSING_TIMESTAMP}))) FROM jsonb_to_recordset("
    ));
    builder.push_bind(names);
    builder.push(") AS nc(logical_name_id text, declared_summary jsonb, provenance jsonb)");
    let rows: Vec<(String, Value)> = builder
        .build_query_as()
        .fetch_all(&mut *conn)
        .await
        .context("failed to read the composed names' keys")?;
    Ok(rows
        .into_iter()
        .map(|(id, keys)| {
            let keys = keys
                .as_object()
                .map(|keys| keys.clone().into_iter().collect())
                .unwrap_or_default();
            (id, keys)
        })
        .collect())
}

/// A former-only row's keys, authority and migration, from the former-owner composition of its
/// name: its keys by the page expressions, its authority and migration by the page filters over
/// a one-name row set.
async fn former_only_keys(
    conn: &mut PgConnection,
    ids: &[String],
) -> Result<BTreeMap<String, (BTreeMap<String, Value>, Option<String>, bool)>> {
    let mut out = BTreeMap::new();
    for chunk in ids.chunks(super::seams::compose_chunk()) {
        for row in compose_base_chunk(conn, chunk).await?.values() {
            let name = super::address_names::name_row(row);
            let names = Value::Array(vec![name]);
            let keys = name_keys(conn, &names)
                .await?
                .remove(&row.logical_name_id)
                .unwrap_or_default();
            let authority =
                crate::families::name::public_authority(&row.provenance).map(str::to_owned);
            let mut migration =
                QueryBuilder::<Postgres>::new("SELECT EXISTS (SELECT 1 FROM jsonb_to_recordset(");
            migration.push_bind(&names);
            migration.push(
                ") AS migration_nc(logical_name_id text, declared_summary jsonb, provenance jsonb)",
            );
            crate::address_names::push_migration_proof(
                &mut migration,
                "migration_nc",
                "migration_nc.logical_name_id",
            );
            migration.push(")");
            let migrated: bool = migration.build_query_scalar().fetch_one(&mut *conn).await?;
            out.insert(row.logical_name_id.clone(), (keys, authority, migrated));
        }
    }
    Ok(out)
}

/// The cited block of each (name, relation) the composed rows carry
/// (`with_history_evidence`), none when the relation cites no event at or below `published`.
fn relation_blocks(rows: &Value, published: i64) -> BTreeMap<(String, String), Option<i64>> {
    let mut out = BTreeMap::new();
    for row in rows.as_array().into_iter().flatten() {
        let block = if row["registry_child"] == Value::Bool(true) {
            None
        } else {
            row.pointer("/chain_positions/block_number")
                .and_then(Value::as_i64)
                .filter(|block| *block <= published)
        };
        out.insert((text(row, "logical_name_id"), text(row, "relation")), block);
    }
    out
}

/// The stored name columns against the served name: label by label equal, except a label the
/// stored name leaves as `[labelhash]`, which an imported preimage may spell at read; the
/// unhealed labels are exactly those; the key, parent and normalized name follow from it.
fn check_spelling(
    chain_id: &str,
    key: &(String, String),
    row: &Value,
    want: &Expected,
) -> Result<()> {
    let display = text(row, "display_name");
    let normalized = text(row, "normalized_name");
    let unhealed: Vec<String> = row["unhealed_labelhashes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect();
    let bracketed: Vec<String> = display
        .split('.')
        .filter_map(bracketed_label)
        .map(|hash| format!("0x{}", hash.to_ascii_lowercase()))
        .collect();
    ensure!(
        bracketed == unhealed,
        "{chain_id} {key:?}: the stored name {display} leaves {bracketed:?} unhealed, the row \
         stores {unhealed:?}"
    );
    for (stored, served) in [
        (&display, &want.display_name),
        (&normalized, &want.normalized_name),
    ] {
        ensure!(
            spelled_like(stored, served),
            "{chain_id} {key:?}: the stored name {stored} is not the served name {served} under \
             chain-observed preimages"
        );
    }
    let display_key = if display.len() <= DISPLAY_KEY_BYTES as usize {
        display.clone()
    } else {
        display.chars().take(DISPLAY_KEY_CHARS as usize).collect()
    };
    ensure!(
        text(row, "display_key") == display_key,
        "{chain_id} {key:?}: the display key is not cut from {display}"
    );
    let parent = normalized
        .find('.')
        .filter(|position| *position > 0)
        .map(|position| normalized[position + 1..].to_owned());
    ensure!(
        row["parent_normalized_name"].as_str().map(str::to_owned) == parent,
        "{chain_id} {key:?}: the parent is not cut from {normalized}"
    );
    Ok(())
}

/// Whether `stored` is `served` with some labels left as `[labelhash]`.
fn spelled_like(stored: &str, served: &str) -> bool {
    let (stored, served): (Vec<&str>, Vec<&str>) =
        (stored.split('.').collect(), served.split('.').collect());
    stored.len() == served.len()
        && stored.iter().zip(&served).all(|(stored, served)| {
            stored == served
                || (bracketed_label(stored).is_some() && bracketed_label(served).is_none())
        })
}

/// In each (address, resource) group of listed rows, the smallest (`display_key`, id) row,
/// bytewise, is the lead, and no other row is.
fn check_leads(chain_id: &str, stored: &BTreeMap<(String, String), Value>) -> Result<()> {
    let mut groups: BTreeMap<(String, String), Vec<(String, String)>> = BTreeMap::new();
    for ((address, id), row) in stored {
        if row["listed"] == Value::Bool(true)
            && let Some(resource) = row["resource_id"].as_str()
        {
            groups
                .entry((address.clone(), resource.to_owned()))
                .or_default()
                .push((text(row, "display_key"), id.clone()));
        }
    }
    let leads: BTreeSet<(String, String)> = groups
        .into_iter()
        .filter_map(|((address, _), members)| {
            members
                .into_iter()
                .min_by(|left, right| {
                    (left.0.as_bytes(), left.1.as_bytes())
                        .cmp(&(right.0.as_bytes(), right.1.as_bytes()))
                })
                .map(|(_, id)| (address, id))
        })
        .collect();
    for (key, row) in stored {
        let lead = row["registration_lead"] == Value::Bool(true);
        let expected = leads.contains(key);
        ensure!(
            lead == expected,
            "{chain_id} {key:?}: registration_lead is {lead}, the group's smallest listed row \
             makes it {expected}"
        );
    }
    Ok(())
}

fn text(row: &Value, column: &str) -> String {
    row[column].as_str().unwrap_or_default().to_owned()
}
