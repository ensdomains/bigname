//! A page's entries from its stored rows: the served name, the relations in rank order, the
//! publication stamps, and for a surface-less registry child the owner, manager and authority its
//! parent's subnames route serves, read again for the page's children only.
use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, Result};
use bigname_domain::normalization::normalize_name;
use serde_json::json;
use sqlx::{PgConnection, Postgres, QueryBuilder};
use uuid::Uuid;

use super::statement::{Read, Row, fence};
use crate::{
    AddressNameCurrentEntry, AddressNameRelation, AddressNamesCurrentDedupe,
    families::{
        name::{
            FamilyPublication,
            rendered::{bracketed_label, composed_surface_sql, rendered_name_sql},
        },
        topology::{RegistryChildRow, load_owned_registry_children},
    },
};

const RANKED: [AddressNameRelation; 3] = [
    AddressNameRelation::TokenHolder,
    AddressNameRelation::EffectiveController,
    AddressNameRelation::RoleHolder,
];

fn flags(row: &Row) -> [bool; 3] {
    [row.token_holder, row.effective_controller, row.role_holder]
}

fn rank(row: &Row) -> usize {
    flags(row)
        .iter()
        .position(|held| *held)
        .unwrap_or(RANKED.len())
}

/// The row an entry of `group` (rows of one name) takes its fields from: the one holding the
/// highest-ranked relation, the first of them in page order.
pub(super) fn representative(group: &[Row]) -> &Row {
    group
        .iter()
        .min_by_key(|row| rank(row))
        .expect("an entry has a row")
}

/// The entries of `groups`, in page order.
pub(super) async fn entries(
    conn: &mut PgConnection,
    read: &Read<'_>,
    publications: &BTreeMap<String, FamilyPublication>,
    groups: &[Vec<Row>],
) -> Result<Vec<AddressNameCurrentEntry>> {
    let registration = read.request.dedupe_by == AddressNamesCurrentDedupe::Resource;
    let merged = if registration {
        registration_relations(conn, read, groups).await?
    } else {
        HashMap::new()
    };
    let children = registry_children(conn, read, publications, groups).await?;
    let mut entries = Vec::with_capacity(groups.len());
    for group in groups {
        let row = representative(group);
        let held = if registration {
            merged
                .get(&(row.chain_id.clone(), row.resource_id))
                .copied()
                .unwrap_or_else(|| flags(row))
        } else {
            group.iter().fold([false; 3], |held, row| {
                let row = flags(row);
                [held[0] || row[0], held[1] || row[1], held[2] || row[2]]
            })
        };
        let relations = RANKED
            .into_iter()
            .zip(held.into_iter().zip(read.wanted))
            .filter(|(_, (held, wanted))| *held && *wanted)
            .map(|(relation, _)| relation)
            .collect();
        let publication = publications
            .get(&row.chain_id)
            .with_context(|| format!("no publication is held for {}", row.chain_id))?;
        let mut entry = entry(read.address, publication, row, relations);
        if row.registry_child {
            // A child the subnames relation no longer lists for the address in this snapshot has
            // no served owner to show; it leaves the page.
            let Some(child) = children.get(&row.logical_name_id) else {
                continue;
            };
            served_child(&mut entry, child);
        }
        entries.push(entry);
    }
    enrich(conn, &mut entries).await?;
    Ok(entries)
}

fn entry(
    address: &str,
    publication: &FamilyPublication,
    row: &Row,
    relations: Vec<AddressNameRelation>,
) -> AddressNameCurrentEntry {
    AddressNameCurrentEntry {
        address: address.to_owned(),
        logical_name_id: row.logical_name_id.clone(),
        namespace: row.namespace.clone(),
        canonical_display_name: row.display_name.clone(),
        normalized_name: row.normalized_name.clone(),
        namehash: row.namehash.clone(),
        surface_binding_id: None,
        resource_id: row.resource_id,
        token_lineage_id: None,
        binding_kind: None,
        relations,
        provenance: json!({
            "chain_id": publication.chain_id,
            "coverage": {"status": "projected", "exhaustiveness": "not_asserted"},
        }),
        coverage: json!({"status": "projected", "exhaustiveness": "not_asserted"}),
        chain_positions: json!({
            "target_block_number": publication.block_number,
            "target_block_hash": publication.block_hash,
        }),
        canonicality_summary: json!({
            "state": "canonical_lineage",
            "target_block_number": publication.block_number,
            "target_block_hash": publication.block_hash,
        }),
        manifest_version: 1,
        last_recomputed_at: publication.block_timestamp,
        served_owner: None,
        served_manager: None,
        served_authority: None,
        served_lifecycle_shadow: false,
        registry_child: row.registry_child,
    }
}

/// The served fields of a registry child, as `families::records::registry_children` serves
/// them: the holder of its `.eth` lease when the registrar retains one, else its registry owner,
/// as owner, and its registry owner as manager.
fn served_child(entry: &mut AddressNameCurrentEntry, child: &RegistryChildRow) {
    entry.canonical_display_name.clone_from(&child.display_name);
    entry.normalized_name.clone_from(&child.display_name);
    entry.served_owner = Some(
        child
            .token_holder
            .clone()
            .unwrap_or_else(|| child.owner.clone()),
    );
    entry.served_manager = Some(child.owner.clone());
    entry.served_authority.clone_from(&child.authority);
    entry.served_lifecycle_shadow = child.lifecycle_shadow;
}

/// The relations of every row the filters keep on each entry's resource, under registration
/// grouping, where an entry stands for all of them.
async fn registration_relations(
    conn: &mut PgConnection,
    read: &Read<'_>,
    groups: &[Vec<Row>],
) -> Result<HashMap<(String, Uuid), [bool; 3]>> {
    if groups.is_empty() {
        return Ok(HashMap::new());
    }
    let (chains, resources): (Vec<String>, Vec<Uuid>) = groups
        .iter()
        .map(|group| (group[0].chain_id.clone(), group[0].resource_id))
        .unzip();
    let mut builder = QueryBuilder::<Postgres>::new(
        "/* storage:address_names.listing_registration_relations */
         SELECT l.chain_id, l.resource_id, bool_or(l.token_holder),
                bool_or(l.effective_controller), bool_or(l.role_holder)
         FROM bigname_phase.project_address_listing l
         JOIN unnest(",
    );
    builder.push_bind(chains);
    builder.push("::text[], ");
    builder.push_bind(resources);
    builder.push(
        "::uuid[]) AS page(chain_id, resource_id)
           ON l.chain_id = page.chain_id AND l.resource_id = page.resource_id
         WHERE",
    );
    read.push_filters(&mut builder, "l");
    builder.push(fence());
    builder.push(" GROUP BY l.chain_id, l.resource_id");
    let rows: Vec<(String, Uuid, bool, bool, bool)> = builder
        .build_query_as()
        .fetch_all(&mut *conn)
        .await
        .with_context(|| {
            format!(
                "failed to read the registrations listed for {}",
                read.address
            )
        })?;
    Ok(rows
        .into_iter()
        .map(|(chain, resource, token, controller, role)| {
            ((chain, resource), [token, controller, role])
        })
        .collect())
}

/// The page's surface-less registry children as the subnames relation lists them now, by id.
async fn registry_children(
    conn: &mut PgConnection,
    read: &Read<'_>,
    publications: &BTreeMap<String, FamilyPublication>,
    groups: &[Vec<Row>],
) -> Result<HashMap<String, RegistryChildRow>> {
    let mut by_chain: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for row in groups.iter().flatten().filter(|row| row.registry_child) {
        by_chain
            .entry(row.chain_id.as_str())
            .or_default()
            .push(row.logical_name_id.clone());
    }
    let mut children = HashMap::new();
    for (chain_id, ids) in by_chain {
        let publication = publications
            .get(chain_id)
            .with_context(|| format!("no publication is held for {chain_id}"))?;
        let rows = load_owned_registry_children(
            conn,
            chain_id,
            read.address,
            &ids,
            publication.block_number,
        )
        .await?;
        // As `families::records::registry_children` leaves them out.
        children.extend(
            rows.into_iter()
                .filter(|child| !child.wrapper_held && !child.released_lease)
                .map(|child| (child.logical_name_id.clone(), child)),
        );
    }
    Ok(children)
}

/// The served name of each entry whose stored name spells a label as `[labelhash]`, read from
/// the name's surface now, as the composed reads serve it: raw bytes observed since the
/// publication give the normalized name and display name the compositor derives from them, and a
/// surface still without bytes gives the name rendered from every usable preimage
/// (`rendered::enrich`). Only the served text changes. The sort position stays the stored one.
async fn enrich(conn: &mut PgConnection, entries: &mut [AddressNameCurrentEntry]) -> Result<()> {
    let ids: Vec<String> = entries
        .iter()
        .filter(|entry| {
            !entry.registry_child
                && entry
                    .normalized_name
                    .split('.')
                    .any(|label| bracketed_label(label).is_some())
        })
        .map(|entry| entry.logical_name_id.clone())
        .collect();
    if ids.is_empty() {
        return Ok(());
    }
    let rows = sqlx::query_as::<_, (String, Option<String>, String)>(&format!(
        "/* storage:address_names.listing_rendered_names */
         SELECT surface.logical_name_id, surface.raw_name, {name}
         FROM bigname_phase.name_surfaces surface
         WHERE surface.logical_name_id = ANY($1::text[]) AND {composed}",
        name = rendered_name_sql("surface"),
        composed = composed_surface_sql("surface")
    ))
    .bind(&ids)
    .fetch_all(&mut *conn)
    .await
    .context("failed to read the current names of listed surfaces")?;
    let mut names: HashMap<String, (String, String)> = HashMap::with_capacity(rows.len());
    for (id, raw_name, rendered) in rows {
        let served = match raw_name {
            Some(raw_name) => {
                let normalized = normalize_name(&raw_name).with_context(|| {
                    format!("listed name surface {id} has an unreadable active raw_name")
                })?;
                (
                    normalized.canonical_display_name,
                    normalized.normalized_name,
                )
            }
            None => (rendered.clone(), rendered),
        };
        names.insert(id, served);
    }
    for entry in entries {
        if let Some((display, normalized)) = names.get(&entry.logical_name_id) {
            entry.canonical_display_name.clone_from(display);
            entry.normalized_name.clone_from(normalized);
        }
    }
    Ok(())
}
