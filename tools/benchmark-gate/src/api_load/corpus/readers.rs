//! Corpus candidates come from family indexes; admission uses the same composed readers as
//! the API. Scans retain one input batch and the requested samples, not a replacement cache.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use bigname_storage::{
    AddressNamesCurrentDedupe, AddressNamesCurrentOrder, AddressNamesCurrentSort, NameCurrentRow,
    PrimaryNameClaimStatus,
    families::{name::load_family_names_by_logical_name_ids, records},
};
use sqlx::PgPool;

const BATCH: i64 = 256;
pub(super) type AddressTarget = (String, String, String, String);

pub(super) async fn namespaces(pool: &PgPool) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar(super::ACTIVE_NAMESPACES_SQL)
        .fetch_all(pool)
        .await?)
}

fn quota(namespaces: &[String], namespace: &str, limit: usize) -> usize {
    let Some(index) = namespaces.iter().position(|value| value == namespace) else {
        return 0;
    };
    limit / namespaces.len() + usize::from(index < limit % namespaces.len())
}

pub(super) fn supported(row: &NameCurrentRow) -> bool {
    row.coverage["status"] != "unsupported"
}

pub(super) async fn name_batch(
    pool: &PgPool,
    namespaces: &[String],
    after: &str,
    parents_only: bool,
) -> Result<(String, Vec<NameCurrentRow>)> {
    let ids: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT surface.logical_name_id FROM bigname_phase.name_surfaces surface
         WHERE surface.namespace = ANY($1) AND surface.logical_name_id > $2
           AND (NOT $3 OR EXISTS (
               SELECT 1 FROM bigname_phase.project_child_edge_candidate edge
               WHERE edge.chain_id = surface.chain_id AND edge.namespace = surface.namespace
                 AND edge.parent_node = surface.namehash)
               OR EXISTS (
                   SELECT 1 FROM bigname_phase.project_parent_subregistry subregistry
                   WHERE subregistry.chain_id = surface.chain_id
                     AND subregistry.logical_name_id = surface.logical_name_id))
         ORDER BY surface.logical_name_id LIMIT $4",
    )
    .bind(namespaces)
    .bind(after)
    .bind(parents_only)
    .bind(BATCH)
    .fetch_all(pool)
    .await?;
    let next = ids.last().cloned().unwrap_or_else(|| after.to_owned());
    let rows = load_family_names_by_logical_name_ids(pool, &ids)
        .await?
        .into_values()
        .filter(supported)
        .collect();
    Ok((next, rows))
}

pub(super) async fn names(
    pool: &PgPool,
    limit: usize,
    parents: bool,
) -> Result<Vec<(String, String)>> {
    let namespaces = namespaces(pool).await?;
    let mut samples = BTreeMap::<String, BTreeSet<(String, String)>>::new();
    let mut after = String::new();
    loop {
        let (next, rows) = name_batch(pool, &namespaces, &after, parents).await?;
        if next == after {
            break;
        }
        after = next;
        let counts: BTreeMap<_, _> = if parents {
            bigname_storage::families::topology::count_children_shadow(
                pool,
                &rows
                    .iter()
                    .map(|row| row.logical_name_id.clone())
                    .collect::<Vec<_>>(),
            )
            .await?
            .into_iter()
            .collect()
        } else {
            BTreeMap::new()
        };
        for row in rows {
            if parents
                && (row.canonical_display_name.is_empty()
                    || counts
                        .get(&row.logical_name_id)
                        .copied()
                        .unwrap_or_default()
                        == 0)
            {
                continue;
            }
            let size = quota(&namespaces, &row.namespace, limit);
            let selected = samples.entry(row.namespace).or_default();
            // Names use logical-id order; parent sampling uses name order, as before.
            let sort = if parents {
                row.canonical_display_name.clone()
            } else {
                row.logical_name_id
            };
            selected.insert((sort, row.canonical_display_name));
            if selected.len() > size {
                selected.pop_last();
            }
        }
    }
    Ok(samples
        .into_iter()
        .flat_map(|(ns, rows)| rows.into_iter().map(move |(_, name)| (ns.clone(), name)))
        .collect())
}

/// Count each supported address/name/relation row, and sample the first address/relation keys
/// per namespace with their minimum display name. Collection pages group a name's relations;
/// expanding those facets reproduces the corpus's relation population.
pub(super) async fn addresses(pool: &PgPool, limit: usize) -> Result<(u64, Vec<AddressTarget>)> {
    let namespaces = namespaces(pool).await?;
    let mut count = 0;
    let mut samples = BTreeMap::<String, BTreeMap<(String, String), String>>::new();
    for ns in &namespaces {
        let size = quota(&namespaces, ns, limit);
        let mut after = String::new();
        loop {
            let addresses: Vec<String> = sqlx::query_scalar(
                "SELECT DISTINCT indexed.address FROM bigname_phase.project_address_name_index indexed
                 JOIN bigname_phase.name_surfaces surface ON surface.logical_name_id = indexed.logical_name_id
                 WHERE surface.namespace = $1 AND indexed.address > $2
                 ORDER BY indexed.address LIMIT $3",
            ).bind(ns).bind(&after).bind(BATCH).fetch_all(pool).await?;
            let Some(last) = addresses.last() else {
                break;
            };
            after = last.clone();
            for address in addresses {
                let mut cursor = None;
                loop {
                    let page = records::load_family_address_names_page(
                        pool,
                        &address,
                        Some(ns),
                        None,
                        AddressNamesCurrentDedupe::Surface,
                        None,
                        None,
                        None,
                        AddressNamesCurrentSort::Name,
                        AddressNamesCurrentOrder::Asc,
                        cursor.as_ref(),
                        None,
                        BATCH as u64,
                    )
                    .await?;
                    for row in page.entries {
                        if row.coverage["status"] == "unsupported" {
                            continue;
                        }
                        for relation in row.relations {
                            count += 1;
                            let selected = samples.entry(ns.clone()).or_default();
                            selected
                                .entry((address.clone(), relation.as_str().to_owned()))
                                .and_modify(|name| {
                                    *name = name.clone().min(row.canonical_display_name.clone())
                                })
                                .or_insert_with(|| row.canonical_display_name.clone());
                            if selected.len() > size {
                                selected.pop_last();
                            }
                        }
                    }
                    cursor = page.next_cursor;
                    if cursor.is_none() {
                        break;
                    }
                }
            }
        }
    }
    Ok((
        count,
        samples
            .into_iter()
            .flat_map(|(ns, rows)| {
                rows.into_iter()
                    .map(move |((address, relation), name)| (address, name, ns.clone(), relation))
            })
            .collect(),
    ))
}

pub(super) async fn primary_names(
    pool: &PgPool,
    limit: usize,
) -> Result<Vec<(String, String, String)>> {
    let namespaces = namespaces(pool).await?;
    let mut selected = Vec::new();
    for ns in &namespaces {
        let size = quota(&namespaces, ns, limit);
        let mut after = (String::new(), String::new());
        let mut count = 0;
        while count < size {
            let keys: Vec<(String, String)> = sqlx::query_as(
                "SELECT address, coin_type FROM bigname_phase.project_reverse_tuple
                 WHERE namespace = $1 AND (address, coin_type) > ($2, $3)
                 ORDER BY address, coin_type LIMIT $4",
            )
            .bind(ns)
            .bind(&after.0)
            .bind(&after.1)
            .bind(BATCH)
            .fetch_all(pool)
            .await?;
            let Some(last) = keys.last() else {
                break;
            };
            after = last.clone();
            for (address, coin) in keys {
                if records::load_family_primary_name_snapshot(pool, &address, ns, &coin)
                    .await?
                    .is_some_and(|claim| claim.row.claim_status == PrimaryNameClaimStatus::Success)
                {
                    selected.push((address, coin, ns.clone()));
                    count += 1;
                    if count == size {
                        break;
                    }
                }
            }
        }
    }
    Ok(selected)
}
