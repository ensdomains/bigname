//! Load and compose many names at once: one statement per input for a batch of names, then the
//! selection, the lifecycle read, the serving pointer and the row per name.
//!
//! Every public reader runs in one read-only REPEATABLE READ transaction ([`read_snapshot`]):
//! the family loop commits a block as one transaction, so every statement of a load, the marker
//! read included, sees the same block, and a row's facts and the publication it is stamped with
//! always agree.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{PgConnection, PgPool, Postgres, Row, Transaction, types::time::OffsetDateTime};
use uuid::Uuid;

use super::{
    CoverageShape, FamilyPublication, FamilyPublicationUnavailable,
    compose::{Parts, Surface, compose},
    heads::{Heads, load_heads},
    loaders::{
        histories, migrations, node_pointers, resource_pointers, resources, root_releases, surfaces,
    },
    selection::select,
    serving::{PointerRow, ownerless_serving, root_tld_serving},
};
use crate::{
    NameCurrentRow,
    families::control::lifecycle::{
        AuthoritySelection, Clock, NameInput, evaluate, load_name_facts_on,
    },
};

/// A read-only REPEATABLE READ transaction on `pool`: its snapshot is taken at its first
/// statement and holds for every statement after it, so a composed read cannot mix two family
/// blocks. The caller commits it (nothing is written) once the read is done.
pub(crate) async fn read_snapshot(pool: &PgPool) -> Result<Transaction<'static, Postgres>> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to begin the composed name read")?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *transaction)
        .await
        .context("failed to pin the composed name read to one snapshot")?;
    Ok(transaction)
}

/// The family marker of `chain_id`, the publication a composed row describes, when it is
/// servable: `live` and written by this build's interpreter, the fence's rule
/// (snapshot_selection/project.rs). None otherwise.
pub async fn load_family_publication(
    pool: &PgPool,
    chain_id: &str,
) -> Result<Option<FamilyPublication>> {
    let mut conn = pool
        .acquire()
        .await
        .context("failed to acquire a connection for the family marker")?;
    publication(&mut conn, chain_id).await
}

/// The publication of `chain_id` read on `conn`, or [`FamilyPublicationUnavailable`] when its
/// marker is not servable. A family read that is not keyed by a name (the address and record
/// readers) checks its chains with it.
pub(crate) async fn servable_publication(
    conn: &mut PgConnection,
    chain_id: &str,
) -> Result<FamilyPublication> {
    match publication(conn, chain_id).await? {
        Some(publication) => Ok(publication),
        None => Err(FamilyPublicationUnavailable {
            chain_id: chain_id.to_owned(),
        }
        .into()),
    }
}

/// Every chain's publication, or [`FamilyPublicationUnavailable`] for the first chain whose
/// marker is not servable (or `none` when no marker exists): an address read that found no rows
/// cannot tell an empty answer from families still being built otherwise.
pub(crate) async fn all_servable_publications(
    conn: &mut PgConnection,
) -> Result<Vec<FamilyPublication>> {
    let chains: Vec<String> = sqlx::query_scalar(
        "/* storage:families.name.marker_chains */
         SELECT chain_id FROM bigname_phase.project_family_marker ORDER BY chain_id",
    )
    .fetch_all(&mut *conn)
    .await
    .context("failed to list the family markers")?;
    if chains.is_empty() {
        return Err(FamilyPublicationUnavailable {
            chain_id: "none".to_owned(),
        }
        .into());
    }
    let mut out = Vec::with_capacity(chains.len());
    for chain_id in chains {
        out.push(servable_publication(conn, &chain_id).await?);
    }
    Ok(out)
}

async fn publication(conn: &mut PgConnection, chain_id: &str) -> Result<Option<FamilyPublication>> {
    let row = sqlx::query(
        "/* storage:families.name.publication */
         SELECT chain_id, current_block_number, current_block_hash, block_timestamp,
                to_jsonb(block_timestamp) AS block_timestamp_json
         FROM bigname_phase.project_family_marker
         WHERE chain_id = $1 AND state = 'live' AND input_content_hash = $2
           AND current_block_number IS NOT NULL",
    )
    .bind(chain_id)
    .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    .fetch_optional(conn)
    .await
    .with_context(|| format!("failed to load the family marker of {chain_id}"))?;
    row.map(|row| {
        Ok(FamilyPublication {
            chain_id: row.try_get("chain_id")?,
            block_number: row.try_get("current_block_number")?,
            block_hash: row.try_get("current_block_hash")?,
            block_timestamp: row.try_get::<OffsetDateTime, _>("block_timestamp")?,
            block_timestamp_json: row.try_get("block_timestamp_json")?,
        })
    })
    .transpose()
}

/// The composed row of one name, with the single-name coverage shape; none when the name has
/// no readable active surface or its chain no family marker.
pub async fn load_family_name(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<Option<NameCurrentRow>> {
    let mut snapshot = read_snapshot(pool).await?;
    let mut rows = load(
        &mut snapshot,
        &[logical_name_id.to_owned()],
        CoverageShape::WithBasis,
    )
    .await?;
    snapshot.commit().await?;
    Ok(rows.remove(logical_name_id))
}

/// The composed rows of `logical_name_ids` keyed by name, missing names omitted, as
/// `load_name_current_by_logical_name_ids` serves them.
pub async fn load_family_names_by_logical_name_ids(
    pool: &PgPool,
    logical_name_ids: &[String],
) -> Result<BTreeMap<String, NameCurrentRow>> {
    let mut snapshot = read_snapshot(pool).await?;
    let rows = load(&mut snapshot, logical_name_ids, CoverageShape::Plain).await?;
    snapshot.commit().await?;
    Ok(rows)
}

/// One composed row per resource: of the names whose row is bound to the resource, the first by
/// raw name then name id (`load_current_names_by_resource_ids`).
pub async fn load_family_names_by_resource_ids(
    pool: &PgPool,
    resource_ids: &[Uuid],
) -> Result<BTreeMap<Uuid, NameCurrentRow>> {
    if resource_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut snapshot = read_snapshot(pool).await?;
    let names: Vec<String> = sqlx::query_scalar(
        "/* storage:families.name.by_resource */
         SELECT DISTINCT candidate.logical_name_id
         FROM bigname_phase.project_binding_candidate candidate
         WHERE candidate.resource_id = ANY($1::uuid[])",
    )
    .bind(resource_ids)
    .fetch_all(&mut *snapshot)
    .await
    .context("failed to load the names bound to resources")?;
    // The served pick orders by raw name then name id in the database's collation, so the
    // order is taken from the database too.
    let ordered: Vec<String> = sqlx::query_scalar(
        "/* storage:families.name.by_resource_order */
         SELECT surface.logical_name_id FROM bigname_phase.name_surfaces surface
         WHERE surface.logical_name_id = ANY($1)
         ORDER BY surface.raw_name ASC, surface.logical_name_id ASC",
    )
    .bind(&names)
    .fetch_all(&mut *snapshot)
    .await
    .context("failed to order the names bound to resources")?;
    let mut rows = load(&mut snapshot, &names, CoverageShape::Plain).await?;
    snapshot.commit().await?;
    let mut out: BTreeMap<Uuid, NameCurrentRow> = BTreeMap::new();
    for name in ordered {
        let Some(row) = rows.remove(&name) else {
            continue;
        };
        if let Some(resource) = row.resource_id.filter(|id| resource_ids.contains(id)) {
            out.entry(resource).or_insert(row);
        }
    }
    Ok(out)
}

/// The composed rows of `logical_name_ids` read on `conn`, which the caller holds in one
/// [`read_snapshot`].
pub(crate) async fn load(
    conn: &mut PgConnection,
    logical_name_ids: &[String],
    shape: CoverageShape,
) -> Result<BTreeMap<String, NameCurrentRow>> {
    let mut out = BTreeMap::new();
    if logical_name_ids.is_empty() {
        return Ok(out);
    }
    let mut by_chain: BTreeMap<String, Vec<Surface>> = BTreeMap::new();
    for surface in surfaces(conn, logical_name_ids).await? {
        by_chain
            .entry(surface.chain_id.clone())
            .or_default()
            .push(surface);
    }
    for (chain_id, surfaces) in by_chain {
        let Some(publication) = publication(conn, &chain_id).await? else {
            return Err(FamilyPublicationUnavailable { chain_id }.into());
        };
        super::seams::after_publication().await;
        // A surface written after the publication is not part of it.
        let surfaces: Vec<Surface> = surfaces
            .into_iter()
            .filter(|surface| surface.block_number <= publication.block_number)
            .collect();
        if surfaces.is_empty() {
            continue;
        }
        out.extend(load_chain(conn, &publication, &surfaces, shape).await?);
    }
    Ok(out)
}

async fn load_chain(
    conn: &mut PgConnection,
    publication: &FamilyPublication,
    surfaces: &[Surface],
    shape: CoverageShape,
) -> Result<BTreeMap<String, NameCurrentRow>> {
    let chain_id = publication.chain_id.as_str();
    let clock = Clock {
        block_number: publication.block_number,
        timestamp_seconds: publication.timestamp_seconds(),
    };
    let ids: Vec<String> = surfaces.iter().map(|s| s.logical_name_id.clone()).collect();
    let inputs: Vec<NameInput> = surfaces
        .iter()
        .map(|surface| NameInput {
            logical_name_id: surface.logical_name_id.clone(),
            namehash: surface.namehash.to_ascii_lowercase(),
            selection: AuthoritySelection::default(),
        })
        .collect();
    let mut facts = load_name_facts_on(conn, chain_id, &inputs).await?;
    let histories = histories(conn, chain_id, &ids).await?;
    let mut migrations = migrations(conn, chain_id, &ids).await?;
    let mut wanted: BTreeSet<String> = BTreeSet::new();
    for facts in &facts {
        wanted.extend(facts.candidates.iter().map(|c| c.resource_id.clone()));
        wanted.extend(facts.events.iter().filter_map(|e| e.resource_id.clone()));
        if let Some(node) = &facts.registry_node {
            wanted.extend(
                node.owner_events
                    .iter()
                    .filter_map(|e| e.resource_id.clone()),
            );
        }
    }
    let wanted: Vec<String> = wanted.into_iter().collect();
    let readable = resources(conn, publication, &wanted).await?;
    let readable_ids: BTreeSet<String> = readable.keys().cloned().collect();
    let nodes: Vec<(String, String)> = surfaces
        .iter()
        .map(|surface| {
            (
                surface.namespace.clone(),
                surface.namehash.to_ascii_lowercase(),
            )
        })
        .collect();
    let (pointers, roots) = resource_pointers(conn, chain_id, &wanted, &nodes).await?;
    let node_pointers = node_pointers(conn, chain_id, &nodes).await?;
    let root_resources: Vec<String> = roots
        .values()
        .flatten()
        .filter_map(|pointer| pointer.resource_id.clone())
        .collect();
    let releases = root_releases(conn, chain_id, &root_resources).await?;
    let staged: Vec<String> = facts
        .iter()
        .flat_map(|facts| {
            facts
                .events
                .iter()
                .filter(|event| event.original_logical_name_id.is_none())
                .map(|event| event.position.event_identity.clone())
        })
        .collect();
    let heads =
        Heads::new(load_heads(conn, chain_id, publication.block_number, &ids, &staged).await?);

    let mut out = BTreeMap::new();
    for (surface, facts) in surfaces.iter().zip(facts.iter_mut()) {
        let name = surface.logical_name_id.as_str();
        let history = histories.get(name);
        let decided = select(
            facts,
            &clock,
            history,
            migrations.remove(name),
            &readable_ids,
        )?;
        facts.input.selection = decided.selection.clone();
        let shadow =
            evaluate(facts, &clock).with_context(|| format!("the composed read of {name}"))?;
        let node = (
            surface.namespace.clone(),
            surface.namehash.to_ascii_lowercase(),
        );
        let serving = if decided.selection.ownerless_registry {
            decided.ownerless_transfer.as_ref().and_then(|transfer| {
                let resource = transfer.resource_id.as_deref()?;
                ownerless_serving(
                    name,
                    pointers.get(resource),
                    readable
                        .get(resource)
                        .is_some_and(|(token, _)| token.is_some()),
                    transfer.owner_getter_reason.clone(),
                )
            })
        } else if decided.selection.unsupported_reason.as_deref()
            == Some("current_authority_not_projected")
            && decided.binding.is_none()
        {
            roots.get(&node).and_then(|candidates| {
                let named: Vec<PointerRow> = candidates.to_vec();
                root_tld_serving(&named, |resource| releases.get(resource).copied())
            })
        } else {
            None
        };
        let event_resource = shadow.trace.get("event_resource").and_then(Value::as_str);
        let resolver_resource = decided.selection.resource_id.as_deref();
        let token = event_resource.and_then(|resource| readable.get(resource));
        // A bound row whose token lineage is not readable is not served
        // (DEFAULT_NAME_CURRENT_READ_FILTER).
        if decided.binding.is_some() && token.is_some_and(|(_, readable)| !readable) {
            continue;
        }
        let row = compose(
            &Parts {
                surface,
                publication,
                facts,
                shadow: &shadow,
                selection: &decided,
                history,
                serving: serving.as_ref(),
                resource_pointer: resolver_resource.and_then(|resource| pointers.get(resource)),
                node_pointer: node_pointers.get(&node),
                heads: &heads,
                token_lineage_id: token.and_then(|(token, _)| *token),
            },
            shape,
        )?;
        out.insert(name.to_owned(), row);
    }
    Ok(out)
}
