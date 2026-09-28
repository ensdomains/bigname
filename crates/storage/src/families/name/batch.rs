//! Load and compose many names at once: one statement per input for a batch of names, then the
//! selection, the lifecycle read, the serving pointer and the row per name.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{PgConnection, PgPool, Row, types::time::OffsetDateTime};
use uuid::Uuid;

use super::{
    CoverageShape, FamilyPublication,
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
    families::control::{
        lifecycle::{
            AuthoritySelection, Clock, NameFacts, NameInput, evaluate, load_name_facts_on,
        },
        wrapper::clock_boundaries,
    },
};

/// The family marker of `chain_id`, the publication a composed row describes. None when the
/// chain has no marker.
pub async fn load_family_publication(
    pool: &PgPool,
    chain_id: &str,
) -> Result<Option<FamilyPublication>> {
    let mut conn = pool
        .acquire()
        .await
        .context("failed to acquire a connection")?;
    publication(&mut conn, chain_id).await
}

async fn publication(conn: &mut PgConnection, chain_id: &str) -> Result<Option<FamilyPublication>> {
    let row = sqlx::query(
        "/* storage:families.name.publication */
         SELECT chain_id, current_block_number, current_block_hash, block_timestamp,
                to_jsonb(block_timestamp) AS block_timestamp_json
         FROM bigname_phase.project_family_marker WHERE chain_id = $1",
    )
    .bind(chain_id)
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
    let mut rows = load(
        pool,
        &[logical_name_id.to_owned()],
        CoverageShape::WithBasis,
    )
    .await?;
    Ok(rows.remove(logical_name_id))
}

/// The composed rows of `logical_name_ids` keyed by name, missing names omitted, as
/// `load_name_current_by_logical_name_ids` serves them.
pub async fn load_family_names_by_logical_name_ids(
    pool: &PgPool,
    logical_name_ids: &[String],
) -> Result<BTreeMap<String, NameCurrentRow>> {
    load(pool, logical_name_ids, CoverageShape::Plain).await
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
    let names: Vec<String> = sqlx::query_scalar(
        "/* storage:families.name.by_resource */
         SELECT DISTINCT candidate.logical_name_id
         FROM bigname_phase.project_binding_candidate candidate
         WHERE candidate.resource_id = ANY($1::uuid[])",
    )
    .bind(resource_ids)
    .fetch_all(pool)
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
    .fetch_all(pool)
    .await
    .context("failed to order the names bound to resources")?;
    let mut rows = load(pool, &names, CoverageShape::Plain).await?;
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

/// Compose the rows of `logical_name_ids` in one read-only snapshot: the marker and every family
/// row a row reads come from the same transaction, so a family block committing meanwhile cannot
/// mix two publications into one row.
async fn load(
    pool: &PgPool,
    logical_name_ids: &[String],
    shape: CoverageShape,
) -> Result<BTreeMap<String, NameCurrentRow>> {
    let mut out = BTreeMap::new();
    if logical_name_ids.is_empty() {
        return Ok(out);
    }
    let mut transaction = pool
        .begin()
        .await
        .context("failed to begin a composed name read")?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *transaction)
        .await
        .context("failed to make the composed name read a snapshot")?;
    let conn: &mut PgConnection = &mut transaction;
    let mut by_chain: BTreeMap<String, Vec<Surface>> = BTreeMap::new();
    for surface in surfaces(&mut *conn, logical_name_ids, None).await? {
        by_chain
            .entry(surface.chain_id.clone())
            .or_default()
            .push(surface);
    }
    for (chain_id, surfaces) in by_chain {
        let Some(publication) = publication(&mut *conn, &chain_id).await? else {
            continue;
        };
        let composed = compose_chain(&mut *conn, &publication, &surfaces, shape, true).await?;
        out.extend(
            composed
                .into_iter()
                .map(|(name, composed)| (name, composed.row)),
        );
    }
    transaction
        .commit()
        .await
        .context("failed to end a composed name read")?;
    Ok(out)
}

/// One composed name: its row, whether its node's latest registry transfer names the zero owner
/// (the selection's `ownerless_transfer`), which the child lists read, and the first clock second
/// after the publication at which the composition can change with no fact changing: a binding
/// interval opening or closing, or a NameWrapper expiry or grace boundary.
pub(super) struct Composed {
    pub(super) row: NameCurrentRow,
    pub(super) zero_owner: bool,
    pub(super) recompose_at: Option<i64>,
}

/// Compose `surfaces` of one chain at `publication`, on `conn`. `with_heads` reads the history
/// heads the binding diagnostics serve; the summary writer, which does not store them, skips
/// that read.
pub(super) async fn compose_chain(
    conn: &mut PgConnection,
    publication: &FamilyPublication,
    surfaces: &[Surface],
    shape: CoverageShape,
    with_heads: bool,
) -> Result<BTreeMap<String, Composed>> {
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
    let mut facts = load_name_facts_on(&mut *conn, chain_id, &inputs).await?;
    let histories = histories(&mut *conn, chain_id, &ids).await?;
    let mut migrations = migrations(&mut *conn, chain_id, &ids).await?;
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
    let readable = resources(&mut *conn, publication, &wanted).await?;
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
    let (pointers, roots) = resource_pointers(&mut *conn, chain_id, &wanted, &nodes).await?;
    let node_pointers = node_pointers(&mut *conn, chain_id, &nodes).await?;
    let root_resources: Vec<String> = roots
        .values()
        .flatten()
        .filter_map(|pointer| pointer.resource_id.clone())
        .collect();
    let releases = root_releases(&mut *conn, chain_id, &root_resources).await?;
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
    let heads = if with_heads {
        Heads::new(
            load_heads(
                &mut *conn,
                chain_id,
                publication.block_number,
                &ids,
                &staged,
            )
            .await?,
        )
    } else {
        Heads::default()
    };

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
        out.insert(
            name.to_owned(),
            Composed {
                row,
                zero_owner: decided.ownerless_transfer.is_some(),
                recompose_at: recompose_at(facts, clock.timestamp_seconds),
            },
        );
    }
    Ok(out)
}

/// The first clock second after `clock_seconds` at which a composition of `facts` can differ:
/// the clock enters the composition only through the binding intervals (`open_at`) and the
/// NameWrapper masks (`effective_wrapper`).
fn recompose_at(facts: &NameFacts, clock_seconds: i64) -> Option<i64> {
    let bindings = facts
        .candidates
        .iter()
        .chain(&facts.lease_candidates)
        .flat_map(|candidate| candidate.clock_boundaries(clock_seconds));
    let wrappers = facts
        .wrappers
        .values()
        .flat_map(|row| clock_boundaries(row, clock_seconds));
    bindings.chain(wrappers).min()
}
