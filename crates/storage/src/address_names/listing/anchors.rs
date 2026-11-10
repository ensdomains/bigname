//! The current anchors of `GET /v1/events?address=` from the stored address listing
//! (`project_address_listing`): the names and resources whose relation to the address has a
//! cited event at or below the read's bound on the row's chain, and whose name was attached to
//! the row's resource at or below that bound.
//!
//! A row stores the block of the event that last set each relation. A relation set again above
//! the bound, as by a transfer to the same holder, still held at the bound, so a row with no
//! cited block at or below it is composed again with its history evidence, as the composed read
//! of every candidate does (`load_address_names_current_at_bound`).
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder};
use uuid::Uuid;

use super::statement::FENCE;
use crate::{
    AddressNameRelation,
    address_names::push_address_names_current_query,
    families::{
        name::servable_publication,
        read_snapshot,
        records::{AddressComposer, includes_roles},
    },
};

/// The relations a request asks about, as the three flags: every relation when none is named.
pub(super) fn wanted(relations: Option<&[AddressNameRelation]>) -> [bool; 3] {
    let relations = relations.filter(|relations| !relations.is_empty());
    let has = |relation| relations.is_none_or(|relations| relations.contains(&relation));
    [
        has(AddressNameRelation::TokenHolder),
        has(AddressNameRelation::EffectiveController),
        has(AddressNameRelation::RoleHolder),
    ]
}

/// The canonical current anchors of `address` at `published`, by name and resource.
pub(crate) async fn load_stored_current_anchors(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
    published: &BTreeMap<String, i64>,
) -> Result<Vec<(String, Uuid)>> {
    let address = address.to_ascii_lowercase();
    let mut snapshot = read_snapshot(pool).await?;
    let statement = || stored_statement(&address, namespace, wanted(relations), published);
    #[cfg(any(test, feature = "test-support"))]
    super::statement::explain(&mut snapshot, "anchors", || Ok(statement())).await?;
    let rows: Vec<(String, String, Uuid, bool)> = statement()
        .build_query_as()
        .fetch_all(&mut *snapshot)
        .await
        .with_context(|| format!("failed to load the stored anchors of {address}"))?;
    let mut anchors = BTreeSet::new();
    let mut recheck: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (chain_id, logical_name_id, resource_id, cited) in rows {
        if cited {
            anchors.insert((logical_name_id, resource_id));
        } else {
            recheck.entry(chain_id).or_default().insert(logical_name_id);
        }
    }
    let composer = AddressComposer {
        address: &address,
        include_roles: includes_roles(relations),
        with_history_evidence: true,
    };
    for (chain_id, names) in recheck {
        let publication = servable_publication(&mut snapshot, &chain_id).await?;
        let names: Vec<String> = names.into_iter().collect();
        let composed = composer
            .compose(&mut snapshot, &publication, &names)
            .await?;
        let composed = Value::Array(composed.into_iter().flat_map(|name| name.rows).collect());
        let mut builder =
            QueryBuilder::<Postgres>::new("SELECT DISTINCT logical_name_id, resource_id FROM (");
        push_address_names_current_query(
            &mut builder,
            &composed,
            &address,
            namespace,
            relations,
            false,
            Some(published),
        );
        builder.push(") current_anchors");
        let current: Vec<(String, Uuid)> = builder
            .build_query_as()
            .fetch_all(&mut *snapshot)
            .await
            .with_context(|| format!("failed to recheck the stored anchors of {address}"))?;
        anchors.extend(current);
    }
    snapshot.commit().await?;
    Ok(anchors.into_iter().collect())
}

/// The stored rows of `address` holding a `wanted` relation on a chain of `published`, each with
/// whether it is an anchor as stored: a wanted relation cited at or below the chain's bound, and
/// the name attached to the row's resource at or below it.
fn stored_statement(
    address: &str,
    namespace: Option<&str>,
    wanted: [bool; 3],
    published: &BTreeMap<String, i64>,
) -> QueryBuilder<'static, Postgres> {
    let relations = [
        ("l.token_holder", "l.token_holder_block"),
        ("l.effective_controller", "l.effective_controller_block"),
        ("l.role_holder", "l.role_holder_block"),
    ]
    .into_iter()
    .zip(wanted)
    .filter(|(_, wanted)| *wanted)
    .map(|(relation, _)| relation);
    let (held, cited): (Vec<_>, Vec<_>) = relations
        .map(|(flag, block)| (flag.to_owned(), format!("{block} <= bound.block_number")))
        .unzip();
    let (chains, blocks): (Vec<String>, Vec<i64>) = published
        .iter()
        .map(|(chain, block)| (chain.clone(), *block))
        .unzip();
    let mut builder = QueryBuilder::<Postgres>::new(format!(
        "/* storage:address_names.listing_anchors */
         SELECT l.chain_id, l.logical_name_id, l.resource_id, COALESCE(({cited})
                AND EXISTS (
                    SELECT 1 FROM bigname_phase.surface_bindings attached
                    WHERE attached.logical_name_id = l.logical_name_id
                      AND attached.resource_id = l.resource_id
                      AND attached.chain_id = l.chain_id
                      AND attached.block_number <= bound.block_number
                      AND attached.canonicality_state IN (
                          'canonical'::bigname_phase.canonicality_state,
                          'safe'::bigname_phase.canonicality_state,
                          'finalized'::bigname_phase.canonicality_state)), FALSE)
         FROM bigname_phase.project_address_listing l
         JOIN unnest(",
        cited = cited.join(" OR ")
    ));
    builder.push_bind(chains);
    builder.push("::text[], ");
    builder.push_bind(blocks);
    builder.push(
        "::bigint[]) AS bound(chain_id, block_number) ON bound.chain_id = l.chain_id
         WHERE l.address = ",
    );
    builder.push_bind(address.to_owned());
    builder.push(" AND l.listed AND NOT l.registry_child");
    if let Some(namespace) = namespace {
        builder.push(" AND l.namespace = ");
        builder.push_bind(namespace.to_owned());
    }
    builder.push(format!(" AND ({})", held.join(" OR ")));
    builder.push(FENCE);
    builder
}

/// [`load_stored_current_anchors`], for tests to compare with the composed anchors.
#[cfg(any(test, feature = "test-support"))]
pub async fn load_stored_address_anchors(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
    published: &BTreeMap<String, i64>,
) -> Result<BTreeSet<(String, Uuid)>> {
    Ok(
        load_stored_current_anchors(pool, address, namespace, relations, published)
            .await?
            .into_iter()
            .collect(),
    )
}

/// The anchors the composed read of every candidate gives `address` at `published`
/// (`load_address_names_current_at_bound`), for tests to compare with the stored ones.
#[cfg(any(test, feature = "test-support"))]
pub async fn load_composed_address_anchors(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
    published: &BTreeMap<String, i64>,
) -> Result<BTreeSet<(String, Uuid)>> {
    let rows = crate::address_names::load_address_names_current_at_bound(
        pool, address, namespace, relations, false, published,
    )
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| (row.logical_name_id, row.resource_id))
        .collect())
}
