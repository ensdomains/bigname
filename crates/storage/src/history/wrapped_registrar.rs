//! The link our adapter records for `NameWrapped` from a NameWrapper resource to
//! the BaseRegistrar lease it wrapped (`after_state.wrapped_registrar_resource_id`
//! on the NameWrapper's `SurfaceBound` row). History follows that link in both
//! directions so a wrapped `.eth` name keeps one registration handle: its lease.
//! The name a BaseRegistrar grant carries (`after_state.namehash`) is followed
//! the same way, so a lease that never received a binding of its own still
//! reaches the name's registration history.
//! Upstream wrapping transfers the registrar token and reclaims registry ownership;
//! the link and public registration handle above are Bigname's representation.
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L246-L278 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L152 @ ens_v1@91c966f)

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, QueryBuilder};
use uuid::Uuid;

/// Load the BaseRegistrar lease resources that canonical `NameWrapped` rows link to one exact
/// name. `published` keeps only links recorded at or below each chain's published block.
pub async fn load_wrapped_registrar_resource_ids_by_logical_name_id(
    pool: &PgPool,
    logical_name_id: &str,
    published: Option<&BTreeMap<String, i64>>,
) -> Result<Vec<Uuid>> {
    load_wrapped_registrar_resource_ids(pool, logical_name_id, true, published).await
}

/// Load the BaseRegistrar leases granted to one exact name: the canonical `ens_v1_registrar_l1`
/// `RegistrationGranted` rows of registrar authority kind whose `after_state.namehash` is the
/// name's surface namehash, on the surface's chain and namespace. A lease granted with
/// `registerOnly` while the name stays bound to a registry-only resource (a registrar token
/// transferred without `reclaim`) has no binding and no `NameWrapped` link of its own, so this is
/// how its rows stay reachable once a later grant replaces it. `published` keeps only grants
/// recorded at or below each chain's published block.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L118-L152 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L175 @ ens_v1@91c966f)
pub async fn load_registrar_grant_resource_ids_by_logical_name_id(
    pool: &PgPool,
    logical_name_id: &str,
    published: Option<&BTreeMap<String, i64>>,
) -> Result<Vec<Uuid>> {
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_registrar_grant_resources_query(&mut builder, logical_name_id, true);
    let grants: Vec<WrappedRegistrarLink> = builder
        .build_query_as()
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!(
                "failed to load registrar grant resources for logical_name_id {logical_name_id}"
            )
        })?;
    Ok(published_resource_ids(grants, published))
}

fn published_resource_ids(
    links: Vec<WrappedRegistrarLink>,
    published: Option<&BTreeMap<String, i64>>,
) -> Vec<Uuid> {
    let mut resource_ids = links
        .into_iter()
        .filter(|link| {
            published.is_none_or(|bounds| {
                link.chain_id
                    .as_ref()
                    .and_then(|chain_id| bounds.get(chain_id))
                    .zip(link.block_number)
                    .is_some_and(|(bound, block_number)| block_number <= *bound)
            })
        })
        .map(|link| link.registrar_resource_id)
        .collect::<Vec<_>>();
    resource_ids.sort_unstable();
    resource_ids.dedup();
    resource_ids
}

/// The grant rows are found through `normalized_events_v1_direct_node_probe_idx`, whose key is
/// the namespace-qualified node an ENSv1 row carries, so the join repeats that expression.
pub(super) fn push_registrar_grant_resources_query<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    logical_name_id: &'a str,
    canonical_only: bool,
) {
    builder.push(
        r#"
        SELECT DISTINCT
            lease.chain_id,
            lease.block_number,
            lease.resource_id AS registrar_resource_id
        FROM bigname_phase.name_surfaces surface
        LEFT JOIN bigname_phase.chain_lineage surface_lineage
          ON surface_lineage.chain_id = surface.chain_id
         AND surface_lineage.block_hash = surface.block_hash
        JOIN bigname_phase.normalized_events lease
          ON lease.chain_id = surface.chain_id
         AND COALESCE(
                 lease.namespace || ':' || lower(COALESCE(
                     lease.after_state ->> 'child_node',
                     lease.after_state ->> 'namehash',
                     lease.after_state ->> 'node',
                     lease.after_state #>> '{grant_source,node}',
                     lease.after_state #>> '{revocation_source,node}'
                 )),
                 lease.logical_name_id
             ) = surface.namespace || ':' || lower(surface.namehash)
        LEFT JOIN bigname_phase.chain_lineage lease_lineage
          ON lease_lineage.chain_id = lease.chain_id
         AND lease_lineage.block_hash = lease.block_hash
        WHERE surface.logical_name_id = "#,
    );
    builder.push_bind(logical_name_id);
    builder.push(
        r#"
          AND lease.resource_id IS NOT NULL
          AND lease.source_family LIKE 'ens\_v1\_%'
          AND lease.source_family = 'ens_v1_registrar_l1'
          AND lease.event_kind = 'RegistrationGranted'
          AND lease.consumer_visibility = 'activated'
          AND lower(lease.after_state ->> 'namehash') = lower(surface.namehash)
          AND COALESCE(NULLIF(lease.after_state ->> 'authority_kind', ''), 'registrar')
              = 'registrar'
        "#,
    );
    if canonical_only {
        push_canonical_row_filter(builder, "surface", "surface_lineage");
        push_canonical_row_filter(builder, "lease", "lease_lineage");
    }
    builder.push(" ORDER BY 3, 1, 2");
}

#[derive(sqlx::FromRow)]
struct WrappedRegistrarLink {
    chain_id: Option<String>,
    block_number: Option<i64>,
    registrar_resource_id: Uuid,
}

pub(super) async fn load_wrapped_registrar_resource_ids(
    pool: &PgPool,
    logical_name_id: &str,
    canonical_only: bool,
    published: Option<&BTreeMap<String, i64>>,
) -> Result<Vec<Uuid>> {
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_wrapped_registrar_resources_query(&mut builder, logical_name_id, canonical_only);
    let links: Vec<WrappedRegistrarLink> = builder
        .build_query_as()
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!(
                "failed to load wrapped registrar resources for logical_name_id {logical_name_id}"
            )
        })?;
    Ok(published_resource_ids(links, published))
}

pub(super) fn push_wrapped_registrar_resources_query<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    logical_name_id: &'a str,
    canonical_only: bool,
) {
    builder.push(
        r#"
        SELECT DISTINCT
            ne.chain_id,
            ne.block_number,
            (ne.after_state ->> 'wrapped_registrar_resource_id')::uuid AS registrar_resource_id
        FROM bigname_phase.normalized_events ne
        LEFT JOIN bigname_phase.chain_lineage lineage
          ON lineage.chain_id = ne.chain_id
         AND lineage.block_hash = ne.block_hash
        WHERE ne.logical_name_id = "#,
    );
    builder.push_bind(logical_name_id);
    builder.push(
        r#"
          AND ne.event_kind = 'SurfaceBound'
          AND ne.source_family = 'ens_v1_wrapper_l1'
          AND ne.consumer_visibility = 'activated'
          AND ne.after_state ->> 'wrapped_registrar_resource_id' IS NOT NULL
        "#,
    );
    if canonical_only {
        push_canonical_row_filter(builder, "ne", "lineage");
    }
    builder.push(" ORDER BY 3, 1, 2");
}

/// The exact names whose `NameWrapped` rows link to `resource_id`. The lease's own rows carry
/// the node, the node selects the exact surfaces, and each surface's links are then checked.
pub(super) async fn load_wrapping_logical_name_ids(
    pool: &PgPool,
    resource_id: Uuid,
    canonical_only: bool,
    published: Option<&BTreeMap<String, i64>>,
) -> Result<Vec<String>> {
    let candidates = load_node_logical_name_ids(pool, resource_id, canonical_only).await?;
    let mut logical_name_ids = Vec::new();
    for logical_name_id in candidates {
        let wrapped_registrars =
            load_wrapped_registrar_resource_ids(pool, &logical_name_id, canonical_only, published)
                .await?;
        if wrapped_registrars.contains(&resource_id) {
            logical_name_ids.push(logical_name_id);
        }
    }
    Ok(logical_name_ids)
}

/// The exact names of the node a resource's own rows carry (a BaseRegistrar lease's grant names
/// its node). These are candidates only: nothing here proves the resource serves the name.
pub(super) async fn load_node_logical_name_ids(
    pool: &PgPool,
    resource_id: Uuid,
    canonical_only: bool,
) -> Result<std::collections::BTreeSet<String>> {
    let mut candidates = std::collections::BTreeSet::new();
    for anchor in load_registrar_namehash_anchors(pool, resource_id, canonical_only).await? {
        candidates.extend(load_namehash_surfaces(pool, &anchor, canonical_only).await?);
    }
    Ok(candidates)
}

/// Exact-name candidates proved by published, activated grants on a registrar resource.
/// The event-level registration filter still checks which rows belong to this lease.
pub(super) async fn load_granted_logical_name_ids(
    pool: &PgPool,
    resource_id: Uuid,
    canonical_only: bool,
    published: Option<&BTreeMap<String, i64>>,
) -> Result<Vec<String>> {
    let mut grants = QueryBuilder::<Postgres>::new(
        "SELECT DISTINCT anchor.chain_id, anchor.namespace,
                anchor.after_state ->> 'namehash' AS namehash
         FROM bigname_phase.normalized_events anchor
         LEFT JOIN bigname_phase.chain_lineage anchor_lineage
           ON anchor_lineage.chain_id = anchor.chain_id
          AND anchor_lineage.block_hash = anchor.block_hash
         WHERE anchor.resource_id = ",
    );
    grants.push_bind(resource_id);
    grants.push(
        " AND anchor.source_family = 'ens_v1_registrar_l1'
          AND anchor.event_kind = 'RegistrationGranted'
          AND anchor.consumer_visibility = 'activated'
          AND anchor.after_state ->> 'namehash' IS NOT NULL",
    );
    super::filters::push_publication_bound(&mut grants, "anchor", published);
    if canonical_only {
        push_canonical_row_filter(&mut grants, "anchor", "anchor_lineage");
    }
    let anchors: Vec<ResourceNamehashAnchor> = grants.build_query_as().fetch_all(pool).await?;
    let mut names = std::collections::BTreeSet::new();
    for anchor in anchors {
        names.extend(load_namehash_surfaces(pool, &anchor, canonical_only).await?);
    }
    Ok(names.into_iter().collect())
}

#[derive(sqlx::FromRow)]
pub(super) struct ResourceNamehashAnchor {
    pub(super) chain_id: String,
    pub(super) namespace: String,
    pub(super) namehash: String,
}

async fn load_registrar_namehash_anchors(
    pool: &PgPool,
    resource_id: Uuid,
    canonical_only: bool,
) -> Result<Vec<ResourceNamehashAnchor>> {
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_registrar_namehash_anchors_query(&mut builder, resource_id, canonical_only);
    builder
        .build_query_as()
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!("failed to load namehash anchors for registrar resource_id {resource_id}")
        })
}

pub(super) fn push_registrar_namehash_anchors_query(
    builder: &mut QueryBuilder<'_, Postgres>,
    resource_id: Uuid,
    canonical_only: bool,
) {
    builder.push(
        r#"
        SELECT DISTINCT
            anchor.chain_id,
            anchor.namespace,
            anchor.after_state ->> 'namehash' AS namehash
        FROM bigname_phase.normalized_events anchor
        LEFT JOIN bigname_phase.chain_lineage anchor_lineage
          ON anchor_lineage.chain_id = anchor.chain_id
         AND anchor_lineage.block_hash = anchor.block_hash
        WHERE anchor.resource_id = "#,
    );
    builder.push_bind(resource_id);
    builder.push(
        r#"
          AND anchor.chain_id IS NOT NULL
          AND anchor.after_state ->> 'namehash' IS NOT NULL
          AND anchor.after_state ->> 'namehash' <> ''
        "#,
    );
    if canonical_only {
        push_canonical_row_filter(builder, "anchor", "anchor_lineage");
    }
    builder.push(" ORDER BY anchor.chain_id, anchor.namespace, namehash");
}

async fn load_namehash_surfaces(
    pool: &PgPool,
    anchor: &ResourceNamehashAnchor,
    canonical_only: bool,
) -> Result<Vec<String>> {
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_namehash_surfaces_query(&mut builder, anchor, canonical_only);
    builder
        .build_query_scalar()
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!(
                "failed to load exact surfaces for registrar namehash {}",
                anchor.namehash
            )
        })
}

pub(super) fn push_namehash_surfaces_query<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    anchor: &'a ResourceNamehashAnchor,
    canonical_only: bool,
) {
    builder.push(
        r#"
        SELECT surface.logical_name_id
        FROM (
            SELECT scoped.*
            FROM bigname_phase.name_surfaces scoped
            WHERE scoped.namespace = "#,
    );
    builder.push_bind(&anchor.namespace);
    builder.push(" AND scoped.namehash = ");
    builder.push_bind(&anchor.namehash);
    builder.push(
        r#"
            OFFSET 0
        ) surface
        LEFT JOIN bigname_phase.chain_lineage surface_lineage
          ON surface_lineage.chain_id = surface.chain_id
         AND surface_lineage.block_hash = surface.block_hash
        WHERE surface.chain_id = "#,
    );
    builder.push_bind(&anchor.chain_id);
    if canonical_only {
        push_canonical_row_filter(builder, "surface", "surface_lineage");
    }
    builder.push(" ORDER BY surface.logical_name_id");
}

fn push_canonical_row_filter(
    builder: &mut QueryBuilder<'_, Postgres>,
    row_alias: &str,
    lineage_alias: &str,
) {
    builder.push(format!(
        r#"
        AND {row_alias}.canonicality_state IN (
            'canonical'::bigname_phase.canonicality_state,
            'safe'::bigname_phase.canonicality_state,
            'finalized'::bigname_phase.canonicality_state
        )
        AND (
            {row_alias}.block_hash IS NULL
            OR {lineage_alias}.canonicality_state IN (
                'canonical'::bigname_phase.canonicality_state,
                'safe'::bigname_phase.canonicality_state,
                'finalized'::bigname_phase.canonicality_state
            )
        )
        "#,
    ));
}
