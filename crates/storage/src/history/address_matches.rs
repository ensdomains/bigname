use std::collections::BTreeSet;

use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, QueryBuilder};
use uuid::Uuid;

use crate::address_names::{
    AddressNameRelation, load_address_names_current_at_bound,
    load_address_names_current_for_relations,
    load_address_names_current_including_noncanonical_for_relations,
};

use super::{
    HistoryScope,
    address_evidence::push_historical_address_matches_query,
    decoders::decode_address_history_anchor,
    selectors::HistorySelector,
    source::{push_history_canonicality_filter, push_history_lineage_join},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct AddressHistoryAnchor {
    pub(super) logical_name_id: Option<String>,
    pub(super) resource_id: Option<Uuid>,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn load_address_history_selector(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
    scope: HistoryScope,
    canonical_only: bool,
    include_candidates: bool,
    published: Option<&std::collections::BTreeMap<String, i64>>,
) -> Result<HistorySelector> {
    // A current relation counts only when some binding of the name to the row's resource and the
    // event Project cites for the row lie at or below the read's published block; one acquired
    // later must not admit the resource's older events.
    // Raw diagnostics audit retained relation evidence, including former controllers. They
    // must not depend on a current family publication during a Project reset or rebuild.
    let raw_audit = include_candidates && published.is_none();
    let current_rows = match published {
        None if raw_audit => Ok(Vec::new()),
        Some(published) => {
            load_address_names_current_at_bound(
                pool,
                address,
                namespace,
                relations,
                !canonical_only,
                published,
            )
            .await
        }
        None if canonical_only => {
            load_address_names_current_for_relations(pool, address, namespace, relations).await
        }
        None => {
            load_address_names_current_including_noncanonical_for_relations(
                pool, address, namespace, relations,
            )
            .await
        }
    }
    .with_context(|| {
        let mut parts = vec![format!("address {address}")];
        if let Some(namespace) = namespace {
            parts.push(format!("namespace {namespace}"));
        }
        if let Some(relations) = relations.filter(|relations| !relations.is_empty()) {
            parts.push(format!(
                "relations {}",
                relations
                    .iter()
                    .map(|relation| relation.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
        format!(
            "failed to load address_names_current anchors for {}",
            parts.join(" ")
        )
    })?;

    let mut logical_name_ids = current_rows
        .iter()
        .map(|row| row.logical_name_id.clone())
        .collect::<BTreeSet<_>>();
    let mut resource_ids = current_rows
        .iter()
        .map(|row| row.resource_id)
        .collect::<BTreeSet<_>>();

    let historical_matches = load_historical_address_history_matches(
        pool,
        address,
        namespace,
        relations,
        canonical_only,
        include_candidates,
        published,
    )
    .await?;
    for anchor in historical_matches {
        if let Some(logical_name_id) = anchor.logical_name_id {
            logical_name_ids.insert(logical_name_id);
        }
        if let Some(resource_id) = anchor.resource_id {
            resource_ids.insert(resource_id);
        }
    }

    if raw_audit
        && relations.is_none_or(|values| values.contains(&AddressNameRelation::EffectiveController))
    {
        for anchor in
            load_retained_controller_matches(pool, address, namespace, canonical_only).await?
        {
            if let Some(name) = anchor.logical_name_id {
                logical_name_ids.insert(name);
            }
            if let Some(resource) = anchor.resource_id {
                resource_ids.insert(resource);
            }
        }
    }

    let logical_name_ids = logical_name_ids.into_iter().collect::<Vec<_>>();
    let resource_ids = resource_ids.into_iter().collect::<Vec<_>>();

    let anchors = match scope {
        HistoryScope::Surface => HistorySelector::logical_names(logical_name_ids),
        HistoryScope::Resource => HistorySelector::resources(resource_ids),
        HistoryScope::Both => {
            HistorySelector::logical_names_or_resources(logical_name_ids, resource_ids)
        }
    };
    // A registry root role belongs to no name: product reads in `both` or `registration` scope
    // that admit `role_holder` list the root role changes made to the address. Diagnostics keep
    // their name and resource anchors only.
    let root_roles = !include_candidates
        && scope != HistoryScope::Surface
        && relations.is_none_or(|values| values.contains(&AddressNameRelation::RoleHolder));
    Ok(if root_roles {
        HistorySelector::OrRootPermissionSubject {
            anchors: Box::new(anchors),
            subject: address.to_owned(),
            namespace: namespace.map(str::to_owned),
        }
    } else {
        anchors
    })
}

async fn load_historical_address_history_matches(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
    canonical_only: bool,
    include_candidates: bool,
    published: Option<&std::collections::BTreeMap<String, i64>>,
) -> Result<Vec<AddressHistoryAnchor>> {
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_historical_address_matches_query(
        &mut builder,
        address,
        namespace,
        relations,
        canonical_only,
        include_candidates,
        published,
    );
    let rows = builder
        .build()
        .fetch_all(pool)
        .await
        .context("failed to fetch historical address-history anchors")?;

    rows.into_iter()
        .map(decode_address_history_anchor)
        .collect()
}

#[cfg(test)]
#[path = "address_plan_tests.rs"]
mod plan_tests;

/// Diagnostic-only control evidence from either side of a permission change. A revocation can
/// itself prove the earlier controller even when the granting event predates retained intake.
async fn load_retained_controller_matches(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    canonical_only: bool,
) -> Result<Vec<AddressHistoryAnchor>> {
    let mut query = QueryBuilder::<Postgres>::new(
        "SELECT DISTINCT ne.logical_name_id, ne.resource_id FROM normalized_events ne ",
    );
    push_history_lineage_join(&mut query);
    query.push(" WHERE ne.resource_id IS NOT NULL AND (");
    query.push(
        "(ne.event_kind='PermissionChanged' AND EXISTS (
        SELECT 1 FROM (VALUES (ne.before_state), (ne.after_state)) states(value)
        WHERE value #>> '{scope,kind}' = 'resource'
          AND value -> 'effective_powers' @> '[\"resource_control\"]'::jsonb
          AND lower(value ->> 'subject') = ",
    );
    query.push_bind(address);
    query.push(
        ")) OR (ne.event_kind='SurfaceBound'
        AND ne.after_state ->> 'state_derived' = 'true'
        AND ne.after_state ->> 'authority_kind' = 'registry_only'
        AND lower(ne.after_state ->> 'owner') = ",
    );
    query.push_bind(address);
    query.push("))");
    push_history_canonicality_filter(&mut query, canonical_only);
    if let Some(namespace) = namespace {
        query.push(" AND ne.namespace = ");
        query.push_bind(namespace);
    }
    query
        .build()
        .fetch_all(pool)
        .await
        .context("failed to load retained diagnostic controller evidence")?
        .into_iter()
        .map(decode_address_history_anchor)
        .collect()
}
