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
    decoders::decode_address_history_anchor,
    selectors::HistorySelector,
    source::{
        push_history_canonicality_filter, push_history_lineage_join,
        push_readable_anchored_row_filter,
    },
};

pub(super) const ENS_V1_AUTHORITY_DERIVATION_KIND: &str = "ens_v1_unwrapped_authority";
pub(super) const ENS_V2_REGISTRY_DERIVATION_KIND: &str = "ens_v2_registry_resource_surface";
const ADDRESS_HISTORY_MATCH_DERIVATION_KINDS: &[&str] = &[
    ENS_V1_AUTHORITY_DERIVATION_KIND,
    ENS_V2_REGISTRY_DERIVATION_KIND,
];
const ADDRESS_HISTORY_MATCH_EVENT_KINDS: &[&str] = &[
    "RegistrationGranted",
    "TokenControlTransferred",
    "AuthorityTransferred",
];

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

    Ok(match scope {
        HistoryScope::Surface => HistorySelector::logical_names(logical_name_ids),
        HistoryScope::Resource => HistorySelector::resources(resource_ids),
        HistoryScope::Both => {
            HistorySelector::logical_names_or_resources(logical_name_ids, resource_ids)
        }
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

/// The names and resources an address held in history: registration grants, token transfers
/// and registry ownership transfers whose new holder is `address`. Each arm matches one partial
/// expression index on `normalized_events` (`normalized_events_address_*_match_idx`).
pub(super) fn push_historical_address_matches_query<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    address: &'a str,
    namespace: Option<&'a str>,
    relations: Option<&'a [AddressNameRelation]>,
    canonical_only: bool,
    include_candidates: bool,
    published: Option<&'a std::collections::BTreeMap<String, i64>>,
) {
    builder.push(
        r#"
        SELECT DISTINCT
            ne.logical_name_id,
            ne.resource_id
        FROM normalized_events ne
        LEFT JOIN resources r
          ON r.resource_id = ne.resource_id
        LEFT JOIN bigname_phase.chain_lineage resource_lineage
          ON resource_lineage.chain_id = r.chain_id
         AND resource_lineage.block_hash = r.block_hash
        "#,
    );
    push_history_lineage_join(builder);
    if include_candidates {
        builder.push(" WHERE ne.derivation_kind IN (");
    } else {
        builder.push(" WHERE ne.consumer_visibility = 'activated' AND ne.derivation_kind IN (");
    }
    let mut separated = builder.separated(", ");
    for derivation_kind in ADDRESS_HISTORY_MATCH_DERIVATION_KINDS {
        separated.push_bind(*derivation_kind);
    }
    separated.push_unseparated(") AND ne.event_kind IN (");
    let mut separated = builder.separated(", ");
    for event_kind in ADDRESS_HISTORY_MATCH_EVENT_KINDS {
        separated.push_bind(*event_kind);
    }
    separated.push_unseparated(")");

    push_history_canonicality_filter(builder, canonical_only);
    if canonical_only {
        builder.push(" AND (ne.resource_id IS NULL OR (TRUE ");
        push_readable_anchored_row_filter(builder, "r", "resource_lineage");
        builder.push("))");
    }

    if let Some(namespace) = namespace {
        builder.push(" AND ne.namespace = ");
        builder.push_bind(namespace);
    }

    if let Some(bounds) = published {
        if bounds.is_empty() {
            builder.push(" AND FALSE");
        } else {
            builder.push(" AND (");
            for (index, (chain, block)) in bounds.iter().enumerate() {
                if index > 0 {
                    builder.push(" OR ");
                }
                builder.push("(ne.chain_id = ");
                builder.push_bind(chain);
                builder.push(" AND ne.block_number <= ");
                builder.push_bind(*block);
                builder.push(")");
            }
            builder.push(")");
        }
    }
    builder.push(" AND ");
    push_address_match_filter(builder, address, relations);
}

fn push_address_match_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    address: &'a str,
    relations: Option<&'a [AddressNameRelation]>,
) {
    let include_token_holder =
        relations.is_none_or(|relations| relations.contains(&AddressNameRelation::TokenHolder));
    let include_controller = relations
        .is_none_or(|relations| relations.contains(&AddressNameRelation::EffectiveController));

    builder.push("(");
    let mut needs_or = false;
    if include_token_holder {
        push_registrant_match_filter(builder, address);
        builder.push(" OR ");
        push_token_holder_match_filter(builder, address);
        needs_or = true;
    }
    // A name with no token is owned by its registry owner, so `owner` also matches the registry
    // ownership transfers `manager` does, except from the position a registry-only binding that
    // stands for a BaseRegistrar lease opens: the lease's holder owns the name after a transfer
    // without `reclaim`, and a released lease has no owner. The registry-only resource is one
    // per node, so an earlier tokenless owner of it keeps its history.
    if include_token_holder || include_controller {
        if needs_or {
            builder.push(" OR ");
        }
        push_registry_owner_match_filter(builder, address, !include_controller);
        needs_or = true;
    }
    if !needs_or {
        builder.push("FALSE");
    }
    builder.push(")");
}

fn push_registrant_match_filter<'a>(builder: &mut QueryBuilder<'a, Postgres>, address: &'a str) {
    builder.push(
        r#"
        (
            (
                r.token_lineage_id IS NOT NULL
                OR ne.namespace =
        "#,
    );
    builder.push_bind("basenames");
    builder.push(" OR ne.derivation_kind = ");
    builder.push_bind(ENS_V2_REGISTRY_DERIVATION_KIND);
    builder.push(
        r#"
            )
            AND (
                (
                    ne.event_kind = 'RegistrationGranted'
                    AND LOWER(COALESCE(ne.after_state ->> 'registrant', '')) =
        "#,
    );
    builder.push_bind(address);
    // A lease registered straight into the NameWrapper names it as registrant, but the wrapped
    // token's holder owns the name; `registerAndWrapETH2LD` wraps it in the same transaction.
    builder.push(
        r#"
                    AND NOT EXISTS (
                        SELECT 1 FROM normalized_events wrap
                        WHERE wrap.chain_id = ne.chain_id
                          AND wrap.block_number = ne.block_number
                          AND wrap.transaction_index IS NOT DISTINCT FROM ne.transaction_index
                          AND wrap.source_family = 'ens_v1_wrapper_l1'
                          AND wrap.after_state ->> 'wrapped_registrar_resource_id'
                              = ne.resource_id::text
                          AND LOWER(wrap.raw_fact_ref ->> 'emitting_address')
                              = LOWER(ne.after_state ->> 'registrant')
                          AND wrap.canonicality_state
                              <> 'orphaned'::bigname_phase.canonicality_state
                    )
                )
            )
        )
        "#,
    );
}

fn push_token_holder_match_filter<'a>(builder: &mut QueryBuilder<'a, Postgres>, address: &'a str) {
    builder.push(
        r#"
        (
            (
                r.token_lineage_id IS NOT NULL
                OR ne.namespace =
        "#,
    );
    builder.push_bind("basenames");
    builder.push(" OR ne.derivation_kind = ");
    builder.push_bind(ENS_V2_REGISTRY_DERIVATION_KIND);
    builder.push(
        r#"
            )
            AND (
                (
                    ne.event_kind = 'TokenControlTransferred'
                    AND LOWER(COALESCE(ne.after_state ->> 'to', '')) =
        "#,
    );
    builder.push_bind(address);
    builder.push(
        r#"
                )
            )
        )
        "#,
    );
}

fn push_registry_owner_match_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    address: &'a str,
    owner_only: bool,
) {
    builder.push(
        r#"
        (
            (
                r.token_lineage_id IS NULL
                OR ne.derivation_kind =
        "#,
    );
    builder.push_bind(ENS_V2_REGISTRY_DERIVATION_KIND);
    builder.push(
        r#"
            )
            AND ne.event_kind = 'AuthorityTransferred'
            AND (ne.after_state ->> 'owner_word_unmasked' = 'true') IS NOT TRUE
            AND LOWER(COALESCE(ne.after_state ->> 'owner', '')) =
        "#,
    );
    builder.push_bind(address);
    if owner_only {
        // As the served owner: a write the admitted Graveyard holds, or one the registry getter
        // reports as zero, names no owner.
        builder.push(
            r#"
            AND (ne.after_state ->> 'owner_getter_reason') IS DISTINCT FROM 'graveyard'
            AND LOWER(COALESCE(ne.after_state ->> 'owner_getter', ''))
                <> '0x0000000000000000000000000000000000000000'
            AND NOT EXISTS (
                SELECT 1
                FROM bigname_phase.project_binding_candidate handoff
                WHERE handoff.chain_id = ne.chain_id
                  AND handoff.resource_id = ne.resource_id
                  AND handoff.registry_only
                  AND ROW(handoff.block_number, COALESCE(handoff.transaction_index, -1),
                          COALESCE(handoff.log_index, -1))
                      <= ROW(ne.block_number, COALESCE(ne.transaction_index, -1),
                             COALESCE(ne.log_index, -1))
                  AND EXISTS (
                      SELECT 1 FROM normalized_events lease
                      WHERE lease.resource_id = handoff.lease_resource_id
                        AND lease.source_family = 'ens_v1_registrar_l1'
                        AND lease.canonicality_state <> 'orphaned'::bigname_phase.canonicality_state
                  )
            )
            "#,
        );
    }
    builder.push(")");
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
