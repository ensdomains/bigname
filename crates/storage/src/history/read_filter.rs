use anyhow::{Context, Result};
use sqlx::PgPool;
#[cfg(any(test, feature = "test-support"))]
use uuid::Uuid;

use super::{
    EventHistoryFilter, EventHistoryReadFilter, EventHistoryResolverFilter, HistoryScope,
    address_matches::load_address_history_selector,
    registration_identity,
    selectors::{
        name_history_selector, product_registration_history_selector, resource_history_selector,
    },
    wrapped_registrar,
};

#[rustfmt::skip]
pub(super) async fn event_history_read_filter(
    pool: &PgPool,
    filter: EventHistoryFilter,
    canonical_only: bool,
    include_candidates: bool,
) -> Result<EventHistoryReadFilter> {
    let mut selectors = Vec::new();
    let registration_id = (!include_candidates).then_some(filter.resource_id).flatten();
    let registration_id_is_public = match registration_id {
        Some(registration_id) => registration_identity::is_public_registration_id(
            pool,
            registration_id,
            canonical_only,
        )
        .await
        .with_context(|| {
            format!("failed to validate public registration_id {registration_id}")
        })?,
        None => false,
    };

    if let Some(logical_name_id) = filter.logical_name_id.as_deref() {
        let resource_ids =
            wrapped_registrar::load_resource_ids_for_logical_name_id(
                pool,
                logical_name_id,
                canonical_only,
                filter.publication_block_bounds.as_ref(),
            )
                .await
                .with_context(|| {
                    format!(
                        "failed to load event history resource anchors for logical_name_id {logical_name_id}"
                    )
                })?;
        selectors.push(name_history_selector(
            logical_name_id,
            &resource_ids,
            HistoryScope::Both,
        ));
    }

    if let Some(resource_id) = filter.resource_id {
        let logical_name_ids = wrapped_registrar::load_logical_name_ids_for_resource_id(
            pool,
            resource_id,
            canonical_only,
            filter.publication_block_bounds.as_ref(),
        )
        .await
        .with_context(|| {
            format!("failed to load event history surface anchors for resource_id {resource_id}")
        })?;
        let mut resource_ids = vec![resource_id];
        for logical_name_id in &logical_name_ids {
            resource_ids.extend(wrapped_registrar::load_resource_ids_for_logical_name_id(pool, logical_name_id, canonical_only, filter.publication_block_bounds.as_ref()).await?);
        }
        resource_ids.sort_unstable(); resource_ids.dedup();
        selectors.push(if include_candidates {
            resource_history_selector(resource_id, &logical_name_ids, HistoryScope::Both)
        } else {
            product_registration_history_selector(
                resource_ids,
                if registration_id_is_public {
                    logical_name_ids
                } else {
                    Vec::new()
                },
            )
        });
    }

    if let Some(address_filter) = filter.address.as_ref() {
        let normalized_address = address_filter.address.to_ascii_lowercase();
        let relations = address_filter.relation.into_iter().collect::<Vec<_>>();
        let relations = (!relations.is_empty()).then_some(relations.as_slice());
        selectors.push(
            load_address_history_selector(
                pool,
                &normalized_address,
                filter.namespace.as_deref(),
                relations,
                HistoryScope::Both,
                canonical_only,
                include_candidates,
                filter.publication_block_bounds.as_ref(),
            )
            .await
            .with_context(|| {
                let mut parts = vec![format!("address {normalized_address}")];
                if let Some(namespace) = filter.namespace.as_ref() {
                    parts.push(format!("namespace {namespace}"));
                }
                if let Some(relation) = address_filter.relation {
                    parts.push(format!("relation {}", relation.as_str()));
                }
                format!(
                    "failed to load event history address anchors for {}",
                    parts.join(" ")
                )
            })?,
        );
    }

    Ok(EventHistoryReadFilter {
        selectors,
        registration_id,
        registration_id_is_public,
        namespace: filter.namespace,
        contract_address: filter
            .contract_address
            .map(|address| address.to_ascii_lowercase()),
        event_kinds: filter.event_kinds,
        bind_cursor_anchor_to_event_kinds: filter.bind_cursor_anchor_to_event_kinds,
        from_block: filter.from_block,
        to_block: filter.to_block,
        order: filter.order,
        block_window: filter.block_window,
        resolver: filter.resolver.map(|resolver| EventHistoryResolverFilter {
            chain_id: resolver.chain_id,
            address: resolver.address.to_ascii_lowercase(),
        }),
    })
}

#[cfg(any(test, feature = "test-support"))]
#[rustfmt::skip]
pub async fn explain_registration_history_filter_for_test(pool: &PgPool, registration_id: Uuid, logical_name_id: &str, chain_id: &str, namespace: &str, namehash: &str) -> Result<String> {
    let filter = event_history_read_filter(pool, EventHistoryFilter { resource_id: Some(registration_id), ..EventHistoryFilter::default() }, true, false).await?;
    super::query_plan::explain_history_filter_for_test(pool, filter, super::query_plan::HistoryPlanLookup { logical_name_id, registration_id, chain_id, namespace, namehash }, true).await
}
