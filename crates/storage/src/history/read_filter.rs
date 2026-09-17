//! The resolved form of an [`EventHistoryFilter`]: anchors expanded into selectors and the
//! registration filter validated, ready for the SQL builders.

use anyhow::{Context, Result};
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    EventHistoryFilter, EventHistoryResolverFilter, HistoryBlockWindow, HistoryOrder,
    HistoryPageOptions, HistoryScope,
    address_matches::load_address_history_selector,
    binding_anchors::{
        load_logical_name_ids_for_resource_id, load_resource_ids_for_logical_name_id,
    },
    registration_identity,
    selectors::{
        HistorySelector, name_history_selector, product_registration_history_selector,
        resource_history_selector,
    },
};

#[derive(Clone, Debug, Default)]
pub(in crate::history) struct EventHistoryReadFilter {
    pub(in crate::history) selectors: Vec<HistorySelector>,
    pub(in crate::history) registration_id: Option<Uuid>,
    /// Whether `registration_id` names a registration at all. A reservation, a registry-only
    /// resource, or a NameWrapper resource standing in for a BaseRegistrar lease does not.
    pub(in crate::history) registration_id_is_public: bool,
    pub(in crate::history) namespace: Option<String>,
    pub(in crate::history) contract_address: Option<String>,
    pub(in crate::history) event_kinds: Vec<String>,
    pub(in crate::history) bind_cursor_anchor_to_event_kinds: bool,
    pub(in crate::history) from_block: Option<i64>,
    pub(in crate::history) to_block: Option<i64>,
    pub(in crate::history) order: HistoryOrder,
    pub(in crate::history) block_window: Option<HistoryBlockWindow>,
    pub(in crate::history) resolver: Option<EventHistoryResolverFilter>,
}

impl EventHistoryReadFilter {
    pub(in crate::history) fn with_page_options(mut self, options: &HistoryPageOptions) -> Self {
        self.event_kinds = options.event_kinds.clone();
        self.bind_cursor_anchor_to_event_kinds = options.bind_cursor_anchor_to_event_kinds;
        self.order = options.order;
        self.block_window = options.block_window.clone();
        self
    }

    /// The candidate rows of a registration-scoped product read, when this is one.
    pub(in crate::history) fn product_registration(&self) -> Option<(&[String], &[Uuid])> {
        self.selectors.iter().find_map(|selector| match selector {
            HistorySelector::ProductRegistration {
                logical_name_ids,
                resource_ids,
            } => Some((logical_name_ids.as_slice(), resource_ids.as_slice())),
            _ => None,
        })
    }
}

pub(in crate::history) async fn event_history_read_filter(
    pool: &PgPool,
    filter: EventHistoryFilter,
    canonical_only: bool,
    include_candidates: bool,
) -> Result<EventHistoryReadFilter> {
    let mut selectors = Vec::new();
    let published = filter.publication_block_bounds.as_ref();
    // Diagnostics read a resource as stored; only product reads treat it as a registration.
    let registration_id = (!include_candidates)
        .then_some(filter.resource_id)
        .flatten();
    let registration_id_is_public = match registration_id {
        Some(registration_id) => {
            registration_identity::is_public_registration_id(pool, registration_id, canonical_only)
                .await
                .with_context(|| {
                    format!("failed to validate public registration_id {registration_id}")
                })?
        }
        None => false,
    };

    if let Some(logical_name_id) = filter.logical_name_id.as_deref() {
        let resource_ids =
            load_resource_ids_for_logical_name_id(pool, logical_name_id, canonical_only, published)
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
        let logical_name_ids =
            load_logical_name_ids_for_resource_id(pool, resource_id, canonical_only, published)
                .await
                .with_context(|| {
                    format!(
                        "failed to load event history surface anchors for resource_id {resource_id}"
                    )
                })?;
        selectors.push(if include_candidates {
            resource_history_selector(resource_id, &logical_name_ids, HistoryScope::Both)
        } else {
            // A lease's rows also live on the NameWrapper resources that wrapped it, so the
            // candidates are every resource of the lease's names; the registration filter then
            // keeps only the rows that belong to this lease.
            let mut resource_ids = vec![resource_id];
            for logical_name_id in &logical_name_ids {
                resource_ids.extend(
                    load_resource_ids_for_logical_name_id(
                        pool,
                        logical_name_id,
                        canonical_only,
                        published,
                    )
                    .await?,
                );
            }
            resource_ids.sort_unstable();
            resource_ids.dedup();
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
                published,
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
