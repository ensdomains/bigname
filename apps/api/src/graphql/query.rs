use async_graphql::{Context, ID, Object, Result};
use bigname_storage::{
    AddressNameRelation, NameCurrentAddressFilter, NameCurrentAddressRelationFilter,
    NameCurrentListFilter, NameCurrentListOrder, NameCurrentListSort,
};

use crate::state::AppState;

use super::enums::{DomainOrderBy, OrderDirection, SubgraphErrorPolicy};
use super::error::internal_error;
use super::inputs::{BlockHeight, DomainFilter, RegistrationFilter};
use super::meta::{SubgraphMeta, resolve_meta};
use super::name_queries::{
    count_phase_graphql_name_list, load_phase_graphql_name_list_page_offset,
    load_phase_graphql_name_row_by_name, load_phase_graphql_name_row_by_namehash,
};
use super::objects::{Domain, DomainConnection, RegistrationConnection};
use super::snapshot::{
    graphql_snapshot_chain_ids, load_graphql_entity_head, load_graphql_head, require_count_at_head,
    require_rows_at_head, revalidate_graphql_head,
};

/// The compatibility surface is scoped to ENS names.
const NAMESPACE: &str = "ens";
/// Page size for `domains` when the subgraph `first` argument is omitted.
const DEFAULT_DOMAINS_PAGE_SIZE: u64 = 100;
/// Ceiling for client-supplied `first`, matching the REST surface's `MAX_PAGE_SIZE` so the public
/// GraphQL path cannot request an unbounded page. Larger values are clamped silently so
/// subgraph-shaped callers do not receive a GraphQL error for oversized windows.
const MAX_DOMAINS_PAGE_SIZE: u64 = crate::v2::MAX_PAGE_SIZE;
/// Ceiling for client-supplied `skip`, so a hostile deep offset cannot force Postgres to scan an
/// arbitrary prefix of the filtered set.
const MAX_DOMAINS_SKIP: u64 = 1_000_000;

pub(crate) struct Query;

#[Object]
impl Query {
    /// `domain(id: ID!)` accepts either an ENS name string (for example `"alice.eth"`) or a
    /// namehash. Resolve by name first, then fall back to the namehash, so callers do not have to
    /// signal which id form they are sending.
    async fn domain(
        &self,
        ctx: &Context<'_>,
        id: ID,
        block: Option<BlockHeight>,
        #[graphql(name = "subgraphError", default)] subgraph_error: SubgraphErrorPolicy,
    ) -> Result<Option<Domain>> {
        let state = ctx.data::<AppState>()?;
        let head = load_graphql_entity_head(ctx, block.as_ref(), subgraph_error, "domain").await?;
        let id = id.as_str();
        let row = match load_phase_graphql_name_row_by_name(&state.pool, NAMESPACE, id)
            .await
            .map_err(|error| internal_error("domain", error))?
        {
            Some(row) => Some(row),
            None => load_phase_graphql_name_row_by_namehash(&state.pool, NAMESPACE, id)
                .await
                .map_err(|error| internal_error("domain", error))?,
        };
        if let Some(row) = row.as_ref() {
            require_rows_at_head(std::slice::from_ref(row), head.as_ref(), "domain")?;
        }
        revalidate_graphql_head(state, head.as_ref(), "domain").await?;
        Ok(row.map(|row| {
            let mut domain = Domain::from(row.row);
            domain.served_head = head;
            domain
        }))
    }

    /// `domains(where, first, skip, orderBy, orderDirection)` — offset-paged list.
    #[allow(clippy::too_many_arguments)]
    async fn domains(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "where")] filter: Option<DomainFilter>,
        first: Option<i32>,
        skip: Option<i32>,
        #[graphql(name = "orderBy")] order_by: Option<DomainOrderBy>,
        #[graphql(name = "orderDirection")] order_direction: Option<OrderDirection>,
        block: Option<BlockHeight>,
        #[graphql(name = "subgraphError", default)] subgraph_error: SubgraphErrorPolicy,
    ) -> Result<Vec<Domain>> {
        let storage_filter = domain_filter_to_storage(filter)?;
        let state = ctx.data::<AppState>()?;
        let head = load_graphql_entity_head(ctx, block.as_ref(), subgraph_error, "domains").await?;
        let limit = match first {
            Some(first) if first <= 0 => {
                revalidate_graphql_head(state, head.as_ref(), "domains").await?;
                return Ok(Vec::new());
            }
            Some(first) => (first as u64).min(MAX_DOMAINS_PAGE_SIZE),
            None => DEFAULT_DOMAINS_PAGE_SIZE,
        };
        let offset = (skip.unwrap_or(0).max(0) as u64).min(MAX_DOMAINS_SKIP);
        let (sort, order) = storage_sort(order_by, order_direction);
        let snapshot_chain_ids = graphql_snapshot_chain_ids(head.as_ref());
        let rows = load_phase_graphql_name_list_page_offset(
            &state.pool,
            &storage_filter,
            &snapshot_chain_ids,
            sort,
            order,
            limit,
            offset,
        )
        .await
        .map_err(|error| internal_error("domains", error))?;
        require_rows_at_head(&rows, head.as_ref(), "domains")?;
        revalidate_graphql_head(state, head.as_ref(), "domains").await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                let mut domain = Domain::from(row.row);
                domain.served_head = head.clone();
                domain
            })
            .collect())
    }

    /// `registrationConnection(first: 0, where) { totalCount }` — backs `OwnedNamesCount`.
    #[graphql(name = "registrationConnection")]
    async fn registration_connection(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "first")] _first: Option<i32>,
        #[graphql(name = "where")] filter: Option<RegistrationFilter>,
    ) -> Result<RegistrationConnection> {
        let filter = filter.unwrap_or_default();
        let storage_filter = NameCurrentListFilter {
            namespace: Some(NAMESPACE.to_owned()),
            address: address_membership(
                filter.registrant,
                filter.registrant_in,
                AddressNameRelation::Registrant,
            ),
            ..Default::default()
        };
        let state = ctx.data::<AppState>()?;
        let head = load_graphql_head(ctx, "registrationConnection").await?;
        let snapshot_chain_ids = graphql_snapshot_chain_ids(head.as_ref());
        let count =
            count_phase_graphql_name_list(&state.pool, &storage_filter, &snapshot_chain_ids)
                .await
                .map_err(|error| internal_error("registrationConnection", error))?;
        require_count_at_head(&count, head.as_ref(), "registrationConnection")?;
        revalidate_graphql_head(state, head.as_ref(), "registrationConnection").await?;
        Ok(RegistrationConnection {
            total_count: Some(count_to_i32(count.total_count)),
        })
    }

    /// `domainConnection(first: 0, where) { totalCount }` — backs `MigratedNamesCount`.
    #[graphql(name = "domainConnection")]
    async fn domain_connection(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "first")] _first: Option<i32>,
        #[graphql(name = "where")] filter: Option<DomainFilter>,
    ) -> Result<DomainConnection> {
        let state = ctx.data::<AppState>()?;
        let head = load_graphql_head(ctx, "domainConnection").await?;
        let snapshot_chain_ids = graphql_snapshot_chain_ids(head.as_ref());
        let storage_filter = domain_filter_to_storage(filter)?;
        let count =
            count_phase_graphql_name_list(&state.pool, &storage_filter, &snapshot_chain_ids)
                .await
                .map_err(|error| internal_error("domainConnection", error))?;
        require_count_at_head(&count, head.as_ref(), "domainConnection")?;
        revalidate_graphql_head(state, head.as_ref(), "domainConnection").await?;
        Ok(DomainConnection {
            total_count: Some(count_to_i32(count.total_count)),
        })
    }

    /// Access metadata for the same publication used by GraphQL entity reads.
    #[graphql(name = "_meta")]
    async fn meta(
        &self,
        ctx: &Context<'_>,
        block: Option<BlockHeight>,
    ) -> Result<Option<SubgraphMeta>> {
        resolve_meta(ctx, block).await
    }
}

fn storage_sort(
    order_by: Option<DomainOrderBy>,
    order_direction: Option<OrderDirection>,
) -> (NameCurrentListSort, NameCurrentListOrder) {
    let sort = match order_by.unwrap_or(DomainOrderBy::Name) {
        DomainOrderBy::CreatedAt => NameCurrentListSort::CreatedAt,
        DomainOrderBy::ExpiryDate => NameCurrentListSort::ExpiryDate,
        DomainOrderBy::RegistrationDate => NameCurrentListSort::RegistrationDate,
        // `id` has no storage sort column; map it to the name sort.
        DomainOrderBy::Id | DomainOrderBy::Name => NameCurrentListSort::Name,
    };
    let order = match order_direction.unwrap_or(OrderDirection::Asc) {
        OrderDirection::Asc => NameCurrentListOrder::Asc,
        OrderDirection::Desc => NameCurrentListOrder::Desc,
    };
    (sort, order)
}

fn domain_filter_to_storage(filter: Option<DomainFilter>) -> Result<NameCurrentListFilter> {
    let filter = filter.unwrap_or_default();
    let contains = filter
        .name_contains
        .as_deref()
        .map(crate::name_filter::normalize_name_contains)
        .transpose()
        .map_err(|error| {
            async_graphql::Error::new(format!(
                "name_contains must be a valid ENSIP-15 name substring: {}",
                error.message()
            ))
        })?;
    Ok(NameCurrentListFilter {
        namespace: Some(NAMESPACE.to_owned()),
        name: filter.name,
        contains,
        address: address_membership(
            filter.owner,
            filter.owner_in,
            AddressNameRelation::TokenHolder,
        ),
        is_migrated: filter.is_migrated,
        ..Default::default()
    })
}

/// Build a storage address-membership filter from a single address and/or an address list, under a
/// fixed relation. A *provided* list takes precedence (subgraph `owner_in`/`registrant_in`) and is
/// honoured exactly — including an empty list, which matches NOTHING (`anc.address = ANY('{}')`),
/// per the compatibility contract. Only a *missing* list (`None`) falls back to the scalar
/// `owner`/`registrant`. Addresses are lowercased to match the stored `address_names_current`
/// convention.
fn address_membership(
    single: Option<String>,
    many: Option<Vec<String>>,
    relation: AddressNameRelation,
) -> Option<NameCurrentAddressFilter> {
    let relation = NameCurrentAddressRelationFilter::Relation(relation);
    match many {
        Some(many) => {
            let many: Vec<String> = many.into_iter().map(|a| a.to_lowercase()).collect();
            Some(NameCurrentAddressFilter {
                // `address` is unused when `addresses` is set (the CTE binds `= ANY($addresses)`);
                // default it for the empty-list case where there is no first element.
                address: many.first().cloned().unwrap_or_default(),
                relation,
                addresses: Some(many),
            })
        }
        None => single.map(|address| NameCurrentAddressFilter {
            address: address.to_lowercase(),
            relation,
            addresses: None,
        }),
    }
}

/// Subgraph `totalCount` is an `Int`; saturate the storage `u64` count into `i32`.
fn count_to_i32(count: u64) -> i32 {
    i32::try_from(count).unwrap_or(i32::MAX)
}
