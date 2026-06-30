use async_graphql::{Context, Object, Result};
use bigname_storage::{
    AddressNameRelation, NameCurrentAddressFilter, NameCurrentAddressRelationFilter,
    NameCurrentListFilter, NameCurrentListOrder, NameCurrentListSort, count_name_current_list,
    load_name_current_list_page_offset, load_name_current_list_row_by_name,
    load_name_current_list_row_by_namehash,
};

use crate::state::AppState;

use super::enums::{DomainOrderBy, OrderDirection};
use super::error::internal_error;
use super::inputs::{DomainFilter, RegistrationFilter};
use super::objects::{Domain, DomainConnection, RegistrationConnection};

/// The compatibility surface is scoped to ENS names.
const NAMESPACE: &str = "ens";
/// Page size for `domains` when the subgraph `first` argument is omitted.
const DEFAULT_DOMAINS_PAGE_SIZE: u64 = 100;
/// Ceiling for client-supplied `first`, matching the REST surface's `MAX_PAGE_SIZE` so the public
/// GraphQL path cannot request an unbounded page. Larger values are clamped silently so
/// subgraph-shaped callers do not receive a GraphQL error for oversized windows.
const MAX_DOMAINS_PAGE_SIZE: u64 = crate::pagination::MAX_PAGE_SIZE;
/// Ceiling for client-supplied `skip`, so a hostile deep offset cannot force Postgres to scan an
/// arbitrary prefix of the filtered set.
const MAX_DOMAINS_SKIP: u64 = 1_000_000;

pub(crate) struct QueryRoot;

#[Object]
impl QueryRoot {
    /// `domain(id: String!)` accepts either an ENS name string (for example `"alice.eth"`) or a
    /// namehash. Resolve by name first, then fall back to the namehash, so callers do not have to
    /// signal which id form they are sending.
    async fn domain(&self, ctx: &Context<'_>, id: String) -> Result<Option<Domain>> {
        let state = ctx.data::<AppState>()?;
        let row = match load_name_current_list_row_by_name(&state.pool, NAMESPACE, &id)
            .await
            .map_err(|error| internal_error("domain", error))?
        {
            Some(row) => Some(row),
            None => load_name_current_list_row_by_namehash(&state.pool, &id)
                .await
                .map_err(|error| internal_error("domain", error))?,
        };
        Ok(row.map(Domain::from))
    }

    /// `domains(where, first, skip, orderBy, orderDirection)` — offset-paged list.
    async fn domains(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "where")] filter: Option<DomainFilter>,
        first: Option<i32>,
        skip: Option<i32>,
        #[graphql(name = "orderBy")] order_by: Option<DomainOrderBy>,
        #[graphql(name = "orderDirection")] order_direction: Option<OrderDirection>,
    ) -> Result<Vec<Domain>> {
        let limit = match first {
            Some(first) if first <= 0 => return Ok(Vec::new()),
            Some(first) => (first as u64).min(MAX_DOMAINS_PAGE_SIZE),
            None => DEFAULT_DOMAINS_PAGE_SIZE,
        };
        let offset = (skip.unwrap_or(0).max(0) as u64).min(MAX_DOMAINS_SKIP);
        let (sort, order) = storage_sort(order_by, order_direction);
        let state = ctx.data::<AppState>()?;
        let rows = load_name_current_list_page_offset(
            &state.pool,
            &domain_filter_to_storage(filter),
            sort,
            order,
            limit,
            offset,
        )
        .await
        .map_err(|error| internal_error("domains", error))?;
        Ok(rows.into_iter().map(Domain::from).collect())
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
        let count = count_name_current_list(&state.pool, &storage_filter)
            .await
            .map_err(|error| internal_error("registrationConnection", error))?;
        Ok(RegistrationConnection {
            total_count: Some(count_to_i32(count)),
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
        let count = count_name_current_list(&state.pool, &domain_filter_to_storage(filter))
            .await
            .map_err(|error| internal_error("domainConnection", error))?;
        Ok(DomainConnection {
            total_count: Some(count_to_i32(count)),
        })
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

fn domain_filter_to_storage(filter: Option<DomainFilter>) -> NameCurrentListFilter {
    let filter = filter.unwrap_or_default();
    NameCurrentListFilter {
        namespace: Some(NAMESPACE.to_owned()),
        name: filter.name,
        contains: filter.name_contains,
        address: address_membership(
            filter.owner,
            filter.owner_in,
            AddressNameRelation::TokenHolder,
        ),
        is_migrated: filter.is_migrated,
        ..Default::default()
    }
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
