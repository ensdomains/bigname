use async_graphql::{ID, InputObject, MaybeUndefined};

use super::scalars::Bytes;

/// Subgraph block constraint. The endpoint currently accepts constraints satisfied by the indexed
/// state eligible for reads; it does not execute queries against historical data.
#[derive(Clone, Default, InputObject)]
#[graphql(name = "Block_height")]
pub(crate) struct BlockHeight {
    pub(crate) hash: MaybeUndefined<Bytes>,
    pub(crate) number: MaybeUndefined<i32>,
    #[graphql(name = "number_gte")]
    pub(crate) number_gte: MaybeUndefined<i32>,
}

/// Subgraph `DomainFilter`. Field names that are snake_case in the subgraph schema (`owner_in`,
/// `name_contains`) are pinned explicitly; async-graphql would otherwise camelCase them. Only
/// `owner`, `owner_in`, `name`, `name_contains`, and `isMigrated` affect storage filters; the rest
/// are declared for compatibility with subgraph-shaped variables.
#[derive(InputObject, Default)]
#[graphql(name = "DomainFilter")]
pub(crate) struct DomainFilter {
    pub(crate) id: Option<String>,
    pub(crate) owner: Option<String>,
    #[graphql(name = "owner_in")]
    pub(crate) owner_in: Option<Vec<String>>,
    pub(crate) name: Option<String>,
    #[graphql(name = "name_contains")]
    pub(crate) name_contains: Option<String>,
    #[graphql(name = "isMigrated")]
    pub(crate) is_migrated: Option<bool>,
}

/// Generated-style partial `Domain_filter` for `Query.domains`.
#[derive(InputObject, Default)]
#[graphql(name = "Domain_filter")]
pub(crate) struct DomainEntityFilter {
    pub(crate) id: Option<ID>,
    #[graphql(name = "id_not")]
    pub(crate) id_not: Option<ID>,
    #[graphql(name = "id_gt")]
    pub(crate) id_gt: Option<ID>,
    #[graphql(name = "id_gte")]
    pub(crate) id_gte: Option<ID>,
    #[graphql(name = "id_lt")]
    pub(crate) id_lt: Option<ID>,
    #[graphql(name = "id_lte")]
    pub(crate) id_lte: Option<ID>,
    #[graphql(name = "id_in")]
    pub(crate) id_in: Option<Vec<ID>>,
    #[graphql(name = "id_not_in")]
    pub(crate) id_not_in: Option<Vec<ID>>,
    pub(crate) owner: Option<String>,
    #[graphql(name = "owner_in")]
    pub(crate) owner_in: Option<Vec<String>>,
    pub(crate) name: Option<String>,
    #[graphql(name = "name_not")]
    pub(crate) name_not: Option<String>,
    #[graphql(name = "name_gt")]
    pub(crate) name_gt: Option<String>,
    #[graphql(name = "name_gte")]
    pub(crate) name_gte: Option<String>,
    #[graphql(name = "name_lt")]
    pub(crate) name_lt: Option<String>,
    #[graphql(name = "name_lte")]
    pub(crate) name_lte: Option<String>,
    #[graphql(name = "name_in")]
    pub(crate) name_in: Option<Vec<String>>,
    #[graphql(name = "name_not_in")]
    pub(crate) name_not_in: Option<Vec<String>>,
    #[graphql(name = "name_contains")]
    pub(crate) name_contains: Option<String>,
    #[graphql(name = "name_contains_nocase")]
    pub(crate) name_contains_nocase: Option<String>,
    #[graphql(name = "name_not_contains")]
    pub(crate) name_not_contains: Option<String>,
    #[graphql(name = "name_not_contains_nocase")]
    pub(crate) name_not_contains_nocase: Option<String>,
    #[graphql(name = "name_starts_with")]
    pub(crate) name_starts_with: Option<String>,
    #[graphql(name = "name_starts_with_nocase")]
    pub(crate) name_starts_with_nocase: Option<String>,
    #[graphql(name = "name_not_starts_with")]
    pub(crate) name_not_starts_with: Option<String>,
    #[graphql(name = "name_not_starts_with_nocase")]
    pub(crate) name_not_starts_with_nocase: Option<String>,
    #[graphql(name = "name_ends_with")]
    pub(crate) name_ends_with: Option<String>,
    #[graphql(name = "name_ends_with_nocase")]
    pub(crate) name_ends_with_nocase: Option<String>,
    #[graphql(name = "name_not_ends_with")]
    pub(crate) name_not_ends_with: Option<String>,
    #[graphql(name = "name_not_ends_with_nocase")]
    pub(crate) name_not_ends_with_nocase: Option<String>,
}

#[derive(InputObject, Default)]
#[graphql(name = "Account_filter")]
pub(crate) struct AccountEntityFilter {
    pub(crate) id: Option<ID>,
    #[graphql(name = "id_in")]
    pub(crate) id_in: Option<Vec<ID>>,
}

#[derive(InputObject, Default)]
#[graphql(name = "Resolver_filter")]
pub(crate) struct ResolverEntityFilter {
    pub(crate) id: Option<ID>,
    pub(crate) address: Option<Bytes>,
    pub(crate) domain: Option<String>,
}

/// Subgraph `RegistrationFilter`.
#[derive(InputObject, Default)]
#[graphql(name = "RegistrationFilter")]
pub(crate) struct RegistrationFilter {
    pub(crate) registrant: Option<String>,
    #[graphql(name = "registrant_in")]
    pub(crate) registrant_in: Option<Vec<String>>,
}
