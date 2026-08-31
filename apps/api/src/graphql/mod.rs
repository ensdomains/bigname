//! Native subgraph-compatible GraphQL surface for the documented compatibility subset.
//!
//! Serves the minimal four-operation subset (`domain`, `domains`, `registrationConnection`,
//! `domainConnection`) over the existing `bigname_storage` reads, preserving the subgraph field
//! shapes exercised by the committed GraphQL schema fixture and API tests.
//! Resolver record fields (`texts`/`contentHash`/`addresses`) are served from the name's
//! `record_inventory_current` projection (text selector keys, retained addr/contenthash values).

mod convert;
mod enums;
mod error;
mod http;
mod inputs;
mod loader;
mod meta;
mod name_queries;
mod objects;
mod query;
mod record_inventory_query;
mod scalars;
mod schema;
mod snapshot;

#[cfg(test)]
pub(crate) use name_queries::count_phase_graphql_name_list;
pub(crate) use schema::graphql_routes;
#[cfg(test)]
pub(crate) use schema::subgraph_sdl;
#[cfg(test)]
pub(crate) use snapshot::graphql_indexing_status_test_hooks;
#[cfg(test)]
pub(crate) use snapshot::nested_inventory_test_hooks;
