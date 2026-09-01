use async_graphql::dataloader::DataLoader;
use async_graphql::{Context, Object, Result, SimpleObject};
use sqlx::types::Uuid;

use super::convert::resolver_from_store;
use super::error::internal_error;
use super::loader::{RecordInventoryLoader, record_inventory_key};
use super::scalars::BigInt;
use super::snapshot::{require_inventory_at_head, revalidate_graphql_head};

/// Subgraph `Account` — the lowercased address as `id`.
#[derive(SimpleObject)]
#[graphql(name = "Account")]
pub(crate) struct Account {
    pub(crate) id: String,
}

/// Subgraph `AddressRecord` — a coin-typed address record. `coinType` is `u32`, not `i32`:
/// some EVM-style coin types can exceed the signed-32-bit GraphQL `Int` range. `coinTypeBig`
/// carries the same coin type as a decimal string for clients that reject an out-of-range
/// `coinType` value.
#[derive(SimpleObject)]
#[graphql(name = "AddressRecord")]
pub(crate) struct AddressRecord {
    #[graphql(name = "coinType")]
    pub(crate) coin_type: u32,
    #[graphql(name = "coinTypeBig")]
    pub(crate) coin_type_big: String,
    pub(crate) address: String,
}

/// Subgraph `Resolver`. `id`/`address` carry the resolver contract address; the record fields
/// (`texts`/`contentHash`/`addresses`) are
/// read from the `record_inventory_current` projection by [`resolver_from_store`] — a name whose
/// resolver has no projected records serves the empty shapes.
#[derive(SimpleObject)]
#[graphql(name = "Resolver")]
pub(crate) struct Resolver {
    pub(crate) id: String,
    pub(crate) address: String,
    pub(crate) texts: Option<Vec<String>>,
    #[graphql(name = "contentHash")]
    pub(crate) content_hash: Option<String>,
    pub(crate) addresses: Option<Vec<AddressRecord>>,
}

/// Subgraph `DomainConnection` — only `totalCount` is exercised (`MigratedNamesCount`).
#[derive(SimpleObject)]
#[graphql(name = "DomainConnection")]
pub(crate) struct DomainConnection {
    #[graphql(name = "totalCount")]
    pub(crate) total_count: Option<i32>,
}

/// Subgraph `RegistrationConnection` — only `totalCount` is exercised (`OwnedNamesCount`).
#[derive(SimpleObject)]
#[graphql(name = "RegistrationConnection")]
pub(crate) struct RegistrationConnection {
    #[graphql(name = "totalCount")]
    pub(crate) total_count: Option<i32>,
}

/// Subgraph `Domain`. A manual `#[Object]` (not `SimpleObject`) so `owner` is non-null `Account!`;
/// the storage fallback (`owner → registrant → zero-address`) is resolved in `convert.rs`.
pub(crate) struct Domain {
    pub(crate) id: String,
    pub(crate) name: Option<String>,
    pub(crate) normalized_name: Option<String>,
    pub(crate) token_id: Option<String>,
    pub(crate) created_at: BigInt,
    pub(crate) expiry_date: Option<BigInt>,
    pub(crate) resolver_address: Option<String>,
    pub(crate) owner_id: String,
    /// Resource and optional version boundary for the name's `record_inventory_current` row.
    pub(crate) record_inventory_key: Option<(Uuid, Option<serde_json::Value>)>,
    pub(crate) served_head: Option<crate::v2::lookup::head::ServedHead>,
}

#[Object]
impl Domain {
    async fn id(&self) -> &str {
        &self.id
    }

    async fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    #[graphql(name = "normalizedName")]
    async fn normalized_name(&self) -> Option<&str> {
        self.normalized_name.as_deref()
    }

    #[graphql(name = "tokenId")]
    async fn token_id(&self) -> Option<&str> {
        self.token_id.as_deref()
    }

    #[graphql(name = "createdAt")]
    async fn created_at(&self) -> &BigInt {
        &self.created_at
    }

    #[graphql(name = "expiryDate")]
    async fn expiry_date(&self) -> Option<&BigInt> {
        self.expiry_date.as_ref()
    }

    /// The page's `resolver` reads are coalesced through a DataLoader, so a list of N domains costs
    /// one batched `record_inventory_current` query (plus a fallback only for rare exact-key misses)
    /// rather than N point reads. Names without a projected inventory row (the common case until the
    /// resolver-log sweep lands) serve the empty record shapes without contributing a key.
    async fn resolver(&self, ctx: &Context<'_>) -> Result<Option<Resolver>> {
        let Some(address) = self.resolver_address.clone() else {
            return Ok(None);
        };
        let inventory = match self.record_inventory_key.as_ref() {
            Some((resource_id, boundary)) => {
                let state = ctx.data::<crate::AppState>()?;
                #[cfg(test)]
                super::snapshot::nested_inventory_test_hooks::run(&state.pool)
                    .await
                    .map_err(|error| internal_error("Domain.resolver", error))?;
                let loader = ctx.data::<DataLoader<RecordInventoryLoader>>()?;
                let inventory = loader
                    .load_one(record_inventory_key(*resource_id, boundary.as_ref()))
                    .await
                    .map_err(|error| {
                        // `{error:#}` keeps the storage layer's full anyhow cause chain in the log
                        // (plain `{error}` would flatten it to just the outermost message).
                        internal_error(
                            "Domain.resolver",
                            anyhow::anyhow!("record inventory batch load failed: {error:#}"),
                        )
                    })?;
                if let Some(inventory) = inventory.as_ref() {
                    require_inventory_at_head(
                        &inventory.chain_positions,
                        inventory.chain_id.as_deref(),
                        self.served_head.as_ref(),
                        "Domain.resolver",
                    )?;
                }
                revalidate_graphql_head(state, self.served_head.as_ref(), "Domain.resolver")
                    .await?;
                inventory
            }
            None => None,
        };
        Ok(Some(resolver_from_store(address, inventory.as_ref())))
    }

    async fn owner(&self) -> Account {
        Account {
            id: self.owner_id.clone(),
        }
    }
}
