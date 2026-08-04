use std::future::{Future, ready};

use futures_util::future::join_all;
use sqlx::PgPool;

use crate::{
    ChainRpcUrls, EnsPrimaryNameLookup, LookupError, LookupRequest, LookupResponse, Result,
    call::{RecordCallContext, execute_record_call},
    primary_name::{EnsPrimaryNameRequest, lookup_ens_primary_name},
    rpc::JsonRpcHttpClient,
    store::{load_ens_primary_name_authority, load_snapshot, persist_comparisons},
};

/// Executes a live, hash-pinned lookup against schema-v2 projected state.
#[derive(Clone, Debug)]
pub struct LookupEngine {
    pool: PgPool,
    rpc_urls: ChainRpcUrls,
}

impl LookupEngine {
    pub fn new(pool: PgPool, rpc_urls: ChainRpcUrls) -> Self {
        Self { pool, rpc_urls }
    }

    pub async fn lookup(&self, request: LookupRequest) -> Result<LookupResponse> {
        self.lookup_before_persist(request, || ready(())).await
    }

    /// Resolves and forward-verifies an ENS address primary name at the readable head.
    pub async fn lookup_ens_primary_name(
        &self,
        normalized_address: &str,
    ) -> Result<EnsPrimaryNameLookup> {
        let authority = load_ens_primary_name_authority(&self.pool).await?;
        lookup_ens_primary_name(EnsPrimaryNameRequest {
            normalized_address,
            registry_address: &authority.registry_address,
            universal_resolver_address: &authority.universal_resolver_address,
            block_number: authority.block_number,
            block_hash: &authority.block_hash,
            chain_rpc_urls: &self.rpc_urls,
        })
        .await
    }

    async fn lookup_before_persist<F, Fut>(
        &self,
        request: LookupRequest,
        before_persist: F,
    ) -> Result<LookupResponse>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = ()>,
    {
        let snapshot = load_snapshot(&self.pool, &request).await?;
        let endpoint = self
            .rpc_urls
            .url_for(&snapshot.entrypoint_chain_id)
            .ok_or_else(|| {
                LookupError::configuration(format!(
                    "lookup RPC provider for {} is not configured",
                    snapshot.entrypoint_chain_id
                ))
            })?;
        let rpc =
            JsonRpcHttpClient::new_for_rpc_urls(endpoint, &self.rpc_urls).map_err(|error| {
                LookupError::configuration(format!(
                    "lookup RPC provider for {} is invalid: {error:#}",
                    snapshot.entrypoint_chain_id
                ))
            })?;
        let context = RecordCallContext {
            dns_name: &snapshot.dns_name,
            node: snapshot.node,
            entrypoint_address: &snapshot.entrypoint_address,
            block: &snapshot.execution_block,
            follow_ccip: snapshot.follow_ccip,
            result_abi: snapshot.result_abi,
            rpc: &rpc,
        };
        let calls = request
            .records
            .iter()
            .map(|record| execute_record_call(&context, record));
        let mut records = join_all(calls)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        before_persist().await;
        persist_comparisons(&self.pool, &snapshot, &mut records).await?;

        Ok(LookupResponse {
            logical_name_id: snapshot.logical_name_id,
            name: snapshot.name,
            resolver_chain_id: snapshot.resolver_chain_id,
            resolver_address: snapshot.resolver_address,
            entrypoint_chain_id: snapshot.entrypoint_chain_id,
            entrypoint_address: snapshot.entrypoint_address,
            observed_positions: snapshot.observed_positions,
            records,
        })
    }

    #[cfg(test)]
    pub(crate) async fn lookup_with_before_persist<F, Fut>(
        &self,
        request: LookupRequest,
        before_persist: F,
    ) -> Result<LookupResponse>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = ()>,
    {
        self.lookup_before_persist(request, before_persist).await
    }
}
