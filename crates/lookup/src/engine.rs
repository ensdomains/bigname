use std::future::{Future, ready};

use futures_util::future::join_all;
use sqlx::PgPool;

use crate::{
    ChainRpcUrls, EnsPrimaryNameLookup, LookupError, LookupPosition, LookupRequest, LookupResponse,
    Result,
    call::{RecordCallContext, execute_record_call_with_resolver},
    primary_name::{EnsPrimaryNameRequest, lookup_ens_primary_name},
    rpc::JsonRpcHttpClient,
    store::{
        load_ens_primary_name_authority, load_snapshot, persist_comparisons,
        revalidate_primary_name_position,
    },
};

const MAX_CONCURRENT_RECORD_CALLS: usize = 16;

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
        self.lookup_before_persist_at_positions(request, None, || ready(()))
            .await
    }

    /// Executes only when the lookup's authoritative position is present in
    /// the caller's admitted snapshot. A cross-chain execution position comes
    /// from the canonical projected row and is returned to the caller.
    pub async fn lookup_at_positions(
        &self,
        request: LookupRequest,
        admitted_positions: &[LookupPosition],
    ) -> Result<LookupResponse> {
        self.lookup_before_persist_at_positions(request, Some(admitted_positions), || ready(()))
            .await
    }

    /// Resolves and forward-verifies an ENS address primary name at the readable head.
    pub async fn lookup_ens_primary_name(
        &self,
        normalized_address: &str,
    ) -> Result<EnsPrimaryNameLookup> {
        self.lookup_ens_primary_name_gated(normalized_address, |_| ready(true))
            .await
    }

    /// As `lookup_ens_primary_name`, but `admit_forward` decides -- from the reverse-claimed name
    /// alone -- whether the forward verification call may be dispatched at all. Refusing yields a
    /// `ForwardRefused` result with the reverse answer intact and no forward call made.
    pub async fn lookup_ens_primary_name_gated<G, GFut>(
        &self,
        normalized_address: &str,
        admit_forward: G,
    ) -> Result<EnsPrimaryNameLookup>
    where
        G: FnOnce(String) -> GFut,
        GFut: Future<Output = bool>,
    {
        self.lookup_ens_primary_name_before_revalidate(normalized_address, admit_forward, || {
            ready(())
        })
        .await
    }

    async fn lookup_ens_primary_name_before_revalidate<G, GFut, F, Fut>(
        &self,
        normalized_address: &str,
        admit_forward: G,
        before_revalidate: F,
    ) -> Result<EnsPrimaryNameLookup>
    where
        G: FnOnce(String) -> GFut,
        GFut: Future<Output = bool>,
        F: FnOnce() -> Fut,
        Fut: Future<Output = ()>,
    {
        let authority = load_ens_primary_name_authority(&self.pool).await?;
        let result = lookup_ens_primary_name(
            EnsPrimaryNameRequest {
                normalized_address,
                registry_address: &authority.registry_address,
                universal_resolver_address: &authority.universal_resolver_address,
                position: &authority.position,
                chain_rpc_urls: &self.rpc_urls,
            },
            admit_forward,
        )
        .await?;
        before_revalidate().await;
        revalidate_primary_name_position(&self.pool, &authority).await?;
        Ok(result)
    }

    async fn lookup_before_persist_at_positions<F, Fut>(
        &self,
        request: LookupRequest,
        admitted_positions: Option<&[LookupPosition]>,
        before_persist: F,
    ) -> Result<LookupResponse>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = ()>,
    {
        let snapshot = load_snapshot(&self.pool, &request).await?;
        if let Some(admitted_positions) = admitted_positions {
            ensure_snapshot_positions_are_admitted(&snapshot, admitted_positions)?;
        }
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
            resolver_not_found_is_not_found: snapshot.route
                == crate::store::LookupRoute::EnsUniversalResolverDiscovery,
            rpc: &rpc,
        };
        let mut records = Vec::with_capacity(request.records.len());
        for chunk in request.records.chunks(MAX_CONCURRENT_RECORD_CALLS) {
            let calls = chunk
                .iter()
                .map(|record| execute_record_call_with_resolver(&context, record));
            let outcomes = join_all(calls)
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()?;
            records.extend(outcomes);
        }

        if snapshot.route == crate::store::LookupRoute::EnsUniversalResolverDiscovery {
            let resolver_not_found = records.iter().any(|outcome| outcome.resolver_not_found);
            let mut effective_resolvers = records
                .iter()
                .filter_map(|outcome| outcome.effective_resolver.as_deref());
            if let Some(first) = effective_resolvers.next()
                && (resolver_not_found
                    || effective_resolvers.any(|resolver| !resolver.eq_ignore_ascii_case(first)))
            {
                return Err(LookupError::execution(
                    "Universal Resolver returned inconsistent effective resolvers for one lookup",
                ));
            }
        }
        let mut records = records
            .into_iter()
            .map(|outcome| outcome.result)
            .collect::<Vec<_>>();

        before_persist().await;
        persist_comparisons(&self.pool, &snapshot, &mut records).await?;

        Ok(LookupResponse {
            logical_name_id: snapshot.logical_name_id,
            name: snapshot.name,
            resolver_chain_id: snapshot.resolver_chain_id,
            resolver_address: snapshot.resolver_address,
            entrypoint_chain_id: snapshot.entrypoint_chain_id,
            entrypoint_address: snapshot.entrypoint_address,
            authoritative_position: snapshot.authoritative_position,
            execution_position: snapshot.execution_position,
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
        self.lookup_before_persist_at_positions(request, None, before_persist)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn lookup_ens_primary_name_with_before_revalidate<F, Fut>(
        &self,
        normalized_address: &str,
        before_revalidate: F,
    ) -> Result<EnsPrimaryNameLookup>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = ()>,
    {
        self.lookup_ens_primary_name_before_revalidate(
            normalized_address,
            |_| ready(true),
            before_revalidate,
        )
        .await
    }
}

fn ensure_snapshot_positions_are_admitted(
    snapshot: &crate::store::LookupSnapshot,
    admitted_positions: &[LookupPosition],
) -> Result<()> {
    let required = &snapshot.authoritative_position;
    let admitted = admitted_positions.iter().any(|position| {
        position.chain_id == required.chain_id
            && position.block_number == required.block_number
            && position
                .block_hash
                .eq_ignore_ascii_case(&required.block_hash)
    });
    if !admitted {
        return Err(LookupError::stale(
            "lookup authoritative position is not present in the caller's admitted snapshot",
        ));
    }
    let execution_is_admitted = admitted_positions.iter().any(|position| {
        if position.chain_id != snapshot.execution_position.chain_id {
            return false;
        }
        snapshot.execution_position.block_number < position.block_number
            || (snapshot.execution_position.block_number == position.block_number
                && snapshot
                    .execution_position
                    .block_hash
                    .eq_ignore_ascii_case(&position.block_hash))
    });
    if !execution_is_admitted {
        return Err(LookupError::stale(
            "lookup execution position is not compatible with the caller's admitted snapshot",
        ));
    }
    Ok(())
}
