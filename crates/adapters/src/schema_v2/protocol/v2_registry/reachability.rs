//! An ENSv2 registry token's resolver pointer is a discovery edge only while the registry is
//! reachable: while Interpret's suffix walk from the registry, through live mounts or a parent
//! claim that points back, reaches an anchor. That walk is the one that decides whether the
//! registry's tokens can have names. Any contract can announce itself with `RegistryCreated()`,
//! so an announced registry that nothing reachable mounts adds no resolver edge, and with it no
//! resolver classification, Project work block or name selection. Its events are still
//! interpreted, because a later mount names the entries it already holds.
use alloy_primitives::{Address, U256};
use alloy_sol_types::SolEvent;
use anyhow::Context;
use std::collections::BTreeMap;

use crate::{
    evm_abi::address_hex,
    schema_v2::{
        model::RawLogInput,
        protocol::{DiscoveryDraft, Interpreted},
        state::State,
    },
};

/// The discovery key of a registry token's pointer. Token ids that differ only in the version
/// bits share it. A pointer opened when the registry became reachable carries the key the
/// registry's own next pointer log closes.
pub(super) fn observation_key(registry: &str, token_id: U256, resolver: bool) -> String {
    let mut bytes = token_id.to_be_bytes::<32>();
    bytes[28..].fill(0);
    let base = format!(
        "{}:{:#x}",
        registry.to_ascii_lowercase(),
        U256::from_be_bytes(bytes)
    );
    if resolver {
        format!("resolver:{base}")
    } else {
        base
    }
}

/// The `ResolverUpdated` topic. A reachability edge runs from the source the registry's own
/// `ResolverUpdated` selects, so it carries the manifest and rule that log's edge would.
pub(in crate::schema_v2) fn resolver_updated_topic0() -> String {
    format!("{:#x}", super::ResolverUpdated::SIGNATURE_HASH)
}

/// The edge a `ResolverUpdated` log opens, when its registry is reachable after the log.
pub(super) fn push_resolver_edge(
    output: &mut Interpreted,
    state: &State,
    raw: &RawLogInput,
    token_id: U256,
    resolver: Address,
) {
    if !state.v2_registry_reachable(&raw.emitting_address, raw.block_timestamp.unix_timestamp()) {
        return;
    }
    output.discovery.push(DiscoveryDraft::Edge {
        edge_kind: "resolver".to_owned(),
        to_address: address_hex(resolver),
        admission_basis: "protocol_event".to_owned(),
        observation_key: observation_key(&raw.emitting_address, token_id, true),
    });
}

/// The resolver edges of every registry whose reachability changed since the last call. A
/// registry that became reachable opens one edge per key a token with a resolver holds: its own,
/// or the earlier token ids it was regenerated from. When two tokens hold one key, the greater
/// token id's resolver wins, so a key has one target. A registry that stopped being reachable
/// closes every such key at once, shared or not, because all of its tokens lose reachability
/// together.
pub(in crate::schema_v2) fn reachability_drafts(
    state: &mut State,
) -> anyhow::Result<Vec<DiscoveryDraft>> {
    let parse = |token_id: &str| {
        token_id
            .parse::<U256>()
            .with_context(|| format!("stored ENSv2 token ID {token_id} is malformed"))
    };
    let mut drafts = Vec::new();
    for (registry, reachable) in state.take_v2_reachability_changes() {
        let mut targets = BTreeMap::new();
        for (token_id, resolver, aliases) in state.v2_registry_resolver_tokens(&registry) {
            let mut tokens = aliases
                .iter()
                .map(|alias| parse(alias))
                .collect::<anyhow::Result<Vec<_>>>()?;
            if !reachable || tokens.is_empty() {
                tokens.push(parse(&token_id)?);
            }
            for token in tokens {
                targets.insert(observation_key(&registry, token, true), resolver.clone());
            }
        }
        drafts.extend(targets.into_iter().map(|(observation_key, to_address)| {
            if reachable {
                DiscoveryDraft::RegistryResolverEdge {
                    registry: registry.clone(),
                    observation_key,
                    to_address,
                }
            } else {
                DiscoveryDraft::RegistryResolverClose {
                    registry: registry.clone(),
                    observation_key,
                }
            }
        }));
    }
    Ok(drafts)
}
