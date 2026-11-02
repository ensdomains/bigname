//! An ENSv2 registry token's resolver pointer is a discovery edge only while the registry is
//! reachable: while Interpret's suffix walk from the registry, through live mounts or a parent
//! claim that points back, reaches an anchor. That walk is the one that decides whether the
//! registry's tokens can have names. Any contract can announce itself with `RegistryCreated()`,
//! so an announced registry that nothing reachable mounts adds no resolver edge, and with it no
//! resolver classification, Project work block or name selection. Its events are still
//! interpreted, because a later mount names the entries it already holds.
use alloy_primitives::U256;
use alloy_sol_types::SolEvent;
use anyhow::Context;
use std::collections::BTreeMap;

use crate::{
    evm_abi::address_hex,
    schema_v2::{
        catalog::Selected,
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
    selected: &Selected,
    state: &State,
    raw: &RawLogInput,
    event: &super::ResolverUpdated,
) {
    if !state.v2_registry_reachable(&raw.emitting_address, raw.block_timestamp.unix_timestamp()) {
        return;
    }
    match selected.event.name.as_str() {
        "ResolverUpdated" => output.discovery.push(DiscoveryDraft::Edge {
            edge_kind: "resolver".to_owned(),
            to_address: address_hex(event.resolver),
            admission_basis: "protocol_event".to_owned(),
            observation_key: observation_key(&raw.emitting_address, event.tokenId, true),
        }),
        _ => unreachable!("resolver edge dispatch selected another manifest event"),
    }
}

/// The resolver edges of every registry whose reachability changed since the last call. A
/// registry that became reachable opens one edge per key a token with a resolver holds: its own,
/// or the earlier token ids it was regenerated from. A registry that stopped being reachable
/// closes every such key at once, shared or not, because all of its tokens lose reachability
/// together.
///
/// Token ids that differ only in their version bits write one link, so a key's edge follows the
/// link's latest `ResolverUpdated`, not each token's own resolver: a key whose latest write
/// cleared it stays closed, and one whose latest write named a resolver reopens to it. A key no
/// `ResolverUpdated` wrote, such as one a regenerated token carries from a write before this
/// state, reopens to the resolver of the greatest token id holding it, so a key has one target.
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
        // Per key: a token id writing its link, and each holder's resolver by token id.
        let mut holders = BTreeMap::<String, (String, Vec<String>)>::new();
        for (token_id, resolver, aliases) in state.v2_registry_resolver_tokens(&registry) {
            let mut links = aliases.into_iter().collect::<Vec<_>>();
            if !reachable || links.is_empty() {
                links.push(token_id);
            }
            for link in links {
                let key = observation_key(&registry, parse(&link)?, true);
                holders
                    .entry(key)
                    .or_insert_with(|| (link, Vec::new()))
                    .1
                    .push(resolver.clone());
            }
        }
        for (observation_key, (link, holders)) in holders {
            let to_address = match state.v2_resolver_link_write(&registry, &link) {
                Some((_, None)) => continue,
                Some((_, Some(resolver))) => resolver.clone(),
                None => match holders.last() {
                    Some(resolver) => resolver.clone(),
                    None => continue,
                },
            };
            drafts.push(if reachable {
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
            });
        }
    }
    Ok(drafts)
}
