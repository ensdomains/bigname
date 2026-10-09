//! Resolvability on a cut-over chain. A chain is cut over while its deployment profile admits an
//! ENSv2 root registry (`families::control::cutover`). The ENSv2 Universal Resolver walks only
//! ENSv2 registries. A `.eth` label without a live `eth` registry entry has no resolver there,
//! because the deployment registers `eth` in the root registry without one. The name and every
//! name below it resolve to nothing, even while ENSv1 still records them. A live reservation
//! keeps resolving through `ENSV1Resolver`, which reads the ENSv1 registry. On a chain that is
//! not cut over, resolution starts at ENSv1 and this rule does not apply.
//! (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/UniversalResolverV2.sol:L55-L63 @ ens_v2_sepolia_20260916@366de741)
//! (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/libraries/LibResolution.sol:L58-L85 @ ens_v2_sepolia_20260916@366de741)
//! (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L283-L286 @ ens_v2_sepolia_20260916@366de741)
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/deploy/01_ETHRegistry.ts:L39-L51 @ ens_v2_sepolia_20261001@07e55a05)
//! (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/resolver/ENSV1Resolver.sol:L40-L43 @ ens_v2_sepolia_20260916@366de741)
//!
//! The rule withholds only what resolution serves (the resolver and records) from a name ENSv1
//! decides; ENSv2-decided names already follow the ENSv2 registry, and ownership and the
//! registration stay as authority selection decides them.
use std::collections::BTreeMap;

use anyhow::Result;
use sqlx::PgConnection;

use crate::families::control::lifecycle::{
    AuthoritySelection, NameFacts, NameInput, NamePlace, has_live_ens_v2_entry, load_name_facts_on,
};

/// Whether each `.eth` second-level name a batch needs has a live ENSv2 entry, read only when
/// the chain is cut over.
#[derive(Default)]
pub(super) struct Resolvability {
    cutover: bool,
    live: BTreeMap<String, bool>,
}

impl Resolvability {
    /// Read the entries the names of `facts` need: each `.eth` second-level name's own, and the
    /// second-level ancestor of each name below one, loaded when the batch does not hold it.
    pub(super) async fn load(
        conn: &mut PgConnection,
        chain_id: &str,
        facts: &[NameFacts],
    ) -> Result<Self> {
        let cutover = facts.iter().any(|facts| facts.resolution_cutover);
        if !cutover {
            return Ok(Self::default());
        }
        let mut live = BTreeMap::new();
        for facts in facts {
            if facts.input.place == NamePlace::EthSecondLevel {
                live.insert(
                    facts.input.logical_name_id.clone(),
                    has_live_ens_v2_entry(facts)?,
                );
            }
        }
        let missing: Vec<NameInput> = facts
            .iter()
            .filter_map(|facts| match &facts.input.place {
                NamePlace::BelowEthSecondLevel(parent) if !live.contains_key(parent) => {
                    Some(parent.clone())
                }
                _ => None,
            })
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .filter_map(|parent| {
                let namehash = parent.split_once(':')?.1.to_ascii_lowercase();
                Some(NameInput {
                    logical_name_id: parent,
                    namehash,
                    selection: AuthoritySelection::default(),
                    place: NamePlace::EthSecondLevel,
                })
            })
            .collect();
        for parent in load_name_facts_on(conn, chain_id, &missing).await? {
            live.insert(
                parent.input.logical_name_id.clone(),
                has_live_ens_v2_entry(&parent)?,
            );
        }
        Ok(Self { cutover, live })
    }

    /// Whether the name, decided by `arm`, resolves to nothing through the Universal Resolver.
    pub(super) fn unresolvable(&self, facts: &NameFacts, arm: Option<&str>) -> bool {
        if !self.cutover || arm != Some("ens_v1") {
            return false;
        }
        let second_level = match &facts.input.place {
            NamePlace::EthSecondLevel => &facts.input.logical_name_id,
            NamePlace::BelowEthSecondLevel(parent) => parent,
            _ => return false,
        };
        !self.live.get(second_level).copied().unwrap_or(false)
    }
}
