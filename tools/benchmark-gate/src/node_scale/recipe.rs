//! Deterministic topology and overlapping cohorts for the local million-name screen.
//! This describes raw-event inputs, never surfaces or projected rows.

use alloy_primitives::{B256, keccak256};
use anyhow::{Result, ensure};
use serde::Serialize;

pub(super) const RECIPE_VERSION: &str = "registry-node-scale-v1";
const GROUP_SIZE: u32 = 1_000;

#[derive(Clone, Debug)]
pub(super) struct Node {
    pub(super) ordinal: u32,
    /// None means the protocol suffix (`eth`), which has its own bootstrap log.
    pub(super) parent: Option<u32>,
    pub(super) depth_below_eth: u8,
    pub(super) raw_label: Vec<u8>,
    pub(super) labelhash: B256,
    pub(super) namehash: B256,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ByteObservation {
    None,
    Valid,
    Invalid,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ResolverCohort {
    None,
    Stable,
    Changing,
}

impl Node {
    pub(super) fn byte_observation(&self) -> ByteObservation {
        let local = self.ordinal % GROUP_SIZE;
        if (600..650).contains(&local) && local.is_multiple_of(5) {
            ByteObservation::Invalid
        } else if local % 5 == 1 {
            ByteObservation::Valid
        } else {
            ByteObservation::None
        }
    }

    pub(super) fn resolver_cohort(&self) -> ResolverCohort {
        match (self.ordinal / 5) % 10 {
            0..=4 => ResolverCohort::None,
            5..=7 => ResolverCohort::Stable,
            _ => ResolverCohort::Changing,
        }
    }

    pub(super) fn later_changes(&self) -> u32 {
        match self.ordinal % GROUP_SIZE {
            0..=799 => 0,
            800..=998 => 2 + self.ordinal % 3,
            _ => 100,
        }
    }
}

pub(super) struct Recipe {
    pub(super) nodes: Vec<Node>,
    pub(super) eth_labelhash: B256,
    pub(super) eth_namehash: B256,
}

impl Recipe {
    /// Every 1,000-name group has identical exact proportions. The 10k and 100k
    /// corpora are strict prefixes of the full topology, including hot parents.
    pub(super) fn new(names: u32) -> Result<Self> {
        ensure!(
            matches!(names, 10_000 | 100_000 | 1_000_000),
            "node-scale supports exactly the 10k, 100k and million-name corpora"
        );
        let eth_labelhash = keccak256(b"eth");
        let eth_namehash = child_hash(B256::ZERO, eth_labelhash);
        let mut recipe = Self {
            nodes: Vec::with_capacity(names as usize),
            eth_labelhash,
            eth_namehash,
        };
        for group in 0..names / GROUP_SIZE {
            recipe.add_group(group);
        }
        ensure!(recipe.nodes.len() == names as usize, "recipe size mismatch");
        Ok(recipe)
    }

    fn add_group(&mut self, group: u32) {
        let base = group * GROUP_SIZE;
        assert_eq!(self.nodes.len(), base as usize);
        // 650 depth-one names. Reserved hot and deep-chain parents are never
        // invalid-byte leaves; their descendants can therefore have valid bytes.
        for index in 0..650 {
            self.push(None, format!("root-{}", base + index));
        }
        // Across 1,000 groups: one parent with 100k children, ten with 10k each.
        for index in 0..100 {
            self.push(Some(0), format!("hot-{}", group * 100 + index));
        }
        for parent in 1..=10 {
            for index in 0..10 {
                self.push(Some(parent), format!("hot-{}", group * 10 + index));
            }
        }
        let mut chains = [[0_u32; 9]; 10];
        for (index, chain) in chains.iter_mut().enumerate() {
            chain[1] = base + 11 + index as u32;
            for depth in 2..=4 {
                chain[depth] = self.push(Some(chain[depth - 1]), format!("branch-{depth}"));
            }
        }
        // 200 hot children + 30 chain ancestors + 20 side children = 250 at 2–4.
        for index in 0..20 {
            let depth = 2 + index % 3;
            let parent = chains[index % 10][depth - 1];
            self.push(Some(parent), format!("side-{index}"));
        }
        for chain in &mut chains {
            for depth in 5..=8 {
                chain[depth] = self.push(Some(chain[depth - 1]), format!("branch-{depth}"));
            }
        }
        for index in 0..50 {
            let depth = 5 + index % 4;
            let parent = chains[index % 10][depth - 1];
            self.push(Some(parent), format!("leaf-{index}"));
        }
        // 40 chain descendants + 50 side children = 90 at depths 5–8.
        let mut deep = [0_u32; 17];
        deep[8] = chains[0][8];
        for depth in 9..=16 {
            deep[depth] = self.push(Some(deep[depth - 1]), format!("branch-{depth}"));
        }
        self.push(Some(chains[1][8]), "deep-side".to_owned());
        self.push(Some(deep[15]), "deep-side".to_owned());
        assert_eq!(self.nodes.len(), (base + GROUP_SIZE) as usize);
    }

    fn push(&mut self, parent: Option<u32>, label: String) -> u32 {
        let ordinal = self.nodes.len() as u32;
        let local = ordinal % GROUP_SIZE;
        let invalid = (600..650).contains(&local) && local.is_multiple_of(5);
        let raw_label = if invalid {
            let mut raw = vec![0xff];
            raw.extend_from_slice(label.as_bytes());
            raw
        } else if local % 10 == 1 {
            format!("é-{label}").into_bytes()
        } else {
            label.into_bytes()
        };
        let (parent_hash, depth_below_eth) = parent.map_or((self.eth_namehash, 1), |index| {
            assert!(index < ordinal, "parents precede children");
            let node = &self.nodes[index as usize];
            (node.namehash, node.depth_below_eth + 1)
        });
        let labelhash = keccak256(&raw_label);
        self.nodes.push(Node {
            ordinal,
            parent,
            depth_below_eth,
            raw_label,
            labelhash,
            namehash: child_hash(parent_hash, labelhash),
        });
        ordinal
    }

    /// Exact bytes to place in a subsequent admitted NameWrapped observation.
    /// The leaf comes first, as in the DNS wire name and stored labelhash path.
    pub(super) fn raw_labels(&self, ordinal: u32) -> Vec<&[u8]> {
        let mut labels = Vec::new();
        let mut current = Some(ordinal);
        while let Some(index) = current {
            let node = &self.nodes[index as usize];
            labels.push(node.raw_label.as_slice());
            current = node.parent;
        }
        labels.push(b"eth");
        labels
    }
}

fn child_hash(parent: B256, labelhash: B256) -> B256 {
    let mut input = [0_u8; 64];
    input[..32].copy_from_slice(parent.as_slice());
    input[32..].copy_from_slice(labelhash.as_slice());
    keccak256(input)
}
