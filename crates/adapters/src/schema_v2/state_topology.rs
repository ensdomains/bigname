use std::collections::BTreeMap;

use imbl::{OrdMap, OrdSet};

use super::{State, V2TokenState, maps};
use crate::schema_v2::common::surface_labels;

pub(in crate::schema_v2) fn v2_expiry_is_live(expiry: Option<u64>, at_unix_timestamp: i64) -> bool {
    expiry.is_some_and(|expiry| u64::try_from(at_unix_timestamp).is_ok_and(|now| now < expiry))
}

/// The registry-level state a registry's name suffix is walked from, as it stood when names were
/// last refreshed. Every loaded token's name then matched its registry's walk, so a dirty
/// registry whose walk still gives the same result holds no token whose name changed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct V2TopologyBaseline {
    at_unix_timestamp: i64,
    anchors: OrdMap<String, (String, Vec<String>)>,
    parent_claims: maps::Covered<maps::RegistryClaim, String, (String, Vec<u8>)>,
    mounts: maps::Covered<maps::RegistryMounts, String, OrdSet<String>>,
    entry_by_parent_label: maps::Covered<maps::RegistryLabel, (String, Vec<u8>), String>,
    tokens: maps::Covered<maps::TokenKey, String, V2TokenState>,
}

struct Topology<'a> {
    anchors: &'a OrdMap<String, (String, Vec<String>)>,
    parent_claims: &'a maps::Covered<maps::RegistryClaim, String, (String, Vec<u8>)>,
    mounts: &'a maps::Covered<maps::RegistryMounts, String, OrdSet<String>>,
    entry_by_parent_label: &'a maps::Covered<maps::RegistryLabel, (String, Vec<u8>), String>,
    tokens: &'a maps::Covered<maps::TokenKey, String, V2TokenState>,
}

#[cfg(test)]
std::thread_local! {
    /// How many registries the suffix walk has entered on this thread.
    pub(super) static V2_WALK_STEPS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// An anchor namespace and the raw labels below it, leaf first.
type Path = (String, Vec<Vec<u8>>);
type Walk = Option<Path>;

/// The preferred path: the one with the fewest labels, and among paths of equal length the one
/// whose labels are smallest, compared bytewise from the anchor downward. Paths with the same
/// labels are ordered by namespace, so the pick does not depend on the order given.
pub(super) fn preferred_path(paths: impl IntoIterator<Item = Path>) -> Walk {
    paths
        .into_iter()
        .min_by(|(left_namespace, left), (right_namespace, right)| {
            left.len()
                .cmp(&right.len())
                .then_with(|| left.iter().rev().cmp(right.iter().rev()))
                .then_with(|| left_namespace.cmp(right_namespace))
        })
}

/// A parent registry and the label of its token that names a child.
type ParentEdge = (String, Vec<u8>);

impl Topology<'_> {
    /// The anchor namespace and raw labels a registry's suffix walk reaches, if any. Each
    /// registry is named by one of its parent edges: the claimed parent when its parent claim
    /// points back, otherwise the preferred path among its mounts. The walk collects the
    /// registries above `registry` once, then names them outward from the anchors, shortest
    /// paths first. Every registry in that set gets the name its own walk would give it.
    fn walk(&self, registry: &str, at_unix_timestamp: i64) -> Walk {
        let mut parents = BTreeMap::<String, Vec<ParentEdge>>::new();
        let mut pending = vec![registry.to_owned()];
        while let Some(current) = pending.pop() {
            if parents.contains_key(&current) {
                continue;
            }
            #[cfg(test)]
            V2_WALK_STEPS.set(V2_WALK_STEPS.get() + 1);
            let edges = match self.anchors.contains_key(&current) {
                true => Vec::new(),
                false => self.parent_edges(&current, at_unix_timestamp),
            };
            pending.extend(edges.iter().map(|(parent, _)| parent.clone()));
            parents.insert(current, edges);
        }
        let mut children = BTreeMap::<&str, Vec<(&str, &[u8])>>::new();
        for (child, edges) in &parents {
            for (parent, label) in edges {
                let named = children.entry(parent).or_default();
                named.push((child, label));
            }
        }
        // Anchors start at the length of their own suffix. A registry is named in the first
        // layer that reaches it, by the preferred path among that layer's candidates.
        let mut waiting = BTreeMap::<usize, BTreeMap<&str, Path>>::new();
        for name in parents.keys() {
            if let Some((namespace, suffix)) = self.anchors.get(name) {
                let labels = suffix.iter().map(|label| label.as_bytes().to_vec());
                let layer = waiting.entry(suffix.len()).or_default();
                layer.insert(name, (namespace.clone(), labels.collect()));
            }
        }
        let mut named = BTreeMap::<&str, Path>::new();
        while let Some((length, layer)) = waiting.pop_first() {
            let mut next = BTreeMap::<&str, Vec<Path>>::new();
            for (name, path) in layer {
                if named.contains_key(name) {
                    continue;
                }
                for (child, label) in children.get(name).into_iter().flatten() {
                    if !named.contains_key(child) {
                        let mut labels = path.1.clone();
                        labels.insert(0, label.to_vec());
                        next.entry(child)
                            .or_default()
                            .push((path.0.clone(), labels));
                    }
                }
                named.insert(name, path);
            }
            for (child, candidates) in next {
                // An anchor is never a child, so no earlier candidate waits in this layer.
                if let Some(path) = preferred_path(candidates) {
                    waiting.entry(length + 1).or_default().insert(child, path);
                }
            }
        }
        named.remove(registry)
    }

    /// The parent edges that can name `registry`. A parent claim that points back is the only
    /// one. Otherwise every mount is one: an unexpired token, current for its label, that
    /// points at the registry.
    fn parent_edges(&self, registry: &str, at_unix_timestamp: i64) -> Vec<ParentEdge> {
        if let Some(claim) = self.claim_pointing_back(registry, at_unix_timestamp) {
            return vec![claim.clone()];
        }
        let Some(mounts) = self.mounts.get(registry) else {
            return Vec::new();
        };
        let edges = mounts.iter().filter_map(|token_key| {
            let (parent, _) = token_key.rsplit_once(':')?;
            let entry = self.tokens.get(token_key)?;
            let label = entry.raw_label.as_ref()?;
            let parent_label = (parent.to_owned(), label.clone());
            (v2_expiry_is_live(entry.expiry, at_unix_timestamp)
                && entry.subregistry.as_deref() == Some(registry)
                && self.entry_by_parent_label.get(&parent_label) == Some(token_key))
            .then_some(parent_label)
        });
        edges.collect()
    }

    /// The registry's parent claim, when the claimed parent holds an unexpired token for the
    /// claimed label and that token points at the registry.
    fn claim_pointing_back(
        &self,
        registry: &str,
        at_unix_timestamp: i64,
    ) -> Option<&(String, Vec<u8>)> {
        let claim = self.parent_claims.get(registry)?;
        let entry = self.tokens.get(self.entry_by_parent_label.get(claim)?)?;
        (v2_expiry_is_live(entry.expiry, at_unix_timestamp)
            && entry.subregistry.as_deref() == Some(registry))
        .then_some(claim)
    }
}

impl State {
    fn v2_topology(&self) -> Topology<'_> {
        Topology {
            anchors: &self.v2_suffix_anchors,
            parent_claims: &self.v2_parent_claims,
            mounts: &self.v2_mounts_by_subregistry,
            entry_by_parent_label: &self.v2_entry_by_parent_label,
            tokens: &self.v2_tokens,
        }
    }

    pub(super) fn v2_registry_suffix(
        &self,
        registry: &str,
        namespace: &str,
        at_unix_timestamp: i64,
    ) -> Option<Vec<String>> {
        surface_labels(&self.v2_registry_raw_suffix(registry, namespace, at_unix_timestamp)?)
    }

    pub(super) fn v2_registry_raw_suffix(
        &self,
        registry: &str,
        namespace: &str,
        at_unix_timestamp: i64,
    ) -> Option<Vec<Vec<u8>>> {
        self.v2_topology()
            .walk(&registry.to_ascii_lowercase(), at_unix_timestamp)
            .filter(|(anchor_namespace, _)| anchor_namespace == namespace)
            .map(|(_, suffix)| suffix)
    }

    /// Records the topology the names just refreshed were derived from.
    pub(super) fn remember_v2_topology(&mut self, at_unix_timestamp: i64) {
        self.v2_topology_baseline = Some(V2TopologyBaseline {
            at_unix_timestamp,
            anchors: self.v2_suffix_anchors.clone(),
            parent_claims: self.v2_parent_claims.clone(),
            mounts: self.v2_mounts_by_subregistry.clone(),
            entry_by_parent_label: self.v2_entry_by_parent_label.clone(),
            tokens: self.v2_tokens.clone(),
        });
    }

    /// Whether `registry`'s suffix walk at `at_unix_timestamp` differs from the one its tokens'
    /// names were last refreshed from. Without a recorded refresh it counts as changed.
    pub(super) fn v2_registry_walk_changed(&self, registry: &str, at_unix_timestamp: i64) -> bool {
        let Some(baseline) = self.v2_topology_baseline.as_ref() else {
            return true;
        };
        let before = Topology {
            anchors: &baseline.anchors,
            parent_claims: &baseline.parent_claims,
            mounts: &baseline.mounts,
            entry_by_parent_label: &baseline.entry_by_parent_label,
            tokens: &baseline.tokens,
        }
        .walk(registry, baseline.at_unix_timestamp);
        let after = self.v2_topology().walk(registry, at_unix_timestamp);
        before != after
    }
}
