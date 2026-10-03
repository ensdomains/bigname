use std::collections::BTreeSet;

use imbl::OrdMap;

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
    entry_by_parent_label: maps::Covered<maps::RegistryLabel, (String, Vec<u8>), String>,
    tokens: maps::Covered<maps::TokenKey, String, V2TokenState>,
}

struct Topology<'a> {
    anchors: &'a OrdMap<String, (String, Vec<String>)>,
    parent_claims: &'a maps::Covered<maps::RegistryClaim, String, (String, Vec<u8>)>,
    entry_by_parent_label: &'a maps::Covered<maps::RegistryLabel, (String, Vec<u8>), String>,
    tokens: &'a maps::Covered<maps::TokenKey, String, V2TokenState>,
}

type Walk = Option<(String, Vec<Vec<u8>>)>;

impl Topology<'_> {
    /// The anchor namespace and raw labels a registry's suffix walk reaches, if any.
    fn walk(
        &self,
        registry: &str,
        at_unix_timestamp: i64,
        visiting: &mut BTreeSet<String>,
    ) -> Walk {
        if let Some((namespace, suffix)) = self.anchors.get(registry) {
            let labels = suffix.iter().map(|label| label.as_bytes().to_vec());
            return Some((namespace.clone(), labels.collect()));
        }
        if !visiting.insert(registry.to_owned()) {
            return None;
        }
        let result = self
            .parent_claims
            .get(registry)
            .and_then(|(parent, label)| {
                let token_key = self
                    .entry_by_parent_label
                    .get(&(parent.clone(), label.clone()))?;
                let entry = self.tokens.get(token_key)?;
                if !v2_expiry_is_live(entry.expiry, at_unix_timestamp)
                    || entry.subregistry.as_deref() != Some(registry)
                {
                    return None;
                }
                let (namespace, mut suffix) = self.walk(parent, at_unix_timestamp, visiting)?;
                suffix.insert(0, label.clone());
                Some((namespace, suffix))
            });
        visiting.remove(registry);
        result
    }
}

impl State {
    fn v2_topology(&self) -> Topology<'_> {
        Topology {
            anchors: &self.v2_suffix_anchors,
            parent_claims: &self.v2_parent_claims,
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
            .walk(
                &registry.to_ascii_lowercase(),
                at_unix_timestamp,
                &mut BTreeSet::new(),
            )
            .filter(|(anchor_namespace, _)| anchor_namespace == namespace)
            .map(|(_, suffix)| suffix)
    }

    /// Records the topology the names just refreshed were derived from.
    pub(super) fn remember_v2_topology(&mut self, at_unix_timestamp: i64) {
        self.v2_topology_baseline = Some(V2TopologyBaseline {
            at_unix_timestamp,
            anchors: self.v2_suffix_anchors.clone(),
            parent_claims: self.v2_parent_claims.clone(),
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
            entry_by_parent_label: &baseline.entry_by_parent_label,
            tokens: &baseline.tokens,
        }
        .walk(registry, baseline.at_unix_timestamp, &mut BTreeSet::new());
        let after = self
            .v2_topology()
            .walk(registry, at_unix_timestamp, &mut BTreeSet::new());
        before != after
    }
}
