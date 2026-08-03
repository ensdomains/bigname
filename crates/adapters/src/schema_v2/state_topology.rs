use std::collections::BTreeSet;

use super::State;
use crate::schema_v2::common::surface_labels;

impl State {
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
        self.v2_registry_raw_suffix_inner(
            &registry.to_ascii_lowercase(),
            namespace,
            at_unix_timestamp,
            &mut BTreeSet::new(),
        )
    }

    fn v2_registry_raw_suffix_inner(
        &self,
        registry: &str,
        namespace: &str,
        at_unix_timestamp: i64,
        visiting: &mut BTreeSet<String>,
    ) -> Option<Vec<Vec<u8>>> {
        if let Some((anchor_namespace, suffix)) = self.v2_suffix_anchors.get(registry) {
            return (anchor_namespace == namespace).then(|| {
                suffix
                    .iter()
                    .map(|label| label.as_bytes().to_vec())
                    .collect()
            });
        }
        if !visiting.insert(registry.to_owned()) {
            return None;
        }
        let result = self
            .v2_parent_claims
            .get(registry)
            .and_then(|(parent, label)| {
                let token_key = self
                    .v2_entry_by_parent_label
                    .get(&(parent.clone(), label.clone()))?;
                let entry = self.v2_tokens.get(token_key)?;
                let expiry = entry.expiry?;
                let now = u64::try_from(at_unix_timestamp).ok()?;
                if now >= expiry || entry.subregistry.as_deref() != Some(registry) {
                    return None;
                }
                let mut suffix = self.v2_registry_raw_suffix_inner(
                    parent,
                    namespace,
                    at_unix_timestamp,
                    visiting,
                )?;
                suffix.insert(0, label.clone());
                Some(suffix)
            });
        visiting.remove(registry);
        result
    }
}
