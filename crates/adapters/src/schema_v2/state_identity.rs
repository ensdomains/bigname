use alloy_primitives::{B256, keccak256};
use anyhow::{Context, ensure};

use super::State;
use crate::schema_v2::{
    common::hash_hex, model::PriorEventInput, seam::NAME_IDENTITY_OBSERVED_KEY,
};

impl State {
    pub(in crate::schema_v2) fn restore_shared_name_evidence(
        &mut self,
        event: &PriorEventInput,
    ) -> anyhow::Result<()> {
        let proven = self.restore_v1_path(event)?;
        if event.logical_name_id.is_some() && event.after_state["visibility_state"] == "shadow" {
            ensure!(
                proven,
                "named shadow observation has no complete raw-label path"
            );
            let namehash = event
                .logical_name_id
                .as_deref()
                .and_then(|name| name.strip_prefix(&format!("{}:", event.namespace)))
                .expect("verified named path has its namespace");
            self.observe_v1_surface(&event.namespace, namehash);
        }
        Ok(())
    }

    pub(in crate::schema_v2) fn restore_v1_path(
        &mut self,
        event: &PriorEventInput,
    ) -> anyhow::Result<bool> {
        let Some(namehash) = event
            .logical_name_id
            .as_deref()
            .and_then(|name| name.strip_prefix(&format!("{}:", event.namespace)))
        else {
            return Ok(false);
        };
        let path = if event.after_state[NAME_IDENTITY_OBSERVED_KEY] == true {
            serde_json::from_value::<Vec<String>>(event.after_state["labelhashes"].clone())
                .context("structural identity observation has no complete label-hash path")?
        } else if event.event_kind == "PreimageObserved" {
            if let Some(labels) = event.after_state["raw_labels"].as_array() {
                labels
                    .iter()
                    .map(|label| {
                        label
                            .as_str()
                            .map(|text| hash_hex(text.as_bytes()))
                            .context("preimage raw label is not text")
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?
            } else if let Some(labels) = event.after_state["raw_labels_hex"].as_array() {
                labels
                    .iter()
                    .map(|label| {
                        let bytes = alloy_primitives::hex::decode(
                            label
                                .as_str()
                                .context("raw-label witness is not hex text")?,
                        )?;
                        Ok(hash_hex(&bytes))
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?
            } else {
                return Ok(false);
            }
        } else {
            return Ok(false);
        };
        if path.is_empty() && event.after_state[NAME_IDENTITY_OBSERVED_KEY] != true {
            return Ok(false);
        }
        if let Some(observed) = event.after_state["namehash"].as_str() {
            ensure!(
                observed == namehash,
                "preimage node differs from its named identity"
            );
        }
        self.remember_v1_path(&event.namespace, namehash, &path)?;
        Ok(true)
    }

    /// Full paths come from admitted structural or raw-byte observations, never label imports.
    pub(in crate::schema_v2) fn remember_v1_path(
        &mut self,
        namespace: &str,
        namehash: &str,
        labelhashes: &[String],
    ) -> anyhow::Result<()> {
        ensure!(
            !labelhashes.is_empty(),
            "name identity {namespace}:{namehash} has no path"
        );
        let mut node = B256::ZERO;
        for label in labelhashes.iter().rev() {
            let hash: B256 = label.parse().context("invalid identity label hash")?;
            ensure!(
                format!("{hash:#x}") == *label,
                "identity label hash is not canonical hex"
            );
            let mut input = [0_u8; 64];
            input[..32].copy_from_slice(node.as_slice());
            input[32..].copy_from_slice(hash.as_slice());
            node = keccak256(input);
        }
        ensure!(
            format!("{node:#x}") == namehash,
            "identity path does not hash to {namespace}:{namehash}"
        );
        self.v1_node_paths
            .insert(format!("{namespace}:{namehash}"), labelhashes.to_vec());
        Ok(())
    }

    pub(in crate::schema_v2) fn v1_node_path(
        &self,
        namespace: &str,
        namehash: &str,
    ) -> Option<Vec<String>> {
        crate::schema_v2::lookahead::observe_node(&format!("{namespace}:{namehash}"));
        if namehash == format!("{:#x}", B256::ZERO) {
            Some(Vec::new())
        } else {
            self.v1_node_paths
                .get(&format!("{namespace}:{namehash}"))
                .cloned()
        }
    }

    /// The parent node of a name whose full label-hash path is known, if any.
    pub(in crate::schema_v2) fn v1_parent_namehash(
        &self,
        namespace: &str,
        namehash: &str,
    ) -> Option<String> {
        let path = self.v1_node_path(namespace, namehash)?;
        if path.is_empty() {
            return None;
        }
        let parent_path = &path[1..];
        let mut node = B256::ZERO;
        for label in parent_path.iter().rev() {
            let hash: B256 = label.parse().ok()?;
            let mut input = [0_u8; 64];
            input[..32].copy_from_slice(node.as_slice());
            input[32..].copy_from_slice(hash.as_slice());
            node = keccak256(input);
        }
        Some(format!("{node:#x}"))
    }

    /// Record a locked boundary proven earlier in the current batch, with
    /// the position that proves it. Restored boundaries need no position:
    /// they belong to an earlier batch, so they precede every read here.
    pub(in crate::schema_v2) fn note_live_locked_migration(
        &mut self,
        logical_name_id: &str,
        position: crate::schema_v2::model::PriorWritePosition,
    ) {
        self.v2_locked_migration_names
            .insert(logical_name_id.to_owned());
        self.v2_locked_migration_positions
            .insert(logical_name_id.to_owned(), position);
    }

    /// Whether the name is a direct child of a parent that an activated
    /// locked-wrapper ENSv1→ENSv2 migration covers. Such a migration only
    /// runs when the WrapperRegistry it creates claims that parent as its
    /// original node, so the parent's restored locked ENSv1→ENSv2 migration
    /// is the proof
    /// (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L149 @ ens_v2_sepolia_20260629@ccaeb58)
    /// (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L32 @ ens_v2@a971bd64).
    /// The parent is reported as a lookahead read before the test, so a
    /// scoped restore that loaded only the child fails and retries with the
    /// parent's boundary loaded.
    pub(in crate::schema_v2) fn v1_is_locked_migrated_original_child(
        &self,
        namespace: &str,
        namehash: &str,
        at: crate::schema_v2::model::PriorWritePosition,
    ) -> bool {
        let Some(parent) = self.v1_parent_namehash(namespace, namehash) else {
            return false;
        };
        let key = format!("{namespace}:{parent}");
        crate::schema_v2::lookahead::observe_node(&key);
        if !self.v2_locked_migration_names.contains(&key) {
            return false;
        }
        match self.v2_locked_migration_positions.get(&key) {
            Some(boundary) => *boundary < at,
            None => true,
        }
    }

    pub(in crate::schema_v2) fn v1_surface_is_shadow(
        &self,
        namespace: &str,
        namehash: &str,
    ) -> bool {
        self.v1_shadow_surfaces
            .contains(&format!("{namespace}:{namehash}"))
    }

    pub(super) fn remember_v1_shadow(&mut self, logical_name_id: String) {
        self.v1_shadow_surfaces.insert(logical_name_id.clone());
        if let Some(key) = self.restoring_state_key.clone()
            && self
                .v1_shadow_sources
                .entry(key)
                .or_default()
                .insert(logical_name_id.clone())
                .is_none()
        {
            *self
                .v1_shadow_counts
                .entry(logical_name_id.clone())
                .or_default() += 1;
        }
        self.known_surfaces.remove(&logical_name_id);
        self.set_v1_surface_visibility(&logical_name_id, false);
    }

    pub(super) fn replace_v1_shadow_source(&mut self, key: &str) {
        let Some(names) = self.v1_shadow_sources.remove(key) else {
            return;
        };
        for name in names {
            let remaining = self
                .v1_shadow_counts
                .get_mut(&name)
                .expect("retained shadow count");
            *remaining -= 1;
            if *remaining == 0 {
                self.v1_shadow_counts.remove(&name);
                self.v1_shadow_surfaces.remove(&name);
                if self.restored_surface_counts.contains_key(&name) {
                    self.known_surfaces.insert(name.clone());
                    self.set_v1_surface_visibility(&name, true);
                }
            }
        }
    }

    fn set_v1_surface_visibility(&mut self, name: &str, active: bool) {
        for states in [
            &mut self.v1_names,
            &mut self.v1_registrars,
            &mut self.v1_registry_authorities,
        ] {
            if let Some(state) = states.get_mut(name) {
                state.surface_known = active;
            }
        }
        if let Some(anchor) = self.v1_registry_read_anchors.get_mut(name) {
            anchor.surface_known = active;
        }
        if !active {
            self.active_resources.remove(name);
        }
    }
}
