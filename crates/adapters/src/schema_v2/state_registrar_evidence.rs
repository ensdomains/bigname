//! Bounded evidence for disclosing an already selected numeric registrar lease.
use std::sync::Arc;

use anyhow::Context;
use serde_json::{Value, json};
use uuid::Uuid;

use super::{State, V1NameState, V1ResolverLink, v1_key};
use crate::schema_v2::{
    common::raw_fact_ref,
    manifest::ManifestProvenance,
    model::{PriorEventInput, RawLogInput},
};

const KEY: &str = "registrar_surface_evidence";

// Evidence is retained for every observed name. Keeping parsed JSON here grows
// with history even though the separate before-state cache has an entry limit.
// Only the in-memory representation changes; snapshots still contain full JSON.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct StoredEvidence {
    bytes: Arc<[u8]>,
    decoded_len: usize,
}

impl StoredEvidence {
    fn new(value: &Value) -> Self {
        let json = serde_json::to_vec(value).expect("JSON evidence serializes");
        let bytes = zstd::bulk::compress(&json, 1).expect("evidence compression succeeds");
        Self {
            bytes: bytes.into(),
            decoded_len: json.len(),
        }
    }

    fn value(&self) -> Value {
        let json = zstd::bulk::decompress(&self.bytes, self.decoded_len)
            .expect("in-process evidence was compressed by StoredEvidence");
        serde_json::from_slice(&json).expect("in-process evidence contains valid JSON")
    }
}

impl State {
    pub(in crate::schema_v2) fn retain_registrar_evidence(
        &mut self,
        source: &ManifestProvenance,
        raw: &RawLogInput,
        kind: &str,
        resource: Option<Uuid>,
        after: &mut Value,
    ) {
        if !matches!(
            raw.canonicality_state.as_str(),
            "canonical" | "safe" | "finalized"
        ) || after["state_derived"] == true
        {
            return;
        }
        let Some(node) = after["child_node"]
            .as_str()
            .or_else(|| after["namehash"].as_str())
            .or_else(|| after["node"].as_str())
            .map(str::to_owned)
        else {
            return;
        };
        let source_event = after["source_event"].as_str().unwrap_or_default();
        let field = match (source.source_family.as_str(), kind, source_event) {
            ("ens_v2_migration_l1", "RegistrationReleased", "NameRegistered")
                if after["registrar_surface_retired"] == true =>
            {
                "retirement"
            }
            ("ens_v1_registrar_l1", "RegistrationGranted", "NameRegistered")
                if after["registration_window"] == "whole_transaction" =>
            {
                "grant"
            }
            ("ens_v1_registrar_l1", "TokenControlTransferred", "Transfer") => "registrar_owner",
            ("ens_v1_registry_l1", "AuthorityTransferred", "NewOwner" | "Transfer") => {
                "registry_owner"
            }
            ("ens_v1_registry_l1", "ResolverChanged", "NewResolver") => "resolver",
            ("ens_v1_registrar_l1", "RegistrationGranted" | "RegistrationRenewed", _) => "lease",
            _ => return,
        };
        let key = v1_key(&source.namespace, &node);
        let mut evidence = self
            .v1_registrar_evidence
            .get(&key)
            .map(StoredEvidence::value)
            .unwrap_or(json!({}));
        let entry = json!({
            "raw_fact_ref":raw_fact_ref(raw), "timestamp":raw.block_timestamp.unix_timestamp(),
            "block_number":raw.block_number,"transaction_index":raw.transaction_index,"log_index":raw.log_index,
            "source_manifest_id":source.manifest_id, "source_family":source.source_family,
            "resource_id":resource,"state":after,
        });
        if field == "grant" {
            // A new lease replaces the old grant; registry evidence remains independently current.
            evidence["registrar_owner"] = entry.clone();
            evidence["lease"] = entry.clone();
        }
        if after["registrar_surface_retired"] == true {
            evidence["retirement"] = entry.clone();
        }
        evidence[field] = entry;
        // Nested evidence is never copied into the next snapshot.
        for value in evidence
            .as_object_mut()
            .expect("evidence object")
            .values_mut()
        {
            if let Some(state) = value["state"].as_object_mut() {
                state.remove(KEY);
            }
        }
        self.v1_registrar_evidence
            .insert(key, StoredEvidence::new(&evidence));
        after[KEY] = evidence;
    }

    pub(in crate::schema_v2) fn restore_registrar_evidence(&mut self, event: &PriorEventInput) {
        let Some(incoming) = event.after_state[KEY].as_object() else {
            return;
        };
        let Some(node) = event.after_state["child_node"]
            .as_str()
            .or_else(|| event.after_state["namehash"].as_str())
            .or_else(|| event.after_state["node"].as_str())
        else {
            return;
        };
        let key = v1_key(&event.namespace, node);
        let mut evidence = self
            .v1_registrar_evidence
            .get(&key)
            .map(StoredEvidence::value)
            .unwrap_or(json!({}));
        for field in [
            "grant",
            "registrar_owner",
            "registry_owner",
            "resolver",
            "lease",
            "retirement",
        ] {
            let Some(entry) = incoming.get(field) else {
                continue;
            };
            let position = |value: &Value| {
                (
                    value["block_number"].as_i64(),
                    value["transaction_index"].as_i64(),
                    value["log_index"].as_i64(),
                )
            };
            if position(entry) >= position(&evidence[field]) {
                evidence[field] = entry.clone();
            }
        }
        self.v1_registrar_evidence
            .insert(key, StoredEvidence::new(&evidence));
    }

    fn registrar_disclosure_candidate(
        &self,
        namespace: &str,
        node: &str,
        labelhash: &str,
        timestamp: i64,
    ) -> Option<(V1NameState, Option<V1ResolverLink>, Value)> {
        let key = v1_key(namespace, node);
        let registrar = self.v1_registrars.get(&key)?.as_ref().clone();
        let selected = self.v1_names.get(&key)?;
        let (registry_owner, registry_contract) = self.v1_registry_binding(namespace, node)?;
        let owner = registrar.owner.as_deref()?;
        let evidence = self.v1_registrar_evidence.get(&key)?.value();
        let grant = &evidence["grant"];
        let registry = &evidence["registry_owner"];
        let owner_evidence = &evidence["registrar_owner"];
        let proven_registry_owner = registry["state"]["owner_getter"].as_str()?;
        let proven_registrar_owner = owner_evidence["state"]["to"]
            .as_str()
            .or_else(|| owner_evidence["state"]["authority_owner"].as_str())?;
        if registrar.surface_known
            || registrar.wrapper_fallback
            || registrar.authority_source_family != "ens_v1_registrar_l1"
            || selected.resource_id != registrar.resource_id
            || selected.token_lineage_id != registrar.token_lineage_id
            || selected.owner != registrar.owner
            || selected.authority_key != registrar.authority_key
            || selected.authority_source_family != registrar.authority_source_family
            || registrar.token_lineage_id.is_none()
            || registrar.authority_key.is_none()
            || registrar.expiry.is_none_or(|expiry| expiry <= timestamp)
            || evidence["retirement"]["block_number"]
                .as_i64()
                .is_some_and(|retired| {
                    retired >= grant["block_number"].as_i64().unwrap_or(i64::MIN)
                })
            || registrar.labelhash.as_deref() != Some(labelhash)
            || owner.eq_ignore_ascii_case("0x0000000000000000000000000000000000000000")
            || !registry_owner.eq_ignore_ascii_case(owner)
            || !proven_registry_owner.eq_ignore_ascii_case(owner)
            || !proven_registrar_owner.eq_ignore_ascii_case(owner)
            || registry["raw_fact_ref"]["emitting_address"]
                .as_str()
                .is_none_or(|address| !address.eq_ignore_ascii_case(&registry_contract))
            || grant["resource_id"] != json!(registrar.resource_id)
            || owner_evidence["resource_id"] != json!(registrar.resource_id)
            || grant["source_manifest_id"].as_i64() != registrar.source_manifest_id
            || grant["timestamp"]
                .as_i64()
                .is_none_or(|time| time > timestamp)
            || grant["state"]["token_lineage_id"] != json!(registrar.token_lineage_id)
        {
            return None;
        }
        let resolver = self.v1_resolver_for_activation(namespace, node, Some(&registrar));
        if let Some(link) = resolver.as_ref()
            && evidence["resolver"]["state"]["resolver"]
                .as_str()
                .is_none_or(|address| !address.eq_ignore_ascii_case(&link.resolver_address))
        {
            return None;
        }
        Some((registrar, resolver, evidence))
    }

    pub(in crate::schema_v2) fn disclose_retained_registrar(
        &mut self,
        namespace: &str,
        node: &str,
        labelhash: &str,
        timestamp: i64,
    ) -> anyhow::Result<Option<(V1NameState, Option<V1ResolverLink>, Value)>> {
        let Some((mut authority, resolver, evidence)) =
            self.registrar_disclosure_candidate(namespace, node, labelhash, timestamp)
        else {
            return Ok(None);
        };
        for field in ["grant", "registrar_owner", "registry_owner", "resolver"] {
            if field == "resolver" && resolver.is_none() {
                continue;
            }
            let id = evidence[field]["source_manifest_id"]
                .as_i64()
                .context("registrar snapshot evidence has no source manifest")?;
            anyhow::ensure!(
                self.known_source_manifest_ids
                    .as_ref()
                    .is_none_or(|ids| ids.contains(&id)),
                "state-derived source manifest is missing for namespace {namespace}, namehash {node}, manifest {id}"
            );
        }
        // Change only readability of the already selected resource, never authority selection.
        self.bind_v1_active_surface(namespace, node);
        authority.surface_known = true;
        Ok(Some((authority, resolver, evidence)))
    }

    pub(in crate::schema_v2) fn restore_registrar_snapshot(
        &mut self,
        event: &PriorEventInput,
    ) -> bool {
        if event.after_state["registrar_surface_snapshot"] != true {
            return false;
        }
        if event.event_kind != "RegistrationGranted" {
            return false;
        }
        let after = &event.after_state;
        let grant = &after[KEY]["grant"];
        if after["state_derived"] != true
            || after["surface_materialization"] != true
            || event.source_family != "ens_v1_registrar_l1"
            || grant["timestamp"].as_i64().is_none()
            || after["original_registered_at"] != grant["timestamp"]
            || grant["resource_id"] != json!(event.resource_id)
            || grant["source_manifest_id"].as_i64() != event.source_manifest_id
            || grant["state"]["token_lineage_id"] != after["token_lineage_id"]
        {
            self.record_restore_error(anyhow::anyhow!(
                "registrar surface snapshot has inconsistent original grant provenance"
            ));
            return true;
        }
        let (Some(node), Some(name), Some(resource), Some(lineage), Some(owner), Some(expiry)) = (
            after["namehash"].as_str(),
            event.logical_name_id.as_deref(),
            event.resource_id,
            after["token_lineage_id"]
                .as_str()
                .and_then(|value| value.parse().ok()),
            after["authority_owner"].as_str(),
            after["expiry"].as_i64(),
        ) else {
            self.record_restore_error(anyhow::anyhow!("incomplete registrar surface snapshot"));
            return true;
        };
        self.observe_v1_registrar(
            &event.namespace,
            node,
            name.to_owned(),
            true,
            resource,
            lineage,
            event.source_family.clone(),
            event.source_manifest_id,
            after["labelhash"].as_str().map(str::to_owned),
            Some(expiry),
            Some(owner.to_owned()),
            after["authority_key"].as_str().map(str::to_owned),
            false,
            true,
        );
        self.bind_v1_active_surface(&event.namespace, node);
        true
    }
}
