//! Per-batch working-set requests. ENSv1-model state (the ENSv1 families and the Basenames Base
//! families the same protocol code interprets) is requested per name, ENSv2 state per name and
//! per [ENSv2 state key](../../../../docs/glossary.md#ensv2-state-key). Persistence and
//! canonical selection stay in Interpret.
use std::collections::BTreeSet;

use anyhow::{Context, ensure};
use serde_json::Value;
use uuid::Uuid;

use super::{
    AdapterSession, AdapterSessionRestore, BatchInput, ManifestInput, PreparedAdapterBatch,
    PriorEventInput, StateCacheCapacity,
    catalog::{Catalog, Selected},
    common::stable_uuid,
};
use crate::evm_abi::hex_string;

mod coverage;
#[path = "lookahead_decode.rs"]
mod decode;

/// Complete facts for this node, never enumeration of its descendants.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct V1NodeRequest {
    pub namespace: String,
    pub node: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct V1BatchDependencies {
    pub nodes: BTreeSet<V1NodeRequest>,
    pub resource_ids: BTreeSet<Uuid>,
    /// ENSv2 state keys (`v2_key`) and whole registries (`v2_registry_key`).
    pub v2_keys: BTreeSet<String>,
    /// The window `(start, end]` of expiries whose ENSv2 tokens were loaded: every token a
    /// batch refresh can release.
    pub v2_due_window: Option<(i64, i64)>,
    pub unsupported: BTreeSet<String>,
}

impl V1BatchDependencies {
    pub(super) fn node(&mut self, namespace: &str, node: &str) -> anyhow::Result<()> {
        let node: alloy_primitives::B256 = node.parse().context("invalid V1 dependency node")?;
        self.nodes.insert(V1NodeRequest {
            namespace: namespace.to_owned(),
            node: format!("{node:#x}"),
        });
        Ok(())
    }

    /// Add each name's registry-only resource, whose history can hold events that name no node.
    pub fn include_registry_only_resources(&mut self, chain_id: &str) {
        for request in &self.nodes {
            self.resource_ids.insert(stable_uuid(&format!(
                "resource:registry-only:{chain_id}:{}",
                request.node
            )));
        }
    }

    /// Expand stored explicit links before interpreting. The caller must fetch new requests
    /// to closure; an empty complete query certifies absence, a query not run does not.
    pub fn include_prior_events<'a>(
        &mut self,
        events: impl IntoIterator<Item = &'a PriorEventInput>,
    ) -> anyhow::Result<()> {
        for event in events {
            if !supported_family(&event.source_family) {
                self.unsupported
                    .insert(format!("prior source family {}", event.source_family));
                continue;
            }
            if let Some(resource) = event.resource_id {
                self.resource_ids.insert(resource);
            }
            if let Some(name) = event.logical_name_id.as_deref()
                && let Some((namespace, node)) = name.split_once(':')
            {
                self.node(namespace, node)?;
            }
            self.prior_links(&event.namespace, &event.after_state)?;
            self.prior_v2_links(event);
        }
        Ok(())
    }

    /// The ENSv2 state a restored event needs beside its own. For a registry event that is its
    /// token under every id it carries, the registry's parent claim, the parent token a claim
    /// names, and the claim and mounts of a subregistry it points at. For a resolver hint or
    /// argument it is every version of it.
    fn prior_v2_links(&mut self, event: &PriorEventInput) {
        let Some(address) = event
            .state_scope
            .as_deref()
            .and_then(|scope| scope.split(':').next())
        else {
            return;
        };
        let after = &event.after_state;
        let text = |field: &str| after.get(field).and_then(Value::as_str);
        match event.source_family.as_str() {
            "ens_v2_registry_l1" | "ens_v2_root_l1" => {
                self.v2_keys.extend(
                    v2_event_keys(event)
                        .into_iter()
                        .filter(|key| !key.ends_with(":*")),
                );
                self.v2_keys.insert(v2_key(address, "-"));
                let ids = [
                    "token_id",
                    "current_token_id",
                    "old_token_id",
                    "new_token_id",
                ]
                .into_iter()
                .filter_map(text)
                .chain(
                    after
                        .get("resolver_discovery_aliases")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(Value::as_str),
                );
                for id in ids {
                    self.v2_keys.insert(v2_key(address, id));
                }
                if let Some(subregistry) = text("subregistry") {
                    self.v2_keys.insert(v2_key(subregistry, "-"));
                }
                if event.event_kind == "ParentChanged"
                    && let (Some(parent), Some(label)) =
                        (text("parent"), super::state::restored_raw_label(after))
                {
                    self.v2_keys.insert(v2_key(
                        parent,
                        &hex_string(alloy_primitives::keccak256(label)),
                    ));
                }
            }
            "ens_v2_resolver_l1"
                if matches!(
                    event.event_kind.as_str(),
                    "PreimageObserved" | "ResolverPermissionArgument"
                ) =>
            {
                if let Some(resource) = v2_event_resource(after) {
                    self.v2_keys.insert(v2_key(address, resource));
                }
            }
            _ => {}
        }
    }

    fn prior_links(&mut self, namespace: &str, value: &Value) -> anyhow::Result<()> {
        match value {
            Value::Object(fields) => {
                for (key, value) in fields {
                    if matches!(
                        key.as_str(),
                        "node"
                            | "namehash"
                            | "child_node"
                            | "reverse_node"
                            | "previous_namehash"
                            | "current_namehash"
                    ) {
                        if let Some(node) = value.as_str() {
                            self.node(namespace, node)?;
                        }
                    } else if key == "resource_id" || key.ends_with("_resource_id") {
                        if let Some(resource) = value.as_str().and_then(|s| s.parse::<Uuid>().ok())
                        {
                            self.resource_ids.insert(resource);
                        }
                    } else if matches!(
                        key.as_str(),
                        "scope"
                            | "grant_source"
                            | "revocation_source"
                            | "previous_authority"
                            | "next_authority"
                            | "registrar_surface_evidence"
                            | "grant"
                            | "state"
                    ) {
                        self.prior_links(namespace, value)?;
                    }
                }
            }
            Value::Array(values) => {
                for value in values {
                    self.prior_links(namespace, value)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
}

/// The ENSv2 state keys a retained ENSv2 event is filed under; the loader loads an event when
/// one of them is requested. The SQL expression of `normalized_events_v2_key_probe_idx` and the
/// lookahead events query must compute the same keys:
///
/// - the first and third `state_scope` segments, the emitter (a contract instance id for
///   resolver arguments) and the token or resource id (`-` or empty when it has none);
/// - the emitter and `new_token_id`, so a regenerated token's new id reaches its history;
/// - the emitter and the upstream resource (`resource`, else `upstream_resource`), so a
///   resource reaches its token and a resolver's hint for it;
/// - the emitter and `labelhash`, so a registry's label reaches its token;
/// - the emitter's whole-registry key;
/// - the registry-level key of the `subregistry` the event names, so a registry reaches every
///   token that points at it, in any registry.
pub fn v2_event_keys(event: &PriorEventInput) -> Vec<String> {
    let Some(scope) = event
        .state_scope
        .as_deref()
        .filter(|_| event.source_family.starts_with("ens_v2_"))
    else {
        return Vec::new();
    };
    let mut segments = scope.split(':');
    let address = segments.next().unwrap_or_default();
    let id = segments.nth(1).unwrap_or_default();
    let mut keys = vec![v2_key(address, id), v2_registry_key(address)];
    if let Some(new) = event
        .after_state
        .get("new_token_id")
        .and_then(Value::as_str)
    {
        keys.push(v2_key(address, new));
    }
    if let Some(resource) = v2_event_resource(&event.after_state) {
        keys.push(v2_key(address, resource));
    }
    if let Some(labelhash) = event.after_state.get("labelhash").and_then(Value::as_str) {
        keys.push(v2_key(address, labelhash));
    }
    if let Some(subregistry) = event.after_state.get("subregistry").and_then(Value::as_str) {
        keys.push(v2_key(subregistry, "-"));
    }
    keys
}

fn v2_event_resource(after: &Value) -> Option<&str> {
    after
        .get("resource")
        .and_then(Value::as_str)
        .or_else(|| after.get("upstream_resource").and_then(Value::as_str))
}

/// Whether lookahead restores everything a manifest of this source family can depend on.
/// Interpret uses it to choose between lookahead and the full-state loader.
pub fn v1_lookahead_supports_family(family: &str) -> bool {
    supported_family(family)
}

fn supported_family(family: &str) -> bool {
    matches!(
        family,
        "ens_v1_registrar_l1"
            | "ens_v1_registry_l1"
            | "ens_v1_resolver_l1"
            | "ens_v1_wrapper_l1"
            | "ens_v1_reverse_l1"
            | "basenames_l1_compat"
            | "basenames_base_registry"
            | "basenames_base_registrar"
            | "basenames_base_resolver"
            | "basenames_base_primary"
            | "ens_v2_root_l1"
            | "ens_v2_registry_l1"
            | "ens_v2_registrar_l1"
            | "ens_v2_resolver_l1"
            | "ens_v2_migration_l1"
    ) || family.ends_with("_execution")
}

/// Decode dependencies before state-dependent protocol branches. No interpretation is run.
/// Unselected resolver logs are conservatively considered against active resolver ABIs so
/// same-batch discovery cannot hide their node. This does not admit or interpret those logs.
pub fn collect_v1_batch_dependencies(
    input: &BatchInput,
    provenance_manifests: &[ManifestInput],
) -> anyhow::Result<V1BatchDependencies> {
    super::validate_order(input)?;
    let catalog = Catalog::new_with_provenance(
        input.manifests.clone(),
        provenance_manifests.to_vec(),
        input.discovery_rules.clone(),
        input.admissions.clone(),
    )?;
    let mut dependencies = V1BatchDependencies::default();
    for manifest in &input.manifests {
        if !supported_family(&manifest.source_family) {
            dependencies
                .unsupported
                .insert(format!("active source family {}", manifest.source_family));
        }
    }
    for raw in &input.raw_logs {
        if let Some(selected) = catalog.select(raw)? {
            match decode::collect(&selected, raw, &mut dependencies) {
                Ok(()) => {}
                Err(_) if !selected.manifest_declared_emitter => {}
                Err(error) => return Err(error),
            }
        } else {
            for manifest in &input.manifests {
                let Some(source) = catalog.source(manifest.manifest_id).filter(|source| {
                    matches!(
                        source.source_family.as_str(),
                        "ens_v1_resolver_l1" | "basenames_base_resolver" | "ens_v2_resolver_l1"
                    )
                }) else {
                    continue;
                };
                for event in source.events.iter().filter(|event| {
                    raw.topics
                        .first()
                        .is_some_and(|topic| event.topic0.eq_ignore_ascii_case(topic))
                }) {
                    let selected = Selected {
                        source: source.clone(),
                        event: event.clone(),
                        contract_instance_id: Uuid::nil(),
                        emitter_role: None,
                        match_all: false,
                        manifest_declared_emitter: false,
                    };
                    // Speculative ABI candidates may be unrelated or malformed. Interpretation
                    // keeps its normal authority/decode policy; successful candidates add a superset.
                    let _ = decode::collect(&selected, raw, &mut dependencies);
                }
            }
        }
    }
    dependencies.include_registry_only_resources(&input.chain_id);
    Ok(dependencies)
}

/// The supplied fresh session must come from complete canonical queries for `loaded`, finished
/// at the batch's predecessor timestamp. State access outside that certificate fails before
/// the prepared result can be published. No prior session is carried between batches.
pub fn prepare_schema_v2_batch_lookahead(
    input: BatchInput,
    provenance_manifests: Vec<ManifestInput>,
    session: AdapterSession,
    loaded: &V1BatchDependencies,
    cache_capacity: StateCacheCapacity,
) -> anyhow::Result<PreparedAdapterBatch> {
    let dependencies = collect_v1_batch_dependencies(&input, &provenance_manifests)?;
    ensure!(
        dependencies.unsupported.is_empty(),
        "unsupported lookahead coverage: {:?}",
        dependencies.unsupported
    );
    ensure!(
        dependencies.nodes.is_subset(&loaded.nodes)
            && dependencies
                .v2_keys
                .iter()
                .all(|key| v2_key_loaded(loaded, key)),
        "lookahead dependencies were not completely loaded"
    );
    coverage::checked(loaded, false, || {
        super::prepare_schema_v2_batch_incremental_with_provenance(
            input,
            provenance_manifests,
            Some(session),
            cache_capacity,
        )
    })
}

/// Restore the session for a lookahead batch from exactly the prior events loaded for
/// `loaded`, then advance time-derived state to the batch's predecessor timestamp.
/// `latest_v2_topology` is the timestamp of the chain's latest ENSv2 registry event before the
/// batch, which a restore of every event would have reached.
pub fn restore_schema_v2_lookahead_session(
    mut restore: AdapterSessionRestore,
    prior_events: Vec<PriorEventInput>,
    resume_predecessor_timestamp: Option<time::OffsetDateTime>,
    latest_v2_topology: Option<time::OffsetDateTime>,
    loaded: &V1BatchDependencies,
) -> anyhow::Result<AdapterSession> {
    coverage::checked(loaded, true, || {
        restore.apply_prior_events(prior_events)?;
        restore.include_v2_topology_timestamp(latest_v2_topology);
        Ok(restore.finish(resume_predecessor_timestamp))
    })
}

/// Whether `key` is loaded, itself or through its registry.
pub fn v2_key_loaded(loaded: &V1BatchDependencies, key: &str) -> bool {
    loaded.v2_keys.contains(key)
        || key
            .rsplit_once(':')
            .is_some_and(|(address, _)| loaded.v2_keys.contains(&v2_registry_key(address)))
}

pub use coverage::{UnloadedKeys, v2_key, v2_registry_key};
pub(super) use coverage::{
    observe_name, observe_node, observe_v2, observe_v2_expiry_window, observe_v2_registry,
    restoring, v2_observation_id,
};
