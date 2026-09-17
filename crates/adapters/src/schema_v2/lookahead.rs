//! ENSv1 per-batch working-set requests. Persistence and canonical selection stay in Interpret.
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

    /// Expand stored explicit links before interpreting. The caller must fetch new requests
    /// to closure; an empty complete query certifies absence, a query not run does not.
    pub fn include_prior_events(&mut self, events: &[PriorEventInput]) -> anyhow::Result<()> {
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
        }
        Ok(())
    }

    fn prior_links(&mut self, namespace: &str, value: &Value) -> anyhow::Result<()> {
        match value {
            Value::Object(fields) => {
                for (key, value) in fields {
                    if matches!(
                        key.as_str(),
                        "node" | "namehash" | "child_node" | "reverse_node"
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
                let Some(source) = catalog
                    .source(manifest.manifest_id)
                    .filter(|source| source.source_family == "ens_v1_resolver_l1")
                else {
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
    for request in &dependencies.nodes {
        dependencies.resource_ids.insert(stable_uuid(&format!(
            "resource:registry-only:{}:{}",
            input.chain_id, request.node
        )));
    }
    Ok(dependencies)
}

/// The supplied fresh session must come from complete canonical queries for `loaded_nodes`,
/// finished at the batch's predecessor timestamp. Node access outside that certificate fails
/// before the prepared result can be published. No prior session is carried between batches.
pub fn prepare_schema_v2_batch_lookahead(
    input: BatchInput,
    provenance_manifests: Vec<ManifestInput>,
    session: AdapterSession,
    loaded_nodes: &BTreeSet<V1NodeRequest>,
    cache_capacity: StateCacheCapacity,
) -> anyhow::Result<PreparedAdapterBatch> {
    let dependencies = collect_v1_batch_dependencies(&input, &provenance_manifests)?;
    ensure!(
        dependencies.unsupported.is_empty(),
        "unsupported V1 lookahead coverage: {:?}",
        dependencies.unsupported
    );
    ensure!(
        dependencies.nodes.is_subset(loaded_nodes),
        "V1 lookahead dependencies were not completely loaded"
    );
    coverage::checked(loaded_nodes, || {
        super::prepare_schema_v2_batch_incremental_with_provenance(
            input,
            provenance_manifests,
            Some(session),
            cache_capacity,
        )
    })
}

/// Restore the session for a lookahead batch from exactly the prior events loaded for
/// `loaded_nodes`, then advance time-derived state to the batch's predecessor timestamp.
/// Restore must only touch names whose complete history was loaded: an event that
/// reaches another name would rebuild that name from part of its history.
pub fn restore_schema_v2_lookahead_session(
    mut restore: AdapterSessionRestore,
    prior_events: Vec<PriorEventInput>,
    resume_predecessor_timestamp: Option<time::OffsetDateTime>,
    loaded_nodes: &BTreeSet<V1NodeRequest>,
) -> anyhow::Result<AdapterSession> {
    coverage::checked(loaded_nodes, || {
        restore.apply_prior_events(prior_events)?;
        Ok(restore.finish(resume_predecessor_timestamp))
    })
}

pub(super) use coverage::observe_node;
