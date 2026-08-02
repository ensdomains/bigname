use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, bail};
use uuid::Uuid;

use super::{
    common::contract_id,
    manifest::{self, ManifestEvent, ManifestSource},
    model::{AddressAdmissionInput, DiscoveryRuleInput, ManifestInput, RawLogInput},
    protocol,
};

#[derive(Clone, Debug)]
pub(super) struct Selected {
    pub source: ManifestSource,
    pub event: ManifestEvent,
    pub contract_instance_id: Uuid,
    pub emitter_role: Option<String>,
    pub match_all: bool,
}

pub(super) struct Catalog {
    manifests: Vec<ManifestSource>,
    by_id: BTreeMap<i64, usize>,
    rules: Vec<DiscoveryRuleInput>,
    admissions: Vec<AddressAdmissionInput>,
}

impl Catalog {
    pub(super) fn new(
        manifests: Vec<ManifestInput>,
        rules: Vec<DiscoveryRuleInput>,
        admissions: Vec<AddressAdmissionInput>,
    ) -> anyhow::Result<Self> {
        let mut manifests = manifests
            .into_iter()
            .map(manifest::decode)
            .collect::<anyhow::Result<Vec<_>>>()?;
        for source in &manifests {
            protocol::validate_manifest(source)?;
        }
        manifests.sort_by_key(|source| source.manifest_id);
        if manifests.is_empty() {
            bail!("interpretation requires at least one active manifest");
        }
        let by_id = manifests
            .iter()
            .enumerate()
            .map(|(index, source)| (source.manifest_id, index))
            .collect();
        Ok(Self {
            manifests,
            by_id,
            rules,
            admissions,
        })
    }

    pub(super) fn v2_suffix_anchors(&self) -> Vec<(String, String, Vec<String>)> {
        let mut anchors = BTreeMap::new();
        for admission in self
            .admissions
            .iter()
            .filter(|admission| admission.discovery_edge_kind.is_none())
        {
            let Some(source) = admission
                .source_manifest_id
                .and_then(|manifest_id| self.source(manifest_id))
            else {
                continue;
            };
            let suffix = match source.source_family.as_str() {
                "ens_v2_root_l1" => Vec::new(),
                "ens_v2_registry_l1" => vec!["eth".to_owned()],
                _ => continue,
            };
            anchors.insert(
                admission.address.to_ascii_lowercase(),
                (source.namespace.clone(), suffix),
            );
        }
        anchors
            .into_iter()
            .map(|(address, (namespace, suffix))| (address, namespace, suffix))
            .collect()
    }

    pub(super) fn select(&self, raw: &RawLogInput) -> anyhow::Result<Option<Selected>> {
        let Some(topic0) = raw.topics.first() else {
            return Ok(None);
        };
        let announcement_namespaces = self
            .admissions
            .iter()
            .filter(|admission| {
                applies(admission, raw)
                    && admission.discovery_edge_kind.as_deref() == Some("registry_announcement")
            })
            .filter_map(|admission| admission.source_manifest_id)
            .filter_map(|manifest_id| self.source(manifest_id))
            .map(|source| source.namespace.as_str())
            .collect::<BTreeSet<_>>();
        let mut candidates = Vec::new();
        for admission in self
            .admissions
            .iter()
            .filter(|admission| applies(admission, raw))
        {
            let Some(manifest_id) = admission.source_manifest_id else {
                continue;
            };
            let source = self
                .source(manifest_id)
                .with_context(|| format!("admission references inactive manifest {manifest_id}"))?;
            let rank = match admission.discovery_edge_kind.as_deref() {
                Some("registry_announcement") => 1,
                None if announcement_namespaces.contains(source.namespace.as_str()) => 0,
                None if !announcement_namespaces.is_empty() => 2,
                _ => 0,
            };
            let target_family = inferred_family(
                &source.source_family,
                admission.discovery_edge_kind.as_deref(),
            );
            if let Some(target_family) =
                target_family.filter(|family| *family != source.source_family)
            {
                for inferred in self.manifests.iter().filter(|candidate| {
                    candidate.namespace == source.namespace
                        && candidate.chain_id == source.chain_id
                        && candidate.deployment_label == source.deployment_label
                        && candidate.source_family == target_family
                }) {
                    push_candidates(
                        &mut candidates,
                        rank,
                        inferred,
                        topic0,
                        None,
                        admission.discovery_edge_kind.as_deref(),
                        admission.contract_instance_id,
                    );
                }
            } else {
                push_candidates(
                    &mut candidates,
                    rank,
                    source,
                    topic0,
                    admission.role.as_deref(),
                    admission.discovery_edge_kind.as_deref(),
                    admission.contract_instance_id,
                );
            }
        }

        for source in &self.manifests {
            for event in source.events.iter().filter(|event| {
                event.topic0.eq_ignore_ascii_case(topic0) && self.is_match_all(source, event)
            }) {
                let contract_instance_id = self
                    .admissions
                    .iter()
                    .filter(|admission| applies(admission, raw))
                    .map(|admission| admission.contract_instance_id)
                    .next()
                    .unwrap_or_else(|| contract_id(&raw.chain_id, &raw.emitting_address));
                candidates.push((
                    2,
                    Selected {
                        source: source.clone(),
                        event: event.clone(),
                        contract_instance_id,
                        emitter_role: None,
                        match_all: true,
                    },
                ));
            }
        }
        select_unambiguous(raw, candidates)
    }

    pub(super) fn rule(
        &self,
        manifest_id: i64,
        edge_kind: &str,
        emitter_role: Option<&str>,
    ) -> Option<&DiscoveryRuleInput> {
        self.rules.iter().find(|rule| {
            rule.manifest_id == manifest_id
                && rule.edge_kind == edge_kind
                && rule
                    .from_role
                    .as_deref()
                    .is_none_or(|required| emitter_role.is_none_or(|actual| required == actual))
        })
    }

    pub(super) fn admit(&mut self, admission: AddressAdmissionInput) {
        if let (Some(edge_kind), Some(from), Some(observation_key)) = (
            admission.discovery_edge_kind.as_deref(),
            admission.discovery_from_contract_instance_id,
            admission.discovery_observation_key.as_deref(),
        ) {
            self.retire(edge_kind, from, observation_key);
        }
        self.admissions.push(admission);
    }

    pub(super) fn retire(&mut self, edge_kind: &str, from: Uuid, observation_key: &str) {
        self.admissions.retain(|existing| {
            existing.discovery_edge_kind.as_deref() != Some(edge_kind)
                || existing.discovery_from_contract_instance_id != Some(from)
                || existing.discovery_observation_key.as_deref() != Some(observation_key)
        });
    }

    pub(super) fn contract_instance_for_address(
        &self,
        address: &str,
        block_number: i64,
    ) -> anyhow::Result<Option<Uuid>> {
        let mut instances = self
            .admissions
            .iter()
            .filter(|admission| {
                admission.address.eq_ignore_ascii_case(address)
                    && admission
                        .active_from_block
                        .is_none_or(|from| block_number >= from)
                    && admission
                        .active_to_block
                        .is_none_or(|to| block_number <= to)
            })
            .map(|admission| admission.contract_instance_id)
            .collect::<BTreeSet<_>>();
        if instances.len() > 1 {
            bail!("address {address} has more than one active contract identity");
        }
        Ok(instances.pop_first())
    }

    pub(super) fn source(&self, manifest_id: i64) -> Option<&ManifestSource> {
        self.by_id
            .get(&manifest_id)
            .and_then(|index| self.manifests.get(*index))
    }

    pub(super) fn source_for_family(&self, source_family: &str) -> Option<&ManifestSource> {
        self.manifests
            .iter()
            .find(|source| source.source_family == source_family)
    }

    pub(super) fn source_for_contract_instance(
        &self,
        contract_instance_id: Uuid,
    ) -> Option<&ManifestSource> {
        self.admissions
            .iter()
            .filter(|admission| admission.contract_instance_id == contract_instance_id)
            .filter_map(|admission| admission.source_manifest_id)
            .find_map(|manifest_id| self.source(manifest_id))
    }

    fn is_match_all(&self, source: &ManifestSource, event: &ManifestEvent) -> bool {
        match source.source_family.as_str() {
            "ens_v1_resolver_l1" | "basenames_base_resolver" => true,
            "ens_v2_registry_l1" if event.name == "RegistryCreated" => self
                .rule(source.manifest_id, "registry_announcement", None)
                .is_some(),
            "ens_v2_resolver_l1" => matches!(
                event.name.as_str(),
                "AliasChanged" | "NamedResource" | "NamedTextResource" | "NamedAddrResource"
            ),
            _ => false,
        }
    }
}

fn applies(admission: &AddressAdmissionInput, raw: &RawLogInput) -> bool {
    admission
        .address
        .eq_ignore_ascii_case(&raw.emitting_address)
        && admission
            .active_from_block
            .is_none_or(|from| raw.block_number >= from)
        && admission
            .active_to_block
            .is_none_or(|to| raw.block_number <= to)
}

fn push_candidates(
    output: &mut Vec<(u8, Selected)>,
    rank: u8,
    source: &ManifestSource,
    topic0: &str,
    role: Option<&str>,
    discovery_kind: Option<&str>,
    instance: Uuid,
) {
    for event in source.events.iter().filter(|event| {
        event.topic0.eq_ignore_ascii_case(topic0)
            && (event.emitter_roles.is_empty()
                || discovery_kind == Some("registry_announcement")
                || role.is_some_and(|role| event.emitter_roles.iter().any(|item| item == role)))
    }) {
        output.push((
            rank,
            Selected {
                source: source.clone(),
                event: event.clone(),
                contract_instance_id: instance,
                emitter_role: role.map(str::to_owned),
                match_all: false,
            },
        ));
    }
}

fn inferred_family(source_family: &str, edge_kind: Option<&str>) -> Option<&'static str> {
    match (edge_kind, source_family) {
        (Some("resolver"), "ens_v2_registry_l1" | "ens_v2_root_l1") => Some("ens_v2_resolver_l1"),
        (Some("resolver"), "basenames_base_registry") => Some("basenames_base_resolver"),
        (Some("registry_announcement"), "ens_v2_registry_l1") => Some("ens_v2_registry_l1"),
        _ => None,
    }
}

fn select_unambiguous(
    raw: &RawLogInput,
    mut candidates: Vec<(u8, Selected)>,
) -> anyhow::Result<Option<Selected>> {
    let Some(rank) = candidates.iter().map(|candidate| candidate.0).min() else {
        return Ok(None);
    };
    candidates.retain(|candidate| candidate.0 == rank);
    candidates.sort_by(|left, right| {
        (left.1.source.manifest_id, &left.1.event.signature)
            .cmp(&(right.1.source.manifest_id, &right.1.event.signature))
    });
    candidates.dedup_by(|left, right| {
        left.1.source.manifest_id == right.1.source.manifest_id
            && left.1.event.signature == right.1.event.signature
    });
    if candidates.len() > 1 {
        let sources = candidates
            .iter()
            .map(|candidate| {
                format!(
                    "{}:{}",
                    candidate.1.source.source_family, candidate.1.event.signature
                )
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
            .join(", ");
        bail!(
            "raw log {}:{} has ambiguous admitted adapters: {sources}",
            raw.block_hash,
            raw.log_index
        );
    }
    Ok(candidates.pop().map(|candidate| candidate.1))
}
