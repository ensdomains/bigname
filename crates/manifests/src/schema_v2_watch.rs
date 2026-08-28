use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{SourceManifest, all_emitter_topic0s, normalize_address};

pub(super) use super::watch_widening::{
    CoverageInterval, PersistedWatchCoverage, normalize_coverage, widening_start,
};

const COMPILED_WATCH_FIELD: &str = "_bigname_compiled_watch";
pub(super) type AdmissionFloors = BTreeMap<(String, String, String, String), u64>;

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum WatchEmitter {
    All,
    Family {
        #[serde(default)]
        namespace: String,
        family: String,
    },
    // Declared intervals retain their manifest IDs, so overlapping declarations produce one
    // runtime query per exact topic vector and collectively fetch the union for this address.
    Address {
        family: String,
        address: String,
    },
}

impl WatchEmitter {
    fn with_legacy_namespace(self, enclosing_namespace: &str) -> Self {
        match self {
            Self::Family { namespace, family } if namespace.is_empty() => Self::Family {
                namespace: enclosing_namespace.to_owned(),
                family,
            },
            emitter => emitter,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct WatchKey {
    pub(super) emitter: WatchEmitter,
    pub(super) topic0: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
/// This is the persisted [compiled watch plan](../../../docs/glossary.md#compiled-watch-plan).
/// Format changes must remain backward-decodable: decode failure stops manifest sync fleet-wide,
/// and a field rename would brick sync with no recovery short of direct database surgery. A
/// coverage-bearing field must not be removed merely because serde can ignore the old key.
struct CompiledWatchEntry {
    emitter: WatchEmitter,
    topic0: String,
    start: u64,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DiscoveryRuleKey {
    namespace: String,
    family: String,
    edge_kind: String,
    from_role: String,
    admission: String,
    emitting_address: Option<String>,
    announcement_backed: bool,
    producer_topic0: Option<String>,
}

impl DiscoveryRuleKey {
    fn same_rule(&self, other: &Self) -> bool {
        self.namespace == other.namespace
            && self.family == other.family
            && self.edge_kind == other.edge_kind
            && self.from_role == other.from_role
            && self.admission == other.admission
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DiscoveryWideningKind {
    Rule,
    SourceReplacement,
    DeploymentEpoch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DiscoveryWidening {
    pub(super) start: u64,
    pub(super) kind: DiscoveryWideningKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DeploymentIdentity {
    epoch: String,
    manifest_version: u64,
}

#[derive(Default)]
pub(super) struct Snapshot {
    pub(super) watch_by_chain: BTreeMap<String, BTreeMap<WatchKey, u64>>,
    discovery_by_chain: BTreeMap<String, BTreeMap<DiscoveryRuleKey, u64>>,
    deployments_by_chain: BTreeMap<String, BTreeMap<(String, String), DeploymentIdentity>>,
}

pub(super) fn manifest_payload(manifest: &SourceManifest) -> Result<Value> {
    let mut payload = serde_json::to_value(manifest).context("failed to serialize manifest")?;
    let Value::Object(fields) = &mut payload else {
        bail!("serialized manifest payload is not a JSON object");
    };
    fields.insert(
        COMPILED_WATCH_FIELD.to_owned(),
        serde_json::to_value(compile_watch_scope(manifest)?)
            .context("failed to serialize compiled watch plan")?,
    );
    Ok(payload)
}

pub(super) fn record(
    snapshot: &mut Snapshot,
    manifest: &SourceManifest,
    payload: &Value,
) -> Result<()> {
    let compiled = payload
        .get(COMPILED_WATCH_FIELD)
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .context("failed to decode persisted compiled watch plan")?
        .map_or_else(|| compile_watch_scope(manifest), Ok)?;
    let watch = snapshot
        .watch_by_chain
        .entry(manifest.chain.clone())
        .or_default();
    for entry in compiled {
        insert_watch(
            watch,
            entry.emitter.with_legacy_namespace(&manifest.namespace),
            &entry.topic0,
            entry.start,
        );
    }
    snapshot
        .deployments_by_chain
        .entry(manifest.chain.clone())
        .or_default()
        .insert(
            (manifest.namespace.clone(), manifest.source_family.clone()),
            DeploymentIdentity {
                epoch: manifest.deployment_epoch.clone(),
                manifest_version: manifest.manifest_version,
            },
        );
    record_discovery_rules(snapshot, manifest)?;
    Ok(())
}

pub(super) fn discovery_widening_start(
    previous: &Snapshot,
    desired: &Snapshot,
    chain_id: &str,
    admission_floors: &AdmissionFloors,
) -> Option<DiscoveryWidening> {
    discovery_widening_start_for(previous, desired, chain_id, admission_floors, |rule| {
        rule.edge_kind == "resolver"
    })
}

pub(super) fn resolver_deployment_widening(
    previous: &Snapshot,
    desired: &Snapshot,
    chain_id: &str,
    admission_floors: &AdmissionFloors,
) -> Option<DiscoveryWidening> {
    let desired_deployments = desired.deployments_by_chain.get(chain_id)?;
    let previous_deployments = previous.deployments_by_chain.get(chain_id);
    desired
        .discovery_by_chain
        .get(chain_id)?
        .iter()
        .filter(|(rule, _)| rule.edge_kind == "resolver")
        .filter_map(|(rule, start)| {
            let source_key = (rule.namespace.clone(), rule.family.clone());
            let source = desired_deployments.get(&source_key)?;
            let target_family = resolver_target_family(&rule.family)?;
            let target_key = (rule.namespace.clone(), target_family.to_owned());
            let target = desired_deployments.get(&target_key)?;
            if source.epoch != target.epoch {
                return None;
            }
            let kind = previous_deployments
                .and_then(|manifests| {
                    Some((manifests.get(&source_key)?, manifests.get(&target_key)?))
                })
                .and_then(|(previous_source, previous_target)| {
                    (previous_source.epoch == source.epoch && previous_target.epoch == source.epoch)
                        .then_some(previous_source)
                })
                .map_or(DiscoveryWideningKind::DeploymentEpoch, |previous_source| {
                    if previous_source.manifest_version == source.manifest_version {
                        DiscoveryWideningKind::Rule
                    } else {
                        DiscoveryWideningKind::SourceReplacement
                    }
                });
            (kind != DiscoveryWideningKind::Rule).then_some(DiscoveryWidening {
                start: discovery_start(rule, *start, admission_floors),
                kind,
            })
        })
        .min_by_key(|widening| widening.start)
}

fn resolver_target_family(source_family: &str) -> Option<&'static str> {
    match source_family {
        "ens_v1_registry_l1" => Some("ens_v1_resolver_l1"),
        "ens_v2_registry_l1" | "ens_v2_root_l1" => Some("ens_v2_resolver_l1"),
        "basenames_base_registry" => Some("basenames_base_resolver"),
        _ => None,
    }
}

pub(super) fn registry_announcement_widening_start(
    previous: &Snapshot,
    desired: &Snapshot,
    chain_id: &str,
    admission_floors: &AdmissionFloors,
) -> Option<u64> {
    discovery_widening_start_for(previous, desired, chain_id, admission_floors, |rule| {
        rule.edge_kind == "registry_announcement"
            && rule.family == crate::ENS_V2_REGISTRY_SOURCE_FAMILY
    })
    .map(|widening| widening.start)
}

pub(super) fn unsupported_registry_announcement_widening(
    previous: &Snapshot,
    desired: &Snapshot,
    chain_id: &str,
    admission_floors: &AdmissionFloors,
) -> Option<DiscoveryWidening> {
    discovery_widening_start_for(previous, desired, chain_id, admission_floors, |rule| {
        rule.edge_kind == "registry_announcement"
            && rule.family != crate::ENS_V2_REGISTRY_SOURCE_FAMILY
    })
}

fn discovery_widening_start_for(
    previous: &Snapshot,
    desired: &Snapshot,
    chain_id: &str,
    admission_floors: &AdmissionFloors,
    include: impl Fn(&DiscoveryRuleKey) -> bool,
) -> Option<DiscoveryWidening> {
    let previous = previous.discovery_by_chain.get(chain_id);
    let desired = desired.discovery_by_chain.get(chain_id)?;
    desired
        .iter()
        .filter(|(rule, _)| include(rule))
        .filter_map(|(rule, start)| {
            let start = discovery_start(rule, *start, admission_floors);
            let covered =
                previous
                    .and_then(|rules| rules.get(rule))
                    .is_some_and(|previous_start| {
                        discovery_start(rule, *previous_start, admission_floors) <= start
                    });
            if covered {
                return None;
            }
            let previous_rule = previous
                .into_iter()
                .flat_map(|rules| rules.keys())
                .filter(|prior| prior.same_rule(rule))
                .collect::<Vec<_>>();
            let previous_emitters = previous_rule
                .iter()
                .filter_map(|prior| prior.emitting_address.as_deref())
                .collect::<BTreeSet<_>>();
            let desired_emitters = desired
                .keys()
                .filter(|candidate| candidate.same_rule(rule))
                .filter_map(|candidate| candidate.emitting_address.as_deref())
                .collect::<BTreeSet<_>>();
            if !previous_rule.is_empty() && desired_emitters.is_empty() && !rule.announcement_backed
            {
                return None;
            }
            Some(DiscoveryWidening {
                start,
                kind: if previous_emitters.is_empty()
                    || previous_emitters.is_subset(&desired_emitters)
                {
                    DiscoveryWideningKind::Rule
                } else {
                    DiscoveryWideningKind::SourceReplacement
                },
            })
        })
        .min_by_key(|widening| (widening.start, widening.kind == DiscoveryWideningKind::Rule))
}

fn discovery_start(
    rule: &DiscoveryRuleKey,
    declared_start: u64,
    admission_floors: &AdmissionFloors,
) -> u64 {
    rule.emitting_address
        .as_ref()
        .and_then(|address| {
            admission_floors.get(&(
                rule.namespace.clone(),
                rule.family.clone(),
                rule.from_role.clone(),
                address.clone(),
            ))
        })
        .map_or(declared_start, |floor| declared_start.min(*floor))
}

fn compile_watch_scope(manifest: &SourceManifest) -> Result<Vec<CompiledWatchEntry>> {
    let topics = manifest
        .abi
        .event_topic0s()
        .with_context(|| format!("failed to compile {} watch topics", manifest.source_family))?
        .into_iter()
        .map(|topic| topic.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let all_emitter_topics = all_emitter_topic0s(&manifest.source_family, &topics)
        .into_iter()
        .collect::<BTreeSet<_>>();
    let mut watch = BTreeMap::new();
    for topic0 in &all_emitter_topics {
        insert_watch(&mut watch, WatchEmitter::All, topic0, 0);
    }
    if crate::uses_discovered_emitters(&manifest.source_family) {
        for topic0 in &topics {
            insert_watch(
                &mut watch,
                WatchEmitter::Family {
                    namespace: manifest.namespace.clone(),
                    family: manifest.source_family.clone(),
                },
                topic0,
                0,
            );
        }
    }
    for (address, start) in manifest
        .roots
        .iter()
        .map(|root| (&root.address, root.start_block))
        .chain(
            manifest
                .contracts
                .iter()
                .map(|contract| (&contract.address, contract.start_block)),
        )
    {
        for topic0 in &topics {
            insert_watch(
                &mut watch,
                WatchEmitter::Address {
                    family: manifest.source_family.clone(),
                    address: normalize_address(address),
                },
                topic0,
                start.unwrap_or(0),
            );
        }
    }
    Ok(watch
        .into_iter()
        .map(|(key, start)| CompiledWatchEntry {
            emitter: key.emitter,
            topic0: key.topic0,
            start,
        })
        .collect())
}

fn record_discovery_rules(snapshot: &mut Snapshot, manifest: &SourceManifest) -> Result<()> {
    let registry_announcement_topic =
        discovery_producer_topic0(manifest, "registry_announcement", None, false)?;
    let resolver_topic = discovery_producer_topic0(manifest, "resolver", None, true)?;
    let resolver_accepts_announced_registries = manifest.source_family
        == crate::ENS_V2_REGISTRY_SOURCE_FAMILY
        && manifest
            .discovery_rules
            .iter()
            .any(|rule| rule.edge_kind == "registry_announcement")
        && registry_announcement_topic.is_some()
        && resolver_topic.is_some();
    let declarations = manifest
        .roots
        .iter()
        .map(|root| {
            (
                root.name.as_str(),
                normalize_address(&root.address),
                root.start_block.unwrap_or(0),
            )
        })
        .chain(manifest.contracts.iter().map(|contract| {
            (
                contract.role.as_str(),
                normalize_address(&contract.address),
                contract.start_block.unwrap_or(0),
            )
        }))
        .collect::<Vec<_>>();
    let rules = snapshot
        .discovery_by_chain
        .entry(manifest.chain.clone())
        .or_default();
    for rule in manifest.discovery_rules.iter().filter(|rule| {
        // Keep address-admitting kinds explicit so every new edge kind is classified here.
        // `subregistry` is topology-only, while the `migration` edge is reserved and has no
        // writer under docs/manifests.md; neither can widen address-scoped historical intake.
        matches!(
            rule.edge_kind.as_str(),
            "resolver" | "registry_announcement"
        )
    }) {
        let mut emitters = BTreeMap::<String, u64>::new();
        for (_, address, start) in declarations
            .iter()
            .filter(|(role, _, _)| *role == rule.from_role)
        {
            emitters
                .entry(address.clone())
                .and_modify(|existing| *existing = (*existing).min(*start))
                .or_insert(*start);
        }
        if emitters.is_empty() {
            insert_discovery_rule(
                rules,
                manifest,
                rule,
                None,
                false,
                discovery_producer_topic0(manifest, &rule.edge_kind, None, false)?,
                0,
            );
        }
        if rule.edge_kind == "resolver" && resolver_accepts_announced_registries {
            // `RegistryCreated` admits a registry without a declaration role, and that registry
            // can emit `ResolverUpdated`. Keep this actual path distinct from the conservative
            // emitterless-rule placeholder.
            // (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L66 @ ens_v2@a971bd64)
            // (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L478 @ ens_v2@a971bd64)
            insert_discovery_rule(rules, manifest, rule, None, true, resolver_topic.clone(), 0);
        }
        for (address, start) in emitters {
            let producer_topic0 =
                discovery_producer_topic0(manifest, &rule.edge_kind, Some(&rule.from_role), false)?;
            insert_discovery_rule(
                rules,
                manifest,
                rule,
                Some(address),
                false,
                producer_topic0.clone(),
                start,
            );
        }
    }
    Ok(())
}

fn discovery_producer_topic0(
    manifest: &SourceManifest,
    edge_kind: &str,
    emitter_role: Option<&str>,
    announcement_backed: bool,
) -> Result<Option<String>> {
    let (name, signature, normalized_event) = match (manifest.source_family.as_str(), edge_kind) {
        ("ens_v2_registry_l1", "registry_announcement") => {
            ("RegistryCreated", "RegistryCreated()", "RegistryCreated")
        }
        ("ens_v2_registry_l1" | "ens_v2_root_l1", "resolver") => (
            "ResolverUpdated",
            "ResolverUpdated(uint256,address,address)",
            "ResolverChanged",
        ),
        _ => return Ok(None),
    };
    for event in manifest.abi.events.iter().filter(|event| {
        event.name == name
            && event
                .normalized_events
                .iter()
                .any(|declared| declared == normalized_event)
            && (event.emitter_roles.is_empty()
                || edge_kind == "registry_announcement"
                || announcement_backed
                || emitter_role.is_some_and(|role| {
                    event
                        .emitter_roles
                        .iter()
                        .any(|candidate| candidate == role)
                }))
    }) {
        let parsed = event.parsed_event_view()?;
        if parsed.canonical_signature() == signature {
            return Ok(parsed.topic0());
        }
    }
    Ok(None)
}

fn insert_discovery_rule(
    rules: &mut BTreeMap<DiscoveryRuleKey, u64>,
    manifest: &SourceManifest,
    rule: &crate::DiscoveryRule,
    emitting_address: Option<String>,
    announcement_backed: bool,
    producer_topic0: Option<String>,
    start: u64,
) {
    rules
        .entry(DiscoveryRuleKey {
            namespace: manifest.namespace.clone(),
            family: manifest.source_family.clone(),
            edge_kind: rule.edge_kind.clone(),
            from_role: rule.from_role.clone(),
            admission: rule.admission.clone(),
            emitting_address,
            announcement_backed,
            producer_topic0,
        })
        .and_modify(|existing| *existing = (*existing).min(start))
        .or_insert(start);
}

fn insert_watch(
    watch: &mut BTreeMap<WatchKey, u64>,
    emitter: WatchEmitter,
    topic0: &str,
    start: u64,
) {
    watch
        .entry(WatchKey {
            emitter,
            topic0: topic0.to_owned(),
        })
        .and_modify(|existing| *existing = (*existing).min(start))
        .or_insert(start);
}

pub(super) fn watch_is_covered(
    previous: Option<&BTreeMap<WatchKey, u64>>,
    desired: &WatchKey,
    desired_start: u64,
) -> bool {
    let Some(previous) = previous else {
        return false;
    };
    let covered = |emitter| {
        previous
            .get(&WatchKey {
                emitter,
                topic0: desired.topic0.clone(),
            })
            .is_some_and(|previous_start| *previous_start <= desired_start)
    };
    if covered(WatchEmitter::All) {
        return true;
    }
    match &desired.emitter {
        WatchEmitter::All => false,
        WatchEmitter::Family { namespace, family } => covered(WatchEmitter::Family {
            namespace: namespace.clone(),
            family: family.clone(),
        }),
        WatchEmitter::Address { family, address } => covered(WatchEmitter::Address {
            family: family.clone(),
            address: address.clone(),
        }),
    }
}
