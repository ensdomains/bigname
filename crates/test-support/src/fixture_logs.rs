//! Fails a test whose batch carries a raw log that interpretation never reads.
//!
//! The schema-v2 adapter drops a raw log that no admitted manifest event selects, with no row and
//! no log line. A fixture log encoded under the wrong event signature, or emitted by an address
//! the batch never admits, leaves the test exercising a smaller batch than the one it builds.
//! [`unmatched`] re-derives the selection decision from a finished batch and reports every log
//! that was neither selected nor recorded as a decode skip. It mirrors `Catalog::select` in
//! `crates/adapters/src/schema_v2/catalog.rs`, and replays the admissions the adapter opens and
//! retires inside the batch (`crates/adapters/src/schema_v2/discovery.rs`). The adapters fixture
//! tests compare the static selection with `Catalog::select` on every batch
//! ([`assert_matches_catalog`]). The in-batch replay has no such comparison, so a change to where
//! `discovery.rs` admits must be mirrored in `Replay::new` by hand.
//!
//! A test that drops a log on purpose names it with [`allow_unmatched`] (in-process batches) or
//! [`allow_unmatched_in`] (batches the Interpret engine runs against a test database), with the
//! reason. An allowance that no unmatched log uses fails the test when its scope ends.

mod adapter;
mod allow;
mod report;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};

use alloy_json_abi::Event;
use serde_json::Value;
use uuid::Uuid;

pub use adapter::{
    CheckedPreparedBatch, Snapshot, interpret_checked, prepare_checked,
    prepare_checked_with_provenance,
};
pub use allow::{AllowGuard, DatabaseAllowGuard, allow_unmatched, allow_unmatched_in};

const MIGRATION_REGISTRY_ASSOCIATION_KIND: &str = "migration_registry_creation";

/// A raw log's place in the chain: block number, transaction index, log index.
pub type Position = (i64, i64, i64);

/// The parts of a schema-v2 batch input that decide selection.
#[derive(Clone, Debug, Default)]
pub struct Batch {
    pub manifests: Vec<Manifest>,
    pub rules: Vec<Rule>,
    pub admissions: Vec<Admission>,
    pub logs: Vec<Log>,
}

#[derive(Clone, Debug)]
pub struct Manifest {
    pub manifest_id: i64,
    pub namespace: String,
    pub source_family: String,
    pub chain_id: String,
    pub deployment_label: String,
    pub payload_json: String,
}

#[derive(Clone, Debug)]
pub struct Rule {
    pub manifest_id: i64,
    pub edge_kind: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Admission {
    pub address: String,
    pub source_manifest_id: Option<i64>,
    pub role: Option<String>,
    pub discovery_edge_kind: Option<String>,
    pub discovery_from: Option<Uuid>,
    pub discovery_observation_key: Option<String>,
    pub active_from_block: Option<i64>,
    pub active_to_block: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct Log {
    pub block_number: i64,
    pub block_hash: String,
    pub transaction_hash: String,
    pub transaction_index: i64,
    pub log_index: i64,
    pub emitting_address: String,
    pub topics: Vec<String>,
}

impl Log {
    fn position(&self) -> Position {
        (self.block_number, self.transaction_index, self.log_index)
    }
}

/// The parts of a schema-v2 batch output that change which logs were selected.
#[derive(Clone, Debug, Default)]
pub struct Outcome {
    /// `(block_hash, transaction_hash, log_index)` of every decode skip.
    pub decode_skips: BTreeSet<(String, String, i64)>,
    pub edges: Vec<Edge>,
    pub closures: Vec<Closure>,
    /// `(contract_instance_id, address, active_from_block_number)` of every contract address row.
    pub contract_addresses: Vec<(Uuid, String, i64)>,
}

#[derive(Clone, Debug)]
pub struct Edge {
    pub edge_kind: String,
    pub from: Uuid,
    pub to: Uuid,
    pub source_manifest_id: i64,
    pub observation_key: String,
    pub position: Position,
}

#[derive(Clone, Debug)]
pub struct Closure {
    pub edge_kind: String,
    pub from: Uuid,
    pub observation_key: String,
    pub position: Position,
}

/// Defines `fixture_batch(&BatchInput) -> Batch` and `fixture_outcome(&BatchOutput) -> Outcome`
/// in the calling module for the schema-v2 input and output types it names. The adapters crate's
/// own tests compile a second copy of those types, so the conversion expands where the types are
/// in scope instead of being written against one copy.
#[macro_export]
macro_rules! fixture_log_views {
    ($vis:vis $input:ty, $output:ty) => {
        $vis fn fixture_batch(input: &$input) -> $crate::fixture_logs::Batch {
            $crate::fixture_logs::Batch {
                manifests: input
                    .manifests
                    .iter()
                    .map(|manifest| $crate::fixture_logs::Manifest {
                        manifest_id: manifest.manifest_id,
                        namespace: manifest.namespace.clone(),
                        source_family: manifest.source_family.clone(),
                        chain_id: manifest.chain_id.clone(),
                        deployment_label: manifest.deployment_label.clone(),
                        payload_json: manifest.payload_json.clone(),
                    })
                    .collect(),
                rules: input
                    .discovery_rules
                    .iter()
                    .map(|rule| $crate::fixture_logs::Rule {
                        manifest_id: rule.manifest_id,
                        edge_kind: rule.edge_kind.clone(),
                    })
                    .collect(),
                admissions: input
                    .admissions
                    .iter()
                    .map(|admission| $crate::fixture_logs::Admission {
                        address: admission.address.clone(),
                        source_manifest_id: admission.source_manifest_id,
                        role: admission.role.clone(),
                        discovery_edge_kind: admission.discovery_edge_kind.clone(),
                        discovery_from: admission.discovery_from_contract_instance_id,
                        discovery_observation_key: admission.discovery_observation_key.clone(),
                        active_from_block: admission.active_from_block,
                        active_to_block: admission.active_to_block,
                    })
                    .collect(),
                logs: input
                    .raw_logs
                    .iter()
                    .map(|raw| $crate::fixture_logs::Log {
                        block_number: raw.block_number,
                        block_hash: raw.block_hash.clone(),
                        transaction_hash: raw.transaction_hash.clone(),
                        transaction_index: raw.transaction_index,
                        log_index: raw.log_index,
                        emitting_address: raw.emitting_address.clone(),
                        topics: raw.topics.clone(),
                    })
                    .collect(),
            }
        }

        $vis fn fixture_outcome(output: &$output) -> $crate::fixture_logs::Outcome {
            $crate::fixture_logs::Outcome {
                decode_skips: output
                    .decode_skips
                    .iter()
                    .map(|skip| {
                        (
                            skip.block_hash.clone(),
                            skip.transaction_hash.clone(),
                            skip.log_index,
                        )
                    })
                    .collect(),
                edges: output
                    .discovery_edges
                    .iter()
                    .map(|edge| $crate::fixture_logs::Edge {
                        edge_kind: edge.edge_kind.clone(),
                        from: edge.from_contract_instance_id,
                        to: edge.to_contract_instance_id,
                        source_manifest_id: edge.source_manifest_id,
                        observation_key: edge.observation_key.clone(),
                        position: (
                            edge.active_from_block_number,
                            edge.provenance["transaction_index"]
                                .as_i64()
                                .unwrap_or(i64::MAX),
                            edge.provenance["log_index"].as_i64().unwrap_or(i64::MAX),
                        ),
                    })
                    .collect(),
                closures: output
                    .discovery_edge_closures
                    .iter()
                    .map(|closure| $crate::fixture_logs::Closure {
                        edge_kind: closure.edge_kind.clone(),
                        from: closure.from_contract_instance_id,
                        observation_key: closure.observation_key.clone(),
                        position: (
                            closure.active_to_block_number,
                            closure.transaction_index,
                            closure.log_index,
                        ),
                    })
                    .collect(),
                contract_addresses: output
                    .contract_addresses
                    .iter()
                    .map(|address| {
                        (
                            address.contract_instance_id,
                            address.address.clone(),
                            address.active_from_block_number,
                        )
                    })
                    .collect(),
            }
        }
    };
}

/// One log no catalog candidate selected, with the reason.
#[derive(Clone, Debug)]
pub struct Unmatched {
    pub log: Log,
    pub why: Why,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Why {
    /// The log has no topics.
    NoTopic0,
    /// No admission covers the emitter at this block. `declared_by` lists every manifest event
    /// with this topic0, so a missing admission or a wrong address is visible.
    NoAdmission { declared_by: Vec<DeclaredEvent> },
    /// The emitter is admitted, but no manifest its admissions reach declares this topic0.
    NoTopic {
        admitted: Vec<Admitted>,
        declared: Vec<DeclaredEvent>,
    },
    /// The topic0 is declared, but only for roles, or in manifests, the emitter's admissions do
    /// not carry.
    RoleMismatch {
        admitted: Vec<Admitted>,
        declared: Vec<DeclaredEvent>,
    },
    /// A PublicResolverV2 admission refuses an event outside the public resolver interface.
    Refused { admitted: Vec<Admitted> },
    /// A discovery admission opens for the emitter after this block, but no pre-admission decode
    /// skip was recorded.
    PreAdmissionUnrecorded { admitted_at: i64 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeclaredEvent {
    pub manifest_id: i64,
    pub source_family: String,
    pub name: String,
    pub signature: String,
    pub topic0: String,
    pub emitter_roles: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Admitted {
    pub manifest_id: i64,
    pub source_family: String,
    pub role: Option<String>,
    pub discovery_edge_kind: Option<String>,
}

/// Every log of a finished batch that no catalog candidate selected and no decode skip records.
pub fn unmatched(batch: &Batch, outcome: &Outcome) -> Vec<Unmatched> {
    let oracle = Oracle::new(batch);
    let replay = Replay::new(batch, outcome);
    batch
        .logs
        .iter()
        .filter(|log| {
            !outcome.decode_skips.contains(&(
                log.block_hash.clone(),
                log.transaction_hash.clone(),
                log.log_index,
            ))
        })
        .filter_map(|log| {
            let admissions = replay.admissions_at(&batch.admissions, log);
            let why = match oracle.select(log, &admissions) {
                Verdict::Selected => return None,
                Verdict::Refused => Why::Refused {
                    admitted: oracle.admitted(&admissions),
                },
                Verdict::Dropped => {
                    oracle.why(log, &admissions, replay.later_admission(batch, log))
                }
            };
            Some(Unmatched {
                log: log.clone(),
                why,
            })
        })
        .collect()
}

/// Panics unless every log of the finished batch was selected, recorded as a decode skip, or
/// accepted by an allowance in scope on this thread.
pub fn assert_selectable(batch: &Batch, outcome: &Outcome) {
    let unmatched = unmatched(batch, outcome);
    if !unmatched.is_empty() {
        allow::fail_unless_allowed(&unmatched, &allow::thread_scopes(), None);
    }
}

/// Whether the oracle selects each log of `batch` from its input admissions alone, as
/// `Catalog::new(input).select(raw)` does before any in-batch discovery.
pub fn static_selection(batch: &Batch) -> Vec<bool> {
    let oracle = Oracle::new(batch);
    batch
        .logs
        .iter()
        .map(|log| {
            let admissions = batch
                .admissions
                .iter()
                .filter(|admission| applies(admission, log))
                .collect::<Vec<_>>();
            oracle.select(log, &admissions) == Verdict::Selected
        })
        .collect()
}

/// The drift guard. `catalog[i]` is whether the real `Catalog::select` selected `batch.logs[i]`
/// from the batch's input admissions, or `None` when it returned an error (the adapter fails such
/// a batch on its own). Panics on any log where the oracle disagrees.
pub fn assert_matches_catalog(batch: &Batch, catalog: &[Option<bool>]) {
    assert_eq!(
        batch.logs.len(),
        catalog.len(),
        "the drift guard needs one Catalog::select verdict per raw log"
    );
    let divergent = batch
        .logs
        .iter()
        .zip(static_selection(batch))
        .zip(catalog)
        .filter_map(|((log, oracle), real)| {
            let real = (*real)?;
            (real != oracle).then(|| {
                format!(
                    "  block {} tx {} log_index {} emitter {} topic0 {}: Catalog::select {}, \
                     fixture-log oracle {}",
                    log.block_number,
                    log.transaction_hash,
                    log.log_index,
                    log.emitting_address,
                    log.topics.first().map_or("none", String::as_str),
                    verdict_word(real),
                    verdict_word(oracle),
                )
            })
        })
        .collect::<Vec<_>>();
    assert!(
        divergent.is_empty(),
        "the fixture-log oracle in crates/test-support/src/fixture_logs.rs disagrees with \
         Catalog::select; update the oracle to match the selector, never the test:\n{}",
        divergent.join("\n")
    );
}

fn verdict_word(selected: bool) -> &'static str {
    if selected { "selects" } else { "drops" }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Verdict {
    Selected,
    Refused,
    Dropped,
}

struct Source {
    manifest_id: i64,
    namespace: String,
    source_family: String,
    chain_id: String,
    deployment_label: String,
    resolver_implementations: BTreeMap<String, i64>,
    events: Vec<DeclaredEvent>,
}

struct Oracle<'a> {
    sources: Vec<Source>,
    rules: &'a [Rule],
}

impl<'a> Oracle<'a> {
    fn new(batch: &'a Batch) -> Self {
        let mut sources = batch.manifests.iter().map(decode).collect::<Vec<_>>();
        sources.sort_by_key(|source| source.manifest_id);
        Self {
            sources,
            rules: &batch.rules,
        }
    }

    fn source(&self, manifest_id: i64) -> Option<&Source> {
        self.sources
            .iter()
            .find(|source| source.manifest_id == manifest_id)
    }

    /// The admissions `Catalog::select` reads candidates from, each with its manifest.
    fn usable<'b>(&'b self, admissions: &[&'b Admission]) -> Vec<(&'b Admission, &'b Source)> {
        admissions
            .iter()
            .copied()
            .filter(|admission| {
                admission.discovery_edge_kind.as_deref()
                    != Some(MIGRATION_REGISTRY_ASSOCIATION_KIND)
            })
            .filter_map(|admission| Some((admission, self.source(admission.source_manifest_id?)?)))
            .filter(|(admission, source)| !is_registry_pointer(source, admission))
            .collect()
    }

    /// The manifests each usable admission reaches and the role its candidates are matched under.
    fn reach<'b>(
        &'b self,
        admission: &'b Admission,
        source: &'b Source,
    ) -> Vec<(&'b Source, Option<&'b str>)> {
        let kind = admission.discovery_edge_kind.as_deref();
        match inferred_family(&source.source_family, kind)
            .filter(|family| *family != source.source_family)
        {
            Some(family) => self
                .sources
                .iter()
                .filter(|candidate| {
                    candidate.namespace == source.namespace
                        && candidate.chain_id == source.chain_id
                        && candidate.deployment_label == source.deployment_label
                        && candidate.source_family == family
                })
                .map(|candidate| (candidate, None))
                .collect(),
            None => vec![(source, admission.role.as_deref())],
        }
    }

    fn select(&self, log: &Log, admissions: &[&Admission]) -> Verdict {
        let Some(topic0) = log.topics.first() else {
            return Verdict::Dropped;
        };
        let namespaces = |kind: Option<&str>| {
            admissions
                .iter()
                .filter(|admission| admission.discovery_edge_kind.as_deref() == kind)
                .filter_map(|admission| self.source(admission.source_manifest_id?))
                .map(|source| source.namespace.as_str())
                .collect::<BTreeSet<_>>()
        };
        let declared = namespaces(None);
        let announced = namespaces(Some("registry_announcement"));
        let resolved = namespaces(Some("resolver"));
        let mut ranks = Vec::new();
        let mut refusals = Vec::new();
        for (admission, source) in self.usable(admissions) {
            let namespace = source.namespace.as_str();
            let kind = admission.discovery_edge_kind.as_deref();
            let rank: u8 = match kind {
                Some("registry_announcement") => 1,
                None if announced.contains(namespace) || resolved.contains(namespace) => 0,
                None if !announced.is_empty() || !resolved.is_empty() => 2,
                Some("resolver") if declared.contains(namespace) => 1,
                _ => 0,
            };
            if kind.is_none()
                && admission.role.as_deref() == Some("public_resolver_v2")
                && source.source_family == "ens_v2_resolver_l1"
                && !source.events.iter().any(|event| {
                    event.topic0.eq_ignore_ascii_case(topic0)
                        && public_resolver_v2_signature(&event.signature)
                })
            {
                refusals.push(rank);
            }
            for (candidate, role) in self.reach(admission, source) {
                for event in &candidate.events {
                    if event.topic0.eq_ignore_ascii_case(topic0)
                        && (event.emitter_roles.is_empty()
                            || kind == Some("registry_announcement")
                            || role.is_some_and(|role| {
                                event.emitter_roles.iter().any(|item| item == role)
                            }))
                    {
                        ranks.push(rank);
                    }
                }
            }
        }
        for source in &self.sources {
            for event in &source.events {
                if event.topic0.eq_ignore_ascii_case(topic0)
                    && (self.is_match_all(source, event)
                        || announces_declared_implementation(source, event, log))
                {
                    ranks.push(3);
                }
            }
        }
        if let Some(refusal) = refusals.into_iter().min()
            && !ranks.iter().any(|rank| *rank < refusal)
        {
            return Verdict::Refused;
        }
        if ranks.is_empty() {
            Verdict::Dropped
        } else {
            Verdict::Selected
        }
    }

    fn is_match_all(&self, source: &Source, event: &DeclaredEvent) -> bool {
        match source.source_family.as_str() {
            "ens_v1_resolver_l1" | "basenames_base_resolver" => {
                !bigname_manifests::is_address_scoped_approval(
                    &source.source_family,
                    &event.signature,
                )
            }
            "ens_v2_registry_l1" if event.name == "RegistryCreated" => {
                self.rules.iter().any(|rule| {
                    rule.manifest_id == source.manifest_id
                        && rule.edge_kind == "registry_announcement"
                })
            }
            "ens_v2_resolver_l1" => matches!(
                event.name.as_str(),
                "ResolverCreated" | "NamedResource" | "NamedTextResource" | "NamedAddrResource"
            ),
            _ => false,
        }
    }

    fn admitted(&self, admissions: &[&Admission]) -> Vec<Admitted> {
        self.usable(admissions)
            .into_iter()
            .map(|(admission, source)| Admitted {
                manifest_id: source.manifest_id,
                source_family: source.source_family.clone(),
                role: admission.role.clone(),
                discovery_edge_kind: admission.discovery_edge_kind.clone(),
            })
            .collect()
    }

    fn why(&self, log: &Log, admissions: &[&Admission], later: Option<i64>) -> Why {
        let Some(topic0) = log.topics.first() else {
            return Why::NoTopic0;
        };
        let usable = self.usable(admissions);
        if usable.is_empty() {
            if let Some(admitted_at) = later {
                return Why::PreAdmissionUnrecorded { admitted_at };
            }
            return Why::NoAdmission {
                declared_by: self
                    .sources
                    .iter()
                    .flat_map(|source| &source.events)
                    .filter(|event| event.topic0.eq_ignore_ascii_case(topic0))
                    .cloned()
                    .collect(),
            };
        }
        let reached = usable
            .iter()
            .flat_map(|(admission, source)| self.reach(admission, source))
            .map(|(source, _)| source.manifest_id)
            .collect::<BTreeSet<_>>();
        let events = self
            .sources
            .iter()
            .filter(|source| reached.contains(&source.manifest_id))
            .flat_map(|source| &source.events);
        let admitted = self.admitted(admissions);
        let with_topic = events
            .clone()
            .filter(|event| event.topic0.eq_ignore_ascii_case(topic0))
            .cloned()
            .collect::<Vec<_>>();
        if !with_topic.is_empty() {
            return Why::RoleMismatch {
                admitted,
                declared: with_topic,
            };
        }
        // A manifest the admissions do not reach may still declare the topic0, for the role the
        // emitter was meant to carry.
        let elsewhere = self
            .sources
            .iter()
            .flat_map(|source| &source.events)
            .filter(|event| event.topic0.eq_ignore_ascii_case(topic0))
            .cloned()
            .collect::<Vec<_>>();
        if elsewhere.is_empty() {
            Why::NoTopic {
                admitted,
                declared: events.cloned().collect(),
            }
        } else {
            Why::RoleMismatch {
                admitted,
                declared: elsewhere,
            }
        }
    }
}

/// Replays the admissions the adapter opened and retired while it read the batch.
///
/// An admission opened in a block is live from the start of that block, because the adapter
/// re-reads a block until a pass opens nothing new, keeping the block's admissions and putting
/// back its retirements (`catalog_admissions.rs`, `end_pass`). A retirement, either an edge
/// closure or the replacement an admission performs for its own observation key, takes effect at
/// its log, after that log's own selection.
struct Replay {
    opened: Vec<(Admission, Position)>,
    events: BTreeMap<RetirementKey, Vec<ReplayEvent>>,
}

/// `(edge_kind, from_contract_instance_id, observation_key)`.
type RetirementKey = (String, Uuid, String);
/// A closure (`0`) or an opened admission (`1`, with its index in `Replay::opened`) at a position.
type ReplayEvent = (Position, u8, Option<usize>);

impl Replay {
    /// Mirrors the `catalog.admit` sites in `discovery.rs`: a `resolver` edge admits its target
    /// unless a registry or root manifest produced it (a registry's resolver pointer binds the
    /// name and admits nothing), and a `registry_announcement` edge always admits. Every edge's
    /// closure is replayed whatever its kind.
    fn new(batch: &Batch, outcome: &Outcome) -> Self {
        let pointer_manifests = batch
            .manifests
            .iter()
            .filter(|manifest| {
                matches!(
                    manifest.source_family.as_str(),
                    "ens_v2_registry_l1" | "ens_v2_root_l1"
                )
            })
            .map(|manifest| manifest.manifest_id)
            .collect::<BTreeSet<_>>();
        let mut opened = Vec::new();
        let mut events: BTreeMap<RetirementKey, Vec<ReplayEvent>> = BTreeMap::new();
        for closure in &outcome.closures {
            events
                .entry((
                    closure.edge_kind.clone(),
                    closure.from,
                    closure.observation_key.clone(),
                ))
                .or_default()
                .push((closure.position, 0, None));
        }
        for edge in outcome
            .edges
            .iter()
            .filter(|edge| match edge.edge_kind.as_str() {
                "resolver" => !pointer_manifests.contains(&edge.source_manifest_id),
                "registry_announcement" => true,
                _ => false,
            })
        {
            let addresses = outcome
                .contract_addresses
                .iter()
                .filter(|(instance, _, _)| *instance == edge.to);
            let Some((_, address, _)) = addresses
                .clone()
                .find(|(_, _, block)| *block == edge.position.0)
                .or_else(|| addresses.clone().next())
            else {
                continue;
            };
            events
                .entry((
                    edge.edge_kind.clone(),
                    edge.from,
                    edge.observation_key.clone(),
                ))
                .or_default()
                .push((edge.position, 1, Some(opened.len())));
            opened.push((
                Admission {
                    address: address.clone(),
                    source_manifest_id: Some(edge.source_manifest_id),
                    role: None,
                    discovery_edge_kind: Some(edge.edge_kind.clone()),
                    discovery_from: Some(edge.from),
                    discovery_observation_key: Some(edge.observation_key.clone()),
                    active_from_block: Some(edge.position.0),
                    active_to_block: None,
                },
                edge.position,
            ));
        }
        Self { opened, events }
    }

    fn live(&self, admission: &Admission, own: Option<(usize, Position)>, at: Position) -> bool {
        let (Some(kind), Some(from), Some(key)) = (
            &admission.discovery_edge_kind,
            admission.discovery_from,
            &admission.discovery_observation_key,
        ) else {
            return true;
        };
        let floor = own.map_or(i64::MIN, |(_, position)| position.0);
        let last = self
            .events
            .get(&(kind.clone(), from, key.clone()))
            .into_iter()
            .flatten()
            .filter(|(position, _, _)| *position < at && position.0 >= floor)
            .max();
        match (last, own) {
            (None, _) => true,
            (Some((_, _, opened)), Some((index, _))) => *opened == Some(index),
            (Some(_), None) => false,
        }
    }

    fn admissions_at<'a>(&'a self, input: &'a [Admission], log: &Log) -> Vec<&'a Admission> {
        let at = log.position();
        input
            .iter()
            .filter(|admission| applies(admission, log) && self.live(admission, None, at))
            .chain(
                self.opened
                    .iter()
                    .enumerate()
                    .filter(|(index, (admission, position))| {
                        applies(admission, log)
                            && self.live(admission, Some((*index, *position)), at)
                    })
                    .map(|(_, (admission, _))| admission),
            )
            .collect()
    }

    fn later_admission(&self, batch: &Batch, log: &Log) -> Option<i64> {
        batch
            .admissions
            .iter()
            .chain(self.opened.iter().map(|(admission, _)| admission))
            .filter(|admission| {
                admission.discovery_edge_kind.is_some()
                    && admission
                        .address
                        .eq_ignore_ascii_case(&log.emitting_address)
            })
            .filter_map(|admission| admission.active_from_block)
            .filter(|from| *from > log.block_number)
            .min()
    }
}

fn decode(manifest: &Manifest) -> Source {
    let payload = serde_json::from_str::<Value>(&manifest.payload_json).unwrap_or(Value::Null);
    let events = payload["abi"]["events"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|event| {
            let parsed = Event::parse(event["fragment"].as_str()?.trim()).ok()?;
            Some(DeclaredEvent {
                manifest_id: manifest.manifest_id,
                source_family: manifest.source_family.clone(),
                signature: parsed.signature(),
                topic0: format!("{:#x}", parsed.selector()),
                name: parsed.name,
                emitter_roles: event["emitter_roles"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect(),
            })
        })
        .collect();
    let resolver_implementations = payload["resolver_implementations"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|implementation| {
            Some((
                implementation["address"].as_str()?.to_ascii_lowercase(),
                implementation["start_block"].as_i64().unwrap_or(0),
            ))
        })
        .collect();
    Source {
        manifest_id: manifest.manifest_id,
        namespace: manifest.namespace.clone(),
        source_family: manifest.source_family.clone(),
        chain_id: manifest.chain_id.clone(),
        deployment_label: manifest.deployment_label.clone(),
        resolver_implementations,
        events,
    }
}

fn applies(admission: &Admission, log: &Log) -> bool {
    admission
        .address
        .eq_ignore_ascii_case(&log.emitting_address)
        && admission
            .active_from_block
            .is_none_or(|from| log.block_number >= from)
        && admission
            .active_to_block
            .is_none_or(|to| log.block_number <= to)
}

fn is_registry_pointer(source: &Source, admission: &Admission) -> bool {
    admission.discovery_edge_kind.as_deref() == Some("resolver")
        && matches!(
            source.source_family.as_str(),
            "ens_v2_registry_l1" | "ens_v2_root_l1"
        )
}

fn inferred_family(source_family: &str, edge_kind: Option<&str>) -> Option<&'static str> {
    match (edge_kind, source_family) {
        (Some("resolver"), "ens_v2_registry_l1" | "ens_v2_root_l1") => Some("ens_v2_resolver_l1"),
        (Some("resolver"), "basenames_base_registry") => Some("basenames_base_resolver"),
        (Some("registry_announcement"), "ens_v2_registry_l1") => Some("ens_v2_registry_l1"),
        _ => None,
    }
}

fn announces_declared_implementation(source: &Source, event: &DeclaredEvent, log: &Log) -> bool {
    source.source_family == "ens_v2_resolver_l1"
        && event.name == "Upgraded"
        && log
            .topics
            .get(1)
            .and_then(|topic| topic_address(topic))
            .is_some_and(|implementation| {
                source
                    .resolver_implementations
                    .get(&implementation)
                    .is_some_and(|start| log.block_number >= *start)
            })
}

fn topic_address(topic: &str) -> Option<String> {
    let hex = topic.strip_prefix("0x")?;
    (hex.len() == 64 && hex[..24].bytes().all(|byte| byte == b'0'))
        .then(|| format!("0x{}", hex[24..].to_ascii_lowercase()))
}

fn public_resolver_v2_signature(signature: &str) -> bool {
    matches!(
        signature,
        "ABIChanged(bytes32,uint256)"
            | "AddrChanged(bytes32,address)"
            | "AddressChanged(bytes32,uint256,bytes)"
            | "TextChanged(bytes32,string,string,string)"
            | "ContenthashChanged(bytes32,bytes)"
            | "NameChanged(bytes32,string)"
            | "VersionChanged(bytes32,uint64)"
    )
}
