use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{SourceManifest, all_emitter_topic0s, normalize_address};

const COMPILED_WATCH_FIELD: &str = "_bigname_compiled_watch";

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum WatchEmitter {
    All,
    Family { family: String },
    Address { family: String, address: String },
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
struct WatchKey {
    emitter: WatchEmitter,
    topic0: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CompiledWatchEntry {
    emitter: WatchEmitter,
    topic0: String,
    start: u64,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DiscoveryRuleKey {
    family: String,
    edge_kind: String,
    from_role: String,
    admission: String,
}

#[derive(Default)]
pub(super) struct Snapshot {
    watch_by_chain: BTreeMap<String, BTreeMap<WatchKey, u64>>,
    discovery_by_chain: BTreeMap<String, BTreeMap<DiscoveryRuleKey, u64>>,
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
        insert_watch(watch, entry.emitter, &entry.topic0, entry.start);
    }
    record_discovery_rules(snapshot, manifest);
    Ok(())
}

pub(super) fn widening_start(
    previous: &Snapshot,
    desired: &Snapshot,
    chain_id: &str,
) -> Option<u64> {
    let previous = previous.watch_by_chain.get(chain_id);
    desired
        .watch_by_chain
        .get(chain_id)?
        .iter()
        .filter_map(|(key, start)| (!watch_is_covered(previous, key, *start)).then_some(*start))
        .min()
}

pub(super) fn discovery_widening_start(
    previous: &Snapshot,
    desired: &Snapshot,
    chain_id: &str,
) -> Option<u64> {
    let previous = previous.discovery_by_chain.get(chain_id);
    desired
        .discovery_by_chain
        .get(chain_id)?
        .iter()
        .filter_map(|(rule, start)| {
            let covered = previous
                .and_then(|rules| rules.get(rule))
                .is_some_and(|previous_start| previous_start <= start);
            (!covered).then_some(*start)
        })
        .min()
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

fn record_discovery_rules(snapshot: &mut Snapshot, manifest: &SourceManifest) {
    let starts = manifest
        .roots
        .iter()
        .map(|root| (root.name.as_str(), root.start_block.unwrap_or(0)))
        .chain(
            manifest
                .contracts
                .iter()
                .map(|contract| (contract.role.as_str(), contract.start_block.unwrap_or(0))),
        )
        .collect::<Vec<_>>();
    let rules = snapshot
        .discovery_by_chain
        .entry(manifest.chain.clone())
        .or_default();
    for rule in manifest
        .discovery_rules
        .iter()
        .filter(|rule| rule.edge_kind == "resolver")
    {
        let start = starts
            .iter()
            .filter_map(|(role, start)| (*role == rule.from_role).then_some(*start))
            .min()
            .unwrap_or(0);
        rules.insert(
            DiscoveryRuleKey {
                family: manifest.source_family.clone(),
                edge_kind: rule.edge_kind.clone(),
                from_role: rule.from_role.clone(),
                admission: rule.admission.clone(),
            },
            start,
        );
    }
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

fn watch_is_covered(
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
        WatchEmitter::Family { family } => covered(WatchEmitter::Family {
            family: family.clone(),
        }),
        WatchEmitter::Address { family, address } => covered(WatchEmitter::Address {
            family: family.clone(),
            address: address.clone(),
        }),
    }
}
