use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};

use super::{CompiledWatchEntry, WatchEmitter, insert_watch};
use crate::{SourceManifest, all_emitter_topic0s, is_address_scoped_approval, normalize_address};

pub(super) fn compile_watch_scope(manifest: &SourceManifest) -> Result<Vec<CompiledWatchEntry>> {
    let mut family_topics = BTreeSet::new();
    let mut role_topics = BTreeMap::<String, BTreeSet<String>>::new();
    for event in &manifest.abi.events {
        let parsed = event.parsed_event_view().with_context(|| {
            format!("failed to compile {} watch topics", manifest.source_family)
        })?;
        let Some(topic0) = parsed.topic0().map(|topic| topic.to_ascii_lowercase()) else {
            continue;
        };
        if is_address_scoped_approval(&manifest.source_family, &parsed.canonical_signature()) {
            for role in &event.emitter_roles {
                role_topics
                    .entry(role.clone())
                    .or_default()
                    .insert(topic0.clone());
            }
        } else {
            family_topics.insert(topic0);
        }
    }

    let family_topics = family_topics.into_iter().collect::<Vec<_>>();
    let all_emitter_topics = all_emitter_topic0s(&manifest.source_family, &family_topics)
        .into_iter()
        .collect::<BTreeSet<_>>();
    let mut watch = BTreeMap::new();
    for topic0 in &all_emitter_topics {
        insert_watch(&mut watch, WatchEmitter::All, topic0, 0);
    }
    if crate::uses_discovered_emitters(&manifest.source_family) {
        for topic0 in &family_topics {
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
    for root in &manifest.roots {
        for topic0 in &family_topics {
            insert_watch(
                &mut watch,
                WatchEmitter::Address {
                    family: manifest.source_family.clone(),
                    address: normalize_address(&root.address),
                },
                topic0,
                root.start_block.unwrap_or(0),
            );
        }
    }
    for contract in &manifest.contracts {
        let role_topics = role_topics
            .get(&contract.role)
            .into_iter()
            .flat_map(BTreeSet::iter);
        for topic0 in family_topics.iter().chain(role_topics) {
            insert_watch(
                &mut watch,
                WatchEmitter::Address {
                    family: manifest.source_family.clone(),
                    address: normalize_address(&contract.address),
                },
                topic0,
                contract.start_block.unwrap_or(0),
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
