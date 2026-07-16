use std::collections::{BTreeSet, HashMap, HashSet};

use anyhow::Result;
use bigname_domain::block_interval::{InclusiveBlockInterval, coalesce_inclusive_block_intervals};

use super::super::super::*;

pub(in crate::ens_v1_unwrapped_authority) struct AuthorityRawLogStreamSourceRouter<'a> {
    watched_emitters: Vec<RoutedActiveEmitter>,
    watched_emitters_by_address: HashMap<String, Vec<usize>>,
    watched_topic0s: HashSet<String>,
    generic_resolver_event_sources: &'a [GenericResolverEventSource],
    generic_topic0s: HashSet<String>,
    generic_resolver_emitter_addresses: Option<HashSet<String>>,
    profile_context_emitter_addresses: Vec<String>,
}

#[derive(Clone, Copy)]
pub(super) enum AuthorityRawLogStreamSource<'a> {
    Watched(&'a RoutedActiveEmitter),
    Generic(&'a GenericResolverEventSource),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct RoutedEmitterKey {
    source_manifest_id: i64,
    namespace: String,
    source_family: String,
    manifest_version: i64,
    normalizer_version: String,
    contract_role: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RoutedActiveEmitter {
    source_manifest_id: i64,
    namespace: String,
    source_family: String,
    manifest_version: i64,
    normalizer_version: String,
    contract_role: Option<String>,
    active_from_block_number: i64,
    active_to_block_number: i64,
}

impl<'a> AuthorityRawLogStreamSourceRouter<'a> {
    pub(in crate::ens_v1_unwrapped_authority) fn new(
        active_emitters: &'a [ActiveEmitter],
        generic_resolver_event_sources: &'a [GenericResolverEventSource],
        event_topics: &AuthorityEventTopics,
        generic_resolver_emitter_addresses: Option<&[String]>,
    ) -> Result<Self> {
        let mut source_emitters_by_address = HashMap::<&str, Vec<&ActiveEmitter>>::new();
        for emitter in active_emitters {
            source_emitters_by_address
                .entry(emitter.address.as_str())
                .or_default()
                .push(emitter);
        }

        let mut watched_emitters = Vec::<RoutedActiveEmitter>::new();
        let mut watched_emitters_by_address = HashMap::<String, Vec<usize>>::new();
        for (address, emitters) in source_emitters_by_address {
            append_routed_emitters_for_address(
                address,
                &emitters,
                &mut watched_emitters,
                &mut watched_emitters_by_address,
            );
        }

        let watched_topic0s = event_topics
            .authority_replay_event_topic0s()
            .into_iter()
            .map(|topic0| topic0.to_ascii_lowercase())
            .collect::<HashSet<_>>();

        let generic_topic0s = if generic_resolver_event_sources.is_empty() {
            HashSet::new()
        } else {
            event_topics
                .ens_resolver_event_topic0s()?
                .into_iter()
                .map(|topic0| topic0.to_ascii_lowercase())
                .collect::<HashSet<_>>()
        };

        Ok(Self {
            watched_emitters,
            watched_emitters_by_address,
            watched_topic0s,
            generic_resolver_event_sources,
            generic_topic0s,
            generic_resolver_emitter_addresses: generic_resolver_emitter_addresses
                .map(|addresses| addresses.iter().cloned().collect::<HashSet<_>>()),
            profile_context_emitter_addresses: active_emitters
                .iter()
                .filter(|emitter| {
                    !matches!(
                        emitter.source_family.as_str(),
                        SOURCE_FAMILY_ENS_V1_RESOLVER_L1 | SOURCE_FAMILY_BASENAMES_BASE_RESOLVER
                    )
                })
                .map(|emitter| emitter.address.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
        })
    }

    pub(super) fn source_candidates<'router>(
        &'router self,
        emitting_address: &str,
        topic0: &str,
        block_number: i64,
    ) -> Vec<AuthorityRawLogStreamSource<'router>> {
        let mut candidates = Vec::new();
        if self.watched_topic0s.contains(topic0) {
            if let Some(emitter_indexes) = self.watched_emitters_by_address.get(emitting_address) {
                candidates.extend(
                    emitter_indexes
                        .iter()
                        .copied()
                        .map(|index| &self.watched_emitters[index])
                        .filter(|emitter| emitter_active_at_block(emitter, block_number))
                        .map(AuthorityRawLogStreamSource::Watched),
                );
            }
        }
        if self.generic_topic0s.contains(topic0)
            && self
                .generic_resolver_emitter_addresses
                .as_ref()
                .is_none_or(|addresses| addresses.contains(emitting_address))
        {
            candidates.extend(
                self.generic_resolver_event_sources
                    .iter()
                    .filter(|source| generic_source_active_at_block(source, block_number))
                    .map(AuthorityRawLogStreamSource::Generic),
            );
        }

        candidates.sort_by(|left, right| {
            left.source_family()
                .cmp(right.source_family())
                .then(left.source_manifest_id().cmp(&right.source_manifest_id()))
        });
        let mut seen_source_families = HashSet::<String>::new();
        candidates
            .into_iter()
            .filter(|candidate| seen_source_families.insert(candidate.source_family().to_owned()))
            .collect()
    }

    pub(super) fn topic0_filters(&self) -> Vec<String> {
        self.watched_topic0s
            .iter()
            .chain(self.generic_topic0s.iter())
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub(super) fn profile_context_emitter_addresses(&self) -> &[String] {
        &self.profile_context_emitter_addresses
    }
}

fn append_routed_emitters_for_address(
    address: &str,
    emitters: &[&ActiveEmitter],
    watched_emitters: &mut Vec<RoutedActiveEmitter>,
    watched_emitters_by_address: &mut HashMap<String, Vec<usize>>,
) {
    let mut intervals_by_key = HashMap::<RoutedEmitterKey, Vec<InclusiveBlockInterval>>::new();
    for emitter in emitters {
        intervals_by_key
            .entry(RoutedEmitterKey::from_emitter(emitter))
            .or_default()
            .push(emitter_interval(emitter));
    }

    let mut address_indexes = Vec::new();
    for (key, intervals) in intervals_by_key {
        for interval in coalesce_inclusive_block_intervals(intervals) {
            let index = watched_emitters.len();
            watched_emitters.push(RoutedActiveEmitter {
                source_manifest_id: key.source_manifest_id,
                namespace: key.namespace.clone(),
                source_family: key.source_family.clone(),
                manifest_version: key.manifest_version,
                normalizer_version: key.normalizer_version.clone(),
                contract_role: key.contract_role.clone(),
                active_from_block_number: interval.from_block(),
                active_to_block_number: interval.through_block(),
            });
            address_indexes.push(index);
        }
    }

    address_indexes.sort_by(|left, right| {
        let left = &watched_emitters[*left];
        let right = &watched_emitters[*right];
        left.source_family
            .cmp(&right.source_family)
            .then(left.source_manifest_id.cmp(&right.source_manifest_id))
            .then(
                left.active_from_block_number
                    .cmp(&right.active_from_block_number),
            )
            .then(
                left.active_to_block_number
                    .cmp(&right.active_to_block_number),
            )
    });
    watched_emitters_by_address.insert(address.to_owned(), address_indexes);
}

impl RoutedEmitterKey {
    fn from_emitter(emitter: &ActiveEmitter) -> Self {
        Self {
            source_manifest_id: emitter.source_manifest_id,
            namespace: emitter.namespace.clone(),
            source_family: emitter.source_family.clone(),
            manifest_version: emitter.manifest_version,
            normalizer_version: emitter.normalizer_version.clone(),
            contract_role: emitter.contract_role.clone(),
        }
    }
}

fn emitter_interval(emitter: &ActiveEmitter) -> InclusiveBlockInterval {
    InclusiveBlockInterval::new(
        emitter.active_from_block_number.unwrap_or(0),
        emitter.active_to_block_number.unwrap_or(i64::MAX),
    )
    .expect("watched emitter interval must not be inverted")
}

impl AuthorityRawLogStreamSource<'_> {
    pub(super) fn source_manifest_id(&self) -> i64 {
        match self {
            Self::Watched(emitter) => emitter.source_manifest_id,
            Self::Generic(source) => source.source_manifest_id,
        }
    }

    pub(super) fn namespace(&self) -> &str {
        match self {
            Self::Watched(emitter) => &emitter.namespace,
            Self::Generic(source) => &source.namespace,
        }
    }

    pub(super) fn source_family(&self) -> &str {
        match self {
            Self::Watched(emitter) => &emitter.source_family,
            Self::Generic(source) => &source.source_family,
        }
    }

    pub(super) fn manifest_version(&self) -> i64 {
        match self {
            Self::Watched(emitter) => emitter.manifest_version,
            Self::Generic(source) => source.manifest_version,
        }
    }

    pub(super) fn normalizer_version(&self) -> &str {
        match self {
            Self::Watched(emitter) => &emitter.normalizer_version,
            Self::Generic(source) => &source.normalizer_version,
        }
    }

    pub(super) fn contract_role(&self) -> Option<&str> {
        match self {
            Self::Watched(emitter) => emitter.contract_role.as_deref(),
            Self::Generic(_) => None,
        }
    }
}

fn emitter_active_at_block(emitter: &RoutedActiveEmitter, block_number: i64) -> bool {
    block_number >= emitter.active_from_block_number
        && block_number <= emitter.active_to_block_number
}

fn generic_source_active_at_block(source: &GenericResolverEventSource, block_number: i64) -> bool {
    source
        .effective_from_block
        .is_none_or(|from_block| block_number >= from_block)
        && source
            .effective_to_block
            .is_none_or(|to_block| block_number <= to_block)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn active_emitter(
        source_family: &str,
        source_manifest_id: i64,
        from_block: i64,
        to_block: Option<i64>,
    ) -> ActiveEmitter {
        ActiveEmitter {
            address: "0x1111111111111111111111111111111111111111".to_owned(),
            contract_instance_id: sqlx::types::Uuid::nil(),
            source_manifest_id,
            namespace: "ens".to_owned(),
            source_family: source_family.to_owned(),
            manifest_version: 7,
            normalizer_version: "normalizer-v7".to_owned(),
            contract_role: Some("registry".to_owned()),
            active_from_block_number: Some(from_block),
            active_to_block_number: to_block,
            source_rank: 0,
        }
    }

    #[test]
    fn routed_emitter_intervals_coalesce_per_metadata_key_in_output_order() {
        let emitters = [
            active_emitter("family-b", 2, i64::MAX, None),
            active_emitter("family-a", 1, 20, Some(20)),
            active_emitter("family-a", 1, 13, Some(15)),
            active_emitter("family-b", 2, i64::MAX - 1, Some(i64::MAX - 1)),
            active_emitter("family-a", 1, 10, Some(12)),
        ];
        let emitter_refs = emitters.iter().collect::<Vec<_>>();
        let mut routed = Vec::new();
        let mut by_address = HashMap::new();

        append_routed_emitters_for_address(
            &emitters[0].address,
            &emitter_refs,
            &mut routed,
            &mut by_address,
        );

        let ordered = by_address[&emitters[0].address]
            .iter()
            .map(|index| &routed[*index])
            .map(|emitter| {
                (
                    emitter.source_family.as_str(),
                    emitter.source_manifest_id,
                    emitter.normalizer_version.as_str(),
                    emitter.contract_role.as_deref(),
                    emitter.active_from_block_number,
                    emitter.active_to_block_number,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            ordered,
            vec![
                ("family-a", 1, "normalizer-v7", Some("registry"), 10, 15,),
                ("family-a", 1, "normalizer-v7", Some("registry"), 20, 20,),
                (
                    "family-b",
                    2,
                    "normalizer-v7",
                    Some("registry"),
                    i64::MAX - 1,
                    i64::MAX,
                ),
            ]
        );
    }
}
