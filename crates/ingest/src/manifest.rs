use std::collections::{BTreeMap, BTreeSet};

use bigname_manifests::SourceManifest;
use sqlx::PgPool;

mod announcements;
mod ranges;
#[cfg(test)]
mod tests;

use crate::{
    ErrorKind, IngestError, Result,
    event_signatures::{
        BASENAMES_BASE_RESOLVER_SOURCE_FAMILY, ENS_V1_RESOLVER_SOURCE_FAMILY,
        ENS_V2_REGISTRY_SOURCE_FAMILY, ENS_V2_RESOLVER_SOURCE_FAMILY, registry_announcement_topic0,
    },
};

#[cfg(test)]
use crate::event_signatures::generic_resolver_topic0s;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WatchFilter {
    address_ranges: Vec<AddressRange>,
    all_emitter_ranges: Vec<AllEmitterRange>,
    registry_announcements: Option<RegistryAnnouncementWatch>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchQuery {
    pub from_block: i64,
    pub to_block: i64,
    pub addresses: Vec<String>,
    pub topic0s: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AddressRange {
    address: String,
    from_block: i64,
    to_block: i64,
    topic0s: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AllEmitterRange {
    from_block: i64,
    to_block: i64,
    topic0s: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RegistryAnnouncementWatch {
    announcement_topic0: String,
    scoped_topic0s: Vec<String>,
}

impl WatchFilter {
    pub fn includes(&self, address: &str, topic0: &str, block_number: i64) -> bool {
        self.all_emitter_ranges.iter().any(|range| {
            (range.from_block..=range.to_block).contains(&block_number)
                && range
                    .topic0s
                    .iter()
                    .any(|expected| expected.eq_ignore_ascii_case(topic0))
        }) || self.address_ranges.iter().any(|range| {
            range.address.eq_ignore_ascii_case(address)
                && (range.from_block..=range.to_block).contains(&block_number)
                && range
                    .topic0s
                    .iter()
                    .any(|expected| expected.eq_ignore_ascii_case(topic0))
        })
    }

    pub fn queries(&self) -> Vec<WatchQuery> {
        let mut addresses_by_range_and_topics =
            BTreeMap::<(i64, i64, Vec<String>), BTreeSet<String>>::new();
        for range in &self.address_ranges {
            addresses_by_range_and_topics
                .entry((range.from_block, range.to_block, range.topic0s.clone()))
                .or_default()
                .insert(range.address.clone());
        }
        let mut queries = addresses_by_range_and_topics
            .into_iter()
            .map(|((from_block, to_block, topic0s), addresses)| WatchQuery {
                from_block,
                to_block,
                addresses: addresses.into_iter().collect(),
                topic0s,
            })
            .collect::<Vec<_>>();
        let all_emitter_queries = self
            .all_emitter_ranges
            .iter()
            .map(|range| (range.from_block, range.to_block, range.topic0s.clone()))
            .collect::<BTreeSet<_>>();
        queries.extend(
            all_emitter_queries
                .into_iter()
                .map(|(from_block, to_block, topic0s)| WatchQuery {
                    from_block,
                    to_block,
                    addresses: Vec::new(),
                    topic0s,
                }),
        );
        queries
    }

    pub(crate) fn registry_announcement_topic0(&self) -> Option<&str> {
        self.registry_announcements
            .as_ref()
            .map(|watch| watch.announcement_topic0.as_str())
    }

    pub(crate) fn admit_registry_announcements(
        &mut self,
        announcements: impl IntoIterator<Item = (String, i64)>,
        from_block: i64,
        to_block: i64,
    ) -> Vec<WatchQuery> {
        let Some(watch) = &self.registry_announcements else {
            return Vec::new();
        };
        let topics = watch.scoped_topic0s.clone();
        if topics.is_empty() {
            return Vec::new();
        }
        let mut addresses_by_start = BTreeMap::<i64, BTreeSet<String>>::new();
        for (address, announced_at) in announcements {
            let start = announced_at.max(from_block);
            if start > to_block {
                continue;
            }
            let address = address.to_ascii_lowercase();
            addresses_by_start
                .entry(start)
                .or_default()
                .insert(address.clone());
            self.address_ranges.push(AddressRange {
                address,
                from_block: start,
                to_block,
                topic0s: topics.clone(),
            });
        }
        addresses_by_start
            .into_iter()
            .map(|(from_block, addresses)| WatchQuery {
                from_block,
                to_block,
                addresses: addresses.into_iter().collect(),
                topic0s: topics.clone(),
            })
            .collect()
    }
}

pub async fn load_watch_filter(
    pool: &PgPool,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<WatchFilter> {
    let mut filter = load_persisted_watch_filter(pool, chain_id, from_block, to_block).await?;
    if let Some(announcement_topic0) = filter.registry_announcement_topic0().map(str::to_owned) {
        let announcements =
            announcements::canonical(pool, chain_id, to_block, &announcement_topic0).await?;
        filter.admit_registry_announcements(announcements, from_block, to_block);
    }
    Ok(filter)
}

/// Loads manifest declarations and persisted discovery edges without supplementing the result
/// from retained announcement logs in the requested window.
pub async fn load_persisted_watch_filter(
    pool: &PgPool,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<WatchFilter> {
    let payloads: Vec<(i64, String)> = sqlx::query_as(
        "
        SELECT manifest_id, manifest_payload::text
        FROM manifest_versions
        WHERE chain_id = $1
          AND rollout_status = 'active'
        ORDER BY namespace, source_family
        ",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await
    .map_err(|error| {
        IngestError::database(
            format!("failed to load active manifests for chain {chain_id}"),
            error,
        )
    })?;
    if payloads.is_empty() {
        return Err(IngestError::configuration(format!(
            "chain {chain_id} has no active manifests to define its ingest watch set"
        )));
    }

    let mut topic0s = BTreeSet::new();
    let mut topics_by_manifest = BTreeMap::new();
    let mut all_emitter_topics_by_manifest = BTreeMap::new();
    let mut all_emitter_ranges = Vec::new();
    let announcement_topic0 = registry_announcement_topic0();
    let mut announced_registry_topics = BTreeSet::new();
    for (manifest_id, payload) in payloads {
        let manifest = serde_json::from_str::<SourceManifest>(&payload).map_err(|error| {
            IngestError::with_source(
                ErrorKind::DataIntegrity,
                format!("stored active manifest for chain {chain_id} is invalid"),
                error,
            )
        })?;
        let manifest_topics = manifest.abi.event_topic0s().map_err(|error| {
            IngestError::with_source(
                ErrorKind::DataIntegrity,
                format!(
                    "stored active manifest {} ABI is invalid",
                    manifest.source_family
                ),
                error,
            )
        })?;
        let mut manifest_topics = manifest_topics
            .into_iter()
            .map(|topic| topic.to_ascii_lowercase())
            .collect::<Vec<_>>();
        manifest_topics.sort();
        manifest_topics.dedup();
        for topic in &manifest_topics {
            topic0s.insert(topic.clone());
        }
        if matches!(
            manifest.source_family.as_str(),
            ENS_V1_RESOLVER_SOURCE_FAMILY
                | BASENAMES_BASE_RESOLVER_SOURCE_FAMILY
                | ENS_V2_RESOLVER_SOURCE_FAMILY
                | ENS_V2_REGISTRY_SOURCE_FAMILY
        ) {
            let all_emitter_topics =
                bigname_manifests::all_emitter_topic0s(&manifest.source_family, &manifest_topics);
            all_emitter_topics_by_manifest.insert(
                manifest_id,
                all_emitter_topics.iter().cloned().collect::<BTreeSet<_>>(),
            );
            if !all_emitter_topics.is_empty() {
                all_emitter_ranges.push(AllEmitterRange {
                    from_block,
                    to_block,
                    topic0s: all_emitter_topics,
                });
            }
        }
        if manifest.source_family == ENS_V2_REGISTRY_SOURCE_FAMILY
            && manifest_topics.contains(&announcement_topic0)
        {
            announced_registry_topics.extend(
                manifest_topics
                    .iter()
                    .filter(|topic| *topic != &announcement_topic0)
                    .cloned(),
            );
        }
        topics_by_manifest.insert(manifest_id, manifest_topics);
    }

    let registry_announcements =
        (!announced_registry_topics.is_empty()).then(|| RegistryAnnouncementWatch {
            announcement_topic0: announcement_topic0.clone(),
            scoped_topic0s: announced_registry_topics.into_iter().collect(),
        });

    ranges::validate(pool, chain_id).await?;

    let address_rows: Vec<(String, i64, i64, i64)> = sqlx::query_as(
        "
        WITH declared_intervals AS (
            SELECT lower(address.address) AS address,
                   GREATEST(
                       COALESCE(declaration.start_block_number, 0),
                       COALESCE(address.active_from_block_number, 0)
                   ) AS active_from_block_number,
                   COALESCE(
                       address.active_to_block_number,
                       9223372036854775807
                   ) AS active_to_block_number,
                   manifest.manifest_id AS watch_manifest_id
            FROM manifest_versions manifest
            JOIN manifest_contract_instances declaration
              ON declaration.manifest_id = manifest.manifest_id
             AND declaration.chain_id = manifest.chain_id
            JOIN contract_instance_addresses address
              ON address.contract_instance_id = declaration.contract_instance_id
             AND address.chain_id = manifest.chain_id
            WHERE manifest.chain_id = $1
              AND manifest.rollout_status = 'active'
              AND (
                  address.deactivated_at IS NULL
                  OR address.active_to_block_number IS NOT NULL
              )
        ),
        discovered_intervals AS (
            SELECT lower(address.address) AS address,
                   GREATEST(
                       COALESCE(edge.active_from_block_number, 0),
                       COALESCE(address.active_from_block_number, 0)
                   ) AS active_from_block_number,
                   LEAST(
                       COALESCE(
                           edge.active_to_block_number,
                           9223372036854775807
                       ),
                       COALESCE(
                           address.active_to_block_number,
                           9223372036854775807
                       )
                   ) AS active_to_block_number,
                   COALESCE(target_manifest.manifest_id, source_manifest.manifest_id)
                       AS watch_manifest_id
            FROM discovery_edges edge
            JOIN manifest_versions source_manifest
              ON source_manifest.manifest_id = edge.source_manifest_id
             AND source_manifest.chain_id = edge.chain_id
            LEFT JOIN manifest_versions target_manifest
              ON target_manifest.rollout_status = 'active'
             AND target_manifest.namespace = source_manifest.namespace
             AND target_manifest.chain_id = edge.chain_id
             AND target_manifest.deployment_label = source_manifest.deployment_label
             AND target_manifest.source_family = CASE
                 WHEN edge.edge_kind = 'resolver'
                  AND source_manifest.source_family = 'ens_v1_registry_l1'
                     THEN 'ens_v1_resolver_l1'
                 WHEN edge.edge_kind = 'resolver'
                  AND source_manifest.source_family IN (
                      'ens_v2_registry_l1',
                      'ens_v2_root_l1'
                  )
                     THEN 'ens_v2_resolver_l1'
                 WHEN edge.edge_kind = 'resolver'
                  AND source_manifest.source_family = 'basenames_base_registry'
                     THEN 'basenames_base_resolver'
                 ELSE NULL
             END
            JOIN contract_instance_addresses address
              ON address.contract_instance_id = edge.to_contract_instance_id
             AND address.chain_id = edge.chain_id
            WHERE edge.chain_id = $1
              AND source_manifest.rollout_status = 'active'
              AND edge.canonicality_state <> 'orphaned'
              AND edge.edge_kind IN ('resolver', 'registry_announcement')
              AND (
                  edge.edge_kind <> 'resolver'
                  OR source_manifest.source_family NOT IN (
                      'ens_v1_registry_l1',
                      'ens_v2_registry_l1',
                      'ens_v2_root_l1',
                      'basenames_base_registry'
                  )
                  OR target_manifest.manifest_id IS NOT NULL
              )
              AND (
                  edge.deactivated_at IS NULL
                  OR edge.active_to_block_number IS NOT NULL
              )
              AND (
                  address.deactivated_at IS NULL
                  OR address.active_to_block_number IS NOT NULL
                  OR edge.active_to_block_number IS NOT NULL
              )
              AND (
                  edge.active_from_block_number IS NULL
                  OR address.active_to_block_number IS NULL
                  OR edge.active_from_block_number <= address.active_to_block_number
              )
              AND (
                  address.active_from_block_number IS NULL
                  OR edge.active_to_block_number IS NULL
                  OR address.active_from_block_number <= edge.active_to_block_number
              )
        ),
        watched_intervals AS (
            SELECT * FROM declared_intervals
            UNION
            SELECT * FROM discovered_intervals
        )
        SELECT address,
               GREATEST(active_from_block_number, $2),
               LEAST(active_to_block_number, $3),
               watch_manifest_id
        FROM watched_intervals
        WHERE active_from_block_number <= $3
          AND active_to_block_number >= $2
        ORDER BY address, 2, 3, watch_manifest_id
        ",
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .fetch_all(pool)
    .await
    .map_err(|error| {
        IngestError::database(
            format!("failed to load admitted ingest addresses for chain {chain_id}"),
            error,
        )
    })?;

    if (address_rows.is_empty() && all_emitter_ranges.is_empty()) || topic0s.is_empty() {
        return Err(IngestError::configuration(format!(
            "chain {chain_id} active manifests provide no admitted addresses or event topics"
        )));
    }
    let address_ranges = address_rows
        .into_iter()
        .filter_map(|(address, from_block, to_block, manifest_id)| {
            if from_block > to_block {
                return None;
            }
            let mut topic0s = topics_by_manifest
                .get(&manifest_id)
                .cloned()
                .unwrap_or_default();
            if let Some(all_emitter_topics) = all_emitter_topics_by_manifest.get(&manifest_id) {
                topic0s.retain(|topic| !all_emitter_topics.contains(topic));
            }
            (!topic0s.is_empty()).then_some(AddressRange {
                address,
                from_block,
                to_block,
                topic0s,
            })
        })
        .collect();
    let filter = WatchFilter {
        address_ranges,
        all_emitter_ranges,
        registry_announcements,
    };
    Ok(filter)
}

#[cfg(test)]
fn all_emitter_topics(source_family: &str, manifest_topics: &[String]) -> Vec<String> {
    bigname_manifests::all_emitter_topic0s(source_family, manifest_topics)
}
