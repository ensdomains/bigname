use std::collections::{BTreeMap, BTreeSet};

use bigname_manifests::SourceManifest;
use sqlx::PgPool;

use crate::{
    ErrorKind, IngestError, Result,
    event_signatures::{ENS_V1_RESOLVER_SOURCE_FAMILY, generic_resolver_topic0s},
};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WatchFilter {
    address_ranges: Vec<AddressRange>,
    all_emitter_ranges: Vec<AllEmitterRange>,
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
}

pub async fn load_watch_filter(
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
        if manifest.source_family == ENS_V1_RESOLVER_SOURCE_FAMILY {
            let all_emitter_topics = all_emitter_topics(&manifest.source_family, &manifest_topics);
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
        topics_by_manifest.insert(manifest_id, manifest_topics);
    }

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
                  AND source_manifest.source_family = 'ens_v2_registry_l1'
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
              AND edge.edge_kind <> 'migration'
              AND (
                  edge.edge_kind <> 'resolver'
                  OR source_manifest.source_family NOT IN (
                      'ens_v1_registry_l1',
                      'ens_v2_registry_l1',
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
    Ok(WatchFilter {
        address_ranges,
        all_emitter_ranges,
    })
}

fn all_emitter_topics(source_family: &str, manifest_topics: &[String]) -> Vec<String> {
    if source_family != ENS_V1_RESOLVER_SOURCE_FAMILY {
        return Vec::new();
    }
    let manifest_topics = manifest_topics.iter().cloned().collect::<BTreeSet<_>>();
    generic_resolver_topic0s()
        .into_iter()
        .filter(|topic| manifest_topics.contains(topic))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn address_range_only_admits_topics_from_its_manifest() {
        let filter = WatchFilter {
            address_ranges: vec![AddressRange {
                address: "0x01".to_owned(),
                from_block: 10,
                to_block: 20,
                topic0s: vec!["0xaa".to_owned()],
            }],
            all_emitter_ranges: Vec::new(),
        };

        assert!(filter.includes("0x01", "0xaa", 10));
        assert!(!filter.includes("0x01", "0xbb", 10));
        assert!(!filter.includes("0x01", "0xaa", 21));
    }

    #[test]
    fn query_windows_do_not_cross_product_manifest_topics() {
        let filter = WatchFilter {
            address_ranges: vec![
                AddressRange {
                    address: "0x01".to_owned(),
                    from_block: 10,
                    to_block: 20,
                    topic0s: vec!["0xaa".to_owned()],
                },
                AddressRange {
                    address: "0x02".to_owned(),
                    from_block: 10,
                    to_block: 20,
                    topic0s: vec!["0xbb".to_owned()],
                },
            ],
            all_emitter_ranges: Vec::new(),
        };

        assert_eq!(
            filter.queries(),
            vec![
                WatchQuery {
                    from_block: 10,
                    to_block: 20,
                    addresses: vec!["0x01".to_owned()],
                    topic0s: vec!["0xaa".to_owned()],
                },
                WatchQuery {
                    from_block: 10,
                    to_block: 20,
                    addresses: vec!["0x02".to_owned()],
                    topic0s: vec!["0xbb".to_owned()],
                },
            ]
        );
    }

    #[test]
    fn generic_resolver_topics_scan_all_emitters() {
        let filter = WatchFilter {
            address_ranges: Vec::new(),
            all_emitter_ranges: vec![AllEmitterRange {
                from_block: 10,
                to_block: 20,
                topic0s: vec!["0xaa".to_owned()],
            }],
        };

        assert!(filter.includes("0x-unlisted", "0xaa", 10));
        assert_eq!(
            filter.queries(),
            vec![WatchQuery {
                from_block: 10,
                to_block: 20,
                addresses: Vec::new(),
                topic0s: vec!["0xaa".to_owned()],
            }]
        );
    }

    #[test]
    fn only_existing_generic_resolver_topics_are_selected_without_addresses() {
        let generic = generic_resolver_topic0s()[0].clone();
        let shared = format!(
            "{}",
            alloy_primitives::keccak256("ApprovalForAll(address,address,bool)".as_bytes())
        );

        assert_eq!(
            all_emitter_topics(
                ENS_V1_RESOLVER_SOURCE_FAMILY,
                &[generic.clone(), shared.clone()],
            ),
            vec![generic]
        );
        assert!(all_emitter_topics("basenames_base_resolver", &[shared]).is_empty());
    }
}
