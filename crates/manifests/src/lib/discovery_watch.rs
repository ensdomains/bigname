use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::PgConnection;

use crate::SourceManifest;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct DiscoveryWatchKey {
    pub namespace: String,
    pub target_family: String,
    pub deployment_label: String,
    pub address: String,
    pub topic0: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiscoveryWatchInterval {
    pub from: i64,
    pub to: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveryWatchCoverage {
    pub authority_fingerprint: String,
    pub lineage_orphaning_epoch: i64,
    pub discovered: BTreeMap<DiscoveryWatchKey, Vec<DiscoveryWatchInterval>>,
    pub independently_covered: BTreeMap<(String, String), Vec<DiscoveryWatchInterval>>,
    pub globally_covered_topics: BTreeSet<String>,
}

/// Loads the same active-manifest, deployment, target-family, canonicality, and address/edge
/// interval relation used to build Ingest's discovery-aware address filter. Resolver topics come
/// from the target resolver manifest. The returned discovery map is the complete concrete union;
/// separate physical-coverage fields identify tuples that do not enlarge Ingest's filter.
pub async fn load_discovery_watch_coverage(
    connection: &mut PgConnection,
    chain_id: &str,
) -> Result<DiscoveryWatchCoverage> {
    let (fingerprint, epoch): (String, i64) = sqlx::query_as(
        r#"
        SELECT
            encode(
                public.digest(
                    COALESCE(
                        (SELECT jsonb_agg(
                            manifest.manifest_payload - 'normalizer_version'
                            ORDER BY manifest.namespace, manifest.source_family
                         )::text
                         FROM manifest_versions manifest
                         WHERE manifest.chain_id = $1
                           AND manifest.rollout_status = 'active'),
                        '[]'
                    ),
                    'sha256'
                ),
                'hex'
            ),
            COALESCE(
                (SELECT lineage_orphaning_epoch FROM chain_heads WHERE chain_id = $1),
                0
            )
        "#,
    )
    .bind(chain_id)
    .fetch_one(&mut *connection)
    .await
    .context("failed to load discovery watch authority")?;

    let manifests: Vec<(String, String, String, Value)> = sqlx::query_as(
        "SELECT namespace, source_family, deployment_label, manifest_payload
         FROM manifest_versions
         WHERE chain_id = $1 AND rollout_status = 'active'
         ORDER BY namespace, source_family, deployment_label",
    )
    .bind(chain_id)
    .fetch_all(&mut *connection)
    .await
    .context("failed to load active manifests for discovery watch coverage")?;
    let mut topics_by_target = BTreeMap::<(String, String, String), BTreeSet<String>>::new();
    let mut global_all_emitter = BTreeSet::new();
    for (namespace, family, deployment, payload) in manifests {
        let (topics, all_emitter) = payload_topics(&family, payload)?;
        topics_by_target.insert((namespace, family, deployment), topics);
        global_all_emitter.extend(all_emitter);
    }

    type DiscoveredRow = (String, String, String, String, i64, i64);
    let discovered: Vec<DiscoveredRow> = sqlx::query_as(
        r#"
        SELECT source_manifest.namespace, target_manifest.source_family,
               target_manifest.deployment_label, lower(address.address),
               GREATEST(
                   COALESCE(edge.active_from_block_number, 0),
                   COALESCE(address.active_from_block_number, 0)
               ),
               LEAST(
                   COALESCE(edge.active_to_block_number, 9223372036854775807),
                   COALESCE(address.active_to_block_number, 9223372036854775807)
               )
        FROM discovery_edges edge
        JOIN manifest_versions source_manifest
          ON source_manifest.manifest_id = edge.source_manifest_id
         AND source_manifest.chain_id = edge.chain_id
         AND source_manifest.rollout_status = 'active'
        JOIN manifest_versions target_manifest
          ON target_manifest.rollout_status = 'active'
         AND target_manifest.namespace = source_manifest.namespace
         AND target_manifest.chain_id = edge.chain_id
         AND target_manifest.deployment_label = source_manifest.deployment_label
         AND target_manifest.source_family = CASE
             WHEN edge.edge_kind = 'resolver'
              AND source_manifest.source_family = 'ens_v1_registry_l1'
                 THEN 'ens_v1_resolver_l1'
             WHEN edge.edge_kind = 'resolver'
              AND source_manifest.source_family IN ('ens_v2_registry_l1', 'ens_v2_root_l1')
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
          AND edge.edge_kind = 'resolver'
          AND edge.canonicality_state <> 'orphaned'
          AND (edge.deactivated_at IS NULL OR edge.active_to_block_number IS NOT NULL)
          AND (address.deactivated_at IS NULL
               OR address.active_to_block_number IS NOT NULL
               OR edge.active_to_block_number IS NOT NULL)
          AND (edge.active_from_block_number IS NULL
               OR address.active_to_block_number IS NULL
               OR edge.active_from_block_number <= address.active_to_block_number)
          AND (address.active_from_block_number IS NULL
               OR edge.active_to_block_number IS NULL
               OR address.active_from_block_number <= edge.active_to_block_number)
        ORDER BY 1, 2, 3, 4, 5, 6
        "#,
    )
    .bind(chain_id)
    .fetch_all(&mut *connection)
    .await
    .context("failed to load discovery-derived address intervals")?;

    let mut discovered_by_key = BTreeMap::<DiscoveryWatchKey, Vec<DiscoveryWatchInterval>>::new();
    for (namespace, target_family, deployment_label, address, from, to) in discovered {
        let interval = DiscoveryWatchInterval { from, to };
        if interval.from > interval.to {
            continue;
        }
        let topics = topics_by_target
            .get(&(
                namespace.clone(),
                target_family.clone(),
                deployment_label.clone(),
            ))
            .cloned()
            .unwrap_or_default();
        for topic0 in topics {
            discovered_by_key
                .entry(DiscoveryWatchKey {
                    namespace: namespace.clone(),
                    target_family: target_family.clone(),
                    deployment_label: deployment_label.clone(),
                    address: address.clone(),
                    topic0,
                })
                .or_default()
                .push(interval);
        }
    }
    normalize_map(&mut discovered_by_key);

    let direct: Vec<(String, String, i64, i64)> = sqlx::query_as(
        r#"
        SELECT lower(address.address), lower(compiled.entry ->> 'topic0'),
               GREATEST(
                   COALESCE((compiled.entry ->> 'start')::bigint, 0),
                   COALESCE(address.active_from_block_number, 0)
               ),
               COALESCE(address.active_to_block_number, 9223372036854775807)
        FROM manifest_versions manifest
        CROSS JOIN LATERAL jsonb_array_elements(
            manifest.manifest_payload -> '_bigname_compiled_watch'
        ) AS compiled(entry)
        JOIN manifest_contract_instances declaration
          ON declaration.manifest_id = manifest.manifest_id
         AND declaration.chain_id = manifest.chain_id
         AND lower(declaration.declared_address) =
             lower(compiled.entry -> 'emitter' ->> 'address')
        JOIN contract_instance_addresses address
          ON address.contract_instance_id = declaration.contract_instance_id
         AND address.chain_id = declaration.chain_id
        WHERE manifest.chain_id = $1
          AND manifest.rollout_status = 'active'
          AND compiled.entry -> 'emitter' ->> 'kind' = 'address'
          AND (address.deactivated_at IS NULL OR address.active_to_block_number IS NOT NULL)
          AND GREATEST(
                COALESCE((compiled.entry ->> 'start')::bigint, 0),
                COALESCE(address.active_from_block_number, 0)
              ) <= COALESCE(address.active_to_block_number, 9223372036854775807)
        ORDER BY 1, 2, 3, 4
        "#,
    )
    .bind(chain_id)
    .fetch_all(&mut *connection)
    .await
    .context("failed to load declared address watch intervals")?;
    let mut independently_covered =
        BTreeMap::<(String, String), Vec<DiscoveryWatchInterval>>::new();
    for (address, topic0, from, to) in direct {
        let interval = DiscoveryWatchInterval { from, to };
        if interval.from > interval.to {
            continue;
        }
        independently_covered
            .entry((address, topic0))
            .or_default()
            .push(interval);
    }
    for intervals in independently_covered.values_mut() {
        normalize_intervals(intervals);
    }
    Ok(DiscoveryWatchCoverage {
        authority_fingerprint: fingerprint,
        lineage_orphaning_epoch: epoch,
        discovered: discovered_by_key,
        independently_covered,
        globally_covered_topics: global_all_emitter,
    })
}

fn payload_topics(
    source_family: &str,
    payload: Value,
) -> Result<(BTreeSet<String>, BTreeSet<String>)> {
    if let Some(compiled) = payload
        .get("_bigname_compiled_watch")
        .and_then(Value::as_array)
    {
        let mut topics = BTreeSet::new();
        let mut all_emitter = BTreeSet::new();
        for entry in compiled {
            let topic0 = entry
                .get("topic0")
                .and_then(Value::as_str)
                .context("compiled watch entry is missing its topic0")?
                .to_ascii_lowercase();
            match entry.pointer("/emitter/kind").and_then(Value::as_str) {
                Some("all") => {
                    topics.insert(topic0.clone());
                    all_emitter.insert(topic0);
                }
                Some("family") => {
                    topics.insert(topic0);
                }
                Some("address") => {}
                Some(kind) => anyhow::bail!("compiled watch entry has unknown emitter kind {kind}"),
                None => anyhow::bail!("compiled watch entry is missing its emitter kind"),
            }
        }
        return Ok((topics, all_emitter));
    }
    let Ok(manifest) = serde_json::from_value::<SourceManifest>(payload) else {
        // Old projection-only fixtures contain no compiled physical watch plan.
        return Ok((BTreeSet::new(), BTreeSet::new()));
    };
    let topics = manifest
        .abi
        .events
        .iter()
        .filter(|event| event.emitter_roles.is_empty())
        .map(|event| event.parsed_event_view().map(|event| event.topic0()))
        .collect::<Result<Vec<Option<String>>>>()?
        .into_iter()
        .flatten()
        .map(|topic| topic.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let all_emitter =
        crate::all_emitter_topic0s(source_family, &topics.iter().cloned().collect::<Vec<_>>())
            .into_iter()
            .collect();
    Ok((topics, all_emitter))
}

fn normalize_map(coverage: &mut BTreeMap<DiscoveryWatchKey, Vec<DiscoveryWatchInterval>>) {
    for intervals in coverage.values_mut() {
        normalize_intervals(intervals);
    }
}

pub fn normalize_intervals(intervals: &mut Vec<DiscoveryWatchInterval>) {
    intervals.retain(|interval| interval.from <= interval.to);
    intervals.sort_by_key(|interval| (interval.from, interval.to));
    let mut merged = Vec::<DiscoveryWatchInterval>::with_capacity(intervals.len());
    for next in intervals.drain(..) {
        let Some(current) = merged.last_mut() else {
            merged.push(next);
            continue;
        };
        if next.from <= current.to.saturating_add(1) {
            current.to = current.to.max(next.to);
        } else {
            merged.push(next);
        }
    }
    *intervals = merged;
}

pub fn subtract_intervals(
    desired: &[DiscoveryWatchInterval],
    covered: &[DiscoveryWatchInterval],
) -> Vec<DiscoveryWatchInterval> {
    let mut remaining = Vec::new();
    for desired in desired {
        let mut cursor = desired.from;
        for covered in covered {
            if covered.to < cursor || covered.from > desired.to {
                continue;
            }
            if covered.from > cursor {
                remaining.push(DiscoveryWatchInterval {
                    from: cursor,
                    to: covered.from - 1,
                });
            }
            if covered.to >= desired.to {
                cursor = desired.to.saturating_add(1);
                break;
            }
            cursor = covered.to.saturating_add(1);
        }
        if cursor <= desired.to {
            remaining.push(DiscoveryWatchInterval {
                from: cursor,
                to: desired.to,
            });
        }
    }
    normalize_intervals(&mut remaining);
    remaining
}

#[cfg(test)]
mod tests {
    use super::*;

    fn interval(from: i64, to: i64) -> DiscoveryWatchInterval {
        DiscoveryWatchInterval { from, to }
    }

    #[test]
    fn inclusive_interval_subtraction_keeps_boundary_demand() {
        assert_eq!(
            subtract_intervals(&[interval(5, 10)], &[interval(5, 9)]),
            [interval(10, 10)]
        );
    }

    #[test]
    fn subtraction_handles_internal_and_disjoint_coverage() {
        assert_eq!(
            subtract_intervals(
                &[interval(5, 20)],
                &[interval(1, 4), interval(8, 10), interval(15, 30)]
            ),
            [interval(5, 7), interval(11, 14)]
        );
    }

    #[test]
    fn compiled_address_topics_do_not_expand_discovered_family_coverage() {
        let (topics, all_emitter) = payload_topics(
            "ens_v1_resolver_l1",
            serde_json::json!({
                "_bigname_compiled_watch": [
                    {"emitter": {"kind": "all"}, "topic0": "0x01", "start": 0},
                    {
                        "emitter": {
                            "kind": "family",
                            "namespace": "ens",
                            "family": "ens_v1_resolver_l1"
                        },
                        "topic0": "0x02",
                        "start": 0
                    },
                    {
                        "emitter": {
                            "kind": "address",
                            "family": "ens_v1_resolver_l1",
                            "address": "0x00000000000000000000000000000000000000aa"
                        },
                        "topic0": "0x03",
                        "start": 10
                    }
                ]
            }),
        )
        .expect("compiled emitter kinds are valid");

        assert_eq!(topics, BTreeSet::from(["0x01".into(), "0x02".into()]));
        assert_eq!(all_emitter, BTreeSet::from(["0x01".into()]));
    }
}
