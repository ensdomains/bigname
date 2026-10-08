//! Registrar grace belongs to a reviewed deployment, never to a suffix alone.
use anyhow::Result;
use serde_json::Value;
use sqlx::PgConnection;
use uuid::Uuid;

use super::{NameFacts, TripleFacts};
use crate::families::control::rows::LifecycleEvent;

// This exact non-proxy ETHRegistry is governed by the reviewed ETHRegistrar / ETHRenewerV1
// with 28-day grace. A custom registry at the same name depth has no inferred grace.
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/ETHRegistry.json:L2 @ ens_v2_sepolia_20261001@07e55a05)
const REGISTRY: &str = "0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4";

#[derive(Clone, Debug)]
pub struct GraceRegistry {
    identifier: String,
    start: i64,
    end: Option<i64>,
}

pub(super) async fn load(
    conn: &mut PgConnection,
    chain: &str,
    triples: &[TripleFacts],
) -> Result<Vec<GraceRegistry>> {
    let ids: Vec<Uuid> = triples
        .iter()
        .filter_map(|triple| triple.key[1].parse().ok())
        .collect();
    if ids.is_empty() || chain != "ethereum-sepolia" {
        return Ok(Vec::new());
    }
    let rows: Vec<(Uuid, Option<i64>, Option<i64>, Value)> = sqlx::query_as(
        "/* storage:families.control.lifecycle.grace_policy */
         SELECT address.contract_instance_id, address.active_from_block_number,
                address.active_to_block_number, manifest.manifest_payload
         FROM bigname_phase.contract_instance_addresses address
         JOIN bigname_phase.manifest_versions manifest ON manifest.manifest_id = address.source_manifest_id
         WHERE address.chain_id = $1 AND address.contract_instance_id = ANY($2)
           AND lower(address.address) = $3 AND manifest.chain_id = $1
           AND manifest.namespace = 'ens' AND manifest.source_family = 'ens_v2_registry_l1'
           AND manifest.deployment_label = 'ens_v2_sepolia_20261001'
           AND manifest.rollout_status = 'active'"
    ).bind(chain).bind(ids).bind(REGISTRY).fetch_all(conn).await?;
    let mut out = Vec::new();
    for (id, start, end, payload) in rows {
        let Some(start) = start else { continue };
        let contracts = payload["contracts"].as_array().into_iter().flatten();
        let declared = contracts
            .filter(|contract| {
                contract["role"] == "registry"
                    && contract["address"]
                        .as_str()
                        .is_some_and(|a| a.eq_ignore_ascii_case(REGISTRY))
                    && contract["proxy_kind"] == "none"
                    && contract["start_block"].as_i64().is_some_and(|b| b <= start)
            })
            .count()
            == 1;
        if declared {
            out.push(GraceRegistry {
                identifier: id.to_string(),
                start,
                end,
            });
        }
    }
    Ok(out)
}

pub(super) fn ens_v2_grace(facts: &NameFacts, event: &LifecycleEvent) -> bool {
    facts
        .triples
        .iter()
        .filter(|triple| {
            triple.state_key() == event.state_key
                || triple.target.as_deref() == event.resource_id.as_deref()
                    && event.resource_id.is_some()
        })
        .any(|triple| {
            facts.grace_registries.iter().any(|registry| {
                registry.identifier == triple.key[1]
                    && registry.start <= event.position.block_number
                    && registry
                        .end
                        .is_none_or(|end| event.position.block_number <= end)
            })
        })
}
