//! Registrar grace belongs to a reviewed deployment, never to a suffix alone.
use anyhow::Result;
use serde_json::Value;
use sqlx::PgConnection;
use uuid::Uuid;

use super::{NameFacts, TripleFacts};
use crate::families::control::rows::LifecycleEvent;

// One row per reviewed deployment: its chain, its deployment label and its exact non-proxy
// ETHRegistry, governed by that deployment's reviewed ETHRegistrar / ETHRenewerV1 with 28-day
// grace. A custom registry at the same name depth has no inferred grace. A redeploy adds a row.
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/ETHRegistry.json:L2 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/ETHRegistry.json:L2 @ ens_v2_mainnet_20261002@b1f97b91)
const DEPLOYMENTS: [(&str, &str, &str); 2] = [
    (
        "ethereum-sepolia",
        "ens_v2_sepolia_20261001",
        "0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4",
    ),
    (
        "ethereum-mainnet",
        "ens_v2_mainnet_20261002",
        "0xf60aba43e1f7f3dab1186ff03b561401c9df85e2",
    ),
];

// The grace is an immutable of the registrar contract, so a deprecated manifest's closed address
// range still carries it for allocations made inside that range. The mainnet ETHRegistrar's
// constructor arguments name this ETHRegistry (word 2) and a 2,419,200-second grace (word 5).
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registrar/ETHRegistrar.sol:L43 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registrar/ETHRegistrar.sol:L97 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/ETHRegistrar.json:L1419 @ ens_v2_mainnet_20261002@b1f97b91)

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
    let ids = triples
        .iter()
        .filter_map(|triple| triple.key[1].parse().ok())
        .collect();
    registries(conn, chain, ids).await
}

async fn registries(
    conn: &mut PgConnection,
    chain: &str,
    ids: Vec<Uuid>,
) -> Result<Vec<GraceRegistry>> {
    let mut out = Vec::new();
    if ids.is_empty() {
        return Ok(out);
    }
    for &(_, label, registry) in DEPLOYMENTS
        .iter()
        .filter(|(deployment_chain, ..)| *deployment_chain == chain)
    {
        let rows: Vec<(Uuid, Option<i64>, Option<i64>, Value)> = sqlx::query_as(
            "/* storage:families.control.lifecycle.grace_policy */
             SELECT address.contract_instance_id, address.active_from_block_number,
                    address.active_to_block_number, manifest.manifest_payload
             FROM bigname_phase.contract_instance_addresses address
             JOIN bigname_phase.manifest_versions manifest ON manifest.manifest_id = address.source_manifest_id
             WHERE address.chain_id = $1 AND address.contract_instance_id = ANY($2)
               AND lower(address.address) = $3 AND manifest.chain_id = $1
               AND manifest.namespace = 'ens' AND manifest.source_family = 'ens_v2_registry_l1'
               AND manifest.deployment_label = $4"
        ).bind(chain).bind(&ids).bind(registry).bind(label).fetch_all(&mut *conn).await?;
        for (id, start, end, payload) in rows {
            let Some(start) = start else { continue };
            let contracts = payload["contracts"].as_array().into_iter().flatten();
            let declared = contracts
                .filter(|contract| {
                    contract["role"] == "registry"
                        && contract["address"]
                            .as_str()
                            .is_some_and(|a| a.eq_ignore_ascii_case(registry))
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

#[cfg(test)]
mod tests {
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};

    use super::*;

    // Bigname deprecating the declaring manifest and closing the address range does not change
    // the registrar's grace, so an allocation inside the closed range keeps it.
    #[tokio::test]
    async fn a_retired_registry_range_keeps_its_registrar_grace() -> Result<()> {
        let database = TestDatabase::create(TestDatabaseConfig::new("grace_policy")).await?;
        let result = check(database.pool()).await;
        database.cleanup().await?;
        result
    }

    async fn check(pool: &sqlx::PgPool) -> Result<()> {
        let (_, _, registry) = DEPLOYMENTS[0];
        let mut conn = pool.acquire().await?;
        let payload = serde_json::json!({"contracts": [{"role": "registry", "address": registry,
            "proxy_kind": "none", "start_block": 10}]});
        sqlx::raw_sql(
            "CREATE SCHEMA bigname_phase;
             CREATE TABLE bigname_phase.manifest_versions (manifest_id bigint PRIMARY KEY,
                 chain_id text, namespace text, source_family text, deployment_label text,
                 rollout_status text, manifest_payload jsonb);
             CREATE TABLE bigname_phase.contract_instance_addresses (contract_instance_id uuid,
                 chain_id text, address text, active_from_block_number bigint,
                 active_to_block_number bigint, source_manifest_id bigint);",
        )
        .execute(&mut *conn)
        .await?;
        let (active, retired) = (Uuid::from_u128(1), Uuid::from_u128(2));
        for (manifest, status, instance, end) in [
            (1_i64, "active", active, None),
            (2, "deprecated", retired, Some(20_i64)),
        ] {
            sqlx::query(
                "INSERT INTO bigname_phase.manifest_versions VALUES
                 ($1,'ethereum-sepolia','ens','ens_v2_registry_l1','ens_v2_sepolia_20261001',$2,$3)",
            )
            .bind(manifest)
            .bind(status)
            .bind(&payload)
            .execute(&mut *conn)
            .await?;
            sqlx::query(
                "INSERT INTO bigname_phase.contract_instance_addresses VALUES
                 ($1,'ethereum-sepolia',$2,10,$3,$4)",
            )
            .bind(instance)
            .bind(registry)
            .bind(end)
            .bind(manifest)
            .execute(&mut *conn)
            .await?;
        }
        let mut loaded: Vec<_> = registries(&mut conn, "ethereum-sepolia", vec![active, retired])
            .await?
            .into_iter()
            .map(|registry| (registry.identifier, registry.start, registry.end))
            .collect();
        loaded.sort();
        assert_eq!(
            loaded,
            [
                (active.to_string(), 10, None),
                (retired.to_string(), 10, Some(20)),
            ]
        );
        Ok(())
    }
}
