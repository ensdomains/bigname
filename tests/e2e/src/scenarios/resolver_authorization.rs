use alloy_primitives::Address;
use anyhow::{Context, Result};
use serde_json::{Value, json};

use super::support;
use crate::harness::{anvil::Anvil, ens_v1, repo_root};

const YEAR: u64 = 365 * 24 * 60 * 60;

async fn raw_sender(run: &support::PipelineRun, transaction_hash: &str) -> Result<String> {
    sqlx::query_scalar(
        "SELECT from_address FROM raw_transactions \
         WHERE transaction_hash = $1 AND canonicality_state = 'canonical'",
    )
    .bind(transaction_hash)
    .fetch_one(&run.db.pool)
    .await
    .with_context(|| format!("load raw sender for transaction {transaction_hash}"))
}

fn assert_sender(actual: &str, expected: Address, context: &str) {
    assert_eq!(
        actual,
        format!("{expected:#x}"),
        "{context} should retain the transaction's true sender"
    );
}

/// Registry-wide operators and resolver node delegates are distinct approval
/// paths on the pinned contracts.
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L19 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L112 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L98 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L128 @ ens_v1@91c966f)
#[tokio::test]
async fn operator_delegate_writes_match_owner_authorship() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (owner, operator) = (accounts[1], accounts[2]);
    let resolver = deployment.public_resolver.address;

    for label in ["ownerwrites", "delegatewrites"] {
        ens_v1::register_eth_name(&rpc, &deployment, label, owner, YEAR, resolver).await?;
    }

    ens_v1::create_subname(&rpc, &deployment, owner, "ownerwrites.eth", "child", owner).await?;
    ens_v1::set_text_record(
        &rpc,
        resolver,
        owner,
        "ownerwrites.eth",
        "description",
        "same value",
    )
    .await?;

    ens_v1::set_registry_approval_for_all(&rpc, &deployment, owner, operator, true).await?;
    ens_v1::approve_resolver_delegate(&rpc, resolver, owner, "delegatewrites.eth", operator, true)
        .await?;
    ens_v1::create_subname(
        &rpc,
        &deployment,
        operator,
        "delegatewrites.eth",
        "child",
        owner,
    )
    .await?;
    ens_v1::set_text_record(
        &rpc,
        resolver,
        operator,
        "delegatewrites.eth",
        "description",
        "same value",
    )
    .await?;

    let owner_child = format!("{:#x}", ens_v1::namehash("child.ownerwrites.eth"));
    let delegate_child = format!("{:#x}", ens_v1::namehash("child.delegatewrites.eth"));
    let ready_sql = format!(
        "SELECT \
           (SELECT count(*) = 2 FROM normalized_events \
            WHERE logical_name_id IN ('ens:ownerwrites.eth', 'ens:delegatewrites.eth') \
              AND event_kind = 'RecordChanged' \
              AND after_state->>'record_key' = 'text:description' \
              AND canonicality_state = 'canonical') \
         AND EXISTS (SELECT 1 FROM normalized_events \
            WHERE event_kind = 'SubregistryChanged' \
              AND after_state->>'child_node' = '{owner_child}' \
              AND canonicality_state = 'canonical') \
         AND EXISTS (SELECT 1 FROM normalized_events \
            WHERE event_kind = 'SubregistryChanged' \
              AND after_state->>'child_node' = '{delegate_child}' \
              AND canonicality_state = 'canonical')"
    );
    let run = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;

    let owner_record: (String, String, String, Value) = sqlx::query_as(
        "SELECT transaction_hash, event_kind, source_family, after_state \
         FROM normalized_events \
         WHERE logical_name_id = 'ens:ownerwrites.eth' \
           AND event_kind = 'RecordChanged' \
           AND after_state->>'record_key' = 'text:description' \
           AND canonicality_state = 'canonical'",
    )
    .fetch_one(&run.db.pool)
    .await?;
    let delegate_record: (String, String, String, Value) = sqlx::query_as(
        "SELECT transaction_hash, event_kind, source_family, after_state \
         FROM normalized_events \
         WHERE logical_name_id = 'ens:delegatewrites.eth' \
           AND event_kind = 'RecordChanged' \
           AND after_state->>'record_key' = 'text:description' \
           AND canonicality_state = 'canonical'",
    )
    .fetch_one(&run.db.pool)
    .await?;

    assert_eq!(owner_record.1, delegate_record.1);
    assert_eq!(owner_record.2, delegate_record.2);
    for field in ["record_key", "record_family", "selector_key", "value"] {
        assert_eq!(
            owner_record.3.get(field),
            delegate_record.3.get(field),
            "delegated record field {field} should match owner-authored semantics"
        );
    }
    assert_eq!(delegate_record.3.get("writer"), None);

    let owner_subname: (String, String, String, Value) = sqlx::query_as(
        "SELECT transaction_hash, event_kind, source_family, after_state \
         FROM normalized_events \
         WHERE event_kind = 'SubregistryChanged' \
           AND after_state->>'child_node' = $1 \
           AND canonicality_state = 'canonical'",
    )
    .bind(&owner_child)
    .fetch_one(&run.db.pool)
    .await?;
    let delegate_subname: (String, String, String, Value) = sqlx::query_as(
        "SELECT transaction_hash, event_kind, source_family, after_state \
         FROM normalized_events \
         WHERE event_kind = 'SubregistryChanged' \
           AND after_state->>'child_node' = $1 \
           AND canonicality_state = 'canonical'",
    )
    .bind(&delegate_child)
    .fetch_one(&run.db.pool)
    .await?;

    assert_eq!(owner_subname.1, delegate_subname.1);
    assert_eq!(owner_subname.2, delegate_subname.2);
    for field in ["source_event", "edge_kind", "owner", "tombstone"] {
        assert_eq!(
            owner_subname.3.get(field),
            delegate_subname.3.get(field),
            "operator-authored subname field {field} should match owner-authored semantics"
        );
    }
    assert_eq!(
        delegate_subname.3.get("owner"),
        Some(&json!(format!("{owner:#x}")))
    );

    let owner_record_sender = raw_sender(&run, &owner_record.0).await?;
    let delegate_record_sender = raw_sender(&run, &delegate_record.0).await?;
    let owner_subname_sender = raw_sender(&run, &owner_subname.0).await?;
    let delegate_subname_sender = raw_sender(&run, &delegate_subname.0).await?;
    assert_sender(&owner_record_sender, owner, "owner-authored record");
    assert_sender(
        &delegate_record_sender,
        operator,
        "delegate-authored record",
    );
    assert_sender(&owner_subname_sender, owner, "owner-authored subname");
    assert_sender(
        &delegate_subname_sender,
        operator,
        "operator-authored subname",
    );

    run.db.cleanup().await?;
    Ok(())
}
