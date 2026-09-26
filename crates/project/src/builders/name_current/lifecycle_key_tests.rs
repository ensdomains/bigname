//! The lifecycle key of an ENSv2 event without a resource (Pro review of 56825409, question 5).
//! Such an event takes the resource of the latest grant or reservation of the same name, registry
//! instance and token id, latest by chain position: block, then transaction and log index with a
//! missing index before any written one, then event id. Before 52042edc the lookup skipped the
//! transaction and log index, so two candidates in one block fell to event id.
//!
//! The input is synthetic. On one registry a token id comes with one resource: `register` mints
//! the token and emits its `TokenResource` once, and registering the label again after its token
//! was minted bumps the token version with the EAC version, so the new token id has its own
//! resource. Two matching candidates with different resources therefore do not come from the
//! contracts; with the same resource the choice cannot change the key.
//! (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L455-L471 @ ens_v2@a971bd64)
use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::json;
use sqlx::raw_sql;

const A: &str = "00000000-0000-0000-0000-00000000000a";
const B: &str = "00000000-0000-0000-0000-00000000000b";
const C: &str = "00000000-0000-0000-0000-00000000000c";
const D: &str = "00000000-0000-0000-0000-00000000000d";

#[tokio::test]
async fn a_resource_less_event_takes_the_latest_candidate_by_chain_position() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("lifecycle_key")).await?;
    let mut tx = database.pool().begin().await?;
    raw_sql(
        "CREATE TEMP TABLE chain_lineage (chain_id text, block_hash text, block_number bigint,
             block_timestamp timestamptz);
         CREATE TEMP TABLE project_events (normalized_event_id bigint, chain_id text,
             block_hash text, logical_name_id text, resource_id uuid, event_kind text,
             source_family text, after_state jsonb, raw_fact_ref jsonb, block_number bigint,
             transaction_index bigint, log_index bigint)",
    )
    .execute(&mut *tx)
    .await?;
    // (id, token, kind, resource, block, transaction index, log index)
    let rows: [(i64, &str, &str, Option<&str>, i64, Option<i64>, Option<i64>); 6] = [
        // Two grants in block 10: the later one by log index has the lower id.
        (
            1,
            "0x01",
            "RegistrationGranted",
            Some(A),
            10,
            Some(0),
            Some(5),
        ),
        (
            2,
            "0x01",
            "RegistrationGranted",
            Some(B),
            10,
            Some(0),
            Some(1),
        ),
        (3, "0x01", "RegistrationRenewed", None, 11, Some(0), Some(0)),
        // A reservation at the block boundary, with no indexes and the higher id, and an indexed
        // one in the same block: the indexed one is later.
        (
            5,
            "0x02",
            "RegistrationReserved",
            Some(C),
            12,
            Some(0),
            Some(0),
        ),
        (6, "0x02", "RegistrationReserved", Some(D), 12, None, None),
        (7, "0x02", "RegistrationReleased", None, 13, None, None),
    ];
    for (id, token, kind, resource, block, transaction, log) in rows {
        sqlx::query(
            "INSERT INTO project_events VALUES ($1, 'chain', 'hash', 'ens:name', $2::uuid, $3,
                 'ens_v2_registry_l1', $4, NULL, $5, $6, $7)",
        )
        .bind(id)
        .bind(resource)
        .bind(kind)
        .bind(json!({"registry_contract_instance_id":"registry","token_id":token}))
        .bind(block)
        .bind(transaction)
        .bind(log)
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query(super::query::STAGE_V2_LIFECYCLE_EVENTS[0])
        .execute(&mut *tx)
        .await?;
    let keys: Vec<(i64, Option<String>)> = sqlx::query_as(
        "SELECT normalized_event_id, lifecycle_key FROM project_v2_lifecycle_events
         WHERE resource_id IS NULL ORDER BY normalized_event_id",
    )
    .fetch_all(&mut *tx)
    .await?;
    assert_eq!(keys, [(3, Some(A.to_owned())), (7, Some(C.to_owned()))]);
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}
