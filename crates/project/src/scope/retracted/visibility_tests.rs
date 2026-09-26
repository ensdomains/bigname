//! The redo's checks of cited events in the wrapper-effect and resolver seeds (Pro review of
//! 0e609402). A cited event that keeps its id and its canonical event and lineage states but is no
//! longer activated is unreadable to Project, so the row citing it is rescoped; a row whose cited
//! events are all activated is not. The candidate and activated events share one valid
//! correlation-id set, so visibility is the only difference.
use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::json;
use sqlx::{PgPool, Postgres, Transaction, raw_sql};
use uuid::Uuid;

const CHAIN: &str = "chain";
const HASH: &str = "0x05";
const BASELINE: &[&str] = &[
    include_str!("../../../../../schema-v2/baseline/01_chain.sql"),
    include_str!("../../../../../schema-v2/baseline/02_raw_facts.sql"),
    include_str!("../../../../../schema-v2/baseline/03_identity.sql"),
    include_str!("../../../../../schema-v2/baseline/04_manifests.sql"),
    include_str!("../../../../../schema-v2/baseline/05_normalized_events.sql"),
    include_str!("../../../../../schema-v2/baseline/06_projections.sql"),
    include_str!("../../../../../schema-v2/baseline/07_labels.sql"),
    include_str!("../../../../../schema-v2/baseline/08_heartbeats.sql"),
    include_str!("../../../../../schema-v2/baseline/09_divergence.sql"),
    include_str!("../../../../../schema-v2/baseline/10_phase_state.sql"),
];

async fn database(name: &str) -> Result<TestDatabase> {
    let database = TestDatabase::create(TestDatabaseConfig::new(name)).await?;
    for script in BASELINE {
        raw_sql(script).execute(database.pool()).await?;
    }
    sqlx::query(
        "INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp,
             canonicality_state)
         VALUES ($1, $2, 5, to_timestamp(5), 'canonical')",
    )
    .bind(CHAIN)
    .bind(HASH)
    .execute(database.pool())
    .await?;
    Ok(database)
}

/// A canonical event at block 5 with the shared correlation-id set, `activated` or not.
async fn cited_event(pool: &PgPool, identity: &str, activated: bool) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, chain_id, block_number, block_hash, transaction_hash,
             transaction_index, log_index, raw_fact_ref, derivation_kind, canonicality_state,
             before_state, after_state, migration_correlation_ids, consumer_visibility)
         VALUES ($1, 'ens', 'ExpiryChanged', 'ens_v1_wrapper_l1', 1, $2, 5, $3, '0x5tx', 0,
             $4, '{}', 'ens_v1_unwrapped_authority', 'canonical', '{}', '{}',
             ARRAY['correlation-1'], $5)
         RETURNING normalized_event_id",
    )
    .bind(identity)
    .bind(CHAIN)
    .bind(HASH)
    .bind(i64::from(u8::try_from(identity.len())?))
    .bind(if activated { "activated" } else { "candidate" })
    .fetch_one(pool)
    .await?)
}

async fn scope_tables(pool: &PgPool) -> Result<Transaction<'static, Postgres>> {
    let mut transaction = pool.begin().await?;
    super::super::create_scope_tables(&mut transaction).await?;
    Ok(transaction)
}

// A permission summary whose wrapper expiry boundary cites a fuses event and an expiry event. One
// resource has its fuses event made a candidate, one its expiry event, and a control keeps both
// activated.
#[tokio::test]
async fn a_wrapper_boundary_event_no_longer_activated_rescopes_its_resource() -> Result<()> {
    let database = database("retracted_wrapper_visibility").await?;
    let pool = database.pool();
    let mut expected = Vec::new();
    let mut control = None;
    for (case, fuses_activated, expiry_activated) in [
        ("fuses", false, true),
        ("expiry", true, false),
        ("control", true, true),
    ] {
        let resource = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO resources (resource_id, chain_id, block_hash, block_number,
                 canonicality_state) VALUES ($1, $2, $3, 5, 'canonical')",
        )
        .bind(resource)
        .bind(CHAIN)
        .bind(HASH)
        .execute(pool)
        .await?;
        let fuses = cited_event(pool, &format!("{case}-fuses"), fuses_activated).await?;
        let expiry = cited_event(pool, &format!("{case}-expiry-event"), expiry_activated).await?;
        sqlx::query(
            "INSERT INTO permissions_current_resource_summary (resource_id, support_status,
                 provenance, manifest_version) VALUES ($1, 'supported', $2, 1)",
        )
        .bind(resource)
        .bind(json!({
            "chain_id": CHAIN,
            "wrapper_expiry_boundary": {
                "fuses_event_id": fuses.to_string(),
                "expiry_event_id": expiry.to_string()
            }
        }))
        .execute(pool)
        .await?;
        if case == "control" {
            control = Some(resource);
        } else {
            expected.push(resource);
        }
    }
    let mut transaction = scope_tables(pool).await?;
    super::handoffs::seed_wrapper_effect_resources(&mut transaction, CHAIN).await?;
    let mut scoped: Vec<Uuid> =
        sqlx::query_scalar("SELECT resource_id FROM project_scope_permission_effect_resources")
            .fetch_all(&mut *transaction)
            .await?;
    transaction.rollback().await?;
    database.cleanup().await?;
    scoped.sort();
    expected.sort();
    assert_eq!(scoped, expected, "control {control:?} must stay out");
    Ok(())
}

// A resolver row that cites a manifest event and an upgrade event. One resolver has its manifest
// event made a candidate, one its upgrade event, and a control keeps both activated.
#[tokio::test]
async fn a_resolver_citation_no_longer_activated_rescopes_the_resolver() -> Result<()> {
    let database = database("retracted_resolver_visibility").await?;
    let pool = database.pool();
    let mut expected = Vec::new();
    for (case, address, manifest_activated, upgrade_activated) in [
        (
            "manifest",
            "0x00000000000000000000000000000000000000a1",
            false,
            true,
        ),
        (
            "upgrade",
            "0x00000000000000000000000000000000000000a2",
            true,
            false,
        ),
        (
            "control",
            "0x00000000000000000000000000000000000000a3",
            true,
            true,
        ),
    ] {
        let manifest = cited_event(pool, &format!("{case}-manifest"), manifest_activated).await?;
        let upgrade =
            cited_event(pool, &format!("{case}-upgrade-event"), upgrade_activated).await?;
        sqlx::query(
            "INSERT INTO resolver_current (chain_id, resolver_address, support_status, provenance,
                 manifest_version) VALUES ($1, $2, 'supported', $3, 1)",
        )
        .bind(CHAIN)
        .bind(address)
        .bind(json!({
            "manifest_event_id": manifest.to_string(),
            "upgrade_event_id": upgrade.to_string()
        }))
        .execute(pool)
        .await?;
        if case != "control" {
            expected.push(address.to_owned());
        }
    }
    let mut transaction = scope_tables(pool).await?;
    super::resolvers::seed_resolvers(&mut transaction, CHAIN, 5, 5).await?;
    let mut scoped: Vec<String> =
        sqlx::query_scalar("SELECT resolver_address FROM project_scope_resolvers")
            .fetch_all(&mut *transaction)
            .await?;
    transaction.rollback().await?;
    database.cleanup().await?;
    scoped.sort();
    assert_eq!(scoped, expected, "the control resolver must stay out");
    Ok(())
}
