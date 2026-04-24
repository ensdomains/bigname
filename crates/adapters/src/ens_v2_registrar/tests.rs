use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use bigname_storage::{
    CanonicalityState, NormalizedEvent, default_database_url, upsert_normalized_events,
};
use serde_json::{Value, json};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
    types::Uuid,
};

use super::{
    REGISTRY_DERIVATION_KIND, SOURCE_FAMILY_ENS_V2_REGISTRAR_L1, decoding::RegistrarObservation,
    event_building::build_registrar_event, raw_logs::RegistrarRawLogRow,
};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

struct TestDatabase {
    admin_pool: PgPool,
    pool: PgPool,
    database_name: String,
}

impl TestDatabase {
    async fn new() -> Result<Self> {
        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| default_database_url().to_owned());
        let base_options = PgConnectOptions::from_str(&database_url)
            .context("failed to parse database URL for ENSv2 registrar tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bn_adapters_e2r_test_{}_{}_{}",
            std::process::id(),
            sequence,
            unique
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for ENSv2 registrar tests")?;
        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect test pool for ENSv2 registrar tests")?;
        bigname_storage::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for ENSv2 registrar tests")?;

        Ok(Self {
            admin_pool,
            pool,
            database_name,
        })
    }

    fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn cleanup(self) -> Result<()> {
        self.pool.close().await;
        sqlx::query(&format!(
            r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
            self.database_name
        ))
        .execute(&self.admin_pool)
        .await
        .with_context(|| format!("failed to drop test database {}", self.database_name))?;
        self.admin_pool.close().await;
        Ok(())
    }
}

#[tokio::test]
async fn ens_v2_registrar_links_pre_regeneration_token_to_registry_resource() -> Result<()> {
    let database = TestDatabase::new().await?;
    let old_token_id = "0x00000000000000000000000000000000000000000000000000000000000000a1";
    let new_token_id = "0x00000000000000000000000000000000000000000000000000000000000000a2";
    let resource_id = Uuid::from_u128(0xfeed);
    let logical_name_id = "ens:alice.eth";

    upsert_normalized_events(
        database.pool(),
        &[
            registry_event(
                "token-resource",
                logical_name_id,
                resource_id,
                "TokenResourceLinked",
                10,
                json!({
                    "token_id": old_token_id,
                    "current_token_id": new_token_id,
                    "upstream_resource": "0x0000000000000000000000000000000000000000000000000000000000000eac",
                }),
            ),
            registry_event(
                "token-regenerated",
                logical_name_id,
                resource_id,
                "TokenRegenerated",
                11,
                json!({
                    "old_token_id": old_token_id,
                    "new_token_id": new_token_id,
                }),
            ),
        ],
    )
    .await?;

    let event = build_registrar_event(
        database.pool(),
        &raw_log(),
        RegistrarObservation::NameRenewed {
            token_id: old_token_id.to_owned(),
            label: "alice".to_owned(),
            duration: 31_536_000,
            new_expiry: 2_000_000_000,
            payment_token: ZERO_ADDRESS_FOR_TEST.to_owned(),
            referrer: format!("0x{}", "00".repeat(32)),
            base: "0x01".to_owned(),
        },
    )
    .await?;

    assert_eq!(event.logical_name_id, Some(logical_name_id.to_owned()));
    assert_eq!(event.resource_id, Some(resource_id));
    assert_eq!(
        event.after_state["token_id"],
        Value::String(old_token_id.to_owned())
    );
    assert_eq!(
        event.after_state["registry_resource_id"],
        Value::String(resource_id.to_string())
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_registrar_links_post_regeneration_token_to_registry_resource() -> Result<()> {
    let database = TestDatabase::new().await?;
    let old_token_id = "0x00000000000000000000000000000000000000000000000000000000000000a1";
    let new_token_id = "0x00000000000000000000000000000000000000000000000000000000000000a2";
    let resource_id = Uuid::from_u128(0xfeee);
    let logical_name_id = "ens:alice.eth";

    upsert_normalized_events(
        database.pool(),
        &[
            registry_event(
                "token-resource-new-path",
                logical_name_id,
                resource_id,
                "TokenResourceLinked",
                10,
                json!({
                    "token_id": old_token_id,
                    "current_token_id": new_token_id,
                    "upstream_resource": "0x0000000000000000000000000000000000000000000000000000000000000eac",
                }),
            ),
            registry_event(
                "token-regenerated-new-path",
                logical_name_id,
                resource_id,
                "TokenRegenerated",
                11,
                json!({
                    "old_token_id": old_token_id,
                    "new_token_id": new_token_id,
                }),
            ),
        ],
    )
    .await?;

    let event = build_registrar_event(
        database.pool(),
        &raw_log(),
        RegistrarObservation::NameRenewed {
            token_id: new_token_id.to_owned(),
            label: "alice".to_owned(),
            duration: 31_536_000,
            new_expiry: 2_000_000_000,
            payment_token: ZERO_ADDRESS_FOR_TEST.to_owned(),
            referrer: format!("0x{}", "00".repeat(32)),
            base: "0x01".to_owned(),
        },
    )
    .await?;

    assert_eq!(event.logical_name_id, Some(logical_name_id.to_owned()));
    assert_eq!(event.resource_id, Some(resource_id));
    assert_eq!(
        event.after_state["token_id"],
        Value::String(new_token_id.to_owned())
    );
    assert_eq!(
        event.after_state["registry_resource_id"],
        Value::String(resource_id.to_string())
    );

    database.cleanup().await
}

fn registry_event(
    suffix: &str,
    logical_name_id: &str,
    resource_id: Uuid,
    event_kind: &str,
    block_number: i64,
    after_state: Value,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: format!("ens-v2-registrar-test:{suffix}"),
        namespace: "ens".to_owned(),
        logical_name_id: Some(logical_name_id.to_owned()),
        resource_id: Some(resource_id),
        event_kind: event_kind.to_owned(),
        source_family: "ens_v2_registry_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some("ethereum-sepolia".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(format!("0xblock{block_number}")),
        transaction_hash: Some(format!("0xtx{block_number}")),
        log_index: Some(0),
        raw_fact_ref: json!({"source": "ens_v2_registrar_test"}),
        derivation_kind: REGISTRY_DERIVATION_KIND.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state,
    }
}

const ZERO_ADDRESS_FOR_TEST: &str = "0x0000000000000000000000000000000000000000";

fn raw_log() -> RegistrarRawLogRow {
    RegistrarRawLogRow {
        chain_id: "ethereum-sepolia".to_owned(),
        block_hash: "0xregistrar".to_owned(),
        block_number: 12,
        transaction_hash: "0xtxregistrar".to_owned(),
        transaction_index: 0,
        log_index: 0,
        emitting_address: "0x00000000000000000000000000000000000000ee".to_owned(),
        topics: Vec::new(),
        data: Vec::new(),
        canonicality_state: CanonicalityState::Finalized,
        source_manifest_id: 1,
        namespace: "ens".to_owned(),
        source_family: SOURCE_FAMILY_ENS_V2_REGISTRAR_L1.to_owned(),
        manifest_version: 1,
    }
}
