use anyhow::{Context, Result};
use serde_json::json;
use sqlx::{Connection, PgConnection};
use uuid::Uuid;

#[sqlx::test]
async fn watch_floor_load_ignores_unrelated_address_on_shared_instance() -> Result<()> {
    let database_url = std::env::var("BIGNAME_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .context("database URL is required for watch-floor test")?;
    let mut connection = PgConnection::connect(&database_url).await?;
    let mut transaction = connection.begin().await?;
    sqlx::query(
        "CREATE TEMP TABLE manifest_versions (
            manifest_id BIGINT PRIMARY KEY,
            chain_id TEXT NOT NULL,
            namespace TEXT NOT NULL,
            source_family TEXT NOT NULL,
            rollout_status TEXT NOT NULL,
            manifest_payload JSONB NOT NULL
        )",
    )
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "CREATE TEMP TABLE manifest_contract_instances (
            manifest_id BIGINT NOT NULL,
            chain_id TEXT NOT NULL,
            contract_instance_id UUID NOT NULL,
            declared_address TEXT NOT NULL,
            start_block_number BIGINT
        )",
    )
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "CREATE TEMP TABLE contract_instance_addresses (
            contract_instance_id UUID NOT NULL,
            chain_id TEXT NOT NULL,
            address TEXT NOT NULL,
            active_from_block_number BIGINT,
            active_to_block_number BIGINT,
            deactivated_at TIMESTAMPTZ
        )",
    )
    .execute(&mut *transaction)
    .await?;

    let chain = "ethereum-mainnet";
    let family = "test_family";
    let address = "0x00000000000000000000000000000000000000aa";
    let unrelated_address = "0x00000000000000000000000000000000000000bb";
    let topic0 = "0x1111111111111111111111111111111111111111111111111111111111111111";
    let instance_id = Uuid::parse_str("00000000-0000-0000-0000-000000000001")?;
    let payload = json!({
        "_bigname_compiled_watch": [{
            "emitter": {"kind": "address", "family": family, "address": address},
            "topic0": topic0,
            "start": 0
        }]
    });
    sqlx::query(
        "INSERT INTO manifest_versions
         (manifest_id, chain_id, namespace, source_family, rollout_status, manifest_payload)
         VALUES (1, $1, 'ens', $2, 'active', $3)",
    )
    .bind(chain)
    .bind(family)
    .bind(payload)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances
         (manifest_id, chain_id, contract_instance_id, declared_address, start_block_number)
         VALUES (1, $1, $2, $3, NULL)",
    )
    .bind(chain)
    .bind(instance_id)
    .bind(address)
    .execute(&mut *transaction)
    .await?;
    for (row_address, active_from, active_to) in [
        (address, 100_i64, None),
        (unrelated_address, 5_i64, Some(99_i64)),
    ] {
        sqlx::query(
            "INSERT INTO contract_instance_addresses
             (contract_instance_id, chain_id, address, active_from_block_number,
              active_to_block_number, deactivated_at)
             VALUES ($1, $2, $3, $4, $5,
                     CASE WHEN $5 IS NULL THEN NULL ELSE NOW() END)",
        )
        .bind(instance_id)
        .bind(chain)
        .bind(row_address)
        .bind(active_from)
        .bind(active_to)
        .execute(&mut *transaction)
        .await?;
    }

    let coverage = super::watch_floors::load(&mut transaction).await?;
    assert_eq!(
        coverage.get(&(
            chain.to_owned(),
            family.to_owned(),
            address.to_owned(),
            topic0.to_owned(),
        )),
        Some(&vec![super::watch::CoverageInterval {
            start: 100,
            end: None,
        }])
    );
    Ok(())
}
