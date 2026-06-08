use std::{
    collections::BTreeMap,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use serde_json::json;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

use super::*;
use crate::{RawBlock, default_database_url, upsert_raw_blocks};

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
            .context("failed to parse database URL for normalized-event tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!("bn_st_ne_{}_{}_{}", std::process::id(), sequence, unique);

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for normalized-event tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect normalized-event test pool")?;

        crate::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for normalized-event tests")?;

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

fn normalized_event(
    event_identity: &str,
    event_kind: &str,
    state: CanonicalityState,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: None,
        resource_id: None,
        event_kind: event_kind.to_owned(),
        source_family: "ens_v2_registry_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some("ethereum-mainnet".to_owned()),
        block_number: None,
        block_hash: None,
        transaction_hash: None,
        log_index: None,
        raw_fact_ref: json!({}),
        derivation_kind: "manifest_sync".to_owned(),
        canonicality_state: state,
        before_state: json!({}),
        after_state: json!({"key": event_identity}),
    }
}

fn basenames_primary_claim_source_repair_event() -> NormalizedEvent {
    let mut event = normalized_event(
        "ens-v1-reverse-claim:record:base-primary-transition",
        "RecordChanged",
        CanonicalityState::Canonical,
    );
    event.namespace = "basenames".to_owned();
    event.source_family = "basenames_base_primary".to_owned();
    event.derivation_kind = "ens_v1_reverse_claim".to_owned();
    event.chain_id = Some("base-mainnet".to_owned());
    event.block_number = Some(46_723_622);
    event.block_hash =
        Some("0x85fbd2e5085b1a1deb62dc0ff2e1a7fc792ef98fb6b1e944890603d699060d84".to_owned());
    event.transaction_hash =
        Some("0x3e6b60619f99ffeb27235dfa86417ebc4d21a9dfb88104cf4bd1243184288ae9".to_owned());
    event.log_index = Some(578);
    event.raw_fact_ref = json!({
        "kind": "raw_log",
        "chain_id": "base-mainnet",
        "block_number": 46723622,
        "block_hash": "0x85fbd2e5085b1a1deb62dc0ff2e1a7fc792ef98fb6b1e944890603d699060d84",
        "transaction_hash": "0x3e6b60619f99ffeb27235dfa86417ebc4d21a9dfb88104cf4bd1243184288ae9",
        "transaction_index": 115,
        "log_index": 578,
    });
    event.before_state = json!({});
    event.after_state = json!({
        "source_event": "NameForAddrChanged",
        "record_key": "name",
        "record_family": "name",
        "selector_key": null,
        "raw_name": "alice.base.eth",
        "primary_claim_source": {
            "address": "0x7e50c29692e8d701a375bf53de93b62f9aa47af8",
            "coin_type": "60",
            "namespace": "basenames",
            "reverse_name": "7e50c29692e8d701a375bf53de93b62f9aa47af8.80002105.reverse",
            "reverse_node": "0x76097049b6146b77e9cd73ee786c29ae4eefb49e4772d0a3cefd99f7087760c5",
            "claim_provenance": {
                "contract_role": "reverse_registrar",
                "source_family": "basenames_base_primary",
                "emitting_address": "0x79ea96012eea67a83431f1701b3dff7e37f9e282",
                "contract_instance_id": "86c6cbd2-19e7-4de1-85a0-1a7842fd8c25"
            }
        }
    });
    event
}

fn basenames_repaired_primary_claim_source_event(event: &NormalizedEvent) -> NormalizedEvent {
    let mut repaired = event.clone();
    repaired.after_state["primary_claim_source"] = json!({
        "address": "0x7e50c29692e8d701a375bf53de93b62f9aa47af8",
        "coin_type": "2147492101",
        "namespace": "basenames",
        "reverse_name": "7e50c29692e8d701a375bf53de93b62f9aa47af8.80002105.reverse",
        "reverse_node": "0x76097049b6146b77e9cd73ee786c29ae4eefb49e4772d0a3cefd99f7087760c5",
        "claim_provenance": {
            "contract_role": "reverse_registrar",
            "source_family": "basenames_base_primary",
            "emitting_address": "0x0000000000d8e504002cc26e3ec46d81971c1664",
            "contract_instance_id": "29dfdbc2-902c-4b98-b38d-5169180d6eb6"
        }
    });
    repaired
}

fn mutate_basenames_primary_claim_tuple(event: &mut NormalizedEvent, case: &str) {
    let source = event
        .after_state
        .get_mut("primary_claim_source")
        .and_then(serde_json::Value::as_object_mut)
        .expect("test event must have primary_claim_source object");
    match case {
        "missing_address" => {
            source.remove("address");
        }
        "blank_reverse_node" => {
            source.insert("reverse_node".to_owned(), json!(""));
        }
        "missing_reverse_name" => {
            source.remove("reverse_name");
        }
        "blank_source_family" => {
            source
                .get_mut("claim_provenance")
                .and_then(serde_json::Value::as_object_mut)
                .expect("test event must have claim_provenance object")
                .insert("source_family".to_owned(), json!(""));
        }
        "missing_contract_role" => {
            source
                .get_mut("claim_provenance")
                .and_then(serde_json::Value::as_object_mut)
                .expect("test event must have claim_provenance object")
                .remove("contract_role");
        }
        _ => panic!("unknown Basenames mutation case {case}"),
    }
}

async fn seed_ens_v1_renewal_resource_repair_identity_rows(
    pool: &PgPool,
    old_resource_id: Uuid,
    repaired_resource_id: Uuid,
    old_surface_binding_id: Uuid,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO name_surfaces (
            logical_name_id,
            namespace,
            input_name,
            canonical_display_name,
            normalized_name,
            dns_encoded_name,
            namehash,
            labelhashes,
            normalizer_version,
            normalization_warnings,
            normalization_errors,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state
        )
        VALUES (
            'ens:alice.eth',
            'ens',
            'alice.eth',
            'alice.eth',
            'alice.eth',
            $1,
            '0xalice_namehash',
            ARRAY['0xcbf005454c11bc7e583aa4a100988b4a893acb2233dbb77afef8d9f931df3735'],
            'ensip15@ens-normalize-0.1.1',
            '[]'::jsonb,
            '[]'::jsonb,
            'ethereum-mainnet',
            '0xsurfaceblock',
            25_238_000,
            '{}'::jsonb,
            'canonical'::canonicality_state
        )
        "#,
    )
    .bind(vec![
        5_u8, b'a', b'l', b'i', b'c', b'e', 3, b'e', b't', b'h', 0,
    ])
    .execute(pool)
    .await
    .context("failed to seed ENSv1 renewal repair name surface")?;

    sqlx::query(
        r#"
        INSERT INTO resources (
            resource_id,
            token_lineage_id,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state
        )
        VALUES
        (
            $1,
            NULL,
            'ethereum-mainnet',
            '0xoldresource',
            25_238_970,
            jsonb_build_object(
                'authority_kind', 'registrar',
                'authority_key', 'registrar:ethereum-mainnet:old',
                'logical_name_id', 'ens:alice.eth',
                'labelhash', '0xcbf005454c11bc7e583aa4a100988b4a893acb2233dbb77afef8d9f931df3735',
                'expiry', '1872542016'
            ),
            'canonical'::canonicality_state
        ),
        (
            $2,
            NULL,
            'ethereum-mainnet',
            '0xrepairedresource',
            25_238_000,
            jsonb_build_object(
                'authority_kind', 'registrar',
                'authority_key', 'registrar:ethereum-mainnet:repaired',
                'logical_name_id', 'ens:alice.eth',
                'labelhash', '0xcbf005454c11bc7e583aa4a100988b4a893acb2233dbb77afef8d9f931df3735',
                'expiry', '1872542016'
            ),
            'canonical'::canonicality_state
        )
        "#,
    )
    .bind(old_resource_id)
    .bind(repaired_resource_id)
    .execute(pool)
    .await
    .context("failed to seed ENSv1 renewal repair resources")?;

    sqlx::query(
        r#"
        INSERT INTO surface_bindings (
            surface_binding_id,
            logical_name_id,
            resource_id,
            binding_kind,
            active_from,
            active_to,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state
        )
        VALUES (
            $1,
            'ens:alice.eth',
            $2,
            'declared_registry_path',
            TIMESTAMPTZ '2026-01-01 00:00:00+00',
            NULL,
            'ethereum-mainnet',
            '0xoldbinding',
            25_238_970,
            '{}'::jsonb,
            'canonical'::canonicality_state
        )
        "#,
    )
    .bind(old_surface_binding_id)
    .bind(old_resource_id)
    .execute(pool)
    .await
    .context("failed to seed ENSv1 renewal repair old surface binding")?;

    Ok(())
}

async fn seed_ens_v1_registry_event_time_repair_resources(
    pool: &PgPool,
    old_resource_id: Uuid,
    repaired_resource_id: Uuid,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO resources (
            resource_id,
            token_lineage_id,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state
        )
        VALUES
        (
            $1,
            NULL,
            'ethereum-mainnet',
            '0xlaterregistrarresource',
            200,
            jsonb_build_object(
                'authority_kind', 'registrar',
                'authority_key', 'registrar:ethereum-mainnet:alice',
                'logical_name_id', 'ens:alice.eth',
                'labelhash', '0xcbf005454c11bc7e583aa4a100988b4a893acb2233dbb77afef8d9f931df3735'
            ),
            'canonical'::canonicality_state
        ),
        (
            $2,
            NULL,
            'ethereum-mainnet',
            '0xeventtimeregistryresource',
            90,
            jsonb_build_object(
                'authority_kind', 'registry_only',
                'authority_key', 'registry-only:ethereum-mainnet:0xalice_namehash',
                'logical_name_id', 'ens:alice.eth',
                'namehash', '0xalice_namehash',
                'labelhash', '0xcbf005454c11bc7e583aa4a100988b4a893acb2233dbb77afef8d9f931df3735',
                'current_registry_owner', '0x0000000000000000000000000000000000000123'
            ),
            'canonical'::canonicality_state
        )
        "#,
    )
    .bind(old_resource_id)
    .bind(repaired_resource_id)
    .execute(pool)
    .await
    .context("failed to seed ENSv1 registry event-time repair resources")?;

    Ok(())
}

async fn seed_ens_v1_registry_event_time_wrapper_repair_resources(
    pool: &PgPool,
    old_resource_id: Uuid,
    repaired_resource_id: Uuid,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO resources (
            resource_id,
            token_lineage_id,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state
        )
        VALUES
        (
            $1,
            NULL,
            'ethereum-mainnet',
            '0xlaterwrapperresource',
            200,
            jsonb_build_object(
                'authority_kind', 'wrapper',
                'authority_key', 'wrapper:ethereum-mainnet:alice',
                'logical_name_id', 'ens:alice.eth'
            ),
            'canonical'::canonicality_state
        ),
        (
            $2,
            NULL,
            'ethereum-mainnet',
            '0xeventtimeregistryresource',
            90,
            jsonb_build_object(
                'authority_kind', 'registry_only',
                'authority_key', 'registry-only:ethereum-mainnet:0xalice_namehash',
                'logical_name_id', 'ens:alice.eth',
                'namehash', '0xalice_namehash',
                'labelhash', '0xcbf005454c11bc7e583aa4a100988b4a893acb2233dbb77afef8d9f931df3735',
                'current_registry_owner', '0x0000000000000000000000000000000000000123'
            ),
            'canonical'::canonicality_state
        )
        "#,
    )
    .bind(old_resource_id)
    .bind(repaired_resource_id)
    .execute(pool)
    .await
    .context("failed to seed ENSv1 registry event-time wrapper repair resources")?;

    Ok(())
}

async fn seed_ens_v1_registry_event_time_registry_collision_repair_resources(
    pool: &PgPool,
    old_resource_id: Uuid,
    repaired_resource_id: Uuid,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO resources (
            resource_id,
            token_lineage_id,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state
        )
        VALUES
        (
            $1,
            NULL,
            'ethereum-mainnet',
            '0xlegacyregistrycollisionresource',
            200,
            jsonb_build_object(
                'authority_kind', 'registry_only',
                'authority_key', 'registry-only:ethereum-mainnet:0xshared_cold_labelhash',
                'logical_name_id', 'ens:cold.eth',
                'namehash', '0xcold_eth_namehash',
                'labelhash', '0xshared_cold_labelhash',
                'current_registry_owner', '0x0000000000000000000000000000000000000abc'
            ),
            'canonical'::canonicality_state
        ),
        (
            $2,
            NULL,
            'ethereum-mainnet',
            '0xeventtimeregistrysubnameresource',
            90,
            jsonb_build_object(
                'authority_kind', 'registry_only',
                'authority_key', 'registry-only:ethereum-mainnet:0xcold_highwind_namehash',
                'logical_name_id', 'ens:cold.highwind.eth',
                'namehash', '0xcold_highwind_namehash',
                'labelhash', '0xshared_cold_labelhash',
                'current_registry_owner', '0x0000000000000000000000000000000000000123'
            ),
            'canonical'::canonicality_state
        )
        "#,
    )
    .bind(old_resource_id)
    .bind(repaired_resource_id)
    .execute(pool)
    .await
    .context("failed to seed ENSv1 registry event-time registry collision repair resources")?;

    Ok(())
}

async fn seed_ens_v1_registry_event_time_registry_key_repair_resources(
    pool: &PgPool,
    old_resource_id: Uuid,
    repaired_resource_id: Uuid,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO resources (
            resource_id,
            token_lineage_id,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state
        )
        VALUES
        (
            $1,
            NULL,
            'ethereum-mainnet',
            '0xlegacyregistrykeyresource',
            90,
            jsonb_build_object(
                'authority_kind', 'registry_only',
                'authority_key', 'registry-only:ethereum-mainnet:0xcubebucks_labelhash',
                'logical_name_id', 'ens:cubebucks.eth',
                'labelhash', '0xcubebucks_labelhash',
                'current_registry_owner', '0x0000000000000000000000000000000000000abc'
            ),
            'canonical'::canonicality_state
        ),
        (
            $2,
            NULL,
            'ethereum-mainnet',
            '0xregistrykeyresource',
            90,
            jsonb_build_object(
                'authority_kind', 'registry_only',
                'authority_key', 'registry-only:ethereum-mainnet:0xcubebucks_namehash',
                'logical_name_id', 'ens:cubebucks.eth',
                'namehash', '0xcubebucks_namehash',
                'labelhash', '0xcubebucks_labelhash',
                'current_registry_owner', '0x0000000000000000000000000000000000000abc'
            ),
            'canonical'::canonicality_state
        )
        "#,
    )
    .bind(old_resource_id)
    .bind(repaired_resource_id)
    .execute(pool)
    .await
    .context("failed to seed ENSv1 registry event-time registry key repair resources")?;

    Ok(())
}

async fn seed_ens_v1_same_transaction_registration_setup_repair_resources(
    pool: &PgPool,
    registry_resource_id: Uuid,
    registrar_resource_id: Uuid,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO resources (
            resource_id,
            token_lineage_id,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state
        )
        VALUES
        (
            $1,
            NULL,
            'ethereum-mainnet',
            '0xsametxregistryresource',
            90,
            jsonb_build_object(
                'authority_kind', 'registry_only',
                'authority_key', 'registry-only:ethereum-mainnet:0xalice_namehash',
                'logical_name_id', 'ens:alice.eth',
                'namehash', '0xalice_namehash',
                'labelhash', '0xcbf005454c11bc7e583aa4a100988b4a893acb2233dbb77afef8d9f931df3735',
                'current_registry_owner', '0x0000000000000000000000000000000000000123'
            ),
            'canonical'::canonicality_state
        ),
        (
            $2,
            NULL,
            'ethereum-mainnet',
            '0xsametxregistrationblock',
            100,
            jsonb_build_object(
                'authority_kind', 'registrar',
                'authority_key', 'registrar:ethereum-mainnet:10:0xcbf005454c11bc7e583aa4a100988b4a893acb2233dbb77afef8d9f931df3735:0xsametxregistrationblock:5',
                'logical_name_id', 'ens:alice.eth',
                'labelhash', '0xcbf005454c11bc7e583aa4a100988b4a893acb2233dbb77afef8d9f931df3735',
                'registrant', '0x0000000000000000000000000000000000000123',
                'expiry', 1800000000
            ),
            'canonical'::canonicality_state
        )
        "#,
    )
    .bind(registry_resource_id)
    .bind(registrar_resource_id)
    .execute(pool)
    .await
    .context("failed to seed ENSv1 same-transaction registration setup repair resources")?;

    Ok(())
}

fn ens_v1_renewal_related_event(
    event_identity: &str,
    event_kind: &str,
    resource_id: Uuid,
    after_state: serde_json::Value,
) -> NormalizedEvent {
    let mut event = normalized_event(event_identity, event_kind, CanonicalityState::Canonical);
    event.logical_name_id = Some("ens:alice.eth".to_owned());
    event.resource_id = Some(resource_id);
    event.source_family = "ens_v1_registrar_l1".to_owned();
    event.derivation_kind = "ens_v1_unwrapped_authority".to_owned();
    event.chain_id = Some("ethereum-mainnet".to_owned());
    event.block_number = Some(25_238_970);
    event.block_hash =
        Some("0x9da3c01e4f15f21e87656f7ba57b31a80709464339389cc0194099b0926ce36f".to_owned());
    event.transaction_hash =
        Some("0x93b81927d785859a89e80c3dd900d63da000f38c3f90e09cbaf0ec0908774627".to_owned());
    event.log_index = Some(1059);
    event.raw_fact_ref = json!({
        "kind": "raw_log",
        "chain_id": "ethereum-mainnet",
        "block_number": 25238970,
        "block_hash": "0x9da3c01e4f15f21e87656f7ba57b31a80709464339389cc0194099b0926ce36f",
        "transaction_hash": "0x93b81927d785859a89e80c3dd900d63da000f38c3f90e09cbaf0ec0908774627",
        "transaction_index": 219,
        "log_index": 1059,
    });
    event.before_state = json!({});
    event.after_state = after_state;
    event
}

fn ens_v1_registry_event_time_repair_event(
    event_identity: &str,
    resource_id: Uuid,
) -> NormalizedEvent {
    let mut event = normalized_event(
        event_identity,
        "ResolverChanged",
        CanonicalityState::Canonical,
    );
    event.logical_name_id = Some("ens:alice.eth".to_owned());
    event.resource_id = Some(resource_id);
    event.source_family = "ens_v1_registry_l1".to_owned();
    event.derivation_kind = "ens_v1_unwrapped_authority".to_owned();
    event.chain_id = Some("ethereum-mainnet".to_owned());
    event.block_number = Some(100);
    event.block_hash = Some("0xregistryeventblock".to_owned());
    event.transaction_hash = Some("0xregistryeventtx".to_owned());
    event.log_index = Some(2);
    event.raw_fact_ref = json!({
        "kind": "raw_log",
        "chain_id": "ethereum-mainnet",
        "block_number": 100,
        "block_hash": "0xregistryeventblock",
        "transaction_hash": "0xregistryeventtx",
        "transaction_index": 5,
        "log_index": 2,
    });
    event.before_state = json!({"resolver": null});
    event.after_state = json!({
        "namehash": "0xalice_namehash",
        "resolver": "0x0000000000000000000000000000000000000456"
    });
    event
}

fn ens_v1_registry_event_time_subname_collision_repair_event(
    event_identity: &str,
    resource_id: Uuid,
) -> NormalizedEvent {
    let mut event = ens_v1_registry_event_time_repair_event(event_identity, resource_id);
    event.logical_name_id = Some("ens:cold.highwind.eth".to_owned());
    event.block_hash = Some("0xregistryeventsubnameblock".to_owned());
    event.transaction_hash = Some("0xregistryeventsubnametx".to_owned());
    event.raw_fact_ref = json!({
        "kind": "raw_log",
        "chain_id": "ethereum-mainnet",
        "block_number": 100,
        "block_hash": "0xregistryeventsubnameblock",
        "transaction_hash": "0xregistryeventsubnametx",
        "transaction_index": 5,
        "log_index": 2,
    });
    event.after_state["namehash"] = json!("0xcold_highwind_namehash");
    event
}

fn ens_v1_registry_event_time_authority_transfer_repair_event(
    event_identity: &str,
    resource_id: Uuid,
) -> NormalizedEvent {
    let mut event = normalized_event(
        event_identity,
        "AuthorityTransferred",
        CanonicalityState::Canonical,
    );
    event.logical_name_id = Some("ens:cubebucks.eth".to_owned());
    event.resource_id = Some(resource_id);
    event.source_family = "ens_v1_registry_l1".to_owned();
    event.derivation_kind = "ens_v1_unwrapped_authority".to_owned();
    event.chain_id = Some("ethereum-mainnet".to_owned());
    event.block_number = Some(100);
    event.block_hash = Some("0xregistrytransferblock".to_owned());
    event.transaction_hash = Some("0xregistrytransfertx".to_owned());
    event.log_index = Some(7);
    event.raw_fact_ref = json!({
        "kind": "raw_log",
        "chain_id": "ethereum-mainnet",
        "block_number": 100,
        "block_hash": "0xregistrytransferblock",
        "transaction_hash": "0xregistrytransfertx",
        "transaction_index": 17,
        "log_index": 7,
    });
    event.before_state = json!({
        "owner": "0x0000000000000000000000000000000000000abc"
    });
    event.after_state = json!({
        "labelhash": "0xcubebucks_labelhash",
        "owner": "0x0000000000000000000000000000000000000123"
    });
    event
}

fn ens_v1_registry_event_time_permission_repair_event(
    resource_id: Uuid,
    authority_kind: &str,
    authority_key: &str,
) -> NormalizedEvent {
    let mut event = normalized_event(
        "ens-v1-unwrapped-authority:registry-event-time:permission",
        "PermissionChanged",
        CanonicalityState::Canonical,
    );
    event.logical_name_id = Some("ens:alice.eth".to_owned());
    event.resource_id = Some(resource_id);
    event.source_family = "ens_v1_registry_l1".to_owned();
    event.derivation_kind = "ens_v1_unwrapped_authority".to_owned();
    event.chain_id = Some("ethereum-mainnet".to_owned());
    event.block_number = Some(100);
    event.block_hash = Some("0xregistryeventpermissionblock".to_owned());
    event.transaction_hash = Some("0xregistryeventpermissiontx".to_owned());
    event.log_index = Some(14);
    event.raw_fact_ref = json!({
        "kind": "raw_log",
        "chain_id": "ethereum-mainnet",
        "block_number": 100,
        "block_hash": "0xregistryeventpermissionblock",
        "transaction_hash": "0xregistryeventpermissiontx",
        "transaction_index": 23,
        "log_index": 14,
    });
    event.before_state = json!({
        "effective_powers": [],
        "grant_source": null,
        "inheritance_path": [],
        "revocation_source": null,
        "scope": {
            "chain_id": "ethereum-mainnet",
            "kind": "resolver",
            "resolver_address": "0x0000000000000000000000000000000000000456"
        },
        "subject": "0x0000000000000000000000000000000000000123",
        "transfer_behavior": "replace_on_authority_change"
    });
    event.after_state = json!({
        "effective_powers": ["resolver_control"],
        "grant_source": {
            "authority_key": authority_key,
            "authority_kind": authority_kind,
            "kind": "ens_v1_authority",
            "source_event_kind": "ResolverChanged"
        },
        "inheritance_path": [],
        "revocation_source": null,
        "scope": {
            "chain_id": "ethereum-mainnet",
            "kind": "resolver",
            "resolver_address": "0x0000000000000000000000000000000000000456"
        },
        "subject": "0x0000000000000000000000000000000000000123",
        "transfer_behavior": "replace_on_authority_change"
    });
    event
}

fn ens_v1_registry_event_time_permission_revoke_repair_event(
    resource_id: Uuid,
    authority_kind: &str,
    authority_key: &str,
) -> NormalizedEvent {
    let mut event = normalized_event(
        "ens-v1-unwrapped-authority:registry-event-time:permission-revoke",
        "PermissionChanged",
        CanonicalityState::Canonical,
    );
    event.logical_name_id = Some("ens:alice.eth".to_owned());
    event.resource_id = Some(resource_id);
    event.source_family = "ens_v1_registry_l1".to_owned();
    event.derivation_kind = "ens_v1_unwrapped_authority".to_owned();
    event.chain_id = Some("ethereum-mainnet".to_owned());
    event.block_number = Some(100);
    event.block_hash = Some("0xregistryeventpermissionrevokeblock".to_owned());
    event.transaction_hash = Some("0xregistryeventpermissionrevoketx".to_owned());
    event.log_index = Some(15);
    event.raw_fact_ref = json!({
        "kind": "raw_log",
        "chain_id": "ethereum-mainnet",
        "block_number": 100,
        "block_hash": "0xregistryeventpermissionrevokeblock",
        "transaction_hash": "0xregistryeventpermissionrevoketx",
        "transaction_index": 23,
        "log_index": 15,
    });
    event.before_state = json!({
        "effective_powers": ["resolver_control"],
        "grant_source": {
            "authority_key": authority_key,
            "authority_kind": authority_kind,
            "kind": "ens_v1_authority",
            "source_event_kind": "ResolverChanged"
        },
        "inheritance_path": [],
        "revocation_source": null,
        "scope": {
            "chain_id": "ethereum-mainnet",
            "kind": "resolver",
            "resolver_address": "0x0000000000000000000000000000000000000456"
        },
        "subject": "0x0000000000000000000000000000000000000123",
        "transfer_behavior": "replace_on_authority_change"
    });
    event.after_state = json!({
        "effective_powers": [],
        "grant_source": null,
        "inheritance_path": [],
        "revocation_source": {
            "authority_key": authority_key,
            "authority_kind": authority_kind,
            "kind": "ens_v1_authority",
            "source_event_kind": "ResolverChanged"
        },
        "scope": {
            "chain_id": "ethereum-mainnet",
            "kind": "resolver",
            "resolver_address": "0x0000000000000000000000000000000000000456"
        },
        "subject": "0x0000000000000000000000000000000000000123",
        "transfer_behavior": "replace_on_authority_change"
    });
    event
}

fn ens_v1_same_transaction_registration_setup_permission_event(
    resource_id: Uuid,
    authority_kind: &str,
    authority_key: &str,
) -> NormalizedEvent {
    let mut event = normalized_event(
        "ens-v1-unwrapped-authority:same-tx-registration:permission",
        "PermissionChanged",
        CanonicalityState::Canonical,
    );
    event.logical_name_id = Some("ens:alice.eth".to_owned());
    event.resource_id = Some(resource_id);
    event.source_family = "ens_v1_registry_l1".to_owned();
    event.derivation_kind = "ens_v1_unwrapped_authority".to_owned();
    event.chain_id = Some("ethereum-mainnet".to_owned());
    event.block_number = Some(100);
    event.block_hash = Some("0xsametxregistrationblock".to_owned());
    event.transaction_hash = Some("0xsametxregistrationtx".to_owned());
    event.log_index = Some(2);
    event.raw_fact_ref = json!({
        "kind": "raw_log",
        "chain_id": "ethereum-mainnet",
        "block_number": 100,
        "block_hash": "0xsametxregistrationblock",
        "transaction_hash": "0xsametxregistrationtx",
        "transaction_index": 3,
        "log_index": 2,
    });
    event.before_state = json!({
        "effective_powers": [],
        "grant_source": null,
        "inheritance_path": [],
        "revocation_source": null,
        "scope": {
            "chain_id": "ethereum-mainnet",
            "kind": "resolver",
            "resolver_address": "0x0000000000000000000000000000000000000456"
        },
        "subject": "0x0000000000000000000000000000000000000123",
        "transfer_behavior": "replace_on_authority_change"
    });
    event.after_state = json!({
        "effective_powers": ["resolver_control"],
        "grant_source": {
            "authority_key": authority_key,
            "authority_kind": authority_kind,
            "kind": "ens_v1_authority",
            "source_event_kind": "ResolverChanged"
        },
        "inheritance_path": [],
        "revocation_source": null,
        "scope": {
            "chain_id": "ethereum-mainnet",
            "kind": "resolver",
            "resolver_address": "0x0000000000000000000000000000000000000456"
        },
        "subject": "0x0000000000000000000000000000000000000123",
        "transfer_behavior": "replace_on_authority_change"
    });
    event
}

fn ens_v1_same_transaction_registration_grant_event(resource_id: Uuid) -> NormalizedEvent {
    let mut event = normalized_event(
        "ens-v1-unwrapped-authority:same-tx-registration:grant",
        "RegistrationGranted",
        CanonicalityState::Canonical,
    );
    event.logical_name_id = Some("ens:alice.eth".to_owned());
    event.resource_id = Some(resource_id);
    event.source_family = "ens_v1_registrar_l1".to_owned();
    event.derivation_kind = "ens_v1_unwrapped_authority".to_owned();
    event.chain_id = Some("ethereum-mainnet".to_owned());
    event.block_number = Some(100);
    event.block_hash = Some("0xsametxregistrationblock".to_owned());
    event.transaction_hash = Some("0xsametxregistrationtx".to_owned());
    event.log_index = Some(5);
    event.raw_fact_ref = json!({
        "kind": "raw_log",
        "chain_id": "ethereum-mainnet",
        "block_number": 100,
        "block_hash": "0xsametxregistrationblock",
        "transaction_hash": "0xsametxregistrationtx",
        "transaction_index": 3,
        "log_index": 5,
    });
    event.before_state = json!({"authority_kind": "registry_only", "registrant": null});
    event.after_state = json!({
        "authority_kind": "registrar",
        "authority_key": "registrar:ethereum-mainnet:10:0xcbf005454c11bc7e583aa4a100988b4a893acb2233dbb77afef8d9f931df3735:0xsametxregistrationblock:5",
        "registrant": "0x0000000000000000000000000000000000000123",
        "expiry": 1800000000,
        "labelhash": "0xcbf005454c11bc7e583aa4a100988b4a893acb2233dbb77afef8d9f931df3735"
    });
    event
}

fn ens_v1_same_transaction_registration_setup_authority_transfer_event(
    resource_id: Uuid,
) -> NormalizedEvent {
    let mut event = normalized_event(
        "ens-v1-unwrapped-authority:same-tx-registration:authority-transfer",
        "AuthorityTransferred",
        CanonicalityState::Canonical,
    );
    event.logical_name_id = Some("ens:alice.eth".to_owned());
    event.resource_id = Some(resource_id);
    event.source_family = "ens_v1_registry_l1".to_owned();
    event.derivation_kind = "ens_v1_unwrapped_authority".to_owned();
    event.chain_id = Some("ethereum-mainnet".to_owned());
    event.block_number = Some(100);
    event.block_hash = Some("0xsametxregistrationblock".to_owned());
    event.transaction_hash = Some("0xsametxregistrationtx".to_owned());
    event.log_index = Some(2);
    event.raw_fact_ref = json!({
        "kind": "raw_log",
        "chain_id": "ethereum-mainnet",
        "block_number": 100,
        "block_hash": "0xsametxregistrationblock",
        "transaction_hash": "0xsametxregistrationtx",
        "transaction_index": 3,
        "log_index": 2,
    });
    event.before_state = json!({"owner": null});
    event.after_state = json!({
        "owner": "0x0000000000000000000000000000000000000123",
        "labelhash": "0xcbf005454c11bc7e583aa4a100988b4a893acb2233dbb77afef8d9f931df3735"
    });
    event
}

fn ens_v1_registry_resolver_observation_key_repair_event(observation_key: &str) -> NormalizedEvent {
    let mut event = normalized_event(
        "ens_v1_registry_resolver_changed:13:0xresolverblock:0xresolvertx:2:0x314159265dd8dbb310642f98f50c066173c1259b",
        "ResolverChanged",
        CanonicalityState::Canonical,
    );
    event.source_family = "ens_v1_registry_l1".to_owned();
    event.manifest_version = 3;
    event.source_manifest_id = None;
    event.chain_id = Some("ethereum-mainnet".to_owned());
    event.block_number = Some(3_745_840);
    event.block_hash = Some("0xresolverblock".to_owned());
    event.transaction_hash = Some("0xresolvertx".to_owned());
    event.log_index = Some(2);
    event.raw_fact_ref = json!({
        "kind": "raw_log",
        "chain_id": "ethereum-mainnet",
        "block_number": 3745840,
        "block_hash": "0xresolverblock",
        "transaction_hash": "0xresolvertx",
        "transaction_index": 9,
        "log_index": 2,
        "emitting_address": "0x314159265dd8dbb310642f98f50c066173c1259b",
        "topic0": "0x335721b01866dc23fbee8b6b2c7b1e14d6f05c28cd35a2c934239f94095602a0",
        "topic1": "0xdea316f9d0b5800de3e6b0d31743113b679d9d30d004a2d4f8e4f257a21173ea",
        "topic2": null,
        "data_hex": "0000000000000000000000000000000000000000000000000000000000000000",
    });
    event.derivation_kind = "ens_v1_registry_resolver_changed".to_owned();
    event.before_state = json!({});
    event.after_state = json!({
        "source_event": "NewResolver",
        "discovery_source": "ens_v1_registry_resolver:ethereum-mainnet",
        "edge_kind": "resolver",
        "observation_key": observation_key,
        "node": "0xdea316f9d0b5800de3e6b0d31743113b679d9d30d004a2d4f8e4f257a21173ea",
        "emitting_address": "0x314159265dd8dbb310642f98f50c066173c1259b",
        "resolver": null,
        "raw_resolver": "0x0000000000000000000000000000000000000000",
        "tombstone": true,
        "from_contract_instance_id": "bbbb47ac-de4f-41e9-b044-11458aa9ba77",
        "to_contract_instance_id": null,
        "active_edge": false,
        "resolver_profile_supported": false,
        "resolver_profile_status": "unsupported",
        "resolver_profile_reason": "registry_resolver_discovery_does_not_admit_typed_resolver_profile",
    });
    event
}

fn ens_v1_renewal_event(event_identity: &str, resource_id: Uuid) -> NormalizedEvent {
    let mut event = normalized_event(
        event_identity,
        "RegistrationRenewed",
        CanonicalityState::Canonical,
    );
    event.logical_name_id = Some("ens:alice.eth".to_owned());
    event.resource_id = Some(resource_id);
    event.source_family = "ens_v1_registrar_l1".to_owned();
    event.derivation_kind = "ens_v1_unwrapped_authority".to_owned();
    event.chain_id = Some("ethereum-mainnet".to_owned());
    event.block_number = Some(25_238_970);
    event.block_hash =
        Some("0x9da3c01e4f15f21e87656f7ba57b31a80709464339389cc0194099b0926ce36f".to_owned());
    event.transaction_hash =
        Some("0x93b81927d785859a89e80c3dd900d63da000f38c3f90e09cbaf0ec0908774627".to_owned());
    event.log_index = Some(1059);
    event.raw_fact_ref = json!({
        "kind": "raw_log",
        "chain_id": "ethereum-mainnet",
        "block_number": 25238970,
        "block_hash": "0x9da3c01e4f15f21e87656f7ba57b31a80709464339389cc0194099b0926ce36f",
        "transaction_hash": "0x93b81927d785859a89e80c3dd900d63da000f38c3f90e09cbaf0ec0908774627",
        "transaction_index": 219,
        "log_index": 1059,
    });
    event.before_state = json!({"expiry": 1872542016});
    event.after_state = json!({
        "expiry": 1872542016,
        "labelhash": "0xcbf005454c11bc7e583aa4a100988b4a893acb2233dbb77afef8d9f931df3735",
    });
    event
}

#[tokio::test]
async fn upserts_and_loads_normalized_events() -> Result<()> {
    let database = TestDatabase::new().await?;

    let inserted = upsert_normalized_events(
        database.pool(),
        &[
            normalized_event(
                "manifest:1:source_manifest",
                "SourceManifestUpdated",
                CanonicalityState::Finalized,
            ),
            normalized_event(
                "manifest:1:capability:verified_resolution",
                "CapabilityChanged",
                CanonicalityState::Finalized,
            ),
        ],
    )
    .await?;
    assert_eq!(inserted.len(), 2);

    let loaded = load_normalized_events_by_namespace(database.pool(), "ens").await?;
    assert_eq!(loaded, inserted);

    let counts = load_normalized_event_counts_by_kind(database.pool(), "ens").await?;
    assert_eq!(
        counts,
        BTreeMap::from([
            ("CapabilityChanged".to_owned(), 1_usize),
            ("SourceManifestUpdated".to_owned(), 1_usize),
        ])
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_upsert_rejects_identity_mismatch() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_normalized_events(
        database.pool(),
        &[normalized_event(
            "manifest:1:source_manifest",
            "SourceManifestUpdated",
            CanonicalityState::Finalized,
        )],
    )
    .await?;

    let mut conflicting = normalized_event(
        "manifest:1:source_manifest",
        "SourceManifestUpdated",
        CanonicalityState::Finalized,
    );
    conflicting.after_state = json!({"key": "different"});
    let error = upsert_normalized_events(database.pool(), &[conflicting])
        .await
        .expect_err("normalized-event identity mismatch must fail");

    assert!(
        error
            .to_string()
            .contains("normalized event identity mismatch for event manifest:1:source_manifest"),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_upsert_rejects_resource_id_change() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut event = normalized_event(
        "raw-log:resolver-changed",
        "ResolverChanged",
        CanonicalityState::Finalized,
    );
    event.logical_name_id = Some("ens:alice.eth".to_owned());
    event.resource_id = Some(Uuid::from_u128(0x100));
    upsert_normalized_events(database.pool(), std::slice::from_ref(&event)).await?;

    event.resource_id = Some(Uuid::from_u128(0x200));
    let error = upsert_normalized_events(database.pool(), std::slice::from_ref(&event))
        .await
        .expect_err("concrete resource-id changes must fail");

    assert!(
        error
            .to_string()
            .contains("normalized event identity mismatch for event raw-log:resolver-changed"),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_count_only_accepts_ens_v1_boundary_manifest_metadata_downgrade()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let source_manifest_id = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO manifest_versions (
            manifest_version,
            namespace,
            source_family,
            chain,
            deployment_epoch,
            rollout_status,
            normalizer_version,
            file_path,
            manifest_payload
        )
        VALUES (
            3,
            'ens',
            'ens_v1_registry_l1',
            'ethereum-mainnet',
            'ens_v1',
            'active',
            'ensip15@ens-normalize-0.1.1',
            'manifests/ens/ens_v1_registry_l1/v3.toml',
            '{"rollout_status":"active"}'::JSONB
        )
        RETURNING manifest_id
        "#,
    )
    .fetch_one(database.pool())
    .await?;
    let mut event = normalized_event(
        "ens_v1_unwrapped_authority:SurfaceUnbound:surface-unbound:0xblock:ens:alice.eth:binding",
        "SurfaceUnbound",
        CanonicalityState::Finalized,
    );
    event.logical_name_id = Some("ens:alice.eth".to_owned());
    event.resource_id = Some(Uuid::from_u128(0xace));
    event.source_family = "ens_v1_registry_l1".to_owned();
    event.derivation_kind = "ens_v1_unwrapped_authority".to_owned();
    event.manifest_version = 3;
    event.source_manifest_id = Some(source_manifest_id);
    event.chain_id = Some("ethereum-mainnet".to_owned());
    event.block_number = Some(100);
    event.block_hash = Some("0xblock".to_owned());
    event.transaction_hash = None;
    event.log_index = None;
    event.raw_fact_ref = json!({
        "kind": "raw_block",
        "chain_id": "ethereum-mainnet",
        "block_number": 100,
        "block_hash": "0xblock",
        "block_timestamp": 1700000000,
    });
    event.before_state = json!({
        "authority_kind": "registry_only",
        "authority_key": "registry-only:ethereum-mainnet:0xalice_namehash",
    });
    event.after_state = json!({
        "authority_kind": "registry_only",
        "authority_key": "registry-only:ethereum-mainnet:0xalice_namehash",
        "active_to": 1700000000,
    });
    upsert_normalized_events(database.pool(), std::slice::from_ref(&event)).await?;

    let mut replayed = event.clone();
    replayed.manifest_version = 1;
    replayed.source_manifest_id = None;
    let inserted_count =
        upsert_normalized_events_count_only(database.pool(), std::slice::from_ref(&replayed))
            .await?;
    assert_eq!(inserted_count, 0);

    let stored = sqlx::query_as::<_, (i64, Option<i64>)>(
        "SELECT manifest_version, source_manifest_id FROM normalized_events WHERE event_identity = $1",
    )
    .bind(&event.event_identity)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(stored, (3, Some(source_manifest_id)));

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_upsert_repairs_ens_v1_renewal_resource_id_transition() -> Result<()> {
    let database = TestDatabase::new().await?;
    let old_resource_id = Uuid::from_u128(0x100);
    let repaired_resource_id = Uuid::from_u128(0x200);
    let old_surface_binding_id = Uuid::from_u128(0x300);
    seed_ens_v1_renewal_resource_repair_identity_rows(
        database.pool(),
        old_resource_id,
        repaired_resource_id,
        old_surface_binding_id,
    )
    .await?;

    let mut event = normalized_event(
        "ens-v1-unwrapped-authority:renewal:resource-transition",
        "RegistrationRenewed",
        CanonicalityState::Canonical,
    );
    event.logical_name_id = Some("ens:alice.eth".to_owned());
    event.resource_id = Some(old_resource_id);
    event.source_family = "ens_v1_registrar_l1".to_owned();
    event.derivation_kind = "ens_v1_unwrapped_authority".to_owned();
    event.chain_id = Some("ethereum-mainnet".to_owned());
    event.block_number = Some(25_238_970);
    event.block_hash =
        Some("0x9da3c01e4f15f21e87656f7ba57b31a80709464339389cc0194099b0926ce36f".to_owned());
    event.transaction_hash =
        Some("0x93b81927d785859a89e80c3dd900d63da000f38c3f90e09cbaf0ec0908774627".to_owned());
    event.log_index = Some(1059);
    event.raw_fact_ref = json!({
        "kind": "raw_log",
        "chain_id": "ethereum-mainnet",
        "block_number": 25238970,
        "block_hash": "0x9da3c01e4f15f21e87656f7ba57b31a80709464339389cc0194099b0926ce36f",
        "transaction_hash": "0x93b81927d785859a89e80c3dd900d63da000f38c3f90e09cbaf0ec0908774627",
        "transaction_index": 219,
        "log_index": 1059,
    });
    event.before_state = json!({"expiry": 1872542016});
    event.after_state = json!({
        "expiry": 1872542016,
        "labelhash": "0xcbf005454c11bc7e583aa4a100988b4a893acb2233dbb77afef8d9f931df3735",
    });
    upsert_normalized_events(database.pool(), std::slice::from_ref(&event)).await?;
    upsert_normalized_events(
        database.pool(),
        &[
            ens_v1_renewal_related_event(
                "ens-v1-unwrapped-authority:renewal:resource-transition:grant",
                "RegistrationGranted",
                old_resource_id,
                json!({
                    "registrant": "0x0000000000000000000000000000000000000123",
                    "labelhash": "0xcbf005454c11bc7e583aa4a100988b4a893acb2233dbb77afef8d9f931df3735"
                }),
            ),
            ens_v1_renewal_related_event(
                "ens-v1-unwrapped-authority:renewal:resource-transition:surface",
                "SurfaceBound",
                old_resource_id,
                json!({"binding_kind": "declared_registry_path"}),
            ),
            ens_v1_renewal_related_event(
                "ens-v1-unwrapped-authority:renewal:resource-transition:record",
                "RecordChanged",
                old_resource_id,
                json!({
                    "record_key": "text:description",
                    "record_family": "text",
                    "selector_key": "description",
                    "value": "stale"
                }),
            ),
        ],
    )
    .await?;

    let mut repaired = event.clone();
    repaired.resource_id = Some(repaired_resource_id);
    let snapshots =
        upsert_normalized_events(database.pool(), std::slice::from_ref(&repaired)).await?;
    assert_eq!(snapshots[0].resource_id, repaired.resource_id);

    let stored_resource_id: Uuid =
        sqlx::query_scalar("SELECT resource_id FROM normalized_events WHERE event_identity = $1")
            .bind(&event.event_identity)
            .fetch_one(database.pool())
            .await?;
    assert_eq!(stored_resource_id, repaired.resource_id.unwrap());

    let queued_change_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM projection_normalized_event_changes change
        JOIN normalized_events event
          ON event.normalized_event_id = change.normalized_event_id
        WHERE event.event_identity = $1
          AND change.change_kind = 'canonicality_update'
        "#,
    )
    .bind(&event.event_identity)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(queued_change_count, 1);

    let old_resource_state: String =
        sqlx::query_scalar("SELECT canonicality_state::text FROM resources WHERE resource_id = $1")
            .bind(old_resource_id)
            .fetch_one(database.pool())
            .await?;
    assert_eq!(old_resource_state, "orphaned");

    let old_binding_state: String = sqlx::query_scalar(
        "SELECT canonicality_state::text FROM surface_bindings WHERE surface_binding_id = $1",
    )
    .bind(old_surface_binding_id)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(old_binding_state, "orphaned");

    let stale_event_states = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT event_identity, canonicality_state::text
        FROM normalized_events
        WHERE event_identity IN (
            'ens-v1-unwrapped-authority:renewal:resource-transition:grant',
            'ens-v1-unwrapped-authority:renewal:resource-transition:surface'
        )
        ORDER BY event_identity
        "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        stale_event_states,
        vec![
            (
                "ens-v1-unwrapped-authority:renewal:resource-transition:grant".to_owned(),
                "orphaned".to_owned()
            ),
            (
                "ens-v1-unwrapped-authority:renewal:resource-transition:surface".to_owned(),
                "orphaned".to_owned()
            )
        ]
    );

    let related_record_resource_id: Uuid =
        sqlx::query_scalar("SELECT resource_id FROM normalized_events WHERE event_identity = $1")
            .bind("ens-v1-unwrapped-authority:renewal:resource-transition:record")
            .fetch_one(database.pool())
            .await?;
    assert_eq!(related_record_resource_id, repaired_resource_id);

    let invalidation_keys = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT projection, projection_key
        FROM projection_invalidations
        WHERE projection IN ('name_current', 'record_inventory_current')
        ORDER BY projection, projection_key
        "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        invalidation_keys,
        vec![
            ("name_current".to_owned(), "ens:alice.eth".to_owned()),
            (
                "record_inventory_current".to_owned(),
                old_resource_id.to_string()
            ),
            (
                "record_inventory_current".to_owned(),
                repaired_resource_id.to_string()
            ),
        ]
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_count_only_upsert_repairs_ens_v1_registry_event_time_resource_id_transition()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let stale_later_registrar_resource_id = Uuid::from_u128(0x1400);
    let event_time_registry_resource_id = Uuid::from_u128(0x1500);
    seed_ens_v1_registry_event_time_repair_resources(
        database.pool(),
        stale_later_registrar_resource_id,
        event_time_registry_resource_id,
    )
    .await?;

    let event = ens_v1_registry_event_time_repair_event(
        "ens-v1-unwrapped-authority:registry-event-time:resolver",
        stale_later_registrar_resource_id,
    );
    upsert_normalized_events(database.pool(), std::slice::from_ref(&event)).await?;

    let repaired = ens_v1_registry_event_time_repair_event(
        "ens-v1-unwrapped-authority:registry-event-time:resolver",
        event_time_registry_resource_id,
    );
    let inserted_count =
        upsert_normalized_events_count_only(database.pool(), std::slice::from_ref(&repaired))
            .await?;
    assert_eq!(inserted_count, 0);

    let stored_resource_id: Uuid =
        sqlx::query_scalar("SELECT resource_id FROM normalized_events WHERE event_identity = $1")
            .bind(&event.event_identity)
            .fetch_one(database.pool())
            .await?;
    assert_eq!(stored_resource_id, event_time_registry_resource_id);

    let queued_change_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM projection_normalized_event_changes change
        JOIN normalized_events event
          ON event.normalized_event_id = change.normalized_event_id
        WHERE event.event_identity = $1
          AND change.change_kind = 'canonicality_update'
        "#,
    )
    .bind(&event.event_identity)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(queued_change_count, 1);

    let invalidation_keys = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT projection, projection_key
        FROM projection_invalidations
        WHERE projection = 'record_inventory_current'
        ORDER BY projection_key
        "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        invalidation_keys,
        vec![
            (
                "record_inventory_current".to_owned(),
                stale_later_registrar_resource_id.to_string()
            ),
            (
                "record_inventory_current".to_owned(),
                event_time_registry_resource_id.to_string()
            ),
        ]
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_count_only_upsert_repairs_ens_v1_registry_event_time_registry_collision()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let legacy_labelhash_registry_resource_id = Uuid::from_u128(0x1550);
    let namehash_registry_resource_id = Uuid::from_u128(0x1560);
    seed_ens_v1_registry_event_time_registry_collision_repair_resources(
        database.pool(),
        legacy_labelhash_registry_resource_id,
        namehash_registry_resource_id,
    )
    .await?;

    let event = ens_v1_registry_event_time_subname_collision_repair_event(
        "ens-v1-unwrapped-authority:registry-event-time:subname-collision",
        legacy_labelhash_registry_resource_id,
    );
    upsert_normalized_events(database.pool(), std::slice::from_ref(&event)).await?;

    let repaired = ens_v1_registry_event_time_subname_collision_repair_event(
        "ens-v1-unwrapped-authority:registry-event-time:subname-collision",
        namehash_registry_resource_id,
    );
    let inserted_count =
        upsert_normalized_events_count_only(database.pool(), std::slice::from_ref(&repaired))
            .await?;
    assert_eq!(inserted_count, 0);

    let stored_resource_id: Uuid =
        sqlx::query_scalar("SELECT resource_id FROM normalized_events WHERE event_identity = $1")
            .bind(&event.event_identity)
            .fetch_one(database.pool())
            .await?;
    assert_eq!(stored_resource_id, namehash_registry_resource_id);

    let invalidation_keys = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT projection, projection_key
        FROM projection_invalidations
        WHERE projection = 'record_inventory_current'
        ORDER BY projection_key
        "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        invalidation_keys,
        vec![
            (
                "record_inventory_current".to_owned(),
                legacy_labelhash_registry_resource_id.to_string()
            ),
            (
                "record_inventory_current".to_owned(),
                namehash_registry_resource_id.to_string()
            ),
        ]
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_count_only_upsert_repairs_ens_v1_registry_event_time_authority_transfer_key()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let legacy_labelhash_registry_resource_id = Uuid::from_u128(0x1570);
    let namehash_registry_resource_id = Uuid::from_u128(0x1580);
    seed_ens_v1_registry_event_time_registry_key_repair_resources(
        database.pool(),
        legacy_labelhash_registry_resource_id,
        namehash_registry_resource_id,
    )
    .await?;

    let event = ens_v1_registry_event_time_authority_transfer_repair_event(
        "ens-v1-unwrapped-authority:registry-event-time:authority-transfer",
        legacy_labelhash_registry_resource_id,
    );
    upsert_normalized_events(database.pool(), std::slice::from_ref(&event)).await?;

    let repaired = ens_v1_registry_event_time_authority_transfer_repair_event(
        "ens-v1-unwrapped-authority:registry-event-time:authority-transfer",
        namehash_registry_resource_id,
    );
    let inserted_count =
        upsert_normalized_events_count_only(database.pool(), std::slice::from_ref(&repaired))
            .await?;
    assert_eq!(inserted_count, 0);

    let stored_resource_id: Uuid =
        sqlx::query_scalar("SELECT resource_id FROM normalized_events WHERE event_identity = $1")
            .bind(&event.event_identity)
            .fetch_one(database.pool())
            .await?;
    assert_eq!(stored_resource_id, namehash_registry_resource_id);

    let invalidation_keys = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT projection, projection_key
        FROM projection_invalidations
        WHERE projection = 'permissions_current'
        ORDER BY projection_key
        "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        invalidation_keys,
        vec![
            (
                "permissions_current".to_owned(),
                legacy_labelhash_registry_resource_id.to_string()
            ),
            (
                "permissions_current".to_owned(),
                namehash_registry_resource_id.to_string()
            ),
        ]
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_count_only_upsert_repairs_ens_v1_registry_event_time_authority_transfer_before_owner()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let legacy_labelhash_registry_resource_id = Uuid::from_u128(0x1590);
    let namehash_registry_resource_id = Uuid::from_u128(0x15a0);
    seed_ens_v1_registry_event_time_registry_key_repair_resources(
        database.pool(),
        legacy_labelhash_registry_resource_id,
        namehash_registry_resource_id,
    )
    .await?;

    let mut event = ens_v1_registry_event_time_authority_transfer_repair_event(
        "ens-v1-unwrapped-authority:registry-event-time:authority-transfer-before-owner",
        legacy_labelhash_registry_resource_id,
    );
    event.before_state = json!({
        "owner": "0x0000000000000000000000000000000000000def"
    });
    upsert_normalized_events(database.pool(), std::slice::from_ref(&event)).await?;

    let mut repaired = ens_v1_registry_event_time_authority_transfer_repair_event(
        "ens-v1-unwrapped-authority:registry-event-time:authority-transfer-before-owner",
        namehash_registry_resource_id,
    );
    repaired.before_state = json!({
        "owner": "0x0000000000000000000000000000000000000abc"
    });
    let inserted_count =
        upsert_normalized_events_count_only(database.pool(), std::slice::from_ref(&repaired))
            .await?;
    assert_eq!(inserted_count, 0);

    let stored = sqlx::query_as::<_, (Uuid, serde_json::Value)>(
        "SELECT resource_id, before_state FROM normalized_events WHERE event_identity = $1",
    )
    .bind(&event.event_identity)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(stored.0, namehash_registry_resource_id);
    assert_eq!(stored.1, repaired.before_state);

    let invalidation_keys = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT projection, projection_key
        FROM projection_invalidations
        WHERE projection = 'permissions_current'
        ORDER BY projection_key
        "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        invalidation_keys,
        vec![
            (
                "permissions_current".to_owned(),
                legacy_labelhash_registry_resource_id.to_string()
            ),
            (
                "permissions_current".to_owned(),
                namehash_registry_resource_id.to_string()
            ),
        ]
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_count_only_upsert_repairs_ens_v1_registry_resolver_observation_key_transition()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let stale_key = "resolver:0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e:0xdea316f9d0b5800de3e6b0d31743113b679d9d30d004a2d4f8e4f257a21173ea";
    let repaired_key = "resolver:0x314159265dd8dbb310642f98f50c066173c1259b:0xdea316f9d0b5800de3e6b0d31743113b679d9d30d004a2d4f8e4f257a21173ea";

    let event = ens_v1_registry_resolver_observation_key_repair_event(stale_key);
    upsert_normalized_events(database.pool(), std::slice::from_ref(&event)).await?;

    let repaired = ens_v1_registry_resolver_observation_key_repair_event(repaired_key);
    let inserted_count =
        upsert_normalized_events_count_only(database.pool(), std::slice::from_ref(&repaired))
            .await?;
    assert_eq!(inserted_count, 0);

    let stored_observation_key = sqlx::query_scalar::<_, String>(
        "SELECT after_state->>'observation_key' FROM normalized_events WHERE event_identity = $1",
    )
    .bind(&event.event_identity)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(stored_observation_key, repaired_key);

    let queued_change_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM projection_normalized_event_changes change
        JOIN normalized_events event
          ON event.normalized_event_id = change.normalized_event_id
        WHERE event.event_identity = $1
          AND change.change_kind = 'canonicality_update'
        "#,
    )
    .bind(&event.event_identity)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(queued_change_count, 1);

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_count_only_upsert_repairs_ens_v1_registry_event_time_permission_grant_source()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let stale_later_registrar_resource_id = Uuid::from_u128(0x1600);
    let event_time_registry_resource_id = Uuid::from_u128(0x1700);
    seed_ens_v1_registry_event_time_repair_resources(
        database.pool(),
        stale_later_registrar_resource_id,
        event_time_registry_resource_id,
    )
    .await?;

    let stale = ens_v1_registry_event_time_permission_repair_event(
        stale_later_registrar_resource_id,
        "registrar",
        "registrar:ethereum-mainnet:alice",
    );
    upsert_normalized_events(database.pool(), std::slice::from_ref(&stale)).await?;

    let repaired = ens_v1_registry_event_time_permission_repair_event(
        event_time_registry_resource_id,
        "registry_only",
        "registry-only:ethereum-mainnet:0xalice_namehash",
    );
    let inserted_count =
        upsert_normalized_events_count_only(database.pool(), std::slice::from_ref(&repaired))
            .await?;
    assert_eq!(inserted_count, 0);

    let stored = sqlx::query_as::<_, (Uuid, serde_json::Value)>(
        "SELECT resource_id, after_state FROM normalized_events WHERE event_identity = $1",
    )
    .bind(&stale.event_identity)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(stored.0, event_time_registry_resource_id);
    assert_eq!(
        stored.1["grant_source"]["authority_kind"].as_str(),
        Some("registry_only")
    );
    assert_eq!(
        stored.1["grant_source"]["authority_key"].as_str(),
        Some("registry-only:ethereum-mainnet:0xalice_namehash")
    );

    let invalidation_keys = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT projection, projection_key
        FROM projection_invalidations
        WHERE projection = 'permissions_current'
        ORDER BY projection_key
        "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        invalidation_keys,
        vec![
            (
                "permissions_current".to_owned(),
                stale_later_registrar_resource_id.to_string()
            ),
            (
                "permissions_current".to_owned(),
                event_time_registry_resource_id.to_string()
            ),
        ]
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_count_only_upsert_repairs_ens_v1_same_tx_registration_setup_permission_to_registrar()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let stale_registry_resource_id = Uuid::from_u128(0x1710);
    let registrar_resource_id = Uuid::from_u128(0x1720);
    seed_ens_v1_same_transaction_registration_setup_repair_resources(
        database.pool(),
        stale_registry_resource_id,
        registrar_resource_id,
    )
    .await?;

    let stale = ens_v1_same_transaction_registration_setup_permission_event(
        stale_registry_resource_id,
        "registry_only",
        "registry-only:ethereum-mainnet:0xalice_namehash",
    );
    let registration = ens_v1_same_transaction_registration_grant_event(registrar_resource_id);
    upsert_normalized_events(database.pool(), &[stale.clone(), registration]).await?;

    let repaired = ens_v1_same_transaction_registration_setup_permission_event(
        registrar_resource_id,
        "registrar",
        "registrar:ethereum-mainnet:10:0xcbf005454c11bc7e583aa4a100988b4a893acb2233dbb77afef8d9f931df3735:0xsametxregistrationblock:5",
    );
    let inserted_count =
        upsert_normalized_events_count_only(database.pool(), std::slice::from_ref(&repaired))
            .await?;
    assert_eq!(inserted_count, 0);

    let stored = sqlx::query_as::<_, (Uuid, serde_json::Value)>(
        "SELECT resource_id, after_state FROM normalized_events WHERE event_identity = $1",
    )
    .bind(&stale.event_identity)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(stored.0, registrar_resource_id);
    assert_eq!(
        stored.1["grant_source"]["authority_kind"].as_str(),
        Some("registrar")
    );
    assert_eq!(
        stored.1["grant_source"]["authority_key"].as_str(),
        Some(
            "registrar:ethereum-mainnet:10:0xcbf005454c11bc7e583aa4a100988b4a893acb2233dbb77afef8d9f931df3735:0xsametxregistrationblock:5"
        )
    );

    let invalidation_keys = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT projection, projection_key
        FROM projection_invalidations
        WHERE projection = 'permissions_current'
        ORDER BY projection_key
        "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        invalidation_keys,
        vec![
            (
                "permissions_current".to_owned(),
                stale_registry_resource_id.to_string()
            ),
            (
                "permissions_current".to_owned(),
                registrar_resource_id.to_string()
            ),
        ]
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_count_only_upsert_repairs_ens_v1_same_tx_registration_before_state_and_orphans_setup_registry_events()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let stale_registry_resource_id = Uuid::from_u128(0x1730);
    let registrar_resource_id = Uuid::from_u128(0x1740);
    seed_ens_v1_same_transaction_registration_setup_repair_resources(
        database.pool(),
        stale_registry_resource_id,
        registrar_resource_id,
    )
    .await?;

    let stale_transfer = ens_v1_same_transaction_registration_setup_authority_transfer_event(
        stale_registry_resource_id,
    );
    let stale_permission = ens_v1_same_transaction_registration_setup_permission_event(
        stale_registry_resource_id,
        "registry_only",
        "registry-only:ethereum-mainnet:0xalice_namehash",
    );
    let stale_registration =
        ens_v1_same_transaction_registration_grant_event(registrar_resource_id);
    upsert_normalized_events(
        database.pool(),
        &[
            stale_transfer.clone(),
            stale_permission.clone(),
            stale_registration.clone(),
        ],
    )
    .await?;

    let mut repaired_registration =
        ens_v1_same_transaction_registration_grant_event(registrar_resource_id);
    repaired_registration.before_state = json!({"authority_kind": null, "registrant": null});
    let inserted_count = upsert_normalized_events_count_only(
        database.pool(),
        std::slice::from_ref(&repaired_registration),
    )
    .await?;
    assert_eq!(inserted_count, 0);

    let stored_registration_before_state = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT before_state FROM normalized_events WHERE event_identity = $1",
    )
    .bind(&stale_registration.event_identity)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        stored_registration_before_state,
        repaired_registration.before_state
    );

    let stale_states = sqlx::query_as::<_, (String, CanonicalityState)>(
        "SELECT event_kind, canonicality_state::TEXT AS canonicality_state
         FROM normalized_events
         WHERE event_identity = ANY($1::TEXT[])
         ORDER BY event_kind",
    )
    .bind(&[
        stale_transfer.event_identity.clone(),
        stale_permission.event_identity.clone(),
    ])
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        stale_states,
        vec![
            (
                "AuthorityTransferred".to_owned(),
                CanonicalityState::Orphaned
            ),
            ("PermissionChanged".to_owned(), CanonicalityState::Orphaned),
        ]
    );

    let invalidation_keys = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT projection, projection_key
        FROM projection_invalidations
        WHERE projection IN ('name_current', 'permissions_current')
        ORDER BY projection, projection_key
        "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert!(invalidation_keys.contains(&("name_current".to_owned(), "ens:alice.eth".to_owned())));
    assert!(invalidation_keys.contains(&(
        "permissions_current".to_owned(),
        stale_registry_resource_id.to_string()
    )));
    assert!(invalidation_keys.contains(&(
        "permissions_current".to_owned(),
        registrar_resource_id.to_string()
    )));

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_count_only_upsert_repairs_ens_v1_registry_event_time_permission_revoke_sources()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let stale_later_registrar_resource_id = Uuid::from_u128(0x1750);
    let event_time_registry_resource_id = Uuid::from_u128(0x1760);
    seed_ens_v1_registry_event_time_repair_resources(
        database.pool(),
        stale_later_registrar_resource_id,
        event_time_registry_resource_id,
    )
    .await?;

    let stale = ens_v1_registry_event_time_permission_revoke_repair_event(
        stale_later_registrar_resource_id,
        "registrar",
        "registrar:ethereum-mainnet:alice",
    );
    upsert_normalized_events(database.pool(), std::slice::from_ref(&stale)).await?;

    let repaired = ens_v1_registry_event_time_permission_revoke_repair_event(
        event_time_registry_resource_id,
        "registry_only",
        "registry-only:ethereum-mainnet:0xalice_namehash",
    );
    let inserted_count =
        upsert_normalized_events_count_only(database.pool(), std::slice::from_ref(&repaired))
            .await?;
    assert_eq!(inserted_count, 0);

    let stored = sqlx::query_as::<_, (Uuid, serde_json::Value, serde_json::Value)>(
        "SELECT resource_id, before_state, after_state FROM normalized_events WHERE event_identity = $1",
    )
    .bind(&stale.event_identity)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(stored.0, event_time_registry_resource_id);
    assert_eq!(
        stored.1["grant_source"]["authority_kind"].as_str(),
        Some("registry_only")
    );
    assert_eq!(
        stored.2["revocation_source"]["authority_kind"].as_str(),
        Some("registry_only")
    );

    let invalidation_keys = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT projection, projection_key
        FROM projection_invalidations
        WHERE projection = 'permissions_current'
        ORDER BY projection_key
        "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        invalidation_keys,
        vec![
            (
                "permissions_current".to_owned(),
                stale_later_registrar_resource_id.to_string()
            ),
            (
                "permissions_current".to_owned(),
                event_time_registry_resource_id.to_string()
            ),
        ]
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_count_only_upsert_repairs_ens_v1_registry_event_time_wrapper_permission_grant_source()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let stale_later_wrapper_resource_id = Uuid::from_u128(0x1800);
    let event_time_registry_resource_id = Uuid::from_u128(0x1900);
    seed_ens_v1_registry_event_time_wrapper_repair_resources(
        database.pool(),
        stale_later_wrapper_resource_id,
        event_time_registry_resource_id,
    )
    .await?;

    let stale = ens_v1_registry_event_time_permission_repair_event(
        stale_later_wrapper_resource_id,
        "wrapper",
        "wrapper:ethereum-mainnet:alice",
    );
    upsert_normalized_events(database.pool(), std::slice::from_ref(&stale)).await?;

    let repaired = ens_v1_registry_event_time_permission_repair_event(
        event_time_registry_resource_id,
        "registry_only",
        "registry-only:ethereum-mainnet:0xalice_namehash",
    );
    let inserted_count =
        upsert_normalized_events_count_only(database.pool(), std::slice::from_ref(&repaired))
            .await?;
    assert_eq!(inserted_count, 0);

    let stored = sqlx::query_as::<_, (Uuid, serde_json::Value)>(
        "SELECT resource_id, after_state FROM normalized_events WHERE event_identity = $1",
    )
    .bind(&stale.event_identity)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(stored.0, event_time_registry_resource_id);
    assert_eq!(
        stored.1["grant_source"]["authority_kind"].as_str(),
        Some("registry_only")
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_upsert_rejects_ens_v1_renewal_resource_repair_for_unknown_target_resource()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let old_resource_id = Uuid::from_u128(0x2100);
    let seeded_repaired_resource_id = Uuid::from_u128(0x2200);
    let unknown_resource_id = Uuid::from_u128(0x2300);
    let old_surface_binding_id = Uuid::from_u128(0x2400);
    seed_ens_v1_renewal_resource_repair_identity_rows(
        database.pool(),
        old_resource_id,
        seeded_repaired_resource_id,
        old_surface_binding_id,
    )
    .await?;

    let event = ens_v1_renewal_event(
        "ens-v1-unwrapped-authority:renewal:unknown-target-resource",
        old_resource_id,
    );
    upsert_normalized_events(database.pool(), std::slice::from_ref(&event)).await?;

    let mut repaired = event.clone();
    repaired.resource_id = Some(unknown_resource_id);
    let error = upsert_normalized_events(database.pool(), std::slice::from_ref(&repaired))
        .await
        .expect_err("ENSv1 renewal repair must reject unknown target resources");
    assert!(
        error
            .to_string()
            .contains("ENSv1 renewal resource_id repair rejected invalid resource anchors"),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_upsert_repairs_ens_v1_renewal_release_identity_collision() -> Result<()> {
    let database = TestDatabase::new().await?;
    let old_resource_id = Uuid::from_u128(0x3100);
    let repaired_resource_id = Uuid::from_u128(0x3200);
    let old_surface_binding_id = Uuid::from_u128(0x3300);
    seed_ens_v1_renewal_resource_repair_identity_rows(
        database.pool(),
        old_resource_id,
        repaired_resource_id,
        old_surface_binding_id,
    )
    .await?;

    let event = ens_v1_renewal_event(
        "ens-v1-unwrapped-authority:renewal:release-collision",
        old_resource_id,
    );
    upsert_normalized_events(database.pool(), std::slice::from_ref(&event)).await?;

    let stale_release_identity = "ens_v1_unwrapped_authority:RegistrationReleased:release:0xreleaseblock:ens:alice.eth:registrar:ethereum-mainnet:old";
    let corrected_release_identity = "ens_v1_unwrapped_authority:RegistrationReleased:release:0xreleaseblock:ens:alice.eth:registrar:ethereum-mainnet:repaired";
    let mut stale_release = ens_v1_renewal_related_event(
        stale_release_identity,
        "RegistrationReleased",
        old_resource_id,
        json!({
            "released_at": 1_717_171_902_i64,
            "labelhash": "0xcbf005454c11bc7e583aa4a100988b4a893acb2233dbb77afef8d9f931df3735"
        }),
    );
    stale_release.before_state = json!({
        "registrant": "0x0000000000000000000000000000000000000123",
        "expiry": 1872542016_i64
    });
    upsert_normalized_events(database.pool(), std::slice::from_ref(&stale_release)).await?;

    let mut corrected_release = stale_release.clone();
    corrected_release.event_identity = corrected_release_identity.to_owned();
    corrected_release.resource_id = Some(repaired_resource_id);
    let mut repaired = event.clone();
    repaired.resource_id = Some(repaired_resource_id);

    upsert_normalized_events(database.pool(), &[corrected_release.clone(), repaired]).await?;

    let release_states = sqlx::query_as::<_, (String, String, Uuid)>(
        r#"
        SELECT event_identity, canonicality_state::text, resource_id
        FROM normalized_events
        WHERE event_identity IN ($1, $2)
        ORDER BY event_identity
        "#,
    )
    .bind(stale_release_identity)
    .bind(corrected_release_identity)
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        release_states,
        vec![
            (
                stale_release_identity.to_owned(),
                "orphaned".to_owned(),
                old_resource_id
            ),
            (
                corrected_release_identity.to_owned(),
                "canonical".to_owned(),
                repaired_resource_id
            )
        ]
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_upsert_keeps_ens_v1_old_resource_when_prior_event_remains() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let old_resource_id = Uuid::from_u128(0x1100);
    let repaired_resource_id = Uuid::from_u128(0x1200);
    let old_surface_binding_id = Uuid::from_u128(0x1300);
    seed_ens_v1_renewal_resource_repair_identity_rows(
        database.pool(),
        old_resource_id,
        repaired_resource_id,
        old_surface_binding_id,
    )
    .await?;

    let mut prior_surface_event = ens_v1_renewal_related_event(
        "ens-v1-unwrapped-authority:renewal:resource-transition:prior-surface",
        "SurfaceBound",
        old_resource_id,
        json!({"binding_kind": "declared_registry_path"}),
    );
    prior_surface_event.block_number = Some(25_238_000);
    upsert_normalized_events(database.pool(), std::slice::from_ref(&prior_surface_event)).await?;

    let mut event = normalized_event(
        "ens-v1-unwrapped-authority:renewal:resource-transition-with-prior",
        "RegistrationRenewed",
        CanonicalityState::Canonical,
    );
    event.logical_name_id = Some("ens:alice.eth".to_owned());
    event.resource_id = Some(old_resource_id);
    event.source_family = "ens_v1_registrar_l1".to_owned();
    event.derivation_kind = "ens_v1_unwrapped_authority".to_owned();
    event.chain_id = Some("ethereum-mainnet".to_owned());
    event.block_number = Some(25_238_970);
    event.block_hash =
        Some("0x9da3c01e4f15f21e87656f7ba57b31a80709464339389cc0194099b0926ce36f".to_owned());
    event.transaction_hash =
        Some("0x93b81927d785859a89e80c3dd900d63da000f38c3f90e09cbaf0ec0908774627".to_owned());
    event.log_index = Some(1059);
    event.raw_fact_ref = json!({
        "kind": "raw_log",
        "chain_id": "ethereum-mainnet",
        "block_number": 25238970,
        "block_hash": "0x9da3c01e4f15f21e87656f7ba57b31a80709464339389cc0194099b0926ce36f",
        "transaction_hash": "0x93b81927d785859a89e80c3dd900d63da000f38c3f90e09cbaf0ec0908774627",
        "transaction_index": 219,
        "log_index": 1059,
    });
    event.before_state = json!({"expiry": 1872542016});
    event.after_state = json!({
        "expiry": 1872542016,
        "labelhash": "0xcbf005454c11bc7e583aa4a100988b4a893acb2233dbb77afef8d9f931df3735",
    });
    upsert_normalized_events(database.pool(), std::slice::from_ref(&event)).await?;

    let mut repaired = event.clone();
    repaired.resource_id = Some(repaired_resource_id);
    upsert_normalized_events(database.pool(), std::slice::from_ref(&repaired)).await?;

    let old_resource_state: String =
        sqlx::query_scalar("SELECT canonicality_state::text FROM resources WHERE resource_id = $1")
            .bind(old_resource_id)
            .fetch_one(database.pool())
            .await?;
    assert_eq!(old_resource_state, "canonical");

    let old_binding_state: String = sqlx::query_scalar(
        "SELECT canonicality_state::text FROM surface_bindings WHERE surface_binding_id = $1",
    )
    .bind(old_surface_binding_id)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(old_binding_state, "canonical");

    let prior_event_state: String = sqlx::query_scalar(
        "SELECT canonicality_state::text FROM normalized_events WHERE event_identity = $1",
    )
    .bind(&prior_surface_event.event_identity)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(prior_event_state, "canonical");

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_upsert_rejects_token_transfer_before_state_change() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut event = normalized_event(
        "raw-log:token-control-transferred",
        "TokenControlTransferred",
        CanonicalityState::Finalized,
    );
    event.logical_name_id = Some("ens:alice.eth".to_owned());
    event.source_family = "ens_v1_registrar_l1".to_owned();
    event.derivation_kind = "ens_v1_unwrapped_authority".to_owned();
    event.before_state = json!({
        "from": "0x0000000000000000000000000000000000000001",
    });
    event.after_state = json!({
        "labelhash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "to": "0x0000000000000000000000000000000000000002",
    });

    upsert_normalized_events(database.pool(), std::slice::from_ref(&event)).await?;

    event.before_state = json!({
        "from": "0x0000000000000000000000000000000000000003",
    });
    let error = upsert_normalized_events(database.pool(), std::slice::from_ref(&event))
        .await
        .expect_err("concrete token-transfer before-state changes must fail");

    assert!(
        error.to_string().contains(
            "normalized event identity mismatch for event raw-log:token-control-transferred"
        ),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_upsert_repairs_basenames_primary_claim_source_transition() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut event = normalized_event(
        "ens-v1-reverse-claim:record:base-primary-transition",
        "RecordChanged",
        CanonicalityState::Canonical,
    );
    event.namespace = "basenames".to_owned();
    event.source_family = "basenames_base_primary".to_owned();
    event.derivation_kind = "ens_v1_reverse_claim".to_owned();
    event.chain_id = Some("base-mainnet".to_owned());
    event.block_number = Some(46_723_622);
    event.block_hash =
        Some("0x85fbd2e5085b1a1deb62dc0ff2e1a7fc792ef98fb6b1e944890603d699060d84".to_owned());
    event.transaction_hash =
        Some("0x3e6b60619f99ffeb27235dfa86417ebc4d21a9dfb88104cf4bd1243184288ae9".to_owned());
    event.log_index = Some(578);
    event.raw_fact_ref = json!({
        "kind": "raw_log",
        "chain_id": "base-mainnet",
        "block_number": 46723622,
        "block_hash": "0x85fbd2e5085b1a1deb62dc0ff2e1a7fc792ef98fb6b1e944890603d699060d84",
        "transaction_hash": "0x3e6b60619f99ffeb27235dfa86417ebc4d21a9dfb88104cf4bd1243184288ae9",
        "transaction_index": 115,
        "log_index": 578,
    });
    event.before_state = json!({});
    event.after_state = json!({
        "source_event": "NameForAddrChanged",
        "record_key": "name",
        "record_family": "name",
        "selector_key": null,
        "raw_name": "alice.base.eth",
        "primary_claim_source": {
            "address": "0x7e50c29692e8d701a375bf53de93b62f9aa47af8",
            "coin_type": "60",
            "namespace": "basenames",
            "reverse_name": "7e50c29692e8d701a375bf53de93b62f9aa47af8.80002105.reverse",
            "reverse_node": "0x76097049b6146b77e9cd73ee786c29ae4eefb49e4772d0a3cefd99f7087760c5",
            "claim_provenance": {
                "contract_role": "reverse_registrar",
                "source_family": "basenames_base_primary",
                "emitting_address": "0x79ea96012eea67a83431f1701b3dff7e37f9e282",
                "contract_instance_id": "86c6cbd2-19e7-4de1-85a0-1a7842fd8c25"
            }
        }
    });
    upsert_normalized_events(database.pool(), std::slice::from_ref(&event)).await?;

    let mut repaired = event.clone();
    repaired.after_state["primary_claim_source"] = json!({
        "address": "0x7e50c29692e8d701a375bf53de93b62f9aa47af8",
        "coin_type": "2147492101",
        "namespace": "basenames",
        "reverse_name": "7e50c29692e8d701a375bf53de93b62f9aa47af8.80002105.reverse",
        "reverse_node": "0x76097049b6146b77e9cd73ee786c29ae4eefb49e4772d0a3cefd99f7087760c5",
        "claim_provenance": {
            "contract_role": "reverse_registrar",
            "source_family": "basenames_base_primary",
            "emitting_address": "0x0000000000d8e504002cc26e3ec46d81971c1664",
            "contract_instance_id": "29dfdbc2-902c-4b98-b38d-5169180d6eb6"
        }
    });

    let snapshots =
        upsert_normalized_events(database.pool(), std::slice::from_ref(&repaired)).await?;
    assert_eq!(snapshots[0].after_state, repaired.after_state);

    let stored_after_state: serde_json::Value =
        sqlx::query_scalar("SELECT after_state FROM normalized_events WHERE event_identity = $1")
            .bind(&event.event_identity)
            .fetch_one(database.pool())
            .await?;
    assert_eq!(stored_after_state, repaired.after_state);

    let queued_change_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM projection_normalized_event_changes change
        JOIN normalized_events event
          ON event.normalized_event_id = change.normalized_event_id
        WHERE event.event_identity = $1
          AND change.change_kind = 'canonicality_update'
        "#,
    )
    .bind(&event.event_identity)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(queued_change_count, 1);

    let invalidation_keys = sqlx::query_as::<_, (String, String, serde_json::Value)>(
        r#"
        SELECT projection, projection_key, key_payload
        FROM projection_invalidations
        WHERE projection = 'primary_names_current'
        ORDER BY projection_key
        "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        invalidation_keys,
        vec![
            (
                "primary_names_current".to_owned(),
                "0x7e50c29692e8d701a375bf53de93b62f9aa47af8:basenames:2147492101".to_owned(),
                json!({
                    "address": "0x7e50c29692e8d701a375bf53de93b62f9aa47af8",
                    "namespace": "basenames",
                    "coin_type": "2147492101"
                })
            ),
            (
                "primary_names_current".to_owned(),
                "0x7e50c29692e8d701a375bf53de93b62f9aa47af8:basenames:60".to_owned(),
                json!({
                    "address": "0x7e50c29692e8d701a375bf53de93b62f9aa47af8",
                    "namespace": "basenames",
                    "coin_type": "60"
                })
            ),
        ]
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_upsert_repairs_basenames_primary_claim_source_with_local_contract_ids()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let mut event = basenames_primary_claim_source_repair_event();
    event.event_identity = "ens-v1-reverse-claim:record:base-primary-local-contract-ids".to_owned();
    event.after_state["primary_claim_source"]["claim_provenance"]["contract_instance_id"] =
        json!("11111111-1111-4111-8111-111111111111");
    upsert_normalized_events(database.pool(), std::slice::from_ref(&event)).await?;

    let mut repaired = basenames_repaired_primary_claim_source_event(&event);
    repaired.after_state["primary_claim_source"]["claim_provenance"]["contract_instance_id"] =
        json!("22222222-2222-4222-8222-222222222222");

    let snapshots =
        upsert_normalized_events(database.pool(), std::slice::from_ref(&repaired)).await?;
    assert_eq!(snapshots[0].after_state, repaired.after_state);

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_upsert_rejects_basenames_primary_claim_source_repair_for_incomplete_tuple()
-> Result<()> {
    for case in [
        "missing_address",
        "blank_reverse_node",
        "missing_reverse_name",
        "blank_source_family",
        "missing_contract_role",
    ] {
        let database = TestDatabase::new().await?;
        let mut event = basenames_primary_claim_source_repair_event();
        event.event_identity =
            format!("ens-v1-reverse-claim:record:base-primary-incomplete-tuple:{case}");
        mutate_basenames_primary_claim_tuple(&mut event, case);
        upsert_normalized_events(database.pool(), std::slice::from_ref(&event)).await?;

        let mut repaired = basenames_repaired_primary_claim_source_event(&event);
        mutate_basenames_primary_claim_tuple(&mut repaired, case);
        let error = upsert_normalized_events(database.pool(), std::slice::from_ref(&repaired))
            .await
            .expect_err(&format!(
                "incomplete tuple case {case} must not be repaired"
            ));
        assert!(
            error.to_string().contains(&format!(
                "normalized event identity mismatch for event {}",
                event.event_identity
            )),
            "unexpected error for {case}: {error:#}"
        );
        database.cleanup().await?;
    }

    Ok(())
}

#[tokio::test]
async fn normalized_event_upsert_rejects_basenames_primary_claim_source_repair_for_resolver_event()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let mut event = basenames_primary_claim_source_repair_event();
    event.event_identity = "ens-v1-reverse-claim:resolver:base-primary-transition".to_owned();
    event.event_kind = "ResolverChanged".to_owned();

    upsert_normalized_events(database.pool(), std::slice::from_ref(&event)).await?;

    let mut repaired = event.clone();
    repaired.after_state["primary_claim_source"]["coin_type"] = json!("2147492101");
    repaired.after_state["primary_claim_source"]["claim_provenance"]["emitting_address"] =
        json!("0x0000000000d8e504002cc26e3ec46d81971c1664");

    let error = upsert_normalized_events(database.pool(), std::slice::from_ref(&repaired))
        .await
        .expect_err("non-primary-name events must not be repaired");
    assert!(
        error.to_string().contains(
            "normalized event identity mismatch for event ens-v1-reverse-claim:resolver:base-primary-transition"
        ),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_upsert_rejects_basenames_primary_claim_source_repair_for_registry_family()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let mut event = basenames_primary_claim_source_repair_event();
    event.event_identity = "ens-v1-unwrapped-authority:record:base-primary-transition".to_owned();
    event.source_family = "basenames_base_registry".to_owned();
    event.derivation_kind = "ens_v1_unwrapped_authority".to_owned();

    upsert_normalized_events(database.pool(), std::slice::from_ref(&event)).await?;

    let mut repaired = event.clone();
    repaired.after_state["primary_claim_source"]["coin_type"] = json!("2147492101");
    repaired.after_state["primary_claim_source"]["claim_provenance"]["emitting_address"] =
        json!("0x0000000000d8e504002cc26e3ec46d81971c1664");

    let error = upsert_normalized_events(database.pool(), std::slice::from_ref(&repaired))
        .await
        .expect_err("registry-family events must not be repaired as primary claims");
    assert!(
        error.to_string().contains(
            "normalized event identity mismatch for event ens-v1-unwrapped-authority:record:base-primary-transition"
        ),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_upsert_rejects_basenames_primary_claim_source_repair_for_wrong_contract()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let event = basenames_primary_claim_source_repair_event();
    upsert_normalized_events(database.pool(), std::slice::from_ref(&event)).await?;

    let mut repaired = event.clone();
    repaired.after_state["primary_claim_source"]["coin_type"] = json!("2147492101");
    repaired.after_state["primary_claim_source"]["claim_provenance"]["emitting_address"] =
        json!("0x0000000000d8e504002cc26e3ec46d81971c1664");
    repaired.after_state["primary_claim_source"]["claim_provenance"]["contract_instance_id"] =
        json!("00000000-0000-0000-0000-000000000000");

    let error = upsert_normalized_events(database.pool(), std::slice::from_ref(&repaired))
        .await
        .expect_err("wrong repaired contract instance must not be accepted");
    assert!(
        error.to_string().contains(
            "normalized event identity mismatch for event ens-v1-reverse-claim:record:base-primary-transition"
        ),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_upsert_promotes_canonicality() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_normalized_events(
        database.pool(),
        &[normalized_event(
            "manifest:1:source_manifest",
            "SourceManifestUpdated",
            CanonicalityState::Canonical,
        )],
    )
    .await?;

    let promoted = upsert_normalized_events(
        database.pool(),
        &[normalized_event(
            "manifest:1:source_manifest",
            "SourceManifestUpdated",
            CanonicalityState::Finalized,
        )],
    )
    .await?;

    assert_eq!(promoted.len(), 1);
    assert_eq!(promoted[0].canonicality_state, CanonicalityState::Finalized);

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_upsert_skips_unchanged_conflicts() -> Result<()> {
    let database = TestDatabase::new().await?;
    let event = normalized_event(
        "manifest:1:unchanged",
        "SourceManifestUpdated",
        CanonicalityState::Finalized,
    );

    upsert_normalized_events(database.pool(), std::slice::from_ref(&event)).await?;

    let anchored_observed_at = sqlx::types::time::OffsetDateTime::from_unix_timestamp(946_684_800)?;
    sqlx::query("UPDATE normalized_events SET observed_at = $1 WHERE event_identity = $2")
        .bind(anchored_observed_at)
        .bind(&event.event_identity)
        .execute(database.pool())
        .await?;

    let snapshots = upsert_normalized_events(database.pool(), std::slice::from_ref(&event)).await?;
    assert_eq!(
        snapshots[0].canonicality_state,
        CanonicalityState::Finalized
    );

    let observed_at = sqlx::query_scalar::<_, sqlx::types::time::OffsetDateTime>(
        "SELECT observed_at FROM normalized_events WHERE event_identity = $1",
    )
    .bind(&event.event_identity)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(observed_at, anchored_observed_at);

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_upsert_escapes_nul_bytes_for_jsonb() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut event = normalized_event(
        "manifest:1:nul-byte",
        "CapabilityChanged",
        CanonicalityState::Finalized,
    );
    event.logical_name_id = Some("name\0with-nul".to_owned());
    event.after_state = json!({
        "record": "before\0after",
        "key\0with-nul": "value",
        "nested": ["left\0right"],
    });

    let inserted = upsert_normalized_events(database.pool(), &[event]).await?;
    assert_eq!(
        inserted[0].logical_name_id.as_deref(),
        Some("name\\u0000with-nul")
    );
    assert_eq!(
        inserted[0].after_state,
        json!({
            "record": "before\\u0000after",
            "key\\u0000with-nul": "value",
            "nested": ["left\\u0000right"],
        })
    );

    let loaded = load_normalized_events_by_namespace(database.pool(), "ens").await?;
    assert_eq!(loaded, inserted);

    database.cleanup().await
}

#[tokio::test]
async fn orphan_range_marks_block_derived_normalized_events_orphaned() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_raw_blocks(
        database.pool(),
        &[
            RawBlock {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0x001".to_owned(),
                parent_hash: None,
                block_number: 1,
                block_timestamp: sqlx::types::time::OffsetDateTime::UNIX_EPOCH,
                logs_bloom: None,
                transactions_root: None,
                receipts_root: None,
                state_root: None,
                canonicality_state: CanonicalityState::Canonical,
            },
            RawBlock {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0x002".to_owned(),
                parent_hash: Some("0x001".to_owned()),
                block_number: 2,
                block_timestamp: sqlx::types::time::OffsetDateTime::UNIX_EPOCH,
                logs_bloom: None,
                transactions_root: None,
                receipts_root: None,
                state_root: None,
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    upsert_normalized_events(
        database.pool(),
        &[
            NormalizedEvent {
                chain_id: Some("ethereum-mainnet".to_owned()),
                block_number: Some(1),
                block_hash: Some("0x001".to_owned()),
                transaction_hash: Some("0xtx1".to_owned()),
                log_index: Some(0),
                event_identity: "preimage:0x001:0".to_owned(),
                event_kind: "PreimageObserved".to_owned(),
                ..normalized_event(
                    "preimage:0x001:0",
                    "PreimageObserved",
                    CanonicalityState::Canonical,
                )
            },
            NormalizedEvent {
                chain_id: Some("ethereum-mainnet".to_owned()),
                block_number: Some(2),
                block_hash: Some("0x002".to_owned()),
                transaction_hash: Some("0xtx2".to_owned()),
                log_index: Some(1),
                event_identity: "preimage:0x002:1".to_owned(),
                event_kind: "PreimageObserved".to_owned(),
                ..normalized_event(
                    "preimage:0x002:1",
                    "PreimageObserved",
                    CanonicalityState::Finalized,
                )
            },
        ],
    )
    .await?;

    let orphaned_count = mark_block_derived_normalized_events_range_orphaned(
        database.pool(),
        "ethereum-mainnet",
        "0x002",
        Some("0x001"),
    )
    .await?;
    assert_eq!(orphaned_count, 1);

    let events = load_normalized_events_by_namespace(database.pool(), "ens").await?;
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].canonicality_state, CanonicalityState::Canonical);
    assert_eq!(events[1].canonicality_state, CanonicalityState::Orphaned);

    database.cleanup().await
}
