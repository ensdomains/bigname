use std::str::FromStr;

use anyhow::{Context, Result};
use serde_json::json;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
    types::{Uuid, time::OffsetDateTime},
};

use super::*;
use crate::{
    CanonicalityState, NameSurface, NormalizedEvent, default_database_url,
    label_preimage_from_label, upsert_label_preimages, upsert_name_surfaces,
    upsert_normalized_events,
};

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
            .context("failed to parse database URL for children_current tests")?;
        let database_name = format!(
            "bn_st_children_{}_{}",
            std::process::id(),
            Uuid::new_v4().simple()
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for children_current tests")?;

        sqlx::query(&format!(
            r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
            database_name
        ))
        .execute(&admin_pool)
        .await
        .with_context(|| format!("failed to drop stale test database {database_name}"))?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect children_current test pool")?;

        crate::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for children_current tests")?;

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

fn timestamp(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp must be valid")
}

fn name_surface(
    logical_name_id: &str,
    display_name: &str,
    namehash: &str,
    block_number: i64,
    canonicality_state: CanonicalityState,
) -> NameSurface {
    name_surface_on_chain(
        logical_name_id,
        display_name,
        namehash,
        "ethereum-mainnet",
        block_number,
        canonicality_state,
    )
}

fn name_surface_on_chain(
    logical_name_id: &str,
    display_name: &str,
    namehash: &str,
    chain_id: &str,
    block_number: i64,
    canonicality_state: CanonicalityState,
) -> NameSurface {
    let namespace = logical_name_id
        .split_once(':')
        .map(|(namespace, _)| namespace)
        .expect("logical_name_id must include namespace")
        .to_owned();

    NameSurface {
        logical_name_id: logical_name_id.to_owned(),
        namespace,
        input_name: display_name.to_owned(),
        canonical_display_name: display_name.to_owned(),
        normalized_name: display_name.to_owned(),
        dns_encoded_name: display_name.as_bytes().to_vec(),
        namehash: namehash.to_owned(),
        labelhashes: labelhashes_for_name(display_name),
        normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id: chain_id.to_owned(),
        block_hash: format!("0xsurface{block_number:02x}"),
        block_number,
        provenance: json!({"source": "children_current_test", "kind": "surface"}),
        canonicality_state,
    }
}

fn children_current_row(
    parent_logical_name_id: &str,
    child_logical_name_id: &str,
    display_name: &str,
    namehash: &str,
    block_number: i64,
) -> ChildrenCurrentRow {
    ChildrenCurrentRow {
        parent_logical_name_id: parent_logical_name_id.to_owned(),
        child_logical_name_id: child_logical_name_id.to_owned(),
        surface_class: DECLARED_SURFACE_CLASS.to_owned(),
        namespace: "ens".to_owned(),
        canonical_display_name: display_name.to_owned(),
        normalized_name: display_name.to_owned(),
        namehash: namehash.to_owned(),
        labelhash: labelhashes_for_name(display_name).into_iter().next(),
        owner: None,
        registrant: None,
        provenance: json!({
            "normalized_event_ids": [block_number],
            "derivation_kind": "children_current_rebuild"
        }),
        chain_positions: json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": block_number,
                "block_hash": format!("0xblock{block_number:02x}"),
                "timestamp": "2026-04-17T00:00:00Z"
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                "ethereum-mainnet": "finalized"
            }
        }),
        manifest_version: 1,
        last_recomputed_at: timestamp(1_717_172_000 + block_number),
    }
}

struct SubregistryEventSeed<'a> {
    event_identity: &'a str,
    namespace: &'a str,
    source_family: &'a str,
    chain_id: &'a str,
    parent_namehash: &'a str,
    child_namehash: &'a str,
    block_number: i64,
    log_index: i64,
    canonicality_state: CanonicalityState,
    tombstone: bool,
    active_edge: bool,
}

fn subregistry_event(seed: SubregistryEventSeed<'_>) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: seed.event_identity.to_owned(),
        namespace: seed.namespace.to_owned(),
        logical_name_id: None,
        resource_id: None,
        event_kind: SUBREGISTRY_EVENT_KIND.to_owned(),
        source_family: seed.source_family.to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some(seed.chain_id.to_owned()),
        block_number: Some(seed.block_number),
        block_hash: Some(format!("0xeventblock{:02x}", seed.block_number)),
        transaction_hash: Some(format!("0xtx{:02x}", seed.block_number)),
        log_index: Some(seed.log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": seed.chain_id,
            "block_number": seed.block_number,
            "log_index": seed.log_index
        }),
        derivation_kind: SUBREGISTRY_DERIVATION_KIND.to_owned(),
        canonicality_state: seed.canonicality_state,
        before_state: json!({}),
        after_state: json!({
            "source_event": "NewOwner",
            "edge_kind": "subregistry",
            "parent_node": seed.parent_namehash,
            "child_node": seed.child_namehash,
            "labelhash": labelhash_for_child_namehash(seed.child_namehash),
            "owner": "0x0000000000000000000000000000000000000001",
            "tombstone": seed.tombstone,
            "active_edge": seed.active_edge
        }),
    }
}

fn labelhashes_for_name(name: &str) -> Vec<String> {
    name.split('.').map(labelhash_for_label).collect()
}

fn labelhash_for_child_namehash(child_namehash: &str) -> String {
    let child_name = child_namehash
        .strip_prefix("node:")
        .unwrap_or(child_namehash);
    labelhash_for_label(child_name.split('.').next().unwrap_or(child_name))
}

fn labelhash_for_label(label: &str) -> String {
    format!(
        "0x{}",
        alloy_primitives::hex::encode(alloy_primitives::keccak256(label.as_bytes()))
    )
}

fn ensv2_subregistry_event(
    event_identity: &str,
    parent_logical_name_id: &str,
    from_contract_instance_id: &str,
    to_contract_instance_id: Option<&str>,
    block_number: i64,
    log_index: i64,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: Some(parent_logical_name_id.to_owned()),
        resource_id: None,
        event_kind: SUBREGISTRY_EVENT_KIND.to_owned(),
        source_family: ENSV2_ROOT_SOURCE_FAMILY.to_owned(),
        manifest_version: 2,
        source_manifest_id: None,
        chain_id: Some("ethereum-sepolia".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(format!("0xensv2eventblock{block_number:02x}")),
        transaction_hash: Some(format!("0xensv2tx{block_number:02x}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "ethereum-sepolia",
            "block_number": block_number,
            "log_index": log_index,
            "emitting_address": "0x00000000000000000000000000000000000000aa"
        }),
        derivation_kind: ENSV2_REGISTRY_DERIVATION_KIND.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "source_event": "SubregistryUpdated",
            "token_id": format!("0xtoken{block_number:02x}"),
            "subregistry": to_contract_instance_id.map(|_| "0x00000000000000000000000000000000000000bb"),
            "from_contract_instance_id": from_contract_instance_id,
            "to_contract_instance_id": to_contract_instance_id,
        }),
    }
}

fn ensv2_parent_event(
    event_identity: &str,
    parent_name: &str,
    parent_contract_instance_id: &str,
    registry_contract_instance_id: &str,
    emitting_address: &str,
    block_number: i64,
    log_index: i64,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: None,
        resource_id: None,
        event_kind: PARENT_EVENT_KIND.to_owned(),
        source_family: ENSV2_REGISTRY_SOURCE_FAMILY.to_owned(),
        manifest_version: 3,
        source_manifest_id: None,
        chain_id: Some("ethereum-sepolia".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(format!("0xensv2eventblock{block_number:02x}")),
        transaction_hash: Some(format!("0xensv2tx{block_number:02x}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "ethereum-sepolia",
            "block_number": block_number,
            "log_index": log_index,
            "emitting_address": emitting_address
        }),
        derivation_kind: ENSV2_REGISTRY_DERIVATION_KIND.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "source_event": "ParentUpdated",
            "parent": "0x00000000000000000000000000000000000000aa",
            "label": parent_name.split('.').next().unwrap_or(parent_name),
            "registry_name": parent_name,
            "registry_contract_instance_id": registry_contract_instance_id,
            "parent_contract_instance_id": parent_contract_instance_id,
        }),
    }
}

fn ensv2_registration_event(
    event_identity: &str,
    child_logical_name_id: &str,
    event_kind: &str,
    registry_contract_instance_id: &str,
    emitting_address: &str,
    block_number: i64,
    log_index: i64,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: Some(child_logical_name_id.to_owned()),
        resource_id: None,
        event_kind: event_kind.to_owned(),
        source_family: ENSV2_REGISTRY_SOURCE_FAMILY.to_owned(),
        manifest_version: 3,
        source_manifest_id: None,
        chain_id: Some("ethereum-sepolia".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(format!("0xensv2eventblock{block_number:02x}")),
        transaction_hash: Some(format!("0xensv2tx{block_number:02x}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "ethereum-sepolia",
            "block_number": block_number,
            "log_index": log_index,
            "emitting_address": emitting_address
        }),
        derivation_kind: ENSV2_REGISTRY_DERIVATION_KIND.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "source_event": event_kind,
            "registry_contract_instance_id": registry_contract_instance_id,
            "status": if event_kind == REGISTRATION_RELEASED_EVENT_KIND {
                "released"
            } else {
                "registered"
            },
        }),
    }
}

#[tokio::test]
async fn children_current_upserts_and_loads_declared_rows() -> Result<()> {
    let database = TestDatabase::new().await?;
    let parent_logical_name_id = "ens:parent.eth";
    let child_logical_name_id = "ens:alice.parent.eth";

    upsert_name_surfaces(
        database.pool(),
        &[
            name_surface(
                parent_logical_name_id,
                "parent.eth",
                "node:parent.eth",
                10,
                CanonicalityState::Finalized,
            ),
            name_surface(
                child_logical_name_id,
                "alice.parent.eth",
                "node:alice.parent.eth",
                11,
                CanonicalityState::Finalized,
            ),
        ],
    )
    .await?;

    let expected = children_current_row(
        parent_logical_name_id,
        child_logical_name_id,
        "alice.parent.eth",
        "node:alice.parent.eth",
        11,
    );

    let inserted =
        upsert_children_current_rows(database.pool(), std::slice::from_ref(&expected)).await?;
    assert_eq!(inserted, vec![expected.clone()]);
    assert_eq!(
        load_children_current(database.pool(), parent_logical_name_id).await?,
        vec![expected.clone()]
    );

    assert_eq!(
        delete_children_current(database.pool(), parent_logical_name_id).await?,
        1
    );
    assert!(
        load_children_current(database.pool(), parent_logical_name_id)
            .await?
            .is_empty()
    );

    upsert_children_current_rows(database.pool(), &[expected]).await?;
    assert_eq!(clear_children_current(database.pool()).await?, 1);

    database.cleanup().await
}

#[tokio::test]
async fn children_current_load_keeps_label_preimage_rows_when_exact_surface_is_noncanonical()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let parent_logical_name_id = "ens:parent.eth";
    let child_logical_name_id = "ens:mystery.parent.eth";

    upsert_name_surfaces(
        database.pool(),
        &[
            name_surface(
                parent_logical_name_id,
                "parent.eth",
                "node:parent.eth",
                12,
                CanonicalityState::Finalized,
            ),
            name_surface(
                child_logical_name_id,
                "mystery.parent.eth",
                "node:mystery.parent.eth",
                13,
                CanonicalityState::Observed,
            ),
        ],
    )
    .await?;

    let mut expected = children_current_row(
        parent_logical_name_id,
        child_logical_name_id,
        "mystery.parent.eth",
        "node:mystery.parent.eth",
        13,
    );
    expected.provenance["label"] = json!({
        "labelhash": expected.labelhash,
        "status": "known",
        "source": "label_preimage",
    });

    upsert_children_current_rows(database.pool(), std::slice::from_ref(&expected)).await?;

    assert_eq!(
        load_children_current(database.pool(), parent_logical_name_id).await?,
        vec![expected]
    );

    database.cleanup().await
}

#[tokio::test]
async fn children_current_load_orders_by_display_name() -> Result<()> {
    let database = TestDatabase::new().await?;
    let parent_logical_name_id = "ens:parent.eth";

    upsert_name_surfaces(
        database.pool(),
        &[
            name_surface(
                parent_logical_name_id,
                "parent.eth",
                "node:parent.eth",
                20,
                CanonicalityState::Finalized,
            ),
            name_surface(
                "ens:bob.parent.eth",
                "bob.parent.eth",
                "node:bob.parent.eth",
                21,
                CanonicalityState::Finalized,
            ),
            name_surface(
                "ens:alice.parent.eth",
                "alice.parent.eth",
                "node:alice.parent.eth",
                22,
                CanonicalityState::Finalized,
            ),
        ],
    )
    .await?;

    let bob = children_current_row(
        parent_logical_name_id,
        "ens:bob.parent.eth",
        "bob.parent.eth",
        "node:bob.parent.eth",
        21,
    );
    let alice = children_current_row(
        parent_logical_name_id,
        "ens:alice.parent.eth",
        "alice.parent.eth",
        "node:alice.parent.eth",
        22,
    );
    upsert_children_current_rows(database.pool(), &[bob.clone(), alice.clone()]).await?;

    assert_eq!(
        load_children_current(database.pool(), parent_logical_name_id).await?,
        vec![alice, bob]
    );

    database.cleanup().await
}

#[tokio::test]
async fn children_current_rejects_malformed_owner_and_registrant_addresses() -> Result<()> {
    let database = TestDatabase::new().await?;
    let parent_logical_name_id = "ens:parent.eth";

    upsert_name_surfaces(
        database.pool(),
        &[name_surface(
            parent_logical_name_id,
            "parent.eth",
            "node:parent.eth",
            22,
            CanonicalityState::Finalized,
        )],
    )
    .await?;

    let mut malformed_owner = children_current_row(
        parent_logical_name_id,
        "ens:bad-owner.parent.eth",
        "bad-owner.parent.eth",
        "node:bad-owner.parent.eth",
        23,
    );
    malformed_owner.owner = Some("not-an-address".to_owned());

    let owner_error = upsert_children_current_rows(database.pool(), &[malformed_owner])
        .await
        .expect_err("malformed owner address must be rejected");
    assert!(
        format!("{owner_error:?}").contains("invalid owner"),
        "unexpected owner validation error: {owner_error:?}"
    );

    let mut malformed_registrant = children_current_row(
        parent_logical_name_id,
        "ens:bad-registrant.parent.eth",
        "bad-registrant.parent.eth",
        "node:bad-registrant.parent.eth",
        24,
    );
    malformed_registrant.registrant = Some("0x00000000000000000000000000000000000000GG".to_owned());

    let registrant_error = upsert_children_current_rows(database.pool(), &[malformed_registrant])
        .await
        .expect_err("malformed registrant address must be rejected");
    assert!(
        format!("{registrant_error:?}").contains("invalid registrant"),
        "unexpected registrant validation error: {registrant_error:?}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn children_current_requires_no_surface_rows_to_use_declared_label_provenance() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let parent_logical_name_id = "ens:parent.eth";

    upsert_name_surfaces(
        database.pool(),
        &[name_surface(
            parent_logical_name_id,
            "parent.eth",
            "node:parent.eth",
            25,
            CanonicalityState::Finalized,
        )],
    )
    .await?;

    let malformed = children_current_row(
        parent_logical_name_id,
        "ens:missing.parent.eth",
        "missing.parent.eth",
        "node:missing.parent.eth",
        26,
    );
    let error = upsert_children_current_rows(database.pool(), &[malformed])
        .await
        .expect_err("no-surface exact row without label provenance must be rejected");
    assert!(
        format!("{error:?}").contains("missing child surface"),
        "unexpected no-surface validation error: {error:?}"
    );

    let mut preimage_row = children_current_row(
        parent_logical_name_id,
        "ens:preimage.parent.eth",
        "preimage.parent.eth",
        "node:preimage.parent.eth",
        27,
    );
    preimage_row.provenance["label"] = json!({
        "labelhash": preimage_row.labelhash,
        "status": "known",
        "source": "label_preimage",
    });
    upsert_children_current_rows(database.pool(), &[preimage_row]).await?;

    let labelhash = labelhash_for_label("unknown");
    let placeholder = format!("[{}].parent.eth", labelhash.trim_start_matches("0x"));
    let mut unknown_row = children_current_row(
        parent_logical_name_id,
        &format!("ens:{placeholder}"),
        &placeholder,
        "node:unknown.parent.eth",
        28,
    );
    unknown_row.labelhash = Some(labelhash);
    unknown_row.provenance["label"] = json!({
        "labelhash": unknown_row.labelhash,
        "status": "unknown",
        "source": "unknown",
    });
    upsert_children_current_rows(database.pool(), &[unknown_row]).await?;

    database.cleanup().await
}

#[tokio::test]
async fn children_current_page_uses_keyset_cursor_and_full_filter_summary() -> Result<()> {
    let database = TestDatabase::new().await?;
    let parent_logical_name_id = "ens:parent.eth";

    upsert_name_surfaces(
        database.pool(),
        &[
            name_surface(
                parent_logical_name_id,
                "parent.eth",
                "node:parent.eth",
                30,
                CanonicalityState::Finalized,
            ),
            name_surface(
                "ens:alice.parent.eth",
                "alice.parent.eth",
                "node:alice.parent.eth",
                31,
                CanonicalityState::Finalized,
            ),
            name_surface(
                "ens:bob.parent.eth",
                "bob.parent.eth",
                "node:bob.parent.eth",
                32,
                CanonicalityState::Finalized,
            ),
            name_surface(
                "ens:carla.parent.eth",
                "carla.parent.eth",
                "node:carla.parent.eth",
                33,
                CanonicalityState::Finalized,
            ),
            name_surface(
                "ens:zara.parent.eth",
                "zara.parent.eth",
                "node:zara.parent.eth",
                34,
                CanonicalityState::Observed,
            ),
        ],
    )
    .await?;

    let alice = children_current_row(
        parent_logical_name_id,
        "ens:alice.parent.eth",
        "alice.parent.eth",
        "node:alice.parent.eth",
        31,
    );
    let bob = children_current_row(
        parent_logical_name_id,
        "ens:bob.parent.eth",
        "bob.parent.eth",
        "node:bob.parent.eth",
        32,
    );
    let carla = children_current_row(
        parent_logical_name_id,
        "ens:carla.parent.eth",
        "carla.parent.eth",
        "node:carla.parent.eth",
        33,
    );
    let zara_observed = children_current_row(
        parent_logical_name_id,
        "ens:zara.parent.eth",
        "zara.parent.eth",
        "node:zara.parent.eth",
        34,
    );
    upsert_children_current_rows(
        database.pool(),
        &[carla.clone(), zara_observed, bob.clone(), alice.clone()],
    )
    .await?;

    let first_page =
        load_children_current_page(database.pool(), parent_logical_name_id, None, 2).await?;
    assert_eq!(first_page.rows, vec![alice.clone(), bob.clone()]);
    assert_eq!(
        first_page.next_cursor,
        Some(ChildrenCurrentKeysetCursor::from(&bob))
    );
    assert_eq!(
        first_page.summary.parent_logical_name_id,
        parent_logical_name_id
    );
    assert_eq!(first_page.summary.child_count, 3);
    assert_eq!(
        first_page.summary.provenance_inputs,
        vec![
            alice.provenance.clone(),
            bob.provenance.clone(),
            carla.provenance.clone()
        ]
    );
    assert_eq!(
        first_page.summary.chain_positions,
        vec![
            alice.chain_positions.clone(),
            bob.chain_positions.clone(),
            carla.chain_positions.clone()
        ]
    );
    assert_eq!(
        first_page.summary.canonicality_summaries,
        vec![
            alice.canonicality_summary.clone(),
            bob.canonicality_summary.clone(),
            carla.canonicality_summary.clone()
        ]
    );
    assert_eq!(
        first_page.summary.last_recomputed_at,
        Some(carla.last_recomputed_at)
    );

    let cursor = ChildrenCurrentKeysetCursor {
        canonical_display_name: bob.canonical_display_name.clone(),
        child_logical_name_id: bob.child_logical_name_id.clone(),
    };
    let second_page =
        load_children_current_page(database.pool(), parent_logical_name_id, Some(&cursor), 2)
            .await?;
    assert_eq!(second_page.rows, vec![carla.clone()]);
    assert_eq!(second_page.next_cursor, None);
    assert_eq!(second_page.summary, first_page.summary);
    assert_eq!(
        load_children_current(database.pool(), parent_logical_name_id).await?,
        vec![alice, bob, carla]
    );

    database.cleanup().await
}

#[tokio::test]
async fn children_current_batch_summaries_preserve_order_and_zero_counts() -> Result<()> {
    let database = TestDatabase::new().await?;
    let parent_a = "ens:alpha.eth";
    let parent_b = "ens:beta.eth";
    let missing_parent = "ens:missing.eth";

    upsert_name_surfaces(
        database.pool(),
        &[
            name_surface(
                parent_a,
                "alpha.eth",
                "node:alpha.eth",
                40,
                CanonicalityState::Finalized,
            ),
            name_surface(
                parent_b,
                "beta.eth",
                "node:beta.eth",
                41,
                CanonicalityState::Finalized,
            ),
            name_surface(
                "ens:one.alpha.eth",
                "one.alpha.eth",
                "node:one.alpha.eth",
                42,
                CanonicalityState::Finalized,
            ),
            name_surface(
                "ens:two.alpha.eth",
                "two.alpha.eth",
                "node:two.alpha.eth",
                43,
                CanonicalityState::Finalized,
            ),
            name_surface(
                "ens:draft.beta.eth",
                "draft.beta.eth",
                "node:draft.beta.eth",
                44,
                CanonicalityState::Observed,
            ),
        ],
    )
    .await?;

    let alpha_one = children_current_row(
        parent_a,
        "ens:one.alpha.eth",
        "one.alpha.eth",
        "node:one.alpha.eth",
        42,
    );
    let alpha_two = children_current_row(
        parent_a,
        "ens:two.alpha.eth",
        "two.alpha.eth",
        "node:two.alpha.eth",
        43,
    );
    let beta_observed = children_current_row(
        parent_b,
        "ens:draft.beta.eth",
        "draft.beta.eth",
        "node:draft.beta.eth",
        44,
    );
    upsert_children_current_rows(
        database.pool(),
        &[alpha_two.clone(), beta_observed, alpha_one.clone()],
    )
    .await?;

    let summaries = load_children_current_summaries(
        database.pool(),
        &[
            parent_b.to_owned(),
            parent_a.to_owned(),
            missing_parent.to_owned(),
        ],
    )
    .await?;

    assert_eq!(summaries.len(), 3);
    assert_eq!(summaries[0].parent_logical_name_id, parent_b);
    assert_eq!(summaries[0].child_count, 0);
    assert!(summaries[0].provenance_inputs.is_empty());
    assert!(summaries[0].chain_positions.is_empty());
    assert!(summaries[0].canonicality_summaries.is_empty());
    assert_eq!(summaries[0].last_recomputed_at, None);

    assert_eq!(summaries[1].parent_logical_name_id, parent_a);
    assert_eq!(summaries[1].child_count, 2);
    assert_eq!(
        summaries[1].provenance_inputs,
        vec![alpha_one.provenance.clone(), alpha_two.provenance.clone()]
    );
    assert_eq!(
        summaries[1].chain_positions,
        vec![
            alpha_one.chain_positions.clone(),
            alpha_two.chain_positions.clone()
        ]
    );
    assert_eq!(
        summaries[1].canonicality_summaries,
        vec![
            alpha_one.canonicality_summary.clone(),
            alpha_two.canonicality_summary.clone()
        ]
    );
    assert_eq!(
        summaries[1].last_recomputed_at,
        Some(alpha_two.last_recomputed_at)
    );

    assert_eq!(summaries[2].parent_logical_name_id, missing_parent);
    assert_eq!(summaries[2].child_count, 0);
    assert!(summaries[2].provenance_inputs.is_empty());
    assert!(summaries[2].chain_positions.is_empty());
    assert!(summaries[2].canonicality_summaries.is_empty());
    assert_eq!(summaries[2].last_recomputed_at, None);

    database.cleanup().await
}

#[tokio::test]
async fn children_current_declared_child_sources_filter_noncanonical_events_and_reassignments()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let parent_a = "ens:parent.eth";
    let parent_b = "ens:other.eth";
    let child_alice = "ens:alice.parent.eth";
    let child_bob = "ens:bob.parent.eth";
    let child_carla = "ens:carla.parent.eth";

    upsert_name_surfaces(
        database.pool(),
        &[
            name_surface(
                parent_a,
                "parent.eth",
                "node:parent.eth",
                30,
                CanonicalityState::Finalized,
            ),
            name_surface(
                parent_b,
                "other.eth",
                "node:other.eth",
                31,
                CanonicalityState::Finalized,
            ),
            name_surface(
                child_alice,
                "alice.parent.eth",
                "node:alice.parent.eth",
                32,
                CanonicalityState::Finalized,
            ),
            name_surface(
                child_bob,
                "bob.parent.eth",
                "node:bob.parent.eth",
                33,
                CanonicalityState::Finalized,
            ),
            name_surface(
                child_carla,
                "carla.parent.eth",
                "node:carla.parent.eth",
                34,
                CanonicalityState::Observed,
            ),
        ],
    )
    .await?;

    upsert_normalized_events(
        database.pool(),
        &[
            subregistry_event(SubregistryEventSeed {
                event_identity: "alice-parent-a",
                namespace: "ens",
                source_family: ENSV1_SUBREGISTRY_SOURCE_FAMILY,
                chain_id: "ethereum-mainnet",
                parent_namehash: "node:parent.eth",
                child_namehash: "node:alice.parent.eth",
                block_number: 100,
                log_index: 0,
                canonicality_state: CanonicalityState::Finalized,
                tombstone: false,
                active_edge: true,
            }),
            subregistry_event(SubregistryEventSeed {
                event_identity: "alice-parent-b",
                namespace: "ens",
                source_family: ENSV1_SUBREGISTRY_SOURCE_FAMILY,
                chain_id: "ethereum-mainnet",
                parent_namehash: "node:other.eth",
                child_namehash: "node:alice.parent.eth",
                block_number: 101,
                log_index: 0,
                canonicality_state: CanonicalityState::Finalized,
                tombstone: false,
                active_edge: true,
            }),
            subregistry_event(SubregistryEventSeed {
                event_identity: "bob-observed",
                namespace: "ens",
                source_family: ENSV1_SUBREGISTRY_SOURCE_FAMILY,
                chain_id: "ethereum-mainnet",
                parent_namehash: "node:other.eth",
                child_namehash: "node:bob.parent.eth",
                block_number: 102,
                log_index: 0,
                canonicality_state: CanonicalityState::Observed,
                tombstone: false,
                active_edge: true,
            }),
            subregistry_event(SubregistryEventSeed {
                event_identity: "carla-finalized",
                namespace: "ens",
                source_family: ENSV1_SUBREGISTRY_SOURCE_FAMILY,
                chain_id: "ethereum-mainnet",
                parent_namehash: "node:other.eth",
                child_namehash: "node:carla.parent.eth",
                block_number: 103,
                log_index: 0,
                canonicality_state: CanonicalityState::Finalized,
                tombstone: false,
                active_edge: true,
            }),
            subregistry_event(SubregistryEventSeed {
                event_identity: "alice-orphaned",
                namespace: "ens",
                source_family: ENSV1_SUBREGISTRY_SOURCE_FAMILY,
                chain_id: "ethereum-mainnet",
                parent_namehash: "node:parent.eth",
                child_namehash: "node:alice.parent.eth",
                block_number: 104,
                log_index: 0,
                canonicality_state: CanonicalityState::Orphaned,
                tombstone: false,
                active_edge: true,
            }),
        ],
    )
    .await?;

    assert!(
        load_canonical_ens_v1_declared_child_sources(database.pool(), Some(parent_a))
            .await?
            .is_empty()
    );

    let current =
        load_canonical_ens_v1_declared_child_sources(database.pool(), Some(parent_b)).await?;
    assert_eq!(current.len(), 2);
    let alice = current
        .iter()
        .find(|source| source.child_logical_name_id == child_alice)
        .expect("alice child should stay visible");
    assert_eq!(alice.parent_logical_name_id, parent_b);
    assert_eq!(alice.event_identity, "alice-parent-b");

    let carla = current
        .iter()
        .find(|source| source.event_identity == "carla-finalized")
        .expect("child edge with noncanonical child surface should stay visible");
    let carla_labelhash = labelhash_for_child_namehash("node:carla.parent.eth");
    assert_eq!(carla.child_logical_name_id, "ens:carla.other.eth");
    assert_eq!(carla.labelhash, Some(carla_labelhash));

    database.cleanup().await
}

#[tokio::test]
async fn children_current_declared_child_sources_dedupe_pairs_across_child_nodes() -> Result<()> {
    // Distinct child_node values can resolve to the same (parent, child)
    // logical pair: live, an unknown label's bracketed-labelhash fallback name
    // collided with a later genuine registration of that literal bracket string
    // as a label (2026-07-08 full rebuild, 3 ens L1 pairs). This fixture forces
    // the same collision class through the fallback branch alone (two child
    // nodes constructing one bracket name); the dedup under test is
    // mechanism-agnostic — it must emit exactly one row per pair, the newest,
    // or the children_current publish collides on its primary key.
    let database = TestDatabase::new().await?;
    let parent = "ens:parent.eth";

    upsert_name_surfaces(
        database.pool(),
        &[name_surface(
            parent,
            "parent.eth",
            "node:parent.eth",
            30,
            CanonicalityState::Finalized,
        )],
    )
    .await?;

    // Same first label ("dup") in both child nodes -> identical labelhash and
    // an identical constructed child_logical_name_id; the distinct child_node
    // strings keep both alive under the per-child-node ranking alone.
    upsert_normalized_events(
        database.pool(),
        &[
            subregistry_event(SubregistryEventSeed {
                event_identity: "dup-child-v1-node",
                namespace: "ens",
                source_family: ENSV1_SUBREGISTRY_SOURCE_FAMILY,
                chain_id: "ethereum-mainnet",
                parent_namehash: "node:parent.eth",
                child_namehash: "node:dup.parent.eth",
                block_number: 100,
                log_index: 0,
                canonicality_state: CanonicalityState::Finalized,
                tombstone: false,
                active_edge: true,
            }),
            subregistry_event(SubregistryEventSeed {
                event_identity: "dup-child-variant-node",
                namespace: "ens",
                source_family: ENSV1_SUBREGISTRY_SOURCE_FAMILY,
                chain_id: "ethereum-mainnet",
                parent_namehash: "node:parent.eth",
                child_namehash: "node:dup.parent.eth.v2",
                block_number: 101,
                log_index: 0,
                canonicality_state: CanonicalityState::Finalized,
                tombstone: false,
                active_edge: true,
            }),
        ],
    )
    .await?;

    let current =
        load_canonical_ens_v1_declared_child_sources(database.pool(), Some(parent)).await?;
    assert_eq!(
        current.len(),
        1,
        "same (parent, child) pair from distinct child nodes must dedupe: {current:#?}"
    );
    let source = &current[0];
    assert_eq!(source.parent_logical_name_id, parent);
    assert_eq!(
        source.event_identity, "dup-child-variant-node",
        "the newest event must win the pair"
    );
    let expected_labelhash = labelhash_for_child_namehash("node:dup.parent.eth");
    assert_eq!(source.labelhash, Some(expected_labelhash));

    database.cleanup().await
}

#[tokio::test]
async fn children_current_parent_surface_upsert_invalidates_retained_registry_edges() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let parent = "ens:parent.eth";
    let child_node = "node:mystery.parent.eth";

    upsert_label_preimages(
        database.pool(),
        &[label_preimage_from_label(
            "mystery",
            "children_current_test",
            100,
            json!({"source": "children_current_test"}),
        )?],
    )
    .await?;
    upsert_normalized_events(
        database.pool(),
        &[subregistry_event(SubregistryEventSeed {
            event_identity: "mystery-before-parent",
            namespace: "ens",
            source_family: ENSV1_SUBREGISTRY_SOURCE_FAMILY,
            chain_id: "ethereum-mainnet",
            parent_namehash: "node:parent.eth",
            child_namehash: child_node,
            block_number: 105,
            log_index: 0,
            canonicality_state: CanonicalityState::Finalized,
            tombstone: false,
            active_edge: true,
        })],
    )
    .await?;

    let invalidation_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM projection_invalidations
        WHERE projection = 'children_current'
          AND projection_key = $1
        "#,
    )
    .bind(parent)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(invalidation_count, 0);

    upsert_name_surfaces(
        database.pool(),
        &[name_surface(
            parent,
            "parent.eth",
            "node:parent.eth",
            35,
            CanonicalityState::Finalized,
        )],
    )
    .await?;

    let invalidation_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM projection_invalidations
        WHERE projection = 'children_current'
          AND projection_key = $1
        "#,
    )
    .bind(parent)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(invalidation_count, 1);

    let sources =
        load_canonical_ens_v1_declared_child_sources(database.pool(), Some(parent)).await?;
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0].child_logical_name_id, "ens:mystery.parent.eth");

    database.cleanup().await
}

#[tokio::test]
async fn children_current_declared_child_sources_ignore_events_without_child_node() -> Result<()> {
    let database = TestDatabase::new().await?;
    let parent = "ens:parent.eth";

    upsert_name_surfaces(
        database.pool(),
        &[name_surface(
            parent,
            "parent.eth",
            "node:parent.eth",
            36,
            CanonicalityState::Finalized,
        )],
    )
    .await?;

    let mut missing_child_node = subregistry_event(SubregistryEventSeed {
        event_identity: "missing-child-node",
        namespace: "ens",
        source_family: ENSV1_SUBREGISTRY_SOURCE_FAMILY,
        chain_id: "ethereum-mainnet",
        parent_namehash: "node:parent.eth",
        child_namehash: "node:mystery.parent.eth",
        block_number: 106,
        log_index: 0,
        canonicality_state: CanonicalityState::Finalized,
        tombstone: false,
        active_edge: true,
    });
    missing_child_node
        .after_state
        .as_object_mut()
        .expect("test event after_state must be an object")
        .remove("child_node");

    upsert_normalized_events(database.pool(), &[missing_child_node]).await?;

    let sources = load_canonical_ens_v1_declared_child_sources(database.pool(), None).await?;
    assert!(sources.is_empty());

    database.cleanup().await
}

#[tokio::test]
async fn children_current_declared_child_sources_include_basenames_base_registry() -> Result<()> {
    let database = TestDatabase::new().await?;
    let parent = "basenames:base.eth";
    let child = "basenames:alice.base.eth";
    let colliding_ens_parent = "ens:base.eth";
    let colliding_ens_child = "ens:alice.base.eth";

    upsert_name_surfaces(
        database.pool(),
        &[
            name_surface_on_chain(
                colliding_ens_parent,
                "base.eth",
                "node:base.eth",
                "ethereum-mainnet",
                39,
                CanonicalityState::Finalized,
            ),
            name_surface_on_chain(
                parent,
                "base.eth",
                "node:base.eth",
                "base-mainnet",
                40,
                CanonicalityState::Finalized,
            ),
            name_surface_on_chain(
                colliding_ens_child,
                "alice.base.eth",
                "node:alice.base.eth",
                "ethereum-mainnet",
                40,
                CanonicalityState::Finalized,
            ),
            name_surface_on_chain(
                child,
                "alice.base.eth",
                "node:alice.base.eth",
                "base-mainnet",
                41,
                CanonicalityState::Finalized,
            ),
        ],
    )
    .await?;

    upsert_normalized_events(
        database.pool(),
        &[
            subregistry_event(SubregistryEventSeed {
                event_identity: "alice-base-registry",
                namespace: "basenames",
                source_family: BASENAMES_BASE_SUBREGISTRY_SOURCE_FAMILY,
                chain_id: "base-mainnet",
                parent_namehash: "node:base.eth",
                child_namehash: "node:alice.base.eth",
                block_number: 200,
                log_index: 0,
                canonicality_state: CanonicalityState::Finalized,
                tombstone: false,
                active_edge: true,
            }),
            subregistry_event(SubregistryEventSeed {
                event_identity: "alice-base-primary",
                namespace: "basenames",
                source_family: "basenames_base_primary",
                chain_id: "base-mainnet",
                parent_namehash: "node:base.eth",
                child_namehash: "node:alice.base.eth",
                block_number: 201,
                log_index: 0,
                canonicality_state: CanonicalityState::Finalized,
                tombstone: false,
                active_edge: true,
            }),
        ],
    )
    .await?;

    assert!(
        load_canonical_declared_child_sources(database.pool(), Some(colliding_ens_parent))
            .await?
            .is_empty()
    );

    let current = load_canonical_declared_child_sources(database.pool(), Some(parent)).await?;
    assert_eq!(current.len(), 1);
    assert_eq!(current[0].parent_logical_name_id, parent);
    assert_eq!(current[0].child_logical_name_id, child);
    assert_eq!(
        current[0].source_family,
        BASENAMES_BASE_SUBREGISTRY_SOURCE_FAMILY
    );
    assert_eq!(current[0].namespace, "basenames");
    assert_eq!(current[0].chain_id, "base-mainnet");
    assert_eq!(current[0].event_identity, "alice-base-registry");

    database.cleanup().await
}

#[tokio::test]
async fn children_current_declared_child_sources_use_unknown_label_placeholders_for_basenames()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let parent = "basenames:base.eth";
    let child_namehash = "node:mystery.base.eth";
    let labelhash = labelhash_for_child_namehash(child_namehash);
    let placeholder = format!("[{}].base.eth", labelhash.trim_start_matches("0x"));

    upsert_name_surfaces(
        database.pool(),
        &[name_surface_on_chain(
            parent,
            "base.eth",
            "node:base.eth",
            "base-mainnet",
            40,
            CanonicalityState::Finalized,
        )],
    )
    .await?;
    upsert_normalized_events(
        database.pool(),
        &[subregistry_event(SubregistryEventSeed {
            event_identity: "mystery-base-registry",
            namespace: "basenames",
            source_family: BASENAMES_BASE_SUBREGISTRY_SOURCE_FAMILY,
            chain_id: "base-mainnet",
            parent_namehash: "node:base.eth",
            child_namehash,
            block_number: 210,
            log_index: 0,
            canonicality_state: CanonicalityState::Finalized,
            tombstone: false,
            active_edge: true,
        })],
    )
    .await?;

    let current = load_canonical_declared_child_sources(database.pool(), Some(parent)).await?;
    assert_eq!(current.len(), 1);
    assert_eq!(current[0].parent_logical_name_id, parent);
    assert_eq!(
        current[0].child_logical_name_id,
        format!("basenames:{placeholder}")
    );
    assert_eq!(current[0].normalized_name, placeholder);
    assert_eq!(current[0].labelhash, Some(labelhash));
    assert_eq!(
        current[0].source_family,
        BASENAMES_BASE_SUBREGISTRY_SOURCE_FAMILY
    );

    database.cleanup().await
}

#[tokio::test]
async fn children_current_declared_child_sources_rank_child_nodes_per_namespace_and_chain()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let ens_parent = "ens:parent.eth";
    let basenames_parent = "basenames:base.eth";
    let shared_child_node = "node:shared-child";

    upsert_name_surfaces(
        database.pool(),
        &[
            name_surface_on_chain(
                ens_parent,
                "parent.eth",
                "node:parent.eth",
                "ethereum-mainnet",
                70,
                CanonicalityState::Finalized,
            ),
            name_surface_on_chain(
                basenames_parent,
                "base.eth",
                "node:base.eth",
                "base-mainnet",
                71,
                CanonicalityState::Finalized,
            ),
        ],
    )
    .await?;

    upsert_normalized_events(
        database.pool(),
        &[
            subregistry_event(SubregistryEventSeed {
                event_identity: "ens-shared-child",
                namespace: "ens",
                source_family: ENSV1_SUBREGISTRY_SOURCE_FAMILY,
                chain_id: "ethereum-mainnet",
                parent_namehash: "node:parent.eth",
                child_namehash: shared_child_node,
                block_number: 200,
                log_index: 0,
                canonicality_state: CanonicalityState::Finalized,
                tombstone: false,
                active_edge: true,
            }),
            subregistry_event(SubregistryEventSeed {
                event_identity: "basenames-shared-child",
                namespace: "basenames",
                source_family: BASENAMES_BASE_SUBREGISTRY_SOURCE_FAMILY,
                chain_id: "base-mainnet",
                parent_namehash: "node:base.eth",
                child_namehash: shared_child_node,
                block_number: 201,
                log_index: 0,
                canonicality_state: CanonicalityState::Finalized,
                tombstone: false,
                active_edge: true,
            }),
        ],
    )
    .await?;

    let current = load_canonical_declared_child_sources(database.pool(), Some(ens_parent)).await?;

    assert_eq!(current.len(), 1);
    assert_eq!(current[0].event_identity, "ens-shared-child");
    assert_eq!(current[0].namespace, "ens");

    database.cleanup().await
}

#[tokio::test]
async fn children_current_declared_child_sources_include_ensv2_linked_subregistry_graph_and_reject_registry_mismatch()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let parent = "ens:alice.eth";
    let child = "ens:bob.alice.eth";
    let wrong_registry_child = "ens:eve.alice.eth";
    let released_child = "ens:carol.alice.eth";
    let parent_registry = "00000000-0000-0000-0000-0000000000aa";
    let child_registry = "00000000-0000-0000-0000-0000000000bb";
    let child_registry_address = "0x00000000000000000000000000000000000000bb";

    upsert_name_surfaces(
        database.pool(),
        &[
            name_surface_on_chain(
                parent,
                "alice.eth",
                "node:alice.eth",
                "ethereum-sepolia",
                50,
                CanonicalityState::Finalized,
            ),
            name_surface_on_chain(
                child,
                "bob.alice.eth",
                "node:bob.alice.eth",
                "ethereum-sepolia",
                51,
                CanonicalityState::Finalized,
            ),
            name_surface_on_chain(
                wrong_registry_child,
                "eve.alice.eth",
                "node:eve.alice.eth",
                "ethereum-sepolia",
                52,
                CanonicalityState::Finalized,
            ),
            name_surface_on_chain(
                released_child,
                "carol.alice.eth",
                "node:carol.alice.eth",
                "ethereum-sepolia",
                53,
                CanonicalityState::Finalized,
            ),
        ],
    )
    .await?;

    upsert_normalized_events(
        database.pool(),
        &[
            ensv2_subregistry_event(
                "ensv2-subregistry-active",
                parent,
                parent_registry,
                Some(child_registry),
                300,
                0,
            ),
            ensv2_parent_event(
                "ensv2-parent-active",
                "alice.eth",
                parent_registry,
                child_registry,
                child_registry_address,
                301,
                0,
            ),
            ensv2_registration_event(
                "ensv2-bob-registered",
                child,
                REGISTRATION_GRANTED_EVENT_KIND,
                child_registry,
                child_registry_address,
                302,
                0,
            ),
            ensv2_registration_event(
                "ensv2-eve-wrong-registry",
                wrong_registry_child,
                REGISTRATION_GRANTED_EVENT_KIND,
                "00000000-0000-0000-0000-0000000000cc",
                child_registry_address,
                303,
                0,
            ),
            ensv2_registration_event(
                "ensv2-carol-registered",
                released_child,
                REGISTRATION_GRANTED_EVENT_KIND,
                child_registry,
                child_registry_address,
                304,
                0,
            ),
            ensv2_registration_event(
                "ensv2-carol-released",
                released_child,
                REGISTRATION_RELEASED_EVENT_KIND,
                child_registry,
                child_registry_address,
                305,
                0,
            ),
        ],
    )
    .await?;

    let current = load_canonical_declared_child_sources(database.pool(), Some(parent)).await?;
    assert_eq!(current.len(), 1);
    assert_eq!(current[0].parent_logical_name_id, parent);
    assert_eq!(current[0].child_logical_name_id, child);
    assert!(
        current
            .iter()
            .all(|source| source.child_logical_name_id != wrong_registry_child),
        "registration with matching raw emitting_address but mismatched registry_contract_instance_id must be rejected"
    );
    assert_eq!(current[0].event_identity, "ensv2-bob-registered");
    assert_eq!(current[0].source_family, ENSV2_REGISTRY_SOURCE_FAMILY);
    assert_eq!(current[0].manifest_version, 3);
    assert_eq!(
        current[0].manifest_versions,
        json!([
            {
                "source_manifest_id": null,
                "source_family": ENSV2_REGISTRY_SOURCE_FAMILY,
                "manifest_version": 3
            },
            {
                "source_manifest_id": null,
                "source_family": ENSV2_ROOT_SOURCE_FAMILY,
                "manifest_version": 2
            }
        ])
    );
    assert_eq!(current[0].chain_id, "ethereum-sepolia");
    assert_eq!(current[0].normalized_event_ids.len(), 3);
    assert_eq!(current[0].raw_fact_refs.as_array().map(Vec::len), Some(3));

    database.cleanup().await
}
