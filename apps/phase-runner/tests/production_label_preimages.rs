#[allow(dead_code)]
mod support;

use alloy_primitives::keccak256;
use anyhow::Result;
use bigname_project::{BatchRequest, Engine, RunMode};
use bigname_storage::{
    ENS_RAINBOW_SOURCE_KIND, ens_namehash_label_bytes, import_label_preimages_from_ens_names_table,
    load_children_current_page,
};
use serde_json::json;
use sqlx::PgPool;

use support::ScratchDatabase;

const CHAIN: &str = "rainbow-fixture";
const OWNER: &str = "0x00000000000000000000000000000000000000a1";
const NORMALIZER: &str = "ensip15@ens-normalize-0.1.1";
const CHAIN_OBSERVED_PRIORITY: i32 = 100;

type PreimageRow = (String, Option<String>, bool, Option<String>, String, i32);
type ChainObservedRow = (
    String,
    i32,
    serde_json::Value,
    sqlx::types::time::OffsetDateTime,
    sqlx::types::time::OffsetDateTime,
);

#[tokio::test]
async fn rainbow_import_then_project_redo_serves_decoded_labels() -> Result<()> {
    let scratch = ScratchDatabase::create("production_rainbow_import_e2e").await?;
    seed_children_fixture(scratch.pool(), &["alice", "mallory", "Alice"]).await?;

    run_project(scratch.pool(), RunMode::Normal, 0, 3).await?;
    assert_eq!(
        child_display_names(scratch.pool()).await?,
        sorted(vec![
            placeholder("Alice"),
            placeholder("alice"),
            placeholder("mallory"),
        ])
    );

    seed_ens_names(
        scratch.pool(),
        &[
            ("alice", "alice"),
            // A rainbow row is untrusted input: this one claims "mallory"'s hash for "bob".
            ("mallory", "bob"),
            ("Alice", "Alice"),
        ],
    )
    .await?;
    let summary = import_label_preimages_from_ens_names_table(scratch.pool(), None, None).await?;
    assert_eq!(summary.scanned_row_count, 3);
    assert_eq!(summary.retained_row_count, 2);
    assert_eq!(summary.rejected_row_count, 1);

    let rows: Vec<PreimageRow> = sqlx::query_as(
        "SELECT labelhash, decoded_label, normalized_under_version, normalization_error,
                source_kind, source_priority
         FROM label_preimages ORDER BY decoded_label COLLATE \"C\"",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        rows,
        vec![
            (
                labelhash_hex("Alice"),
                Some("Alice".into()),
                false,
                Some("raw label is not byte-identical to its normalized form".into()),
                ENS_RAINBOW_SOURCE_KIND.into(),
                10
            ),
            (
                labelhash_hex("alice"),
                Some("alice".into()),
                true,
                None,
                ENS_RAINBOW_SOURCE_KIND.into(),
                10
            ),
        ]
    );
    let lying_row: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM label_preimages WHERE labelhash = $1)")
            .bind(labelhash_hex("mallory"))
            .fetch_one(scratch.pool())
            .await?;
    assert!(
        !lying_row,
        "the hash-mismatched candidate must leave no row"
    );

    run_project(scratch.pool(), RunMode::Redo, 0, 3).await?;
    assert_eq!(
        child_display_names(scratch.pool()).await?,
        sorted(vec![
            "Alice.eth".to_owned(),
            "alice.eth".to_owned(),
            placeholder("mallory"),
        ])
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn rainbow_import_rerun_is_a_no_op_and_preserves_chain_observed_rows() -> Result<()> {
    let scratch = ScratchDatabase::create("production_rainbow_import_rerun").await?;
    sqlx::query(
        "INSERT INTO label_preimages (
             labelhash, raw_label, decoded_label, normalizer_version,
             normalized_under_version, source_kind, source_priority, provenance
         ) VALUES ($1, convert_to('alice', 'UTF8'), 'alice', $2, true,
                   'chain_observed', $3, jsonb_build_object('source', 'normalized_event'))",
    )
    .bind(labelhash_hex("alice"))
    .bind(NORMALIZER)
    .bind(CHAIN_OBSERVED_PRIORITY)
    .execute(scratch.pool())
    .await?;
    seed_ens_names(scratch.pool(), &[("alice", "alice"), ("bob", "bob")]).await?;

    let first = import_label_preimages_from_ens_names_table(scratch.pool(), None, None).await?;
    assert_eq!(first.scanned_row_count, 2);
    assert_eq!(first.retained_row_count, 1);
    assert_eq!(first.rejected_row_count, 0);

    let alice_before: ChainObservedRow = sqlx::query_as(
        "SELECT source_kind, source_priority, provenance, observed_at, inserted_at
         FROM label_preimages WHERE labelhash = $1",
    )
    .bind(labelhash_hex("alice"))
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        alice_before.0, "chain_observed",
        "the import must not clobber a chain-observed preimage"
    );

    let second = import_label_preimages_from_ens_names_table(scratch.pool(), None, None).await?;
    assert_eq!(second.scanned_row_count, 2);
    assert_eq!(second.retained_row_count, 0);
    assert_eq!(second.rejected_row_count, 0);
    let alice_after: ChainObservedRow = sqlx::query_as(
        "SELECT source_kind, source_priority, provenance, observed_at, inserted_at
         FROM label_preimages WHERE labelhash = $1",
    )
    .bind(labelhash_hex("alice"))
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        alice_after, alice_before,
        "a re-run must leave an existing verified row untouched"
    );
    let stored: i64 = sqlx::query_scalar("SELECT count(*) FROM label_preimages")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(stored, 2);
    scratch.cleanup().await
}

#[tokio::test]
async fn rainbow_import_paginates_batches_and_honors_the_limit() -> Result<()> {
    let scratch = ScratchDatabase::create("production_rainbow_import_pages").await?;
    seed_ens_names(scratch.pool(), &[("aa", "aa"), ("bb", "bb"), ("cc", "cc")]).await?;

    let limited =
        import_label_preimages_from_ens_names_table(scratch.pool(), Some(1), Some(2)).await?;
    assert_eq!(limited.scanned_row_count, 2);
    assert_eq!(limited.retained_row_count, 2);
    assert_eq!(limited.rejected_row_count, 0);
    let stored_after_limit: i64 = sqlx::query_scalar("SELECT count(*) FROM label_preimages")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(stored_after_limit, 2);

    let remainder =
        import_label_preimages_from_ens_names_table(scratch.pool(), Some(1), None).await?;
    assert_eq!(remainder.scanned_row_count, 3);
    assert_eq!(remainder.retained_row_count, 1);
    assert_eq!(remainder.rejected_row_count, 0);
    let stored_after_full: i64 = sqlx::query_scalar("SELECT count(*) FROM label_preimages")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(stored_after_full, 3);
    scratch.cleanup().await
}

fn labelhash_hex(label: &str) -> String {
    format!("{:#x}", keccak256(label.as_bytes()))
}

fn namehash_hex(labels: &[&[u8]]) -> String {
    format!("{:#x}", ens_namehash_label_bytes(labels))
}

fn parent_logical_name_id() -> String {
    format!("ens:{}", namehash_hex(&[b"eth"]))
}

fn placeholder(label: &str) -> String {
    format!("[{}].eth", &labelhash_hex(label)[2..])
}

async fn child_display_names(pool: &PgPool) -> Result<Vec<String>> {
    let page = load_children_current_page(pool, &parent_logical_name_id(), None, 100).await?;
    Ok(sorted(
        page.rows
            .iter()
            .map(|row| row.canonical_display_name.clone())
            .collect(),
    ))
}

fn sorted(mut names: Vec<String>) -> Vec<String> {
    names.sort();
    names
}

async fn run_project(pool: &PgPool, mode: RunMode, from_block: i64, to_block: i64) -> Result<()> {
    let outcome = Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.into(),
            target_block: to_block,
            affected_from_block: from_block,
            affected_to_block: to_block,
            resume_current: None,
            mode,
        })
        .await?;
    assert!(outcome.complete);
    Ok(())
}

async fn seed_lineage(pool: &PgPool, through: i64) -> Result<()> {
    for number in 0..=through {
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, $4, to_timestamp($4), 'canonical')",
        )
        .bind(CHAIN)
        .bind(block_hash(number))
        .bind((number > 0).then(|| block_hash(number - 1)))
        .bind(number)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn seed_children_fixture(pool: &PgPool, labels: &[&str]) -> Result<()> {
    seed_lineage(pool, 3).await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version,
             visibility_state, chain_id, block_hash, block_number,
             canonicality_state
         ) VALUES ($1, 'ens', 'eth', ARRAY['eth'], decode('00', 'hex'), $2,
                   ARRAY[$3], $4, 'active', $5, $6, 1, 'canonical')",
    )
    .bind(parent_logical_name_id())
    .bind(namehash_hex(&[b"eth"]))
    .bind(labelhash_hex("eth"))
    .bind(NORMALIZER)
    .bind(CHAIN)
    .bind(block_hash(1))
    .execute(pool)
    .await?;
    for label in labels {
        sqlx::query(
            "INSERT INTO normalized_events (
                 event_identity, namespace, event_kind, source_family, manifest_version,
                 chain_id, block_number, block_hash, raw_fact_ref, derivation_kind,
                 canonicality_state, before_state, after_state
             ) VALUES ($1, 'ens', 'SubregistryChanged', 'ens_v1_registry_l1', 1, $2, 1, $3,
                       '{}'::jsonb, 'ens_v1_unwrapped_authority', 'canonical',
                       '{}'::jsonb, $4)",
        )
        .bind(format!("{CHAIN}:SubregistryChanged:{label}"))
        .bind(CHAIN)
        .bind(block_hash(1))
        .bind(json!({
            "node": namehash_hex(&[b"eth"]),
            "child_node": namehash_hex(&[label.as_bytes(), b"eth"]),
            "labelhash": labelhash_hex(label),
            "owner": OWNER
        }))
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn seed_ens_names(pool: &PgPool, rows: &[(&str, &str)]) -> Result<()> {
    for (hash_label, claimed_name) in rows {
        sqlx::query("INSERT INTO ens_names (hash, name) VALUES ($1, $2)")
            .bind(labelhash_hex(hash_label))
            .bind(claimed_name)
            .execute(pool)
            .await?;
    }
    Ok(())
}

fn block_hash(number: i64) -> String {
    format!("{CHAIN}-block-{number}")
}
