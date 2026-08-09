#[allow(dead_code)]
mod support;

use std::{sync::Arc, time::Duration};

use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use anyhow::Result;
use bigname_interpret::{
    BatchRequest as InterpretBatchRequest, Engine as InterpretEngine,
    NORMALIZATION_STATE_REPAIR_REASON, RunMode as InterpretRunMode, finalize_recompute_flags,
};
use bigname_project::{BatchRequest, Engine, Marker, RunMode};
use bigname_storage::{
    ENS_RAINBOW_SOURCE_KIND, ens_namehash_label_bytes, import_label_preimages_from_ens_names_table,
    load_children_current_page,
};
use phase_runner::{
    INTERPRETER_CONTENT_HASH,
    capacity::CapacityGuard,
    config::{CapacityConfig, ChainConfig, SeedBasis, SourceConfig, TimingConfig},
    interpret_phase::InterpretPhase,
    phase::{BlockRange, LoopbackPhase, PhaseName, PhaseSet},
    project_phase::ProjectPhase,
    runner::{PhaseRunner, RedoPhase},
    state::PhaseStore,
};
use serde_json::json;
use sqlx::{PgPool, types::Uuid};
use tokio_util::sync::CancellationToken;

use support::ScratchDatabase;

sol! {
    event NameRegistered(
        string name,
        bytes32 indexed label,
        address indexed owner,
        uint256 expires
    );
}

const CHAIN: &str = "rainbow-fixture";
const OWNER: &str = "0x00000000000000000000000000000000000000a1";
const REGISTRAR: &str = "0x00000000000000000000000000000000000000b2";
const SENDER: &str = "0x00000000000000000000000000000000000000c3";
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

    run_project(scratch.pool(), None, RunMode::Normal, 0, 3).await?;
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

    run_project(scratch.pool(), None, RunMode::Redo, 0, 3).await?;
    // The proof-checked "Alice" row keeps its raw bytes and honest verdict in the store, but
    // its text fails normalization, so serving must not attach it to the raw-byte node: the
    // row serves the same placeholder as an unobserved label.
    assert_eq!(
        child_display_names(scratch.pool()).await?,
        sorted(vec![
            placeholder("Alice"),
            "alice.eth".to_owned(),
            placeholder("mallory"),
        ])
    );
    let gated: (Option<Vec<u8>>, Option<String>, Vec<u8>, Option<String>) = sqlx::query_as(
        "SELECT raw_name, decoded_name, raw_label, decoded_label
         FROM children_current WHERE labelhash = $1",
    )
    .bind(labelhash_hex("Alice"))
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(gated, (None, None, b"Alice".to_vec(), None));
    scratch.cleanup().await
}

#[tokio::test]
async fn surface_less_verdict_flip_serves_stale_text_until_full_range_project_redo() -> Result<()> {
    let scratch = ScratchDatabase::create("production_surface_less_verdict_flip").await?;
    seed_children_fixture(scratch.pool(), &["alice"]).await?;
    seed_ens_names(scratch.pool(), &[("alice", "alice")]).await?;
    let summary = import_label_preimages_from_ens_names_table(scratch.pool(), None, None).await?;
    assert_eq!(summary.retained_row_count, 1);

    run_project(scratch.pool(), None, RunMode::Normal, 0, 3).await?;
    assert_eq!(
        child_display_names(scratch.pool()).await?,
        vec!["alice.eth".to_owned()]
    );

    // A normalizer bump can flip the label's verdict; recompute-flags rewrites the verdict
    // columns unconditionally with this UPDATE shape, so applying it directly simulates the
    // bumped normalizer's verdict for the same raw bytes.
    sqlx::query(
        "UPDATE label_preimages
         SET normalizer_version = $2,
             normalized_under_version = $3,
             normalization_error = $4
         WHERE labelhash = $1",
    )
    .bind(labelhash_hex("alice"))
    .bind("ensip15@ens-normalize-0.1.2")
    .bind(false)
    .bind(Some(
        "raw label is not byte-identical to its normalized form",
    ))
    .execute(scratch.pool())
    .await?;

    // The child is registry-event-only and has no name surface, so the flip produces no
    // visibility-class transition. The redo trigger keys on the summary's earliest transition
    // block, so no redo is stamped and the stale text keeps serving: the limitation the
    // deployment runbook's full-range redo requirement exists for.
    let mut transaction = scratch.pool().begin().await?;
    let recompute = finalize_recompute_flags(&mut transaction, CHAIN, 0, 3).await?;
    transaction.commit().await?;
    assert!(
        recompute.earliest_transition_block().is_none(),
        "a surface-less verdict flip must not report a transition: {recompute:?}"
    );
    assert_eq!(
        (
            recompute.same_class_names,
            recompute.shadow_to_active_names,
            recompute.active_to_shadow_names
        ),
        (1, 0, 0)
    );
    assert_eq!(
        child_display_names(scratch.pool()).await?,
        vec!["alice.eth".to_owned()]
    );

    run_project(scratch.pool(), None, RunMode::Redo, 0, 3).await?;
    assert_eq!(
        child_display_names(scratch.pool()).await?,
        vec![placeholder("alice")]
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn chain_observed_unnormalized_label_serves_the_placeholder() -> Result<()> {
    let scratch = ScratchDatabase::create("production_chain_observed_verdict_gate").await?;
    seed_children_fixture(scratch.pool(), &["Alice"]).await?;
    seed_registrar_manifest_and_log(scratch.pool(), "Alice").await?;

    // The interpreter's own chain-observation path stores the same honest shape the rainbow
    // import does: proven raw bytes, decoded text, verdict false.
    run_interpret(scratch.pool(), 0, 1).await?;
    let stored: (Option<String>, bool, Option<String>) = sqlx::query_as(
        "SELECT decoded_label, normalized_under_version, normalization_error
         FROM label_preimages WHERE labelhash = $1",
    )
    .bind(labelhash_hex("Alice"))
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(stored.0.as_deref(), Some("Alice"));
    assert!(!stored.1);
    assert!(stored.2.as_deref().is_some_and(|error| !error.is_empty()));

    // The projection join is shared with the rainbow path, so the same gate applies.
    run_project(scratch.pool(), None, RunMode::Normal, 0, 3).await?;
    assert_eq!(
        child_display_names(scratch.pool()).await?,
        vec![placeholder("Alice")]
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

#[tokio::test]
async fn rainbow_rows_are_reachable_by_recompute_flags_after_a_version_bump() -> Result<()> {
    let scratch = ScratchDatabase::create("production_rainbow_recompute_repair").await?;
    seed_registrar_fixture(scratch.pool(), "alice").await?;
    seed_ens_names(scratch.pool(), &[("alice", "alice")]).await?;
    let summary = import_label_preimages_from_ens_names_table(scratch.pool(), None, None).await?;
    assert_eq!(summary.retained_row_count, 1);

    // A normalizer-version bump lands after the import; rows written under the old version
    // are stale until recompute-flags re-derives their flags.
    sqlx::query(
        "UPDATE label_preimages SET normalizer_version = 'stale-version'
         WHERE decoded_label = 'alice'",
    )
    .execute(scratch.pool())
    .await?;

    // The chain's first observation of the label halts interpretation with the
    // recompute-flags repair instruction.
    let error = run_interpret(scratch.pool(), 0, 1)
        .await
        .expect_err("a stale rainbow preimage must wedge the interpret upsert");
    assert!(
        error
            .to_string()
            .contains(NORMALIZATION_STATE_REPAIR_REASON),
        "unexpected interpret error: {error:#}"
    );

    let store = PhaseStore::new(scratch.pool().clone());
    store.initialize_chain(CHAIN).await?;
    seed_completed_project_extent(scratch.pool(), 1).await?;
    let phases = PhaseSet::with_ingest_interpret_and_project(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(InterpretPhase::new(scratch.pool().clone())),
        Arc::new(ProjectPhase::new(scratch.pool().clone())),
    )?;
    PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "rainbow-recompute-repair",
        test_timing(),
    )?
    .redo(
        &chain_config()?,
        RedoPhase::RecomputeFlags,
        BlockRange::new(0, 1)?,
        CancellationToken::new(),
    )
    .await?;

    let repaired: (String, bool, Option<String>) = sqlx::query_as(
        "SELECT normalizer_version, normalized_under_version, normalization_error
         FROM label_preimages WHERE decoded_label = 'alice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(repaired, (NORMALIZER.into(), true, None));

    run_interpret(scratch.pool(), 0, 1).await?;
    let upgraded: (String, i32) = sqlx::query_as(
        "SELECT source_kind, source_priority FROM label_preimages WHERE decoded_label = 'alice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(upgraded.1, CHAIN_OBSERVED_PRIORITY);
    assert_ne!(upgraded.0, ENS_RAINBOW_SOURCE_KIND);
    scratch.cleanup().await
}

#[tokio::test]
async fn windowed_project_run_does_not_pick_up_a_newly_imported_preimage() -> Result<()> {
    let scratch = ScratchDatabase::create("production_rainbow_windowed_run").await?;
    seed_children_fixture(scratch.pool(), &["alice"]).await?;
    seed_lineage(scratch.pool(), 4, 5).await?;

    run_project(scratch.pool(), None, RunMode::Normal, 0, 3).await?;
    assert_eq!(
        child_display_names(scratch.pool()).await?,
        vec![placeholder("alice")]
    );

    seed_ens_names(scratch.pool(), &[("alice", "alice")]).await?;
    import_label_preimages_from_ens_names_table(scratch.pool(), None, None).await?;

    // A windowed catch-up run re-derives only names whose events fall inside the window, so
    // the imported preimage does not re-enter scope here; the documented repair is the
    // full-range redo below.
    let resume = Marker {
        number: 3,
        hash: block_hash(3),
    };
    run_project(scratch.pool(), Some(resume), RunMode::Normal, 4, 5).await?;
    assert_eq!(
        child_display_names(scratch.pool()).await?,
        vec![placeholder("alice")]
    );

    run_project(scratch.pool(), None, RunMode::Redo, 0, 5).await?;
    assert_eq!(
        child_display_names(scratch.pool()).await?,
        vec!["alice.eth".to_owned()]
    );
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

async fn run_project(
    pool: &PgPool,
    resume: Option<Marker>,
    mode: RunMode,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    let outcome = Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.into(),
            target_block: to_block,
            affected_from_block: from_block,
            affected_to_block: to_block,
            resume_current: resume,
            mode,
        })
        .await?;
    assert!(outcome.complete);
    Ok(())
}

async fn run_interpret(pool: &PgPool, from_block: i64, to_block: i64) -> Result<()> {
    let outcome = InterpretEngine::new(pool.clone())
        .run_batch(InterpretBatchRequest {
            chain_id: CHAIN.into(),
            from_block,
            to_block,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    assert!(outcome.complete);
    Ok(())
}

async fn seed_lineage(pool: &PgPool, from: i64, through: i64) -> Result<()> {
    for number in from..=through {
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
    seed_lineage(pool, 0, 3).await?;
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

async fn seed_registrar_fixture(pool: &PgPool, label: &str) -> Result<()> {
    seed_lineage(pool, 0, 1).await?;
    seed_registrar_manifest_and_log(pool, label).await
}

async fn seed_registrar_manifest_and_log(pool: &PgPool, label: &str) -> Result<()> {
    let contract_instance_id = Uuid::new_v4();
    sqlx::query("INSERT INTO contract_instances VALUES ($1, $2, 'contract', '{}'::jsonb, now())")
        .bind(contract_instance_id)
        .bind(CHAIN)
        .execute(pool)
        .await?;
    let payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v1_registrar_l1",
        "chain": CHAIN,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [{
            "role": "registrar",
            "address": REGISTRAR,
            "proxy_kind": "none",
            "implementation": null,
            "start_block": 0
        }],
        "discovery_rules": [],
        "abi": { "events": [{
            "name": "NameRegistered",
            "fragment": "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
            "emitter_roles": ["registrar"],
            "normalized_events": ["RegistrationGranted"]
        }], "calls": [] }
    });
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (
             manifest_version, namespace, source_family, chain_id, deployment_label,
             rollout_status, normalizer_version, file_path, manifest_payload
         ) VALUES (1, 'ens', 'ens_v1_registrar_l1', $1, 'fixture', 'active', $2, $3, $4)
         RETURNING manifest_id",
    )
    .bind(CHAIN)
    .bind(NORMALIZER)
    .bind(format!("tests/{CHAIN}-registrar.toml"))
    .bind(payload)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances (
             manifest_id, chain_id, declaration_kind, declaration_name,
             contract_instance_id, declared_address, role, proxy_kind, start_block_number
         ) VALUES ($1, $2, 'contract', 'registrar', $3, $4, 'registrar', 'none', 0)",
    )
    .bind(manifest_id)
    .bind(CHAIN)
    .bind(contract_instance_id)
    .bind(REGISTRAR)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (
             contract_instance_id, chain_id, address, active_from_block_number,
             source_manifest_id, provenance
         ) VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)",
    )
    .bind(contract_instance_id)
    .bind(CHAIN)
    .bind(REGISTRAR)
    .bind(manifest_id)
    .execute(pool)
    .await?;
    let transaction_hash = format!("{CHAIN}-transaction-1");
    sqlx::query(
        "INSERT INTO raw_transactions (
             chain_id, block_hash, block_number, transaction_hash, transaction_index,
             from_address, to_address
         ) VALUES ($1, $2, 1, $3, 0, $4, $5)",
    )
    .bind(CHAIN)
    .bind(block_hash(1))
    .bind(&transaction_hash)
    .bind(SENDER)
    .bind(REGISTRAR)
    .execute(pool)
    .await?;
    let encoded = NameRegistered {
        name: label.to_owned(),
        label: B256::from(keccak256(label.as_bytes())),
        owner: OWNER.parse::<Address>()?,
        expires: U256::from(1_000_000_u64),
    }
    .encode_log_data();
    let topics = encoded
        .topics()
        .iter()
        .map(|topic| format!("{topic:#x}"))
        .collect::<Vec<_>>();
    sqlx::query(
        "INSERT INTO raw_logs (
             chain_id, block_hash, block_number, transaction_hash, transaction_index,
             log_index, emitting_address, topics, data
         ) VALUES ($1, $2, 1, $3, 0, 0, $4, $5, $6)",
    )
    .bind(CHAIN)
    .bind(block_hash(1))
    .bind(&transaction_hash)
    .bind(REGISTRAR)
    .bind(topics)
    .bind(encoded.data.as_ref())
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_completed_project_extent(pool: &PgPool, head: i64) -> Result<()> {
    let hash = block_hash(head);
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed',
             current_block_number = $2,
             current_block_hash = $3,
             target_block_number = $2,
             target_block_hash = $3,
             live_handoff_block_number = CASE
                 WHEN phase_name = 'ingest' THEN $2
             END,
             live_handoff_block_hash = CASE
                 WHEN phase_name = 'ingest' THEN $3
             END,
             input_content_hash = CASE
                 WHEN phase_name IN ('interpret', 'project') THEN $4
             END,
             started_at = now(),
             finished_at = now()
         WHERE chain_id = $1
           AND phase_name IN ('ingest', 'interpret', 'project')",
    )
    .bind(CHAIN)
    .bind(head)
    .bind(&hash)
    .bind(INTERPRETER_CONTENT_HASH)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO ingest_cursors (
             chain_id, source_key, source_kind, seed_basis, start_block_number,
             next_block_number, target_block_number, last_processed_block_number,
             last_processed_block_hash
         ) VALUES ($1, 'source', 'test', 'new_signature_range', 0,
                   $2, $3, $3, $4)",
    )
    .bind(CHAIN)
    .bind(head.saturating_add(1))
    .bind(head)
    .bind(hash)
    .execute(pool)
    .await?;
    Ok(())
}

fn chain_config() -> phase_runner::error::RunnerResult<ChainConfig> {
    ChainConfig::new(
        CHAIN,
        vec![SourceConfig::new(
            CHAIN,
            "source",
            "test",
            SeedBasis::NewSignatureRange,
            0,
            "http://source.invalid",
        )?],
        false,
    )
}

fn test_timing() -> TimingConfig {
    TimingConfig {
        initial_backoff: Duration::from_millis(1),
        maximum_backoff: Duration::from_millis(4),
        live_poll_interval: Duration::from_millis(1),
    }
}

fn block_hash(number: i64) -> String {
    format!("{CHAIN}-block-{number}")
}
