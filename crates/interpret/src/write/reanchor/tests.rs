//! Redo repair of a name surface's two provenances: the identity anchor, which a preimage or a
//! label-hash-path observation can hold, and the preimage witness of its raw labels.
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::PgPool;

use super::{NAME_IDENTITY_OBSERVED_KEY, PREIMAGE_OBSERVATION_EVENT_KIND};

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const CHAIN: &str = "ethereum";
const NAME: &str = "ens:0xchild";

async fn database(name: &str) -> TestResult<TestDatabase> {
    let database = TestDatabase::create(TestDatabaseConfig::new(name)).await?;
    database.create_phase_schema().await?;
    for sql in [
        include_str!("../../../../storage/schema/baseline/01_chain.sql"),
        include_str!("../../../../storage/schema/baseline/03_identity.sql"),
        include_str!("../../../../storage/schema/baseline/04_manifests.sql"),
        include_str!("../../../../storage/schema/baseline/05_normalized_events.sql"),
    ] {
        sqlx::raw_sql(sql).execute(database.pool()).await?;
    }
    sqlx::raw_sql(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) SELECT 'ethereum', '0x0' || number, number, to_timestamp(number), 'canonical'
           FROM generate_series(1, 5) number",
    )
    .execute(database.pool())
    .await?;
    Ok(database)
}

/// A surface row as the writer leaves it: `raw` chooses between a hash-path-only identity
/// and one whose raw labels the preimage event at `witness_block` carried.
async fn insert_surface(
    pool: &PgPool,
    anchor_block: i64,
    witness_block: Option<i64>,
    shadow: bool,
) -> TestResult {
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash,
             labelhashes, normalizer_version, visibility_state, deactivation_reason,
             deactivated_at, chain_id, block_hash, block_number, provenance,
             canonicality_state, preimage_event_identity
         ) SELECT $1, 'ens', raw.name, raw.labels, raw.dns, '0xchild',
                  ARRAY['0xchildlabel', '0xeth'], 'test',
                  CASE WHEN $4 THEN 'shadow' ELSE 'active' END,
                  CASE WHEN $4 THEN 'normalization_gate' END,
                  CASE WHEN $4 THEN to_timestamp($3) END,
                  'ethereum', '0x0' || $2, $2, jsonb_build_object('block', $2),
                  'canonical', 'preimage-' || $3
           FROM (VALUES (
               CASE WHEN $3 IS NOT NULL THEN 'child.eth' END,
               CASE WHEN $3 IS NOT NULL THEN ARRAY['child', 'eth'] END,
               CASE WHEN $3 IS NOT NULL THEN '\\x05'::bytea END
           )) raw(name, labels, dns)",
    )
    .bind(NAME)
    .bind(anchor_block)
    .bind(witness_block)
    .bind(shadow)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_event(
    pool: &PgPool,
    identity: &str,
    kind: &str,
    block: i64,
    after: Value,
) -> TestResult {
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, event_kind, source_family,
             manifest_version, chain_id, block_number, block_hash, transaction_hash, transaction_index,
             log_index, raw_fact_ref, derivation_kind, canonicality_state, after_state
         ) VALUES ($1, 'ens', $2, $3, 'ens_v1_registry_l1', 1, 'ethereum', $4, '0x0' || $4,
                   '0xtx' || $4, 0, 0, jsonb_build_object('event', $1), 'raw_log_preimage_observation', 'canonical', $5)",
    )
    .bind(identity)
    .bind(NAME)
    .bind(kind)
    .bind(block)
    .bind(after)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_preimage(pool: &PgPool, block: i64) -> TestResult {
    insert_event(
        pool,
        &format!("preimage-{block}"),
        PREIMAGE_OBSERVATION_EVENT_KIND,
        block,
        json!({"raw_name": "child.eth", "raw_labels": ["child", "eth"], "namehash": "0xchild"}),
    )
    .await
}

async fn insert_hash_path_observation(pool: &PgPool, block: i64) -> TestResult {
    insert_event(
        pool,
        &format!("new-owner-{block}"),
        "SubregistryChanged",
        block,
        json!({"source_event": "NewOwner", "child_node": "0xchild", (NAME_IDENTITY_OBSERVED_KEY): true}),
    )
    .await
}

/// The redo of `from..=to` when the replayed range re-observes nothing.
async fn redo_without_reobservation(pool: &PgPool, from: i64, to: i64) -> TestResult {
    let mut transaction = pool.begin().await?;
    super::super::prepare_redo_range(&mut transaction, CHAIN, from, to).await?;
    super::stable_identities(&mut transaction, CHAIN, from, to).await?;
    transaction.commit().await?;
    Ok(())
}

type Stored = (
    Option<String>,
    Option<String>,
    String,
    i64,
    String,
    Option<time::OffsetDateTime>,
);

type Bundle = (Option<Vec<String>>, Option<Vec<u8>>, Value, Option<String>);

async fn stored(pool: &PgPool) -> TestResult<Stored> {
    Ok(sqlx::query_as(
        "SELECT raw_name, preimage_event_identity, visibility_state, block_number,
                canonicality_state::text, deactivated_at
         FROM name_surfaces WHERE logical_name_id = $1",
    )
    .bind(NAME)
    .fetch_one(pool)
    .await?)
}

#[tokio::test]
async fn rolled_back_raw_evidence_leaves_the_older_hash_path_identity() -> TestResult {
    let database = database("interpret_reanchor_evidence_rollback").await?;
    let pool = database.pool();
    insert_surface(pool, 1, Some(3), true).await?;
    insert_hash_path_observation(pool, 1).await?;
    insert_preimage(pool, 3).await?;

    redo_without_reobservation(pool, 3, 4).await?;

    assert_eq!(
        stored(pool).await?,
        (None, None, "active".into(), 1, "canonical".into(), None)
    );
    let bundle: Bundle = sqlx::query_as(
        "SELECT raw_labels, dns_encoded_name, normalization_errors, deactivation_reason
         FROM name_surfaces WHERE logical_name_id = $1",
    )
    .bind(NAME)
    .fetch_one(pool)
    .await?;
    assert_eq!(bundle, (None, None, json!([]), None));
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn released_witness_moves_to_the_earliest_surviving_preimage() -> TestResult {
    let database = database("interpret_reanchor_witness_moves").await?;
    let pool = database.pool();
    insert_surface(pool, 1, Some(3), false).await?;
    insert_hash_path_observation(pool, 1).await?;
    insert_preimage(pool, 3).await?;
    insert_preimage(pool, 5).await?;

    redo_without_reobservation(pool, 3, 4).await?;

    assert_eq!(
        stored(pool).await?,
        (
            Some("child.eth".into()),
            Some("preimage-5".into()),
            "active".into(),
            1,
            "canonical".into(),
            None
        )
    );
    database.cleanup().await?;
    Ok(())
}

/// A shadow cannot have been deactivated before its earliest surviving raw-label observation.
#[tokio::test]
async fn moved_shadow_witness_moves_the_deactivation_time() -> TestResult {
    let database = database("interpret_reanchor_shadow_witness_moves").await?;
    let pool = database.pool();
    insert_surface(pool, 1, Some(3), true).await?;
    insert_hash_path_observation(pool, 1).await?;
    insert_preimage(pool, 3).await?;
    insert_preimage(pool, 5).await?;

    redo_without_reobservation(pool, 3, 4).await?;

    assert_eq!(
        stored(pool).await?,
        (
            Some("child.eth".into()),
            Some("preimage-5".into()),
            "shadow".into(),
            1,
            "canonical".into(),
            Some(time::OffsetDateTime::from_unix_timestamp(5)?)
        )
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn preimage_anchored_surface_reanchors_with_its_witness() -> TestResult {
    let database = database("interpret_reanchor_preimage_anchor").await?;
    let pool = database.pool();
    insert_surface(pool, 2, Some(2), true).await?;
    insert_preimage(pool, 2).await?;
    insert_preimage(pool, 4).await?;

    redo_without_reobservation(pool, 1, 3).await?;

    assert_eq!(
        stored(pool).await?,
        (
            Some("child.eth".into()),
            Some("preimage-4".into()),
            "shadow".into(),
            4,
            "canonical".into(),
            Some(time::OffsetDateTime::from_unix_timestamp(4)?)
        )
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn hash_path_identity_reanchors_from_a_surviving_observation_or_stays_orphaned() -> TestResult
{
    let database = database("interpret_reanchor_hash_path").await?;
    let pool = database.pool();
    insert_surface(pool, 2, None, false).await?;
    insert_hash_path_observation(pool, 2).await?;
    insert_hash_path_observation(pool, 4).await?;
    // A later reference of another kind does not establish the identity.
    insert_event(
        pool,
        "transfer-5",
        "AuthorityTransferred",
        5,
        json!({"node": "0xchild"}),
    )
    .await?;

    redo_without_reobservation(pool, 1, 3).await?;
    assert_eq!(
        stored(pool).await?,
        (None, None, "active".into(), 4, "canonical".into(), None)
    );

    redo_without_reobservation(pool, 4, 4).await?;
    assert_eq!(
        stored(pool).await?,
        (None, None, "active".into(), 4, "orphaned".into(), None)
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn a_surface_with_no_hash_path_observation_keeps_unwitnessed_raw_labels() -> TestResult {
    let database = database("interpret_reanchor_unwitnessed").await?;
    let pool = database.pool();
    // A row from before the witness column, whose preimage rows no longer exist.
    insert_surface(pool, 1, Some(1), false).await?;
    sqlx::query("UPDATE name_surfaces SET preimage_event_identity = NULL")
        .execute(pool)
        .await?;

    redo_without_reobservation(pool, 3, 4).await?;

    assert_eq!(
        stored(pool).await?,
        (
            Some("child.eth".into()),
            None,
            "active".into(),
            1,
            "canonical".into(),
            None
        )
    );
    database.cleanup().await?;
    Ok(())
}

/// Redo completion settles the witness on the earliest same-block preimage by log position,
/// whichever one the replay's writes left on the row.
#[tokio::test]
async fn redo_completion_orders_same_block_witnesses_by_log_position() -> TestResult {
    let database = database("interpret_reanchor_witness_log_order").await?;
    let pool = database.pool();
    insert_surface(pool, 3, Some(3), true).await?;
    insert_preimage(pool, 3).await?;
    insert_event(
        pool,
        "preimage-3-recovered-late",
        PREIMAGE_OBSERVATION_EVENT_KIND,
        3,
        json!({"raw_name": "child.eth", "raw_labels": ["child", "eth"], "namehash": "0xchild"}),
    )
    .await?;
    sqlx::raw_sql(
        "UPDATE normalized_events SET log_index = 7
         WHERE event_identity = 'preimage-3-recovered-late';
         UPDATE name_surfaces SET preimage_event_identity = 'preimage-3-recovered-late'",
    )
    .execute(pool)
    .await?;

    let mut transaction = pool.begin().await?;
    super::stable_identities(&mut transaction, CHAIN, 3, 3).await?;
    transaction.commit().await?;

    assert_eq!(
        stored(pool).await?,
        (
            Some("child.eth".into()),
            Some("preimage-3".into()),
            "shadow".into(),
            3,
            "canonical".into(),
            Some(time::OffsetDateTime::from_unix_timestamp(3)?)
        )
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn byte_shadow_keeps_a_valid_bundle_during_prepare_and_drops_it_at_completion() -> TestResult
{
    let database = database("interpret_byte_shadow_prepare").await?;
    let pool = database.pool();
    insert_surface(pool, 1, Some(3), true).await?;
    insert_hash_path_observation(pool, 1).await?;
    insert_preimage(pool, 3).await?;
    sqlx::query(
        "UPDATE name_surfaces SET raw_name='', raw_labels='{}', dns_encoded_name=''::bytea",
    )
    .execute(pool)
    .await?;
    let mut transaction = pool.begin().await?;
    super::super::prepare_redo_range(&mut transaction, CHAIN, 3, 3).await?;
    transaction.commit().await?;
    let prepared = stored(pool).await?;
    assert_eq!(prepared.0, Some(String::new()));
    assert_eq!(
        prepared.1,
        Some("preimage-3".into()),
        "the strict byte-shadow check requires a nonempty witness during unpublished repair"
    );
    let mut transaction = pool.begin().await?;
    super::stable_identities(&mut transaction, CHAIN, 3, 3).await?;
    transaction.commit().await?;
    assert_eq!(
        stored(pool).await?,
        (None, None, "active".into(), 1, "canonical".into(), None)
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn same_block_null_positions_repair_identity_and_plain_timestamp() -> TestResult {
    let database = database("interpret_null_witness_repair").await?;
    let pool = database.pool();
    insert_surface(pool, 1, Some(3), true).await?;
    insert_hash_path_observation(pool, 1).await?;
    insert_preimage(pool, 3).await?;
    insert_event(
        pool,
        "earliest-null",
        PREIMAGE_OBSERVATION_EVENT_KIND,
        3,
        json!({"raw_labels_hex":["6368696c64","657468"]}),
    )
    .await?;
    sqlx::raw_sql("UPDATE normalized_events SET transaction_index=NULL,log_index=NULL WHERE event_identity='earliest-null'; UPDATE name_surfaces SET deactivated_at=to_timestamp(3)+interval '9 microseconds'").execute(pool).await?;
    let mut transaction = pool.begin().await?;
    super::stable_identities(&mut transaction, CHAIN, 3, 3).await?;
    transaction.commit().await?;
    let row = stored(pool).await?;
    assert_eq!(row.1, Some("earliest-null".into()));
    assert_eq!(row.5, Some(time::OffsetDateTime::from_unix_timestamp(3)?));
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn real_batch_redo_replaces_lost_raw_search_evidence_atomically() -> TestResult {
    let database = database("interpret_search_witness_redo").await?;
    for sql in [
        include_str!("../../../../storage/schema/baseline/02_raw_facts.sql"),
        include_str!("../../../../storage/schema/baseline/06_projections.sql"),
        include_str!("../../../../storage/schema/baseline/07_labels.sql"),
        include_str!("../../../../storage/schema/baseline/08_heartbeats.sql"),
        include_str!("../../../../storage/schema/baseline/09_divergence.sql"),
        include_str!("../../../../storage/schema/baseline/10_phase_state.sql"),
        include_str!("../../../../storage/schema/baseline/11_manifest_authority_attestations.sql"),
        include_str!("../../../../storage/schema/baseline/12_project_generation_failures.sql"),
        include_str!("../../../../storage/schema/baseline/13_interpret_decode_skips.sql"),
        include_str!("../../../../storage/schema/baseline/14_discovery_watch_admissions.sql"),
    ] {
        sqlx::raw_sql(sql).execute(database.pool()).await?;
    }
    insert_surface(database.pool(), 1, Some(3), false).await?;
    insert_hash_path_observation(database.pool(), 1).await?;
    insert_preimage(database.pool(), 3).await?;
    let mut tx = database.pool().begin().await?;
    bigname_storage::identity_search::prepare(&mut tx, &[], &[], &[NAME.to_owned()]).await?;
    bigname_storage::identity_search::refresh(&mut tx, &[NAME.to_owned()], &[]).await?;
    tx.commit().await?;
    let before: (String, i16) =
        sqlx::query_as("SELECT name,spelling_class FROM name_search_documents")
            .fetch_one(database.pool())
            .await?;
    assert_eq!(before, ("child.eth".to_owned(), 0));
    let expected = [(3, "0x03".to_owned()), (4, "0x04".to_owned())];
    for _ in 0..2 {
        super::super::batch(
            database.pool(),
            CHAIN,
            Some((3, 4)),
            true,
            true,
            0,
            &expected,
            &bigname_adapters::schema_v2::BatchOutput::default(),
        )
        .await?;
        let after: (String, i16) =
            sqlx::query_as("SELECT name,spelling_class FROM name_search_documents")
                .fetch_one(database.pool())
                .await?;
        assert!(after.0.starts_with('['));
        assert_eq!(after.1, 1);
        let stale: i64 =
            sqlx::query_scalar("SELECT count(*) FROM name_search_postings WHERE spelling_class=0")
                .fetch_one(database.pool())
                .await?;
        assert_eq!(stale, 0);
    }
    assert_eq!(stored(database.pool()).await?.0, None);
    database.cleanup().await?;
    Ok(())
}
