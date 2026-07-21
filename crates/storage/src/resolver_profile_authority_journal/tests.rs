use std::collections::BTreeSet;

use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};

use super::advance::ResolverProfileAuthorityJournalBatchSizes;
use super::*;

fn entry_payload(
    chain: &str,
    source_family: &str,
    address_number: u64,
    is_seed: bool,
    semantics_marker: &str,
) -> Value {
    json!({
        "chain": chain,
        "source_family": source_family,
        "address": format!("0x{address_number:040x}"),
        "contract_instance_id": format!("00000000-0000-0000-0000-{address_number:012x}"),
        "source": "discovery_edge",
        "source_manifest_id": 1,
        "active_from_block_number": 1,
        "active_to_block_number": null,
        "is_seed": is_seed,
        "admission_semantics": [{
            "profile": "ens_v1_public_resolver_compatible",
            "fact_family": "resolver_record",
            "status": semantics_marker,
            "admission_basis": if is_seed {
                "manifest_public_resolver_seed"
            } else {
                "code_hash_match"
            },
            "matched_code_hash": "0x01",
            "matched_contract_instance_id": "00000000-0000-0000-0000-000000000001"
        }]
    })
}

fn entry(
    chain: &str,
    source_family: &str,
    address_number: u64,
    is_seed: bool,
    semantics_marker: &str,
) -> Result<ResolverProfileAuthorityJournalEntry> {
    ResolverProfileAuthorityJournalEntry::from_payload(entry_payload(
        chain,
        source_family,
        address_number,
        is_seed,
        semantics_marker,
    ))
}

async fn stage_entries(
    advance: &mut ResolverProfileAuthorityJournalAdvance,
    entries: &[ResolverProfileAuthorityJournalEntry],
) -> Result<()> {
    advance.stage_entries(entries).await
}

async fn stored_payloads(database: &TestDatabase) -> Result<BTreeSet<String>> {
    Ok(sqlx::query_scalar::<_, Value>(
        r#"
        SELECT entry_payload
        FROM resolver_profile_authority_journal_entries
        WHERE journal_key = 'active_resolver_profiles'
        ORDER BY entry_key
        "#,
    )
    .fetch_all(database.pool())
    .await?
    .into_iter()
    .map(|payload| payload.to_string())
    .collect())
}

#[test]
fn authority_entry_key_is_the_canonical_natural_identity() -> Result<()> {
    let payload = entry_payload(
        "ethereum-mainnet",
        "ens_v1_resolver_l1",
        1,
        false,
        "supported",
    );
    assert_eq!(
        resolver_profile_authority_entry_key(&payload)?,
        concat!(
            "[\"ethereum-mainnet\", \"ens_v1_resolver_l1\", ",
            "\"0x0000000000000000000000000000000000000001\", ",
            "\"00000000-0000-0000-0000-000000000001\", ",
            "\"discovery_edge\", 1, 1, null]"
        )
    );
    Ok(())
}

#[tokio::test]
async fn migration_decomposes_existing_authority_snapshot() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new(
        "storage_resolver_profile_authority_journal_migration",
    ))
    .await?;
    sqlx::raw_sql(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations/20260715130000_resolver_profile_authority_journal.sql"
    )))
    .execute(database.pool())
    .await?;

    let payload = entry_payload(
        "ethereum-mainnet",
        "ens_v1_resolver_l1",
        1,
        false,
        "supported",
    );
    sqlx::query(
        r#"
        UPDATE resolver_profile_authority_journal
        SET authority_snapshot = jsonb_build_object('entries', jsonb_build_array($1::JSONB))
        WHERE journal_key = 'active_resolver_profiles'
        "#,
    )
    .bind(&payload)
    .execute(database.pool())
    .await?;
    sqlx::raw_sql(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations/20260720122000_normalize_resolver_profile_authority_journal.sql"
    )))
    .execute(database.pool())
    .await?;

    let (entry_key, stored_payload) = sqlx::query_as::<_, (String, Value)>(
        r#"
        SELECT entry_key, entry_payload
        FROM resolver_profile_authority_journal_entries
        WHERE journal_key = 'active_resolver_profiles'
        "#,
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(entry_key, resolver_profile_authority_entry_key(&payload)?);
    assert_eq!(stored_payload, payload);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'resolver_profile_authority_journal'
              AND column_name = 'authority_snapshot'
            "#,
        )
        .fetch_one(database.pool())
        .await?,
        0
    );
    database.cleanup().await
}

#[tokio::test]
async fn first_revision_stores_entries_without_enqueuing_targets() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("storage_resolver_profile_authority_first_revision"),
        &crate::MIGRATOR,
        "failed to apply migrations for resolver-profile authority first-revision test",
    )
    .await?;
    let initial = load_resolver_profile_authority_journal(database.pool()).await?;
    let entries = vec![entry(
        "ethereum-mainnet",
        "ens_v1_resolver_l1",
        1,
        true,
        "supported",
    )?];
    let mut advance =
        begin_resolver_profile_authority_journal_advance(database.pool(), initial.revision).await?;
    stage_entries(&mut advance, &entries).await?;
    let summary = advance.publish(&json!({"ethereum-mainnet": 1})).await?;
    let summary = summary.expect("initial revision must advance");
    assert_eq!(summary.enqueued_target_count, 0);
    assert_eq!(summary.upserted_entry_count, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM resolver_profile_input_changes")
            .fetch_one(database.pool())
            .await?,
        0
    );
    assert_eq!(
        load_resolver_profile_authority_journal(database.pool())
            .await?
            .revision,
        1
    );
    database.cleanup().await
}

#[tokio::test]
async fn stale_revision_rolls_back_queue_and_entry_mutations() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("storage_resolver_profile_authority_stale_revision"),
        &crate::MIGRATOR,
        "failed to apply migrations for resolver-profile authority CAS test",
    )
    .await?;
    let baseline_entry = entry(
        "ethereum-mainnet",
        "ens_v1_resolver_l1",
        1,
        false,
        "baseline",
    )?;
    let mut baseline = begin_resolver_profile_authority_journal_advance(database.pool(), 0).await?;
    stage_entries(&mut baseline, std::slice::from_ref(&baseline_entry)).await?;
    baseline.publish(&json!({})).await?.unwrap();

    let current_entry = entry(
        "ethereum-mainnet",
        "ens_v1_resolver_l1",
        1,
        false,
        "current",
    )?;
    let mut current = begin_resolver_profile_authority_journal_advance(database.pool(), 1).await?;
    stage_entries(&mut current, std::slice::from_ref(&current_entry)).await?;
    current.publish(&json!({})).await?.unwrap();
    let generation_before_stale = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT generation
        FROM resolver_profile_input_changes
        WHERE chain_id = 'ethereum-mainnet'
          AND contract_address = '0x0000000000000000000000000000000000000001'
        "#,
    )
    .fetch_one(database.pool())
    .await?;
    let payloads_before_stale = stored_payloads(&database).await?;

    let stale_entries = vec![
        entry("ethereum-mainnet", "ens_v1_resolver_l1", 1, false, "stale")?,
        entry("ethereum-mainnet", "ens_v1_resolver_l1", 2, false, "stale")?,
    ];
    let mut stale = begin_resolver_profile_authority_journal_advance(database.pool(), 1).await?;
    stage_entries(&mut stale, &stale_entries).await?;
    assert!(stale.publish(&json!({})).await?.is_none());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT generation
            FROM resolver_profile_input_changes
            WHERE chain_id = 'ethereum-mainnet'
              AND contract_address = '0x0000000000000000000000000000000000000001'
            "#,
        )
        .fetch_one(database.pool())
        .await?,
        generation_before_stale
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM resolver_profile_input_changes
            WHERE contract_address = '0x0000000000000000000000000000000000000002'
            "#,
        )
        .fetch_one(database.pool())
        .await?,
        0
    );
    assert_eq!(stored_payloads(&database).await?, payloads_before_stale);
    assert_eq!(
        load_resolver_profile_authority_journal(database.pool())
            .await?
            .revision,
        2
    );
    database.cleanup().await
}

#[tokio::test]
async fn batched_diff_preserves_seed_family_target_expansion() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("storage_resolver_profile_authority_batched_diff"),
        &crate::MIGRATOR,
        "failed to apply migrations for resolver-profile authority batched-diff test",
    )
    .await?;
    let batch_sizes = ResolverProfileAuthorityJournalBatchSizes {
        entry_mutation: 2,
        target_enqueue: 2,
    };
    let before = vec![
        entry("ethereum-mainnet", "ens_v1_resolver_l1", 1, true, "a")?,
        entry("ethereum-mainnet", "ens_v1_resolver_l1", 2, false, "a")?,
        entry("ethereum-mainnet", "ens_v1_resolver_l1", 3, false, "a")?,
        entry("ethereum-mainnet", "ens_v1_resolver_l1", 4, false, "a")?,
        entry("base-mainnet", "basenames_base_resolver", 9, false, "a")?,
    ];
    let mut baseline = ResolverProfileAuthorityJournalAdvance::begin_with_batch_sizes(
        database.pool(),
        0,
        batch_sizes,
    )
    .await?;
    stage_entries(&mut baseline, &before).await?;
    baseline.publish(&json!({})).await?.unwrap();

    let after = vec![
        entry("ethereum-mainnet", "ens_v1_resolver_l1", 1, true, "b")?,
        entry("ethereum-mainnet", "ens_v1_resolver_l1", 2, false, "b")?,
        entry("ethereum-mainnet", "ens_v1_resolver_l1", 4, false, "a")?,
        entry("ethereum-mainnet", "ens_v1_resolver_l1", 5, false, "a")?,
        entry("base-mainnet", "basenames_base_resolver", 9, false, "a")?,
    ];
    let mut advance = ResolverProfileAuthorityJournalAdvance::begin_with_batch_sizes(
        database.pool(),
        1,
        batch_sizes,
    )
    .await?;
    stage_entries(&mut advance, &after).await?;
    let summary = advance.publish(&json!({})).await?.unwrap();
    assert_eq!(summary.changed_entry_count, 4);
    assert_eq!(summary.enqueued_target_count, 5);
    assert_eq!(summary.target_enqueue_statement_count, 3);
    assert_eq!(summary.max_target_enqueue_batch_size, 2);
    assert_eq!(summary.upserted_entry_count, 3);
    assert_eq!(summary.deleted_entry_count, 1);
    assert_eq!(summary.entry_mutation_statement_count, 3);
    assert_eq!(summary.max_entry_mutation_batch_size, 2);

    let queued = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT chain_id, contract_address
        FROM resolver_profile_input_changes
        ORDER BY chain_id, contract_address
        "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        queued,
        (1..=5)
            .map(|address| ("ethereum-mainnet".to_owned(), format!("0x{address:040x}")))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        stored_payloads(&database).await?,
        after
            .iter()
            .map(|entry| entry.entry_payload.to_string())
            .collect()
    );
    database.cleanup().await
}

#[tokio::test]
async fn scale_shaped_capture_keeps_every_statement_bounded() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("storage_resolver_profile_authority_scale_shape"),
        &crate::MIGRATOR,
        "failed to apply migrations for resolver-profile authority scale-shape test",
    )
    .await?;
    let entries = (1..=2_501)
        .map(|address| {
            entry(
                "ethereum-mainnet",
                "ens_v1_resolver_l1",
                address,
                false,
                "supported",
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let mut advance = begin_resolver_profile_authority_journal_advance(database.pool(), 0).await?;
    stage_entries(&mut advance, &entries).await?;
    let summary = advance.publish(&json!({})).await?.unwrap();

    assert_eq!(summary.staged_entry_count, 2_501);
    assert_eq!(summary.staging_statement_count, 3);
    assert_eq!(
        summary.max_staged_entry_batch_size,
        RESOLVER_PROFILE_AUTHORITY_JOURNAL_ENTRY_BATCH_SIZE
    );
    assert_eq!(summary.upserted_entry_count, 2_501);
    assert_eq!(summary.entry_mutation_statement_count, 3);
    assert_eq!(
        summary.max_entry_mutation_batch_size,
        RESOLVER_PROFILE_AUTHORITY_JOURNAL_ENTRY_BATCH_SIZE
    );
    assert_eq!(summary.enqueued_target_count, 0);
    database.cleanup().await
}
