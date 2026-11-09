use bigname_adapters::schema_v2::{BatchOutput, LabelPreimage, NameSurface, RawNameEvidence};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::json;

use super::*;

pub(super) type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

pub(super) async fn database(name: &str) -> TestResult<TestDatabase> {
    let database = TestDatabase::create(TestDatabaseConfig::new(name)).await?;
    for sql in [
        include_str!("../../../../storage/schema/baseline/01_chain.sql"),
        include_str!("../../../../storage/schema/baseline/03_identity.sql"),
        include_str!("../../../../storage/schema/baseline/04_manifests.sql"),
        include_str!("../../../../storage/schema/baseline/05_normalized_events.sql"),
        include_str!("../../../../storage/schema/baseline/07_labels.sql"),
    ] {
        sqlx::raw_sql(sql).execute(database.pool()).await?;
    }
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) VALUES ('batch-test', '0x01', 1, to_timestamp(1), 'canonical')",
    )
    .execute(database.pool())
    .await?;
    Ok(database)
}

pub(super) async fn write_output(database: &TestDatabase, output: &BatchOutput) -> TestResult {
    let mut transaction = database.pool().begin().await?;
    write(&mut transaction, output).await?;
    transaction.commit().await?;
    Ok(())
}

fn observed_preimage(
    labelhash: &str,
    raw_label: &[u8],
    priority: i32,
    provenance: serde_json::Value,
) -> LabelPreimage {
    LabelPreimage {
        labelhash: labelhash.to_owned(),
        raw_label: raw_label.to_vec(),
        decoded_label: std::str::from_utf8(raw_label).ok().map(str::to_owned),
        normalizer_version: "test".to_owned(),
        normalized_under_version: true,
        normalization_error: None,
        source_kind: format!("source-{priority}"),
        source_priority: priority,
        provenance,
    }
}

pub(super) fn surface(logical_name_id: &str, raw_name: &str) -> NameSurface {
    let namehash = logical_name_id
        .strip_prefix("ens:")
        .expect("test logical IDs use the ENS namespace");
    NameSurface {
        logical_name_id: logical_name_id.to_owned(),
        namespace: "ens".to_owned(),
        raw: Some(RawNameEvidence {
            raw_name: raw_name.to_owned(),
            raw_labels: vec![raw_name.to_owned()],
            dns_encoded_name: raw_name.as_bytes().to_vec(),
            preimage_event_identity: format!("preimage:{raw_name}"),
        }),
        namehash: namehash.to_owned(),
        labelhashes: vec![format!("label:{raw_name}")],
        normalizer_version: "test".to_owned(),
        visibility_state: "active".to_owned(),
        normalization_errors: json!([]),
        deactivation_reason: None,
        deactivated_at: None,
        chain_id: "batch-test".to_owned(),
        block_hash: "0x01".to_owned(),
        block_number: 1,
        provenance: json!({"raw_name": raw_name}),
        canonicality_state: "canonical".to_owned(),
    }
}

#[tokio::test]
async fn conflicting_preimage_identifies_row_and_rolls_back_prefix() -> TestResult {
    let database = database("interpret_preimage_conflict_rollback").await?;
    write_output(
        &database,
        &BatchOutput {
            label_preimages: vec![observed_preimage(
                "conflict",
                b"stored",
                5,
                json!({"stored": true}),
            )],
            ..BatchOutput::default()
        },
    )
    .await?;
    let output = BatchOutput {
        label_preimages: vec![
            observed_preimage("prefix", b"prefix", 1, json!({})),
            observed_preimage("conflict", b"different", 5, json!({})),
            observed_preimage("suffix", b"suffix", 1, json!({})),
        ],
        ..BatchOutput::default()
    };

    let mut transaction = database.pool().begin().await?;
    let error = write(&mut transaction, &output)
        .await
        .expect_err("the divergent stored preimage must fail");
    transaction.rollback().await?;
    assert_eq!(error.kind(), crate::ErrorKind::DataIntegrity);
    assert!(error.to_string().contains("label hashes are already bound"));
    assert!(error.to_string().contains("1=conflict"));
    let hashes =
        sqlx::query_scalar::<_, String>("SELECT labelhash FROM label_preimages ORDER BY labelhash")
            .fetch_all(database.pool())
            .await?;
    assert_eq!(hashes, vec!["conflict"]);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn conflicting_surface_identifies_row_and_rolls_back_prefix() -> TestResult {
    let database = database("interpret_surface_conflict_rollback").await?;
    write_output(
        &database,
        &BatchOutput {
            name_surfaces: vec![surface("ens:conflict", "stored.eth")],
            ..BatchOutput::default()
        },
    )
    .await?;
    let output = BatchOutput {
        name_surfaces: vec![
            surface("ens:prefix", "prefix.eth"),
            surface("ens:conflict", "different.eth"),
            surface("ens:suffix", "suffix.eth"),
        ],
        ..BatchOutput::default()
    };

    let mut transaction = database.pool().begin().await?;
    let error = write(&mut transaction, &output)
        .await
        .expect_err("the divergent stored surface must fail");
    transaction.rollback().await?;
    assert_eq!(error.kind(), crate::ErrorKind::DataIntegrity);
    assert!(
        error
            .to_string()
            .contains("logical name IDs are already bound")
    );
    assert!(error.to_string().contains("1=ens:conflict"));
    let ids = sqlx::query_scalar::<_, String>(
        "SELECT logical_name_id FROM name_surfaces ORDER BY logical_name_id",
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(ids, vec!["ens:conflict"]);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn canonical_reobservation_refreshes_stale_orphaned_normalization_state() -> TestResult {
    let database = database("interpret_surface_orphan_replacement").await?;
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) VALUES ('batch-test', '0x00', 1, to_timestamp(1), 'orphaned')",
    )
    .execute(database.pool())
    .await?;
    let mut orphaned = surface("ens:reobserved", "same.eth");
    orphaned.block_hash = "0x00".to_owned();
    orphaned.normalizer_version = "old".to_owned();
    orphaned.canonicality_state = "orphaned".to_owned();
    write_output(
        &database,
        &BatchOutput {
            name_surfaces: vec![orphaned],
            ..BatchOutput::default()
        },
    )
    .await?;

    write_output(
        &database,
        &BatchOutput {
            name_surfaces: vec![surface("ens:reobserved", "same.eth")],
            ..BatchOutput::default()
        },
    )
    .await?;
    let row: (String, String, String, String) = sqlx::query_as(
        "SELECT raw_name, normalizer_version, visibility_state, canonicality_state::text
         FROM name_surfaces WHERE logical_name_id = 'ens:reobserved'",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        row,
        (
            "same.eth".into(),
            "test".into(),
            "active".into(),
            "canonical".into()
        )
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn tied_preimage_priority_preserves_last_occurrence() -> TestResult {
    let database = database("interpret_preimage_occurrence_order").await?;
    write_output(
        &database,
        &BatchOutput {
            label_preimages: vec![
                observed_preimage("tie", b"same", 5, json!({"occurrence": "first"})),
                observed_preimage("tie", b"same", 3, json!({"occurrence": "middle"})),
                observed_preimage("tie", b"same", 5, json!({"occurrence": "last"})),
            ],
            ..BatchOutput::default()
        },
    )
    .await?;

    let stored: (i32, serde_json::Value) =
        sqlx::query_as("SELECT source_priority, provenance FROM label_preimages")
            .fetch_one(database.pool())
            .await?;
    assert_eq!(stored, (5, json!({"occurrence": "last"})));
    database.cleanup().await?;
    Ok(())
}

/// A lock timeout while preparing the search locks is a retryable database error, not a data
/// integrity fault that stops the chain.
#[tokio::test]
async fn search_lock_timeout_is_transient() -> TestResult {
    let database = database("interpret_search_lock_timeout").await?;
    let output = BatchOutput {
        name_surfaces: vec![surface("ens:held", "held.eth")],
        ..BatchOutput::default()
    };
    write_output(&database, &output).await?;
    let mut holder = database.pool().begin().await?;
    sqlx::query("SELECT 1 FROM name_surfaces WHERE logical_name_id='ens:held' FOR UPDATE")
        .execute(&mut *holder)
        .await?;
    let mut transaction = database.pool().begin().await?;
    sqlx::query("SET LOCAL lock_timeout = '200ms'")
        .execute(&mut *transaction)
        .await?;
    let error = write(&mut transaction, &output)
        .await
        .expect_err("the held surface must time out the search lock");
    transaction.rollback().await?;
    holder.rollback().await?;
    assert_eq!(error.kind(), crate::ErrorKind::Transient, "{error}");
    assert!(
        error
            .to_string()
            .contains("failed to lock affected search identities"),
        "{error}"
    );
    database.cleanup().await?;
    Ok(())
}

/// A label observed many times in one batch is inserted once, and a stored label is updated
/// once.
#[tokio::test]
async fn repeated_label_observations_are_written_once_per_batch() -> TestResult {
    let database = database("interpret_preimage_statement_counts").await?;
    write_output(
        &database,
        &BatchOutput {
            label_preimages: vec![observed_preimage("stored", b"stored", 5, json!({}))],
            ..BatchOutput::default()
        },
    )
    .await?;
    let output = BatchOutput {
        label_preimages: (0..10)
            .flat_map(|step| {
                [
                    observed_preimage("new", b"new", 1, json!({"step": step})),
                    observed_preimage("stored", b"stored", 1, json!({"step": step})),
                ]
            })
            .collect(),
        ..BatchOutput::default()
    };
    // The connection may still hold unreported counts of earlier transactions, so compare two
    // readings inside this one.
    let statistics = "SELECT n_tup_ins, n_tup_upd FROM pg_stat_xact_user_tables
                      WHERE relname = 'label_preimages'";
    let mut transaction = database.pool().begin().await?;
    let before: (i64, i64) = sqlx::query_as(statistics)
        .fetch_one(&mut *transaction)
        .await?;
    write(&mut transaction, &output).await?;
    let after: (i64, i64) = sqlx::query_as(statistics)
        .fetch_one(&mut *transaction)
        .await?;
    transaction.rollback().await?;
    assert_eq!((after.0 - before.0, after.1 - before.1), (1, 1));
    database.cleanup().await?;
    Ok(())
}

/// A stored row that outranks every observation in the batch keeps its source, and the write
/// still records that the label was observed in this transaction.
#[tokio::test]
async fn stored_higher_priority_keeps_source_and_refreshes_observed_time() -> TestResult {
    let database = database("interpret_preimage_stored_outranks_batch").await?;
    write_output(
        &database,
        &BatchOutput {
            label_preimages: vec![observed_preimage(
                "stored",
                b"stored",
                9,
                json!({"stored": true}),
            )],
            ..BatchOutput::default()
        },
    )
    .await?;
    let output = BatchOutput {
        label_preimages: [1, 5, 3, 5]
            .into_iter()
            .map(|priority| {
                observed_preimage("stored", b"stored", priority, json!({"batch": priority}))
            })
            .collect(),
        ..BatchOutput::default()
    };
    let mut transaction = database.pool().begin().await?;
    write(&mut transaction, &output).await?;
    let stored: (String, i32, serde_json::Value, bool) = sqlx::query_as(
        "SELECT source_kind, source_priority, provenance, observed_at = now()
         FROM label_preimages WHERE labelhash = 'stored'",
    )
    .fetch_one(&mut *transaction)
    .await?;
    transaction.commit().await?;
    assert_eq!(
        stored,
        ("source-9".to_owned(), 9, json!({"stored": true}), true)
    );
    database.cleanup().await?;
    Ok(())
}

/// An ENSv2 resolver's `NamedResource` log, from any address, names a resource by a DNS-encoded
/// name of any length. One log of thousands of distinct labels is interpreted and written whole:
/// one name surface and one preimage per label. The surface's raw name is over 2,000 bytes, so
/// it gets no search document and no btree index reads it. Each test prints how long
/// interpreting and writing the log took, and asserts nothing about it.
mod long_resolver_dns_name {
    use std::time::Instant;

    use alloy_primitives::U256;
    use alloy_sol_types::{SolEvent, sol};
    use bigname_adapters::schema_v2::{BatchInput, ManifestInput, RawBlockInput, RawLogInput};
    use bigname_domain::normalization::ENS_NORMALIZER_VERSION;
    use bigname_test_support::fixture_logs::interpret_checked;
    use time::OffsetDateTime;

    use super::{TestResult, database, write_output};

    sol! {
        event NamedResource(uint256 indexed resource, bytes name);
    }

    const CHAIN: &str = "batch-test";

    /// `count` distinct four-letter labels: `aaaa`, `aaab`, and so on.
    fn labels(count: usize) -> Vec<Vec<u8>> {
        (0..count)
            .map(|index| {
                (0..4_u32)
                    .rev()
                    .map(|place| b"abcdefghijklmnopqrstuvwxyz"[index / 26_usize.pow(place) % 26])
                    .collect()
            })
            .collect()
    }

    fn named_resource(labels: &[Vec<u8>]) -> BatchInput {
        let mut name = Vec::new();
        for label in labels {
            name.push(u8::try_from(label.len()).expect("a DNS label is at most 255 bytes"));
            name.extend_from_slice(label);
        }
        name.push(0);
        let log = NamedResource {
            resource: U256::from(1),
            name: name.into(),
        }
        .encode_log_data();
        let block_timestamp = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1);
        BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![ManifestInput {
                manifest_id: 1,
                manifest_version: 1,
                namespace: "ens".to_owned(),
                source_family: "ens_v2_resolver_l1".to_owned(),
                chain_id: CHAIN.to_owned(),
                deployment_label: "test".to_owned(),
                normalizer_version: ENS_NORMALIZER_VERSION.to_owned(),
                payload_json: serde_json::json!({
                    "abi": {
                        "events": [{
                            "name": "NamedResource",
                            "fragment": "event NamedResource(uint256 indexed resource, bytes name)",
                            "emitter_roles": [],
                            "normalized_events": ["PreimageObserved"]
                        }]
                    }
                })
                .to_string(),
            }],
            discovery_rules: Vec::new(),
            admissions: Vec::new(),
            prior_events: Vec::new(),
            blocks: vec![RawBlockInput {
                chain_id: CHAIN.to_owned(),
                block_hash: "0x01".to_owned(),
                block_number: 1,
                block_timestamp,
                canonicality_state: "canonical".to_owned(),
            }],
            raw_logs: vec![RawLogInput {
                chain_id: CHAIN.to_owned(),
                block_hash: "0x01".to_owned(),
                block_number: 1,
                block_timestamp,
                canonicality_state: "canonical".to_owned(),
                transaction_hash: "transaction-1".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: "0x0000000000000000000000000000000000000042".to_owned(),
                topics: log
                    .topics()
                    .iter()
                    .map(|topic| format!("{topic:#x}"))
                    .collect(),
                data: log.data.to_vec(),
            }],
        }
    }

    async fn interpreted_and_stored_whole(count: usize) -> TestResult {
        let database = database(&format!("interpret_long_resolver_dns_name_{count}")).await?;
        let input = named_resource(&labels(count));
        let started = Instant::now();
        let output = interpret_checked(input)?;
        let interpreted = started.elapsed();
        write_output(&database, &output).await?;
        let elapsed = started.elapsed();
        println!(
            "resolver_dns_name wall time: labels={count} elapsed_ms={} interpret_ms={} write_ms={}",
            elapsed.as_millis(),
            interpreted.as_millis(),
            (elapsed - interpreted).as_millis(),
        );

        let surface: (i64, Option<i32>, Option<i32>, Option<String>) = sqlx::query_as(
            "SELECT count(*), max(octet_length(raw_name)), max(cardinality(labelhashes)),
                    max(visibility_state)
             FROM name_surfaces",
        )
        .fetch_one(database.pool())
        .await?;
        let label_count = i32::try_from(count)?;
        let raw_name_bytes = 5 * label_count - 1;
        assert_eq!(
            surface,
            (
                1,
                Some(raw_name_bytes),
                Some(label_count),
                Some("active".to_owned())
            )
        );
        let rows: (i64, i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM label_preimages),
                    (SELECT count(*) FROM name_search_documents)",
        )
        .fetch_one(database.pool())
        .await?;
        assert_eq!(rows, (i64::from(label_count), 0));
        database.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_resolver_dns_name_of_4000_distinct_labels_is_stored_whole() -> TestResult {
        interpreted_and_stored_whole(4_000).await
    }

    #[tokio::test]
    async fn a_resolver_dns_name_of_40000_distinct_labels_is_stored_whole() -> TestResult {
        interpreted_and_stored_whole(40_000).await
    }
}
