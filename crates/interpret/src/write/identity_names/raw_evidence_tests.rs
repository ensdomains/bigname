//! A name surface may be written before the raw bytes of its labels are known, and gains
//! them from a later hash-consistent observation.
use bigname_adapters::schema_v2::{BatchOutput, NameSurface};
use bigname_test_support::TestDatabase;
use serde_json::json;
use time::OffsetDateTime;

use super::coverage_tests::{TestResult, database, surface, write_output};
use super::write;

type Stored = (
    Option<String>,
    Option<Vec<String>>,
    Option<Vec<u8>>,
    Option<String>,
    String,
    i64,
    String,
);

async fn stored(database: &TestDatabase, logical_name_id: &str) -> TestResult<Stored> {
    Ok(sqlx::query_as(
        "SELECT raw_name, raw_labels, dns_encoded_name, preimage_event_identity,
                visibility_state, block_number, canonicality_state::text
         FROM name_surfaces WHERE logical_name_id = $1",
    )
    .bind(logical_name_id)
    .fetch_one(database.pool())
    .await?)
}

async fn add_block(database: &TestDatabase, number: i64) -> TestResult {
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) VALUES ('batch-test', $1, $2, to_timestamp($2), 'canonical')",
    )
    .bind(format!("0x{number:02}"))
    .bind(number)
    .execute(database.pool())
    .await?;
    Ok(())
}

fn at(mut row: NameSurface, block_number: i64) -> NameSurface {
    row.block_hash = format!("0x{block_number:02}");
    row.block_number = block_number;
    row
}

/// The same identity as `surface(..)`, observed from its label-hash path alone.
fn hash_path_only(logical_name_id: &str, raw_name: &str) -> NameSurface {
    NameSurface {
        raw: None,
        provenance: json!({"hash_path": raw_name}),
        ..surface(logical_name_id, raw_name)
    }
}

fn shadow(mut row: NameSurface, deactivated_at: i64) -> TestResult<NameSurface> {
    row.visibility_state = "shadow".to_owned();
    row.normalization_errors = json!([{"raw_label": "x", "error": "disallowed"}]);
    row.deactivation_reason = Some("normalization_gate".to_owned());
    row.deactivated_at = Some(OffsetDateTime::from_unix_timestamp(deactivated_at)?);
    Ok(row)
}

async fn write_one(database: &TestDatabase, row: NameSurface) -> TestResult {
    write_output(
        database,
        &BatchOutput {
            name_surfaces: vec![row],
            ..BatchOutput::default()
        },
    )
    .await
}

#[tokio::test]
async fn hash_path_identity_is_stored_without_raw_bytes_and_replays() -> TestResult {
    let database = database("interpret_surface_hash_path_only").await?;
    for _ in 0..2 {
        write_one(&database, hash_path_only("ens:child", "child.eth")).await?;
    }
    assert_eq!(
        stored(&database, "ens:child").await?,
        (
            None,
            None,
            None,
            None,
            "active".into(),
            1,
            "canonical".into()
        )
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn later_raw_observation_enriches_the_identity_and_keeps_its_anchor() -> TestResult {
    let database = database("interpret_surface_enrichment").await?;
    add_block(&database, 2).await?;
    write_one(&database, hash_path_only("ens:child", "child.eth")).await?;
    write_one(&database, at(surface("ens:child", "child.eth"), 2)).await?;

    let enriched = (
        Some("child.eth".to_owned()),
        Some(vec!["child.eth".to_owned()]),
        Some(b"child.eth".to_vec()),
        Some("preimage:child.eth".to_owned()),
        "active".to_owned(),
        1,
        "canonical".to_owned(),
    );
    assert_eq!(stored(&database, "ens:child").await?, enriched);
    let provenance: serde_json::Value = sqlx::query_scalar(
        "SELECT provenance FROM name_surfaces WHERE logical_name_id = 'ens:child'",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(provenance, json!({"hash_path": "child.eth"}));

    // A later hash-path observation of the same node leaves the raw evidence in place.
    write_one(&database, at(hash_path_only("ens:child", "child.eth"), 2)).await?;
    assert_eq!(stored(&database, "ens:child").await?, enriched);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn unnormalizable_raw_observation_shadows_the_enriched_identity() -> TestResult {
    let database = database("interpret_surface_shadow_enrichment").await?;
    add_block(&database, 2).await?;
    write_one(&database, hash_path_only("ens:child", "child.eth")).await?;
    write_one(
        &database,
        shadow(at(surface("ens:child", "child.eth"), 2), 2)?,
    )
    .await?;
    // The hash-path observation carries no verdict, so it cannot reactivate the row.
    write_one(&database, hash_path_only("ens:child", "child.eth")).await?;

    let row: (String, Option<String>, Option<OffsetDateTime>, i64) = sqlx::query_as(
        "SELECT visibility_state, deactivation_reason, deactivated_at, block_number
         FROM name_surfaces WHERE logical_name_id = 'ens:child'",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        row,
        (
            "shadow".into(),
            Some("normalization_gate".into()),
            Some(OffsetDateTime::from_unix_timestamp(2)?),
            1
        )
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn evidence_for_a_different_hash_path_or_different_bytes_is_rejected() -> TestResult {
    let database = database("interpret_surface_evidence_conflict").await?;
    write_one(&database, hash_path_only("ens:child", "child.eth")).await?;
    write_one(&database, surface("ens:named", "named.eth")).await?;

    for conflicting in [
        // Raw bytes whose label hashes are not the stored path.
        surface("ens:child", "other.eth"),
        // A hash path that is not the one the raw bytes proved.
        hash_path_only("ens:named", "other.eth"),
        // Different bytes for a name whose bytes are known.
        NameSurface {
            labelhashes: surface("ens:named", "named.eth").labelhashes,
            ..surface("ens:named", "other.eth")
        },
    ] {
        let mut transaction = database.pool().begin().await?;
        let error = write(
            &mut transaction,
            &BatchOutput {
                name_surfaces: vec![conflicting],
                ..BatchOutput::default()
            },
        )
        .await
        .expect_err("conflicting identity evidence must fail");
        transaction.rollback().await?;
        assert_eq!(error.kind(), crate::ErrorKind::DataIntegrity);
    }
    assert_eq!(stored(&database, "ens:child").await?.0, None);
    assert_eq!(
        stored(&database, "ens:named").await?.0.as_deref(),
        Some("named.eth")
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn absent_raw_bytes_are_not_empty_bytes_or_a_partial_bundle() -> TestResult {
    let database = database("interpret_surface_absent_not_empty").await?;
    let insert = |raw_name: Option<&'static str>,
                  raw_labels: Option<Vec<String>>,
                  dns: Option<Vec<u8>>,
                  labelhashes: Vec<String>,
                  witness: Option<&'static str>,
                  visibility: &'static str| {
        let pool = database.pool().clone();
        async move {
            sqlx::query(
                "INSERT INTO name_surfaces (
                     logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
                     namehash, labelhashes, normalizer_version, visibility_state,
                     deactivation_reason, deactivated_at, chain_id, block_hash, block_number,
                     preimage_event_identity
                 ) VALUES (
                     'ens:probe', 'ens', $1, $2, $3, 'probe', $4, 'test', $6,
                     CASE WHEN $6 = 'shadow' THEN 'normalization_gate' END,
                     CASE WHEN $6 = 'shadow' THEN to_timestamp(1) END,
                     'batch-test', '0x01', 1, $5
                 )",
            )
            .bind(raw_name)
            .bind(raw_labels)
            .bind(dns)
            .bind(labelhashes)
            .bind(witness)
            .bind(visibility)
            .execute(&pool)
            .await
        }
    };
    let path = vec!["0xlabel".to_owned()];
    // Each of these is neither "all absent" nor "all present and consistent".
    for (raw_name, raw_labels, dns, labelhashes, witness, visibility) in [
        (Some("a"), None, None, path.clone(), None, "active"),
        (
            None,
            Some(vec!["a".to_owned()]),
            None,
            path.clone(),
            None,
            "active",
        ),
        (
            None,
            None,
            Some(vec![1, b'a', 0]),
            path.clone(),
            None,
            "active",
        ),
        (None, None, None, Vec::new(), None, "active"),
        (None, None, None, path.clone(), Some("witness"), "active"),
        (None, None, None, path.clone(), None, "shadow"),
        (
            Some("a"),
            Some(Vec::new()),
            Some(Vec::new()),
            path.clone(),
            None,
            "active",
        ),
        (
            Some("a"),
            Some(vec!["a".to_owned()]),
            Some(Vec::new()),
            path.clone(),
            Some(" "),
            "active",
        ),
    ] {
        let error = insert(raw_name, raw_labels, dns, labelhashes, witness, visibility)
            .await
            .expect_err("an inconsistent raw-evidence bundle must violate the check");
        assert!(
            error
                .to_string()
                .contains("name_surfaces_raw_evidence_check"),
            "unexpected error: {error}"
        );
    }
    // The empty bundle a shadow surface with undecodable bytes stores today stays valid.
    insert(
        Some(""),
        Some(Vec::new()),
        Some(Vec::new()),
        Vec::new(),
        None,
        "shadow",
    )
    .await?;
    database.cleanup().await?;
    Ok(())
}
