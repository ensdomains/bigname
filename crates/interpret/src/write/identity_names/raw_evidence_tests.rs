//! A name surface may be written before the raw bytes of its labels are known, and gains
//! them from a later hash-consistent observation.
use bigname_adapters::schema_v2::{
    BatchOutput, NameSurface, NormalizedEvent, seam::PREIMAGE_OBSERVATION_EVENT_KIND,
};
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
    // The legacy empty bundle for a shadow with undecodable bytes stays valid until re-derived.
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

fn preimage_event(identity: &str, logical_name_id: &str, log_index: i64) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: Some(logical_name_id.to_owned()),
        resource_id: None,
        event_kind: PREIMAGE_OBSERVATION_EVENT_KIND.to_owned(),
        source_family: "ens_v2_registry_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: "batch-test".to_owned(),
        block_number: Some(1),
        block_hash: Some("0x01".to_owned()),
        transaction_hash: Some("0xtx".to_owned()),
        transaction_index: Some(0),
        log_index: Some(log_index),
        raw_fact_ref: json!({}),
        derivation_kind: "raw_log_preimage_observation".to_owned(),
        canonicality_state: "canonical".to_owned(),
        before_state: json!({}),
        after_state: json!({}),
        migration_correlation_ids: Vec::new(),
        consumer_visibility: "internal".to_owned(),
        before_state_explicit: false,
    }
}

/// A recovered same-block observation reaches the writer after a later one of the same name.
#[tokio::test]
async fn witness_is_the_earliest_same_block_preimage_not_the_first_written() -> TestResult {
    let database = database("interpret_surface_witness_order").await?;
    let id = "ens:0xrecovered";
    let observed_at = |identity: &str| {
        let mut row = surface(id, "recovered");
        if let Some(raw) = row.raw.as_mut() {
            raw.preimage_event_identity = identity.to_owned();
        }
        row
    };
    let output = BatchOutput {
        name_surfaces: vec![observed_at("preimage:log-7"), observed_at("preimage:log-3")],
        normalized_events: vec![
            preimage_event("preimage:log-3", id, 3),
            preimage_event("preimage:other-name", "ens:0xother", 1),
            preimage_event("preimage:log-7", id, 7),
        ],
        ..BatchOutput::default()
    };

    write_output(&database, &output).await?;
    assert_eq!(
        stored(&database, id).await?.3.as_deref(),
        Some("preimage:log-3")
    );
    write_output(&database, &output).await?;
    assert_eq!(
        stored(&database, id).await?.3.as_deref(),
        Some("preimage:log-3")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn legacy_byte_shadow_gains_its_path_only_with_the_rederived_raw_bundle() -> TestResult {
    use alloy_primitives::{B256, keccak256};
    let database = database("interpret_surface_legacy_byte_path").await?;
    add_block(&database, 2).await?;
    let labels = [b"\xff".as_slice(), b"eth"];
    let hashes = labels
        .map(|label| format!("{:#x}", keccak256(label)))
        .to_vec();
    let namehash = labels.iter().rev().fold(B256::ZERO, |node, label| {
        keccak256([node.as_slice(), keccak256(label).as_slice()].concat())
    });
    let id = format!("ens:{namehash:#x}");
    let mut legacy = shadow(at(surface(&id, ""), 2), 2)?;
    legacy.labelhashes.clear();
    let raw = legacy.raw.as_mut().unwrap();
    raw.raw_labels.clear();
    raw.dns_encoded_name = vec![1, 0xff, 3, b'e', b't', b'h', 0];
    raw.preimage_event_identity = "legacy-witness".into();
    write_one(&database, legacy.clone()).await?;
    let mut structural = hash_path_only(&id, "");
    structural.labelhashes = hashes.clone();
    write_one(&database, structural.clone()).await?;
    let path: Vec<String> =
        sqlx::query_scalar("SELECT labelhashes FROM name_surfaces WHERE logical_name_id=$1")
            .bind(&id)
            .fetch_one(database.pool())
            .await?;
    assert!(
        path.is_empty(),
        "a hash-only observation must not repair a legacy raw bundle"
    );
    assert_eq!(stored(&database, &id).await?.4, "shadow");
    let mut rederived = legacy;
    rederived.labelhashes = hashes.clone();
    rederived.raw.as_mut().unwrap().preimage_event_identity = "rederived-witness".into();
    write_one(&database, rederived).await?;
    write_one(&database, structural).await?;
    let path: Vec<String> =
        sqlx::query_scalar("SELECT labelhashes FROM name_surfaces WHERE logical_name_id=$1")
            .bind(&id)
            .fetch_one(database.pool())
            .await?;
    assert_eq!(path, hashes);
    let row = stored(&database, &id).await?;
    assert_eq!(row.0.as_deref(), Some(""));
    assert_eq!(row.1, Some(Vec::new()));
    assert_eq!(row.3.as_deref(), Some("rederived-witness"));
    assert_eq!(row.4, "shadow");
    assert_eq!(row.5, 1);
    database.cleanup().await?;
    Ok(())
}
