use bigname_adapters::schema_v2::BatchOutput;
use sqlx::{Postgres, QueryBuilder, Transaction};
use std::collections::HashSet;

use crate::{InterpretError, NORMALIZATION_STATE_REPAIR_REASON, Result};

use super::batching::{batch_row_context, conflict_free_batches};

pub(super) async fn write(
    transaction: &mut Transaction<'_, Postgres>,
    output: &BatchOutput,
) -> Result<()> {
    write_preimages(transaction, output).await?;
    write_surfaces(transaction, output).await
}

async fn write_preimages(
    transaction: &mut Transaction<'_, Postgres>,
    output: &BatchOutput,
) -> Result<()> {
    let preimages = preimages_for_submission(output).collect::<Vec<_>>();
    for (start, batch) in conflict_free_batches(&preimages, |preimage| preimage.labelhash.clone()) {
        let mut query = QueryBuilder::<Postgres>::new(
            "
            INSERT INTO label_preimages (
                labelhash, raw_label, decoded_label, normalizer_version,
                normalized_under_version, normalization_error,
                source_kind, source_priority, provenance
            )
            ",
        );
        query.push_values(batch, |mut row, preimage| {
            row.push_bind(&preimage.labelhash)
                .push_bind(&preimage.raw_label)
                .push_bind(&preimage.decoded_label)
                .push_bind(&preimage.normalizer_version)
                .push_bind(preimage.normalized_under_version)
                .push_bind(&preimage.normalization_error)
                .push_bind(&preimage.source_kind)
                .push_bind(preimage.source_priority)
                .push_bind(&preimage.provenance);
        });
        query.push(
            "
            ON CONFLICT (labelhash) DO UPDATE
            SET source_kind = CASE
                    WHEN EXCLUDED.source_priority >= label_preimages.source_priority
                        THEN EXCLUDED.source_kind
                    ELSE label_preimages.source_kind
                END,
                source_priority = GREATEST(
                    label_preimages.source_priority,
                    EXCLUDED.source_priority
                ),
                provenance = CASE
                    WHEN EXCLUDED.source_priority >= label_preimages.source_priority
                        THEN EXCLUDED.provenance
                    ELSE label_preimages.provenance
                END,
                observed_at = now()
            WHERE label_preimages.raw_label = EXCLUDED.raw_label
              AND label_preimages.decoded_label IS NOT DISTINCT FROM EXCLUDED.decoded_label
              AND label_preimages.normalizer_version = EXCLUDED.normalizer_version
              AND label_preimages.normalized_under_version = EXCLUDED.normalized_under_version
              AND label_preimages.normalization_error IS NOT DISTINCT FROM EXCLUDED.normalization_error
            RETURNING labelhash
            ",
        );
        let written = query
            .build_query_scalar::<String>()
            .fetch_all(&mut **transaction)
            .await
            .map_err(|error| {
                let context =
                    batch_row_context(start, batch.iter().map(|preimage| &preimage.labelhash));
                InterpretError::database(
                    format!("failed to write raw-label-preimage batch; {context}"),
                    error,
                )
            })?
            .into_iter()
            .collect::<HashSet<_>>();
        let conflicting = batch
            .iter()
            .enumerate()
            .filter(|(_, preimage)| !written.contains(&preimage.labelhash))
            .map(|(offset, preimage)| format!("{}={}", start + offset, preimage.labelhash))
            .collect::<Vec<_>>();
        if !conflicting.is_empty() {
            return Err(InterpretError::data_integrity(format!(
                "label hashes are already bound to different raw bytes or normalization state; conflicting batch rows [{}]; {}",
                conflicting.join(", "),
                NORMALIZATION_STATE_REPAIR_REASON
            )));
        }
    }
    Ok(())
}

fn preimages_for_submission(
    output: &BatchOutput,
) -> impl Iterator<Item = &bigname_adapters::schema_v2::LabelPreimage> {
    output
        .label_preimages
        .iter()
        .filter(|preimage| !preimage.raw_label.is_empty())
}

async fn write_surfaces(
    transaction: &mut Transaction<'_, Postgres>,
    output: &BatchOutput,
) -> Result<()> {
    for (start, batch) in conflict_free_batches(&output.name_surfaces, |surface| {
        surface.logical_name_id.clone()
    }) {
        let mut query = QueryBuilder::<Postgres>::new(
            "
            INSERT INTO name_surfaces (
                logical_name_id, namespace, raw_name, raw_labels,
                dns_encoded_name, namehash, labelhashes, normalizer_version,
                visibility_state, normalization_errors, deactivation_reason,
                deactivated_at, chain_id, block_hash, block_number,
                provenance, canonicality_state
            ) ",
        );
        query.push_values(batch, |mut row, surface| {
            row.push_bind(&surface.logical_name_id)
                .push_bind(&surface.namespace)
                .push_bind(&surface.raw_name)
                .push_bind(&surface.raw_labels)
                .push_bind(&surface.dns_encoded_name)
                .push_bind(&surface.namehash)
                .push_bind(&surface.labelhashes)
                .push_bind(&surface.normalizer_version)
                .push_bind(&surface.visibility_state)
                .push_bind(&surface.normalization_errors)
                .push_bind(&surface.deactivation_reason)
                .push_bind(surface.deactivated_at)
                .push_bind(&surface.chain_id)
                .push_bind(&surface.block_hash)
                .push_bind(surface.block_number)
                .push_bind(&surface.provenance)
                .push_bind(&surface.canonicality_state)
                .push_unseparated("::canonicality_state");
        });
        query.push(
            "
            ON CONFLICT (logical_name_id) DO UPDATE
            SET normalizer_version = EXCLUDED.normalizer_version,
                visibility_state = EXCLUDED.visibility_state,
                normalization_errors = EXCLUDED.normalization_errors,
                deactivation_reason = EXCLUDED.deactivation_reason,
                block_hash = CASE
                    WHEN name_surfaces.canonicality_state = 'orphaned'
                      OR EXCLUDED.block_number < name_surfaces.block_number
                        THEN EXCLUDED.block_hash
                    ELSE name_surfaces.block_hash
                END,
                block_number = CASE
                    WHEN name_surfaces.canonicality_state = 'orphaned'
                      OR EXCLUDED.block_number < name_surfaces.block_number
                        THEN EXCLUDED.block_number
                    ELSE name_surfaces.block_number
                END,
                provenance = CASE
                    WHEN name_surfaces.canonicality_state = 'orphaned'
                      OR EXCLUDED.block_number < name_surfaces.block_number
                        THEN EXCLUDED.provenance
                    ELSE name_surfaces.provenance
                END,
                deactivated_at = CASE
                    WHEN name_surfaces.canonicality_state = 'orphaned'
                      OR EXCLUDED.block_number < name_surfaces.block_number
                        THEN EXCLUDED.deactivated_at
                    ELSE name_surfaces.deactivated_at
                END,
                canonicality_state = CASE
                    WHEN name_surfaces.canonicality_state = 'orphaned'
                      OR EXCLUDED.block_number < name_surfaces.block_number
                      OR (
                          EXCLUDED.block_number = name_surfaces.block_number
                          AND EXCLUDED.block_hash = name_surfaces.block_hash
                      )
                        THEN EXCLUDED.canonicality_state
                    ELSE name_surfaces.canonicality_state
                END,
                observed_at = CASE
                    WHEN name_surfaces.canonicality_state = 'orphaned'
                      OR EXCLUDED.block_number < name_surfaces.block_number
                        THEN now()
                    ELSE name_surfaces.observed_at
                END
            WHERE name_surfaces.namespace = EXCLUDED.namespace
              AND name_surfaces.raw_name = EXCLUDED.raw_name
              AND name_surfaces.raw_labels = EXCLUDED.raw_labels
              AND name_surfaces.dns_encoded_name = EXCLUDED.dns_encoded_name
              AND name_surfaces.namehash = EXCLUDED.namehash
              AND name_surfaces.labelhashes = EXCLUDED.labelhashes
              AND name_surfaces.chain_id = EXCLUDED.chain_id
              AND (
                    name_surfaces.canonicality_state = 'orphaned'
                OR (
                    name_surfaces.normalizer_version = EXCLUDED.normalizer_version
                AND name_surfaces.visibility_state = EXCLUDED.visibility_state
                AND name_surfaces.normalization_errors = EXCLUDED.normalization_errors
                AND name_surfaces.deactivation_reason IS NOT DISTINCT FROM EXCLUDED.deactivation_reason
                  )
              )
            RETURNING logical_name_id
            ",
        );
        let written = query
            .build_query_scalar::<String>()
            .fetch_all(&mut **transaction)
            .await
            .map_err(|error| {
                let context =
                    batch_row_context(start, batch.iter().map(|surface| &surface.logical_name_id));
                InterpretError::database(
                    format!("failed to write raw-name-identity batch; {context}"),
                    error,
                )
            })?
            .into_iter()
            .collect::<HashSet<_>>();
        let conflicting = batch
            .iter()
            .enumerate()
            .filter(|(_, surface)| !written.contains(&surface.logical_name_id))
            .map(|(offset, surface)| format!("{}={}", start + offset, surface.logical_name_id))
            .collect::<Vec<_>>();
        if !conflicting.is_empty() {
            return Err(InterpretError::data_integrity(format!(
                "logical name IDs are already bound to different raw identity or normalization state; conflicting batch rows [{}]; {}",
                conflicting.join(", "),
                NORMALIZATION_STATE_REPAIR_REASON
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use bigname_adapters::schema_v2::{LabelPreimage, NameSurface};
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};
    use serde_json::json;

    use super::*;

    type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

    fn preimage(raw_label: &[u8]) -> LabelPreimage {
        LabelPreimage {
            labelhash: format!("test-{}", raw_label.len()),
            raw_label: raw_label.to_vec(),
            decoded_label: std::str::from_utf8(raw_label).ok().map(str::to_owned),
            normalizer_version: "test".to_owned(),
            normalized_under_version: false,
            normalization_error: Some("test".to_owned()),
            source_kind: "test".to_owned(),
            source_priority: 0,
            provenance: json!({}),
        }
    }

    #[test]
    fn empty_label_preimages_are_filtered_before_submission() {
        let output = BatchOutput {
            label_preimages: vec![preimage(b""), preimage(b"alice")],
            ..BatchOutput::default()
        };

        let submitted = preimages_for_submission(&output)
            .map(|preimage| preimage.raw_label.as_slice())
            .collect::<Vec<_>>();
        assert_eq!(submitted, vec![b"alice".as_slice()]);
    }

    #[tokio::test]
    async fn values_boundary_and_idempotent_replay_preserve_name_rows() -> TestResult {
        let database = TestDatabase::create(TestDatabaseConfig::new(
            "interpret_identity_names_values_boundary",
        ))
        .await?;
        for sql in [
            include_str!("../../../../schema-v2/baseline/01_chain.sql"),
            include_str!("../../../../schema-v2/baseline/03_identity.sql"),
            include_str!("../../../../schema-v2/baseline/07_labels.sql"),
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
        let output = BatchOutput {
            label_preimages: (0..501)
                .map(|index| {
                    let label = format!("label-{index:03}");
                    LabelPreimage {
                        labelhash: format!("0xlabel{index:03}"),
                        raw_label: label.as_bytes().to_vec(),
                        decoded_label: Some(label),
                        normalizer_version: "test".to_owned(),
                        normalized_under_version: true,
                        normalization_error: None,
                        source_kind: "chain_observation".to_owned(),
                        source_priority: 100,
                        provenance: json!({"row": index}),
                    }
                })
                .collect(),
            name_surfaces: (0..501)
                .map(|index| {
                    let namehash = format!("0xname{index:03}");
                    NameSurface {
                        logical_name_id: format!("ens:{namehash}"),
                        namespace: "ens".to_owned(),
                        raw_name: format!("name-{index:03}.eth"),
                        raw_labels: vec![format!("name-{index:03}"), "eth".to_owned()],
                        dns_encoded_name: vec![index as u8],
                        namehash,
                        labelhashes: vec![format!("0xlabel{index:03}"), "0xeth".to_owned()],
                        normalizer_version: "test".to_owned(),
                        visibility_state: "active".to_owned(),
                        normalization_errors: json!([]),
                        deactivation_reason: None,
                        deactivated_at: None,
                        chain_id: "batch-test".to_owned(),
                        block_hash: "0x01".to_owned(),
                        block_number: 1,
                        provenance: json!({"row": index}),
                        canonicality_state: "canonical".to_owned(),
                    }
                })
                .collect(),
            ..BatchOutput::default()
        };
        let mut transaction = database.pool().begin().await?;
        write(&mut transaction, &output).await?;
        transaction.commit().await?;
        let mut replay = database.pool().begin().await?;
        write(&mut replay, &output).await?;
        replay.commit().await?;

        let counts: (i64, i64) = sqlx::query_as(
            "SELECT
                 (SELECT count(*) FROM label_preimages),
                 (SELECT count(*) FROM name_surfaces)",
        )
        .fetch_one(database.pool())
        .await?;
        assert_eq!(counts, (501, 501));
        let last: (String, String, serde_json::Value) = sqlx::query_as(
            "SELECT logical_name_id, raw_name, provenance
             FROM name_surfaces ORDER BY logical_name_id DESC LIMIT 1",
        )
        .fetch_one(database.pool())
        .await?;
        assert_eq!(
            last,
            (
                "ens:0xname500".to_owned(),
                "name-500.eth".to_owned(),
                json!({"row": 500}),
            )
        );
        database.cleanup().await?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "identity_names/coverage_tests.rs"]
mod coverage_tests;
