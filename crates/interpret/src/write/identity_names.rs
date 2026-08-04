use bigname_adapters::schema_v2::BatchOutput;
use sqlx::{Postgres, Transaction};

use crate::{InterpretError, NORMALIZATION_STATE_REPAIR_REASON, Result};

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
    for preimage in preimages_for_submission(output) {
        let written: Option<String> = sqlx::query_scalar(
            "
            INSERT INTO label_preimages (
                labelhash, raw_label, decoded_label, normalizer_version,
                normalized_under_version, normalization_error,
                source_kind, source_priority, provenance
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
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
        )
        .bind(&preimage.labelhash)
        .bind(&preimage.raw_label)
        .bind(&preimage.decoded_label)
        .bind(&preimage.normalizer_version)
        .bind(preimage.normalized_under_version)
        .bind(&preimage.normalization_error)
        .bind(&preimage.source_kind)
        .bind(preimage.source_priority)
        .bind(&preimage.provenance)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|error| InterpretError::database("failed to write raw label preimage", error))?;
        if written.is_none() {
            return Err(InterpretError::data_integrity(format!(
                "label hash {} is already bound to different raw bytes or normalization state; {}",
                preimage.labelhash, NORMALIZATION_STATE_REPAIR_REASON
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
    for surface in &output.name_surfaces {
        let written: Option<String> = sqlx::query_scalar(
            "
            INSERT INTO name_surfaces (
                logical_name_id, namespace, raw_name, raw_labels,
                dns_encoded_name, namehash, labelhashes, normalizer_version,
                visibility_state, normalization_errors, deactivation_reason,
                deactivated_at, chain_id, block_hash, block_number,
                provenance, canonicality_state
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17::canonicality_state
            )
            ON CONFLICT (logical_name_id) DO UPDATE
            SET block_hash = CASE
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
              AND name_surfaces.normalizer_version = EXCLUDED.normalizer_version
              AND name_surfaces.visibility_state = EXCLUDED.visibility_state
              AND name_surfaces.normalization_errors = EXCLUDED.normalization_errors
              AND name_surfaces.deactivation_reason IS NOT DISTINCT FROM EXCLUDED.deactivation_reason
              AND name_surfaces.chain_id = EXCLUDED.chain_id
            RETURNING logical_name_id
            ",
        )
        .bind(&surface.logical_name_id)
        .bind(&surface.namespace)
        .bind(&surface.raw_name)
        .bind(&surface.raw_labels)
        .bind(&surface.dns_encoded_name)
        .bind(&surface.namehash)
        .bind(&surface.labelhashes)
        .bind(&surface.normalizer_version)
        .bind(&surface.visibility_state)
        .bind(&surface.normalization_errors)
        .bind(&surface.deactivation_reason)
        .bind(surface.deactivated_at)
        .bind(&surface.chain_id)
        .bind(&surface.block_hash)
        .bind(surface.block_number)
        .bind(&surface.provenance)
        .bind(&surface.canonicality_state)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|error| InterpretError::database("failed to write raw name identity", error))?;
        if written.is_none() {
            return Err(InterpretError::data_integrity(format!(
                "logical name ID {} is already bound to different raw identity or normalization state; {}",
                surface.logical_name_id, NORMALIZATION_STATE_REPAIR_REASON
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use bigname_adapters::schema_v2::LabelPreimage;
    use serde_json::json;

    use super::*;

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
}
