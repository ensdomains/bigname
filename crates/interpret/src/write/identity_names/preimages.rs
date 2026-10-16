use std::collections::HashMap;

use bigname_adapters::schema_v2::{BatchOutput, LabelPreimage};

use crate::{InterpretError, NORMALIZATION_STATE_REPAIR_REASON, Result};

/// Fold only the mutable source evidence: bytes and normalization must agree in
/// every observation. The SQL writer keeps the greatest priority and the last
/// observation at that priority. `now()` is stable throughout its transaction.
pub(super) fn deduplicate(output: &BatchOutput) -> Result<Vec<&LabelPreimage>> {
    let mut positions = HashMap::<&str, usize>::new();
    let mut selected = Vec::<&LabelPreimage>::new();
    for (index, preimage) in super::preimages_for_submission(output).enumerate() {
        // These source fields differ between otherwise identical observations.
        // Validate even discarded sources so folding cannot hide a table CHECK
        // violation that the original sequential submissions would reject.
        if preimage.source_priority < 0
            || preimage.source_kind.trim_matches(' ').is_empty()
            || preimage.source_kind.contains('\0')
            || !preimage.provenance.is_object()
            || json_contains_nul(&preimage.provenance)
        {
            return Err(InterpretError::data_integrity(format!(
                "raw-label-preimage row {index}={} has invalid source evidence",
                preimage.labelhash
            )));
        }
        let Some(&position) = positions.get(preimage.labelhash.as_str()) else {
            positions.insert(&preimage.labelhash, selected.len());
            selected.push(preimage);
            continue;
        };
        let prior = selected[position];
        if prior.raw_label != preimage.raw_label
            || prior.decoded_label != preimage.decoded_label
            || prior.normalizer_version != preimage.normalizer_version
            || prior.normalized_under_version != preimage.normalized_under_version
            || prior.normalization_error != preimage.normalization_error
        {
            return Err(InterpretError::data_integrity(format!(
                "label hashes are already bound to different raw bytes or normalization state; conflicting batch row [{index}={}]; {}",
                preimage.labelhash, NORMALIZATION_STATE_REPAIR_REASON
            )));
        }
        if preimage.source_priority >= prior.source_priority {
            selected[position] = preimage;
        }
    }
    Ok(selected)
}

// PostgreSQL text and jsonb cannot represent U+0000, including in object keys.
// Inspect discarded source metadata too, without recursively consuming the stack.
fn json_contains_nul(value: &serde_json::Value) -> bool {
    let mut pending = vec![value];
    while let Some(value) = pending.pop() {
        match value {
            serde_json::Value::String(value) if value.contains('\0') => return true,
            serde_json::Value::Array(values) => pending.extend(values),
            serde_json::Value::Object(fields) => {
                if fields.keys().any(|key| key.contains('\0')) {
                    return true;
                }
                pending.extend(fields.values());
            }
            _ => {}
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn label(hash: &str, priority: i32, source: &str) -> LabelPreimage {
        LabelPreimage {
            labelhash: hash.into(),
            raw_label: b"same".to_vec(),
            decoded_label: Some("same".into()),
            normalizer_version: "test".into(),
            normalized_under_version: true,
            normalization_error: None,
            source_kind: source.into(),
            source_priority: priority,
            provenance: json!({"source": source}),
        }
    }

    #[test]
    fn source_winners_keep_first_key_order_and_last_highest_priority() -> Result<()> {
        let output = BatchOutput {
            label_preimages: vec![
                label("one", 2, "first"),
                label("two", 1, "other"),
                label("one", 5, "higher"),
                label("one", 3, "lower"),
                label("one", 5, "tie"),
                label("two", 0, "last-lower"),
            ],
            ..Default::default()
        };
        let selected = deduplicate(&output)?;
        assert_eq!(selected.len(), 2);
        assert!(std::ptr::eq(selected[0], &output.label_preimages[4]));
        assert!(std::ptr::eq(selected[1], &output.label_preimages[1]));
        Ok(())
    }

    #[test]
    fn every_immutable_field_is_checked_even_when_a_later_row_restores_it() {
        for field in 0..5 {
            let first = label("one", 10, "first");
            let mut different = label("one", 1, "different");
            match field {
                0 => different.raw_label = b"different".to_vec(),
                1 => different.decoded_label = None,
                2 => different.normalizer_version = "different".into(),
                3 => different.normalized_under_version = false,
                4 => different.normalization_error = Some("different".into()),
                _ => unreachable!(),
            }
            let output = BatchOutput {
                label_preimages: vec![first.clone(), different, first],
                ..Default::default()
            };
            assert_eq!(
                deduplicate(&output).unwrap_err().kind(),
                crate::ErrorKind::DataIntegrity
            );
        }
    }

    #[test]
    fn discarded_invalid_sources_still_fail() {
        for field in 0..6 {
            let mut invalid = label("one", 1, "discarded");
            match field {
                0 => invalid.source_priority = -1,
                1 => invalid.source_kind = "   ".into(),
                2 => invalid.provenance = json!([]),
                3 => invalid.source_kind = "bad\0source".into(),
                4 => invalid.provenance = json!({"nested": [{"source": "bad\0value"}]}),
                5 => invalid.provenance = json!({"nested": [{"bad\0key": "value"}]}),
                _ => unreachable!(),
            }
            let output = BatchOutput {
                label_preimages: vec![label("one", 5, "kept"), invalid],
                ..Default::default()
            };
            assert!(deduplicate(&output).is_err());
        }
    }

    #[test]
    fn repeated_common_suffix_is_submitted_once_and_empty_labels_stay_omitted() -> Result<()> {
        let mut output = BatchOutput::default();
        for _ in 0..10_000 {
            output.label_preimages.push(label("eth", 1, "common"));
        }
        let mut empty = label("empty", -1, "");
        empty.raw_label.clear();
        output.label_preimages.push(empty);
        assert_eq!(deduplicate(&output)?.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn folded_sources_match_sequential_sql_with_existing_and_new_labels()
    -> super::super::coverage_tests::TestResult {
        use super::super::{coverage_tests::database, write, write_preimages};

        let db = database("interpret_preimage_fold_parity").await?;
        let seed = [
            label("stored-higher", 9, "stored"),
            label("stored-tie", 5, "stored"),
            label("stored-lower", 1, "stored"),
        ];
        let mut tx = db.pool().begin().await?;
        write_preimages(&mut tx, &seed.iter().collect::<Vec<_>>()).await?;
        tx.commit().await?;
        let mut output = BatchOutput::default();
        for hash in ["stored-higher", "stored-tie", "stored-lower", "new"] {
            output.label_preimages.extend([
                label(hash, 2, "first"),
                label(hash, 5, "higher"),
                label(hash, 1, "lower"),
                label(hash, 5, "last-tie"),
            ]);
        }
        let snapshot = "SELECT (to_jsonb(preimage)-'observed_at'-'inserted_at')::text
                        FROM label_preimages preimage ORDER BY labelhash";
        // The unfolded submissions use the existing SQL writer and its original
        // sequential conflict behavior as the reference, within a rolled-back tx.
        let mut reference = db.pool().begin().await?;
        write_preimages(
            &mut reference,
            &output.label_preimages.iter().collect::<Vec<_>>(),
        )
        .await?;
        let expected: Vec<String> = sqlx::query_scalar(snapshot)
            .fetch_all(&mut *reference)
            .await?;
        reference.rollback().await?;

        let mut optimized = db.pool().begin().await?;
        write(&mut optimized, &output).await?;
        let actual: Vec<String> = sqlx::query_scalar(snapshot)
            .fetch_all(&mut *optimized)
            .await?;
        assert_eq!(actual, expected);
        optimized.rollback().await?;
        db.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn conflicting_repeat_is_rejected_before_a_later_valid_repeat_can_hide_it()
    -> super::super::coverage_tests::TestResult {
        use super::super::{coverage_tests::database, write};

        let db = database("interpret_preimage_fold_conflict").await?;
        let valid = label("conflict", 10, "valid");
        let mut invalid = label("conflict", 1, "different");
        invalid.raw_label = b"different".to_vec();
        invalid.decoded_label = Some("different".into());
        let output = BatchOutput {
            label_preimages: vec![label("prefix", 1, "prefix"), valid.clone(), invalid, valid],
            ..Default::default()
        };
        let mut tx = db.pool().begin().await?;
        let error = write(&mut tx, &output).await.unwrap_err();
        assert_eq!(error.kind(), crate::ErrorKind::DataIntegrity);
        assert!(error.to_string().contains("2=conflict"));
        tx.rollback().await?;
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM label_preimages")
            .fetch_one(db.pool())
            .await?;
        assert_eq!(count, 0);
        db.cleanup().await?;
        Ok(())
    }
}
