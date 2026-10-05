use serde_json::json;

use super::*;

fn preimage(labelhash: &str, raw_label: &[u8], priority: i32, ordinal: i64) -> LabelPreimage {
    LabelPreimage {
        labelhash: labelhash.to_owned(),
        raw_label: raw_label.to_vec(),
        decoded_label: Some(String::from_utf8(raw_label.to_vec()).expect("UTF-8")),
        normalizer_version: "test-normalizer".to_owned(),
        normalized_under_version: true,
        normalization_error: None,
        source_kind: "LabelReserved_label".to_owned(),
        source_priority: priority,
        provenance: json!({"ordinal":ordinal}),
    }
}

fn other_preimage(hash: &str, label: &[u8], priority: i32, ordinal: i64) -> LabelPreimage {
    LabelPreimage {
        source_kind: "Resolver_name".to_owned(),
        ..preimage(hash, label, priority, ordinal)
    }
}

fn sequential_winners(observations: &[LabelPreimage]) -> BTreeMap<String, LabelPreimage> {
    let mut winners = BTreeMap::<String, LabelPreimage>::new();
    for candidate in observations {
        if winners
            .get(&candidate.labelhash)
            .is_none_or(|winner| candidate.source_priority >= winner.source_priority)
        {
            winners.insert(candidate.labelhash.clone(), candidate.clone());
        }
    }
    winners
}

#[test]
fn reserved_label_preimage_compaction_preserves_writer_winner_and_position() -> anyhow::Result<()> {
    let mut output = BatchOutput {
        label_preimages: vec![
            preimage("hash-a", b"a", 100, 1),
            preimage("hash-b", b"b", 100, 2),
            preimage("hash-a", b"a", 90, 3),
            preimage("hash-a", b"a", 200, 4),
            preimage("hash-a", b"a", 200, 5),
        ],
        ..BatchOutput::default()
    };

    compact_reserved_label_preimages(&mut output)?;

    assert_eq!(output.label_preimages.len(), 2);
    assert_eq!(output.label_preimages[0].labelhash, "hash-b");
    assert_eq!(output.label_preimages[1].labelhash, "hash-a");
    assert_eq!(output.label_preimages[1].source_priority, 200);
    assert_eq!(output.label_preimages[1].provenance, json!({"ordinal":5}));
    Ok(())
}

#[test]
fn label_preimage_compaction_rejects_inconsistent_same_hash_observations() {
    let mut output = BatchOutput {
        label_preimages: vec![
            preimage("hash-a", b"a", 100, 1),
            preimage("hash-a", b"different", 200, 2),
        ],
        ..BatchOutput::default()
    };

    let error =
        compact_reserved_label_preimages(&mut output).expect_err("conflict must be rejected");
    assert!(
        error
            .to_string()
            .contains("inconsistent preimage observations"),
        "unexpected error: {error:#}"
    );
}

#[test]
fn interleaved_compaction_matches_writer() -> anyhow::Result<()> {
    let original = vec![
        preimage("hash-a", b"a", 100, 1),
        other_preimage("hash-a", b"a", 100, 2),
        preimage("hash-a", b"a", 100, 3),
        other_preimage("hash-a", b"a", 90, 4),
        preimage("hash-a", b"a", 200, 5),
        other_preimage("hash-a", b"a", 200, 6),
        preimage("hash-a", b"a", 200, 7),
        preimage("hash-b", b"b", 300, 8),
        other_preimage("hash-b", b"b", 300, 9),
        preimage("hash-b", b"b", 200, 10),
    ];
    let expected_winners = sequential_winners(&original);
    let expected_other_rows = original
        .iter()
        .filter(|observation| observation.source_kind != "LabelReserved_label")
        .count();
    let mut output = BatchOutput {
        label_preimages: original,
        ..BatchOutput::default()
    };

    compact_reserved_label_preimages(&mut output)?;

    assert_eq!(
        sequential_winners(&output.label_preimages),
        expected_winners
    );
    assert_eq!(
        output
            .label_preimages
            .iter()
            .filter(|observation| observation.source_kind != "LabelReserved_label")
            .count(),
        expected_other_rows,
        "compaction must retain every non-reservation submission"
    );
    Ok(())
}
