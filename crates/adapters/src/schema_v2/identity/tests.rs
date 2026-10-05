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

mod node_identity {
    use std::collections::BTreeMap;

    use alloy_primitives::keccak256;

    use super::super::node::{self, NodeIdentityDraft};
    use crate::schema_v2::{
        catalog::Selected,
        common::{hash_hex, namehash},
        manifest::{ManifestEvent, ManifestSource},
        model::{BatchOutput, RawLogInput},
        state::State,
    };

    fn selected() -> Selected {
        let event = ManifestEvent {
            name: "NewOwner".to_owned(),
            signature: "NewOwner(bytes32,bytes32,address)".to_owned(),
            topic0: format!("{:#x}", keccak256(b"NewOwner(bytes32,bytes32,address)")),
            emitter_roles: vec!["registry".to_owned()],
            normalized_events: vec!["SubregistryChanged".to_owned()],
        };
        Selected {
            source: ManifestSource {
                manifest_id: 1,
                manifest_version: 1,
                namespace: "ens".to_owned(),
                source_family: "ens_v1_registry_l1".to_owned(),
                chain_id: "ethereum-sepolia".to_owned(),
                deployment_label: "unit-test".to_owned(),
                correlation_addresses: BTreeMap::new(),
                resolver_implementations: BTreeMap::new(),
                universal_resolver_implementations: Vec::new(),
                universal_resolver_proxies: Vec::new(),
                events: vec![event.clone()],
            },
            event,
            contract_instance_id: uuid::Uuid::from_u128(1),
            emitter_role: Some("registry".to_owned()),
            match_all: false,
            manifest_declared_emitter: true,
        }
    }

    fn raw() -> RawLogInput {
        RawLogInput {
            chain_id: "ethereum-sepolia".to_owned(),
            block_hash: format!("0x{:064x}", 7_u64),
            block_number: 7,
            block_timestamp: time::OffsetDateTime::from_unix_timestamp(1_700_000_000)
                .expect("timestamp"),
            canonicality_state: "canonical".to_owned(),
            transaction_hash: format!("0x{:064x}", 70_u64),
            transaction_index: 0,
            log_index: 3,
            emitting_address: "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned(),
            topics: Vec::new(),
            data: Vec::new(),
        }
    }

    fn draft() -> NodeIdentityDraft {
        let labels = ["sub".to_owned(), "leon".to_owned(), "eth".to_owned()];
        NodeIdentityDraft {
            labelhashes: labels
                .iter()
                .map(|label| hash_hex(label.as_bytes()))
                .collect(),
            namehash: namehash(&labels),
        }
    }

    #[test]
    fn hash_path_draft_becomes_a_surface_without_raw_bytes_or_preimage() -> anyhow::Result<()> {
        let draft = draft();
        let mut state = State::new(Vec::new(), Vec::new());
        let mut output = BatchOutput::default();
        node::materialize(
            &selected(),
            &raw(),
            std::slice::from_ref(&draft),
            &serde_json::json!({"log_index": 3}),
            &mut state,
            &mut output,
        )?;

        let [surface] = output.name_surfaces.as_slice() else {
            panic!("one surface expected");
        };
        assert_eq!(surface.logical_name_id, format!("ens:{}", draft.namehash));
        assert_eq!(surface.raw, None);
        assert_eq!(surface.labelhashes, draft.labelhashes);
        assert_eq!(surface.visibility_state, "active");
        assert_eq!(surface.normalization_errors, serde_json::json!([]));
        assert_eq!(surface.block_number, 7);
        assert!(output.normalized_events.is_empty());
        assert!(output.label_preimages.is_empty());
        assert!(state.v1_active_surface_materialized("ens", &draft.namehash));
        Ok(())
    }

    #[test]
    fn draft_whose_path_does_not_hash_to_its_node_is_rejected() {
        for broken in [
            NodeIdentityDraft {
                labelhashes: draft().labelhashes[1..].to_vec(),
                ..draft()
            },
            NodeIdentityDraft {
                labelhashes: Vec::new(),
                ..draft()
            },
            NodeIdentityDraft {
                labelhashes: vec!["[sub]".to_owned(), "eth".to_owned()],
                ..draft()
            },
        ] {
            let mut output = BatchOutput::default();
            let result = node::materialize(
                &selected(),
                &raw(),
                &[broken],
                &serde_json::json!({}),
                &mut State::new(Vec::new(), Vec::new()),
                &mut output,
            );
            assert!(result.is_err());
            assert!(output.name_surfaces.is_empty());
        }
    }
}
