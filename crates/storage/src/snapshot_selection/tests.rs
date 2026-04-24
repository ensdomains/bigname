use serde_json::json;

use super::*;

fn scope() -> SnapshotSelectionScope {
    SnapshotSelectionScope::new(
        vec![SnapshotPositionRequirement::new(
            "ethereum",
            "ethereum-mainnet",
        )],
        Some("ethereum".to_owned()),
    )
    .expect("test scope must be valid")
}

#[test]
fn explicit_chain_positions_reject_duplicate_slots() {
    let error = ChainPositions::parse_explicit_json(
        r#"{
                "ethereum": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 1,
                    "block_hash": "0x1",
                    "timestamp": "2026-04-17T00:00:01Z"
                },
                "ethereum": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 2,
                    "block_hash": "0x2",
                    "timestamp": "2026-04-17T00:00:02Z"
                }
            }"#,
        &scope(),
    )
    .expect_err("duplicate slots must be invalid");

    assert_eq!(error.kind(), SnapshotSelectionErrorKind::InvalidInput);
    assert!(error.message().contains("repeats position slot ethereum"));
}

#[test]
fn explicit_chain_positions_reject_missing_and_wrong_profile_slots() {
    let missing = ChainPositions::parse_explicit_json("{}", &scope())
        .expect_err("missing required slot must be invalid");
    assert_eq!(missing.kind(), SnapshotSelectionErrorKind::InvalidInput);

    let wrong_chain = ChainPositions::parse_explicit_json(
        r#"{
                "ethereum": {
                    "chain_id": "ethereum-sepolia",
                    "block_number": 1,
                    "block_hash": "0x1",
                    "timestamp": "2026-04-17T00:00:01Z"
                }
            }"#,
        &scope(),
    )
    .expect_err("mixed profile chain must be invalid");
    assert_eq!(wrong_chain.kind(), SnapshotSelectionErrorKind::InvalidInput);
    assert!(wrong_chain.message().contains("expected ethereum-mainnet"));
}

#[test]
fn projection_chain_positions_match_by_chain_identity() {
    let selected = ChainPositions::from_value(&json!({
        "ethereum": {
            "chain_id": "ethereum-mainnet",
            "block_number": 7,
            "block_hash": "0x7",
            "timestamp": "2026-04-17T00:00:07Z"
        }
    }))
    .expect("selected positions must decode");
    let projected = json!({
        "ethereum-mainnet": {
            "chain_id": "ethereum-mainnet",
            "block_number": 7,
            "block_hash": "0x7",
            "timestamp": "2026-04-17T00:00:07Z"
        }
    });

    ensure_projection_chain_positions_match("name_current", &projected, &selected)
        .expect("slot aliases with the same chain identity should match");

    let stale = ensure_projection_chain_positions_match(
        "name_current",
        &json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 8,
                "block_hash": "0x8",
                "timestamp": "2026-04-17T00:00:08Z"
            }
        }),
        &selected,
    )
    .expect_err("different chain position must be stale");
    assert_eq!(stale.kind(), SnapshotSelectionErrorKind::Stale);
}
