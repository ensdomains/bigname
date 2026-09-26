//! F12 through the family loop: reverse tuples keep the latest ReverseChanged and the latest
//! direct claim apart, a node keeps its latest name record or version change, and every claim
//! event keeps its normalization. Each case undoes its last block byte for byte and equals a
//! rebuild.
mod families_support;

use anyhow::Result;
use bigname_project::families::FamilyMode;
use families_support::Fixture;
use serde_json::{Value, json};

const REVERSE: &str = "0x00000000000000000000000000000000000000e2";
const R1: &str = "0x00000000000000000000000000000000000000a1";
const ALICE: &str = "0x00000000000000000000000000000000000000AA";

fn node(n: u64) -> String {
    format!("0x{n:064x}")
}

fn pick(row: &Value, names: &[&str]) -> Value {
    Value::Object(
        names
            .iter()
            .map(|name| ((*name).to_owned(), row[*name].clone()))
            .collect(),
    )
}

#[tokio::test]
async fn tuples_keep_the_latest_reverse_change_and_direct_claim_apart() -> Result<()> {
    let fixture = Fixture::new("families_reverse_tuples", 20).await?;
    let reverse = |source: &str, reverse_node: &str| {
        json!({"address": ALICE, "coin_type": "60", "namespace": "ens",
               "reverse_node": reverse_node, "source_event": source,
               "claim_provenance": "reverse_registrar"})
    };
    let direct = |name: Value| {
        json!({"node": node(9), "record_key": "name", "source_event": "NameForAddrChanged",
               "raw_name": name,
               "primary_claim_source": {"address": ALICE, "coin_type": "60", "namespace": "ens",
                                        "reverse_node": node(9)}})
    };
    let v1 = "ens_v1_reverse_registrar_l1";
    let resolver = "ens_v1_resolver_l1";
    fixture
        .write(
            10,
            1,
            "ReverseChanged",
            v1,
            None,
            None,
            reverse("ReverseClaimed", &node(9)),
            REVERSE,
        )
        .await?;
    fixture
        .write(
            11,
            1,
            "RecordChanged",
            resolver,
            None,
            None,
            direct(json!("Alice.eth")),
            R1,
        )
        .await?;
    fixture
        .write(
            12,
            1,
            "RecordChanged",
            resolver,
            None,
            None,
            direct(json!("alice.eth")),
            R1,
        )
        .await?;
    // A later ReverseChanged moves the reverse fields and leaves the claim fields alone.
    fixture
        .write(
            12,
            2,
            "ReverseChanged",
            v1,
            None,
            None,
            reverse("NameForAddrChanged", &node(8)),
            REVERSE,
        )
        .await?;
    fixture.apply(12, FamilyMode::Normal).await;

    let tuples = fixture.rows("project_reverse_tuple").await?;
    assert_eq!(tuples.len(), 1, "one tuple for the lower-cased address");
    let tuple = &tuples[0];
    assert_eq!(
        pick(
            tuple,
            &[
                "address",
                "coin_type",
                "namespace",
                "reverse_node",
                "source_event",
                "claim_provenance",
                "raw_name",
                "claim_event_identity",
                "hydrated_name"
            ]
        ),
        json!({"address": ALICE.to_lowercase(), "coin_type": "60", "namespace": "ens",
               "reverse_node": node(8), "source_event": "NameForAddrChanged",
               "claim_provenance": "reverse_registrar", "raw_name": "alice.eth",
               "claim_event_identity": "RecordChanged:12:1", "hydrated_name": null})
    );
    assert_eq!(tuple["reverse_position"]["log_index"], json!(2));
    assert_eq!(tuple["claim_position"]["block_number"], json!(12));

    let mut claims = fixture.rows("project_claim_normalization").await?;
    claims.sort_by_key(|row| row["claim_event_identity"].to_string());
    assert_eq!(
        claims
            .iter()
            .map(|row| pick(
                row,
                &[
                    "claim_event_identity",
                    "status",
                    "normalized_name",
                    "reason"
                ]
            ))
            .collect::<Vec<_>>(),
        vec![
            json!({"claim_event_identity": "RecordChanged:11:1", "status": "success",
                   "normalized_name": "alice.eth", "reason": null}),
            json!({"claim_event_identity": "RecordChanged:12:1", "status": "success",
                   "normalized_name": "alice.eth", "reason": null}),
        ],
        "each claim event keeps its own result"
    );

    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn nodes_keep_their_latest_name_record_and_claims_their_classification() -> Result<()> {
    let fixture = Fixture::new("families_reverse_nodes", 20).await?;
    let resolver = "ens_v1_resolver_l1";
    let name_record = |name: Value| {
        json!({"node": node(9), "record_key": "name", "source_event": "NameChanged",
               "resolver": R1.to_uppercase().replace("0X", "0x"), "raw_name": name})
    };
    fixture
        .write(
            10,
            1,
            "RecordChanged",
            resolver,
            None,
            None,
            name_record(json!("bob.eth")),
            R1,
        )
        .await?;
    // An undecodable name keeps only its bytes; an empty name is not found; a bad name fails.
    fixture
        .write(
            10,
            2,
            "RecordChanged",
            resolver,
            None,
            None,
            json!({"node": node(7), "record_key": "name", "source_event": "NameChanged",
                      "resolver": R1, "raw_name_bytes": "0xff"}),
            R1,
        )
        .await?;
    fixture
        .write(
            10,
            3,
            "RecordChanged",
            resolver,
            None,
            None,
            json!({"node": node(6), "record_key": "name", "source_event": "NameChanged",
                      "resolver": R1, "raw_name": "  "}),
            R1,
        )
        .await?;
    fixture
        .write(
            10,
            4,
            "RecordChanged",
            resolver,
            None,
            None,
            json!({"node": node(5), "record_key": "name", "source_event": "NameChanged",
                      "resolver": R1, "raw_name": "a..eth"}),
            R1,
        )
        .await?;
    // A text record at the node is not a claim.
    fixture
        .write(
            11,
            1,
            "RecordChanged",
            resolver,
            None,
            None,
            json!({"node": node(9), "record_key": "text:url", "source_event": "TextChanged",
                      "resolver": R1, "value": "u"}),
            R1,
        )
        .await?;
    // A version change becomes the node's latest claim event and carries no name.
    fixture
        .write(
            12,
            1,
            "RecordVersionChanged",
            resolver,
            None,
            None,
            json!({"node": node(9), "resolver": R1, "version": "1"}),
            R1,
        )
        .await?;
    fixture.apply(12, FamilyMode::Normal).await;

    let nodes = fixture.rows("project_reverse_node_claim").await?;
    let nine = nodes
        .iter()
        .find(|row| row["reverse_node"] == json!(node(9)))
        .expect("node 9 row");
    assert_eq!(
        pick(
            nine,
            &[
                "resolver_address",
                "raw_name",
                "block_number",
                "event_identity"
            ]
        ),
        json!({"resolver_address": R1, "raw_name": null, "block_number": 12,
               "event_identity": "RecordVersionChanged:12:1"})
    );
    assert_eq!(nodes.len(), 4);

    let mut claims = fixture.rows("project_claim_normalization").await?;
    claims.sort_by_key(|row| row["claim_event_identity"].to_string());
    assert_eq!(
        claims
            .iter()
            .map(|row| pick(
                row,
                &[
                    "claim_event_identity",
                    "status",
                    "normalized_name",
                    "reason"
                ]
            ))
            .collect::<Vec<_>>(),
        vec![
            json!({"claim_event_identity": "RecordChanged:10:1", "status": "success",
                   "normalized_name": "bob.eth", "reason": null}),
            json!({"claim_event_identity": "RecordChanged:10:2", "status": "unsupported",
                   "normalized_name": null, "reason": "claim_name_not_decodable"}),
            json!({"claim_event_identity": "RecordChanged:10:3", "status": "not_found",
                   "normalized_name": null, "reason": null}),
            json!({"claim_event_identity": "RecordChanged:10:4", "status": "invalid_name",
                   "normalized_name": null, "reason": null}),
        ]
    );

    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}
