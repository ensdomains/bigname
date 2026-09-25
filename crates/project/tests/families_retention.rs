//! What the families keep for the readers of steps 3 and 4 beyond the latest value: the wrapper
//! lifecycle of a wrapper resource (F2b) and the owner-setting registry events of a node (F2c).
//! Every case undoes its last block byte for byte and equals a rebuild.
mod families_support;

use anyhow::Result;
use bigname_project::families::FamilyMode;
use families_support::{Fixture, uuid};
use serde_json::{Value, json};

const WRAPPER: &str = "0x00000000000000000000000000000000000000e3";
const HOLDER: &str = "0x00000000000000000000000000000000000000b1";

fn node(n: u64) -> String {
    format!("0x{n:064x}")
}

fn name(n: u64) -> String {
    format!("ens:{}", node(n))
}

fn columns(row: &Value, names: &[&str]) -> Value {
    Value::Object(
        names
            .iter()
            .map(|name| ((*name).to_owned(), row[*name].clone()))
            .collect(),
    )
}

fn at(block: i64, log: i64, identity: &str) -> Value {
    json!({"block_number": block, "transaction_index": 0, "log_index": log,
           "event_identity": identity})
}

// The served permissions summary serves the wrapper restrictions only while the newest of a
// resource's mint, unwrap and holder grant or revoke is a mint or a grant
// (resource_summary.rs, `wrapper_lifecycles`). The wrapper row keeps that newest event, whether
// it unwraps, and the latest unwrap on its own, so an unwrap that revokes no holder grant is not
// lost.
#[tokio::test]
async fn the_wrapper_row_keeps_its_lifecycle_and_its_latest_unwrap() -> Result<()> {
    let fixture = Fixture::new("families_retention_unwrap", 20).await?;
    let resource = uuid(1);
    let wrapper = "ens_v1_wrapper_l1";
    fixture
        .write(
            10,
            1,
            "TokenControlTransferred",
            wrapper,
            Some(&name(1)),
            Some(&resource),
            json!({"source_event": "NameWrapped", "to": HOLDER}),
            WRAPPER,
        )
        .await?;
    fixture.apply(10, FamilyMode::Normal).await;
    let lifecycle = [
        "lifecycle_source",
        "lifecycle_unwrapped",
        "lifecycle_position",
        "unwrapped_position",
    ];
    assert_eq!(
        columns(&fixture.rows("project_wrapper_state").await?[0], &lifecycle),
        json!({"lifecycle_source": "NameWrapped", "lifecycle_unwrapped": false,
               "lifecycle_position": at(10, 1, "TokenControlTransferred:10:1"),
               "unwrapped_position": null})
    );

    fixture
        .write(
            12,
            1,
            "AuthorityEpochChanged",
            wrapper,
            Some(&name(1)),
            Some(&resource),
            json!({"source_event": "NameUnwrapped", "node": node(1)}),
            WRAPPER,
        )
        .await?;
    fixture.apply(12, FamilyMode::Normal).await;
    let unwrapped = at(12, 1, "AuthorityEpochChanged:12:1");
    assert_eq!(
        columns(&fixture.rows("project_wrapper_state").await?[0], &lifecycle),
        json!({"lifecycle_source": "NameUnwrapped", "lifecycle_unwrapped": true,
               "lifecycle_position": unwrapped, "unwrapped_position": unwrapped})
    );
    fixture.assert_undo_restores(12).await?;

    // A holder grant after the unwrap is the newest lifecycle event; the unwrap stays recorded.
    fixture
        .write(
            13,
            1,
            "PermissionChanged",
            wrapper,
            Some(&name(1)),
            Some(&resource),
            json!({"scope": {"kind": "resource"}, "effective_powers": ["owner"],
                   "grant_source": {"relation_kind": "holder"}, "subject": HOLDER}),
            WRAPPER,
        )
        .await?;
    fixture.apply(13, FamilyMode::Normal).await;
    assert_eq!(
        columns(&fixture.rows("project_wrapper_state").await?[0], &lifecycle),
        json!({"lifecycle_source": "holder_grant", "lifecycle_unwrapped": false,
               "lifecycle_position": at(13, 1, "PermissionChanged:13:1"),
               "unwrapped_position": unwrapped})
    );
    fixture.assert_undo_restores(13).await?;
    fixture.assert_rebuild_equal(13).await?;
    fixture.cleanup().await
}
