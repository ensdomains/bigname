//! What the families keep for the readers of steps 3 and 4 beyond the latest value: the wrapper
//! lifecycle of a wrapper resource (F2b) and the owner-setting registry events of a node (F2c).
//! Every case undoes its last block byte for byte and equals a rebuild.
mod families_support;

use anyhow::Result;
use bigname_project::{BatchRequest, Engine, RunMode, families::FamilyMode};
use families_support::{CHAIN, Fixture, uuid};
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

// The node row keeps only the latest owner group, and a SubregistryChanged after a zero-getter
// transfer replaces it. Step 3 reads the owner-setting registry events of a node in order
// (name_authority/stage.rs, `registry_records`), so each is kept by position with the name,
// resource and authority kind it carried.
#[tokio::test]
async fn every_owner_setting_registry_event_of_a_node_is_kept() -> Result<()> {
    const REGISTRY: &str = "0x00000000000000000000000000000000000000a3";
    const OWNER: &str = "0x00000000000000000000000000000000000000c1";
    const ZERO: &str = "0x0000000000000000000000000000000000000000";
    let fixture = Fixture::new("families_retention_owner", 20).await?;
    let registry = "ens_v1_registry_l1";
    let (first, second) = (uuid(2), uuid(3));
    fixture
        .write(
            10,
            1,
            "AuthorityTransferred",
            registry,
            Some(&name(2)),
            Some(&first),
            json!({"node": node(2), "owner": ZERO, "owner_getter": ZERO,
                   "owner_getter_reason": "zero_owner", "authority_kind": "registry_only"}),
            REGISTRY,
        )
        .await?;
    fixture.apply(10, FamilyMode::Normal).await;
    fixture
        .write(
            12,
            2,
            "SubregistryChanged",
            registry,
            Some(&name(2)),
            Some(&second),
            json!({"child_node": node(2), "owner": OWNER, "owner_getter": OWNER,
                   "authority_kind": "registry_only"}),
            REGISTRY,
        )
        .await?;
    fixture.apply(12, FamilyMode::Normal).await;
    let kept = [
        "namespace",
        "node",
        "logical_name_id",
        "resource_id",
        "event_kind",
        "authority_kind",
        "owner",
        "owner_getter",
        "owner_getter_reason",
        "block_number",
        "log_index",
        "event_identity",
        "source_family",
    ];
    let rows = fixture.rows("project_registry_owner_event").await?;
    let rows: Vec<Value> = rows.iter().map(|row| columns(row, &kept)).collect();
    assert_eq!(
        rows,
        [
            json!({"namespace": "ens", "node": node(2), "logical_name_id": name(2),
                   "resource_id": first, "event_kind": "AuthorityTransferred",
                   "authority_kind": "registry_only", "owner": ZERO, "owner_getter": ZERO,
                   "owner_getter_reason": "zero_owner", "block_number": 10, "log_index": 1,
                   "event_identity": "AuthorityTransferred:10:1", "source_family": registry}),
            json!({"namespace": "ens", "node": node(2), "logical_name_id": name(2),
                   "resource_id": second, "event_kind": "SubregistryChanged",
                   "authority_kind": "registry_only", "owner": OWNER, "owner_getter": OWNER,
                   "owner_getter_reason": null, "block_number": 12, "log_index": 2,
                   "event_identity": "SubregistryChanged:12:2", "source_family": registry}),
        ]
    );
    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

/// The served wrapper of a name after a served build to `target`: name_current's
/// `wrapper_state` and fuses, masked against the target block's clock.
async fn served_wrapper(fixture: &Fixture, logical_name_id: &str, target: i64) -> Result<Value> {
    Engine::new(fixture.pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: target,
            affected_from_block: 0,
            affected_to_block: target,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;
    Ok(sqlx::query_scalar(
        "SELECT jsonb_build_object('wrapper_state', declared_summary -> 'wrapper_state',
                                   'fuses', declared_summary #> '{wrapper_fuses,fuses}')
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(logical_name_id)
    .fetch_optional(&fixture.pool)
    .await?
    .unwrap_or(Value::Null))
}

/// A block's timestamp in the fixture lineage.
fn block_time(block: i64) -> i64 {
    1_800_000_000 + block * 12
}

// A wrap whose wrapper expiry equals block 12's timestamp, an unwrap and a new expiry in the same
// block, and a rewrap of the same resource. The raw wrapper row keeps each fact unmasked; the
// served name masks the fuses and a locked state only strictly after the expiry (name_current,
// `effective_wrapper`).
#[tokio::test]
async fn the_wrapper_row_stays_raw_through_expiry_unwrap_and_rewrap() -> Result<()> {
    let fixture = Fixture::new("families_retention_rewrap", 20).await?;
    let (logical, resource) = (name(3), uuid(3));
    fixture
        .binding(&uuid(30), &logical, &resource, "ens_v1", 10, 0, None)
        .await?;
    let wrapper = "ens_v1_wrapper_l1";
    let wrap = |log: i64, kind: &'static str, after: Value| (log, kind, after);
    for (log, kind, after) in [
        wrap(
            1,
            "TokenControlTransferred",
            json!({"source_event": "NameWrapped", "to": HOLDER}),
        ),
        wrap(
            2,
            "PermissionScopeChanged",
            json!({"source_event": "NameWrapped", "wrapper_state": "locked", "fuses": 65537}),
        ),
        wrap(
            3,
            "ExpiryChanged",
            json!({"source_event": "NameWrapped", "expiry": block_time(12)}),
        ),
    ] {
        fixture
            .write(
                10,
                log,
                kind,
                wrapper,
                Some(&logical),
                Some(&resource),
                after,
                WRAPPER,
            )
            .await?;
    }
    let raw = [
        "wrapper_state",
        "fuses",
        "expiry_seconds",
        "lifecycle_source",
        "lifecycle_unwrapped",
        "unwrapped_position",
    ];
    let mut seen = Vec::new();
    for target in [11, 12, 13] {
        fixture.apply(target, FamilyMode::Normal).await;
        seen.push((
            target,
            columns(&fixture.rows("project_wrapper_state").await?[0], &raw),
            served_wrapper(&fixture, &logical, target).await?,
        ));
    }
    // Block 14: the unwrap and a new expiry in one block.
    fixture
        .write(
            14,
            1,
            "AuthorityEpochChanged",
            wrapper,
            Some(&logical),
            Some(&resource),
            json!({"source_event": "NameUnwrapped", "node": node(3)}),
            WRAPPER,
        )
        .await?;
    fixture
        .write(
            14,
            2,
            "ExpiryChanged",
            wrapper,
            Some(&logical),
            Some(&resource),
            json!({"source_event": "NameUnwrapped", "expiry": block_time(30)}),
            WRAPPER,
        )
        .await?;
    fixture.apply(14, FamilyMode::Normal).await;
    seen.push((
        14,
        columns(&fixture.rows("project_wrapper_state").await?[0], &raw),
        served_wrapper(&fixture, &logical, 14).await?,
    ));
    // Block 15: a rewrap of the same resource.
    fixture
        .write(
            15,
            1,
            "TokenControlTransferred",
            wrapper,
            Some(&logical),
            Some(&resource),
            json!({"source_event": "NameWrapped", "to": HOLDER}),
            WRAPPER,
        )
        .await?;
    fixture
        .write(
            15,
            2,
            "PermissionScopeChanged",
            wrapper,
            Some(&logical),
            Some(&resource),
            json!({"source_event": "NameWrapped", "wrapper_state": "wrapped", "fuses": 0}),
            WRAPPER,
        )
        .await?;
    fixture.apply(15, FamilyMode::Normal).await;
    seen.push((
        15,
        columns(&fixture.rows("project_wrapper_state").await?[0], &raw),
        served_wrapper(&fixture, &logical, 15).await?,
    ));
    let wrapped_raw = |expiry: i64, fuses: i64, state: &str, source: &str, unwrapped: Value| {
        let lifecycle_unwrapped = source == "NameUnwrapped";
        json!({"wrapper_state": state, "fuses": fuses, "expiry_seconds": expiry,
               "lifecycle_source": source, "lifecycle_unwrapped": lifecycle_unwrapped,
               "unwrapped_position": unwrapped})
    };
    let unwrap = at(14, 1, "AuthorityEpochChanged:14:1");
    let locked = json!({"wrapper_state": "locked", "fuses": 65537});
    let expected = vec![
        // Before the expiry and at it (expiry equal to the block time) nothing is masked.
        (
            11,
            wrapped_raw(block_time(12), 65537, "locked", "NameWrapped", Value::Null),
            locked.clone(),
        ),
        (
            12,
            wrapped_raw(block_time(12), 65537, "locked", "NameWrapped", Value::Null),
            locked.clone(),
        ),
        // Just after it the served name drops the locked state and its fuses; the row does not.
        (
            13,
            wrapped_raw(block_time(12), 65537, "locked", "NameWrapped", Value::Null),
            json!({"wrapper_state": null, "fuses": null}),
        ),
        // The unwrap and a new expiry in one block: the row keeps both; the served name, which
        // does not read the lifecycle, serves the locked state again under the new expiry.
        (
            14,
            wrapped_raw(
                block_time(30),
                65537,
                "locked",
                "NameUnwrapped",
                unwrap.clone(),
            ),
            locked,
        ),
        // The rewrap is the newest lifecycle event and the unwrap stays recorded.
        (
            15,
            wrapped_raw(block_time(30), 0, "wrapped", "NameWrapped", unwrap),
            json!({"wrapper_state": "wrapped", "fuses": 0}),
        ),
    ];
    assert_eq!(seen, expected);
    fixture.assert_undo_restores(15).await?;
    fixture.assert_rebuild_equal(15).await?;
    fixture.cleanup().await
}

// The node row keeps registry_owner and the unmasked-word flag of its latest owner-setting event
// only. When an earlier transfer wins the control owner, the reader needs them from that event, so
// each owner-event row carries them too.
#[tokio::test]
async fn an_earlier_transfer_keeps_its_registry_owner_and_unmasked_flag() -> Result<()> {
    const REGISTRY: &str = "0x00000000000000000000000000000000000000a3";
    const OWNER: &str = "0x00000000000000000000000000000000000000c1";
    const RECORDED: &str = "0x00000000000000000000000000000000000000c2";
    let fixture = Fixture::new("families_retention_owner_word", 20).await?;
    let registry = "ens_v1_registry_l1";
    let resource = uuid(4);
    fixture
        .write(
            10,
            1,
            "AuthorityTransferred",
            registry,
            Some(&name(4)),
            Some(&resource),
            json!({"node": node(4), "owner": OWNER, "owner_getter": OWNER,
                   "registry_owner": RECORDED, "owner_word_unmasked": true}),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            12,
            1,
            "SubregistryChanged",
            registry,
            Some(&name(4)),
            Some(&resource),
            json!({"child_node": node(4), "owner": OWNER, "owner_getter": OWNER}),
            REGISTRY,
        )
        .await?;
    fixture.apply(12, FamilyMode::Normal).await;
    let rows = fixture.rows("project_registry_owner_event").await?;
    let kept: Vec<Value> = rows
        .iter()
        .map(|row| {
            columns(
                row,
                &["event_identity", "registry_owner", "owner_word_unmasked"],
            )
        })
        .collect();
    assert_eq!(
        kept,
        [
            json!({"event_identity": "AuthorityTransferred:10:1", "registry_owner": RECORDED,
                   "owner_word_unmasked": true}),
            json!({"event_identity": "SubregistryChanged:12:1", "registry_owner": null,
                   "owner_word_unmasked": null}),
        ]
    );
    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}
