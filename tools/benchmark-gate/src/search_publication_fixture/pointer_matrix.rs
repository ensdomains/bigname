//! Explicit reader fault/permissiveness probes, rolled back after each case. These are not
//! represented as states produced by Project; the initial pointer is produced by normal inputs.
use super::*;
use serde_json::{Value, json};
use uuid::Uuid;

pub async fn run(pool: &PgPool) -> Result<Value> {
    let resource = Uuid::from_u128(0xa230);
    let resources = [resource, resource, Uuid::from_u128(0xffffff)];
    let cases = [
        ("normal_pointer", None, false, 1),
        (
            "manual_null_address",
            Some(
                "UPDATE project_resource_pointer SET nonzero_resolver_address=NULL WHERE resource_id=$1",
            ),
            false,
            1,
        ),
        (
            "missing_nonzero_position",
            Some("UPDATE project_resource_pointer SET nonzero_position=NULL WHERE resource_id=$1"),
            false,
            0,
        ),
        (
            "missing_boundary_position",
            Some("UPDATE project_resource_pointer SET boundary_position=NULL WHERE resource_id=$1"),
            false,
            0,
        ),
        (
            "missing_boundary_kind",
            Some("UPDATE project_resource_pointer SET boundary_kind=NULL WHERE resource_id=$1"),
            false,
            0,
        ),
        (
            "missing_boundary_event",
            Some(
                "UPDATE project_resource_pointer SET boundary_position=jsonb_set(boundary_position,'{event_identity}','\"gate1-missing-event\"') WHERE resource_id=$1",
            ),
            false,
            0,
        ),
        (
            "present_event_null_required_hash",
            Some(
                "UPDATE normalized_events SET block_hash=NULL,block_number=NULL,transaction_hash=NULL,transaction_index=NULL,log_index=NULL WHERE event_identity=(SELECT boundary_position->>'event_identity' FROM project_resource_pointer WHERE resource_id=$1)",
            ),
            true,
            0,
        ),
    ];
    let mut results = Vec::new();
    for (label, mutation, reject, count) in cases {
        let mut tx = pool.begin().await?;
        if let Some(sql) = mutation {
            ensure!(
                sqlx::query(sql)
                    .bind(resource)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
                    == 1,
                "pointer probe did not reach its input: {label}"
            );
        }
        let value = bigname_storage::families::search_dictionary::verify_basenames_pointer_batch(
            &mut tx, &resources,
        )
        .await?;
        ensure!(
            value["both_reject"] == json!(reject),
            "pointer rejection result: {label}"
        );
        if !reject {
            ensure!(
                value["qualified"] == json!(count),
                "pointer qualification result: {label}"
            );
        }
        tx.rollback().await?;
        results.push(json!({"case":label,"result":value,"rolled_back":true}));
    }
    let mut conn = pool.acquire().await?;
    let empty = bigname_storage::families::search_dictionary::verify_basenames_pointer_batch(
        &mut conn,
        &[],
    )
    .await?;
    ensure!(
        empty["qualified"] == json!(0),
        "empty pointer request differs"
    );
    Ok(
        json!({"kind":"reader equivalence with explicit manually supplied edge states","cases":results,"empty":empty}),
    )
}
