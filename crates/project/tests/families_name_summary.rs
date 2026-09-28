//! The name summary family (TYR-36 step 7b slice 2b, `project_name_summary`): the per-name fields
//! the child and label lists read inside one statement, written by the family step for the names
//! a block touches and journalled like every other family. A block that touches one name rewrites
//! that name's row and no other, undo puts the previous row back, and a rebuild writes the same
//! rows as the incremental follow. Every row carries the fields of the name's served row.
#[path = "families_shadow_support/mod.rs"]
mod shadow_support;
#[path = "families_support/mod.rs"]
mod support;

use anyhow::{Result, ensure};
use bigname_project::families;
use serde_json::{Value, json};
use shadow_support::{publish, served};
use support::{CHAIN, Fixture, uuid};

const REGISTRAR: &str = "0x00000000000000000000000000000000000000e3";
const REGISTRY: &str = "0x00000000000000000000000000000000000000e5";
const OWNER: &str = "0x00000000000000000000000000000000000000aa";
const V1_REGISTRAR: &str = "ens_v1_registrar_l1";

fn name(n: u64) -> String {
    format!("ens:0x{n:064x}")
}

/// Name `n` bound at `block` to its own lease under arm ens_v1 and granted one block later
/// until `expiry`.
async fn registered(fixture: &Fixture, n: u64, block: i64, expiry: u64) -> Result<()> {
    let lease = uuid(0x1000 + u32::try_from(n)?);
    fixture
        .binding(
            &uuid(100 + u32::try_from(n)?),
            &name(n),
            &lease,
            "ens_v1",
            block,
            0,
            None,
        )
        .await?;
    fixture
        .write(
            block,
            0,
            "SurfaceBound",
            V1_REGISTRAR,
            Some(&name(n)),
            Some(&lease),
            json!({"authority_kind": "registrar", "state_derived": false,
                   "registry_contract": REGISTRY, "owner_getter": OWNER}),
            REGISTRAR,
        )
        .await?;
    fixture
        .write(
            block + 1,
            0,
            "RegistrationGranted",
            V1_REGISTRAR,
            Some(&name(n)),
            Some(&lease),
            json!({"authority_kind": "registrar", "status": "registered", "registrant": OWNER,
                   "expiry": expiry}),
            REGISTRAR,
        )
        .await?;
    Ok(())
}

/// The summary row of `logical_name_id` without its chain, and the row version it was written in.
async fn summary(fixture: &Fixture, logical_name_id: &str) -> Result<Option<(Value, String)>> {
    Ok(sqlx::query_as(
        "SELECT to_jsonb(summary) - 'chain_id', summary.xmin::text
         FROM project_name_summary summary
         WHERE summary.chain_id = $1 AND summary.logical_name_id = $2",
    )
    .bind(CHAIN)
    .bind(logical_name_id)
    .fetch_optional(&fixture.pool)
    .await?)
}

/// The summary row of `logical_name_id` must carry its served row's selected arm, serving flag,
/// registration status and expiry.
async fn assert_matches_served(fixture: &Fixture, logical_name_id: &str) -> Result<Value> {
    let (row, _) = summary(fixture, logical_name_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("{logical_name_id} has no summary row"))?;
    let served = served(fixture, logical_name_id).await?;
    let expiry: Option<i64> = sqlx::query_scalar(
        "SELECT extract(epoch FROM expires_at)::bigint FROM project_name_summary
         WHERE chain_id = $1 AND logical_name_id = $2",
    )
    .bind(CHAIN)
    .bind(logical_name_id)
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(
        row["authority_arm"] == served.provenance["authority_selection"]["authority_arm"],
        "{logical_name_id}: arm {row} against {}",
        served.provenance
    );
    ensure!(
        row["serving"]
            == json!(
                !served.provenance["read_reachability"]["serving_resource_id"].is_null()
                    && served.provenance["read_reachability"]["serving_resource_id"] != json!(null)
            ),
        "{logical_name_id}: serving {row} against {}",
        served.provenance
    );
    ensure!(
        row["registration_status"] == served.registration("status"),
        "{logical_name_id}: status {row} against {}",
        served.summary
    );
    ensure!(
        expiry.map(Value::from).unwrap_or(Value::Null) == served.registration("expiry"),
        "{logical_name_id}: expiry {expiry:?} against {}",
        served.summary
    );
    Ok(row)
}

#[tokio::test]
async fn a_block_rewrites_the_summary_of_the_name_it_touches_and_undo_restores_it() -> Result<()> {
    let fixture = Fixture::new("families_name_summary", 12).await?;
    registered(&fixture, 1, 2, 2_000_000_000).await?;
    registered(&fixture, 2, 4, 2_100_000_000).await?;
    publish(&fixture, 7).await?;
    let first = assert_matches_served(&fixture, &name(1)).await?;
    let second = assert_matches_served(&fixture, &name(2)).await?;
    assert_eq!(first["authority_arm"], json!("ens_v1"));
    assert_eq!(first["registration_status"], json!("active"));
    assert_eq!(second["registration_status"], json!("active"));
    let (_, first_version) = summary(&fixture, &name(1)).await?.expect("first row");
    let (_, second_version) = summary(&fixture, &name(2)).await?.expect("second row");

    // Block 8 renews the first name only.
    fixture
        .write(
            8,
            0,
            "RegistrationRenewed",
            V1_REGISTRAR,
            Some(&name(1)),
            Some(&uuid(0x1001)),
            json!({"expiry": 2_050_000_000u64}),
            REGISTRAR,
        )
        .await?;
    publish(&fixture, 8).await?;
    let renewed = assert_matches_served(&fixture, &name(1)).await?;
    assert_matches_served(&fixture, &name(2)).await?;
    ensure!(renewed != first, "the renewal left {renewed}");
    let (_, renewed_version) = summary(&fixture, &name(1)).await?.expect("first row");
    let (untouched, untouched_version) = summary(&fixture, &name(2)).await?.expect("second row");
    ensure!(
        renewed_version != first_version,
        "block 8 did not rewrite the renewed name's summary"
    );
    ensure!(
        untouched_version == second_version && untouched == second,
        "block 8 rewrote the summary of a name it does not touch"
    );

    // Undo block 8: the renewed name's summary is the block-7 row again.
    let undone = families::undo_to(&fixture.pool, CHAIN, 7).await?;
    ensure!(undone == 1, "undid {undone} blocks");
    let (restored, _) = summary(&fixture, &name(1)).await?.expect("first row");
    ensure!(restored == first, "undo left {restored}, not {first}");
    let (kept, _) = summary(&fixture, &name(2)).await?.expect("second row");
    ensure!(kept == second, "undo changed {kept}");

    // Replay, then the block-by-block and ranged rebuilds write the same rows.
    fixture
        .apply(8, bigname_project::families::FamilyMode::Normal)
        .await;
    let (replayed, _) = summary(&fixture, &name(1)).await?.expect("first row");
    ensure!(
        replayed == renewed,
        "the replay wrote {replayed}, not {renewed}"
    );
    fixture.assert_rebuild_equal(8).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn the_family_undo_and_rebuild_keep_every_summary_row() -> Result<()> {
    let fixture = Fixture::new("families_name_summary_undo", 12).await?;
    registered(&fixture, 1, 2, 2_000_000_000).await?;
    registered(&fixture, 2, 6, 2_100_000_000).await?;
    shadow_support::publish_served(&fixture, 9).await?;
    // Block 7 grants the second name: undoing it restores its summary row as block 6 left it.
    fixture.assert_undo_restores(7).await?;
    fixture
        .apply(9, bigname_project::families::FamilyMode::Normal)
        .await;
    fixture.assert_rebuild_equal(9).await?;
    let rows = fixture.rows("project_name_summary").await?;
    ensure!(rows.len() == 2, "{rows:#?}");
    fixture.cleanup().await
}
