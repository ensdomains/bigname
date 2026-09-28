//! Retained child-registration history through the production family loop and history reader.
#[path = "families_support/mod.rs"]
mod support;

use alloy_primitives::{B256, keccak256};
use anyhow::Result;
use bigname_project::families::{FamilyMode, FamilyOptions, RebuildRanges};
use bigname_storage::{HistoryPageOptions, HistoryScope, HistorySummaryMode};
use serde_json::{Value, json};
use support::{CHAIN, CONTENT_HASH, Event, Fixture};

fn labels(name: &str) -> Vec<B256> {
    name.split('.')
        .map(|label| keccak256(label.as_bytes()))
        .collect()
}
fn namehash(name: &str) -> B256 {
    labels(name).iter().rev().fold(B256::ZERO, |parent, label| {
        keccak256([parent.as_slice(), label.as_slice()].concat())
    })
}
fn id(name: &str) -> String {
    format!("ens:{:#x}", namehash(name))
}

async fn surface(fixture: &Fixture, name: &str) -> Result<()> {
    fixture
        .surface(&id(name), &format!("{:#x}", namehash(name)))
        .await?;
    sqlx::query("UPDATE name_surfaces SET raw_name=$2, raw_labels=$3, labelhashes=$4 WHERE logical_name_id=$1")
        .bind(id(name)).bind(name).bind(name.split('.').collect::<Vec<_>>())
        .bind(labels(name).iter().map(|hash| format!("{hash:#x}")).collect::<Vec<_>>())
        .execute(&fixture.pool).await?;
    Ok(())
}

async fn registration(
    fixture: &Fixture,
    identity: &str,
    name: &str,
    block: i64,
    after: Value,
) -> Result<()> {
    fixture
        .event(
            Event::new(
                identity,
                block,
                0,
                "RegistrationGranted",
                "ens_v2_registry_l1",
            )
            .name(&id(name))
            .after(after),
        )
        .await?;
    Ok(())
}

async fn rows(fixture: &Fixture) -> Result<Vec<Value>> {
    Ok(sqlx::query_scalar(
        "SELECT to_jsonb(row)
        FROM child_registration_events row ORDER BY event_identity",
    )
    .fetch_all(&fixture.pool)
    .await?)
}

async fn history(fixture: &Fixture, name: &str) -> Result<Vec<String>> {
    let page = bigname_storage::load_name_history_page_with_child_registrations(
        &fixture.pool,
        &id(name),
        &[],
        HistoryScope::Surface,
        None,
        50,
        HistorySummaryMode::Count,
        &HistoryPageOptions::default(),
        None,
    )
    .await?;
    Ok(page
        .rows
        .into_iter()
        .map(|row| row.event.event_identity)
        .collect())
}

#[tokio::test]
async fn follow_publishes_child_history_and_release_preserves_it() -> Result<()> {
    let fixture = Fixture::new("family_child_history", 4).await?;
    for name in [
        "parent.eth",
        "alice.parent.eth",
        "bob.parent.eth",
        "alice.eth",
        "alice.base.eth",
        "snapshot.parent.eth",
        "hidden.parent.eth",
    ] {
        surface(&fixture, name).await?;
    }
    sqlx::query("UPDATE name_surfaces SET visibility_state='shadow', deactivation_reason='fixture', deactivated_at=now() WHERE logical_name_id=$1")
        .bind(id("hidden.parent.eth")).execute(&fixture.pool).await?;
    fixture.apply(0, FamilyMode::Normal).await?;
    registration(&fixture, "alice:grant:1", "alice.parent.eth", 1, json!({})).await?;
    registration(&fixture, "bob:grant:2", "bob.parent.eth", 2, json!({})).await?;
    registration(&fixture, "eth:grant:2", "alice.eth", 2, json!({})).await?;
    registration(&fixture, "base:grant:2", "alice.base.eth", 2, json!({})).await?;
    registration(
        &fixture,
        "snapshot:grant:2",
        "snapshot.parent.eth",
        2,
        json!({"state_derived":true,"registrar_surface_snapshot":true}),
    )
    .await?;
    registration(
        &fixture,
        "hidden:grant:2",
        "hidden.parent.eth",
        2,
        json!({}),
    )
    .await?;
    fixture.apply(2, FamilyMode::Normal).await?;
    let before = rows(&fixture).await?;
    assert_eq!(before.len(), 2);
    assert_eq!(
        history(&fixture, "parent.eth").await?,
        ["bob:grant:2", "alice:grant:1"]
    );
    fixture
        .event(
            Event::new(
                "alice:release:3",
                3,
                0,
                "RegistrationReleased",
                "ens_v2_registry_l1",
            )
            .name(&id("alice.parent.eth")),
        )
        .await?;
    fixture.apply(3, FamilyMode::Normal).await?;
    assert_eq!(rows(&fixture).await?, before);
    assert_eq!(
        history(&fixture, "parent.eth").await?,
        ["bob:grant:2", "alice:grant:1"]
    );
    let journal: i64 = sqlx::query_scalar("SELECT count(*) FROM project_family_undo WHERE chain_id=$1 AND family='child_registration_events'")
        .bind(CHAIN).fetch_one(&fixture.pool).await?;
    assert_eq!(journal, 2);
    fixture.cleanup().await
}

#[tokio::test]
async fn undo_replay_and_ranged_rebuild_repair_child_history() -> Result<()> {
    let fixture = Fixture::new("family_child_replay", 4).await?;
    for name in ["parent.eth", "alice.parent.eth", "bob.parent.eth"] {
        surface(&fixture, name).await?;
    }
    fixture.apply(0, FamilyMode::Normal).await?;
    registration(&fixture, "alice:grant:1", "alice.parent.eth", 1, json!({})).await?;
    registration(&fixture, "bob:grant:2", "bob.parent.eth", 2, json!({})).await?;
    fixture.apply(3, FamilyMode::Normal).await?;
    // Interpret retracts the second grant. Project's ordinary redo undoes its membership before
    // replaying the corrected event set; the first grant and its original attribution survive.
    sqlx::query("DELETE FROM normalized_events WHERE event_identity='bob:grant:2'")
        .execute(&fixture.pool)
        .await?;
    fixture
        .project_row(1, Some((2, 3, "operator redo")))
        .await?;
    fixture
        .apply(3, FamilyMode::Redo { from: 2, to: 3 })
        .await?;
    assert_eq!(history(&fixture, "parent.eth").await?, ["alice:grant:1"]);
    let expected = rows(&fixture).await?;
    fixture
        .apply_with(
            3,
            FamilyMode::Rebuild,
            &FamilyOptions::new(CONTENT_HASH).with_rebuild_ranges(RebuildRanges::Through(3)),
        )
        .await?;
    assert_eq!(rows(&fixture).await?, expected);
    // A recomputed surface's loss of visibility retracts membership when its event is replayed.
    sqlx::query("UPDATE name_surfaces SET visibility_state='shadow', deactivation_reason='fixture', deactivated_at=now() WHERE logical_name_id=$1")
        .bind(id("alice.parent.eth")).execute(&fixture.pool).await?;
    fixture
        .project_row(2, Some((1, 3, "operator redo")))
        .await?;
    fixture
        .apply(3, FamilyMode::Redo { from: 1, to: 3 })
        .await?;
    assert!(rows(&fixture).await?.is_empty());
    assert!(history(&fixture, "parent.eth").await?.is_empty());
    fixture.cleanup().await
}
