//! A composed name load reads one snapshot (TYR-36 step 7b): its publication (the family
//! marker) and every family and identity statement after it see the same committed state, so the
//! row's facts and the position it is stamped with name the same block. The test pauses a load
//! right after its publication read (`families::name::seams`), commits the next family block on
//! another connection, then lets the load finish.
#[path = "families_support/mod.rs"]
mod support;

use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use bigname_project::families::{self, FamilyMode, FamilyOptions};
use bigname_storage::{
    NameCurrentRow,
    families::name::{load_family_name, seams::with_pause_after_publication},
};
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;
use support::{CHAIN, CONTENT_HASH, Fixture, marker, uuid};
use tokio::sync::Notify;

const REGISTRAR: &str = "0x00000000000000000000000000000000000000e3";
const REGISTRY: &str = "0x00000000000000000000000000000000000000e5";
const OWNER: &str = "0x00000000000000000000000000000000000000aa";
const V1_REGISTRAR: &str = "ens_v1_registrar_l1";
const NAME: &str = "ens:0x0000000000000000000000000000000000000000000000000000000000000001";

fn block_of(row: &NameCurrentRow) -> Value {
    row.chain_positions
        .as_object()
        .and_then(|positions| positions.values().next())
        .map_or(Value::Null, |position| position["block_number"].clone())
}

#[tokio::test]
async fn a_composed_load_reads_one_snapshot_across_a_family_commit() -> Result<()> {
    let fixture = Fixture::new("families_name_snapshot", 20).await?;
    let lease = uuid(1);
    fixture
        .binding(&uuid(100), NAME, &lease, "ens_v1", 9, 0, None)
        .await?;
    fixture
        .write(
            9,
            0,
            "SurfaceBound",
            V1_REGISTRAR,
            Some(NAME),
            Some(&lease),
            json!({"authority_kind": "registrar", "state_derived": false,
                   "registry_contract": REGISTRY, "owner_getter": OWNER}),
            REGISTRAR,
        )
        .await?;
    fixture
        .write(
            10,
            1,
            "RegistrationGranted",
            V1_REGISTRAR,
            Some(NAME),
            Some(&lease),
            json!({"authority_kind": "registrar", "status": "registered", "registrant": OWNER,
                   "expiry": 2_000_000_000u64}),
            REGISTRAR,
        )
        .await?;
    let outcome = fixture.apply(12, FamilyMode::Normal).await;
    ensure!(outcome.skipped.is_none(), "families at 12: {outcome:?}");
    fixture
        .write(
            13,
            1,
            "RegistrationRenewed",
            V1_REGISTRAR,
            Some(NAME),
            Some(&lease),
            json!({"authority_kind": "registrar", "expiry": 2_100_000_000u64}),
            REGISTRAR,
        )
        .await?;

    let (reached, resume) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let pool = fixture.pool.clone();
    let reader = tokio::spawn(with_pause_after_publication(
        Arc::clone(&reached),
        Arc::clone(&resume),
        async move { load_family_name(&pool, NAME).await },
    ));
    reached.notified().await;
    // The fixture pool holds one connection, which the paused load may be holding.
    let other = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(fixture.pool.connect_options().as_ref().clone())
        .await?;
    let token = families::input_token(&other, CHAIN).await?;
    let outcome = families::apply(
        &other,
        CHAIN,
        &marker(13),
        FamilyMode::Normal,
        &token,
        &FamilyOptions::new(CONTENT_HASH),
    )
    .await;
    ensure!(
        outcome.skipped.is_none() && outcome.marker.as_ref().map(|m| m.number) == Some(13),
        "families at 13: {outcome:?}"
    );
    other.close().await;
    resume.notify_one();
    let row = reader
        .await??
        .context("the paused load composes the name")?;
    assert_eq!(
        (
            block_of(&row),
            row.declared_summary["registration"]["expiry"].clone()
        ),
        (json!(12), json!(2_000_000_000u64)),
        "the paused load's row is stamped with block 12 and must carry block 12's expiry"
    );

    let fresh = load_family_name(&fixture.pool, NAME)
        .await?
        .context("the name composes after the commit")?;
    assert_eq!(
        (
            block_of(&fresh),
            fresh.declared_summary["registration"]["expiry"].clone()
        ),
        (json!(13), json!(2_100_000_000u64))
    );
    fixture.cleanup().await
}

/// A marker that is not servable (a rebuild populating the families, or written by another
/// interpreter build) makes the composed read refuse rather than compose from half-built
/// families: the fence's rule (snapshot_selection/project.rs).
#[tokio::test]
async fn a_composed_load_refuses_an_unservable_marker() -> Result<()> {
    let fixture = Fixture::new("families_name_unservable", 20).await?;
    let lease = uuid(1);
    fixture
        .binding(&uuid(100), NAME, &lease, "ens_v1", 9, 0, None)
        .await?;
    fixture
        .write(
            10,
            1,
            "RegistrationGranted",
            V1_REGISTRAR,
            Some(NAME),
            Some(&lease),
            json!({"authority_kind": "registrar", "status": "registered", "registrant": OWNER,
                   "expiry": 2_000_000_000u64}),
            REGISTRAR,
        )
        .await?;
    let outcome = fixture.apply(12, FamilyMode::Normal).await;
    ensure!(outcome.skipped.is_none(), "families at 12: {outcome:?}");
    ensure!(load_family_name(&fixture.pool, NAME).await?.is_some());
    for (state, hash) in [
        ("bootstrap_pending", CONTENT_HASH),
        ("live", "another-interpreter-build"),
    ] {
        sqlx::query("UPDATE project_family_marker SET state = $1, input_content_hash = $2")
            .bind(state)
            .bind(hash)
            .execute(&fixture.pool)
            .await?;
        let read = load_family_name(&fixture.pool, NAME).await;
        assert!(
            read.as_ref()
                .is_err_and(bigname_storage::families::name::is_publication_unavailable),
            "a {state} marker of {hash} must not serve a composed row: {read:?}"
        );
    }
    fixture.cleanup().await
}
