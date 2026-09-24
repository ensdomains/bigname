//! The owned key family loop (docs/projections.md, "Owned key families"): one transaction per
//! block after the served batch commits, a compare-and-swap on the shadow marker, a marker journal
//! row on every block, catch-up from a lagging marker, and a failure that stops the loop without
//! touching anything the served batch published.
#[path = "families_support/mod.rs"]
mod support;

use anyhow::Result;
use bigname_project::{BatchRequest, Engine, RunMode, families::FamilyMode};
use serde_json::json;
use support::{CHAIN, Event, Fixture, hash};

async fn bootstrapped(prefix: &str) -> Result<Fixture> {
    let fixture = Fixture::new(prefix, 20).await?;
    let outcome = fixture.apply(10, FamilyMode::Rebuild).await;
    assert_eq!(outcome.skipped, None);
    assert_eq!(fixture.marker().await?.0, Some(10));
    Ok(fixture)
}

#[tokio::test]
async fn catch_up_applies_and_journals_every_block_to_the_served_marker() -> Result<()> {
    let fixture = bootstrapped("families_loop_catch_up").await?;
    let (_, _, sequence) = fixture.marker().await?;
    let outcome = fixture.apply(14, FamilyMode::Normal).await;
    assert_eq!(outcome.skipped, None);
    assert_eq!(outcome.blocks, 4);
    assert_eq!(outcome.lag_blocks(), 0);
    assert_eq!(
        fixture.marker().await?,
        (Some(14), Some(hash(14)), sequence + 4)
    );
    assert_eq!(fixture.journalled_blocks().await?, [10, 11, 12, 13, 14]);
    fixture.cleanup().await
}

#[tokio::test]
async fn a_second_run_at_the_same_head_changes_nothing() -> Result<()> {
    let fixture = bootstrapped("families_loop_same_head").await?;
    fixture.apply(14, FamilyMode::Normal).await;
    let marker = fixture.marker().await?;
    let snapshot = fixture.snapshot().await?;
    let outcome = fixture.apply(14, FamilyMode::Normal).await;
    assert_eq!((outcome.blocks, outcome.skipped), (0, None));
    assert_eq!(fixture.marker().await?, marker);
    assert_eq!(fixture.snapshot().await?, snapshot);
    fixture.cleanup().await
}

#[tokio::test]
async fn a_lagging_marker_catches_up_from_where_it_stands() -> Result<()> {
    let fixture = bootstrapped("families_loop_lagging").await?;
    fixture.apply(11, FamilyMode::Normal).await;
    let outcome = fixture.apply(14, FamilyMode::Normal).await;
    assert_eq!(outcome.blocks, 3, "blocks 12 to 14, from the family marker");
    assert_eq!(fixture.marker().await?.0, Some(14));
    fixture.cleanup().await
}

#[tokio::test]
async fn a_failing_block_stops_the_loop_and_the_next_run_resumes_there() -> Result<()> {
    let fixture = bootstrapped("families_loop_failure").await?;
    // A failure inside block 13's transaction: its first journal write raises.
    sqlx::raw_sql(
        "CREATE FUNCTION fail_block_13() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.block_number = 13 THEN RAISE EXCEPTION 'injected family failure'; END IF;
             RETURN NEW;
         END $$;
         CREATE TRIGGER fail_block_13 BEFORE INSERT ON project_family_undo
         FOR EACH ROW EXECUTE FUNCTION fail_block_13();",
    )
    .execute(&fixture.pool)
    .await?;
    let outcome = fixture.apply(14, FamilyMode::Normal).await;
    let skipped = outcome
        .skipped
        .clone()
        .expect("the failing block stops the loop");
    assert!(skipped.contains("block 13"), "{skipped}");
    assert!(skipped.contains("injected family failure"), "{skipped}");
    assert_eq!(outcome.blocks, 2);
    assert_eq!(outcome.lag_blocks(), 2);
    assert_eq!(fixture.marker().await?.0, Some(12));
    assert_eq!(fixture.journalled_blocks().await?, [10, 11, 12]);

    sqlx::raw_sql("DROP TRIGGER fail_block_13 ON project_family_undo")
        .execute(&fixture.pool)
        .await?;
    let outcome = fixture.apply(14, FamilyMode::Normal).await;
    assert_eq!((outcome.blocks, outcome.skipped), (2, None));
    assert_eq!(fixture.marker().await?.0, Some(14));
    fixture.cleanup().await
}

#[tokio::test]
async fn a_family_failure_leaves_the_served_publication_as_committed() -> Result<()> {
    let fixture = Fixture::new("families_loop_served", 14).await?;
    let name = "ens:0x01";
    fixture.surface(name, "0x01").await?;
    fixture
        .event(
            Event::new("fixture:grant", 12, 0, "RegistrationGranted", "ens_v2_registry_l1")
                .name(name)
                .after(json!({"status": "registered", "registrant": "0x00000000000000000000000000000000000000aa",
                              "token_id": "1", "registry_contract_instance_id": "r"})),
        )
        .await?;
    let served = Engine::new(fixture.pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: 14,
            affected_from_block: 0,
            affected_to_block: 14,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;
    let published = served_digest(&fixture).await?;
    sqlx::raw_sql(
        "CREATE FUNCTION fail_families() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'injected family failure'; END $$;
         CREATE TRIGGER fail_families BEFORE INSERT ON project_family_undo
         FOR EACH ROW EXECUTE FUNCTION fail_families();",
    )
    .execute(&fixture.pool)
    .await?;
    let outcome = fixture
        .apply(served.current.number, FamilyMode::Rebuild)
        .await;
    assert!(outcome.skipped.is_some());
    assert_eq!(
        served_digest(&fixture).await?,
        published,
        "the served rows are what the batch committed"
    );
    assert_eq!(
        fixture.marker().await?.0,
        None,
        "the families never applied a block"
    );
    fixture.cleanup().await
}

/// Every row of the served tables the batch wrote, as text.
async fn served_digest(fixture: &Fixture) -> Result<Vec<String>> {
    let mut digest = Vec::new();
    for table in [
        "name_current",
        "children_current",
        "permissions_current",
        "record_inventory_current",
        "resolver_current",
        "address_names_current",
        "primary_names_current",
        "child_registration_events",
    ] {
        let rows: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT (to_jsonb(served) - 'inserted_at' - 'last_recomputed_at')::text
             FROM {table} served ORDER BY 1"
        ))
        .fetch_all(&fixture.pool)
        .await?;
        digest.push(format!("{table}: {}", rows.join("\n")));
    }
    Ok(digest)
}

// The marker records the input revision each block read: the Interpret row's content hash and
// redo attempt, or nothing while Interpret is in redo.
#[tokio::test]
async fn each_block_records_the_interpret_revision_it_read() -> Result<()> {
    let fixture = Fixture::new("families_loop_revision", 20).await?;
    fixture.interpret_row("interpret-hash-a", 3, false).await?;
    fixture.apply(10, FamilyMode::Normal).await;
    assert_eq!(
        fixture.marker_revision().await?,
        (Some("interpret-hash-a".to_owned()), Some(3))
    );

    fixture.interpret_row("interpret-hash-b", 4, true).await?;
    fixture.apply(11, FamilyMode::Normal).await;
    assert_eq!(fixture.marker().await?.0, Some(11), "the block still ran");
    assert_eq!(fixture.marker_revision().await?, (None, None));

    fixture.interpret_row("interpret-hash-b", 4, false).await?;
    fixture.apply(12, FamilyMode::Normal).await;
    assert_eq!(
        fixture.marker_revision().await?,
        (Some("interpret-hash-b".to_owned()), Some(4))
    );
    fixture.cleanup().await
}
