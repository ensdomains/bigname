//! The ordering rows: each surface state a redo of a range can meet, with the set that serves it.
//! A row that a wrong bound would miss shows that bound dropping the surface.
use bigname_adapters::schema_v2::{BatchOutput, NormalizedEvent};
use serde_json::json;

use super::*;

fn plain(name: &str, witness: Option<&str>) -> Raw {
    (
        Some(format!("{name}.eth")),
        witness.map(str::to_owned),
        "active".to_owned(),
    )
}

fn bytes_dropped() -> Raw {
    (None, None, "active".to_owned())
}

/// O1, served by the range observation arm.
#[tokio::test]
async fn o1_a_witness_in_the_range_moves_to_the_earliest_survivor() -> TestResult {
    let database = database("witness_o1", 8).await?;
    let pool = database.pool();
    surface(pool, "a", 1, Some("pre-a-3"), Shape::Plain).await?;
    hash_path(pool, "a", 1).await?;
    preimage(pool, "pre-a-3", "a", 3).await?;
    preimage(pool, "pre-a-5", "a", 5).await?;

    let (names, targets) = single_batch_redo(pool, (3, 3), "").await?;

    assert_eq!(names, list(&["a"]));
    assert_eq!(targets, set(&["a"]));
    assert_eq!(raw(pool, "a").await?, plain("a", Some("pre-a-5")));
    database.cleanup().await?;
    Ok(())
}

/// O2, served by the released-witness set. A range bound alone misses it.
#[tokio::test]
async fn o2_raw_bytes_without_a_witness_are_found_outside_the_range() -> TestResult {
    let database = database("witness_o2", 8).await?;
    let pool = database.pool();
    surface(pool, "b", 1, None, Shape::Plain).await?;

    let mut batch = Batch::begin(pool, (3, 3)).await?;
    assert_eq!(batch.names().await?, list(&["b"]));
    for mutant in [Mutant::WithoutReleased, Mutant::RangeOnly] {
        assert_eq!(batch.mutant_names(mutant).await?, list(&[]), "{mutant:?}");
    }
    batch.prepare().await?;
    batch.reanchor().await?;
    for mutant in [Mutant::WithoutReleased, Mutant::RangeOnly] {
        assert_eq!(batch.mutant_repair(mutant).await?.0, set(&[]), "{mutant:?}");
    }
    assert_eq!(batch.repair().await?, set(&["b"]));
    batch.commit().await?;

    // No label-hash-path observation establishes it, so it keeps its bytes.
    assert_eq!(raw(pool, "b").await?, plain("b", None));
    database.cleanup().await?;
    Ok(())
}

/// O3, served by the orphaned-block set. No redo range covers the orphaned block, as after a
/// reorg above the head Interpret had recorded.
#[tokio::test]
async fn o3_a_witness_on_an_orphaned_block_outside_the_range() -> TestResult {
    let database = database("witness_o3", 8).await?;
    let pool = database.pool();
    surface(pool, "c", 1, Some("pre-c-2"), Shape::Plain).await?;
    hash_path(pool, "c", 1).await?;
    preimage(pool, "pre-c-2", "c", 2).await?;
    orphan(pool, 2).await?;

    let mut batch = Batch::begin(pool, (3, 3)).await?;
    assert_eq!(batch.names().await?, list(&["c"]));
    for mutant in [Mutant::WithoutOrphaned, Mutant::RangeOnly] {
        assert_eq!(batch.mutant_names(mutant).await?, list(&[]), "{mutant:?}");
    }
    batch.prepare().await?;
    batch.reanchor().await?;
    let (missed, kept_bytes) = batch.mutant_repair(Mutant::WithoutOrphaned).await?;
    assert_eq!(missed, set(&[]));
    assert_eq!(batch.repair().await?, set(&["c"]));
    assert_ne!(batch.rows().await?, kept_bytes);
    batch.commit().await?;

    assert_eq!(raw(pool, "c").await?, bytes_dropped());
    database.cleanup().await?;
    Ok(())
}

/// O4, in no set: the rows the bound must not read.
#[tokio::test]
async fn o4_a_readable_witness_outside_the_range_is_left_alone() -> TestResult {
    let database = database("witness_o4", 8).await?;
    let pool = database.pool();
    surface(pool, "d", 1, Some("pre-d-1"), Shape::Plain).await?;
    preimage(pool, "pre-d-1", "d", 1).await?;
    let mut connection = pool.acquire().await?;
    let before = surfaces(&mut connection).await?;
    drop(connection);

    let (names, targets) = single_batch_redo(pool, (3, 3), "").await?;

    assert_eq!((names, targets), (list(&[]), set(&[])));
    let mut connection = pool.acquire().await?;
    assert_eq!(surfaces(&mut connection).await?, before);
    drop(connection);
    database.cleanup().await?;
    Ok(())
}

/// O5, served by the byte-shadow set. The released and orphaned sets alone miss it.
#[tokio::test]
async fn o5_a_byte_shadow_witness_that_names_no_event() -> TestResult {
    let database = database("witness_o5", 8).await?;
    let pool = database.pool();
    surface(pool, "e", 1, Some("pre-e-gone"), Shape::ByteShadow).await?;
    hash_path(pool, "e", 1).await?;

    let mut batch = Batch::begin(pool, (3, 3)).await?;
    assert_eq!(batch.names().await?, list(&["e"]));
    for mutant in [Mutant::WithoutByteShadow, Mutant::RangeOnly] {
        assert_eq!(batch.mutant_names(mutant).await?, list(&[]), "{mutant:?}");
    }
    batch.prepare().await?;
    batch.reanchor().await?;
    let (missed, kept_bytes) = batch.mutant_repair(Mutant::WithoutByteShadow).await?;
    assert_eq!(missed, set(&[]));
    assert_eq!(batch.repair().await?, set(&["e"]));
    assert_ne!(batch.rows().await?, kept_bytes);
    batch.commit().await?;

    assert_eq!(raw(pool, "e").await?, bytes_dropped());
    database.cleanup().await?;
    Ok(())
}

/// O6: the range preparation releases every witness it deletes except a byte shadow's, so no
/// other surface can point at a missing event.
#[tokio::test]
async fn o6_a_redo_leaves_only_byte_shadows_pointing_at_deleted_events() -> TestResult {
    let database = database("witness_o6", 8).await?;
    let pool = database.pool();
    for (name, shape) in [
        ("f1", Shape::Plain),
        ("f2", Shape::Shadow),
        ("f3", Shape::ByteShadow),
    ] {
        let witness = format!("pre-{name}-3");
        surface(pool, name, 1, Some(&witness), shape).await?;
        hash_path(pool, name, 1).await?;
        preimage(pool, &witness, name, 3).await?;
    }
    surface(pool, "f4", 1, Some("pre-f4-1"), Shape::Plain).await?;
    preimage(pool, "pre-f4-1", "f4", 1).await?;
    surface(pool, "f5", 3, None, Shape::HashPath).await?;
    hash_path(pool, "f5", 3).await?;

    let mut batch = Batch::begin(pool, (3, 3)).await?;
    assert_eq!(batch.names().await?, list(&["f1", "f2", "f3", "f5"]));
    batch.prepare().await?;
    assert_eq!(batch.count(DANGLING_OUTSIDE_BYTE_SHADOWS).await?, 0);
    let dangling_byte_shadows = batch
        .count(
            "SELECT count(*) FROM name_surfaces surface
             WHERE NOT EXISTS (
                 SELECT 1 FROM normalized_events event
                 WHERE event.event_identity = surface.preimage_event_identity)
               AND surface.preimage_event_identity IS NOT NULL",
        )
        .await?;
    assert_eq!(dangling_byte_shadows, 1);
    batch.reanchor().await?;
    assert_eq!(batch.repair().await?, set(&["f1", "f2", "f3"]));
    batch.commit().await?;

    assert_eq!(count(pool, DANGLING_OUTSIDE_BYTE_SHADOWS).await?, 0);
    assert_eq!(count(pool, UNREADABLE_EVENTS).await?, 0);
    database.cleanup().await?;
    Ok(())
}

fn replayed_event(block: i64, hash: &str) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: format!("replayed-{hash}"),
        namespace: "ens".to_owned(),
        logical_name_id: None,
        resource_id: None,
        event_kind: PREIMAGE_OBSERVATION_EVENT_KIND.to_owned(),
        source_family: "ens_v1_registry_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: CHAIN.to_owned(),
        block_number: Some(block),
        block_hash: Some(hash.to_owned()),
        transaction_hash: Some("0xtx".to_owned()),
        transaction_index: Some(0),
        log_index: Some(0),
        raw_fact_ref: json!({}),
        derivation_kind: "raw_log_preimage_observation".to_owned(),
        canonicality_state: "canonical".to_owned(),
        before_state: json!({}),
        after_state: json!({}),
        migration_correlation_ids: Vec::new(),
        consumer_visibility: "activated".to_owned(),
        before_state_explicit: false,
    }
}

/// O7: a batch writes events only onto lineage that is still readable when it commits, so no
/// event of an orphaned block is written. Its state is the readable state the loader read.
#[tokio::test]
async fn o7_a_batch_writes_no_event_onto_a_block_orphaned_before_it_commits() -> TestResult {
    let database = database("witness_o7", 8).await?;
    let pool = database.pool();
    let loaded = vec![(4, "h4".to_owned())];
    let output = BatchOutput {
        normalized_events: vec![replayed_event(4, "h4")],
        ..BatchOutput::default()
    };
    orphan(pool, 4).await?;

    let refused = super::super::super::batch(pool, CHAIN, None, false, false, 0, &loaded, &output)
        .await
        .expect_err("the loaded block is orphaned");
    assert!(refused.to_string().contains("lineage changed"), "{refused}");
    assert_eq!(
        count(pool, "SELECT count(*) FROM normalized_events").await?,
        0
    );

    let replaced = vec![(4, "r4".to_owned())];
    let output = BatchOutput {
        normalized_events: vec![replayed_event(4, "r4")],
        ..BatchOutput::default()
    };
    super::super::super::batch(pool, CHAIN, None, false, false, 0, &replaced, &output).await?;
    assert_eq!(
        count(pool, "SELECT count(*) FROM normalized_events").await?,
        1
    );
    assert_eq!(count(pool, UNREADABLE_EVENTS).await?, 0);
    database.cleanup().await?;
    Ok(())
}

/// O8, served by the range observation arm: the replay observes the preimage earlier than the
/// stored witness.
#[tokio::test]
async fn o8_an_earlier_observation_the_replay_writes_becomes_the_witness() -> TestResult {
    let database = database("witness_o8", 8).await?;
    let pool = database.pool();
    surface(pool, "g", 1, Some("pre-g-5"), Shape::Plain).await?;
    hash_path(pool, "g", 1).await?;
    preimage(pool, "pre-g-5", "g", 5).await?;

    let replay = replayed_preimage("pre-g-3", "g", 3);
    let (names, targets) = single_batch_redo(pool, (3, 3), &replay).await?;

    assert_eq!((names, targets), (list(&[]), set(&["g"])));
    assert_eq!(raw(pool, "g").await?, plain("g", Some("pre-g-3")));
    database.cleanup().await?;
    Ok(())
}

/// O9: residue stays in its set on every redo until an observation decides it, so the redo
/// that drops its bytes is the same one as before.
#[tokio::test]
async fn o9_residue_is_found_on_every_redo_until_its_bytes_drop() -> TestResult {
    let database = database("witness_o9", 8).await?;
    let pool = database.pool();
    surface(pool, "h", 1, None, Shape::Plain).await?;
    surface(pool, "i", 1, Some("pre-i-gone"), Shape::ByteShadow).await?;

    let first = single_batch_redo(pool, (3, 3), "").await?;
    assert_eq!(first, (list(&["h", "i"]), set(&["h", "i"])));
    assert_eq!(raw(pool, "h").await?, plain("h", None));

    hash_path(pool, "h", 7).await?;
    hash_path(pool, "i", 7).await?;
    let second = single_batch_redo(pool, (3, 3), "").await?;
    assert_eq!(second, (list(&["h", "i"]), set(&["h", "i"])));
    assert_eq!(raw(pool, "h").await?, bytes_dropped());
    assert_eq!(raw(pool, "i").await?, bytes_dropped());
    database.cleanup().await?;
    Ok(())
}

/// O11, served by the released-witness set at completion: the first batch released the witness
/// and no later batch observed the name again.
#[tokio::test]
async fn o11_a_witness_released_by_the_first_batch_is_repaired_at_completion() -> TestResult {
    let database = database("witness_o11", 8).await?;
    let pool = database.pool();
    surface(pool, "j", 1, Some("pre-j-3"), Shape::Plain).await?;
    hash_path(pool, "j", 1).await?;
    preimage(pool, "pre-j-3", "j", 3).await?;

    let mut first = Batch::begin(pool, (3, 4)).await?;
    assert_eq!(first.names().await?, list(&["j"]));
    first.prepare().await?;
    first.commit().await?;

    let mut last = Batch::begin(pool, (3, 4)).await?;
    assert_eq!(last.names().await?, list(&["j"]));
    assert_eq!(last.mutant_names(Mutant::WithoutReleased).await?, list(&[]));
    last.reanchor().await?;
    let (missed, kept_bytes) = last.mutant_repair(Mutant::WithoutReleased).await?;
    assert_eq!(missed, set(&[]));
    assert_eq!(last.repair().await?, set(&["j"]));
    assert_ne!(last.rows().await?, kept_bytes);
    last.commit().await?;

    assert_eq!(raw(pool, "j").await?, bytes_dropped());
    database.cleanup().await?;
    Ok(())
}

fn takes_evidence(identity: &str, name: &str, block: i64) -> String {
    format!(
        "{}
         UPDATE name_surfaces SET preimage_event_identity = '{identity}'
         WHERE logical_name_id = 'ens:{name}';",
        replayed_preimage(identity, name, block)
    )
}

/// Brief row 1, one batch: NULL when the redo starts, so the released-witness set serves
/// statement 1. The replay then gives the surface a witness in the range, which the range
/// observation arm serves at the repair.
#[tokio::test]
async fn a_null_witness_the_replay_fills_in_one_batch() -> TestResult {
    let database = database("witness_null_filled_one", 8).await?;
    let pool = database.pool();
    surface(pool, "k", 1, None, Shape::Plain).await?;
    hash_path(pool, "k", 1).await?;

    let mut batch = Batch::begin(pool, (3, 3)).await?;
    assert_eq!(batch.names().await?, list(&["k"]));
    assert_eq!(
        batch.mutant_names(Mutant::WithoutReleased).await?,
        list(&[])
    );
    batch.prepare().await?;
    batch.write(&takes_evidence("pre-k-3", "k", 3)).await?;
    batch.reanchor().await?;
    assert_eq!(
        batch.mutant_repair(Mutant::WithoutReleased).await?.0,
        set(&["k"])
    );
    assert_eq!(batch.repair().await?, set(&["k"]));
    batch.commit().await?;

    assert_eq!(raw(pool, "k").await?, plain("k", Some("pre-k-3")));
    database.cleanup().await?;
    Ok(())
}

/// Brief row 1 across batches: a middle batch fills the witness, so at completion the range
/// observation arm serves both statements.
#[tokio::test]
async fn a_null_witness_a_middle_batch_fills() -> TestResult {
    let database = database("witness_null_filled_middle", 8).await?;
    let pool = database.pool();
    surface(pool, "l", 1, None, Shape::Plain).await?;

    let mut first = Batch::begin(pool, (3, 4)).await?;
    assert_eq!(first.names().await?, list(&["l"]));
    first.prepare().await?;
    first.commit().await?;

    let mut middle = Batch::begin(pool, (3, 4)).await?;
    middle.write(&takes_evidence("pre-l-3", "l", 3)).await?;
    middle.commit().await?;

    let mut last = Batch::begin(pool, (3, 4)).await?;
    assert_eq!(last.names().await?, list(&["l"]));
    assert_eq!(last.mutant_names(Mutant::RangeOnly).await?, list(&["l"]));
    last.reanchor().await?;
    assert_eq!(last.repair().await?, set(&["l"]));
    last.commit().await?;

    assert_eq!(raw(pool, "l").await?, plain("l", Some("pre-l-3")));
    database.cleanup().await?;
    Ok(())
}

/// Brief row 2: an orphaned block that becomes canonical again makes its witness readable again,
/// and the orphaned-block set stops serving it.
#[tokio::test]
async fn an_orphaned_witness_block_that_becomes_canonical_again() -> TestResult {
    let database = database("witness_reorg_back", 8).await?;
    let pool = database.pool();
    surface(pool, "m", 1, Some("pre-m-2"), Shape::Plain).await?;
    hash_path(pool, "m", 1).await?;
    preimage(pool, "pre-m-2", "m", 2).await?;
    orphan(pool, 2).await?;

    let mut orphaned = Batch::begin(pool, (5, 5)).await?;
    assert_eq!(orphaned.names().await?, list(&["m"]));
    assert_eq!(orphaned.targets().await?, set(&["m"]));
    assert_eq!(
        orphaned.mutant_names(Mutant::WithoutOrphaned).await?,
        list(&[])
    );
    orphaned.rollback().await?;

    restore(pool, 2).await?;
    let (names, targets) = single_batch_redo(pool, (5, 5), "").await?;

    assert_eq!((names, targets), (list(&[]), set(&[])));
    assert_eq!(raw(pool, "m").await?, plain("m", Some("pre-m-2")));
    database.cleanup().await?;
    Ok(())
}

/// Brief row 3: a byte shadow whose witness the range preparation deletes. Statement 1 finds it
/// through the range observation, the repair through the byte-shadow set.
#[tokio::test]
async fn a_byte_shadow_whose_witness_the_range_deletes() -> TestResult {
    let database = database("witness_byte_shadow_deleted", 8).await?;
    let pool = database.pool();
    surface(pool, "n", 1, Some("pre-n-3"), Shape::ByteShadow).await?;
    hash_path(pool, "n", 1).await?;
    preimage(pool, "pre-n-3", "n", 3).await?;

    let mut batch = Batch::begin(pool, (3, 3)).await?;
    assert_eq!(batch.names().await?, list(&["n"]));
    assert_eq!(
        batch.mutant_names(Mutant::WithoutByteShadow).await?,
        list(&["n"])
    );
    batch.prepare().await?;
    let kept: i64 = batch
        .count(
            "SELECT count(*) FROM name_surfaces
             WHERE preimage_event_identity = 'pre-n-3'
               AND NOT EXISTS (SELECT 1 FROM normalized_events WHERE event_identity = 'pre-n-3')",
        )
        .await?;
    assert_eq!(kept, 1, "the release keeps a byte shadow's witness");
    batch.reanchor().await?;
    let (missed, kept_bytes) = batch.mutant_repair(Mutant::WithoutByteShadow).await?;
    assert_eq!(missed, set(&[]));
    assert_eq!(batch.repair().await?, set(&["n"]));
    assert_ne!(batch.rows().await?, kept_bytes);
    batch.commit().await?;

    assert_eq!(raw(pool, "n").await?, bytes_dropped());
    database.cleanup().await?;
    Ok(())
}
