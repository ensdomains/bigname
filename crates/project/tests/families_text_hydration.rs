//! F6 through the actual Follow preparation, pinned RPC, publication, reader and undo paths.
//! The admitted legacy resolver is the existing text hydration allowlist's first address.
//! (upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L71 @ ens_app_v3@7175858)
#[path = "families_hydration/reverse.rs"]
mod reverse;
#[path = "families_hydration/rpc.rs"]
mod rpc;
#[path = "families_support/mod.rs"]
mod support;

use anyhow::Result;
use bigname_project::families::{self, FamilyMode, FamilyOptions, FamilyOutcome};
use serde_json::{Value, json};
use support::{CONTENT_HASH, Event, Fixture, hash, marker};

const CHAIN: &str = "ethereum-mainnet";
const RESOLVER: &str = "0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41";
const NODE: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
const RESOURCE: &str = "00000000-0000-0000-0000-000000000001";

/// One family run to `block` with hydration configured, whatever the readable head is.
async fn apply(
    fixture: &Fixture,
    block: i64,
    mode: FamilyMode,
    rpc: &rpc::Rpc,
) -> (FamilyOutcome, Option<bigname_project::ProjectError>) {
    let token = families::input_token(&fixture.pool, CHAIN)
        .await
        .expect("input token");
    families::run(
        &fixture.pool,
        CHAIN,
        &marker(block),
        mode,
        &token,
        &FamilyOptions::new(CONTENT_HASH).with_hydration(rpc.urls()),
    )
    .await
}

/// Make `block` the highest readable block, then run the families to it.
async fn run(
    fixture: &Fixture,
    block: i64,
    mode: FamilyMode,
    rpc: &rpc::Rpc,
) -> Result<FamilyOutcome> {
    rpc::head(&fixture.pool, block).await?;
    let (outcome, error) = apply(fixture, block, mode, rpc).await;
    if let Some(error) = error {
        return Err(error.into());
    }
    assert_eq!(outcome.marker, Some(marker(block)));
    Ok(outcome)
}

async fn manifest(fixture: &Fixture, block: i64, active: bool) -> Result<()> {
    let payload = json!({"contracts":[{"address":RESOLVER,"role":"public_resolver","read_features":["text"]}]});
    let id = match sqlx::query_scalar::<_, i64>("SELECT manifest_id FROM manifest_versions LIMIT 1")
        .fetch_optional(&fixture.pool).await? {
        Some(id) => id,
        None => sqlx::query_scalar("INSERT INTO manifest_versions (manifest_version,namespace,source_family,
            chain_id,deployment_label,rollout_status,normalizer_version,file_path,manifest_payload)
            VALUES (1,'ens','ens_v1_resolver_l1',$1,'fixture','active','fixture','fixture/text.yaml',$2)
            RETURNING manifest_id")
            .bind(CHAIN).bind(&payload).fetch_one(&fixture.pool).await?,
    };
    sqlx::query(
        "INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,
        manifest_version,source_manifest_id,chain_id,block_number,block_hash,derivation_kind,
        canonicality_state,before_state,after_state,raw_fact_ref)
        VALUES ($1,'ens','SourceManifestUpdated','ens_v1_resolver_l1',1,$2,$3,$4,$5,
        'ens_v2_registry_resource_surface','canonical','{}',
        jsonb_build_object('rollout_status',$6::text,'manifest_payload',$7::jsonb),'{}')",
    )
    .bind(format!("manifest:{block}"))
    .bind(id)
    .bind(CHAIN)
    .bind(block)
    .bind(hash(block))
    .bind(if active { "active" } else { "retired" })
    .bind(payload)
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

async fn fixture() -> Result<(Fixture, rpc::Rpc)> {
    let fixture = Fixture::new("family_text_hydration", 8).await?;
    fixture.lineage(CHAIN, 8).await?;
    manifest(&fixture, 0, true).await?;
    let name = format!("ens:{NODE}");
    fixture.surface_on(CHAIN, &name, NODE).await?;
    sqlx::query(
        "INSERT INTO resources (resource_id,chain_id,block_hash,block_number,canonicality_state)
        VALUES ($1::uuid,$2,$3,0,'canonical')",
    )
    .bind(RESOURCE)
    .bind(CHAIN)
    .bind(hash(0))
    .execute(&fixture.pool)
    .await?;
    fixture
        .event(
            Event::new("pointer:1", 1, 1, "ResolverChanged", "ens_v1_registry_l1")
                .on(CHAIN)
                .name(&name)
                .resource(RESOURCE)
                .after(json!({"node":NODE,"resolver":RESOLVER})),
        )
        .await?;
    let rpc = rpc::Rpc::new().await?;
    run(&fixture, 0, FamilyMode::Normal, &rpc).await?;
    Ok((fixture, rpc))
}

async fn text(fixture: &Fixture, block: i64, value: Option<&str>) -> Result<()> {
    let mut after = json!({"node":NODE,"resolver":RESOLVER,"record_key":"text:url",
        "record_family":"text","selector_key":"url","source_event":"TextChanged"});
    if let Some(value) = value {
        after["value"] = json!(value);
    }
    fixture
        .event(
            Event::new(
                &format!("text:{block}"),
                block,
                2,
                "RecordChanged",
                "ens_v1_resolver_l1",
            )
            .on(CHAIN)
            .after(after)
            .raw(json!({"emitting_address":RESOLVER})),
        )
        .await?;
    Ok(())
}

async fn value_row(fixture: &Fixture) -> Result<Value> {
    Ok(
        sqlx::query_scalar("SELECT to_jsonb(v) FROM project_node_record_value v")
            .fetch_one(&fixture.pool)
            .await?,
    )
}

async fn entry(fixture: &Fixture) -> Result<Value> {
    let inventory = bigname_storage::families::records::load_family_record_inventory_detail(
        &fixture.pool,
        CHAIN,
        RESOURCE.parse()?,
        bigname_storage::families::records::FamilyAttribution::Given(Default::default()),
    )
    .await?
    .expect("serving pointer");
    Ok(inventory.row.entries[0].clone())
}

#[tokio::test]
async fn text_follow_hydrates_new_writes_retries_failures_and_preserves_empty_results() -> Result<()>
{
    let (fixture, rpc) = fixture().await?;
    text(&fixture, 1, None).await?;
    rpc.answer(1, Some("https://example.test"));
    run(&fixture, 1, FamilyMode::Normal, &rpc).await?;
    let first = value_row(&fixture).await?;
    assert!(first["value"].is_null(), "raw event baseline is unchanged");
    assert_eq!(first["status"], "unsupported");
    assert_eq!(first["hydrated_at_block"], 1);
    assert_eq!(entry(&fixture).await?["value"], "https://example.test");
    run(&fixture, 2, FamilyMode::Normal, &rpc).await?;
    assert_eq!(
        rpc.calls(),
        vec![(hash(1), 1)],
        "canonical text is not reread just because the head advanced"
    );
    // Block 3 replaces the write, and the endpoint fails every aggregate sent at block 3. The
    // batch observed nothing, so hydration writes nothing: the overlay and the block it was read
    // at stay as block 1 left them. The event's own write still lands, and the old overlay is
    // not served for the new write.
    text(&fixture, 3, None).await?;
    rpc.answer(3, None);
    let outcome = run(&fixture, 3, FamilyMode::Normal, &rpc).await?;
    let unobserved = value_row(&fixture).await?;
    assert_eq!(
        unobserved["block_number"], 3,
        "the event write is published"
    );
    assert_eq!(unobserved["hydrated_value"], first["hydrated_value"]);
    assert_eq!(unobserved["hydrated_at_block"], 1);
    assert_eq!(
        entry(&fixture).await?["status"],
        "unsupported",
        "an overlay read for the replaced write is not served"
    );
    let text_outcome = outcome.hydration.text;
    assert_eq!(
        (
            text_outcome.rpc_calls,
            text_outcome.rpc_failures,
            text_outcome.not_observed,
            text_outcome.value_writes + text_outcome.schedule_writes
        ),
        (1, 1, 1, 0)
    );
    assert_eq!(outcome.hydration.unserved_passes, 1);
    assert_eq!(rpc.probes(), 1, "one probe told the endpoint failure apart");
    // Block 4's aggregate is answered, but the resolver call inside it fails. That is the
    // fail-closed case: a null overlay stamped with the attempt.
    rpc.answer(4, Some("unused"));
    rpc.fail_call(RESOLVER);
    let outcome = run(&fixture, 4, FamilyMode::Normal, &rpc).await?;
    rpc.clear_faults();
    let failed = value_row(&fixture).await?;
    assert!(failed["hydrated_value"].is_null());
    assert_eq!(failed["hydrated_at_block"], 4);
    assert_eq!(entry(&fixture).await?["status"], "unsupported");
    let text_outcome = outcome.hydration.text;
    assert_eq!(
        (
            text_outcome.rpc_failures,
            text_outcome.failed_calls,
            text_outcome.value_writes
        ),
        (0, 1, 1)
    );
    rpc.answer(5, Some(""));
    run(&fixture, 5, FamilyMode::Normal, &rpc).await?;
    assert_eq!(entry(&fixture).await?["status"], "not_found");
    let journal: Value = sqlx::query_scalar(
        "SELECT before_image FROM project_family_undo
        WHERE chain_id=$1 AND block_number=5 AND family='project_node_record_value'",
    )
    .bind(CHAIN)
    .fetch_one(&fixture.pool)
    .await?;
    assert!(
        journal["hydrated_value"].is_null(),
        "empty-block retry is journalled"
    );
    let calls = rpc.calls().len();
    run(&fixture, 5, FamilyMode::Redo { from: 5, to: 5 }, &rpc).await?;
    assert_eq!(rpc.calls().len(), calls, "replay never hydrates");
    assert!(
        value_row(&fixture).await?["hydrated_value"].is_null(),
        "undo restored baseline"
    );
    rpc.answer(6, Some("refreshed"));
    run(&fixture, 6, FamilyMode::Normal, &rpc).await?;
    assert_eq!(entry(&fixture).await?["value"], "refreshed");
    run(&fixture, 6, FamilyMode::Rebuild, &rpc).await?;
    assert_eq!(rpc.calls().len(), calls + 1, "rebuild never hydrates");
    assert!(value_row(&fixture).await?["hydrated_value"].is_null());
    rpc.answer(7, Some("after rebuild"));
    run(&fixture, 7, FamilyMode::Normal, &rpc).await?;
    assert_eq!(entry(&fixture).await?["value"], "after rebuild");
    fixture.cleanup().await
}

#[tokio::test]
async fn text_admission_loss_retracts_without_rpc_and_undo_restores_the_overlay() -> Result<()> {
    let (fixture, rpc) = fixture().await?;
    text(&fixture, 1, None).await?;
    rpc.answer(1, Some("admitted"));
    run(&fixture, 1, FamilyMode::Normal, &rpc).await?;
    let first = value_row(&fixture).await?;
    manifest(&fixture, 2, false).await?;
    run(&fixture, 2, FamilyMode::Normal, &rpc).await?;
    assert_eq!(rpc.calls().len(), 1);
    assert!(value_row(&fixture).await?["hydrated_value"].is_null());
    assert!(value_row(&fixture).await?["hydrated_at_block"].is_null());
    assert_eq!(entry(&fixture).await?["status"], "unsupported");
    families::undo_to(&fixture.pool, CHAIN, 1).await?;
    assert_eq!(
        value_row(&fixture).await?,
        first,
        "normal family undo restores the whole row"
    );
    assert_eq!(entry(&fixture).await?["value"], "admitted");
    fixture.cleanup().await
}

#[tokio::test]
async fn text_read_rejects_orphaned_hydration_and_follow_invalidates_old_record_versions()
-> Result<()> {
    let (fixture, rpc) = fixture().await?;
    text(&fixture, 1, None).await?;
    run(&fixture, 1, FamilyMode::Normal, &rpc).await?;
    rpc.answer(2, Some("first"));
    run(&fixture, 2, FamilyMode::Normal, &rpc).await?;
    assert_eq!(entry(&fixture).await?["value"], "first");
    sqlx::query("UPDATE chain_lineage SET canonicality_state='orphaned' WHERE chain_id=$1 AND block_number=2")
        .bind(CHAIN).execute(&fixture.pool).await?;
    assert_eq!(
        entry(&fixture).await?["status"],
        "unsupported",
        "read rejects orphaned execution immediately"
    );
    sqlx::query("UPDATE chain_lineage SET canonicality_state='canonical' WHERE chain_id=$1 AND block_number=2")
        .bind(CHAIN).execute(&fixture.pool).await?;
    fixture
        .event(
            Event::new(
                "version:3",
                3,
                1,
                "RecordVersionChanged",
                "ens_v1_resolver_l1",
            )
            .on(CHAIN)
            .after(json!({"node":NODE,"resolver":RESOLVER})),
        )
        .await?;
    run(&fixture, 3, FamilyMode::Normal, &rpc).await?;
    assert!(value_row(&fixture).await?["hydrated_value"].is_null());
    assert!(
        entry(&fixture).await?.is_null(),
        "version boundary excludes the old record"
    );
    assert_eq!(rpc.calls().len(), 2, "excluded value is not hydrated");
    text(&fixture, 4, Some("retained in event")).await?;
    run(&fixture, 4, FamilyMode::Normal, &rpc).await?;
    assert_eq!(entry(&fixture).await?["value"], "retained in event");
    assert_eq!(rpc.calls().len(), 2, "retained values need no call");
    fixture.cleanup().await
}

async fn keyed_text(fixture: &Fixture, block: i64, log: i64, key: &str) -> Result<()> {
    fixture
        .event(
            Event::new(
                &format!("text:{block}:{key}"),
                block,
                log,
                "RecordChanged",
                "ens_v1_resolver_l1",
            )
            .on(CHAIN)
            .after(
                json!({"node":NODE,"resolver":RESOLVER,"record_key":format!("text:{key}"),
                "record_family":"text","selector_key":key,"source_event":"TextChanged"}),
            )
            .raw(json!({"emitting_address":RESOLVER})),
        )
        .await?;
    Ok(())
}

/// Each key's `hydrated_at_block` and overlay status, in key order.
async fn attempts(fixture: &Fixture) -> Result<Vec<(String, Option<i64>, Option<String>)>> {
    Ok(sqlx::query_as(
        "SELECT selector_key, hydrated_at_block, hydrated_value ->> 'status'
        FROM project_node_record_value ORDER BY record_key",
    )
    .fetch_all(&fixture.pool)
    .await?)
}

fn keys_at(rows: &[(String, Option<i64>, Option<String>)], block: i64) -> Vec<String> {
    rows.iter()
        .filter(|(_, at, _)| *at == Some(block))
        .map(|(key, _, _)| key.clone())
        .collect()
}

#[tokio::test]
async fn text_rebuild_backlog_rolls_over_blocks_under_the_limit_with_changes_first() -> Result<()> {
    const BACKLOG: usize = 600;
    let key = |index: usize| format!("k{index:04}");
    let range = |from: usize, to: usize| (from..to).map(key).collect::<Vec<_>>();
    let (fixture, rpc) = fixture().await?;
    for index in 0..BACKLOG {
        keyed_text(&fixture, 1, index as i64 + 2, &key(index)).await?;
    }
    // A rebuild never hydrates, so every eligible selector is left with a null overlay.
    run(&fixture, 1, FamilyMode::Rebuild, &rpc).await?;
    assert!(rpc.calls().is_empty());
    // Block 2's aggregate is answered but every resolver call in it fails: the first 250 keys are
    // stamped with the attempt and nothing is served.
    rpc.answer(2, Some("unused"));
    rpc.fail_call(RESOLVER);
    run(&fixture, 2, FamilyMode::Normal, &rpc).await?;
    rpc.clear_faults();
    let rows = attempts(&fixture).await?;
    assert_eq!(keys_at(&rows, 2), range(0, 250));
    assert!(rows.iter().all(|(_, _, status)| status.is_none()));
    // Block 3 writes a key that sorts last: it is read first, then never-read keys, not retries.
    keyed_text(&fixture, 3, 2, "zz").await?;
    for block in 3..=6 {
        rpc.answer(block, Some("hydrated"));
    }
    run(&fixture, 3, FamilyMode::Normal, &rpc).await?;
    let mut expected = range(250, 499);
    expected.push("zz".to_owned());
    assert_eq!(keys_at(&attempts(&fixture).await?, 3), expected);
    // Then the rest of the never-read keys, then the oldest failed attempts.
    run(&fixture, 4, FamilyMode::Normal, &rpc).await?;
    let mut expected = range(0, 149);
    expected.extend(range(499, BACKLOG));
    assert_eq!(keys_at(&attempts(&fixture).await?, 4), expected);
    run(&fixture, 5, FamilyMode::Normal, &rpc).await?;
    assert_eq!(keys_at(&attempts(&fixture).await?, 5), range(149, 250));
    run(&fixture, 6, FamilyMode::Normal, &rpc).await?;
    let rows = attempts(&fixture).await?;
    assert_eq!(rows.len(), BACKLOG + 1);
    assert!(
        rows.iter()
            .all(|(_, _, status)| status.as_deref() == Some("success")),
        "the whole backlog is hydrated"
    );
    let per_call: Vec<usize> = rpc.calls().into_iter().map(|(_, count)| count).collect();
    assert_eq!(
        per_call,
        vec![250, 250, 250, 101],
        "one bounded batch per block, none once the backlog is drained"
    );
    fixture.cleanup().await
}

/// The production selection query, run as the Follow path runs it with no block changes.
async fn selected(fixture: &Fixture, block: i64) -> Result<Vec<String>> {
    let sql = text_selection_sql();
    let rows: Vec<Value> = sqlx::query_scalar(&sql)
        .bind(CHAIN)
        .bind(block)
        .bind(json!([]))
        .bind(json!([]))
        .bind(json!([]))
        .bind(vec![RESOLVER])
        .bind(250_i64)
        .bind(json!([]))
        .bind(false)
        .fetch_all(&fixture.pool)
        .await?;
    Ok(rows
        .iter()
        .map(|row| row["selector_key"].as_str().unwrap().to_owned())
        .collect())
}

fn text_selection_sql() -> String {
    include_str!("../src/families/hydrate/text.sql")
        .replace(
            "{value_emission_ordinal}",
            &bigname_storage::families::position::emission_ordinal_sql(
                "value.event_identity",
                "value.transaction_index",
                "value.log_index",
            ),
        )
        .replace(
            "{boundary_emission_ordinal}",
            &bigname_storage::families::position::emission_ordinal_sql(
                "boundary.event_identity",
                "boundary.transaction_index",
                "boundary.log_index",
            ),
        )
}

/// Restore the derived index after this test deliberately writes source rows outside Project.
async fn sync_text_work(fixture: &Fixture, block: i64) -> Result<()> {
    let targets: Value = sqlx::query_scalar(
        "SELECT COALESCE(jsonb_agg(to_jsonb(v)), '[]') FROM project_node_record_value v",
    )
    .fetch_one(&fixture.pool)
    .await?;
    let work: Vec<Value> = sqlx::query_scalar(&text_selection_sql())
        .bind(CHAIN)
        .bind(block)
        .bind(json!([]))
        .bind(json!([]))
        .bind(json!([]))
        .bind(vec![RESOLVER])
        .bind(250_i64)
        .bind(targets)
        .bind(true)
        .fetch_all(&fixture.pool)
        .await?;
    sqlx::query("DELETE FROM project_text_hydration_work")
        .execute(&fixture.pool)
        .await?;
    sqlx::query(
        "INSERT INTO project_text_hydration_work SELECT * FROM
        jsonb_populate_recordset(NULL::project_text_hydration_work, $1)",
    )
    .bind(json!(work))
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

/// Copies the hydrated `url` row under `count` new keys `{prefix}{index:05}`, overriding columns.
async fn copies(fixture: &Fixture, prefix: &str, count: i32, columns: Value) -> Result<()> {
    sqlx::query(
        "INSERT INTO project_node_record_value
        SELECT (jsonb_populate_record(v, to_jsonb(v) || $3 || jsonb_build_object(
            'record_key', 'text:' || $1 || lpad(i::text, 5, '0'),
            'selector_key', $1 || lpad(i::text, 5, '0')))).*
        FROM project_node_record_value v, generate_series(1, $2) i
        WHERE v.record_key = 'text:url'",
    )
    .bind(prefix)
    .bind(count)
    .bind(columns)
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

#[tokio::test]
async fn text_backlog_is_cut_in_the_query_behind_thousands_of_current_selectors() -> Result<()> {
    let key = |prefix: &str, index: usize| format!("{prefix}{index:05}");
    let keys = |prefix: &str, from: usize, to: usize| {
        (from..=to)
            .map(|index| key(prefix, index))
            .collect::<Vec<_>>()
    };
    let (fixture, rpc) = fixture().await?;
    text(&fixture, 1, None).await?;
    rpc.answer(1, Some("current"));
    run(&fixture, 1, FamilyMode::Normal, &rpc).await?;
    let hydrated = value_row(&fixture).await?;
    assert_eq!(hydrated["hydrated_value"]["status"], "success");
    // Thousands of selectors already current at block 1, which no later block may transfer.
    copies(&fixture, "c", 3000, json!({})).await?;
    // Never-read selectors, as a rebuild leaves them.
    copies(
        &fixture,
        "n",
        300,
        json!({"hydrated_value": null, "hydrated_at_block": null}),
    )
    .await?;
    // Failed reads at block 1. They sort first by key, yet wait behind every never-read selector.
    copies(
        &fixture,
        "a",
        5,
        json!({"hydrated_value": null, "hydrated_at_block": 1}),
    )
    .await?;
    // Reads made on a block 1 that a reorg orphaned: no longer readable, so stale again.
    let fork = format!("0x{}", "f".repeat(64));
    sqlx::query(
        "INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state)
         VALUES ($1, $2, $3, 1, to_timestamp(1800000012), 'orphaned')",
    )
    .bind(CHAIN)
    .bind(&fork)
    .bind(hash(0))
    .execute(&fixture.pool)
    .await?;
    let mut orphaned = hydrated["hydrated_value"].clone();
    orphaned["block_hash"] = json!(fork);
    copies(&fixture, "r", 5, json!({"hydrated_value": orphaned})).await?;

    sync_text_work(&fixture, 2).await?;

    // The query itself returns only the block's share: never-read selectors, in key order.
    assert_eq!(selected(&fixture, 2).await?, keys("n", 1, 250));
    rpc.answer(2, Some("fresh"));
    run(&fixture, 2, FamilyMode::Normal, &rpc).await?;
    assert_eq!(keys_at(&attempts(&fixture).await?, 2), keys("n", 1, 250));
    // Then the rest of the never-read selectors, then the older attempts, failed or orphaned.
    let mut expected = keys("a", 1, 5);
    expected.extend(keys("n", 251, 300));
    expected.extend(keys("r", 1, 5));
    let mut share = selected(&fixture, 3).await?;
    share.sort();
    assert_eq!(share, expected);
    rpc.answer(3, Some("fresh"));
    run(&fixture, 3, FamilyMode::Normal, &rpc).await?;
    assert_eq!(keys_at(&attempts(&fixture).await?, 3), expected);
    // Nothing is left, and the current selectors were never read again.
    assert!(selected(&fixture, 4).await?.is_empty());
    run(&fixture, 4, FamilyMode::Normal, &rpc).await?;
    let per_call: Vec<usize> = rpc.calls().into_iter().map(|(_, count)| count).collect();
    assert_eq!(per_call, vec![1, 250, 60]);
    let rows = attempts(&fixture).await?;
    assert_eq!(rows.len(), 1 + 3000 + 300 + 5 + 5);
    assert!(
        rows.iter()
            .all(|(_, _, status)| status.as_deref() == Some("success"))
    );
    fixture.cleanup().await
}

// The query reads a record version position as Position::from_map does, part by part through
// serde_json's `as_i64`: a fractional or out-of-range block number leaves no boundary, and an
// out-of-range transaction or log index reads as absent (families/position_tests.rs, SHARED_JSON).
#[tokio::test]
async fn text_selection_reads_the_version_position_as_rust_does() -> Result<()> {
    let (fixture, rpc) = fixture().await?;
    text(&fixture, 1, None).await?;
    rpc.answer(1, Some("current"));
    run(&fixture, 1, FamilyMode::Normal, &rpc).await?;
    // The selector sits at block 1, never read. A boundary after it keeps it inactive.
    sqlx::query(
        "UPDATE project_node_record_value SET hydrated_value = NULL, hydrated_at_block = NULL",
    )
    .execute(&fixture.pool)
    .await?;
    for version in [
        json!({"block_number": 5, "event_identity": "e"}),
        json!({"block_number": 0, "event_identity": "e"}),
        json!({"block_number": 7.5, "event_identity": "e"}),
        json!({"block_number": 1e20, "event_identity": "e"}),
        json!({"block_number": 5, "event_identity": 5}),
        json!({"block_number": 5, "transaction_index": 1e20, "log_index": 0.5,
               "event_identity": "e"}),
        json!({"block_number": 0, "transaction_index": 1e20, "log_index": 0,
               "event_identity": "e"}),
    ] {
        // As Rust reads it back from the database.
        let stored: Value = sqlx::query_scalar(
            "UPDATE project_node_record_partition SET version_position = $1
             RETURNING version_position",
        )
        .bind(&version)
        .fetch_one(&fixture.pool)
        .await?;
        sync_text_work(&fixture, 2).await?;
        let boundary = stored["block_number"]
            .as_i64()
            .filter(|_| stored["event_identity"].is_string());
        let active = boundary.is_none_or(|block| 1 > block);
        let expected: Vec<String> = if active {
            vec!["url".into()]
        } else {
            Vec::new()
        };
        assert_eq!(selected(&fixture, 2).await?, expected, "{stored}");
    }
    fixture.cleanup().await
}

#[path = "families_hydration/plans.rs"]
mod plans;

async fn pending_text(fixture: &Fixture) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT count(*) FROM project_text_hydration_work")
            .fetch_one(&fixture.pool)
            .await?,
    )
}

async fn text_plan(fixture: &Fixture, block: i64) -> Result<Value> {
    Ok(sqlx::query_scalar(&format!(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {}",
        text_selection_sql()
    ))
    .bind(CHAIN)
    .bind(block)
    .bind(json!([]))
    .bind(json!([]))
    .bind(json!([]))
    .bind(vec![RESOLVER])
    .bind(250_i64)
    .bind(json!([]))
    .bind(false)
    .fetch_one(&fixture.pool)
    .await?)
}

#[tokio::test]
async fn text_selection_visits_only_pending_rows_among_fifty_thousand_completed_values()
-> Result<()> {
    let (fixture, rpc) = fixture().await?;
    text(&fixture, 1, None).await?;
    rpc.answer(1, Some("complete"));
    run(&fixture, 1, FamilyMode::Normal, &rpc).await?;
    assert_eq!(pending_text(&fixture).await?, 0);
    copies(&fixture, "complete", 50_000, json!({})).await?;
    // A small pending set must also avoid retained dependencies and canonical chain history.
    sqlx::raw_sql(
        "INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         SELECT 'ethereum-mainnet', 'extra-' || n, n, now(), 'canonical'
         FROM generate_series(100, 50099) n;
         INSERT INTO project_node_record_partition
         SELECT (jsonb_populate_record(NULL::project_node_record_partition,
             to_jsonb(p) || jsonb_build_object('arm_identity', 'extra-' || n))).*
         FROM (SELECT * FROM project_node_record_partition LIMIT 1) p,
             generate_series(1, 50000) n;
         INSERT INTO project_resolver_classification
         SELECT (jsonb_populate_record(NULL::project_resolver_classification,
             to_jsonb(c) || jsonb_build_object('resolver_address', 'extra-' || n))).*
         FROM (SELECT * FROM project_resolver_classification LIMIT 1) c,
             generate_series(1, 50000) n;
         INSERT INTO name_surfaces
         SELECT (jsonb_populate_record(NULL::name_surfaces,
             to_jsonb(s) || jsonb_build_object('logical_name_id', 'ens:extra-' || n,
                 'namehash', 'extra-' || n))).*
         FROM (SELECT * FROM name_surfaces LIMIT 1) s, generate_series(1, 50000) n;
         ANALYZE chain_lineage; ANALYZE project_node_record_partition;
         ANALYZE project_resolver_classification; ANALYZE name_surfaces",
    )
    .execute(&fixture.pool)
    .await?;
    copies(
        &fixture,
        "pending",
        8,
        json!({"hydrated_value": null, "hydrated_at_block": null}),
    )
    .await?;
    sync_text_work(&fixture, 2).await?;
    sqlx::raw_sql("ANALYZE project_node_record_value; ANALYZE project_text_hydration_work")
        .execute(&fixture.pool)
        .await?;
    assert_eq!(pending_text(&fixture).await?, 8);
    // A text write changes the partition's event position, but not its record version. That
    // must not expand one new key into every retained sibling in the same partition.
    let mut partition: Value =
        sqlx::query_scalar("SELECT to_jsonb(p) FROM project_node_record_partition p LIMIT 1")
            .fetch_one(&fixture.pool)
            .await?;
    partition["block_number"] = json!(2);
    let target_plan: Value = sqlx::query_scalar(&format!(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {}",
        include_str!("../src/families/hydrate/text_keys.sql")
    ))
    .bind(CHAIN)
    .bind(json!([]))
    .bind(json!([partition]))
    .bind(json!([]))
    .fetch_one(&fixture.pool)
    .await?;
    plans::save("text-unchanged-version-among-50000", &target_plan)?;
    assert_eq!(
        plans::visits(&target_plan, "project_node_record_value"),
        0.0,
        "{target_plan}"
    );
    let plan = text_plan(&fixture, 50_100).await?;
    plans::save("text-pending-among-50000", &plan)?;
    let generic = plans::generic(
        &fixture.pool,
        &text_selection_sql(),
        &format!(
            "'ethereum-mainnet', 50100, '[]', '[]', '[]', ARRAY['{RESOLVER}'], 250, '[]', false"
        ),
    )
    .await?;
    plans::save("text-pending-generic-among-50000", &generic)?;
    for relation in [
        "chain_lineage",
        "project_node_record_partition",
        "project_resolver_classification",
        "name_surfaces",
    ] {
        assert!(
            plans::visits(&generic, relation) <= 8.0,
            "unbounded {relation}"
        );
        assert!(
            plans::visits(&plan, relation) <= 8.0,
            "unbounded {relation}"
        );
    }
    assert!(
        plans::visits(&generic, "project_node_record_value") <= 8.0,
        "{generic}"
    );
    let dependency_generic = plans::generic(
        &fixture.pool,
        include_str!("../src/families/hydrate/text_keys.sql"),
        &format!(
            "'ethereum-mainnet', '[]', '{}', '[]'",
            json!([partition]).to_string().replace('\'', "''")
        ),
    )
    .await?;
    plans::save(
        "text-unchanged-version-generic-among-50000",
        &dependency_generic,
    )?;
    assert_eq!(
        plans::visits(&dependency_generic, "project_node_record_value"),
        0.0,
        "{dependency_generic}"
    );
    assert!(
        plans::visits(&plan, "project_node_record_value") <= 8.0,
        "{plan}"
    );
    // The extra history leaves the readable lineage, so block 2 is the head again.
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'orphaned' WHERE block_hash LIKE 'extra-%'",
    )
    .execute(&fixture.pool)
    .await?;
    keyed_text(&fixture, 2, 1, "new-same-partition").await?;
    rpc.answer(2, Some("filled"));
    run(&fixture, 2, FamilyMode::Normal, &rpc).await?;
    assert_eq!(rpc.calls(), vec![(hash(1), 1), (hash(2), 9)]);
    assert_eq!(pending_text(&fixture).await?, 0);
    let plan = text_plan(&fixture, 3).await?;
    plans::save("text-drained-among-50000", &plan)?;
    assert_eq!(
        plans::visits(&plan, "project_node_record_value"),
        0.0,
        "{plan}"
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn undo_requeues_a_completed_text_read_after_its_block_is_orphaned() -> Result<()> {
    let (fixture, rpc) = fixture().await?;
    text(&fixture, 1, None).await?;
    run(&fixture, 1, FamilyMode::Normal, &rpc).await?;
    assert_eq!(pending_text(&fixture).await?, 1);
    rpc.answer(2, Some("completed"));
    run(&fixture, 2, FamilyMode::Normal, &rpc).await?;
    assert_eq!(pending_text(&fixture).await?, 0);
    sqlx::query("UPDATE chain_lineage SET canonicality_state='orphaned' WHERE chain_id=$1 AND block_number=2")
        .bind(CHAIN).execute(&fixture.pool).await?;
    assert_eq!(entry(&fixture).await?["status"], "unsupported");
    assert_eq!(families::undo_to(&fixture.pool, CHAIN, 1).await?, 1);
    assert_eq!(
        pending_text(&fixture).await?,
        1,
        "undo re-derives pending work from the restored baseline"
    );
    assert!(value_row(&fixture).await?["hydrated_value"].is_null());
    // The same branch becomes canonical again. A new run resumes only from committed work.
    sqlx::query("UPDATE chain_lineage SET canonicality_state='canonical' WHERE chain_id=$1 AND block_number=2")
        .bind(CHAIN).execute(&fixture.pool).await?;
    run(&fixture, 3, FamilyMode::Normal, &rpc).await?;
    assert_eq!(
        pending_text(&fixture).await?,
        1,
        "repair replays without RPC"
    );
    rpc.answer(4, Some("refreshed"));
    run(&fixture, 4, FamilyMode::Normal, &rpc).await?;
    assert_eq!(entry(&fixture).await?["value"], "refreshed");
    assert_eq!(pending_text(&fixture).await?, 0);
    fixture.cleanup().await
}

#[tokio::test]
async fn hydration_work_upgrade_reset_is_atomic_idempotent_and_rebuilds_pending_work() -> Result<()>
{
    const MIGRATION: &str =
        include_str!("../../../migrations/20260930220000_project_hydration_work.sql");
    let reset_tables: std::collections::BTreeSet<_> = MIGRATION
        .split_once("ARRAY ARRAY[")
        .unwrap()
        .1
        .split_once(']')
        .unwrap()
        .0
        .split('\'')
        .skip(1)
        .step_by(2)
        .collect();
    // The ENSv2 registry entry tables came later, with
    // 20261005140000_project_ens_v2_registry_entries.sql.
    let expected: std::collections::BTreeSet<_> = families::family_tables()
        .filter(|table| {
            !matches!(
                *table,
                "project_ens_v2_entry_owner" | "project_ens_v2_registry_parent"
            )
        })
        .chain([
            "project_family_marker",
            "project_family_undo",
            "project_repair_record",
        ])
        .chain(support::RETIRED_FAMILY_TABLES)
        .collect();
    assert_eq!(
        reset_tables, expected,
        "the first installation resets every owned table"
    );
    let (fixture, rpc) = fixture().await?;
    text(&fixture, 1, None).await?;
    rpc.answer(1, Some("complete"));
    run(&fixture, 1, FamilyMode::Normal, &rpc).await?;
    let before = fixture.exact().await?;
    for _ in 0..2 {
        sqlx::raw_sql(MIGRATION).execute(&fixture.pool).await?;
    }
    assert_eq!(
        fixture.exact().await?,
        before,
        "fresh baseline and reruns preserve publication"
    );
    sqlx::raw_sql(
        "DROP TABLE project_text_hydration_work; DROP TABLE project_reverse_hydration_work",
    )
    .execute(&fixture.pool)
    .await?;
    let mut tx = fixture.pool.begin().await?;
    sqlx::raw_sql(MIGRATION).execute(&mut *tx).await?;
    let markers: i64 = sqlx::query_scalar("SELECT count(*) FROM project_family_marker")
        .fetch_one(&mut *tx)
        .await?;
    assert_eq!(markers, 0);
    tx.rollback().await?;
    assert_eq!(
        value_row(&fixture).await?["hydrated_value"]["value"],
        "complete"
    );
    let table: Option<String> =
        sqlx::query_scalar("SELECT to_regclass('project_text_hydration_work')::text")
            .fetch_one(&fixture.pool)
            .await?;
    assert!(
        table.is_none(),
        "interrupted upgrade restores both schema and publication"
    );
    sqlx::raw_sql(MIGRATION).execute(&fixture.pool).await?;
    let markers: i64 = sqlx::query_scalar("SELECT count(*) FROM project_family_marker")
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(markers, 0);
    run(&fixture, 1, FamilyMode::Normal, &rpc).await?;
    assert_eq!(
        pending_text(&fixture).await?,
        1,
        "bootstrap repopulates work without RPC"
    );
    assert_eq!(rpc.calls().len(), 1);
    rpc.answer(2, Some("rebuilt"));
    run(&fixture, 2, FamilyMode::Normal, &rpc).await?;
    assert_eq!(entry(&fixture).await?["value"], "rebuilt");
    assert_eq!(pending_text(&fixture).await?, 0);
    let rebuilt = fixture.exact().await?;
    sqlx::raw_sql(MIGRATION).execute(&fixture.pool).await?;
    assert_eq!(fixture.exact().await?, rebuilt);
    fixture.cleanup().await
}

#[tokio::test]
async fn range_and_per_block_rebuilds_derive_identical_work_after_dependency_changes() -> Result<()>
{
    use bigname_project::families::RebuildRanges;
    let (fixture, rpc) = fixture().await?;
    keyed_text(&fixture, 1, 2, "old-a").await?;
    keyed_text(&fixture, 2, 1, "old-b").await?;
    fixture
        .event(
            Event::new(
                "range-version:3",
                3,
                1,
                "RecordVersionChanged",
                "ens_v1_resolver_l1",
            )
            .on(CHAIN)
            .after(json!({"node":NODE,"resolver":RESOLVER})),
        )
        .await?;
    keyed_text(&fixture, 4, 1, "current").await?;
    manifest(&fixture, 5, false).await?;
    manifest(&fixture, 6, true).await?;
    let mut expected = None;
    rpc::head(&fixture.pool, 6).await?;
    for ranges in [RebuildRanges::Through(6), RebuildRanges::Off] {
        let token = families::input_token(&fixture.pool, CHAIN).await?;
        let outcome = families::apply(
            &fixture.pool,
            CHAIN,
            &marker(6),
            FamilyMode::Rebuild,
            &token,
            &FamilyOptions::new(CONTENT_HASH)
                .with_hydration(rpc.urls())
                .with_rebuild_ranges(ranges),
        )
        .await?;
        if matches!(ranges, RebuildRanges::Through(_)) {
            assert!(outcome.ranges > 0);
        }
        assert!(rpc.calls().is_empty());
        let work = selected(&fixture, 7).await?;
        assert_eq!(work, vec!["current"]);
        let state = fixture.exact().await?;
        if let Some(expected) = &expected {
            assert_eq!(&state, expected);
        } else {
            expected = Some(state);
        }
    }
    rpc.answer(7, Some("rebuilt"));
    run(&fixture, 7, FamilyMode::Normal, &rpc).await?;
    assert_eq!(pending_text(&fixture).await?, 0);
    assert_eq!(rpc.calls(), vec![(hash(7), 1)]);
    fixture.cleanup().await
}

#[tokio::test]
async fn completed_delta_keys_do_not_consume_the_rolling_text_share() -> Result<()> {
    let (fixture, rpc) = fixture().await?;
    for index in 0..251 {
        keyed_text(&fixture, 1, index + 2, &format!("key{index:03}")).await?;
    }
    run(&fixture, 1, FamilyMode::Rebuild, &rpc).await?;
    // The first 250 queued keys now have event-carried values. They need no RPC and must not
    // hide the unchanged pending key behind an early work-index cut.
    for index in 0..250 {
        let key = format!("key{index:03}");
        fixture
            .event(
                Event::new(
                    &format!("value:2:{index}"),
                    2,
                    index + 1,
                    "RecordChanged",
                    "ens_v1_resolver_l1",
                )
                .on(CHAIN)
                .after(json!({
                    "node":NODE,"resolver":RESOLVER,"record_key":format!("text:{key}"),
                    "record_family":"text","selector_key":key,"source_event":"TextChanged",
                    "value":"from event"
                })),
            )
            .await?;
    }
    rpc.answer(2, Some("last"));
    run(&fixture, 2, FamilyMode::Normal, &rpc).await?;
    assert_eq!(rpc.calls(), vec![(hash(2), 1)]);
    assert_eq!(pending_text(&fixture).await?, 0);
    fixture.cleanup().await
}

#[tokio::test]
async fn text_split_stamps_the_selector_it_cannot_read_and_an_unserved_block_writes_nothing()
-> Result<()> {
    let (fixture, rpc) = fixture().await?;
    keyed_text(&fixture, 1, 2, "badkey").await?;
    keyed_text(&fixture, 1, 3, "goodkey").await?;
    rpc.answer(1, Some("value"));
    // Any aggregate asking for `badkey` fails whole; the endpoint serves the block otherwise.
    rpc.poison(&alloy_primitives::hex::encode("badkey"));
    let outcome = run(&fixture, 1, FamilyMode::Normal, &rpc).await?;
    let text_outcome = outcome.hydration.text;
    assert_eq!(
        (
            text_outcome.rpc_calls,
            text_outcome.rpc_failures,
            text_outcome.answered,
            text_outcome.deferred,
            text_outcome.value_writes,
            text_outcome.schedule_writes,
        ),
        (3, 2, 1, 1, 1, 1)
    );
    assert_eq!(outcome.hydration.probes, 1);
    // The readable selector is hydrated; the other has nothing observed and is stamped with the
    // attempt, so it waits behind the never-read backlog.
    assert_eq!(
        attempts(&fixture).await?,
        vec![
            ("badkey".to_owned(), Some(1), None),
            ("goodkey".to_owned(), Some(1), Some("success".to_owned())),
        ]
    );
    assert_eq!(pending_text(&fixture).await?, 1);

    // The endpoint fails every aggregate at block 2: no text row or work entry changes.
    let before = fixture.rows("project_node_record_value").await?;
    let outcome = run(&fixture, 2, FamilyMode::Normal, &rpc).await?;
    assert_eq!(fixture.rows("project_node_record_value").await?, before);
    assert_eq!(
        (
            outcome.hydration.text.not_observed,
            outcome.hydration.unserved_passes
        ),
        (1, 1)
    );
    assert_eq!(pending_text(&fixture).await?, 1);

    // Once it can be read, it is: the selector stayed work throughout.
    rpc.clear_faults();
    rpc.answer(3, Some("late"));
    run(&fixture, 3, FamilyMode::Normal, &rpc).await?;
    assert_eq!(rpc.calls().last(), Some(&(hash(3), 1)));
    assert!(
        attempts(&fixture)
            .await?
            .iter()
            .all(|(_, _, status)| status.as_deref() == Some("success"))
    );
    assert_eq!(pending_text(&fixture).await?, 0);
    fixture.cleanup().await
}

/// Keys whose overlay holds a successful read.
async fn read_keys(fixture: &Fixture) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM project_node_record_value
         WHERE hydrated_value ->> 'status' = 'success'",
    )
    .fetch_one(&fixture.pool)
    .await?)
}

#[tokio::test]
async fn text_selectors_beside_unreadable_ones_are_all_read_over_the_following_heads() -> Result<()>
{
    const UNREADABLE: [i64; 9] = [1, 2, 3, 4, 8, 16, 32, 63, 126];
    let (fixture, rpc) = fixture().await?;
    for position in 1..=250 {
        keyed_text(&fixture, 1, position + 1, &format!("key{position:03}")).await?;
    }
    for block in 1..=8 {
        rpc.answer(block, Some("value"));
    }
    // Any aggregate asking for one of nine keys fails whole, at every block. They sit where
    // almost every aggregate the first head has calls for holds one of them.
    for position in UNREADABLE {
        rpc.poison(&alloy_primitives::hex::encode(format!("key{position:03}")));
    }
    let outcome = run(&fixture, 1, FamilyMode::Normal, &rpc).await?;
    let first = outcome.hydration.text;
    assert_eq!(
        (first.answered, first.deferred, first.rpc_calls),
        (3, 247, 17)
    );
    assert_eq!(read_keys(&fixture).await?, 3);

    // Each later head starts from the sizes the one before left, so the readable keys are read.
    let mut heads = 1;
    while read_keys(&fixture).await? < 241 {
        heads += 1;
        assert!(heads <= 8, "readable keys were never read");
        let outcome = run(&fixture, heads, FamilyMode::Normal, &rpc).await?;
        assert!(outcome.hydration.text.rpc_calls <= 17);
    }
    assert!(heads <= 6, "{heads} heads");
    // The nine stay pending, each alone, and are tried again without holding anything back.
    assert_eq!(pending_text(&fixture).await?, 9);
    let limits: Vec<Option<i32>> = sqlx::query_scalar(
        "SELECT hydration_limit FROM project_node_record_value
         WHERE hydrated_value IS NULL ORDER BY record_key",
    )
    .fetch_all(&fixture.pool)
    .await?;
    assert_eq!(limits, vec![Some(1); 9]);
    let outcome = run(&fixture, heads + 1, FamilyMode::Normal, &rpc).await?;
    let text_outcome = outcome.hydration.text;
    assert_eq!((text_outcome.rpc_calls, text_outcome.deferred), (9, 9));
    assert_eq!(
        (text_outcome.value_writes, text_outcome.schedule_writes),
        (0, 9)
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn slow_reverse_reads_leave_time_for_the_text_selectors_of_the_same_block() -> Result<()> {
    use std::time::Duration;
    let (fixture, rpc) = fixture().await?;
    reverse::seed(&fixture, 1, 1).await?;
    keyed_text(&fixture, 1, 9, "waiting").await?;
    rpc.answer(1, Some("value"));
    // The reverse tuple's aggregate outlasts the whole block's time. Reverse names are read
    // first, but only within half of that time while a text selector is waiting.
    rpc.slow(&reverse::node(1), Duration::from_secs(20));
    rpc::head(&fixture.pool, 1).await?;
    let limits = bigname_project::families::HydrationTimeLimits {
        call: Duration::from_secs(4),
        block: Duration::from_secs(4),
    };
    let options = reverse::options(&rpc).with_hydration_time_limits(limits);
    let (outcome, error) = reverse::apply(&fixture, &marker(1), FamilyMode::Normal, &options).await;
    assert!(error.is_none(), "{error:?}");
    let hydration = &outcome.hydration;
    assert_eq!(
        (hydration.reverse.rpc_calls, hydration.reverse.not_observed),
        (1, 1)
    );
    assert_eq!(
        (
            hydration.reverse.deferred,
            hydration.reverse.schedule_writes
        ),
        (0, 0)
    );
    assert_eq!(
        (hydration.text.answered, hydration.text.value_writes),
        (1, 1)
    );
    assert_eq!(
        (hydration.timed_out_passes, hydration.unserved_passes),
        (1, 0)
    );
    assert_eq!(
        attempts(&fixture).await?,
        vec![("waiting".to_owned(), Some(1), Some("success".to_owned()))]
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn fast_rejections_followed_by_a_slow_half_still_record_where_to_resume() -> Result<()> {
    use std::time::Duration;
    let (fixture, rpc) = fixture().await?;
    for index in 1..=16 {
        reverse::seed(&fixture, 1, index).await?;
        keyed_text(&fixture, 1, 40 + index, &format!("key{index:02}")).await?;
    }
    for block in 1..=8 {
        rpc.answer(block, Some("value"));
    }
    // The endpoint rejects an aggregate of four or more calls after 500 ms and takes 1.9 s, most
    // of a call's time, to answer a smaller one; the probe answers at once. Reverse has 3.5 of
    // the block's seven seconds: after the aggregates of 16, 8 and 4 are rejected it has less
    // than one call's time left, so a half sent then would be cut and nothing recorded.
    rpc.reject_from(4, Duration::from_millis(500), Duration::from_millis(1900));
    let limits = bigname_project::families::HydrationTimeLimits {
        call: Duration::from_secs(2),
        block: Duration::from_secs(7),
    };
    let options = reverse::options(&rpc).with_hydration_time_limits(limits);
    let observed = |table: &'static str, column: &'static str| {
        let pool = fixture.pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>(&format!(
                "SELECT count(*) FROM {table} WHERE {column} IS NOT NULL"
            ))
            .fetch_one(&pool)
            .await
        }
    };

    // The first head observes no reverse name, but every rejected aggregate leaves its tuples
    // a smaller size and no call is cut. How far the splitting got depends on the machine: the
    // third rejection, at the latest, leaves less than a call's time.
    rpc::head(&fixture.pool, 1).await?;
    let (outcome, error) = reverse::apply(&fixture, &marker(1), FamilyMode::Normal, &options).await;
    assert!(error.is_none(), "{error:?}");
    let first = outcome.hydration.reverse;
    assert_eq!(
        (first.answered, first.deferred, first.not_observed),
        (0, 16, 0),
        "{first:?}"
    );
    assert_eq!(first.schedule_writes, 16);
    assert_eq!(first.rpc_calls, first.rpc_failures);
    let limits: Vec<Option<i32>> =
        sqlx::query_scalar("SELECT attempt_limit FROM project_reverse_tuple ORDER BY address")
            .fetch_all(&fixture.pool)
            .await?;
    assert!(
        limits
            .iter()
            .all(|limit| limit.is_some_and(|limit| (1..=8).contains(&limit))),
        "{limits:?}"
    );
    let text_first = outcome.hydration.text;
    assert!(
        text_first.answered + text_first.deferred > 0,
        "text is read or records where to resume: {text_first:?}"
    );

    // The second head, against the same endpoint, starts from those sizes: it reads tuples or
    // records smaller sizes again, for both kinds.
    rpc::head(&fixture.pool, 2).await?;
    let (outcome, error) = reverse::apply(&fixture, &marker(2), FamilyMode::Normal, &options).await;
    assert!(error.is_none(), "{error:?}");
    let second = outcome.hydration.reverse;
    assert!(second.answered + second.deferred > 0, "{second:?}");
    let smaller: Vec<Option<i32>> =
        sqlx::query_scalar("SELECT attempt_limit FROM project_reverse_tuple ORDER BY address")
            .fetch_all(&fixture.pool)
            .await?;
    assert!(
        smaller
            .iter()
            .zip(&limits)
            .all(|(now, before)| now.is_none() || now <= before),
        "{smaller:?}"
    );
    assert_ne!(smaller, limits, "the second head made progress");
    let text_second = outcome.hydration.text;
    assert!(
        text_second.answered + text_second.deferred > 0,
        "text is read or records where to resume: {text_second:?}"
    );

    // With small aggregates answered quickly and large ones still rejected, the following heads
    // read everything left, for both kinds.
    rpc.clear_faults();
    rpc.reject_from(4, Duration::from_millis(100), Duration::from_millis(10));
    let mut heads = 2;
    while observed("project_reverse_tuple", "hydrated_name").await? < 16
        || observed("project_node_record_value", "hydrated_value").await? < 16
    {
        heads += 1;
        assert!(heads <= 8, "selectors were never observed");
        rpc::head(&fixture.pool, heads).await?;
        let (_, error) =
            reverse::apply(&fixture, &marker(heads), FamilyMode::Normal, &options).await;
        assert!(error.is_none(), "{error:?}");
    }
    fixture.cleanup().await
}

#[tokio::test]
async fn undo_restores_a_deferred_text_selector_and_its_work_entry() -> Result<()> {
    let (fixture, rpc) = fixture().await?;
    keyed_text(&fixture, 1, 2, "badkey").await?;
    keyed_text(&fixture, 1, 3, "goodkey").await?;
    // The endpoint does not serve block 1: both selectors stay unread and untouched.
    run(&fixture, 1, FamilyMode::Normal, &rpc).await?;
    let rows = fixture.rows("project_node_record_value").await?;
    let work = fixture.rows("project_text_hydration_work").await?;

    // At block 2 the aggregate holding `badkey` fails: it is deferred with scheduling state.
    rpc.answer(2, Some("value"));
    rpc.poison(&alloy_primitives::hex::encode("badkey"));
    let outcome = run(&fixture, 2, FamilyMode::Normal, &rpc).await?;
    assert_eq!(
        (
            outcome.hydration.text.answered,
            outcome.hydration.text.deferred
        ),
        (1, 1)
    );
    let deferred: (Option<i64>, Option<i32>, Option<i32>) = sqlx::query_as(
        "SELECT hydrated_at_block, hydration_limit, hydration_failures
         FROM project_node_record_value WHERE selector_key = 'badkey'",
    )
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(deferred, (Some(2), Some(1), Some(1)));
    let failures: Vec<Option<i32>> =
        sqlx::query_scalar("SELECT hydration_failures FROM project_text_hydration_work")
            .fetch_all(&fixture.pool)
            .await?;
    assert_eq!(failures, vec![Some(1)], "the work index follows the row");

    // Block 2 is replaced: the undo restores both rows and both work entries exactly.
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND block_number = 2",
    )
    .bind(CHAIN)
    .execute(&fixture.pool)
    .await?;
    let replacement = reverse::fork_block(&fixture, 2, &hash(1)).await?;
    let calls = (rpc.calls(), rpc.probes());
    let options = reverse::options(&rpc).with_max_blocks_per_run(1);
    let (outcome, error) =
        reverse::apply(&fixture, &replacement, FamilyMode::Normal, &options).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!((outcome.undone_blocks, outcome.blocks), (1, 0));
    assert_eq!(fixture.rows("project_node_record_value").await?, rows);
    assert_eq!(fixture.rows("project_text_hydration_work").await?, work);
    // The replay makes no call and leaves them so.
    let (outcome, error) =
        reverse::apply(&fixture, &replacement, FamilyMode::Normal, &options).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(outcome.marker, Some(replacement));
    assert_eq!((rpc.calls(), rpc.probes()), calls);
    assert_eq!(fixture.rows("project_node_record_value").await?, rows);
    assert_eq!(fixture.rows("project_text_hydration_work").await?, work);
    fixture.cleanup().await
}
