//! F6 through the actual Follow preparation, pinned RPC, publication, reader and undo paths.
//! The admitted legacy resolver is the existing text hydration allowlist's first address.
//! (upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L71 @ ens_app_v3@7175858)
#[path = "families_hydration/rpc.rs"]
mod rpc;
#[path = "families_support/mod.rs"]
mod support;

use anyhow::Result;
use bigname_project::families::{self, FamilyMode, FamilyOptions};
use serde_json::{Value, json};
use support::{CONTENT_HASH, Event, Fixture, hash, marker};

const CHAIN: &str = "ethereum-mainnet";
const RESOLVER: &str = "0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41";
const NODE: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
const RESOURCE: &str = "00000000-0000-0000-0000-000000000001";

async fn run(fixture: &Fixture, block: i64, mode: FamilyMode, rpc: &rpc::Rpc) -> Result<()> {
    let token = families::input_token(&fixture.pool, CHAIN).await?;
    let outcome = families::apply(
        &fixture.pool,
        CHAIN,
        &marker(block),
        mode,
        &token,
        &FamilyOptions::new(CONTENT_HASH).with_hydration(rpc.urls()),
    )
    .await?;
    assert_eq!(outcome.marker, Some(marker(block)));
    Ok(())
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
    text(&fixture, 3, None).await?;
    rpc.answer(3, None);
    run(&fixture, 3, FamilyMode::Normal, &rpc).await?;
    assert!(value_row(&fixture).await?["hydrated_value"].is_null());
    assert_eq!(
        entry(&fixture).await?["status"],
        "unsupported",
        "failure restores baseline and still publishes"
    );
    rpc.answer(4, Some(""));
    run(&fixture, 4, FamilyMode::Normal, &rpc).await?;
    assert_eq!(entry(&fixture).await?["status"], "not_found");
    let journal: Value = sqlx::query_scalar(
        "SELECT before_image FROM project_family_undo
        WHERE chain_id=$1 AND block_number=4 AND family='project_node_record_value'",
    )
    .bind(CHAIN)
    .fetch_one(&fixture.pool)
    .await?;
    assert!(
        journal["hydrated_value"].is_null(),
        "empty-block retry is journalled"
    );
    let calls = rpc.calls().len();
    run(&fixture, 4, FamilyMode::Redo { from: 4, to: 4 }, &rpc).await?;
    assert_eq!(rpc.calls().len(), calls, "replay never hydrates");
    assert!(
        value_row(&fixture).await?["hydrated_value"].is_null(),
        "undo restored baseline"
    );
    rpc.answer(5, Some("refreshed"));
    run(&fixture, 5, FamilyMode::Normal, &rpc).await?;
    assert_eq!(entry(&fixture).await?["value"], "refreshed");
    run(&fixture, 5, FamilyMode::Rebuild, &rpc).await?;
    assert_eq!(rpc.calls().len(), calls + 1, "rebuild never hydrates");
    assert!(value_row(&fixture).await?["hydrated_value"].is_null());
    rpc.answer(6, Some("after rebuild"));
    run(&fixture, 6, FamilyMode::Normal, &rpc).await?;
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
    // Block 2's reads fail: the first 250 keys are stamped with the attempt and nothing is served.
    run(&fixture, 2, FamilyMode::Normal, &rpc).await?;
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
    let sql = include_str!("../src/families/hydrate/text.sql")
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
        );
    let rows: Vec<Value> = sqlx::query_scalar(&sql)
        .bind(CHAIN)
        .bind(block)
        .bind(json!([]))
        .bind(json!([]))
        .bind(json!([]))
        .bind(vec![RESOLVER])
        .bind(250_i64)
        .fetch_all(&fixture.pool)
        .await?;
    Ok(rows
        .iter()
        .map(|row| row["selector_key"].as_str().unwrap().to_owned())
        .collect())
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
