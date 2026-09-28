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
