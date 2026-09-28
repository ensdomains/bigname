#[path = "families_hydration/rpc.rs"]
mod rpc;
#[path = "families_support/mod.rs"]
mod support;

use anyhow::Result;
use bigname_project::families::{self, FamilyMode, FamilyOptions};
use serde_json::{Value, json};
use support::{CONTENT_HASH, Event, Fixture, hash, marker};

const CHAIN: &str = "ethereum-mainnet";
const SILENT: &str = "0xa2c122be93b0074270ebee7f6b7292c7deb45047";
const ADDRESS: &str = "0x0000000000000000000000000000000000000001";
const NODE: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";

async fn run(fixture: &Fixture, block: i64, mode: FamilyMode, rpc: &rpc::Rpc) -> Result<()> {
    let token = families::input_token(&fixture.pool, CHAIN).await?;
    families::apply(
        &fixture.pool,
        CHAIN,
        &marker(block),
        mode,
        &token,
        &FamilyOptions::new(CONTENT_HASH).with_hydration(rpc.urls()),
    )
    .await?;
    Ok(())
}

async fn seed(fixture: &Fixture, block: i64, index: i64) -> Result<()> {
    let address = format!("0x{index:040x}");
    let node = if index == 1 {
        NODE.to_owned()
    } else {
        format!("0x{index:064x}")
    };
    fixture
        .event(
            Event::new(
                &format!("claim:{block}:{index}"),
                block,
                index * 2,
                "ReverseChanged",
                "ens_v1_reverse_registrar_l1",
            )
            .on(CHAIN)
            .after(json!({
                "source_event":"ReverseClaimed", "address":address, "coin_type":"60",
                "namespace":"ens", "reverse_node":node
            })),
        )
        .await?;
    fixture
        .event(
            Event::new(
                &format!("pointer:{block}:{index}"),
                block,
                index * 2 + 1,
                "ResolverChanged",
                "ens_v1_registry_l1",
            )
            .on(CHAIN)
            .after(json!({
                "source_event":"NewResolver", "node":node, "resolver":SILENT
            })),
        )
        .await?;
    Ok(())
}

async fn tuple(fixture: &Fixture) -> Result<Value> {
    Ok(
        sqlx::query_scalar("SELECT to_jsonb(t) FROM project_reverse_tuple t WHERE address=$1")
            .bind(ADDRESS)
            .fetch_one(&fixture.pool)
            .await?,
    )
}

async fn claim(fixture: &Fixture) -> Result<bigname_storage::PrimaryNameCurrentSnapshot> {
    Ok(
        bigname_storage::families::records::load_family_primary_name_snapshot(
            &fixture.pool,
            ADDRESS,
            "ens",
            "60",
        )
        .await?
        .expect("reverse tuple"),
    )
}

#[tokio::test]
async fn follow_hydrates_same_block_and_empty_blocks_and_undo_restores_overlay() -> Result<()> {
    let fixture = Fixture::new("family_reverse_hydration", 6).await?;
    fixture.lineage(CHAIN, 6).await?;
    let rpc = rpc::Rpc::new().await?;
    run(&fixture, 0, FamilyMode::Normal, &rpc).await?;
    seed(&fixture, 1, 1).await?;
    rpc.answer(1, Some("alice.eth"));
    run(&fixture, 1, FamilyMode::Normal, &rpc).await?;
    let first = tuple(&fixture).await?;
    assert_eq!(
        first["hydrated_name"], "alice.eth",
        "N's new tuple hydrates at N"
    );
    assert_eq!(first["baseline"]["reverse_node"], NODE);
    assert_eq!(first["baseline"]["resolver_address"], SILENT);
    assert_eq!(rpc.calls(), vec![(hash(1), 1)]);
    assert_eq!(
        claim(&fixture).await?.row.raw_claim_name.as_deref(),
        Some("alice.eth")
    );

    rpc.answer(2, Some(""));
    run(&fixture, 2, FamilyMode::Normal, &rpc).await?;
    assert_eq!(
        tuple(&fixture).await?["hydrated_name"],
        "",
        "not found is an overlay"
    );
    assert_eq!(
        claim(&fixture).await?.row.claim_status.as_str(),
        "not_found"
    );
    rpc.answer(3, None);
    run(&fixture, 3, FamilyMode::Normal, &rpc).await?;
    let failed = tuple(&fixture).await?;
    assert!(
        failed["hydrated_name"].is_null(),
        "failure retracts the previous answer"
    );
    assert_eq!(failed["attempt_block"], 3);
    assert_eq!(
        claim(&fixture).await?.row.claim_status.as_str(),
        "not_found"
    );
    let journal: Value = sqlx::query_scalar("SELECT before_image FROM project_family_undo WHERE chain_id=$1 AND block_number=3 AND family='project_reverse_tuple'")
        .bind(CHAIN).fetch_one(&fixture.pool).await?;
    assert_eq!(
        journal["hydrated_name"], "",
        "empty-block work is journalled"
    );

    let calls = rpc.calls().len();
    run(&fixture, 3, FamilyMode::Redo { from: 3, to: 3 }, &rpc).await?;
    assert_eq!(rpc.calls().len(), calls, "replay performs no RPC");
    assert_eq!(
        tuple(&fixture).await?["hydrated_name"],
        "",
        "undo restored N-1 overlay"
    );
    rpc.answer(4, Some("invalid..eth"));
    run(&fixture, 4, FamilyMode::Normal, &rpc).await?;
    assert_eq!(
        tuple(&fixture).await?["hydrated_name"],
        "invalid..eth",
        "raw invalid result retained for reader classification"
    );
    assert_eq!(
        claim(&fixture).await?.row.claim_status.as_str(),
        "invalid_name"
    );
    rpc.answer(5, Some("new.eth"));
    run(&fixture, 5, FamilyMode::Normal, &rpc).await?;
    assert_eq!(tuple(&fixture).await?["hydrated_name"], "new.eth");

    fixture
        .event(
            Event::new("pointer:6:1", 6, 1, "ResolverChanged", "ens_v1_registry_l1")
                .on(CHAIN)
                .after(
                    json!({"node":NODE,"resolver":"0x0000000000000000000000000000000000000000"}),
                ),
        )
        .await?;
    run(&fixture, 6, FamilyMode::Normal, &rpc).await?;
    let stale = tuple(&fixture).await?;
    assert!(stale["hydrated_name"].is_null());
    assert!(stale["attempt_block"].is_null());
    assert_eq!(
        rpc.calls().len(),
        calls + 2,
        "ineligible selector makes no call"
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn rebuild_skips_rpc_and_failed_rolling_page_does_not_starve_next_tuple() -> Result<()> {
    let fixture = Fixture::new("family_reverse_rolling", 4).await?;
    fixture.lineage(CHAIN, 4).await?;
    let rpc = rpc::Rpc::new().await?;
    for index in 1..=251 {
        seed(&fixture, 1, index).await?;
    }
    run(&fixture, 1, FamilyMode::Rebuild, &rpc).await?;
    assert!(rpc.calls().is_empty());
    rpc.answer(2, None);
    run(&fixture, 2, FamilyMode::Normal, &rpc).await?;
    rpc.answer(3, Some("next.eth"));
    run(&fixture, 3, FamilyMode::Normal, &rpc).await?;
    assert_eq!(rpc.calls(), vec![(hash(2), 250), (hash(3), 1)]);
    let attempted: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_reverse_tuple WHERE attempt_block IS NOT NULL",
    )
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(attempted, 251);
    let last: Option<String> =
        sqlx::query_scalar("SELECT hydrated_name FROM project_reverse_tuple WHERE address=$1")
            .bind(format!("0x{:040x}", 251))
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(last.as_deref(), Some("next.eth"));
    fixture.cleanup().await
}
