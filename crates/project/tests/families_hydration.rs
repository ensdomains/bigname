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
// ReverseClaimed is admitted only for namehash(<lowercase address>.addr.reverse), as
// crates/adapters/src/schema_v2/protocol/v1/reverse.rs:88-91 enforces.
fn node(index: i64) -> String {
    let labels = [
        format!("{index:040x}"),
        "addr".to_owned(),
        "reverse".to_owned(),
    ];
    let hash = labels
        .iter()
        .rev()
        .fold(alloy_primitives::B256::ZERO, |parent, label| {
            let label = alloy_primitives::keccak256(label.as_bytes());
            alloy_primitives::keccak256([parent.as_slice(), label.as_slice()].concat())
        });
    format!("{hash:#x}")
}

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
    let node = node(index);
    fixture
        .event(
            Event::new(
                &format!("claim:{block}:{index}"),
                block,
                index * 2,
                "ReverseChanged",
                "ens_v1_reverse_l1",
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
    assert_eq!(first["baseline"]["reverse_node"], node(1));
    assert_eq!(first["baseline"]["resolver_address"], SILENT);
    assert_eq!(rpc.calls(), vec![(hash(1), 1)]);
    let provenance = claim(&fixture).await?.row.claim_provenance;
    assert_eq!(
        provenance["canonical_head_multicall_hydration"],
        json!({
            "source":"multicall_at_canonical_head", "chain_id":CHAIN, "block_number":1,
            "block_hash":hash(1), "resolver_address":SILENT, "reverse_node":node(1),
            "baseline":{"claim_status":"not_found","raw_claim_name":null,
                "claim_name_is_normalized":false,"unsupported_reason":null}
        })
    );
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
                    json!({"node":node(1),"resolver":"0x0000000000000000000000000000000000000000"}),
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

#[tokio::test]
async fn replayed_resolver_change_cannot_reuse_the_old_hydrated_claim() -> Result<()> {
    let fixture = Fixture::new("family_reverse_replay_selector", 2).await?;
    fixture.lineage(CHAIN, 2).await?;
    let rpc = rpc::Rpc::new().await?;
    run(&fixture, 0, FamilyMode::Normal, &rpc).await?;
    seed(&fixture, 1, 1).await?;
    rpc.answer(1, Some("alice.eth"));
    rpc.answer(2, Some("alice.eth"));
    run(&fixture, 2, FamilyMode::Normal, &rpc).await?;
    assert_eq!(
        claim(&fixture).await?.row.raw_claim_name.as_deref(),
        Some("alice.eth")
    );
    // Interpret replays a resolver event; Project undoes 2 and folds the corrected event.
    fixture
        .event(
            Event::new(
                "replayed:pointer:2",
                2,
                1,
                "ResolverChanged",
                "ens_v1_registry_l1",
            )
            .on(CHAIN)
            .after(json!({"node":node(1),"resolver":"0x0000000000000000000000000000000000000022"})),
        )
        .await?;
    run(&fixture, 2, FamilyMode::Redo { from: 2, to: 2 }, &rpc).await?;
    assert_eq!(rpc.calls().len(), 2, "replay performs no RPC");
    assert_eq!(
        tuple(&fixture).await?["hydrated_name"],
        "alice.eth",
        "undo restores the complete row image"
    );
    let current = claim(&fixture).await?;
    assert_eq!(current.row.claim_status.as_str(), "not_found");
    assert!(
        current
            .row
            .claim_provenance
            .get("canonical_head_multicall_hydration")
            .is_none()
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn preparation_releases_transaction_and_changed_inputs_refuse_publication() -> Result<()> {
    for (label, statement) in [
        ("revision", "INSERT INTO chain_phase_state (chain_id, phase_name, input_content_hash)
            VALUES ('ethereum-mainnet','interpret','rotated') ON CONFLICT (chain_id,phase_name)
            DO UPDATE SET input_content_hash='rotated'"),
        ("block", "UPDATE chain_lineage SET canonicality_state='orphaned'
            WHERE chain_id='ethereum-mainnet' AND block_number=1;
            INSERT INTO chain_lineage (chain_id,block_hash,parent_hash,block_number,block_timestamp,canonicality_state)
            SELECT chain_id,'0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff',
                parent_hash,block_number,block_timestamp,'canonical' FROM chain_lineage
                WHERE chain_id='ethereum-mainnet' AND block_number=1")
    ] {
        let fixture = Fixture::new(&format!("family_reverse_fence_{label}"), 1).await?;
        fixture.lineage(CHAIN, 1).await?;
        let rpc = rpc::Rpc::new().await?;
        run(&fixture, 0, FamilyMode::Normal, &rpc).await?;
        seed(&fixture, 1, 1).await?;
        rpc.answer(1, Some("must-not-publish.eth"));
        rpc.before_reply(&fixture.pool, statement);
        let error = run(&fixture, 1, FamilyMode::Normal, &rpc).await.unwrap_err();
        assert!(error.to_string().contains(if label == "revision" { "revision changed" } else { "prepared block changed" }), "{error:#}");
        assert_eq!(rpc.calls().len(), 1);
        let position: i64 = sqlx::query_scalar("SELECT current_block_number FROM project_family_marker WHERE chain_id=$1")
            .bind(CHAIN).fetch_one(&fixture.pool).await?;
        assert_eq!(position, 0);
        let tuples: i64 = sqlx::query_scalar("SELECT count(*) FROM project_reverse_tuple")
            .fetch_one(&fixture.pool).await?;
        assert_eq!(tuples, 0, "preparation and refused publication leave no rows");
        fixture.cleanup().await?;
    }
    Ok(())
}

#[path = "families_hydration/plans.rs"]
mod plans;

fn reverse_selection_sql() -> String {
    include_str!("../src/families/hydrate/reverse.sql").replace(
        "{pointer_emission_ordinal}",
        &bigname_storage::families::position::emission_ordinal_sql(
            "p.event_identity",
            "p.transaction_index",
            "p.log_index",
        ),
    )
}

#[tokio::test]
async fn reverse_selection_reads_a_bounded_rotation_among_fifty_thousand_refresh_tuples()
-> Result<()> {
    let fixture = Fixture::new("family_reverse_work_plan", 4).await?;
    fixture.lineage(CHAIN, 4).await?;
    let rpc = rpc::Rpc::new().await?;
    seed(&fixture, 1, 1).await?;
    run(&fixture, 1, FamilyMode::Rebuild, &rpc).await?;
    // Duplicate the persisted rows as a large already-built fixture. All retain the admitted
    // pointer, so this measures the active rotation, rather than only excluding inactive rows.
    sqlx::query(
        "INSERT INTO project_reverse_tuple SELECT (jsonb_populate_record(t,
        jsonb_build_object('address', '0x' || lpad(to_hex(i), 40, '0')))).*
        FROM project_reverse_tuple t, generate_series(2, 50001) i WHERE t.address=$1",
    )
    .bind(ADDRESS)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(
        "INSERT INTO project_reverse_hydration_work SELECT (jsonb_populate_record(w,
        jsonb_build_object('address', '0x' || lpad(to_hex(i), 40, '0')))).*
        FROM project_reverse_hydration_work w, generate_series(2, 50001) i WHERE w.address=$1",
    )
    .bind(ADDRESS)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(
        "INSERT INTO project_resource_pointer
        (chain_id, resource_id, block_number, event_identity, namespace, namehash, pointer_position)
        SELECT $1, md5(i::text)::uuid, 1, 'pointer:' || i, 'ens', 'unrelated:' || i,
            jsonb_build_object('block_number', 1, 'event_identity', 'pointer:' || i)
        FROM generate_series(1, 50000) i",
    )
    .bind(CHAIN)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(
        "INSERT INTO project_reverse_node_claim
        (namespace, reverse_node, chain_id, block_number, event_identity, resolver_address)
        SELECT 'ens', 'unrelated:' || i, $1, 1, 'unrelated:' || i, $2
        FROM generate_series(1, 50000) i",
    )
    .bind(CHAIN)
    .bind(SILENT)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(
        "INSERT INTO project_claim_normalization
        (chain_id, claim_event_identity, block_number, event_identity, status)
        SELECT $1, 'unrelated:' || i, 1, 'unrelated:' || i, 'not_found'
        FROM generate_series(1, 50000) i",
    )
    .bind(CHAIN)
    .execute(&fixture.pool)
    .await?;
    sqlx::raw_sql("ANALYZE project_reverse_node_claim; ANALYZE project_claim_normalization; ANALYZE project_resource_pointer; ANALYZE project_reverse_tuple; ANALYZE project_reverse_hydration_work")
        .execute(&fixture.pool)
        .await?;
    let plan: Value = sqlx::query_scalar(&format!(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {}",
        reverse_selection_sql()
    ))
    .bind(CHAIN)
    .bind(2_i64)
    .bind(hash(2))
    .bind(json!([]))
    .bind(json!([]))
    .bind(json!([]))
    .bind(json!([]))
    .bind(json!([]))
    .bind(vec![SILENT])
    .bind(json!([]))
    .bind(false)
    .fetch_one(&fixture.pool)
    .await?;
    plans::save("reverse-rotation-among-50000", &plan)?;
    let generic = plans::generic(&fixture.pool, &reverse_selection_sql(), &format!(
        "'ethereum-mainnet', 2, '{}', '[]', '[]', '[]', '[]', '[]', ARRAY['{SILENT}'], '[]', false", hash(2))).await?;
    plans::save("reverse-rotation-generic-among-50000", &generic)?;
    assert!(
        plans::visits(&generic, "project_reverse_tuple") <= 250.0,
        "{generic}"
    );
    assert!(
        plans::visits(&generic, "project_reverse_hydration_work") <= 250.0,
        "{generic}"
    );
    assert_eq!(
        plans::visits(&generic, "project_resource_pointer"),
        0.0,
        "{generic}"
    );
    for relation in ["project_reverse_node_claim", "project_claim_normalization"] {
        assert_eq!(
            plans::visits(&generic, relation),
            0.0,
            "{relation}: {generic}"
        );
        assert_eq!(plans::visits(&plan, relation), 0.0, "{relation}: {plan}");
    }
    let dependencies = plans::generic(
        &fixture.pool,
        include_str!("../src/families/hydrate/reverse_keys.sql"),
        "'ethereum-mainnet', '[]', '[]', '[]', '[]', '[]'",
    )
    .await?;
    plans::save(
        "reverse-empty-dependencies-generic-among-50000",
        &dependencies,
    )?;
    assert_eq!(
        plans::visits(&dependencies, "project_reverse_tuple"),
        0.0,
        "{dependencies}"
    );
    assert_eq!(
        plans::visits(&dependencies, "project_resource_pointer"),
        0.0,
        "{dependencies}"
    );
    assert_eq!(
        plans::visits(&plan, "project_resource_pointer"),
        0.0,
        "{plan}"
    );
    assert!(
        plans::visits(&plan, "project_reverse_tuple") <= 250.0,
        "{plan}"
    );
    assert!(
        plans::visits(&plan, "project_reverse_hydration_work") <= 250.0,
        "{plan}"
    );
    rpc.answer(2, Some("first.eth"));
    run(&fixture, 2, FamilyMode::Normal, &rpc).await?;
    rpc.answer(3, Some("next.eth"));
    run(&fixture, 3, FamilyMode::Normal, &rpc).await?;
    assert_eq!(rpc.calls(), vec![(hash(2), 250), (hash(3), 250)]);
    let attempts: Vec<(i64, i64)> = sqlx::query_as("SELECT attempt_block, count(*) FROM
        project_reverse_tuple WHERE attempt_block IS NOT NULL GROUP BY attempt_block ORDER BY attempt_block")
        .fetch_all(&fixture.pool).await?;
    assert_eq!(
        attempts,
        vec![(2, 250), (3, 250)],
        "the next cohort progresses"
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn another_chains_shared_tuple_write_and_undo_update_mainnet_work() -> Result<()> {
    let fixture = Fixture::new("family_reverse_shared_work", 4).await?;
    fixture.lineage(CHAIN, 4).await?;
    let rpc = rpc::Rpc::new().await?;
    fixture.apply_on(support::CHAIN, 0).await?;
    seed(&fixture, 1, 1).await?;
    run(&fixture, 1, FamilyMode::Rebuild, &rpc).await?;
    let pending = async || -> Result<i64> {
        Ok(
            sqlx::query_scalar("SELECT count(*) FROM project_reverse_hydration_work")
                .fetch_one(&fixture.pool)
                .await?,
        )
    };
    assert_eq!(pending().await?, 1);
    fixture
        .event(
            Event::new("other-claim:1", 1, 1, "ReverseChanged", "ens_v1_reverse_l1").after(
                json!({"source_event":"ReverseClaimed", "address":ADDRESS, "coin_type":"60",
            "namespace":"ens", "reverse_node":node(1)}),
            ),
        )
        .await?;
    fixture.apply_on(support::CHAIN, 1).await?;
    assert_eq!(tuple(&fixture).await?["chain_id"], support::CHAIN);
    assert_eq!(
        pending().await?,
        0,
        "displaced mainnet rows cannot consume rolling slots"
    );
    families::undo_to(&fixture.pool, support::CHAIN, 0).await?;
    assert_eq!(tuple(&fixture).await?["chain_id"], CHAIN);
    assert_eq!(
        pending().await?,
        1,
        "undo restores mainnet work with its shared source row"
    );
    rpc.answer(2, Some("restored.eth"));
    run(&fixture, 2, FamilyMode::Normal, &rpc).await?;
    assert_eq!(tuple(&fixture).await?["hydrated_name"], "restored.eth");
    // Reset on the other chain removes its displaced row; subsequent mainnet rebuild restores
    // both its source rows and work entries from canonical input.
    fixture.apply(1, FamilyMode::Rebuild).await?;
    assert_eq!(pending().await?, 0);
    run(&fixture, 2, FamilyMode::Rebuild, &rpc).await?;
    assert_eq!(pending().await?, 1);
    assert_eq!(
        rpc.calls().len(),
        1,
        "other-chain writes and rebuilds perform no hydration RPC"
    );
    fixture.cleanup().await
}
