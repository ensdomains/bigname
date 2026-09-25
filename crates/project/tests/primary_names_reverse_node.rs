#[path = "support/family_shadow.rs"]
mod family_shadow;

use anyhow::Result;
use bigname_project::{BatchRequest, Engine, Marker, RunMode};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, raw_sql};

const CHAIN: &str = "fixture-reverse-node";
const ADDRESS: &str = "0x0000000000000000000000000000000000000001";
const RESOLVER: &str = "0x0000000000000000000000000000000000000011";
const OTHER: &str = "0x0000000000000000000000000000000000000022";
const NODE: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";

#[tokio::test]
async fn reverse_node_claims_follow_resolver_storage_in_full_incremental_and_redo() -> Result<()> {
    let (database, pool) = database("storage").await?;
    seed(&pool).await?;
    let expected = [
        None,
        Some(("success", Some("alice.eth"))),
        Some(("success", Some("updated.eth"))),
        Some(("success", Some("other.eth"))),
        Some(("not_found", None)),
        Some(("success", Some("updated.eth"))),
        Some(("not_found", None)),
        Some(("not_found", None)),
        Some(("unsupported", None)),
        Some(("success", Some("owner.eth"))),
        Some(("success", Some("owner.eth"))),
    ];
    let mut unrepresented = Vec::new();
    for (index, expected) in expected.into_iter().enumerate() {
        let block = index as i64 + 1;
        if run(
            &pool,
            block,
            (block > 1).then_some(block - 1),
            RunMode::Normal,
        )
        .await?
            > 0
        {
            unrepresented.push(block);
        }
        let incremental = snapshot(&pool).await?;
        match expected {
            None => assert!(incremental.is_none()),
            Some((status, name)) => {
                let row = incremental.as_ref().expect("retained reverse tuple");
                assert_eq!(row["claim_status"], status, "block {block}");
                assert_eq!(row["raw_claim_name"], json!(name), "block {block}");
                assert_eq!(row["claim_name_is_normalized"], status == "success");
            }
        }
        run(&pool, block, Some(block), RunMode::Redo).await?;
        assert_eq!(snapshot(&pool).await?, incremental, "redo block {block}");
        run(&pool, block, None, RunMode::Normal).await?;
        assert_eq!(snapshot(&pool).await?, incremental, "full block {block}");
    }
    // Step 2 finding: the node claim family keeps one row per node, so while the node points at
    // OTHER (block 4) or its latest name record was written at OTHER (block 11), the older record
    // at the current resolver that today's reader serves is not in the families.
    assert_eq!(
        unrepresented,
        [4, 11],
        "reverse claims the families cannot represent"
    );
    let surfaces: i64 = sqlx::query_scalar("SELECT count(*) FROM name_surfaces")
        .fetch_one(&pool)
        .await?;
    assert_eq!(surfaces, 0, "reverse records need no forward-name identity");
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reverse_node_claim_reorg_and_deleted_reset_restore_canonical_history() -> Result<()> {
    let (database, pool) = database("reorg").await?;
    seed(&pool).await?;
    run(&pool, 7, None, RunMode::Normal).await?;
    assert_eq!(snapshot(&pool).await?.unwrap()["claim_status"], "not_found");
    // Redo must recover the projected clearing event even after Interpret replaces it.
    sqlx::query("DELETE FROM normalized_events WHERE event_identity = '7:2'")
        .execute(&pool)
        .await?;
    run(&pool, 7, Some(7), RunMode::Redo).await?;
    assert_eq!(
        snapshot(&pool).await?.unwrap()["raw_claim_name"],
        "before-reset.eth"
    );
    sqlx::query(
        "UPDATE normalized_events SET canonicality_state = 'orphaned' WHERE event_identity = '7:1'",
    )
    .execute(&pool)
    .await?;
    run(&pool, 7, Some(7), RunMode::Redo).await?;
    let repaired = snapshot(&pool).await?;
    assert_eq!(repaired.as_ref().unwrap()["raw_claim_name"], "updated.eth");
    run(&pool, 7, None, RunMode::Normal).await?;
    assert_eq!(snapshot(&pool).await?, repaired);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn explicit_tuple_claims_keep_their_existing_path() -> Result<()> {
    let (database, pool) = database("explicit").await?;
    seed(&pool).await?;
    event(
        &pool,
        2,
        9,
        "ReverseChanged",
        json!({
            "source_event":"NameForAddrChanged", "address": ADDRESS,
            "coin_type":"60", "namespace":"ens", "reverse_node":NODE
        }),
    )
    .await?;
    event(
        &pool,
        2,
        10,
        "RecordChanged",
        json!({
            "source_event":"NameForAddrChanged", "raw_name":"explicit.eth",
            "record_key":"name", "primary_claim_source":{
                "address":ADDRESS, "coin_type":"60", "namespace":"ens"
            }
        }),
    )
    .await?;
    run(&pool, 3, None, RunMode::Normal).await?;
    assert_eq!(
        snapshot(&pool).await?.unwrap()["raw_claim_name"],
        "explicit.eth"
    );
    database.cleanup().await?;
    Ok(())
}

async fn seed(pool: &PgPool) -> Result<()> {
    for block in 1..=11 {
        sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,$3,to_timestamp($3::double precision),'canonical')")
            .bind(CHAIN).bind(hash(block)).bind(block).execute(pool).await?;
    }
    name(pool, 1, 1, RESOLVER, json!("before-claim.eth")).await?;
    name(pool, 1, 2, OTHER, json!("other.eth")).await?;
    event(
        pool,
        2,
        0,
        "ReverseChanged",
        json!({
            "source_event":"ReverseClaimed", "address":ADDRESS,
            "coin_type":"60", "namespace":"ens", "reverse_node":NODE
        }),
    )
    .await?;
    pointer(pool, 2, RESOLVER).await?;
    // Deliberately insert higher log position first; IDs cannot override chain order.
    name(pool, 2, 3, RESOLVER, json!("alice.eth")).await?;
    name(pool, 2, 2, RESOLVER, json!("earlier.eth")).await?;
    name(pool, 3, 1, RESOLVER, json!("updated.eth")).await?;
    pointer(pool, 4, OTHER).await?;
    pointer(pool, 5, "0x0000000000000000000000000000000000000000").await?;
    pointer(pool, 6, RESOLVER).await?;
    name(pool, 7, 1, RESOLVER, json!("before-reset.eth")).await?;
    event(
        pool,
        7,
        2,
        "RecordVersionChanged",
        json!({
            "source_event":"VersionChanged", "node":NODE, "resolver":RESOLVER,
            "record_version":1
        }),
    )
    .await?;
    name(pool, 8, 1, RESOLVER, json!(" \t")).await?;
    name(
        pool,
        9,
        1,
        RESOLVER,
        json!({"encoding":"hex","bytes":"0xff"}),
    )
    .await?;
    name(pool, 10, 1, RESOLVER, json!("owner.eth")).await?;
    event(
        pool,
        10,
        2,
        "AuthorityTransferred",
        json!({"node":NODE,"owner":OTHER}),
    )
    .await?;
    name(pool, 11, 1, OTHER, json!("wrong-resolver.eth")).await?;
    event(
        pool,
        11,
        2,
        "RecordChanged",
        json!({
            "source_event":"NameChanged", "node":hash(99), "resolver":RESOLVER,
            "record_key":"name", "raw_name":"wrong-node.eth"
        }),
    )
    .await?;
    Ok(())
}

async fn name(pool: &PgPool, block: i64, log: i64, resolver: &str, value: Value) -> Result<()> {
    event(
        pool,
        block,
        log,
        "RecordChanged",
        json!({
            "source_event":"NameChanged", "node":NODE, "resolver":resolver,
            "record_key":"name", "record_family":"name", "raw_name":value
        }),
    )
    .await
}

async fn pointer(pool: &PgPool, block: i64, resolver: &str) -> Result<()> {
    event(
        pool,
        block,
        1,
        "ResolverChanged",
        json!({"node":NODE,"resolver":resolver}),
    )
    .await
}

async fn event(pool: &PgPool, block: i64, log: i64, kind: &str, after: Value) -> Result<()> {
    let family = match kind {
        "ReverseChanged" => "ens_v1_reverse_l1",
        "ResolverChanged" | "AuthorityTransferred" => "ens_v1_registry_l1",
        _ => "ens_v1_resolver_l1",
    };
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state) VALUES ($1,'ens',$2,$3,1,$4,$5,$6,$6,0,$7,'ens_v1_unwrapped_authority','canonical',$8)")
        .bind(format!("{block}:{log}")).bind(kind).bind(family).bind(CHAIN)
        .bind(block).bind(hash(block)).bind(log).bind(after).execute(pool).await?;
    Ok(())
}

fn hash(block: i64) -> String {
    format!("0x{block:064x}")
}

/// Run Project, then compare the family reads with today's. Returns the blocks' reverse claims the
/// node claim family cannot represent (one row per node, so a node whose latest name record was
/// written at another resolver than its current one loses the older record at the current one).
async fn run(pool: &PgPool, block: i64, previous: Option<i64>, mode: RunMode) -> Result<usize> {
    let outcome = Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: block,
            affected_from_block: block,
            affected_to_block: block,
            resume_current: previous.map(|number| Marker {
                number,
                hash: hash(number),
            }),
            mode,
        })
        .await?;
    let report = family_shadow::assert_family_reads_match(pool, &outcome.current).await?;
    Ok(report.node_claim_findings.len())
}

async fn snapshot(pool: &PgPool) -> Result<Option<Value>> {
    Ok(sqlx::query_scalar("SELECT to_jsonb(row) - 'claim_provenance' || jsonb_build_object('claim_provenance', claim_provenance - 'target_block_number' - 'target_block_hash') FROM primary_names_current row WHERE address=$1 AND coin_type='60' AND namespace='ens'")
        .bind(ADDRESS).fetch_optional(pool).await?)
}

async fn database(name: &str) -> Result<(TestDatabase, PgPool)> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new(format!("reverse_node_{name}"))).await?;
    let pool = database.pool().clone();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    let mut transaction = pool.begin().await?;
    raw_sql(&format!("CREATE SCHEMA bigname_phase; ALTER DATABASE \"{}\" SET search_path TO bigname_phase, public; SET LOCAL search_path TO bigname_phase, public", database_name.replace('"', "\"\""))).execute(&mut *transaction).await?;
    for script in [
        include_str!("../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../schema-v2/baseline/05_normalized_events.sql"),
        include_str!("../../../schema-v2/baseline/06_projections.sql"),
        include_str!("../../../schema-v2/baseline/07_labels.sql"),
        include_str!("../../../schema-v2/baseline/08_heartbeats.sql"),
        include_str!("../../../schema-v2/baseline/09_divergence.sql"),
        include_str!("../../../schema-v2/baseline/10_phase_state.sql"),
    ] {
        raw_sql(script).execute(&mut *transaction).await?;
    }
    transaction.commit().await?;
    pool.set_connect_options(
        pool.connect_options()
            .as_ref()
            .clone()
            .options([("search_path", "bigname_phase,public"), ("jit", "off")]),
    );
    let mut connections = Vec::new();
    for _ in 0..pool.options().get_max_connections() {
        connections.push(pool.acquire().await?);
    }
    for connection in &mut connections {
        raw_sql("SET search_path TO bigname_phase, public; SET jit = off")
            .execute(&mut **connection)
            .await?;
    }
    Ok((database, pool))
}
