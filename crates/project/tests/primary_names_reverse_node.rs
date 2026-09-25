#[path = "support/family_shadow.rs"]
mod family_shadow;

use anyhow::Result;
use bigname_project::{BatchRequest, Engine, Marker, RunMode};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use family_shadow::{Expectations, ExpectedDifference};
use serde_json::{Value, json};
use sqlx::{PgPool, raw_sql};

const CHAIN: &str = "fixture-reverse-node";
const ADDRESS: &str = "0x0000000000000000000000000000000000000001";
const RESOLVER: &str = "0x0000000000000000000000000000000000000011";
const OTHER: &str = "0x0000000000000000000000000000000000000022";
/// How the comparison shows a field one side does not have.
const ABSENT: &str = "<absent>";
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
    let shadow = claims_at_other_resolver();
    for (index, expected) in expected.into_iter().enumerate() {
        let block = index as i64 + 1;
        run(
            &pool,
            block,
            (block > 1).then_some(block - 1),
            RunMode::Normal,
            &shadow,
        )
        .await?;
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
        run(&pool, block, Some(block), RunMode::Redo, &shadow).await?;
        assert_eq!(snapshot(&pool).await?, incremental, "redo block {block}");
        run(&pool, block, None, RunMode::Normal, &shadow).await?;
        assert_eq!(snapshot(&pool).await?, incremental, "full block {block}");
    }
    shadow.finish()?;
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
    run(&pool, 7, None, RunMode::Normal, &Expectations::none()).await?;
    assert_eq!(snapshot(&pool).await?.unwrap()["claim_status"], "not_found");
    // Redo must recover the projected clearing event even after Interpret replaces it.
    sqlx::query("DELETE FROM normalized_events WHERE event_identity = '7:2'")
        .execute(&pool)
        .await?;
    run(&pool, 7, Some(7), RunMode::Redo, &Expectations::none()).await?;
    assert_eq!(
        snapshot(&pool).await?.unwrap()["raw_claim_name"],
        "before-reset.eth"
    );
    sqlx::query(
        "UPDATE normalized_events SET canonicality_state = 'orphaned' WHERE event_identity = '7:1'",
    )
    .execute(&pool)
    .await?;
    run(&pool, 7, Some(7), RunMode::Redo, &Expectations::none()).await?;
    let repaired = snapshot(&pool).await?;
    assert_eq!(repaired.as_ref().unwrap()["raw_claim_name"], "updated.eth");
    run(&pool, 7, None, RunMode::Normal, &Expectations::none()).await?;
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
    run(&pool, 3, None, RunMode::Normal, &Expectations::none()).await?;
    assert_eq!(
        snapshot(&pool).await?.unwrap()["raw_claim_name"],
        "explicit.eth"
    );
    database.cleanup().await?;
    Ok(())
}

// Retention gap, kept precise: today's reverse claim follows the latest ResolverChanged at the
// reverse node from any source family. An ENSv2 registry pointer with no resource or name lands
// in neither the ENSv1 registry-node pointer family (F4) nor the resource pointer family (F5), so
// the family read finds no resolver for the node and no claim.
#[tokio::test]
async fn an_unnamed_ens_v2_reverse_node_pointer_is_outside_the_pointer_families() -> Result<()> {
    let (database, pool) = database("unnamed_v2_pointer").await?;
    for block in 1..=2 {
        sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,$3,to_timestamp($3::double precision),'canonical')")
            .bind(CHAIN).bind(hash(block)).bind(block).execute(&pool).await?;
    }
    event(
        &pool,
        2,
        0,
        "ReverseChanged",
        json!({
            "source_event":"ReverseClaimed", "address":ADDRESS,
            "coin_type":"60", "namespace":"ens", "reverse_node":NODE
        }),
    )
    .await?;
    event_in(
        &pool,
        "ens_v2_registry_l1",
        2,
        1,
        "ResolverChanged",
        json!({"node":NODE,"resolver":RESOLVER}),
    )
    .await?;
    name(&pool, 2, 2, RESOLVER, json!("alice.eth")).await?;
    let key = format!("primary_name {ADDRESS} ens 60");
    let shadow = Expectations {
        // Today follows the pointer (event 2) to the name record (event 3); the family read has
        // neither the resolver nor the claim.
        differences: vec![ExpectedDifference {
            target: 2,
            key,
            fields: vec![
                ("claim_name_is_normalized".into(), json!(true), json!(false)),
                (
                    "claim_provenance.claim_event_id".into(),
                    json!(3),
                    json!(ABSENT),
                ),
                (
                    "claim_provenance.resolver_address".into(),
                    json!(RESOLVER),
                    json!(ABSENT),
                ),
                (
                    "claim_provenance.resolver_event_id".into(),
                    json!(2),
                    json!(ABSENT),
                ),
                ("claim_status".into(), json!("success"), json!("not_found")),
                ("raw_claim_name".into(), json!("alice.eth"), Value::Null),
            ],
            times: 1,
        }],
        ..Expectations::none()
    };
    run(&pool, 2, None, RunMode::Normal, &shadow).await?;
    shadow.finish()?;
    let today = snapshot(&pool).await?.expect("today's claim");
    assert_eq!(today["raw_claim_name"], "alice.eth");
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
    event_in(pool, family, block, log, kind, after).await
}

async fn event_in(
    pool: &PgPool,
    family: &str,
    block: i64,
    log: i64,
    kind: &str,
    after: Value,
) -> Result<()> {
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state) VALUES ($1,'ens',$2,$3,1,$4,$5,$6,$6,0,$7,'ens_v1_unwrapped_authority','canonical',$8)")
        .bind(format!("{block}:{log}")).bind(kind).bind(family).bind(CHAIN)
        .bind(block).bind(hash(block)).bind(log).bind(after).execute(pool).await?;
    Ok(())
}

fn hash(block: i64) -> String {
    format!("0x{block:064x}")
}

/// Run Project, then compare the family reads with today's, expecting `shadow`.
async fn run(
    pool: &PgPool,
    block: i64,
    previous: Option<i64>,
    mode: RunMode,
    shadow: &Expectations,
) -> Result<()> {
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
    family_shadow::compare_family_reads_at(pool, &outcome.current, shadow).await?;
    Ok(())
}

/// Step 2 keeps one node claim row per node and resolver, so the record at the current resolver
/// that today serves is in the families at blocks 4 and 11 and no difference is expected. At
/// block 5 the node's only claim is at OTHER, and today serves no claim either, so the diagnostic
/// shows with no difference.
fn claims_at_other_resolver() -> Expectations {
    Expectations {
        node_claims_at_other_resolver: vec![(5, format!("primary_name {ADDRESS} ens 60"))],
        ..Expectations::none()
    }
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
