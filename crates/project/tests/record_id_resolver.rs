use anyhow::Result;
use bigname_project::{BatchRequest, Engine, Marker, RunMode};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, raw_sql};

const CHAIN: &str = "ethereum-sepolia";
const REGISTRY: &str = "0x4444444444444444444444444444444444444444";
const RESOLVER: &str = "0x1111111111111111111111111111111111111111";
const IMPLEMENTATION: &str = "0x2222222222222222222222222222222222222222";
fn hash(n: i64) -> String {
    format!("0x{n:064x}")
}
fn resource(n: i64) -> String {
    format!("66000000-0000-0000-0000-{n:012}")
}
fn node(n: i64) -> String {
    bigname_lookup::ens_namehash_hex(&format!("record{n}.eth")).unwrap()
}

// Exercise canonical normalized link/value facts through the production Project engine.
#[tokio::test]
async fn shared_records_relink_default_and_empty_values_survive_incremental_and_redo() -> Result<()>
{
    let (db, pool) = database("record_id_links").await?;
    seed(&pool).await?;
    run(&pool, 12, None, RunMode::Normal).await?;
    assert_text(&pool, 1, "one").await?;
    assert_text(&pool, 2, "one").await?;
    assert_text(&pool, 3, "default").await?;
    run(&pool, 13, Some(12), RunMode::Normal).await?;
    assert_text(&pool, 1, "shared").await?;
    assert_text(&pool, 2, "shared").await?;
    run(&pool, 14, Some(13), RunMode::Normal).await?;
    assert_text(&pool, 2, "default").await?;
    let relink = inventory(&pool, 2).await?;
    assert_eq!(relink["boundary"]["event_kind"], "ResolverRecordLinked");
    assert_eq!(relink["last_change"]["chain_position"]["block_number"], 14);
    run(&pool, 15, Some(14), RunMode::Normal).await?;
    assert_text(&pool, 1, "default").await?;
    run(&pool, 16, Some(15), RunMode::Normal).await?;
    assert_text(&pool, 1, "new default").await?;
    assert_text(&pool, 3, "new default").await?;
    assert_text(&pool, 2, "default").await?;
    run(&pool, 17, Some(16), RunMode::Normal).await?;
    assert_text(&pool, 2, "").await?;
    run(&pool, 18, Some(17), RunMode::Normal).await?;
    let empty = inventory(&pool, 2).await?;
    for key in ["addr:60", "contenthash"] {
        let item = empty["entries"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["record_key"] == key)
            .unwrap();
        assert_eq!(item["status"], "not_found");
        assert!(item.get("value").is_none());
    }
    let incremental = snapshot(&pool).await?;
    run(&pool, 18, Some(17), RunMode::Normal).await?;
    assert_eq!(snapshot(&pool).await?, incremental, "repeat changed result");
    run(&pool, 18, None, RunMode::Normal).await?;
    assert_eq!(snapshot(&pool).await?, incremental, "full rebuild drift");
    run(&pool, 18, Some(17), RunMode::Redo).await?;
    assert_eq!(snapshot(&pool).await?, incremental, "redo drift");
    let nameless: i64 = sqlx::query_scalar("SELECT count(*) FROM normalized_events WHERE event_kind = 'RecordChanged' AND (logical_name_id IS NOT NULL OR resource_id IS NOT NULL)").fetch_one(&pool).await?;
    assert_eq!(nameless, 0, "Project mutated immutable record attribution");
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn deleted_link_and_default_changes_rebuild_every_consumer() -> Result<()> {
    let (db, pool) = database("record_id_retracted").await?;
    seed(&pool).await?;
    run(&pool, 18, None, RunMode::Normal).await?;
    // Interpret redo can remove a normalized event; retained projection citations must scope it.
    sqlx::query(
        "DELETE FROM normalized_events WHERE event_identity IN ('link-b-two','default-three')",
    )
    .execute(&pool)
    .await?;
    run(&pool, 18, Some(18), RunMode::Redo).await?;
    assert_text(&pool, 2, "shared").await?;
    assert_text(&pool, 1, "").await?;
    assert_text(&pool, 3, "").await?;
    let replay = snapshot(&pool).await?;
    run(&pool, 18, None, RunMode::Normal).await?;
    assert_eq!(
        snapshot(&pool).await?,
        replay,
        "retraction differs from clean rebuild"
    );
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn record_history_survives_relinks_and_excludes_later_unselected_writes() -> Result<()> {
    let (db, pool) = database("record_id_history").await?;
    seed(&pool).await?;
    text(&pool, "unselected-later", 16, 4, 1, "unrelated").await?;
    run(&pool, 13, None, RunMode::Normal).await?;
    assert_history(&pool, 2, &["record-one", "shared-update"], &[]).await?;
    run(&pool, 14, Some(13), RunMode::Normal).await?;
    assert_history(
        &pool,
        2,
        &["record-one", "shared-update", "record-two"],
        &[],
    )
    .await?;
    run(&pool, 18, Some(14), RunMode::Normal).await?;
    let expected = ["record-one", "shared-update", "record-two", "empty-text"];
    assert_history(&pool, 2, &expected, &["unselected-later", "record-three"]).await?;
    assert_text(&pool, 2, "").await?;
    assert_history(
        &pool,
        1,
        &["record-one", "shared-update", "record-two", "record-three"],
        &["unselected-later", "empty-text"],
    )
    .await?;
    assert_history(
        &pool,
        3,
        &["record-two", "record-three"],
        &["record-one", "empty-text"],
    )
    .await?;
    let incremental = snapshot(&pool).await?;
    run(&pool, 18, None, RunMode::Normal).await?;
    assert_eq!(
        snapshot(&pool).await?,
        incremental,
        "history full rebuild drift"
    );
    run(&pool, 18, Some(18), RunMode::Redo).await?;
    assert_eq!(snapshot(&pool).await?, incremental, "history redo drift");
    sqlx::query("DELETE FROM normalized_events WHERE event_identity = 'link-b-two'")
        .execute(&pool)
        .await?;
    run(&pool, 18, Some(18), RunMode::Redo).await?;
    assert_history(
        &pool,
        2,
        &["record-one", "shared-update", "unselected-later"],
        &["record-two", "empty-text"],
    )
    .await?;
    let replay = snapshot(&pool).await?;
    run(&pool, 18, None, RunMode::Normal).await?;
    assert_eq!(
        snapshot(&pool).await?,
        replay,
        "history retraction rebuild drift"
    );
    event(
        &pool,
        "clear-pointer",
        18,
        5,
        "ResolverChanged",
        Some(1),
        json!({"resolver":"0x0000000000000000000000000000000000000000"}),
    )
    .await?;
    text(&pool, "after-clear", 18, 6, 3, "unrelated").await?;
    run(&pool, 18, Some(18), RunMode::Normal).await?;
    assert_history(
        &pool,
        1,
        &["record-one", "shared-update", "record-two", "record-three"],
        &["after-clear", "unselected-later", "empty-text"],
    )
    .await?;
    assert_eq!(inventory(&pool, 1).await?["support"], "unsupported");
    let cleared = snapshot(&pool).await?;
    run(&pool, 18, None, RunMode::Normal).await?;
    assert_eq!(
        snapshot(&pool).await?,
        cleared,
        "cleared history rebuild drift"
    );
    db.cleanup().await?;
    Ok(())
}

// The overview's link section follows the latest `Linked` per node: record 0 drops the node,
// the empty-name node is the default record, and a name is attached only when a surface knows it.
#[tokio::test]
async fn resolver_links_summary_follows_latest_link_per_node() -> Result<()> {
    let (db, pool) = database("record_id_link_summary").await?;
    seed(&pool).await?;
    run(&pool, 12, None, RunMode::Normal).await?;
    let links = links_summary(&pool, RESOLVER).await?;
    assert_eq!(links["status"], "supported", "{links}");
    assert_eq!(links["count"], 3);
    assert_eq!(links["record_count"], 2);
    let items = links["items"].as_array().unwrap();
    let mut first_record: Vec<_> = [node(1), node(2)].into();
    first_record.sort();
    assert_eq!(items.len(), 3);
    assert_eq!(items[0]["record_id"], "1");
    assert_eq!(items[0]["namehash"], first_record[0]);
    assert_eq!(items[1]["record_id"], "1");
    assert_eq!(items[1]["namehash"], first_record[1]);
    for item in &items[..2] {
        let n = if item["namehash"] == node(1) { 1 } else { 2 };
        assert_eq!(item["name"], format!("record{n}.eth"));
        assert_eq!(item["logical_name_id"], format!("ens:{}", node(n)));
        assert_eq!(item["namespace"], "ens");
        assert_eq!(item["default"], false);
        assert_eq!(item["chain_position"]["block_number"], 11);
    }
    assert_eq!(
        items[2],
        json!({
            "record_id": "2", "namehash": hash(0), "default": true,
            "normalized_event_id": items[2]["normalized_event_id"],
            "chain_position": {
                "chain_id": CHAIN, "block_number": 12, "block_hash": hash(12),
                "transaction_hash": hash(1200), "log_index": 0,
                "timestamp": items[2]["chain_position"]["timestamp"],
            }
        })
    );
    assert!(items[2]["chain_position"]["timestamp"].is_string());

    run(&pool, 18, Some(12), RunMode::Normal).await?;
    let links = links_summary(&pool, RESOLVER).await?;
    assert_eq!(links["count"], 2, "{links}");
    assert_eq!(links["record_count"], 2);
    let items = links["items"].as_array().unwrap();
    assert_eq!(items[0]["record_id"], "2");
    assert_eq!(items[0]["namehash"], node(2));
    assert_eq!(items[0]["name"], "record2.eth");
    assert_eq!(items[0]["chain_position"]["block_number"], 14);
    assert_eq!(items[1]["record_id"], "3");
    assert_eq!(items[1]["default"], true);
    assert!(items[1].get("name").is_none());
    assert_eq!(items[1]["chain_position"]["block_number"], 16);
    let incremental = links.clone();
    run(&pool, 18, None, RunMode::Normal).await?;
    assert_eq!(
        links_summary(&pool, RESOLVER).await?,
        incremental,
        "full rebuild drift"
    );
    run(&pool, 18, Some(18), RunMode::Redo).await?;
    assert_eq!(
        links_summary(&pool, RESOLVER).await?,
        incremental,
        "redo drift"
    );
    db.cleanup().await?;
    Ok(())
}

// Redo retracting the link of a node that no name and no resource consumes leaves
// nothing else to scope the resolver; the section's digest against the canonical
// link set is what rebuilds it. A second resolver isolates that: the redo window
// carries record events for the first resolver only.
#[tokio::test]
async fn resolver_links_summary_follows_a_retracted_link_through_redo() -> Result<()> {
    const OTHER: &str = "0x5555555555555555555555555555555555555555";
    let (db, pool) = database("record_id_link_retraction").await?;
    seed(&pool).await?;
    event(
        &pool,
        "upgrade-other",
        10,
        8,
        "Upgraded",
        None,
        json!({"proxy_address":OTHER,"implementation":IMPLEMENTATION}),
    )
    .await?;
    // Node 7 has no surface, no resource, and nothing reads its record.
    event(&pool,"link-orphan",17,9,"ResolverRecordLinked",None,json!({"source_event":"Linked","storage_model":"resolver_record_id","resolver":OTHER,"node":node(7),"resolver_record_id":"1","dns_encoded_name":"0x00"})).await?;
    run(&pool, 18, None, RunMode::Normal).await?;
    let before = links_summary(&pool, OTHER).await?;
    assert_eq!(before["count"], 1, "{before}");
    assert_eq!(before["items"][0]["namehash"], node(7));
    assert!(before["digest"].is_string());
    sqlx::query("DELETE FROM normalized_events WHERE event_identity = 'link-orphan'")
        .execute(&pool)
        .await?;
    run(&pool, 18, Some(18), RunMode::Redo).await?;
    let after = links_summary(&pool, OTHER).await?;
    assert_eq!(after["count"], 0, "{after}");
    assert_eq!(after["items"], json!([]));
    assert_ne!(after["digest"], before["digest"]);
    let redone = after.clone();
    run(&pool, 18, None, RunMode::Normal).await?;
    assert_eq!(
        links_summary(&pool, OTHER).await?,
        redone,
        "redo differs from rebuild"
    );
    db.cleanup().await?;
    Ok(())
}

// A node linked before its name was observed gains the name in the resolver's
// summary on the incremental run that discovers the surface -- on a resolver the
// run has no other reason to touch -- and a shadow surface never counts as a name.
#[tokio::test]
async fn resolver_links_summary_picks_up_a_name_discovered_later() -> Result<()> {
    const OTHER: &str = "0x5555555555555555555555555555555555555555";
    let (db, pool) = database("record_id_link_late_name").await?;
    seed(&pool).await?;
    let late = bigname_domain::normalization::normalize_name("record9.eth")?;
    let late_node = node(9);
    event(
        &pool,
        "upgrade-other",
        10,
        8,
        "Upgraded",
        None,
        json!({"proxy_address":OTHER,"implementation":IMPLEMENTATION}),
    )
    .await?;
    event(&pool,"link-late",11,5,"ResolverRecordLinked",None,json!({"source_event":"Linked","storage_model":"resolver_record_id","resolver":OTHER,"node":late_node,"resolver_record_id":"1","dns_encoded_name":"0x00"})).await?;
    // The root surface -- the empty name at the all-zero node -- exists on every ENS
    // chain; the default record's link must not pick it up as a name.
    sqlx::query("INSERT INTO name_surfaces (logical_name_id,namespace,raw_name,raw_labels,dns_encoded_name,namehash,labelhashes,normalizer_version,visibility_state,chain_id,block_hash,block_number,canonicality_state) VALUES ($1,'ens','',ARRAY[]::text[],'\\x00'::bytea,$2,ARRAY[]::text[],'fixture','active',$3,$4,10,'canonical')")
        .bind(format!("ens:{}", hash(0))).bind(hash(0)).bind(CHAIN).bind(hash(10)).execute(&pool).await?;
    // The node's surface does not exist yet, so the link is served by namehash alone.
    run(&pool, 12, None, RunMode::Normal).await?;
    let item = |links: &Value| {
        links["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["namehash"] == late_node)
            .cloned()
            .unwrap()
    };
    let links = links_summary(&pool, OTHER).await?;
    assert!(item(&links).get("name").is_none(), "{links}");
    let default = links_summary(&pool, RESOLVER).await?;
    let default = default["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["default"] == true)
        .unwrap();
    assert!(default.get("name").is_none(), "{default}");
    // Block 13 observes the name: a surface plus the event that carries its logical name.
    sqlx::query("INSERT INTO name_surfaces (logical_name_id,namespace,raw_name,raw_labels,dns_encoded_name,namehash,labelhashes,normalizer_version,visibility_state,chain_id,block_hash,block_number,canonicality_state) VALUES ($1,'ens','record9.eth',ARRAY['record9','eth'],$2,$3,ARRAY['a','b'],'fixture','active',$4,$5,13,'canonical')")
        .bind(format!("ens:{late_node}")).bind(late.dns_encoded_name.clone()).bind(&late_node).bind(CHAIN).bind(hash(13)).execute(&pool).await?;
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,logical_name_id,event_kind,source_family,manifest_version,source_manifest_id,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state,raw_fact_ref) SELECT 'preimage-late','ens',$1,'PreimageObserved','ens_v2_resolver_l1',1,manifest_id,$2,13,$3,$4,0,7,'ens_v2_resolver','canonical','{}'::jsonb,'{}'::jsonb FROM manifest_versions WHERE source_family='ens_v2_resolver_l1' AND chain_id=$2")
        .bind(format!("ens:{late_node}")).bind(CHAIN).bind(hash(13)).bind(hash(1300)).execute(&pool).await?;
    run(&pool, 13, Some(12), RunMode::Normal).await?;
    let links = links_summary(&pool, OTHER).await?;
    assert_eq!(item(&links)["name"], "record9.eth", "{links}");
    assert_eq!(item(&links)["logical_name_id"], format!("ens:{late_node}"));
    // A shadow surface is not a name to show: demote it and rebuild.
    sqlx::query("UPDATE name_surfaces SET visibility_state = 'shadow', deactivation_reason = 'fixture', deactivated_at = now() WHERE logical_name_id = $1")
        .bind(format!("ens:{late_node}")).execute(&pool).await?;
    run(&pool, 13, None, RunMode::Normal).await?;
    let links = links_summary(&pool, OTHER).await?;
    assert!(item(&links).get("name").is_none(), "{links}");
    db.cleanup().await?;
    Ok(())
}

// A row built by an earlier deploy, before a section existed, is rebuilt on the
// next run even when nothing it cites changed and no event in the run's range
// names the resolver: the row's summary_version is what scopes it.
#[tokio::test]
async fn resolver_summary_reshaped_by_a_deploy_is_rebuilt_without_new_evidence() -> Result<()> {
    const OTHER: &str = "0x5555555555555555555555555555555555555555";
    let (db, pool) = database("record_id_summary_version").await?;
    seed(&pool).await?;
    event(
        &pool,
        "upgrade-other",
        10,
        8,
        "Upgraded",
        None,
        json!({"proxy_address":OTHER,"implementation":IMPLEMENTATION}),
    )
    .await?;
    run(&pool, 12, None, RunMode::Normal).await?;
    let built = summary(&pool, OTHER).await?;
    assert!(built["links"].is_object(), "{built}");
    assert_eq!(built["summary_version"], json!(1), "{built}");
    // The database an older deploy left behind: no links section, no version.
    sqlx::query("UPDATE resolver_current SET declared_summary = declared_summary - 'links' - 'summary_version' WHERE resolver_address = $1")
        .bind(OTHER).execute(&pool).await?;
    // Nothing in 13..=18 names OTHER.
    run(&pool, 18, Some(12), RunMode::Normal).await?;
    assert_eq!(
        summary(&pool, OTHER).await?,
        built,
        "stale row was not rebuilt"
    );
    // A row on the current version and untouched by the range is carried, not rebuilt.
    sqlx::query("UPDATE resolver_current SET declared_summary = declared_summary || '{\"fixture_marker\": true}' WHERE resolver_address = $1")
        .bind(OTHER).execute(&pool).await?;
    run(&pool, 18, Some(18), RunMode::Normal).await?;
    assert_eq!(
        summary(&pool, OTHER).await?["fixture_marker"],
        json!(true),
        "a current row was rebuilt without a reason"
    );
    db.cleanup().await?;
    Ok(())
}

async fn summary(pool: &PgPool, resolver: &str) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current WHERE resolver_address = $1",
    )
    .bind(resolver)
    .fetch_one(pool)
    .await?)
}

async fn links_summary(pool: &PgPool, resolver: &str) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT declared_summary -> 'links' FROM resolver_current WHERE resolver_address = $1",
    )
    .bind(resolver)
    .fetch_one(pool)
    .await?)
}

async fn assert_history(pool: &PgPool, id: i64, present: &[&str], absent: &[&str]) -> Result<()> {
    let page = bigname_storage::load_name_history_page(
        pool,
        &format!("ens:{}", node(id)),
        &[resource(id).parse()?],
        bigname_storage::HistoryScope::Both,
        true,
        None,
        100,
        bigname_storage::HistorySummaryMode::None,
        &bigname_storage::HistoryPageOptions {
            event_kinds: vec!["RecordChanged".into()],
            ..Default::default()
        },
        None,
    )
    .await?;
    let identities: Vec<_> = page
        .rows
        .iter()
        .map(|event| event.event_identity.as_str())
        .collect();
    for identity in present {
        assert_eq!(
            identities.iter().filter(|value| *value == identity).count(),
            1,
            "expected exactly one {identity}: {identities:?}"
        );
    }
    for identity in absent {
        assert!(
            !identities.contains(identity),
            "unexpected {identity}: {identities:?}"
        );
    }
    Ok(())
}

async fn run(pool: &PgPool, target: i64, previous: Option<i64>, mode: RunMode) -> Result<()> {
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: target,
            affected_from_block: previous.map_or(10, |p| (p + 1).min(target)),
            affected_to_block: target,
            resume_current: previous.map(|p| Marker {
                number: p,
                hash: hash(p),
            }),
            mode,
        })
        .await?;
    Ok(())
}
async fn inventory(pool: &PgPool, id: i64) -> Result<Value> {
    Ok(sqlx::query_scalar("SELECT jsonb_build_object('entries',entries,'last_change',last_change,'boundary',record_version_boundary,'provenance',provenance,'support',support_status) FROM record_inventory_current WHERE resource_id=$1::uuid")
        .bind(resource(id)).fetch_one(pool).await?)
}
async fn assert_text(pool: &PgPool, id: i64, value: &str) -> Result<()> {
    let row = inventory(pool, id).await?;
    assert_eq!(row["support"], "supported", "{row}");
    let item = row["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["record_key"] == "text:url")
        .unwrap();
    assert_eq!(item["value"], value, "resource{id}: {row}");
    Ok(())
}
async fn snapshot(pool: &PgPool) -> Result<Value> {
    Ok(sqlx::query_scalar("SELECT jsonb_agg(jsonb_build_object('resource',resource_id,'entries',entries,'boundary',record_version_boundary,'last_change',last_change,'provenance',provenance,'positions',chain_positions) ORDER BY resource_id) FROM record_inventory_current").fetch_one(pool).await?)
}

async fn event(
    pool: &PgPool,
    identity: &str,
    block: i64,
    log: i64,
    kind: &str,
    name: Option<i64>,
    mut after: Value,
) -> Result<()> {
    let family = if kind == "ResolverChanged" {
        "ens_v2_registry_l1"
    } else {
        "ens_v2_resolver_l1"
    };
    let derivation = match kind {
        "ResolverChanged" => "ens_v2_registry_resource_surface",
        "Upgraded" => "proxy_upgrade",
        _ => "ens_v2_resolver",
    };
    let emitter = if kind == "ResolverChanged" {
        REGISTRY
    } else {
        after["resolver"]
            .as_str()
            .or_else(|| after["proxy_address"].as_str())
            .unwrap_or(RESOLVER)
    }
    .to_owned();
    let resolver_id = resource(if emitter == RESOLVER { 900 } else { 901 });
    if kind == "ResolverChanged" {
        after = json!({"source_event":"ResolverUpdated", "resolver":after["resolver"], "sender":REGISTRY, "token_id":hash(name.unwrap())});
    } else if kind == "Upgraded" {
        after["source_event"] = json!("Upgraded");
    } else {
        after["resolver_contract_instance_id"] = json!(resolver_id);
    }
    let scope = if after["storage_model"] == "resolver_record_id" {
        if kind == "ResolverRecordLinked" {
            format!("{resolver_id}:link:{}", after["node"].as_str().unwrap())
        } else {
            format!(
                "{resolver_id}:record:{}:{}",
                after["resolver_record_id"].as_str().unwrap(),
                after["record_key"].as_str().unwrap()
            )
        }
    } else {
        format!(
            "{emitter}:{}:{}:-:{}",
            after["node"].as_str().unwrap_or("-"),
            after["token_id"].as_str().unwrap_or("-"),
            after["record_key"]
                .as_str()
                .or_else(|| after["source_event"].as_str())
                .unwrap_or(kind)
        )
    };
    let facet = match kind {
        "RecordChanged" => "records",
        "ResolverChanged" => "resolver",
        _ => kind,
    };
    let state_key = format!(
        "ens:{family}:{}:{}:{facet}:{scope}",
        name.map(|n| format!("ens:{}", node(n)))
            .unwrap_or_else(|| "-".into()),
        name.map(resource).unwrap_or_else(|| "-".into())
    );
    let before: Value = sqlx::query_scalar("SELECT after_state FROM normalized_events WHERE raw_fact_ref->>'interpreter_state_key'=$1 ORDER BY block_number DESC, transaction_index DESC, log_index DESC LIMIT 1")
        .bind(&state_key).fetch_optional(pool).await?.unwrap_or_else(||json!({}));
    let manifest_id: i64 = sqlx::query_scalar(
        "SELECT manifest_id FROM manifest_versions WHERE source_family=$1 AND chain_id=$2",
    )
    .bind(family)
    .bind(CHAIN)
    .fetch_one(pool)
    .await?;
    let source_ref = json!({"kind":"raw_log","chain_id":CHAIN,"block_hash":hash(block),"block_number":block,"transaction_hash":hash(block*100),"transaction_index":0,"log_index":log,"emitting_address":emitter,"state_scope":scope,"interpreter_state_key":state_key});
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,logical_name_id,resource_id,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state,raw_fact_ref,before_state,source_manifest_id) VALUES ($1,'ens',$2,$3::uuid,$4,$5,1,$6,$7,$8,$9,0,$10,$13,'canonical',$11,$12,$14,$15)")
        .bind(identity).bind(name.map(|n|format!("ens:{}",node(n)))).bind(name.map(resource)).bind(kind).bind(family).bind(CHAIN).bind(block).bind(hash(block)).bind(hash(block*100)).bind(log).bind(after).bind(source_ref).bind(derivation).bind(before).bind(manifest_id).execute(pool).await?;
    Ok(())
}
async fn link(
    pool: &PgPool,
    identity: &str,
    block: i64,
    log: i64,
    n: Option<i64>,
    record: i64,
) -> Result<()> {
    event(pool,identity,block,log,"ResolverRecordLinked",None,json!({"source_event":"Linked","storage_model":"resolver_record_id","resolver":RESOLVER,"node":n.map_or_else(||hash(0),node),"resolver_record_id":record.to_string(),"dns_encoded_name":n.map_or_else(||"0x00".to_owned(),|n|format!("0x{}",alloy_primitives::hex::encode(bigname_domain::normalization::normalize_name(&format!("record{n}.eth")).unwrap().dns_encoded_name)))})).await
}
async fn text(
    pool: &PgPool,
    identity: &str,
    block: i64,
    log: i64,
    record: i64,
    value: &str,
) -> Result<()> {
    event(pool,identity,block,log,"RecordChanged",None,json!({"source_event":"TextUpdated","storage_model":"resolver_record_id","resolver":RESOLVER,"resolver_record_id":record.to_string(),"record_key":"text:url","record_family":"text","selector_key":"url","value_retained":true,"value":value,"value_length":value.len()})).await
}
async fn seed(pool: &PgPool) -> Result<()> {
    for n in 10..=18 {
        sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,$3,to_timestamp($3::double precision),'canonical')").bind(CHAIN).bind(hash(n)).bind(n).execute(pool).await?;
    }
    let payload = json!({"deployment_epoch":"record_id_fixture","resolver_implementations":[{"role":"permissioned_resolver","address":IMPLEMENTATION}],"contracts":[],"capability_flags":{},"abi":{"events":[{"name":"Linked","fragment":"event Linked(uint256 indexed recordId, bytes32 indexed node, bytes name)","normalized_events":["ResolverRecordLinked","PreimageObserved"]}]}});
    let manifest: i64 = sqlx::query_scalar("INSERT INTO manifest_versions (manifest_version,namespace,source_family,chain_id,deployment_label,rollout_status,normalizer_version,file_path,manifest_payload) VALUES (1,'ens','ens_v2_resolver_l1',$1,'record_id_fixture','active','fixture','fixture/record-id.toml',$2) RETURNING manifest_id").bind(CHAIN).bind(&payload).fetch_one(pool).await?;
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,source_manifest_id,chain_id,derivation_kind,canonicality_state,after_state) VALUES ('manifest','ens','SourceManifestUpdated','ens_v2_resolver_l1',1,$1,$2,'manifest_sync','canonical',$3)").bind(manifest).bind(CHAIN).bind(json!({"rollout_status":"active","normalizer_version":"fixture","manifest_payload":payload})).execute(pool).await?;
    sqlx::query("INSERT INTO manifest_versions (manifest_version,namespace,source_family,chain_id,deployment_label,rollout_status,normalizer_version,file_path,manifest_payload) VALUES (1,'ens','ens_v2_registry_l1',$1,'record_id_fixture','active','fixture','fixture/registry.toml','{}')").bind(CHAIN).execute(pool).await?;
    for n in 1..=3 {
        let normalized = bigname_domain::normalization::normalize_name(&format!("record{n}.eth"))?;
        let labelhashes: Vec<_> = normalized
            .normalized_labels
            .iter()
            .map(|label| format!("{:#x}", alloy_primitives::keccak256(label)))
            .collect();
        sqlx::query("INSERT INTO name_surfaces (logical_name_id,namespace,raw_name,raw_labels,dns_encoded_name,namehash,labelhashes,normalizer_version,visibility_state,chain_id,block_hash,block_number,canonicality_state) VALUES ($1,'ens',$2,string_to_array($2,'.'),$6,$3,$7,'fixture','active',$4,$5,10,'canonical')").bind(format!("ens:{}",node(n))).bind(format!("record{n}.eth")).bind(node(n)).bind(CHAIN).bind(hash(10)).bind(normalized.dns_encoded_name).bind(labelhashes).execute(pool).await?;
        sqlx::query("INSERT INTO resources (resource_id,chain_id,block_hash,block_number,canonicality_state) VALUES ($1::uuid,$2,$3,10,'canonical')").bind(resource(n)).bind(CHAIN).bind(hash(10)).execute(pool).await?;
        sqlx::query("INSERT INTO surface_bindings (surface_binding_id,logical_name_id,resource_id,binding_kind,authority_arm,active_from,chain_id,block_hash,block_number,canonicality_state) VALUES ($1::uuid,$2,$3::uuid,'declared_registry_path','ens_v2',to_timestamp(10),$4,$5,10,'canonical')").bind(resource(100+n)).bind(format!("ens:{}",node(n))).bind(resource(n)).bind(CHAIN).bind(hash(10)).execute(pool).await?;
        event(
            pool,
            &format!("pointer{n}"),
            10,
            n,
            "ResolverChanged",
            Some(n),
            json!({"resolver":RESOLVER,"node":node(n)}),
        )
        .await?;
    }
    event(
        pool,
        "upgrade",
        10,
        4,
        "Upgraded",
        None,
        json!({"proxy_address":RESOLVER,"implementation":IMPLEMENTATION}),
    )
    .await?;
    // Writes precede later links and need not have name/resource attribution.
    text(pool, "record-one", 10, 5, 1, "one").await?;
    text(pool, "record-two", 10, 6, 2, "default").await?;
    text(pool, "record-three", 10, 7, 3, "new default").await?;
    link(pool, "link-a-one", 11, 0, Some(1), 1).await?;
    link(pool, "link-b-one", 11, 1, Some(2), 1).await?;
    link(pool, "default-two", 12, 0, None, 2).await?;
    text(pool, "shared-update", 13, 0, 1, "shared").await?;
    link(pool, "link-b-two", 14, 0, Some(2), 2).await?;
    link(pool, "unlink-a", 15, 0, Some(1), 0).await?;
    link(pool, "default-three", 16, 0, None, 3).await?;
    text(pool, "empty-text", 17, 0, 2, "").await?;
    for (log, family, key, extra) in [
        (
            0,
            "addr",
            "addr:60",
            json!({"selector_key":"60","coin_type":"60","address_bytes_hex":"0x"}),
        ),
        (
            1,
            "contenthash",
            "contenthash",
            json!({"selector_key":null,"contenthash_hex":"0x"}),
        ),
    ] {
        let mut after = json!({"source_event":if family == "addr" {"AddressUpdated"} else {"ContenthashUpdated"},"storage_model":"resolver_record_id","resolver":RESOLVER,"resolver_record_id":"2","record_family":family,"record_key":key,"value_retained":false});
        after
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        event(
            pool,
            &format!("empty-{family}"),
            18,
            log,
            "RecordChanged",
            None,
            after,
        )
        .await?;
    }
    Ok(())
}

async fn database(name: &str) -> Result<(TestDatabase, PgPool)> {
    let database = TestDatabase::create(TestDatabaseConfig::new(name.to_string())).await?;
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
            .options([("search_path", "bigname_phase,public")]),
    );
    let mut connections = Vec::new();
    for _ in 0..pool.options().get_max_connections() {
        connections.push(pool.acquire().await?);
    }
    for connection in &mut connections {
        sqlx::query("SET search_path TO bigname_phase, public")
            .execute(&mut **connection)
            .await?;
    }
    Ok((database, pool))
}

#[tokio::test]
async fn official_sepolia_direct_resolver_projects_ensip19_default_for_missing_eth_address()
-> Result<()> {
    use bigname_domain::resolver_read::{IndexedRecordStatus, evaluate_indexed_record};
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let repository = bigname_manifests::load_repository(root.join("manifests/sepolia"))?;
    let manifest = &repository
        .manifests()
        .iter()
        .find(|m| m.manifest.source_family == "ens_v2_resolver_l1")
        .unwrap()
        .manifest;
    let direct = manifest
        .contracts
        .iter()
        .find(|c| c.role == "public_resolver_v2")
        .unwrap();
    let target = i64::try_from(direct.start_block.unwrap())? + 1;
    let address = direct.address.to_ascii_lowercase();
    let payload = serde_json::to_value(manifest)?;
    let (db, pool) = database("record_id_public_default").await?;
    seed(&pool).await?;
    let _: i64 = sqlx::query_scalar("UPDATE manifest_versions SET manifest_payload=$1, normalizer_version=$2, deployment_label=$3 WHERE source_family='ens_v2_resolver_l1' RETURNING manifest_id")
        .bind(&payload).bind(&manifest.normalizer_version).bind(&manifest.deployment_epoch).fetch_one(&pool).await?;
    sqlx::query("UPDATE normalized_events SET after_state=$1 WHERE event_identity='manifest'")
        .bind(json!({"rollout_status":"active","normalizer_version":manifest.normalizer_version,"manifest_payload":payload})).execute(&pool).await?;
    sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,$3,to_timestamp($3::double precision),'canonical')")
        .bind(CHAIN).bind(hash(target)).bind(target).execute(&pool).await?;
    event(
        &pool,
        "public-pointer",
        target,
        0,
        "ResolverChanged",
        Some(1),
        json!({"resolver":address,"node":node(1)}),
    )
    .await?;
    let value = "0x3333333333333333333333333333333333333333";
    event(
        &pool,
        "public-default",
        target,
        1,
        "RecordChanged",
        None,
        json!({
            "source_event":"AddressChanged","resolver":address,"node":node(1),
            "record_family":"addr","record_key":"addr:2147483648","selector_key":"2147483648",
            "coin_type":"2147483648","value_retained":false,"address_bytes_hex":value
        }),
    )
    .await?;
    run(&pool, target, None, RunMode::Normal).await?;
    // A direct node-keyed declaration has no link state, so the section is unsupported
    // by kind rather than reported empty.
    assert_eq!(
        links_summary(&pool, &address).await?,
        json!({"status": "unsupported", "unsupported_reason": "record_links_not_applicable"})
    );
    let row = inventory(&pool, 1).await?;
    assert_eq!(row["support"], "supported", "{row}");
    assert_eq!(
        row["provenance"]["read_rules"],
        json!([{"kind":"ensip19_default_address","source_record_key":"addr:2147483648"}])
    );
    assert!(
        !row["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["record_key"] == "addr:60")
    );
    let answer = evaluate_indexed_record(
        &row["entries"],
        &row["provenance"],
        &json!({"status":"projected"}),
        "addr:60",
        "addr",
        Some("60"),
    );
    assert_eq!(answer.status, IndexedRecordStatus::Success);
    assert_eq!(answer.value, Some(json!(value)));
    db.cleanup().await?;
    Ok(())
}
