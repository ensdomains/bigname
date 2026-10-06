//! Reader seams: produced pair semantics are exercised through the HTTP producer fixture;
//! these retained rows exercise eligibility, malformed evidence, pagination and exact plans.

use std::collections::BTreeSet;

use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgConnection, Postgres, QueryBuilder};
use uuid::Uuid;

use super::super::{
    EventHistoryReadFilter, HistoryCursor, HistoryOrder,
    contract_count::{contract_count_filter, count_contract_events},
    decoders::decode_history_event,
    keyset::{HistoryKeyset, history_cursor_from_row},
    paging::push_history_page_query,
    summary::push_history_count_query,
};

const VALUE_A: &str = "0x000000000000000000000000000000000000000a";
const VALUE_B: &str = "0x000000000000000000000000000000000000000b";
const ZERO: &str = "0x0000000000000000000000000000000000000000";
const GENERATIONS: [(&str, &str, &str, &str, bool); 7] = [
    (
        "ethereum-mainnet",
        "ens_v1_resolver_l1",
        "public_resolver",
        "0xf29100983e058b709f3d539b0c765937b804ac15",
        true,
    ),
    (
        "ethereum-sepolia",
        "ens_v1_resolver_l1",
        "public_resolver",
        "0xe99638b40e4fff0129d56f03b55b6bbc4bbe49b5",
        true,
    ),
    (
        "ethereum-mainnet",
        "ens_v1_resolver_l1",
        "public_resolver_231b0ee",
        "0x231b0ee14048e9dccd1d247744d114a4eb5e8e63",
        false,
    ),
    (
        "ethereum-sepolia",
        "ens_v1_resolver_l1",
        "public_resolver_8948458",
        "0x8948458626811dd0c23eb25cc74291247077cc51",
        false,
    ),
    (
        "ethereum-sepolia",
        "ens_v1_resolver_l1",
        "public_resolver_8fade66",
        "0x8fade66b79cc9f707ab26799354482eb93a5b7dd",
        false,
    ),
    (
        "base-mainnet",
        "basenames_base_resolver",
        "resolver",
        "0xc6d566a56a1aff6508b41f6c90ff131615583bcd",
        false,
    ),
    (
        "ethereum-sepolia",
        "ens_v2_resolver_l1",
        "public_resolver_v2",
        "0xdc4a563d00f5c3012b699794eb9e13a561be386f",
        true,
    ),
];

#[tokio::test]
async fn address_pairs_keep_repeated_writes_and_page_count_contract_parity() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("history_address_pairs")).await?;
    let result = async {
        let mut conn = database.pool().acquire().await?;
        install(&mut conn).await?;
        let mut expected = BTreeSet::new();
        // A→A→B→A is four writes, even in a single transaction.
        for (index, value) in [VALUE_A, VALUE_A, VALUE_B, VALUE_A].iter().enumerate() {
            pair(&mut conn, 0, index as i64 * 2, value, false).await?;
            expected.insert(identity(0, index as i64 * 2));
        }
        for log in [8, 9] {
            event(&mut conn, 0, log, "TextChanged", VALUE_A, false).await?;
            expected.insert(identity(0, log));
        }
        // Standalone and nonadjacent writes remain distinct.
        event(&mut conn, 0, 10, "AddressChanged", VALUE_A, false).await?;
        event(&mut conn, 0, 12, "AddrChanged", VALUE_A, false).await?;
        expected.extend([identity(0,10),identity(0,12)]);
        // A custom emitter with identical ABI and declared role is not source proof.
        pair(&mut conn, 0, 13, VALUE_A, false).await?;
        sqlx::query("UPDATE normalized_events SET raw_fact_ref=jsonb_set(raw_fact_ref,'{emitting_address}',to_jsonb($1::text)),after_state=jsonb_set(after_state,'{resolver}',to_jsonb($1::text)) WHERE log_index IN (13,14)")
            .bind("0x00000000000000000000000000000000000000ff").execute(&mut *conn).await?;
        expected.extend([identity(0,13),identity(0,14)]);
        // Same value but different nodes cannot be a pair.
        pair(&mut conn, 0, 15, VALUE_A, false).await?;
        sqlx::query("UPDATE normalized_events SET after_state=jsonb_set(after_state,'{node}',to_jsonb($1::text)) WHERE log_index=16")
            .bind(format!("0x{:064x}",2)).execute(&mut *conn).await?;
        expected.extend([identity(0,15),identity(0,16)]);
        let filter = EventHistoryReadFilter::default();
        for order in [HistoryOrder::Asc,HistoryOrder::Desc] {
            let filter = EventHistoryReadFilter {order,..filter.clone()};
            assert_eq!(walk(&mut conn,&filter).await?.into_iter().collect::<BTreeSet<_>>(),expected);
            assert_eq!(count(&mut conn,&filter,None).await?,expected.len() as i64);
            assert_eq!(count(&mut conn,&filter,Some(4)).await?,4);
        }
        let contract = contract_count_filter(GENERATIONS[0].0,GENERATIONS[0].3,&["RecordChanged".into()],Some(100));
        let contract_rows = walk(&mut conn,&contract).await?;
        assert_eq!(count_contract_events(&mut *conn,GENERATIONS[0].0,GENERATIONS[0].3,&["RecordChanged".into()],Some(100)).await?,contract_rows.len() as u64);
        assert_eq!(contract_rows.len(),expected.len()-2);
        let key_filter = EventHistoryReadFilter {record_key:Some("text:description".into()),..Default::default()};
        assert_eq!(walk(&mut conn,&key_filter).await?.len(),2,"same-value text writes stay separate");
        // The sole eligible legacy row must survive a retracted or unpublished representative.
        sqlx::raw_sql("UPDATE normalized_events SET canonicality_state='orphaned' WHERE log_index=0;").execute(&mut *conn).await?;
        let visible = walk(&mut conn,&filter).await?;
        assert!(!visible.contains(&identity(0,0))); assert!(visible.contains(&identity(0,1)));
        sqlx::raw_sql("UPDATE normalized_events SET canonicality_state='canonical',consumer_visibility='candidate',migration_correlation_ids=ARRAY['pair-read-test'] WHERE log_index=0;").execute(&mut *conn).await?;
        assert!(walk(&mut conn,&filter).await?.contains(&identity(0,1)));
        sqlx::raw_sql("UPDATE normalized_events SET consumer_visibility='activated' WHERE log_index=0; UPDATE normalized_events SET canonicality_state='orphaned' WHERE log_index=1;").execute(&mut *conn).await?;
        assert!(walk(&mut conn,&filter).await?.contains(&identity(0,0)));
        assert_eq!(count(&mut conn,&EventHistoryReadFilter {to_block:Some(99),..Default::default()},None).await?,0);
        // Different physical forks never provide a mate, even with matching transaction data.
        sqlx::raw_sql("INSERT INTO chain_lineage(chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES('ethereum-mainnet','alternate',100,to_timestamp(100),'orphaned'); UPDATE normalized_events SET block_hash='alternate',canonicality_state='orphaned' WHERE log_index=0; UPDATE normalized_events SET canonicality_state='canonical' WHERE log_index=1;").execute(&mut *conn).await?;
        assert!(walk(&mut conn,&filter).await?.contains(&identity(0,1)));
        Ok(())
    }.await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn address_pair_source_generations_and_empty_clears_are_exact() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("history_pair_sources")).await?;
    let result = async {
        let mut conn = database.pool().acquire().await?;
        install(&mut conn).await?;
        let mut expected = BTreeSet::new();
        for (generation, (_, _, _, _, empty_clear)) in GENERATIONS.iter().enumerate() {
            pair(&mut conn, generation, 0, VALUE_A, false).await?;
            pair(&mut conn, generation, 2, ZERO, true).await?;
            expected.extend([identity(generation, 0), identity(generation, 2)]);
            if !empty_clear {
                expected.insert(identity(generation, 3));
            }
        }
        assert_eq!(
            walk(&mut conn, &EventHistoryReadFilter::default())
                .await?
                .into_iter()
                .collect::<BTreeSet<_>>(),
            expected
        );
        // A missing source declaration is not inferred from a known address or ABI.
        sqlx::query("DELETE FROM manifest_contract_instances WHERE manifest_id=1")
            .execute(&mut *conn)
            .await?;
        let visible = walk(&mut conn, &EventHistoryReadFilter::default()).await?;
        assert!(visible.contains(&identity(0, 1)));
        assert!(visible.contains(&identity(0, 3)));
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn adjacent_address_pair_probes_use_full_physical_index_keys() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("history_pair_plan")).await?;
    let result=async {
        let mut conn=database.pool().acquire().await?;install(&mut conn).await?;
        pair(&mut conn,0,0,VALUE_A,false).await?;
        sqlx::raw_sql("INSERT INTO normalized_events(event_identity,namespace,event_kind,source_family,manifest_version,chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state) SELECT 'noise-'||n,'ens','RecordChanged','ens_v1_resolver_l1',1,'ethereum-mainnet','block',100,'noise-'||n,n,0,'ens_v1_unwrapped_authority','canonical',jsonb_build_object('source_event','TextChanged','record_key','text:name') FROM generate_series(1,12000) n; ANALYZE normalized_events; ANALYZE chain_lineage; SET jit=off;").execute(&mut *conn).await?;
        let filter=EventHistoryReadFilter {record_key:Some("addr:60".into()),..Default::default()};
        let mut query=QueryBuilder::<Postgres>::new("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) ");
        push_history_count_query(&mut query,&filter,true,None);
        let plan: Value=query.build_query_scalar().fetch_one(&mut *conn).await?;
        let mut scans=Vec::new(); scan_alias(&plan[0]["Plan"],"paired",&mut scans);
        assert!(!scans.is_empty(),"production statement did not probe pairs: {plan}");
        for scan in scans {
            let keys=scan["Index Cond"].as_str().unwrap_or_default();
            assert!(keys.contains("chain_id") && keys.contains("block_hash") && keys.contains("transaction_index") && keys.contains("log_index"),"pair probe lacks exact physical keys: {scan}");
        }
        if let Ok(directory)=std::env::var("BIGNAME_HISTORY_ACTIONS_PLAN_DIR") {
            std::fs::create_dir_all(&directory)?;
            std::fs::write(std::path::Path::new(&directory).join("address-pair-plan.json"),serde_json::to_vec_pretty(&plan)?)?;
        }
        Ok(())
    }.await;
    database.cleanup().await?;
    result
}

fn scan_alias<'a>(node: &'a Value, alias: &str, scans: &mut Vec<&'a Value>) {
    if node["Alias"] == alias {
        scans.push(node);
    }
    for child in node["Plans"].as_array().into_iter().flatten() {
        scan_alias(child, alias, scans);
    }
}

async fn count(
    conn: &mut PgConnection,
    filter: &EventHistoryReadFilter,
    limit: Option<i64>,
) -> Result<i64> {
    let mut query = QueryBuilder::<Postgres>::new("");
    push_history_count_query(&mut query, filter, true, limit);
    Ok(query.build_query_scalar().fetch_one(conn).await?)
}

async fn walk(conn: &mut PgConnection, filter: &EventHistoryReadFilter) -> Result<Vec<String>> {
    let mut seen = Vec::new();
    let mut cursor: Option<HistoryCursor> = None;
    loop {
        let keyset = cursor.as_ref().map(|cursor| HistoryKeyset {
            cursor,
            block_number: cursor.position.as_ref().and_then(|p| p.block_number),
        });
        let mut query = QueryBuilder::<Postgres>::new("");
        push_history_page_query(&mut query, filter, true, keyset.as_ref(), false, 2);
        let rows = query
            .build()
            .fetch_all(&mut *conn)
            .await?
            .into_iter()
            .map(decode_history_event)
            .collect::<Result<Vec<_>>>()?;
        let Some(first) = rows.first() else { break };
        assert!(
            !seen.contains(&first.event_identity),
            "duplicate across page boundary"
        );
        seen.push(first.event_identity.clone());
        if rows.len() == 1 {
            break;
        }
        cursor = Some(history_cursor_from_row(first));
    }
    Ok(seen)
}

fn identity(generation: usize, log: i64) -> String {
    format!("pair:{generation}:{log}")
}
async fn pair(
    conn: &mut PgConnection,
    generation: usize,
    log: i64,
    value: &str,
    empty: bool,
) -> Result<()> {
    event(conn, generation, log, "AddressChanged", value, empty).await?;
    event(conn, generation, log + 1, "AddrChanged", value, false).await
}
async fn event(
    conn: &mut PgConnection,
    generation: usize,
    log: i64,
    source: &str,
    value: &str,
    empty: bool,
) -> Result<()> {
    let (chain, family, _, address, _) = GENERATIONS[generation];
    let mut after = json!({"source_event":source,"resolver":address,"resolver_contract_instance_id":Uuid::from_u128(generation as u128+1).to_string(),"node":format!("0x{:064x}",1),"record_key":"addr:60","record_family":"addr","selector_key":"60","value_retained":!empty});
    if source == "TextChanged" {
        after["record_key"] = json!("text:description");
    }
    if empty {
        after["address_bytes_hex"] = json!("0x");
        after["coin_type"] = json!("60");
    } else {
        after["value"] = json!(value);
    }
    sqlx::query("INSERT INTO normalized_events(event_identity,namespace,event_kind,source_family,manifest_version,source_manifest_id,chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state,raw_fact_ref) VALUES($1,$2,'RecordChanged',$3,1,$4,$5,'block',100,'tx',0,$6,'ens_v1_unwrapped_authority','canonical',$7,$8)")
        .bind(identity(generation,log)).bind(if family.starts_with("basenames"){"basenames"}else{"ens"}).bind(family).bind(generation as i64+1).bind(chain).bind(log).bind(after).bind(json!({"emitting_address":address})).execute(conn).await?;
    Ok(())
}
async fn install(conn: &mut PgConnection) -> Result<()> {
    sqlx::raw_sql("CREATE SCHEMA bigname_phase;SET search_path=bigname_phase,public")
        .execute(&mut *conn)
        .await?;
    for schema in [
        include_str!("../../schema/baseline/01_chain.sql"),
        include_str!("../../schema/baseline/02_raw_facts.sql"),
        include_str!("../../schema/baseline/03_identity.sql"),
        include_str!("../../schema/baseline/04_manifests.sql"),
        include_str!("../../schema/baseline/05_normalized_events.sql"),
    ] {
        sqlx::raw_sql(schema).execute(&mut *conn).await?;
    }
    for chain in ["ethereum-mainnet", "ethereum-sepolia", "base-mainnet"] {
        sqlx::query("INSERT INTO chain_lineage(chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES($1,'block',100,to_timestamp(100),'canonical')").bind(chain).execute(&mut *conn).await?;
    }
    for (generation, (chain, family, role, address, _)) in GENERATIONS.iter().enumerate() {
        let id = generation as i64 + 1;
        let instance = Uuid::from_u128(id as u128);
        sqlx::query("INSERT INTO contract_instances(contract_instance_id,chain_id,contract_kind) VALUES($1,$2,'contract')").bind(instance).bind(chain).execute(&mut *conn).await?;
        sqlx::query("INSERT INTO manifest_versions(manifest_version,namespace,source_family,chain_id,deployment_label,rollout_status,normalizer_version,file_path,manifest_payload) VALUES(1,$1,$2,$3,$4,'shadow','test',$4,'{}')")
            .bind(if family.starts_with("basenames"){"basenames"}else{"ens"}).bind(family).bind(chain).bind(format!("generation-{id}")).execute(&mut *conn).await?;
        sqlx::query("INSERT INTO manifest_contract_instances(manifest_id,chain_id,declaration_kind,declaration_name,contract_instance_id,declared_address,role,proxy_kind,start_block_number) VALUES($1,$2,'contract',$3,$4,$5,$3,'none',0)")
            .bind(id).bind(chain).bind(role).bind(instance).bind(address).execute(&mut *conn).await?;
    }
    Ok(())
}
