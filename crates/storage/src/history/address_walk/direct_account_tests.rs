//! Direct account membership must agree across bounded walks, catalogue discovery/pages and
//! complete counts. Hand-shaped fixtures here exercise reader seams; HTTP tests use producers.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgConnection, Postgres, QueryBuilder, Row};

use super::{
    AddressRead, Witness, catalogue_count, catalogue_source, catalogue_tests::install_catalogue,
    matches::Membership, plan_tests, source, walk,
};
use crate::{
    AddressNameRelation,
    history::{
        EventHistoryReadFilter, HistoryOrder, HistoryScope, HistorySummaryMode,
        keyset::HistoryKeyset,
    },
};

const OWNER: &str = "0x0000000000000000000000000000000000000a11";
const OPERATOR: &str = "0x0000000000000000000000000000000000000b11";
const DIRECT_ONLY: &str = "0x0000000000000000000000000000000000000f11";

#[tokio::test]
async fn direct_accounts_have_exact_scope_relation_and_catalogue_count_parity() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("history_direct_accounts")).await?;
    let result = async {
        let mut conn = database.pool().acquire().await?;
        install(&mut conn, 8, 120).await?;
        let bounds = BTreeMap::from([("ethereum-mainnet".to_owned(), 129)]);
        let filter = EventHistoryReadFilter {
            event_kinds: vec!["AccountPermissionChanged".into(), "ReverseChanged".into()],
            ..Default::default()
        };
        for (address, relations, expected) in [
            (OWNER, None, 4),
            (
                OWNER,
                Some(vec![
                    AddressNameRelation::TokenHolder,
                    AddressNameRelation::EffectiveController,
                    AddressNameRelation::RoleHolder,
                ]),
                4,
            ),
            (OWNER, Some(vec![AddressNameRelation::TokenHolder]), 3),
            (OWNER, Some(vec![AddressNameRelation::RoleHolder]), 1),
            (
                OWNER,
                Some(vec![AddressNameRelation::EffectiveController]),
                0,
            ),
            (OPERATOR, None, 2),
            (OPERATOR, Some(vec![AddressNameRelation::TokenHolder]), 0),
            (OPERATOR, Some(vec![AddressNameRelation::RoleHolder]), 2),
            (DIRECT_ONLY, None, 2),
        ] {
            for scope in [
                HistoryScope::Both,
                HistoryScope::Surface,
                HistoryScope::Resource,
            ] {
                let expected = if scope == HistoryScope::Both {
                    expected
                } else {
                    0
                };
                for order in [HistoryOrder::Asc, HistoryOrder::Desc] {
                    let filter = EventHistoryReadFilter {
                        order,
                        ..filter.clone()
                    };
                    let bounded = AddressRead {
                        address,
                        namespace: Some("ens"),
                        relations: relations.as_deref(),
                        scope,
                        canonical_only: true,
                        published: Some(&bounds),
                        catalogue: false,
                    };
                    let catalogue = AddressRead {
                        catalogue: true,
                        ..bounded
                    };
                    let left = candidates(&mut conn, &bounded, &filter).await?;
                    let right = catalogue_walk(&mut conn, &catalogue, &filter).await?;
                    assert_eq!(
                        left.iter().cloned().collect::<BTreeSet<_>>(),
                        right.iter().cloned().collect::<BTreeSet<_>>(),
                        "{address} {relations:?} {scope:?} {order:?}"
                    );
                    assert_eq!(
                        left.len(),
                        expected,
                        "direct events must appear once per participant"
                    );
                    // The direct-name scalar shortcut cannot omit these separate arms.
                    assert_eq!(
                        catalogue_count::count(
                            &mut conn,
                            &catalogue,
                            &filter,
                            HistorySummaryMode::Count
                        )
                        .await?,
                        catalogue_count::CountOutcome::Unknown
                    );
                    sqlx::raw_sql("BEGIN READ ONLY").execute(&mut *conn).await?;
                    let mut count = walk::Accumulator::new(0, None);
                    walk::collect(
                        &mut conn,
                        &bounded,
                        &filter,
                        None,
                        None,
                        &mut Membership::new(),
                        &mut count,
                    )
                    .await?;
                    assert_eq!(count.count, expected as u64);
                    let mut capped = walk::Accumulator::new(0, Some(2));
                    walk::collect(
                        &mut conn,
                        &bounded,
                        &filter,
                        None,
                        None,
                        &mut Membership::new(),
                        &mut capped,
                    )
                    .await?;
                    assert_eq!(capped.count, expected.min(2) as u64);
                    sqlx::raw_sql("ROLLBACK").execute(&mut *conn).await?;
                }
            }
        }
        // Catalogue discovery must find direct-only participants with no name anchors.
        let read = AddressRead {
            address: DIRECT_ONLY,
            namespace: Some("ens"),
            relations: None,
            scope: HistoryScope::Both,
            canonical_only: true,
            published: Some(&bounds),
            catalogue: true,
        };
        let mut query = QueryBuilder::<Postgres>::new("");
        catalogue_source::push_seek_query(&mut query, &read, &filter, None);
        assert_eq!(
            query
                .build_query_scalar::<Option<i64>>()
                .fetch_one(&mut *conn)
                .await?,
            Some(0)
        );
        // A lifecycle row with an accidental retained anchor never enters either address feed.
        let lifecycle = EventHistoryReadFilter {
            event_kinds: vec![
                "RegistryCreated".into(),
                "ParentChanged".into(),
                "Upgraded".into(),
            ],
            ..Default::default()
        };
        let owner = AddressRead {
            address: OWNER,
            ..read
        };
        assert!(
            candidates(
                &mut conn,
                &AddressRead {
                    catalogue: false,
                    ..owner
                },
                &lifecycle
            )
            .await?
            .is_empty()
        );
        assert!(
            catalogue_walk(&mut conn, &owner, &lifecycle)
                .await?
                .is_empty()
        );
        assert_eq!(
            catalogue_count::count(&mut conn, &owner, &lifecycle, HistorySummaryMode::Count)
                .await?,
            catalogue_count::CountOutcome::Exact(0)
        );
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn direct_account_dense_address_reader_and_bucket_plans_are_participant_keyed() -> Result<()>
{
    let database =
        TestDatabase::create(TestDatabaseConfig::new("history_direct_account_plans")).await?;
    let result=async {
        let mut conn=database.pool().acquire().await?;install(&mut conn,32,320).await?;
        // 128 additional approvals for the dense participant and many distinct unrelated
        // participants. This keeps the full name/resource reader populated too.
        sqlx::raw_sql(&format!("INSERT INTO normalized_events(event_identity,namespace,event_kind,source_family,manifest_version,chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state)
            SELECT 'approval-noise-'||n,'ens','AccountPermissionChanged','ens_v1_registry_l1',1,'ethereum-mainnet','block-1',1,'account-noise',n,0,'ens_v1_unwrapped_authority','canonical',jsonb_build_object('subject','0x'||lpad(to_hex(n+100000),40,'0'),'relation_kind','operator','scope',jsonb_build_object('kind','account','owner',CASE WHEN n<=128 THEN '{OWNER}' ELSE '0x'||lpad(to_hex(n+1000000),40,'0') END)) FROM generate_series(1,12000) n; ANALYZE normalized_events; SET jit=off;"))
            .execute(&mut *conn).await?;
        let bounds=BTreeMap::from([("ethereum-mainnet".to_owned(),353)]);
        let read=AddressRead {address:OWNER,namespace:Some("ens"),relations:None,scope:HistoryScope::Both,canonical_only:true,published:Some(&bounds),catalogue:false};
        let filter=EventHistoryReadFilter {event_kinds:vec!["AccountPermissionChanged".into(),"ReverseChanged".into()],..Default::default()};
        assert_eq!(candidates(&mut conn,&read,&filter).await?.len(),132);
        sqlx::raw_sql("BEGIN READ ONLY").execute(&mut *conn).await?;
        let mut exact=walk::Accumulator::new(2,None);
        walk::collect(&mut conn,&read,&filter,None,None,&mut Membership::new(),&mut exact).await?;
        assert_eq!(exact.count,132);assert_eq!(exact.ids.len(),2);
        sqlx::raw_sql("ROLLBACK").execute(&mut *conn).await?;
        for label in ["bounded-and-count","catalogue-page","catalogue-first-bucket","catalogue-next-bucket"] {
            let mut query=QueryBuilder::<Postgres>::new("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) ");
            match label {
                "bounded-and-count"=>source::push_candidate_query(&mut query,&read,&filter,None),
                "catalogue-page"=>catalogue_source::push_candidate_query(&mut query,&read,&filter,None,0,None,2),
                "catalogue-first-bucket"=>catalogue_source::push_seek_query(&mut query,&read,&filter,None),
                _=>catalogue_source::push_seek_query(&mut query,&read,&filter,Some(0)),
            }
            let sql=query.sql().to_owned();
            let custom:Value=query.build_query_scalar().persistent(false).fetch_one(&mut *conn).await?;
            let generic_sql=sql.replacen("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON)","EXPLAIN (GENERIC_PLAN,FORMAT JSON)",1);
            let generic:Value=sqlx::raw_sql(&generic_sql).fetch_one(&mut *conn).await?.get(0);
            for (mode,plan) in [("custom",&custom),("generic",&generic)] {
                let mut scans=Vec::new();scans_for_events(&plan[0]["Plan"],&mut scans);
                for scan in scans {
                    if mode=="generic"||scan["Actual Loops"].as_u64().unwrap_or(0)>0 {
                        assert_ne!(scan["Node Type"],"Seq Scan","{label}/{mode} scanned all events: {scan}");
                    }
                }
                if let Ok(directory)=std::env::var("BIGNAME_HISTORY_ACTIONS_PLAN_DIR") {
                    std::fs::create_dir_all(&directory)?;
                    std::fs::write(std::path::Path::new(&directory).join(format!("direct-account-{label}-{mode}.json")),serde_json::to_vec_pretty(plan)?)?;
                    std::fs::write(std::path::Path::new(&directory).join(format!("direct-account-{label}.sql")),&sql)?;
                }
            }
        }
        Ok(())
    }.await;
    database.cleanup().await?;
    result
}

fn scans_for_events<'a>(node: &'a Value, scans: &mut Vec<&'a Value>) {
    if node["Relation Name"] == "normalized_events" {
        scans.push(node);
    }
    for child in node["Plans"].as_array().into_iter().flatten() {
        scans_for_events(child, scans);
    }
}

async fn candidates(
    conn: &mut PgConnection,
    read: &AddressRead<'_>,
    filter: &EventHistoryReadFilter,
) -> Result<Vec<String>> {
    let mut query = QueryBuilder::<Postgres>::new("");
    source::push_candidate_query(&mut query, read, filter, None);
    let rows: Vec<Witness> = query.build_query_as().fetch_all(conn).await?;
    Ok(rows
        .into_iter()
        .map(|w| w.event_identity)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

async fn catalogue_walk(
    conn: &mut PgConnection,
    read: &AddressRead<'_>,
    filter: &EventHistoryReadFilter,
) -> Result<Vec<String>> {
    let mut cursor = None;
    let mut ids = Vec::new();
    loop {
        let keyset = cursor.as_ref().map(|cursor| HistoryKeyset {
            cursor,
            block_number: Some(1),
        });
        let mut query = QueryBuilder::<Postgres>::new("");
        catalogue_source::push_candidate_query(
            &mut query,
            read,
            filter,
            keyset.as_ref(),
            0,
            None,
            2,
        );
        let rows: Vec<Witness> = query.build_query_as().fetch_all(&mut *conn).await?;
        let Some(first) = rows.first() else { break };
        assert!(!ids.contains(&first.event_identity));
        ids.push(first.event_identity.clone());
        if rows.len() == 1 {
            break;
        };
        cursor = Some(first.cursor());
    }
    Ok(ids)
}

async fn install(conn: &mut PgConnection, target: usize, unrelated: usize) -> Result<()> {
    plan_tests::install(conn, target, unrelated).await?;
    install_catalogue(conn).await?;
    for (label, kind, after) in [
        (
            "approval-on",
            "AccountPermissionChanged",
            json!({"subject":OPERATOR,"relation_kind":"operator","approved":true,"scope":{"kind":"account","owner":OWNER}}),
        ),
        (
            "approval-off",
            "AccountPermissionChanged",
            json!({"subject":OPERATOR,"relation_kind":"operator","approved":false,"scope":{"kind":"account","owner":OWNER}}),
        ),
        (
            "self-approval",
            "AccountPermissionChanged",
            json!({"subject":OWNER,"relation_kind":"operator","approved":true,"scope":{"kind":"account","owner":OWNER}}),
        ),
        (
            "reverse",
            "ReverseChanged",
            json!({"address":OWNER,"source_event":"ReverseClaimed","coin_type":"60"}),
        ),
        (
            "only-approval",
            "AccountPermissionChanged",
            json!({"subject":DIRECT_ONLY,"relation_kind":"operator","approved":true,"scope":{"kind":"account","owner":DIRECT_ONLY}}),
        ),
        (
            "only-reverse",
            "ReverseChanged",
            json!({"address":DIRECT_ONLY,"source_event":"NameForAddrChanged","coin_type":"60"}),
        ),
        ("registry", "RegistryCreated", json!({"sender":OWNER})),
        ("parent", "ParentChanged", json!({"sender":OWNER})),
        ("upgrade", "Upgraded", json!({"sender":OWNER})),
    ] {
        sqlx::query("INSERT INTO normalized_events(event_identity,namespace,event_kind,source_family,manifest_version,chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state) VALUES($1,'ens',$2,'ens_v1_registry_l1',1,'ethereum-mainnet','block-1',1,'account-tx',500,0,'ens_v1_unwrapped_authority','canonical',$3)")
            .bind(label).bind(kind).bind(after).execute(&mut *conn).await?;
    }
    // These rows must not leak through an unrelated held name's anchor, even if malformed
    // retained evidence associates them with that name. Direct participants are independent.
    sqlx::raw_sql("UPDATE normalized_events SET logical_name_id=(SELECT logical_name_id FROM normalized_events WHERE event_identity='plan:grant:1'),resource_id=(SELECT resource_id FROM normalized_events WHERE event_identity='plan:grant:1') WHERE event_identity IN ('registry','parent','upgrade','approval-on','approval-off','self-approval','reverse','only-approval','only-reverse');ANALYZE normalized_events;")
        .execute(conn).await?;
    Ok(())
}
