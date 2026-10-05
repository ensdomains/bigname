//! Access-plan fixtures only. Product membership/lifecycle tests publish with Project through
//! the API suite; these hand-shaped rows test the actual reader SQL, not producer semantics.

use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{Postgres, QueryBuilder};

use super::{AddressRead, Witness, catalogue_count, catalogue_source, plan_tests};
use crate::history::{EventHistoryReadFilter, HistoryOrder, HistoryScope, keyset::HistoryKeyset};

const ADDRESS: &str = "0x0000000000000000000000000000000000000a11";

#[tokio::test]
async fn catalogue_prefixes_keep_complete_keys_and_seek_sparse_sources() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("catalogue_reader_sql")).await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        plan_tests::install(&mut connection, 32, 320).await?;
        install_catalogue(&mut connection).await?;
        let read = AddressRead {
            address: ADDRESS, namespace: Some("ens"), relations: Some(&[crate::AddressNameRelation::TokenHolder]),
            scope: HistoryScope::Both, canonical_only: true, published: None, catalogue: true,
        };
        let filter = EventHistoryReadFilter { order: HistoryOrder::Asc, ..Default::default() };
        let mut query = QueryBuilder::<Postgres>::new("");
        catalogue_source::push_candidate_query(&mut query, &read, &filter, None, 0, None, 2);
        let rows: Vec<Witness> = query.build_query_as().fetch_all(&mut *connection).await?;
        assert_eq!(rows.iter().map(|row| row.event_identity.as_str()).collect::<Vec<_>>(), vec!["plan:grant:1", "plan:pointer:1"]);
        let cursor = rows[1].cursor();
        let keyset = HistoryKeyset { cursor: &cursor, block_number: Some(1) };
        let mut query = QueryBuilder::<Postgres>::new("");
        catalogue_source::push_candidate_query(&mut query, &read, &filter, Some(&keyset), 0, None, 2);
        let rows: Vec<Witness> = query.build_query_as().fetch_all(&mut *connection).await?;
        assert_eq!(rows.iter().map(|row| (row.event_identity.as_str(), row.witness_kind)).collect::<Vec<_>>(), vec![("plan:record:1", 3), ("plan:grant:2", 0)]);
        assert_eq!(catalogue_count::prove_over_cap(&mut connection, &read, &filter, 2).await?, Some(3));
        let desc = EventHistoryReadFilter { order: HistoryOrder::Desc, ..Default::default() };
        let mut query = QueryBuilder::<Postgres>::new("");
        catalogue_source::push_candidate_query(&mut query, &read, &desc, None, 0, None, 1);
        let rows: Vec<Witness> = query.build_query_as().fetch_all(&mut *connection).await?;
        assert_eq!(rows[0].event_identity, "plan:record:32");

        check_link_sources(&mut connection, &read, &filter).await?;
        check_seek_shapes(&mut connection, &read, "equal", &[
            (HistoryOrder::Asc, None, Some(0)), (HistoryOrder::Asc, Some(0), None),
            (HistoryOrder::Desc, None, Some(0)), (HistoryOrder::Desc, Some(0), None),
        ]).await?;

        // A single name spans a million empty buckets. Its direct name source extends,
        // while its resource and resolver sources retain their short envelopes.
        sqlx::raw_sql("INSERT INTO chain_lineage(chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES('ethereum-mainnet','far',256000001,to_timestamp(256000001),'canonical');
          INSERT INTO normalized_events(event_identity,namespace,logical_name_id,event_kind,source_family,manifest_version,chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state)
          SELECT 'far-event','ens',logical_name_id,'RegistrationRenewed',source_family,manifest_version,chain_id,'far',256000001,'far-tx',0,0,derivation_kind,canonicality_state,'{}' FROM normalized_events WHERE event_identity='plan:grant:1';
          UPDATE project_history_source SET last_bucket=1000000,bucket_range=int8range(first_bucket,1000001) WHERE source_kind=0 AND source_key=(SELECT logical_name_id FROM normalized_events WHERE event_identity='far-event');
          UPDATE project_address_history_anchor SET last_bucket=1000000,bucket_range=int8range(first_bucket,1000001) WHERE anchor_kind=0 AND anchor_id=(SELECT logical_name_id FROM normalized_events WHERE event_identity='far-event');")
            .execute(&mut *connection).await?;
        let mut query = QueryBuilder::<Postgres>::new("");
        catalogue_source::push_seek_query(&mut query, &read, &filter, Some(0));
        let bucket: Option<i64> = query.build_query_scalar().fetch_one(&mut *connection).await?;
        assert_eq!(bucket, Some(1_000_000), "seek must jump over the empty range");
        let sparse = EventHistoryReadFilter { record_key: Some("missing".to_owned()), ..filter.clone() };
        let mut query = QueryBuilder::<Postgres>::new("");
        catalogue_source::push_seek_query(&mut query, &read, &sparse, Some(0));
        let bucket: Option<i64> = query.build_query_scalar().fetch_one(&mut *connection).await?;
        assert_eq!(bucket, None, "next-source probes must apply the actual event filter");
        check_seek_shapes(&mut connection, &read, "sparse", &[
            (HistoryOrder::Asc, None, Some(0)), (HistoryOrder::Asc, Some(0), Some(1_000_000)),
            (HistoryOrder::Desc, None, Some(1_000_000)), (HistoryOrder::Desc, Some(1_000_000), Some(0)),
        ]).await?;
        check_null_order(&mut connection, &read, &filter).await?;
        check_seek_shapes(&mut connection, &read, "null", &[
            (HistoryOrder::Asc, None, Some(-1)), (HistoryOrder::Asc, Some(-1), Some(0)),
            (HistoryOrder::Asc, Some(1_000_000), None), (HistoryOrder::Desc, None, Some(1_000_000)),
            (HistoryOrder::Desc, Some(0), Some(-1)), (HistoryOrder::Desc, Some(-1), None),
        ]).await?;
        check_handoff_membership(&mut connection, &read, &filter).await?;
        assert_eq!(catalogue_count::prove_over_cap(&mut connection, &read, &filter, 65).await?, Some(66));
        assert_eq!(catalogue_count::prove_over_cap(&mut connection, &read, &filter, 66).await?, None);
        Ok(())
    }.await;
    if result.is_ok()
        && let Ok(path) = std::env::var("BIGNAME_HISTORY_RETAINED_FIXTURE_RECEIPT")
    {
        let name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(database.pool())
            .await?;
        std::fs::write(
            path,
            serde_json::to_vec_pretty(&json!({
                "database":name,"chain_id":"ethereum-mainnet","after_block":0,"through_block":1,
                "default_link":"plan:default-link","default_record":"plan:default-record",
                "resolver":"0x0000000000000000000000000000000000000d11",
                "source":"isolated reader SQL access fixture; not a producer lifecycle proof"
            }))?,
        )?;
    } else {
        database.cleanup().await?;
    }
    result
}

pub(super) async fn install_catalogue(connection: &mut sqlx::PgConnection) -> Result<()> {
    sqlx::raw_sql("INSERT INTO project_history_source(chain_id,source_kind,source_key,first_bucket,last_bucket,bucket_range,event_mask,key_bloom)
      SELECT chain_id,kind,key,min(block_number/256),max(block_number/256),int8range(min(block_number/256),max(block_number/256)+1),9223372036854775807,~B'0'::bit(256)
      FROM (SELECT chain_id,0::smallint AS kind,logical_name_id AS key,block_number FROM normalized_events WHERE logical_name_id IS NOT NULL
        UNION ALL SELECT chain_id,1::smallint,resource_id::text,block_number FROM normalized_events WHERE resource_id IS NOT NULL
        UNION ALL SELECT chain_id,2::smallint,lower(after_state->>'node'),block_number FROM normalized_events WHERE event_kind IN ('RecordChanged','RecordVersionChanged') AND after_state->>'node' IS NOT NULL) sources
      GROUP BY chain_id,kind,key;
      INSERT INTO project_address_history_anchor(chain_id,address,namespace,anchor_kind,anchor_id,current_mask,historical_mask,first_bucket,last_bucket,bucket_range,event_mask,key_bloom)
      SELECT event.chain_id,lower(event.after_state->>'registrant'),'ens',source.source_kind,source.source_key,0,1,source.first_bucket,source.last_bucket,source.bucket_range,source.event_mask,source.key_bloom FROM normalized_events event
        JOIN project_history_source source ON source.chain_id=event.chain_id AND ((source.source_kind=0 AND source.source_key=event.logical_name_id) OR (source.source_kind=1 AND source.source_key=event.resource_id::text)) WHERE event.event_kind='RegistrationGranted';
      INSERT INTO project_history_source_edge(chain_id,resource_id,source_kind,source_key,pointer_event_identity,pointer_resolver,node,pointer_block_number,first_bucket,last_bucket,bucket_range,event_mask,key_bloom)
      SELECT event.chain_id,event.resource_id,2,source.source_key,event.event_identity,lower(event.after_state->>'resolver'),source.source_key,event.block_number,source.first_bucket,source.last_bucket,source.bucket_range,source.event_mask,source.key_bloom FROM normalized_events event JOIN project_history_source source ON source.chain_id=event.chain_id AND source.source_kind=2 AND source.source_key=lower(event.after_state->>'node') WHERE event.event_kind='ResolverChanged';
      ANALYZE;")
        .execute(connection).await?;
    Ok(())
}

async fn check_link_sources(
    connection: &mut sqlx::PgConnection,
    read: &AddressRead<'_>,
    filter: &EventHistoryReadFilter,
) -> Result<()> {
    sqlx::raw_sql("INSERT INTO project_history_source(chain_id,source_kind,source_key,resolver_address,first_bucket,last_bucket,bucket_range,event_mask,key_bloom)
      SELECT chain_id,3,after_state->>'resolver_record_id',lower(after_state->>'resolver'),min(block_number/256),max(block_number/256),int8range(min(block_number/256),max(block_number/256)+1),9223372036854775807,~B'0'::bit(256)
      FROM normalized_events WHERE event_kind='RecordChanged' AND after_state->>'storage_model'='resolver_record_id' GROUP BY chain_id,after_state->>'resolver_record_id',lower(after_state->>'resolver');
      INSERT INTO project_history_source_edge(chain_id,resource_id,source_kind,source_key,source_resolver,pointer_event_identity,link_event_identity,pointer_resolver,node,pointer_block_number,link_block_number,first_bucket,last_bucket,bucket_range,event_mask,key_bloom)
      SELECT pointer.chain_id,pointer.resource_id,3,source.source_key,source.resolver_address,pointer.event_identity,link.event_identity,source.resolver_address,lower(pointer.after_state->>'node'),pointer.block_number,link.block_number,source.first_bucket,source.last_bucket,source.bucket_range,source.event_mask,source.key_bloom
      FROM normalized_events pointer JOIN normalized_events link ON link.chain_id=pointer.chain_id AND link.event_kind='ResolverRecordLinked' AND lower(link.after_state->>'resolver')=lower(pointer.after_state->>'resolver') AND lower(link.after_state->>'node') IN (lower(pointer.after_state->>'node'),'0x0000000000000000000000000000000000000000000000000000000000000000')
      JOIN project_history_source source ON source.chain_id=link.chain_id AND source.source_kind=3 AND source.source_key=link.after_state->>'resolver_record_id' AND source.resolver_address=lower(link.after_state->>'resolver') WHERE pointer.event_kind='ResolverChanged'; ANALYZE;")
        .execute(&mut *connection).await?;
    for identity in ["plan:default-link", "plan:default-record"] {
        let mut query = QueryBuilder::<Postgres>::new("");
        catalogue_source::push_candidate_query(
            &mut query,
            read,
            filter,
            None,
            0,
            Some(identity),
            1,
        );
        let rows: Vec<Witness> = query.build_query_as().fetch_all(&mut *connection).await?;
        assert_eq!(
            rows.len(),
            32,
            "one selected event retains every resource witness"
        );
        assert!(
            rows.iter()
                .all(|row| row.event_identity == identity && row.witness_kind == 3)
        );
        let mut query = QueryBuilder::<Postgres>::new("");
        catalogue_source::push_candidate_query(
            &mut query,
            read,
            filter,
            None,
            0,
            Some(identity),
            1,
        );
        let compared =
            compare_layout_query(connection, &format!("{identity}-witnesses"), query).await?;
        assert_eq!(compared.as_array().unwrap().len(), 32);
    }
    Ok(())
}

async fn check_seek_shapes(
    connection: &mut sqlx::PgConnection,
    read: &AddressRead<'_>,
    stage: &str,
    cases: &[(HistoryOrder, Option<i64>, Option<i64>)],
) -> Result<()> {
    for namespace in [Some("ens"), None] {
        let scoped = AddressRead {
            address: read.address,
            namespace,
            relations: read.relations,
            scope: read.scope,
            canonical_only: read.canonical_only,
            published: read.published,
            catalogue: read.catalogue,
        };
        for &(order, after, expected) in cases {
            let filter = EventHistoryReadFilter {
                order,
                ..Default::default()
            };
            let mut query = QueryBuilder::<Postgres>::new("");
            catalogue_source::push_seek_query(&mut query, &scoped, &filter, after);
            let label = format!(
                "{stage}-{}-{}-{after:?}",
                namespace.unwrap_or("all"),
                order.as_str()
            );
            let result = compare_layout_query(connection, &label, query).await?;
            let field = if order == HistoryOrder::Asc {
                "min"
            } else {
                "max"
            };
            assert_eq!(result[0][field], json!(expected), "{label}");
        }
    }
    Ok(())
}

async fn check_handoff_membership(
    connection: &mut sqlx::PgConnection,
    read: &AddressRead<'_>,
    filter: &EventHistoryReadFilter,
) -> Result<()> {
    sqlx::raw_sql("INSERT INTO normalized_events(event_identity,namespace,logical_name_id,resource_id,event_kind,source_family,manifest_version,chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state)
      SELECT 'layout-handoff:ResolverChanged:registry-fallback-handoff:'||lpad(block_number::text,4,'0'),'ens',logical_name_id,resource_id,'ResolverChanged','ens_v1_registry_l1',1,'ethereum-mainnet','block-1',1,'layout-handoff',0,10,'ens_v1_unwrapped_authority','canonical',jsonb_build_object('node','layout-node','resolver','0x0000000000000000000000000000000000000d11')
      FROM normalized_events WHERE event_kind='RegistrationGranted' AND block_number<=32;
      INSERT INTO normalized_events(event_identity,namespace,event_kind,source_family,manifest_version,chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state)
      VALUES('layout-handoff:ResolverChanged:registry-fallback-handoff:0000-outside','ens','ResolverChanged','ens_v1_registry_l1',1,'ethereum-mainnet','block-1',1,'layout-handoff',0,10,'ens_v1_unwrapped_authority','canonical',jsonb_build_object('node','layout-node','resolver','0x0000000000000000000000000000000000000d11')); ANALYZE;")
        .execute(&mut *connection).await?;
    let groups = json!([{"chain":"ethereum-mainnet","block":1,"hash":"block-1",
        "node":"layout-node","origin":"layout-handoff"}]);
    for scope in [
        HistoryScope::Both,
        HistoryScope::Surface,
        HistoryScope::Resource,
    ] {
        let scoped = AddressRead {
            address: read.address,
            namespace: read.namespace,
            relations: read.relations,
            scope,
            canonical_only: read.canonical_only,
            published: read.published,
            catalogue: read.catalogue,
        };
        let mut query = QueryBuilder::<Postgres>::new("");
        catalogue_source::push_handoff_query(&mut query, &scoped, filter, &groups);
        let peers =
            compare_layout_query(connection, &format!("handoff-{}", scope.as_str()), query).await?;
        assert_eq!(peers.as_array().unwrap().len(), 32);
        assert_eq!(
            peers[0]["event_identity"],
            "layout-handoff:ResolverChanged:registry-fallback-handoff:0001"
        );
    }
    let relations = [crate::AddressNameRelation::RoleHolder];
    let scoped = AddressRead {
        address: read.address,
        namespace: read.namespace,
        relations: Some(&relations),
        scope: read.scope,
        canonical_only: read.canonical_only,
        published: read.published,
        catalogue: read.catalogue,
    };
    let mut query = QueryBuilder::<Postgres>::new("");
    catalogue_source::push_handoff_query(&mut query, &scoped, filter, &groups);
    let peers = compare_layout_query(connection, "handoff-unselected-relation", query).await?;
    assert_eq!(peers, json!([]));
    Ok(())
}

async fn compare_layout_query(
    connection: &mut sqlx::PgConnection,
    label: &str,
    query: QueryBuilder<'_, Postgres>,
) -> Result<Value> {
    let directory = std::env::var("BIGNAME_ADDRESS_HISTORY_PLAN_DIR")
        .ok()
        .map(std::path::PathBuf::from);
    super::catalogue_layout_tests::compare_query(
        connection,
        None,
        directory.as_deref(),
        label,
        query,
        false,
    )
    .await
}

async fn check_null_order(
    connection: &mut sqlx::PgConnection,
    read: &AddressRead<'_>,
    filter: &EventHistoryReadFilter,
) -> Result<()> {
    sqlx::raw_sql("      INSERT INTO normalized_events(event_identity,namespace,logical_name_id,event_kind,source_family,manifest_version,chain_id,derivation_kind,canonicality_state)
      SELECT 'unpositioned','ens',logical_name_id,'RegistrationRenewed','ens_v1_registrar_l1',1,'ethereum-mainnet','ens_v1_unwrapped_authority','canonical' FROM normalized_events WHERE event_identity='plan:grant:1';
      UPDATE project_history_source SET first_bucket=-1,bucket_range=int8range(-1,last_bucket+1) WHERE chain_id='ethereum-mainnet' AND source_kind=0 AND source_key=(SELECT logical_name_id FROM normalized_events WHERE event_identity='unpositioned');
      UPDATE project_address_history_anchor SET first_bucket=-1,bucket_range=int8range(-1,last_bucket+1) WHERE anchor_kind=0 AND anchor_id=(SELECT logical_name_id FROM normalized_events WHERE event_identity='unpositioned');")
        .execute(&mut *connection).await?;
    let mut query = QueryBuilder::<Postgres>::new("");
    catalogue_source::push_seek_query(&mut query, read, filter, None);
    let first: Option<i64> = query
        .build_query_scalar()
        .fetch_one(&mut *connection)
        .await?;
    assert_eq!(first, Some(-1));
    let mut query = QueryBuilder::<Postgres>::new("");
    catalogue_source::push_candidate_query(&mut query, read, filter, None, -1, None, 1);
    let rows: Vec<Witness> = query.build_query_as().fetch_all(&mut *connection).await?;
    assert_eq!(rows[0].event_identity, "unpositioned");
    Ok(())
}
