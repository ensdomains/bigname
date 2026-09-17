use super::{ENS_GRACE_PERIOD_SECS, due_names, events, events_with_byte_limit};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, types::Uuid};
use time::OffsetDateTime;

type Result<T = ()> = anyhow::Result<T>;
const CHAIN: &str = "lookahead-test";

async fn database() -> Result<TestDatabase> {
    let db = TestDatabase::create(TestDatabaseConfig::new("v1_lookahead_query")).await?;
    for sql in [
        include_str!("../../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../../schema-v2/baseline/05_normalized_events.sql"),
    ] {
        sqlx::raw_sql(sql).execute(db.pool()).await?;
    }
    sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) SELECT $1,'block-'||n,n,to_timestamp(n),'canonical' FROM generate_series(1,8) n")
        .bind(CHAIN).execute(db.pool()).await?;
    // The explicit experimental DDL must install on the actual baseline. Concurrent
    // creation is an operator concern; fixtures install the same expressions normally.
    let indexes = include_str!("../../../../ops/experimental/v1-lookahead-indexes.sql")
        .replace("CONCURRENTLY ", "")
        .replace("bigname_phase.", "public.");
    sqlx::raw_sql(&indexes).execute(db.pool()).await?;
    Ok(db)
}

#[allow(clippy::too_many_arguments)]
async fn seed(
    pool: &PgPool,
    identity: &str,
    name: Option<&str>,
    resource: Option<Uuid>,
    block: i64,
    position: Option<i64>,
    key: Value,
    mut after: Value,
) -> Result {
    if let Some(name) = name {
        sqlx::query("INSERT INTO name_surfaces (logical_name_id,namespace,raw_name,raw_labels,dns_encoded_name,namehash,labelhashes,normalizer_version,visibility_state,chain_id,block_hash,block_number) VALUES ($1,'ens',$1,'{}','',split_part($1,':',2),'{}','test','active',$2,'block-1',1) ON CONFLICT DO NOTHING")
            .bind(name).bind(CHAIN).execute(pool).await?;
    }
    if let Some(resource) = resource {
        sqlx::query("INSERT INTO resources (resource_id,chain_id,block_hash,block_number) VALUES ($1,$2,'block-1',1) ON CONFLICT DO NOTHING")
            .bind(resource).bind(CHAIN).execute(pool).await?;
    }
    after["fixture_identity"] = json!(identity);
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,logical_name_id,resource_id,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,raw_fact_ref,derivation_kind,canonicality_state,after_state) VALUES ($1,'ens',$2,$3,'RegistrationGranted','ens_v1_registrar_l1',1,$4,$5,'block-'||$5::text,'tx',$6,$6,$7,'ens_v1_unwrapped_authority','canonical',$8)")
        .bind(identity).bind(name).bind(resource).bind(CHAIN).bind(block).bind(position)
        .bind(key).bind(after).execute(pool).await?;
    Ok(())
}

fn key(value: &str) -> Value {
    json!({"interpreter_state_key":value})
}

fn identities(events: &[bigname_adapters::schema_v2::PriorEventInput]) -> Vec<&str> {
    events
        .iter()
        .map(|event| event.after_state["fixture_identity"].as_str().unwrap())
        .collect()
}

#[tokio::test]
async fn global_winners_preserve_partitions_positions_and_canonical_hashes() -> Result {
    let db = database().await?;
    for (id, block, position, state_key, after) in [
        ("fallback", 1, None, json!({}), json!({})),
        ("explicit-fallback", 1, None, key("fallback"), json!({})),
        (
            "null-key",
            1,
            None,
            json!({"interpreter_state_key":null}),
            json!({}),
        ),
        ("empty-old", 1, None, key(""), json!({})),
        ("empty-new", 2, None, key(""), json!({})),
        ("ordinary-old", 1, None, key("shared"), json!({})),
        ("ordinary-block", 2, None, key("shared"), json!({})),
        ("ordinary-log", 2, Some(0), key("shared"), json!({})),
        ("ordinary-id", 2, Some(0), key("shared"), json!({})),
        (
            "clear-old",
            1,
            None,
            key("shared"),
            json!({"subregistry_invalidated_token_ids":[]}),
        ),
        (
            "clear-new",
            2,
            None,
            key("shared"),
            json!({"subregistry_invalidated_token_ids":[]}),
        ),
        ("move-old", 1, None, key("moved"), json!({})),
        ("canonical", 2, None, key("canonical"), json!({})),
        ("event-orphan", 3, None, key("canonical"), json!({})),
        ("lineage-orphan", 4, None, key("canonical"), json!({})),
        ("cutoff", 5, None, key("canonical"), json!({})),
    ] {
        seed(
            db.pool(),
            id,
            Some("ens:a"),
            None,
            block,
            position,
            state_key,
            after,
        )
        .await?;
    }
    seed(
        db.pool(),
        "moved-latest",
        Some("ens:b"),
        None,
        3,
        None,
        key("moved"),
        json!({}),
    )
    .await?;
    sqlx::query("UPDATE normalized_events SET canonicality_state='orphaned' WHERE event_identity='event-orphan'")
        .execute(db.pool()).await?;
    sqlx::query("UPDATE chain_lineage SET canonicality_state='orphaned' WHERE chain_id=$1 AND block_hash='block-4'")
        .bind(CHAIN).execute(db.pool()).await?;
    // Same height, different hash must not make the orphaned event readable.
    sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,'replacement-4',4,to_timestamp(4),'safe')")
        .bind(CHAIN).execute(db.pool()).await?;
    let mut connection = db.pool().acquire().await?;
    let selected = events(&mut connection, CHAIN, 5, &["ens:a".into()], &[], 100).await?;
    assert_eq!(
        identities(&selected),
        [
            "fallback",
            "explicit-fallback",
            "null-key",
            "empty-new",
            "ordinary-id",
            "clear-new",
            "canonical",
            "moved-latest"
        ]
    );
    assert_ne!(
        selected[0].retained_state_key,
        selected[1].retained_state_key
    );
    assert_ne!(
        selected[4].retained_state_key,
        selected[5].retained_state_key
    );
    assert_eq!(
        selected.last().unwrap().logical_name_id.as_deref(),
        Some("ens:b")
    );
    assert_eq!(
        selected
            .last()
            .unwrap()
            .block_timestamp
            .unwrap()
            .unix_timestamp(),
        3
    );
    drop(connection);
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn unnamed_direct_facts_route_by_child_without_parent_descendant_expansion() -> Result {
    let db = database().await?;
    let resource = Uuid::from_u128(1);
    for (id, name, res, after) in [
        (
            "parent",
            Some("ens:parent"),
            Some(resource),
            json!({"node":"parent"}),
        ),
        (
            "named-child",
            Some("ens:parent"),
            Some(resource),
            json!({"node":"parent","child_node":"child"}),
        ),
        (
            "unnamed-child",
            None,
            None,
            json!({"node":"parent","namehash":"parent","child_node":"CHILD"}),
        ),
        ("unnamed-node", None, None, json!({"node":"child"})),
        ("unnamed-namehash", None, None, json!({"namehash":"child"})),
        (
            "delegate",
            None,
            None,
            json!({"grant_source":{"node":"child"}}),
        ),
        (
            "revocation",
            None,
            None,
            json!({"revocation_source":{"node":"child"}}),
        ),
        ("resource-only", None, Some(resource), json!({})),
        (
            "sibling",
            Some("ens:parent"),
            Some(resource),
            json!({"node":"parent","child_node":"sibling"}),
        ),
    ] {
        seed(db.pool(), id, name, res, 1, None, key(id), after).await?;
    }
    let mut connection = db.pool().acquire().await?;
    let parent = events(
        &mut connection,
        CHAIN,
        2,
        &["ens:parent".into()],
        &[resource],
        100,
    )
    .await?;
    assert_eq!(identities(&parent), ["parent", "resource-only"]);
    let child = events(&mut connection, CHAIN, 2, &["ens:child".into()], &[], 100).await?;
    assert_eq!(
        identities(&child),
        [
            "named-child",
            "unnamed-child",
            "unnamed-node",
            "unnamed-namehash",
            "delegate",
            "revocation"
        ]
    );
    let resource_only = events(&mut connection, CHAIN, 2, &[], &[resource], 100).await?;
    assert_eq!(identities(&resource_only), ["resource-only"]);
    drop(connection);
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn expiry_candidates_match_strict_grace_boundary_and_total_i64_parsing() -> Result {
    let db = database().await?;
    for (id, expiry) in [
        ("past", json!(99)),
        ("predecessor", json!(100)),
        ("between", json!(150)),
        ("last", json!(200)),
        ("later", json!(201)),
        ("zeros", json!("+0000000000000000150")),
        ("bad", json!("1e2")),
        ("fraction", json!(150.5)),
        ("bool", json!(true)),
        ("huge", json!("999999999999999999999999999999999")),
        ("overflow", json!(i64::MAX)),
        ("underflow", json!("-9223372036854775809")),
        ("minimum", json!(i64::MIN)),
    ] {
        seed(
            db.pool(),
            id,
            None,
            None,
            1,
            None,
            key(id),
            json!({"namehash":id,"expiry":expiry}),
        )
        .await?;
    }
    seed(
        db.pool(),
        "future-block",
        None,
        None,
        3,
        None,
        key("future-block"),
        json!({"namehash":"future-block","expiry":150}),
    )
    .await?;
    seed(
        db.pool(),
        "orphan",
        None,
        None,
        2,
        None,
        key("orphan"),
        json!({"namehash":"orphan","expiry":150}),
    )
    .await?;
    sqlx::query("UPDATE chain_lineage SET canonicality_state='orphaned' WHERE chain_id=$1 AND block_number=2")
        .bind(CHAIN).execute(db.pool()).await?;
    let predecessor = OffsetDateTime::from_unix_timestamp(ENS_GRACE_PERIOD_SECS + 100)?;
    let last = OffsetDateTime::from_unix_timestamp(ENS_GRACE_PERIOD_SECS + 200)?;
    let mut connection = db.pool().acquire().await?;
    assert_eq!(
        due_names(&mut connection, CHAIN, 3, Some(predecessor), last, 100).await?,
        ["ens:between", "ens:predecessor", "ens:zeros"]
    );
    assert_eq!(
        due_names(&mut connection, CHAIN, 3, None, last, 100).await?,
        [
            "ens:between",
            "ens:minimum",
            "ens:past",
            "ens:predecessor",
            "ens:zeros"
        ]
    );
    assert!(
        due_names(&mut connection, CHAIN, 3, Some(last), last, 100)
            .await?
            .is_empty()
    );
    let error = due_names(&mut connection, CHAIN, 3, None, last, 2)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("refusing incomplete prior state")
    );
    drop(connection);
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn row_and_byte_limits_fail_instead_of_returning_partial_state() -> Result {
    let db = database().await?;
    for id in ["one", "two"] {
        seed(
            db.pool(),
            id,
            None,
            None,
            1,
            None,
            key(id),
            json!({"node":"a","value":"x".repeat(1024)}),
        )
        .await?;
    }
    let mut connection = db.pool().acquire().await?;
    let names = ["ens:a".into()];
    assert_eq!(
        events(&mut connection, CHAIN, 2, &names, &[], 2)
            .await?
            .len(),
        2
    );
    for limit in [0, 1] {
        let error = events(&mut connection, CHAIN, 2, &names, &[], limit)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("event count"));
    }
    let error = events_with_byte_limit(&mut connection, CHAIN, 2, &names, &[], 2, 100)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("event bytes"));
    // Each body fits independently; their combined retained size exceeds this bound.
    let error = events_with_byte_limit(&mut connection, CHAIN, 2, &names, &[], 2, 2000)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("event bytes"));
    assert!(
        events(&mut connection, CHAIN, 2, &[], &[], 0)
            .await?
            .is_empty()
    );
    drop(connection);
    db.cleanup().await?;
    Ok(())
}
