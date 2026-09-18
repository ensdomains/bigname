use super::{ENS_GRACE_PERIOD_SECS, due_names, events};
use bigname_adapters::schema_v2::seam::{
    INTERPRETER_STATE_KEY, SUBREGISTRY_INVALIDATED_TOKEN_IDS_KEY,
};
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
    json!({(INTERPRETER_STATE_KEY):value})
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
            json!({(INTERPRETER_STATE_KEY):null}),
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
            json!({(SUBREGISTRY_INVALIDATED_TOKEN_IDS_KEY):[]}),
        ),
        (
            "clear-new",
            2,
            None,
            key("shared"),
            json!({(SUBREGISTRY_INVALIDATED_TOKEN_IDS_KEY):[]}),
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
    let selected = events(&mut connection, CHAIN, 5, &["ens:a".into()], &[]).await?;
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
    )
    .await?;
    assert_eq!(identities(&parent), ["parent", "resource-only"]);
    let child = events(&mut connection, CHAIN, 2, &["ens:child".into()], &[]).await?;
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
    let resource_only = events(&mut connection, CHAIN, 2, &[], &[resource]).await?;
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
    sqlx::query("SET plan_cache_mode = force_generic_plan")
        .execute(&mut *connection)
        .await?;
    assert_eq!(
        due_names(&mut connection, CHAIN, 3, Some(predecessor), last).await?,
        ["ens:between", "ens:predecessor", "ens:zeros"]
    );
    assert_eq!(
        due_names(&mut connection, CHAIN, 3, None, last).await?,
        [
            "ens:between",
            "ens:minimum",
            "ens:past",
            "ens:predecessor",
            "ens:zeros"
        ]
    );
    assert!(
        due_names(&mut connection, CHAIN, 3, Some(last), last)
            .await?
            .is_empty()
    );
    drop(connection);
    db.cleanup().await?;
    Ok(())
}

/// The loader once stopped at 100,000 rows and 64 MiB. Removing those stops must not leave a
/// silent truncation behind: a working set larger than the old row limit comes back whole.
#[tokio::test]
async fn working_sets_larger_than_the_removed_limits_are_returned_whole() -> Result {
    const NAMES: i64 = 100_500;
    let db = database().await?;
    sqlx::query(
        "INSERT INTO normalized_events
         (event_identity,namespace,event_kind,source_family,manifest_version,chain_id,
          block_number,block_hash,transaction_hash,raw_fact_ref,derivation_kind,
          canonicality_state,after_state)
         SELECT 'mass-expiry-'||n,'ens','RegistrationGranted','ens_v1_registrar_l1',1,$1,
                1,'block-1','tx',jsonb_build_object($2::text,'key-'||n),
                'ens_v1_unwrapped_authority','canonical',
                jsonb_build_object('namehash','node-'||n,'expiry',1000)
         FROM generate_series(1,$3) n",
    )
    .bind(CHAIN)
    .bind(INTERPRETER_STATE_KEY)
    .bind(NAMES)
    .execute(db.pool())
    .await?;
    let mut connection = db.pool().acquire().await?;
    // Fresh statistics, as autovacuum keeps them in production; without them the planner
    // chooses a plan for this many names that does not finish in minutes.
    sqlx::raw_sql("ANALYZE normalized_events")
        .execute(&mut *connection)
        .await?;
    // Every one of these registrations falls due at the same timestamp.
    let last = OffsetDateTime::from_unix_timestamp(ENS_GRACE_PERIOD_SECS + 1001)?;
    let names = due_names(&mut connection, CHAIN, 2, None, last).await?;
    assert_eq!(names.len(), usize::try_from(NAMES)?);
    let loaded = events(&mut connection, CHAIN, 2, &names, &[]).await?;
    assert_eq!(loaded.len(), usize::try_from(NAMES)?);
    assert!(
        events(&mut connection, CHAIN, 2, &[], &[])
            .await?
            .is_empty()
    );
    for sql in [super::EVENTS, super::DUE_NAMES] {
        let limits: Vec<_> = sql
            .lines()
            .map(|line| line.split("--").next().unwrap_or("").trim())
            .filter(|line| line.contains("LIMIT") && !line.ends_with("LIMIT 1"))
            .collect();
        assert!(
            limits.is_empty(),
            "only LIMIT 1 probes may remain: {limits:?}"
        );
    }
    drop(connection);
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn expiry_generic_plan_uses_both_timestamp_index_bounds() -> Result {
    let db = database().await?;
    sqlx::query(
        "INSERT INTO normalized_events
         (event_identity,namespace,event_kind,source_family,manifest_version,chain_id,
          block_number,block_hash,transaction_hash,raw_fact_ref,derivation_kind,
          canonicality_state,after_state)
         SELECT 'expiry-plan-'||n,'ens','RegistrationGranted','ens_v1_registrar_l1',1,$1,
                1,'block-1','tx',jsonb_build_object($2::text,n::text),
                'ens_v1_unwrapped_authority','canonical',
                jsonb_build_object('namehash','node-'||n,'expiry',n)
         FROM generate_series(1,2000) n",
    )
    .bind(CHAIN)
    .bind(INTERPRETER_STATE_KEY)
    .execute(db.pool())
    .await?;
    let mut connection = db.pool().acquire().await?;
    sqlx::raw_sql("ANALYZE normalized_events; SET plan_cache_mode=force_generic_plan;")
        .execute(&mut *connection)
        .await?;
    sqlx::raw_sql(&format!(
        "PREPARE expiry_plan(text,bigint,bigint,bigint,bigint) AS {}",
        super::DUE_NAMES
    ))
    .execute(&mut *connection)
    .await?;
    let plan: Value = sqlx::query_scalar(&format!(
        "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) EXECUTE expiry_plan(
         '{CHAIN}',3,{}, {},{ENS_GRACE_PERIOD_SECS})",
        ENS_GRACE_PERIOD_SECS + 1900,
        ENS_GRACE_PERIOD_SECS + 1910
    ))
    .fetch_one(&mut *connection)
    .await?;
    let mut pending = vec![&plan[0]["Plan"]];
    let mut bounded = false;
    while let Some(node) = pending.pop() {
        if node["Index Name"] == "normalized_events_v1_due_probe_idx" {
            let condition = node["Index Cond"].as_str().unwrap_or("");
            bounded |= condition.contains("$3") && condition.contains("$4");
        }
        if let Some(children) = node["Plans"].as_array() {
            pending.extend(children);
        }
    }
    assert!(
        bounded,
        "generic expiry plan must index both timestamp bounds: {plan}"
    );
    let names = due_names(
        &mut connection,
        CHAIN,
        3,
        Some(OffsetDateTime::from_unix_timestamp(
            ENS_GRACE_PERIOD_SECS + 1900,
        )?),
        OffsetDateTime::from_unix_timestamp(ENS_GRACE_PERIOD_SECS + 1910)?,
    )
    .await?;
    assert_eq!(
        names,
        (1900..1910)
            .map(|n| format!("ens:node-{n}"))
            .collect::<Vec<_>>()
    );
    drop(connection);
    db.cleanup().await?;
    Ok(())
}

fn index_names(plan: &Value, names: &mut Vec<String>) {
    if let Some(name) = plan["Index Name"].as_str() {
        names.push(name.to_owned());
    }
    for child in plan["Plans"].as_array().into_iter().flatten() {
        index_names(child, names);
    }
}

/// The fixture database holds only the checked-in baseline schema, so this proves the
/// baseline defines indexes whose expressions the two lookahead queries can use. A drifted
/// expression in either place would fall back to scanning `normalized_events`.
#[tokio::test]
async fn lookahead_sql_uses_baseline_indexes() -> Result {
    let db = database().await?;
    sqlx::query(
        "INSERT INTO normalized_events
         (event_identity,namespace,event_kind,source_family,manifest_version,chain_id,
          block_number,block_hash,transaction_hash,raw_fact_ref,derivation_kind,
          canonicality_state,after_state)
         SELECT 'plan-'||n,'ens','RegistrationGranted','ens_v1_registrar_l1',1,$1,
                1,'block-1','tx',jsonb_build_object($2::text,'key-'||n),
                'ens_v1_unwrapped_authority','canonical',
                jsonb_build_object('namehash','node-'||n,'expiry',n)
         FROM generate_series(1,5000) n",
    )
    .bind(CHAIN)
    .bind(INTERPRETER_STATE_KEY)
    .execute(db.pool())
    .await?;
    let mut connection = db.pool().acquire().await?;
    sqlx::raw_sql("ANALYZE normalized_events; ANALYZE chain_lineage;")
        .execute(&mut *connection)
        .await?;
    let events_sql = super::EVENTS
        .replace("{state_key}", INTERPRETER_STATE_KEY)
        .replace("{state_scope}", super::STATE_SCOPE_KEY)
        .replace("{clear_marker}", SUBREGISTRY_INVALIDATED_TOKEN_IDS_KEY);
    for (statement, signature, arguments, index) in [
        (
            events_sql.as_str(),
            "text,bigint,text[],uuid[]",
            format!("'{CHAIN}',3,ARRAY['ens:node-7','ens:node-4000'],ARRAY[]::uuid[]"),
            "normalized_events_v1_direct_node_probe_idx",
        ),
        (
            super::DUE_NAMES,
            "text,bigint,bigint,bigint,bigint",
            format!(
                "'{CHAIN}',3,{},{},{ENS_GRACE_PERIOD_SECS}",
                ENS_GRACE_PERIOD_SECS + 100,
                ENS_GRACE_PERIOD_SECS + 110
            ),
            "normalized_events_v1_due_probe_idx",
        ),
    ] {
        // Both plan kinds matter: sqlx prepares the statement, and PostgreSQL may switch a
        // prepared statement to its generic plan.
        for mode in ["force_custom_plan", "force_generic_plan"] {
            let prepared = format!("{index}_{mode}");
            sqlx::raw_sql(&format!(
                "SET plan_cache_mode={mode}; PREPARE {prepared}({signature}) AS {statement}"
            ))
            .execute(&mut *connection)
            .await?;
            let plan: Value = sqlx::query_scalar(&format!(
                "EXPLAIN (FORMAT JSON) EXECUTE {prepared}({arguments})"
            ))
            .fetch_one(&mut *connection)
            .await?;
            let mut used = Vec::new();
            index_names(&plan[0]["Plan"], &mut used);
            assert!(
                used.iter().any(|name| name == index),
                "{mode} plan must use {index}, used {used:?}"
            );
        }
    }
    drop(connection);
    db.cleanup().await?;
    Ok(())
}

/// The index files each event under one name, so the query must find an event under that
/// name and no other. The order of fields is the adapter's `V1_EVENT_NODE_FIELDS`.
#[tokio::test]
async fn events_sql_files_an_event_under_the_same_name_as_restore() -> Result {
    use bigname_adapters::schema_v2::seam::V1_EVENT_NODE_FIELDS;
    let coalesce = V1_EVENT_NODE_FIELDS
        .map(|field| format!("after_state ->> '{field}'"))
        .join(", ");
    for (what, sql) in [
        ("events.sql", super::EVENTS.replace("event.", "")),
        ("due_names.sql", super::DUE_NAMES.replace("event.", "")),
        (
            "the baseline index",
            include_str!("../../../../schema-v2/baseline/05_normalized_events.sql").to_owned(),
        ),
    ] {
        assert!(
            sql.contains(&format!("COALESCE({coalesce}")),
            "{what} must read the name fields in the order {V1_EVENT_NODE_FIELDS:?}"
        );
    }
    let db = database().await?;
    seed(
        db.pool(),
        "both-fields",
        None,
        None,
        1,
        None,
        key("both-fields"),
        json!({"node":"by-node","namehash":"by-namehash"}),
    )
    .await?;
    let mut connection = db.pool().acquire().await?;
    let by_namehash = events(&mut connection, CHAIN, 2, &["ens:by-namehash".into()], &[]).await?;
    assert_eq!(identities(&by_namehash), ["both-fields"]);
    assert!(
        events(&mut connection, CHAIN, 2, &["ens:by-node".into()], &[])
            .await?
            .is_empty()
    );
    drop(connection);
    db.cleanup().await?;
    Ok(())
}
