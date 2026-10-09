use super::{ENS_GRACE_PERIOD_SECS, due_names, events};
use bigname_adapters::schema_v2::seam::{
    INTERPRETER_STATE_KEY, SUBREGISTRY_INVALIDATED_TOKEN_IDS_KEY,
};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgConnection, PgPool, types::Uuid};
use time::OffsetDateTime;

type Result<T = ()> = anyhow::Result<T>;
const CHAIN: &str = "lookahead-test";

async fn database() -> Result<TestDatabase> {
    let db = TestDatabase::create(TestDatabaseConfig::new("v1_lookahead_query")).await?;
    for sql in [
        include_str!("../../../storage/schema/baseline/01_chain.sql"),
        include_str!("../../../storage/schema/baseline/03_identity.sql"),
        include_str!("../../../storage/schema/baseline/04_manifests.sql"),
        include_str!("../../../storage/schema/baseline/05_normalized_events.sql"),
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

/// In each candidate arm (names, resources, ENSv2 state keys), a key seen only on an
/// orphaned block restores nothing, and an orphaned latest event falls back to the older
/// readable one.
#[tokio::test]
async fn keys_seen_only_on_orphaned_blocks_restore_nothing() -> Result {
    let db = database().await?;
    let resource = Uuid::from_u128(1);
    let token = format!("0x{:064x}", 1u128 << 32);
    for arm in ["name", "res", "v2"] {
        for (event, block, state_key) in [
            ("older", 1, "older"),
            ("readable", 2, "readable"),
            ("orphaned-latest", 4, "older"),
            ("orphaned-only", 4, "orphaned-only"),
        ] {
            let (id, state_key) = (format!("{arm}-{event}"), format!("{arm}-{state_key}"));
            if arm == "v2" {
                sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,transaction_hash,raw_fact_ref,derivation_kind,canonicality_state,after_state) VALUES ($1,'ens','RegistrationGranted','ens_v2_registry_l1',1,$2,$3,'block-'||$3::text,'tx',jsonb_build_object($4::text,$5::text,$6::text,'0xreg:-:'||$7||':-:LabelRegistered'),'ens_v1_unwrapped_authority','canonical',jsonb_build_object('fixture_identity',$1))")
                    .bind(&id).bind(CHAIN).bind(block).bind(INTERPRETER_STATE_KEY).bind(&state_key)
                    .bind(super::STATE_SCOPE_KEY).bind(&token).execute(db.pool()).await?;
                continue;
            }
            let (name, res) = if arm == "name" {
                (Some("ens:a"), None)
            } else {
                (None, Some(resource))
            };
            seed(
                db.pool(),
                &id,
                name,
                res,
                block,
                None,
                key(&state_key),
                json!({}),
            )
            .await?;
        }
    }
    sqlx::query("UPDATE chain_lineage SET canonicality_state='orphaned' WHERE chain_id=$1 AND block_hash='block-4'")
        .bind(CHAIN).execute(db.pool()).await?;
    sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,'replacement-4',4,to_timestamp(4),'safe')")
        .bind(CHAIN).execute(db.pool()).await?;
    let mut connection = db.pool().acquire().await?;
    let selected = super::ordered_events(
        &mut connection,
        CHAIN,
        5,
        &["ens:a".into()],
        &[resource],
        &[format!("0xreg:{token}")],
    )
    .await?
    .into_iter()
    .map(|ordered| ordered.event)
    .collect::<Vec<_>>();
    assert_eq!(
        identities(&selected),
        [
            "name-older",
            "res-older",
            "v2-older",
            "name-readable",
            "res-readable",
            "v2-readable"
        ]
    );
    drop(connection);
    db.cleanup().await?;
    Ok(())
}

/// A registry may emit `LabelRegistered` for one token under a second label. The adapter keys
/// the token's state by (registry, token) with the token, not the labelhash, in the state scope,
/// while `v2_keys.sql` also files each event under its labelhash. Requesting only the orphaned
/// label's ENSv2 state key must not reach the older event.
#[tokio::test]
async fn an_ensv2_key_seen_only_under_an_orphaned_alias_restores_nothing() -> Result {
    let db = database().await?;
    let token = format!("0x{:064x}", 1u128 << 32);
    let (alpha, beta) = (
        format!("0x{}", "a".repeat(64)),
        format!("0x{}", "b".repeat(64)),
    );
    for (id, block, labelhash) in [("alpha", 1, &alpha), ("beta", 4, &beta)] {
        sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,transaction_hash,raw_fact_ref,derivation_kind,canonicality_state,after_state) VALUES ($1,'ens','RegistrationGranted','ens_v2_registry_l1',1,$2,$3,'block-'||$3::text,'tx',jsonb_build_object($4::text,'registration',$5::text,'0xreg:-:'||$6||':-:LabelRegistered'),'ens_v1_unwrapped_authority','canonical',jsonb_build_object('fixture_identity',$1,'labelhash',$7))")
            .bind(id).bind(CHAIN).bind(block).bind(INTERPRETER_STATE_KEY)
            .bind(super::STATE_SCOPE_KEY).bind(&token).bind(labelhash).execute(db.pool()).await?;
    }
    sqlx::query("UPDATE chain_lineage SET canonicality_state='orphaned' WHERE chain_id=$1 AND block_hash='block-4'")
        .bind(CHAIN).execute(db.pool()).await?;
    let mut connection = db.pool().acquire().await?;
    let routed = |labelhash: &str| format!("0xreg:{}00000000", &labelhash[..58]);
    for (request, expected) in [(routed(&alpha), vec!["alpha"]), (routed(&beta), vec![])] {
        let selected = super::ordered_events(&mut connection, CHAIN, 5, &[], &[], &[request])
            .await?
            .into_iter()
            .map(|ordered| ordered.event)
            .collect::<Vec<_>>();
        assert_eq!(identities(&selected), expected);
    }
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
    for (namespace, family) in [
        ("ens", "ens_v1_registrar_l1"),
        ("basenames", "basenames_base_registrar"),
    ] {
        sqlx::query(
            "INSERT INTO normalized_events
             (event_identity,namespace,event_kind,source_family,manifest_version,chain_id,
              block_number,block_hash,transaction_hash,raw_fact_ref,derivation_kind,
              canonicality_state,after_state)
             SELECT 'expiry-plan-'||$3||'-'||n,$3,'RegistrationGranted',$4,1,$1,
                    1,'block-1','tx',jsonb_build_object($2::text,$3||n::text),
                    'ens_v1_unwrapped_authority','canonical',
                    jsonb_build_object('namehash','node-'||n,'expiry',n)
             FROM generate_series(1,2000) n",
        )
        .bind(CHAIN)
        .bind(INTERPRETER_STATE_KEY)
        .bind(namespace)
        .bind(family)
        .execute(db.pool())
        .await?;
    }
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
    for index in [
        "normalized_events_v1_due_probe_idx",
        "normalized_events_basenames_due_probe_idx",
    ] {
        let mut pending = vec![&plan[0]["Plan"]];
        let mut bounded = false;
        while let Some(node) = pending.pop() {
            if node["Index Name"] == index {
                let condition = node["Index Cond"].as_str().unwrap_or("");
                bounded |= condition.contains("$3") && condition.contains("$4");
            }
            if let Some(children) = node["Plans"].as_array() {
                pending.extend(children);
            }
        }
        assert!(
            bounded,
            "generic expiry plan must index both timestamp bounds on {index}: {plan}"
        );
    }
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
    let mut expected: Vec<_> = ["basenames", "ens"]
        .into_iter()
        .flat_map(|namespace| (1900..1910).map(move |n| format!("{namespace}:node-{n}")))
        .collect();
    expected.sort();
    assert_eq!(names, expected);
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

fn relation_scans(plan: &Value, relation: &str) -> usize {
    usize::from(plan["Relation Name"] == relation)
        + plan["Plans"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|child| relation_scans(child, relation))
            .sum::<usize>()
}

/// The fixture database holds only the checked-in baseline schema, so this proves the
/// baseline defines indexes whose expressions the lookahead queries can use, for the ENSv1,
/// Basenames Base and ENSv2 families alike. A drifted expression or family predicate
/// in either place would fall back to scanning `normalized_events`.
#[tokio::test]
async fn lookahead_sql_uses_baseline_indexes() -> Result {
    let db = database().await?;
    for (namespace, family, rows) in [
        ("ens", "ens_v1_registrar_l1", 5000),
        ("basenames", "basenames_base_registrar", 5000),
        ("ens", "ens_v2_registry_l1", 500),
    ] {
        sqlx::query(
            "INSERT INTO normalized_events
             (event_identity,namespace,event_kind,source_family,manifest_version,chain_id,
              block_number,block_hash,transaction_hash,raw_fact_ref,derivation_kind,
              canonicality_state,after_state)
             SELECT 'plan-'||$4||'-'||n,$3,'RegistrationGranted',$4,1,$1,
                    1,'block-1','tx',jsonb_build_object($2::text,'key-'||$3||'-'||n,
                        $6::text,'0x60:-:token-'||n||':-:LabelRegistered'),
                    'ens_v1_unwrapped_authority','canonical',
                    jsonb_build_object('namehash','node-'||n,'expiry',n)
             FROM generate_series(1,$5) n",
        )
        .bind(CHAIN)
        .bind(INTERPRETER_STATE_KEY)
        .bind(namespace)
        .bind(family)
        .bind(rows)
        .bind(super::STATE_SCOPE_KEY)
        .execute(db.pool())
        .await?;
    }
    let mut connection = db.pool().acquire().await?;
    sqlx::raw_sql("ANALYZE normalized_events; ANALYZE chain_lineage;")
        .execute(&mut *connection)
        .await?;
    let events_sql = statement(super::EVENTS);
    let v2_due_keys_sql = super::V2_DUE_KEYS.replace("{state_scope}", super::STATE_SCOPE_KEY);
    for (name, statement, signature, arguments, indexes) in [
        (
            "events",
            events_sql.as_str(),
            "text,bigint,text[],uuid[],text[]",
            format!(
                "'{CHAIN}',3,ARRAY['ens:node-7','basenames:node-4000'],ARRAY[]::uuid[],ARRAY['0x60:0x0000000000000000000000000000000000000000000000000000000100000000']"
            ),
            &[
                "normalized_events_v1_direct_node_probe_idx",
                "normalized_events_basenames_direct_node_probe_idx",
                "normalized_events_v2_direct_node_probe_idx",
                "normalized_events_v2_key_probe_idx",
            ][..],
        ),
        (
            "v2_due_keys",
            v2_due_keys_sql.as_str(),
            "text,bigint,bigint,bigint",
            format!("'{CHAIN}',3,100,110"),
            &["normalized_events_v2_due_probe_idx"][..],
        ),
        (
            "v2_latest_topology",
            super::V2_LATEST_TOPOLOGY,
            "text,bigint",
            format!("'{CHAIN}',3"),
            &["normalized_events_v2_lookahead_probe_idx"][..],
        ),
        (
            "due_names",
            super::DUE_NAMES,
            "text,bigint,bigint,bigint,bigint",
            format!(
                "'{CHAIN}',3,{},{},{ENS_GRACE_PERIOD_SECS}",
                ENS_GRACE_PERIOD_SECS + 100,
                ENS_GRACE_PERIOD_SECS + 110
            ),
            &[
                "normalized_events_v1_due_probe_idx",
                "normalized_events_basenames_due_probe_idx",
            ][..],
        ),
    ] {
        // Both plan kinds matter: sqlx prepares the statement, and PostgreSQL may switch a
        // prepared statement to its generic plan.
        for mode in ["force_custom_plan", "force_generic_plan"] {
            let prepared = format!("{name}_{mode}");
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
            for index in indexes {
                assert!(
                    used.iter().any(|name| name == index),
                    "{mode} {name} plan must use {index}, used {used:?}"
                );
            }
            // Two lineage probes per state key in `winners`, one for the restored event's
            // timestamp and one in the ENSv2 key arm; a probe per candidate of a name or
            // resource would scale with its history.
            if name == "events" {
                let probes = relation_scans(&plan[0]["Plan"], "chain_lineage");
                assert!(
                    probes <= 4,
                    "{mode} events plan probes chain_lineage {probes} times"
                );
            }
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
            include_str!("../../../storage/schema/baseline/05_normalized_events.sql").to_owned(),
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

/// `events.sql` as `ordered_events` binds it.
fn statement(events: &str) -> String {
    events
        .replace("{v2_keys}", super::V2_KEYS.trim_end())
        .replace("{state_key}", INTERPRETER_STATE_KEY)
        .replace("{state_scope}", super::STATE_SCOPE_KEY)
        .replace("{clear_marker}", SUBREGISTRY_INVALIDATED_TOKEN_IDS_KEY)
        .replace("{transaction_index}", super::TRANSACTION_INDEX_KEY)
        .replace("{log_index}", super::LOG_INDEX_KEY)
}

const KEY_PROBE_INDEX: &str = "normalized_events_v2_key_probe_idx";

/// The ENSv2 key arm of `candidates` as one read for all keys, with the array overlap as a
/// filter. The tests below splice it back into `events.sql` as the oracle for the per-key
/// probes. Both forms expand the same `v2_keys.sql`, whatever its length.
const OVERLAP_ARM: &str = r"    UNION ALL
    -- ENSv2 events filed under a requested ENSv2 state key. The array must stay identical to
    -- normalized_events_v2_key_probe_idx and to `v2_event_keys` in the adapter crate.
    SELECT event.normalized_event_id,
           event.raw_fact_ref ? '{state_key}',
           COALESCE(event.raw_fact_ref ->> '{state_key}', event.event_identity),
           event.after_state ? '{clear_marker}'
    FROM normalized_events event
    JOIN LATERAL (
        SELECT 1 FROM chain_lineage lineage
        WHERE lineage.chain_id = event.chain_id
          AND lineage.block_number = event.block_number AND lineage.block_hash = event.block_hash
          AND lineage.canonicality_state IN ('canonical','safe','finalized')
        LIMIT 1
    ) readable ON TRUE
    WHERE {v2_keys} && $5::text[]
      AND event.chain_id = $1 AND event.block_number < $2
      AND event.source_family LIKE 'ens\_v2\_%'
      AND event.canonicality_state IN ('canonical','safe','finalized')
";

/// `events.sql` with its ENSv2 key arm replaced by `OVERLAP_ARM`.
fn overlap_form() -> String {
    let events = super::EVENTS;
    let start = events
        .find("    UNION ALL\n    -- ENSv2 events filed under a requested ENSv2 state key")
        .expect("events.sql has an ENSv2 key arm");
    let end = start
        + events[start..]
            .find("), keys AS MATERIALIZED (")
            .expect("the ENSv2 key arm ends the candidates");
    statement(&format!(
        "{}{OVERLAP_ARM}{}",
        &events[..start],
        &events[end..]
    ))
}

type EventRow = (Value, Option<OffsetDateTime>);

async fn key_rows(
    connection: &mut PgConnection,
    sql: &str,
    before: i64,
    keys: &[String],
) -> Result<Vec<EventRow>> {
    Ok(sqlx::query_as(sql)
        .bind(CHAIN)
        .bind(before)
        .bind(Vec::<String>::new())
        .bind(Vec::<Uuid>::new())
        .bind(keys)
        .fetch_all(connection)
        .await?)
}

fn row_identities(rows: &[EventRow]) -> Vec<&str> {
    let mut identities: Vec<_> = rows
        .iter()
        .map(|(body, _)| body["event_identity"].as_str().unwrap())
        .collect();
    identities.sort_unstable();
    identities
}

/// A token id whose low 32 bits are `low`. ENSv2 state keys zero them.
fn token(high: u64, low: u32) -> String {
    format!("0x{high:056x}{low:08x}")
}

fn v2_key(emitter: &str, id: &str) -> String {
    format!(
        "{}:{}00000000",
        emitter.to_lowercase(),
        id[..58].to_lowercase()
    )
}

fn text_array(keys: &[String]) -> String {
    format!("'{{{}}}'::text[]", keys.join(","))
}

/// The child of the `candidates` union that reads ENSv2 state keys: the one holding the
/// `v2_keys.sql` array.
fn key_arm(plan: &Value) -> Option<&Value> {
    let mut children = plan["Plans"].as_array().into_iter().flatten();
    if plan["Node Type"] == "Append"
        && let Some(arm) = children
            .clone()
            .find(|child| child.to_string().contains("array_remove("))
    {
        return Some(arm);
    }
    children.find_map(key_arm)
}

/// The key arm tests the array only in the inverted index, and reads `normalized_events`
/// through no other index and no sequential scan.
fn assert_probes_per_key(plan: &Value, table_indexes: &[String], context: &str) {
    let arm = key_arm(plan).unwrap_or_else(|| panic!("{context}: no ENSv2 key arm in {plan}"));
    let mut probed = false;
    let mut pending = vec![arm];
    while let Some(node) = pending.pop() {
        let index = node["Index Name"].as_str().unwrap_or("");
        assert!(
            index == KEY_PROBE_INDEX || !table_indexes.iter().any(|name| name == index),
            "{context}: the ENSv2 key arm reads {index}: {arm}"
        );
        assert!(
            !(node["Node Type"] == "Seq Scan" && node["Relation Name"] == "normalized_events"),
            "{context}: the ENSv2 key arm scans normalized_events: {arm}"
        );
        assert!(
            !node["Filter"]
                .as_str()
                .unwrap_or("")
                .contains("array_remove("),
            "{context}: the ENSv2 key arm filters rows by the key array: {arm}"
        );
        probed |= index == KEY_PROBE_INDEX
            && node["Index Cond"]
                .as_str()
                .unwrap_or("")
                .contains("array_remove(");
        pending.extend(node["Plans"].as_array().into_iter().flatten());
    }
    assert!(
        probed,
        "{context}: the key array is not an index condition on {KEY_PROBE_INDEX}: {arm}"
    );
}

/// `Actual Loops` of the lineage probe in the key arm: the inner side of the nested loop that
/// joins the arm's events to `chain_lineage`.
fn lineage_loops(plan: &Value) -> Option<i64> {
    let mut children = plan["Plans"].as_array().into_iter().flatten();
    if plan["Node Type"] == "Nested Loop"
        && let Some(inner) = children.clone().find(|child| {
            child["Parent Relationship"] == "Inner" && relation_scans(child, "chain_lineage") > 0
        })
    {
        return inner["Actual Loops"].as_i64();
    }
    children.find_map(lineage_loops)
}

/// The per-key probes return what the single overlap read returns, for every shape of key set
/// and with the inverted index unusable.
#[tokio::test]
async fn ensv2_key_arm_matches_the_overlap_form() -> Result {
    const OTHER_CHAIN: &str = "lookahead-other";
    const REGISTRY: &str = "0x00000000000000000000000000000000000000aa";
    const SECOND: &str = "0x00000000000000000000000000000000000000bb";
    const MIXED: &str = "0x00000000000000000000000000000000000000CC";
    const SUBREGISTRY: &str = "0x00000000000000000000000000000000000000DD";
    let db = database().await?;
    sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,'block-1',1,to_timestamp(1),'canonical')")
        .bind(OTHER_CHAIN).execute(db.pool()).await?;
    let mixed_token = format!("0x{}", "AB".repeat(32));
    for (identity, block, emitter, id, after) in [
        (
            "reg-1",
            1,
            REGISTRY,
            token(1, 7),
            json!({"labelhash": token(101, 0)}),
        ),
        (
            "link-1",
            2,
            REGISTRY,
            token(1, 7),
            json!({"resource": token(201, 0)}),
        ),
        (
            "reg-2",
            2,
            REGISTRY,
            token(2, 0),
            json!({"labelhash": token(102, 0), "new_token_id": token(3, 0)}),
        ),
        (
            "upstream",
            3,
            REGISTRY,
            token(4, 0),
            json!({"upstream_resource": token(202, 0)}),
        ),
        (
            "second-emitter",
            3,
            SECOND,
            token(1, 7),
            json!({"labelhash": token(101, 0)}),
        ),
        (
            "other-chain",
            1,
            REGISTRY,
            token(1, 7),
            json!({"labelhash": token(101, 0)}),
        ),
        (
            "orphaned-block",
            4,
            REGISTRY,
            token(5, 0),
            json!({"labelhash": token(105, 0)}),
        ),
        ("orphaned-row", 2, REGISTRY, token(6, 0), json!({})),
        ("late", 6, REGISTRY, token(1, 7), json!({})),
        (
            "mixed-case",
            3,
            MIXED,
            mixed_token.clone(),
            json!({"labelhash": token(103, 0)}),
        ),
        (
            "subregistry",
            3,
            REGISTRY,
            token(7, 0),
            json!({"subregistry": SUBREGISTRY}),
        ),
        (
            "cleared",
            3,
            REGISTRY,
            token(8, 0),
            json!({(SUBREGISTRY_INVALIDATED_TOKEN_IDS_KEY): []}),
        ),
        ("v1-family", 1, REGISTRY, token(1, 7), json!({})),
    ] {
        let chain = if identity == "other-chain" {
            OTHER_CHAIN
        } else {
            CHAIN
        };
        let family = if identity == "v1-family" {
            "ens_v1_registrar_l1"
        } else {
            "ens_v2_registry_l1"
        };
        sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,transaction_hash,raw_fact_ref,derivation_kind,canonicality_state,after_state) VALUES ($1,'ens','RegistrationGranted',$2,1,$3,$4,'block-'||$4::text,'tx',jsonb_build_object($5::text,'v2:'||$1,$6::text,$7||':-:'||$8||':-:LabelRegistered'),'ens_v2_registrar','canonical',$9)")
            .bind(identity).bind(family).bind(chain).bind(block).bind(INTERPRETER_STATE_KEY)
            .bind(super::STATE_SCOPE_KEY).bind(emitter).bind(&id).bind(after)
            .execute(db.pool()).await?;
    }
    sqlx::query("UPDATE normalized_events SET canonicality_state='orphaned' WHERE event_identity='orphaned-row'")
        .execute(db.pool()).await?;
    sqlx::query("UPDATE chain_lineage SET canonicality_state='orphaned' WHERE chain_id=$1 AND block_hash='block-4'")
        .bind(CHAIN).execute(db.pool()).await?;
    sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,'replacement-4',4,to_timestamp(4),'safe')")
        .bind(CHAIN).execute(db.pool()).await?;
    let mut connection = db.pool().acquire().await?;
    let mut universe: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT DISTINCT key FROM normalized_events event, unnest({}) key ORDER BY key",
        super::V2_KEYS
            .trim_end()
            .replace("{state_scope}", super::STATE_SCOPE_KEY)
    ))
    .fetch_all(&mut *connection)
    .await?;
    let absent: Vec<String> = (0..1200)
        .map(|n| v2_key(SECOND, &token(1_000 + n, 0)))
        .collect();
    universe.extend(absent.iter().take(10).cloned());
    let subregistry_key = format!("{}:00000000", SUBREGISTRY.to_lowercase());
    // The subregistry an event points at files the event under this key only once
    // `v2_keys.sql` lists it. Both forms read the same file.
    let subregistry_expected = if super::V2_KEYS.contains("'subregistry'") {
        vec!["subregistry"]
    } else {
        vec![]
    };
    let readable = [
        "cleared",
        "link-1",
        "mixed-case",
        "reg-1",
        "reg-2",
        "second-emitter",
        "subregistry",
        "upstream",
    ];
    let registry = format!("{REGISTRY}:*");
    let cases: Vec<(&str, i64, Vec<String>, Vec<&str>)> = vec![
        ("no keys", 5, vec![], vec![]),
        (
            "one key",
            5,
            vec![v2_key(REGISTRY, &token(2, 9))],
            vec!["reg-2"],
        ),
        ("keys matching nothing", 5, absent.clone(), vec![]),
        (
            "one event under two keys, one key over two events",
            5,
            vec![
                v2_key(REGISTRY, &token(1, 0)),
                v2_key(REGISTRY, &token(101, 0)),
            ],
            vec!["link-1", "reg-1"],
        ),
        (
            "new token id",
            5,
            vec![v2_key(REGISTRY, &token(3, 0))],
            vec!["reg-2"],
        ),
        (
            "upstream resource",
            5,
            vec![v2_key(REGISTRY, &token(202, 0))],
            vec!["upstream"],
        ),
        (
            "second emitter",
            5,
            vec![v2_key(SECOND, &token(1, 0))],
            vec!["second-emitter"],
        ),
        (
            "whole registry",
            5,
            vec![registry.clone()],
            vec![
                "cleared",
                "link-1",
                "reg-1",
                "reg-2",
                "subregistry",
                "upstream",
            ],
        ),
        (
            "whole registry before most of it",
            3,
            vec![registry],
            vec!["link-1", "reg-1", "reg-2"],
        ),
        (
            "orphaned block and orphaned event",
            5,
            vec![
                v2_key(REGISTRY, &token(5, 0)),
                v2_key(REGISTRY, &token(6, 0)),
            ],
            vec![],
        ),
        (
            "uppercase spelling",
            5,
            vec![v2_key(REGISTRY, &token(2, 0)).to_uppercase()],
            vec![],
        ),
        (
            "uppercase source",
            5,
            vec![v2_key(MIXED, &mixed_token)],
            vec!["mixed-case"],
        ),
        (
            "clear marker",
            5,
            vec![v2_key(REGISTRY, &token(8, 0))],
            vec!["cleared"],
        ),
        (
            "subregistry",
            5,
            vec![subregistry_key],
            subregistry_expected,
        ),
        ("every key", 5, universe.clone(), readable.to_vec()),
        ("every key, later batch", 7, universe.clone(), {
            let mut later = readable.to_vec();
            later.push("late");
            later.sort_unstable();
            later
        }),
    ];
    let (overlap, per_key) = (overlap_form(), statement(super::EVENTS));
    let mut results = Vec::new();
    for (case, before, keys, expected) in &cases {
        let old = key_rows(&mut connection, &overlap, *before, keys).await?;
        let new = key_rows(&mut connection, &per_key, *before, keys).await?;
        assert_eq!(new, old, "{case}");
        assert_eq!(row_identities(&new), *expected, "{case}");
        results.push(new);
    }
    // An invalid inverted index (a failed concurrent build) leaves the same rows.
    let mut transaction = db.pool().begin().await?;
    sqlx::query("UPDATE pg_index SET indisvalid = false WHERE indexrelid = $1::regclass")
        .bind(KEY_PROBE_INDEX)
        .execute(&mut *transaction)
        .await?;
    let plan: Value = sqlx::query_scalar(&format!("EXPLAIN (FORMAT JSON) {per_key}"))
        .bind(CHAIN)
        .bind(5_i64)
        .bind(Vec::<String>::new())
        .bind(Vec::<Uuid>::new())
        .bind(&universe)
        .fetch_one(&mut *transaction)
        .await?;
    let mut used = Vec::new();
    index_names(&plan[0]["Plan"], &mut used);
    assert!(!used.iter().any(|name| name == KEY_PROBE_INDEX), "{used:?}");
    for ((case, before, keys, _), valid) in cases.iter().zip(&results) {
        let new = key_rows(&mut transaction, &per_key, *before, keys).await?;
        assert_eq!(&new, valid, "{case} with an invalid index");
    }
    transaction.rollback().await?;
    drop(connection);
    db.cleanup().await?;
    assert_ne!(overlap, per_key, "events.sql still reads keys by overlap");
    Ok(())
}

async fn seed_key_population(pool: &PgPool, events: std::ops::Range<i64>) -> Result {
    sqlx::query(
        "INSERT INTO normalized_events
         (event_identity,namespace,event_kind,source_family,manifest_version,chain_id,
          block_number,block_hash,transaction_hash,raw_fact_ref,derivation_kind,
          canonicality_state,after_state)
         SELECT 'dense-'||n,'ens','RegistrationGranted','ens_v2_registry_l1',1,$1,
                (n % 3000) / 3,'block-'||((n % 3000) / 3),'tx',
                jsonb_build_object($2::text,'v2:dense-'||n,$3::text,
                    '0x'||lpad(to_hex(1 + n % 30),40,'0')||':-:0x'
                    ||encode(sha256(n::text::bytea),'hex')||':-:LabelRegistered'),
                'ens_v2_registrar','canonical','{}'
         FROM generate_series($4::bigint,$5::bigint - 1) n",
    )
    .bind(CHAIN)
    .bind(INTERPRETER_STATE_KEY)
    .bind(super::STATE_SCOPE_KEY)
    .bind(events.start)
    .bind(events.end)
    .execute(pool)
    .await?;
    Ok(())
}

/// Around a dense ENSv1→ENSv2 migration range a batch requests over a thousand ENSv2 state
/// keys. The planner does not use the statistics of a partial expression index, so it
/// estimates an array overlap from the number of requested keys alone, and a single read for
/// all keys turns into a read of the chain's ENSv2 history by block. The key arm must stay
/// one inverted-index probe per key at any key count, in both plan modes, with fresh or stale
/// column statistics.
#[tokio::test]
async fn ensv2_key_arm_probes_the_inverted_index_per_key() -> Result {
    let db = database().await?;
    sqlx::raw_sql(
        "ALTER TABLE normalized_events SET (autovacuum_enabled = false);
         INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state)
         SELECT 'lookahead-test','block-'||n,n,to_timestamp(n),'canonical'
         FROM generate_series(0,1000) n ON CONFLICT DO NOTHING",
    )
    .execute(db.pool())
    .await?;
    seed_key_population(db.pool(), 0..3_000).await?;
    let mut connection = db.pool().acquire().await?;
    // Without column statistics the planner misjudges the family predicate itself, in every
    // arm. Operators run ANALYZE once the indexes are built.
    sqlx::raw_sql("ANALYZE normalized_events; ANALYZE chain_lineage;")
        .execute(&mut *connection)
        .await?;
    let requested: Vec<String> = sqlx::query_scalar(
        "SELECT lower(split_part(raw_fact_ref ->> $1, ':', 1)) || ':'
             || left(split_part(raw_fact_ref ->> $1, ':', 3), 58) || '00000000'
         FROM normalized_events WHERE block_number < 500
         ORDER BY md5(event_identity) LIMIT 1200",
    )
    .bind(super::STATE_SCOPE_KEY)
    .fetch_all(&mut *connection)
    .await?;
    assert_eq!(requested.len(), 1200);
    let mut most = requested.clone();
    most.extend((0..48_800).map(|n| v2_key("0xff", &token(n, 0))));
    let table_indexes: Vec<String> = sqlx::query_scalar(
        "SELECT indexname::text FROM pg_indexes WHERE tablename = 'normalized_events'",
    )
    .fetch_all(&mut *connection)
    .await?;
    let per_key = statement(super::EVENTS);
    for pass in ["fresh", "stale"] {
        if pass == "stale" {
            seed_key_population(db.pool(), 3_000..33_000).await?;
        }
        for mode in ["force_custom_plan", "force_generic_plan"] {
            let prepared = format!("key_arm_{pass}_{mode}");
            sqlx::raw_sql(&format!(
                "SET plan_cache_mode={mode}; PREPARE {prepared}(text,bigint,text[],uuid[],text[]) AS {per_key}"
            ))
            .execute(&mut *connection)
            .await?;
            for keys in [&requested, &most] {
                let plan: Value = sqlx::query_scalar(&format!(
                    "EXPLAIN (FORMAT JSON) EXECUTE {prepared}('{CHAIN}',500,'{{}}'::text[],'{{}}'::uuid[],{})",
                    text_array(keys)
                ))
                .fetch_one(&mut *connection)
                .await?;
                assert_probes_per_key(
                    &plan[0]["Plan"],
                    &table_indexes,
                    &format!("{pass} statistics, {mode}, {} keys", keys.len()),
                );
            }
        }
    }
    drop(connection);
    db.cleanup().await?;
    Ok(())
}

/// An event filed under several requested keys has its lineage checked once.
#[tokio::test]
async fn ensv2_key_arm_probes_lineage_once_per_event() -> Result {
    const REGISTRY: &str = "0x00000000000000000000000000000000000000aa";
    let db = database().await?;
    let mut requested = Vec::new();
    for n in 1..=3_u64 {
        sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,transaction_hash,raw_fact_ref,derivation_kind,canonicality_state,after_state) VALUES ($1,'ens','RegistrationGranted','ens_v2_registry_l1',1,$2,$3,'block-'||$3::text,'tx',jsonb_build_object($4::text,'v2:'||$1,$5::text,$6||':-:'||$7||':-:LabelRegistered'),'ens_v2_registrar','canonical',jsonb_build_object('labelhash',$8::text))")
            .bind(format!("event-{n}")).bind(CHAIN).bind(i64::try_from(n)?).bind(INTERPRETER_STATE_KEY)
            .bind(super::STATE_SCOPE_KEY).bind(REGISTRY).bind(token(n, 0)).bind(token(100 + n, 0))
            .execute(db.pool()).await?;
        requested.extend([
            v2_key(REGISTRY, &token(n, 0)),
            v2_key(REGISTRY, &token(100 + n, 0)),
        ]);
    }
    let absent: Vec<String> = (0..1200)
        .map(|n| v2_key(REGISTRY, &token(1_000 + n, 0)))
        .collect();
    let mut connection = db.pool().acquire().await?;
    sqlx::raw_sql("ANALYZE normalized_events; ANALYZE chain_lineage;")
        .execute(&mut *connection)
        .await?;
    let per_key = statement(super::EVENTS);
    for mode in ["force_custom_plan", "force_generic_plan"] {
        sqlx::raw_sql(&format!(
            "SET plan_cache_mode={mode}; PREPARE lineage_{mode}(text,bigint,text[],uuid[],text[]) AS {per_key}"
        ))
        .execute(&mut *connection)
        .await?;
        for (keys, loops) in [(&requested, 3), (&absent, 0)] {
            let plan: Value = sqlx::query_scalar(&format!(
                "EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE lineage_{mode}('{CHAIN}',5,'{{}}'::text[],'{{}}'::uuid[],{})",
                text_array(keys)
            ))
            .fetch_one(&mut *connection)
            .await?;
            let arm = key_arm(&plan[0]["Plan"]).expect("an ENSv2 key arm");
            assert_eq!(
                lineage_loops(arm),
                Some(loops),
                "{mode}, {} keys: {arm}",
                keys.len()
            );
        }
    }
    drop(connection);
    db.cleanup().await?;
    Ok(())
}

/// The dense range at the scale it was found: 1.5 M ENSv2 events over 200,000 blocks, and
/// 1,200 keys of tokens registered in the first half, read at block 100,000. Prints the key
/// arm of each form. The overlap form takes tens of seconds here.
#[tokio::test]
#[ignore = "seeds 1.5 M events and builds the loader's indexes, a minute or more; run with \
            scripts/test-db -- cargo test -p bigname-interpret --features \
            bigname-adapters/test-activation,bigname-storage/test-support --lib -- --ignored \
            ensv2_key_arm_dense_range_measurement --nocapture"]
async fn ensv2_key_arm_dense_range_measurement() -> Result {
    // The indexes `events.sql` reads. The others are dropped so the seed takes about a minute.
    const READ: [&str; 9] = [
        "normalized_events_interpreter_state_history_idx",
        "normalized_events_resource_history_idx",
        "normalized_events_chain_block_number_idx",
        "normalized_events_chain_block_number_desc_idx",
        "normalized_events_v1_direct_node_probe_idx",
        "normalized_events_basenames_direct_node_probe_idx",
        "normalized_events_v2_direct_node_probe_idx",
        "normalized_events_v2_lookahead_probe_idx",
        KEY_PROBE_INDEX,
    ];
    let db = database().await?;
    let mut connection = db.pool().acquire().await?;
    let indexes: Vec<(String, String)> = sqlx::query_as(
        "SELECT indexrelid::regclass::text, pg_get_indexdef(indexrelid) FROM pg_index
         WHERE indrelid = 'normalized_events'::regclass
           AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conindid = indexrelid)",
    )
    .fetch_all(&mut *connection)
    .await?;
    for (name, _) in &indexes {
        sqlx::raw_sql(&format!("DROP INDEX {name}"))
            .execute(&mut *connection)
            .await?;
    }
    let started = std::time::Instant::now();
    // Replica mode skips the per-row foreign-key checks of the seed.
    sqlx::raw_sql("SET session_replication_role = replica")
        .execute(&mut *connection)
        .await?;
    sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) SELECT $1,'block-'||n,n,to_timestamp(n),'canonical' FROM generate_series(0,200100) n ON CONFLICT DO NOTHING")
        .bind(CHAIN).execute(&mut *connection).await?;
    sqlx::query(
        "INSERT INTO normalized_events
         (event_identity,namespace,event_kind,source_family,manifest_version,chain_id,
          block_number,block_hash,transaction_hash,raw_fact_ref,derivation_kind,
          canonicality_state,after_state)
         SELECT 'synth-'||n||'-'||kind,'ens','RegistrationGranted','ens_v2_registry_l1',1,$1,
                block,'block-'||block,'tx',
                jsonb_build_object($2::text,'v2:'||emitter||':'||token||':'||kind,
                    $3::text,emitter||':-:'||token||':-:LabelRegistered'),
                'ens_v2_registrar','canonical',
                CASE kind
                    WHEN 0 THEN jsonb_build_object('labelhash','0x'||encode(sha256(('label'||n)::bytea),'hex'))
                    WHEN 1 THEN jsonb_build_object('resource','0x'||encode(sha256(('resource'||n)::bytea),'hex'))
                    ELSE '{}'
                END
         FROM generate_series(0,499999) n, generate_series(0,2) kind,
         LATERAL (SELECT '0x'||lpad(to_hex(1 + n % 300),40,'0') AS emitter,
                         '0x'||encode(sha256(n::text::bytea),'hex') AS token,
                         (2 * n) / 5 + 50 * kind AS block) shape",
    )
    .bind(CHAIN)
    .bind(INTERPRETER_STATE_KEY)
    .bind(super::STATE_SCOPE_KEY)
    .execute(&mut *connection)
    .await?;
    sqlx::raw_sql("SET session_replication_role = DEFAULT")
        .execute(&mut *connection)
        .await?;
    for (name, definition) in &indexes {
        if READ.contains(&name.as_str()) {
            sqlx::raw_sql(definition).execute(&mut *connection).await?;
        }
    }
    sqlx::raw_sql("ANALYZE normalized_events; ANALYZE chain_lineage;")
        .execute(&mut *connection)
        .await?;
    println!("seeded and indexed in {:?}", started.elapsed());
    let requested: Vec<String> = sqlx::query_scalar(
        "SELECT lower(split_part(raw_fact_ref ->> $1, ':', 1)) || ':'
             || left(split_part(raw_fact_ref ->> $1, ':', 3), 58) || '00000000'
         FROM normalized_events WHERE block_number < 100000 AND event_identity LIKE '%-0'
         ORDER BY md5(event_identity) LIMIT 1200",
    )
    .bind(super::STATE_SCOPE_KEY)
    .fetch_all(&mut *connection)
    .await?;
    let table_indexes: Vec<String> = READ.iter().map(|name| (*name).to_owned()).collect();
    for (form, sql) in [
        ("overlap", overlap_form()),
        ("per key", statement(super::EVENTS)),
    ] {
        for mode in ["force_custom_plan", "force_generic_plan"] {
            let prepared = format!("measure_{}_{mode}", form.replace(' ', "_"));
            sqlx::raw_sql(&format!(
                "SET plan_cache_mode={mode}; PREPARE {prepared}(text,bigint,text[],uuid[],text[]) AS {sql}"
            ))
            .execute(&mut *connection)
            .await?;
            let plan: Value = sqlx::query_scalar(&format!(
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE {prepared}('{CHAIN}',100000,'{{}}'::text[],'{{}}'::uuid[],{})",
                text_array(&requested)
            ))
            .fetch_one(&mut *connection)
            .await?;
            let milliseconds = plan[0]["Execution Time"].as_f64().unwrap_or(f64::NAN);
            let arm = key_arm(&plan[0]["Plan"]).expect("an ENSv2 key arm");
            println!(
                "{form}, {mode}: {milliseconds:.0} ms\n{}",
                serde_json::to_string_pretty(arm)?
            );
            if form == "per key" {
                assert_probes_per_key(&plan[0]["Plan"], &table_indexes, mode);
                assert!(milliseconds < 10_000.0, "{mode}: {milliseconds} ms");
            }
        }
    }
    drop(connection);
    db.cleanup().await?;
    Ok(())
}
