//! A registry's labels read keeps only the ENSv2 candidates under the parent's current
//! subregistry and decides the other arms only for those children. Its rows and total must equal
//! the statement without that narrowing on every case the arm selection distinguishes, and the
//! narrowed count, executed under custom and generic plans, must probe the registration index by
//! registry and the edge candidate index by child node in the latest-edge veto's cross-parent
//! lookup. A read without a registry skips the join that carries the registration's emitter.
use anyhow::{Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::Value;
use sqlx::{Row, raw_sql};

use super::*;

const CHAIN: &str = "ethereum-sepolia";
const ETH: &str = "ens:0xe7e7";
const OTHER_PARENT: &str = "ens:0xb0b0";
const PLAN_MODES: [&str; 2] = ["force_custom_plan", "force_generic_plan"];
const SUBREGISTRY_FILTER: &str = " AND subregistry.subregistry_address = $3";
const CHILD_FILTER: &str =
    " AND edge.child_node IN (SELECT lower(v2.namehash) FROM v2_candidates v2)";

fn registry(n: i64) -> String {
    format!("0x{n:040x}")
}

/// The statement before the narrowing, with the same binds and the subregistry bind unused.
fn unnarrowed(sql: &str) -> Result<String> {
    ensure!(
        sql.contains(SUBREGISTRY_FILTER) && sql.contains(CHILD_FILTER),
        "the registry narrowing moved:\n{sql}"
    );
    Ok(sql
        .replace(SUBREGISTRY_FILTER, " AND $3::text IS NOT NULL")
        .replace(CHILD_FILTER, ""))
}

#[tokio::test]
async fn registry_labels_match_the_unnarrowed_relation_and_probe_the_indexes() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("registry_labels_narrowing").pool_max_connections(1),
    )
    .await?;
    let result = async {
        let mut conn = database.pool().acquire().await?;
        install(&mut conn).await?;
        let hash = bigname_content_hash::PROJECTION_CONTENT_HASH;
        let filter = ChildrenCurrentPageFilter::default();
        let owner = registry(0xb0b);
        let cases = [
            (ETH, registry(1), None),
            (
                ETH,
                registry(1),
                Some(RegistryLabelOwnerFilter::Owner(&owner)),
            ),
            (
                ETH,
                registry(1),
                Some(RegistryLabelOwnerFilter::ExcludeOwner(&owner)),
            ),
            (ETH, registry(2), None),
            (OTHER_PARENT, registry(2), None),
            (OTHER_PARENT, registry(1), None),
        ];
        for mode in PLAN_MODES {
            raw_sql(&format!("SET plan_cache_mode = {mode}"))
                .execute(&mut *conn)
                .await?;
            for (parent, registry, owner) in &cases {
                let labels = RegistryLabels {
                    registry,
                    owner: *owner,
                    registration_limit: None,
                };
                let label = format!("{mode} {parent} {registry} {owner:?}");
                let narrowed =
                    page(&mut conn, parent, &filter, Some(labels), None, 10_000, None).await?;
                let query = page_query(parent, &filter, Some(labels), None, 10_001, None);
                let before_sql = unnarrowed(query.sql())?;
                let mut before = sqlx::query(&before_sql)
                    .bind(*parent)
                    .bind(hash)
                    .bind(registry.as_str())
                    .bind(registry.as_str());
                if let Some(
                    RegistryLabelOwnerFilter::Owner(owner)
                    | RegistryLabelOwnerFilter::ExcludeOwner(owner),
                ) = owner
                {
                    before = before.bind(*owner);
                }
                let rows = before.bind(10_001_i64).fetch_all(&mut *conn).await?;
                let total = match rows.first() {
                    Some(row) => u64::try_from(row.try_get::<i64, _>("total_count")?)?,
                    None => 0,
                };
                let mut before_rows = Vec::new();
                for row in &rows {
                    if let Some((row, _)) = decode(row)? {
                        before_rows.push(row);
                    }
                }
                ensure!(
                    narrowed.total_count == total,
                    "{label}: total {} before {total}",
                    narrowed.total_count
                );
                ensure!(narrowed.rows == before_rows, "{label}: the rows differ");
                if owner.is_none() {
                    ensure!(
                        count_registry_labels(&mut conn, parent, registry, 10_000).await?
                            == Some(total)
                    );
                }
            }
        }
        // 800 labels less 50 released and the 30 the arm selection refuses (10 with no arm and
        // both arms, 10 with no summary and both arms, 10 whose selected arm is ENSv1), two of
        // which are also released. Labels that expired without a release are held; another
        // instance's registrations are not.
        let eth = count_registry_labels(&mut conn, ETH, &registry(1), 10_000).await?;
        ensure!(eth == Some(800 - 50 - 30 + 2), "eth labels: {eth:?}");
        ensure!(count_registry_labels(&mut conn, ETH, &registry(2), 10_000).await? == Some(0));
        ensure!(
            count_registry_labels(&mut conn, OTHER_PARENT, &registry(2), 10_000).await?
                == Some(100)
        );

        let mut count_query = QueryBuilder::<Postgres>::new("WITH ");
        let eth_registry = registry(1);
        push_children(
            &mut count_query,
            Parents::One(ETH),
            &filter,
            Some(RegistryLabels {
                registry: &eth_registry,
                owner: None,
                registration_limit: None,
            }),
            None,
        );
        count_query.push(") SELECT count(*) FROM children");
        raw_sql(&format!(
            "PREPARE labels_count (text, text, text, text) AS {}",
            count_query.sql()
        ))
        .execute(&mut *conn)
        .await?;
        for mode in PLAN_MODES {
            raw_sql(&format!("SET plan_cache_mode = {mode}"))
                .execute(&mut *conn)
                .await?;
            let plan: Value = sqlx::query_scalar(&format!(
                "EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE labels_count \
                 ('{ETH}', '{hash}', '{eth_registry}', '{eth_registry}')"
            ))
            .fetch_one(&mut *conn)
            .await?;
            let mut found = Vec::new();
            scans(&plan[0]["Plan"], &mut found);
            // The registration join by registry; the edge lookup, driven by the ENSv2
            // candidates' nodes; and, separately, the latest-edge veto's cross-parent `other`
            // probe by child node.
            for (relation, alias, index, column) in [
                (
                    "project_child_registration_state",
                    "registration",
                    "project_child_registration_state_registry_idx",
                    "registry_contract_instance_id",
                ),
                (
                    "project_child_edge_candidate",
                    "edge",
                    "project_child_edge_candidate_child_idx",
                    "child_node",
                ),
                (
                    "project_child_edge_candidate",
                    "other",
                    "project_child_edge_candidate_child_idx",
                    "child_node",
                ),
            ] {
                let probes: Vec<&Scan> = found
                    .iter()
                    .filter(|scan| scan.relation == relation && scan.alias == alias)
                    .collect();
                ensure!(
                    probes.iter().any(|probe| probe.loops > 0),
                    "{mode}: no executed scan of {relation} as {alias}\n{plan:#}"
                );
                for probe in probes {
                    ensure!(
                        !probe.indexes.is_empty()
                            && probe.indexes.iter().all(|(name, condition)| {
                                *name == index && condition.contains(column)
                            }),
                        "{mode}: {} of {relation} as {alias} is not a probe of {index} by \
                         {column}: {:?}\n{plan:#}",
                        probe.node_type,
                        probe.indexes
                    );
                }
            }
        }
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

/// The capped registry count: exact while the registry holds at most `cap` registrations under
/// the parent, `None` above, whatever the labels among them number. Registry 1 holds 750
/// unreleased registrations under the eth parent, 722 of them labels; registry 2 holds 100 under
/// the other parent, all labels.
#[tokio::test]
async fn registry_label_count_is_null_above_the_cap() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("registry_label_count_cap").pool_max_connections(1),
    )
    .await?;
    let result = async {
        let mut conn = database.pool().acquire().await?;
        install(&mut conn).await?;
        for (parent, registry, cap, expected) in [
            // At the cap the count is exact; one registration over it, none is reported, even
            // when the labels among them are within the cap.
            (ETH, registry(1), 750, Some(722)),
            (ETH, registry(1), 749, None),
            (ETH, registry(1), 722, None),
            (OTHER_PARENT, registry(2), 100, Some(100)),
            (OTHER_PARENT, registry(2), 99, None),
            // No registration under the parent: zero at any cap.
            (ETH, registry(2), 0, Some(0)),
        ] {
            let count = count_registry_labels(&mut conn, parent, &registry, cap).await?;
            ensure!(
                count == expected,
                "{parent} {registry} cap {cap}: {count:?}, expected {expected:?}"
            );
        }
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

/// The capped count's statement keeps `cap + 1` registrations under a `Limit` inside
/// `v2_candidates` and reads everything after them per kept registration: the registrations it
/// visits, the edge probes by child node, and the candidate windows are bounded by the cap, not by
/// the registry's 750 registrations or the parent's 4,000 ENSv1 edges. The wrapper and migration
/// relations are not scanned per candidate.
#[tokio::test]
async fn registry_label_count_plan_stops_at_the_cap() -> Result<()> {
    const CAP: u64 = 200;
    const KEPT: u64 = CAP + 1;
    let database = TestDatabase::create(
        TestDatabaseConfig::new("registry_label_count_cap_plan").pool_max_connections(1),
    )
    .await?;
    let result = async {
        let mut conn = database.pool().acquire().await?;
        install(&mut conn).await?;
        let hash = bigname_content_hash::PROJECTION_CONTENT_HASH;
        let eth_registry = registry(1);
        let limit = i64::try_from(KEPT)?;
        let query = registry_labels_count_query(ETH, &eth_registry, limit);
        raw_sql(&format!(
            "PREPARE labels_capped (text, text, text, bigint, text) AS {}",
            query.sql()
        ))
        .execute(&mut *conn)
        .await?;
        let mut failures = Vec::new();
        for mode in PLAN_MODES {
            raw_sql(&format!("SET plan_cache_mode = {mode}"))
                .execute(&mut *conn)
                .await?;
            let plan: Value = sqlx::query_scalar(&format!(
                "EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE labels_capped \
                 ('{ETH}', '{hash}', '{eth_registry}', {limit}, '{eth_registry}')"
            ))
            .fetch_one(&mut *conn)
            .await?;
            let root = &plan[0]["Plan"];
            let mut nodes = Vec::new();
            every_node(root, &mut nodes);
            let rows = |node: &Value| node["Actual Rows"].as_u64().unwrap_or_default();
            let loops = |node: &Value| node["Actual Loops"].as_u64().unwrap_or_default();
            let visited = |node: &Value| {
                (node["Actual Rows"].as_u64().unwrap_or_default()
                    + node["Rows Removed by Filter"].as_u64().unwrap_or_default())
                    * node["Actual Loops"].as_u64().unwrap_or_default()
            };
            match nodes
                .iter()
                .find(|node| node["Subplan Name"] == "CTE v2_candidates")
            {
                Some(cte) if cte["Node Type"] == "Limit" && rows(cte) == KEPT => {}
                cte => failures.push(format!(
                    "{mode}: v2_candidates is not a Limit that keeps {KEPT}: {cte:?}"
                )),
            }
            // The edge lookups are probes by child node, at most one per kept registration (and
            // per edge found, for the latest-edge veto), not an enumeration of the parent's edges.
            let mut probes = Vec::new();
            scans(root, &mut probes);
            for probe in probes.iter().filter(|scan| {
                scan.relation == "project_child_edge_candidate"
                    && matches!(scan.alias, "edge" | "other")
                    && scan.loops > 0
            }) {
                let by_child = !probe.indexes.is_empty()
                    && probe.indexes.iter().all(|(name, condition)| {
                        *name == "project_child_edge_candidate_child_idx"
                            && condition.contains("child_node")
                    });
                if !by_child || probe.loops > KEPT {
                    failures.push(format!(
                        "{mode}: {} of the edge candidates as {} is not a bounded probe by \
                         child node: {:?}, {} loops",
                        probe.node_type, probe.alias, probe.indexes, probe.loops
                    ));
                }
            }
            let registrations: u64 = nodes
                .iter()
                .filter(|node| {
                    node["Relation Name"] == "project_child_registration_state"
                        && node["Alias"] == "registration"
                })
                .map(|node| visited(node))
                .sum();
            if registrations > 2 * KEPT {
                failures.push(format!(
                    "{mode}: the count visited {registrations} registrations, over 2 x (cap + 1)"
                ));
            }
            for node in &nodes {
                let relation = node["Relation Name"].as_str().unwrap_or_default();
                let alias = node["Alias"].as_str().unwrap_or_default();
                if relation == "project_child_edge_candidate"
                    && matches!(alias, "edge" | "other")
                    && visited(node) > 2 * KEPT
                {
                    failures.push(format!(
                        "{mode}: {alias} visited {} edge candidates, over 2 x (cap + 1)",
                        visited(node)
                    ));
                }
                if node["Node Type"] == "WindowAgg" && rows(node) > 2 * KEPT {
                    failures.push(format!(
                        "{mode}: a candidate window read {} rows, over 2 x (cap + 1)",
                        rows(node)
                    ));
                }
                if matches!(
                    relation,
                    "project_wrapper_state" | "migration_discovery_associations"
                ) && node["Node Type"] == "Seq Scan"
                    && loops(node) > 1
                {
                    failures.push(format!(
                        "{mode}: {relation} is scanned {} times",
                        loops(node)
                    ));
                }
            }
            let counted: (i64, i64) = sqlx::query_as(&format!(
                "EXECUTE labels_capped ('{ETH}', '{hash}', '{eth_registry}', {limit}, \
                 '{eth_registry}')"
            ))
            .fetch_one(&mut *conn)
            .await?;
            if counted.0 != limit {
                failures.push(format!("{mode}: kept {} registrations", counted.0));
            }
            if !failures.is_empty() {
                eprintln!("{mode}: {plan:#}");
            }
        }
        ensure!(failures.is_empty(), "{}", failures.join("\n"));
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

/// The join that carries an ENSv2 registration's emitter, which only a registry's labels read.
const EMITTER_JOIN: &str = "
            LEFT JOIN bigname_phase.normalized_events registration_event
              ON registration_event.event_identity = registration.event_identity";
const PREIMAGE_JOIN: &str = "ON preimage.labelhash = lower(child.labelhashes[1])";

/// The statement with the emitter join restored after the ENSv2 preimage join.
fn with_emitter_join(sql: &str) -> Result<String> {
    ensure!(
        sql.matches(PREIMAGE_JOIN).count() == 1,
        "the ENSv2 preimage join moved:\n{sql}"
    );
    Ok(sql.replace(PREIMAGE_JOIN, &format!("{PREIMAGE_JOIN}{EMITTER_JOIN}")))
}

enum Bind<'a> {
    Text(&'a str),
    Texts(&'a [String]),
    Int(i64),
}

/// Every row of `sql` as JSON text, sorted, so two statements over the same rows with the same
/// `ORDER BY` compare equal.
async fn json_rows(conn: &mut PgConnection, sql: &str, binds: &[Bind<'_>]) -> Result<Vec<String>> {
    let wrapped = format!("SELECT to_jsonb(result)::text FROM ({sql}) result");
    let mut query = sqlx::query_scalar::<_, String>(&wrapped);
    for bind in binds {
        query = match bind {
            Bind::Text(value) => query.bind(*value),
            Bind::Texts(value) => query.bind(*value),
            Bind::Int(value) => query.bind(*value),
        };
    }
    let mut rows = query.fetch_all(&mut *conn).await?;
    rows.sort();
    Ok(rows)
}

/// A read without a registry reads no registration's emitter, so its statement has no join to
/// `normalized_events` for it. The join is at most one row per registration
/// (`normalized_events_event_identity_key`) and no such read projects or filters the emitter, so
/// every read shape returns the same rows with the join restored: the subnames page under both
/// sorts and the expiry fence, with and without a total cap, the per-parent counts and the
/// by-node read. Under custom and generic plans, the subnames page scans `normalized_events` only
/// in the shadow flags' `observed` probes, while a registry's labels page still reads the emitter.
#[tokio::test]
async fn children_without_a_registry_skip_the_emitter_join() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("children_emitter_join").pool_max_connections(1),
    )
    .await?;
    let result = async {
        let mut conn = database.pool().acquire().await?;
        install(&mut conn).await?;
        let hash = bigname_content_hash::PROJECTION_CONTENT_HASH;
        let mut failures = Vec::new();
        let by_expiry = ChildrenCurrentPageFilter {
            include_expired: false,
            sort: ChildrenCurrentSort::ExpiresAt,
            order: ChildrenCurrentOrder::Desc,
            ..ChildrenCurrentPageFilter::default()
        };
        let filters = [ChildrenCurrentPageFilter::default(), by_expiry];
        let parents = [ETH.to_owned(), OTHER_PARENT.to_owned()];
        // ENSv1 only; both arms with no arm, with no summary, with ENSv1 and with ENSv2
        // selected; moved under another parent; ENSv2 only with no arm; another registry's.
        let nodes: Vec<String> = [3790_u64, 3805, 3815, 3825, 3850, 3833, 4595, 4005]
            .into_iter()
            .map(|n| format!("0x{n:064x}"))
            .collect();
        let mut statements = Vec::new();
        for mode in PLAN_MODES {
            for parent in [ETH, OTHER_PARENT] {
                for filter in &filters {
                    for total_limit in [None, Some(10_001_i64)] {
                        let query = page_query(parent, filter, None, None, 10_001, total_limit);
                        let mut binds =
                            vec![Bind::Text(parent), Bind::Text(hash), Bind::Int(10_001)];
                        binds.extend(total_limit.map(Bind::Int));
                        statements.push((
                            mode,
                            format!("page {parent} {:?} cap {total_limit:?}", filter.sort),
                            query.sql().to_owned(),
                            binds,
                        ));
                    }
                }
            }
            statements.push((
                mode,
                "counts".to_owned(),
                counts_query(&parents).sql().to_owned(),
                vec![Bind::Texts(&parents), Bind::Text(hash)],
            ));
            let mut by_node = QueryBuilder::<Postgres>::new("WITH ");
            push_children(
                &mut by_node,
                Parents::One(ETH),
                &filters[0],
                None,
                Some(nodes.as_slice()),
            );
            by_node.push(") SELECT * FROM children");
            statements.push((
                mode,
                "by node".to_owned(),
                by_node.sql().to_owned(),
                vec![
                    Bind::Text(ETH),
                    Bind::Text(hash),
                    Bind::Texts(&nodes),
                    Bind::Texts(&nodes),
                ],
            ));
        }
        for (mode, label, sql, binds) in &statements {
            raw_sql(&format!("SET plan_cache_mode = {mode}"))
                .execute(&mut *conn)
                .await?;
            if sql.contains("registration_event") {
                failures.push(format!("{mode} {label}: joins the registration's emitter"));
                continue;
            }
            let rows = json_rows(&mut conn, sql, binds).await?;
            let joined = json_rows(&mut conn, &with_emitter_join(sql)?, binds).await?;
            if rows.is_empty() || rows != joined {
                failures.push(format!(
                    "{mode} {label}: {} rows, {} with the emitter join",
                    rows.len(),
                    joined.len()
                ));
            }
        }

        let filter = ChildrenCurrentPageFilter::default();
        let eth_registry = registry(1);
        let subnames = page_query(ETH, &filter, None, None, 11, None);
        raw_sql(&format!(
            "PREPARE subnames_page (text, text, bigint) AS {}",
            subnames.sql()
        ))
        .execute(&mut *conn)
        .await?;
        let labels = page_query(
            ETH,
            &filter,
            Some(RegistryLabels {
                registry: &eth_registry,
                owner: None,
                registration_limit: None,
            }),
            None,
            11,
            None,
        );
        raw_sql(&format!(
            "PREPARE labels_page (text, text, text, text, bigint) AS {}",
            labels.sql()
        ))
        .execute(&mut *conn)
        .await?;
        for mode in PLAN_MODES {
            raw_sql(&format!("SET plan_cache_mode = {mode}"))
                .execute(&mut *conn)
                .await?;
            for (statement, arguments, reads_emitter) in [
                ("subnames_page", format!("'{ETH}', '{hash}', 11"), false),
                (
                    "labels_page",
                    format!("'{ETH}', '{hash}', '{eth_registry}', '{eth_registry}', 11"),
                    true,
                ),
            ] {
                let plan: Value = sqlx::query_scalar(&format!(
                    "EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE {statement} ({arguments})"
                ))
                .fetch_one(&mut *conn)
                .await?;
                let mut nodes = Vec::new();
                every_node(&plan[0]["Plan"], &mut nodes);
                let events: Vec<&str> = nodes
                    .iter()
                    .filter(|node| node["Relation Name"] == "normalized_events")
                    .map(|node| node["Alias"].as_str().unwrap_or_default())
                    .collect();
                if events.contains(&"registration_event") != reads_emitter
                    || events
                        .iter()
                        .any(|alias| !matches!(*alias, "registration_event" | "observed"))
                {
                    failures.push(format!(
                        "{mode} {statement}: scans normalized_events as {events:?}"
                    ));
                }
            }
        }
        ensure!(failures.is_empty(), "{}", failures.join("\n"));
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

/// Every node of a plan, sub-plans, init plans and CTE definitions included.
fn every_node<'a>(node: &'a Value, output: &mut Vec<&'a Value>) {
    output.push(node);
    for child in node["Plans"].as_array().into_iter().flatten() {
        every_node(child, output);
    }
}

/// One scan of a table in an `EXPLAIN (ANALYZE, FORMAT JSON)` plan, with each index it reads and
/// that index's condition: an Index Scan or Index Only Scan reads its own index, a Bitmap Heap
/// Scan the Bitmap Index Scans beneath it, and any other scan none.
struct Scan<'a> {
    relation: &'a str,
    alias: &'a str,
    node_type: &'a str,
    indexes: Vec<(&'a str, &'a str)>,
    loops: u64,
}

/// Every table scan in the plan, sub-plans and init plans included.
fn scans<'a>(node: &'a Value, output: &mut Vec<Scan<'a>>) {
    fn bitmap_indexes<'a>(node: &'a Value, output: &mut Vec<(&'a str, &'a str)>) {
        for child in node["Plans"].as_array().into_iter().flatten() {
            if child["Node Type"] == "Bitmap Index Scan" {
                output.push((
                    child["Index Name"].as_str().unwrap_or_default(),
                    child["Index Cond"].as_str().unwrap_or_default(),
                ));
            }
            bitmap_indexes(child, output);
        }
    }
    if let (Some(relation), Some(alias)) = (node["Relation Name"].as_str(), node["Alias"].as_str())
    {
        let node_type = node["Node Type"].as_str().unwrap_or_default();
        let mut indexes = Vec::new();
        match node_type {
            "Index Scan" | "Index Only Scan" => indexes.push((
                node["Index Name"].as_str().unwrap_or_default(),
                node["Index Cond"].as_str().unwrap_or_default(),
            )),
            "Bitmap Heap Scan" => bitmap_indexes(node, &mut indexes),
            _ => {}
        }
        output.push(Scan {
            relation,
            alias,
            node_type,
            indexes,
            loops: node["Actual Loops"].as_u64().unwrap_or_default(),
        });
    }
    for child in node["Plans"].as_array().into_iter().flatten() {
        scans(child, output);
    }
}

/// The eth parent's subregistry is registry 1 and the other parent's registry 2. Child n is node
/// n with labelhash n. The eth parent has ENSv1 children 1..=4000 and ENSv2 labels 3801..=4600
/// in registry 1; the other parent has 100 labels in registry 2; other parents hold 10,000 more
/// ENSv1 edges and other registries 3,000 more registrations.
async fn install(conn: &mut PgConnection) -> Result<()> {
    raw_sql("CREATE SCHEMA bigname_phase; SET search_path TO bigname_phase, public; SET jit = off")
        .execute(&mut *conn)
        .await?;
    for baseline in [
        include_str!("../../../../schema/baseline/01_chain.sql"),
        include_str!("../../../../schema/baseline/02_raw_facts.sql"),
        include_str!("../../../../schema/baseline/03_identity.sql"),
        include_str!("../../../../schema/baseline/04_manifests.sql"),
        include_str!("../../../../schema/baseline/05_normalized_events.sql"),
        include_str!("../../../../schema/baseline/06_projections.sql"),
        include_str!("../../../../schema/baseline/07_labels.sql"),
    ] {
        raw_sql(baseline).execute(&mut *conn).await?;
    }
    raw_sql(&format!(
        r#"
        CREATE FUNCTION pg_temp.node(n bigint) RETURNS text
            LANGUAGE sql IMMUTABLE AS $$ SELECT '0x' || lpad(to_hex(n), 64, '0') $$;
        CREATE FUNCTION pg_temp.label(n bigint) RETURNS text
            LANGUAGE sql IMMUTABLE AS $$ SELECT '0xaa' || lpad(to_hex(n), 62, '0') $$;
        CREATE FUNCTION pg_temp.address(n bigint) RETURNS text
            LANGUAGE sql IMMUTABLE AS $$ SELECT '0x' || lpad(to_hex(n), 40, '0') $$;

        INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp,
            canonicality_state)
        SELECT '{CHAIN}', 'b' || n, n, to_timestamp(1700000000 + n * 12), 'finalized'
        FROM generate_series(1, 2000) n;
        INSERT INTO project_family_marker (chain_id, current_block_number, current_block_hash,
            block_timestamp, input_content_hash, state)
        VALUES ('{CHAIN}', 2000, 'b2000', to_timestamp(1700000000 + 2000 * 12), '{hash}', 'live');
        INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
            dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
            chain_id, block_hash, block_number, canonicality_state)
        VALUES ('{ETH}', 'ens', 'eth', ARRAY['eth'], '\x00', '0xe7e7', ARRAY['0xe7e7lh'], 'v',
                'active', '{CHAIN}', 'b1', 1, 'finalized'),
               ('{OTHER_PARENT}', 'ens', 'bob', ARRAY['bob'], '\x00', '0xb0b0', ARRAY['0xb0b0lh'],
                'v', 'active', '{CHAIN}', 'b1', 1, 'finalized');

        INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
        SELECT lpad(to_hex(r), 32, '0')::uuid, '{CHAIN}', 'contract' FROM generate_series(1, 51) r;
        INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address,
            active_from_block_number)
        SELECT lpad(to_hex(r), 32, '0')::uuid, '{CHAIN}', pg_temp.address(r), 1
        FROM generate_series(1, 51) r;
        INSERT INTO project_parent_subregistry (chain_id, logical_name_id, block_number,
            event_identity, subregistry_address)
        VALUES ('{CHAIN}', '{ETH}', 2, 'sub:eth', pg_temp.address(1)),
               ('{CHAIN}', '{OTHER_PARENT}', 2, 'sub:bob', pg_temp.address(2));

        INSERT INTO project_child_edge_candidate (chain_id, namespace, parent_node, child_node,
            authority_arm, block_number, transaction_index, log_index, event_identity, owner,
            labelhash, source_family)
        SELECT '{CHAIN}', 'ens', '0xe7e7', pg_temp.node(n), 'ens_v1', 1 + n % 1900, 0, n,
               'v1:' || n, pg_temp.address(n % 97 + 1000), pg_temp.label(n), 'ens_v1_registry_l1'
        FROM generate_series(1, 4000) n
        UNION ALL
        SELECT '{CHAIN}', 'ens', '0xbb' || lpad(to_hex(p), 62, '0'),
               '0xcc' || lpad(to_hex(p * 100 + k), 62, '0'), 'ens_v1', 1 + k, 0, k,
               'v1o:' || p || ':' || k, pg_temp.address(k + 1000), pg_temp.label(p * 100 + k),
               'ens_v1_registry_l1'
        FROM generate_series(1, 200) p, generate_series(1, 50) k
        -- Children 3831..=3835 later moved under another parent, which leaves them ENSv2 only.
        UNION ALL
        SELECT '{CHAIN}', 'ens', '0xbbff', pg_temp.node(n), 'ens_v1', 1990, 0, n, 'moved:' || n,
               pg_temp.address(1000), pg_temp.label(n), 'ens_v1_registry_l1'
        FROM generate_series(3831, 3835) n;

        INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
            dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
            chain_id, block_hash, block_number, canonicality_state)
        SELECT 'ens:' || pg_temp.node(n), 'ens', 'label' || n || '.eth',
               ARRAY['label' || n, 'eth'], '\x00', pg_temp.node(n),
               ARRAY[pg_temp.label(n), '0xe7e7lh'], 'v', 'active', '{CHAIN}',
               'b' || (1 + n % 1900), 1 + n % 1900, 'finalized'
        FROM generate_series(1, 4600) n;
        INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
            dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
            chain_id, block_hash, block_number, canonicality_state)
        SELECT 'ens:' || pg_temp.node(n), 'ens', 'label' || n || '.bob',
               ARRAY['label' || n, 'bob'], '\x00', pg_temp.node(n),
               ARRAY[pg_temp.label(n), '0xb0b0lh'], 'v', 'active', '{CHAIN}', 'b5', 5, 'finalized'
        FROM generate_series(5001, 5100) n;
        INSERT INTO label_preimages (labelhash, raw_label, decoded_label, normalizer_version,
            normalized_under_version, source_kind, source_priority)
        SELECT pg_temp.label(n), convert_to('label' || n, 'UTF8'), 'label' || n, 'v', true,
               'event', 0
        FROM generate_series(1, 5100) n;

        -- Registry 1 holds 3801..=4600 for the eth parent, every 16th released; registry 2
        -- holds 5001..=5100 for the other parent and 4001..=4010, which are not its children.
        INSERT INTO project_child_registration_state (chain_id, logical_name_id,
            registry_contract_instance_id, event_kind, registrant, block_number,
            transaction_index, log_index, event_identity, exists)
        SELECT '{CHAIN}', 'ens:' || pg_temp.node(n), lpad(to_hex(r), 32, '0')::uuid::text,
               CASE WHEN r = 1 AND n % 16 = 0 THEN 'RegistrationReleased'
                    ELSE 'RegistrationGranted' END,
               pg_temp.address(n % 7 + 2000), 1 + n % 1900, 1, n, 'reg:' || r || ':' || n, true
        FROM (SELECT n, 1 AS r FROM generate_series(3801, 4600) n
              UNION ALL SELECT n, 2 FROM generate_series(5001, 5100) n
              UNION ALL SELECT n, 2 FROM generate_series(4001, 4010) n
              UNION ALL SELECT n, 3 + n % 49 FROM generate_series(100001, 103000) n) registration;
        INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
            manifest_version, chain_id, block_number, block_hash, transaction_hash,
            transaction_index, log_index, raw_fact_ref, derivation_kind, canonicality_state)
        SELECT state.event_identity, 'ens', state.event_kind, 'ens_v2_registry_l1', 1, '{CHAIN}',
               state.block_number, 'b' || state.block_number, 't' || state.event_identity, 1,
               state.log_index,
               jsonb_build_object('emitting_address',
                   pg_temp.address(('x' || right(state.registry_contract_instance_id, 8))::bit(32)::int)),
               'ens_v2_registry_resource_surface', 'finalized'
        FROM project_child_registration_state state;

        -- Summaries: ENSv1 below 3801; ENSv2 above, every tenth expired but not released, and
        -- among the children of both arms: 3801..=3810 and 3831..=3835 with no arm, 3811..=3820
        -- with no summary, 3821..=3830 ENSv1, 4591..=4600 (ENSv2 only) with no arm.
        INSERT INTO project_name_summary (chain_id, logical_name_id, namespace, authority_arm,
            serving, registration_status, expires_at, registered_at, zero_owner, owner,
            expiry_listable, search_supported, resolver_listing_eligible,
            resolver_support_exempt, namehash)
        SELECT '{CHAIN}', 'ens:' || pg_temp.node(n), 'ens',
               CASE WHEN n BETWEEN 3801 AND 3810 OR n BETWEEN 3831 AND 3835
                         OR n BETWEEN 4591 AND 4600 THEN NULL
                    WHEN n BETWEEN 3821 AND 3830 OR n < 3801 THEN 'ens_v1'
                    ELSE 'ens_v2' END,
               true,
               CASE WHEN n BETWEEN 3801 AND 4600 AND n % 16 = 0 THEN 'released' ELSE 'active' END,
               CASE WHEN n % 10 = 0 THEN 1600000000 ELSE 1900000000 END,
               to_timestamp(1700000000 + n), false,
               CASE WHEN n % 3 = 0 THEN pg_temp.address(2827) ELSE pg_temp.address(n) END,
               true, false, false, false, pg_temp.node(n)
        FROM generate_series(1, 5100) n
        WHERE n NOT BETWEEN 3811 AND 3820;
        INSERT INTO project_registry_node_state (chain_id, namespace, node, block_number,
            transaction_index, log_index, event_identity, owner)
        SELECT '{CHAIN}', 'ens', pg_temp.node(n), 1 + n % 1900, 0, n, 'v1:' || n,
               pg_temp.address(n % 97 + 1000)
        FROM generate_series(1, 4000) n;
        ANALYZE;
        "#,
        hash = bigname_content_hash::PROJECTION_CONTENT_HASH,
    ))
    .execute(&mut *conn)
    .await?;
    Ok(())
}
