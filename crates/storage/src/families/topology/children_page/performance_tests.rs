//! The actual child-page statement over many registry children without name surfaces. Such
//! children are produced by unnamed SubregistryChanged events (the API family-children and
//! Project registry-children fixtures exercise that write path); summary composition has no
//! surface to compose for them. The previous correlated arm fallback scanned all candidates
//! once per child, even when the requested page had only five rows.
use anyhow::{Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::Value;
use sqlx::raw_sql;

use super::*;

const CHILDREN: i64 = 2_000;
const PARENT: &str = "ens:parent";
const CHAIN: &str = "ethereum-mainnet";

#[tokio::test]
async fn a_small_child_page_does_not_rescan_all_candidates_per_child() -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new("children_page_plan").pool_max_connections(1))
            .await?;
    let result = async {
        let mut conn = database.pool().acquire().await?;
        install(&mut conn).await?;
        let filter = ChildrenCurrentPageFilter::default();
        let query = page_query(PARENT, &filter, None, None, 6);
        raw_sql(&format!(
            "PREPARE children (text, text, bigint) AS {}",
            query.sql()
        ))
        .execute(&mut *conn)
        .await?;
        for mode in ["force_custom_plan", "force_generic_plan"] {
            raw_sql(&format!("SET plan_cache_mode = {mode}"))
                .execute(&mut *conn)
                .await?;
            let plan: Value = sqlx::query_scalar(&format!(
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE children \
                 ('{PARENT}', '{}', 6)",
                bigname_content_hash::INTERPRETER_CONTENT_HASH
            ))
            .fetch_one(&mut *conn)
            .await?;
            assert_linear_candidates(&plan[0]["Plan"])?;
            ensure!(plan[0]["Plan"]["Actual Rows"] == 6, "{mode}: {plan}");
            let first = page(&mut conn, PARENT, &filter, None, None, 5).await?;
            ensure!(first.total_count == CHILDREN as u64 && first.rows.len() == 5);
            ensure!(first.rows[0].child_logical_name_id == child_id(1));
            let second = page(
                &mut conn,
                PARENT,
                &filter,
                None,
                first.next_cursor.as_ref(),
                5,
            )
            .await?;
            ensure!(second.total_count == first.total_count && second.rows.len() == 5);
            ensure!(second.rows[0].child_logical_name_id == child_id(6));
        }
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

fn assert_linear_candidates(plan: &Value) -> Result<()> {
    if plan["CTE Name"] == "candidates" {
        ensure!(
            plan["Actual Loops"].as_u64().unwrap_or_default() <= 1,
            "the page rescans its candidate relation per child: {plan}"
        );
    }
    for child in plan["Plans"].as_array().into_iter().flatten() {
        assert_linear_candidates(child)?;
    }
    Ok(())
}

fn child_id(n: i64) -> String {
    format!("ens:0x{n:064x}")
}

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
        "INSERT INTO chain_lineage(chain_id, block_hash, block_number, block_timestamp,
             canonicality_state) VALUES ('{CHAIN}', 'block', 100, to_timestamp(100), 'canonical');
         INSERT INTO name_surfaces(logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state)
         VALUES ('{PARENT}', 'ens', 'eth', ARRAY['eth'], '\\x00', 'parent', ARRAY['eth'],
             'fixture', 'active', '{CHAIN}', 'block', 100, 'canonical');
         INSERT INTO project_family_marker(chain_id, current_block_number, current_block_hash,
             block_timestamp, input_content_hash, state)
         VALUES ('{CHAIN}', 100, 'block', to_timestamp(100), '{}', 'live');
         INSERT INTO project_child_edge_candidate(chain_id, namespace, parent_node, child_node,
             authority_arm, block_number, transaction_index, log_index, event_identity,
             owner, labelhash, source_family)
         SELECT '{CHAIN}', 'ens', 'parent', '0x' || lpad(to_hex(n), 64, '0'), 'ens_v1',
             100, 0, n, 'child:' || n || ':0', '0x1111111111111111111111111111111111111111',
             '0x' || lpad(to_hex(n), 64, '0'), 'ens_v1_registry_l1'
         FROM generate_series(1, {CHILDREN}) n;
         ANALYZE name_surfaces; ANALYZE chain_lineage; ANALYZE project_family_marker;
         ANALYZE project_child_edge_candidate; ANALYZE project_name_summary;",
        bigname_content_hash::INTERPRETER_CONTENT_HASH
    ))
    .execute(conn)
    .await?;
    Ok(())
}

#[tokio::test]
async fn child_arm_selection_and_a_later_edge_under_another_parent_are_preserved() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("children_arm_selection").pool_max_connections(1),
    )
    .await?;
    let result = async {
        let mut conn = database.pool().acquire().await?;
        install(&mut conn).await?;
        let child = child_id(1);
        let registry = "00000000-0000-0000-0000-000000000001";
        raw_sql(&format!(
            "INSERT INTO contract_instances(contract_instance_id, chain_id, contract_kind)
             VALUES ('{registry}', '{CHAIN}', 'contract');
             INSERT INTO contract_instance_addresses(contract_instance_id, chain_id, address)
             VALUES ('{registry}', '{CHAIN}', '0xregistry');
             INSERT INTO project_parent_subregistry(chain_id, logical_name_id, block_number,
                 event_identity, subregistry_address)
             VALUES ('{CHAIN}', '{PARENT}', 100, 'subregistry', '0xregistry');
             INSERT INTO name_surfaces(logical_name_id, namespace, raw_name, raw_labels,
                 dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
                 chain_id, block_hash, block_number, canonicality_state)
             VALUES ('{child}', 'ens', 'one.eth', ARRAY['one','eth'], '\\x00', '{}',
                 ARRAY['label','eth'], 'fixture', 'active', '{CHAIN}', 'block', 100, 'canonical');
             INSERT INTO project_child_registration_state(chain_id, logical_name_id,
                 registry_contract_instance_id, event_kind, registrant, block_number,
                 transaction_index, log_index, event_identity, exists)
             VALUES ('{CHAIN}', '{child}', '{registry}', 'RegistrationGranted', '0xregistrant',
                 100, 0, 2, 'registration:0', true);",
            child.strip_prefix("ens:").unwrap()
        ))
        .execute(&mut *conn)
        .await?;
        let filter = ChildrenCurrentPageFilter::default();
        // Both arms, no summary: refuse the ambiguous child.
        let ambiguous = page(&mut conn, PARENT, &filter, None, None, CHILDREN as u64).await?;
        ensure!(ambiguous.total_count == (CHILDREN - 1) as u64);
        ensure!(
            ambiguous
                .rows
                .iter()
                .all(|row| row.child_logical_name_id != child)
        );
        for arm in [None, Some("ens_v1"), Some("ens_v2"), Some("unsupported")] {
            sqlx::query(
                "INSERT INTO project_name_summary(chain_id, logical_name_id, namespace,
                     authority_arm, serving, zero_owner) VALUES ($1, $2, 'ens', $3, false, false)
                 ON CONFLICT (chain_id, logical_name_id)
                 DO UPDATE SET authority_arm = EXCLUDED.authority_arm",
            )
            .bind(CHAIN)
            .bind(&child)
            .bind(arm)
            .execute(&mut *conn)
            .await?;
            let selected = page(&mut conn, PARENT, &filter, None, None, CHILDREN as u64).await?;
            let row = selected
                .rows
                .iter()
                .find(|row| row.child_logical_name_id == child);
            match arm {
                Some("ens_v1") => {
                    ensure!(row.is_some_and(|row| row.owner.is_some() && row.registrant.is_none()))
                }
                Some("ens_v2") => {
                    ensure!(row.is_some_and(|row| row.owner.is_none()
                        && row.registrant.as_deref() == Some("0xregistrant")))
                }
                _ => ensure!(row.is_none()),
            }
        }
        // A later same-node edge under a different parent still removes the old parent's
        // registry candidate; skipping a row's comparison against itself must not skip this.
        sqlx::query("DELETE FROM project_name_summary WHERE logical_name_id = $1")
            .bind(&child)
            .execute(&mut *conn)
            .await?;
        raw_sql(&format!(
            "INSERT INTO project_child_edge_candidate
             SELECT chain_id, namespace, 'other-parent', child_node, authority_arm,
                    block_number, transaction_index, log_index + 1, 'moved:0',
                    normalized_event_id, owner, owner_getter, labelhash, source_family
             FROM project_child_edge_candidate WHERE child_node = '{}'",
            child.strip_prefix("ens:").unwrap()
        ))
        .execute(&mut *conn)
        .await?;
        let moved = page(&mut conn, PARENT, &filter, None, None, CHILDREN as u64).await?;
        ensure!(moved.total_count == CHILDREN as u64);
        ensure!(
            moved
                .rows
                .iter()
                .find(|row| row.child_logical_name_id == child)
                .is_some_and(|row| row.registrant.as_deref() == Some("0xregistrant"))
        );
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

/// The released-lease and token-holder probes of a child with no name surface enter
/// `project_lifecycle_event_namehash_idx` by chain and child node, never by chain alone, and
/// read the newest lease event at or below the clock: a release, then a later grant, then a
/// transfer, whose recipient holds the lease.
#[tokio::test]
async fn the_released_lease_probe_reads_the_namehash_index() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("children_released_lease_plan").pool_max_connections(1),
    )
    .await?;
    let result = async {
        let mut conn = database.pool().acquire().await?;
        install(&mut conn).await?;
        let child = child_id(1);
        let node = child.strip_prefix("ens:").unwrap();
        let lease_to = |identity: &str, kind: &str, block: i64, position: &str, to: &str| {
            format!(
                "INSERT INTO project_lifecycle_event(chain_id, state_kind, state_key,
                     block_number, transaction_index, log_index, event_identity, event_kind,
                     source_family, namehash, to_address)
                 VALUES ('{CHAIN}', 'resource', 'lease', {block}, {position}, '{identity}',
                     '{kind}', 'ens_v1_registrar_l1', '{node}', NULLIF('{to}', ''));"
            )
        };
        let lease = |identity: &str, kind: &str, block: i64, position: &str| {
            lease_to(identity, kind, block, position, "")
        };
        raw_sql(&format!(
            "{}{}
             INSERT INTO project_lifecycle_event(chain_id, state_kind, state_key, block_number,
                 event_identity, event_kind, source_family, namehash)
             SELECT '{CHAIN}', 'resource', 'other:' || n, 90, 'other:' || n,
                 'RegistrationReleased', 'ens_v1_registrar_l1', '0x' || lpad(to_hex(n), 64, '0')
             FROM generate_series(2, {CHILDREN}) n;
             ANALYZE project_lifecycle_event; SET enable_seqscan = off;",
            lease("grant", "RegistrationGranted", 90, "0, 0"),
            lease("release", "RegistrationReleased", 100, "NULL, NULL"),
        ))
        .execute(&mut *conn)
        .await?;
        let filter = ChildrenCurrentPageFilter::default();
        let query = page_query(PARENT, &filter, None, None, 6);
        raw_sql(&format!(
            "PREPARE children (text, text, bigint) AS {}",
            query.sql()
        ))
        .execute(&mut *conn)
        .await?;
        let plan: Value = sqlx::query_scalar(&format!(
            "EXPLAIN (FORMAT JSON) EXECUTE children ('{PARENT}', '{}', 6)",
            bigname_content_hash::INTERPRETER_CONTENT_HASH
        ))
        .fetch_one(&mut *conn)
        .await?;
        let probes = lifecycle_scans(&plan[0]["Plan"]);
        ensure!(!probes.is_empty(), "no lifecycle probe: {plan}");
        for probe in probes {
            let condition = probe["Index Cond"].as_str().unwrap_or_default();
            ensure!(
                probe["Index Name"] == "project_lifecycle_event_namehash_idx"
                    && condition.contains("chain_id")
                    && condition.contains("namehash"),
                "the probe does not enter the namehash index by chain and node: {probe}"
            );
        }
        let served = |page: &FamilyChildrenPage| {
            page.rows
                .iter()
                .find(|row| row.child_logical_name_id == child)
                .map(|row| (row.released_lease, row.token_holder.clone()))
        };
        let first = page(&mut conn, PARENT, &filter, None, None, 5).await?;
        ensure!(served(&first) == Some((true, None)), "{first:?}");
        raw_sql(&lease("again", "RegistrationGranted", 100, "0, 0"))
            .execute(&mut *conn)
            .await?;
        let again = page(&mut conn, PARENT, &filter, None, None, 5).await?;
        ensure!(served(&again) == Some((false, None)), "{again:?}");
        let buyer = "0x2222222222222222222222222222222222222222";
        raw_sql(&lease_to(
            "late",
            "TokenControlTransferred",
            101,
            "0, 0",
            buyer,
        ))
        .execute(&mut *conn)
        .await?;
        let late = page(&mut conn, PARENT, &filter, None, None, 5).await?;
        ensure!(served(&late) == Some((false, None)), "{late:?}");
        raw_sql(&lease_to(
            "sold",
            "TokenControlTransferred",
            100,
            "0, 1",
            buyer,
        ))
        .execute(&mut *conn)
        .await?;
        let sold = page(&mut conn, PARENT, &filter, None, None, 5).await?;
        ensure!(
            served(&sold) == Some((false, Some(buyer.to_owned()))),
            "{sold:?}"
        );
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

fn lifecycle_scans(plan: &Value) -> Vec<&Value> {
    let mut scans: Vec<&Value> = Vec::new();
    if plan["Relation Name"] == "project_lifecycle_event" {
        scans.push(plan);
    }
    for child in plan["Plans"].as_array().into_iter().flatten() {
        scans.extend(lifecycle_scans(child));
    }
    scans
}

/// The child counts read no served field of a child, so the planner drops the lifecycle probes
/// of the children relation from their statements: a parent count never probes the lifecycle
/// table, however many of its children have no name surface.
#[tokio::test]
async fn child_counts_do_not_probe_the_lifecycle_table() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("children_count_lifecycle_plan").pool_max_connections(1),
    )
    .await?;
    let result = async {
        let mut conn = database.pool().acquire().await?;
        install(&mut conn).await?;
        let parents = vec![PARENT.to_owned()];
        for (name, types, query, arguments) in [
            (
                "one_count",
                "text, text",
                count_query(PARENT, None),
                format!("'{PARENT}'"),
            ),
            (
                "many_counts",
                "text[], text",
                counts_query(&parents),
                format!("ARRAY['{PARENT}']"),
            ),
        ] {
            ensure!(
                !query.sql().contains("child_node = ANY("),
                "{name} narrows its children"
            );
            raw_sql(&format!("PREPARE {name} ({types}) AS {}", query.sql()))
                .execute(&mut *conn)
                .await?;
            let plan: Value = sqlx::query_scalar(&format!(
                "EXPLAIN (FORMAT JSON) EXECUTE {name} ({arguments}, '{}')",
                bigname_content_hash::INTERPRETER_CONTENT_HASH
            ))
            .fetch_one(&mut *conn)
            .await?;
            ensure!(
                lifecycle_scans(&plan[0]["Plan"]).is_empty(),
                "{name} probes the lifecycle table: {plan}"
            );
        }
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

/// The surface-less children an address owns or manages are read by candidate node: the edge
/// scan enters `project_child_edge_candidate_child_idx` by the candidates' nodes instead of
/// enumerating every edge of the parent, which the candidate filter outside the relation, an
/// expression over the name id, cannot do. The page statement is not narrowed.
#[tokio::test]
async fn owned_registry_children_read_only_their_candidates() -> Result<()> {
    use crate::families::topology::registry_children::owned_children_query;

    let database = TestDatabase::create(
        TestDatabaseConfig::new("owned_registry_children_plan").pool_max_connections(1),
    )
    .await?;
    let result = async {
        let mut conn = database.pool().acquire().await?;
        install(&mut conn).await?;
        let child = child_id(1);
        let node = child.strip_prefix("ens:").unwrap().to_owned();
        let owner = "0x1111111111111111111111111111111111111111";
        raw_sql(&format!(
            "INSERT INTO project_registry_node_state(chain_id, namespace, node, block_number,
                 transaction_index, log_index, event_identity, owner, owner_resource_id)
             VALUES ('{CHAIN}', 'ens', '{node}', 100, 0, 1, 'child:1:0', '{owner}',
                 '00000000-0000-0000-0000-000000000011');
             ANALYZE project_registry_node_state;"
        ))
        .execute(&mut *conn)
        .await?;
        let filter = ChildrenCurrentPageFilter::default();
        ensure!(
            !page_query(PARENT, &filter, None, None, 6)
                .sql()
                .contains("child_node = ANY(")
        );
        let parents = vec![PARENT.to_owned()];
        let candidates = vec![child.clone()];
        let nodes = vec![node];
        let query = owned_children_query(owner, &candidates, &nodes, &parents, 100);
        raw_sql(&format!(
            "PREPARE owned (text[], text, text[], text[], text[], text, text, bigint) AS {}",
            query.sql()
        ))
        .execute(&mut *conn)
        .await?;
        for mode in ["force_custom_plan", "force_generic_plan"] {
            raw_sql(&format!("SET plan_cache_mode = {mode}"))
                .execute(&mut *conn)
                .await?;
            let plan: Value = sqlx::query_scalar(&format!(
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE owned (ARRAY['{PARENT}'],
                     '{hash}', ARRAY['{node}'], ARRAY['{node}'], ARRAY['{child}'], '{owner}',
                     '{owner}', 100)",
                node = nodes[0],
                hash = bigname_content_hash::INTERPRETER_CONTENT_HASH
            ))
            .fetch_one(&mut *conn)
            .await?;
            ensure!(plan[0]["Plan"]["Actual Rows"] == 1, "{mode}: {plan}");
            let scans = edge_scans(&plan[0]["Plan"]);
            ensure!(
                scans.iter().any(|scan| {
                    scan["Index Name"] == "project_child_edge_candidate_child_idx"
                        && scan["Index Cond"]
                            .as_str()
                            .is_some_and(|condition| condition.contains("child_node = ANY"))
                }),
                "{mode}: no edge scan by candidate node: {plan}"
            );
            // Rows a scan examined: those it emitted and those its filter or recheck dropped.
            for scan in scans {
                let examined = [
                    "Actual Rows",
                    "Rows Removed by Filter",
                    "Rows Removed by Index Recheck",
                ]
                .iter()
                .map(|key| scan[*key].as_f64().unwrap_or_default())
                .sum::<f64>()
                    * scan["Actual Loops"].as_f64().unwrap_or_default();
                ensure!(
                    examined <= candidates.len() as f64,
                    "{mode}: an edge scan examines more rows than the candidates: {scan}"
                );
            }
        }
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

fn edge_scans(plan: &Value) -> Vec<&Value> {
    let mut scans: Vec<&Value> = Vec::new();
    if plan["Relation Name"] == "project_child_edge_candidate" {
        scans.push(plan);
    }
    for child in plan["Plans"].as_array().into_iter().flatten() {
        scans.extend(edge_scans(child));
    }
    scans
}
