//! Differential guards for the publication writer's complete five-field holdings.
//! The baseline freezes the actual statement at d6a04b6a; shared predicates remain shared.
//! An intentional membership-rule change must update or remove this oracle.
use super::*;
use anyhow::{Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::PgConnection;
use uuid::Uuid;

type Holding = (String, String, Option<String>, Option<Uuid>, i16);
const CHAIN: &str = "ethereum-mainnet";
const NAME: &str = "ens:wanted";
const OTHER_NAME: &str = "ens:other";

fn resource(n: u128) -> Uuid {
    Uuid::from_u128(n)
}
fn holding(address: &str, name: Option<&str>, id: Option<u128>, mask: i16) -> Holding {
    (
        address.into(),
        "ens".into(),
        name.map(str::to_owned),
        id.map(resource),
        mask,
    )
}
fn sorted(rows: Vec<HistoricalHistoryRelation>) -> Vec<Holding> {
    let mut rows: Vec<_> = rows
        .into_iter()
        .map(|r| {
            (
                r.address,
                r.namespace,
                r.logical_name_id,
                r.resource_id,
                r.relation_mask,
            )
        })
        .collect();
    rows.sort();
    rows
}
async fn assert_holdings(
    c: &mut PgConnection,
    names: &[String],
    resources: &[Uuid],
    mut expected: Vec<Holding>,
) -> Result<()> {
    expected.sort();
    let baseline = sorted(baseline_relations(c, CHAIN, 5, names, resources).await?);
    let candidate = sorted(historical_history_relations(c, CHAIN, 5, names, resources).await?);
    ensure!(
        baseline == expected,
        "baseline literal holdings: {baseline:?} != {expected:?}"
    );
    ensure!(
        candidate == expected,
        "candidate literal holdings: {candidate:?} != {expected:?}"
    );
    Ok(())
}
async fn with_fixture(
    prefix: &str,
    check: impl AsyncFnOnce(&mut PgConnection) -> Result<()>,
) -> Result<()> {
    let db = TestDatabase::create(TestDatabaseConfig::new(prefix).pool_max_connections(1)).await?;
    let result = async {
        let mut c = db.pool().acquire().await?;
        sqlx::raw_sql("CREATE SCHEMA bigname_phase; SET search_path TO bigname_phase, public")
            .execute(&mut *c).await?;
        for baseline in [
            include_str!("../../schema/baseline/01_chain.sql"),
            include_str!("../../schema/baseline/02_raw_facts.sql"),
            include_str!("../../schema/baseline/03_identity.sql"),
            include_str!("../../schema/baseline/04_manifests.sql"),
            include_str!("../../schema/baseline/05_normalized_events.sql"),
            include_str!("../../schema/baseline/06_projections.sql"),
        ] { sqlx::raw_sql(baseline).execute(&mut *c).await?; }
        sqlx::raw_sql(r#"
            INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
            SELECT 'ethereum-mainnet', 'block-' || n, n, to_timestamp(n), 'canonical'::canonicality_state
            FROM generate_series(1, 11) n;
            INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
            VALUES ('ethereum-mainnet', 'orphan-block', 20, to_timestamp(20), 'orphaned'),
                   ('base-mainnet', 'other-chain', 1, to_timestamp(1), 'canonical');
            INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
                namehash, labelhashes, normalizer_version, visibility_state, chain_id, block_hash,
                block_number, canonicality_state)
            SELECT name, 'ens', name, ARRAY[name], '\x00'::bytea, split_part(name, ':', 2), ARRAY[name], 'test', 'active',
                'ethereum-mainnet', 'block-1', 1, 'canonical'::canonicality_state
            FROM (VALUES ('ens:wanted'), ('ens:other'), ('ens:empty')) names(name);
            INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state)
            SELECT lpad(to_hex(n), 32, '0')::uuid, 'ethereum-mainnet', 'block-1', 1, 'canonical'::canonicality_state
            FROM generate_series(1, 9) n;
            INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state)
            VALUES ('00000000-0000-0000-0000-00000000000a', 'ethereum-mainnet', 'orphan-block', 20, 'canonical'),
                   ('00000000-0000-0000-0000-00000000000b', 'ethereum-mainnet', 'block-1', 1, 'observed'),
                   ('00000000-0000-0000-0000-00000000000c', 'base-mainnet', 'other-chain', 1, 'canonical');
            SET statement_timeout='30s';
        "#).execute(&mut *c).await?;
        check(&mut c).await
    }.await;
    db.cleanup().await?;
    result
}
#[allow(clippy::too_many_arguments)]
async fn event(
    c: &mut PgConnection,
    id: &str,
    name: Option<&str>,
    res: Option<u128>,
    kind: &str,
    block: Option<i64>,
    state: Value,
) -> Result<()> {
    sqlx::query("INSERT INTO normalized_events (event_identity, namespace, logical_name_id, resource_id,
        event_kind, source_family, manifest_version, chain_id, block_number, block_hash,
        transaction_hash, transaction_index, log_index, derivation_kind, canonicality_state, after_state)
        VALUES ($1, 'ens', $2, $3, $4, 'ens_v2_registry_l1', 1, $5, $6, $7, $8, $9, $9,
        'ens_v2_registry_resource_surface', 'canonical', $10)")
        .bind(id).bind(name).bind(res.map(resource)).bind(kind).bind(CHAIN).bind(block)
        .bind(block.map(|n| format!("block-{n}"))).bind(block.map(|_| format!("tx-{id}")))
        .bind(block.map(|_| 0_i64)).bind(state).execute(c).await?;
    Ok(())
}

#[tokio::test]
async fn evidence_arms_keep_name_resource_nameless_and_repeated_keys() -> Result<()> {
    with_fixture("publication_membership_arms", async |c| {
        let cases = [("both", Some(NAME), Some(1)), ("resource", Some(OTHER_NAME), Some(2)),
                     ("nameless", None, Some(3)), ("name", Some(NAME), Some(4)),
                     ("empty-name", Some("ens:empty"), Some(5))];
        for (id, name, res) in cases {
            event(c, id, name, res, "TokenControlTransferred", Some(2), json!({"to": id.to_uppercase()})).await?;
        }
        event(c, "unrelated", Some(OTHER_NAME), Some(6), "TokenControlTransferred", Some(2), json!({"to":"unrelated"})).await?;
        sqlx::raw_sql("INSERT INTO normalized_events (event_identity, namespace, resource_id, event_kind,
            source_family, manifest_version, chain_id, block_number, block_hash, derivation_kind, canonicality_state, after_state)
            VALUES ('other-chain', 'basenames', '00000000-0000-0000-0000-00000000000c', 'TokenControlTransferred',
            'basenames_base_registry', 1, 'base-mainnet', 1, 'other-chain', 'ens_v2_registry_resource_surface', 'canonical', '{\"to\":\"other-chain\"}')")
            .execute(&mut *c).await?;
        let names = vec![NAME.into()]; let resources = vec![resource(1), resource(2), resource(3), resource(5), resource(12)];
        let all = vec![holding("both", Some(NAME), Some(1), 1), holding("resource", Some(OTHER_NAME), Some(2), 1),
            holding("nameless", None, Some(3), 1), holding("name", Some(NAME), Some(4), 1),
            holding("empty-name", Some("ens:empty"), Some(5), 1)];
        assert_holdings(c, &names, &resources, all.clone()).await?;
        assert_holdings(c, &[NAME.into(), NAME.into()], &resources.repeat(2), all.clone()).await?;
        let mut reversed = resources.clone(); reversed.reverse();
        assert_holdings(c, &names, &reversed, all).await?;
        assert_holdings(c, &[], &[], vec![]).await?;
        assert_holdings(c, &[String::new()], &[], vec![]).await?;
        assert_holdings(c, &names, &[], vec![holding("both", Some(NAME), Some(1), 1), holding("name", Some(NAME), Some(4), 1)]).await?;
        assert_holdings(c, &[], &resources, vec![holding("both", Some(NAME), Some(1), 1), holding("resource", Some(OTHER_NAME), Some(2), 1),
            holding("nameless", None, Some(3), 1), holding("empty-name", Some("ens:empty"), Some(5), 1)]).await?;
        Ok(())
    }).await
}

#[tokio::test]
async fn publication_bounds_and_eligibility_keep_only_readable_evidence() -> Result<()> {
    with_fixture("publication_membership_eligibility", async |c| {
        for (id, block, res) in [("eligible", Some(2), 1), ("future", Some(6), 1), ("unpositioned", None, 1),
            ("orphan-event", Some(2), 1), ("observed", Some(2), 1), ("candidate", Some(2), 1),
            ("orphan-lineage", Some(2), 1), ("unreadable-resource", Some(2), 11),
            ("orphan-resource-lineage", Some(2), 10), ("wrong-derivation", Some(2), 1)] {
            event(c, id, Some(NAME), Some(res), "TokenControlTransferred", block, json!({"to":id})).await?;
        }
        event(c, "wrong-kind", Some(NAME), Some(1), "RecordChanged", Some(2), json!({"to":"wrong-kind"})).await?;
        sqlx::raw_sql(r#"
            UPDATE normalized_events SET canonicality_state='orphaned' WHERE event_identity='orphan-event';
            UPDATE normalized_events SET canonicality_state='observed' WHERE event_identity='observed';
            UPDATE normalized_events SET consumer_visibility='candidate', migration_correlation_ids=ARRAY['candidate'] WHERE event_identity='candidate';
            UPDATE normalized_events SET block_hash='orphan-block', block_number=20 WHERE event_identity='orphan-lineage';
            UPDATE normalized_events SET derivation_kind='ens_v2_permissions' WHERE event_identity='wrong-derivation';
        "#).execute(&mut *c).await?;
        // An orphaned lineage below the publication bound isolates lineage from the bound.
        sqlx::raw_sql("INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
            VALUES ('ethereum-mainnet', 'orphan-2', 2, to_timestamp(2), 'orphaned');
            UPDATE normalized_events SET block_hash='orphan-2', block_number=2 WHERE event_identity='orphan-lineage';")
            .execute(&mut *c).await?;
        assert_holdings(c, &[NAME.into()], &[resource(1), resource(10), resource(11)],
            vec![holding("eligible", Some(NAME), Some(1), 1)]).await
    }).await
}

#[tokio::test]
async fn positional_handoff_removes_owner_but_keeps_controller() -> Result<()> {
    with_fixture("publication_membership_handoff", async |c| {
        event(c, "before", Some(NAME), Some(1), "AuthorityTransferred", Some(3), json!({"owner":"BEFORE"})).await?;
        event(c, "after", Some(NAME), Some(1), "AuthorityTransferred", Some(3), json!({"owner":"AFTER"})).await?;
        event(c, "lease", None, Some(2), "RegistrationRenewed", Some(11), json!({})).await?;
        sqlx::raw_sql(r#"
            UPDATE normalized_events SET derivation_kind='ens_v1_unwrapped_authority', source_family='ens_v1_registry_l1',
                transaction_index=1, log_index=CASE event_identity WHEN 'before' THEN 0 ELSE 2 END
                WHERE event_identity IN ('before','after');
            UPDATE normalized_events SET source_family='ens_v1_registrar_l1', canonicality_state='observed',
                consumer_visibility='candidate', migration_correlation_ids=ARRAY['lease'] WHERE event_identity='lease';
            INSERT INTO project_binding_candidate (surface_binding_id, logical_name_id, namespace, chain_id,
                authority_arm, resource_id, binding_kind, canonicality_state, block_number, transaction_index, log_index,
                event_identity, registry_only, lease_resource_id)
            VALUES ('00000000-0000-0000-0000-0000000000f0', 'ens:wanted', 'ens', 'ethereum-mainnet', 'test',
                '00000000-0000-0000-0000-000000000001', 'test', 'canonical', 3, 1, 1,
                'handoff', true, '00000000-0000-0000-0000-000000000002');
        "#).execute(&mut *c).await?;
        assert_holdings(c, &[NAME.into()], &[resource(1)], vec![holding("before", Some(NAME), Some(1), 1),
            holding("before", Some(NAME), Some(1), 2), holding("after", Some(NAME), Some(1), 2)]).await
    }).await
}

#[tokio::test]
async fn wrapper_exclusion_matches_only_the_grants_transaction() -> Result<()> {
    with_fixture("publication_membership_wrapper", async |c| {
        for (id, res) in [("same", 1), ("other", 2)] {
            event(c, id, Some(NAME), Some(res), "RegistrationGranted", Some(2), json!({"registrant":id,"namehash":id})).await?;
            event(c, &format!("wrap-{id}"), None, None, "TokenControlTransferred", Some(2),
                json!({"wrapped_registrar_resource_id":resource(res).to_string(),"source_event":"NameWrapped","node":id})).await?;
        }
        sqlx::raw_sql(r#"
            UPDATE normalized_events SET source_family='ens_v1_registrar_l1', derivation_kind='ens_v1_unwrapped_authority'
                WHERE event_identity IN ('same','other');
            INSERT INTO token_lineages (token_lineage_id, chain_id, block_hash, block_number, canonicality_state)
                SELECT resource_id, chain_id, block_hash, block_number, canonicality_state FROM resources
                WHERE resource_id IN ('00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-000000000002');
            UPDATE resources SET token_lineage_id=resource_id
                WHERE resource_id IN ('00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-000000000002');
            UPDATE normalized_events SET source_family='ens_v1_wrapper_l1',
                raw_fact_ref=jsonb_build_object('emitting_address', CASE event_identity WHEN 'wrap-same' THEN 'SAME' ELSE 'OTHER' END),
                transaction_index=CASE event_identity WHEN 'wrap-same' THEN 0 ELSE 1 END
                WHERE event_identity IN ('wrap-same','wrap-other');
        "#).execute(&mut *c).await?;
        assert_holdings(c, &[NAME.into()], &[resource(1),resource(2)], vec![holding("other", Some(NAME), Some(2), 1)]).await
    }).await
}

/// One frozen, low-memory synthetic comparison. The old wide UNION must spill; the
/// narrow candidate must reduce both total temp counters, including independent joins.
/// Access paths may differ. Source work is attributed to wanted-key arms, not index names.
/// Calls and EXECUTE warm the fixture before each EXPLAIN; timings are descriptive only.
#[tokio::test]
async fn narrow_publication_evidence_reduces_spilled_temp_work() -> Result<()> {
    with_fixture("publication_membership_plans", async |c| {
        install_plan_fixture(c).await?;
        let names: Vec<_> = (0..3).map(|n| format!("ens:plan-{n}")).collect();
        let resources: Vec<_> = (100..109).map(resource).collect();
        let mut literal = Vec::new();
        for n in 100..109 {
            for address in ["holder-0", "holder-1"] {
                for mask in [1,2] {
                    literal.push(holding(address, Some(&format!("ens:plan-{}", (n-100)/3)), Some(n), mask));
                }
            }
        }
        literal.sort();
        sqlx::raw_sql("ANALYZE; BEGIN; SET LOCAL work_mem='256kB'; SET LOCAL hash_mem_multiplier=2; SET LOCAL jit=off")
            .execute(&mut *c).await?;
        let mut captures = Vec::new();
        for mode in ["force_custom_plan", "force_generic_plan"] {
            sqlx::raw_sql(&format!("SET LOCAL plan_cache_mode={mode}"))
                .execute(&mut *c).await?;
            let old = sorted(baseline_relations(c, CHAIN, 5, &names, &resources).await?);
            ensure!(old == literal, "baseline production literal holdings: {old:?} != {literal:?}");
            let baseline = capture(c, &names, &resources, true, &literal).await?;
            save_pair(&json!({"completed_modes":captures,"current_mode":mode,"baseline":baseline,
                "literal_holdings":literal,"state":"baseline captured before comparison"}))?;
            let new = sorted(historical_history_relations(c, CHAIN, 5, &names, &resources).await?);
            ensure!(new == literal, "candidate production literal holdings: {new:?} != {literal:?}");
            let candidate = capture(c, &names, &resources, false, &literal).await?;
            let mut pair = json!({"mode":mode,"baseline":baseline,"candidate":candidate,
                "literal_holdings":literal,"state":"both captures saved before cost acceptance"});
            save_pair(&json!({"completed_modes":captures,"current_pair":pair}))?;
            pair["baseline_report"] = evidence_plan_report(&pair["baseline"], false)?;
            pair["candidate_report"] = evidence_plan_report(&pair["candidate"], true)?;
            save_pair(&json!({"completed_modes":captures,"current_pair":pair}))?;
            ensure!(pair["baseline_report"]["wide_evidence_spills"].as_array().is_some_and(|v| !v.is_empty()),
                "baseline wide-evidence spill did not reproduce in {mode}; STOP without tuning: {pair}");
            ensure!(pair["candidate_report"]["wide_evidence_dedupes"].as_array().is_some_and(Vec::is_empty),
                "candidate still deduplicates wide evidence in {mode}; STOP: {pair}");
            for counter in ["Temp Read Blocks", "Temp Written Blocks"] {
                let old = pair["baseline"]["plan"][0]["Plan"][counter].as_u64();
                let new = pair["candidate"]["plan"][0]["Plan"][counter].as_u64();
                ensure!(old.zip(new).is_some_and(|(old,new)| new < old),
                    "candidate {counter} is not strictly lower in {mode}; STOP: {pair}");
            }
            pair["state"] = json!("paired finite acceptance passed");
            captures.push(pair);
            save_pair(&json!({"completed_modes":captures}))?;
        }
        sqlx::raw_sql("ROLLBACK").execute(&mut *c).await?;
        Ok(())
    }).await
}

fn save_pair(value: &Value) -> Result<()> {
    if let Some(path) = std::env::var_os("BIGNAME_MEMBERSHIP_PLANS") {
        std::fs::write(path, serde_json::to_vec_pretty(value)?)?;
    }
    Ok(())
}

/// Paths/counters over the existing main plan only. Matcher SubPlans are not evidence arms.
fn plan_nodes<'a>(node: &'a Value, path: String, output: &mut Vec<(String, &'a Value)>) {
    if node["Parent Relationship"] == "SubPlan" {
        return;
    }
    output.push((path.clone(), node));
    for (index, child) in node["Plans"].as_array().into_iter().flatten().enumerate() {
        plan_nodes(child, format!("{path}.Plans[{index}]"), output);
    }
}
fn below(path: &str, ancestor: &str) -> bool {
    path == ancestor || path.starts_with(&format!("{ancestor}.Plans["))
}

fn evidence_plan_report(capture: &Value, candidate: bool) -> Result<Value> {
    let sql = capture["statement"].as_str().unwrap_or("");
    let compact = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    ensure!(
        compact.contains("wanted(name)")
            && compact.contains("wanted(resource)")
            && compact.contains("ne.logical_name_id = wanted.name AND ne.chain_id =")
            && compact.contains("ne.resource_id = wanted.resource AND ne.chain_id =")
            && compact.matches("AND ne.block_number <=").count() == 2,
        "UNPROVEN: emitted SQL does not establish the wanted key/chain/block arms"
    );
    if candidate {
        let evidence = compact
            .split("SELECT DISTINCT matched_address.address")
            .next()
            .unwrap_or("");
        let columns = [
            "ne.namespace",
            "ne.logical_name_id",
            "ne.resource_id",
            "ne.event_kind",
            "ne.source_family",
            "ne.chain_id",
            "ne.block_number",
            "ne.block_hash",
            "ne.transaction_index",
            "ne.log_index",
            "ne.derivation_kind",
            "ne.canonicality_state",
            "ne.consumer_visibility",
            "ne.after_state",
        ];
        let projections: Vec<_> = evidence
            .split("SELECT ")
            .skip(1)
            .map(|select| {
                select
                    .split(" FROM ")
                    .next()
                    .unwrap_or("")
                    .split(',')
                    .map(str::trim)
                    .collect::<Vec<_>>()
            })
            .collect();
        ensure!(
            projections.len() == 4
                && projections.iter().all(|p| p == &columns)
                && evidence.contains("UNION ALL")
                && evidence.contains("ne.logical_name_id IS NULL OR ne.logical_name_id <> ALL("),
            "UNPROVEN: actual candidate evidence projection is not the frozen disjoint 14-column SQL"
        );
    }
    let mut nodes = Vec::new();
    plan_nodes(&capture["plan"][0]["Plan"], "Plan".into(), &mut nodes);
    let appends: Vec<_> = nodes
        .iter()
        .filter(|(path, node)| {
            node["Node Type"] == "Append"
                && nodes
                    .iter()
                    .filter(|(p, n)| {
                        below(p, path)
                            && n["Node Type"] == "Function Scan"
                            && n["Function Name"] == "unnest"
                    })
                    .count()
                    == 2
        })
        .collect();
    ensure!(
        appends.len() == 1,
        "UNPROVEN: evidence Append ancestry is erased or ambiguous"
    );
    let append = &appends[0].0;
    let mut roles = Vec::new();
    for (arm_index, arm) in appends[0].1["Plans"]
        .as_array()
        .into_iter()
        .flatten()
        .enumerate()
    {
        let branch = format!("{append}.Plans[{arm_index}]");
        let subtree: Vec<_> = nodes.iter().filter(|(p, _)| below(p, &branch)).collect();
        let functions: Vec<_> = subtree
            .iter()
            .filter(|(_, n)| n["Node Type"] == "Function Scan" && n["Function Name"] == "unnest")
            .collect();
        ensure!(
            functions.len() == 1,
            "UNPROVEN: wanted function is missing/ambiguous in {branch}"
        );
        let (function_path, function) = functions[0];
        let alias = function["Alias"].as_str().unwrap_or("");
        ensure!(
            !alias.is_empty(),
            "UNPROVEN: wanted function alias is absent"
        );
        let roots: Vec<_> = subtree
            .iter()
            .filter(|(_, n)| {
                n["Relation Name"] == "normalized_events"
                    && matches!(
                        n["Node Type"].as_str(),
                        Some(
                            "Seq Scan"
                                | "Index Scan"
                                | "Index Only Scan"
                                | "Bitmap Heap Scan"
                                | "Tid Scan"
                                | "Tid Range Scan"
                        )
                    )
            })
            .collect();
        ensure!(
            !roots.is_empty(),
            "UNPROVEN: physical source root was pruned without role markers in {branch}"
        );
        let mut role = None;
        let mut scans = Vec::new();
        let mut loops = 0.0;
        for (path, node) in roots {
            let conditions: Vec<_> = subtree
                .iter()
                .filter(|(p, _)| below(p, path))
                .flat_map(|(_, n)| {
                    ["Filter", "Index Cond", "Recheck Cond"]
                        .into_iter()
                        .filter_map(|k| n[k].as_str())
                })
                .collect();
            let joined = conditions.join(" ");
            let name =
                joined.contains("logical_name_id") && joined.contains(&format!("{alias}.name"));
            let resource =
                joined.contains("resource_id") && joined.contains(&format!("{alias}.resource"));
            ensure!(
                name != resource && joined.contains("chain_id") && joined.contains("block_number"),
                "UNPROVEN: key/chain/block source correlation in {path}: {conditions:?}"
            );
            let current = if name { "name" } else { "resource" };
            ensure!(
                role.is_none_or(|role| role == current),
                "UNPROVEN: mixed roles in {branch}"
            );
            role = Some(current);
            let actual = node["Actual Loops"]
                .as_f64()
                .ok_or_else(|| anyhow::anyhow!("UNPROVEN: loops absent at {path}"))?;
            loops += actual;
            scans.push(json!({"path":path,"node":node,"conditions":conditions}));
        }
        let role = role.ok_or_else(|| anyhow::anyhow!("UNPROVEN: source role absent"))?;
        let limit = if role == "name" { 3.0 } else { 9.0 };
        ensure!(
            !candidate || loops <= limit,
            "candidate {role} source scans {loops} exceed {limit}"
        );
        let ancestry: Vec<_> = nodes
            .iter()
            .filter(|(p, _)| below(&branch, p) || below(p, &branch))
            .filter(|(_, n)| {
                matches!(
                    n["Node Type"].as_str(),
                    Some(
                        "Materialize"
                            | "Sort"
                            | "Unique"
                            | "Aggregate"
                            | "Append"
                            | "Subquery Scan"
                    )
                )
            })
            .map(|(p, n)| json!({"path":p,"node":n}))
            .collect();
        roles.push(
            json!({"role":role,"branch_path":branch,"branch_loops":arm["Actual Loops"],
            "wanted_function_path":function_path,"wanted_function":function,"source_scans":scans,
            "source_loops":loops,"source_limit":limit,"ancestry":ancestry}),
        );
    }
    ensure!(
        roles.len() == 2
            && roles.iter().filter(|r| r["role"] == "name").count() == 1
            && roles.iter().filter(|r| r["role"] == "resource").count() == 1,
        "UNPROVEN: both wanted source roles are not positively accounted for"
    );
    let wide: Vec<_> = nodes
        .iter()
        .filter(|(path, node)| {
            below(append, path)
                && matches!(node["Node Type"].as_str(), Some("Sort" | "Aggregate"))
                && ["Sort Key", "Group Key"].iter().any(|key| {
                    node[key].as_array().is_some_and(|keys| {
                        [
                            "raw_fact_ref",
                            "before_state",
                            "event_identity",
                            "manifest_version",
                        ]
                        .iter()
                        .all(|column| {
                            keys.iter().any(|k| {
                                k.as_str()
                                    .is_some_and(|k| k.ends_with(&format!(".{column}")))
                            })
                        })
                    })
                })
        })
        .map(|(path, node)| json!({"path":path,"node":node}))
        .collect();
    let spills: Vec<_> = wide
        .iter()
        .filter(|v| {
            v["node"]["Temp Written Blocks"]
                .as_u64()
                .is_some_and(|v| v > 0)
                && (v["node"]["Sort Space Type"] == "Disk"
                    || v["node"]["Disk Usage"].as_u64().is_some_and(|v| v > 0)
                    || v["node"]["HashAgg Batches"].as_u64().is_some_and(|v| v > 1))
        })
        .cloned()
        .collect();
    let operators: Vec<_> = nodes
        .iter()
        .map(|(path, node)| json!({"path":path,"node":node}))
        .collect();
    Ok(
        json!({"evidence_append_path":append,"roles":roles,"wide_evidence_dedupes":wide,
        "wide_evidence_spills":spills,"operators":operators,
        "column_proof":if candidate {"actual four 14-column SQL projections; no Materialize Output/heap-fetch claim"} else {"actual whole-event SQL and event-column dedupe keys"}}),
    )
}

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Capture the prepared statement actually emitted by SQLx, its parameter types and complete
/// EXECUTE result before explaining. Only the frozen baseline and production writer supply SQL.
async fn capture(
    c: &mut PgConnection,
    names: &[String],
    resources: &[Uuid],
    baseline: bool,
    expected: &[Holding],
) -> Result<Value> {
    let tag = if baseline {
        "/* storage:history.publication_memberships_baseline */%"
    } else {
        "/* storage:history.publication_memberships */%"
    };
    let (name, statement, types): (String, String, Vec<String>) = sqlx::query_as(
        "SELECT name, statement, parameter_types::text[] FROM pg_prepared_statements WHERE statement LIKE $1")
        .bind(tag).fetch_one(&mut *c).await?;
    let mut expected_types: Vec<_> = ["text[]", "text", "bigint", "uuid[]", "text", "bigint"]
        .into_iter()
        .map(str::to_owned)
        .collect();
    if !baseline {
        expected_types.push("text[]".into());
    }
    expected_types.extend(std::iter::repeat_n("text".to_owned(), 11));
    ensure!(
        types == expected_types,
        "actual bind types/order: {types:?} != {expected_types:?}"
    );
    let names = format!(
        "ARRAY[{}]::text[]",
        names.iter().map(|v| quote(v)).collect::<Vec<_>>().join(",")
    );
    let resources = format!(
        "ARRAY[{}]::uuid[]",
        resources
            .iter()
            .map(|v| quote(&v.to_string()))
            .collect::<Vec<_>>()
            .join(",")
    );
    let mut args = vec![
        names.clone(),
        quote(CHAIN),
        "5".into(),
        resources,
        quote(CHAIN),
        "5".into(),
    ];
    let eligibility = [
        "ens_v1_unwrapped_authority",
        "ens_v2_registry_resource_surface",
        "RegistrationGranted",
        "TokenControlTransferred",
        "AuthorityTransferred",
    ];
    let matcher = [
        "basenames",
        "ens_v2_registry_resource_surface",
        "basenames",
        "ens_v2_registry_resource_surface",
        "ens_v2_registry_resource_surface",
        "ens_v2_registry_resource_surface",
    ];
    if baseline {
        args.extend(eligibility.map(quote));
        args.extend(matcher.map(quote));
    } else {
        args.push(names);
        args.extend(matcher.map(quote));
        args.extend(eligibility.map(quote));
    }
    ensure!(args.len() == types.len(), "bind count");
    let execute = format!(
        "EXECUTE \"{}\" ({})",
        name.replace('"', "\"\""),
        args.join(", ")
    );
    let mut actual: Vec<Holding> = sqlx::query_as(&execute).fetch_all(&mut *c).await?;
    actual.sort();
    ensure!(
        actual == expected,
        "captured EXECUTE complete holdings differ"
    );
    let plan: Value = sqlx::query_scalar(&format!(
        "EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT JSON) {execute}"
    ))
    .fetch_one(&mut *c)
    .await?;
    let environment: Value = sqlx::query_scalar("SELECT jsonb_build_object('version',version(),
        'work_mem',current_setting('work_mem'),'hash_mem_multiplier',current_setting('hash_mem_multiplier'),
        'jit',current_setting('jit'),'plan_cache_mode',current_setting('plan_cache_mode'))")
        .fetch_one(&mut *c).await?;
    let indexes: Vec<(String,String)> = sqlx::query_as("SELECT indexname,indexdef FROM pg_indexes
        WHERE schemaname='bigname_phase' AND tablename IN ('normalized_events','resources','chain_lineage') ORDER BY indexname")
        .fetch_all(&mut *c).await?;
    let fixture: Value = sqlx::query_scalar(
        "SELECT jsonb_build_object('events',count(*),
        'matching',count(*) FILTER (WHERE event_kind='AuthorityTransferred'),
        'avg_row_bytes',avg(pg_column_size(ne)), 'wanted_names',3,'wanted_resources',9,
        'publication_bound',5) FROM normalized_events ne",
    )
    .fetch_one(&mut *c)
    .await?;
    let statistics: Vec<Value> = sqlx::query_scalar("SELECT to_jsonb(s) FROM pg_stats s
        WHERE schemaname='bigname_phase' AND tablename IN ('normalized_events','resources','chain_lineage') ORDER BY tablename,attname")
        .fetch_all(&mut *c).await?;
    Ok(
        json!({"statement":statement,"parameter_types":types,"parameter_count":types.len(),
        "arguments":args,"production_holdings":expected,"execute_holdings":actual,"plan":plan,"environment":environment,
        "indexes":indexes,"fixture":fixture,"statistics":statistics}),
    )
}

async fn install_plan_fixture(c: &mut PgConnection) -> Result<()> {
    sqlx::raw_sql(r#"
        INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
            namehash, labelhashes, normalizer_version, visibility_state, chain_id, block_hash,
            block_number, canonicality_state)
        SELECT 'ens:plan-' || n, 'ens', 'plan-' || n, ARRAY['plan-' || n], '\x00'::bytea,
            'plan-' || n, ARRAY['plan-' || n], 'test', 'active', 'ethereum-mainnet', 'block-1', 1, 'canonical'::canonicality_state
        FROM generate_series(0, 2) n;
        INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state)
        SELECT lpad(to_hex(n),32,'0')::uuid, 'ethereum-mainnet', 'block-1', 1, 'canonical'::canonicality_state
        FROM generate_series(100,6108) n;
        INSERT INTO normalized_events (event_identity, namespace, logical_name_id, resource_id, event_kind,
            source_family, manifest_version, chain_id, block_hash, block_number, transaction_hash,
            transaction_index, log_index, derivation_kind, canonicality_state, raw_fact_ref, before_state, after_state)
        SELECT 'plan:' || r || ':' || e, 'ens', 'ens:plan-' || ((r-100)/3), lpad(to_hex(r),32,'0')::uuid,
            CASE WHEN e%10=0 THEN 'AuthorityTransferred' ELSE 'RecordChanged' END,
            'ens_v2_registry_l1',1,'ethereum-mainnet','block-2',2,'plan-tx:' || r || ':' || e,0,e,
            'ens_v2_registry_resource_surface','canonical'::canonicality_state,
            jsonb_build_object('payload',padding),jsonb_build_object('payload',padding),
            jsonb_build_object('owner','holder-' || ((e/10)%2))
        FROM generate_series(100,108) r CROSS JOIN generate_series(1,1500) e
        CROSS JOIN LATERAL (SELECT string_agg(md5(r::text || ':' || e || ':' || p), '') AS padding FROM generate_series(1,32) p) pad;
        INSERT INTO normalized_events (event_identity, namespace, logical_name_id, resource_id, event_kind,
            source_family, manifest_version, chain_id, block_hash, block_number, derivation_kind, canonicality_state, after_state)
        SELECT 'noise:' || n, 'ens', 'ens:other', lpad(to_hex(n),32,'0')::uuid,'RecordChanged',
            'ens_v2_registry_l1',1,'ethereum-mainnet','block-2',2,'ens_v2_registry_resource_surface',
            'canonical'::canonicality_state,'{}'::jsonb FROM generate_series(109,6108) n;
    "#).execute(c).await?;
    Ok(())
}

async fn baseline_relations(
    connection: &mut sqlx::PgConnection,
    chain_id: &str,
    block_number: i64,
    names: &[String],
    resources: &[uuid::Uuid],
) -> anyhow::Result<Vec<HistoricalHistoryRelation>> {
    use anyhow::Context;
    let mut query = QueryBuilder::<Postgres>::new(
        "/* storage:history.publication_memberships_baseline */ WITH evidence AS (
        SELECT ne.* FROM unnest(",
    );
    query.push_bind(names).push(
        "::text[]) wanted(name)
        CROSS JOIN LATERAL (SELECT ne.* FROM normalized_events ne
            WHERE ne.logical_name_id = wanted.name AND ne.chain_id = ",
    );
    query
        .push_bind(chain_id)
        .push(" AND ne.block_number <= ")
        .push_bind(block_number);
    query
        .push(
            " AND ne.canonicality_state IN (
                'canonical'::bigname_phase.canonicality_state,
                'safe'::bigname_phase.canonicality_state,
                'finalized'::bigname_phase.canonicality_state
              ) OFFSET 0) ne UNION
        SELECT ne.* FROM unnest(",
        )
        .push_bind(resources)
        .push(
            "::uuid[]) wanted(resource)
        CROSS JOIN LATERAL (SELECT ne.* FROM normalized_events ne
            WHERE ne.resource_id = wanted.resource AND ne.chain_id = ",
        );
    query
        .push_bind(chain_id)
        .push(" AND ne.block_number <= ")
        .push_bind(block_number);
    query.push(
        " AND ne.canonicality_state IN (
            'canonical'::bigname_phase.canonicality_state,
            'safe'::bigname_phase.canonicality_state,
            'finalized'::bigname_phase.canonicality_state
          ) OFFSET 0) ne)
        SELECT DISTINCT matched_address.address, ne.namespace, ne.logical_name_id,
               ne.resource_id, relation.mask AS relation_mask
        FROM evidence ne
        CROSS JOIN LATERAL (
            SELECT DISTINCT lower(address) AS address FROM (VALUES
                (ne.after_state ->> 'registrant'), (ne.after_state ->> 'to'),
                (ne.after_state ->> 'owner')) candidate(address)
            WHERE address IS NOT NULL
        ) matched_address
        CROSS JOIN (VALUES (1::smallint), (2::smallint)) relation(mask)
        LEFT JOIN resources r ON r.resource_id = ne.resource_id
        LEFT JOIN bigname_phase.chain_lineage resource_lineage
          ON resource_lineage.chain_id = r.chain_id
         AND resource_lineage.block_hash = r.block_hash ",
    );
    push_evidence_eligibility(&mut query, true, false);
    query.push(" AND ((relation.mask = 1 AND ");
    push_address_match_filter(
        &mut query,
        Address::Candidate,
        Some(&[AddressNameRelation::TokenHolder]),
    );
    query.push(") OR (relation.mask = 2 AND ");
    push_address_match_filter(
        &mut query,
        Address::Candidate,
        Some(&[AddressNameRelation::EffectiveController]),
    );
    query.push("))");
    query
        .build_query_as()
        .fetch_all(connection)
        .await
        .context("failed to derive historical history membership")
}
