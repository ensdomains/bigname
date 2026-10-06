//! Exercise the complete production membership statement on the parent's real adapter handoff.
use super::*;
use sqlx::PgConnection;

const INDEX: &str = "normalized_events_history_registrar_lease_idx";
const CREATE_INDEX: &str = "CREATE INDEX normalized_events_history_registrar_lease_idx
    ON bigname_phase.normalized_events (resource_id)
    WHERE source_family = 'ens_v1_registrar_l1'
      AND canonicality_state <> 'orphaned'::bigname_phase.canonicality_state";
type Holding = (String, String, Option<String>, Option<Uuid>, i16);

async fn holdings(
    connection: &mut PgConnection,
    names: &[String],
    resources: &[Uuid],
) -> Result<Vec<Holding>> {
    let rows =
        bigname_storage::historical_history_relations(connection, CHAIN, 132, names, resources)
            .await?;
    let mut result: Vec<_> = rows
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
    result.sort();
    Ok(result)
}

/// Explain the statement PostgreSQL actually received, then verify the supplied EXECUTE
/// arguments reproduce the complete result of the production call. No copied query builder.
async fn executed_plan(
    connection: &mut PgConnection,
    names: &[String],
    resources: &[Uuid],
) -> Result<Value> {
    let expected = holdings(connection, names, resources).await?;
    let (statement_name, statement, parameter_count): (String, String, i32) = sqlx::query_as(
        "SELECT name, statement, cardinality(parameter_types) FROM pg_prepared_statements
         WHERE statement LIKE '/* storage:history.publication_memberships */%'",
    )
    .fetch_one(&mut *connection)
    .await?;
    assert_eq!(parameter_count, 17);
    let name_array = names.iter().map(|n| quote(n)).collect::<Vec<_>>().join(",");
    let resource_array = resources
        .iter()
        .map(|id| quote(&id.to_string()))
        .collect::<Vec<_>>()
        .join(",");
    let arguments = format!(
        "ARRAY[{name_array}]::text[], {}, 132, ARRAY[{resource_array}]::uuid[], {}, 132,
         'ens_v1_unwrapped_authority', 'ens_v2_registry_resource_surface',
         'RegistrationGranted', 'TokenControlTransferred', 'AuthorityTransferred',
         'basenames', 'ens_v2_registry_resource_surface',
         'basenames', 'ens_v2_registry_resource_surface',
         'ens_v2_registry_resource_surface', 'ens_v2_registry_resource_surface'",
        quote(CHAIN),
        quote(CHAIN),
    );
    let execute = format!(
        "EXECUTE \"{}\" ({arguments})",
        statement_name.replace('"', "\"\"")
    );
    let mut actual: Vec<Holding> = sqlx::query_as(&execute).fetch_all(&mut *connection).await?;
    actual.sort();
    assert_eq!(actual, expected, "captured statement bind reconstruction");
    let plan: Value = sqlx::query_scalar(&format!(
        "EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT JSON) {execute}"
    ))
    .fetch_one(&mut *connection)
    .await?;
    Ok(json!({"statement": statement, "arguments": arguments, "plan": plan}))
}

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn bounded_lease_probe(plan: &Value) -> bool {
    match plan {
        Value::Object(map) => {
            (map.get("Index Name").and_then(Value::as_str) == Some(INDEX)
                && map
                    .get("Index Cond")
                    .and_then(Value::as_str)
                    .is_some_and(|condition| condition.contains("handoff.lease_resource_id"))
                && map.get("Actual Loops").and_then(Value::as_u64).unwrap_or(0) > 0)
                || map.values().any(bounded_lease_probe)
        }
        Value::Array(items) => items.iter().any(bounded_lease_probe),
        _ => false,
    }
}

#[tokio::test]
async fn complete_membership_query_uses_bounded_nonorphaned_lease_probe() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_blocks(&database, REGISTERED..=132).await?;
    // Unrelated observed registrar resources must remain in the lease predicate. Seed
    // them before the handoff's lease so the old sequential probe cannot win by finding
    // the wanted lease at the beginning of the heap. All FK identities stay on this chain.
    sqlx::raw_sql(&format!(
        "INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state)
         SELECT md5('lease-plan-noise:' || n)::uuid, {}, '0xhistory130', 130, 'observed'
         FROM generate_series(1,12000) n;
         INSERT INTO normalized_events (event_identity, namespace, resource_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash,
             derivation_kind, canonicality_state)
         SELECT 'lease-plan-noise:' || n, 'ens', md5('lease-plan-noise:' || n)::uuid,
             'RegistrationRenewed', 'ens_v1_registrar_l1', 1, {}, 130, '0xhistory130',
             'ens_v1_unwrapped_authority', 'observed' FROM generate_series(1,12000) n;",
        quote(CHAIN), quote(CHAIN),
    ))
    .execute(&database.pool)
    .await?;
    let tokenless = raw(
        NewOwner {
            node: eth_node(),
            label: keccak256(LABEL.as_bytes()),
            owner: OTHER.parse()?,
        }
        .encode_log_data(),
        REGISTERED,
        0,
        REGISTRY,
    );
    let (held, session) = interpret(REGISTERED, vec![tokenless], None)?;
    let (registered, session) = interpret(
        131,
        registration_at(131, NEW_HOLDER, 4_102_444_800, None),
        Some(session),
    )?;
    let (handed_off, _) = interpret(132, token_moved(132), Some(session))?;
    for (block, output) in [(130, &held), (131, &registered), (132, &handed_off)] {
        persist(&database.pool, output).await?;
        project_to(&database.pool, block, None).await?;
    }
    let handoffs: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "SELECT resource_id, lease_resource_id FROM project_binding_candidate
         WHERE registry_only AND lease_resource_id IS NOT NULL",
    )
    .fetch_all(&database.pool)
    .await?;
    assert_eq!(
        handoffs.len(),
        1,
        "the real Project path must produce the tested handoff"
    );
    let registry_resources: Vec<_> = handoffs.iter().map(|(resource, _)| *resource).collect();
    let leases: Vec<_> = handoffs.iter().map(|(_, lease)| *lease).collect();
    let resources: Vec<_> = registry_resources.iter().chain(&leases).copied().collect();
    let names = vec![bigname_storage::logical_name_id_for_name(
        "ens",
        &format!("{LABEL}.eth"),
    )];
    let mut connection = database.pool.acquire().await?;
    sqlx::raw_sql(&format!("DROP INDEX bigname_phase.{INDEX}; ANALYZE normalized_events; ANALYZE project_binding_candidate"))
        .execute(&mut *connection).await?;
    let variants = [
        "observed",
        "canonical",
        "safe",
        "finalized",
        "orphaned",
        "missing",
        "wrong-family",
    ];
    let mut expected = Vec::new();
    let mut captured = Vec::new();
    for indexed in [false, true] {
        if indexed {
            sqlx::raw_sql(CREATE_INDEX)
                .execute(&mut *connection)
                .await?;
            sqlx::raw_sql("ANALYZE normalized_events")
                .execute(&mut *connection)
                .await?;
        }
        for (i, variant) in variants.iter().enumerate() {
            sqlx::raw_sql("BEGIN").execute(&mut *connection).await?;
            match *variant {
                "missing" => {
                    sqlx::query("DELETE FROM normalized_events WHERE resource_id=ANY($1) AND source_family='ens_v1_registrar_l1'").bind(&leases).execute(&mut *connection).await?;
                }
                "wrong-family" => {
                    sqlx::query("UPDATE normalized_events SET source_family='ens_v1_registry_l1', source_manifest_id=NULL WHERE resource_id=ANY($1) AND source_family='ens_v1_registrar_l1'").bind(&leases).execute(&mut *connection).await?;
                }
                state => {
                    sqlx::query("UPDATE normalized_events SET canonicality_state=$2::bigname_phase.canonicality_state WHERE resource_id=ANY($1) AND source_family='ens_v1_registrar_l1'").bind(&leases).bind(state).execute(&mut *connection).await?;
                }
            }
            let rows = holdings(&mut connection, &names, &resources).await?;
            for registry_resource in &registry_resources {
                let owner = rows
                    .iter()
                    .any(|r| r.0 == NEW_HOLDER && r.3 == Some(*registry_resource) && r.4 == 1);
                assert_eq!(
                    owner,
                    matches!(*variant, "orphaned" | "missing" | "wrong-family"),
                    "{variant}: {rows:?}"
                );
                assert!(
                    rows.iter().any(|r| r.0 == OTHER && r.4 == 1),
                    "earlier tokenless owner: {rows:?}"
                );
                assert!(
                    rows.iter()
                        .any(|r| r.0 == NEW_HOLDER && r.3 == Some(*registry_resource) && r.4 == 2),
                    "controller: {rows:?}"
                );
            }
            if indexed {
                assert_eq!(rows, expected[i], "complete tuple equality: {variant}");
            } else {
                expected.push(rows);
            }
            if *variant == "canonical" {
                for mode in ["force_custom_plan", "force_generic_plan"] {
                    sqlx::raw_sql(&format!("SET LOCAL plan_cache_mode={mode}"))
                        .execute(&mut *connection)
                        .await?;
                    let capture = executed_plan(&mut connection, &names, &resources).await?;
                    assert_eq!(
                        bounded_lease_probe(&capture["plan"]),
                        indexed,
                        "indexed={indexed} mode={mode}: {capture}"
                    );
                    captured.push(json!({"indexed": indexed, "mode": mode, "capture": capture}));
                }
            }
            sqlx::raw_sql("ROLLBACK").execute(&mut *connection).await?;
        }
    }
    if let Some(path) = std::env::var_os("BIGNAME_LEASE_INDEX_PLANS") {
        std::fs::write(path, serde_json::to_vec_pretty(&captured)?)?;
    }
    drop(connection);
    database.cleanup().await?;
    Ok(())
}
