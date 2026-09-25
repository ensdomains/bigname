use super::*;

fn target() -> Target {
    Target {
        number: 20,
        hash: "new".into(),
        timestamp: json!("new-time"),
    }
}
fn row() -> Value {
    json!({"logical_name_id":"ancestor","chain_positions":{"ethereum-sepolia":{"block_number":10,"block_hash":"old","timestamp":"old-time"}},"canonicality_summary":{"target_block_number":10,"target_block_hash":"old"},"payload":{"source_timestamp":"keep","null":null},"inserted_at":"old-op"})
}
#[test]
fn retention_requires_independent_reason_and_exact_baseline() -> Result<()> {
    let b = row();
    let r = target().refresh("name_current", &b)?;
    let mut old = Scopes::default();
    old.names.insert("ancestor".into());
    let mut required = Scopes::default();
    assert_eq!(
        compare_row(
            "name_current",
            Some(&b),
            Some(&b),
            Some(&r),
            &required,
            &old,
            &target()
        )?,
        Verdict::Retained
    );
    assert!(
        compare_row(
            "name_current",
            Some(&b),
            Some(&r),
            Some(&r),
            &required,
            &old,
            &target()
        )
        .is_err(),
        "old broad refresh wrongly passes the new retention contract"
    );
    // Same-value real invalidation must refresh target even though payload is unchanged.
    required.names.insert("ancestor".into());
    assert!(
        compare_row(
            "name_current",
            Some(&b),
            Some(&b),
            Some(&r),
            &required,
            &old,
            &target()
        )
        .is_err()
    );
    required.names.clear();
    assert!(
        compare_row(
            "name_current",
            Some(&b),
            Some(&b),
            Some(&r),
            &required,
            &required,
            &target()
        )
        .is_err()
    );
    assert_eq!(
        compare_row(
            "name_current",
            Some(&b),
            Some(&b),
            Some(&b),
            &required,
            &required,
            &target()
        )?,
        Verdict::Exact
    );
    let mut outside = b.clone();
    outside["inserted_at"] = json!("changed-outside-scope");
    assert!(
        compare_row(
            "name_current",
            Some(&b),
            Some(&outside),
            Some(&b),
            &required,
            &required,
            &target()
        )
        .is_err()
    );
    let mut wrong = b.clone();
    wrong["inserted_at"] = json!("changed-op");
    assert!(
        compare_row(
            "name_current",
            Some(&b),
            Some(&wrong),
            Some(&r),
            &required,
            &old,
            &target()
        )
        .is_err()
    );
    for (base, candidate, reference) in [
        (None, Some(&b), Some(&r)),
        (Some(&b), None, Some(&r)),
        (Some(&b), Some(&b), None),
    ] {
        assert!(
            compare_row(
                "name_current",
                base,
                candidate,
                reference,
                &required,
                &old,
                &target()
            )
            .is_err()
        );
    }
    let mut wrong_ref = r.clone();
    wrong_ref["payload"]["null"] = json!("");
    assert!(
        compare_row(
            "name_current",
            Some(&b),
            Some(&b),
            Some(&wrong_ref),
            &required,
            &old,
            &target()
        )
        .is_err()
    );
    wrong_ref = r.clone();
    wrong_ref["payload"].as_object_mut().unwrap().remove("null");
    assert!(
        compare_row(
            "name_current",
            Some(&b),
            Some(&b),
            Some(&wrong_ref),
            &required,
            &old,
            &target()
        )
        .is_err()
    );
    wrong_ref = r;
    wrong_ref["payload"]["source_timestamp"] = json!("changed");
    assert!(
        compare_row(
            "name_current",
            Some(&b),
            Some(&b),
            Some(&wrong_ref),
            &required,
            &old,
            &target()
        )
        .is_err()
    );
    Ok(())
}
#[test]
fn ownership_matches_composite_and_nullable_publication_predicates() -> Result<()> {
    let mut s = Scopes::default();
    s.resources.insert("record-resource".into());
    s.children.insert("parent".into());
    assert!(s.owns(
        "address_records_current",
        &json!({"resource_id":null,"record_resource_id":"record-resource"})
    )?);
    assert!(!s.owns("address_names_current", &json!({"resource_id":null}))?);
    assert!(s.owns(
        "children_current",
        &json!({"parent_logical_name_id":"parent","child_logical_name_id":"other"})
    )?);
    s.primary
        .insert(vec!["address".into(), "60".into(), "ens".into()]);
    assert!(s.owns(
        "primary_names_current",
        &json!({"address":"address","coin_type":"60","namespace":"ens"})
    )?);
    assert!(!s.owns(
        "primary_names_current",
        &json!({"address":"address","coin_type":"60","namespace":"other"})
    )?);
    assert!(
        s.owns(
            "primary_names_current",
            &json!({"address":"address","coin_type":null,"namespace":"ens"})
        )
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn contract_audit_distinguishes_forward_inputs_late_keys_and_same_value_changes() -> Result<()>
{
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};
    use sqlx::Acquire;
    let database =
        TestDatabase::create(TestDatabaseConfig::new("contract_audit_directions")).await?;
    let mut tx = database.pool().begin().await?;
    sqlx::raw_sql(include_str!("../testdata/sql/scope/mirror_fixture.sql"))
        .execute(&mut *tx)
        .await?;
    sqlx::query("TRUNCATE project_changed_events")
        .execute(&mut *tx)
        .await?;
    assert!(!audit::enabled(&mut tx).await?);
    {
        let mut branch = tx.begin().await?;
        sqlx::query("SET LOCAL bigname.contract_audit='on'")
            .execute(&mut *branch)
            .await?;
        assert!(
            audit::enabled(&mut branch).await.is_err(),
            "audit alone silently changes production path"
        );
        branch.rollback().await?;
    }
    for changed in [false, true] {
        let mut branch = tx.begin().await?;
        sqlx::query("SET LOCAL bigname.benchmark_reference='on'")
            .execute(&mut *branch)
            .await?;
        sqlx::query("SET LOCAL bigname.contract_audit='on'")
            .execute(&mut *branch)
            .await?;
        assert!(audit::enabled(&mut branch).await?);
        if changed {
            // Identical after-state is a real invalidation. Ancestor has NULL resource.
            sqlx::query("INSERT INTO project_changed_events SELECT * FROM normalized_events WHERE normalized_event_id=200")
                .execute(&mut *branch).await?;
        } else {
            sqlx::query("INSERT INTO project_scope_resources VALUES(md5('mirror-26')::uuid)")
                .execute(&mut *branch)
                .await?;
        }
        audit::mirror_stage(&mut branch, "bench", 10).await?;
        audit::mirror_include(&mut branch).await?;
        let global_names: i64 = sqlx::query_scalar("SELECT count(*) FROM project_scope_names")
            .fetch_one(&mut *branch)
            .await?;
        assert_eq!(
            global_names, 0,
            "consulted evidence leaked into required outputs"
        );
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM project_scope_resources")
            .fetch_one(&mut *branch)
            .await?;
        if !changed {
            assert_eq!(
                count, 1,
                "unchanged ancestor created symmetric sibling fanout"
            );
            let inputs: Vec<String> = sqlx::query_scalar(
                "SELECT logical_name_id FROM project_contract_audit_input_names ORDER BY 1",
            )
            .fetch_all(&mut *branch)
            .await?;
            assert_eq!(inputs, vec!["eth", "name-26"]);
            // A subsequent independent operator really changes the ancestor name.
            sqlx::query("INSERT INTO project_scope_names VALUES('eth')")
                .execute(&mut *branch)
                .await?;
            audit::mirror_include(&mut branch).await?;
        } else {
            assert!(
                count > 1,
                "same-value changed ancestor did not invalidate subscribers"
            );
        }
        let wrong:i64=sqlx::query_scalar("SELECT count(*) FROM ((SELECT mirror_resource_id FROM project_mirror_pairs WHERE consulted_logical_name_id='eth' EXCEPT SELECT resource_id FROM project_scope_resources) UNION ALL (SELECT resource_id FROM project_scope_resources EXCEPT SELECT mirror_resource_id FROM project_mirror_pairs WHERE consulted_logical_name_id='eth')) difference")
            .fetch_one(&mut *branch).await?;
        assert_eq!(
            wrong, 0,
            "required reverse expansion differs from literal complete graph"
        );
        audit::mirror_finish(&mut branch).await?;
        branch.rollback().await?;
        assert!(
            !audit::enabled(&mut tx).await? && !crate::reference::enabled(&mut tx).await?,
            "mode leaked after audit rollback"
        );
        let exists: bool = sqlx::query_scalar(
            "SELECT to_regclass('pg_temp.project_contract_audit_pairs') IS NOT NULL",
        )
        .fetch_one(&mut *tx)
        .await?;
        assert!(!exists, "audit temp graph leaked");
    }
    // A SQL failure also restores the caller and both independent switches.
    {
        let mut branch = tx.begin().await?;
        sqlx::query("SET LOCAL bigname.benchmark_reference='on'")
            .execute(&mut *branch)
            .await?;
        sqlx::query("SET LOCAL bigname.contract_audit='on'")
            .execute(&mut *branch)
            .await?;
        assert!(
            sqlx::query("SELECT 1/0")
                .execute(&mut *branch)
                .await
                .is_err()
        );
        branch.rollback().await?;
    }
    assert!(!audit::enabled(&mut tx).await? && !crate::reference::enabled(&mut tx).await?);
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn contract_private_snapshots_validate_every_table_and_reject_unnecessary_refresh()
-> Result<()> {
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};
    use sqlx::Acquire;
    let database = TestDatabase::create(TestDatabaseConfig::new("contract_every_table")).await?;
    let mut tx = database.pool().begin().await?;
    sqlx::raw_sql("CREATE TEMP TABLE chain_lineage(chain_id text,block_number bigint,block_hash text,block_timestamp timestamptz,canonicality_state text);
        INSERT INTO chain_lineage VALUES('ethereum-sepolia',10,'old',to_timestamp(1800000010),'canonical'),('ethereum-sepolia',20,'new',to_timestamp(1800000020),'canonical')")
        .execute(&mut *tx).await?;
    let old_time: Value = sqlx::query_scalar(
        "SELECT to_jsonb(block_timestamp) FROM chain_lineage WHERE block_number=10",
    )
    .fetch_one(&mut *tx)
    .await?;
    let target = Target::load(
        &mut tx,
        &crate::Marker {
            number: 20,
            hash: "new".into(),
        },
    )
    .await?;
    let mut old = Scopes::default();
    old.names.insert("name".into());
    old.resources.insert("resource".into());
    old.children.insert("parent".into());
    old.resolvers.insert("0xabc".into());
    old.primary
        .insert(vec!["address".into(), "60".into(), "ens".into()]);
    old.accounts.insert(vec![
        "ethereum-sepolia".into(),
        "kind".into(),
        "contract".into(),
        "owner".into(),
        "subject".into(),
        "relation".into(),
    ]);
    old.window = (1, 10);
    let mut baselines = Vec::new();
    for (table, keys) in TABLES {
        let mut row = json!({"logical_name_id":"name","parent_logical_name_id":"parent","child_logical_name_id":"child","resource_id":"resource","record_resource_id":null,
            "chain_id":"ethereum-sepolia","resolver_address":"0xabc","authority_kind":"kind","authority_contract":"contract","owner":"owner","subject":"subject","relation_kind":"relation",
            "address":"address","coin_type":"60","namespace":"ens",
            "chain_positions":{"block_number":5,"block_hash":"source-event","target_block_number":10,"target_block_hash":"old"},
            "canonicality_summary":{"target_block_number":10,"target_block_hash":"old"},
            "claim_provenance":{"chain_id":"ethereum-sepolia","target_block_number":10,"target_block_hash":"old","timestamp":"source-kept"},
            "inserted_at":"old-operational","last_recomputed_at":"old-operational","semantic":{"clear":null,"source_timestamp":"unchanged","array":[1,1]}});
        for key in keys.split(',') {
            if row.get(key).is_none() {
                row[key] = json!(format!("key-{key}"));
            }
        }
        if *table == "name_current" {
            row["chain_positions"] = json!({"ethereum-sepolia":{"block_number":10,"block_hash":"old","timestamp":old_time}});
        }
        if *table == "child_registration_events" {
            row["block_number"] = json!(7);
            row["target_block_number"] = json!(10);
            row["target_block_hash"] = json!("old");
        }
        let columns = row
            .as_object()
            .unwrap()
            .keys()
            .map(|k| format!("\"{k}\" jsonb"))
            .collect::<Vec<_>>()
            .join(",");
        sqlx::query(&format!("CREATE TEMP TABLE {table}({columns})"))
            .execute(&mut *tx)
            .await?;
        sqlx::query(&format!(
            "INSERT INTO {table} SELECT * FROM jsonb_populate_record(NULL::{table},$1)"
        ))
        .bind(&row)
        .execute(&mut *tx)
        .await?;
        baselines.push(row);
    }
    let root = std::env::temp_dir().join(format!("contract-ten-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root)?;
    let mut baseline = Snapshot::capture(&mut tx, &root).await?;
    let mut candidate = Snapshot::capture(&mut tx, &root).await?;
    let mut branch = tx.begin().await?;
    for ((table, _), row) in TABLES.iter().zip(&baselines) {
        sqlx::query(&format!("DELETE FROM {table}"))
            .execute(&mut *branch)
            .await?;
        sqlx::query(&format!(
            "INSERT INTO {table} SELECT * FROM jsonb_populate_record(NULL::{table},$1)"
        ))
        .bind(target.refresh(table, row)?)
        .execute(&mut *branch)
        .await?;
    }
    let mut reference = Snapshot::capture(&mut branch, &root).await?;
    let mut wrongly_refreshed = Snapshot::capture(&mut branch, &root).await?;
    branch.rollback().await?;
    snapshot::compare(
        &mut tx,
        snapshot::Snapshots {
            baseline: &mut baseline,
            candidate: &mut candidate,
            reference: &mut reference,
        },
        snapshot::Expectations {
            mandatory: &Scopes::default(),
            old_scope: &old,
            target: &target,
            previous: 10,
        },
    )
    .await?;
    assert!(
        snapshot::compare(
            &mut tx,
            snapshot::Snapshots {
                baseline: &mut baseline,
                candidate: &mut wrongly_refreshed,
                reference: &mut reference,
            },
            snapshot::Expectations {
                mandatory: &Scopes::default(),
                old_scope: &old,
                target: &target,
                previous: 10,
            },
        )
        .await
        .is_err()
    );
    let reports = std::fs::read_dir(&root)?.collect::<std::io::Result<Vec<_>>>()?;
    let mut retained = 0;
    let mut rejected = 0;
    for directory in &reports {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(directory.metadata()?.permissions().mode() & 0o777, 0o700);
        }
        for entry in std::fs::read_dir(directory.path())? {
            let entry = entry?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                assert_eq!(entry.metadata()?.permissions().mode() & 0o777, 0o600);
            }
            if entry.file_name() == "differences.jsonl" {
                for line in std::fs::read_to_string(entry.path())?.lines() {
                    let report: Value = serde_json::from_str(line)?;
                    assert!(
                        report.get("baseline").is_some()
                            && report.get("candidate").is_some()
                            && report.get("reference").is_some()
                    );
                    if report["result"] == "rejected" {
                        rejected += 1;
                    } else {
                        retained += 1;
                    }
                }
            }
        }
    }
    assert_eq!((retained, rejected), (TABLES.len(), TABLES.len()));
    drop((baseline, candidate, reference, wrongly_refreshed));
    std::fs::remove_dir_all(root)?;
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn contract_audit_rejects_unmodeled_classification_and_invalid_input_targets() -> Result<()> {
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};
    let database = TestDatabase::create(TestDatabaseConfig::new("contract_input_guards")).await?;
    let mut tx = database.pool().begin().await?;
    sqlx::raw_sql(include_str!("../testdata/sql/scope/mirror_fixture.sql"))
        .execute(&mut *tx)
        .await?;
    sqlx::raw_sql("TRUNCATE project_changed_events;
        CREATE TEMP TABLE project_scope_resolvers(resolver_address text);
        CREATE TEMP TABLE project_scope_resolver_dependents(resolver_address text);
        CREATE TEMP TABLE resolver_current(chain_id text,resolver_address text,chain_positions jsonb);
        CREATE TEMP TABLE record_inventory_current(resource_id uuid,provenance jsonb);
        INSERT INTO project_scope_resources VALUES(md5('mirror-26')::uuid)")
        .execute(&mut *tx).await?;
    audit::mirror_stage(&mut tx, "bench", 10).await?;
    audit::mirror_include(&mut tx).await?;
    let request = crate::BatchRequest {
        chain_id: "bench".into(),
        target_block: 10,
        affected_from_block: 10,
        affected_to_block: 10,
        resume_current: Some(crate::Marker {
            number: 10,
            hash: "block".into(),
        }),
        mode: crate::RunMode::Normal,
    };
    let target = crate::Marker {
        number: 10,
        hash: "block".into(),
    };
    assert!(
        audit::reject_unsupported(&mut tx, &request, &target)
            .await
            .is_err(),
        "missing evidence classification accepted"
    );
    // A record-only/passthrough key is not proof that an invalid target was rebuilt.
    sqlx::query("INSERT INTO project_scope_resolvers VALUES('0xshared')")
        .execute(&mut *tx)
        .await?;
    assert!(
        audit::reject_unsupported(&mut tx, &request, &target)
            .await
            .is_err()
    );
    sqlx::query("INSERT INTO resolver_current VALUES('bench','0xshared','{\"target_block_number\":10,\"target_block_hash\":\"block\"}')").execute(&mut *tx).await?;
    audit::reject_unsupported(&mut tx, &request, &target).await?;
    for context in [
        json!({"target_block_number":11,"target_block_hash":"future"}),
        json!({"target_block_number":9,"target_block_hash":"orphan"}),
        json!({"target_block_number":10,"target_block_hash":"missing"}),
    ] {
        sqlx::query("UPDATE resolver_current SET chain_positions=$1")
            .bind(context)
            .execute(&mut *tx)
            .await?;
        assert!(
            audit::reject_unsupported(&mut tx, &request, &target)
                .await
                .is_err(),
            "invalid/future retained input accepted"
        );
    }
    sqlx::raw_sql("UPDATE resolver_current SET chain_positions='{\"target_block_number\":10,\"target_block_hash\":\"block\"}';
        INSERT INTO project_scope_resolver_dependents VALUES('0xshared');
        INSERT INTO record_inventory_current VALUES(md5('mirror-27')::uuid,'{\"chain_id\":\"ethereum-sepolia\",\"mirror\":{\"mirrored_resolver_address\":\"0xshared\"}}')")
        .execute(&mut *tx).await?;
    assert!(
        audit::reject_unsupported(&mut tx, &request, &target)
            .await
            .is_err(),
        "new mirror classification invalidation absent old audit was accepted"
    );
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

/// Temporary stand-ins for every compared table, empty except one child registration row at
/// block 15, and the lineage the target metadata reads.
async fn child_registration_fixture(tx: &mut Transaction<'_, Postgres>) -> Result<Target> {
    sqlx::raw_sql("CREATE TEMP TABLE chain_lineage(chain_id text,block_number bigint,block_hash text,block_timestamp timestamptz,canonicality_state text);
        INSERT INTO chain_lineage VALUES('ethereum-sepolia',10,'old',to_timestamp(1800000010),'canonical'),('ethereum-sepolia',20,'new',to_timestamp(1800000020),'canonical')")
        .execute(&mut **tx).await?;
    for (table, keys) in TABLES {
        if *table == "child_registration_events" {
            continue;
        }
        let columns = keys
            .split(',')
            .map(|key| format!("{key} text"))
            .collect::<Vec<_>>()
            .join(", ");
        sqlx::query(&format!("CREATE TEMP TABLE {table}({columns})"))
            .execute(&mut **tx)
            .await?;
    }
    sqlx::query(
        "CREATE TEMP TABLE child_registration_events(parent_logical_name_id text,
         event_identity text, child_logical_name_id text, chain_id text, block_number bigint,
         block_hash text, provenance jsonb, target_block_number bigint, target_block_hash text,
         last_recomputed_at text)",
    )
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "INSERT INTO child_registration_events VALUES
         ('ens:parent', 'grant-15', 'ens:child', 'ethereum-sepolia', 15, 'block-15',
          '{\"source_family\":\"ens_v2_registry_l1\"}', 20, 'new', 'operational')",
    )
    .execute(&mut **tx)
    .await?;
    Target::load(
        tx,
        &crate::Marker {
            number: 20,
            hash: "new".into(),
        },
    )
    .await
}

async fn compare_child_registration_candidate(
    tx: &mut Transaction<'_, Postgres>,
    mandatory: &Scopes,
    root: &std::path::Path,
    candidate_change: &str,
    reference_change: Option<&str>,
) -> Result<()> {
    child_registration_fixture(tx).await?;
    compare_child_registration_fixture(tx, mandatory, root, candidate_change, reference_change)
        .await
}

/// Compares a candidate and a reference change against the fixture already in `tx`.
async fn compare_child_registration_fixture(
    tx: &mut Transaction<'_, Postgres>,
    mandatory: &Scopes,
    root: &std::path::Path,
    candidate_change: &str,
    reference_change: Option<&str>,
) -> Result<()> {
    use sqlx::Acquire;
    let target = Target::load(
        tx,
        &crate::Marker {
            number: 20,
            hash: "new".into(),
        },
    )
    .await?;
    let mut baseline = Snapshot::capture(tx, root).await?;
    let mut branch = tx.begin().await?;
    sqlx::query(candidate_change).execute(&mut *branch).await?;
    let mut candidate = Snapshot::capture(&mut branch, root).await?;
    branch.rollback().await?;
    let mut branch = tx.begin().await?;
    if let Some(change) = reference_change {
        sqlx::query(change).execute(&mut *branch).await?;
    }
    let mut reference = Snapshot::capture(&mut branch, root).await?;
    branch.rollback().await?;
    snapshot::compare(
        tx,
        snapshot::Snapshots {
            baseline: &mut baseline,
            candidate: &mut candidate,
            reference: &mut reference,
        },
        snapshot::Expectations {
            mandatory,
            old_scope: mandatory,
            target: &target,
            previous: 10,
        },
    )
    .await
}

/// A candidate that rewrites or drops a published child registration row outside the batch's
/// blocks must fail the comparison, like any other served table.
#[tokio::test]
async fn contract_rejects_a_rewritten_or_dropped_child_registration_row() -> Result<()> {
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};
    let database =
        TestDatabase::create(TestDatabaseConfig::new("contract_child_registrations")).await?;
    let root = std::env::temp_dir().join(format!("contract-child-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root)?;
    for change in [
        "UPDATE child_registration_events SET provenance = '{\"source_family\":\"rewritten\"}'",
        "DELETE FROM child_registration_events",
    ] {
        let mut tx = database.pool().begin().await?;
        let result =
            compare_child_registration_candidate(&mut tx, &Scopes::default(), &root, change, None)
                .await;
        tx.rollback().await?;
        let error = result
            .err()
            .unwrap_or_else(|| panic!("the comparator accepted a candidate that ran: {change}"));
        assert!(
            error
                .to_string()
                .contains("contract comparison rejected 1 rows"),
            "{change}: {error:#}"
        );
    }
    std::fs::remove_dir_all(root)?;
    database.cleanup().await?;
    Ok(())
}

/// Rows of an owned key: absent everywhere, deleted by both derivations, created by both, or
/// deleted or kept by only one of them.
#[test]
fn owned_absent_and_deleted_rows_follow_the_reference() -> Result<()> {
    let b = row();
    let mut owned = Scopes::default();
    owned.names.insert("ancestor".into());
    let mut refreshed = target().refresh("name_current", &b)?;
    refreshed["inserted_at"] = json!("new-op");
    let verdict = |base, candidate, reference| {
        compare_row(
            "name_current",
            base,
            candidate,
            reference,
            &owned,
            &owned,
            &target(),
        )
    };
    // A key the batch owns that neither derivation produces, and never had a row.
    assert_eq!(verdict(None, None, None)?, Verdict::Exact);
    // A released name: both derivations delete the published row.
    assert_eq!(verdict(Some(&b), None, None)?, Verdict::Exact);
    // A new row both derivations produce, equal apart from operational timestamps.
    let produced = target().refresh("name_current", &b)?;
    assert_eq!(
        verdict(None, Some(&refreshed), Some(&produced))?,
        Verdict::Exact
    );
    // Only one side deletes.
    assert!(verdict(Some(&b), None, Some(&refreshed)).is_err());
    assert!(verdict(Some(&b), Some(&refreshed), None).is_err());
    Ok(())
}

/// Child registration rows belong to the batch by block. Inside the window both derivations
/// may delete or refresh a row as long as they agree; outside it nothing may change.
#[tokio::test]
async fn child_registration_rows_in_the_batch_window_follow_the_reference() -> Result<()> {
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};
    let database = TestDatabase::create(TestDatabaseConfig::new("contract_child_window")).await?;
    let root = std::env::temp_dir().join(format!("contract-window-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root)?;
    let window = Scopes {
        window: (11, 20),
        ..Scopes::default()
    };
    let orphaned = "DELETE FROM child_registration_events";
    let refreshed = "UPDATE child_registration_events SET last_recomputed_at = 'later'";
    let rewritten =
        "UPDATE child_registration_events SET provenance = '{\"source_family\":\"rewritten\"}'";
    for (candidate, reference, accepted) in [
        (orphaned, Some(orphaned), true),
        (refreshed, None, true),
        (orphaned, None, false),
        (rewritten, None, false),
    ] {
        let mut tx = database.pool().begin().await?;
        let result =
            compare_child_registration_candidate(&mut tx, &window, &root, candidate, reference)
                .await;
        tx.rollback().await?;
        assert_eq!(
            result.is_ok(),
            accepted,
            "{candidate} against {reference:?}: {result:?}"
        );
    }
    std::fs::remove_dir_all(root)?;
    database.cleanup().await?;
    Ok(())
}

/// The publisher's window delete also removes rows at or above the window start whose block is
/// no longer readable canonical lineage, even above the window, and never touches another
/// chain. The mandatory scope must own exactly those rows, captured from the table the delete
/// runs against.
#[tokio::test]
async fn child_registration_rows_the_publisher_deletes_above_the_window_are_owned() -> Result<()> {
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};
    let database = TestDatabase::create(TestDatabaseConfig::new("contract_child_orphans")).await?;
    let root = std::env::temp_dir().join(format!("contract-orphans-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root)?;
    let deleted = "DELETE FROM child_registration_events";
    let kept = "SELECT 1";
    let canonical = "INSERT INTO chain_lineage VALUES
        ('ethereum-sepolia', 15, 'block-15', to_timestamp(1800000015), 'canonical')";
    let foreign = "UPDATE child_registration_events SET chain_id = 'ethereum-mainnet'";
    // The fixture row sits at block 15 on a block hash the lineage does not hold.
    for (setup, window, candidate, reference, accepted) in [
        // Orphaned above a window that ends below it: both derivations delete it.
        (None, (11, 12), deleted, Some(deleted), true),
        // Only the reference deletes the orphan.
        (None, (11, 12), kept, Some(deleted), false),
        // Only the candidate deletes the orphan.
        (None, (11, 12), deleted, None, false),
        // A canonical row above the window stays, so deleting it is a change.
        (Some(canonical), (11, 12), deleted, Some(deleted), false),
        // An orphan below the window start stays too.
        (None, (16, 20), deleted, Some(deleted), false),
        // Another chain's rows are never in the batch, inside the window or above it.
        (Some(foreign), (11, 20), deleted, Some(deleted), false),
        (Some(foreign), (11, 12), deleted, Some(deleted), false),
    ] {
        let mut tx = database.pool().begin().await?;
        child_registration_fixture(&mut tx).await?;
        if let Some(setup) = setup {
            sqlx::query(setup).execute(&mut *tx).await?;
        }
        sqlx::raw_sql(
            "CREATE TEMP TABLE project_scope_names(logical_name_id text);
             CREATE TEMP TABLE project_scope_resources(resource_id text);
             CREATE TEMP TABLE project_scope_children(logical_name_id text);
             CREATE TEMP TABLE project_scope_resolvers(resolver_address text);
             CREATE TEMP TABLE project_scope_account_permissions(chain_id text,
                 authority_kind text, authority_contract text, owner text, subject text,
                 relation_kind text);
             CREATE TEMP TABLE project_scope_primary(address text, coin_type text,
                 namespace text)",
        )
        .execute(&mut *tx)
        .await?;
        let mandatory = Scopes::capture(&mut tx, window).await?;
        let result =
            compare_child_registration_fixture(&mut tx, &mandatory, &root, candidate, reference)
                .await;
        tx.rollback().await?;
        assert_eq!(
            result.is_ok(),
            accepted,
            "{setup:?} window {window:?}: {candidate} against {reference:?}: {result:?}"
        );
    }
    std::fs::remove_dir_all(root)?;
    database.cleanup().await?;
    Ok(())
}

/// The publication record must be the same in all three snapshots, and a same-head rerun must
/// reproduce every row apart from operational timestamps.
#[tokio::test]
async fn publication_record_and_same_head_output_must_not_change() -> Result<()> {
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};
    use sqlx::Acquire;
    let database = TestDatabase::create(TestDatabaseConfig::new("contract_publication")).await?;
    let root = std::env::temp_dir().join(format!("contract-publication-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root)?;
    let mut tx = database.pool().begin().await?;
    let target = child_registration_fixture(&mut tx).await?;
    sqlx::raw_sql(
        "CREATE TEMP TABLE chain_phase_state(chain_id text, phase_name text,
             current_block_number bigint, current_block_hash text, input_content_hash text,
             phase_status text, redo_in_progress boolean);
         INSERT INTO chain_phase_state VALUES
             ('ethereum-sepolia', 'project', 10, 'old', 'hash', 'completed', false),
             ('ethereum-sepolia', 'interpret', 20, 'new', 'hash', 'completed', false);
         CREATE TEMP TABLE chain_heads(chain_id text, latest_block_number bigint);
         INSERT INTO chain_heads VALUES ('ethereum-sepolia', 20)",
    )
    .execute(&mut *tx)
    .await?;
    let mut baseline = Snapshot::capture(&mut tx, &root).await?;
    assert_eq!(
        baseline.publication["project"][0]["current_block_number"],
        10
    );
    let mut branch = tx.begin().await?;
    sqlx::query(
        "UPDATE chain_phase_state SET current_block_number = 20 WHERE phase_name = 'project'",
    )
    .execute(&mut *branch)
    .await?;
    let mut moved = Snapshot::capture(&mut branch, &root).await?;
    branch.rollback().await?;
    let mut reference = Snapshot::capture(&mut tx, &root).await?;
    let error = snapshot::compare(
        &mut tx,
        snapshot::Snapshots {
            baseline: &mut baseline,
            candidate: &mut moved,
            reference: &mut reference,
        },
        snapshot::Expectations {
            mandatory: &Scopes::default(),
            old_scope: &Scopes::default(),
            target: &target,
            previous: 10,
        },
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("publication record"),
        "{error:#}"
    );

    let mut rerun = Snapshot::capture(&mut tx, &root).await?;
    assert_eq!(assert_same_output(&mut reference, &mut rerun)?, 1);
    for (change, same) in [
        (
            "UPDATE child_registration_events SET last_recomputed_at = 'rerun'",
            true,
        ),
        (
            "UPDATE child_registration_events SET target_block_number = 21",
            false,
        ),
        ("UPDATE chain_heads SET latest_block_number = 21", false),
    ] {
        let mut branch = tx.begin().await?;
        sqlx::query(change).execute(&mut *branch).await?;
        let mut changed = Snapshot::capture(&mut branch, &root).await?;
        branch.rollback().await?;
        let mut first = Snapshot::capture(&mut tx, &root).await?;
        let result = assert_same_output(&mut first, &mut changed);
        assert_eq!(result.is_ok(), same, "{change}: {result:?}");
    }
    drop((baseline, moved, reference, rerun));
    std::fs::remove_dir_all(root)?;
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}
