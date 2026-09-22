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
    sqlx::raw_sql(include_str!("scope/mirror_fixture.sql"))
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
async fn contract_private_snapshots_validate_all_ten_rows_and_reject_unnecessary_refresh()
-> Result<()> {
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};
    use sqlx::Acquire;
    let database = TestDatabase::create(TestDatabaseConfig::new("contract_all_ten")).await?;
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
    assert_eq!((retained, rejected), (10, 10));
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
    sqlx::raw_sql(include_str!("scope/mirror_fixture.sql"))
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
