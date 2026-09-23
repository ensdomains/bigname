use anyhow::{Context, Result};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::json;
use sqlx::raw_sql;

use super::include_changed_record_consumers;

#[tokio::test]
async fn record_consumer_scope_preserves_resolver_and_canonicality_guards() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("inventory_scope_keys")).await?;
    let mut transaction = database.pool().begin().await?;
    raw_sql(
        "CREATE TEMP TABLE project_changed_events (
             logical_name_id text, event_kind text, source_family text,
             raw_fact_ref jsonb, after_state jsonb) ON COMMIT DROP;
         CREATE TEMP TABLE record_inventory_current (
             resource_id uuid, provenance jsonb) ON COMMIT DROP;
         CREATE TEMP TABLE name_surfaces (
             logical_name_id text, chain_id text, namehash text,
             block_number bigint, block_hash text, canonicality_state text) ON COMMIT DROP;
         CREATE TEMP TABLE chain_lineage (
             chain_id text, block_number bigint, block_hash text,
             canonicality_state text) ON COMMIT DROP;
         CREATE TEMP TABLE normalized_events (
             normalized_event_id bigint, source_family text) ON COMMIT DROP;
         CREATE TEMP TABLE project_scope_names (logical_name_id text PRIMARY KEY) ON COMMIT DROP;
         CREATE TEMP TABLE project_scope_resources (resource_id uuid PRIMARY KEY) ON COMMIT DROP;",
    )
    .execute(&mut *transaction)
    .await?;

    let cases = [
        ("direct", true),
        ("mirror_only", true),
        ("duplicate_keys", true),
        ("null_keys", false),
        ("wrong_inventory_chain", false),
        ("wrong_resolver", false),
        ("node_only", true),
        ("wrong_node", false),
        ("attributed_other_name", false),
        ("v2_attributed", true),
        ("v2_node_only", false),
        ("basenames_pointer", true),
        ("basenames_wrong_pointer", false),
        ("basenames_missing_pointer", false),
        ("orphaned_surface", false),
        ("orphaned_lineage", false),
        ("wrong_block_hash", false),
        ("future_surface", false),
        ("wrong_surface_chain", false),
        ("wrong_event_kind", false),
        ("wrong_source_family", false),
        ("missing_emitter", false),
    ];
    let mut expected_names = Vec::new();
    let mut expected_resources = Vec::new();
    for (index, (case, expected)) in cases.into_iter().enumerate() {
        let name = format!("name-{index:02}");
        let resource = format!("00000000-0000-0000-0000-{index:012}");
        let resolver = format!("0xAB{index:04}");
        let node = format!("0xCD{index:04}");
        let node_only = matches!(case, "node_only" | "wrong_node" | "v2_node_only")
            || case.starts_with("basenames_");
        let logical_name = if node_only {
            None
        } else if case == "attributed_other_name" {
            Some("other-name")
        } else {
            Some(name.as_str())
        };
        let direct = match case {
            "mirror_only" | "null_keys" => None,
            "wrong_resolver" => Some("0xwrong"),
            _ => Some(resolver.as_str()),
        };
        let mirrored =
            matches!(case, "mirror_only" | "duplicate_keys").then_some(resolver.to_lowercase());
        let pointer = (index as i64) + 1;
        sqlx::query("INSERT INTO record_inventory_current VALUES ($1::text::uuid, $2)")
            .bind(&resource)
            .bind(json!({
                "logical_name_id": name,
                "chain_id": if case == "wrong_inventory_chain" { "other-chain" } else { "test-chain" },
                "resolver_address": direct,
                "mirror": { "mirrored_resolver_address": mirrored },
                "resolver_pointer_event_id": pointer,
            }))
            .execute(&mut *transaction)
            .await?;
        sqlx::query("INSERT INTO project_changed_events VALUES ($1, $2, $3, $4, $5)")
            .bind(logical_name)
            .bind(if case == "wrong_event_kind" { "ResolverChanged" } else if case == "duplicate_keys" { "RecordVersionChanged" } else { "RecordChanged" })
            .bind(if case.starts_with("basenames_") { "basenames_base_resolver" } else if case.starts_with("v2_") { "ens_v2_resolver_l1" } else if case == "wrong_source_family" { "unknown" } else { "ens_v1_resolver_l1" })
            .bind(json!({"emitting_address": (case != "missing_emitter").then_some(resolver.to_lowercase())}))
            .bind(json!({"node": if case == "wrong_node" { "0xwrong" } else { &node }}))
            .execute(&mut *transaction)
            .await?;
        if case != "basenames_missing_pointer" {
            sqlx::query("INSERT INTO normalized_events VALUES ($1, $2)")
                .bind(pointer)
                .bind(if case == "basenames_pointer" {
                    "basenames_base_registry"
                } else {
                    "ens_v1_registry_l1"
                })
                .execute(&mut *transaction)
                .await?;
        }
        let block = if case == "future_surface" { 11_i64 } else { 10 };
        let hash = format!("block-{index}");
        sqlx::query("INSERT INTO name_surfaces VALUES ($1, $2, $3, $4, $5, $6)")
            .bind(&name)
            .bind(if case == "wrong_surface_chain" {
                "other-chain"
            } else {
                "test-chain"
            })
            .bind(node.to_lowercase())
            .bind(block)
            .bind(&hash)
            .bind(if case == "orphaned_surface" {
                "orphaned"
            } else {
                "canonical"
            })
            .execute(&mut *transaction)
            .await?;
        sqlx::query("INSERT INTO chain_lineage VALUES ('test-chain', $1, $2, $3)")
            .bind(block)
            .bind(if case == "wrong_block_hash" {
                "different-hash"
            } else {
                &hash
            })
            .bind(if case == "orphaned_lineage" {
                "orphaned"
            } else {
                "finalized"
            })
            .execute(&mut *transaction)
            .await?;
        if expected {
            expected_names.push(name);
            expected_resources.push(resource);
        }
    }
    for _ in 0..2 {
        include_changed_record_consumers(&mut transaction, "test-chain", 10).await?;
        let names: Vec<String> = sqlx::query_scalar(
            "SELECT logical_name_id FROM project_scope_names ORDER BY logical_name_id",
        )
        .fetch_all(&mut *transaction)
        .await?;
        let resources: Vec<String> = sqlx::query_scalar(
            "SELECT resource_id::text FROM project_scope_resources ORDER BY resource_id",
        )
        .fetch_all(&mut *transaction)
        .await?;
        assert_eq!(names, expected_names);
        assert_eq!(resources, expected_resources);
    }
    transaction.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

// Frozen pre-optimization query: keep this independent of the production query.
const ORIGINAL_RECORD_CONSUMER_QUERY: &str = r#"WITH inventory_resolvers AS MATERIALIZED (
             SELECT inventory.resource_id, inventory.provenance, resolver.address
             FROM record_inventory_current inventory
             CROSS JOIN LATERAL (VALUES
                 (lower(inventory.provenance ->> 'resolver_address')),
                 (lower(inventory.provenance #>> '{mirror,mirrored_resolver_address}'))
             ) resolver(address)
             WHERE inventory.provenance ->> 'chain_id' = $1
               AND resolver.address IS NOT NULL
         ), matched AS MATERIALIZED (
             SELECT DISTINCT inventory.resource_id,
                    inventory.provenance ->> 'logical_name_id' AS logical_name_id
             FROM project_changed_events event
             JOIN inventory_resolvers inventory
               ON lower(event.raw_fact_ref ->> 'emitting_address') = inventory.address
             JOIN name_surfaces surface
               ON surface.logical_name_id =
                  inventory.provenance ->> 'logical_name_id'
              AND surface.chain_id = $1
             JOIN chain_lineage lineage
               ON lineage.chain_id = surface.chain_id
              AND lineage.block_number = surface.block_number
              AND lineage.block_hash = surface.block_hash
             WHERE event.event_kind IN ('RecordChanged', 'RecordVersionChanged')
               AND event.source_family IN (
                   'ens_v1_resolver_l1', 'ens_v2_resolver_l1',
                   'basenames_base_resolver'
               )
               AND event.raw_fact_ref ->> 'emitting_address' IS NOT NULL
               AND (
                   event.logical_name_id = surface.logical_name_id
                   OR (
                       event.logical_name_id IS NULL
                       AND (
                           event.source_family = 'ens_v1_resolver_l1'
                           OR (
                               event.source_family = 'basenames_base_resolver'
                               AND EXISTS (
                                   SELECT 1
                                   FROM normalized_events pointer
                                   WHERE pointer.normalized_event_id =
                                       (inventory.provenance ->>
                                           'resolver_pointer_event_id')::bigint
                                     AND pointer.source_family =
                                         'basenames_base_registry'
                               )
                           )
                       )
                       AND lower(event.after_state ->> 'node') =
                           lower(surface.namehash)
                   )
               )
               AND surface.block_number <= $2
               AND surface.canonicality_state IN (
                   'canonical', 'safe', 'finalized'
               )
               AND lineage.canonicality_state IN (
                   'canonical', 'safe', 'finalized'
               )
         ), inserted_resources AS (
             INSERT INTO original_scope_resources
             SELECT resource_id FROM matched
             ON CONFLICT DO NOTHING
             RETURNING resource_id
         )
         INSERT INTO original_scope_names
         SELECT logical_name_id FROM matched
         WHERE logical_name_id IS NOT NULL
         ON CONFLICT DO NOTHING"#;

#[tokio::test]
async fn record_consumer_scope_matches_original_for_shared_resolvers() -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new("inventory_scope_differential")).await?;
    // The original SQL may eagerly cast unrelated inventory pointers when Basenames node-only
    // changes are present. Check malformed pointers separately in a window without those events,
    // then exercise the complete shared-resolver matrix with well-formed pointer values.
    for basenames_node_changes in [false, true] {
        let mut transaction = database.pool().begin().await?;
        raw_sql(
            "CREATE TEMP TABLE project_changed_events (
             logical_name_id text, event_kind text, source_family text,
             raw_fact_ref jsonb, after_state jsonb) ON COMMIT DROP;
         CREATE TEMP TABLE record_inventory_current (
             resource_id uuid, provenance jsonb) ON COMMIT DROP;
         CREATE TEMP TABLE name_surfaces (
             logical_name_id text, chain_id text, namehash text,
             block_number bigint, block_hash text, canonicality_state text) ON COMMIT DROP;
         CREATE TEMP TABLE chain_lineage (
             chain_id text, block_number bigint, block_hash text,
             canonicality_state text) ON COMMIT DROP;
         CREATE TEMP TABLE normalized_events (
             normalized_event_id bigint, source_family text) ON COMMIT DROP;
         CREATE TEMP TABLE project_scope_names (logical_name_id text PRIMARY KEY) ON COMMIT DROP;
         CREATE TEMP TABLE project_scope_resources (resource_id uuid PRIMARY KEY) ON COMMIT DROP;
         CREATE TEMP TABLE original_scope_names (logical_name_id text PRIMARY KEY) ON COMMIT DROP;
         CREATE TEMP TABLE original_scope_resources (resource_id uuid PRIMARY KEY) ON COMMIT DROP;",
        )
        .execute(&mut *transaction)
        .await?;

        // Every ordinary row shares a public resolver. A resolver-only match therefore expands many
        // unrelated names unless both the event identity and the surface guards remain intact.
        let cases = [
            ("attributed_v1", true),
            ("attributed_v2", true),
            ("attributed_basenames_without_pointer", true),
            ("attributed_wrong_node", true),
            ("attributed_missing_node", true),
            ("attributed_malformed_pointer", true),
            ("attributed_null_surface_node", true),
            ("attributed_wrong_name", false),
            ("node_v1", true),
            ("node_v1_malformed_pointer", true),
            ("node_multiple_surfaces", true),
            ("node_v2", false),
            ("node_basenames", true),
            ("node_basenames_duplicate_versions", true),
            ("node_basenames_wrong_pointer", false),
            ("node_basenames_missing_pointer", false),
            ("node_basenames_null_pointer", false),
            ("node_missing_node", false),
            ("node_null_node", false),
            ("mirror_only", true),
            ("duplicate_keys_and_events", true),
            ("different_mirror_key", true),
            ("missing_keys", false),
            ("null_keys", false),
            ("wrong_resolver", false),
            ("missing_emitter", false),
            ("null_emitter", false),
            ("empty_keys", true),
            ("missing_inventory_name", false),
            ("null_inventory_name", false),
            ("missing_inventory_chain", false),
            ("wrong_inventory_chain", false),
            ("wrong_surface_chain", false),
            ("surface_orphaned", false),
            ("lineage_orphaned", false),
            ("lineage_missing", false),
            ("lineage_wrong_chain", false),
            ("lineage_wrong_hash", false),
            ("lineage_wrong_height", false),
            ("surface_after_target", false),
            ("surface_at_target", true),
            ("surface_safe", true),
            ("lineage_safe", true),
            ("wrong_event_kind", false),
            ("unknown_family", false),
            ("record_version", true),
            ("multiple_surfaces", true),
            ("duplicate_inventory_versions", true),
            ("multiple_resources", true),
        ];
        let mut expected_names = vec!["preexisting-name".to_owned()];
        let mut expected_resources = vec!["ffffffff-ffff-ffff-ffff-ffffffffffff".to_owned()];
        for (index, (case, expected)) in cases.into_iter().enumerate() {
            let expected =
                expected && (basenames_node_changes || !case.starts_with("node_basenames"));
            let name = format!("fixture-{case}");
            let resource = format!("00000000-0000-0000-0000-{index:012}");
            let node = format!("0xAbCd{index:04}");
            let pointer_id = index as i64 + 1;
            let mut provenance = json!({
                "logical_name_id": name, "chain_id": "test-chain",
                "resolver_address": "0xPuBlIc", "resolver_pointer_event_id": pointer_id,
            });
            match case {
                "attributed_malformed_pointer" | "node_v1_malformed_pointer"
                    if !basenames_node_changes =>
                {
                    provenance["resolver_pointer_event_id"] = json!("irrelevant-not-an-integer");
                }
                "mirror_only" => {
                    provenance
                        .as_object_mut()
                        .unwrap()
                        .remove("resolver_address");
                    provenance["mirror"] = json!({"mirrored_resolver_address": "0xPUBLIC"});
                }
                "duplicate_keys_and_events" => {
                    provenance["mirror"] = json!({"mirrored_resolver_address": "0xpublic"});
                }
                "different_mirror_key" => {
                    provenance["resolver_address"] = json!("0xunmatched-direct");
                    provenance["mirror"] = json!({"mirrored_resolver_address": "0xpublic"});
                }
                "missing_keys" => {
                    provenance
                        .as_object_mut()
                        .unwrap()
                        .remove("resolver_address");
                }
                "null_keys" => {
                    provenance["resolver_address"] = json!(null);
                    provenance["mirror"] = json!({"mirrored_resolver_address": null});
                }
                "wrong_resolver" => {
                    provenance["resolver_address"] = json!("0xunmatched");
                }
                "empty_keys" => {
                    provenance["resolver_address"] = json!("");
                }
                "missing_inventory_name" => {
                    provenance
                        .as_object_mut()
                        .unwrap()
                        .remove("logical_name_id");
                }
                "null_inventory_name" => {
                    provenance["logical_name_id"] = json!(null);
                }
                "missing_inventory_chain" => {
                    provenance.as_object_mut().unwrap().remove("chain_id");
                }
                "wrong_inventory_chain" => {
                    provenance["chain_id"] = json!("other-chain");
                }
                "node_basenames_missing_pointer" => {
                    provenance
                        .as_object_mut()
                        .unwrap()
                        .remove("resolver_pointer_event_id");
                }
                "node_basenames_null_pointer" => {
                    provenance["resolver_pointer_event_id"] = json!(null);
                }
                _ => {}
            }
            sqlx::query("INSERT INTO record_inventory_current VALUES ($1::text::uuid, $2)")
                .bind(&resource)
                .bind(&provenance)
                .execute(&mut *transaction)
                .await?;
            if case == "node_basenames_duplicate_versions" {
                let mut wrong_pointer_version = provenance.clone();
                wrong_pointer_version["resolver_pointer_event_id"] = json!(999_999);
                sqlx::query("INSERT INTO record_inventory_current VALUES ($1::text::uuid, $2)")
                    .bind(&resource)
                    .bind(wrong_pointer_version)
                    .execute(&mut *transaction)
                    .await?;
            }
            if case == "duplicate_inventory_versions" {
                // Distinct inventory versions sharing a resource must retain their individual name
                // identities: only this version has an event; the other has an equally valid surface.
                let mut other_version = provenance.clone();
                other_version["logical_name_id"] = json!("unrelated-version-name");
                sqlx::query("INSERT INTO record_inventory_current VALUES ($1::text::uuid, $2), ($1::text::uuid, $3)")
                .bind(&resource).bind(&provenance).bind(other_version)
                .execute(&mut *transaction).await?;
                sqlx::query("INSERT INTO name_surfaces VALUES ('unrelated-version-name', 'test-chain', '0xother-version', 10, 'shared-block', 'canonical')")
                .execute(&mut *transaction).await?;
            }
            if case == "multiple_resources" {
                let extra_resource = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee";
                sqlx::query("INSERT INTO record_inventory_current VALUES ($1::text::uuid, $2)")
                    .bind(extra_resource)
                    .bind(&provenance)
                    .execute(&mut *transaction)
                    .await?;
                expected_resources.push(extra_resource.to_owned());
            }
            let node_only = case.starts_with("node_");
            let logical_name = if node_only {
                None
            } else if case == "attributed_wrong_name" {
                Some("nonexistent-attributed-name")
            } else {
                Some(name.as_str())
            };
            let family = if case.contains("basenames") {
                "basenames_base_resolver"
            } else if case.ends_with("v2") {
                "ens_v2_resolver_l1"
            } else if case == "unknown_family" {
                "unknown"
            } else {
                "ens_v1_resolver_l1"
            };
            let mut raw_fact_ref = json!({"emitting_address": "0xpUbLiC"});
            match case {
                "missing_emitter" => {
                    raw_fact_ref = json!({});
                }
                "null_emitter" => {
                    raw_fact_ref["emitting_address"] = json!(null);
                }
                "empty_keys" => {
                    raw_fact_ref["emitting_address"] = json!("");
                }
                _ => {}
            }
            let after_state = match case {
                "attributed_wrong_node" => json!({"node": "0xother"}),
                "attributed_missing_node" | "node_missing_node" => json!({}),
                "node_null_node" => json!({"node": null}),
                _ => json!({"node": node.to_uppercase()}),
            };
            let event_count = if case.starts_with("node_basenames") && !basenames_node_changes {
                0
            } else if case == "duplicate_keys_and_events" {
                2
            } else {
                1
            };
            for _ in 0..event_count {
                sqlx::query("INSERT INTO project_changed_events VALUES ($1, $2, $3, $4, $5)")
                    .bind(logical_name)
                    .bind(if case == "wrong_event_kind" {
                        "ResolverChanged"
                    } else if case == "record_version" {
                        "RecordVersionChanged"
                    } else {
                        "RecordChanged"
                    })
                    .bind(family)
                    .bind(&raw_fact_ref)
                    .bind(&after_state)
                    .execute(&mut *transaction)
                    .await?;
            }
            if matches!(
                case,
                "node_basenames"
                    | "node_basenames_wrong_pointer"
                    | "node_basenames_duplicate_versions"
            ) {
                sqlx::query("INSERT INTO normalized_events VALUES ($1, $2)")
                    .bind(pointer_id)
                    .bind(if case != "node_basenames_wrong_pointer" {
                        "basenames_base_registry"
                    } else {
                        "ens_v1_registry_l1"
                    })
                    .execute(&mut *transaction)
                    .await?;
            }
            let block_number = if case == "surface_after_target" {
                11_i64
            } else {
                10
            };
            let block_hash = format!("block-{index}");
            let surface_chain = if case == "wrong_surface_chain" {
                "other-chain"
            } else {
                "test-chain"
            };
            sqlx::query("INSERT INTO name_surfaces VALUES ($1, $2, $3, $4, $5, $6)")
                .bind(&name)
                .bind(surface_chain)
                .bind(if case == "attributed_null_surface_node" {
                    None
                } else if case == "node_multiple_surfaces" {
                    Some("0xunmatched-initial-surface")
                } else {
                    Some(node.as_str())
                })
                .bind(block_number)
                .bind(&block_hash)
                .bind(if case == "surface_orphaned" {
                    "orphaned"
                } else if case == "surface_safe" {
                    "safe"
                } else {
                    "finalized"
                })
                .execute(&mut *transaction)
                .await?;
            if case != "lineage_missing" {
                sqlx::query("INSERT INTO chain_lineage VALUES ($1, $2, $3, $4)")
                    .bind(if case == "lineage_wrong_chain" {
                        "other-chain"
                    } else {
                        "test-chain"
                    })
                    .bind(if case == "lineage_wrong_height" {
                        9
                    } else {
                        block_number
                    })
                    .bind(if case == "lineage_wrong_hash" {
                        "wrong-hash"
                    } else {
                        &block_hash
                    })
                    .bind(if case == "lineage_orphaned" {
                        "orphaned"
                    } else if case == "lineage_safe" {
                        "safe"
                    } else {
                        "canonical"
                    })
                    .execute(&mut *transaction)
                    .await?;
            }
            if matches!(case, "multiple_surfaces" | "node_multiple_surfaces") {
                // Additional invalid and duplicate valid surfaces may not hide the valid one or
                // duplicate its scope rows. Distinct namehashes must also remain per-surface.
                sqlx::query(
                    "INSERT INTO name_surfaces VALUES
                ($1, 'test-chain', '0xwrong-node', 11, 'future', 'canonical'),
                ($1, 'test-chain', $2, 10, $3, 'orphaned'),
                ($1, 'test-chain', $2, 10, $3, 'canonical')",
                )
                .bind(&name)
                .bind(&node)
                .bind(&block_hash)
                .execute(&mut *transaction)
                .await?;
            }
            if expected {
                expected_names.push(name);
                expected_resources.push(resource);
            }
        }
        raw_sql(
            "INSERT INTO chain_lineage VALUES ('test-chain', 10, 'shared-block', 'canonical');
             INSERT INTO project_scope_names VALUES ('preexisting-name'), ('fixture-attributed_v1');
             INSERT INTO project_scope_resources VALUES
                 ('ffffffff-ffff-ffff-ffff-ffffffffffff'), ('00000000-0000-0000-0000-000000000000');
             INSERT INTO original_scope_names SELECT * FROM project_scope_names;
             INSERT INTO original_scope_resources SELECT * FROM project_scope_resources;",
        )
        .execute(&mut *transaction)
        .await?;
        expected_names.sort();
        expected_resources.sort();
        sqlx::query(ORIGINAL_RECORD_CONSUMER_QUERY)
            .bind("test-chain")
            .bind(10_i64)
            .execute(&mut *transaction)
            .await
            .context("original record-consumer query")?;
        let original_names: Vec<String> = sqlx::query_scalar(
            "SELECT logical_name_id FROM original_scope_names ORDER BY logical_name_id",
        )
        .fetch_all(&mut *transaction)
        .await?;
        let original_resources: Vec<String> = sqlx::query_scalar(
            "SELECT resource_id::text FROM original_scope_resources ORDER BY resource_id",
        )
        .fetch_all(&mut *transaction)
        .await?;
        // Expectations are authored independently of the frozen oracle. The oracle additionally
        // catches behavioral changes when adding more adversarial cases here in the future.
        assert_eq!(original_names, expected_names);
        assert_eq!(original_resources, expected_resources);
        for _ in 0..2 {
            include_changed_record_consumers(&mut transaction, "test-chain", 10).await?;
            let names: Vec<String> = sqlx::query_scalar(
                "SELECT logical_name_id FROM project_scope_names ORDER BY logical_name_id",
            )
            .fetch_all(&mut *transaction)
            .await?;
            let resources: Vec<String> = sqlx::query_scalar(
                "SELECT resource_id::text FROM project_scope_resources ORDER BY resource_id",
            )
            .fetch_all(&mut *transaction)
            .await?;
            assert_eq!(names, original_names);
            assert_eq!(resources, original_resources);
        }
        transaction.rollback().await?;
    }
    database.cleanup().await?;
    Ok(())
}
