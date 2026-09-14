use anyhow::Result;
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
