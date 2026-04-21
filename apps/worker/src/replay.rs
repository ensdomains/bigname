use anyhow::{Context, Result};
use sqlx::PgPool;

use crate::{
    address_names, children, name_current, permissions, primary_name, record_inventory, resolver,
};

pub const ALL_CURRENT_PROJECTION_ORDER: &[&str] = &[
    "name_current",
    "children_current",
    "permissions_current",
    "record_inventory_current",
    "resolver_current",
    "address_names_current",
    "primary_names_current",
];

pub const ALL_CURRENT_PROJECTION_JSON_ORDER: &[&str] = &[
    "address_names_current",
    "children_current",
    "coverage_current",
    "name_current",
    "permissions_current",
    "primary_names_current",
    "record_inventory_current",
    "resolver_current",
    "surface_bindings_current",
];

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AllCurrentProjectionsReplaySummary {
    pub steps: Vec<CurrentProjectionReplayStepSummary>,
}

impl AllCurrentProjectionsReplaySummary {
    pub fn projection_order(&self) -> Vec<&'static str> {
        self.steps.iter().map(|step| step.projection).collect()
    }

    pub fn total_upserted_row_count(&self) -> usize {
        self.steps.iter().map(|step| step.upserted_row_count).sum()
    }

    pub fn total_deleted_row_count(&self) -> u64 {
        self.steps.iter().map(|step| step.deleted_row_count).sum()
    }

    pub fn json_summary_value(&self) -> serde_json::Value {
        let projections = ALL_CURRENT_PROJECTION_JSON_ORDER
            .iter()
            .map(|projection| {
                let counts = self.projection_json_counts(projection);
                serde_json::json!({
                    "projection": projection,
                    "requested": counts.requested,
                    "upserted": counts.upserted,
                    "deleted": counts.deleted,
                })
            })
            .collect::<Vec<_>>();
        let totals = self.json_totals();

        serde_json::json!({
            "command": "all-current-projections",
            "projections": projections,
            "totals": {
                "requested": totals.requested,
                "upserted": totals.upserted,
                "deleted": totals.deleted,
            },
        })
    }

    pub fn json_summary_string(&self) -> serde_json::Result<String> {
        serde_json::to_string(&self.json_summary_value())
    }

    fn json_totals(&self) -> ProjectionJsonCounts {
        ALL_CURRENT_PROJECTION_JSON_ORDER.iter().fold(
            ProjectionJsonCounts::default(),
            |mut totals, projection| {
                let counts = self.projection_json_counts(projection);
                totals.requested += counts.requested;
                totals.upserted += counts.upserted;
                totals.deleted += counts.deleted;
                totals
            },
        )
    }

    fn projection_json_counts(&self, projection: &str) -> ProjectionJsonCounts {
        self.steps
            .iter()
            .find(|step| step.projection == projection)
            .map(|step| ProjectionJsonCounts {
                requested: step.requested_key_count as u64,
                upserted: step.upserted_row_count as u64,
                deleted: step.deleted_row_count,
            })
            .unwrap_or_default()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrentProjectionReplayStepSummary {
    pub projection: &'static str,
    pub requested_key_count: usize,
    pub upserted_row_count: usize,
    pub deleted_row_count: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ProjectionJsonCounts {
    requested: u64,
    upserted: u64,
    deleted: u64,
}

pub async fn rebuild_all_current_projections(
    pool: &PgPool,
) -> Result<AllCurrentProjectionsReplaySummary> {
    let mut steps = Vec::with_capacity(ALL_CURRENT_PROJECTION_ORDER.len());

    let summary = name_current::rebuild_name_current(pool, None)
        .await
        .context("failed to replay name_current")?;
    steps.push(CurrentProjectionReplayStepSummary {
        projection: "name_current",
        requested_key_count: summary.requested_name_count,
        upserted_row_count: summary.upserted_row_count,
        deleted_row_count: summary.deleted_row_count,
    });

    let summary = children::rebuild_children_current(pool, None)
        .await
        .context("failed to replay children_current")?;
    steps.push(CurrentProjectionReplayStepSummary {
        projection: "children_current",
        requested_key_count: summary.requested_parent_count,
        upserted_row_count: summary.upserted_row_count,
        deleted_row_count: summary.deleted_row_count,
    });

    let summary = permissions::rebuild_permissions_current(pool, None)
        .await
        .context("failed to replay permissions_current")?;
    steps.push(CurrentProjectionReplayStepSummary {
        projection: "permissions_current",
        requested_key_count: summary.requested_resource_count,
        upserted_row_count: summary.upserted_row_count,
        deleted_row_count: summary.deleted_row_count,
    });

    let summary = record_inventory::rebuild_record_inventory_current(pool, None)
        .await
        .context("failed to replay record_inventory_current")?;
    steps.push(CurrentProjectionReplayStepSummary {
        projection: "record_inventory_current",
        requested_key_count: summary.requested_resource_count,
        upserted_row_count: summary.upserted_row_count,
        deleted_row_count: summary.deleted_row_count,
    });

    let summary = resolver::rebuild_resolver_current(pool, None, None)
        .await
        .context("failed to replay resolver_current")?;
    steps.push(CurrentProjectionReplayStepSummary {
        projection: "resolver_current",
        requested_key_count: summary.requested_resolver_count,
        upserted_row_count: summary.upserted_row_count,
        deleted_row_count: summary.deleted_row_count,
    });

    let summary = address_names::rebuild_address_names_current(pool, None)
        .await
        .context("failed to replay address_names_current")?;
    steps.push(CurrentProjectionReplayStepSummary {
        projection: "address_names_current",
        requested_key_count: summary.requested_address_count,
        upserted_row_count: summary.upserted_row_count,
        deleted_row_count: summary.deleted_row_count,
    });

    let summary = primary_name::rebuild_primary_names_current(pool, None, None, None)
        .await
        .context("failed to replay primary_names_current")?;
    steps.push(CurrentProjectionReplayStepSummary {
        projection: "primary_names_current",
        requested_key_count: summary.requested_tuple_count,
        upserted_row_count: summary.upserted_row_count,
        deleted_row_count: summary.deleted_row_count,
    });

    debug_assert_eq!(
        steps.iter().map(|step| step.projection).collect::<Vec<_>>(),
        ALL_CURRENT_PROJECTION_ORDER
    );

    Ok(AllCurrentProjectionsReplaySummary { steps })
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        str::FromStr,
        sync::atomic::{AtomicU64, Ordering},
    };

    use anyhow::Result;
    use bigname_storage::{
        CanonicalityState, NameSurface, NormalizedEvent, RawBlock, Resource, SurfaceBinding,
        SurfaceBindingKind, TokenLineage, default_database_url, upsert_name_surfaces,
        upsert_normalized_events, upsert_raw_blocks, upsert_resources, upsert_surface_bindings,
        upsert_token_lineages,
    };
    use serde_json::{Value, json};
    use sqlx::{
        Row,
        postgres::{PgConnectOptions, PgPoolOptions},
        types::time::OffsetDateTime,
    };
    use uuid::Uuid;

    use super::*;

    const LOGICAL_NAME_ID: &str = "ens:alice.eth";
    const DISPLAY_NAME: &str = "alice.eth";
    const CHILD_LOGICAL_NAME_ID: &str = "ens:bob.alice.eth";
    const CHILD_DISPLAY_NAME: &str = "bob.alice.eth";
    const HOLDER_ADDRESS: &str = "0x0000000000000000000000000000000000000abc";
    const RESOLVER_ADDRESS: &str = "0x0000000000000000000000000000000000000def";

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

    struct TestDatabase {
        admin_pool: PgPool,
        pool: PgPool,
        database_name: String,
    }

    impl TestDatabase {
        async fn new() -> Result<Self> {
            let database_url = std::env::var("BIGNAME_DATABASE_URL")
                .or_else(|_| std::env::var("DATABASE_URL"))
                .unwrap_or_else(|_| default_database_url().to_owned());
            let base_options = PgConnectOptions::from_str(&database_url)
                .context("failed to parse database URL for worker replay tests")?;
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bg_wr_replay_{}_{}_{}",
                std::process::id(),
                sequence,
                &Uuid::new_v4().simple().to_string()[..8]
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for worker replay tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect worker replay test pool")?;

            bigname_storage::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for worker replay tests")?;

            Ok(Self {
                admin_pool,
                pool,
                database_name,
            })
        }

        fn pool(&self) -> &PgPool {
            &self.pool
        }

        async fn cleanup(self) -> Result<()> {
            self.pool.close().await;
            sqlx::query(&format!(
                r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
                self.database_name
            ))
            .execute(&self.admin_pool)
            .await
            .with_context(|| format!("failed to drop test database {}", self.database_name))?;
            self.admin_pool.close().await;
            Ok(())
        }
    }

    #[test]
    fn all_current_projection_json_summary_has_frozen_shape_order_counts_and_totals() -> Result<()>
    {
        let summary = AllCurrentProjectionsReplaySummary {
            steps: vec![
                CurrentProjectionReplayStepSummary {
                    projection: "name_current",
                    requested_key_count: 2,
                    upserted_row_count: 2,
                    deleted_row_count: 0,
                },
                CurrentProjectionReplayStepSummary {
                    projection: "children_current",
                    requested_key_count: 1,
                    upserted_row_count: 1,
                    deleted_row_count: 0,
                },
                CurrentProjectionReplayStepSummary {
                    projection: "permissions_current",
                    requested_key_count: 1,
                    upserted_row_count: 1,
                    deleted_row_count: 0,
                },
                CurrentProjectionReplayStepSummary {
                    projection: "record_inventory_current",
                    requested_key_count: 1,
                    upserted_row_count: 1,
                    deleted_row_count: 0,
                },
                CurrentProjectionReplayStepSummary {
                    projection: "resolver_current",
                    requested_key_count: 1,
                    upserted_row_count: 1,
                    deleted_row_count: 0,
                },
                CurrentProjectionReplayStepSummary {
                    projection: "address_names_current",
                    requested_key_count: 2,
                    upserted_row_count: 3,
                    deleted_row_count: 0,
                },
                CurrentProjectionReplayStepSummary {
                    projection: "primary_names_current",
                    requested_key_count: 1,
                    upserted_row_count: 1,
                    deleted_row_count: 0,
                },
            ],
        };

        let encoded = summary.json_summary_string()?;
        let value: Value = serde_json::from_str(&encoded)?;
        assert_json_object_fields(&value, ["command", "projections", "totals"]);
        assert_eq!(value["command"], "all-current-projections");

        let projections = value["projections"]
            .as_array()
            .context("projections must be an array")?;
        let projection_order = projections
            .iter()
            .map(|projection| {
                projection["projection"]
                    .as_str()
                    .context("projection name must be a string")
            })
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(projection_order, ALL_CURRENT_PROJECTION_JSON_ORDER.to_vec());

        let expected_counts = BTreeMap::from([
            ("address_names_current", (2, 3, 0)),
            ("children_current", (1, 1, 0)),
            ("coverage_current", (0, 0, 0)),
            ("name_current", (2, 2, 0)),
            ("permissions_current", (1, 1, 0)),
            ("primary_names_current", (1, 1, 0)),
            ("record_inventory_current", (1, 1, 0)),
            ("resolver_current", (1, 1, 0)),
            ("surface_bindings_current", (0, 0, 0)),
        ]);

        for projection in projections {
            assert_json_object_fields(
                projection,
                ["projection", "requested", "upserted", "deleted"],
            );
            let projection_name = projection["projection"]
                .as_str()
                .context("projection name must be a string")?;
            let (requested, upserted, deleted) = expected_counts
                .get(projection_name)
                .copied()
                .with_context(|| format!("unexpected projection {projection_name}"))?;
            assert_eq!(projection["requested"].as_u64(), Some(requested));
            assert_eq!(projection["upserted"].as_u64(), Some(upserted));
            assert_eq!(projection["deleted"].as_u64(), Some(deleted));
        }

        assert_json_object_fields(&value["totals"], ["requested", "upserted", "deleted"]);
        assert_eq!(value["totals"]["requested"].as_u64(), Some(9));
        assert_eq!(value["totals"]["upserted"].as_u64(), Some(10));
        assert_eq!(value["totals"]["deleted"].as_u64(), Some(0));

        Ok(())
    }

    #[tokio::test]
    async fn all_current_projection_replay_clears_stale_rows_and_is_idempotent() -> Result<()> {
        let database = TestDatabase::new().await?;
        seed_replay_inputs(database.pool()).await?;

        let first_summary = rebuild_all_current_projections(database.pool()).await?;
        assert_eq!(
            first_summary.projection_order(),
            ALL_CURRENT_PROJECTION_ORDER
        );
        assert_eq!(first_summary.total_deleted_row_count(), 0);

        let first_snapshot = load_api_visible_projection_snapshot(database.pool()).await?;
        assert_projection_counts(
            &first_snapshot,
            [
                ("name_current", 2),
                ("children_current", 1),
                ("permissions_current", 1),
                ("record_inventory_current", 1),
                ("resolver_current", 1),
                ("address_names_current", 3),
                ("primary_names_current", 1),
            ],
        );

        insert_stale_projection_rows(database.pool()).await?;
        let stale_snapshot = load_api_visible_projection_snapshot(database.pool()).await?;
        for projection in ALL_CURRENT_PROJECTION_ORDER {
            assert!(
                stale_snapshot.row_count(projection) > first_snapshot.row_count(projection),
                "{projection} should contain an injected stale row before replay"
            );
        }

        let second_summary = rebuild_all_current_projections(database.pool()).await?;
        assert_eq!(
            second_summary.projection_order(),
            ALL_CURRENT_PROJECTION_ORDER
        );
        assert!(second_summary.total_deleted_row_count() >= 6);
        let second_snapshot = load_api_visible_projection_snapshot(database.pool()).await?;
        assert_eq!(first_snapshot, second_snapshot);

        let third_summary = rebuild_all_current_projections(database.pool()).await?;
        assert_eq!(
            third_summary.projection_order(),
            ALL_CURRENT_PROJECTION_ORDER
        );
        let third_snapshot = load_api_visible_projection_snapshot(database.pool()).await?;
        assert_eq!(second_snapshot.row_counts(), third_snapshot.row_counts());
        assert_eq!(second_snapshot, third_snapshot);

        database.cleanup().await
    }

    fn assert_json_object_fields<const N: usize>(value: &Value, expected: [&str; N]) {
        let object = value.as_object().expect("value must be a JSON object");
        assert_eq!(object.len(), N);
        for key in expected {
            assert!(object.contains_key(key), "missing JSON field {key}");
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct ProjectionSnapshot {
        rows: BTreeMap<&'static str, Vec<Value>>,
    }

    impl ProjectionSnapshot {
        fn row_count(&self, projection: &str) -> usize {
            self.rows.get(projection).map(Vec::len).unwrap_or_default()
        }

        fn row_counts(&self) -> BTreeMap<&'static str, usize> {
            self.rows
                .iter()
                .map(|(projection, rows)| (*projection, rows.len()))
                .collect()
        }
    }

    fn assert_projection_counts<const N: usize>(
        snapshot: &ProjectionSnapshot,
        expected: [(&'static str, usize); N],
    ) {
        for (projection, count) in expected {
            assert_eq!(
                snapshot.row_count(projection),
                count,
                "{projection} row count mismatch"
            );
        }
    }

    async fn load_api_visible_projection_snapshot(pool: &PgPool) -> Result<ProjectionSnapshot> {
        let mut rows = BTreeMap::new();
        rows.insert(
            "name_current",
            projection_rows(
                pool,
                r#"
                SELECT jsonb_build_object(
                    'logical_name_id', logical_name_id,
                    'namespace', namespace,
                    'canonical_display_name', canonical_display_name,
                    'normalized_name', normalized_name,
                    'namehash', namehash,
                    'surface_binding_id', surface_binding_id,
                    'resource_id', resource_id,
                    'token_lineage_id', token_lineage_id,
                    'binding_kind', binding_kind,
                    'declared_summary', declared_summary,
                    'provenance', provenance,
                    'coverage', coverage,
                    'chain_positions', chain_positions,
                    'canonicality_summary', canonicality_summary,
                    'manifest_version', manifest_version,
                    'last_recomputed_at', last_recomputed_at
                ) AS row
                FROM name_current
                ORDER BY logical_name_id
                "#,
            )
            .await?,
        );
        rows.insert(
            "children_current",
            projection_rows(
                pool,
                r#"
                SELECT jsonb_build_object(
                    'parent_logical_name_id', parent_logical_name_id,
                    'child_logical_name_id', child_logical_name_id,
                    'surface_class', surface_class,
                    'namespace', namespace,
                    'canonical_display_name', canonical_display_name,
                    'normalized_name', normalized_name,
                    'namehash', namehash,
                    'provenance', provenance,
                    'chain_positions', chain_positions,
                    'canonicality_summary', canonicality_summary,
                    'manifest_version', manifest_version,
                    'last_recomputed_at', last_recomputed_at
                ) AS row
                FROM children_current
                ORDER BY parent_logical_name_id, child_logical_name_id, surface_class
                "#,
            )
            .await?,
        );
        rows.insert(
            "permissions_current",
            projection_rows(
                pool,
                r#"
                SELECT jsonb_build_object(
                    'resource_id', resource_id,
                    'subject', subject,
                    'scope', scope,
                    'scope_kind', scope_kind,
                    'scope_detail', scope_detail,
                    'effective_powers', effective_powers,
                    'grant_source', grant_source,
                    'revocation_source', revocation_source,
                    'inheritance_path', inheritance_path,
                    'transfer_behavior', transfer_behavior,
                    'provenance', provenance,
                    'coverage', coverage,
                    'chain_positions', chain_positions,
                    'canonicality_summary', canonicality_summary,
                    'manifest_version', manifest_version,
                    'last_recomputed_at', last_recomputed_at
                ) AS row
                FROM permissions_current
                ORDER BY resource_id, subject, scope
                "#,
            )
            .await?,
        );
        rows.insert(
            "record_inventory_current",
            projection_rows(
                pool,
                r#"
                SELECT jsonb_build_object(
                    'resource_id', resource_id,
                    'record_version_boundary_key', record_version_boundary_key,
                    'record_version_boundary', record_version_boundary,
                    'enumeration_basis', enumeration_basis,
                    'selectors', selectors,
                    'explicit_gaps', explicit_gaps,
                    'unsupported_families', unsupported_families,
                    'last_change', last_change,
                    'entries', entries,
                    'provenance', provenance,
                    'coverage', coverage,
                    'chain_positions', chain_positions,
                    'canonicality_summary', canonicality_summary,
                    'manifest_version', manifest_version,
                    'last_recomputed_at', last_recomputed_at
                ) AS row
                FROM record_inventory_current
                ORDER BY resource_id, record_version_boundary_key
                "#,
            )
            .await?,
        );
        rows.insert(
            "resolver_current",
            projection_rows(
                pool,
                r#"
                SELECT jsonb_build_object(
                    'chain_id', chain_id,
                    'resolver_address', resolver_address,
                    'declared_summary', declared_summary,
                    'provenance', provenance,
                    'coverage', coverage,
                    'chain_positions', chain_positions,
                    'canonicality_summary', canonicality_summary,
                    'manifest_version', manifest_version,
                    'last_recomputed_at', last_recomputed_at
                ) AS row
                FROM resolver_current
                ORDER BY chain_id, resolver_address
                "#,
            )
            .await?,
        );
        rows.insert(
            "address_names_current",
            projection_rows(
                pool,
                r#"
                SELECT jsonb_build_object(
                    'address', address,
                    'logical_name_id', logical_name_id,
                    'relation', relation,
                    'namespace', namespace,
                    'canonical_display_name', canonical_display_name,
                    'normalized_name', normalized_name,
                    'namehash', namehash,
                    'surface_binding_id', surface_binding_id,
                    'resource_id', resource_id,
                    'token_lineage_id', token_lineage_id,
                    'binding_kind', binding_kind,
                    'provenance', provenance,
                    'coverage', coverage,
                    'chain_positions', chain_positions,
                    'canonicality_summary', canonicality_summary,
                    'manifest_version', manifest_version,
                    'last_recomputed_at', last_recomputed_at
                ) AS row
                FROM address_names_current
                ORDER BY address, namespace, canonical_display_name, logical_name_id, relation
                "#,
            )
            .await?,
        );
        rows.insert(
            "primary_names_current",
            projection_rows(
                pool,
                r#"
                SELECT jsonb_build_object(
                    'address', address,
                    'namespace', namespace,
                    'coin_type', coin_type,
                    'claim_status', claim_status,
                    'raw_claim_name', raw_claim_name,
                    'normalized_claim_name', normalized_claim_name,
                    'claim_provenance', claim_provenance
                ) AS row
                FROM primary_names_current
                ORDER BY address, namespace, coin_type
                "#,
            )
            .await?,
        );

        Ok(ProjectionSnapshot { rows })
    }

    async fn projection_rows(pool: &PgPool, query: &str) -> Result<Vec<Value>> {
        let rows = sqlx::query(query)
            .fetch_all(pool)
            .await
            .context("failed to load projection snapshot rows")?;

        rows.into_iter()
            .map(|row| {
                row.try_get("row")
                    .context("missing projection snapshot row")
            })
            .collect()
    }

    async fn seed_replay_inputs(pool: &PgPool) -> Result<()> {
        let token_lineage_id = Uuid::from_u128(0x1001);
        let resource_id = Uuid::from_u128(0x1002);
        let surface_binding_id = Uuid::from_u128(0x1003);

        upsert_raw_blocks(
            pool,
            &[
                raw_block("0xreplay0100", 100, 1_776_300_100),
                raw_block("0xreplay0101", 101, 1_776_300_101),
                raw_block("0xreplay0102", 102, 1_776_300_102),
                raw_block("0xreplay0103", 103, 1_776_300_103),
                raw_block("0xreplay0104", 104, 1_776_300_104),
                raw_block("0xreplay0105", 105, 1_776_300_105),
                raw_block("0xreplay0106", 106, 1_776_300_106),
                raw_block("0xreplay0107", 107, 1_776_300_107),
            ],
        )
        .await?;
        upsert_token_lineages(
            pool,
            &[TokenLineage {
                token_lineage_id,
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0xreplay0100".to_owned(),
                block_number: 100,
                provenance: json!({"source": "worker_replay_test", "kind": "token_lineage"}),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;
        upsert_resources(
            pool,
            &[Resource {
                resource_id,
                token_lineage_id: Some(token_lineage_id),
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0xreplay0100".to_owned(),
                block_number: 100,
                provenance: json!({"source": "worker_replay_test", "kind": "resource"}),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;
        upsert_name_surfaces(
            pool,
            &[
                name_surface(LOGICAL_NAME_ID, DISPLAY_NAME, CanonicalityState::Finalized),
                name_surface(
                    CHILD_LOGICAL_NAME_ID,
                    CHILD_DISPLAY_NAME,
                    CanonicalityState::Finalized,
                ),
            ],
        )
        .await?;
        upsert_surface_bindings(
            pool,
            &[SurfaceBinding {
                surface_binding_id,
                logical_name_id: LOGICAL_NAME_ID.to_owned(),
                resource_id,
                binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
                active_from: timestamp(1_776_300_100),
                active_to: None,
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0xreplay0100".to_owned(),
                block_number: 100,
                provenance: json!({"source": "worker_replay_test", "kind": "surface_binding"}),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;
        upsert_normalized_events(
            pool,
            &[
                registration_granted_event(resource_id),
                resolver_changed_event(resource_id),
                subregistry_event(),
                permission_changed_event(resource_id),
                record_version_changed_event(resource_id),
                record_changed_event(resource_id),
                reverse_changed_event(),
            ],
        )
        .await?;

        Ok(())
    }

    async fn insert_stale_projection_rows(pool: &PgPool) -> Result<()> {
        let stale_resource_id = Uuid::from_u128(0x9001);
        let stale_surface_binding_id = Uuid::from_u128(0x9002);
        upsert_resources(
            pool,
            &[Resource {
                resource_id: stale_resource_id,
                token_lineage_id: None,
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0xstale-resource".to_owned(),
                block_number: 1,
                provenance: json!({"source": "worker_replay_test", "kind": "stale_resource"}),
                canonicality_state: CanonicalityState::Observed,
            }],
        )
        .await?;
        upsert_name_surfaces(
            pool,
            &[
                name_surface("ens:stale.eth", "stale.eth", CanonicalityState::Observed),
                name_surface(
                    "ens:stale-child.eth",
                    "stale-child.eth",
                    CanonicalityState::Observed,
                ),
            ],
        )
        .await?;
        upsert_surface_bindings(
            pool,
            &[SurfaceBinding {
                surface_binding_id: stale_surface_binding_id,
                logical_name_id: "ens:stale.eth".to_owned(),
                resource_id: stale_resource_id,
                binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
                active_from: timestamp(1_776_300_001),
                active_to: None,
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0xstale-binding".to_owned(),
                block_number: 1,
                provenance: json!({"source": "worker_replay_test", "kind": "stale_binding"}),
                canonicality_state: CanonicalityState::Observed,
            }],
        )
        .await?;

        sqlx::query(
            r#"
            INSERT INTO name_current (
                logical_name_id,
                namespace,
                canonical_display_name,
                normalized_name,
                namehash,
                declared_summary,
                provenance,
                coverage,
                chain_positions,
                canonicality_summary,
                manifest_version,
                last_recomputed_at
            )
            VALUES (
                'ens:stale.eth',
                'ens',
                'stale.eth',
                'stale.eth',
                'namehash:stale.eth',
                '{}'::jsonb,
                '{}'::jsonb,
                '{}'::jsonb,
                '{}'::jsonb,
                '{}'::jsonb,
                1,
                '2026-04-20T00:00:00Z'::timestamptz
            )
            "#,
        )
        .execute(pool)
        .await
        .context("failed to insert stale name_current row")?;

        sqlx::query(
            r#"
            INSERT INTO children_current (
                parent_logical_name_id,
                child_logical_name_id,
                surface_class,
                namespace,
                canonical_display_name,
                normalized_name,
                namehash,
                provenance,
                chain_positions,
                canonicality_summary,
                manifest_version,
                last_recomputed_at
            )
            VALUES (
                'ens:stale.eth',
                'ens:stale-child.eth',
                'declared',
                'ens',
                'stale-child.eth',
                'stale-child.eth',
                'namehash:stale-child.eth',
                '{}'::jsonb,
                '{}'::jsonb,
                '{}'::jsonb,
                1,
                '2026-04-20T00:00:00Z'::timestamptz
            )
            "#,
        )
        .execute(pool)
        .await
        .context("failed to insert stale children_current row")?;

        sqlx::query(
            r#"
            INSERT INTO permissions_current (
                resource_id,
                subject,
                scope,
                scope_kind,
                scope_detail,
                effective_powers,
                grant_source,
                inheritance_path,
                transfer_behavior,
                provenance,
                coverage,
                chain_positions,
                canonicality_summary,
                manifest_version,
                last_recomputed_at
            )
            VALUES (
                $1,
                '0x0000000000000000000000000000000000000bad',
                'resource',
                'resource',
                '{}'::jsonb,
                '["set_records"]'::jsonb,
                '{}'::jsonb,
                '[]'::jsonb,
                '{}'::jsonb,
                '{}'::jsonb,
                '{}'::jsonb,
                '{}'::jsonb,
                '{}'::jsonb,
                1,
                '2026-04-20T00:00:00Z'::timestamptz
            )
            "#,
        )
        .bind(stale_resource_id)
        .execute(pool)
        .await
        .context("failed to insert stale permissions_current row")?;

        sqlx::query(
            r#"
            INSERT INTO record_inventory_current (
                resource_id,
                record_version_boundary_key,
                record_version_boundary,
                enumeration_basis,
                selectors,
                explicit_gaps,
                unsupported_families,
                entries,
                provenance,
                coverage,
                chain_positions,
                canonicality_summary,
                manifest_version,
                last_recomputed_at
            )
            VALUES (
                $1,
                'stale',
                '{}'::jsonb,
                '{}'::jsonb,
                '[]'::jsonb,
                '[]'::jsonb,
                '[]'::jsonb,
                '[]'::jsonb,
                '{}'::jsonb,
                '{}'::jsonb,
                '{}'::jsonb,
                '{}'::jsonb,
                1,
                '2026-04-20T00:00:00Z'::timestamptz
            )
            "#,
        )
        .bind(stale_resource_id)
        .execute(pool)
        .await
        .context("failed to insert stale record_inventory_current row")?;

        sqlx::query(
            r#"
            INSERT INTO resolver_current (
                chain_id,
                resolver_address,
                declared_summary,
                provenance,
                coverage,
                chain_positions,
                canonicality_summary,
                manifest_version,
                last_recomputed_at
            )
            VALUES (
                'ethereum-mainnet',
                '0x0000000000000000000000000000000000000bad',
                '{}'::jsonb,
                '{}'::jsonb,
                '{}'::jsonb,
                '{}'::jsonb,
                '{}'::jsonb,
                1,
                '2026-04-20T00:00:00Z'::timestamptz
            )
            "#,
        )
        .execute(pool)
        .await
        .context("failed to insert stale resolver_current row")?;

        sqlx::query(
            r#"
            INSERT INTO address_names_current (
                address,
                logical_name_id,
                relation,
                namespace,
                canonical_display_name,
                normalized_name,
                namehash,
                surface_binding_id,
                resource_id,
                binding_kind,
                provenance,
                coverage,
                chain_positions,
                canonicality_summary,
                manifest_version,
                last_recomputed_at
            )
            VALUES (
                '0x0000000000000000000000000000000000000bad',
                'ens:stale.eth',
                'registrant',
                'ens',
                'stale.eth',
                'stale.eth',
                'namehash:stale.eth',
                $1,
                $2,
                'declared_registry_path',
                '{}'::jsonb,
                '{}'::jsonb,
                '{}'::jsonb,
                '{}'::jsonb,
                1,
                '2026-04-20T00:00:00Z'::timestamptz
            )
            "#,
        )
        .bind(stale_surface_binding_id)
        .bind(stale_resource_id)
        .execute(pool)
        .await
        .context("failed to insert stale address_names_current row")?;

        sqlx::query(
            r#"
            INSERT INTO primary_names_current (
                address,
                coin_type,
                namespace,
                claim_status,
                raw_claim_name,
                claim_provenance,
                normalized_claim_name
            )
            VALUES (
                '0x0000000000000000000000000000000000000bad',
                '60',
                'ens',
                'unsupported',
                NULL,
                '{}'::jsonb,
                NULL
            )
            "#,
        )
        .execute(pool)
        .await
        .context("failed to insert stale primary_names_current row")?;

        Ok(())
    }

    fn raw_block(block_hash: &str, block_number: i64, unix_timestamp: i64) -> RawBlock {
        RawBlock {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: block_hash.to_owned(),
            parent_hash: None,
            block_number,
            block_timestamp: timestamp(unix_timestamp),
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    fn name_surface(
        logical_name_id: &str,
        display_name: &str,
        canonicality_state: CanonicalityState,
    ) -> NameSurface {
        NameSurface {
            logical_name_id: logical_name_id.to_owned(),
            namespace: "ens".to_owned(),
            input_name: display_name.to_owned(),
            canonical_display_name: display_name.to_owned(),
            normalized_name: display_name.to_owned(),
            dns_encoded_name: display_name.as_bytes().to_vec(),
            namehash: format!("namehash:{display_name}"),
            labelhashes: vec![format!("labelhash:{display_name}")],
            normalizer_version: "ensip15@2026-04-16".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: "0xreplay0100".to_owned(),
            block_number: 100,
            provenance: json!({"source": "worker_replay_test", "kind": "name_surface"}),
            canonicality_state,
        }
    }

    fn registration_granted_event(resource_id: Uuid) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: "worker-replay:registration-granted".to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: Some(LOGICAL_NAME_ID.to_owned()),
            resource_id: Some(resource_id),
            event_kind: "RegistrationGranted".to_owned(),
            source_family: "ens_v1_registrar_l1".to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            chain_id: Some("ethereum-mainnet".to_owned()),
            block_number: Some(101),
            block_hash: Some("0xreplay0101".to_owned()),
            transaction_hash: Some("0xreplaytx0101".to_owned()),
            log_index: Some(0),
            raw_fact_ref: raw_fact_ref("0xreplay0101", 101, 0),
            derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({}),
            after_state: json!({
                "authority_kind": "registrar",
                "authority_key": "registrar:ethereum-mainnet:alice",
                "registrant": HOLDER_ADDRESS,
                "expiry": 1_900_000_000_i64,
            }),
        }
    }

    fn resolver_changed_event(resource_id: Uuid) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: "worker-replay:resolver-changed".to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: Some(LOGICAL_NAME_ID.to_owned()),
            resource_id: Some(resource_id),
            event_kind: "ResolverChanged".to_owned(),
            source_family: "ens_v1_resolver_l1".to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            chain_id: Some("ethereum-mainnet".to_owned()),
            block_number: Some(102),
            block_hash: Some("0xreplay0102".to_owned()),
            transaction_hash: Some("0xreplaytx0102".to_owned()),
            log_index: Some(0),
            raw_fact_ref: raw_fact_ref("0xreplay0102", 102, 0),
            derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({}),
            after_state: json!({
                "resolver": RESOLVER_ADDRESS,
                "namehash": format!("namehash:{DISPLAY_NAME}"),
            }),
        }
    }

    fn subregistry_event() -> NormalizedEvent {
        NormalizedEvent {
            event_identity: "worker-replay:subregistry".to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: None,
            resource_id: None,
            event_kind: "SubregistryChanged".to_owned(),
            source_family: "ens_v1_registry_l1".to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            chain_id: Some("ethereum-mainnet".to_owned()),
            block_number: Some(103),
            block_hash: Some("0xreplay0103".to_owned()),
            transaction_hash: Some("0xreplaytx0103".to_owned()),
            log_index: Some(0),
            raw_fact_ref: raw_fact_ref("0xreplay0103", 103, 0),
            derivation_kind: "ens_v1_subregistry_changed".to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({}),
            after_state: json!({
                "source_event": "NewOwner",
                "edge_kind": "subregistry",
                "parent_node": format!("namehash:{DISPLAY_NAME}"),
                "child_node": format!("namehash:{CHILD_DISPLAY_NAME}"),
                "labelhash": format!("labelhash:{CHILD_DISPLAY_NAME}"),
                "owner": HOLDER_ADDRESS,
                "tombstone": false,
                "active_edge": true,
            }),
        }
    }

    fn permission_changed_event(resource_id: Uuid) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: "worker-replay:permission-changed".to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: Some(LOGICAL_NAME_ID.to_owned()),
            resource_id: Some(resource_id),
            event_kind: "PermissionChanged".to_owned(),
            source_family: "ens_v1_resolver_l1".to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            chain_id: Some("ethereum-mainnet".to_owned()),
            block_number: Some(104),
            block_hash: Some("0xreplay0104".to_owned()),
            transaction_hash: Some("0xreplaytx0104".to_owned()),
            log_index: Some(0),
            raw_fact_ref: raw_fact_ref("0xreplay0104", 104, 0),
            derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({}),
            after_state: json!({
                "subject": HOLDER_ADDRESS,
                "scope": {
                    "kind": "resolver",
                    "chain_id": "ethereum-mainnet",
                    "resolver_address": RESOLVER_ADDRESS,
                },
                "effective_powers": ["set_resolver"],
                "grant_source": {
                    "kind": "normalized_event",
                    "event_identity": "worker-replay:permission-changed",
                },
                "revocation_source": Value::Null,
                "inheritance_path": [],
                "transfer_behavior": {},
            }),
        }
    }

    fn record_version_changed_event(resource_id: Uuid) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: "worker-replay:record-version-changed".to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: Some(LOGICAL_NAME_ID.to_owned()),
            resource_id: Some(resource_id),
            event_kind: "RecordVersionChanged".to_owned(),
            source_family: "ens_v1_resolver_l1".to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            chain_id: Some("ethereum-mainnet".to_owned()),
            block_number: Some(105),
            block_hash: Some("0xreplay0105".to_owned()),
            transaction_hash: Some("0xreplaytx0105".to_owned()),
            log_index: Some(0),
            raw_fact_ref: raw_fact_ref("0xreplay0105", 105, 0),
            derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({"record_version": 0}),
            after_state: json!({"record_version": 1}),
        }
    }

    fn record_changed_event(resource_id: Uuid) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: "worker-replay:record-changed".to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: Some(LOGICAL_NAME_ID.to_owned()),
            resource_id: Some(resource_id),
            event_kind: "RecordChanged".to_owned(),
            source_family: "ens_v1_resolver_l1".to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            chain_id: Some("ethereum-mainnet".to_owned()),
            block_number: Some(106),
            block_hash: Some("0xreplay0106".to_owned()),
            transaction_hash: Some("0xreplaytx0106".to_owned()),
            log_index: Some(0),
            raw_fact_ref: raw_fact_ref("0xreplay0106", 106, 0),
            derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({}),
            after_state: json!({
                "record_key": "text",
                "record_family": "text",
                "selector_key": Value::Null,
            }),
        }
    }

    fn reverse_changed_event() -> NormalizedEvent {
        NormalizedEvent {
            event_identity: "worker-replay:reverse-changed".to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: None,
            resource_id: None,
            event_kind: "ReverseChanged".to_owned(),
            source_family: "ens_v1_reverse_l1".to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            chain_id: Some("ethereum-mainnet".to_owned()),
            block_number: Some(107),
            block_hash: Some("0xreplay0107".to_owned()),
            transaction_hash: Some("0xreplaytx0107".to_owned()),
            log_index: Some(0),
            raw_fact_ref: raw_fact_ref("0xreplay0107", 107, 0),
            derivation_kind: "ens_v1_reverse_claim".to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({}),
            after_state: json!({
                "source_event": "ReverseClaimed",
                "address": HOLDER_ADDRESS,
                "coin_type": "60",
                "namespace": "ens",
                "reverse_namespace": "ens",
                "reverse_label": HOLDER_ADDRESS.trim_start_matches("0x"),
                "reverse_name": format!("{}.addr.reverse", HOLDER_ADDRESS.trim_start_matches("0x")),
                "reverse_node": "0xreplayreverse",
                "claim_provenance": {
                    "source_family": "ens_v1_reverse_l1",
                    "contract_role": "reverse_registrar",
                    "contract_instance_id": "00000000-0000-0000-0000-000000000107",
                    "emitting_address": "0x00000000000000000000000000000000000000ad",
                },
            }),
        }
    }

    fn raw_fact_ref(block_hash: &str, block_number: i64, log_index: i64) -> Value {
        json!({
            "kind": "raw_log",
            "chain_id": "ethereum-mainnet",
            "block_hash": block_hash,
            "block_number": block_number,
            "log_index": log_index,
        })
    }

    fn timestamp(value: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(value).expect("timestamp must be valid")
    }
}
