use anyhow::{Context, Result, bail};
use sqlx::Row;

use super::state::Step;

mod permissions_delete_sql;
use permissions_delete_sql::permissions_current_sql;

pub(super) struct DeletedBatch {
    pub(super) row_count: i64,
    pub(super) range_start: Option<String>,
    pub(super) range_end: Option<String>,
}

const ADDRESS_NAMES_CANDIDATES: &str = "base_rederive_delete_address_names_current_candidates";
const NAME_CURRENT_CANDIDATES: &str = "base_rederive_delete_name_current_candidates";
const CHILDREN_CURRENT_CANDIDATES: &str = "base_rederive_delete_children_current_candidates";
const PERMISSIONS_CURRENT_CANDIDATES: &str = "base_rederive_delete_permissions_current_candidates";
const RECORD_INVENTORY_CURRENT_CANDIDATES: &str =
    "base_rederive_delete_record_inventory_current_candidates";
const PROJECTION_CHANGES_CANDIDATES: &str = "base_rederive_delete_projection_change_candidates";
const NORMALIZED_EVENTS_CANDIDATES: &str = "base_rederive_delete_normalized_event_candidates";
const SURFACE_BINDINGS_CANDIDATES: &str = "base_rederive_delete_surface_binding_candidates";
const RESOURCES_CANDIDATES: &str = "base_rederive_delete_resource_candidates";
const NAME_SURFACES_CANDIDATES: &str = "base_rederive_delete_name_surface_candidates";
const TOKEN_LINEAGES_CANDIDATES: &str = "base_rederive_delete_token_lineage_candidates";

const DELETE_CANDIDATE_TABLES: &[&str] = &[
    ADDRESS_NAMES_CANDIDATES,
    NAME_CURRENT_CANDIDATES,
    CHILDREN_CURRENT_CANDIDATES,
    PERMISSIONS_CURRENT_CANDIDATES,
    RECORD_INVENTORY_CURRENT_CANDIDATES,
    PROJECTION_CHANGES_CANDIDATES,
    NORMALIZED_EVENTS_CANDIDATES,
    SURFACE_BINDINGS_CANDIDATES,
    RESOURCES_CANDIDATES,
    NAME_SURFACES_CANDIDATES,
    TOKEN_LINEAGES_CANDIDATES,
];

const ADDRESS_NAMES_CURRENT_SIDECAR_TRIGGERS: SidecarTriggerSet = SidecarTriggerSet {
    table_name: "address_names_current",
    triggers: &[
        "address_names_current_identity_counts_after_delete",
        "address_names_current_identity_counts_after_insert",
        "address_names_current_identity_counts_after_update",
        "address_names_current_identity_feed_after_insert_delete",
        "address_names_current_identity_feed_after_anchor_update",
    ],
};

const NAME_CURRENT_SIDECAR_TRIGGERS: SidecarTriggerSet = SidecarTriggerSet {
    table_name: "name_current",
    triggers: &[
        "address_names_current_identity_counts_name_current_insert_delet",
        "address_names_current_identity_counts_name_current_update",
        "name_current_identity_feed_after_insert_delete",
        "name_current_identity_feed_after_anchor_update",
    ],
};

const SURFACE_BINDINGS_SIDECAR_TRIGGERS: SidecarTriggerSet = SidecarTriggerSet {
    table_name: "surface_bindings",
    triggers: &["surface_bindings_identity_feed_after_delete"],
};

const RESOURCES_SIDECAR_TRIGGERS: SidecarTriggerSet = SidecarTriggerSet {
    table_name: "resources",
    triggers: &["resources_identity_feed_after_delete"],
};

const NAME_SURFACES_SIDECAR_TRIGGERS: SidecarTriggerSet = SidecarTriggerSet {
    table_name: "name_surfaces",
    triggers: &["name_surfaces_identity_feed_after_delete"],
};

const TOKEN_LINEAGES_SIDECAR_TRIGGERS: SidecarTriggerSet = SidecarTriggerSet {
    table_name: "token_lineages",
    triggers: &["token_lineages_identity_feed_after_delete"],
};

struct SidecarTriggerSet {
    table_name: &'static str,
    triggers: &'static [&'static str],
}

struct CandidateTableSpec {
    table_name: &'static str,
    create_sql: &'static str,
    index_sql: &'static str,
    insert_sqls: Vec<String>,
}

pub(super) async fn reset_delete_candidate_tables(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    for table in DELETE_CANDIDATE_TABLES {
        execute_sql(transaction, &format!("DROP TABLE IF EXISTS {table}")).await?;
    }
    Ok(())
}

pub(super) async fn prepare_delete_step_candidates(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    step: Step,
) -> Result<()> {
    match step {
        Step::AddressNamesCurrent => {
            ensure_candidate_table(transaction, address_names_candidates_spec()).await
        }
        Step::NameCurrent => {
            ensure_candidate_table(transaction, name_current_candidates_spec()).await
        }
        Step::ChildrenCurrent => {
            ensure_candidate_table(transaction, children_current_candidates_spec()).await
        }
        Step::PermissionsCurrent => {
            ensure_candidate_table(transaction, permissions_current_candidates_spec()).await
        }
        Step::RecordInventoryCurrent => {
            ensure_candidate_table(transaction, record_inventory_current_candidates_spec()).await
        }
        Step::ProjectionNormalizedEventChanges => {
            ensure_candidate_table(transaction, projection_changes_candidates_spec()).await
        }
        Step::NormalizedEvents => {
            ensure_candidate_table(transaction, normalized_events_candidates_spec()).await
        }
        Step::SurfaceBindings => {
            ensure_candidate_table(transaction, surface_bindings_candidates_spec()).await
        }
        Step::Resources => ensure_candidate_table(transaction, resources_candidates_spec()).await,
        Step::NameSurfaces => {
            ensure_candidate_table(transaction, name_surfaces_candidates_spec()).await
        }
        Step::TokenLineages => {
            ensure_candidate_table(transaction, token_lineages_candidates_spec()).await
        }
        Step::FinalReplayReset | Step::Completed => Ok(()),
    }
}

pub(super) async fn delete_step_batch(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    step: Step,
    batch_size: i64,
) -> Result<DeletedBatch> {
    prepare_delete_step_candidates(transaction, step).await?;
    match step {
        Step::AddressNamesCurrent => {
            query_batch_with_sidecar_triggers_disabled(
                transaction,
                address_names_sql(),
                batch_size,
                &ADDRESS_NAMES_CURRENT_SIDECAR_TRIGGERS,
            )
            .await
        }
        Step::NameCurrent => {
            query_batch_with_sidecar_triggers_disabled(
                transaction,
                name_current_sql(),
                batch_size,
                &NAME_CURRENT_SIDECAR_TRIGGERS,
            )
            .await
        }
        Step::ChildrenCurrent => query_batch(transaction, children_current_sql(), batch_size).await,
        Step::PermissionsCurrent => {
            query_batch(transaction, permissions_current_sql(), batch_size).await
        }
        Step::RecordInventoryCurrent => {
            query_batch(transaction, record_inventory_current_sql(), batch_size).await
        }
        Step::ProjectionNormalizedEventChanges => {
            query_batch(transaction, projection_changes_sql(), batch_size).await
        }
        Step::NormalizedEvents => {
            query_batch(transaction, normalized_events_sql(), batch_size).await
        }
        Step::SurfaceBindings => {
            query_batch_with_sidecar_triggers_disabled(
                transaction,
                surface_bindings_sql(),
                batch_size,
                &SURFACE_BINDINGS_SIDECAR_TRIGGERS,
            )
            .await
        }
        Step::Resources => {
            query_batch_with_sidecar_triggers_disabled(
                transaction,
                resources_sql(),
                batch_size,
                &RESOURCES_SIDECAR_TRIGGERS,
            )
            .await
        }
        Step::NameSurfaces => {
            query_batch_with_sidecar_triggers_disabled(
                transaction,
                name_surfaces_sql(),
                batch_size,
                &NAME_SURFACES_SIDECAR_TRIGGERS,
            )
            .await
        }
        Step::TokenLineages => {
            query_batch_with_sidecar_triggers_disabled(
                transaction,
                token_lineages_sql(),
                batch_size,
                &TOKEN_LINEAGES_SIDECAR_TRIGGERS,
            )
            .await
        }
        Step::FinalReplayReset | Step::Completed => {
            bail!("unsupported delete batch step {}", step.as_str())
        }
    }
}

async fn ensure_candidate_table(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    spec: CandidateTableSpec,
) -> Result<()> {
    let exists = sqlx::query_scalar::<_, bool>("SELECT to_regclass($1) IS NOT NULL")
        .bind(format!("pg_temp.{}", spec.table_name))
        .fetch_one(&mut **transaction)
        .await
        .with_context(|| {
            format!(
                "failed to inspect Base normalized-event rederive candidate table {}",
                spec.table_name
            )
        })?;
    if exists {
        return Ok(());
    }

    execute_sql(transaction, spec.create_sql).await?;
    execute_sql(transaction, spec.index_sql).await?;
    for insert_sql in &spec.insert_sqls {
        execute_sql(transaction, insert_sql).await?;
    }
    execute_sql(transaction, &format!("ANALYZE {}", spec.table_name)).await?;
    Ok(())
}

async fn query_batch(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sql: impl AsRef<str>,
    batch_size: i64,
) -> Result<DeletedBatch> {
    let sql = sql.as_ref();
    let row = sqlx::query(sql)
        .bind(batch_size)
        .fetch_one(&mut **transaction)
        .await
        .with_context(|| format!("failed to delete Base normalized-event rederive batch: {sql}"))?;
    deleted_batch_from_row(row)
}

async fn query_batch_with_sidecar_triggers_disabled(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sql: impl AsRef<str>,
    batch_size: i64,
    triggers: &SidecarTriggerSet,
) -> Result<DeletedBatch> {
    set_sidecar_triggers(transaction, triggers, false).await?;
    let deleted = query_batch(transaction, sql, batch_size).await?;
    set_sidecar_triggers(transaction, triggers, true).await?;
    Ok(deleted)
}

async fn set_sidecar_triggers(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    triggers: &SidecarTriggerSet,
    enabled: bool,
) -> Result<()> {
    let action = if enabled { "ENABLE" } else { "DISABLE" };
    for trigger in triggers.triggers {
        let sql = format!(
            "ALTER TABLE {} {action} TRIGGER {trigger}",
            triggers.table_name
        );
        execute_sql(transaction, &sql).await?;
    }
    Ok(())
}

fn deleted_batch_from_row(row: sqlx::postgres::PgRow) -> Result<DeletedBatch> {
    Ok(DeletedBatch {
        row_count: row.try_get("row_count")?,
        range_start: row.try_get("range_start")?,
        range_end: row.try_get("range_end")?,
    })
}

async fn execute_sql(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sql: &str,
) -> Result<()> {
    sqlx::query(sql)
        .execute(&mut **transaction)
        .await
        .with_context(|| format!("failed to execute Base normalized-event rederive SQL: {sql}"))?;
    Ok(())
}

fn address_names_candidates_spec() -> CandidateTableSpec {
    CandidateTableSpec {
        table_name: ADDRESS_NAMES_CANDIDATES,
        create_sql: r#"
            CREATE TEMP TABLE base_rederive_delete_address_names_current_candidates (
                address TEXT NOT NULL,
                logical_name_id TEXT NOT NULL,
                relation TEXT NOT NULL
            ) ON COMMIT PRESERVE ROWS
        "#,
        index_sql: "CREATE UNIQUE INDEX base_rederive_delete_anc_candidates_idx ON base_rederive_delete_address_names_current_candidates (address, logical_name_id, relation)",
        insert_sqls: identity_projection_candidate_insert_sqls(
            ADDRESS_NAMES_CANDIDATES,
            &["address", "logical_name_id", "relation"],
            "address_names_current",
            &["address", "logical_name_id", "relation"],
            &[
                (
                    "base_rederive_scope_resources",
                    "p.resource_id = identity_scope.resource_id",
                ),
                (
                    "base_rederive_scope_token_lineages",
                    "p.token_lineage_id = identity_scope.token_lineage_id",
                ),
                (
                    "base_rederive_scope_name_surfaces",
                    "p.logical_name_id = identity_scope.logical_name_id",
                ),
                (
                    "base_rederive_scope_surface_bindings",
                    "p.surface_binding_id = identity_scope.surface_binding_id",
                ),
            ],
        ),
    }
}

fn name_current_candidates_spec() -> CandidateTableSpec {
    CandidateTableSpec {
        table_name: NAME_CURRENT_CANDIDATES,
        create_sql: r#"
            CREATE TEMP TABLE base_rederive_delete_name_current_candidates (
                logical_name_id TEXT NOT NULL
            ) ON COMMIT PRESERVE ROWS
        "#,
        index_sql: "CREATE UNIQUE INDEX base_rederive_delete_name_current_candidates_idx ON base_rederive_delete_name_current_candidates (logical_name_id)",
        insert_sqls: identity_projection_candidate_insert_sqls(
            NAME_CURRENT_CANDIDATES,
            &["logical_name_id"],
            "name_current",
            &["logical_name_id"],
            &[
                (
                    "base_rederive_scope_resources",
                    "p.resource_id = identity_scope.resource_id",
                ),
                (
                    "base_rederive_scope_token_lineages",
                    "p.token_lineage_id = identity_scope.token_lineage_id",
                ),
                (
                    "base_rederive_scope_name_surfaces",
                    "p.logical_name_id = identity_scope.logical_name_id",
                ),
                (
                    "base_rederive_scope_surface_bindings",
                    "p.surface_binding_id = identity_scope.surface_binding_id",
                ),
            ],
        ),
    }
}

fn children_current_candidates_spec() -> CandidateTableSpec {
    CandidateTableSpec {
        table_name: CHILDREN_CURRENT_CANDIDATES,
        create_sql: r#"
            CREATE TEMP TABLE base_rederive_delete_children_current_candidates (
                parent_logical_name_id TEXT NOT NULL,
                child_logical_name_id TEXT NOT NULL,
                surface_class TEXT NOT NULL
            ) ON COMMIT PRESERVE ROWS
        "#,
        index_sql: "CREATE UNIQUE INDEX base_rederive_delete_children_current_candidates_idx ON base_rederive_delete_children_current_candidates (parent_logical_name_id, child_logical_name_id, surface_class)",
        insert_sqls: identity_projection_candidate_insert_sqls(
            CHILDREN_CURRENT_CANDIDATES,
            &[
                "parent_logical_name_id",
                "child_logical_name_id",
                "surface_class",
            ],
            "children_current",
            &[
                "parent_logical_name_id",
                "child_logical_name_id",
                "surface_class",
            ],
            &[
                (
                    "base_rederive_scope_name_surfaces",
                    "p.parent_logical_name_id = identity_scope.logical_name_id",
                ),
                (
                    "base_rederive_scope_name_surfaces",
                    "p.child_logical_name_id = identity_scope.logical_name_id",
                ),
            ],
        ),
    }
}

fn permissions_current_candidates_spec() -> CandidateTableSpec {
    CandidateTableSpec {
        table_name: PERMISSIONS_CURRENT_CANDIDATES,
        create_sql: r#"
            CREATE TEMP TABLE base_rederive_delete_permissions_current_candidates (
                resource_id UUID NOT NULL,
                subject TEXT NOT NULL,
                scope TEXT NOT NULL
            ) ON COMMIT PRESERVE ROWS
        "#,
        index_sql: "CREATE UNIQUE INDEX base_rederive_delete_permissions_current_candidates_idx ON base_rederive_delete_permissions_current_candidates (resource_id, subject, scope)",
        insert_sqls: identity_projection_candidate_insert_sqls(
            PERMISSIONS_CURRENT_CANDIDATES,
            &["resource_id", "subject", "scope"],
            "permissions_current",
            &["resource_id", "subject", "scope"],
            &[(
                "base_rederive_scope_resources",
                "p.resource_id = identity_scope.resource_id",
            )],
        ),
    }
}

fn record_inventory_current_candidates_spec() -> CandidateTableSpec {
    CandidateTableSpec {
        table_name: RECORD_INVENTORY_CURRENT_CANDIDATES,
        create_sql: r#"
            CREATE TEMP TABLE base_rederive_delete_record_inventory_current_candidates (
                resource_id UUID NOT NULL,
                record_version_boundary_key TEXT NOT NULL
            ) ON COMMIT PRESERVE ROWS
        "#,
        index_sql: "CREATE UNIQUE INDEX base_rederive_delete_record_inventory_current_candidates_idx ON base_rederive_delete_record_inventory_current_candidates (resource_id, record_version_boundary_key)",
        insert_sqls: identity_projection_candidate_insert_sqls(
            RECORD_INVENTORY_CURRENT_CANDIDATES,
            &["resource_id", "record_version_boundary_key"],
            "record_inventory_current",
            &["resource_id", "record_version_boundary_key"],
            &[(
                "base_rederive_scope_resources",
                "p.resource_id = identity_scope.resource_id",
            )],
        ),
    }
}

fn projection_changes_candidates_spec() -> CandidateTableSpec {
    CandidateTableSpec {
        table_name: PROJECTION_CHANGES_CANDIDATES,
        create_sql: r#"
            CREATE TEMP TABLE base_rederive_delete_projection_change_candidates (
                change_id BIGINT NOT NULL
            ) ON COMMIT PRESERVE ROWS
        "#,
        index_sql: "CREATE UNIQUE INDEX base_rederive_delete_projection_change_candidates_idx ON base_rederive_delete_projection_change_candidates (change_id)",
        insert_sqls: vec![format!(
            r#"
            INSERT INTO {PROJECTION_CHANGES_CANDIDATES} (change_id)
            SELECT change.change_id
            FROM base_rederive_scope_normalized_events scoped
            CROSS JOIN LATERAL (
                SELECT p.change_id
                FROM projection_normalized_event_changes p
                WHERE p.normalized_event_id = scoped.normalized_event_id
                OFFSET 0
            ) change
            ON CONFLICT DO NOTHING
            "#
        )],
    }
}

fn normalized_events_candidates_spec() -> CandidateTableSpec {
    CandidateTableSpec {
        table_name: NORMALIZED_EVENTS_CANDIDATES,
        create_sql: r#"
            CREATE TEMP TABLE base_rederive_delete_normalized_event_candidates (
                block_number BIGINT NOT NULL,
                normalized_event_id BIGINT NOT NULL
            ) ON COMMIT PRESERVE ROWS
        "#,
        index_sql: "CREATE UNIQUE INDEX base_rederive_delete_normalized_event_candidates_idx ON base_rederive_delete_normalized_event_candidates (block_number, normalized_event_id)",
        insert_sqls: vec![format!(
            r#"
            INSERT INTO {NORMALIZED_EVENTS_CANDIDATES} (block_number, normalized_event_id)
            SELECT event.block_number, event.normalized_event_id
            FROM base_rederive_scope_normalized_events scoped
            CROSS JOIN LATERAL (
                SELECT p.block_number, p.normalized_event_id
                FROM normalized_events p
                WHERE p.normalized_event_id = scoped.normalized_event_id
                OFFSET 0
            ) event
            ON CONFLICT DO NOTHING
            "#
        )],
    }
}

fn surface_bindings_candidates_spec() -> CandidateTableSpec {
    CandidateTableSpec {
        table_name: SURFACE_BINDINGS_CANDIDATES,
        create_sql: r#"
            CREATE TEMP TABLE base_rederive_delete_surface_binding_candidates (
                surface_binding_id UUID NOT NULL
            ) ON COMMIT PRESERVE ROWS
        "#,
        index_sql: "CREATE UNIQUE INDEX base_rederive_delete_surface_binding_candidates_idx ON base_rederive_delete_surface_binding_candidates (surface_binding_id)",
        insert_sqls: vec![format!(
            r#"
            INSERT INTO {SURFACE_BINDINGS_CANDIDATES} (surface_binding_id)
            SELECT binding.surface_binding_id
            FROM base_rederive_scope_surface_bindings scoped
            CROSS JOIN LATERAL (
                SELECT p.surface_binding_id
                FROM surface_bindings p
                WHERE p.surface_binding_id = scoped.surface_binding_id
                OFFSET 0
            ) binding
            ON CONFLICT DO NOTHING
            "#
        )],
    }
}

fn resources_candidates_spec() -> CandidateTableSpec {
    CandidateTableSpec {
        table_name: RESOURCES_CANDIDATES,
        create_sql: r#"
            CREATE TEMP TABLE base_rederive_delete_resource_candidates (
                resource_id UUID NOT NULL
            ) ON COMMIT PRESERVE ROWS
        "#,
        index_sql: "CREATE UNIQUE INDEX base_rederive_delete_resource_candidates_idx ON base_rederive_delete_resource_candidates (resource_id)",
        insert_sqls: vec![format!(
            r#"
            INSERT INTO {RESOURCES_CANDIDATES} (resource_id)
            SELECT resource.resource_id
            FROM base_rederive_scope_resources scoped
            CROSS JOIN LATERAL (
                SELECT p.resource_id
                FROM resources p
                WHERE p.resource_id = scoped.resource_id
                OFFSET 0
            ) resource
            ON CONFLICT DO NOTHING
            "#
        )],
    }
}

fn name_surfaces_candidates_spec() -> CandidateTableSpec {
    CandidateTableSpec {
        table_name: NAME_SURFACES_CANDIDATES,
        create_sql: r#"
            CREATE TEMP TABLE base_rederive_delete_name_surface_candidates (
                logical_name_id TEXT NOT NULL
            ) ON COMMIT PRESERVE ROWS
        "#,
        index_sql: "CREATE UNIQUE INDEX base_rederive_delete_name_surface_candidates_idx ON base_rederive_delete_name_surface_candidates (logical_name_id)",
        insert_sqls: vec![format!(
            r#"
            INSERT INTO {NAME_SURFACES_CANDIDATES} (logical_name_id)
            SELECT surface.logical_name_id
            FROM base_rederive_scope_name_surfaces scoped
            CROSS JOIN LATERAL (
                SELECT p.logical_name_id
                FROM name_surfaces p
                WHERE p.logical_name_id = scoped.logical_name_id
                OFFSET 0
            ) surface
            ON CONFLICT DO NOTHING
            "#
        )],
    }
}

fn token_lineages_candidates_spec() -> CandidateTableSpec {
    CandidateTableSpec {
        table_name: TOKEN_LINEAGES_CANDIDATES,
        create_sql: r#"
            CREATE TEMP TABLE base_rederive_delete_token_lineage_candidates (
                token_lineage_id UUID NOT NULL
            ) ON COMMIT PRESERVE ROWS
        "#,
        index_sql: "CREATE UNIQUE INDEX base_rederive_delete_token_lineage_candidates_idx ON base_rederive_delete_token_lineage_candidates (token_lineage_id)",
        insert_sqls: vec![format!(
            r#"
            INSERT INTO {TOKEN_LINEAGES_CANDIDATES} (token_lineage_id)
            SELECT token.token_lineage_id
            FROM base_rederive_scope_token_lineages scoped
            CROSS JOIN LATERAL (
                SELECT p.token_lineage_id
                FROM token_lineages p
                WHERE p.token_lineage_id = scoped.token_lineage_id
                OFFSET 0
            ) token
            ON CONFLICT DO NOTHING
            "#
        )],
    }
}

fn identity_projection_candidate_insert_sqls(
    target_table: &str,
    target_columns: &[&str],
    projection_table: &str,
    key_columns: &[&str],
    branches: &[(&str, &str)],
) -> Vec<String> {
    let target_column_list = target_columns.join(", ");
    let outer_key_select = key_columns
        .iter()
        .map(|column| format!("projection.{column}"))
        .collect::<Vec<_>>()
        .join(", ");
    let inner_key_select = key_columns
        .iter()
        .map(|column| format!("p.{column}"))
        .collect::<Vec<_>>()
        .join(", ");
    branches
        .iter()
        .map(|(scope_table, join_predicate)| {
            format!(
                r#"
                INSERT INTO {target_table} ({target_column_list})
                SELECT {outer_key_select}
                FROM {scope_table} identity_scope
                CROSS JOIN LATERAL (
                    SELECT {inner_key_select}
                    FROM {projection_table} p
                    WHERE {join_predicate}
                    OFFSET 0
                ) projection
                ON CONFLICT DO NOTHING
                "#
            )
        })
        .collect()
}

fn address_names_sql() -> &'static str {
    r#"
    WITH candidate_rows AS (
        SELECT ctid, address, logical_name_id, relation
        FROM base_rederive_delete_address_names_current_candidates
        ORDER BY address, logical_name_id, relation
        LIMIT $1
    ),
    removed_candidates AS (
        DELETE FROM base_rederive_delete_address_names_current_candidates candidate
        USING candidate_rows selected
        WHERE candidate.ctid = selected.ctid
        RETURNING candidate.address, candidate.logical_name_id, candidate.relation
    ),
    deleted AS (
        DELETE FROM address_names_current p
        USING removed_candidates c
        WHERE p.address = c.address
          AND p.logical_name_id = c.logical_name_id
          AND p.relation = c.relation
        RETURNING p.address || '|' || p.logical_name_id || '|' || p.relation AS key_text
    )
    SELECT COUNT(*)::BIGINT AS row_count,
           MIN(key_text)::TEXT AS range_start,
           MAX(key_text)::TEXT AS range_end
    FROM deleted
    "#
}

fn name_current_sql() -> &'static str {
    r#"
    WITH candidate_rows AS (
        SELECT ctid, logical_name_id
        FROM base_rederive_delete_name_current_candidates
        ORDER BY logical_name_id
        LIMIT $1
    ),
    removed_candidates AS (
        DELETE FROM base_rederive_delete_name_current_candidates candidate
        USING candidate_rows selected
        WHERE candidate.ctid = selected.ctid
        RETURNING candidate.logical_name_id
    ),
    deleted AS (
        DELETE FROM name_current p
        USING removed_candidates c
        WHERE p.logical_name_id = c.logical_name_id
        RETURNING p.logical_name_id AS key_text
    )
    SELECT COUNT(*)::BIGINT AS row_count,
           MIN(key_text)::TEXT AS range_start,
           MAX(key_text)::TEXT AS range_end
    FROM deleted
    "#
}

fn children_current_sql() -> &'static str {
    r#"
    WITH candidate_rows AS (
        SELECT ctid, parent_logical_name_id, child_logical_name_id, surface_class
        FROM base_rederive_delete_children_current_candidates
        ORDER BY parent_logical_name_id, child_logical_name_id, surface_class
        LIMIT $1
    ),
    removed_candidates AS (
        DELETE FROM base_rederive_delete_children_current_candidates candidate
        USING candidate_rows selected
        WHERE candidate.ctid = selected.ctid
        RETURNING candidate.parent_logical_name_id,
                  candidate.child_logical_name_id,
                  candidate.surface_class
    ),
    deleted AS (
        DELETE FROM children_current p
        USING removed_candidates c
        WHERE p.parent_logical_name_id = c.parent_logical_name_id
          AND p.child_logical_name_id = c.child_logical_name_id
          AND p.surface_class = c.surface_class
        RETURNING p.parent_logical_name_id || '|' || p.child_logical_name_id || '|' || p.surface_class AS key_text
    )
    SELECT COUNT(*)::BIGINT AS row_count,
           MIN(key_text)::TEXT AS range_start,
           MAX(key_text)::TEXT AS range_end
    FROM deleted
    "#
}

fn record_inventory_current_sql() -> &'static str {
    r#"
    WITH candidate_rows AS (
        SELECT ctid, resource_id, record_version_boundary_key
        FROM base_rederive_delete_record_inventory_current_candidates
        ORDER BY resource_id, record_version_boundary_key
        LIMIT $1
    ),
    removed_candidates AS (
        DELETE FROM base_rederive_delete_record_inventory_current_candidates candidate
        USING candidate_rows selected
        WHERE candidate.ctid = selected.ctid
        RETURNING candidate.resource_id, candidate.record_version_boundary_key
    ),
    deleted AS (
        DELETE FROM record_inventory_current p
        USING removed_candidates c
        WHERE p.resource_id = c.resource_id
          AND p.record_version_boundary_key = c.record_version_boundary_key
        RETURNING p.resource_id::TEXT || '|' || p.record_version_boundary_key AS key_text
    )
    SELECT COUNT(*)::BIGINT AS row_count,
           MIN(key_text)::TEXT AS range_start,
           MAX(key_text)::TEXT AS range_end
    FROM deleted
    "#
}

fn projection_changes_sql() -> &'static str {
    r#"
    WITH candidate_rows AS (
        SELECT ctid, change_id
        FROM base_rederive_delete_projection_change_candidates
        ORDER BY change_id
        LIMIT $1
    ),
    removed_candidates AS (
        DELETE FROM base_rederive_delete_projection_change_candidates candidate
        USING candidate_rows selected
        WHERE candidate.ctid = selected.ctid
        RETURNING candidate.change_id
    ),
    deleted AS (
        DELETE FROM projection_normalized_event_changes p
        USING removed_candidates c
        WHERE p.change_id = c.change_id
        RETURNING p.change_id::TEXT AS key_text
    )
    SELECT COUNT(*)::BIGINT AS row_count,
           MIN(key_text)::TEXT AS range_start,
           MAX(key_text)::TEXT AS range_end
    FROM deleted
    "#
}

fn normalized_events_sql() -> &'static str {
    r#"
    WITH candidate_rows AS (
        SELECT ctid, block_number, normalized_event_id
        FROM base_rederive_delete_normalized_event_candidates
        ORDER BY block_number, normalized_event_id
        LIMIT $1
    ),
    removed_candidates AS (
        DELETE FROM base_rederive_delete_normalized_event_candidates candidate
        USING candidate_rows selected
        WHERE candidate.ctid = selected.ctid
        RETURNING candidate.block_number, candidate.normalized_event_id
    ),
    deleted AS (
        DELETE FROM normalized_events p
        USING removed_candidates c
        WHERE p.normalized_event_id = c.normalized_event_id
        RETURNING c.block_number::TEXT || ':' || p.normalized_event_id::TEXT AS key_text
    )
    SELECT COUNT(*)::BIGINT AS row_count,
           MIN(key_text)::TEXT AS range_start,
           MAX(key_text)::TEXT AS range_end
    FROM deleted
    "#
}

fn surface_bindings_sql() -> &'static str {
    r#"
    WITH candidate_rows AS (
        SELECT ctid, surface_binding_id
        FROM base_rederive_delete_surface_binding_candidates
        ORDER BY surface_binding_id
        LIMIT $1
    ),
    removed_candidates AS (
        DELETE FROM base_rederive_delete_surface_binding_candidates candidate
        USING candidate_rows selected
        WHERE candidate.ctid = selected.ctid
        RETURNING candidate.surface_binding_id
    ),
    deleted AS (
        DELETE FROM surface_bindings p
        USING removed_candidates c
        WHERE p.surface_binding_id = c.surface_binding_id
        RETURNING p.surface_binding_id::TEXT AS key_text
    )
    SELECT COUNT(*)::BIGINT AS row_count,
           MIN(key_text)::TEXT AS range_start,
           MAX(key_text)::TEXT AS range_end
    FROM deleted
    "#
}

fn resources_sql() -> &'static str {
    r#"
    WITH candidate_rows AS (
        SELECT ctid, resource_id
        FROM base_rederive_delete_resource_candidates
        ORDER BY resource_id
        LIMIT $1
    ),
    removed_candidates AS (
        DELETE FROM base_rederive_delete_resource_candidates candidate
        USING candidate_rows selected
        WHERE candidate.ctid = selected.ctid
        RETURNING candidate.resource_id
    ),
    deleted AS (
        DELETE FROM resources p
        USING removed_candidates c
        WHERE p.resource_id = c.resource_id
        RETURNING p.resource_id::TEXT AS key_text
    )
    SELECT COUNT(*)::BIGINT AS row_count,
           MIN(key_text)::TEXT AS range_start,
           MAX(key_text)::TEXT AS range_end
    FROM deleted
    "#
}

fn name_surfaces_sql() -> &'static str {
    r#"
    WITH candidate_rows AS (
        SELECT ctid, logical_name_id
        FROM base_rederive_delete_name_surface_candidates
        ORDER BY logical_name_id
        LIMIT $1
    ),
    removed_candidates AS (
        DELETE FROM base_rederive_delete_name_surface_candidates candidate
        USING candidate_rows selected
        WHERE candidate.ctid = selected.ctid
        RETURNING candidate.logical_name_id
    ),
    deleted AS (
        DELETE FROM name_surfaces p
        USING removed_candidates c
        WHERE p.logical_name_id = c.logical_name_id
        RETURNING p.logical_name_id AS key_text
    )
    SELECT COUNT(*)::BIGINT AS row_count,
           MIN(key_text)::TEXT AS range_start,
           MAX(key_text)::TEXT AS range_end
    FROM deleted
    "#
}

fn token_lineages_sql() -> &'static str {
    r#"
    WITH candidate_rows AS (
        SELECT ctid, token_lineage_id
        FROM base_rederive_delete_token_lineage_candidates
        ORDER BY token_lineage_id
        LIMIT $1
    ),
    removed_candidates AS (
        DELETE FROM base_rederive_delete_token_lineage_candidates candidate
        USING candidate_rows selected
        WHERE candidate.ctid = selected.ctid
        RETURNING candidate.token_lineage_id
    ),
    deleted AS (
        DELETE FROM token_lineages p
        USING removed_candidates c
        WHERE p.token_lineage_id = c.token_lineage_id
        RETURNING p.token_lineage_id::TEXT AS key_text
    )
    SELECT COUNT(*)::BIGINT AS row_count,
           MIN(key_text)::TEXT AS range_start,
           MAX(key_text)::TEXT AS range_end
    FROM deleted
    "#
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projection_candidate_build_sql_drives_from_temp_scope_tables() {
        for spec in [
            address_names_candidates_spec(),
            name_current_candidates_spec(),
            children_current_candidates_spec(),
            permissions_current_candidates_spec(),
            record_inventory_current_candidates_spec(),
        ] {
            let insert_sql = spec.insert_sqls.join("\n");
            assert!(
                insert_sql.contains("base_rederive_scope_"),
                "{} must drive from materialized scope tables",
                spec.table_name
            );
            assert!(
                insert_sql.contains("CROSS JOIN LATERAL"),
                "{} must force scope-keyed projection lookup",
                spec.table_name
            );
            assert!(
                !insert_sql.contains("EXISTS"),
                "{} must not scan projections and probe scope",
                spec.table_name
            );
            assert!(
                !insert_sql.contains("provenance->>'adapter'"),
                "{} must use the already materialized scope",
                spec.table_name
            );
        }
    }

    #[test]
    fn event_and_identity_candidate_build_sql_uses_scope_sets() {
        for spec in [
            projection_changes_candidates_spec(),
            normalized_events_candidates_spec(),
            surface_bindings_candidates_spec(),
            resources_candidates_spec(),
            name_surfaces_candidates_spec(),
            token_lineages_candidates_spec(),
        ] {
            let insert_sql = spec.insert_sqls.join("\n");
            assert!(
                insert_sql.contains("base_rederive_scope_"),
                "{} must drive from materialized scope tables",
                spec.table_name
            );
            assert!(
                insert_sql.contains("CROSS JOIN LATERAL"),
                "{} must use scope-keyed lookups",
                spec.table_name
            );
            assert!(
                !insert_sql.contains("EXISTS"),
                "{} must not scan target tables and probe scope",
                spec.table_name
            );
        }
    }

    #[test]
    fn delete_batch_sql_reads_candidate_tables_not_scope_predicates() {
        for sql in [
            address_names_sql(),
            name_current_sql(),
            children_current_sql(),
            permissions_current_sql(),
            record_inventory_current_sql(),
            projection_changes_sql(),
            normalized_events_sql(),
            surface_bindings_sql(),
            resources_sql(),
            name_surfaces_sql(),
            token_lineages_sql(),
        ] {
            assert!(sql.contains("candidate_rows"));
            assert!(sql.contains("removed_candidates"));
            assert!(!sql.contains("EXISTS"));
            assert!(!sql.contains("provenance->>'adapter'"));
        }
    }

    #[test]
    fn sidecar_trigger_sets_cover_delete_heavy_projection_tables() {
        assert_eq!(
            ADDRESS_NAMES_CURRENT_SIDECAR_TRIGGERS.table_name,
            "address_names_current"
        );
        assert!(
            ADDRESS_NAMES_CURRENT_SIDECAR_TRIGGERS
                .triggers
                .contains(&"address_names_current_identity_counts_after_delete")
        );
        assert_eq!(NAME_CURRENT_SIDECAR_TRIGGERS.table_name, "name_current");
        assert!(
            NAME_CURRENT_SIDECAR_TRIGGERS
                .triggers
                .contains(&"name_current_identity_feed_after_insert_delete")
        );
        assert_eq!(RESOURCES_SIDECAR_TRIGGERS.table_name, "resources");
        assert!(
            RESOURCES_SIDECAR_TRIGGERS
                .triggers
                .contains(&"resources_identity_feed_after_delete")
        );
        assert_eq!(
            SURFACE_BINDINGS_SIDECAR_TRIGGERS.table_name,
            "surface_bindings"
        );
        assert!(
            SURFACE_BINDINGS_SIDECAR_TRIGGERS
                .triggers
                .contains(&"surface_bindings_identity_feed_after_delete")
        );
        assert_eq!(NAME_SURFACES_SIDECAR_TRIGGERS.table_name, "name_surfaces");
        assert!(
            NAME_SURFACES_SIDECAR_TRIGGERS
                .triggers
                .contains(&"name_surfaces_identity_feed_after_delete")
        );
        assert_eq!(TOKEN_LINEAGES_SIDECAR_TRIGGERS.table_name, "token_lineages");
        assert!(
            TOKEN_LINEAGES_SIDECAR_TRIGGERS
                .triggers
                .contains(&"token_lineages_identity_feed_after_delete")
        );
    }
}
