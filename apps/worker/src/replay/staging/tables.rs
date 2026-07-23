use anyhow::{Context, Result, bail, ensure};
use sqlx::{Postgres, Transaction, types::Uuid};

const STAGE_TABLE_PREFIX: &str = "cprs_";

#[derive(Clone, Debug)]
pub(super) struct StageTableSpec {
    target_table: &'static str,
    unique_columns: &'static [&'static str],
    has_inserted_at: bool,
}

pub(super) async fn stage_tables_exist(
    transaction: &mut Transaction<'_, Postgres>,
    stage_tables: &[String],
) -> Result<bool> {
    for stage_table in stage_tables {
        validate_stage_table_name(stage_table)?;
    }
    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT COALESCE(BOOL_AND(to_regclass(format('public.%I', stage_table)) IS NOT NULL), false)
        FROM UNNEST($1::TEXT[]) AS stage_table
        "#,
    )
    .bind(stage_tables)
    .fetch_one(&mut **transaction)
    .await
    .context("failed to inspect durable projection stage tables")
}

pub(super) async fn create_stage_tables(
    transaction: &mut Transaction<'_, Postgres>,
    projection: &str,
    specs: &[StageTableSpec],
) -> Result<Vec<String>> {
    let suffix = Uuid::new_v4().simple().to_string();
    let projection_token = projection.trim_end_matches("_current");
    let mut stage_tables = Vec::with_capacity(specs.len());
    for (index, spec) in specs.iter().enumerate() {
        let stage_table = format!("{STAGE_TABLE_PREFIX}{projection_token}_{index}_{suffix}");
        validate_stage_table_name(&stage_table)?;
        validate_identifier(spec.target_table)?;
        sqlx::query(&format!(
            "CREATE TABLE public.{stage_table} (LIKE public.{} INCLUDING DEFAULTS)",
            spec.target_table
        ))
        .execute(&mut **transaction)
        .await
        .with_context(|| format!("failed to create durable stage table {stage_table}"))?;
        if spec.has_inserted_at {
            sqlx::query(&format!(
                "ALTER TABLE public.{stage_table} ALTER COLUMN inserted_at SET DEFAULT TIMESTAMPTZ 'epoch'"
            ))
            .execute(&mut **transaction)
            .await
            .with_context(|| {
                format!("failed to make {stage_table}.inserted_at deterministic")
            })?;
        }
        if !spec.unique_columns.is_empty() {
            for column in spec.unique_columns {
                validate_identifier(column)?;
            }
            sqlx::query(&format!(
                "CREATE UNIQUE INDEX {stage_table}_key ON public.{stage_table} ({})",
                spec.unique_columns.join(", ")
            ))
            .execute(&mut **transaction)
            .await
            .with_context(|| format!("failed to index durable stage table {stage_table}"))?;
        }
        stage_tables.push(stage_table);
    }
    Ok(stage_tables)
}

pub(super) async fn drop_stage_tables(
    transaction: &mut Transaction<'_, Postgres>,
    stage_tables: &[String],
) -> Result<()> {
    for stage_table in stage_tables {
        validate_stage_table_name(stage_table)?;
        sqlx::query(&format!("DROP TABLE IF EXISTS public.{stage_table}"))
            .execute(&mut **transaction)
            .await
            .with_context(|| format!("failed to drop durable stage table {stage_table}"))?;
    }
    Ok(())
}

fn validate_stage_table_name(stage_table: &str) -> Result<()> {
    ensure!(
        stage_table.starts_with(STAGE_TABLE_PREFIX)
            && stage_table
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
        "unsafe projection stage table name {stage_table:?}"
    );
    Ok(())
}

fn validate_identifier(identifier: &str) -> Result<()> {
    ensure!(
        !identifier.is_empty()
            && identifier
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
        "unsafe projection table or column name {identifier:?}"
    );
    Ok(())
}

pub(super) fn projection_stage_specs(projection: &str) -> Result<Vec<StageTableSpec>> {
    let specs = match projection {
        "name_current" => vec![StageTableSpec {
            target_table: "name_current",
            unique_columns: &["logical_name_id"],
            has_inserted_at: true,
        }],
        "children_current" => vec![StageTableSpec {
            target_table: "children_current",
            unique_columns: &[
                "parent_logical_name_id",
                "child_logical_name_id",
                "surface_class",
            ],
            has_inserted_at: true,
        }],
        "permissions_current" => vec![
            StageTableSpec {
                target_table: "permissions_current",
                unique_columns: &["resource_id", "subject", "scope"],
                has_inserted_at: true,
            },
            StageTableSpec {
                target_table: "permissions_current_resource_summary",
                unique_columns: &["resource_id"],
                has_inserted_at: false,
            },
        ],
        "record_inventory_current" => vec![StageTableSpec {
            target_table: "record_inventory_current",
            unique_columns: &["resource_id", "record_version_boundary_key"],
            has_inserted_at: true,
        }],
        "resolver_current" => vec![StageTableSpec {
            target_table: "resolver_current",
            unique_columns: &["chain_id", "resolver_address"],
            has_inserted_at: true,
        }],
        "address_names_current" => vec![StageTableSpec {
            target_table: "address_names_current",
            unique_columns: &["address", "logical_name_id", "relation"],
            has_inserted_at: true,
        }],
        "primary_names_current" => vec![StageTableSpec {
            target_table: "primary_names_current",
            unique_columns: &["address", "coin_type", "namespace"],
            has_inserted_at: false,
        }],
        _ => bail!("unsupported current projection staging family {projection}"),
    };
    Ok(specs)
}
