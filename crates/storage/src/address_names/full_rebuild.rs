use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::projection_staging::ADDRESS_NAMES_CURRENT_STAGING_COLUMNS;

use super::{types::AddressNameCurrentRow, write};

#[derive(Clone, Debug)]
pub struct AddressNamesCurrentFullRebuild {
    table_name: String,
    previous_row_count: u64,
}

impl AddressNamesCurrentFullRebuild {
    pub fn from_durable_stage(table_name: String, previous_row_count: u64) -> Result<Self> {
        quote_identifier(&table_name)?;
        Ok(Self {
            table_name,
            previous_row_count,
        })
    }

    pub fn previous_row_count(&self) -> u64 {
        self.previous_row_count
    }

    fn table_sql(&self) -> String {
        quote_identifier(&self.table_name).expect("generated table name must be safe")
    }
}

pub async fn begin_address_names_current_full_rebuild(
    pool: &PgPool,
) -> Result<AddressNamesCurrentFullRebuild> {
    let previous_row_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM address_names_current
        "#,
    )
    .fetch_one(pool)
    .await
    .context("failed to count address_names_current rows before full rebuild")?;

    let table_name = create_address_names_current_staging_table(pool, "full rebuild").await?;

    Ok(AddressNamesCurrentFullRebuild {
        table_name,
        previous_row_count: previous_row_count as u64,
    })
}

pub async fn drop_address_names_current_full_rebuild(
    pool: &PgPool,
    rebuild: &AddressNamesCurrentFullRebuild,
) -> Result<()> {
    drop_address_names_current_staging_table(pool, &rebuild.table_sql(), "full rebuild").await
}

pub async fn insert_address_names_current_full_rebuild_rows(
    pool: &PgPool,
    rebuild: &AddressNamesCurrentFullRebuild,
    rows: &[AddressNameCurrentRow],
) -> Result<Vec<AddressNameCurrentRow>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let table_sql = rebuild.table_sql();
    insert_address_names_current_staging_rows(pool, &table_sql, rows, "full rebuild").await
}

pub async fn insert_address_names_current_full_rebuild_rows_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    rebuild: &AddressNamesCurrentFullRebuild,
    rows: &[AddressNameCurrentRow],
) -> Result<Vec<AddressNameCurrentRow>> {
    let table_sql = rebuild.table_sql();
    insert_address_names_current_staging_rows_in_transaction(transaction, &table_sql, rows).await
}

async fn create_address_names_current_staging_table(
    pool: &PgPool,
    purpose: &str,
) -> Result<String> {
    let table_name = format!("address_names_current_rebuild_{}", Uuid::new_v4().simple());
    let table_sql = quote_identifier(&table_name)?;
    let unique_index_name = format!("anc_full_{}_uniq", Uuid::new_v4().simple());
    let unique_index_sql = quote_identifier(&unique_index_name)?;

    sqlx::query(&format!(
        r#"
        CREATE UNLOGGED TABLE {table_sql} (
            LIKE address_names_current INCLUDING DEFAULTS INCLUDING CONSTRAINTS
        )
        "#
    ))
    .execute(pool)
    .await
    .with_context(|| format!("failed to create address_names_current {purpose} staging table"))?;

    sqlx::query(&format!(
        r#"
        CREATE UNIQUE INDEX {unique_index_sql}
        ON {table_sql} (address, logical_name_id, relation)
        "#
    ))
    .execute(pool)
    .await
    .with_context(|| format!("failed to create address_names_current {purpose} staging key"))?;

    Ok(table_name)
}

async fn drop_address_names_current_staging_table(
    pool: &PgPool,
    table_sql: &str,
    purpose: &str,
) -> Result<()> {
    sqlx::query(&format!("DROP TABLE IF EXISTS {table_sql}"))
        .execute(pool)
        .await
        .with_context(|| format!("failed to drop address_names_current {purpose} staging table"))?;
    Ok(())
}

async fn insert_address_names_current_staging_rows(
    pool: &PgPool,
    table_sql: &str,
    rows: &[AddressNameCurrentRow],
    purpose: &str,
) -> Result<Vec<AddressNameCurrentRow>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool.begin().await.with_context(|| {
        format!("failed to open transaction for address_names_current {purpose} staging")
    })?;

    let snapshots =
        insert_address_names_current_staging_rows_in_transaction(&mut transaction, table_sql, rows)
            .await?;

    transaction.commit().await.with_context(|| {
        format!("failed to commit address_names_current {purpose} staging batch")
    })?;

    Ok(snapshots)
}

async fn insert_address_names_current_staging_rows_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    table_sql: &str,
    rows: &[AddressNameCurrentRow],
) -> Result<Vec<AddressNameCurrentRow>> {
    let mut snapshots = Vec::with_capacity(rows.len());
    for row in rows {
        snapshots.push(
            write::upsert_address_name_current_row_into_table(transaction, table_sql, row).await?,
        );
    }
    Ok(snapshots)
}

pub async fn publish_address_names_current_full_rebuild(
    pool: &PgPool,
    rebuild: &AddressNamesCurrentFullRebuild,
) -> Result<(u64, u64)> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for address_names_current full rebuild publish")?;
    let counts =
        publish_address_names_current_full_rebuild_in_transaction(&mut transaction, rebuild)
            .await?;
    transaction
        .commit()
        .await
        .context("failed to commit address_names_current full rebuild publish")?;
    Ok(counts)
}

pub async fn publish_address_names_current_full_rebuild_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    rebuild: &AddressNamesCurrentFullRebuild,
) -> Result<(u64, u64)> {
    let table_sql = rebuild.table_sql();
    write::set_address_names_current_sidecar_triggers(transaction, false).await?;

    sqlx::query(
        r#"
        TRUNCATE TABLE
            address_names_current,
            address_names_current_identity_counts,
            address_names_current_identity_feed
        "#,
    )
    .execute(&mut **transaction)
    .await
    .context("failed to truncate address_names_current projection and identity sidecars")?;

    let columns = ADDRESS_NAMES_CURRENT_STAGING_COLUMNS.join(", ");
    let inserted_row_count = sqlx::query(&format!(
        "INSERT INTO address_names_current ({columns}) SELECT {columns} FROM {table_sql}"
    ))
    .execute(&mut **transaction)
    .await
    .context("failed to publish staged address_names_current rows")?
    .rows_affected();

    write::set_address_names_current_sidecar_triggers(transaction, true).await?;
    rebuild_address_names_current_identity_sidecars_in_transaction(transaction).await?;

    Ok((rebuild.previous_row_count, inserted_row_count))
}

/// Rebuild address-name identity sidecars from the current public projection.
pub async fn rebuild_address_names_current_identity_sidecars(pool: &PgPool) -> Result<()> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for address_names_current sidecar rebuild")?;

    rebuild_address_names_current_identity_sidecars_in_transaction(&mut transaction).await?;

    transaction
        .commit()
        .await
        .context("failed to commit address_names_current sidecar rebuild")?;

    Ok(())
}

pub(crate) async fn rebuild_address_names_current_identity_sidecars_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<()> {
    sqlx::query(
        r#"
        TRUNCATE TABLE address_names_current_identity_counts
        "#,
    )
    .execute(&mut **transaction)
    .await
    .context("failed to truncate address_names_current_identity_counts")?;

    sqlx::query(
        r#"
        WITH relation_groups AS (
            SELECT
                readable.address,
                readable.logical_name_id,
                BOOL_OR(readable.relation IN ('registrant', 'token_holder')) AS owned,
                BOOL_OR(readable.relation = 'effective_controller') AS managed
            FROM address_names_current_identity_readable_relation_rows(NULL::text) readable
            GROUP BY readable.address, readable.logical_name_id
        ),
        counts AS (
            SELECT address, 'owned'::text AS roles, COUNT(*)::bigint AS total_count
            FROM relation_groups
            WHERE owned
            GROUP BY address
            UNION ALL
            SELECT address, 'managed'::text AS roles, COUNT(*)::bigint AS total_count
            FROM relation_groups
            WHERE managed
            GROUP BY address
            UNION ALL
            SELECT address, 'both'::text AS roles, COUNT(*)::bigint AS total_count
            FROM relation_groups
            GROUP BY address
        )
        INSERT INTO address_names_current_identity_counts (address, roles, total_count)
        SELECT address, roles, total_count
        FROM counts
        WHERE total_count > 0
        ON CONFLICT (address, roles) DO UPDATE
        SET
            total_count = EXCLUDED.total_count,
            updated_at = now()
        "#,
    )
    .execute(&mut **transaction)
    .await
    .context("failed to rebuild address_names_current_identity_counts")?;

    sqlx::query(
        r#"
        TRUNCATE TABLE address_names_current_identity_feed
        "#,
    )
    .execute(&mut **transaction)
    .await
    .context("failed to truncate address_names_current_identity_feed")?;

    sqlx::query(
        r#"
        INSERT INTO address_names_current_identity_feed (
            address,
            roles,
            coin_type,
            logical_name_id,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            chain_positions,
            coverage,
            is_primary,
            relation_facets,
            last_recomputed_at
        )
        SELECT
            candidate.address,
            candidate.roles,
            candidate.coin_type,
            candidate.logical_name_id,
            candidate.namespace,
            candidate.canonical_display_name,
            candidate.normalized_name,
            candidate.namehash,
            candidate.chain_positions,
            candidate.coverage,
            candidate.is_primary,
            candidate.relation_facets,
            now()
        FROM address_names_current_identity_feed_candidate_rows(NULL::text) candidate
        ON CONFLICT (address, roles, coin_type) DO UPDATE
        SET
            logical_name_id = EXCLUDED.logical_name_id,
            namespace = EXCLUDED.namespace,
            canonical_display_name = EXCLUDED.canonical_display_name,
            normalized_name = EXCLUDED.normalized_name,
            namehash = EXCLUDED.namehash,
            chain_positions = EXCLUDED.chain_positions,
            coverage = EXCLUDED.coverage,
            is_primary = EXCLUDED.is_primary,
            relation_facets = EXCLUDED.relation_facets,
            last_recomputed_at = EXCLUDED.last_recomputed_at
        "#,
    )
    .execute(&mut **transaction)
    .await
    .context("failed to rebuild address_names_current_identity_feed")?;

    Ok(())
}

fn quote_identifier(identifier: &str) -> Result<String> {
    if identifier.is_empty()
        || !identifier
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        bail!("unsafe SQL identifier {identifier:?}");
    }
    Ok(format!("\"{identifier}\""))
}
