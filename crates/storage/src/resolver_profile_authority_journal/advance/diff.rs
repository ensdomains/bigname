use anyhow::{Context, Result};
use sqlx::{Postgres, Transaction};

use crate::resolver_profile_input_changes::ResolverProfileReconciliationTarget;

pub(super) async fn create_after_entries_table(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TEMP TABLE pg_temp.resolver_profile_authority_after_entries (
            entry_key TEXT COLLATE "C" PRIMARY KEY,
            entry_payload JSONB NOT NULL,
            CONSTRAINT resolver_profile_authority_after_payload_check CHECK (
                jsonb_typeof(entry_payload) = 'object'
            )
        ) ON COMMIT DROP
        "#,
    )
    .execute(&mut **transaction)
    .await
    .context("failed to create resolver-profile authority staging table")?;
    Ok(())
}

pub(super) async fn materialize_changed_entry_keys(
    transaction: &mut Transaction<'_, Postgres>,
    journal_key: &str,
) -> Result<i64> {
    sqlx::query(
        r#"
        CREATE TEMP TABLE pg_temp.resolver_profile_authority_changed_entries (
            entry_key TEXT COLLATE "C" PRIMARY KEY
        ) ON COMMIT DROP
        "#,
    )
    .execute(&mut **transaction)
    .await
    .context("failed to create resolver-profile authority changed-entry table")?;

    let changed_entry_count = sqlx::query_scalar::<_, i64>(
        r#"
        WITH inserted AS (
            INSERT INTO pg_temp.resolver_profile_authority_changed_entries (entry_key)
            SELECT COALESCE(before.entry_key, after.entry_key)
            FROM (
                SELECT entry_key, entry_payload
                FROM resolver_profile_authority_journal_entries
                WHERE journal_key = $1
            ) before
            FULL OUTER JOIN pg_temp.resolver_profile_authority_after_entries after
                USING (entry_key)
            WHERE before.entry_payload IS DISTINCT FROM after.entry_payload
            RETURNING 1
        )
        SELECT COUNT(*)::BIGINT FROM inserted
        "#,
    )
    .bind(journal_key)
    .fetch_one(&mut **transaction)
    .await
    .context("failed to materialize resolver-profile authority changed-entry keys")?;

    sqlx::query("ANALYZE pg_temp.resolver_profile_authority_changed_entries")
        .execute(&mut **transaction)
        .await
        .context("failed to analyze resolver-profile authority changed-entry keys")?;
    Ok(changed_entry_count)
}

pub(super) async fn materialize_reconciliation_targets(
    transaction: &mut Transaction<'_, Postgres>,
    journal_key: &str,
) -> Result<()> {
    create_target_tables(transaction).await?;
    materialize_direct_targets_and_seed_families(transaction, journal_key).await?;
    materialize_seed_family_targets(transaction, journal_key).await?;
    sqlx::query("ANALYZE pg_temp.resolver_profile_authority_changed_targets")
        .execute(&mut **transaction)
        .await
        .context("failed to analyze resolver-profile authority changed targets")?;
    Ok(())
}

async fn create_target_tables(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TEMP TABLE pg_temp.resolver_profile_authority_changed_seed_families (
            chain_id TEXT COLLATE "C" NOT NULL,
            source_family TEXT COLLATE "C" NOT NULL,
            PRIMARY KEY (chain_id, source_family)
        ) ON COMMIT DROP
        "#,
    )
    .execute(&mut **transaction)
    .await
    .context("failed to create resolver-profile authority seed-family table")?;
    sqlx::query(
        r#"
        CREATE TEMP TABLE pg_temp.resolver_profile_authority_changed_targets (
            chain_id TEXT COLLATE "C" NOT NULL,
            contract_address TEXT COLLATE "C" NOT NULL,
            PRIMARY KEY (chain_id, contract_address)
        ) ON COMMIT DROP
        "#,
    )
    .execute(&mut **transaction)
    .await
    .context("failed to create resolver-profile authority target table")?;
    Ok(())
}

async fn materialize_direct_targets_and_seed_families(
    transaction: &mut Transaction<'_, Postgres>,
    journal_key: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        WITH changed_payloads AS (
            SELECT before.entry_payload AS payload
            FROM pg_temp.resolver_profile_authority_changed_entries changed
            JOIN resolver_profile_authority_journal_entries before
              ON before.journal_key = $1
             AND before.entry_key = changed.entry_key

            UNION ALL

            SELECT after.entry_payload AS payload
            FROM pg_temp.resolver_profile_authority_changed_entries changed
            JOIN pg_temp.resolver_profile_authority_after_entries after
              ON after.entry_key = changed.entry_key
        )
        INSERT INTO pg_temp.resolver_profile_authority_changed_targets (
            chain_id,
            contract_address
        )
        SELECT DISTINCT payload ->> 'chain', payload ->> 'address'
        FROM changed_payloads
        ON CONFLICT (chain_id, contract_address) DO NOTHING
        "#,
    )
    .bind(journal_key)
    .execute(&mut **transaction)
    .await
    .context("failed to materialize direct resolver-profile authority targets")?;

    sqlx::query(
        r#"
        WITH changed_payloads AS (
            SELECT before.entry_payload AS payload
            FROM pg_temp.resolver_profile_authority_changed_entries changed
            JOIN resolver_profile_authority_journal_entries before
              ON before.journal_key = $1
             AND before.entry_key = changed.entry_key

            UNION ALL

            SELECT after.entry_payload AS payload
            FROM pg_temp.resolver_profile_authority_changed_entries changed
            JOIN pg_temp.resolver_profile_authority_after_entries after
              ON after.entry_key = changed.entry_key
        )
        INSERT INTO pg_temp.resolver_profile_authority_changed_seed_families (
            chain_id,
            source_family
        )
        SELECT DISTINCT payload ->> 'chain', payload ->> 'source_family'
        FROM changed_payloads
        WHERE (payload ->> 'is_seed')::BOOLEAN
        ON CONFLICT (chain_id, source_family) DO NOTHING
        "#,
    )
    .bind(journal_key)
    .execute(&mut **transaction)
    .await
    .context("failed to materialize changed resolver-profile seed families")?;
    Ok(())
}

async fn materialize_seed_family_targets(
    transaction: &mut Transaction<'_, Postgres>,
    journal_key: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        WITH all_payloads AS (
            SELECT entry_payload AS payload
            FROM resolver_profile_authority_journal_entries
            WHERE journal_key = $1

            UNION ALL

            SELECT entry_payload AS payload
            FROM pg_temp.resolver_profile_authority_after_entries
        )
        INSERT INTO pg_temp.resolver_profile_authority_changed_targets (
            chain_id,
            contract_address
        )
        SELECT DISTINCT payload ->> 'chain', payload ->> 'address'
        FROM all_payloads
        JOIN pg_temp.resolver_profile_authority_changed_seed_families family
          ON family.chain_id = payload ->> 'chain'
         AND family.source_family = payload ->> 'source_family'
        ON CONFLICT (chain_id, contract_address) DO NOTHING
        "#,
    )
    .bind(journal_key)
    .execute(&mut **transaction)
    .await
    .context("failed to expand changed resolver-profile seed families")?;
    Ok(())
}

pub(super) async fn load_reconciliation_target_page(
    transaction: &mut Transaction<'_, Postgres>,
    after: Option<&(String, String)>,
    limit: usize,
) -> Result<Vec<ResolverProfileReconciliationTarget>> {
    let rows = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT chain_id, contract_address
        FROM pg_temp.resolver_profile_authority_changed_targets
        WHERE $1::TEXT IS NULL
           OR (chain_id, contract_address) > ($1, $2)
        ORDER BY chain_id, contract_address
        LIMIT $3
        "#,
    )
    .bind(after.map(|(chain, _)| chain.as_str()))
    .bind(after.map(|(_, address)| address.as_str()))
    .bind(i64::try_from(limit)?)
    .fetch_all(&mut **transaction)
    .await
    .context("failed to load resolver-profile authority target page")?;
    Ok(rows
        .into_iter()
        .map(
            |(chain_id, contract_address)| ResolverProfileReconciliationTarget {
                chain_id,
                contract_address,
            },
        )
        .collect())
}
