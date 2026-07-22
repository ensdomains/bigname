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
    sqlx::query(
        r#"
        CREATE INDEX resolver_profile_authority_after_entries_family_idx
        ON pg_temp.resolver_profile_authority_after_entries (
            ((entry_payload ->> 'chain') COLLATE "C"),
            ((entry_payload ->> 'source_family') COLLATE "C"),
            ((entry_payload ->> 'address') COLLATE "C")
        )
        "#,
    )
    .execute(&mut **transaction)
    .await
    .context("failed to index resolver-profile authority staging families")?;
    Ok(())
}

pub(super) async fn materialize_changed_entry_keys(
    transaction: &mut Transaction<'_, Postgres>,
    journal_key: &str,
) -> Result<i64> {
    create_changed_entry_table(transaction).await?;
    let mut after = None::<String>;
    let mut changed_entry_count = 0_i64;
    loop {
        let Some((last_key, inserted_count)) =
            materialize_changed_entry_key_page(transaction, journal_key, after.as_deref(), 10_000)
                .await?
        else {
            break;
        };
        after = Some(last_key);
        changed_entry_count += inserted_count;
    }
    finish_changed_entry_keys(transaction).await?;
    Ok(changed_entry_count)
}

pub(super) async fn create_changed_entry_table(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<()> {
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

    Ok(())
}

pub(super) async fn materialize_changed_entry_key_page(
    transaction: &mut Transaction<'_, Postgres>,
    journal_key: &str,
    after: Option<&str>,
    limit: usize,
) -> Result<Option<(String, i64)>> {
    let (last_key, inserted_count) = sqlx::query_as::<_, (Option<String>, i64)>(
        r#"
        WITH before_keys AS (
            SELECT entry_key
            FROM resolver_profile_authority_journal_entries
            WHERE journal_key = $1
              AND ($2::TEXT IS NULL OR entry_key > $2)
            ORDER BY entry_key
            LIMIT $3
        ),
        after_keys AS (
            SELECT entry_key
            FROM pg_temp.resolver_profile_authority_after_entries
            WHERE $2::TEXT IS NULL OR entry_key > $2
            ORDER BY entry_key
            LIMIT $3
        ),
        candidate_keys AS (
            SELECT entry_key
            FROM (
                SELECT entry_key FROM before_keys

                UNION ALL

                SELECT entry_key FROM after_keys
            ) bounded_keys
            GROUP BY entry_key
            ORDER BY entry_key
            LIMIT $3
        ),
        inserted AS (
            INSERT INTO pg_temp.resolver_profile_authority_changed_entries (entry_key)
            SELECT candidate.entry_key
            FROM candidate_keys candidate
            LEFT JOIN resolver_profile_authority_journal_entries before
              ON before.journal_key = $1
             AND before.entry_key = candidate.entry_key
            LEFT JOIN pg_temp.resolver_profile_authority_after_entries after
              ON after.entry_key = candidate.entry_key
            WHERE before.entry_payload IS DISTINCT FROM after.entry_payload
            ON CONFLICT (entry_key) DO NOTHING
            RETURNING 1
        )
        SELECT
            (SELECT MAX(entry_key) FROM candidate_keys),
            (SELECT COUNT(*)::BIGINT FROM inserted)
        "#,
    )
    .bind(journal_key)
    .bind(after)
    .bind(i64::try_from(limit)?)
    .fetch_one(&mut **transaction)
    .await
    .context("failed to materialize resolver-profile authority changed-entry key page")?;
    Ok(last_key.map(|last_key| (last_key, inserted_count)))
}

pub(super) async fn finish_changed_entry_keys(
    _transaction: &mut Transaction<'_, Postgres>,
) -> Result<()> {
    Ok(())
}

pub(super) async fn materialize_reconciliation_targets(
    transaction: &mut Transaction<'_, Postgres>,
    journal_key: &str,
    progress: &mut Option<&mut dyn super::ResolverProfileAuthorityJournalProgress>,
) -> Result<()> {
    create_target_tables(transaction).await?;
    materialize_direct_targets_and_seed_families(transaction, journal_key, progress).await?;
    materialize_seed_family_targets(transaction, journal_key, progress).await?;
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
    progress: &mut Option<&mut dyn super::ResolverProfileAuthorityJournalProgress>,
) -> Result<()> {
    let mut after = None::<String>;
    loop {
        let last_key = materialize_direct_target_page(
            transaction,
            journal_key,
            after.as_deref(),
            super::RESOLVER_PROFILE_AUTHORITY_INTERNAL_PAGE_SIZE,
        )
        .await?;
        let Some(last_key) = last_key else {
            break;
        };
        after = Some(last_key);
        super::record_journal_progress(progress).await?;
    }
    Ok(())
}

async fn materialize_direct_target_page(
    transaction: &mut Transaction<'_, Postgres>,
    journal_key: &str,
    after: Option<&str>,
    limit: usize,
) -> Result<Option<String>> {
    sqlx::query_scalar::<_, Option<String>>(
        r#"
        WITH candidate_keys AS (
            SELECT entry_key
            FROM pg_temp.resolver_profile_authority_changed_entries
            WHERE $2::TEXT IS NULL OR entry_key > $2
            ORDER BY entry_key
            LIMIT $3
        ),
        changed_payloads AS (
            SELECT before.entry_payload AS payload
            FROM candidate_keys candidate
            JOIN resolver_profile_authority_journal_entries before
              ON before.journal_key = $1
             AND before.entry_key = candidate.entry_key

            UNION ALL

            SELECT after.entry_payload AS payload
            FROM candidate_keys candidate
            JOIN pg_temp.resolver_profile_authority_after_entries after
              ON after.entry_key = candidate.entry_key
        ),
        inserted_targets AS (
            INSERT INTO pg_temp.resolver_profile_authority_changed_targets (
                chain_id,
                contract_address
            )
            SELECT DISTINCT payload ->> 'chain', payload ->> 'address'
            FROM changed_payloads
            ON CONFLICT (chain_id, contract_address) DO NOTHING
            RETURNING 1
        ),
        inserted_seed_families AS (
            INSERT INTO pg_temp.resolver_profile_authority_changed_seed_families (
                chain_id,
                source_family
            )
            SELECT DISTINCT payload ->> 'chain', payload ->> 'source_family'
            FROM changed_payloads
            WHERE (payload ->> 'is_seed')::BOOLEAN
            ON CONFLICT (chain_id, source_family) DO NOTHING
            RETURNING 1
        )
        SELECT MAX(entry_key)
        FROM candidate_keys
        WHERE (SELECT COUNT(*) FROM inserted_targets) >= 0
          AND (SELECT COUNT(*) FROM inserted_seed_families) >= 0
        "#,
    )
    .bind(journal_key)
    .bind(after)
    .bind(i64::try_from(limit)?)
    .fetch_one(&mut **transaction)
    .await
    .context("failed to materialize a direct resolver-profile authority target page")
}

async fn materialize_seed_family_targets(
    transaction: &mut Transaction<'_, Postgres>,
    journal_key: &str,
    progress: &mut Option<&mut dyn super::ResolverProfileAuthorityJournalProgress>,
) -> Result<()> {
    let families = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT chain_id, source_family
        FROM pg_temp.resolver_profile_authority_changed_seed_families
        ORDER BY chain_id, source_family
        "#,
    )
    .fetch_all(&mut **transaction)
    .await
    .context("failed to load changed resolver-profile seed families")?;

    for (chain_id, source_family) in families {
        let mut after = None::<String>;
        loop {
            let last_address = materialize_seed_family_target_page(
                transaction,
                journal_key,
                &chain_id,
                &source_family,
                after.as_deref(),
                super::RESOLVER_PROFILE_AUTHORITY_INTERNAL_PAGE_SIZE,
            )
            .await?;
            let Some(last_address) = last_address else {
                break;
            };
            after = Some(last_address);
            super::record_journal_progress(progress).await?;
        }
    }
    Ok(())
}

async fn materialize_seed_family_target_page(
    transaction: &mut Transaction<'_, Postgres>,
    journal_key: &str,
    chain_id: &str,
    source_family: &str,
    after: Option<&str>,
    limit: usize,
) -> Result<Option<String>> {
    sqlx::query_scalar::<_, Option<String>>(
        r#"
        WITH before_addresses AS (
            SELECT (entry_payload ->> 'address') COLLATE "C" AS address
            FROM resolver_profile_authority_journal_entries
            WHERE journal_key = $1
              AND (entry_payload ->> 'chain') COLLATE "C" = $2
              AND (entry_payload ->> 'source_family') COLLATE "C" = $3
              AND (
                    $4::TEXT IS NULL
                 OR (entry_payload ->> 'address') COLLATE "C" > $4
              )
            ORDER BY (entry_payload ->> 'address') COLLATE "C"
            LIMIT $5
        ),
        after_addresses AS (
            SELECT (entry_payload ->> 'address') COLLATE "C" AS address
            FROM pg_temp.resolver_profile_authority_after_entries
            WHERE (entry_payload ->> 'chain') COLLATE "C" = $2
              AND (entry_payload ->> 'source_family') COLLATE "C" = $3
              AND (
                    $4::TEXT IS NULL
                 OR (entry_payload ->> 'address') COLLATE "C" > $4
              )
            ORDER BY (entry_payload ->> 'address') COLLATE "C"
            LIMIT $5
        ),
        candidate_addresses AS (
            SELECT address
            FROM (
                SELECT address FROM before_addresses

                UNION ALL

                SELECT address FROM after_addresses
            ) bounded_addresses
            GROUP BY address
            ORDER BY address
            LIMIT $5
        ),
        inserted AS (
            INSERT INTO pg_temp.resolver_profile_authority_changed_targets (
                chain_id,
                contract_address
            )
            SELECT $2, address
            FROM candidate_addresses
            ON CONFLICT (chain_id, contract_address) DO NOTHING
            RETURNING 1
        )
        SELECT MAX(address)
        FROM candidate_addresses
        WHERE (SELECT COUNT(*) FROM inserted) >= 0
        "#,
    )
    .bind(journal_key)
    .bind(chain_id)
    .bind(source_family)
    .bind(after)
    .bind(i64::try_from(limit)?)
    .fetch_one(&mut **transaction)
    .await
    .context("failed to expand a resolver-profile seed-family target page")
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
