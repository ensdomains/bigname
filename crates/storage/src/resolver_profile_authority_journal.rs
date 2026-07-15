use anyhow::{Context, Result, ensure};
use serde_json::Value;
use sqlx::{PgPool, Row};

use crate::resolver_profile_input_changes::{
    ResolverProfileReconciliationTarget, enqueue_resolver_profile_reconciliations_with_executor,
};

const RESOLVER_PROFILE_AUTHORITY_JOURNAL_KEY: &str = "active_resolver_profiles";

/// Last resolver-profile authority snapshot whose diff was durably queued.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolverProfileAuthorityJournal {
    pub revision: i64,
    pub authority_snapshot: Value,
    pub discovery_epoch_snapshot: Value,
}

pub async fn load_resolver_profile_authority_journal(
    pool: &PgPool,
) -> Result<ResolverProfileAuthorityJournal> {
    let row = sqlx::query(
        r#"
        SELECT revision, authority_snapshot, discovery_epoch_snapshot
        FROM resolver_profile_authority_journal
        WHERE journal_key = $1
        "#,
    )
    .bind(RESOLVER_PROFILE_AUTHORITY_JOURNAL_KEY)
    .fetch_one(pool)
    .await
    .context("failed to load resolver-profile authority journal")?;

    let revision = row.try_get("revision")?;
    let authority_snapshot = row.try_get("authority_snapshot")?;
    let discovery_epoch_snapshot = row.try_get("discovery_epoch_snapshot")?;
    validate_journal_snapshot(revision, &authority_snapshot, &discovery_epoch_snapshot)?;
    Ok(ResolverProfileAuthorityJournal {
        revision,
        authority_snapshot,
        discovery_epoch_snapshot,
    })
}

/// Atomically queue an authority diff and replace the journal when the caller
/// still owns `expected_revision`.
///
/// Targets are queued before the journal update inside one transaction. A
/// stale revision rolls back those queue increments instead of publishing
/// duplicate work derived from an obsolete snapshot.
pub async fn advance_resolver_profile_authority_journal(
    pool: &PgPool,
    expected_revision: i64,
    authority_snapshot: &Value,
    discovery_epoch_snapshot: &Value,
    targets: &[ResolverProfileReconciliationTarget],
) -> Result<Option<i64>> {
    validate_journal_snapshot(
        expected_revision,
        authority_snapshot,
        discovery_epoch_snapshot,
    )?;

    let mut transaction = pool
        .begin()
        .await
        .context("failed to begin resolver-profile authority journal handoff")?;
    let enqueued_target_count =
        enqueue_resolver_profile_reconciliations_with_executor(&mut *transaction, targets).await?;
    let updated_revision = sqlx::query_scalar::<_, i64>(
        r#"
        UPDATE resolver_profile_authority_journal
        SET
            revision = revision + 1,
            authority_snapshot = $2,
            discovery_epoch_snapshot = $4,
            updated_at = now()
        WHERE journal_key = $1
          AND revision = $3
        RETURNING revision
        "#,
    )
    .bind(RESOLVER_PROFILE_AUTHORITY_JOURNAL_KEY)
    .bind(authority_snapshot)
    .bind(expected_revision)
    .bind(discovery_epoch_snapshot)
    .fetch_optional(&mut *transaction)
    .await
    .context("failed to compare-and-set resolver-profile authority journal")?;

    if updated_revision.is_none() {
        transaction
            .rollback()
            .await
            .context("failed to roll back stale resolver-profile authority handoff")?;
        return Ok(None);
    }

    transaction
        .commit()
        .await
        .context("failed to commit resolver-profile authority journal handoff")?;
    Ok(Some(enqueued_target_count))
}

fn validate_journal_snapshot(
    revision: i64,
    authority_snapshot: &Value,
    discovery_epoch_snapshot: &Value,
) -> Result<()> {
    ensure!(
        revision >= 0,
        "resolver-profile authority journal revision must not be negative"
    );
    ensure!(
        authority_snapshot
            .get("entries")
            .is_some_and(Value::is_array),
        "resolver-profile authority snapshot must contain an entries array"
    );
    ensure!(
        discovery_epoch_snapshot.is_object(),
        "resolver-profile discovery-epoch snapshot must be an object"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn authority_journal_update_is_revision_fenced() -> Result<()> {
        let database = TestDatabase::create_migrated(
            TestDatabaseConfig::new("storage_resolver_profile_authority_journal"),
            &crate::MIGRATOR,
            "failed to apply migrations for resolver-profile authority journal test",
        )
        .await?;
        let initial = load_resolver_profile_authority_journal(database.pool()).await?;
        assert_eq!(initial.revision, 0);
        assert_eq!(initial.authority_snapshot, json!({"entries": []}));
        assert_eq!(initial.discovery_epoch_snapshot, json!({}));

        let existing_target = ResolverProfileReconciliationTarget {
            chain_id: "ethereum-mainnet".to_owned(),
            contract_address: "0x0000000000000000000000000000000000000001".to_owned(),
        };
        let first = json!({"entries": [{"chain": "ethereum-mainnet"}]});
        let first_epochs = json!({"ethereum-mainnet": 1});
        assert_eq!(
            advance_resolver_profile_authority_journal(
                database.pool(),
                initial.revision,
                &first,
                &first_epochs,
                std::slice::from_ref(&existing_target),
            )
            .await?,
            Some(1)
        );
        let generation_before_stale = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT generation
            FROM resolver_profile_input_changes
            WHERE chain_id = $1 AND contract_address = $2
            "#,
        )
        .bind(&existing_target.chain_id)
        .bind(&existing_target.contract_address)
        .fetch_one(database.pool())
        .await?;

        let stale = json!({"entries": [{"chain": "base-mainnet"}]});
        let stale_epochs = json!({"base-mainnet": 1});
        let new_target = ResolverProfileReconciliationTarget {
            chain_id: "ethereum-mainnet".to_owned(),
            contract_address: "0x0000000000000000000000000000000000000002".to_owned(),
        };
        assert_eq!(
            advance_resolver_profile_authority_journal(
                database.pool(),
                initial.revision,
                &stale,
                &stale_epochs,
                &[existing_target.clone(), new_target.clone()],
            )
            .await?,
            None
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                r#"
                SELECT generation
                FROM resolver_profile_input_changes
                WHERE chain_id = $1 AND contract_address = $2
                "#,
            )
            .bind(&existing_target.chain_id)
            .bind(&existing_target.contract_address)
            .fetch_one(database.pool())
            .await?,
            generation_before_stale,
            "a stale journal revision must roll back an existing queue generation increment"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                r#"
                SELECT COUNT(*)
                FROM resolver_profile_input_changes
                WHERE chain_id = $1 AND contract_address = $2
                "#,
            )
            .bind(&new_target.chain_id)
            .bind(&new_target.contract_address)
            .fetch_one(database.pool())
            .await?,
            0,
            "a stale journal revision must roll back a new queue row"
        );

        let stored = load_resolver_profile_authority_journal(database.pool()).await?;
        assert_eq!(stored.revision, 1);
        assert_eq!(stored.authority_snapshot, first);
        assert_eq!(stored.discovery_epoch_snapshot, first_epochs);
        database.cleanup().await
    }
}
