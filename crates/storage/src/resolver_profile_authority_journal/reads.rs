use std::collections::BTreeSet;

use anyhow::{Context, Result, ensure};
use serde_json::Value;
use sqlx::PgPool;

use crate::resolver_profile_input_changes::ResolverProfileReconciliationTarget;

use super::{
    RESOLVER_PROFILE_AUTHORITY_JOURNAL_ENTRY_BATCH_SIZE, RESOLVER_PROFILE_AUTHORITY_JOURNAL_KEY,
    ResolverProfileAuthorityJournalEntry,
};

/// Load only journal entries matching one bounded target page.
pub async fn load_resolver_profile_authority_entries_for_targets(
    pool: &PgPool,
    targets: &[ResolverProfileReconciliationTarget],
) -> Result<Vec<ResolverProfileAuthorityJournalEntry>> {
    ensure!(
        targets.len() <= RESOLVER_PROFILE_AUTHORITY_JOURNAL_ENTRY_BATCH_SIZE,
        "resolver-profile authority target read exceeds the bounded {}-target batch",
        RESOLVER_PROFILE_AUTHORITY_JOURNAL_ENTRY_BATCH_SIZE
    );
    if targets.is_empty() {
        return Ok(Vec::new());
    }
    let targets = targets
        .iter()
        .map(|target| (target.chain_id.clone(), target.contract_address.clone()))
        .collect::<BTreeSet<_>>();
    let chains = targets
        .iter()
        .map(|(chain, _)| chain.clone())
        .collect::<Vec<_>>();
    let addresses = targets
        .iter()
        .map(|(_, address)| address.clone())
        .collect::<Vec<_>>();
    let rows = sqlx::query_as::<_, (String, Value)>(
        r#"
        WITH target_addresses AS (
            SELECT DISTINCT chain_id, contract_address
            FROM UNNEST($1::TEXT[], $2::TEXT[])
                AS target(chain_id, contract_address)
        )
        SELECT entry.entry_key, entry.entry_payload
        FROM resolver_profile_authority_journal_entries entry
        JOIN target_addresses target
          ON (entry.entry_payload ->> 'chain') COLLATE "C"
               = target.chain_id COLLATE "C"
         AND (entry.entry_payload ->> 'address') COLLATE "C"
               = target.contract_address COLLATE "C"
        WHERE entry.journal_key = $3
        ORDER BY entry.entry_key
        "#,
    )
    .bind(chains)
    .bind(addresses)
    .bind(RESOLVER_PROFILE_AUTHORITY_JOURNAL_KEY)
    .fetch_all(pool)
    .await
    .context("failed to load bounded resolver-profile authority target entries")?;

    rows.into_iter()
        .map(|(stored_key, payload)| {
            let entry = ResolverProfileAuthorityJournalEntry::from_payload(payload)?;
            ensure!(
                entry.entry_key == stored_key,
                "resolver-profile authority journal entry key does not match its payload"
            );
            Ok(entry)
        })
        .collect()
}

/// Page the distinct addresses in the requested seed families without loading
/// the complete journal into the caller.
pub async fn load_resolver_profile_authority_family_target_page(
    pool: &PgPool,
    families: &[(String, String)],
    after: Option<&ResolverProfileReconciliationTarget>,
    limit: usize,
) -> Result<Vec<ResolverProfileReconciliationTarget>> {
    ensure!(
        limit > 0,
        "resolver-profile family target page limit must be positive"
    );
    ensure!(
        families.len() <= RESOLVER_PROFILE_AUTHORITY_JOURNAL_ENTRY_BATCH_SIZE,
        "resolver-profile family target read exceeds the bounded {}-family batch",
        RESOLVER_PROFILE_AUTHORITY_JOURNAL_ENTRY_BATCH_SIZE
    );
    if families.is_empty() {
        return Ok(Vec::new());
    }
    let families = families.iter().cloned().collect::<BTreeSet<_>>();
    let chains = families
        .iter()
        .map(|(chain, _)| chain.clone())
        .collect::<Vec<_>>();
    let source_families = families
        .iter()
        .map(|(_, source_family)| source_family.clone())
        .collect::<Vec<_>>();
    let rows = sqlx::query_as::<_, (String, String)>(
        r#"
        WITH target_families AS (
            SELECT DISTINCT chain_id, source_family
            FROM UNNEST($1::TEXT[], $2::TEXT[])
                AS family(chain_id, source_family)
        )
        SELECT
            (entry.entry_payload ->> 'chain') COLLATE "C" AS chain_id,
            (entry.entry_payload ->> 'address') COLLATE "C" AS contract_address
        FROM resolver_profile_authority_journal_entries entry
        JOIN target_families family
          ON (entry.entry_payload ->> 'chain') COLLATE "C"
               = family.chain_id COLLATE "C"
         AND (entry.entry_payload ->> 'source_family') COLLATE "C"
               = family.source_family COLLATE "C"
        WHERE entry.journal_key = $3
          AND (
              $4::TEXT IS NULL
              OR (
                  (entry.entry_payload ->> 'chain') COLLATE "C",
                  (entry.entry_payload ->> 'address') COLLATE "C"
              ) > (
                  $4::TEXT COLLATE "C",
                  $5::TEXT COLLATE "C"
              )
          )
        GROUP BY 1, 2
        ORDER BY 1, 2
        LIMIT $6
        "#,
    )
    .bind(chains)
    .bind(source_families)
    .bind(RESOLVER_PROFILE_AUTHORITY_JOURNAL_KEY)
    .bind(after.map(|target| target.chain_id.as_str()))
    .bind(after.map(|target| target.contract_address.as_str()))
    .bind(i64::try_from(limit)?)
    .fetch_all(pool)
    .await
    .context("failed to load resolver-profile seed-family target page")?;

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
