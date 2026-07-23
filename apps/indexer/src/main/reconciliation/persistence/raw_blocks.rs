use super::*;

#[allow(dead_code)]
pub(crate) async fn persist_reconciled_raw_blocks(
    pool: &sqlx::PgPool,
    chain: &str,
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
    header_audit_mode: HeaderAuditMode,
) -> Result<()> {
    persist_reconciled_raw_blocks_inner(pool, chain, heads, canonical, header_audit_mode, &mut None)
        .await
}

pub(super) async fn persist_reconciled_raw_blocks_inner(
    pool: &sqlx::PgPool,
    chain: &str,
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
    header_audit_mode: HeaderAuditMode,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    let mut blocks = BTreeMap::<String, bigname_storage::RawBlock>::new();

    let canonical_state = canonical_raw_state(canonical.status);
    for (index, block) in canonical.reconciled_blocks.iter().enumerate() {
        insert_raw_block_candidate(
            &mut blocks,
            chain,
            block,
            canonical_state,
            header_audit_mode,
        );
        if index + 1 == canonical.reconciled_blocks.len()
            || (index + 1).is_multiple_of(LIVE_RAW_FACT_PROGRESS_ROWS)
        {
            record_progress(pool, progress).await?;
        }
    }
    if let Some(safe) = &heads.safe {
        insert_raw_block_candidate(
            &mut blocks,
            chain,
            safe,
            CanonicalityState::Safe,
            header_audit_mode,
        );
    }
    if let Some(finalized) = &heads.finalized {
        insert_raw_block_candidate(
            &mut blocks,
            chain,
            finalized,
            CanonicalityState::Finalized,
            header_audit_mode,
        );
    }

    let recanonicalize = canonical.status != CanonicalReconciliationStatus::AwaitingAncestor;
    let mut page = Vec::with_capacity(LIVE_RAW_FACT_PROGRESS_ROWS);
    for block in blocks.into_values() {
        page.push(block);
        if page.len() == LIVE_RAW_FACT_PROGRESS_ROWS {
            persist_raw_block_page(pool, &page, recanonicalize).await?;
            page.clear();
            record_progress(pool, progress).await?;
        }
    }
    if !page.is_empty() {
        persist_raw_block_page(pool, &page, recanonicalize).await?;
        record_progress(pool, progress).await?;
    }
    Ok(())
}

async fn persist_raw_block_page(
    pool: &sqlx::PgPool,
    blocks: &[RawBlock],
    recanonicalize: bool,
) -> Result<()> {
    if recanonicalize {
        upsert_raw_blocks_recanonicalizing_orphaned(pool, blocks).await?;
    } else {
        upsert_raw_blocks(pool, blocks).await?;
    }
    Ok(())
}

// Raw-state persistence keeps deployment, heads, canonicality, and refresh inputs explicit.
