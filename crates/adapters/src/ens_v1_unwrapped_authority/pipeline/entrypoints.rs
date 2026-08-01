use super::*;

pub async fn sync_ens_v1_unwrapped_authority(
    pool: &PgPool,
    chain: &str,
) -> Result<EnsV1UnwrappedAuthoritySyncSummary> {
    sync_ens_v1_unwrapped_authority_with_scope(pool, chain, None, false, &[], None, None).await
}

pub async fn sync_ens_v1_unwrapped_authority_through_block(
    pool: &PgPool,
    chain: &str,
    target_block_number: i64,
) -> Result<EnsV1UnwrappedAuthoritySyncSummary> {
    sync_ens_v1_unwrapped_authority_with_scope(
        pool,
        chain,
        Some(target_block_number),
        false,
        &[],
        None,
        None,
    )
    .await
}

impl EnsV1UnwrappedAuthoritySyncSummary {
    pub async fn sync_for_block_hashes(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
    ) -> Result<Self> {
        sync_ens_v1_unwrapped_authority_with_scope(
            pool,
            chain,
            None,
            true,
            block_hashes,
            None,
            None,
        )
        .await
    }

    pub async fn sync_for_block_hashes_with_source_scope(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
        source_scope: &[(String, String, i64, i64)],
    ) -> Result<Self> {
        sync_ens_v1_unwrapped_authority_with_scope(
            pool,
            chain,
            None,
            true,
            block_hashes,
            None,
            Some(source_scope),
        )
        .await
    }

    pub async fn sync_for_block_hashes_with_source_scope_and_transactions(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
        source_scope: &[(String, String, i64, i64)],
        transaction_hashes: &[String],
    ) -> Result<Self> {
        sync_ens_v1_unwrapped_authority_with_scope(
            pool,
            chain,
            None,
            true,
            block_hashes,
            Some(transaction_hashes),
            Some(source_scope),
        )
        .await
    }
}
