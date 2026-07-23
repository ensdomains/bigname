use super::*;

pub(super) async fn load_active_manifest_metadata(
    pool: &PgPool,
    manifest_ids: &[i64],
) -> Result<HashMap<i64, ActiveManifestMetadata>> {
    let rows = sqlx::query(
        r#"
        SELECT manifest_id, chain, namespace, source_family, manifest_version, normalizer_version
        FROM manifest_versions
        WHERE rollout_status = 'active'
          AND manifest_id = ANY($1::BIGINT[])
        "#,
    )
    .bind(manifest_ids)
    .fetch_all(pool)
    .await
    .context("failed to load active manifest metadata for ENSv1 unwrapped authority")?;

    rows.into_iter()
        .map(|row| {
            let manifest = ActiveManifestMetadata {
                manifest_id: sql_row::get(&row, "manifest_id")?,
                chain: sql_row::get(&row, "chain")?,
                namespace: sql_row::get(&row, "namespace")?,
                source_family: sql_row::get(&row, "source_family")?,
                manifest_version: sql_row::get(&row, "manifest_version")?,
                normalizer_version: sql_row::get(&row, "normalizer_version")?,
            };
            Ok((manifest.manifest_id, manifest))
        })
        .collect()
}

pub(super) async fn load_manifest_contract_roles(
    pool: &PgPool,
    watched_contracts: &[WatchedContract],
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<HashMap<(i64, Uuid), String>> {
    let mut manifest_ids = HashSet::new();
    for (index, contract) in watched_contracts.iter().enumerate() {
        manifest_ids.extend(contract.source_manifest_id);
        record_emitter_progress(pool, progress, index + 1, watched_contracts.len()).await?;
    }
    let manifest_ids = manifest_ids.into_iter().collect::<Vec<_>>();
    if manifest_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT manifest_id, contract_instance_id, role
        FROM manifest_contract_instances
        WHERE declaration_kind = 'contract'
          AND manifest_id = ANY($1::BIGINT[])
        "#,
    )
    .bind(&manifest_ids)
    .fetch_all(pool)
    .await
    .context("failed to load manifest contract roles for ENSv1 unwrapped authority")?;

    rows.into_iter()
        .map(|row| {
            Ok((
                (
                    sql_row::get(&row, "manifest_id")?,
                    sql_row::get(&row, "contract_instance_id")?,
                ),
                sql_row::get(&row, "role")?,
            ))
        })
        .collect()
}

pub(super) fn unwrapped_authority_source_families() -> Vec<String> {
    [
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
        SOURCE_FAMILY_ENS_V1_WRAPPER_L1,
        SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR,
        SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
        SOURCE_FAMILY_BASENAMES_BASE_RESOLVER,
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

pub(super) async fn record_emitter_progress(
    pool: &PgPool,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
    completed: usize,
    total: usize,
) -> Result<()> {
    if completed == total || completed.is_multiple_of(STARTUP_ADAPTER_PROGRESS_PAGE_ROWS) {
        record_startup_adapter_progress(pool, progress).await?;
    }
    Ok(())
}

pub(super) fn source_rank(source: WatchedContractSource) -> i32 {
    crate::adapter_manifest::source_rank(source)
}

pub(super) async fn load_active_manifest_metadata_for_source_family(
    pool: &PgPool,
    chain: &str,
    source_family: &str,
) -> Result<Vec<ActiveManifestMetadata>> {
    let rows = sqlx::query(
        r#"
        SELECT manifest_id, chain, namespace, source_family, manifest_version, normalizer_version
        FROM manifest_versions
        WHERE rollout_status = 'active'
          AND chain = $1
          AND source_family = $2
        ORDER BY manifest_id
        "#,
    )
    .bind(chain)
    .bind(source_family)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load active {source_family} manifest metadata for {chain}")
    })?;

    rows.into_iter()
        .map(|row| {
            Ok(ActiveManifestMetadata {
                manifest_id: sql_row::get(&row, "manifest_id")?,
                chain: sql_row::get(&row, "chain")?,
                namespace: sql_row::get(&row, "namespace")?,
                source_family: sql_row::get(&row, "source_family")?,
                manifest_version: sql_row::get(&row, "manifest_version")?,
                normalizer_version: sql_row::get(&row, "normalizer_version")?,
            })
        })
        .collect()
}
