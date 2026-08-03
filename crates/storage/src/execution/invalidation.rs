use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::{PgPool, Postgres};

use super::{
    keying::{
        execution_cache_key_storage_key, manifest_version_identity_key,
        manifest_versions_contain_identity, validate_version_boundary,
        version_boundary_storage_key,
    },
    outcome::load_execution_outcomes_for_scope_internal,
    types::{
        ExecutionBoundaryInvalidation, ExecutionManifestInvalidation, ExecutionOutcome,
        ExecutionOutcomeInvalidationSummary,
    },
};

/// Delete cached execution outcomes for one exact stale manifest identity/version.
pub async fn invalidate_execution_outcomes_for_manifest_version(
    pool: &PgPool,
    invalidation: &ExecutionManifestInvalidation,
) -> Result<ExecutionOutcomeInvalidationSummary> {
    invalidate_execution_outcomes_for_manifest_version_internal(pool, invalidation, None).await
}

/// Delete cached execution outcomes for one exact stale manifest identity/version and
/// one exact request key.
pub async fn invalidate_execution_outcomes_for_manifest_version_and_request_key(
    pool: &PgPool,
    invalidation: &ExecutionManifestInvalidation,
    request_key: &str,
) -> Result<ExecutionOutcomeInvalidationSummary> {
    let request_key = normalize_execution_invalidation_request_key(
        request_key,
        "execution manifest invalidation",
    )?;
    invalidate_execution_outcomes_for_manifest_version_internal(
        pool,
        invalidation,
        Some(request_key.as_str()),
    )
    .await
}

async fn invalidate_execution_outcomes_for_manifest_version_internal(
    pool: &PgPool,
    invalidation: &ExecutionManifestInvalidation,
    request_key: Option<&str>,
) -> Result<ExecutionOutcomeInvalidationSummary> {
    let invalidation = normalize_execution_manifest_invalidation(invalidation)?;
    let target_identity = invalidation.identity_key();

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for execution manifest invalidation")?;

    let outcomes = load_execution_outcomes_for_scope_internal(
        &mut *transaction,
        &invalidation.request_type,
        &invalidation.namespace,
    )
    .await?;
    let mut cache_keys = Vec::new();
    for outcome in outcomes {
        if !outcome_matches_request_key(&outcome, request_key) {
            continue;
        }
        if manifest_versions_contain_identity(
            &outcome.cache_key.manifest_versions,
            &outcome.cache_key.request_key,
            &target_identity,
        )? {
            cache_keys.push(execution_cache_key_storage_key(&outcome.cache_key)?);
        }
    }

    let deleted_outcome_count =
        delete_execution_outcomes_by_keys(&mut transaction, &cache_keys).await?;

    transaction
        .commit()
        .await
        .context("failed to commit execution manifest invalidation")?;

    Ok(ExecutionOutcomeInvalidationSummary {
        deleted_outcome_count,
    })
}

/// Delete cached execution outcomes for one exact stale topology boundary.
pub async fn invalidate_execution_outcomes_for_topology_boundary(
    pool: &PgPool,
    invalidation: &ExecutionBoundaryInvalidation,
) -> Result<ExecutionOutcomeInvalidationSummary> {
    invalidate_execution_outcomes_for_boundary(
        pool,
        invalidation,
        None,
        "topology_version_boundary",
        |outcome| &outcome.cache_key.topology_version_boundary,
    )
    .await
}

/// Delete cached execution outcomes for one exact stale topology boundary and
/// one exact request key.
pub async fn invalidate_execution_outcomes_for_topology_boundary_and_request_key(
    pool: &PgPool,
    invalidation: &ExecutionBoundaryInvalidation,
    request_key: &str,
) -> Result<ExecutionOutcomeInvalidationSummary> {
    let request_key = normalize_execution_invalidation_request_key(
        request_key,
        "execution topology boundary invalidation",
    )?;
    invalidate_execution_outcomes_for_boundary(
        pool,
        invalidation,
        Some(request_key.as_str()),
        "topology_version_boundary",
        |outcome| &outcome.cache_key.topology_version_boundary,
    )
    .await
}

/// Delete cached execution outcomes for one exact stale record boundary.
pub async fn invalidate_execution_outcomes_for_record_boundary(
    pool: &PgPool,
    invalidation: &ExecutionBoundaryInvalidation,
) -> Result<ExecutionOutcomeInvalidationSummary> {
    invalidate_execution_outcomes_for_boundary(
        pool,
        invalidation,
        None,
        "record_version_boundary",
        |outcome| &outcome.cache_key.record_version_boundary,
    )
    .await
}

/// Delete cached execution outcomes for one exact stale record boundary and one exact
/// request key.
pub async fn invalidate_execution_outcomes_for_record_boundary_and_request_key(
    pool: &PgPool,
    invalidation: &ExecutionBoundaryInvalidation,
    request_key: &str,
) -> Result<ExecutionOutcomeInvalidationSummary> {
    let request_key = normalize_execution_invalidation_request_key(
        request_key,
        "execution record boundary invalidation",
    )?;
    invalidate_execution_outcomes_for_boundary(
        pool,
        invalidation,
        Some(request_key.as_str()),
        "record_version_boundary",
        |outcome| &outcome.cache_key.record_version_boundary,
    )
    .await
}

impl ExecutionManifestInvalidation {
    fn identity_key(&self) -> String {
        manifest_version_identity_key(
            self.source_manifest_id,
            self.source_family.as_deref(),
            self.manifest_version,
        )
    }
}

fn normalize_execution_manifest_invalidation(
    invalidation: &ExecutionManifestInvalidation,
) -> Result<ExecutionManifestInvalidation> {
    let request_type = invalidation.request_type.trim();
    if request_type.is_empty() {
        bail!("execution manifest invalidation has empty request_type");
    }

    let namespace = invalidation.namespace.trim();
    if namespace.is_empty() {
        bail!("execution manifest invalidation has empty namespace");
    }

    let source_manifest_id = match invalidation.source_manifest_id {
        Some(value) if value > 0 => Some(value),
        Some(value) => bail!(
            "execution manifest invalidation for request_type {request_type} namespace {namespace} source_manifest_id must be positive, got {value}"
        ),
        None => None,
    };
    let source_family = match invalidation.source_family.as_deref() {
        Some(value) if !value.trim().is_empty() => Some(value.trim().to_owned()),
        Some(_) => bail!(
            "execution manifest invalidation for request_type {request_type} namespace {namespace} source_family must be non-empty when present"
        ),
        None => None,
    };
    if source_manifest_id.is_none() && source_family.is_none() {
        bail!(
            "execution manifest invalidation for request_type {request_type} namespace {namespace} must include source_manifest_id or source_family"
        );
    }
    if invalidation.manifest_version <= 0 {
        bail!(
            "execution manifest invalidation for request_type {request_type} namespace {namespace} manifest_version must be positive, got {}",
            invalidation.manifest_version
        );
    }

    Ok(ExecutionManifestInvalidation {
        request_type: request_type.to_owned(),
        namespace: namespace.to_owned(),
        source_manifest_id,
        source_family,
        manifest_version: invalidation.manifest_version,
    })
}

fn normalize_execution_boundary_invalidation(
    invalidation: &ExecutionBoundaryInvalidation,
    field_name: &str,
) -> Result<ExecutionBoundaryInvalidation> {
    let request_type = invalidation.request_type.trim();
    if request_type.is_empty() {
        bail!("execution boundary invalidation has empty request_type");
    }

    let namespace = invalidation.namespace.trim();
    if namespace.is_empty() {
        bail!("execution boundary invalidation has empty namespace");
    }

    validate_version_boundary(
        &invalidation.boundary,
        field_name,
        &format!("{request_type}/{namespace}"),
    )?;

    Ok(ExecutionBoundaryInvalidation {
        request_type: request_type.to_owned(),
        namespace: namespace.to_owned(),
        boundary: invalidation.boundary.clone(),
    })
}

async fn invalidate_execution_outcomes_for_boundary(
    pool: &PgPool,
    invalidation: &ExecutionBoundaryInvalidation,
    request_key: Option<&str>,
    field_name: &str,
    boundary: impl Fn(&ExecutionOutcome) -> &Value,
) -> Result<ExecutionOutcomeInvalidationSummary> {
    let invalidation = normalize_execution_boundary_invalidation(invalidation, field_name)?;
    let target_boundary = version_boundary_storage_key(
        &invalidation.boundary,
        field_name,
        &format!("{}/{}", invalidation.request_type, invalidation.namespace),
    )?;

    let mut transaction = pool.begin().await.with_context(|| {
        format!("failed to open transaction for execution {field_name} invalidation")
    })?;

    let outcomes = load_execution_outcomes_for_scope_internal(
        &mut *transaction,
        &invalidation.request_type,
        &invalidation.namespace,
    )
    .await?;
    let mut cache_keys = Vec::new();
    for outcome in outcomes {
        if !outcome_matches_request_key(&outcome, request_key) {
            continue;
        }
        let outcome_boundary = version_boundary_storage_key(
            boundary(&outcome),
            field_name,
            &outcome.cache_key.request_key,
        )?;
        if outcome_boundary == target_boundary {
            cache_keys.push(execution_cache_key_storage_key(&outcome.cache_key)?);
        }
    }

    let deleted_outcome_count =
        delete_execution_outcomes_by_keys(&mut transaction, &cache_keys).await?;

    transaction
        .commit()
        .await
        .with_context(|| format!("failed to commit execution {field_name} invalidation"))?;

    Ok(ExecutionOutcomeInvalidationSummary {
        deleted_outcome_count,
    })
}

fn normalize_execution_invalidation_request_key(
    request_key: &str,
    context: &str,
) -> Result<String> {
    let request_key = request_key.trim();
    if request_key.is_empty() {
        bail!("{context} has empty request_key");
    }
    Ok(request_key.to_owned())
}

fn outcome_matches_request_key(outcome: &ExecutionOutcome, request_key: Option<&str>) -> bool {
    request_key.is_none_or(|request_key| outcome.cache_key.request_key == request_key)
}

async fn delete_execution_outcomes_by_keys(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    execution_cache_keys: &[String],
) -> Result<u64> {
    if execution_cache_keys.is_empty() {
        return Ok(0);
    }

    let deleted_outcome_count = sqlx::query(
        r#"
        DELETE FROM execution_cache_outcomes
        WHERE execution_cache_key = ANY($1::text[])
        "#,
    )
    .bind(execution_cache_keys)
    .execute(&mut **executor)
    .await
    .context("failed to delete execution outcomes by cache key batch")?
    .rows_affected();

    Ok(deleted_outcome_count)
}
