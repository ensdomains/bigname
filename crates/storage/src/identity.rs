use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;
use sqlx::{Executor, PgPool, Postgres, Row, postgres::PgRow};
use uuid::Uuid;

use crate::CanonicalityState;

const DEFAULT_IDENTITY_READ_FILTER: &str = r#"
  AND canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
"#;

/// Persisted stable token lineage anchor for one resource ownership history.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TokenLineage {
    pub token_lineage_id: Uuid,
    pub chain_id: String,
    pub block_hash: String,
    pub block_number: i64,
    pub provenance: Value,
    pub canonicality_state: CanonicalityState,
}

/// Persisted stable backing-resource anchor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Resource {
    pub resource_id: Uuid,
    pub token_lineage_id: Option<Uuid>,
    pub chain_id: String,
    pub block_hash: String,
    pub block_number: i64,
    pub provenance: Value,
    pub canonicality_state: CanonicalityState,
}

/// Persisted canonical public-surface identity for one logical name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NameSurface {
    pub logical_name_id: String,
    pub namespace: String,
    pub input_name: String,
    pub canonical_display_name: String,
    pub normalized_name: String,
    pub dns_encoded_name: Vec<u8>,
    pub namehash: String,
    pub labelhashes: Vec<String>,
    pub normalizer_version: String,
    pub normalization_warnings: Value,
    pub normalization_errors: Value,
    pub chain_id: String,
    pub block_hash: String,
    pub block_number: i64,
    pub provenance: Value,
    pub canonicality_state: CanonicalityState,
}

/// Storage-local binding taxonomy between surfaces and backing resources.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SurfaceBindingKind {
    DeclaredRegistryPath,
    LinkedSubregistryPath,
    ResolverAliasPath,
    ObservedWildcardPath,
    MigrationRebind,
    ObservedOnly,
}

impl SurfaceBindingKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DeclaredRegistryPath => "declared_registry_path",
            Self::LinkedSubregistryPath => "linked_subregistry_path",
            Self::ResolverAliasPath => "resolver_alias_path",
            Self::ObservedWildcardPath => "observed_wildcard_path",
            Self::MigrationRebind => "migration_rebind",
            Self::ObservedOnly => "observed_only",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "declared_registry_path" => Ok(Self::DeclaredRegistryPath),
            "linked_subregistry_path" => Ok(Self::LinkedSubregistryPath),
            "resolver_alias_path" => Ok(Self::ResolverAliasPath),
            "observed_wildcard_path" => Ok(Self::ObservedWildcardPath),
            "migration_rebind" => Ok(Self::MigrationRebind),
            "observed_only" => Ok(Self::ObservedOnly),
            _ => bail!("unknown surface binding kind {value}"),
        }
    }
}

/// Persisted time-ranged mapping from a public surface to a backing resource.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SurfaceBinding {
    pub surface_binding_id: Uuid,
    pub logical_name_id: String,
    pub resource_id: Uuid,
    pub binding_kind: SurfaceBindingKind,
    pub active_from: OffsetDateTime,
    pub active_to: Option<OffsetDateTime>,
    pub chain_id: String,
    pub block_hash: String,
    pub block_number: i64,
    pub provenance: Value,
    pub canonicality_state: CanonicalityState,
}

/// Counts of identity rows orphaned during one losing-branch repair.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IdentityOrphanCounts {
    pub token_lineage_count: u64,
    pub resource_count: u64,
    pub name_surface_count: u64,
    pub surface_binding_count: u64,
}

/// Load one token lineage anchor by stable identity from the default canonical read set.
pub async fn load_token_lineage(
    pool: &PgPool,
    token_lineage_id: Uuid,
) -> Result<Option<TokenLineage>> {
    load_token_lineage_internal(pool, token_lineage_id, false).await
}

/// Load one token lineage anchor by stable identity, including observed and orphaned rows.
pub async fn load_token_lineage_including_noncanonical(
    pool: &PgPool,
    token_lineage_id: Uuid,
) -> Result<Option<TokenLineage>> {
    load_token_lineage_internal(pool, token_lineage_id, true).await
}

/// Insert missing token lineage rows or refresh canonicality on re-observation.
pub async fn upsert_token_lineages(
    pool: &PgPool,
    token_lineages: &[TokenLineage],
) -> Result<Vec<TokenLineage>> {
    if token_lineages.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for token-lineage upsert")?;

    let mut snapshots = Vec::with_capacity(token_lineages.len());
    for token_lineage in token_lineages {
        validate_token_lineage(token_lineage)?;
        snapshots.push(upsert_token_lineage(&mut transaction, token_lineage).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit token-lineage upsert")?;

    Ok(snapshots)
}

/// Load one backing resource by stable identity.
pub async fn load_resource(pool: &PgPool, resource_id: Uuid) -> Result<Option<Resource>> {
    load_resource_internal(pool, resource_id, false).await
}

/// Load one backing resource by stable identity, including observed and orphaned rows.
pub async fn load_resource_including_noncanonical(
    pool: &PgPool,
    resource_id: Uuid,
) -> Result<Option<Resource>> {
    load_resource_internal(pool, resource_id, true).await
}

/// Insert missing resource rows or anchor an existing resource to a token lineage.
pub async fn upsert_resources(pool: &PgPool, resources: &[Resource]) -> Result<Vec<Resource>> {
    if resources.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for resource upsert")?;

    let mut snapshots = Vec::with_capacity(resources.len());
    for resource in resources {
        validate_resource(resource)?;
        snapshots.push(upsert_resource(&mut transaction, resource).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit resource upsert")?;

    Ok(snapshots)
}

/// Load one canonical surface row by deterministic logical name identity.
pub async fn load_name_surface(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<Option<NameSurface>> {
    load_name_surface_internal(pool, logical_name_id, false).await
}

/// Load one surface row by deterministic logical name identity, including observed and orphaned rows.
pub async fn load_name_surface_including_noncanonical(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<Option<NameSurface>> {
    load_name_surface_internal(pool, logical_name_id, true).await
}

/// Insert missing canonical surface rows or refresh canonicality on re-observation.
pub async fn upsert_name_surfaces(
    pool: &PgPool,
    name_surfaces: &[NameSurface],
) -> Result<Vec<NameSurface>> {
    if name_surfaces.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for name-surface upsert")?;

    let mut snapshots = Vec::with_capacity(name_surfaces.len());
    for name_surface in name_surfaces {
        validate_name_surface(name_surface)?;
        snapshots.push(upsert_name_surface(&mut transaction, name_surface).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit name-surface upsert")?;

    Ok(snapshots)
}

/// Load one time-ranged surface binding by stable identity.
pub async fn load_surface_binding(
    pool: &PgPool,
    surface_binding_id: Uuid,
) -> Result<Option<SurfaceBinding>> {
    load_surface_binding_internal(pool, surface_binding_id, false).await
}

/// Load one time-ranged surface binding by stable identity, including observed and orphaned rows.
pub async fn load_surface_binding_including_noncanonical(
    pool: &PgPool,
    surface_binding_id: Uuid,
) -> Result<Option<SurfaceBinding>> {
    load_surface_binding_internal(pool, surface_binding_id, true).await
}

/// Load all bindings for one logical surface in chronological order from the default canonical read set.
pub async fn load_surface_bindings_by_logical_name_id(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<Vec<SurfaceBinding>> {
    load_surface_bindings_by_logical_name_id_internal(pool, logical_name_id, false).await
}

/// Load all bindings for one logical surface in chronological order, including observed and orphaned rows.
pub async fn load_surface_bindings_by_logical_name_id_including_noncanonical(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<Vec<SurfaceBinding>> {
    load_surface_bindings_by_logical_name_id_internal(pool, logical_name_id, true).await
}

/// Load all bindings for one backing resource in chronological order from the default canonical read set.
pub async fn load_surface_bindings_by_resource_id(
    pool: &PgPool,
    resource_id: Uuid,
) -> Result<Vec<SurfaceBinding>> {
    load_surface_bindings_by_resource_id_internal(pool, resource_id, false).await
}

/// Load all bindings for one backing resource in chronological order, including observed and orphaned rows.
pub async fn load_surface_bindings_by_resource_id_including_noncanonical(
    pool: &PgPool,
    resource_id: Uuid,
) -> Result<Vec<SurfaceBinding>> {
    load_surface_bindings_by_resource_id_internal(pool, resource_id, true).await
}

/// Insert missing surface-binding rows or close an existing open interval.
pub async fn upsert_surface_bindings(
    pool: &PgPool,
    bindings: &[SurfaceBinding],
) -> Result<Vec<SurfaceBinding>> {
    if bindings.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for surface-binding upsert")?;

    let mut snapshots = Vec::with_capacity(bindings.len());
    for binding in bindings {
        validate_surface_binding(binding)?;
        snapshots.push(upsert_surface_binding(&mut transaction, binding).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit surface-binding upsert")?;

    Ok(snapshots)
}

/// Walk one stored lineage branch from `from_hash` and mark matching surface
/// bindings `orphaned` until `stop_before_hash` is reached.
pub async fn mark_surface_binding_range_orphaned(
    pool: &PgPool,
    chain_id: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
) -> Result<u64> {
    if stop_before_hash == Some(from_hash) {
        return Ok(0);
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for surface-binding orphaning")?;

    let block_hashes =
        load_chain_lineage_hash_path(&mut *transaction, chain_id, from_hash, stop_before_hash)
            .await
            .with_context(|| {
                format!(
                    "failed to load chain lineage path for surface-binding orphaning on chain {chain_id} from block {from_hash}"
                )
            })?;
    if block_hashes.is_empty() {
        bail!("missing stored lineage row for chain {chain_id} block {from_hash}");
    }

    let surface_binding_count = mark_identity_table_orphaned(
        &mut transaction,
        "surface_bindings",
        chain_id,
        &block_hashes,
    )
    .await?;

    transaction
        .commit()
        .await
        .context("failed to commit surface-binding orphaning")?;

    Ok(surface_binding_count)
}

/// Walk one stored lineage branch from `from_hash` and mark matching identity
/// rows `orphaned` until `stop_before_hash` is reached.
pub async fn mark_identity_rows_range_orphaned(
    pool: &PgPool,
    chain_id: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
) -> Result<IdentityOrphanCounts> {
    if stop_before_hash == Some(from_hash) {
        return Ok(IdentityOrphanCounts::default());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for identity orphaning")?;

    let block_hashes =
        load_chain_lineage_hash_path(&mut *transaction, chain_id, from_hash, stop_before_hash)
            .await
            .with_context(|| {
                format!(
                    "failed to load chain lineage path for identity orphaning on chain {chain_id} from block {from_hash}"
                )
            })?;
    if block_hashes.is_empty() {
        bail!("missing stored lineage row for chain {chain_id} block {from_hash}");
    }

    let token_lineage_count =
        mark_identity_table_orphaned(&mut transaction, "token_lineages", chain_id, &block_hashes)
            .await?;
    let resource_count =
        mark_identity_table_orphaned(&mut transaction, "resources", chain_id, &block_hashes)
            .await?;
    let name_surface_count =
        mark_identity_table_orphaned(&mut transaction, "name_surfaces", chain_id, &block_hashes)
            .await?;
    let surface_binding_count = mark_identity_table_orphaned(
        &mut transaction,
        "surface_bindings",
        chain_id,
        &block_hashes,
    )
    .await?;

    transaction
        .commit()
        .await
        .context("failed to commit identity orphaning")?;

    Ok(IdentityOrphanCounts {
        token_lineage_count,
        resource_count,
        name_surface_count,
        surface_binding_count,
    })
}

async fn upsert_token_lineage(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    token_lineage: &TokenLineage,
) -> Result<TokenLineage> {
    let provenance = serde_json::to_string(&token_lineage.provenance)
        .context("failed to serialize token-lineage provenance")?;

    if let Some(snapshot) = sqlx::query(
        r#"
        INSERT INTO token_lineages (
            token_lineage_id,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, $5::jsonb, $6::canonicality_state)
        ON CONFLICT (token_lineage_id) DO NOTHING
        RETURNING
            token_lineage_id,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(token_lineage.token_lineage_id)
    .bind(&token_lineage.chain_id)
    .bind(&token_lineage.block_hash)
    .bind(token_lineage.block_number)
    .bind(provenance)
    .bind(token_lineage.canonicality_state.as_str())
    .fetch_optional(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to insert token lineage {}",
            token_lineage.token_lineage_id
        )
    })? {
        return decode_token_lineage(snapshot);
    }

    let existing =
        load_token_lineage_internal(&mut **executor, token_lineage.token_lineage_id, true)
            .await?
            .with_context(|| {
                format!(
                    "failed to reload existing token lineage {} after insert conflict",
                    token_lineage.token_lineage_id
                )
            })?;

    ensure_token_lineage_identity_matches(&existing, token_lineage)?;
    let next_observation = merge_stable_row_observation(
        existing.canonicality_state,
        &existing.chain_id,
        &existing.block_hash,
        existing.block_number,
        &existing.provenance,
        &token_lineage.chain_id,
        &token_lineage.block_hash,
        token_lineage.block_number,
        &token_lineage.provenance,
    )
    .with_context(|| {
        format!(
            "token lineage {} cannot refresh observation metadata",
            token_lineage.token_lineage_id
        )
    })?;
    let next_state = merge_canonicality(
        existing.canonicality_state,
        token_lineage.canonicality_state,
    );

    let snapshot = sqlx::query(
        r#"
        UPDATE token_lineages
        SET
            chain_id = $2,
            block_hash = $3,
            block_number = $4,
            provenance = $5::jsonb,
            canonicality_state = $6::canonicality_state,
            observed_at = now()
        WHERE token_lineage_id = $1
        RETURNING
            token_lineage_id,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(token_lineage.token_lineage_id)
    .bind(&next_observation.chain_id)
    .bind(&next_observation.block_hash)
    .bind(next_observation.block_number)
    .bind(next_observation.provenance)
    .bind(next_state.as_str())
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to refresh existing token lineage {}",
            token_lineage.token_lineage_id
        )
    })?;

    decode_token_lineage(snapshot)
}

async fn upsert_resource(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    resource: &Resource,
) -> Result<Resource> {
    let provenance = serde_json::to_string(&resource.provenance)
        .context("failed to serialize resource provenance")?;

    if let Some(snapshot) = sqlx::query(
        r#"
        INSERT INTO resources (
            resource_id,
            token_lineage_id,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::canonicality_state)
        ON CONFLICT (resource_id) DO NOTHING
        RETURNING
            resource_id,
            token_lineage_id,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(resource.resource_id)
    .bind(resource.token_lineage_id)
    .bind(&resource.chain_id)
    .bind(&resource.block_hash)
    .bind(resource.block_number)
    .bind(provenance)
    .bind(resource.canonicality_state.as_str())
    .fetch_optional(&mut **executor)
    .await
    .with_context(|| format!("failed to insert resource {}", resource.resource_id))?
    {
        return decode_resource(snapshot);
    }

    let existing = load_resource_internal(&mut **executor, resource.resource_id, true)
        .await?
        .with_context(|| {
            format!(
                "failed to reload existing resource {} after insert conflict",
                resource.resource_id
            )
        })?;

    ensure_resource_identity_matches(&existing, resource)?;
    let next_token_lineage_id =
        merge_token_lineage_anchor(existing.token_lineage_id, resource.token_lineage_id)?;
    let next_observation = merge_stable_row_observation(
        existing.canonicality_state,
        &existing.chain_id,
        &existing.block_hash,
        existing.block_number,
        &existing.provenance,
        &resource.chain_id,
        &resource.block_hash,
        resource.block_number,
        &resource.provenance,
    )
    .with_context(|| {
        format!(
            "resource {} cannot refresh observation metadata",
            resource.resource_id
        )
    })?;
    let next_state = merge_canonicality(existing.canonicality_state, resource.canonicality_state);

    let snapshot = sqlx::query(
        r#"
        UPDATE resources
        SET
            token_lineage_id = $2,
            chain_id = $3,
            block_hash = $4,
            block_number = $5,
            provenance = $6::jsonb,
            canonicality_state = $7::canonicality_state,
            observed_at = now()
        WHERE resource_id = $1
        RETURNING
            resource_id,
            token_lineage_id,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(resource.resource_id)
    .bind(next_token_lineage_id)
    .bind(&next_observation.chain_id)
    .bind(&next_observation.block_hash)
    .bind(next_observation.block_number)
    .bind(next_observation.provenance)
    .bind(next_state.as_str())
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to refresh existing resource {}",
            resource.resource_id
        )
    })?;

    decode_resource(snapshot)
}

async fn upsert_name_surface(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    name_surface: &NameSurface,
) -> Result<NameSurface> {
    let normalization_warnings = serde_json::to_string(&name_surface.normalization_warnings)
        .context("failed to serialize name-surface normalization_warnings")?;
    let normalization_errors = serde_json::to_string(&name_surface.normalization_errors)
        .context("failed to serialize name-surface normalization_errors")?;
    let provenance = serde_json::to_string(&name_surface.provenance)
        .context("failed to serialize name-surface provenance")?;

    if let Some(snapshot) = sqlx::query(
        r#"
        INSERT INTO name_surfaces (
            logical_name_id,
            namespace,
            input_name,
            canonical_display_name,
            normalized_name,
            dns_encoded_name,
            namehash,
            labelhashes,
            normalizer_version,
            normalization_warnings,
            normalization_errors,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state
        )
        VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7,
            $8,
            $9,
            $10::jsonb,
            $11::jsonb,
            $12,
            $13,
            $14,
            $15::jsonb,
            $16::canonicality_state
        )
        ON CONFLICT (logical_name_id) DO NOTHING
        RETURNING
            logical_name_id,
            namespace,
            input_name,
            canonical_display_name,
            normalized_name,
            dns_encoded_name,
            namehash,
            labelhashes,
            normalizer_version,
            normalization_warnings,
            normalization_errors,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(&name_surface.logical_name_id)
    .bind(&name_surface.namespace)
    .bind(&name_surface.input_name)
    .bind(&name_surface.canonical_display_name)
    .bind(&name_surface.normalized_name)
    .bind(&name_surface.dns_encoded_name)
    .bind(&name_surface.namehash)
    .bind(&name_surface.labelhashes)
    .bind(&name_surface.normalizer_version)
    .bind(normalization_warnings)
    .bind(normalization_errors)
    .bind(&name_surface.chain_id)
    .bind(&name_surface.block_hash)
    .bind(name_surface.block_number)
    .bind(provenance)
    .bind(name_surface.canonicality_state.as_str())
    .fetch_optional(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to insert name surface {}",
            name_surface.logical_name_id
        )
    })? {
        return decode_name_surface(snapshot);
    }

    let existing = load_name_surface_internal(&mut **executor, &name_surface.logical_name_id, true)
        .await?
        .with_context(|| {
            format!(
                "failed to reload existing name surface {} after insert conflict",
                name_surface.logical_name_id
            )
        })?;

    ensure_name_surface_identity_matches(&existing, name_surface)?;
    let next_observation = merge_stable_row_observation(
        existing.canonicality_state,
        &existing.chain_id,
        &existing.block_hash,
        existing.block_number,
        &existing.provenance,
        &name_surface.chain_id,
        &name_surface.block_hash,
        name_surface.block_number,
        &name_surface.provenance,
    )
    .with_context(|| {
        format!(
            "name surface {} cannot refresh observation metadata",
            name_surface.logical_name_id
        )
    })?;
    let next_state =
        merge_canonicality(existing.canonicality_state, name_surface.canonicality_state);

    let snapshot = sqlx::query(
        r#"
        UPDATE name_surfaces
        SET
            chain_id = $2,
            block_hash = $3,
            block_number = $4,
            provenance = $5::jsonb,
            canonicality_state = $6::canonicality_state,
            observed_at = now()
        WHERE logical_name_id = $1
        RETURNING
            logical_name_id,
            namespace,
            input_name,
            canonical_display_name,
            normalized_name,
            dns_encoded_name,
            namehash,
            labelhashes,
            normalizer_version,
            normalization_warnings,
            normalization_errors,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(&name_surface.logical_name_id)
    .bind(&next_observation.chain_id)
    .bind(&next_observation.block_hash)
    .bind(next_observation.block_number)
    .bind(next_observation.provenance)
    .bind(next_state.as_str())
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to refresh existing name surface {}",
            name_surface.logical_name_id
        )
    })?;

    decode_name_surface(snapshot)
}

async fn upsert_surface_binding(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    binding: &SurfaceBinding,
) -> Result<SurfaceBinding> {
    let provenance = serde_json::to_string(&binding.provenance)
        .context("failed to serialize surface-binding provenance")?;

    if let Some(snapshot) = sqlx::query(
        r#"
        INSERT INTO surface_bindings (
            surface_binding_id,
            logical_name_id,
            resource_id,
            binding_kind,
            active_from,
            active_to,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11::canonicality_state)
        ON CONFLICT (surface_binding_id) DO NOTHING
        RETURNING
            surface_binding_id,
            logical_name_id,
            resource_id,
            binding_kind,
            active_from,
            active_to,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(binding.surface_binding_id)
    .bind(&binding.logical_name_id)
    .bind(binding.resource_id)
    .bind(binding.binding_kind.as_str())
    .bind(binding.active_from)
    .bind(binding.active_to)
    .bind(&binding.chain_id)
    .bind(&binding.block_hash)
    .bind(binding.block_number)
    .bind(provenance)
    .bind(binding.canonicality_state.as_str())
    .fetch_optional(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to insert surface binding {}",
            binding.surface_binding_id
        )
    })? {
        return decode_surface_binding(snapshot);
    }

    let existing = load_surface_binding_internal(&mut **executor, binding.surface_binding_id, true)
        .await?
        .with_context(|| {
            format!(
                "failed to reload existing surface binding {} after insert conflict",
                binding.surface_binding_id
            )
        })?;

    ensure_surface_binding_identity_matches(&existing, binding)?;
    let next_active_to = merge_binding_active_to(existing.active_to, binding.active_to)?;
    let next_state = merge_canonicality(existing.canonicality_state, binding.canonicality_state);

    let snapshot = sqlx::query(
        r#"
        UPDATE surface_bindings
        SET
            active_to = $2,
            canonicality_state = $3::canonicality_state,
            observed_at = now()
        WHERE surface_binding_id = $1
        RETURNING
            surface_binding_id,
            logical_name_id,
            resource_id,
            binding_kind,
            active_from,
            active_to,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(binding.surface_binding_id)
    .bind(next_active_to)
    .bind(next_state.as_str())
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to refresh existing surface binding {}",
            binding.surface_binding_id
        )
    })?;

    decode_surface_binding(snapshot)
}

async fn load_token_lineage_internal<'e, E>(
    executor: E,
    token_lineage_id: Uuid,
    include_noncanonical: bool,
) -> Result<Option<TokenLineage>>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(&format!(
        r#"
        SELECT
            token_lineage_id,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state::TEXT AS canonicality_state
        FROM token_lineages
        WHERE token_lineage_id = $1
        {}
        "#,
        identity_read_filter(include_noncanonical),
    ))
    .bind(token_lineage_id)
    .fetch_optional(executor)
    .await
    .with_context(|| format!("failed to load token lineage {token_lineage_id}"))?;

    row.map(decode_token_lineage).transpose()
}

async fn load_resource_internal<'e, E>(
    executor: E,
    resource_id: Uuid,
    include_noncanonical: bool,
) -> Result<Option<Resource>>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(&format!(
        r#"
        SELECT
            resource_id,
            token_lineage_id,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state::TEXT AS canonicality_state
        FROM resources
        WHERE resource_id = $1
        {}
        "#,
        identity_read_filter(include_noncanonical),
    ))
    .bind(resource_id)
    .fetch_optional(executor)
    .await
    .with_context(|| format!("failed to load resource {resource_id}"))?;

    row.map(decode_resource).transpose()
}

async fn load_name_surface_internal<'e, E>(
    executor: E,
    logical_name_id: &str,
    include_noncanonical: bool,
) -> Result<Option<NameSurface>>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(&format!(
        r#"
        SELECT
            logical_name_id,
            namespace,
            input_name,
            canonical_display_name,
            normalized_name,
            dns_encoded_name,
            namehash,
            labelhashes,
            normalizer_version,
            normalization_warnings,
            normalization_errors,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state::TEXT AS canonicality_state
        FROM name_surfaces
        WHERE logical_name_id = $1
        {}
        "#,
        identity_read_filter(include_noncanonical),
    ))
    .bind(logical_name_id)
    .fetch_optional(executor)
    .await
    .with_context(|| format!("failed to load name surface {logical_name_id}"))?;

    row.map(decode_name_surface).transpose()
}

async fn load_surface_binding_internal<'e, E>(
    executor: E,
    surface_binding_id: Uuid,
    include_noncanonical: bool,
) -> Result<Option<SurfaceBinding>>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(&format!(
        r#"
        SELECT
            surface_binding_id,
            logical_name_id,
            resource_id,
            binding_kind,
            active_from,
            active_to,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state::TEXT AS canonicality_state
        FROM surface_bindings
        WHERE surface_binding_id = $1
        {}
        "#,
        identity_read_filter(include_noncanonical),
    ))
    .bind(surface_binding_id)
    .fetch_optional(executor)
    .await
    .with_context(|| format!("failed to load surface binding {surface_binding_id}"))?;

    row.map(decode_surface_binding).transpose()
}

async fn load_surface_bindings_by_logical_name_id_internal<'e, E>(
    executor: E,
    logical_name_id: &str,
    include_noncanonical: bool,
) -> Result<Vec<SurfaceBinding>>
where
    E: Executor<'e, Database = Postgres>,
{
    let rows = sqlx::query(&format!(
        r#"
        SELECT
            surface_binding_id,
            logical_name_id,
            resource_id,
            binding_kind,
            active_from,
            active_to,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state::TEXT AS canonicality_state
        FROM surface_bindings
        WHERE logical_name_id = $1
        {}
        ORDER BY active_from, active_to NULLS LAST, surface_binding_id
        "#,
        identity_read_filter(include_noncanonical),
    ))
    .bind(logical_name_id)
    .fetch_all(executor)
    .await
    .with_context(|| {
        format!("failed to load surface bindings for logical name {logical_name_id}")
    })?;

    rows.into_iter().map(decode_surface_binding).collect()
}

async fn load_surface_bindings_by_resource_id_internal<'e, E>(
    executor: E,
    resource_id: Uuid,
    include_noncanonical: bool,
) -> Result<Vec<SurfaceBinding>>
where
    E: Executor<'e, Database = Postgres>,
{
    let rows = sqlx::query(&format!(
        r#"
        SELECT
            surface_binding_id,
            logical_name_id,
            resource_id,
            binding_kind,
            active_from,
            active_to,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state::TEXT AS canonicality_state
        FROM surface_bindings
        WHERE resource_id = $1
        {}
        ORDER BY active_from, active_to NULLS LAST, logical_name_id, surface_binding_id
        "#,
        identity_read_filter(include_noncanonical),
    ))
    .bind(resource_id)
    .fetch_all(executor)
    .await
    .with_context(|| format!("failed to load surface bindings for resource {resource_id}"))?;

    rows.into_iter().map(decode_surface_binding).collect()
}

async fn load_chain_lineage_hash_path<'e, E>(
    executor: E,
    chain_id: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
) -> Result<Vec<String>>
where
    E: Executor<'e, Database = Postgres>,
{
    let rows = sqlx::query(
        r#"
        WITH RECURSIVE lineage_path AS (
            SELECT chain_id, block_hash, parent_hash, 0 AS depth
            FROM chain_lineage
            WHERE chain_id = $1
              AND block_hash = $2

            UNION ALL

            SELECT parent.chain_id, parent.block_hash, parent.parent_hash, lineage_path.depth + 1
            FROM chain_lineage AS parent
            JOIN lineage_path
              ON parent.chain_id = lineage_path.chain_id
             AND parent.block_hash = lineage_path.parent_hash
            WHERE $3::TEXT IS NULL
               OR parent.block_hash <> $3::TEXT
        )
        SELECT block_hash
        FROM lineage_path
        ORDER BY depth
        "#,
    )
    .bind(chain_id)
    .bind(from_hash)
    .bind(stop_before_hash)
    .fetch_all(executor)
    .await?;

    rows.into_iter()
        .map(|row| {
            row.try_get::<String, _>("block_hash")
                .context("failed to decode identity orphaning block_hash")
        })
        .collect()
}

async fn mark_identity_table_orphaned(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    table_name: &str,
    chain_id: &str,
    block_hashes: &[String],
) -> Result<u64> {
    let statement = format!(
        r#"
        UPDATE {table_name}
        SET
            canonicality_state = 'orphaned'::canonicality_state,
            observed_at = now()
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
          AND canonicality_state <> 'orphaned'::canonicality_state
        "#,
    );

    sqlx::query(&statement)
        .bind(chain_id)
        .bind(block_hashes)
        .execute(&mut **executor)
        .await
        .with_context(|| {
            format!("failed to mark orphaned identity rows in {table_name} for chain {chain_id}")
        })
        .map(|result| result.rows_affected())
}

fn validate_token_lineage(token_lineage: &TokenLineage) -> Result<()> {
    validate_anchor_fields(
        "token lineage",
        &token_lineage.chain_id,
        &token_lineage.block_hash,
        token_lineage.block_number,
    )?;
    if !token_lineage.provenance.is_object() {
        bail!(
            "token lineage {} must store provenance as a JSON object",
            token_lineage.token_lineage_id
        );
    }

    Ok(())
}

fn validate_resource(resource: &Resource) -> Result<()> {
    validate_anchor_fields(
        "resource",
        &resource.chain_id,
        &resource.block_hash,
        resource.block_number,
    )?;
    if !resource.provenance.is_object() {
        bail!(
            "resource {} must store provenance as a JSON object",
            resource.resource_id
        );
    }

    Ok(())
}

fn validate_name_surface(name_surface: &NameSurface) -> Result<()> {
    if name_surface.logical_name_id.is_empty() {
        bail!("name surface has empty logical_name_id");
    }
    if name_surface.namespace.is_empty() {
        bail!(
            "name surface {} has empty namespace",
            name_surface.logical_name_id
        );
    }
    if name_surface.input_name.is_empty() {
        bail!(
            "name surface {} has empty input_name",
            name_surface.logical_name_id
        );
    }
    if name_surface.canonical_display_name.is_empty() {
        bail!(
            "name surface {} has empty canonical_display_name",
            name_surface.logical_name_id
        );
    }
    if name_surface.normalized_name.is_empty() {
        bail!(
            "name surface {} has empty normalized_name",
            name_surface.logical_name_id
        );
    }
    if name_surface.logical_name_id
        != format!(
            "{}:{}",
            name_surface.namespace, name_surface.normalized_name
        )
    {
        bail!(
            "name surface {} does not match namespace {} and normalized_name {}",
            name_surface.logical_name_id,
            name_surface.namespace,
            name_surface.normalized_name
        );
    }
    if name_surface.dns_encoded_name.is_empty() {
        bail!(
            "name surface {} has empty dns_encoded_name",
            name_surface.logical_name_id
        );
    }
    if name_surface.namehash.is_empty() {
        bail!(
            "name surface {} has empty namehash",
            name_surface.logical_name_id
        );
    }
    if name_surface.labelhashes.is_empty() {
        bail!(
            "name surface {} has empty labelhashes",
            name_surface.logical_name_id
        );
    }
    if name_surface.normalizer_version.is_empty() {
        bail!(
            "name surface {} has empty normalizer_version",
            name_surface.logical_name_id
        );
    }
    if !name_surface.normalization_warnings.is_array() {
        bail!(
            "name surface {} must store normalization_warnings as a JSON array",
            name_surface.logical_name_id
        );
    }
    if !name_surface.normalization_errors.is_array() {
        bail!(
            "name surface {} must store normalization_errors as a JSON array",
            name_surface.logical_name_id
        );
    }
    validate_anchor_fields(
        "name surface",
        &name_surface.chain_id,
        &name_surface.block_hash,
        name_surface.block_number,
    )?;
    if !name_surface.provenance.is_object() {
        bail!(
            "name surface {} must store provenance as a JSON object",
            name_surface.logical_name_id
        );
    }

    Ok(())
}

fn validate_surface_binding(binding: &SurfaceBinding) -> Result<()> {
    if binding.logical_name_id.is_empty() {
        bail!(
            "surface binding {} has empty logical_name_id",
            binding.surface_binding_id
        );
    }
    if let Some(active_to) = binding.active_to
        && active_to <= binding.active_from
    {
        bail!(
            "surface binding {} must have active_to after active_from",
            binding.surface_binding_id
        );
    }
    validate_anchor_fields(
        "surface binding",
        &binding.chain_id,
        &binding.block_hash,
        binding.block_number,
    )?;
    if !binding.provenance.is_object() {
        bail!(
            "surface binding {} must store provenance as a JSON object",
            binding.surface_binding_id
        );
    }

    Ok(())
}

fn validate_anchor_fields(
    row_kind: &str,
    chain_id: &str,
    block_hash: &str,
    block_number: i64,
) -> Result<()> {
    if chain_id.trim().is_empty() || chain_id == "unknown" {
        bail!("{row_kind} must provide a real chain_id anchor");
    }
    if block_hash.trim().is_empty() || block_hash == "unknown" {
        bail!("{row_kind} must provide a real block_hash anchor");
    }
    if block_number < 0 {
        bail!("{row_kind} has negative block_number {block_number}");
    }

    Ok(())
}

fn ensure_token_lineage_identity_matches(
    existing: &TokenLineage,
    incoming: &TokenLineage,
) -> Result<()> {
    let _ = (existing, incoming);
    Ok(())
}

fn ensure_resource_identity_matches(existing: &Resource, incoming: &Resource) -> Result<()> {
    let _ = (existing, incoming);
    Ok(())
}

fn ensure_name_surface_identity_matches(
    existing: &NameSurface,
    incoming: &NameSurface,
) -> Result<()> {
    if existing.namespace != incoming.namespace
        || existing.input_name != incoming.input_name
        || existing.canonical_display_name != incoming.canonical_display_name
        || existing.normalized_name != incoming.normalized_name
        || existing.dns_encoded_name != incoming.dns_encoded_name
        || existing.namehash != incoming.namehash
        || existing.labelhashes != incoming.labelhashes
        || existing.normalizer_version != incoming.normalizer_version
        || existing.normalization_warnings != incoming.normalization_warnings
        || existing.normalization_errors != incoming.normalization_errors
    {
        bail!(
            "name surface identity mismatch for {}",
            existing.logical_name_id
        );
    }

    Ok(())
}

fn ensure_surface_binding_identity_matches(
    existing: &SurfaceBinding,
    incoming: &SurfaceBinding,
) -> Result<()> {
    if existing.logical_name_id != incoming.logical_name_id
        || existing.resource_id != incoming.resource_id
        || existing.binding_kind != incoming.binding_kind
        || existing.active_from != incoming.active_from
        || existing.chain_id != incoming.chain_id
        || existing.block_hash != incoming.block_hash
        || existing.block_number != incoming.block_number
        || existing.provenance != incoming.provenance
    {
        bail!(
            "surface binding identity mismatch for {}",
            existing.surface_binding_id
        );
    }

    Ok(())
}

struct StableObservationRefresh {
    chain_id: String,
    block_hash: String,
    block_number: i64,
    provenance: String,
}

fn merge_token_lineage_anchor(
    current: Option<Uuid>,
    incoming: Option<Uuid>,
) -> Result<Option<Uuid>> {
    match (current, incoming) {
        (Some(current), Some(incoming)) if current != incoming => {
            bail!("resource token_lineage_id mismatch: stored {current}, incoming {incoming}")
        }
        (Some(current), _) => Ok(Some(current)),
        (None, incoming) => Ok(incoming),
    }
}

fn merge_stable_row_observation(
    current_state: CanonicalityState,
    current_chain_id: &str,
    current_block_hash: &str,
    current_block_number: i64,
    current_provenance: &Value,
    incoming_chain_id: &str,
    incoming_block_hash: &str,
    incoming_block_number: i64,
    incoming_provenance: &Value,
) -> Result<StableObservationRefresh> {
    let same_anchor = current_chain_id == incoming_chain_id
        && current_block_hash == incoming_block_hash
        && current_block_number == incoming_block_number;

    if !same_anchor && current_state != CanonicalityState::Orphaned {
        bail!(
            "stable identity row cannot change observation anchor before orphaning: stored {current_chain_id}/{current_block_hash}/{current_block_number}, incoming {incoming_chain_id}/{incoming_block_hash}/{incoming_block_number}"
        );
    }

    let provenance = if same_anchor && current_provenance == incoming_provenance {
        serde_json::to_string(current_provenance)
            .context("failed to serialize stable-row provenance")?
    } else {
        serde_json::to_string(incoming_provenance)
            .context("failed to serialize stable-row provenance")?
    };

    Ok(StableObservationRefresh {
        chain_id: incoming_chain_id.to_owned(),
        block_hash: incoming_block_hash.to_owned(),
        block_number: incoming_block_number,
        provenance,
    })
}

fn merge_binding_active_to(
    current: Option<OffsetDateTime>,
    incoming: Option<OffsetDateTime>,
) -> Result<Option<OffsetDateTime>> {
    match (current, incoming) {
        (Some(current), Some(incoming)) if current != incoming => {
            bail!("surface binding active_to mismatch: stored {current}, incoming {incoming}")
        }
        (Some(current), _) => Ok(Some(current)),
        (None, incoming) => Ok(incoming),
    }
}

fn merge_canonicality(
    current: CanonicalityState,
    incoming: CanonicalityState,
) -> CanonicalityState {
    match incoming {
        CanonicalityState::Orphaned => CanonicalityState::Orphaned,
        CanonicalityState::Observed => {
            if current == CanonicalityState::Orphaned {
                CanonicalityState::Observed
            } else {
                current
            }
        }
        CanonicalityState::Canonical | CanonicalityState::Safe | CanonicalityState::Finalized => {
            if current == CanonicalityState::Orphaned {
                incoming
            } else {
                current.promote_to(incoming)
            }
        }
    }
}

fn identity_read_filter(include_noncanonical: bool) -> &'static str {
    if include_noncanonical {
        ""
    } else {
        DEFAULT_IDENTITY_READ_FILTER
    }
}

fn decode_token_lineage(row: PgRow) -> Result<TokenLineage> {
    Ok(TokenLineage {
        token_lineage_id: row
            .try_get("token_lineage_id")
            .context("missing token_lineage_id")?,
        chain_id: row.try_get("chain_id").context("missing chain_id")?,
        block_hash: row.try_get("block_hash").context("missing block_hash")?,
        block_number: row
            .try_get("block_number")
            .context("missing block_number")?,
        provenance: row.try_get("provenance").context("missing provenance")?,
        canonicality_state: CanonicalityState::parse(
            &row.try_get::<String, _>("canonicality_state")
                .context("missing canonicality_state")?,
        )?,
    })
}

fn decode_resource(row: PgRow) -> Result<Resource> {
    Ok(Resource {
        resource_id: row.try_get("resource_id").context("missing resource_id")?,
        token_lineage_id: row
            .try_get("token_lineage_id")
            .context("missing token_lineage_id")?,
        chain_id: row.try_get("chain_id").context("missing chain_id")?,
        block_hash: row.try_get("block_hash").context("missing block_hash")?,
        block_number: row
            .try_get("block_number")
            .context("missing block_number")?,
        provenance: row.try_get("provenance").context("missing provenance")?,
        canonicality_state: CanonicalityState::parse(
            &row.try_get::<String, _>("canonicality_state")
                .context("missing canonicality_state")?,
        )?,
    })
}

fn decode_name_surface(row: PgRow) -> Result<NameSurface> {
    Ok(NameSurface {
        logical_name_id: row
            .try_get("logical_name_id")
            .context("missing logical_name_id")?,
        namespace: row.try_get("namespace").context("missing namespace")?,
        input_name: row.try_get("input_name").context("missing input_name")?,
        canonical_display_name: row
            .try_get("canonical_display_name")
            .context("missing canonical_display_name")?,
        normalized_name: row
            .try_get("normalized_name")
            .context("missing normalized_name")?,
        dns_encoded_name: row
            .try_get("dns_encoded_name")
            .context("missing dns_encoded_name")?,
        namehash: row.try_get("namehash").context("missing namehash")?,
        labelhashes: row.try_get("labelhashes").context("missing labelhashes")?,
        normalizer_version: row
            .try_get("normalizer_version")
            .context("missing normalizer_version")?,
        normalization_warnings: row
            .try_get("normalization_warnings")
            .context("missing normalization_warnings")?,
        normalization_errors: row
            .try_get("normalization_errors")
            .context("missing normalization_errors")?,
        chain_id: row.try_get("chain_id").context("missing chain_id")?,
        block_hash: row.try_get("block_hash").context("missing block_hash")?,
        block_number: row
            .try_get("block_number")
            .context("missing block_number")?,
        provenance: row.try_get("provenance").context("missing provenance")?,
        canonicality_state: CanonicalityState::parse(
            &row.try_get::<String, _>("canonicality_state")
                .context("missing canonicality_state")?,
        )?,
    })
}

fn decode_surface_binding(row: PgRow) -> Result<SurfaceBinding> {
    Ok(SurfaceBinding {
        surface_binding_id: row
            .try_get("surface_binding_id")
            .context("missing surface_binding_id")?,
        logical_name_id: row
            .try_get("logical_name_id")
            .context("missing logical_name_id")?,
        resource_id: row.try_get("resource_id").context("missing resource_id")?,
        binding_kind: SurfaceBindingKind::parse(
            &row.try_get::<String, _>("binding_kind")
                .context("missing binding_kind")?,
        )?,
        active_from: row.try_get("active_from").context("missing active_from")?,
        active_to: row.try_get("active_to").context("missing active_to")?,
        chain_id: row.try_get("chain_id").context("missing chain_id")?,
        block_hash: row.try_get("block_hash").context("missing block_hash")?,
        block_number: row
            .try_get("block_number")
            .context("missing block_number")?,
        provenance: row.try_get("provenance").context("missing provenance")?,
        canonicality_state: CanonicalityState::parse(
            &row.try_get::<String, _>("canonicality_state")
                .context("missing canonicality_state")?,
        )?,
    })
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use anyhow::{Context, Result};
    use serde_json::json;
    use sqlx::PgPool;
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use sqlx::types::time::OffsetDateTime;
    use uuid::Uuid;

    use super::{
        IdentityOrphanCounts, NameSurface, Resource, SurfaceBinding, SurfaceBindingKind,
        TokenLineage, load_name_surface, load_name_surface_including_noncanonical, load_resource,
        load_resource_including_noncanonical, load_surface_binding,
        load_surface_binding_including_noncanonical, load_surface_bindings_by_logical_name_id,
        load_surface_bindings_by_logical_name_id_including_noncanonical,
        load_surface_bindings_by_resource_id,
        load_surface_bindings_by_resource_id_including_noncanonical, load_token_lineage,
        load_token_lineage_including_noncanonical, mark_identity_rows_range_orphaned,
        mark_surface_binding_range_orphaned, upsert_name_surfaces, upsert_resources,
        upsert_surface_bindings, upsert_token_lineages,
    };
    use crate::{
        CanonicalityState, ChainLineageBlock, default_database_url, upsert_chain_lineage_blocks,
    };

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

    struct TestDatabase {
        admin_pool: PgPool,
        pool: PgPool,
        database_name: String,
    }

    impl TestDatabase {
        async fn new() -> Result<Self> {
            let database_url = std::env::var("BIGNAME_DATABASE_URL")
                .or_else(|_| std::env::var("DATABASE_URL"))
                .unwrap_or_else(|_| default_database_url().to_owned());
            let base_options = PgConnectOptions::from_str(&database_url)
                .context("failed to parse database URL for storage identity integration tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bigname_storage_identity_test_{}_{}_{}",
                std::process::id(),
                unique,
                sequence
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for storage identity integration tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect storage identity integration test pool")?;

            crate::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for storage identity integration tests")?;

            Ok(Self {
                admin_pool,
                pool,
                database_name,
            })
        }

        fn pool(&self) -> &PgPool {
            &self.pool
        }

        async fn cleanup(self) -> Result<()> {
            self.pool.close().await;
            sqlx::query(&format!(
                r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
                self.database_name
            ))
            .execute(&self.admin_pool)
            .await
            .with_context(|| format!("failed to drop test database {}", self.database_name))?;
            self.admin_pool.close().await;
            Ok(())
        }
    }

    fn timestamp(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp must be valid")
    }

    fn lineage_block(
        chain_id: &str,
        block_hash: &str,
        parent_hash: Option<&str>,
        block_number: i64,
        block_timestamp: OffsetDateTime,
        canonicality_state: CanonicalityState,
    ) -> ChainLineageBlock {
        ChainLineageBlock {
            chain_id: chain_id.to_owned(),
            block_hash: block_hash.to_owned(),
            parent_hash: parent_hash.map(str::to_owned),
            block_number,
            block_timestamp,
            logs_bloom: Some(vec![block_number as u8]),
            transactions_root: Some(format!("0xtx{:02x}", block_number)),
            receipts_root: Some(format!("0xrc{:02x}", block_number)),
            state_root: Some(format!("0xst{:02x}", block_number)),
            canonicality_state,
        }
    }

    fn anchor(label: &str, block_number: i64) -> (String, String, i64) {
        (
            format!("chain:{label}"),
            format!("0x{label}_{block_number:08x}"),
            block_number,
        )
    }

    fn token_lineage(
        token_lineage_id: Uuid,
        namespace: &str,
        chain_label: &str,
        block_number: i64,
        canonicality_state: CanonicalityState,
    ) -> TokenLineage {
        let (chain_id, block_hash, block_number) = anchor(chain_label, block_number);
        TokenLineage {
            token_lineage_id,
            chain_id,
            block_hash,
            block_number,
            provenance: json!({"source": namespace, "anchor": "token_lineage"}),
            canonicality_state,
        }
    }

    fn resource(
        resource_id: Uuid,
        token_lineage_id: Option<Uuid>,
        namespace: &str,
        chain_label: &str,
        block_number: i64,
        canonicality_state: CanonicalityState,
    ) -> Resource {
        let (chain_id, block_hash, block_number) = anchor(chain_label, block_number);
        Resource {
            resource_id,
            token_lineage_id,
            chain_id,
            block_hash,
            block_number,
            provenance: json!({"source": namespace, "anchor": "resource"}),
            canonicality_state,
        }
    }

    fn name_surface(
        logical_name_id: &str,
        input_name: &str,
        normalized_name: &str,
        chain_label: &str,
        block_number: i64,
        canonicality_state: CanonicalityState,
    ) -> NameSurface {
        let (chain_id, block_hash, block_number) = anchor(chain_label, block_number);
        NameSurface {
            logical_name_id: logical_name_id.to_owned(),
            namespace: "ens".to_owned(),
            input_name: input_name.to_owned(),
            canonical_display_name: input_name.to_owned(),
            normalized_name: normalized_name.to_owned(),
            dns_encoded_name: vec![4, b't', b'e', b's', b't', 3, b'e', b't', b'h', 0],
            namehash: format!("namehash:{normalized_name}"),
            labelhashes: vec![format!("labelhash:{normalized_name}")],
            normalizer_version: "ensip15@2026-04-16".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id,
            block_hash,
            block_number,
            provenance: json!({"source": "registry_sync", "surface": logical_name_id}),
            canonicality_state,
        }
    }

    fn binding(
        surface_binding_id: Uuid,
        logical_name_id: &str,
        resource_id: Uuid,
        binding_kind: SurfaceBindingKind,
        active_from: OffsetDateTime,
        active_to: Option<OffsetDateTime>,
        source: &str,
        chain_label: &str,
        block_number: i64,
        canonicality_state: CanonicalityState,
    ) -> SurfaceBinding {
        let (chain_id, block_hash, block_number) = anchor(chain_label, block_number);
        SurfaceBinding {
            surface_binding_id,
            logical_name_id: logical_name_id.to_owned(),
            resource_id,
            binding_kind,
            active_from,
            active_to,
            chain_id,
            block_hash,
            block_number,
            provenance: json!({"source": source}),
            canonicality_state,
        }
    }

    #[tokio::test]
    async fn persists_canonical_surface_round_trip_with_resource_and_token_lineage() -> Result<()> {
        let database = TestDatabase::new().await?;
        let token_lineage_id = Uuid::from_u128(0x1000);
        let resource_id = Uuid::from_u128(0x2000);
        let surface_binding_id = Uuid::from_u128(0x3000);

        let expected_token_lineage = token_lineage(
            token_lineage_id,
            "ens",
            "token_round_trip",
            101,
            CanonicalityState::Finalized,
        );
        let expected_resource = resource(
            resource_id,
            Some(token_lineage_id),
            "ens",
            "resource_round_trip",
            102,
            CanonicalityState::Canonical,
        );
        let expected_surface = name_surface(
            "ens:test.eth",
            "test.eth",
            "test.eth",
            "surface_round_trip",
            103,
            CanonicalityState::Finalized,
        );
        let expected_binding = binding(
            surface_binding_id,
            "ens:test.eth",
            resource_id,
            SurfaceBindingKind::DeclaredRegistryPath,
            timestamp(1_717_171_700),
            None,
            "declared_registry_path",
            "binding_round_trip",
            104,
            CanonicalityState::Safe,
        );

        assert_eq!(
            upsert_token_lineages(database.pool(), &[expected_token_lineage.clone()]).await?,
            vec![expected_token_lineage.clone()]
        );
        assert_eq!(
            upsert_resources(database.pool(), &[expected_resource.clone()]).await?,
            vec![expected_resource.clone()]
        );
        assert_eq!(
            upsert_name_surfaces(database.pool(), &[expected_surface.clone()]).await?,
            vec![expected_surface.clone()]
        );
        assert_eq!(
            upsert_surface_bindings(database.pool(), &[expected_binding.clone()]).await?,
            vec![expected_binding.clone()]
        );

        assert_eq!(
            load_token_lineage(database.pool(), token_lineage_id).await?,
            Some(expected_token_lineage)
        );
        assert_eq!(
            load_resource(database.pool(), resource_id).await?,
            Some(expected_resource)
        );
        assert_eq!(
            load_name_surface(database.pool(), "ens:test.eth").await?,
            Some(expected_surface)
        );
        assert_eq!(
            load_surface_binding(database.pool(), surface_binding_id).await?,
            Some(expected_binding.clone())
        );
        assert_eq!(
            load_surface_bindings_by_logical_name_id(database.pool(), "ens:test.eth").await?,
            vec![expected_binding.clone()]
        );
        assert_eq!(
            load_surface_bindings_by_resource_id(database.pool(), resource_id).await?,
            vec![expected_binding]
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn closes_open_binding_interval_on_rebind_and_preserves_history_continuity() -> Result<()>
    {
        let database = TestDatabase::new().await?;
        let old_token_lineage_id = Uuid::from_u128(0x4000);
        let new_token_lineage_id = Uuid::from_u128(0x5000);
        let old_resource_id = Uuid::from_u128(0x6000);
        let new_resource_id = Uuid::from_u128(0x7000);
        let first_binding_id = Uuid::from_u128(0x8000);
        let second_binding_id = Uuid::from_u128(0x9000);
        let first_start = timestamp(1_717_171_710);
        let rebind_at = timestamp(1_717_171_900);

        upsert_token_lineages(
            database.pool(),
            &[
                token_lineage(
                    old_token_lineage_id,
                    "ens-old",
                    "token_old",
                    201,
                    CanonicalityState::Finalized,
                ),
                token_lineage(
                    new_token_lineage_id,
                    "ens-new",
                    "token_new",
                    202,
                    CanonicalityState::Finalized,
                ),
            ],
        )
        .await?;
        upsert_resources(
            database.pool(),
            &[
                resource(
                    old_resource_id,
                    Some(old_token_lineage_id),
                    "ens-old",
                    "resource_old",
                    203,
                    CanonicalityState::Canonical,
                ),
                resource(
                    new_resource_id,
                    Some(new_token_lineage_id),
                    "ens-new",
                    "resource_new",
                    204,
                    CanonicalityState::Canonical,
                ),
            ],
        )
        .await?;
        upsert_name_surfaces(
            database.pool(),
            &[name_surface(
                "ens:rebind.eth",
                "rebind.eth",
                "rebind.eth",
                "surface_rebind",
                205,
                CanonicalityState::Finalized,
            )],
        )
        .await?;

        let initial_binding = binding(
            first_binding_id,
            "ens:rebind.eth",
            old_resource_id,
            SurfaceBindingKind::DeclaredRegistryPath,
            first_start,
            None,
            "initial_bind",
            "binding_initial",
            206,
            CanonicalityState::Finalized,
        );
        upsert_surface_bindings(database.pool(), &[initial_binding]).await?;

        let closed_binding = binding(
            first_binding_id,
            "ens:rebind.eth",
            old_resource_id,
            SurfaceBindingKind::DeclaredRegistryPath,
            first_start,
            Some(rebind_at),
            "initial_bind",
            "binding_initial",
            206,
            CanonicalityState::Finalized,
        );
        let rebound_binding = binding(
            second_binding_id,
            "ens:rebind.eth",
            new_resource_id,
            SurfaceBindingKind::MigrationRebind,
            rebind_at,
            None,
            "migration_rebind",
            "binding_rebind",
            207,
            CanonicalityState::Safe,
        );
        upsert_surface_bindings(
            database.pool(),
            &[closed_binding.clone(), rebound_binding.clone()],
        )
        .await?;

        let bindings =
            load_surface_bindings_by_logical_name_id(database.pool(), "ens:rebind.eth").await?;
        assert_eq!(
            bindings,
            vec![closed_binding.clone(), rebound_binding.clone()]
        );
        assert_eq!(bindings[0].active_to, Some(bindings[1].active_from));
        assert_ne!(bindings[0].resource_id, bindings[1].resource_id);
        assert_eq!(
            load_surface_binding(database.pool(), first_binding_id).await?,
            Some(closed_binding)
        );
        assert_eq!(
            load_surface_binding(database.pool(), second_binding_id).await?,
            Some(rebound_binding)
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn loads_shared_resource_bindings_for_multiple_surfaces() -> Result<()> {
        let database = TestDatabase::new().await?;
        let token_lineage_id = Uuid::from_u128(0xa000);
        let shared_resource_id = Uuid::from_u128(0xb000);
        let first_binding = binding(
            Uuid::from_u128(0xc000),
            "ens:alpha.eth",
            shared_resource_id,
            SurfaceBindingKind::DeclaredRegistryPath,
            timestamp(1_717_171_720),
            None,
            "alpha_declared",
            "binding_alpha",
            305,
            CanonicalityState::Finalized,
        );
        let second_binding = binding(
            Uuid::from_u128(0xd000),
            "ens:beta.eth",
            shared_resource_id,
            SurfaceBindingKind::LinkedSubregistryPath,
            timestamp(1_717_171_730),
            None,
            "beta_linked",
            "binding_beta",
            306,
            CanonicalityState::Safe,
        );

        upsert_token_lineages(
            database.pool(),
            &[token_lineage(
                token_lineage_id,
                "ens",
                "token_shared",
                301,
                CanonicalityState::Finalized,
            )],
        )
        .await?;
        upsert_resources(
            database.pool(),
            &[resource(
                shared_resource_id,
                Some(token_lineage_id),
                "ens",
                "resource_shared",
                302,
                CanonicalityState::Canonical,
            )],
        )
        .await?;
        upsert_name_surfaces(
            database.pool(),
            &[
                name_surface(
                    "ens:alpha.eth",
                    "alpha.eth",
                    "alpha.eth",
                    "surface_alpha",
                    303,
                    CanonicalityState::Finalized,
                ),
                name_surface(
                    "ens:beta.eth",
                    "beta.eth",
                    "beta.eth",
                    "surface_beta",
                    304,
                    CanonicalityState::Finalized,
                ),
            ],
        )
        .await?;
        upsert_surface_bindings(
            database.pool(),
            &[first_binding.clone(), second_binding.clone()],
        )
        .await?;

        assert_eq!(
            load_surface_bindings_by_logical_name_id(database.pool(), "ens:alpha.eth").await?,
            vec![first_binding.clone()]
        );
        assert_eq!(
            load_surface_bindings_by_logical_name_id(database.pool(), "ens:beta.eth").await?,
            vec![second_binding.clone()]
        );
        assert_eq!(
            load_surface_bindings_by_resource_id(database.pool(), shared_resource_id).await?,
            vec![first_binding, second_binding]
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rejects_placeholder_anchor_defaults_in_identity_rows() -> Result<()> {
        let database = TestDatabase::new().await?;
        let error = upsert_token_lineages(
            database.pool(),
            &[TokenLineage {
                token_lineage_id: Uuid::from_u128(0xe000),
                chain_id: "unknown".to_owned(),
                block_hash: "unknown".to_owned(),
                block_number: 0,
                provenance: json!({"source": "bad_anchor"}),
                canonicality_state: CanonicalityState::Observed,
            }],
        )
        .await
        .expect_err("placeholder migration defaults must be rejected");

        assert!(
            error
                .to_string()
                .contains("must provide a real chain_id anchor"),
            "unexpected error: {error:#}"
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rejects_overlapping_or_duplicate_current_bindings_for_one_logical_name_id()
    -> Result<()> {
        let database = TestDatabase::new().await?;
        let first_resource_id = Uuid::from_u128(0xe100);
        let second_resource_id = Uuid::from_u128(0xe101);

        upsert_token_lineages(
            database.pool(),
            &[
                token_lineage(
                    Uuid::from_u128(0xe102),
                    "ens",
                    "token_overlap_1",
                    401,
                    CanonicalityState::Finalized,
                ),
                token_lineage(
                    Uuid::from_u128(0xe103),
                    "ens",
                    "token_overlap_2",
                    402,
                    CanonicalityState::Finalized,
                ),
            ],
        )
        .await?;
        upsert_resources(
            database.pool(),
            &[
                resource(
                    first_resource_id,
                    Some(Uuid::from_u128(0xe102)),
                    "ens",
                    "resource_overlap_1",
                    403,
                    CanonicalityState::Canonical,
                ),
                resource(
                    second_resource_id,
                    Some(Uuid::from_u128(0xe103)),
                    "ens",
                    "resource_overlap_2",
                    404,
                    CanonicalityState::Canonical,
                ),
            ],
        )
        .await?;
        upsert_name_surfaces(
            database.pool(),
            &[name_surface(
                "ens:overlap.eth",
                "overlap.eth",
                "overlap.eth",
                "surface_overlap",
                405,
                CanonicalityState::Finalized,
            )],
        )
        .await?;

        upsert_surface_bindings(
            database.pool(),
            &[binding(
                Uuid::from_u128(0xe104),
                "ens:overlap.eth",
                first_resource_id,
                SurfaceBindingKind::DeclaredRegistryPath,
                timestamp(1_717_172_000),
                None,
                "current_1",
                "binding_overlap_1",
                406,
                CanonicalityState::Finalized,
            )],
        )
        .await?;

        let error = upsert_surface_bindings(
            database.pool(),
            &[binding(
                Uuid::from_u128(0xe105),
                "ens:overlap.eth",
                second_resource_id,
                SurfaceBindingKind::MigrationRebind,
                timestamp(1_717_172_100),
                None,
                "current_2",
                "binding_overlap_2",
                407,
                CanonicalityState::Finalized,
            )],
        )
        .await
        .expect_err("overlapping current bindings must be rejected");

        let error_chain = format!("{error:#}");
        assert!(
            error_chain.contains("surface_bindings_no_overlap"),
            "unexpected error: {error:#}"
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn orphaned_binding_can_coexist_with_overlapping_replacement_after_repair() -> Result<()>
    {
        let database = TestDatabase::new().await?;
        let chain_id = "chain:binding_reorg";
        let parent_hash = "0xparent_binding_reorg";
        let losing_hash = "0xlosing_binding_reorg";
        let replacement_hash = "0xreplacement_binding_reorg";
        let active_from = timestamp(1_717_172_400);
        let old_binding_id = Uuid::from_u128(0xe110);
        let replacement_binding_id = Uuid::from_u128(0xe111);
        let old_resource_id = Uuid::from_u128(0xe112);
        let replacement_resource_id = Uuid::from_u128(0xe113);
        let old_token_lineage_id = Uuid::from_u128(0xe114);
        let replacement_token_lineage_id = Uuid::from_u128(0xe115);

        upsert_chain_lineage_blocks(
            database.pool(),
            &[
                lineage_block(
                    chain_id,
                    parent_hash,
                    Some("0xgenesis_binding_reorg"),
                    9,
                    timestamp(1_717_172_390),
                    CanonicalityState::Finalized,
                ),
                lineage_block(
                    chain_id,
                    losing_hash,
                    Some(parent_hash),
                    10,
                    timestamp(1_717_172_395),
                    CanonicalityState::Finalized,
                ),
                lineage_block(
                    chain_id,
                    replacement_hash,
                    Some(parent_hash),
                    10,
                    timestamp(1_717_172_396),
                    CanonicalityState::Finalized,
                ),
            ],
        )
        .await?;

        upsert_token_lineages(
            database.pool(),
            &[
                TokenLineage {
                    token_lineage_id: old_token_lineage_id,
                    chain_id: chain_id.to_owned(),
                    block_hash: losing_hash.to_owned(),
                    block_number: 10,
                    provenance: json!({"source": "losing_branch"}),
                    canonicality_state: CanonicalityState::Finalized,
                },
                TokenLineage {
                    token_lineage_id: replacement_token_lineage_id,
                    chain_id: chain_id.to_owned(),
                    block_hash: replacement_hash.to_owned(),
                    block_number: 10,
                    provenance: json!({"source": "replacement_branch"}),
                    canonicality_state: CanonicalityState::Finalized,
                },
            ],
        )
        .await?;
        upsert_resources(
            database.pool(),
            &[
                Resource {
                    resource_id: old_resource_id,
                    token_lineage_id: Some(old_token_lineage_id),
                    chain_id: chain_id.to_owned(),
                    block_hash: losing_hash.to_owned(),
                    block_number: 10,
                    provenance: json!({"source": "losing_branch"}),
                    canonicality_state: CanonicalityState::Finalized,
                },
                Resource {
                    resource_id: replacement_resource_id,
                    token_lineage_id: Some(replacement_token_lineage_id),
                    chain_id: chain_id.to_owned(),
                    block_hash: replacement_hash.to_owned(),
                    block_number: 10,
                    provenance: json!({"source": "replacement_branch"}),
                    canonicality_state: CanonicalityState::Finalized,
                },
            ],
        )
        .await?;
        upsert_name_surfaces(
            database.pool(),
            &[NameSurface {
                logical_name_id: "ens:repair.eth".to_owned(),
                namespace: "ens".to_owned(),
                input_name: "repair.eth".to_owned(),
                canonical_display_name: "repair.eth".to_owned(),
                normalized_name: "repair.eth".to_owned(),
                dns_encoded_name: vec![
                    6, b'r', b'e', b'p', b'a', b'i', b'r', 3, b'e', b't', b'h', 0,
                ],
                namehash: "namehash:repair.eth".to_owned(),
                labelhashes: vec!["labelhash:repair.eth".to_owned()],
                normalizer_version: "ensip15@2026-04-16".to_owned(),
                normalization_warnings: json!([]),
                normalization_errors: json!([]),
                chain_id: chain_id.to_owned(),
                block_hash: parent_hash.to_owned(),
                block_number: 9,
                provenance: json!({"source": "surface_branch"}),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;

        let old_binding = SurfaceBinding {
            surface_binding_id: old_binding_id,
            logical_name_id: "ens:repair.eth".to_owned(),
            resource_id: old_resource_id,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            active_from,
            active_to: None,
            chain_id: chain_id.to_owned(),
            block_hash: losing_hash.to_owned(),
            block_number: 10,
            provenance: json!({"source": "losing_binding"}),
            canonicality_state: CanonicalityState::Finalized,
        };
        upsert_surface_bindings(database.pool(), &[old_binding.clone()]).await?;

        let orphaned_count = mark_surface_binding_range_orphaned(
            database.pool(),
            chain_id,
            losing_hash,
            Some(parent_hash),
        )
        .await?;
        assert_eq!(orphaned_count, 1);
        assert_eq!(
            load_token_lineage(database.pool(), old_token_lineage_id).await?,
            Some(TokenLineage {
                token_lineage_id: old_token_lineage_id,
                chain_id: chain_id.to_owned(),
                block_hash: losing_hash.to_owned(),
                block_number: 10,
                provenance: json!({"source": "losing_branch"}),
                canonicality_state: CanonicalityState::Finalized,
            })
        );
        assert_eq!(
            load_resource(database.pool(), old_resource_id).await?,
            Some(Resource {
                resource_id: old_resource_id,
                token_lineage_id: Some(old_token_lineage_id),
                chain_id: chain_id.to_owned(),
                block_hash: losing_hash.to_owned(),
                block_number: 10,
                provenance: json!({"source": "losing_branch"}),
                canonicality_state: CanonicalityState::Finalized,
            })
        );
        assert_eq!(
            load_name_surface(database.pool(), "ens:repair.eth").await?,
            Some(NameSurface {
                logical_name_id: "ens:repair.eth".to_owned(),
                namespace: "ens".to_owned(),
                input_name: "repair.eth".to_owned(),
                canonical_display_name: "repair.eth".to_owned(),
                normalized_name: "repair.eth".to_owned(),
                dns_encoded_name: vec![
                    6, b'r', b'e', b'p', b'a', b'i', b'r', 3, b'e', b't', b'h', 0,
                ],
                namehash: "namehash:repair.eth".to_owned(),
                labelhashes: vec!["labelhash:repair.eth".to_owned()],
                normalizer_version: "ensip15@2026-04-16".to_owned(),
                normalization_warnings: json!([]),
                normalization_errors: json!([]),
                chain_id: chain_id.to_owned(),
                block_hash: parent_hash.to_owned(),
                block_number: 9,
                provenance: json!({"source": "surface_branch"}),
                canonicality_state: CanonicalityState::Finalized,
            })
        );

        let replacement_binding = SurfaceBinding {
            surface_binding_id: replacement_binding_id,
            logical_name_id: "ens:repair.eth".to_owned(),
            resource_id: replacement_resource_id,
            binding_kind: SurfaceBindingKind::MigrationRebind,
            active_from,
            active_to: None,
            chain_id: chain_id.to_owned(),
            block_hash: replacement_hash.to_owned(),
            block_number: 10,
            provenance: json!({"source": "replacement_binding"}),
            canonicality_state: CanonicalityState::Finalized,
        };
        upsert_surface_bindings(database.pool(), &[replacement_binding.clone()]).await?;

        let orphaned_binding =
            load_surface_binding_including_noncanonical(database.pool(), old_binding_id)
                .await?
                .expect("orphaned binding should remain accessible via history path");
        assert_eq!(
            orphaned_binding.canonicality_state,
            CanonicalityState::Orphaned
        );
        assert_eq!(
            load_surface_binding(database.pool(), old_binding_id).await?,
            None
        );
        assert_eq!(
            load_surface_bindings_by_logical_name_id(database.pool(), "ens:repair.eth").await?,
            vec![replacement_binding.clone()]
        );
        assert_eq!(
            load_surface_bindings_by_logical_name_id_including_noncanonical(
                database.pool(),
                "ens:repair.eth",
            )
            .await?,
            vec![orphaned_binding, replacement_binding.clone()]
        );
        assert_eq!(
            load_surface_binding(database.pool(), replacement_binding_id).await?,
            Some(replacement_binding)
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn orphaned_stable_identity_rows_can_be_reobserved_with_same_ids_on_winning_branch()
    -> Result<()> {
        let database = TestDatabase::new().await?;
        let chain_id = "chain:stable_identity_reorg";
        let parent_hash = "0xparent_stable_identity_reorg";
        let losing_hash = "0xlosing_stable_identity_reorg";
        let winning_hash = "0xwinning_stable_identity_reorg";
        let token_lineage_id = Uuid::from_u128(0xe120);
        let resource_id = Uuid::from_u128(0xe121);

        upsert_chain_lineage_blocks(
            database.pool(),
            &[
                lineage_block(
                    chain_id,
                    parent_hash,
                    Some("0xgenesis_stable_identity_reorg"),
                    20,
                    timestamp(1_717_172_500),
                    CanonicalityState::Finalized,
                ),
                lineage_block(
                    chain_id,
                    losing_hash,
                    Some(parent_hash),
                    21,
                    timestamp(1_717_172_510),
                    CanonicalityState::Finalized,
                ),
                lineage_block(
                    chain_id,
                    winning_hash,
                    Some(parent_hash),
                    21,
                    timestamp(1_717_172_511),
                    CanonicalityState::Finalized,
                ),
            ],
        )
        .await?;

        upsert_token_lineages(
            database.pool(),
            &[TokenLineage {
                token_lineage_id,
                chain_id: chain_id.to_owned(),
                block_hash: losing_hash.to_owned(),
                block_number: 21,
                provenance: json!({"source": "losing_token"}),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;
        upsert_resources(
            database.pool(),
            &[Resource {
                resource_id,
                token_lineage_id: Some(token_lineage_id),
                chain_id: chain_id.to_owned(),
                block_hash: losing_hash.to_owned(),
                block_number: 21,
                provenance: json!({"source": "losing_resource"}),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;
        upsert_name_surfaces(
            database.pool(),
            &[NameSurface {
                logical_name_id: "ens:stable.eth".to_owned(),
                namespace: "ens".to_owned(),
                input_name: "stable.eth".to_owned(),
                canonical_display_name: "stable.eth".to_owned(),
                normalized_name: "stable.eth".to_owned(),
                dns_encoded_name: vec![
                    6, b's', b't', b'a', b'b', b'l', b'e', 3, b'e', b't', b'h', 0,
                ],
                namehash: "namehash:stable.eth".to_owned(),
                labelhashes: vec!["labelhash:stable.eth".to_owned()],
                normalizer_version: "ensip15@2026-04-16".to_owned(),
                normalization_warnings: json!([]),
                normalization_errors: json!([]),
                chain_id: chain_id.to_owned(),
                block_hash: losing_hash.to_owned(),
                block_number: 21,
                provenance: json!({"source": "losing_surface"}),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;

        let orphan_counts = mark_identity_rows_range_orphaned(
            database.pool(),
            chain_id,
            losing_hash,
            Some(parent_hash),
        )
        .await?;
        assert_eq!(
            orphan_counts,
            IdentityOrphanCounts {
                token_lineage_count: 1,
                resource_count: 1,
                name_surface_count: 1,
                surface_binding_count: 0,
            }
        );

        let winning_token_lineage = TokenLineage {
            token_lineage_id,
            chain_id: chain_id.to_owned(),
            block_hash: winning_hash.to_owned(),
            block_number: 21,
            provenance: json!({"source": "winning_token"}),
            canonicality_state: CanonicalityState::Finalized,
        };
        let winning_resource = Resource {
            resource_id,
            token_lineage_id: Some(token_lineage_id),
            chain_id: chain_id.to_owned(),
            block_hash: winning_hash.to_owned(),
            block_number: 21,
            provenance: json!({"source": "winning_resource"}),
            canonicality_state: CanonicalityState::Finalized,
        };
        let winning_surface = NameSurface {
            logical_name_id: "ens:stable.eth".to_owned(),
            namespace: "ens".to_owned(),
            input_name: "stable.eth".to_owned(),
            canonical_display_name: "stable.eth".to_owned(),
            normalized_name: "stable.eth".to_owned(),
            dns_encoded_name: vec![
                6, b's', b't', b'a', b'b', b'l', b'e', 3, b'e', b't', b'h', 0,
            ],
            namehash: "namehash:stable.eth".to_owned(),
            labelhashes: vec!["labelhash:stable.eth".to_owned()],
            normalizer_version: "ensip15@2026-04-16".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: chain_id.to_owned(),
            block_hash: winning_hash.to_owned(),
            block_number: 21,
            provenance: json!({"source": "winning_surface"}),
            canonicality_state: CanonicalityState::Finalized,
        };

        upsert_token_lineages(database.pool(), &[winning_token_lineage.clone()]).await?;
        upsert_resources(database.pool(), &[winning_resource.clone()]).await?;
        upsert_name_surfaces(database.pool(), &[winning_surface.clone()]).await?;

        assert_eq!(
            load_token_lineage(database.pool(), token_lineage_id).await?,
            Some(winning_token_lineage.clone())
        );
        assert_eq!(
            load_resource(database.pool(), resource_id).await?,
            Some(winning_resource.clone())
        );
        assert_eq!(
            load_name_surface(database.pool(), "ens:stable.eth").await?,
            Some(winning_surface.clone())
        );
        assert_eq!(
            load_token_lineage_including_noncanonical(database.pool(), token_lineage_id).await?,
            Some(winning_token_lineage)
        );
        assert_eq!(
            load_resource_including_noncanonical(database.pool(), resource_id).await?,
            Some(winning_resource)
        );
        assert_eq!(
            load_name_surface_including_noncanonical(database.pool(), "ens:stable.eth").await?,
            Some(winning_surface)
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn canonical_only_default_reads_exclude_observed_and_orphaned() -> Result<()> {
        let database = TestDatabase::new().await?;
        let token_lineage_id = Uuid::from_u128(0xe200);
        let resource_id = Uuid::from_u128(0xe201);
        let surface_binding_id = Uuid::from_u128(0xe202);

        let observed_token_lineage = token_lineage(
            token_lineage_id,
            "ens",
            "token_observed",
            501,
            CanonicalityState::Observed,
        );
        let observed_resource = resource(
            resource_id,
            Some(token_lineage_id),
            "ens",
            "resource_observed",
            502,
            CanonicalityState::Observed,
        );
        let orphaned_surface = name_surface(
            "ens:hidden.eth",
            "hidden.eth",
            "hidden.eth",
            "surface_orphaned",
            503,
            CanonicalityState::Orphaned,
        );
        let observed_binding = binding(
            surface_binding_id,
            "ens:hidden.eth",
            resource_id,
            SurfaceBindingKind::ObservedOnly,
            timestamp(1_717_172_200),
            None,
            "observed_only",
            "binding_observed",
            504,
            CanonicalityState::Observed,
        );

        upsert_token_lineages(database.pool(), &[observed_token_lineage.clone()]).await?;
        upsert_resources(database.pool(), &[observed_resource.clone()]).await?;
        upsert_name_surfaces(database.pool(), &[orphaned_surface.clone()]).await?;
        upsert_surface_bindings(database.pool(), &[observed_binding.clone()]).await?;

        assert_eq!(
            load_token_lineage(database.pool(), token_lineage_id).await?,
            None
        );
        assert_eq!(load_resource(database.pool(), resource_id).await?, None);
        assert_eq!(
            load_name_surface(database.pool(), "ens:hidden.eth").await?,
            None
        );
        assert_eq!(
            load_surface_binding(database.pool(), surface_binding_id).await?,
            None
        );
        assert!(
            load_surface_bindings_by_logical_name_id(database.pool(), "ens:hidden.eth")
                .await?
                .is_empty()
        );
        assert!(
            load_surface_bindings_by_resource_id(database.pool(), resource_id)
                .await?
                .is_empty()
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn explicit_noncanonical_opt_in_reads_include_observed_and_orphaned_history() -> Result<()>
    {
        let database = TestDatabase::new().await?;
        let token_lineage_id = Uuid::from_u128(0xe300);
        let resource_id = Uuid::from_u128(0xe301);
        let surface_binding_id = Uuid::from_u128(0xe302);

        let observed_token_lineage = token_lineage(
            token_lineage_id,
            "ens",
            "token_history",
            601,
            CanonicalityState::Observed,
        );
        let orphaned_resource = resource(
            resource_id,
            Some(token_lineage_id),
            "ens",
            "resource_history",
            602,
            CanonicalityState::Orphaned,
        );
        let observed_surface = name_surface(
            "ens:history.eth",
            "history.eth",
            "history.eth",
            "surface_history",
            603,
            CanonicalityState::Observed,
        );
        let orphaned_binding = binding(
            surface_binding_id,
            "ens:history.eth",
            resource_id,
            SurfaceBindingKind::ObservedOnly,
            timestamp(1_717_172_300),
            None,
            "observed_history",
            "binding_history",
            604,
            CanonicalityState::Orphaned,
        );

        upsert_token_lineages(database.pool(), &[observed_token_lineage.clone()]).await?;
        upsert_resources(database.pool(), &[orphaned_resource.clone()]).await?;
        upsert_name_surfaces(database.pool(), &[observed_surface.clone()]).await?;
        upsert_surface_bindings(database.pool(), &[orphaned_binding.clone()]).await?;

        assert_eq!(
            load_token_lineage_including_noncanonical(database.pool(), token_lineage_id).await?,
            Some(observed_token_lineage)
        );
        assert_eq!(
            load_resource_including_noncanonical(database.pool(), resource_id).await?,
            Some(orphaned_resource)
        );
        assert_eq!(
            load_name_surface_including_noncanonical(database.pool(), "ens:history.eth").await?,
            Some(observed_surface)
        );
        assert_eq!(
            load_surface_binding_including_noncanonical(database.pool(), surface_binding_id)
                .await?,
            Some(orphaned_binding.clone())
        );
        assert_eq!(
            load_surface_bindings_by_logical_name_id_including_noncanonical(
                database.pool(),
                "ens:history.eth",
            )
            .await?,
            vec![orphaned_binding.clone()]
        );
        assert_eq!(
            load_surface_bindings_by_resource_id_including_noncanonical(
                database.pool(),
                resource_id,
            )
            .await?,
            vec![orphaned_binding]
        );

        database.cleanup().await
    }
}
