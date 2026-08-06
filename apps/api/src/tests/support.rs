use std::{
    str::FromStr,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::Context;
use axum::{
    body::{Body, to_bytes},
    http::Request,
    response::Response,
};
use bigname_storage::{
    CanonicalityState, ExecutionCacheKey, ExecutionOutcome, ExecutionTrace, ExecutionTraceStep,
    NameSurface, NormalizedEvent, PermissionScope, PermissionsCurrentRow, PrimaryNameClaimStatus,
    PrimaryNameCurrentRow, PrimaryNameCurrentSnapshot, RawBlock, ResolverCurrentRow, Resource,
    SurfaceBinding, SurfaceBindingKind, TokenLineage, default_database_url,
    load_primary_name_current, parse_rfc3339_utc_timestamp, upsert_execution_outcome,
    upsert_execution_trace, upsert_primary_name_current_rows,
    upsert_primary_name_current_snapshots,
};
use bigname_test_support::TestDatabaseConfig;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sqlx::{
    ConnectOptions, PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
    raw_sql,
    types::{Uuid, time::OffsetDateTime},
};
use tower::ServiceExt;

use super::*;

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);
static WORKER_CARGO_LOCK: Mutex<()> = Mutex::new(());

struct TestDatabase {
    database: bigname_test_support::TestDatabase,
    pool: PgPool,
    lookup_pool: PgPool,
    database_name: String,
}


impl TestDatabase {
    async fn new(initialize_manifest_schema: bool) -> Result<Self> {
        Self::new_with_schemas(initialize_manifest_schema, false).await
    }

    async fn new_with_schemas(
        initialize_manifest_schema: bool,
        initialize_name_current_schema: bool,
    ) -> Result<Self> {
        let database = bigname_test_support::TestDatabase::create(
            TestDatabaseConfig::new("bigname_api_test")
                .admin_database_from_url()
                .pool_max_connections(1)
                .parse_context("failed to parse database URL for API tests")
                .admin_connect_context("failed to connect admin pool for API tests")
                .pool_connect_context("failed to connect API test pool"),
        )
        .await?;
        let pool = database.pool().clone();
        let database_name = database.database_name().to_owned();

        if initialize_manifest_schema {
            sqlx::query(
                r#"
                    CREATE TYPE manifest_rollout_status AS ENUM (
                        'draft',
                        'shadow',
                        'active',
                        'deprecated'
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create manifest_rollout_status for API tests")?;
            sqlx::query(
                r#"
                    CREATE TYPE capability_support_status AS ENUM (
                        'unsupported',
                        'shadow',
                        'supported'
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create capability_support_status for API tests")?;
            sqlx::query(
                r#"
                    CREATE TABLE manifest_versions (
                        manifest_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        manifest_version BIGINT NOT NULL CHECK (manifest_version > 0),
                        namespace TEXT NOT NULL,
                        source_family TEXT NOT NULL,
                        chain TEXT NOT NULL,
                        deployment_epoch TEXT NOT NULL,
                        rollout_status manifest_rollout_status NOT NULL,
                        normalizer_version TEXT NOT NULL,
                        file_path TEXT NOT NULL,
                        manifest_payload JSONB NOT NULL,
                        loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create manifest_versions for API tests")?;
            sqlx::query(
                    r#"
                    CREATE TABLE manifest_capability_flags (
                        manifest_id BIGINT NOT NULL REFERENCES manifest_versions (manifest_id) ON DELETE CASCADE,
                        capability_name TEXT NOT NULL,
                        status capability_support_status NOT NULL,
                        notes TEXT,
                        PRIMARY KEY (manifest_id, capability_name)
                    )
                    "#,
                )
                .execute(&pool)
                .await
                .context("failed to create manifest_capability_flags for API tests")?;
        }

        if initialize_name_current_schema {
            sqlx::query(
                r#"
                    CREATE TYPE canonicality_state AS ENUM (
                        'observed',
                        'canonical',
                        'safe',
                        'finalized',
                        'orphaned'
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create canonicality_state for API tests")?;
            sqlx::query(
                r#"
                    CREATE TABLE chain_checkpoints (
                        chain_id TEXT PRIMARY KEY,
                        canonical_block_hash TEXT,
                        canonical_block_number BIGINT,
                        safe_block_hash TEXT,
                        safe_block_number BIGINT,
                        finalized_block_hash TEXT,
                        finalized_block_number BIGINT,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        CHECK ((canonical_block_hash IS NULL) = (canonical_block_number IS NULL)),
                        CHECK ((safe_block_hash IS NULL) = (safe_block_number IS NULL)),
                        CHECK ((finalized_block_hash IS NULL) = (finalized_block_number IS NULL))
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create chain_checkpoints for API tests")?;
            sqlx::query(
                r#"
                    CREATE TABLE chain_lineage (
                        chain_id TEXT NOT NULL,
                        block_hash TEXT NOT NULL,
                        parent_hash TEXT,
                        block_number BIGINT NOT NULL CHECK (block_number >= 0),
                        block_timestamp TIMESTAMPTZ NOT NULL,
                        canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
                        observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        PRIMARY KEY (chain_id, block_hash)
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create chain_lineage for API tests")?;
            sqlx::query(
                r#"
                    CREATE TABLE chain_header_audit (
                        chain_id TEXT NOT NULL,
                        block_hash TEXT NOT NULL,
                        logs_bloom BYTEA,
                        transactions_root TEXT,
                        receipts_root TEXT,
                        state_root TEXT,
                        observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        PRIMARY KEY (chain_id, block_hash),
                        FOREIGN KEY (chain_id, block_hash)
                            REFERENCES chain_lineage (chain_id, block_hash)
                            ON DELETE CASCADE,
                        CHECK (
                            logs_bloom IS NOT NULL
                            OR transactions_root IS NOT NULL
                            OR receipts_root IS NOT NULL
                            OR state_root IS NOT NULL
                        )
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create chain_header_audit for API tests")?;
            sqlx::query(
                r#"
                    CREATE TABLE name_surfaces (
                        logical_name_id TEXT PRIMARY KEY,
                        namespace TEXT NOT NULL,
                        canonical_display_name TEXT NOT NULL,
                        normalized_name TEXT NOT NULL,
                        namehash TEXT NOT NULL,
                        chain_id TEXT NOT NULL DEFAULT 'ethereum-mainnet',
                        block_hash TEXT NOT NULL DEFAULT '0xsurface',
                        block_number BIGINT NOT NULL DEFAULT 20999998,
                        canonicality_state canonicality_state NOT NULL DEFAULT 'finalized',
                        CHECK (logical_name_id = namespace || ':' || normalized_name)
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create name_surfaces for API tests")?;
            sqlx::query(
                r#"
                    CREATE TABLE resources (
                        resource_id UUID PRIMARY KEY,
                        chain_id TEXT NOT NULL DEFAULT 'ethereum-mainnet',
                        block_hash TEXT NOT NULL DEFAULT '0xresource',
                        block_number BIGINT NOT NULL DEFAULT 21000001,
                        canonicality_state canonicality_state NOT NULL DEFAULT 'finalized'
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create resources for API tests")?;
            sqlx::query(
                r#"
                    CREATE TABLE token_lineages (
                        token_lineage_id UUID PRIMARY KEY,
                        chain_id TEXT NOT NULL DEFAULT 'ethereum-mainnet',
                        block_hash TEXT NOT NULL DEFAULT '0xlineage',
                        block_number BIGINT NOT NULL DEFAULT 21000000,
                        canonicality_state canonicality_state NOT NULL DEFAULT 'finalized'
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create token_lineages for API tests")?;
            sqlx::query(
                r#"
                    CREATE TABLE surface_bindings (
                        surface_binding_id UUID PRIMARY KEY,
                        logical_name_id TEXT NOT NULL REFERENCES name_surfaces (logical_name_id),
                        resource_id UUID NOT NULL REFERENCES resources (resource_id),
                        binding_kind TEXT NOT NULL,
                        active_to TIMESTAMPTZ,
                        chain_id TEXT NOT NULL DEFAULT 'ethereum-mainnet',
                        block_hash TEXT NOT NULL DEFAULT '0xbinding',
                        block_number BIGINT NOT NULL DEFAULT 21000003,
                        canonicality_state canonicality_state NOT NULL DEFAULT 'finalized',
                        CHECK (
                            binding_kind IN (
                                'declared_registry_path',
                                'linked_subregistry_path',
                                'resolver_alias_path',
                                'observed_wildcard_path',
                                'migration_rebind',
                                'observed_only'
                            )
                        )
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create surface_bindings for API tests")?;
            sqlx::query(
                r#"
                    CREATE TABLE name_current (
                        logical_name_id TEXT PRIMARY KEY REFERENCES name_surfaces (logical_name_id),
                        namespace TEXT NOT NULL,
                        canonical_display_name TEXT NOT NULL,
                        normalized_name TEXT NOT NULL,
                        namehash TEXT NOT NULL,
                        surface_binding_id UUID REFERENCES surface_bindings (surface_binding_id),
                        resource_id UUID REFERENCES resources (resource_id),
                        token_lineage_id UUID REFERENCES token_lineages (token_lineage_id),
                        binding_kind TEXT,
                        declared_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
                        provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
                        coverage JSONB NOT NULL DEFAULT '{}'::jsonb,
                        chain_positions JSONB NOT NULL DEFAULT '{}'::jsonb,
                        canonicality_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
                        manifest_version BIGINT NOT NULL CHECK (manifest_version > 0),
                        last_recomputed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        CHECK (logical_name_id = namespace || ':' || normalized_name),
                        CHECK (
                            (surface_binding_id IS NULL AND resource_id IS NULL AND binding_kind IS NULL)
                            OR
                            (surface_binding_id IS NOT NULL AND resource_id IS NOT NULL AND binding_kind IS NOT NULL)
                        ),
                        CHECK (
                            token_lineage_id IS NULL
                            OR resource_id IS NOT NULL
                        ),
                        CHECK (
                            binding_kind IS NULL
                            OR binding_kind IN (
                                'declared_registry_path',
                                'linked_subregistry_path',
                                'resolver_alias_path',
                                'observed_wildcard_path',
                                'migration_rebind',
                                'observed_only'
                            )
                        )
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create name_current for API tests")?;
            sqlx::query(
                r#"
                    CREATE TABLE record_inventory_current (
                        resource_id UUID NOT NULL REFERENCES resources (resource_id),
                        record_version_boundary_key TEXT NOT NULL,
                        record_version_boundary JSONB NOT NULL DEFAULT '{}'::jsonb,
                        enumeration_basis JSONB NOT NULL DEFAULT '{}'::jsonb,
                        selectors JSONB NOT NULL DEFAULT '[]'::jsonb,
                        explicit_gaps JSONB NOT NULL DEFAULT '[]'::jsonb,
                        unsupported_families JSONB NOT NULL DEFAULT '[]'::jsonb,
                        last_change JSONB,
                        entries JSONB NOT NULL DEFAULT '[]'::jsonb,
                        provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
                        coverage JSONB NOT NULL DEFAULT '{}'::jsonb,
                        chain_positions JSONB NOT NULL DEFAULT '{}'::jsonb,
                        canonicality_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
                        manifest_version BIGINT NOT NULL CHECK (manifest_version > 0),
                        last_recomputed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        PRIMARY KEY (resource_id, record_version_boundary_key),
                        CHECK (record_version_boundary_key <> '')
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create record_inventory_current for API tests")?;
            sqlx::query(
                r#"
                    CREATE TABLE execution_traces (
                        execution_trace_id UUID PRIMARY KEY,
                        request_type TEXT NOT NULL,
                        request_key TEXT NOT NULL,
                        namespace TEXT NOT NULL,
                        chain_context JSONB NOT NULL DEFAULT '{}'::jsonb,
                        manifest_context JSONB NOT NULL DEFAULT '{}'::jsonb,
                        contracts_called JSONB NOT NULL DEFAULT '[]'::jsonb,
                        gateway_digests JSONB NOT NULL DEFAULT '[]'::jsonb,
                        final_payload JSONB,
                        failure_payload JSONB,
                        request_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                        finished_at TIMESTAMPTZ NOT NULL,
                        inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        CHECK (jsonb_typeof(chain_context) = 'object' AND chain_context <> '{}'::jsonb),
                        CHECK (
                            jsonb_typeof(manifest_context) = 'object'
                            AND manifest_context <> '{}'::jsonb
                        ),
                        CHECK (jsonb_typeof(contracts_called) = 'array'),
                        CHECK (jsonb_typeof(gateway_digests) = 'array'),
                        CHECK (jsonb_typeof(request_metadata) = 'object'),
                        CHECK (final_payload IS NOT NULL OR failure_payload IS NOT NULL)
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create execution_traces for API tests")?;
            sqlx::query(
                r#"
                    CREATE TABLE execution_steps (
                        execution_trace_id UUID NOT NULL REFERENCES execution_traces (execution_trace_id) ON DELETE CASCADE,
                        step_index BIGINT NOT NULL CHECK (step_index >= 0),
                        step_kind TEXT NOT NULL,
                        input_digest TEXT,
                        output_digest TEXT,
                        latency_ms BIGINT CHECK (latency_ms IS NULL OR latency_ms >= 0),
                        canonicality_dependency JSONB NOT NULL DEFAULT '{}'::jsonb,
                        step_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                        inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        PRIMARY KEY (execution_trace_id, step_index),
                        CHECK (
                            jsonb_typeof(canonicality_dependency) = 'object'
                            AND canonicality_dependency <> '{}'::jsonb
                        ),
                        CHECK (jsonb_typeof(step_payload) = 'object')
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create execution_steps for API tests")?;
            sqlx::query(
                r#"
                    CREATE TABLE execution_cache_outcomes (
                        execution_cache_key TEXT PRIMARY KEY,
                        request_key TEXT NOT NULL,
                        requested_chain_positions JSONB NOT NULL DEFAULT '[]'::jsonb,
                        manifest_versions JSONB NOT NULL DEFAULT '[]'::jsonb,
                        topology_version_boundary JSONB NOT NULL DEFAULT '{}'::jsonb,
                        record_version_boundary JSONB NOT NULL DEFAULT '{}'::jsonb,
                        execution_trace_id UUID NOT NULL REFERENCES execution_traces (execution_trace_id) ON DELETE CASCADE,
                        request_type TEXT NOT NULL,
                        namespace TEXT NOT NULL,
                        outcome_payload JSONB,
                        failure_payload JSONB,
                        finished_at TIMESTAMPTZ NOT NULL,
                        inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        CHECK (request_key <> ''),
                        CHECK (
                            jsonb_typeof(requested_chain_positions) = 'array'
                            AND requested_chain_positions <> '[]'::jsonb
                        ),
                        CHECK (
                            jsonb_typeof(manifest_versions) = 'array'
                            AND manifest_versions <> '[]'::jsonb
                        ),
                        CHECK (
                            jsonb_typeof(topology_version_boundary) = 'object'
                            AND topology_version_boundary <> '{}'::jsonb
                        ),
                        CHECK (
                            jsonb_typeof(record_version_boundary) = 'object'
                            AND record_version_boundary <> '{}'::jsonb
                        ),
                        CHECK (outcome_payload IS NOT NULL OR failure_payload IS NOT NULL)
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create execution_cache_outcomes for API tests")?;
        }

        let mut database = Self {
            database,
            lookup_pool: pool.clone(),
            pool,
            database_name,
        };
        if initialize_name_current_schema {
            database.initialize_lookup_schema().await?;
            database.lookup_pool = database.open_lookup_pool().await?;
        }
        Ok(database)
    }

    async fn new_migrated() -> Result<Self> {
        let mut database = Self::new(false).await?;
        database
            .database
            .apply_migrations(
                &bigname_storage::MIGRATOR,
                "failed to apply checked-in migrations for API tests",
            )
            .await?;
        database.initialize_lookup_schema().await?;
        database.lookup_pool = database.open_lookup_pool().await?;
        Ok(database)
    }

    async fn initialize_lookup_schema(&self) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("CREATE SCHEMA IF NOT EXISTS bigname_phase")
            .execute(&mut *transaction)
            .await?;
        sqlx::query("SET LOCAL search_path TO bigname_phase, public")
            .execute(&mut *transaction)
            .await?;
        for script in [
            include_str!("../../../../schema-v2/baseline/01_chain.sql"),
            include_str!("../../../../schema-v2/baseline/02_raw_facts.sql"),
            include_str!("../../../../schema-v2/baseline/03_identity.sql"),
            include_str!("../../../../schema-v2/baseline/04_manifests.sql"),
            include_str!("../../../../schema-v2/baseline/05_normalized_events.sql"),
            include_str!("../../../../schema-v2/baseline/06_projections.sql"),
            include_str!("../../../../schema-v2/baseline/07_labels.sql"),
            include_str!("../../../../schema-v2/baseline/08_heartbeats.sql"),
            include_str!("../../../../schema-v2/baseline/09_divergence.sql"),
            include_str!("../../../../schema-v2/baseline/10_phase_state.sql"),
        ] {
            raw_sql(script).execute(&mut *transaction).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn lookup_pool(&self) -> Result<PgPool> {
        Ok(self.lookup_pool.clone())
    }

    async fn open_lookup_pool(&self) -> Result<PgPool> {
        let config = self.database_config(6)?;
        let options = bigname_storage::stamp_projection_replay_version(
            PgConnectOptions::from_str(
                config
                    .database_url
                    .as_deref()
                    .context("lookup test database URL is missing")?,
            )?
            .options([("search_path", "bigname_phase".to_owned())]),
        );
        PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect_with(options)
            .await
            .context("failed to connect API lookup test pool")
    }

    async fn app_state_with_lookup_chain_rpc_urls(
        &self,
        chain_rpc_urls: bigname_lookup::ChainRpcUrls,
    ) -> Result<AppState> {
        Ok(AppState::new_with_rpc_urls(
            self.pool.clone(),
            self.lookup_pool.clone(),
            chain_rpc_urls,
        ))
    }

    fn app_state(&self) -> AppState {
        AppState::new_with_rpc_urls(
            self.pool.clone(),
            self.lookup_pool.clone(),
            bigname_lookup::ChainRpcUrls::default(),
        )
    }

    fn database_config(&self, max_connections: u32) -> Result<bigname_storage::DatabaseConfig> {
        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| default_database_url().to_owned());
        let options = PgConnectOptions::from_str(&database_url)
            .context("failed to parse database URL for API pool configuration test")?
            .database(&self.database_name);
        Ok(bigname_storage::DatabaseConfig {
            database_url: Some(options.to_url_lossy().to_string()),
            max_connections,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_manifest(
        &self,
        namespace: &str,
        source_family: &str,
        chain: &str,
        deployment_epoch: &str,
        manifest_version: u64,
        rollout_status: &str,
        normalizer_version: &str,
    ) -> Result<i64> {
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let file_path =
            format!("tests/{namespace}/{source_family}/{manifest_version}-{sequence}.toml");

        sqlx::query(
            r#"
                INSERT INTO manifest_versions (
                    manifest_version,
                    namespace,
                    source_family,
                    chain,
                    deployment_epoch,
                    rollout_status,
                    normalizer_version,
                    file_path,
                    manifest_payload
                )
                VALUES ($1, $2, $3, $4, $5, $6::manifest_rollout_status, $7, $8, $9::jsonb)
                RETURNING manifest_id
                "#,
        )
        .bind(i64::try_from(manifest_version).context("manifest_version exceeds BIGINT")?)
        .bind(namespace)
        .bind(source_family)
        .bind(chain)
        .bind(deployment_epoch)
        .bind(rollout_status)
        .bind(normalizer_version)
        .bind(file_path)
        .bind("{}")
        .fetch_one(&self.pool)
        .await
        .context("failed to insert manifest_version for API test")?
        .try_get("manifest_id")
        .context("failed to read manifest_id for API test")
    }

    async fn insert_capability_flag(
        &self,
        manifest_id: i64,
        capability_name: &str,
        status: &str,
        notes: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            r#"
                INSERT INTO manifest_capability_flags (
                    manifest_id,
                    capability_name,
                    status,
                    notes
                )
                VALUES ($1, $2, $3::capability_support_status, $4)
                "#,
        )
        .bind(manifest_id)
        .bind(capability_name)
        .bind(status)
        .bind(notes)
        .execute(&self.pool)
        .await
        .context("failed to insert manifest capability flag for API test")?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn seed_name_current_binding(
        &self,
        logical_name_id: &str,
        namespace: &str,
        normalized_name: &str,
        canonical_display_name: &str,
        namehash: &str,
        resource_id: Uuid,
        token_lineage_id: Uuid,
        surface_binding_id: Uuid,
    ) -> Result<()> {
        seed_readable_lineage_anchors(
            &self.pool,
            [
                (
                    "ethereum-mainnet",
                    "0xlineage",
                    21_000_000,
                    CanonicalityState::Finalized,
                ),
                (
                    "ethereum-mainnet",
                    "0xresource",
                    21_000_001,
                    CanonicalityState::Finalized,
                ),
                (
                    "ethereum-mainnet",
                    "0xsurface",
                    20_999_998,
                    CanonicalityState::Finalized,
                ),
                (
                    "ethereum-mainnet",
                    "0xbinding",
                    21_000_003,
                    CanonicalityState::Finalized,
                ),
            ],
        )
        .await?;

        sqlx::query(
            r#"
                INSERT INTO name_surfaces (
                    logical_name_id,
                    namespace,
                    canonical_display_name,
                    normalized_name,
                    namehash
                )
                VALUES ($1, $2, $3, $4, $5)
                "#,
        )
        .bind(logical_name_id)
        .bind(namespace)
        .bind(canonical_display_name)
        .bind(normalized_name)
        .bind(namehash)
        .execute(&self.pool)
        .await
        .context("failed to insert name_surface for API test")?;

        sqlx::query("INSERT INTO resources (resource_id) VALUES ($1)")
            .bind(resource_id)
            .execute(&self.pool)
            .await
            .context("failed to insert resource for API test")?;

        sqlx::query("INSERT INTO token_lineages (token_lineage_id) VALUES ($1)")
            .bind(token_lineage_id)
            .execute(&self.pool)
            .await
            .context("failed to insert token_lineage for API test")?;

        sqlx::query(
            r#"
                INSERT INTO surface_bindings (
                    surface_binding_id,
                    logical_name_id,
                    resource_id,
                    binding_kind
                )
                VALUES ($1, $2, $3, $4)
                "#,
        )
        .bind(surface_binding_id)
        .bind(logical_name_id)
        .bind(resource_id)
        .bind("declared_registry_path")
        .execute(&self.pool)
        .await
        .context("failed to insert surface_binding for API test")?;

        Ok(())
    }

    async fn seed_name_current_binding_migrated(
        &self,
        logical_name_id: &str,
        resource_id: Uuid,
        token_lineage_id: Uuid,
        surface_binding_id: Uuid,
    ) -> Result<()> {
        bigname_storage::upsert_raw_blocks(
            &self.pool,
            &[
                raw_block("ethereum-mainnet", "0xsurface", None, 98, 1_717_171_698),
                raw_block("ethereum-mainnet", "0xresource", None, 99, 1_717_171_699),
                raw_block("ethereum-mainnet", "0xbinding", None, 100, 1_717_171_700),
            ],
        )
        .await?;
        upsert_test_name_surfaces(&self.pool, &[name_surface(logical_name_id)]).await?;
        upsert_test_token_lineages(
            &self.pool,
            &[address_name_token_lineage(
                token_lineage_id,
                "0xresource",
                99,
            )],
        )
        .await?;
        upsert_test_resources(
            &self.pool,
            &[address_name_resource(
                resource_id,
                Some(token_lineage_id),
                "0xresource",
                99,
            )],
        )
        .await?;
        upsert_test_surface_bindings(
            &self.pool,
            &[surface_binding(
                surface_binding_id,
                logical_name_id,
                resource_id,
                timestamp(1_717_171_700),
            )],
        )
        .await?;

        Ok(())
    }

    async fn insert_name_current_row(&self, row: bigname_storage::NameCurrentRow) -> Result<()> {
        self.seed_snapshot_selector_chain_positions(&row.chain_positions)
            .await?;
        bigname_storage::upsert_name_current_rows(&self.pool, &[row])
            .await
            .context("failed to upsert name_current row for API test")?;
        Ok(())
    }

    async fn insert_record_inventory_current_row(
        &self,
        row: bigname_storage::RecordInventoryCurrentRow,
    ) -> Result<()> {
        bigname_storage::upsert_record_inventory_current_rows(&self.pool, &[row])
            .await
            .context("failed to upsert record_inventory_current row for API test")?;
        Ok(())
    }

    async fn seed_snapshot_selector_chain_positions(&self, chain_positions: &Value) -> Result<()> {
        let Some(positions) = chain_positions.as_object() else {
            return Ok(());
        };

        for position in positions.values() {
            let chain_id = position
                .get("chain_id")
                .and_then(Value::as_str)
                .context("chain_position.chain_id must be present for API selector test seed")?;
            let block_hash = position
                .get("block_hash")
                .and_then(Value::as_str)
                .context("chain_position.block_hash must be present for API selector test seed")?;
            let block_number = position
                .get("block_number")
                .and_then(Value::as_i64)
                .context(
                    "chain_position.block_number must be present for API selector test seed",
                )?;
            let timestamp_value = position
                .get("timestamp")
                .and_then(Value::as_str)
                .context("chain_position.timestamp must be present for API selector test seed")?;
            let timestamp = parse_rfc3339_utc_timestamp(timestamp_value)
                .map_err(|error| anyhow::anyhow!("{error}"))?;

            sqlx::query(
                r#"
                INSERT INTO chain_lineage (
                    chain_id,
                    block_hash,
                    block_number,
                    block_timestamp,
                    canonicality_state
                )
                VALUES ($1, $2, $3, $4, 'finalized'::canonicality_state)
                ON CONFLICT (chain_id, block_hash) DO UPDATE SET
                    block_number = EXCLUDED.block_number,
                    block_timestamp = EXCLUDED.block_timestamp,
                    canonicality_state = EXCLUDED.canonicality_state
                "#,
            )
            .bind(chain_id)
            .bind(block_hash)
            .bind(block_number)
            .bind(timestamp)
            .execute(&self.pool)
            .await
            .with_context(|| {
                format!("failed to seed chain_lineage for {chain_id} block {block_hash}")
            })?;

            sqlx::query(
                r#"
                INSERT INTO chain_lineage (
                    chain_id,
                    block_hash,
                    block_number,
                    block_timestamp,
                    canonicality_state
                )
                VALUES ($1, $2, $3, $4, 'canonical'::canonicality_state)
                ON CONFLICT (chain_id, block_hash) DO NOTHING
                "#,
            )
            .bind(chain_id)
            .bind(block_hash)
            .bind(block_number)
            .bind(timestamp)
            .execute(&self.lookup_pool)
            .await
            .with_context(|| format!("failed to seed phase lineage for {chain_id}"))?;

            sqlx::query(
                "UPDATE chain_lineage
                 SET canonicality_state = 'safe'
                 WHERE chain_id = $1 AND block_hash = $2
                   AND canonicality_state = 'canonical'",
            )
            .bind(chain_id)
            .bind(block_hash)
            .execute(&self.lookup_pool)
            .await?;
            sqlx::query(
                "UPDATE chain_lineage
                 SET canonicality_state = 'finalized'
                 WHERE chain_id = $1 AND block_hash = $2
                   AND canonicality_state = 'safe'",
            )
            .bind(chain_id)
            .bind(block_hash)
            .execute(&self.lookup_pool)
            .await?;

            sqlx::query(
                r#"
                INSERT INTO chain_heads (
                    chain_id,
                    latest_block_hash,
                    latest_block_number,
                    safe_block_hash,
                    safe_block_number,
                    finalized_block_hash,
                    finalized_block_number
                )
                VALUES ($1, $2, $3, $2, $3, $2, $3)
                ON CONFLICT (chain_id) DO UPDATE SET
                    latest_block_hash = EXCLUDED.latest_block_hash,
                    latest_block_number = EXCLUDED.latest_block_number,
                    safe_block_hash = EXCLUDED.safe_block_hash,
                    safe_block_number = EXCLUDED.safe_block_number,
                    finalized_block_hash = EXCLUDED.finalized_block_hash,
                    finalized_block_number = EXCLUDED.finalized_block_number,
                    updated_at = now()
                "#,
            )
            .bind(chain_id)
            .bind(block_hash)
            .bind(block_number)
            .execute(&self.lookup_pool)
            .await
            .with_context(|| format!("failed to seed phase head for {chain_id}"))?;

            sqlx::query(
                r#"
                INSERT INTO chain_phase_state (
                    chain_id,
                    phase_name,
                    phase_status,
                    current_block_number,
                    current_block_hash,
                    target_block_number,
                    target_block_hash,
                    input_content_hash,
                    started_at,
                    finished_at
                )
                VALUES ($1, 'project', 'completed', $2, $3, $2, $3, $4, now(), now())
                ON CONFLICT (chain_id, phase_name) DO UPDATE SET
                    phase_status = EXCLUDED.phase_status,
                    current_block_number = EXCLUDED.current_block_number,
                    current_block_hash = EXCLUDED.current_block_hash,
                    target_block_number = EXCLUDED.target_block_number,
                    target_block_hash = EXCLUDED.target_block_hash,
                    input_content_hash = EXCLUDED.input_content_hash,
                    started_at = EXCLUDED.started_at,
                    finished_at = EXCLUDED.finished_at,
                    updated_at = now()
                "#,
            )
            .bind(chain_id)
            .bind(block_number)
            .bind(block_hash)
            .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
            .execute(&self.lookup_pool)
            .await
            .with_context(|| format!("failed to seed project phase state for {chain_id}"))?;

        }

        Ok(())
    }

    async fn seed_default_ens_snapshot_selector_position(&self) -> Result<()> {
        self.seed_snapshot_selector_chain_positions(&json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbinding",
                "timestamp": "2026-04-17T00:00:03Z"
            }
        }))
        .await
    }

    async fn seed_default_ens_primary_name_fallback_context(&self) -> Result<()> {
        self.seed_default_ens_snapshot_selector_position().await?;
        self.insert_manifest(
            "ens",
            bigname_lookup::ENS_EXECUTION_SOURCE_FAMILY,
            "ethereum-mainnet",
            "ens_v1",
            1,
            "shadow",
            bigname_domain::normalization::ENS_NORMALIZER_VERSION,
        )
        .await?;
        Ok(())
    }

    async fn rebuild_name_current(&self, logical_name_id: &str) -> Result<()> {
        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| default_database_url().to_owned());
        let base_options = PgConnectOptions::from_str(&database_url)
            .context("failed to parse database URL for API worker rebuild")?;
        let rebuild_database_url = base_options
            .database(&self.database_name)
            .to_url_lossy()
            .to_string();
        let logical_name_id = logical_name_id.to_owned();
        let logical_name_id_for_seed = logical_name_id.clone();
        let worker_manifest_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../apps/worker/Cargo.toml");

        tokio::task::spawn_blocking(move || -> Result<()> {
            let _guard = WORKER_CARGO_LOCK
                .lock()
                .expect("worker cargo lock must not be poisoned");
            let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
            let output = std::process::Command::new(cargo)
                .arg("run")
                .arg("--quiet")
                .arg("--manifest-path")
                .arg(worker_manifest_path)
                .arg("--")
                .arg("name-current")
                .arg("rebuild")
                .arg("--database-url")
                .arg(&rebuild_database_url)
                .arg("--logical-name-id")
                .arg(&logical_name_id)
                .output()
                .with_context(|| {
                    format!(
                        "failed to invoke worker name_current rebuild for {logical_name_id}"
                    )
                })?;

            if !output.status.success() {
                return Err(anyhow::anyhow!(
                    "worker name_current rebuild failed for {logical_name_id}\nstdout:\n{}\nstderr:\n{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr),
                ));
            }

            Ok(())
        })
        .await
        .context("worker name_current rebuild task panicked")??;

        if let Some(row) = bigname_storage::load_name_current(&self.pool, &logical_name_id_for_seed)
            .await
            .with_context(|| {
                format!(
                    "failed to load rebuilt name_current row {logical_name_id_for_seed} for selector seed"
                )
            })?
        {
            self.seed_snapshot_selector_chain_positions(&row.chain_positions)
                .await?;
        }

        Ok(())
    }

    async fn seed_basenames_exact_name_rebuild_inputs(
        &self,
        logical_name_id: &str,
        resource_id: Uuid,
        token_lineage_id: Uuid,
        surface_binding_id: Uuid,
    ) -> Result<()> {
        bigname_storage::upsert_raw_blocks(
            &self.pool,
            &[
                raw_block("base-mainnet", "0xbase-surface", None, 98, 1_717_171_698),
                raw_block("base-mainnet", "0xbase-resource", None, 99, 1_717_171_699),
                raw_block("base-mainnet", "0xbase-binding", None, 100, 1_717_171_700),
                raw_block("base-mainnet", "0xbase-grant", None, 101, 1_717_171_701),
                raw_block("base-mainnet", "0xbase-authority", None, 102, 1_717_171_702),
                raw_block("base-mainnet", "0xbase-resolver", None, 103, 1_717_171_703),
            ],
        )
        .await
        .context("failed to upsert raw blocks for basenames exact-name API test")?;
        upsert_test_name_surfaces(
            &self.pool,
            &[NameSurface {
                logical_name_id: logical_name_id.to_owned(),
                namespace: "basenames".to_owned(),
                input_name: "alice.base.eth".to_owned(),
                canonical_display_name: "Alice.base.eth".to_owned(),
                normalized_name: "alice.base.eth".to_owned(),
                dns_encoded_name: b"alice.base.eth".to_vec(),
                namehash: "namehash:alice.base.eth".to_owned(),
                labelhashes: vec!["labelhash:alice.base.eth".to_owned()],
                normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
                normalization_warnings: json!([]),
                normalization_errors: json!([]),
                chain_id: "base-mainnet".to_owned(),
                block_hash: "0xbase-surface".to_owned(),
                block_number: 98,
                provenance: json!({"seed": "basenames_exact_name_surface"}),
                canonicality_state: CanonicalityState::Canonical,
            }],
        )
        .await
        .context("failed to upsert basenames name surface for API test")?;
        upsert_test_token_lineages(
            &self.pool,
            &[TokenLineage {
                token_lineage_id,
                chain_id: "base-mainnet".to_owned(),
                block_hash: "0xbase-resource".to_owned(),
                block_number: 99,
                provenance: json!({"seed": "basenames_exact_name_token_lineage"}),
                canonicality_state: CanonicalityState::Canonical,
            }],
        )
        .await
        .context("failed to upsert basenames token lineage for API test")?;
        upsert_test_resources(
            &self.pool,
            &[Resource {
                resource_id,
                token_lineage_id: Some(token_lineage_id),
                chain_id: "base-mainnet".to_owned(),
                block_hash: "0xbase-resource".to_owned(),
                block_number: 99,
                provenance: json!({"seed": "basenames_exact_name_resource"}),
                canonicality_state: CanonicalityState::Canonical,
            }],
        )
        .await
        .context("failed to upsert basenames resource for API test")?;
        upsert_test_surface_bindings(
            &self.pool,
            &[SurfaceBinding {
                surface_binding_id,
                logical_name_id: logical_name_id.to_owned(),
                resource_id,
                binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
                active_from: timestamp(1_717_171_700),
                active_to: None,
                chain_id: "base-mainnet".to_owned(),
                block_hash: "0xbase-binding".to_owned(),
                block_number: 100,
                provenance: json!({"seed": "basenames_exact_name_binding"}),
                canonicality_state: CanonicalityState::Canonical,
            }],
        )
        .await
        .context("failed to upsert basenames surface binding for API test")?;
        bigname_storage::insert_normalized_event_fixtures(
            &self.pool,
            &[
                NormalizedEvent {
                    event_identity: "api-test:basenames:grant".to_owned(),
                    namespace: "basenames".to_owned(),
                    logical_name_id: Some(logical_name_id.to_owned()),
                    resource_id: Some(resource_id),
                    event_kind: "RegistrationGranted".to_owned(),
                    source_family: "basenames_base_registrar".to_owned(),
                    manifest_version: 3,
                    source_manifest_id: None,
                    chain_id: Some("base-mainnet".to_owned()),
                    block_number: Some(101),
                    block_hash: Some("0xbase-grant".to_owned()),
                    transaction_hash: Some("0xtxbasegrant".to_owned()),
                    log_index: Some(0),
                    raw_fact_ref: json!({"kind": "raw_log", "event_identity": "api-test:basenames:grant"}),
                    derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                    canonicality_state: CanonicalityState::Canonical,
                    before_state: json!({}),
                    after_state: json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:base-mainnet:alice",
                        "registrant": "0x00000000000000000000000000000000000000aa",
                        "expiry": 1_900_000_000_i64,
                    }),
                },
                NormalizedEvent {
                    event_identity: "api-test:basenames:authority".to_owned(),
                    namespace: "basenames".to_owned(),
                    logical_name_id: Some(logical_name_id.to_owned()),
                    resource_id: Some(resource_id),
                    event_kind: "AuthorityTransferred".to_owned(),
                    source_family: "basenames_base_registry".to_owned(),
                    manifest_version: 3,
                    source_manifest_id: None,
                    chain_id: Some("base-mainnet".to_owned()),
                    block_number: Some(102),
                    block_hash: Some("0xbase-authority".to_owned()),
                    transaction_hash: Some("0xtxbaseauthority".to_owned()),
                    log_index: Some(0),
                    raw_fact_ref: json!({"kind": "raw_log", "event_identity": "api-test:basenames:authority"}),
                    derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                    canonicality_state: CanonicalityState::Canonical,
                    before_state: json!({}),
                    after_state: json!({
                        "owner": "0x00000000000000000000000000000000000000bb",
                    }),
                },
                NormalizedEvent {
                    event_identity: "api-test:basenames:resolver".to_owned(),
                    namespace: "basenames".to_owned(),
                    logical_name_id: Some(logical_name_id.to_owned()),
                    resource_id: Some(resource_id),
                    event_kind: "ResolverChanged".to_owned(),
                    source_family: "basenames_base_resolver".to_owned(),
                    manifest_version: 4,
                    source_manifest_id: None,
                    chain_id: Some("base-mainnet".to_owned()),
                    block_number: Some(103),
                    block_hash: Some("0xbase-resolver".to_owned()),
                    transaction_hash: Some("0xtxbaseresolver".to_owned()),
                    log_index: Some(0),
                    raw_fact_ref: json!({"kind": "raw_log", "event_identity": "api-test:basenames:resolver"}),
                    derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                    canonicality_state: CanonicalityState::Canonical,
                    before_state: json!({}),
                    after_state: json!({
                        "resolver": "0x0000000000000000000000000000000000000abc",
                        "namehash": "namehash:alice.base.eth",
                    }),
                },
            ],
        )
        .await
        .context("failed to upsert basenames normalized events for API test")?;

        Ok(())
    }

    async fn insert_primary_name_current_claim_row(
        &self,
        address: &str,
        namespace: &str,
        coin_type: &str,
        claim_status: PrimaryNameClaimStatus,
        raw_claim_name: Option<&str>,
    ) -> Result<()> {
        self.insert_primary_name_current_claim_row_with_provenance(
            address,
            namespace,
            coin_type,
            claim_status,
            raw_claim_name,
            json!({}),
        )
        .await
    }

    async fn insert_primary_name_current_claim_row_with_provenance(
        &self,
        address: &str,
        namespace: &str,
        coin_type: &str,
        claim_status: PrimaryNameClaimStatus,
        raw_claim_name: Option<&str>,
        claim_provenance: Value,
    ) -> Result<()> {
        upsert_primary_name_current_rows(
            &self.pool,
            &[PrimaryNameCurrentRow {
                address: address.to_ascii_lowercase(),
                namespace: namespace.to_owned(),
                coin_type: coin_type.to_owned(),
                claim_status,
                raw_claim_name: raw_claim_name.map(str::to_owned),
                claim_provenance,
            }],
        )
        .await
        .context("failed to upsert primary_names_current row for API tests")?;
        Ok(())
    }

    async fn insert_primary_name_current_normalized_claim_name(
        &self,
        address: &str,
        namespace: &str,
        coin_type: &str,
        normalized_claim_name: Option<&str>,
        claim_name_is_normalized: bool,
    ) -> Result<()> {
        let row = load_primary_name_current(&self.pool, address, namespace, coin_type)
            .await
            .context("failed to load primary_names_current row for API test")?
            .with_context(|| {
                format!(
                    "missing primary_names_current row for API test address {} namespace {} coin_type {}",
                    address, namespace, coin_type
                )
            })?;

        upsert_primary_name_current_snapshots(
            &self.pool,
            &[PrimaryNameCurrentSnapshot {
                row,
                normalized_claim_name: normalized_claim_name.map(str::to_owned),
                claim_name_is_normalized,
            }],
        )
        .await
        .context("failed to upsert primary_names_current snapshot for API test")?;
        Ok(())
    }

    async fn cleanup(self) -> Result<()> {
        let Self {
            database,
            pool,
            lookup_pool,
            database_name: _,
        } = self;
        drop(pool);
        drop(lookup_pool);
        database.cleanup().await
    }
}

async fn seed_schema_v2_ens_lookup_head(
    pool: &PgPool,
    block_number: i64,
    block_hash: &str,
    timestamp: &str,
) -> Result<()> {
    seed_schema_v2_lookup_head(
        pool,
        "ethereum-mainnet",
        block_number,
        block_hash,
        timestamp,
    )
    .await
}

async fn seed_schema_v2_lookup_head(
    pool: &PgPool,
    chain_id: &str,
    block_number: i64,
    block_hash: &str,
    timestamp: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO chain_lineage
            (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         VALUES ($1, $2, $3, $4::timestamptz, 'canonical')
         ON CONFLICT (chain_id, block_hash) DO NOTHING",
    )
    .bind(chain_id)
    .bind(block_hash)
    .bind(block_number)
    .bind(timestamp)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO chain_heads (chain_id, latest_block_hash, latest_block_number)
         VALUES ($1, $2, $3)
         ON CONFLICT (chain_id) DO UPDATE SET
             latest_block_hash = EXCLUDED.latest_block_hash,
             latest_block_number = EXCLUDED.latest_block_number,
             updated_at = now()",
    )
    .bind(chain_id)
    .bind(block_hash)
    .bind(block_number)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO chain_phase_state
            (chain_id, phase_name, phase_status, current_block_number, current_block_hash,
             target_block_number, target_block_hash, input_content_hash, started_at, finished_at)
         VALUES ($1, 'project', 'completed', $2, $3, $2, $3, $4, now(), now())
         ON CONFLICT (chain_id, phase_name) DO UPDATE SET
             phase_status = EXCLUDED.phase_status,
             current_block_number = EXCLUDED.current_block_number,
             current_block_hash = EXCLUDED.current_block_hash,
             target_block_number = EXCLUDED.target_block_number,
             target_block_hash = EXCLUDED.target_block_hash,
             input_content_hash = EXCLUDED.input_content_hash,
             started_at = EXCLUDED.started_at,
             finished_at = EXCLUDED.finished_at,
             updated_at = now()",
    )
    .bind(chain_id)
    .bind(block_number)
    .bind(block_hash)
    .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_schema_v2_ens_manifest(
    pool: &PgPool,
    source_family: &str,
    role: &str,
    address: &str,
    contract_instance_id: Uuid,
    resolution_capability: bool,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO contract_instances
            (contract_instance_id, chain_id, contract_kind)
         VALUES ($1, 'ethereum-mainnet', 'contract')",
    )
    .bind(contract_instance_id)
    .execute(pool)
    .await?;
    let manifest_payload = if resolution_capability {
        json!({
            "capability_flags": {
                "verified_resolution": { "status": "supported" }
            }
        })
    } else {
        json!({})
    };
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions
            (manifest_version, namespace, source_family, chain_id, deployment_label,
             rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES (1, 'ens', $1, 'ethereum-mainnet', 'api-test', 'active', 'test', $2, $3)
         RETURNING manifest_id",
    )
    .bind(source_family)
    .bind(format!("test/ens/{source_family}.toml"))
    .bind(manifest_payload)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances
            (manifest_id, chain_id, declaration_kind, declaration_name,
             contract_instance_id, declared_address, role, proxy_kind)
         VALUES ($1, 'ethereum-mainnet', 'contract', $2, $3, $4, $2, 'none')",
    )
    .bind(manifest_id)
    .bind(role)
    .bind(contract_instance_id)
    .bind(address)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_schema_v2_ens_record_lookup(
    pool: &PgPool,
    block_number: i64,
    block_hash: &str,
    timestamp: &str,
    indexed_address: &str,
) -> Result<String> {
    seed_schema_v2_ens_lookup_head(pool, block_number, block_hash, timestamp).await?;
    seed_schema_v2_ens_manifest(
        pool,
        "ens_execution",
        "universal_resolver",
        "0xeeeeeeee14d718c2b47d9923deab1335e144eeee",
        Uuid::from_u128(0xc200_0000_0000_0000_0000_0000_0000_0103),
        true,
    )
    .await?;
    let namehash = bigname_lookup::ens_namehash_hex("alice.eth")?;
    let logical_name_id = format!("ens:{namehash}");
    let resource_id = Uuid::from_u128(0xc200_0000_0000_0000_0000_0000_0000_0101);
    let binding_id = Uuid::from_u128(0xc200_0000_0000_0000_0000_0000_0000_0102);
    let positions = json!({
        "ethereum": {
            "chain_id": "ethereum-mainnet",
            "block_number": block_number,
            "block_hash": block_hash,
            "timestamp": timestamp
        }
    });
    let boundary = json!({
        "logical_name_id": logical_name_id,
        "resource_id": resource_id,
        "normalized_event_id": 1,
        "event_kind": "ResolverChanged",
        "chain_position": positions["ethereum"]
    });
    let topology = json!({
        "registry_path": [],
        "subregistry_path": [],
        "resolver_path": [{
            "logical_name_id": logical_name_id,
            "resource_id": resource_id,
            "chain_id": "ethereum-mainnet",
            "address": "0x1000000000000000000000000000000000000001"
        }],
        "wildcard": { "source": null, "matched_labels": [] },
        "alias": { "final_target": null, "hops": [] },
        "version_boundaries": { "record_version_boundary": boundary },
        "transport": {
            "source_chain_id": null,
            "target_chain_id": null,
            "contract_address": null,
            "latest_event_kind": null
        }
    });
    sqlx::query(
        "INSERT INTO resources
            (resource_id, chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1, 'ethereum-mainnet', $2, $3, 'canonical')",
    )
    .bind(resource_id)
    .bind(block_hash)
    .bind(block_number)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces
            (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
             namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1, 'ens', 'alice.eth', ARRAY['alice.eth'], $2, $3, ARRAY[$3], 'test',
                 'active', 'ethereum-mainnet', $4, $5, 'canonical')",
    )
    .bind(&logical_name_id)
    .bind(b"\x05alice\x03eth\0".as_slice())
    .bind(&namehash)
    .bind(block_hash)
    .bind(block_number)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings
            (surface_binding_id, logical_name_id, resource_id, binding_kind, active_from,
             chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1, $2, $3, 'declared_registry_path', $4::timestamptz,
                 'ethereum-mainnet', $5, $6, 'canonical')",
    )
    .bind(binding_id)
    .bind(&logical_name_id)
    .bind(resource_id)
    .bind(timestamp)
    .bind(block_hash)
    .bind(block_number)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO name_current
            (logical_name_id, namespace, raw_name, namehash, surface_binding_id,
             resource_id, binding_kind, declared_summary, support_status,
             provenance, chain_positions, canonicality_summary, manifest_version)
         VALUES ($1, 'ens', 'alice.eth', $2, $3, $4, 'declared_registry_path',
                 jsonb_build_object('topology', $5::jsonb), 'supported', '{}', $6,
                 jsonb_build_object('state', 'canonical'), 1)",
    )
    .bind(&logical_name_id)
    .bind(&namehash)
    .bind(binding_id)
    .bind(resource_id)
    .bind(&topology)
    .bind(&positions)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO record_inventory_current
            (resource_id, record_version_boundary_key, record_version_boundary,
             selectors, unsupported_families, entries, support_status, provenance,
             chain_positions, canonicality_summary, manifest_version)
         VALUES ($1, 'boundary-1', $2, $3, '[]', $4, 'supported', '{}', $5,
                 jsonb_build_object('state', 'canonical'), 1)",
    )
    .bind(resource_id)
    .bind(&boundary)
    .bind(json!([{
        "record_key": "addr:60",
        "record_family": "addr",
        "selector_key": "60"
    }]))
    .bind(json!([{
        "record_key": "addr:60",
        "record_family": "addr",
        "selector_key": "60",
        "status": "success",
        "value": { "coin_type": "60", "value": indexed_address }
    }]))
    .bind(json!({
        "target_block_number": block_number,
        "target_block_hash": block_hash
    }))
    .execute(pool)
    .await?;
    Ok(namehash)
}

async fn seed_schema_v2_basenames_record_lookup(
    pool: &PgPool,
    block_number: i64,
    base_block_hash: &str,
    ethereum_block_hash: &str,
    timestamp: &str,
    indexed_address: &str,
) -> Result<String> {
    seed_schema_v2_lookup_head(
        pool,
        "base-mainnet",
        block_number,
        base_block_hash,
        timestamp,
    )
    .await?;
    seed_schema_v2_lookup_head(
        pool,
        "ethereum-mainnet",
        block_number,
        ethereum_block_hash,
        timestamp,
    )
    .await?;

    let l1_resolver = "0xde9049636f4a1dfe0a64d1bfe3155c0a14c54f31";
    let contract_instance_id = Uuid::from_u128(0xc200_0000_0000_0000_0000_0000_0000_0203);
    sqlx::query(
        "INSERT INTO contract_instances
            (contract_instance_id, chain_id, contract_kind)
         VALUES ($1, 'ethereum-mainnet', 'contract')",
    )
    .bind(contract_instance_id)
    .execute(pool)
    .await?;
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions
            (manifest_version, namespace, source_family, chain_id, deployment_label,
             rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES (2, 'basenames', 'basenames_execution', 'ethereum-mainnet',
                 'api-test', 'active', 'test', 'test/basenames/execution.toml', $1)
         RETURNING manifest_id",
    )
    .bind(json!({
        "capability_flags": {
            "verified_resolution": { "status": "supported" }
        }
    }))
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances
            (manifest_id, chain_id, declaration_kind, declaration_name,
             contract_instance_id, declared_address, role, proxy_kind)
         VALUES ($1, 'ethereum-mainnet', 'contract', 'l1_resolver', $2, $3,
                 'l1_resolver', 'none')",
    )
    .bind(manifest_id)
    .bind(contract_instance_id)
    .bind(l1_resolver)
    .execute(pool)
    .await?;

    let namehash = bigname_lookup::ens_namehash_hex("alice.base.eth")?;
    let logical_name_id = format!("basenames:{namehash}");
    let resource_id = Uuid::from_u128(0xc200_0000_0000_0000_0000_0000_0000_0201);
    let binding_id = Uuid::from_u128(0xc200_0000_0000_0000_0000_0000_0000_0202);
    let positions = json!({
        "base": {
            "chain_id": "base-mainnet",
            "block_number": block_number,
            "block_hash": base_block_hash,
            "timestamp": timestamp
        },
        "ethereum": {
            "chain_id": "ethereum-mainnet",
            "block_number": block_number,
            "block_hash": ethereum_block_hash,
            "timestamp": timestamp
        }
    });
    let boundary = json!({
        "logical_name_id": logical_name_id,
        "resource_id": resource_id,
        "normalized_event_id": 1,
        "event_kind": "ResolverChanged",
        "chain_position": positions["base"]
    });
    let topology = json!({
        "registry_path": [],
        "subregistry_path": [],
        "resolver_path": [{
            "logical_name_id": logical_name_id,
            "resource_id": resource_id,
            "chain_id": "base-mainnet",
            "address": "0x1000000000000000000000000000000000000001"
        }],
        "wildcard": { "source": null, "matched_labels": [] },
        "alias": { "final_target": null, "hops": [] },
        "version_boundaries": { "record_version_boundary": boundary },
        "transport": {
            "source_chain_id": "base-mainnet",
            "target_chain_id": "ethereum-mainnet",
            "contract_address": l1_resolver,
            "latest_event_kind": "ResolverChanged"
        }
    });
    sqlx::query(
        "INSERT INTO resources
            (resource_id, chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1, 'base-mainnet', $2, $3, 'canonical')",
    )
    .bind(resource_id)
    .bind(base_block_hash)
    .bind(block_number)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces
            (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
             namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1, 'basenames', 'alice.base.eth', ARRAY['alice.base.eth'], $2, $3,
                 ARRAY[$3], 'test', 'active', 'base-mainnet', $4, $5, 'canonical')",
    )
    .bind(&logical_name_id)
    .bind(b"\x05alice\x04base\x03eth\0".as_slice())
    .bind(&namehash)
    .bind(base_block_hash)
    .bind(block_number)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings
            (surface_binding_id, logical_name_id, resource_id, binding_kind, active_from,
             chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1, $2, $3, 'declared_registry_path', $4::timestamptz,
                 'base-mainnet', $5, $6, 'canonical')",
    )
    .bind(binding_id)
    .bind(&logical_name_id)
    .bind(resource_id)
    .bind(timestamp)
    .bind(base_block_hash)
    .bind(block_number)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO name_current
            (logical_name_id, namespace, raw_name, namehash, surface_binding_id,
             resource_id, binding_kind, declared_summary, support_status,
             provenance, chain_positions, canonicality_summary, manifest_version)
         VALUES ($1, 'basenames', 'alice.base.eth', $2, $3, $4,
                 'declared_registry_path', jsonb_build_object('topology', $5::jsonb),
                 'supported', '{}', $6, jsonb_build_object('state', 'canonical'), 2)",
    )
    .bind(&logical_name_id)
    .bind(&namehash)
    .bind(binding_id)
    .bind(resource_id)
    .bind(&topology)
    .bind(&positions)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO record_inventory_current
            (resource_id, record_version_boundary_key, record_version_boundary,
             selectors, unsupported_families, entries, support_status, provenance,
             chain_positions, canonicality_summary, manifest_version)
         VALUES ($1, 'boundary-1', $2, $3, '[]', $4, 'supported', '{}', $5,
                 jsonb_build_object('state', 'canonical'), 2)",
    )
    .bind(resource_id)
    .bind(&boundary)
    .bind(json!([{
        "record_key": "addr:60",
        "record_family": "addr",
        "selector_key": "60"
    }]))
    .bind(json!([{
        "record_key": "addr:60",
        "record_family": "addr",
        "selector_key": "60",
        "status": "success",
        "value": { "coin_type": "60", "value": indexed_address }
    }]))
    .bind(json!({
        "target_block_number": block_number,
        "target_block_hash": base_block_hash
    }))
    .execute(pool)
    .await?;
    Ok(namehash)
}

async fn seed_schema_v2_ens_primary_name_authority(
    pool: &PgPool,
    block_number: i64,
    block_hash: &str,
    timestamp: &str,
) -> Result<()> {
    seed_schema_v2_ens_lookup_head(pool, block_number, block_hash, timestamp).await?;
    seed_schema_v2_ens_manifest(
        pool,
        "ens_v1_registry_l1",
        "registry",
        "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e",
        Uuid::from_u128(0xc200_0000_0000_0000_0000_0000_0000_0104),
        false,
    )
    .await?;
    seed_schema_v2_ens_manifest(
        pool,
        "ens_execution",
        "universal_resolver",
        "0xeeeeeeee14d718c2b47d9923deab1335e144eeee",
        Uuid::from_u128(0xc200_0000_0000_0000_0000_0000_0000_0105),
        true,
    )
    .await
}

async fn read_json<T: DeserializeOwned>(response: Response) -> Result<T> {
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .context("failed to read API response body")?;
    serde_json::from_slice(&bytes).context("failed to decode API response JSON")
}

fn timestamp(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp must be valid")
}

async fn seed_readable_lineage_anchors<'a>(
    pool: &PgPool,
    anchors: impl IntoIterator<Item = (&'a str, &'a str, i64, CanonicalityState)>,
) -> Result<()> {
    for (chain_id, block_hash, block_number, canonicality_state) in anchors {
        if !matches!(
            canonicality_state,
            CanonicalityState::Canonical
                | CanonicalityState::Safe
                | CanonicalityState::Finalized
        ) {
            continue;
        }

        sqlx::query(
            r#"
            INSERT INTO chain_lineage (
                chain_id,
                block_hash,
                block_number,
                block_timestamp,
                canonicality_state
            )
            VALUES ($1, $2, $3, $4, $5::canonicality_state)
            ON CONFLICT (chain_id, block_hash) DO NOTHING
            "#,
        )
        .bind(chain_id)
        .bind(block_hash)
        .bind(block_number)
        .bind(timestamp(1_700_000_000 + block_number))
        .bind(canonicality_state.as_str())
        .execute(pool)
        .await
        .with_context(|| {
            format!("failed to seed readable lineage for {chain_id} block {block_hash}")
        })?;
    }

    Ok(())
}

async fn upsert_test_token_lineages(
    pool: &PgPool,
    token_lineages: &[TokenLineage],
) -> Result<Vec<TokenLineage>> {
    seed_readable_lineage_anchors(
        pool,
        token_lineages.iter().map(|row| {
            (
                row.chain_id.as_str(),
                row.block_hash.as_str(),
                row.block_number,
                row.canonicality_state,
            )
        }),
    )
    .await?;
    bigname_storage::upsert_token_lineages(pool, token_lineages).await
}

async fn upsert_test_resources(
    pool: &PgPool,
    resources: &[Resource],
) -> Result<Vec<Resource>> {
    seed_readable_lineage_anchors(
        pool,
        resources.iter().map(|row| {
            (
                row.chain_id.as_str(),
                row.block_hash.as_str(),
                row.block_number,
                row.canonicality_state,
            )
        }),
    )
    .await?;
    bigname_storage::upsert_resources(pool, resources).await
}

async fn upsert_test_name_surfaces(
    pool: &PgPool,
    name_surfaces: &[NameSurface],
) -> Result<Vec<NameSurface>> {
    seed_readable_lineage_anchors(
        pool,
        name_surfaces.iter().map(|row| {
            (
                row.chain_id.as_str(),
                row.block_hash.as_str(),
                row.block_number,
                row.canonicality_state,
            )
        }),
    )
    .await?;
    bigname_storage::upsert_name_surfaces(pool, name_surfaces).await
}

async fn upsert_test_surface_bindings(
    pool: &PgPool,
    bindings: &[SurfaceBinding],
) -> Result<Vec<SurfaceBinding>> {
    seed_readable_lineage_anchors(
        pool,
        bindings.iter().map(|row| {
            (
                row.chain_id.as_str(),
                row.block_hash.as_str(),
                row.block_number,
                row.canonicality_state,
            )
        }),
    )
    .await?;
    bigname_storage::upsert_surface_bindings(pool, bindings).await
}

fn raw_block(
    chain_id: &str,
    block_hash: &str,
    parent_hash: Option<&str>,
    block_number: i64,
    block_timestamp: i64,
) -> RawBlock {
    RawBlock {
        chain_id: chain_id.to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: parent_hash.map(str::to_owned),
        block_number,
        block_timestamp: timestamp(block_timestamp),
        logs_bloom: None,
        transactions_root: None,
        receipts_root: None,
        state_root: None,
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn resource(resource_id: Uuid) -> Resource {
    Resource {
        resource_id,
        token_lineage_id: None,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xresource".to_owned(),
        block_number: 99,
        provenance: json!({"seed": "resource"}),
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn name_surface(logical_name_id: &str) -> NameSurface {
    let (namespace, normalized_name) = logical_name_id
        .split_once(':')
        .expect("logical_name_id must include namespace");
    let chain_id = chain_id_for_namespace(namespace);

    NameSurface {
        logical_name_id: logical_name_id.to_owned(),
        namespace: namespace.to_owned(),
        input_name: normalized_name.to_owned(),
        canonical_display_name: "Alice.eth".to_owned(),
        normalized_name: normalized_name.to_owned(),
        dns_encoded_name: vec![5, b'a', b'l', b'i', b'c', b'e'],
        namehash: format!("namehash:{normalized_name}"),
        labelhashes: vec!["labelhash:alice".to_owned()],
        normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id: chain_id.to_owned(),
        block_hash: "0xsurface".to_owned(),
        block_number: 98,
        provenance: json!({"seed": "surface"}),
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn surface_binding(
    surface_binding_id: Uuid,
    logical_name_id: &str,
    resource_id: Uuid,
    active_from: OffsetDateTime,
) -> SurfaceBinding {
    SurfaceBinding {
        surface_binding_id,
        logical_name_id: logical_name_id.to_owned(),
        resource_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        active_from,
        active_to: None,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xbinding".to_owned(),
        block_number: 100,
        provenance: json!({"seed": "binding"}),
        canonicality_state: CanonicalityState::Canonical,
    }
}

#[allow(clippy::too_many_arguments)]
fn history_event(
    event_identity: &str,
    logical_name_id: Option<&str>,
    resource_id: Option<Uuid>,
    chain_id: Option<&str>,
    block_number: Option<i64>,
    block_hash: Option<&str>,
    transaction_hash: Option<&str>,
    log_index: Option<i64>,
    canonicality_state: CanonicalityState,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: logical_name_id.map(str::to_owned),
        resource_id,
        event_kind: "HistoryEvent".to_owned(),
        source_family: "ens_v1_registry_l1".to_owned(),
        manifest_version: 7,
        source_manifest_id: None,
        chain_id: chain_id.map(str::to_owned),
        block_number,
        block_hash: block_hash.map(str::to_owned),
        transaction_hash: transaction_hash.map(str::to_owned),
        log_index,
        raw_fact_ref: json!({
            "kind": "raw_log",
            "event_identity": event_identity,
        }),
        derivation_kind: "history_test".to_owned(),
        canonicality_state,
        before_state: json!({
            "provenance": {
                "before": event_identity,
            }
        }),
        after_state: json!({
            "provenance": {
                "after": event_identity,
            },
            "coverage": {
                "status": "full",
                "exhaustiveness": "authoritative",
                "source_classes_considered": ["normalized_events"],
                "enumeration_basis": event_identity,
                "unsupported_reason": null,
            }
        }),
    }
}

fn permission_current_row(
    resource_id: Uuid,
    subject: &str,
    scope: PermissionScope,
    manifest_version: i64,
    block_number: i64,
) -> PermissionsCurrentRow {
    PermissionsCurrentRow {
        resource_id,
        subject: subject.to_owned(),
        scope,
        effective_powers: json!([
            "set_resolver",
            if manifest_version % 2 == 0 {
                "create_subnames"
            } else {
                "set_records"
            }
        ]),
        grant_source: json!({
            "kind": "raw_log",
            "source_event": "EACRolesChanged",
            "upstream_resource": resource_id.to_string(),
            "root_resource": false,
            "changed_powers": [
                "set_resolver",
                if manifest_version % 2 == 0 {
                    "create_subnames"
                } else {
                    "set_records"
                }
            ],
            "registry_contract_instance_id": "00000000-0000-0000-0000-00000000c001",
        }),
        revocation_source: None,
        inheritance_path: json!([]),
        transfer_behavior: json!({}),
        provenance: json!({
            "normalized_event_ids": [block_number, block_number + 1],
            "raw_fact_refs": [{
                "kind": "raw_log",
                "block_number": block_number,
            }],
            "manifest_versions": [{
                "manifest_version": manifest_version,
                "source_family": "ens_v2_registry_l1",
                "chain": "ethereum-mainnet",
                "deployment_epoch": "ens_v2",
            }],
            "derivation_kind": "permissions_current_rebuild",
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["permissions_current"],
            "enumeration_basis": "resource_permissions",
            "unsupported_reason": null,
        }),
        chain_positions: json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": block_number,
                "block_hash": format!("0xperm{block_number:02x}"),
                "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60),
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                "ethereum-mainnet": "finalized",
            }
        }),
        manifest_version,
        last_recomputed_at: timestamp(1_717_174_000 + block_number),
    }
}

fn permission_current_resource_summary(
    resource_id: Uuid,
    authority_kind: Option<&str>,
) -> bigname_storage::PermissionsCurrentResourceSummary {
    let authority_kind = authority_kind.map(str::to_owned);
    let coverage = match authority_kind.as_deref() {
        Some("wrapper") => bigname_storage::ResourcePermissionCoverage::ensv1_wrapper_holder_permissions_not_projected(),
        Some(_) => bigname_storage::ResourcePermissionCoverage::authoritative(["permissions_current"]),
        None => bigname_storage::ResourcePermissionCoverage::resource_authority_not_projected(),
    };
    bigname_storage::PermissionsCurrentResourceSummary {
        resource_id,
        authority_kind,
        root_resource_id: None,
        coverage,
        provenance: json!({
            "derivation_kind": "permissions_current_resource_summary_rebuild",
        }),
        chain_positions: json!({
            "ethereum-mainnet": {
                "chain_id": "ethereum-mainnet",
                "block_number": 1,
                "block_hash": "0xpermission-summary",
                "timestamp": "2024-05-31T01:13:20Z",
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {"ethereum-mainnet": "finalized"},
        }),
        manifest_version: 1,
        last_recomputed_at: timestamp(1_717_174_000),
    }
}

async fn mark_permissions_current_projection_ready(database: &TestDatabase) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO permissions_current_publication (
            projection,
            publication_version,
            data_revision,
            published_at
        )
        VALUES ('permissions_current', $1, 1, now())
        ON CONFLICT (projection) DO UPDATE SET
            publication_version = EXCLUDED.publication_version,
            data_revision = permissions_current_publication.data_revision + 1,
            published_at = EXCLUDED.published_at
        "#,
    )
    .bind(bigname_storage::PERMISSIONS_CURRENT_PUBLICATION_VERSION)
    .execute(&database.pool)
    .await?;
    Ok(())
}

fn resolver_current_row(chain_id: &str, resolver_address: &str) -> ResolverCurrentRow {
    ResolverCurrentRow {
        chain_id: chain_id.to_owned(),
        resolver_address: resolver_address.to_owned(),
        declared_summary: json!({
            "bindings": {
                "status": "supported",
                "count": 2,
                "items": [
                    {
                        "logical_name_id": "ens:alice.eth",
                        "canonical_display_name": "Alice.eth",
                        "normalized_name": "alice.eth",
                        "namehash": "namehash:alice.eth",
                        "resource_id": "00000000-0000-0000-0000-00000000b100",
                        "surface_binding_id": "00000000-0000-0000-0000-00000000b101",
                        "binding_kind": "declared_registry_path",
                    },
                    {
                        "logical_name_id": "ens:beta.eth",
                        "canonical_display_name": "Beta.eth",
                        "normalized_name": "beta.eth",
                        "namehash": "namehash:beta.eth",
                        "resource_id": "00000000-0000-0000-0000-00000000b102",
                        "surface_binding_id": "00000000-0000-0000-0000-00000000b103",
                        "binding_kind": "resolver_alias_path",
                    }
                ],
            },
            "aliases": {
                "status": "supported",
                "count": 1,
                "items": [{
                    "logical_name_id": "ens:beta.eth",
                    "canonical_display_name": "Beta.eth",
                    "normalized_name": "beta.eth",
                    "namehash": "namehash:beta.eth",
                    "resource_id": "00000000-0000-0000-0000-00000000b102",
                    "surface_binding_id": "00000000-0000-0000-0000-00000000b103",
                    "binding_kind": "resolver_alias_path",
                }],
            },
            "permissions": {
                "status": "supported",
                "count": 1,
                "items": [{
                    "resource_id": "00000000-0000-0000-0000-00000000b100",
                    "subject": "0x0000000000000000000000000000000000000abc",
                    "effective_powers": ["set_resolver", "set_records"],
                    "grant_source": {
                        "kind": "raw_log",
                        "source_event": "EACRolesChanged",
                        "upstream_resource": "root",
                        "root_resource": true,
                        "changed_powers": ["set_resolver", "set_records"],
                        "resolver_contract_instance_id": "00000000-0000-0000-0000-00000000c202",
                    },
                    "revocation_source": null,
                }],
            },
            "role_holders": {
                "status": "supported",
                "count": 1,
                "items": [{
                    "subject": "0x0000000000000000000000000000000000000abc",
                    "resource_count": 1,
                    "permission_row_count": 1,
                    "effective_powers": ["set_records", "set_resolver"],
                    "resource_ids": ["00000000-0000-0000-0000-00000000b100"],
                }],
            },
            "event_summary": {
                "status": "supported",
                "count": 3,
                "by_kind": {
                    "PermissionChanged": 1,
                    "ResolverChanged": 2,
                },
            },
        }),
        provenance: json!({
            "normalized_event_ids": [101, 202],
            "raw_fact_refs": [{
                "kind": "raw_log",
                "chain_id": chain_id,
                "block_number": 202,
            }],
            "manifest_versions": [{
                "manifest_version": 7,
                "source_family": "ens_v2_registry_l1",
                "chain": chain_id,
                "deployment_epoch": "ens_v2",
            }],
            "execution_trace_id": null,
            "derivation_kind": "resolver_current_rebuild",
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["ens_v2_registry_l1", "permissions_current"],
            "unsupported_reason": null,
            "enumeration_basis": "resolver_target",
        }),
        chain_positions: json!({
            "ethereum": {
                "chain_id": chain_id,
                "block_number": 202,
                "block_hash": "0xresolverc8",
                "timestamp": "2026-04-17T00:00:22Z",
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                chain_id: "finalized",
            }
        }),
        manifest_version: 7,
        last_recomputed_at: timestamp(1_748_800_202),
    }
}

fn resolver_current_row_with_writer_alias(
    chain_id: &str,
    resolver_address: &str,
) -> ResolverCurrentRow {
    let mut row = resolver_current_row(chain_id, resolver_address);
    row.declared_summary["aliases"]["count"] = json!(2);
    row.declared_summary["aliases"]["items"]
        .as_array_mut()
        .expect("resolver aliases fixture must be an array")
        .push(json!({
            "logical_name_id": "ens:alias.eth",
            "resource_id": "00000000-0000-0000-0000-00000000b104",
            "binding_kind": "resolver_alias_path",
            "alias_state": "active",
            "active": true,
            "chain_id": chain_id,
            "resolver_address": resolver_address,
            "from_dns_encoded_name": "0x05616c6961730365746800",
            "to_dns_encoded_name": "0x04626574610365746800",
            "from_name": "alias.eth",
            "to_name": "beta.eth",
            "to_logical_name_id": "ens:beta.eth",
            "to_resource_id": "00000000-0000-0000-0000-00000000b102",
            "latest_event_kind": "AliasChanged",
        }));
    row.declared_summary["event_summary"]["count"] = json!(4);
    row.declared_summary["event_summary"]["by_kind"]["AliasChanged"] = json!(1);
    row
}

fn exact_name_row(
    logical_name_id: &str,
    surface_binding_id: Uuid,
    resource_id: Uuid,
    token_lineage_id: Uuid,
) -> bigname_storage::NameCurrentRow {
    bigname_storage::NameCurrentRow {
        logical_name_id: logical_name_id.to_owned(),
        namespace: "ens".to_owned(),
        canonical_display_name: "Alice.eth".to_owned(),
        normalized_name: "alice.eth".to_owned(),
        namehash: "namehash:alice.eth".to_owned(),
        surface_binding_id: Some(surface_binding_id),
        resource_id: Some(resource_id),
        token_lineage_id: Some(token_lineage_id),
        binding_kind: Some(bigname_storage::SurfaceBindingKind::DeclaredRegistryPath),
        declared_summary: json!({
            "registration": {
                "status": "active",
                "authority_kind": "registrar"
            },
            "resolver": {
                "chain_id": "ethereum-mainnet",
                "address": "0x0000000000000000000000000000000000000abc",
                "latest_event_kind": "ResolverChanged"
            }
        }),
        provenance: json!({
            "normalized_event_ids": [101, 102],
            "raw_fact_refs": [
                {
                    "kind": "log",
                    "chain_id": "ethereum-mainnet",
                    "block_hash": "0xabc"
                }
            ],
            "manifest_versions": [
                {
                    "manifest_version": 3,
                    "source_family": "ens_v1_registry",
                    "chain": "ethereum-mainnet",
                    "deployment_epoch": "ens_v1"
                }
            ],
            "execution_trace_id": null,
            "derivation_kind": "projection_apply"
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["ensv1_registry_path"],
            "unsupported_reason": null,
            "enumeration_basis": "exact_name"
        }),
        chain_positions: json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbinding",
                "timestamp": "2026-04-17T00:00:03Z"
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                "ethereum-mainnet": "finalized"
            }
        }),
        manifest_version: 3,
        last_recomputed_at: timestamp(1_717_171_717),
    }
}

fn record_inventory_boundary_with_pointer(
    logical_name_id: &str,
    resource_id: Uuid,
    normalized_event_id: Option<i64>,
    event_kind: Option<&str>,
) -> Value {
    json!({
        "logical_name_id": logical_name_id,
        "resource_id": resource_id.to_string(),
        "normalized_event_id": normalized_event_id,
        "event_kind": event_kind,
        "chain_position": {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_000_003,
            "block_hash": "0xbinding",
            "timestamp": "2026-04-17T00:00:03Z"
        }
    })
}

fn record_inventory_boundary(logical_name_id: &str, resource_id: Uuid) -> Value {
    record_inventory_boundary_with_pointer(logical_name_id, resource_id, None, None)
}

fn record_inventory_current_row(
    logical_name_id: &str,
    resource_id: Uuid,
) -> bigname_storage::RecordInventoryCurrentRow {
    bigname_storage::RecordInventoryCurrentRow {
        resource_id,
        record_version_boundary: record_inventory_boundary(logical_name_id, resource_id),
        enumeration_basis: json!({
            "observed_selectors": true,
            "capability_declared_families": true,
            "globally_enumerable": false
        }),
        selectors: json!([
            {
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "cacheable": true
            },
            {
                "record_key": "avatar",
                "record_family": "avatar",
                "selector_key": null,
                "cacheable": true
            },
            {
                "record_key": "text:com.twitter",
                "record_family": "text",
                "selector_key": "com.twitter",
                "cacheable": false
            }
        ]),
        explicit_gaps: json!([
            {
                "record_key": "contenthash",
                "record_family": "contenthash",
                "selector_key": null,
                "gap_reason": "not_observed_on_current_resolver"
            }
        ]),
        unsupported_families: json!([
            {
                "record_family": "abi",
                "unsupported_reason": "resolver_family_pending"
            },
            {
                "record_family": "pubkey",
                "unsupported_reason": "resolver_family_pending"
            }
        ]),
        last_change: Some(json!({
            "normalized_event_id": 1200,
            "event_kind": "RecordsChanged",
            "chain_position": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xlastchange",
                "timestamp": "2026-04-17T00:00:04Z"
            }
        })),
        entries: json!([
            {
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x0000000000000000000000000000000000000abc"
                }
            },
            {
                "record_key": "avatar",
                "record_family": "avatar",
                "selector_key": null,
                "status": "unsupported",
                "unsupported_reason": "resolver_family_pending"
            }
        ]),
        provenance: json!({
            "normalized_event_ids": [1200],
            "derivation_kind": "record_inventory_current_rebuild"
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "enumeration_basis": "declared_record_inventory"
        }),
        chain_positions: json!({
            "ethereum-mainnet": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbinding",
                "timestamp": "2026-04-17T00:00:03Z"
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                "ethereum-mainnet": "finalized"
            }
        }),
        manifest_version: 3,
        last_recomputed_at: timestamp(1_717_171_718),
    }
}

fn worker_record_inventory_current_row(
    logical_name_id: &str,
    resource_id: Uuid,
) -> bigname_storage::RecordInventoryCurrentRow {
    bigname_storage::RecordInventoryCurrentRow {
        resource_id,
        record_version_boundary: record_inventory_boundary_with_pointer(
            logical_name_id,
            resource_id,
            Some(1201),
            Some("RecordVersionChanged"),
        ),
        enumeration_basis: json!({
            "observed_selectors": true,
            "capability_declared_families": true,
            "globally_enumerable": false
        }),
        selectors: json!([
            {
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "cacheable": true
            },
            {
                "record_key": "text",
                "record_family": "text",
                "selector_key": null,
                "cacheable": true
            }
        ]),
        explicit_gaps: json!([]),
        unsupported_families: json!([]),
        last_change: Some(json!({
            "normalized_event_id": 1202,
            "event_kind": "RecordChanged",
            "chain_position": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_004,
                "block_hash": "0xlastchange",
                "timestamp": "2026-04-17T00:00:04Z"
            }
        })),
        entries: json!([
            {
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "status": "unsupported",
                "unsupported_reason": "value_not_retained_in_normalized_events"
            },
            {
                "record_key": "text",
                "record_family": "text",
                "selector_key": null,
                "status": "unsupported",
                "unsupported_reason": "value_not_retained_in_normalized_events"
            }
        ]),
        provenance: json!({
            "normalized_event_ids": [1201, 1202],
            "derivation_kind": "record_inventory_current_rebuild"
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "enumeration_basis": "declared_record_inventory"
        }),
        chain_positions: json!({
            "ethereum-mainnet": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbinding",
                "timestamp": "2026-04-17T00:00:03Z"
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                "ethereum-mainnet": "finalized"
            }
        }),
        manifest_version: 3,
        last_recomputed_at: timestamp(1_717_171_719),
    }
}

fn resolution_execution_requested_chain_positions() -> Value {
    json!([{
        "chain_id": "ethereum-mainnet",
        "block_number": 21_000_003,
        "block_hash": "0xbinding"
    }])
}

fn resolution_execution_request_key(records: &[&str]) -> String {
    let mut records = records
        .iter()
        .map(|record| (*record).to_owned())
        .collect::<Vec<_>>();
    records.sort_unstable();
    format!("ens:alice.eth:{}", records.join(","))
}

fn resolution_execution_trace(
    execution_trace_id: Uuid,
    request_key: &str,
    request_record_keys: &[&str],
    verified_queries: Value,
) -> ExecutionTrace {
    ExecutionTrace {
        execution_trace_id,
        request_type: VERIFIED_RESOLUTION_REQUEST_TYPE.to_owned(),
        request_key: request_key.to_owned(),
        namespace: "ens".to_owned(),
        chain_context: json!({
            "requested_positions": resolution_execution_requested_chain_positions(),
        }),
        manifest_context: json!({
            "manifest_versions": [{
                "source_family": "ens_execution",
                "manifest_version": 5
            }]
        }),
        contracts_called: json!([
            {
                "chain_id": "ethereum-mainnet",
                "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe",
                "selector": "0x9061b923"
            }
        ]),
        gateway_digests: json!([]),
        final_payload: Some(json!({
            "verified_queries": verified_queries.clone()
        })),
        failure_payload: None,
        request_metadata: json!({
            "surface": "alice.eth",
            "record_keys": request_record_keys,
            "entrypoint": "universal_resolver",
            "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe"
        }),
        finished_at: Some(timestamp(1_717_171_900)),
        steps: vec![
            ExecutionTraceStep {
                step_index: 0,
                step_kind: "load_declared_topology".to_owned(),
                input_digest: Some("sha256:topology-input".to_owned()),
                output_digest: Some("sha256:topology-output".to_owned()),
                latency_ms: Some(4),
                canonicality_dependency: json!({
                    "ethereum-mainnet": {
                        "block_hash": "0xbinding",
                        "block_number": 21_000_003,
                        "state": "finalized"
                    }
                }),
                step_payload: json!({
                    "entrypoint": "universal_resolver",
                    "resolver": "0x0000000000000000000000000000000000000abc"
                }),
            },
            ExecutionTraceStep {
                step_index: 1,
                step_kind: "call_universal_resolver".to_owned(),
                input_digest: Some("sha256:resolver-input".to_owned()),
                output_digest: Some("sha256:resolver-output".to_owned()),
                latency_ms: Some(28),
                canonicality_dependency: json!({
                    "ethereum-mainnet": {
                        "block_hash": "0xbinding",
                        "block_number": 21_000_003,
                        "state": "finalized"
                    }
                }),
                step_payload: json!({
                    "name": "alice.eth",
                    "record_count": 2
                }),
            },
        ],
    }
}

fn resolution_execution_outcome(
    execution_trace_id: Uuid,
    request_key: &str,
    verified_queries: Value,
    logical_name_id: &str,
    resource_id: Uuid,
) -> ExecutionOutcome {
    resolution_execution_outcome_with_boundaries(
        execution_trace_id,
        request_key,
        verified_queries,
        record_inventory_boundary(logical_name_id, resource_id),
        record_inventory_boundary(logical_name_id, resource_id),
    )
}

fn resolution_execution_outcome_with_boundaries(
    execution_trace_id: Uuid,
    request_key: &str,
    verified_queries: Value,
    topology_version_boundary: Value,
    record_version_boundary: Value,
) -> ExecutionOutcome {
    ExecutionOutcome {
        cache_key: ExecutionCacheKey {
            request_key: request_key.to_owned(),
            requested_chain_positions: resolution_execution_requested_chain_positions(),
            manifest_versions: json!([
                {
                    "manifest_version": 3,
                    "source_family": "ens_v1_registry",
                    "chain": "ethereum-mainnet",
                    "deployment_epoch": "ens_v1"
                }
            ]),
            topology_version_boundary,
            record_version_boundary,
        },
        execution_trace_id,
        request_type: VERIFIED_RESOLUTION_REQUEST_TYPE.to_owned(),
        namespace: "ens".to_owned(),
        outcome_payload: Some(json!({
            "verified_queries": verified_queries
        })),
        failure_payload: None,
        finished_at: timestamp(1_717_171_900),
    }
}

fn primary_name_execution_requested_chain_positions() -> Value {
    json!([{
        "chain_id": "ethereum-mainnet",
        "block_number": 21_000_010,
        "block_hash": "0xprimary"
    }])
}

fn primary_name_execution_manifest_versions_for_namespace(namespace: &str) -> Value {
    match namespace {
        "ens" => json!([{
            "manifest_version": 3,
            "source_family": "ens_execution"
        }]),
        "basenames" => json!([{
            "manifest_version": 4,
            "source_family": "basenames_execution"
        }]),
        other => panic!("unsupported primary-name test namespace {other}"),
    }
}

fn primary_name_topology_version_boundary() -> Value {
    record_inventory_boundary(
        "ens:alice.eth",
        Uuid::from_u128(0x0e7ec7ace0000000000000000000bbb1),
    )
}

fn primary_name_record_version_boundary() -> Value {
    record_inventory_boundary(
        "ens:alice.eth",
        Uuid::from_u128(0x0e7ec7ace0000000000000000000bbb2),
    )
}

fn primary_name_execution_request_key(namespace: &str, address: &str, coin_type: &str) -> String {
    format!("{namespace}:{}:{coin_type}", address.to_ascii_lowercase())
}

fn primary_name_execution_trace(
    execution_trace_id: Uuid,
    namespace: &str,
    address: &str,
    coin_type: &str,
    verified_primary_name: Value,
    finished_at: OffsetDateTime,
) -> ExecutionTrace {
    let normalized_address = address.to_ascii_lowercase();
    let manifest_versions = primary_name_execution_manifest_versions_for_namespace(namespace);
    let status = verified_primary_name
        .get("status")
        .and_then(Value::as_str)
        .expect("verified_primary_name payload must include string status");
    let (contracts_called, gateway_digests, steps) = match (namespace, status) {
        ("ens", "success" | "mismatch" | "execution_failed") => (
            json!([{
                "chain_id": "ethereum-mainnet",
                "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe",
                "selector": "0x9061b923"
            }]),
            json!([]),
            vec![ExecutionTraceStep {
                step_index: 0,
                step_kind: "call_universal_resolver".to_owned(),
                input_digest: Some("sha256:primary-input".to_owned()),
                output_digest: Some("sha256:primary-output".to_owned()),
                latency_ms: Some(14),
                canonicality_dependency: json!({
                    "ethereum-mainnet": {
                        "block_hash": "0xprimary",
                        "block_number": 21_000_010,
                        "state": "finalized"
                    }
                }),
                step_payload: json!({
                    "address": normalized_address,
                    "coin_type": coin_type
                }),
            }],
        ),
        ("basenames", "success" | "mismatch" | "execution_failed") => (
            json!([{
                "chain_id": "ethereum-mainnet",
                "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31",
                "selector": "0x9061b923"
            }]),
            json!(["sha256:basenames-primary-name"]),
            vec![
                ExecutionTraceStep {
                    step_index: 0,
                    step_kind: "call_l1_resolver".to_owned(),
                    input_digest: Some("sha256:primary-input".to_owned()),
                    output_digest: Some("sha256:primary-output".to_owned()),
                    latency_ms: Some(14),
                    canonicality_dependency: json!({
                        "ethereum-mainnet": {
                            "block_hash": "0xprimary",
                            "block_number": 21_000_010,
                            "state": "finalized"
                        }
                    }),
                    step_payload: json!({
                        "address": normalized_address,
                        "coin_type": coin_type
                    }),
                },
                ExecutionTraceStep {
                    step_index: 1,
                    step_kind: "complete_offchain_lookup".to_owned(),
                    input_digest: Some("sha256:gateway-input".to_owned()),
                    output_digest: Some("sha256:gateway-output".to_owned()),
                    latency_ms: Some(19),
                    canonicality_dependency: json!({
                        "ethereum-mainnet": {
                            "block_hash": "0xprimary",
                            "block_number": 21_000_010,
                            "state": "finalized"
                        }
                    }),
                    step_payload: json!({
                        "gateway": "https://basenames.example.test"
                    }),
                },
            ],
        ),
        ("ens" | "basenames", "not_found" | "unsupported") => (
            json!([]),
            json!([]),
            vec![ExecutionTraceStep {
                step_index: 0,
                step_kind: "load_primary_name_claim".to_owned(),
                input_digest: Some("sha256:claim-input".to_owned()),
                output_digest: Some("sha256:claim-output".to_owned()),
                latency_ms: Some(2),
                canonicality_dependency: json!({
                    "ethereum-mainnet": {
                        "block_hash": "0xprimary",
                        "block_number": 21_000_010,
                        "state": "finalized"
                    }
                }),
                step_payload: json!({
                    "address": normalized_address,
                    "coin_type": coin_type
                }),
            }],
        ),
        ("ens" | "basenames", "invalid_name") => (
            json!([]),
            json!([]),
            vec![
                ExecutionTraceStep {
                    step_index: 0,
                    step_kind: "load_primary_name_claim".to_owned(),
                    input_digest: Some("sha256:claim-input".to_owned()),
                    output_digest: Some("sha256:claim-output".to_owned()),
                    latency_ms: Some(2),
                    canonicality_dependency: json!({
                        "ethereum-mainnet": {
                            "block_hash": "0xprimary",
                            "block_number": 21_000_010,
                            "state": "finalized"
                        }
                    }),
                    step_payload: json!({
                        "address": normalized_address,
                        "coin_type": coin_type
                    }),
                },
                ExecutionTraceStep {
                    step_index: 1,
                    step_kind: "normalize_claimed_name".to_owned(),
                    input_digest: Some("sha256:normalize-input".to_owned()),
                    output_digest: Some("sha256:normalize-output".to_owned()),
                    latency_ms: Some(1),
                    canonicality_dependency: json!({
                        "ethereum-mainnet": {
                            "block_hash": "0xprimary",
                            "block_number": 21_000_010,
                            "state": "finalized"
                        }
                    }),
                    step_payload: json!({
                        "normalizer_version": "ensip15@ens-normalize-0.1.1",
                        "error": "claim_name_not_normalizable"
                    }),
                },
            ],
        ),
        (other, _) if other != "ens" && other != "basenames" => {
            panic!("unsupported primary-name test namespace {other}")
        }
        (_, other) => panic!("unsupported primary-name test status {other}"),
    };
    ExecutionTrace {
        execution_trace_id,
        request_type: bigname_storage::VERIFIED_PRIMARY_NAME_REQUEST_TYPE.to_owned(),
        request_key: primary_name_execution_request_key(namespace, &normalized_address, coin_type),
        namespace: namespace.to_owned(),
        chain_context: json!({
            "requested_positions": primary_name_execution_requested_chain_positions(),
        }),
        manifest_context: json!({
            "manifest_versions": manifest_versions,
        }),
        contracts_called,
        gateway_digests,
        final_payload: Some(json!({
            "verified_primary_name": verified_primary_name.clone()
        })),
        failure_payload: None,
        request_metadata: json!({
            "normalized_address": normalized_address,
            "coin_type": coin_type,
            "namespace": namespace,
            "cache_identity": {
                "requested_chain_positions": primary_name_execution_requested_chain_positions(),
                "manifest_versions": manifest_versions,
                "topology_version_boundary": primary_name_topology_version_boundary(),
                "record_version_boundary": primary_name_record_version_boundary(),
            }
        }),
        finished_at: Some(finished_at),
        steps,
    }
}

fn primary_name_execution_outcome(
    execution_trace_id: Uuid,
    namespace: &str,
    address: &str,
    coin_type: &str,
    verified_primary_name: Value,
    finished_at: OffsetDateTime,
) -> ExecutionOutcome {
    let normalized_address = address.to_ascii_lowercase();
    ExecutionOutcome {
        cache_key: ExecutionCacheKey {
            request_key: primary_name_execution_request_key(
                namespace,
                &normalized_address,
                coin_type,
            ),
            requested_chain_positions: primary_name_execution_requested_chain_positions(),
            manifest_versions: primary_name_execution_manifest_versions_for_namespace(namespace),
            topology_version_boundary: primary_name_topology_version_boundary(),
            record_version_boundary: primary_name_record_version_boundary(),
        },
        execution_trace_id,
        request_type: bigname_storage::VERIFIED_PRIMARY_NAME_REQUEST_TYPE.to_owned(),
        namespace: namespace.to_owned(),
        outcome_payload: Some(json!({
            "verified_primary_name": verified_primary_name
        })),
        failure_payload: None,
        finished_at,
    }
}

#[allow(clippy::too_many_arguments)]
fn address_name_name_current_row(
    logical_name_id: &str,
    canonical_display_name: &str,
    normalized_name: &str,
    namehash: &str,
    surface_binding_id: Uuid,
    resource_id: Uuid,
    token_lineage_id: Option<Uuid>,
    block_number: i64,
    declared_summary: Value,
) -> bigname_storage::NameCurrentRow {
    bigname_storage::NameCurrentRow {
        logical_name_id: logical_name_id.to_owned(),
        namespace: logical_name_id
            .split_once(':')
            .map(|(namespace, _)| namespace)
            .expect("logical_name_id must include namespace")
            .to_owned(),
        canonical_display_name: canonical_display_name.to_owned(),
        normalized_name: normalized_name.to_owned(),
        namehash: namehash.to_owned(),
        surface_binding_id: Some(surface_binding_id),
        resource_id: Some(resource_id),
        token_lineage_id,
        binding_kind: Some(bigname_storage::SurfaceBindingKind::DeclaredRegistryPath),
        declared_summary,
        provenance: json!({
            "normalized_event_ids": [block_number, block_number + 1],
            "raw_fact_refs": [{
                "kind": "raw_log",
                "block_number": block_number,
            }],
            "manifest_versions": [{
                "manifest_version": 3,
                "source_family": "ens_v1_registry",
                "chain": "ethereum-mainnet",
                "deployment_epoch": "ens_v1",
            }],
            "execution_trace_id": null,
            "derivation_kind": "projection_apply",
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["ensv1_registry_path"],
            "unsupported_reason": null,
            "enumeration_basis": "exact_name",
        }),
        chain_positions: json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": block_number,
                "block_hash": format!("0xname{block_number:02x}"),
                "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60),
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                "ethereum-mainnet": "finalized"
            }
        }),
        manifest_version: 3,
        last_recomputed_at: timestamp(1_717_175_000 + block_number),
    }
}

fn collection_name_surface(
    logical_name_id: &str,
    display_name: &str,
    namehash: &str,
    block_number: i64,
) -> NameSurface {
    let namespace = logical_name_id
        .split_once(':')
        .map(|(namespace, _)| namespace)
        .expect("logical_name_id must include namespace")
        .to_owned();
    let chain_id = chain_id_for_namespace(&namespace).to_owned();

    NameSurface {
        logical_name_id: logical_name_id.to_owned(),
        namespace,
        input_name: display_name.to_owned(),
        canonical_display_name: display_name.to_owned(),
        normalized_name: display_name.to_owned(),
        dns_encoded_name: display_name.as_bytes().to_vec(),
        namehash: namehash.to_owned(),
        labelhashes: labelhash_for_display_name(display_name)
            .into_iter()
            .collect(),
        normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id,
        block_hash: format!("0xsurface{block_number:02x}"),
        block_number,
        provenance: json!({"seed": "children_surface"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn declared_child_row(
    parent_logical_name_id: &str,
    child_logical_name_id: &str,
    display_name: &str,
    namehash: &str,
    normalized_event_id: i64,
    block_number: i64,
) -> bigname_storage::ChildrenCurrentRow {
    let namespace = parent_logical_name_id
        .split_once(':')
        .map(|(namespace, _)| namespace)
        .expect("parent_logical_name_id must include namespace");
    let chain_id = chain_id_for_namespace(namespace);
    let chain_slot = chain_slot_for_namespace(namespace);

    bigname_storage::ChildrenCurrentRow {
        parent_logical_name_id: parent_logical_name_id.to_owned(),
        child_logical_name_id: child_logical_name_id.to_owned(),
        surface_class: "declared".to_owned(),
        namespace: namespace.to_owned(),
        canonical_display_name: display_name.to_owned(),
        normalized_name: display_name.to_owned(),
        namehash: namehash.to_owned(),
        labelhash: labelhash_for_display_name(display_name),
        owner: None,
        registrant: None,
        provenance: json!({
            "normalized_event_ids": [normalized_event_id],
            "raw_fact_refs": [{
                "kind": "raw_log",
                "block_number": block_number,
            }],
            "manifest_versions": [{
                "manifest_version": 1,
                "source_family": source_family_for_namespace(namespace),
                "source_manifest_id": null,
            }],
            "execution_trace_id": null,
            "derivation_kind": "children_current_rebuild",
        }),
        chain_positions: json!({
            chain_slot: {
                "chain_id": chain_id,
                "block_number": block_number,
                "block_hash": format!("0xblock{block_number:02x}"),
                "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60),
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                chain_id: "finalized"
            }
        }),
        manifest_version: 1,
        last_recomputed_at: timestamp(1_717_172_000 + block_number),
    }
}

fn labelhash_for_display_name(display_name: &str) -> Option<String> {
    display_name
        .split('.')
        .next()
        .filter(|label| !label.is_empty())
        .map(|label| {
            bigname_storage::label_preimage_from_label(label, "api_test", 1, json!({}))
                .expect("test label must hash")
                .labelhash
        })
}

fn chain_id_for_namespace(namespace: &str) -> &'static str {
    match namespace {
        "basenames" => "base-mainnet",
        _ => "ethereum-mainnet",
    }
}

fn chain_slot_for_namespace(namespace: &str) -> &'static str {
    match namespace {
        "basenames" => "base",
        _ => "ethereum",
    }
}

fn source_family_for_namespace(namespace: &str) -> &'static str {
    match namespace {
        "basenames" => "basenames_base_registry",
        _ => "ens_v1_registry_l1",
    }
}

fn address_name_token_lineage(
    token_lineage_id: Uuid,
    block_hash: &str,
    block_number: i64,
) -> TokenLineage {
    TokenLineage {
        token_lineage_id,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        provenance: json!({"seed": "address_name_token_lineage"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn address_name_resource(
    resource_id: Uuid,
    token_lineage_id: Option<Uuid>,
    block_hash: &str,
    block_number: i64,
) -> Resource {
    Resource {
        resource_id,
        token_lineage_id,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        provenance: json!({"seed": "address_name_resource"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn address_name_surface_binding(
    surface_binding_id: Uuid,
    logical_name_id: &str,
    resource_id: Uuid,
    block_hash: &str,
    block_number: i64,
    active_from: i64,
) -> SurfaceBinding {
    SurfaceBinding {
        surface_binding_id,
        logical_name_id: logical_name_id.to_owned(),
        resource_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        active_from: timestamp(active_from),
        active_to: None,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        provenance: json!({"seed": "address_name_binding"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

#[allow(clippy::too_many_arguments)]
fn address_name_current_row(
    address: &str,
    logical_name_id: &str,
    relation: bigname_storage::AddressNameRelation,
    display_name: &str,
    normalized_name: &str,
    namehash: &str,
    surface_binding_id: Uuid,
    resource_id: Uuid,
    token_lineage_id: Option<Uuid>,
    block_number: i64,
) -> bigname_storage::AddressNameCurrentRow {
    bigname_storage::AddressNameCurrentRow {
        address: address.to_owned(),
        logical_name_id: logical_name_id.to_owned(),
        relation,
        namespace: logical_name_id
            .split_once(':')
            .map(|(namespace, _)| namespace)
            .expect("logical_name_id must include namespace")
            .to_owned(),
        canonical_display_name: display_name.to_owned(),
        normalized_name: normalized_name.to_owned(),
        namehash: namehash.to_owned(),
        surface_binding_id,
        resource_id,
        token_lineage_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        provenance: json!({
            "normalized_event_ids": [block_number],
            "raw_fact_refs": [{
                "kind": "raw_log",
                "block_number": block_number,
            }],
            "manifest_versions": [{
                "manifest_version": 3,
                "source_family": "ens_v1_registrar_l1",
                "source_manifest_id": null,
            }],
            "execution_trace_id": null,
            "derivation_kind": "address_names_current_rebuild",
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["ensv1_registry_path"],
            "unsupported_reason": null,
            "enumeration_basis": "surface_current_relations",
        }),
        chain_positions: json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": block_number,
                "block_hash": format!("0xaddr{block_number:02x}"),
                "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60),
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                "ethereum-mainnet": "finalized"
            }
        }),
        manifest_version: 3,
        last_recomputed_at: timestamp(1_717_173_000 + block_number),
    }
}

fn compact_name_declared_summary(
    owner: &str,
    registrant: &str,
    resolver: &str,
    expiry: i64,
    registered_at: &str,
    created_at: &str,
) -> Value {
    json!({
        "registration": {
            "status": "active",
            "registrant": registrant,
            "expiry": expiry,
            "registered_at": registered_at,
            "created_at": created_at,
        },
        "control": {
            "registry_owner": owner,
            "registrant": registrant,
            "expiry": expiry,
        },
        "resolver": {
            "chain_id": "ethereum-mainnet",
            "address": resolver,
            "latest_event_kind": "ResolverChanged",
        }
    })
}

fn compact_records_inventory_current_row(
    logical_name_id: &str,
    resource_id: Uuid,
) -> bigname_storage::RecordInventoryCurrentRow {
    let mut row = record_inventory_current_row(logical_name_id, resource_id);
    row.selectors = json!([
        {
            "record_key": "addr:0",
            "record_family": "addr",
            "selector_key": "0",
            "cacheable": true,
        },
        {
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "cacheable": true,
        },
        {
            "record_key": "avatar",
            "record_family": "avatar",
            "selector_key": null,
            "cacheable": true,
        },
        {
            "record_key": "contenthash",
            "record_family": "contenthash",
            "selector_key": null,
            "cacheable": true,
        },
        {
            "record_key": "text:com.twitter",
            "record_family": "text",
            "selector_key": "com.twitter",
            "cacheable": true,
        },
    ]);
    row.explicit_gaps = json!([]);
    row.entries = json!([
        {
            "record_key": "addr:0",
            "record_family": "addr",
            "selector_key": "0",
            "status": "not_found",
        },
        {
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x0000000000000000000000000000000000000abc",
            },
        },
        {
            "record_key": "avatar",
            "record_family": "avatar",
            "selector_key": null,
            "status": "success",
            "value": { "value": "ipfs://avatar" },
        },
        {
            "record_key": "contenthash",
            "record_family": "contenthash",
            "selector_key": null,
            "status": "success",
            "value": { "value": "ipfs://content" },
        },
        {
            "record_key": "text:com.twitter",
            "record_family": "text",
            "selector_key": "com.twitter",
            "status": "success",
            "value": {
                "key": "com.twitter",
                "value": "@alice",
            },
        },
    ]);
    row
}

#[allow(clippy::too_many_arguments)]
async fn seed_identity_name(
    database: &TestDatabase,
    logical_name_id: &str,
    display_name: &str,
    normalized_name: &str,
    namehash: &str,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
    address: &str,
    relation: bigname_storage::AddressNameRelation,
    block_number: i64,
) -> Result<()> {
    let name_row = address_name_name_current_row(
        logical_name_id,
        display_name,
        normalized_name,
        namehash,
        surface_binding_id,
        resource_id,
        Some(token_lineage_id),
        block_number,
        compact_name_declared_summary(
            address,
            address,
            address,
            1_900_000_000,
            "2026-04-17T00:00:21Z",
            "2026-04-17T00:00:11Z",
        ),
    );
    let publication_positions = name_row.chain_positions.clone();
    let mut inventory = compact_records_inventory_current_row(logical_name_id, resource_id);
    inventory.chain_positions = publication_positions.clone();
    let address_row = address_name_current_row(
        address,
        logical_name_id,
        relation,
        display_name,
        normalized_name,
        namehash,
        surface_binding_id,
        resource_id,
        Some(token_lineage_id),
        block_number,
    );

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database.insert_name_current_row(name_row.clone()).await?;
    database
        .insert_record_inventory_current_row(inventory.clone())
        .await?;
    bigname_storage::upsert_address_names_current_rows(
        &database.pool,
        std::slice::from_ref(&address_row),
    )
    .await?;
    seed_phase_identity_name(
        database,
        display_name,
        normalized_name,
        resource_id,
        token_lineage_id,
        surface_binding_id,
        address,
        relation,
        &name_row.declared_summary,
        &inventory,
    )
    .await?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn seed_phase_identity_name(
    database: &TestDatabase,
    display_name: &str,
    normalized_name: &str,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
    address: &str,
    relation: bigname_storage::AddressNameRelation,
    declared_summary: &Value,
    inventory: &bigname_storage::RecordInventoryCurrentRow,
) -> Result<()> {
    let namespace = if normalized_name.ends_with(".base.eth") && normalized_name != "base.eth" {
        "basenames"
    } else {
        "ens"
    };
    let namehash = bigname_lookup::ens_namehash_hex(normalized_name)?;
    let logical_name_id = format!("{namespace}:{namehash}");
    let normalized = bigname_domain::normalization::normalize_name(display_name)
        .map_err(|error| anyhow::anyhow!(error.message().to_owned()))?;
    let labelhashes = normalized
        .normalized_labels
        .iter()
        .map(|label| {
            bigname_storage::label_preimage_from_label(label, "api_test", 1, json!({}))
                .map(|preimage| preimage.labelhash)
        })
        .collect::<Result<Vec<_>>>()?;
    let chain_id = if namespace == "basenames" {
        "base-mainnet"
    } else {
        "ethereum-mainnet"
    };
    let (block_hash, block_number, timestamp, timestamp_text): (
        String,
        i64,
        OffsetDateTime,
        String,
    ) = sqlx::query_as(
        r#"
        SELECT head.latest_block_hash, head.latest_block_number, lineage.block_timestamp,
               to_char(
                   lineage.block_timestamp AT TIME ZONE 'UTC',
                   'YYYY-MM-DD"T"HH24:MI:SS"Z"'
               )
        FROM chain_heads head
        JOIN chain_lineage lineage
          ON lineage.chain_id = head.chain_id
         AND lineage.block_hash = head.latest_block_hash
         AND lineage.block_number = head.latest_block_number
        WHERE head.chain_id = $1
        "#,
    )
    .bind(chain_id)
    .fetch_one(&database.lookup_pool)
    .await?;
    let slot = if chain_id == "base-mainnet" { "base" } else { "ethereum" };
    let publication_positions = json!({
        slot: {
            "chain_id": chain_id,
            "block_number": block_number,
            "block_hash": block_hash,
            "timestamp": timestamp_text,
        }
    });
    let target_positions = json!({
        "target_block_number": block_number,
        "target_block_hash": block_hash,
    });
    let boundary_key = format!(
        "{logical_name_id}:{resource_id}:0:{block_number}:{block_hash}"
    );

    let mut transaction = database.lookup_pool.begin().await?;
    sqlx::query(
        r#"
        INSERT INTO token_lineages (
            token_lineage_id, chain_id, block_hash, block_number,
            provenance, canonicality_state
        ) VALUES ($1, $2, $3, $4, '{}'::jsonb, 'finalized')
        ON CONFLICT (token_lineage_id) DO NOTHING
        "#,
    )
    .bind(token_lineage_id)
    .bind(chain_id)
    .bind(&block_hash)
    .bind(block_number)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO resources (
            resource_id, token_lineage_id, chain_id, block_hash, block_number,
            provenance, canonicality_state
        ) VALUES ($1, $2, $3, $4, $5, '{}'::jsonb, 'finalized')
        ON CONFLICT (resource_id) DO NOTHING
        "#,
    )
    .bind(resource_id)
    .bind(token_lineage_id)
    .bind(chain_id)
    .bind(&block_hash)
    .bind(block_number)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO name_surfaces (
            logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
            namehash, labelhashes, normalizer_version, visibility_state,
            normalization_errors, chain_id, block_hash, block_number,
            provenance, canonicality_state
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, 'active', '[]'::jsonb,
            $9, $10, $11, '{}'::jsonb, 'finalized'
        ) ON CONFLICT (logical_name_id) DO NOTHING
        "#,
    )
    .bind(&logical_name_id)
    .bind(namespace)
    .bind(&normalized.normalized_name)
    .bind(&normalized.normalized_labels)
    .bind(&normalized.dns_encoded_name)
    .bind(&namehash)
    .bind(labelhashes)
    .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
    .bind(chain_id)
    .bind(&block_hash)
    .bind(block_number)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO surface_bindings (
            surface_binding_id, logical_name_id, resource_id, binding_kind,
            active_from, chain_id, block_hash, block_number, provenance,
            canonicality_state
        ) VALUES (
            $1, $2, $3, 'declared_registry_path', $4, $5, $6, $7,
            '{}'::jsonb, 'finalized'
        ) ON CONFLICT (surface_binding_id) DO NOTHING
        "#,
    )
    .bind(surface_binding_id)
    .bind(&logical_name_id)
    .bind(resource_id)
    .bind(timestamp)
    .bind(chain_id)
    .bind(&block_hash)
    .bind(block_number)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO name_current (
            logical_name_id, namespace, raw_name, namehash, surface_binding_id,
            resource_id, token_lineage_id, binding_kind, declared_summary,
            support_status, provenance, chain_positions, canonicality_summary,
            manifest_version
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, 'declared_registry_path', $8,
            'supported', '{}'::jsonb, $9, $10, 1
        ) ON CONFLICT (logical_name_id) DO UPDATE SET
            declared_summary = EXCLUDED.declared_summary,
            chain_positions = EXCLUDED.chain_positions
        "#,
    )
    .bind(&logical_name_id)
    .bind(namespace)
    .bind(&normalized.normalized_name)
    .bind(&namehash)
    .bind(surface_binding_id)
    .bind(resource_id)
    .bind(token_lineage_id)
    .bind(declared_summary)
    .bind(&publication_positions)
    .bind(json!({
        "state": "canonical_lineage",
        "target_block_number": block_number,
        "target_block_hash": block_hash,
    }))
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO record_inventory_current (
            resource_id, record_version_boundary_key, record_version_boundary,
            selectors, unsupported_families, entries, support_status,
            provenance, chain_positions, canonicality_summary, manifest_version
        ) VALUES ($1, $2, $3, $4, $5, $6, 'supported', '{}'::jsonb, $7, $8, 1)
        ON CONFLICT (resource_id, record_version_boundary_key) DO UPDATE SET
            entries = EXCLUDED.entries,
            chain_positions = EXCLUDED.chain_positions
        "#,
    )
    .bind(resource_id)
    .bind(boundary_key)
    .bind(&inventory.record_version_boundary)
    .bind(&inventory.selectors)
    .bind(&inventory.unsupported_families)
    .bind(&inventory.entries)
    .bind(&target_positions)
    .bind(json!({
        "state": "canonical_lineage",
        "target_block_number": block_number,
        "target_block_hash": block_hash,
    }))
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO address_names_current (
            address, logical_name_id, relation, namespace, raw_name, namehash,
            surface_binding_id, resource_id, token_lineage_id, binding_kind,
            support_status, provenance, chain_positions, canonicality_summary,
            manifest_version
        ) VALUES (
            lower($1), $2, $3, $4, $5, $6, $7, $8, $9,
            'declared_registry_path', 'supported', '{}'::jsonb, $10, $11, 1
        ) ON CONFLICT (address, logical_name_id, relation) DO UPDATE SET
            chain_positions = EXCLUDED.chain_positions
        "#,
    )
    .bind(address)
    .bind(&logical_name_id)
    .bind(relation.as_str())
    .bind(namespace)
    .bind(&normalized.normalized_name)
    .bind(&namehash)
    .bind(surface_binding_id)
    .bind(resource_id)
    .bind(token_lineage_id)
    .bind(target_positions)
    .bind(json!({
        "state": "canonical_lineage",
        "target_block_number": block_number,
        "target_block_hash": block_hash,
    }))
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(())
}

async fn seed_phase_primary_name_snapshot(
    database: &TestDatabase,
    address: &str,
    namespace: &str,
    coin_type: &str,
    claim_status: bigname_storage::PrimaryNameClaimStatus,
    raw_claim_name: Option<&str>,
) -> Result<()> {
    let chain_id = if namespace == "basenames" {
        "base-mainnet"
    } else {
        "ethereum-mainnet"
    };
    let (block_number, block_hash): (i64, String) = sqlx::query_as(
        "SELECT latest_block_number, latest_block_hash
         FROM chain_heads
         WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_one(&database.lookup_pool)
    .await?;
    let claim_provenance = json!({
        "chain_id": chain_id,
        "target_block_number": block_number,
        "target_block_hash": block_hash,
    });
    let status = match claim_status {
        bigname_storage::PrimaryNameClaimStatus::Success => "success",
        bigname_storage::PrimaryNameClaimStatus::NotFound => "not_found",
        bigname_storage::PrimaryNameClaimStatus::Unsupported => "unsupported",
        bigname_storage::PrimaryNameClaimStatus::InvalidName => "invalid_name",
    };
    let unsupported_reason = (status == "unsupported").then_some("unsupported_test_claim");
    sqlx::query(
        r#"
        INSERT INTO primary_names_current (
            address, coin_type, namespace, claim_status, raw_claim_name,
            claim_name_is_normalized, unsupported_reason, claim_provenance
        ) VALUES (lower($1), $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (address, coin_type, namespace) DO UPDATE SET
            claim_status = EXCLUDED.claim_status,
            raw_claim_name = EXCLUDED.raw_claim_name,
            claim_name_is_normalized = EXCLUDED.claim_name_is_normalized,
            unsupported_reason = EXCLUDED.unsupported_reason,
            claim_provenance = EXCLUDED.claim_provenance
        "#,
    )
    .bind(address)
    .bind(coin_type)
    .bind(namespace)
    .bind(status)
    .bind(raw_claim_name)
    .bind(status == "success")
    .bind(unsupported_reason)
    .bind(claim_provenance)
    .execute(&database.lookup_pool)
    .await?;
    Ok(())
}

fn basenames_execution_manifest_version() -> Value {
    json!({
        "source_family": "basenames_execution",
        "manifest_version": 2,
        "chain": "ethereum-mainnet",
        "deployment_epoch": "basenames_v1",
    })
}

fn basenames_dynamic_resolver_record_inventory_boundary(
    logical_name_id: &str,
    resource_id: Uuid,
    normalized_event_id: Option<i64>,
    event_kind: Option<&str>,
) -> Value {
    json!({
        "logical_name_id": logical_name_id,
        "resource_id": resource_id.to_string(),
        "normalized_event_id": normalized_event_id,
        "event_kind": event_kind,
        "chain_position": {
            "chain_id": "base-mainnet",
            "block_number": 21_000_003,
            "block_hash": "0xbase-binding",
            "timestamp": "2026-04-17T00:00:03Z",
        }
    })
}

fn basenames_l2resolver_record_inventory_current_row(
    logical_name_id: &str,
    resource_id: Uuid,
) -> bigname_storage::RecordInventoryCurrentRow {
    bigname_storage::RecordInventoryCurrentRow {
        resource_id,
        record_version_boundary: basenames_dynamic_resolver_record_inventory_boundary(
            logical_name_id,
            resource_id,
            Some(1201),
            Some("RecordChanged"),
        ),
        enumeration_basis: json!({
            "observed_selectors": true,
            "capability_declared_families": true,
            "globally_enumerable": false,
        }),
        selectors: json!([{
            "record_key": "text",
            "record_family": "text",
            "selector_key": null,
            "cacheable": true,
        }]),
        explicit_gaps: json!([]),
        unsupported_families: json!([]),
        last_change: Some(json!({
            "normalized_event_id": 1201,
            "event_kind": "RecordChanged",
            "chain_position": {
                "chain_id": "base-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbase-binding",
                "timestamp": "2026-04-17T00:00:03Z",
            }
        })),
        entries: json!([{
            "record_key": "text",
            "record_family": "text",
            "selector_key": null,
            "status": "unsupported",
            "unsupported_reason": "value_not_retained_in_normalized_events",
        }]),
        provenance: json!({
            "normalized_event_ids": [1201],
            "derivation_kind": "record_inventory_current_rebuild",
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": [
                "basenames_base_registry",
                "basenames_base_resolver",
            ],
            "unsupported_reason": null,
            "enumeration_basis": "declared_record_inventory",
        }),
        chain_positions: json!({
            "base-mainnet": {
                "chain_id": "base-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbase-binding",
                "timestamp": "2026-04-17T00:00:03Z",
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": { "base-mainnet": "finalized" }
        }),
        manifest_version: 6,
        last_recomputed_at: timestamp(1_717_171_719),
    }
}

fn primary_name_universal_resolver_addr60_response(address: &str) -> Value {
    json!(format!(
        "0x{}{}{}{}",
        primary_name_left_pad_hex("40", 64),
        primary_name_padded_address_hex("0xa2c122be93b0074270ebee7f6b7292c7deb45047"),
        primary_name_left_pad_hex("20", 64),
        primary_name_padded_address_hex(address),
    ))
}

fn primary_name_reverse_name_response(name: &str) -> Value {
    let name_hex = name
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let padded_name_hex_len = name_hex.len().next_multiple_of(64);
    json!(format!(
        "0x{}{}{}",
        primary_name_left_pad_hex("20", 64),
        primary_name_left_pad_hex(&format!("{:x}", name.len()), 64),
        format!("{name_hex:0<padded_name_hex_len$}"),
    ))
}

fn primary_name_padded_address_hex(address: &str) -> String {
    let stripped = address
        .strip_prefix("0x")
        .expect("test address must be 0x-prefixed");
    assert_eq!(stripped.len(), 40, "test address must be 20 bytes");
    primary_name_left_pad_hex(stripped, 64)
}

fn primary_name_left_pad_hex(value: &str, width: usize) -> String {
    assert!(value.len() <= width, "test hex value must fit padded width");
    format!("{value:0>width$}")
}

async fn spawn_primary_name_mock_rpc(
    responses: Vec<Value>,
) -> Result<(String, tokio::task::JoinHandle<Result<Vec<Value>>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind mock primary-name RPC listener")?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let mut requests = Vec::new();
        for response in responses {
            let (mut socket, _) = listener
                .accept()
                .await
                .context("failed to accept mock primary-name RPC request")?;
            requests.push(read_primary_name_mock_rpc_request(&mut socket).await?);
            write_primary_name_mock_rpc_response(&mut socket, response).await?;
        }
        Ok(requests)
    });
    Ok((url, handle))
}

async fn spawn_hanging_primary_name_rpc()
-> Result<(String, tokio::task::JoinHandle<Result<()>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind hanging mock primary-name RPC listener")?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let (mut socket, _) = listener
            .accept()
            .await
            .context("failed to accept hanging mock primary-name RPC request")?;
        read_primary_name_mock_rpc_request(&mut socket).await?;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        Ok(())
    });
    Ok((url, handle))
}

async fn spawn_primary_name_mock_rpc_with_last_response_gate(
    responses: Vec<Value>,
) -> Result<(
    String,
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<Vec<Value>>>,
)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind gated mock primary-name RPC listener")?;
    let url = format!("http://{}", listener.local_addr()?);
    let (request_reached_tx, request_reached_rx) = tokio::sync::oneshot::channel();
    let (release_response_tx, release_response_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        let response_count = responses.len();
        let mut requests = Vec::new();
        let mut request_reached_tx = Some(request_reached_tx);
        let mut release_response_rx = Some(release_response_rx);
        for (index, response) in responses.into_iter().enumerate() {
            let (mut socket, _) = listener
                .accept()
                .await
                .context("failed to accept gated mock primary-name RPC request")?;
            requests.push(read_primary_name_mock_rpc_request(&mut socket).await?);
            if index + 1 == response_count {
                request_reached_tx
                    .take()
                    .context("gated RPC reached its last request twice")?
                    .send(())
                    .map_err(|_| anyhow::anyhow!("gated RPC request receiver dropped"))?;
                release_response_rx
                    .take()
                    .context("gated RPC release receiver missing")?
                    .await
                    .context("gated RPC release sender dropped")?;
            }
            write_primary_name_mock_rpc_response(&mut socket, response).await?;
        }
        Ok(requests)
    });
    Ok((url, request_reached_rx, release_response_tx, handle))
}

async fn read_primary_name_mock_rpc_request(
    socket: &mut tokio::net::TcpStream,
) -> Result<Value> {
    use tokio::io::AsyncReadExt;

    let mut buffer = Vec::new();
    let mut scratch = [0_u8; 1024];
    let (body_start, content_length) = loop {
        let bytes_read = socket
            .read(&mut scratch)
            .await
            .context("failed to read mock primary-name RPC request")?;
        if bytes_read == 0 {
            anyhow::bail!("mock primary-name RPC request closed before headers finished");
        }
        buffer.extend_from_slice(&scratch[..bytes_read]);
        if let Some(body_start) = primary_name_mock_header_end(&buffer) {
            let headers = std::str::from_utf8(&buffer[..body_start])
                .context("mock primary-name RPC request headers were not utf8")?;
            break (body_start, primary_name_mock_content_length(headers)?);
        }
    };
    while buffer.len() < body_start + content_length {
        let bytes_read = socket
            .read(&mut scratch)
            .await
            .context("failed to read mock primary-name RPC request body")?;
        if bytes_read == 0 {
            anyhow::bail!("mock primary-name RPC request closed before body finished");
        }
        buffer.extend_from_slice(&scratch[..bytes_read]);
    }
    serde_json::from_slice(&buffer[body_start..body_start + content_length])
        .context("failed to parse mock primary-name RPC request body")
}

fn primary_name_mock_header_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| position + 4)
}

fn primary_name_mock_content_length(headers: &str) -> Result<usize> {
    headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>())
        })
        .transpose()
        .context("mock primary-name RPC request content-length was invalid")?
        .with_context(|| "mock primary-name RPC request did not include content-length")
}

async fn write_primary_name_mock_rpc_response(
    socket: &mut tokio::net::TcpStream,
    result: Value,
) -> Result<()> {
    use tokio::io::AsyncWriteExt;

    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": result,
    })
    .to_string();
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    socket
        .write_all(response.as_bytes())
        .await
        .context("failed to write mock primary-name RPC response")
}

async fn join_primary_name_mock_rpc_requests(
    handle: tokio::task::JoinHandle<Result<Vec<Value>>>,
) -> Result<Vec<Value>> {
    handle
        .await
        .context("mock primary-name RPC task panicked or was cancelled")?
}
