use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder, Row, postgres::PgRow, types::time::OffsetDateTime};
use uuid::Uuid;

use crate::CanonicalityState;

/// Anchor selection for normalized-event history reads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryScope {
    Surface,
    Resource,
    Both,
}

impl HistoryScope {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Surface => "surface",
            Self::Resource => "resource",
            Self::Both => "both",
        }
    }
}

/// Replay-stable normalized event exposed to history readers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryEvent {
    pub normalized_event_id: i64,
    pub event_identity: String,
    pub namespace: String,
    pub logical_name_id: Option<String>,
    pub resource_id: Option<Uuid>,
    pub event_kind: String,
    pub source_family: String,
    pub manifest_version: i64,
    pub source_manifest_id: Option<i64>,
    pub chain_id: Option<String>,
    pub block_number: Option<i64>,
    pub block_hash: Option<String>,
    pub block_timestamp: Option<OffsetDateTime>,
    pub transaction_hash: Option<String>,
    pub log_index: Option<i64>,
    pub raw_fact_ref: Value,
    pub derivation_kind: String,
    pub canonicality_state: CanonicalityState,
    pub before_state: Value,
    pub after_state: Value,
    pub provenance: Value,
    pub coverage: Value,
}

/// Load history rows for one logical name anchor.
pub async fn load_name_history(
    pool: &PgPool,
    logical_name_id: &str,
    resource_ids: &[Uuid],
    scope: HistoryScope,
    canonical_only: bool,
) -> Result<Vec<HistoryEvent>> {
    load_history(
        pool,
        name_history_selector(logical_name_id, resource_ids, scope),
        canonical_only,
    )
    .await
    .with_context(|| {
        format!(
            "failed to load history for logical_name_id {logical_name_id} with scope {}",
            scope.as_str()
        )
    })
}

/// Load history rows for one resource anchor.
pub async fn load_resource_history(
    pool: &PgPool,
    resource_id: Uuid,
    logical_name_ids: &[String],
    scope: HistoryScope,
    canonical_only: bool,
) -> Result<Vec<HistoryEvent>> {
    load_history(
        pool,
        resource_history_selector(resource_id, logical_name_ids, scope),
        canonical_only,
    )
    .await
    .with_context(|| {
        format!(
            "failed to load history for resource_id {resource_id} with scope {}",
            scope.as_str()
        )
    })
}

#[derive(Clone, Debug)]
enum HistorySelector {
    None,
    LogicalNames(Vec<String>),
    Resources(Vec<Uuid>),
    LogicalNamesOrResources {
        logical_name_ids: Vec<String>,
        resource_ids: Vec<Uuid>,
    },
}

impl HistorySelector {
    fn logical_names(logical_name_ids: Vec<String>) -> Self {
        if logical_name_ids.is_empty() {
            Self::None
        } else {
            Self::LogicalNames(logical_name_ids)
        }
    }

    fn resources(resource_ids: Vec<Uuid>) -> Self {
        if resource_ids.is_empty() {
            Self::None
        } else {
            Self::Resources(resource_ids)
        }
    }

    fn logical_names_or_resources(logical_name_ids: Vec<String>, resource_ids: Vec<Uuid>) -> Self {
        match (logical_name_ids.is_empty(), resource_ids.is_empty()) {
            (true, true) => Self::None,
            (false, true) => Self::LogicalNames(logical_name_ids),
            (true, false) => Self::Resources(resource_ids),
            (false, false) => Self::LogicalNamesOrResources {
                logical_name_ids,
                resource_ids,
            },
        }
    }
}

fn name_history_selector(
    logical_name_id: &str,
    resource_ids: &[Uuid],
    scope: HistoryScope,
) -> HistorySelector {
    let logical_name_ids = vec![logical_name_id.to_owned()];
    let resource_ids = resource_ids.to_vec();

    match scope {
        HistoryScope::Surface => HistorySelector::logical_names(logical_name_ids),
        HistoryScope::Resource => HistorySelector::resources(resource_ids),
        HistoryScope::Both => {
            HistorySelector::logical_names_or_resources(logical_name_ids, resource_ids)
        }
    }
}

fn resource_history_selector(
    resource_id: Uuid,
    logical_name_ids: &[String],
    scope: HistoryScope,
) -> HistorySelector {
    let logical_name_ids = logical_name_ids.to_vec();
    let resource_ids = vec![resource_id];

    match scope {
        HistoryScope::Surface => HistorySelector::logical_names(logical_name_ids),
        HistoryScope::Resource => HistorySelector::resources(resource_ids),
        HistoryScope::Both => {
            HistorySelector::logical_names_or_resources(logical_name_ids, resource_ids)
        }
    }
}

async fn load_history(
    pool: &PgPool,
    selector: HistorySelector,
    canonical_only: bool,
) -> Result<Vec<HistoryEvent>> {
    if matches!(selector, HistorySelector::None) {
        return Ok(Vec::new());
    }

    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        SELECT
            ne.normalized_event_id,
            ne.event_identity,
            ne.namespace,
            ne.logical_name_id,
            ne.resource_id,
            ne.event_kind,
            ne.source_family,
            ne.manifest_version,
            ne.source_manifest_id,
            ne.chain_id,
            ne.block_number,
            ne.block_hash,
            rb.block_timestamp,
            ne.transaction_hash,
            ne.log_index,
            ne.raw_fact_ref,
            ne.derivation_kind,
            ne.canonicality_state::TEXT AS canonicality_state,
            ne.before_state,
            ne.after_state,
            COALESCE(
                CASE
                    WHEN jsonb_typeof(ne.after_state -> 'provenance') = 'object'
                        THEN ne.after_state -> 'provenance'
                END,
                CASE
                    WHEN jsonb_typeof(ne.before_state -> 'provenance') = 'object'
                        THEN ne.before_state -> 'provenance'
                END,
                '{}'::jsonb
            ) AS provenance,
            COALESCE(
                CASE
                    WHEN jsonb_typeof(ne.after_state -> 'coverage') = 'object'
                        THEN ne.after_state -> 'coverage'
                END,
                CASE
                    WHEN jsonb_typeof(ne.before_state -> 'coverage') = 'object'
                        THEN ne.before_state -> 'coverage'
                END,
                '{}'::jsonb
            ) AS coverage
        FROM normalized_events ne
        LEFT JOIN raw_blocks rb
          ON rb.chain_id = ne.chain_id
         AND rb.block_hash = ne.block_hash
        WHERE
        "#,
    );

    match &selector {
        HistorySelector::LogicalNames(logical_name_ids) => {
            push_string_filter(&mut builder, "ne.logical_name_id", logical_name_ids);
        }
        HistorySelector::Resources(resource_ids) => {
            push_uuid_filter(&mut builder, "ne.resource_id", resource_ids);
        }
        HistorySelector::LogicalNamesOrResources {
            logical_name_ids,
            resource_ids,
        } => {
            builder.push("(");
            push_string_filter(&mut builder, "ne.logical_name_id", logical_name_ids);
            builder.push(" OR ");
            push_uuid_filter(&mut builder, "ne.resource_id", resource_ids);
            builder.push(")");
        }
        HistorySelector::None => unreachable!("none selector handled before query build"),
    }

    if canonical_only {
        builder.push(
            r#"
            AND ne.canonicality_state IN (
                'canonical'::canonicality_state,
                'safe'::canonicality_state,
                'finalized'::canonicality_state
            )
            "#,
        );
    }

    builder.push(
        r#"
        ORDER BY
            CASE WHEN ne.block_number IS NULL THEN 1 ELSE 0 END,
            ne.block_number DESC,
            CASE WHEN ne.chain_id IS NULL THEN 1 ELSE 0 END,
            ne.chain_id ASC,
            CASE WHEN ne.block_hash IS NULL THEN 1 ELSE 0 END,
            ne.block_hash DESC,
            CASE WHEN ne.transaction_hash IS NULL THEN 1 ELSE 0 END,
            ne.transaction_hash DESC,
            COALESCE(ne.log_index, -1) DESC,
            ne.event_identity DESC
        "#,
    );

    let rows = builder
        .build()
        .fetch_all(pool)
        .await
        .context("failed to fetch normalized-event history rows")?;

    rows.into_iter().map(decode_history_event).collect()
}

fn push_string_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    column: &str,
    values: &'a [String],
) {
    builder.push(column);
    push_string_filter_tail(builder, values);
}

fn push_string_filter_tail<'a>(builder: &mut QueryBuilder<'a, Postgres>, values: &'a [String]) {
    builder.push(" IN (");
    let mut separated = builder.separated(", ");
    for value in values {
        separated.push_bind(value);
    }
    separated.push_unseparated(")");
}

fn push_uuid_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    column: &str,
    values: &'a [Uuid],
) {
    builder.push(column);
    push_uuid_filter_tail(builder, values);
}

fn push_uuid_filter_tail<'a>(builder: &mut QueryBuilder<'a, Postgres>, values: &'a [Uuid]) {
    builder.push(" IN (");
    let mut separated = builder.separated(", ");
    for value in values {
        separated.push_bind(value);
    }
    separated.push_unseparated(")");
}

fn decode_history_event(row: PgRow) -> Result<HistoryEvent> {
    let provenance: Value = row.try_get("provenance").context("missing provenance")?;
    let coverage: Value = row.try_get("coverage").context("missing coverage")?;
    ensure_json_object(&provenance, "provenance")?;
    ensure_json_object(&coverage, "coverage")?;

    Ok(HistoryEvent {
        normalized_event_id: row
            .try_get("normalized_event_id")
            .context("missing normalized_event_id")?,
        event_identity: row
            .try_get("event_identity")
            .context("missing event_identity")?,
        namespace: row.try_get("namespace").context("missing namespace")?,
        logical_name_id: row
            .try_get("logical_name_id")
            .context("missing logical_name_id")?,
        resource_id: row.try_get("resource_id").context("missing resource_id")?,
        event_kind: row.try_get("event_kind").context("missing event_kind")?,
        source_family: row
            .try_get("source_family")
            .context("missing source_family")?,
        manifest_version: row
            .try_get("manifest_version")
            .context("missing manifest_version")?,
        source_manifest_id: row
            .try_get("source_manifest_id")
            .context("missing source_manifest_id")?,
        chain_id: row.try_get("chain_id").context("missing chain_id")?,
        block_number: row
            .try_get("block_number")
            .context("missing block_number")?,
        block_hash: row.try_get("block_hash").context("missing block_hash")?,
        block_timestamp: row
            .try_get("block_timestamp")
            .context("missing block_timestamp")?,
        transaction_hash: row
            .try_get("transaction_hash")
            .context("missing transaction_hash")?,
        log_index: row.try_get("log_index").context("missing log_index")?,
        raw_fact_ref: row
            .try_get("raw_fact_ref")
            .context("missing raw_fact_ref")?,
        derivation_kind: row
            .try_get("derivation_kind")
            .context("missing derivation_kind")?,
        canonicality_state: CanonicalityState::parse(
            &row.try_get::<String, _>("canonicality_state")
                .context("missing canonicality_state")?,
        )?,
        before_state: row
            .try_get("before_state")
            .context("missing before_state")?,
        after_state: row.try_get("after_state").context("missing after_state")?,
        provenance,
        coverage,
    })
}

fn ensure_json_object(value: &Value, field_name: &str) -> Result<()> {
    if !value.is_object() {
        bail!("history field {field_name} must be a JSON object");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        str::FromStr,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use anyhow::Result;
    use serde_json::json;
    use sqlx::{
        PgPool,
        postgres::{PgConnectOptions, PgPoolOptions},
    };

    use super::*;
    use crate::{
        NameSurface, NormalizedEvent, RawBlock, Resource, SurfaceBinding, SurfaceBindingKind,
        default_database_url, upsert_name_surfaces, upsert_normalized_events, upsert_raw_blocks,
        upsert_resources, upsert_surface_bindings,
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
                .context("failed to parse database URL for history tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bigname_storage_history_test_{}_{}_{}",
                std::process::id(),
                unique,
                sequence
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for history tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect history test pool")?;

            crate::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for history tests")?;

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

    fn timestamp(unix_timestamp: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(unix_timestamp).expect("valid unix timestamp")
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
        let normalized_name = logical_name_id
            .split_once(':')
            .map(|(_, normalized_name)| normalized_name)
            .expect("logical_name_id must include namespace");

        NameSurface {
            logical_name_id: logical_name_id.to_owned(),
            namespace: "ens".to_owned(),
            input_name: normalized_name.to_owned(),
            canonical_display_name: "Alice.eth".to_owned(),
            normalized_name: normalized_name.to_owned(),
            dns_encoded_name: vec![5, b'a', b'l', b'i', b'c', b'e'],
            namehash: format!("namehash:{normalized_name}"),
            labelhashes: vec!["labelhash:alice".to_owned()],
            normalizer_version: "uts46-v1".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: "ethereum-mainnet".to_owned(),
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
                "transaction_index": transaction_hash.map(|_| 3),
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
                    "event_identity": event_identity,
                }
            }),
        }
    }

    #[tokio::test]
    async fn canonical_only_history_excludes_observed_and_orphaned_rows() -> Result<()> {
        let database = TestDatabase::new().await?;
        let resource_id = Uuid::from_u128(0xa001);

        upsert_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0x100", None, 100, 1_700_000_100),
                raw_block(
                    "ethereum-mainnet",
                    "0x101",
                    Some("0x100"),
                    101,
                    1_700_000_101,
                ),
                raw_block(
                    "ethereum-mainnet",
                    "0x102",
                    Some("0x101"),
                    102,
                    1_700_000_102,
                ),
                raw_block(
                    "ethereum-mainnet",
                    "0x103",
                    Some("0x102"),
                    103,
                    1_700_000_103,
                ),
            ],
        )
        .await?;

        upsert_normalized_events(
            database.pool(),
            &[
                history_event(
                    "history:canonical",
                    Some("ens:alice.eth"),
                    Some(resource_id),
                    Some("ethereum-mainnet"),
                    Some(100),
                    Some("0x100"),
                    Some("0xtx100"),
                    Some(0),
                    CanonicalityState::Canonical,
                ),
                history_event(
                    "history:safe",
                    Some("ens:alice.eth"),
                    Some(resource_id),
                    Some("ethereum-mainnet"),
                    Some(101),
                    Some("0x101"),
                    Some("0xtx101"),
                    Some(0),
                    CanonicalityState::Safe,
                ),
                history_event(
                    "history:finalized",
                    Some("ens:alice.eth"),
                    Some(resource_id),
                    Some("ethereum-mainnet"),
                    Some(102),
                    Some("0x102"),
                    Some("0xtx102"),
                    Some(0),
                    CanonicalityState::Finalized,
                ),
                history_event(
                    "history:observed",
                    Some("ens:alice.eth"),
                    Some(resource_id),
                    Some("ethereum-mainnet"),
                    Some(103),
                    Some("0x103"),
                    Some("0xtx103"),
                    Some(0),
                    CanonicalityState::Observed,
                ),
                history_event(
                    "history:orphaned",
                    Some("ens:alice.eth"),
                    Some(resource_id),
                    None,
                    None,
                    None,
                    None,
                    None,
                    CanonicalityState::Orphaned,
                ),
            ],
        )
        .await?;

        let canonical_only = load_name_history(
            database.pool(),
            "ens:alice.eth",
            &[resource_id],
            HistoryScope::Both,
            true,
        )
        .await?;

        assert_eq!(
            canonical_only
                .iter()
                .map(|row| row.event_identity.as_str())
                .collect::<Vec<_>>(),
            vec!["history:finalized", "history:safe", "history:canonical"]
        );

        let all_rows = load_name_history(
            database.pool(),
            "ens:alice.eth",
            &[resource_id],
            HistoryScope::Both,
            false,
        )
        .await?;
        assert_eq!(all_rows.len(), 5);

        database.cleanup().await
    }

    #[tokio::test]
    async fn name_history_scope_uses_logical_name_and_resource_filters() -> Result<()> {
        let database = TestDatabase::new().await?;
        let resource_id = Uuid::from_u128(0xa100);
        let other_resource_id = Uuid::from_u128(0xa101);

        upsert_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0x200", None, 200, 1_700_000_200),
                raw_block(
                    "ethereum-mainnet",
                    "0x201",
                    Some("0x200"),
                    201,
                    1_700_000_201,
                ),
                raw_block(
                    "ethereum-mainnet",
                    "0x202",
                    Some("0x201"),
                    202,
                    1_700_000_202,
                ),
                raw_block(
                    "ethereum-mainnet",
                    "0x203",
                    Some("0x202"),
                    203,
                    1_700_000_203,
                ),
                raw_block(
                    "ethereum-mainnet",
                    "0x204",
                    Some("0x203"),
                    204,
                    1_700_000_204,
                ),
            ],
        )
        .await?;

        upsert_normalized_events(
            database.pool(),
            &[
                history_event(
                    "surface-only",
                    Some("ens:alice.eth"),
                    None,
                    Some("ethereum-mainnet"),
                    Some(200),
                    Some("0x200"),
                    Some("0xtx200"),
                    Some(0),
                    CanonicalityState::Canonical,
                ),
                history_event(
                    "resource-only",
                    None,
                    Some(resource_id),
                    Some("ethereum-mainnet"),
                    Some(201),
                    Some("0x201"),
                    Some("0xtx201"),
                    Some(0),
                    CanonicalityState::Canonical,
                ),
                history_event(
                    "both-anchors",
                    Some("ens:alice.eth"),
                    Some(resource_id),
                    Some("ethereum-mainnet"),
                    Some(202),
                    Some("0x202"),
                    Some("0xtx202"),
                    Some(0),
                    CanonicalityState::Canonical,
                ),
                history_event(
                    "same-resource-other-name",
                    Some("ens:other.eth"),
                    Some(resource_id),
                    Some("ethereum-mainnet"),
                    Some(203),
                    Some("0x203"),
                    Some("0xtx203"),
                    Some(0),
                    CanonicalityState::Canonical,
                ),
                history_event(
                    "same-name-other-resource",
                    Some("ens:alice.eth"),
                    Some(other_resource_id),
                    Some("ethereum-mainnet"),
                    Some(204),
                    Some("0x204"),
                    Some("0xtx204"),
                    Some(0),
                    CanonicalityState::Canonical,
                ),
            ],
        )
        .await?;

        let surface_rows = load_name_history(
            database.pool(),
            "ens:alice.eth",
            &[resource_id],
            HistoryScope::Surface,
            true,
        )
        .await?;
        assert_eq!(
            surface_rows
                .iter()
                .map(|row| row.event_identity.as_str())
                .collect::<Vec<_>>(),
            vec!["same-name-other-resource", "both-anchors", "surface-only"]
        );

        let resource_rows = load_name_history(
            database.pool(),
            "ens:alice.eth",
            &[resource_id],
            HistoryScope::Resource,
            true,
        )
        .await?;
        assert_eq!(
            resource_rows
                .iter()
                .map(|row| row.event_identity.as_str())
                .collect::<Vec<_>>(),
            vec!["same-resource-other-name", "both-anchors", "resource-only"]
        );

        let both_rows = load_name_history(
            database.pool(),
            "ens:alice.eth",
            &[resource_id],
            HistoryScope::Both,
            true,
        )
        .await?;
        assert_eq!(
            both_rows
                .iter()
                .map(|row| row.event_identity.as_str())
                .collect::<Vec<_>>(),
            vec![
                "same-name-other-resource",
                "same-resource-other-name",
                "both-anchors",
                "resource-only",
                "surface-only",
            ]
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn name_history_resource_scope_preserves_rebound_resource_ids() -> Result<()> {
        let database = TestDatabase::new().await?;
        let logical_name_id = "ens:alice.eth";
        let old_resource_id = Uuid::from_u128(0xa110);
        let current_resource_id = Uuid::from_u128(0xa111);

        upsert_name_surfaces(database.pool(), &[name_surface(logical_name_id)]).await?;
        upsert_resources(
            database.pool(),
            &[resource(old_resource_id), resource(current_resource_id)],
        )
        .await?;
        upsert_surface_bindings(
            database.pool(),
            &[
                SurfaceBinding {
                    active_to: Some(timestamp(1_700_000_250)),
                    ..surface_binding(
                        Uuid::from_u128(0xb110),
                        logical_name_id,
                        old_resource_id,
                        timestamp(1_700_000_200),
                    )
                },
                surface_binding(
                    Uuid::from_u128(0xb111),
                    logical_name_id,
                    current_resource_id,
                    timestamp(1_700_000_251),
                ),
            ],
        )
        .await?;

        upsert_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0x210", None, 210, 1_700_000_210),
                raw_block(
                    "ethereum-mainnet",
                    "0x211",
                    Some("0x210"),
                    211,
                    1_700_000_211,
                ),
            ],
        )
        .await?;

        upsert_normalized_events(
            database.pool(),
            &[
                history_event(
                    "resource-old",
                    None,
                    Some(old_resource_id),
                    Some("ethereum-mainnet"),
                    Some(210),
                    Some("0x210"),
                    Some("0xtx210"),
                    Some(0),
                    CanonicalityState::Canonical,
                ),
                history_event(
                    "resource-current",
                    None,
                    Some(current_resource_id),
                    Some("ethereum-mainnet"),
                    Some(211),
                    Some("0x211"),
                    Some("0xtx211"),
                    Some(0),
                    CanonicalityState::Canonical,
                ),
            ],
        )
        .await?;

        let rows = load_name_history(
            database.pool(),
            logical_name_id,
            &[old_resource_id, current_resource_id],
            HistoryScope::Resource,
            true,
        )
        .await?;

        assert_eq!(
            rows.iter()
                .map(|row| row.event_identity.as_str())
                .collect::<Vec<_>>(),
            vec!["resource-current", "resource-old"]
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn resource_history_scope_uses_resource_and_logical_name_filters() -> Result<()> {
        let database = TestDatabase::new().await?;
        let resource_id = Uuid::from_u128(0xa200);
        let other_resource_id = Uuid::from_u128(0xa201);

        upsert_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0x300", None, 300, 1_700_000_300),
                raw_block(
                    "ethereum-mainnet",
                    "0x301",
                    Some("0x300"),
                    301,
                    1_700_000_301,
                ),
                raw_block(
                    "ethereum-mainnet",
                    "0x302",
                    Some("0x301"),
                    302,
                    1_700_000_302,
                ),
                raw_block(
                    "ethereum-mainnet",
                    "0x303",
                    Some("0x302"),
                    303,
                    1_700_000_303,
                ),
                raw_block(
                    "ethereum-mainnet",
                    "0x304",
                    Some("0x303"),
                    304,
                    1_700_000_304,
                ),
            ],
        )
        .await?;

        upsert_normalized_events(
            database.pool(),
            &[
                history_event(
                    "surface-only",
                    Some("ens:alice.eth"),
                    None,
                    Some("ethereum-mainnet"),
                    Some(300),
                    Some("0x300"),
                    Some("0xtx300"),
                    Some(0),
                    CanonicalityState::Canonical,
                ),
                history_event(
                    "resource-only",
                    None,
                    Some(resource_id),
                    Some("ethereum-mainnet"),
                    Some(301),
                    Some("0x301"),
                    Some("0xtx301"),
                    Some(0),
                    CanonicalityState::Canonical,
                ),
                history_event(
                    "both-anchors",
                    Some("ens:alice.eth"),
                    Some(resource_id),
                    Some("ethereum-mainnet"),
                    Some(302),
                    Some("0x302"),
                    Some("0xtx302"),
                    Some(0),
                    CanonicalityState::Canonical,
                ),
                history_event(
                    "same-resource-other-name",
                    Some("ens:other.eth"),
                    Some(resource_id),
                    Some("ethereum-mainnet"),
                    Some(303),
                    Some("0x303"),
                    Some("0xtx303"),
                    Some(0),
                    CanonicalityState::Canonical,
                ),
                history_event(
                    "same-name-other-resource",
                    Some("ens:alice.eth"),
                    Some(other_resource_id),
                    Some("ethereum-mainnet"),
                    Some(304),
                    Some("0x304"),
                    Some("0xtx304"),
                    Some(0),
                    CanonicalityState::Canonical,
                ),
            ],
        )
        .await?;

        let surface_rows = load_resource_history(
            database.pool(),
            resource_id,
            &["ens:alice.eth".to_owned()],
            HistoryScope::Surface,
            true,
        )
        .await?;
        assert_eq!(
            surface_rows
                .iter()
                .map(|row| row.event_identity.as_str())
                .collect::<Vec<_>>(),
            vec!["same-name-other-resource", "both-anchors", "surface-only"]
        );

        let resource_rows = load_resource_history(
            database.pool(),
            resource_id,
            &["ens:alice.eth".to_owned()],
            HistoryScope::Resource,
            true,
        )
        .await?;
        assert_eq!(
            resource_rows
                .iter()
                .map(|row| row.event_identity.as_str())
                .collect::<Vec<_>>(),
            vec!["same-resource-other-name", "both-anchors", "resource-only"]
        );

        let both_rows = load_resource_history(
            database.pool(),
            resource_id,
            &["ens:alice.eth".to_owned()],
            HistoryScope::Both,
            true,
        )
        .await?;
        assert_eq!(
            both_rows
                .iter()
                .map(|row| row.event_identity.as_str())
                .collect::<Vec<_>>(),
            vec![
                "same-name-other-resource",
                "same-resource-other-name",
                "both-anchors",
                "resource-only",
                "surface-only",
            ]
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn resource_history_surface_scope_preserves_multiple_bound_surfaces() -> Result<()> {
        let database = TestDatabase::new().await?;
        let resource_id = Uuid::from_u128(0xa220);
        let primary_logical_name_id = "ens:alice.eth";
        let alias_logical_name_id = "ens:alice-base.eth";

        upsert_name_surfaces(
            database.pool(),
            &[
                name_surface(primary_logical_name_id),
                name_surface(alias_logical_name_id),
            ],
        )
        .await?;
        upsert_resources(database.pool(), &[resource(resource_id)]).await?;
        upsert_surface_bindings(
            database.pool(),
            &[
                surface_binding(
                    Uuid::from_u128(0xb220),
                    primary_logical_name_id,
                    resource_id,
                    timestamp(1_700_000_300),
                ),
                surface_binding(
                    Uuid::from_u128(0xb221),
                    alias_logical_name_id,
                    resource_id,
                    timestamp(1_700_000_301),
                ),
            ],
        )
        .await?;

        upsert_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0x320", None, 320, 1_700_000_320),
                raw_block(
                    "ethereum-mainnet",
                    "0x321",
                    Some("0x320"),
                    321,
                    1_700_000_321,
                ),
            ],
        )
        .await?;

        upsert_normalized_events(
            database.pool(),
            &[
                history_event(
                    "surface-primary",
                    Some(primary_logical_name_id),
                    None,
                    Some("ethereum-mainnet"),
                    Some(320),
                    Some("0x320"),
                    Some("0xtx320"),
                    Some(0),
                    CanonicalityState::Canonical,
                ),
                history_event(
                    "surface-alias",
                    Some(alias_logical_name_id),
                    None,
                    Some("ethereum-mainnet"),
                    Some(321),
                    Some("0x321"),
                    Some("0xtx321"),
                    Some(0),
                    CanonicalityState::Canonical,
                ),
            ],
        )
        .await?;

        let rows = load_resource_history(
            database.pool(),
            resource_id,
            &[
                primary_logical_name_id.to_owned(),
                alias_logical_name_id.to_owned(),
            ],
            HistoryScope::Surface,
            true,
        )
        .await?;

        assert_eq!(
            rows.iter()
                .map(|row| row.event_identity.as_str())
                .collect::<Vec<_>>(),
            vec!["surface-alias", "surface-primary"]
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn history_reads_use_deterministic_chain_position_desc_ordering() -> Result<()> {
        let database = TestDatabase::new().await?;
        let resource_id = Uuid::from_u128(0xa300);

        upsert_raw_blocks(
            database.pool(),
            &[
                raw_block("base-mainnet", "0xb101", None, 101, 1_700_000_401),
                raw_block("ethereum-mainnet", "0xe100", None, 100, 1_700_000_400),
                raw_block("base-mainnet", "0xb100", Some("0xb101"), 100, 1_700_000_399),
            ],
        )
        .await?;

        upsert_normalized_events(
            database.pool(),
            &[
                history_event(
                    "no-chain-position",
                    Some("ens:alice.eth"),
                    Some(resource_id),
                    None,
                    None,
                    None,
                    None,
                    None,
                    CanonicalityState::Canonical,
                ),
                history_event(
                    "ethereum-lower-log",
                    Some("ens:alice.eth"),
                    Some(resource_id),
                    Some("ethereum-mainnet"),
                    Some(100),
                    Some("0xe100"),
                    Some("0xtx100"),
                    Some(1),
                    CanonicalityState::Canonical,
                ),
                history_event(
                    "ethereum-higher-log",
                    Some("ens:alice.eth"),
                    Some(resource_id),
                    Some("ethereum-mainnet"),
                    Some(100),
                    Some("0xe100"),
                    Some("0xtx100"),
                    Some(7),
                    CanonicalityState::Canonical,
                ),
                history_event(
                    "base-same-height",
                    Some("ens:alice.eth"),
                    Some(resource_id),
                    Some("base-mainnet"),
                    Some(100),
                    Some("0xb100"),
                    Some("0xtx090"),
                    Some(0),
                    CanonicalityState::Canonical,
                ),
                history_event(
                    "base-higher-height",
                    Some("ens:alice.eth"),
                    Some(resource_id),
                    Some("base-mainnet"),
                    Some(101),
                    Some("0xb101"),
                    Some("0xtx101"),
                    Some(0),
                    CanonicalityState::Canonical,
                ),
            ],
        )
        .await?;

        let rows = load_name_history(
            database.pool(),
            "ens:alice.eth",
            &[resource_id],
            HistoryScope::Both,
            true,
        )
        .await?;

        assert_eq!(
            rows.iter()
                .map(|row| row.event_identity.as_str())
                .collect::<Vec<_>>(),
            vec![
                "base-higher-height",
                "base-same-height",
                "ethereum-higher-log",
                "ethereum-lower-log",
                "no-chain-position",
            ]
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn history_rows_expose_object_provenance_and_coverage_payloads() -> Result<()> {
        let database = TestDatabase::new().await?;
        let resource_id = Uuid::from_u128(0xa400);

        upsert_normalized_events(
            database.pool(),
            &[
                history_event(
                    "with-payload",
                    Some("ens:alice.eth"),
                    Some(resource_id),
                    None,
                    None,
                    None,
                    None,
                    None,
                    CanonicalityState::Canonical,
                ),
                NormalizedEvent {
                    after_state: json!({
                        "coverage": "invalid-scalar"
                    }),
                    before_state: json!({
                        "provenance": {
                            "fallback": true,
                        }
                    }),
                    ..history_event(
                        "payload-defaults",
                        Some("ens:alice.eth"),
                        Some(resource_id),
                        None,
                        None,
                        None,
                        None,
                        None,
                        CanonicalityState::Canonical,
                    )
                },
            ],
        )
        .await?;

        let rows = load_name_history(
            database.pool(),
            "ens:alice.eth",
            &[resource_id],
            HistoryScope::Both,
            true,
        )
        .await?;

        let with_payload = rows
            .iter()
            .find(|row| row.event_identity == "with-payload")
            .context("missing with-payload row")?;
        assert_eq!(with_payload.provenance, json!({"after": "with-payload"}));
        assert_eq!(
            with_payload.coverage,
            json!({
                "status": "full",
                "event_identity": "with-payload",
            })
        );

        let defaults = rows
            .iter()
            .find(|row| row.event_identity == "payload-defaults")
            .context("missing payload-defaults row")?;
        assert_eq!(defaults.provenance, json!({"fallback": true}));
        assert_eq!(defaults.coverage, json!({}));

        database.cleanup().await
    }
}
