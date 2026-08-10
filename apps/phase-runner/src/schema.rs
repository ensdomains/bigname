use std::collections::BTreeSet;

use anyhow::{Context, Result, bail, ensure};
use sqlx::PgPool;

const SCHEMA_INSTALL_LOCK_ID: i64 = 7_312_026_073_000_001;
pub const PHASE_SCHEMA_NAME: &str = "bigname_phase";

const EXPECTED_TABLES: &[&str] = &[
    "address_names_current",
    "chain_heads",
    "chain_header_audit",
    "chain_lineage",
    "chain_phase_state",
    "children_current",
    "contract_instance_addresses",
    "contract_instances",
    "discovery_edges",
    "ens_names",
    "ingest_cursors",
    "label_preimages",
    "manifest_contract_instances",
    "manifest_discovery_rules",
    "manifest_authority_attestations",
    "manifest_versions",
    "name_current",
    "name_surfaces",
    "normalized_events",
    "permissions_current",
    "permissions_current_resource_summary",
    "primary_names_current",
    "raw_logs",
    "raw_receipts",
    "raw_transactions",
    "record_inventory_current",
    "resolution_divergences",
    "resolver_current",
    "resources",
    "service_heartbeats",
    "surface_bindings",
    "token_lineages",
];

const BASELINE: &[(&str, &str)] = &[
    (
        "chain",
        include_str!("../../../schema-v2/baseline/01_chain.sql"),
    ),
    (
        "raw facts",
        include_str!("../../../schema-v2/baseline/02_raw_facts.sql"),
    ),
    (
        "identity",
        include_str!("../../../schema-v2/baseline/03_identity.sql"),
    ),
    (
        "manifests",
        include_str!("../../../schema-v2/baseline/04_manifests.sql"),
    ),
    (
        "normalized events",
        include_str!("../../../schema-v2/baseline/05_normalized_events.sql"),
    ),
    (
        "projections",
        include_str!("../../../schema-v2/baseline/06_projections.sql"),
    ),
    (
        "labels",
        include_str!("../../../schema-v2/baseline/07_labels.sql"),
    ),
    (
        "heartbeats",
        include_str!("../../../schema-v2/baseline/08_heartbeats.sql"),
    ),
    (
        "resolution differences",
        include_str!("../../../schema-v2/baseline/09_divergence.sql"),
    ),
    (
        "phase state",
        include_str!("../../../schema-v2/baseline/10_phase_state.sql"),
    ),
    (
        "manifest authority attestations",
        include_str!("../../../schema-v2/baseline/11_manifest_authority_attestations.sql"),
    ),
];

/// Install the fresh schema-v2 baseline into an empty phase schema.
///
/// Until schema-v2 has an upgrade mechanism, this installer refuses
/// every nonempty `bigname_phase` schema rather than treating a matching table
/// list as proof that the table definitions are current.
pub async fn initialize_schema_v2(pool: &PgPool) -> Result<()> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to begin fresh-schema initialization")?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(SCHEMA_INSTALL_LOCK_ID)
        .execute(&mut *transaction)
        .await
        .context("failed to lock fresh-schema initialization")?;

    sqlx::query("CREATE SCHEMA IF NOT EXISTS bigname_phase")
        .execute(&mut *transaction)
        .await
        .context("failed to create the phase schema")?;
    ensure!(
        !schema_has_objects(&mut transaction).await?,
        "schema-v2 initialization requires an empty {PHASE_SCHEMA_NAME} schema; existing schemas must be replaced through a reviewed upgrade or rebuild"
    );
    sqlx::query("SET LOCAL search_path TO bigname_phase, public")
        .execute(&mut *transaction)
        .await
        .context("failed to select the phase schema for initialization")?;

    for (name, sql) in BASELINE {
        sqlx::raw_sql(sql)
            .execute(&mut *transaction)
            .await
            .with_context(|| format!("failed to apply schema-v2 {name} baseline"))?;
    }

    let installed = load_base_tables(&mut transaction).await?;
    require_exact_inventory(&installed, "after initialization")?;
    transaction
        .commit()
        .await
        .context("failed to commit fresh-schema initialization")?;
    Ok(())
}

async fn schema_has_objects(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<bool> {
    sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM pg_class relation
            JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = $1
            UNION ALL
            SELECT 1
            FROM pg_proc function
            JOIN pg_namespace namespace ON namespace.oid = function.pronamespace
            WHERE namespace.nspname = $1
            UNION ALL
            SELECT 1
            FROM pg_type type
            JOIN pg_namespace namespace ON namespace.oid = type.typnamespace
            WHERE namespace.nspname = $1
        )
        "#,
    )
    .bind(PHASE_SCHEMA_NAME)
    .fetch_one(&mut **transaction)
    .await
    .context("failed to inspect the phase schema")
}

async fn load_base_tables(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<BTreeSet<String>> {
    Ok(sqlx::query_scalar(
        r#"
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = $1
          AND table_type = 'BASE TABLE'
        "#,
    )
    .bind(PHASE_SCHEMA_NAME)
    .fetch_all(&mut **transaction)
    .await
    .context("failed to inspect the target database schema")?
    .into_iter()
    .collect())
}

fn require_exact_inventory(actual: &BTreeSet<String>, context: &str) -> Result<()> {
    let expected = EXPECTED_TABLES
        .iter()
        .map(|table| (*table).to_owned())
        .collect::<BTreeSet<_>>();
    if actual == &expected {
        return Ok(());
    }

    let missing = expected.difference(actual).cloned().collect::<Vec<_>>();
    let unexpected = actual.difference(&expected).cloned().collect::<Vec<_>>();
    ensure!(!actual.is_empty(), "schema-v2 inventory is empty {context}");
    bail!(
        "schema-v2 installation produced an unexpected table inventory; {context}: missing [{}], unexpected [{}]",
        missing.join(", "),
        unexpected.join(", ")
    )
}
