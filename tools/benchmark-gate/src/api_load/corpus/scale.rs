use anyhow::{Context, Result};
use bigname_storage::{
    DEFAULT_ADDRESS_NAMES_CURRENT_IDENTITY_JOINS, DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER,
    DEFAULT_NAME_CURRENT_LINEAGE_JOINS, DEFAULT_NAME_CURRENT_READ_FILTER,
};
use sqlx::PgPool;

use crate::budgets::GateBudgets;

#[derive(Clone, Copy, Debug)]
pub(in crate::api_load) struct TableScale {
    pub(in crate::api_load) name_current_rows: u64,
    pub(in crate::api_load) address_names_current_rows: u64,
}

pub(in crate::api_load) async fn load_table_scale(pool: &PgPool) -> Result<TableScale> {
    Ok(TableScale {
        name_current_rows: table_count(pool, "name_current").await?,
        address_names_current_rows: table_count(pool, "address_names_current").await?,
    })
}

impl TableScale {
    pub(in crate::api_load) fn failures(self, budgets: &GateBudgets) -> Vec<String> {
        table_scale_failures(
            self.name_current_rows,
            self.address_names_current_rows,
            budgets.api_min_name_current_rows,
            budgets.api_min_address_names_current_rows,
        )
    }
}

pub(super) fn table_scale_failures(
    name_rows: u64,
    address_rows: u64,
    min_name_rows: u64,
    min_address_rows: u64,
) -> Vec<String> {
    let mut failures = Vec::new();
    if name_rows < min_name_rows {
        failures.push(format!(
            "name_current has {name_rows} API-visible supported rows in active public namespaces after canonical projection and identity filtering; release profile requires {min_name_rows}"
        ));
    }
    if address_rows < min_address_rows {
        failures.push(format!(
            "address_names_current has {address_rows} API-visible supported rows in active public namespaces after canonical projection and identity filtering; release profile requires {min_address_rows}"
        ));
    }
    failures
}

async fn table_count(pool: &PgPool, table: &str) -> Result<u64> {
    let count: i64 = match table {
        "name_current" => sqlx::query_scalar(&name_scale_sql()).fetch_one(pool).await,
        "address_names_current" => {
            sqlx::query_scalar(&address_scale_sql())
                .fetch_one(pool)
                .await
        }
        _ => unreachable!("benchmark table names are fixed"),
    }
    .with_context(|| format!("failed to count {table} benchmark rows"))?;
    u64::try_from(count).with_context(|| format!("{table} returned a negative row count"))
}

fn name_scale_sql() -> String {
    format!(
        r#"SELECT count(*)
           FROM bigname_phase.name_current nc
           JOIN bigname_phase.name_surfaces surface
             ON surface.logical_name_id = nc.logical_name_id
           LEFT JOIN bigname_phase.resources resource
             ON resource.resource_id = nc.resource_id
           LEFT JOIN bigname_phase.surface_bindings binding
             ON binding.surface_binding_id = nc.surface_binding_id
           LEFT JOIN bigname_phase.token_lineages token_lineage
             ON token_lineage.token_lineage_id = nc.token_lineage_id
           {DEFAULT_NAME_CURRENT_LINEAGE_JOINS}
           JOIN (
               SELECT DISTINCT namespace
               FROM bigname_phase.manifest_versions
               WHERE rollout_status = 'active'
                 AND namespace IN ('ens', 'basenames')
           ) active ON active.namespace = nc.namespace
           WHERE nc.support_status = 'supported'
             {DEFAULT_NAME_CURRENT_READ_FILTER}"#
    )
}

fn address_scale_sql() -> String {
    format!(
        r#"SELECT count(*)
           FROM bigname_phase.address_names_current anc
           {DEFAULT_ADDRESS_NAMES_CURRENT_IDENTITY_JOINS}
           JOIN (
               SELECT DISTINCT namespace
               FROM bigname_phase.manifest_versions
               WHERE rollout_status = 'active'
                 AND namespace IN ('ens', 'basenames')
           ) active ON active.namespace = anc.namespace
           WHERE anc.support_status = 'supported'
             {DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER}"#
    )
}
