use anyhow::Result;
use sqlx::PgPool;

use crate::budgets::GateBudgets;

#[derive(Clone, Copy, Debug)]
pub(in crate::api_load) struct TableScale {
    pub(in crate::api_load) name_current_rows: u64,
    pub(in crate::api_load) address_names_current_rows: u64,
}

pub(in crate::api_load) async fn load_table_scale(pool: &PgPool) -> Result<TableScale> {
    let namespaces = super::readers::namespaces(pool).await?;
    let mut after = String::new();
    let mut name_current_rows = 0;
    loop {
        let (next, rows) = super::readers::name_batch(pool, &namespaces, &after, false).await?;
        if next == after {
            break;
        }
        after = next;
        name_current_rows += rows.len() as u64;
    }
    let (address_names_current_rows, _) = super::readers::addresses(pool, 0).await?;
    Ok(TableScale {
        name_current_rows,
        address_names_current_rows,
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
