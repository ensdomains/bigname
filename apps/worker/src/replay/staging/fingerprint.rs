use std::fmt::Write as _;

use anyhow::{Context, Result};

use super::{
    CURRENT_PROJECTION_STAGING_SCHEMA_VERSION, cursor::shape_tag, tables::projection_stage_specs,
};
#[path = "../../staged_rebuild.rs"]
mod staged_rebuild_contract;
use staged_rebuild_contract::{
    CHILDREN_CURRENT_COLUMNS, PERMISSIONS_CURRENT_COLUMNS,
    PERMISSIONS_CURRENT_RESOURCE_SUMMARY_COLUMNS, PRIMARY_NAMES_CURRENT_COLUMNS,
    RECORD_INVENTORY_CURRENT_COLUMNS, RESOLVER_CURRENT_COLUMNS,
};

const STAGED_PROJECTIONS: &[&str] = &[
    "name_current",
    "children_current",
    "permissions_current",
    "record_inventory_current",
    "resolver_current",
    "address_names_current",
    "primary_names_current",
];

pub(crate) fn staging_contract_fingerprint() -> Result<String> {
    let mut fingerprint = String::new();
    writeln!(
        fingerprint,
        "schema_version={CURRENT_PROJECTION_STAGING_SCHEMA_VERSION}"
    )?;
    writeln!(
        fingerprint,
        "completion_fence=post_empty_page_full_range|channels=normalized_event,manifest_current,direct_invalidation_generation"
    )?;
    for projection in STAGED_PROJECTIONS {
        let cursor = shape_tag(projection)
            .with_context(|| format!("missing staging cursor contract for {projection}"))?;
        writeln!(fingerprint, "projection={projection}|cursor={cursor}")?;
        for spec in projection_stage_specs(projection)? {
            writeln!(
                fingerprint,
                "stage={}|unique={}|has_inserted_at={}",
                spec.target_table,
                spec.unique_columns.join(","),
                spec.has_inserted_at
            )?;
        }
    }

    for (table, columns) in [
        ("children_current", CHILDREN_CURRENT_COLUMNS),
        ("permissions_current", PERMISSIONS_CURRENT_COLUMNS),
        (
            "permissions_current_resource_summary",
            PERMISSIONS_CURRENT_RESOURCE_SUMMARY_COLUMNS,
        ),
        ("primary_names_current", PRIMARY_NAMES_CURRENT_COLUMNS),
        ("record_inventory_current", RECORD_INVENTORY_CURRENT_COLUMNS),
        ("resolver_current", RESOLVER_CURRENT_COLUMNS),
    ] {
        writeln!(fingerprint, "columns={table}|{}", columns.join(","))?;
    }
    Ok(fingerprint)
}
