use std::fmt::Write as _;

use anyhow::{Context, Result};
use bigname_storage::projection_staging::{
    ADDRESS_NAMES_CURRENT_STAGING_COLUMNS, NAME_CURRENT_STAGING_COLUMNS,
};

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

pub(crate) fn staging_contract_fingerprint(
    channel_tags: impl Fn(&str) -> Option<Vec<&'static str>>,
) -> Result<String> {
    let mut fingerprint = String::new();
    writeln!(
        fingerprint,
        "schema_version={CURRENT_PROJECTION_STAGING_SCHEMA_VERSION}"
    )?;
    writeln!(
        fingerprint,
        "completion_fence=post_empty_page_full_range|publish_fence=pre_replace_full_range"
    )?;
    for projection in STAGED_PROJECTIONS {
        let cursor = shape_tag(projection)
            .with_context(|| format!("missing staging cursor contract for {projection}"))?;
        let channels = channel_tags(projection)
            .with_context(|| format!("missing staging input channels for {projection}"))?;
        writeln!(
            fingerprint,
            "projection={projection}|cursor={cursor}|channels={}",
            channels.join(",")
        )?;
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
        ("name_current", NAME_CURRENT_STAGING_COLUMNS),
        ("children_current", CHILDREN_CURRENT_COLUMNS),
        ("permissions_current", PERMISSIONS_CURRENT_COLUMNS),
        (
            "permissions_current_resource_summary",
            PERMISSIONS_CURRENT_RESOURCE_SUMMARY_COLUMNS,
        ),
        ("primary_names_current", PRIMARY_NAMES_CURRENT_COLUMNS),
        ("record_inventory_current", RECORD_INVENTORY_CURRENT_COLUMNS),
        ("resolver_current", RESOLVER_CURRENT_COLUMNS),
        (
            "address_names_current",
            ADDRESS_NAMES_CURRENT_STAGING_COLUMNS,
        ),
    ] {
        writeln!(fingerprint, "columns={table}|{}", columns.join(","))?;
    }
    Ok(fingerprint)
}
