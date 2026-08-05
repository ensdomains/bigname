use super::*;

mod common {
    use super::*;

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/v2/support/projections/common.rs"
    ));
}

mod coverage {
    use super::*;

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/v2/support/projections/coverage.rs"
    ));
}

mod declared_state {
    use super::common::summary_is_unsupported;
    use super::*;

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/v2/support/projections/declared_state.rs"
    ));
}

mod records {
    use super::*;

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/v2/support/projections/records.rs"
    ));
}

pub(crate) fn build_name_coverage_declared_state(coverage: &JsonValue) -> JsonValue {
    coverage::build_name_coverage_declared_state(coverage)
}

pub(crate) fn build_name_surface_binding_explain_declared_state(row: &NameCurrentRow) -> JsonValue {
    declared_state::build_name_surface_binding_explain_declared_state(row)
}

pub(crate) fn build_name_authority_control_explain_declared_state(
    row: &NameCurrentRow,
) -> JsonValue {
    declared_state::build_name_authority_control_explain_declared_state(row)
}

pub(crate) fn build_record_cache_section_for_name(
    name_row: &NameCurrentRow,
    row: Option<&RecordInventoryCurrentRow>,
    records: &[ResolutionRecordKey],
    unsupported_reason: &str,
) -> JsonValue {
    records::build_record_cache_section_for_name(name_row, row, records, unsupported_reason)
}

pub(crate) fn build_record_inventory_section_for_name(
    name_row: &NameCurrentRow,
    row: Option<&RecordInventoryCurrentRow>,
    unsupported_reason: &str,
) -> JsonValue {
    records::build_record_inventory_section_for_name(name_row, row, unsupported_reason)
}
