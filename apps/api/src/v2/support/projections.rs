use super::*;

mod common {
    use super::*;

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/v2/support/projections/common.rs"
    ));
}

mod consistency {
    use super::*;

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/v2/support/projections/consistency.rs"
    ));
}

mod coverage {
    use super::*;

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/v2/support/projections/coverage.rs"
    ));
}

mod data {
    use super::common::supported_summary_field;
    use super::*;

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/v2/support/projections/data.rs"
    ));
}

mod declared_state {
    use super::common::summary_is_unsupported;
    use super::records::build_record_inventory_section_for_name;
    use super::*;

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/v2/support/projections/declared_state.rs"
    ));
}

mod provenance {
    use super::*;

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/v2/support/projections/provenance.rs"
    ));
}

mod records {
    use super::*;

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/v2/support/projections/records.rs"
    ));
}

pub(crate) fn summary_is_unsupported(section: Option<&JsonValue>) -> bool {
    common::summary_is_unsupported(section)
}

pub(crate) fn canonicality_consistency(canonicality_summary: &JsonValue) -> &'static str {
    consistency::canonicality_consistency(canonicality_summary)
}

pub(crate) fn collection_consistency<'a>(
    summaries: impl Iterator<Item = &'a JsonValue>,
) -> &'static str {
    consistency::collection_consistency(summaries)
}

pub(crate) fn build_name_coverage(coverage: &JsonValue) -> JsonValue {
    coverage::build_name_coverage(coverage)
}

pub(crate) fn build_name_coverage_declared_state(coverage: &JsonValue) -> JsonValue {
    coverage::build_name_coverage_declared_state(coverage)
}

pub(crate) fn build_address_name_expansion_facts(
    row: &NameCurrentRow,
) -> AddressNameExpansionFacts {
    data::build_address_name_expansion_facts(row)
}

pub(crate) fn build_name_data(row: &NameCurrentRow) -> JsonValue {
    data::build_name_data(row)
}

pub(crate) fn build_name_declared_state(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> JsonValue {
    declared_state::build_name_declared_state(row, record_inventory_row)
}

pub(crate) fn build_name_surface_binding_explain_declared_state(row: &NameCurrentRow) -> JsonValue {
    declared_state::build_name_surface_binding_explain_declared_state(row)
}

pub(crate) fn build_name_authority_control_explain_declared_state(
    row: &NameCurrentRow,
) -> JsonValue {
    declared_state::build_name_authority_control_explain_declared_state(row)
}

pub(crate) fn build_name_provenance(provenance: &JsonValue) -> JsonValue {
    provenance::build_name_provenance(provenance)
}

pub(crate) fn build_name_provenance_with_execution_trace(
    provenance: &JsonValue,
    execution_trace_id: Option<Uuid>,
) -> JsonValue {
    provenance::build_name_provenance_with_execution_trace(provenance, execution_trace_id)
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

#[cfg(test)]
pub(crate) fn build_record_cache_section(
    row: Option<&RecordInventoryCurrentRow>,
    records: &[ResolutionRecordKey],
    unsupported_reason: &str,
) -> JsonValue {
    records::build_record_cache_section(row, records, unsupported_reason)
}
