use bigname_domain::resolver_read::evaluate_indexed_record;
use serde_json::Value;

use crate::RecordSelector;

pub(crate) fn answer(
    entries: &Value,
    provenance: &Value,
    coverage: &Value,
    selector: &RecordSelector,
) -> Value {
    evaluate_indexed_record(
        entries,
        provenance,
        coverage,
        &selector.record_key,
        &selector.record_family,
        selector.selector_key.as_deref(),
    )
    .comparison_value()
}
