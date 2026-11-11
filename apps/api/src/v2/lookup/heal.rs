//! The batch's name inputs prove their plain labels for every record it serves
//! (`crate::v2::support::LabelHeal`), the records of its address inputs included.
use super::{dto::LookupResult, parse::ParsedNameLookup};
use crate::{AppState, v2::support::LabelHeal};

pub(super) fn heal_results(
    state: &AppState,
    inputs: &[ParsedNameLookup],
    results: &mut [LookupResult],
) {
    let mut heal = LabelHeal::from_names(
        inputs
            .iter()
            .filter_map(|input| input.lookup.as_ref())
            .map(|lookup| lookup.normalized_name.as_str()),
    );
    for result in results {
        let records = result.records.iter_mut().flatten();
        for record in result.record.iter_mut().chain(records) {
            heal.heal(&mut record.name);
            heal.heal(&mut record.display_name);
        }
    }
    heal.stage(state);
}
