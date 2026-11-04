//! Label text a request proves, shown in its own response and offered for import.
//!
//! A plain label of a name input is normalized before the read, and the node read is the
//! namehash of those labels (docs/api-v1.md, "Name inputs"). So the text is the preimage of
//! `keccak256(text)`, wherever that labelhash appears. A served name brackets a label bigname
//! holds no usable preimage for (docs/glossary.md, "Rendered name"), and the placeholder is
//! reserved syntax that no normalized label takes, so replacing a placeholder whose labelhash
//! the request proves is exact. A bracketed label of the request proves nothing, except a
//! bracketed `eth` or `base`, which the route reads as text.
//!
//! Only the name strings of the response change. Cursors are built from the stored rows, and
//! order, totals and `meta` read nothing this touches.
use std::collections::{BTreeMap, BTreeSet};

use bigname_storage::rendered_name::{bracketed_label, label_hash};

use super::NormalizedRouteNameInput;
use crate::state::AppState;
use crate::v2::{name_record::NameRecord, name_records::NameRecords, subnames::Subname};

/// The labels one request proves, and those its response served as placeholders.
#[derive(Debug, Default)]
pub(crate) struct LabelHeal {
    proven: BTreeMap<[u8; 32], String>,
    served: BTreeSet<[u8; 32]>,
}

impl LabelHeal {
    /// The plain labels of normalized name inputs.
    pub(crate) fn from_names<'a>(names: impl IntoIterator<Item = &'a str>) -> Self {
        let mut proven = BTreeMap::new();
        for label in names.into_iter().flat_map(|name| name.split('.')) {
            if bracketed_label(label).is_none() {
                proven
                    .entry(label_hash(label))
                    .or_insert_with(|| label.to_owned());
            }
        }
        Self {
            proven,
            served: BTreeSet::new(),
        }
    }

    pub(crate) fn of_route(input: &NormalizedRouteNameInput) -> Self {
        Self::from_names([input.normalized_name.as_str()])
    }

    /// Replace every placeholder of `name` whose labelhash the request proves with its text.
    pub(crate) fn heal(&mut self, name: &mut String) {
        if let Some(healed) = self.substitute(name) {
            *name = healed;
        }
    }

    /// Note the proven placeholders of a served name the response does not carry.
    pub(crate) fn observe(&mut self, name: &str) {
        self.substitute(name);
    }

    fn substitute(&mut self, name: &str) -> Option<String> {
        if self.proven.is_empty() || !name.contains('[') {
            return None;
        }
        let mut healed = false;
        let labels: Vec<&str> = name
            .split('.')
            .map(|label| {
                let text = bracketed_label(label)
                    .and_then(|_| self.proven.get_key_value(&label_hash(label)));
                match text {
                    Some((labelhash, text)) => {
                        self.served.insert(*labelhash);
                        healed = true;
                        text.as_str()
                    }
                    None => label,
                }
            })
            .collect();
        healed.then(|| labels.join("."))
    }

    /// The proven labels served as placeholders, labelhash to text.
    pub(crate) fn candidates(&self) -> BTreeMap<[u8; 32], String> {
        self.served
            .iter()
            .filter_map(|labelhash| Some((*labelhash, self.proven.get(labelhash)?.clone())))
            .collect()
    }

    /// Offer the candidates to the staging writer (`crate::label_staging`). Called once the
    /// response is complete, so a request that fails stages nothing.
    pub(crate) fn stage(self, state: &AppState) {
        state.label_staging.submit(&state.pool, self.candidates());
    }
}

pub(crate) fn heal_name_record(
    state: &AppState,
    input: &NormalizedRouteNameInput,
    record: &mut NameRecord,
) {
    if state.label_staging.refused() {
        return;
    }
    let mut heal = LabelHeal::of_route(input);
    heal.heal(&mut record.name);
    heal.heal(&mut record.display_name);
    if let Some(name) = record.canonical_name.as_mut() {
        heal.heal(name);
    }
    heal.stage(state);
}

/// A child's own label is not part of the request, so it heals only when the request spells
/// that same label elsewhere, as `x` in `x.x.eth`.
pub(crate) fn heal_subnames(
    state: &AppState,
    input: &NormalizedRouteNameInput,
    mut subnames: Vec<Subname>,
) -> Vec<Subname> {
    if state.label_staging.refused() {
        return subnames;
    }
    let mut heal = LabelHeal::of_route(input);
    for subname in &mut subnames {
        heal.heal(&mut subname.name);
        heal.heal(&mut subname.display_name);
    }
    heal.stage(state);
    subnames
}

/// The records route serves no name of the row it read, only `canonical_name` on an alias
/// path. The row's served names still decide what is staged.
pub(crate) fn heal_name_records(
    state: &AppState,
    input: &NormalizedRouteNameInput,
    row: &bigname_storage::NameCurrentRow,
    records: &mut NameRecords,
) {
    if state.label_staging.refused() {
        return;
    }
    let mut heal = LabelHeal::of_route(input);
    heal.observe(&row.normalized_name);
    heal.observe(&row.canonical_display_name);
    if let Some(name) = records.canonical_name.as_mut() {
        heal.heal(name);
    }
    heal.stage(state);
}

#[cfg(test)]
#[path = "label_heal_tests.rs"]
mod tests;
