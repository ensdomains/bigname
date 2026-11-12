//! Label text a request proves, shown in its own response and offered for import.
//!
//! A plain label of a name input is normalized before the read, and the node read is the
//! namehash of those labels (docs/api-v1.md, "Name inputs"). So the text is the preimage of
//! `keccak256(text)`, wherever that labelhash appears. A served name brackets a label bigname
//! holds no usable preimage for (docs/glossary.md, "Rendered name"), and the placeholder is
//! reserved syntax that no normalized label takes, so replacing a placeholder whose labelhash
//! the request proves is exact. A child's served name can start with the escape form of raw
//! label bytes instead, which is not reserved, so only its parent's labels and its own
//! placeholder are replaced there. A bracketed label of the request proves nothing, except a
//! bracketed `eth` or `base`, which the route reads as text.
//!
//! Only the name strings of the response change. Cursors are built from the stored rows, and
//! order, totals and `meta` read nothing this touches.
use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
};

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

    /// Replace every placeholder of the served name `name` whose labelhash the request proves
    /// with its text. Each label of a served name row is normalized text or a placeholder,
    /// neither holding a dot (docs/glossary.md, "Rendered name").
    pub(crate) fn heal(&mut self, name: &mut String) {
        if let Some(healed) = self.substitute(name) {
            *name = healed;
        }
    }

    /// Note the proven placeholders of a served name the response does not carry.
    pub(crate) fn observe(&mut self, name: &str) {
        self.substitute(name);
    }

    /// [`Self::heal`] for a child's served name: its own label, then its parent's served name
    /// of `parent_labels` labels. The own label can be the escape form of raw bytes
    /// (docs/glossary.md, "Non-name form"), which may hold dots and brackets, so it heals only
    /// when it is the placeholder of the child's `labelhash`. In the escape form a healed
    /// parent label is escape-encoded like the rest of the string.
    pub(crate) fn heal_child(
        &mut self,
        name: &mut String,
        labelhash: Option<&str>,
        parent_labels: usize,
    ) {
        if let Some(healed) = self.substitute_child(name, labelhash, parent_labels) {
            *name = healed;
        }
    }

    fn substitute(&mut self, name: &str) -> Option<String> {
        if self.proven.is_empty() || !name.contains('[') {
            return None;
        }
        self.substitute_labels(name.split('.').map(|label| (true, label)), false)
    }

    fn substitute_child(
        &mut self,
        name: &str,
        labelhash: Option<&str>,
        parent_labels: usize,
    ) -> Option<String> {
        if self.proven.is_empty() || !name.contains('[') {
            return None;
        }
        let mut labels: Vec<&str> = name.rsplitn(parent_labels + 1, '.').collect();
        if labels.len() != parent_labels + 1 {
            return None;
        }
        labels.reverse();
        let own = labels[0];
        let own_placeholder = bracketed_label(own)
            .zip(labelhash)
            .is_some_and(|(digits, hash)| {
                hash.strip_prefix("0x")
                    .unwrap_or(hash)
                    .eq_ignore_ascii_case(digits)
            });
        // Only the escape form puts a backslash in a served label.
        let escaped = own.contains('\\');
        let labels = labels
            .iter()
            .enumerate()
            .map(|(position, label)| (position > 0 || own_placeholder, *label));
        self.substitute_labels(labels, escaped)
    }

    /// `labels` joined by dots, each placeholder marked as a placeholder position whose
    /// labelhash the request proves replaced by its text, or `None` when none is.
    fn substitute_labels<'a>(
        &mut self,
        labels: impl Iterator<Item = (bool, &'a str)>,
        escaped: bool,
    ) -> Option<String> {
        let mut healed = false;
        let labels: Vec<Cow<'_, str>> = labels
            .map(|(position, label)| {
                let text = if position && bracketed_label(label).is_some() {
                    self.proven.get_key_value(&label_hash(label))
                } else {
                    None
                };
                match text {
                    Some((labelhash, text)) => {
                        self.served.insert(*labelhash);
                        healed = true;
                        if escaped {
                            Cow::Owned(escape_encoded(text))
                        } else {
                            Cow::Borrowed(text.as_str())
                        }
                    }
                    None => Cow::Borrowed(label),
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
    let mut heal = LabelHeal::of_route(input);
    let parent_labels = input.normalized_name.split('.').count();
    for subname in &mut subnames {
        let labelhash = subname.labelhash.as_deref();
        heal.heal_child(&mut subname.name, labelhash, parent_labels);
        heal.heal_child(&mut subname.display_name, labelhash, parent_labels);
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
    let mut heal = LabelHeal::of_route(input);
    heal.observe(&row.normalized_name);
    heal.observe(&row.canonical_display_name);
    if let Some(name) = records.canonical_name.as_mut() {
        heal.heal(name);
    }
    heal.stage(state);
}

/// PostgreSQL's `escape` encoding of `text`: a NUL or a byte with the high bit set as a
/// three-digit octal escape, a backslash doubled.
fn escape_encoded(text: &str) -> String {
    let mut encoded = String::with_capacity(text.len());
    for byte in text.bytes() {
        match byte {
            b'\\' => encoded.push_str("\\\\"),
            0 | 0x80..=u8::MAX => encoded.push_str(&format!("\\{byte:03o}")),
            _ => encoded.push(char::from(byte)),
        }
    }
    encoded
}

#[cfg(test)]
#[path = "label_heal_tests.rs"]
mod tests;
