use std::{collections::HashSet, fmt::Write as _, hash::Hash};

const ROWS_PER_INSERT: usize = 500;

pub(super) fn conflict_free_batches_with_singletons<'a, T, K>(
    rows: &'a [T],
    key: impl Fn(&T) -> K,
    singleton_keys: &HashSet<K>,
) -> Vec<(usize, &'a [T])>
where
    K: Clone + Eq + Hash,
{
    let mut seen = HashSet::new();
    let mut duplicated = HashSet::new();
    let row_keys = rows
        .iter()
        .map(|row| {
            let row_key = key(row);
            if !seen.insert(row_key.clone()) {
                duplicated.insert(row_key.clone());
            }
            row_key
        })
        .collect::<Vec<_>>();
    let mut batches = Vec::new();
    let mut start = 0;
    for (index, row_key) in row_keys.iter().enumerate() {
        if duplicated.contains(row_key) || singleton_keys.contains(row_key) {
            if start < index {
                batches.push((start, &rows[start..index]));
            }
            batches.push((index, &rows[index..index + 1]));
            start = index + 1;
            continue;
        }
        if index - start == ROWS_PER_INSERT {
            batches.push((start, &rows[start..index]));
            start = index;
        }
    }
    if start < rows.len() {
        batches.push((start, &rows[start..]));
    }
    batches
}

pub(super) fn batch_row_context(
    start: usize,
    identities: impl IntoIterator<Item = impl std::fmt::Display>,
) -> String {
    let mut context = String::from("batch rows [");
    for (offset, identity) in identities.into_iter().enumerate() {
        if offset > 0 {
            context.push_str(", ");
        }
        write!(context, "{}={identity}", start + offset).expect("writing to String cannot fail");
    }
    context.push(']');
    context
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_conflict_keys_are_singletons_at_their_original_indexes() {
        let rows = ["before", "duplicate", "between", "duplicate", "after"];
        let batches = conflict_free_batches_with_singletons(&rows, |row| *row, &HashSet::new())
            .into_iter()
            .map(|(start, rows)| (start, rows.to_vec()))
            .collect::<Vec<_>>();

        assert_eq!(
            batches,
            vec![
                (0, vec!["before"]),
                (1, vec!["duplicate"]),
                (2, vec!["between"]),
                (3, vec!["duplicate"]),
                (4, vec!["after"]),
            ]
        );
    }

    #[test]
    fn unique_rows_split_at_the_established_insert_size() {
        let rows = (0..=ROWS_PER_INSERT).collect::<Vec<_>>();
        let batches = conflict_free_batches_with_singletons(&rows, |row| *row, &HashSet::new());

        assert_eq!(batches.len(), 2);
        assert_eq!((batches[0].0, batches[0].1.len()), (0, ROWS_PER_INSERT));
        assert_eq!((batches[1].0, batches[1].1.len()), (ROWS_PER_INSERT, 1));
    }

    #[test]
    fn requested_conflict_keys_are_singletons() {
        let rows = ["new-before", "stored", "new-after"];
        let batches =
            conflict_free_batches_with_singletons(&rows, |row| *row, &HashSet::from(["stored"]))
                .into_iter()
                .map(|(start, rows)| (start, rows.to_vec()))
                .collect::<Vec<_>>();

        assert_eq!(
            batches,
            vec![
                (0, vec!["new-before"]),
                (1, vec!["stored"]),
                (2, vec!["new-after"]),
            ]
        );
    }
}
