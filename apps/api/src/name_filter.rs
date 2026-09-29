pub(crate) fn normalize_name_prefix(prefix: &str) -> bigname_domain::normalization::Result<String> {
    let (normalization_input, append_label_boundary) = prefix
        .strip_suffix('.')
        .filter(|prefix_without_dot| {
            !prefix_without_dot.is_empty() && !prefix_without_dot.ends_with('.')
        })
        .map_or((prefix, false), |prefix_without_dot| {
            (prefix_without_dot, true)
        });
    let mut normalized_prefix =
        bigname_domain::normalization::normalize_name(normalization_input)?.normalized_name;
    if append_label_boundary {
        normalized_prefix.push('.');
    }
    Ok(normalized_prefix)
}

pub(crate) fn normalize_name_contains(
    fragment: &str,
) -> bigname_domain::normalization::Result<String> {
    let (normalization_input, prepend_label_boundary) = fragment
        .strip_prefix('.')
        .filter(|fragment_without_dot| {
            !fragment_without_dot.is_empty() && !fragment_without_dot.starts_with('.')
        })
        .map_or((fragment, false), |fragment_without_dot| {
            (fragment_without_dot, true)
        });
    let mut normalized_fragment = normalize_name_prefix(normalization_input)?;
    if prepend_label_boundary {
        normalized_fragment.insert(0, '.');
    }
    Ok(normalized_fragment)
}

#[cfg(test)]
mod tests {
    use super::normalize_name_contains;

    // A contains fragment is normalized as a name, so it must be valid on its own. Some
    // substrings of valid names are therefore not admissible queries, while a valid fragment
    // matches wherever its bytes occur, including inside a longer emoji sequence.
    #[test]
    fn contains_fragments_must_be_valid_names_on_their_own() {
        for name in ["नमस्ते.eth", "👨\u{200d}💻.eth", "👍🏽.eth"] {
            let normalized = bigname_domain::normalization::normalize_name(name)
                .expect("the indexed name is valid")
                .normalized_name;
            assert_eq!(normalized, name, "{name} is stored as written");
        }
        for (fragment, inside) in [
            // Ends with the virama combining mark; the mark stays attached to its base.
            ("स\u{94d}", "नमस्ते.eth"),
            ("ते", "नमस्ते.eth"),
            // Each emoji of a ZWJ sequence, and the base of a skin-tone sequence.
            ("👨", "👨\u{200d}💻.eth"),
            ("💻", "👨\u{200d}💻.eth"),
            ("👍", "👍🏽.eth"),
        ] {
            let normalized = normalize_name_contains(fragment).expect("admissible fragment");
            assert_eq!(normalized, fragment);
            assert!(inside.contains(&normalized), "{fragment} inside {inside}");
        }
        for (fragment, inside) in [
            // A leading combining mark cannot start a name.
            ("\u{94d}ते", "नमस्ते.eth"),
            ("\u{94d}", "नमस्ते.eth"),
            // A joiner or a lone skin-tone modifier is not a valid name.
            ("\u{200d}", "👨\u{200d}💻.eth"),
            ("👨\u{200d}", "👨\u{200d}💻.eth"),
            ("\u{200d}💻", "👨\u{200d}💻.eth"),
            ("🏽", "👍🏽.eth"),
        ] {
            assert!(
                inside.contains(fragment),
                "{fragment:?} is a substring of {inside}"
            );
            assert!(
                normalize_name_contains(fragment).is_err(),
                "{fragment:?} is not an admissible fragment"
            );
        }
    }
}
