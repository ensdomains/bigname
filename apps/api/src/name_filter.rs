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
