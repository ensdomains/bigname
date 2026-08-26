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
