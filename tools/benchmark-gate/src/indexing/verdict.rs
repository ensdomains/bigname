pub(super) fn projection_scale_failures(
    pre_rebuild: u64,
    post_rebuild: u64,
    minimum: u64,
) -> Vec<String> {
    let mut failures = Vec::new();
    if pre_rebuild < minimum {
        failures.push(format!(
            "family readers composed {pre_rebuild} supported names before rebuild; release profile requires {minimum}"
        ));
    }
    if post_rebuild < minimum {
        failures.push(format!(
            "family readers compose {post_rebuild} supported names after rebuild; release profile requires {minimum}"
        ));
    }
    failures
}

pub(super) fn database_instance_identity_failures(
    preflight: &str,
    postflight: &str,
) -> Vec<String> {
    if preflight == postflight {
        Vec::new()
    } else {
        vec!["database instance identity changed during the indexing benchmark".to_owned()]
    }
}
