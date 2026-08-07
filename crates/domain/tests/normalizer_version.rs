//! `ENS_NORMALIZER_VERSION` is stamped onto every name surface and is a covered interpreter input,
//! but nothing in the type system ties it to the normalizer actually compiled in. A silent bump of
//! the dependency would re-normalize names under a version the stored rows still claim.

use bigname_domain::normalization::ENS_NORMALIZER_VERSION;

#[test]
fn the_constant_names_the_compiled_ens_normalize_version() {
    let lock = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../Cargo.lock"),
    )
    .expect("workspace Cargo.lock must be readable");

    let mut locked = lock
        .split("[[package]]")
        .filter(|entry| entry.contains("name = \"ens-normalize\""))
        .filter_map(|entry| {
            entry
                .lines()
                .find_map(|line| line.strip_prefix("version = \""))
                .and_then(|line| line.strip_suffix('"'))
        });
    let version = locked
        .next()
        .expect("Cargo.lock must lock an ens-normalize version");
    assert_eq!(locked.next(), None, "one ens-normalize version at a time");

    assert_eq!(
        ENS_NORMALIZER_VERSION,
        format!("ensip15@ens-normalize-{version}"),
        "bump ENS_NORMALIZER_VERSION with the dependency: the constant is stamped on stored name \
         surfaces and is a covered interpreter input, so the bump is a re-derivation"
    );
}
