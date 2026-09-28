use super::*;

fn wrapper(fuses: Option<i64>, expiry: Option<&str>) -> WrapperRow {
    WrapperRow {
        resource_id: "r".into(),
        logical_name_id: None,
        wrapper_state: Some("wrapped".into()),
        fuses,
        has_modifier: true,
        expiry_seconds: expiry.map(str::to_owned),
        has_expiry: expiry.is_some(),
        lifecycle_unwrapped: None,
    }
}

// The served `in_grace` is null, not false, when the fuses or the expiry is unknown, so the
// wrapped-and-out-of-grace mask stays closed there.
#[test]
fn grace_is_unknown_without_fuses_or_expiry() {
    assert_eq!(in_grace(&wrapper(None, Some("100")), 50), None);
    assert_eq!(in_grace(&wrapper(Some(IS_DOT_ETH), None), 50), None);
    // Inside the last grace period before the wrapper expiry.
    let expiry = 10_000_000_i64;
    let clock = expiry - 10;
    assert_eq!(
        in_grace(&wrapper(Some(IS_DOT_ETH), Some(&expiry.to_string())), clock),
        Some(true)
    );
    // Past the expiry the served test is false (expiry >= clock fails).
    assert_eq!(
        in_grace(
            &wrapper(Some(IS_DOT_ETH), Some(&expiry.to_string())),
            expiry + 1
        ),
        Some(false)
    );
    // Not a .eth name: never in grace.
    assert_eq!(
        in_grace(&wrapper(Some(0), Some(&expiry.to_string())), clock),
        Some(false)
    );
}

#[test]
fn arms_follow_the_source_family_prefix() {
    assert_eq!(arm_of("ens_v1_registry_l1"), Some("ens_v1"));
    assert_eq!(arm_of("ens_v2_root_l1"), Some("ens_v2"));
    assert_eq!(arm_of("basenames_base_registry"), Some("basenames"));
    assert_eq!(arm_of("other"), None);
}
