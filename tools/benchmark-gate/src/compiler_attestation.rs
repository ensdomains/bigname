use anyhow::{Result, ensure};

pub(super) const fn cargo_profile_for_debug_assertions(debug_assertions: bool) -> &'static str {
    if debug_assertions { "dev" } else { "release" }
}

pub(super) fn cargo_profile() -> String {
    cargo_profile_for_debug_assertions(cfg!(debug_assertions)).to_owned()
}

pub(super) fn require_no_compiler_overrides(
    mut value: impl FnMut(&str) -> Option<String>,
) -> Result<()> {
    for name in [
        "RUSTC",
        "CARGO_BUILD_RUSTC",
        "RUSTC_WRAPPER",
        "RUSTC_WORKSPACE_WRAPPER",
    ] {
        ensure!(
            value(name).is_none_or(|value| value.is_empty()),
            "production benchmark commands refuse non-empty {name}; clear it before running the gate"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_profile_rejects_every_compiler_override_variable() {
        for rejected in [
            "RUSTC",
            "CARGO_BUILD_RUSTC",
            "RUSTC_WRAPPER",
            "RUSTC_WORKSPACE_WRAPPER",
        ] {
            let error = require_no_compiler_overrides(|name| {
                (name == rejected).then(|| "/tmp/compiler-shim".to_owned())
            })
            .unwrap_err()
            .to_string();
            assert!(error.contains(rejected), "{rejected}: {error}");
            assert!(error.contains("clear it before running the gate"));
        }
    }
}
#[test]
fn reported_cargo_profile_comes_from_compiled_assertion_mode() {
    assert_eq!(cargo_profile_for_debug_assertions(true), "dev");
    assert_eq!(cargo_profile_for_debug_assertions(false), "release");
    assert_eq!(
        cargo_profile(),
        cargo_profile_for_debug_assertions(cfg!(debug_assertions))
    );
}
