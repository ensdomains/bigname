use anyhow::{Context, Result, ensure};

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

pub(super) fn require_release_profile(
    compiled_profile: String,
    mut value: impl FnMut(&str) -> Option<String>,
) -> Result<()> {
    ensure!(
        compiled_profile == "release",
        "production benchmark commands require the release Cargo profile"
    );
    let launched_profile = value("BIGNAME_BENCHMARK_CARGO_PROFILE")
        .context("production benchmark must be launched by scripts/benchmark-gate")?;
    ensure!(
        launched_profile == "release",
        "production benchmark wrapper used Cargo profile {launched_profile:?}; release is required"
    );
    for name in [
        "BIGNAME_BENCHMARK_RUSTFLAGS",
        "BIGNAME_BENCHMARK_CARGO_ENCODED_RUSTFLAGS",
    ] {
        let value = value(name)
            .with_context(|| format!("production benchmark wrapper did not attest {name}"))?;
        ensure!(
            value.is_empty(),
            "production benchmark wrapper reported non-empty {name}"
        );
    }
    require_no_compiler_overrides(value)
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

    #[test]
    fn release_profile_wiring_rechecks_compiler_overrides() {
        let error = require_release_profile("release".to_owned(), |name| match name {
            "BIGNAME_BENCHMARK_CARGO_PROFILE" => Some("release".to_owned()),
            "BIGNAME_BENCHMARK_RUSTFLAGS" | "BIGNAME_BENCHMARK_CARGO_ENCODED_RUSTFLAGS" => {
                Some(String::new())
            }
            "RUSTC" => Some("/tmp/compiler-shim".to_owned()),
            _ => None,
        })
        .unwrap_err()
        .to_string();

        assert!(error.contains("non-empty RUSTC"), "{error}");
        assert!(
            error.contains("clear it before running the gate"),
            "{error}"
        );
    }

    #[test]
    fn release_wrapper_scrubs_user_config_rustflags_from_the_build() {
        let wrapper = include_str!("../../../scripts/benchmark-gate");
        let release_branch = wrapper
            .split_once("if [ \"$profile\" = \"release\" ]; then")
            .expect("wrapper must distinguish release builds")
            .1
            .split_once("\nfi\n")
            .expect("release branch must terminate")
            .0;
        let configured = release_branch
            .lines()
            .find_map(|line| {
                line.trim()
                    .strip_prefix("release_rustflags_env=(")
                    .and_then(|value| value.strip_suffix(')'))
            })
            .expect("release build environment omits the Rust-flags scrub");

        assert_eq!(
            configured.split_ascii_whitespace().collect::<Vec<_>>(),
            ["RUSTFLAGS=", "CARGO_ENCODED_RUSTFLAGS="],
            "release build must set both Rust-flags variables present-and-empty"
        );
    }

    #[test]
    fn release_wrapper_preserves_caller_rustflags_for_non_release_builds() {
        let wrapper = include_str!("../../../scripts/benchmark-gate");
        let release_branch = wrapper
            .split_once("if [ \"$profile\" = \"release\" ]; then")
            .expect("wrapper must distinguish release builds")
            .1
            .split_once("\nfi\n")
            .expect("release branch must terminate")
            .0;
        let build_environment = wrapper
            .split_once("if ! build_artifacts=")
            .expect("wrapper must capture the Cargo build")
            .1
            .split_once("cargo build")
            .expect("wrapper must invoke Cargo")
            .0;

        assert!(wrapper.contains("release_rustflags_env=()"));
        assert!(
            release_branch.contains("release_rustflags_env=(RUSTFLAGS= CARGO_ENCODED_RUSTFLAGS=)")
        );
        assert!(build_environment.contains("\"${release_rustflags_env[@]}\""));
        assert_eq!(wrapper.matches("release_rustflags_env=(").count(), 2);
        assert!(
            !build_environment
                .lines()
                .any(|line| line.trim_start().starts_with("RUSTFLAGS="))
        );
        assert!(
            !build_environment
                .lines()
                .any(|line| line.trim_start().starts_with("CARGO_ENCODED_RUSTFLAGS="))
        );
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
