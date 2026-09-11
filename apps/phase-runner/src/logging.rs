//! `BIGNAME_LOG_JSON` is documented in `.env.server.example` as a switch, so it
//! has to be one: this process previously emitted JSON unconditionally, which
//! made the documented knob inert and the compact format unreachable.

use tracing_subscriber::EnvFilter;

/// Treat unset, empty, and the usual negative spellings as off. Presence alone
/// is not enough, because Compose passes an unset variable through as an empty
/// string — a bare `is_some()` check would pin every deployment to JSON.
pub fn json_requested() -> bool {
    json_requested_from(std::env::var("BIGNAME_LOG_JSON").ok().as_deref())
}

/// The environment read is kept out of this function so the parsing rule can be
/// tested without mutating the process environment, which the parallel test
/// harness shares with every other test in the binary.
pub fn json_requested_from(value: Option<&str>) -> bool {
    value.is_some_and(|value| {
        !matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "" | "0" | "false" | "no" | "off"
        )
    })
}

pub fn init() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    if json_requested() {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .compact()
            .init();
    }
}

#[cfg(test)]
mod tests {
    use super::json_requested_from;

    #[test]
    fn negative_and_empty_spellings_do_not_enable_json() {
        assert!(!json_requested_from(None), "unset enabled JSON");
        for value in ["", " ", "0", "false", "FALSE", "no", "off"] {
            assert!(!json_requested_from(Some(value)), "{value:?} enabled JSON");
        }
        for value in ["1", "true", "yes", "json"] {
            assert!(
                json_requested_from(Some(value)),
                "{value:?} did not enable JSON"
            );
        }
    }
}
