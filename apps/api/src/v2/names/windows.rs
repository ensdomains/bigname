//! Exact disjoint expiry windows, retaining input order for membership and cursors.

use bigname_storage::{NameCurrentExpiryWindow, UnixSeconds};

use crate::v2::{V2Error, V2Result};

pub(super) const WINDOW_KEY: &str = "expires_window";
const MAX_WINDOWS: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ExpiryWindow {
    pub(super) after: UnixSeconds,
    pub(super) before: UnixSeconds,
}

/// Validated windows in request order; sorting for overlap detection must not change indices.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ExpiryWindows(Vec<ExpiryWindow>);

impl ExpiryWindows {
    pub(super) fn parse(values: &[&str]) -> V2Result<Option<Self>> {
        if values.is_empty() {
            return Ok(None);
        }
        if values.len() > MAX_WINDOWS {
            return Err(V2Error::invalid_input(
                "date window parameter accepts at most 32 windows",
            ));
        }
        let windows = values
            .iter()
            .map(|value| {
                let (after, before) = value.split_once("..").ok_or_else(|| {
                    V2Error::invalid_input("date window must contain after..before")
                })?;
                let parse = |bound: &str| {
                    bound.trim().parse::<UnixSeconds>().map_err(|_| {
                        V2Error::invalid_input(
                            "date window bounds must be Unix seconds or RFC 3339 timestamps",
                        )
                    })
                };
                let window = ExpiryWindow {
                    after: parse(after)?,
                    before: parse(before)?,
                };
                if window.after >= window.before {
                    return Err(V2Error::invalid_input(
                        "date window after must be earlier than before",
                    ));
                }
                Ok(window)
            })
            .collect::<V2Result<Vec<_>>>()?;

        let mut sorted = windows.clone();
        sorted.sort_unstable_by_key(|window| window.after);
        if sorted.windows(2).any(|pair| pair[0].before > pair[1].after) {
            return Err(V2Error::invalid_input(
                "date window ranges must not overlap or repeat",
            ));
        }
        Ok(Some(Self(windows)))
    }

    /// Exact canonical bounds, preserving request order for cursor-bound membership indices.
    pub(super) fn canonical(&self) -> String {
        self.0
            .iter()
            .map(|window| format!("{}..{}", window.after, window.before))
            .collect::<Vec<_>>()
            .join(",")
    }

    pub(super) fn storage_windows(&self) -> Vec<NameCurrentExpiryWindow> {
        self.0
            .iter()
            .map(|window| NameCurrentExpiryWindow {
                expires_after: Some(window.after),
                expires_before: Some(window.before),
            })
            .collect()
    }

    pub(super) fn index_of(&self, expiry: Option<UnixSeconds>) -> Option<u8> {
        let expiry = expiry?;
        self.0
            .iter()
            .position(|window| window.after <= expiry && expiry < window.before)
            .map(|index| index as u8)
    }
}
