//! Verified-execution admission taken inside a handler, for a read whose URI does not classify as
//! verified execution but that may still execute a verified lookup: the records route's default
//! read answers its `value_not_retained` keys through one. The bounds layer inserts a
//! [`FillAdmission`] into every request's extensions, and the handler admits only once it knows
//! it has a key to fill, so a default read with nothing to fill takes no permit and no token.
use super::{ClientKey, RateLimitDecision, VerifiedExecutionAdmission};

/// The process's verified-execution admission and the request's client, as the bounds layer
/// computed it.
#[derive(Clone)]
pub(crate) struct FillAdmission {
    pub(super) admission: VerifiedExecutionAdmission,
    pub(super) client: ClientKey,
}

/// A verified-execution slot held for one fill. It counts toward the verified in-flight ceiling
/// and gauge until dropped.
pub(crate) struct FillPermit {
    _permit: tokio::sync::OwnedSemaphorePermit,
    _in_flight: crate::metrics::GaugeGuard,
}

impl FillAdmission {
    /// The client's rate-limit token and a verified in-flight slot, as `source=verified` takes
    /// them, or `None` when either is refused.
    pub(crate) fn admit(&self) -> Option<FillPermit> {
        if let Some(rate_limiter) = self.admission.rate_limiter.as_ref()
            && rate_limiter.check(self.client) != RateLimitDecision::Allowed
        {
            return None;
        }
        let permit = self.admission.semaphore.clone().try_acquire_owned().ok()?;
        Some(FillPermit {
            _permit: permit,
            _in_flight: crate::metrics::verified_in_flight_guard(),
        })
    }
}
