use anyhow::Error;

use super::CcipReadSummary;

impl CcipReadSummary {
    pub(crate) fn for_durable_timeout(mut self) -> Self {
        // Verified-resolution persistence does not admit successful gateway
        // response digests. The failure detail remains in the call summary.
        self.gateway_digests.clear();
        self
    }
}

#[derive(Debug)]
struct GatewayTransportFailure {
    configured_timeout: bool,
    summary: CcipReadSummary,
}

#[derive(Debug)]
pub(crate) struct CcipReadError {
    inner: Error,
    gateway_transport: Option<GatewayTransportFailure>,
}

impl CcipReadError {
    pub(super) fn gateway_transport(
        inner: Error,
        configured_timeout: bool,
        summary: CcipReadSummary,
    ) -> Self {
        Self {
            inner,
            gateway_transport: Some(GatewayTransportFailure {
                configured_timeout,
                summary,
            }),
        }
    }

    pub(crate) const fn is_gateway_transport_failure(&self) -> bool {
        self.gateway_transport.is_some()
    }

    pub(crate) fn is_configured_timeout(&self) -> bool {
        self.gateway_transport
            .as_ref()
            .is_some_and(|failure| failure.configured_timeout)
    }

    pub(crate) fn summary(&self) -> Option<&CcipReadSummary> {
        self.gateway_transport
            .as_ref()
            .map(|failure| &failure.summary)
    }
}

impl From<Error> for CcipReadError {
    fn from(inner: Error) -> Self {
        Self {
            inner,
            gateway_transport: None,
        }
    }
}

impl std::fmt::Display for CcipReadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.gateway_transport {
            Some(failure) if failure.configured_timeout => {
                formatter.write_str("configured CCIP gateway timeout expired")
            }
            Some(_) => formatter.write_str("CCIP gateway transport failed"),
            None => write!(formatter, "{}", self.inner),
        }
    }
}

impl std::error::Error for CcipReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.inner.source()
    }
}

pub(super) fn gateway_transport_classification(error: &Error) -> Option<bool> {
    error.chain().find_map(|cause| {
        let error = cause.downcast_ref::<reqwest::Error>()?;
        if error.is_timeout() {
            return Some(true);
        }
        (error.is_connect()
            || error.is_body()
            || (error.is_request()
                && !error.is_builder()
                && !error.is_redirect()
                && !error.is_status()
                && !error.is_decode()))
        .then_some(false)
    })
}

pub(super) fn retain_gateway_transport_error(
    selected: &mut Option<(bool, Error)>,
    error: Error,
) -> std::result::Result<(), Error> {
    let Some(configured_timeout) = gateway_transport_classification(&error) else {
        return Err(error);
    };
    let replace = match selected.as_ref() {
        None => true,
        Some((current_is_timeout, _)) => *current_is_timeout && !configured_timeout,
    };
    if replace {
        *selected = Some((configured_timeout, error));
    }
    Ok(())
}
