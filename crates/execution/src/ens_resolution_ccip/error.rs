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
enum CcipTransportLeg {
    Gateway,
    ProviderCallback,
}

#[derive(Debug)]
struct CcipTransportFailure {
    leg: CcipTransportLeg,
    configured_timeout: bool,
    summary: CcipReadSummary,
}

#[derive(Debug)]
pub(crate) struct CcipReadError {
    inner: Error,
    transport: Option<CcipTransportFailure>,
}

impl CcipReadError {
    pub(super) fn gateway_transport(
        inner: Error,
        configured_timeout: bool,
        summary: CcipReadSummary,
    ) -> Self {
        Self {
            inner,
            transport: Some(CcipTransportFailure {
                leg: CcipTransportLeg::Gateway,
                configured_timeout,
                summary,
            }),
        }
    }

    pub(super) fn provider_callback_transport(
        inner: Error,
        configured_timeout: bool,
        summary: CcipReadSummary,
    ) -> Self {
        Self {
            inner,
            transport: Some(CcipTransportFailure {
                leg: CcipTransportLeg::ProviderCallback,
                configured_timeout,
                summary,
            }),
        }
    }

    #[cfg(test)]
    pub(crate) const fn is_gateway_transport_failure(&self) -> bool {
        matches!(
            self.transport,
            Some(CcipTransportFailure {
                leg: CcipTransportLeg::Gateway,
                ..
            })
        )
    }

    pub(crate) const fn is_transport_failure(&self) -> bool {
        self.transport.is_some()
    }

    pub(crate) fn is_configured_timeout(&self) -> bool {
        self.transport
            .as_ref()
            .is_some_and(|failure| failure.configured_timeout)
    }

    pub(crate) fn summary(&self) -> Option<&CcipReadSummary> {
        self.transport.as_ref().map(|failure| &failure.summary)
    }
}

impl From<Error> for CcipReadError {
    fn from(inner: Error) -> Self {
        Self {
            inner,
            transport: None,
        }
    }
}

impl std::fmt::Display for CcipReadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.transport {
            Some(CcipTransportFailure {
                leg: CcipTransportLeg::Gateway,
                configured_timeout: true,
                ..
            }) => formatter.write_str("configured CCIP gateway response timeout expired"),
            Some(CcipTransportFailure {
                leg: CcipTransportLeg::Gateway,
                ..
            }) => formatter.write_str("CCIP gateway transport failed"),
            Some(CcipTransportFailure {
                leg: CcipTransportLeg::ProviderCallback,
                configured_timeout: true,
                ..
            }) => formatter.write_str("configured CCIP callback provider response timeout expired"),
            Some(CcipTransportFailure {
                leg: CcipTransportLeg::ProviderCallback,
                ..
            }) => formatter.write_str("CCIP callback provider transport failed"),
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
            return Some(!error.is_connect());
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
