use super::OnDemandEnsPrimaryNameExecutionEvidence;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OnDemandEnsPrimaryNameErrorKind {
    Configuration,
    Execution,
}

#[derive(Debug)]
pub struct OnDemandEnsPrimaryNameError {
    kind: OnDemandEnsPrimaryNameErrorKind,
    message: String,
    transport_failure: bool,
    configured_timeout: bool,
    plain_execution_revert: bool,
    offchain_lookup_required: bool,
    evidence: OnDemandEnsPrimaryNameExecutionEvidence,
}

impl OnDemandEnsPrimaryNameError {
    pub(super) fn configuration(message: impl Into<String>) -> Self {
        Self {
            kind: OnDemandEnsPrimaryNameErrorKind::Configuration,
            message: message.into(),
            transport_failure: false,
            configured_timeout: false,
            plain_execution_revert: false,
            offchain_lookup_required: false,
            evidence: OnDemandEnsPrimaryNameExecutionEvidence::default(),
        }
    }

    pub(super) fn execution(message: impl Into<String>) -> Self {
        Self::execution_with_rpc_flags(message, false, false)
    }

    pub(super) fn transport(message: impl Into<String>, configured_timeout: bool) -> Self {
        Self {
            kind: OnDemandEnsPrimaryNameErrorKind::Execution,
            message: message.into(),
            transport_failure: true,
            configured_timeout,
            plain_execution_revert: false,
            offchain_lookup_required: false,
            evidence: OnDemandEnsPrimaryNameExecutionEvidence::default(),
        }
    }

    pub(super) fn execution_with_rpc_flags(
        message: impl Into<String>,
        plain_execution_revert: bool,
        offchain_lookup_required: bool,
    ) -> Self {
        Self {
            kind: OnDemandEnsPrimaryNameErrorKind::Execution,
            message: message.into(),
            transport_failure: false,
            configured_timeout: false,
            plain_execution_revert,
            offchain_lookup_required,
            evidence: OnDemandEnsPrimaryNameExecutionEvidence::default(),
        }
    }

    pub const fn kind(&self) -> OnDemandEnsPrimaryNameErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub const fn is_transport_failure(&self) -> bool {
        self.transport_failure
    }

    pub const fn is_configured_timeout(&self) -> bool {
        self.configured_timeout
    }

    pub const fn is_plain_execution_revert(&self) -> bool {
        self.plain_execution_revert
    }

    pub const fn is_offchain_lookup_required(&self) -> bool {
        self.offchain_lookup_required
    }

    pub fn evidence(&self) -> &OnDemandEnsPrimaryNameExecutionEvidence {
        &self.evidence
    }

    pub(super) fn with_evidence(
        mut self,
        evidence: OnDemandEnsPrimaryNameExecutionEvidence,
    ) -> Self {
        self.evidence = evidence;
        self
    }

    #[doc(hidden)]
    pub fn synthetic_execution_rpc_error_for_tests(
        message: impl Into<String>,
        plain_execution_revert: bool,
        offchain_lookup_required: bool,
    ) -> Self {
        Self::execution_with_rpc_flags(message, plain_execution_revert, offchain_lookup_required)
    }
}

impl std::fmt::Display for OnDemandEnsPrimaryNameError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.message)
    }
}

impl std::error::Error for OnDemandEnsPrimaryNameError {}
