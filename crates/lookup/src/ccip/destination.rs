//! Red variant only. It carries the gateway destination filter's test seam and refusal class so
//! the filter's tests compile against the unfiltered gateway client, and filters nothing.
#![allow(dead_code)]

pub(crate) mod seam;

/// The refused range a gateway peer address falls in. `as_str` is the metric label and the log
/// field, so the three uses cannot drift.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum GatewayRefusalClass {
    Loopback,
    Private,
    LinkLocal,
    Cgnat,
    UniqueLocal,
    Unspecified,
    Multicast,
    Reserved,
    Ipv4Compatible,
    /// A range from the operator's `BIGNAME_API_CCIP_GATEWAY_DENY_CIDRS` list.
    Operator,
}

impl GatewayRefusalClass {
    pub const ALL: [Self; 10] = [
        Self::Loopback,
        Self::Private,
        Self::LinkLocal,
        Self::Cgnat,
        Self::UniqueLocal,
        Self::Unspecified,
        Self::Multicast,
        Self::Reserved,
        Self::Ipv4Compatible,
        Self::Operator,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Loopback => "loopback",
            Self::Private => "private",
            Self::LinkLocal => "link_local",
            Self::Cgnat => "cgnat",
            Self::UniqueLocal => "unique_local",
            Self::Unspecified => "unspecified",
            Self::Multicast => "multicast",
            Self::Reserved => "reserved",
            Self::Ipv4Compatible => "ipv4_compatible",
            Self::Operator => "operator",
        }
    }
}
