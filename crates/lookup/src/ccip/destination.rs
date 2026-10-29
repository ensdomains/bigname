//! Which peer addresses a CCIP-Read gateway fetch may connect to. The URL comes from contract
//! revert data, so the gateway client judges the exact address list its connector is about to
//! dial: in the DNS resolver the connector calls, and in `fetch_one` for an IP-literal host, which
//! the connector dials without resolving. Nothing runs between that judgement and the connect, so
//! a name that answers differently on each query cannot route around it.

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::OnceLock,
};

use anyhow::{Result, bail};
use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use reqwest::dns::{Addrs, Name, Resolve, Resolving};

#[cfg(test)]
pub(crate) mod seam;
#[cfg(test)]
mod tests;

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

use GatewayRefusalClass as Class;

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

const fn v4(a: u8, b: u8, c: u8, d: u8, prefix_len: u8) -> Ipv4Net {
    Ipv4Net::new_assert(Ipv4Addr::new(a, b, c, d), prefix_len)
}

const fn v6(segments: [u16; 8], prefix_len: u8) -> Ipv6Net {
    let [a, b, c, d, e, f, g, h] = segments;
    Ipv6Net::new_assert(Ipv6Addr::new(a, b, c, d, e, f, g, h), prefix_len)
}

/// Explicit prefixes rather than `std::net` predicates, so the table does not depend on which of
/// them are stable on the pinned toolchain. The documentation ranges are deliberately absent: an
/// operator who wants them refused lists them.
const V4_REFUSED: &[(Ipv4Net, Class)] = &[
    (v4(0, 0, 0, 0, 8), Class::Unspecified),
    (v4(10, 0, 0, 0, 8), Class::Private),
    (v4(100, 64, 0, 0, 10), Class::Cgnat),
    (v4(127, 0, 0, 0, 8), Class::Loopback),
    (v4(169, 254, 0, 0, 16), Class::LinkLocal),
    (v4(172, 16, 0, 0, 12), Class::Private),
    (v4(192, 168, 0, 0, 16), Class::Private),
    (v4(224, 0, 0, 0, 4), Class::Multicast),
    (v4(240, 0, 0, 0, 4), Class::Reserved),
];

/// First match wins: `::1` and `::` sit inside `::/96`.
const V6_REFUSED: &[(Ipv6Net, Class)] = &[
    (v6([0, 0, 0, 0, 0, 0, 0, 1], 128), Class::Loopback),
    (v6([0; 8], 128), Class::Unspecified),
    (v6([0; 8], 96), Class::Ipv4Compatible),
    (v6([0xfc00, 0, 0, 0, 0, 0, 0, 0], 7), Class::UniqueLocal),
    (v6([0xfe80, 0, 0, 0, 0, 0, 0, 0], 10), Class::LinkLocal),
    (v6([0xff00, 0, 0, 0, 0, 0, 0, 0], 8), Class::Multicast),
];

/// IPv6 forms that carry an IPv4 address a dual-stack host or a translator would reach.
const MAPPED: Ipv6Net = v6([0, 0, 0, 0, 0, 0xffff, 0, 0], 96);
const NAT64: Ipv6Net = v6([0x64, 0xff9b, 0, 0, 0, 0, 0, 0], 96);
const SIX_TO_FOUR: Ipv6Net = v6([0x2002, 0, 0, 0, 0, 0, 0, 0], 16);

/// The ranges refused on top of the built-in table. Empty unless the operator configures some.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GatewayDestinationPolicy {
    deny: Vec<IpNet>,
}

impl GatewayDestinationPolicy {
    /// Parses a comma-separated list of IPv4 or IPv6 CIDR ranges, trimming whitespace around each
    /// entry. A blank value is the empty list. Any other entry that is not a range is an error
    /// naming it, so a typo cannot silently narrow the list.
    pub fn from_deny_list(value: &str) -> Result<Self> {
        if value.trim().is_empty() {
            return Ok(Self::default());
        }
        let mut deny = Vec::new();
        for entry in value.split(',').map(str::trim) {
            let Ok(range) = entry.parse::<IpNet>() else {
                bail!("deny-list entry `{entry}` is not an IPv4 or IPv6 CIDR range");
            };
            deny.push(range);
        }
        Ok(Self { deny })
    }

    pub fn len(&self) -> usize {
        self.deny.len()
    }

    pub fn is_empty(&self) -> bool {
        self.deny.is_empty()
    }

    fn denies(&self, address: IpAddr) -> bool {
        self.deny.iter().any(|range| range.contains(&address))
    }
}

static POLICY: OnceLock<GatewayDestinationPolicy> = OnceLock::new();
static REFUSAL_OBSERVER: OnceLock<fn(GatewayRefusalClass)> = OnceLock::new();

/// Installs the operator's deny list for the process. Call it before the first gateway fetch:
/// the first fetch without an install fixes the built-in table alone. Installing an equal policy
/// again is accepted, and a different one is an error.
pub fn install_gateway_destination_policy(policy: GatewayDestinationPolicy) -> Result<()> {
    match POLICY.set(policy) {
        Ok(()) => Ok(()),
        Err(policy) if POLICY.get() == Some(&policy) => Ok(()),
        Err(_) => bail!("a different CCIP-Read gateway destination policy is already in force"),
    }
}

/// Installs the callback told the class of each refused lookup, once per lookup, on the request
/// task. The first install wins.
pub fn install_gateway_refusal_observer(observer: fn(GatewayRefusalClass)) {
    let _ = REFUSAL_OBSERVER.set(observer);
}

pub(crate) fn observe_refusal(class: GatewayRefusalClass) {
    if let Some(observer) = REFUSAL_OBSERVER.get() {
        observer(class);
    }
    #[cfg(test)]
    seam::record_refusal(class);
}

fn active_policy() -> &'static GatewayDestinationPolicy {
    POLICY.get_or_init(default_policy)
}

#[cfg(not(test))]
fn default_policy() -> GatewayDestinationPolicy {
    GatewayDestinationPolicy::default()
}

#[cfg(test)]
fn default_policy() -> GatewayDestinationPolicy {
    GatewayDestinationPolicy::from_deny_list(seam::TEST_DENY_LIST)
        .expect("the test deny list must parse")
}

pub(crate) fn classify(address: IpAddr, policy: &GatewayDestinationPolicy) -> Option<Class> {
    match address {
        IpAddr::V4(address) => classify_v4(address, policy),
        IpAddr::V6(address) => classify_v6(address, policy),
    }
}

fn classify_v4(address: Ipv4Addr, policy: &GatewayDestinationPolicy) -> Option<Class> {
    V4_REFUSED
        .iter()
        .find(|(range, _)| range.contains(&address))
        .map(|(_, class)| *class)
        .or_else(|| {
            policy
                .denies(IpAddr::V4(address))
                .then_some(Class::Operator)
        })
}

/// An embedded IPv4 address is judged by its own class and reported under it.
fn classify_v6(address: Ipv6Addr, policy: &GatewayDestinationPolicy) -> Option<Class> {
    if let Some((_, class)) = V6_REFUSED
        .iter()
        .find(|(range, _)| range.contains(&address))
    {
        return Some(*class);
    }
    if let Some(embedded) = embedded_ipv4(address)
        && let Some(class) = classify_v4(embedded, policy)
    {
        return Some(class);
    }
    policy
        .denies(IpAddr::V6(address))
        .then_some(Class::Operator)
}

fn embedded_ipv4(address: Ipv6Addr) -> Option<Ipv4Addr> {
    let bits = u128::from(address);
    if MAPPED.contains(&address) || NAT64.contains(&address) {
        Some(Ipv4Addr::from(bits as u32))
    } else if SIX_TO_FOUR.contains(&address) {
        Some(Ipv4Addr::from((bits >> 80) as u32))
    } else {
        None
    }
}

/// A gateway fetch the filter stopped before any connection was opened. It travels up the
/// connector's error chain as the source of a connect error, so callers must look for it before
/// classifying that chain as a transport failure.
#[derive(Debug)]
pub(crate) struct GatewayDestinationRefused {
    class: GatewayRefusalClass,
    peer: IpAddr,
    host: String,
}

impl std::fmt::Display for GatewayDestinationRefused {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "CCIP gateway host {} is refused: {} is in a refused {} range",
            self.host,
            self.peer,
            self.class.as_str()
        )
    }
}

impl std::error::Error for GatewayDestinationRefused {}

pub(crate) fn refusal_class(error: &anyhow::Error) -> Option<GatewayRefusalClass> {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<GatewayDestinationRefused>())
        .map(|refused| refused.class)
}

fn judge(host: &str, peer: SocketAddr) -> std::result::Result<(), GatewayDestinationRefused> {
    if admitted_for_test(peer) {
        return Ok(());
    }
    match classify(peer.ip(), active_policy()) {
        Some(class) => Err(GatewayDestinationRefused {
            class,
            peer: peer.ip(),
            host: host.to_owned(),
        }),
        None => {
            #[cfg(test)]
            seam::record_judged_admitted(peer);
            Ok(())
        }
    }
}

#[cfg(not(test))]
const fn admitted_for_test(_peer: SocketAddr) -> bool {
    false
}

#[cfg(test)]
fn admitted_for_test(peer: SocketAddr) -> bool {
    seam::is_admitted(peer)
}

/// The connector dials an IP-literal host without calling the resolver, so the literal is judged
/// here. `Url` has already canonicalised hex, octal and shorthand IPv4 forms, and its host string
/// is what the connector parses.
pub(crate) fn check_literal_host(
    url: &reqwest::Url,
) -> std::result::Result<(), GatewayDestinationRefused> {
    let Some(host) = url.host_str() else {
        return Ok(());
    };
    let Ok(address) = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .parse::<IpAddr>()
    else {
        return Ok(());
    };
    judge(
        host,
        SocketAddr::new(address, url.port_or_known_default().unwrap_or(0)),
    )
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The gateway client's DNS resolver. Every answer is judged and one refused address refuses the
/// whole name: narrowing a mixed answer to its public members would let a zone probe which of its
/// records bigname dials.
pub(crate) struct FilteringResolver;

impl Resolve for FilteringResolver {
    fn resolve(&self, name: Name) -> Resolving {
        Box::pin(resolve_admitted(name.as_str().to_owned()))
    }
}

async fn resolve_admitted(host: String) -> std::result::Result<Addrs, BoxError> {
    let peers = resolve_host(&host).await?;
    for peer in &peers {
        judge(&host, *peer)?;
    }
    let addrs: Addrs = Box::new(peers.into_iter());
    Ok(addrs)
}

/// getaddrinfo on the blocking pool, the same lookup reqwest's default resolver makes.
#[cfg(not(test))]
async fn resolve_host(host: &str) -> std::io::Result<Vec<SocketAddr>> {
    Ok(tokio::net::lookup_host((host, 0_u16)).await?.collect())
}

/// Tests never reach real DNS: a name the seam has not scripted fails to resolve.
#[cfg(test)]
async fn resolve_host(host: &str) -> std::io::Result<Vec<SocketAddr>> {
    seam::scripted_answer(host)
}
