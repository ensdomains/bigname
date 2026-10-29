//! The refused-class table, one test per class. Each class has refused members at its edges and
//! the nearest admitted address beyond each edge where one exists. Mapped, NAT64 and 6to4 twins
//! are judged by their embedded IPv4 address and reported under its class.

use super::*;

fn ip(text: &str) -> IpAddr {
    text.parse()
        .unwrap_or_else(|_| panic!("{text} is an IP address"))
}

fn built_in() -> GatewayDestinationPolicy {
    GatewayDestinationPolicy::default()
}

#[track_caller]
fn assert_class(
    policy: &GatewayDestinationPolicy,
    refused: &[&str],
    admitted: &[&str],
    class: Class,
) {
    for address in refused {
        assert_eq!(classify(ip(address), policy), Some(class), "{address}");
    }
    for address in admitted {
        assert_eq!(classify(ip(address), policy), None, "{address}");
    }
}

#[test]
fn loopback() {
    assert_class(
        &built_in(),
        &[
            "127.0.0.0",
            "127.0.0.1",
            "127.255.255.255",
            "::1",
            "::ffff:127.0.0.1",
            "64:ff9b::7f00:1",
            "2002:7f00:1::",
        ],
        &["126.255.255.255", "128.0.0.0", "::1:0:0"],
        Class::Loopback,
    );
}

#[test]
fn private() {
    assert_class(
        &built_in(),
        &[
            "10.0.0.0",
            "10.0.0.1",
            "10.255.255.255",
            "172.16.0.0",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.0.0",
            "192.168.0.1",
            "192.168.255.255",
            "::ffff:10.0.0.1",
            "::ffff:172.16.0.1",
            "::ffff:192.168.0.1",
            "64:ff9b::a00:1",
            "2002:a00:1::1",
        ],
        &[
            "9.255.255.255",
            "11.0.0.0",
            "172.15.255.255",
            "172.32.0.0",
            "192.167.255.255",
            "192.169.0.0",
            "::ffff:11.0.0.0",
            "::ffff:172.32.0.0",
        ],
        Class::Private,
    );
}

#[test]
fn link_local() {
    assert_class(
        &built_in(),
        &[
            "169.254.0.0",
            "169.254.169.254",
            "169.254.255.255",
            "fe80::",
            "fe80::1",
            "febf:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
            "::ffff:169.254.169.254",
        ],
        &[
            "169.253.255.255",
            "169.255.0.0",
            "fe7f:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
            "fec0::",
            "::ffff:169.255.0.0",
        ],
        Class::LinkLocal,
    );
}

#[test]
fn cgnat() {
    assert_class(
        &built_in(),
        &[
            "100.64.0.0",
            "100.64.0.1",
            "100.127.255.255",
            "::ffff:100.64.0.1",
        ],
        &["100.63.255.255", "100.128.0.0", "::ffff:100.128.0.0"],
        Class::Cgnat,
    );
}

#[test]
fn unique_local() {
    assert_class(
        &built_in(),
        &[
            "fc00::",
            "fd00::1",
            "fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
        ],
        &["fbff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "fe00::"],
        Class::UniqueLocal,
    );
}

/// `0.0.0.0/8` starts the address space, so it has no lower neighbour.
#[test]
fn unspecified() {
    assert_class(
        &built_in(),
        &[
            "0.0.0.0",
            "0.1.2.3",
            "0.255.255.255",
            "::",
            "::ffff:0.0.0.0",
        ],
        &["1.0.0.0", "::1:0:0"],
        Class::Unspecified,
    );
}

/// `239.255.255.255` is the last multicast address and `240.0.0.0` the first reserved one, so
/// multicast has an admitted neighbour only below.
#[test]
fn multicast() {
    assert_class(
        &built_in(),
        &[
            "224.0.0.0",
            "224.0.0.1",
            "239.255.255.255",
            "ff00::",
            "ff02::1",
            "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
            "::ffff:224.0.0.1",
        ],
        &["223.255.255.255", "feff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"],
        Class::Multicast,
    );
}

/// `240.0.0.0/4` ends the IPv4 space and sits directly above multicast, so no admitted address
/// is adjacent to it. The edges asserted are the class boundary on one side and the end of the
/// space, the limited broadcast address, on the other.
#[test]
fn reserved() {
    let policy = built_in();
    assert_class(
        &policy,
        &[
            "240.0.0.0",
            "240.0.0.1",
            "255.255.255.255",
            "::ffff:240.0.0.1",
        ],
        &[],
        Class::Reserved,
    );
    assert_eq!(
        classify(ip("239.255.255.255"), &policy),
        Some(Class::Multicast)
    );
}

#[test]
fn ipv4_compatible() {
    assert_class(
        &built_in(),
        &["::2", "::127.0.0.1", "::10.0.0.1", "::ffff:ffff"],
        &["::1:0:0", "::fffe:ffff:ffff"],
        Class::Ipv4Compatible,
    );
    let policy = built_in();
    assert_eq!(classify(ip("::"), &policy), Some(Class::Unspecified));
    assert_eq!(classify(ip("::1"), &policy), Some(Class::Loopback));
}

/// A translated or tunnelled address is admitted when the IPv4 address it carries is.
#[test]
fn embedded_ipv4_prefixes_follow_the_embedded_address() {
    assert_class(
        &built_in(),
        &[],
        &[
            "::ffff:203.0.113.1",
            "64:ff9b::cb00:7101",
            "2002:cb00:7101::1",
            "64:ff9a:ffff:ffff:ffff:ffff:ffff:ffff",
            "64:ff9b::1:0:0",
            "64:ff9b:1::a00:1",
            "2001:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
            "2003::",
        ],
        Class::Loopback,
    );
}

/// Documentation, benchmarking, IETF-assignment, site-local, discard and Teredo ranges are not
/// built in. An operator who wants them refused lists them.
#[test]
fn excluded_ranges_are_admitted_by_the_built_in_table() {
    assert_class(
        &built_in(),
        &[],
        &[
            "192.0.2.1",
            "198.51.100.1",
            "203.0.113.1",
            "2001:db8::1",
            "198.18.0.1",
            "192.0.0.1",
            "fec0::1",
            "100::1",
            "2001::1",
        ],
        Class::Loopback,
    );
}

#[test]
fn operator() -> Result<()> {
    let policy = GatewayDestinationPolicy::from_deny_list("198.51.100.0/25, 2001:db8:1::/48")?;
    assert_class(
        &policy,
        &[
            "198.51.100.0",
            "198.51.100.7",
            "198.51.100.127",
            "::ffff:198.51.100.7",
            "2001:db8:1::",
            "2001:db8:1::7",
            "2001:db8:1:ffff:ffff:ffff:ffff:ffff",
        ],
        &[
            "198.51.100.128",
            "2001:db8:0:ffff:ffff:ffff:ffff:ffff",
            "2001:db8:2::",
            "2001:db8:2::7",
            "203.0.113.7",
        ],
        Class::Operator,
    );
    // A built-in class wins over a listed range that overlaps it.
    let overlapping = GatewayDestinationPolicy::from_deny_list("127.0.0.0/8")?;
    assert_eq!(
        classify(ip("127.0.0.1"), &overlapping),
        Some(Class::Loopback)
    );
    Ok(())
}

#[test]
fn deny_list_parsing_trims_entries_and_accepts_blank() -> Result<()> {
    for blank in ["", "   "] {
        assert!(GatewayDestinationPolicy::from_deny_list(blank)?.is_empty());
    }
    let policy = GatewayDestinationPolicy::from_deny_list(" 198.51.100.0/24 , 2001:db8:1::/48 ")?;
    assert_eq!(policy.len(), 2);
    assert_eq!(classify(ip("198.51.100.9"), &policy), Some(Class::Operator));
    assert_eq!(
        classify(ip("2001:db8:1::9"), &policy),
        Some(Class::Operator)
    );
    Ok(())
}

#[test]
fn deny_list_parsing_names_the_malformed_entry() {
    for (value, entry) in [
        ("198.51.100.0/33", "198.51.100.0/33"),
        ("nonsense", "nonsense"),
        ("198.51.100.0/24,", ""),
        ("198.51.100.7", "198.51.100.7"),
        ("2001:db8:1::/129", "2001:db8:1::/129"),
    ] {
        let error = GatewayDestinationPolicy::from_deny_list(value)
            .expect_err("a malformed entry must be refused");
        assert!(
            error.to_string().contains(&format!("`{entry}`")),
            "{value}: {error}"
        );
    }
}

#[test]
fn class_labels_are_distinct_snake_case() {
    let labels = Class::ALL.map(Class::as_str);
    for label in labels {
        assert!(
            label.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
            "{label}"
        );
    }
    let mut unique = labels.to_vec();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), labels.len());
}
