use super::*;

#[test]
fn address_range_only_admits_topics_from_its_manifest() {
    let filter = WatchFilter {
        address_ranges: vec![AddressRange {
            address: "0x01".to_owned(),
            from_block: 10,
            to_block: 20,
            topic0s: vec!["0xaa".to_owned()],
        }],
        all_emitter_ranges: Vec::new(),
        registry_announcements: None,
    };

    assert!(filter.includes("0x01", "0xaa", 10));
    assert!(!filter.includes("0x01", "0xbb", 10));
    assert!(!filter.includes("0x01", "0xaa", 21));
}

#[test]
fn registry_announcements_are_collected_from_all_emitters() {
    let topic = registry_announcement_topic0();

    assert_eq!(
        all_emitter_topics(ENS_V2_REGISTRY_SOURCE_FAMILY, std::slice::from_ref(&topic)),
        vec![topic]
    );
}

#[test]
fn announced_registry_topics_are_address_scoped_forward_only() {
    let mut filter = WatchFilter {
        address_ranges: Vec::new(),
        all_emitter_ranges: Vec::new(),
        registry_announcements: Some(RegistryAnnouncementWatch {
            announcement_topic0: "0xaa".to_owned(),
            scoped_topic0s: vec!["0xbb".to_owned()],
        }),
    };

    let queries = filter.admit_registry_announcements([("0x01".to_owned(), 10)], 0, 20);

    assert!(!filter.includes("0x01", "0xbb", 9));
    assert!(filter.includes("0x01", "0xbb", 10));
    assert_eq!(
        queries,
        [WatchQuery {
            from_block: 10,
            to_block: 20,
            addresses: vec!["0x01".to_owned()],
            topic0s: vec!["0xbb".to_owned()],
        }]
    );
}

#[test]
fn ens_v2_resolver_signatures_remain_address_scoped() {
    let name_changed = format!(
        "{}",
        alloy_primitives::keccak256("NameChanged(bytes32,string)".as_bytes())
    );
    let upgraded = format!(
        "{}",
        alloy_primitives::keccak256("Upgraded(address)".as_bytes())
    );

    assert!(all_emitter_topics("ens_v2_resolver_l1", &[name_changed, upgraded]).is_empty());
}

#[test]
fn query_windows_do_not_cross_product_manifest_topics() {
    let filter = WatchFilter {
        address_ranges: vec![
            AddressRange {
                address: "0x01".to_owned(),
                from_block: 10,
                to_block: 20,
                topic0s: vec!["0xaa".to_owned()],
            },
            AddressRange {
                address: "0x02".to_owned(),
                from_block: 10,
                to_block: 20,
                topic0s: vec!["0xbb".to_owned()],
            },
        ],
        all_emitter_ranges: Vec::new(),
        registry_announcements: None,
    };

    assert_eq!(
        filter.queries(),
        vec![
            WatchQuery {
                from_block: 10,
                to_block: 20,
                addresses: vec!["0x01".to_owned()],
                topic0s: vec!["0xaa".to_owned()],
            },
            WatchQuery {
                from_block: 10,
                to_block: 20,
                addresses: vec!["0x02".to_owned()],
                topic0s: vec!["0xbb".to_owned()],
            },
        ]
    );
}

#[test]
fn generic_resolver_topics_scan_all_emitters() {
    let filter = WatchFilter {
        address_ranges: Vec::new(),
        all_emitter_ranges: vec![AllEmitterRange {
            from_block: 10,
            to_block: 20,
            topic0s: vec!["0xaa".to_owned()],
        }],
        registry_announcements: None,
    };

    assert!(filter.includes("0x-unlisted", "0xaa", 10));
    assert_eq!(
        filter.queries(),
        vec![WatchQuery {
            from_block: 10,
            to_block: 20,
            addresses: Vec::new(),
            topic0s: vec!["0xaa".to_owned()],
        }]
    );
}

#[test]
fn only_existing_generic_resolver_topics_are_selected_without_addresses() {
    let generic = generic_resolver_topic0s()[0].clone();
    let shared = format!(
        "{}",
        alloy_primitives::keccak256("ApprovalForAll(address,address,bool)".as_bytes())
    );

    assert_eq!(
        all_emitter_topics(
            ENS_V1_RESOLVER_SOURCE_FAMILY,
            &[generic.clone(), shared.clone()],
        ),
        vec![generic.clone()]
    );
    assert!(all_emitter_topics(BASENAMES_BASE_RESOLVER_SOURCE_FAMILY, &[shared]).is_empty());
    assert_eq!(
        all_emitter_topics(
            BASENAMES_BASE_RESOLVER_SOURCE_FAMILY,
            std::slice::from_ref(&generic),
        ),
        vec![generic]
    );
}
