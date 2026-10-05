//! `NamePlace::of` reads label hashes only. For a surface that stores its raw bytes the result
//! must equal the classification by label text it replaced, which `by_text` keeps for the
//! comparison.
use alloy_primitives::keccak256;

use super::{BASE_LABELHASH, ETH_LABELHASH, NamePlace, expiry};

fn labelhash(label: &str) -> String {
    format!("{:#x}", keccak256(label.as_bytes()))
}

fn labelhashes(name: &str) -> Vec<String> {
    name.split('.').map(labelhash).collect()
}

fn by_text(namespace: &str, raw_name: &str, labelhashes: &[String]) -> NamePlace {
    let labels: Vec<&str> = raw_name.split('.').collect();
    match (namespace, labels.as_slice()) {
        ("ens", [_, "eth"]) => NamePlace::EthSecondLevel,
        ("ens", [.., _, "eth"]) if labelhashes.len() == labels.len() => {
            let second_level = &labelhashes[labelhashes.len() - 2..];
            expiry::logical_name_of_labelhashes(namespace, second_level)
                .map_or(NamePlace::Other, NamePlace::BelowEthSecondLevel)
        }
        ("basenames", [_, "base", "eth"]) => NamePlace::BasenamesSecondLevel,
        _ => NamePlace::Other,
    }
}

#[test]
fn the_known_labelhashes_are_the_hashes_of_their_labels() {
    assert_eq!(ETH_LABELHASH, labelhash("eth"));
    assert_eq!(BASE_LABELHASH, labelhash("base"));
}

#[test]
fn a_surface_with_bytes_is_placed_as_its_text_was() {
    let names = [
        "eth",
        "foo.eth",
        "sub.foo.eth",
        "deep.sub.foo.eth",
        "fooeth",
        "x.fooeth",
        "eth.foo",
        "foo.eth.box",
        "base.eth",
        "foo.base.eth",
        "sub.foo.base.eth",
        "base",
        "foo.base",
        "foo.base.box",
        "eth.eth",
        "eth.base.eth",
        "base.base.eth",
        "reverse",
        "addr.reverse",
        "0123.addr.reverse",
    ];
    for namespace in ["ens", "basenames", "other"] {
        for name in names {
            let hashes = labelhashes(name);
            assert_eq!(
                NamePlace::of(namespace, &hashes),
                by_text(namespace, name, &hashes),
                "{namespace} {name}"
            );
        }
    }
}

#[test]
fn placement_reads_only_the_label_hashes() {
    let unknown = format!("0x{}", "ab".repeat(32));
    let eth = ETH_LABELHASH.to_owned();
    assert_eq!(
        NamePlace::of("ens", &[unknown.clone(), eth.clone()]),
        NamePlace::EthSecondLevel
    );
    assert_eq!(
        NamePlace::of(
            "ens",
            &[
                unknown.clone(),
                eth.to_ascii_uppercase().replace("0X", "0x")
            ]
        ),
        NamePlace::EthSecondLevel
    );
    let parent = expiry::logical_name_of_labelhashes("ens", &[unknown.clone(), eth.clone()])
        .expect("two labelhashes fold to a node");
    assert_eq!(
        NamePlace::of("ens", &[labelhash("child"), unknown.clone(), eth.clone()]),
        NamePlace::BelowEthSecondLevel(parent.clone())
    );
    assert_eq!(
        NamePlace::of(
            "ens",
            &[
                unknown.clone(),
                unknown.clone(),
                unknown.clone(),
                eth.clone()
            ]
        ),
        NamePlace::BelowEthSecondLevel(parent)
    );
    assert_eq!(
        NamePlace::of(
            "basenames",
            &[unknown.clone(), BASE_LABELHASH.to_owned(), eth.clone()]
        ),
        NamePlace::BasenamesSecondLevel
    );
    assert_eq!(NamePlace::of("ens", &[eth]), NamePlace::Other);
    assert_eq!(
        NamePlace::of("ens", &[unknown.clone(), unknown]),
        NamePlace::Other
    );
    assert_eq!(NamePlace::of("ens", &[]), NamePlace::Other);
}
