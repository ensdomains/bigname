//! The nodes the history mirror walk consults for a name with and without raw labels.
use serde_json::{Value, json};
use uuid::Uuid;

use super::{MirrorPointer, MirrorWalk};
use crate::families::textless_tests::{Fixture, node};

fn pointer(fixture: &Fixture, raw_labels: Option<&[&str]>) -> MirrorPointer {
    let path = fixture.alpha_path();
    MirrorPointer {
        resource_id: Uuid::nil(),
        chain_id: "ethereum-sepolia".to_owned(),
        namespace: "ens".to_owned(),
        raw_labels: raw_labels
            .map(|labels| labels.iter().map(|label| (*label).to_owned()).collect()),
        labelhashes: path
            .iter()
            .map(|hash| hash.to_uppercase().replace("0X", "0x"))
            .collect(),
        namehash: node(&path),
        followable: true,
    }
}

#[test]
fn a_name_without_raw_labels_is_walked_by_its_labelhashes() {
    let fixture = Fixture::new();
    let nodes = vec![node(&fixture.alpha_path()), node(&[&fixture.eth])];
    let hashes: Vec<Value> = vec![json!([fixture.alpha, fixture.eth]), json!([fixture.eth])];

    let with_labels =
        MirrorWalk::new(&[pointer(&fixture, Some(&["alpha", "eth"]))]).expect("a walk");
    assert_eq!(with_labels.depths, [0, 1]);
    assert_eq!(with_labels.nodes, nodes);
    assert_eq!(
        with_labels.labels,
        [Some(json!(["alpha", "eth"])), Some(json!(["eth"]))]
    );
    assert_eq!(with_labels.hashes, hashes);

    let without = MirrorWalk::new(&[pointer(&fixture, None)]).expect("a walk");
    assert_eq!(without.depths, [0, 1]);
    assert_eq!(without.nodes, nodes);
    assert_eq!(without.labels, [None, None]);
    assert_eq!(without.hashes, hashes);
    assert_eq!(without.queried_nodes, [nodes[0].clone(), nodes[0].clone()]);
}
