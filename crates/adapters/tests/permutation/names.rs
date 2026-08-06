use alloy_primitives::{B256, keccak256};

pub fn labelhash(label: &str) -> B256 {
    keccak256(label.as_bytes())
}

pub fn namehash(labels: &[&str]) -> B256 {
    let mut node = [0_u8; 32];
    for label in labels.iter().rev() {
        let mut path = [0_u8; 64];
        path[..32].copy_from_slice(&node);
        path[32..].copy_from_slice(labelhash(label).as_slice());
        node.copy_from_slice(keccak256(path).as_slice());
    }
    B256::from(node)
}

pub fn child_node(parent: B256, label: &str) -> B256 {
    let mut path = [0_u8; 64];
    path[..32].copy_from_slice(parent.as_slice());
    path[32..].copy_from_slice(labelhash(label).as_slice());
    keccak256(path)
}

pub fn dns_encode(labels: &[&str]) -> Vec<u8> {
    let mut encoded = Vec::new();
    for label in labels {
        encoded.push(u8::try_from(label.len()).expect("generated label is short"));
        encoded.extend_from_slice(label.as_bytes());
    }
    encoded.push(0);
    encoded
}

pub fn reverse_labels(address: &str) -> Vec<String> {
    let hex = address.trim_start_matches("0x").to_ascii_lowercase();
    vec![hex, "addr".to_owned(), "reverse".to_owned()]
}
