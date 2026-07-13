use alloy_sol_types::{SolCall, sol};
use bigname_domain::normalization::normalize_name;

use crate::evm_abi::{hex_string, namehash_hex};

use super::account_execution::InnerCall;

sol! {
    // Resolver record writes. Shared function shapes across the admitted
    // resolver generations; each takes the node as its first argument.
    // (upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L15 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L383 @ ens_v2@554c309)
    function setText(bytes32 node, string calldata key, string calldata value) external;
    // (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L26 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L327 @ ens_v2@554c309)
    function setAddr(bytes32 node, address addr) external;
    // (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L47 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L539 @ ens_v2@554c309)
    function setAddr(bytes32 node, uint256 coinType, bytes calldata address_) external;
    // (upstream: .refs/ens_v1/contracts/resolvers/profiles/ContentHashResolver.sol:L14 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L334 @ ens_v2@554c309)
    function setContenthash(bytes32 node, bytes calldata hash) external;
    // (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L311 @ ens_v2@554c309)
    function setABI(bytes32 node, uint256 contentType, bytes calldata data) external;
    // (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L359 @ ens_v2@554c309)
    function setPubkey(bytes32 node, bytes32 x, bytes32 y) external;
    // (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L346 @ ens_v2@554c309)
    function setInterface(bytes32 node, bytes4 interfaceID, address implementer) external;
    // Resolver-local name record (forward `name()` value), distinct from the
    // reverse-registrar primary claim below.
    // (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L371 @ ens_v2@554c309)
    function setName(bytes32 node, string calldata newName) external;
    function clearRecords(bytes32 node) external;

    // Resolver batch wrappers.
    // (upstream: .refs/ens_v1/contracts/resolvers/Multicallable.sol:L40 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L520 @ ens_v2@554c309)
    function multicall(bytes[] calldata data) external;
    // (upstream: .refs/ens_v1/contracts/resolvers/Multicallable.sol:L33 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L405 @ ens_v2@554c309)
    function multicallWithNodeCheck(bytes32 nodehash, bytes[] calldata data) external;

    // Reverse-registrar primary-name claims; the name string is normalized and
    // namehashed for attribution.
    // (upstream: .refs/ens_v2/contracts/src/reverse-registrar/L2ReverseRegistrar.sol:L116 @ ens_v2@554c309)
    function setName(string calldata name) external;
    // (upstream: .refs/ens_v2/contracts/src/reverse-registrar/L2ReverseRegistrar.sol:L121 @ ens_v2@554c309)
    function setNameForAddr(address addr, string calldata name) external;
}

/// Nested resolver multicalls beyond this depth stop recursing; deeper
/// nesting has no legitimate sponsored-write shape.
const MAX_MULTICALL_DEPTH: usize = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WriteKind {
    Records,
    Primary,
}

impl WriteKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Records => "records",
            Self::Primary => "primary",
        }
    }
}

/// One name-attributed write recovered from an operation's inner calls.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NameWrite {
    pub(crate) write_kind: WriteKind,
    /// Namehash of the written name. `None` only for primary claims whose
    /// name string does not normalize.
    pub(crate) node: Option<String>,
    /// The claimed name string for primary writes; record writes carry only
    /// the node.
    pub(crate) name: Option<String>,
    pub(crate) target: String,
    pub(crate) source_call: &'static str,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ClassifiedWrites {
    pub(crate) writes: Vec<NameWrite>,
    /// Inner calls that matched no known sponsored-write selector.
    pub(crate) unrecognized_call_count: usize,
}

pub(crate) fn classify_inner_calls(inner_calls: &[InnerCall]) -> ClassifiedWrites {
    let mut classified = ClassifiedWrites::default();
    for inner_call in inner_calls {
        classify_call(&inner_call.target, &inner_call.data, 0, &mut classified);
    }
    dedupe_writes(&mut classified.writes);
    classified
}

fn classify_call(target: &str, data: &[u8], depth: usize, classified: &mut ClassifiedWrites) {
    let Some(selector) = data.get(..4) else {
        classified.unrecognized_call_count += 1;
        return;
    };
    let selector: [u8; 4] = selector.try_into().expect("selector slice is four bytes");

    match selector {
        multicallCall::SELECTOR => {
            if depth >= MAX_MULTICALL_DEPTH {
                classified.unrecognized_call_count += 1;
                return;
            }
            let Ok(call) = multicallCall::abi_decode_validate(data) else {
                classified.unrecognized_call_count += 1;
                return;
            };
            for entry in call.data {
                classify_call(target, entry.as_ref(), depth + 1, classified);
            }
        }
        multicallWithNodeCheckCall::SELECTOR => {
            if depth >= MAX_MULTICALL_DEPTH {
                classified.unrecognized_call_count += 1;
                return;
            }
            let Ok(call) = multicallWithNodeCheckCall::abi_decode_validate(data) else {
                classified.unrecognized_call_count += 1;
                return;
            };
            for entry in call.data {
                classify_call(target, entry.as_ref(), depth + 1, classified);
            }
        }
        setName_1Call::SELECTOR => {
            let Ok(call) = setName_1Call::abi_decode_validate(data) else {
                classified.unrecognized_call_count += 1;
                return;
            };
            push_primary_write(target, &call.name, "setName", classified);
        }
        setNameForAddrCall::SELECTOR => {
            let Ok(call) = setNameForAddrCall::abi_decode_validate(data) else {
                classified.unrecognized_call_count += 1;
                return;
            };
            push_primary_write(target, &call.name, "setNameForAddr", classified);
        }
        _ => {
            let Some((node, source_call)) = records_write_node(selector, data) else {
                classified.unrecognized_call_count += 1;
                return;
            };
            classified.writes.push(NameWrite {
                write_kind: WriteKind::Records,
                node: Some(node),
                name: None,
                target: target.to_owned(),
                source_call,
            });
        }
    }
}

fn records_write_node(selector: [u8; 4], data: &[u8]) -> Option<(String, &'static str)> {
    let source_call = match selector {
        setTextCall::SELECTOR => "setText",
        setAddr_0Call::SELECTOR => "setAddr",
        setAddr_1Call::SELECTOR => "setAddrCoinType",
        setContenthashCall::SELECTOR => "setContenthash",
        setABICall::SELECTOR => "setABI",
        setPubkeyCall::SELECTOR => "setPubkey",
        setInterfaceCall::SELECTOR => "setInterface",
        setName_0Call::SELECTOR => "setNameRecord",
        clearRecordsCall::SELECTOR => "clearRecords",
        _ => return None,
    };
    // Every record setter takes the node as its first ABI word.
    let node_word = data.get(4..4 + 32)?;
    Some((hex_string(node_word), source_call))
}

fn push_primary_write(
    target: &str,
    claimed_name: &str,
    source_call: &'static str,
    classified: &mut ClassifiedWrites,
) {
    if claimed_name.is_empty() {
        // Clearing the primary name sponsors no name-attributable write.
        classified.unrecognized_call_count += 1;
        return;
    }
    let node = normalize_name(claimed_name).ok().map(|normalized| {
        let labels = normalized
            .normalized_labels
            .iter()
            .map(|label| label.as_bytes().to_vec())
            .collect::<Vec<_>>();
        namehash_hex(&labels)
    });
    classified.writes.push(NameWrite {
        write_kind: WriteKind::Primary,
        node,
        name: Some(claimed_name.to_owned()),
        target: target.to_owned(),
        source_call,
    });
}

fn dedupe_writes(writes: &mut Vec<NameWrite>) {
    let mut seen = std::collections::HashSet::new();
    writes.retain(|write| {
        seen.insert((
            write.write_kind.as_str(),
            write.node.clone(),
            write.name.clone(),
        ))
    });
}
