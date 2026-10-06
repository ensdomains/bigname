//! Event ordering follows the pinned contract calls. Synthetic transactions
//! retain authorizing callers and approvals; no named event is hand-inserted.

use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::sol;
use anyhow::Result;

use super::{
    manifests::address,
    raw::{Writer, alternate_owner, owner},
    recipe::{ByteObservation, Recipe, ResolverCohort},
};

// (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L6-L17 @ ens_v1@91c966f)
sol! {
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    event Transfer(bytes32 indexed node, address owner);
    event NewResolver(bytes32 indexed node, address resolver);
    event ApprovalForAll(address indexed owner, address indexed operator, bool approved);
}
// (upstream: .refs/ens_v1/contracts/ethregistrar/IBaseRegistrar.sol:L8-L20 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/deployments/sepolia/BaseRegistrarImplementation.json:L188-L206 @ ens_v1@91c966f)
mod registrar {
    use alloy_sol_types::sol;
    sol! {
        event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
        event ControllerAdded(address indexed controller);
        event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
    }
}
// (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L27-L33 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L258 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L5 @ ens_v1@91c966f)
sol! {
    event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);
    event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
    event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value);
}

pub(super) fn structural(writer: &mut Writer<'_>, recipe: &Recipe) -> Result<i64> {
    let bootstrap = writer.log(
        "registry",
        NewOwner {
            node: B256::ZERO,
            label: recipe.eth_labelhash,
            owner: owner(0),
        },
    )?;
    writer.transaction_at(1, owner(0), address("registry"), vec![bootstrap])?;
    // setSubnodeOwner computes the child from the parent and label and emits
    // NewOwner. (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f)
    for node in &recipe.nodes {
        let parent = node.parent.map_or(recipe.eth_namehash, |index| {
            recipe.nodes[index as usize].namehash
        });
        let sender = owner(node.parent.unwrap_or(0));
        let local = node.ordinal % 1_000;
        let block = 2
            + i64::from(node.ordinal / 1_000) * 160
            + if local < 512 {
                0
            } else {
                1 + i64::from((local - 512) / 4)
            };
        let event = writer.log(
            "registry",
            NewOwner {
                node: parent,
                label: node.labelhash,
                owner: owner(node.ordinal),
            },
        )?;
        writer.transaction_at(block, sender, address("registry"), vec![event])?;
    }
    Ok(1 + recipe.nodes.len() as i64 / 1_000 * 160)
}

pub(super) fn changes(writer: &mut Writer<'_>, recipe: &Recipe, from: i64) -> Result<i64> {
    writer.begin_epoch(from)?;
    for node in &recipe.nodes {
        let original = owner(node.ordinal);
        let parent_hash = node.parent.map_or(recipe.eth_namehash, |index| {
            recipe.nodes[index as usize].namehash
        });
        let mut current = original;
        let changes = node.later_changes();
        for step in 0..changes {
            let target = match changes {
                2 => {
                    if step == 0 {
                        alternate_owner()
                    } else {
                        original
                    }
                }
                3 => {
                    if step == 0 {
                        Address::ZERO
                    } else {
                        original
                    }
                }
                _ => match step % 4 {
                    0 => alternate_owner(),
                    1 => Address::ZERO,
                    _ => original,
                },
            };
            if current == Address::ZERO {
                let event = writer.log(
                    "registry",
                    NewOwner {
                        node: parent_hash,
                        label: node.labelhash,
                        owner: target,
                    },
                )?;
                writer.transaction(
                    owner(node.parent.unwrap_or(0)),
                    address("registry"),
                    vec![event],
                )?;
            } else {
                let event = writer.log(
                    "registry",
                    Transfer {
                        node: node.namehash,
                        owner: target,
                    },
                )?;
                writer.transaction(current, address("registry"), vec![event])?;
            }
            current = target;
        }
        match node.resolver_cohort() {
            ResolverCohort::None => {}
            ResolverCohort::Stable => {
                pointer(writer, node.namehash, original, "public_resolver")?;
                record(
                    writer,
                    node.namehash,
                    original,
                    "public_resolver",
                    "current",
                )?;
            }
            ResolverCohort::Changing => {
                pointer(writer, node.namehash, original, "public_resolver")?;
                record(writer, node.namehash, original, "public_resolver", "prior")?;
                pointer(writer, node.namehash, original, "public_resolver_8948458")?;
                record(
                    writer,
                    node.namehash,
                    original,
                    "public_resolver",
                    "obsolete",
                )?;
                record(
                    writer,
                    node.namehash,
                    original,
                    "public_resolver_8948458",
                    "current",
                )?;
            }
        }
    }
    Ok(writer.counts.last_block)
}

fn pointer(writer: &mut Writer<'_>, node: B256, sender: Address, role: &str) -> Result<()> {
    let event = writer.log(
        "registry",
        NewResolver {
            node,
            resolver: address(role),
        },
    )?;
    writer.transaction(sender, address("registry"), vec![event])
}

fn record(
    writer: &mut Writer<'_>,
    node: B256,
    sender: Address,
    role: &str,
    value: &str,
) -> Result<()> {
    let event = writer.log(
        role,
        TextChanged {
            node,
            indexedKey: keccak256(b"url"),
            key: "url".to_owned(),
            value: format!("https://{value}.example/{node:#x}"),
        },
    )?;
    writer.transaction(sender, address(role), vec![event])
}

pub(super) fn bytes(writer: &mut Writer<'_>, recipe: &Recipe, from: i64) -> Result<i64> {
    writer.begin_epoch(from)?;
    let registrar = address("registrar");
    let wrapper = address("name_wrapper");
    let controller = alternate_owner();
    let owner_event = writer.log(
        "registry",
        Transfer {
            node: recipe.eth_namehash,
            owner: registrar,
        },
    )?;
    writer.transaction(owner(0), address("registry"), vec![owner_event])?;
    let controller_event = writer.log("registrar", registrar::ControllerAdded { controller })?;
    writer.transaction(owner(0), registrar, vec![controller_event])?;
    for number in 0..100 {
        for role in ["registry", "registrar"] {
            let approval = writer.log(
                role,
                ApprovalForAll {
                    owner: owner(number),
                    operator: wrapper,
                    approved: true,
                },
            )?;
            writer.transaction(owner(number), address(role), vec![approval])?;
        }
    }
    for node in &recipe.nodes {
        if matches!(node.byte_observation(), ByteObservation::None) {
            continue;
        }
        let node_owner = owner(node.ordinal);
        let expiry = 2_000_000_000_u64 + u64::from(node.ordinal % 4) * 86_400;
        let mut logs = Vec::new();
        let (fuses, wrapper_expiry) = if node.parent.is_none() {
            // registerOnly emits mint then numeric grant, without changing ENS.
            // wrapETH2LD transfers the registrar token, reclaims ENS, then mints.
            // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L122-L157 @ ens_v1@91c966f)
            // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L246-L278 @ ens_v1@91c966f)
            let id = U256::from_be_slice(node.labelhash.as_slice());
            let mint = writer.log(
                "registrar",
                registrar::Transfer {
                    from: Address::ZERO,
                    to: node_owner,
                    tokenId: id,
                },
            )?;
            let grant = writer.log(
                "registrar",
                registrar::NameRegistered {
                    id,
                    owner: node_owner,
                    expires: U256::from(expiry),
                },
            )?;
            writer.transaction(controller, registrar, vec![mint, grant])?;
            logs.push(writer.log(
                "registrar",
                registrar::Transfer {
                    from: node_owner,
                    to: wrapper,
                    tokenId: id,
                },
            )?);
            logs.push(writer.log(
                "registry",
                NewOwner {
                    node: recipe.eth_namehash,
                    label: node.labelhash,
                    owner: wrapper,
                },
            )?);
            (196_608, expiry + 90 * 86_400)
        } else {
            // Generic wrap retains the resolver and uses zero fuses/expiry.
            // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L347-L374 @ ens_v1@91c966f)
            logs.push(writer.log(
                "registry",
                Transfer {
                    node: node.namehash,
                    owner: wrapper,
                },
            )?);
            (0, 0)
        };
        logs.push(writer.log(
            "name_wrapper",
            TransferSingle {
                operator: node_owner,
                from: Address::ZERO,
                to: node_owner,
                id: U256::from_be_slice(node.namehash.as_slice()),
                value: U256::from(1),
            },
        )?);
        let mut dns = Vec::new();
        for label in recipe.raw_labels(node.ordinal) {
            dns.push(label.len() as u8);
            dns.extend_from_slice(label);
        }
        dns.push(0);
        logs.push(writer.log(
            "name_wrapper",
            NameWrapped {
                node: node.namehash,
                name: dns.into(),
                owner: node_owner,
                fuses,
                expiry: wrapper_expiry,
            },
        )?);
        writer.transaction(node_owner, wrapper, logs)?;
    }
    Ok(writer.counts.last_block)
}
