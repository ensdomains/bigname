use alloy_primitives::B256;
use alloy_sol_types::sol;
use anyhow::{Context, bail};

use super::V1BatchDependencies;
use crate::{
    evm_abi::{decode_event_log, decode_event_log_data_as, hex_string},
    schema_v2::{
        catalog::Selected,
        common::{decode_dns_labels, namehash_raw},
        model::RawLogInput,
        protocol::v1::{
            authority_transition::child_node,
            registrar::{decode, identity::registrar_namehash},
            wrapper,
        },
    },
};

sol! {
    event RawNameChanged(bytes32 indexed node, bytes name);
    event RawNameForAddrChanged(address indexed addr, bytes name);
    event RawNamedResource(uint256 indexed resource, bytes name);
    event RawNamedAddrResource(uint256 indexed resource, bytes name, uint256 indexed coinType);
    event RawLinked(uint256 indexed recordId, bytes32 indexed node, bytes name);
    event RawNamedTextResource(uint256 indexed resource, bytes name, bytes32 indexed keyHash, bytes key);
    event RawBridgeNameRenewed(uint256 indexed tokenId, bytes label, uint64 duration, uint64 newExpiry, address paymentToken, bytes32 indexed referrer, uint256 amount);
}

fn topic(raw: &RawLogInput, index: usize) -> anyhow::Result<B256> {
    raw.topics
        .get(index)
        .context("missing dependency topic")?
        .parse()
        .context("invalid dependency topic")
}

fn labels(
    dependencies: &mut V1BatchDependencies,
    namespace: &str,
    labels: &[Vec<u8>],
) -> anyhow::Result<()> {
    for start in 0..=labels.len() {
        dependencies.node(
            namespace,
            &namehash_raw(labels[start..].iter().map(Vec::as_slice)),
        )?;
    }
    Ok(())
}

pub(super) fn collect(
    selected: &Selected,
    raw: &RawLogInput,
    out: &mut V1BatchDependencies,
) -> anyhow::Result<()> {
    let namespace = &selected.source.namespace;
    let event = selected.event.name.as_str();
    let family = selected.source.source_family.as_str();
    if family.ends_with("_execution") || family == "basenames_l1_compat" {
        return Ok(());
    }
    // Account-scoped approvals do not read node state. Their prior stream tail is loaded by
    // the existing state_value_requests path after prepare.
    match selected.event.signature.as_str() {
        bigname_manifests::APPROVAL_FOR_ALL_SIGNATURE | bigname_manifests::APPROVED_SIGNATURE => {
            return Ok(());
        }
        bigname_manifests::APPROVAL_SIGNATURE => {
            if family == "ens_v1_wrapper_l1" {
                out.node(namespace, &format!("{:#x}", topic(raw, 3)?))?;
            }
            return Ok(());
        }
        _ => {}
    }
    match family {
        "ens_v1_registry_l1" | "basenames_base_registry" => match event {
            "NewOwner" => {
                let parent = topic(raw, 1)?;
                out.node(namespace, &format!("{parent:#x}"))?;
                out.node(namespace, &child_node(parent, topic(raw, 2)?))?;
            }
            "Transfer" | "NewResolver" | "NewTTL" => {
                out.node(namespace, &format!("{:#x}", topic(raw, 1)?))?
            }
            _ => unsupported(out, selected),
        },
        "ens_v1_registrar_l1" | "basenames_base_registrar" => match event {
            "NameRegistered" | "NameRenewed" => {
                let label = if selected
                    .event
                    .signature
                    .starts_with("NameRegistered(uint256,")
                    || selected.event.signature.starts_with("NameRenewed(uint256,")
                {
                    topic(raw, 1)?
                } else {
                    let (label, hash, _) = decode::name(selected, raw)?;
                    let mut name = vec![label];
                    if family == "basenames_base_registrar" {
                        name.push(b"base".to_vec());
                    }
                    name.push(b"eth".to_vec());
                    labels(out, namespace, &name)?;
                    hash
                };
                out.node(namespace, &registrar_namehash(selected, label))?;
            }
            "Transfer" => out.node(namespace, &registrar_namehash(selected, topic(raw, 3)?))?,
            // ENSv1→ENSv2 migration correlation only: the controller set is emptied at the start
            // of every transaction, and the correlation reads the batch, not prior state.
            "ControllerAdded" | "ControllerRemoved" => {}
            // A registrar proxy upgrade (the Basenames upgradeable controller declares one)
            // reads no name state.
            "Upgraded" => {}
            _ => unsupported(out, selected),
        },
        "ens_v1_wrapper_l1" => match event {
            "NameWrapped" => {
                let event = decode_event_log::<wrapper::NameWrapped>(
                    &raw.topics,
                    &raw.data,
                    "NameWrapped log is malformed",
                )?;
                let raw_labels = decode_dns_labels(&event.name)?;
                labels(out, namespace, &raw_labels)?;
                out.node(namespace, &hex_string(event.node))?;
            }
            "NameUnwrapped" | "ExpiryExtended" | "FusesSet" => {
                out.node(namespace, &format!("{:#x}", topic(raw, 1)?))?
            }
            "TransferSingle" => {
                let event = decode_event_log::<wrapper::TransferSingle>(
                    &raw.topics,
                    &raw.data,
                    "TransferSingle log is malformed",
                )?;
                out.node(
                    namespace,
                    &format!("{:#x}", B256::from(event.id.to_be_bytes::<32>())),
                )?;
            }
            "TransferBatch" => {
                let event = decode_event_log::<wrapper::TransferBatch>(
                    &raw.topics,
                    &raw.data,
                    "TransferBatch log is malformed",
                )?;
                for id in event.ids {
                    out.node(
                        namespace,
                        &format!("{:#x}", B256::from(id.to_be_bytes::<32>())),
                    )?;
                }
            }
            _ => unsupported(out, selected),
        },
        "ens_v1_resolver_l1" | "basenames_base_resolver" => match event {
            "AddrChanged" | "AddressChanged" | "NameChanged" | "TextChanged" | "ContentChanged"
            | "ContenthashChanged" | "ABIChanged" | "DNSRecordChanged" | "DNSRecordDeleted"
            | "DNSZonehashChanged" | "InterfaceChanged" | "VersionChanged" | "DataChanged" => {
                out.node(namespace, &format!("{:#x}", topic(raw, 1)?))?;
                if event == "NameChanged" {
                    let decoded = decode_event_log_data_as::<RawNameChanged>(
                        &raw.topics,
                        &raw.data,
                        &selected.event.topic0,
                        "NameChanged log is malformed",
                    )?;
                    labels(
                        out,
                        namespace,
                        &decoded
                            .name
                            .split(|b| *b == b'.')
                            .map(<[u8]>::to_vec)
                            .collect::<Vec<_>>(),
                    )?;
                }
            }
            _ => unsupported(out, selected),
        },
        "ens_v1_reverse_l1" | "basenames_base_primary" => match event {
            "ReverseClaimed" => out.node(namespace, &format!("{:#x}", topic(raw, 2)?))?,
            "NameForAddrChanged" => {
                let event = decode_event_log_data_as::<RawNameForAddrChanged>(
                    &raw.topics,
                    &raw.data,
                    &selected.event.topic0,
                    "NameForAddrChanged log is malformed",
                )?;
                labels(
                    out,
                    namespace,
                    &event
                        .name
                        .split(|b| *b == b'.')
                        .map(<[u8]>::to_vec)
                        .collect::<Vec<_>>(),
                )?;
            }
            _ => unsupported(out, selected),
        },
        // The registry's own parent claim, and its state under each indexed word: token ids,
        // resources and account words alike, a superset. Data-only ids (ERC-1155 transfers)
        // and the parent a claim names are found by the loader's retry.
        "ens_v2_root_l1" | "ens_v2_registry_l1" => match event {
            "RegistryCreated" | "LabelRegistered" | "LabelReserved" | "LabelUnregistered"
            | "ExpiryUpdated" | "SubregistryUpdated" | "ResolverUpdated" | "TokenResource"
            | "TransferSingle" | "TransferBatch" | "EACRolesChanged" | "TokenRegenerated"
            | "ParentUpdated" | "Upgraded" => {
                out.v2_keys
                    .insert(super::v2_key(&raw.emitting_address, "-"));
                v2_topic_keys(out, &raw.emitting_address, raw);
            }
            _ => unsupported(out, selected),
        },
        "ens_v2_registrar_l1" => match event {
            "NameRegistered" | "NameRenewed" => {}
            _ => unsupported(out, selected),
        },
        "ens_v2_resolver_l1" => {
            // Preimage hints are kept per resolver address, resource arguments per contract
            // instance.
            v2_topic_keys(out, &raw.emitting_address, raw);
            v2_topic_keys(out, &selected.contract_instance_id.to_string(), raw);
            v2_resolver(selected, raw, out, namespace, event)?;
        }
        "ens_v2_migration_l1" => match event {
            "ProxyDeployed" => {}
            "NameRenewed" => {
                let decoded = decode_event_log_data_as::<RawBridgeNameRenewed>(
                    &raw.topics,
                    &raw.data,
                    &selected.event.topic0,
                    "migration bridge NameRenewed log is malformed",
                )?;
                // The bridge renews `.eth` names only (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registrar/interfaces/IETHRenewer.sol:L15 @ ens_v2_sepolia_20260916@366de741).
                labels(out, namespace, &[decoded.label.to_vec(), b"eth".to_vec()])?;
            }
            _ => unsupported(out, selected),
        },
        family if !super::supported_family(family) => {
            unsupported(out, selected);
        }
        _ => bail!("V1 lookahead has no decoder for {family}"),
    }
    Ok(())
}

fn v2_resolver(
    selected: &Selected,
    raw: &RawLogInput,
    out: &mut V1BatchDependencies,
    namespace: &str,
    event: &str,
) -> anyhow::Result<()> {
    match event {
        // Node record events look the node's name up in ENSv1-model name state.
        "ABIChanged" | "AddrChanged" | "AddressChanged" | "TextChanged" | "ContenthashChanged"
        | "VersionChanged" => out.node(namespace, &format!("{:#x}", topic(raw, 1)?))?,
        "NameChanged" => {
            out.node(namespace, &format!("{:#x}", topic(raw, 1)?))?;
            let decoded = decode_event_log_data_as::<RawNameChanged>(
                &raw.topics,
                &raw.data,
                &selected.event.topic0,
                "NameChanged log is malformed",
            )?;
            labels(
                out,
                namespace,
                &decoded
                    .name
                    .split(|b| *b == b'.')
                    .map(<[u8]>::to_vec)
                    .collect::<Vec<_>>(),
            )?;
        }
        "NamedResource" | "NamedAddrResource" | "NamedTextResource" | "Linked" => {
            let (topics, data, topic0) = (&raw.topics, &raw.data, &selected.event.topic0);
            let context = "named resolver log is malformed";
            let name = match event {
                "NamedResource" => {
                    decode_event_log_data_as::<RawNamedResource>(topics, data, topic0, context)?
                        .name
                }
                "NamedAddrResource" => {
                    decode_event_log_data_as::<RawNamedAddrResource>(topics, data, topic0, context)?
                        .name
                }
                "NamedTextResource" => {
                    decode_event_log_data_as::<RawNamedTextResource>(topics, data, topic0, context)?
                        .name
                }
                _ => decode_event_log_data_as::<RawLinked>(topics, data, topic0, context)?.name,
            };
            if let Ok(raw_labels) = decode_dns_labels(&name)
                && !raw_labels.is_empty()
            {
                labels(out, namespace, &raw_labels)?;
            }
        }
        "ResolverCreated" | "AddressUpdated" | "ContenthashUpdated" | "ABIUpdated"
        | "InterfaceUpdated" | "TextUpdated" | "DataUpdated" | "NameUpdated"
        | "ResourceArgument" | "EACRolesChanged" | "Upgraded" => {}
        _ => unsupported(out, selected),
    }
    Ok(())
}

fn v2_topic_keys(out: &mut V1BatchDependencies, address: &str, raw: &RawLogInput) {
    for word in raw.topics.iter().skip(1) {
        out.v2_keys.insert(super::v2_key(address, word));
    }
}

fn unsupported(out: &mut V1BatchDependencies, selected: &Selected) {
    out.unsupported.insert(format!(
        "{}:{}",
        selected.source.source_family, selected.event.signature
    ));
}
