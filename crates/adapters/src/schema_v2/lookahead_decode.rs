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
        "ens_v1_registry_l1" => match event {
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
        "ens_v1_registrar_l1" => match event {
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
                    labels(out, namespace, &[label, b"eth".to_vec()])?;
                    hash
                };
                out.node(namespace, &registrar_namehash(selected, label))?;
            }
            "Transfer" => out.node(namespace, &registrar_namehash(selected, topic(raw, 3)?))?,
            // The admitted V1 dispatcher only uses these for V2 migration correlation;
            // active migration manifests are rejected by the caller before interpretation.
            "ControllerAdded" | "ControllerRemoved" => {}
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
        "ens_v1_resolver_l1" => match event {
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
        "ens_v1_reverse_l1" => match event {
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
        family if !super::supported_family(family) => {
            unsupported(out, selected);
        }
        _ => bail!("V1 lookahead has no decoder for {family}"),
    }
    Ok(())
}

fn unsupported(out: &mut V1BatchDependencies, selected: &Selected) {
    out.unsupported.insert(format!(
        "{}:{}",
        selected.source.source_family, selected.event.signature
    ));
}
