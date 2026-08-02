pub(super) mod permissions;
mod v1;
mod v2_registry;
mod v2_resolver;

use anyhow::bail;
use serde_json::Value;
use uuid::Uuid;

use super::{catalog::Selected, manifest::ManifestSource, model::RawLogInput, state::State};

#[derive(Clone, Debug)]
pub(super) struct Interpreted {
    pub events: Vec<EventDraft>,
    pub labels: Vec<LabelDraft>,
    pub names: Vec<NameDraft>,
    pub resources: Vec<ResourceDraft>,
    pub binding_closures: Vec<BindingClosureDraft>,
    pub bindings: Vec<BindingDraft>,
    pub discovery: Vec<DiscoveryDraft>,
}

impl Interpreted {
    pub(super) fn new() -> Self {
        Self {
            events: Vec::new(),
            labels: Vec::new(),
            names: Vec::new(),
            resources: Vec::new(),
            binding_closures: Vec::new(),
            bindings: Vec::new(),
            discovery: Vec::new(),
        }
    }

    pub(super) fn append(&mut self, other: &mut Self) {
        self.events.append(&mut other.events);
        self.labels.append(&mut other.labels);
        self.names.append(&mut other.names);
        self.resources.append(&mut other.resources);
        self.binding_closures.append(&mut other.binding_closures);
        self.bindings.append(&mut other.bindings);
        self.discovery.append(&mut other.discovery);
    }
}

pub(super) fn v2_boundary_expiration(
    transition: super::state::V2NameTransition,
) -> anyhow::Result<Interpreted> {
    v2_registry::boundary_expiration(transition)
}

#[derive(Clone, Debug)]
pub(super) struct EventDraft {
    pub event_kind: String,
    pub logical_name_id: Option<String>,
    pub resource_id: Option<Uuid>,
    pub identity_suffix: String,
    pub explicit_before: Option<Value>,
    pub after_state: Value,
    pub state_scope: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct LabelDraft {
    pub raw_label: String,
    pub source_kind: String,
}

#[derive(Clone, Debug)]
pub(super) struct NameDraft {
    pub labels: Vec<String>,
    pub namehash: String,
    pub resource_id: Option<Uuid>,
    pub token_lineage_id: Option<Uuid>,
    pub surface_binding_id: Option<Uuid>,
    pub bind: bool,
    pub binding_kind: String,
    pub source_kind: String,
    pub preimage_metadata: Option<Value>,
}

#[derive(Clone, Debug)]
pub(super) struct ResourceDraft {
    pub resource_id: Uuid,
    pub token_lineage_id: Option<Uuid>,
}

#[derive(Clone, Debug)]
pub(super) struct BindingClosureDraft {
    pub logical_name_id: String,
}

#[derive(Clone, Debug)]
pub(super) struct BindingDraft {
    pub logical_name_id: String,
    pub resource_id: Uuid,
    pub binding_kind: String,
    pub surface_binding_id: Option<Uuid>,
}

#[derive(Clone, Debug)]
pub(super) enum DiscoveryDraft {
    RegistryAnnouncement,
    Close {
        edge_kind: String,
        observation_key: String,
    },
    Edge {
        edge_kind: String,
        to_address: String,
        admission_basis: String,
        observation_key: String,
    },
}

pub(super) fn interpret(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    let mut output = match selected.source.source_family.as_str() {
        family if family.starts_with("ens_v1_") || family.starts_with("basenames_") => {
            v1::interpret(selected, raw, state)
        }
        "ens_v2_registry_l1" | "ens_v2_root_l1" | "ens_v2_registrar_l1" => {
            v2_registry::interpret(selected, raw, state)
        }
        "ens_v2_resolver_l1" => v2_resolver::interpret(selected, raw, state),
        family => bail!("source family {family} has no schema-v2 adapter"),
    }?;
    for event in &mut output.events {
        if event.state_scope.is_empty() {
            event.state_scope = state_scope(selected, raw, event);
        }
    }
    Ok(output)
}

pub(super) fn reconcile_batch(output: &mut super::model::BatchOutput) {
    v1::reconcile_same_transaction_setups(output);
}

fn state_scope(selected: &Selected, raw: &RawLogInput, event: &EventDraft) -> String {
    let after = &event.after_state;
    let subject = after.get("subject").and_then(Value::as_str).unwrap_or("-");
    let node = after
        .get("child_node")
        .or_else(|| after.get("reverse_node"))
        .or_else(|| after.get("node"))
        .or_else(|| {
            after
                .get("primary_claim_source")
                .and_then(|source| source.get("reverse_node"))
        })
        .and_then(Value::as_str)
        .unwrap_or("-");
    let token = after
        .get("token_id")
        .or_else(|| after.get("current_token_id"))
        .or_else(|| after.get("old_token_id"))
        .and_then(Value::as_str)
        .unwrap_or("-");
    let source_event = after
        .get("source_event")
        .and_then(Value::as_str)
        .unwrap_or(event.event_kind.as_str());
    let selector = if selected.source.source_family == "ens_v1_wrapper_l1"
        && matches!(source_event, "NameWrapped" | "ExpiryExtended" | "FusesSet")
    {
        "wrapper".to_owned()
    } else if event.event_kind == "RecordChanged"
        && let Some(record_key) = after.get("record_key").and_then(Value::as_str)
    {
        record_key.to_owned()
    } else {
        match source_event {
            "AddrChanged" => "address:60".to_owned(),
            "AddressChanged" | "NamedAddrResource" => format!(
                "address:{}",
                after
                    .get("coin_type")
                    .and_then(Value::as_str)
                    .unwrap_or("-")
            ),
            "TextChanged" | "NamedTextResource" => format!(
                "text:{}",
                after.get("key").and_then(Value::as_str).unwrap_or("-")
            ),
            "VersionChanged" => "version".to_owned(),
            "NameChanged" => "name".to_owned(),
            "ContentChanged" => "content".to_owned(),
            "ContenthashChanged" => "contenthash".to_owned(),
            "ABIChanged" => format!(
                "abi:{}",
                after
                    .get("content_type")
                    .and_then(Value::as_str)
                    .unwrap_or("-")
            ),
            "InterfaceChanged" => format!(
                "interface:{}",
                after
                    .get("interface_id")
                    .and_then(Value::as_str)
                    .unwrap_or("-")
            ),
            "DNSRecordChanged" | "DNSRecordDeleted" => format!(
                "dns:{}:{}",
                after.get("name").and_then(Value::as_str).unwrap_or("-"),
                after
                    .get("resource")
                    .map(Value::to_string)
                    .unwrap_or_else(|| "-".to_owned())
            ),
            "DNSZonehashChanged" => "dns-zonehash".to_owned(),
            source => source.to_owned(),
        }
    };
    format!(
        "{}:{node}:{token}:{subject}:{selector}",
        raw.emitting_address.to_ascii_lowercase()
    )
}

pub(super) fn validate_manifest(source: &ManifestSource) -> anyhow::Result<()> {
    for event in &source.events {
        if !supports_signature(&source.source_family, &event.signature) {
            bail!(
                "source family {} has no typed schema-v2 adapter for {}",
                source.source_family,
                event.signature
            );
        }
    }
    Ok(())
}

fn supports_signature(source_family: &str, signature: &str) -> bool {
    match source_family {
        "ens_v1_registrar_l1" | "basenames_base_registrar" => matches!(
            signature,
            "NameRegistered(string,bytes32,address,uint256)"
                | "NameRegistered(string,bytes32,address,uint256,uint256)"
                | "NameRegistered(string,bytes32,address,uint256,uint256,uint256)"
                | "NameRegistered(string,bytes32,address,uint256,uint256,uint256,bytes32)"
                | "NameRenewed(string,bytes32,uint256)"
                | "NameRenewed(string,bytes32,uint256,uint256)"
                | "NameRenewed(string,bytes32,uint256,uint256,bytes32)"
                | "Transfer(address,address,uint256)"
                | "Upgraded(address)"
        ),
        "ens_v1_registry_l1" | "basenames_base_registry" => matches!(
            signature,
            "NewOwner(bytes32,bytes32,address)"
                | "Transfer(bytes32,address)"
                | "NewResolver(bytes32,address)"
                | "NewTTL(bytes32,uint64)"
        ),
        "ens_v1_resolver_l1" | "basenames_base_resolver" => matches!(
            signature,
            "ABIChanged(bytes32,uint256)"
                | "AddrChanged(bytes32,address)"
                | "AddressChanged(bytes32,uint256,bytes)"
                | "ContentChanged(bytes32,bytes32)"
                | "ContenthashChanged(bytes32,bytes)"
                | "DNSRecordChanged(bytes32,bytes,uint16,bytes)"
                | "DNSRecordDeleted(bytes32,bytes,uint16)"
                | "DNSZonehashChanged(bytes32,bytes,bytes)"
                | "DataChanged(bytes32,string,string,bytes)"
                | "InterfaceChanged(bytes32,bytes4,address)"
                | "NameChanged(bytes32,string)"
                | "TextChanged(bytes32,string,string)"
                | "TextChanged(bytes32,string,string,string)"
                | "VersionChanged(bytes32,uint64)"
        ),
        "ens_v1_wrapper_l1" => matches!(
            signature,
            "ExpiryExtended(bytes32,uint64)"
                | "FusesSet(bytes32,uint32)"
                | "NameUnwrapped(bytes32,address)"
                | "NameWrapped(bytes32,bytes,address,uint32,uint64)"
                | "TransferBatch(address,address,address,uint256[],uint256[])"
                | "TransferSingle(address,address,address,uint256,uint256)"
        ),
        "ens_v1_reverse_l1" => signature == "ReverseClaimed(address,bytes32)",
        "basenames_base_primary" => signature == "NameForAddrChanged(address,string)",
        "ens_v2_registrar_l1" => matches!(
            signature,
            "NameRegistered(uint256,string,address,address,address,uint64,address,bytes32,uint256,uint256)"
                | "NameRenewed(uint256,string,uint64,uint64,address,bytes32,uint256)"
        ),
        "ens_v2_registry_l1" | "ens_v2_root_l1" => matches!(
            signature,
            "RegistryCreated()"
                | "LabelRegistered(uint256,bytes32,string,address,uint64,address)"
                | "LabelReserved(uint256,bytes32,string,uint64,address)"
                | "LabelUnregistered(uint256,address)"
                | "ExpiryUpdated(uint256,uint64,address)"
                | "SubregistryUpdated(uint256,address,address)"
                | "ResolverUpdated(uint256,address,address)"
                | "TokenResource(uint256,uint256)"
                | "TransferSingle(address,address,address,uint256,uint256)"
                | "TransferBatch(address,address,address,uint256[],uint256[])"
                | "EACRolesChanged(uint256,address,uint256,uint256)"
                | "TokenRegenerated(uint256,uint256)"
                | "ParentUpdated(address,string,address)"
                | "Upgraded(address)"
        ),
        "ens_v2_resolver_l1" => matches!(
            signature,
            "AddressChanged(bytes32,uint256,bytes)"
                | "TextChanged(bytes32,string,string,string)"
                | "ContenthashChanged(bytes32,bytes)"
                | "NameChanged(bytes32,string)"
                | "VersionChanged(bytes32,uint64)"
                | "AliasChanged(bytes,bytes,bytes,bytes)"
                | "NamedResource(uint256,bytes)"
                | "NamedTextResource(uint256,bytes,bytes32,string)"
                | "NamedAddrResource(uint256,bytes,uint256)"
                | "EACRolesChanged(uint256,address,uint256,uint256)"
                | "Upgraded(address)"
        ),
        _ => false,
    }
}

pub(super) fn ensure_declared(selected: &Selected, expected: &[&str]) -> anyhow::Result<()> {
    for event_kind in expected {
        if !selected
            .event
            .normalized_events
            .iter()
            .any(|declared| declared == event_kind)
        {
            bail!(
                "manifest event {} for {} does not declare required normalized event {}",
                selected.event.signature,
                selected.source.source_family,
                event_kind
            );
        }
    }
    Ok(())
}
