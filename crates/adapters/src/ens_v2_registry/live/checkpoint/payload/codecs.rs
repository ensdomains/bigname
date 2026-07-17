use anyhow::{Context, Result, bail, ensure};
use bigname_storage::{CanonicalityState, SurfaceBindingKind};
use sqlx::types::time::OffsetDateTime;

use crate::ens_v2_registry::types::{
    NameMetadata, ObservationRef, RegistryNameState, RegistryResourceLink,
};

use super::{
    NameMetadataPayload, ObservationRefPayload, RegistryNameStatePayload,
    RegistryResourceLinkPayload, parse_uuid,
};

impl RegistryNameStatePayload {
    pub(super) fn from_state(state: &RegistryNameState) -> Self {
        Self {
            token_id: state.token_id.clone(),
            labelhash: state.labelhash.clone(),
            label: state.label.clone(),
            full_name: state.full_name.clone(),
            name: NameMetadataPayload::from_metadata(&state.name),
            owner: state.owner.clone(),
            expiry: state.expiry,
            status: state.status.to_owned(),
            first_ref: ObservationRefPayload::from_ref(&state.first_ref),
            current_ref: ObservationRefPayload::from_ref(&state.current_ref),
            registry_address: state.registry_address.clone(),
            registry_contract_instance_id: state.registry_contract_instance_id.to_string(),
            source_manifest_id: state.source_manifest_id,
            source_family: state.source_family.clone(),
            manifest_version: state.manifest_version,
            resource: state
                .resource
                .as_ref()
                .map(RegistryResourceLinkPayload::from_link),
            resolver: state.resolver.clone(),
            subregistry: state.subregistry.clone(),
            binding_kind: state.binding_kind.as_str().to_owned(),
        }
    }

    pub(super) fn into_state(self, chain: &str) -> Result<RegistryNameState> {
        Ok(RegistryNameState {
            token_id: self.token_id,
            labelhash: self.labelhash,
            label: self.label,
            full_name: self.full_name,
            name: self.name.into_metadata(),
            owner: self.owner,
            expiry: self.expiry,
            status: parse_status(&self.status)?,
            first_ref: self.first_ref.into_ref(chain)?,
            current_ref: self.current_ref.into_ref(chain)?,
            registry_address: self.registry_address,
            registry_contract_instance_id: parse_uuid(
                &self.registry_contract_instance_id,
                "registry state",
            )?,
            source_manifest_id: self.source_manifest_id,
            source_family: self.source_family,
            manifest_version: self.manifest_version,
            resource: self
                .resource
                .map(|resource| resource.into_link(chain))
                .transpose()?,
            resolver: self.resolver,
            subregistry: self.subregistry,
            binding_kind: SurfaceBindingKind::parse(&self.binding_kind)?,
        })
    }
}

impl RegistryResourceLinkPayload {
    fn from_link(link: &RegistryResourceLink) -> Self {
        Self {
            upstream_resource: link.upstream_resource.clone(),
            observed_token_id: link.observed_token_id.clone(),
            observed_expiry: link.observed_expiry,
            resource_id: link.resource_id.to_string(),
            token_lineage_id: link.token_lineage_id.to_string(),
            surface_binding_id: link.surface_binding_id.to_string(),
            linked_ref: ObservationRefPayload::from_ref(&link.linked_ref),
        }
    }

    fn into_link(self, chain: &str) -> Result<RegistryResourceLink> {
        Ok(RegistryResourceLink {
            upstream_resource: self.upstream_resource,
            observed_token_id: self.observed_token_id,
            observed_expiry: self.observed_expiry,
            resource_id: parse_uuid(&self.resource_id, "resource")?,
            token_lineage_id: parse_uuid(&self.token_lineage_id, "token lineage")?,
            surface_binding_id: parse_uuid(&self.surface_binding_id, "surface binding")?,
            linked_ref: self.linked_ref.into_ref(chain)?,
        })
    }
}

impl NameMetadataPayload {
    fn from_metadata(name: &NameMetadata) -> Self {
        Self {
            namespace: name.namespace.clone(),
            logical_name_id: name.logical_name_id.clone(),
            input_name: name.input_name.clone(),
            canonical_display_name: name.canonical_display_name.clone(),
            normalized_name: name.normalized_name.clone(),
            dns_encoded_name: name.dns_encoded_name.clone(),
            namehash: name.namehash.clone(),
            labelhashes: name.labelhashes.clone(),
            normalizer_version: name.normalizer_version.clone(),
        }
    }

    fn into_metadata(self) -> NameMetadata {
        NameMetadata {
            namespace: self.namespace,
            logical_name_id: self.logical_name_id,
            input_name: self.input_name,
            canonical_display_name: self.canonical_display_name,
            normalized_name: self.normalized_name,
            dns_encoded_name: self.dns_encoded_name,
            namehash: self.namehash,
            labelhashes: self.labelhashes,
            normalizer_version: self.normalizer_version,
        }
    }
}

impl ObservationRefPayload {
    fn from_ref(reference: &ObservationRef) -> Self {
        Self {
            chain_id: reference.chain_id.clone(),
            block_hash: reference.block_hash.clone(),
            block_number: reference.block_number,
            block_timestamp_seconds: reference.block_timestamp.unix_timestamp(),
            block_timestamp_nanoseconds: reference.block_timestamp.nanosecond(),
            transaction_hash: reference.transaction_hash.clone(),
            transaction_index: reference.transaction_index,
            log_index: reference.log_index,
            emitting_address: reference.emitting_address.clone(),
            emitting_contract_instance_id: reference.emitting_contract_instance_id.to_string(),
            canonicality_state: reference.canonicality_state.as_str().to_owned(),
            namespace: reference.namespace.clone(),
            source_manifest_id: reference.source_manifest_id,
            source_family: reference.source_family.clone(),
            manifest_version: reference.manifest_version,
        }
    }

    fn into_ref(self, chain: &str) -> Result<ObservationRef> {
        ensure!(
            self.chain_id == chain,
            "ENSv2 live checkpoint observation chain mismatch"
        );
        let block_timestamp = OffsetDateTime::from_unix_timestamp(self.block_timestamp_seconds)
            .context("invalid ENSv2 live checkpoint block timestamp")?
            .replace_nanosecond(self.block_timestamp_nanoseconds)
            .context("invalid ENSv2 live checkpoint block timestamp nanoseconds")?;
        Ok(ObservationRef {
            chain_id: self.chain_id,
            block_hash: self.block_hash,
            block_number: self.block_number,
            block_timestamp,
            transaction_hash: self.transaction_hash,
            transaction_index: self.transaction_index,
            log_index: self.log_index,
            emitting_address: self.emitting_address,
            emitting_contract_instance_id: parse_uuid(
                &self.emitting_contract_instance_id,
                "observation emitter",
            )?,
            canonicality_state: CanonicalityState::parse(&self.canonicality_state)?,
            namespace: self.namespace,
            source_manifest_id: self.source_manifest_id,
            source_family: self.source_family,
            manifest_version: self.manifest_version,
        })
    }
}

fn parse_status(value: &str) -> Result<&'static str> {
    match value {
        "registered" => Ok("registered"),
        "reserved" => Ok("reserved"),
        "unregistered" => Ok("unregistered"),
        _ => bail!("unknown ENSv2 live checkpoint registry status {value}"),
    }
}
