use anyhow::{Context, Result};
use uuid::Uuid;

use crate::LoadedManifest;
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ManifestStorageKey {
    pub(crate) namespace: String,
    pub(crate) source_family: String,
    pub(crate) chain: String,
    pub(crate) deployment_epoch: String,
    pub(crate) manifest_version: i64,
}

impl ManifestStorageKey {
    pub(crate) fn from_loaded_manifest(loaded_manifest: &LoadedManifest) -> Result<Self> {
        Ok(Self {
            namespace: loaded_manifest.manifest.namespace.clone(),
            source_family: loaded_manifest.manifest.source_family.clone(),
            chain: loaded_manifest.manifest.chain.clone(),
            deployment_epoch: loaded_manifest.manifest.deployment_epoch.clone(),
            manifest_version: i64::try_from(loaded_manifest.manifest.manifest_version)
                .context("manifest_version does not fit into BIGINT")?,
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct DeclarationKey {
    pub(crate) declaration_kind: String,
    pub(crate) declaration_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PersistedManifestEntry {
    pub(crate) key: DeclarationKey,
    pub(crate) contract_instance_id: Uuid,
    pub(crate) declared_address: String,
    pub(crate) code_hash: Option<String>,
    pub(crate) abi_ref: Option<String>,
    pub(crate) role: Option<String>,
    pub(crate) proxy_kind: Option<String>,
    pub(crate) implementation_contract_instance_id: Option<Uuid>,
    pub(crate) declared_implementation_address: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManifestTransition {
    pub(crate) source_manifest_id: i64,
    pub(crate) chain: String,
    pub(crate) declaration_kind: String,
    pub(crate) declaration_name: String,
    pub(crate) from_contract_instance_id: Uuid,
    pub(crate) from_address: String,
    pub(crate) to_contract_instance_id: Uuid,
    pub(crate) to_address: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ManagedEdgeSpec {
    pub(crate) chain: String,
    pub(crate) edge_kind: String,
    pub(crate) from_contract_instance_id: Uuid,
    pub(crate) to_contract_instance_id: Uuid,
    pub(crate) discovery_source: String,
    pub(crate) source_manifest_id: i64,
    pub(crate) admission: String,
    pub(crate) provenance_json: String,
}

#[derive(Clone, Debug)]
pub(crate) struct ExistingManagedEdge {
    pub(crate) discovery_edge_id: i64,
    pub(crate) spec: ManagedEdgeSpec,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ActiveAddressSpec {
    pub(crate) contract_instance_id: Uuid,
    pub(crate) chain: String,
    pub(crate) address: String,
    pub(crate) source_manifest_id: Option<i64>,
    pub(crate) provenance_json: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ManifestLineageKey {
    pub(crate) namespace: String,
    pub(crate) source_family: String,
    pub(crate) chain: String,
    pub(crate) deployment_epoch: String,
    pub(crate) declaration_kind: String,
    pub(crate) declaration_name: String,
}

#[derive(Clone, Debug)]
pub(crate) struct OrderedManifestEntry {
    pub(crate) manifest_id: i64,
    pub(crate) manifest_version: i64,
    pub(crate) rollout_status: String,
    pub(crate) chain: String,
    pub(crate) lineage_key: ManifestLineageKey,
    pub(crate) contract_instance_id: Uuid,
    pub(crate) declared_address: String,
}

#[derive(Clone, Debug)]
pub(crate) struct CurrentActiveAddressRow {
    pub(crate) contract_instance_id: Uuid,
    pub(crate) chain: String,
    pub(crate) address: String,
}
