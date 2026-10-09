//! Exact factory-origin and uninterrupted-code recognition from serving facts only.
use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::PgConnection;
use uuid::Uuid;

use crate::{families::name::FamilyPublication, identity::ens_v2_registry_root_resource_id};

// Exact reviewed implementations; a manifest role alone cannot establish a code model.
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/WrapperRegistryImpl.json:L2 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/UserRegistryImpl.json:L2 @ ens_v2_sepolia_20261001@07e55a05)
const WRAPPER: &str = "0xbe768b63e5fbbfbb0ae97e9064e0002df8001880";
const USER: &str = "0x9bd8a88719068d09ecee662f36c0e3856708366a";
const FACTORY: &str = "0xda70306c98e97ece36f997a21368e53298572991";
const EPOCH: &str = "ens_v2_sepolia_20261001";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Model {
    Declared,
    User,
    Wrapper,
}

#[derive(Clone, Debug)]
pub(crate) struct SupportedRegistry {
    pub root: Uuid,
    pub namespace: String,
    pub model: Model,
}

#[derive(sqlx::FromRow)]
struct Declaration {
    source_manifest_id: i64,
    manifest_version: i64,
    namespace: String,
    source_family: String,
    after_state: Value,
}

pub(crate) struct Declarations(Vec<Declaration>);

impl Declaration {
    fn active(&self, chain: &str, family: &str, namespace: &str, version: i64) -> bool {
        let payload = &self.after_state["manifest_payload"];
        self.source_family == family
            && self.namespace == namespace
            && self.manifest_version == version
            && self.after_state["rollout_status"] == "active"
            && payload["manifest_version"].as_i64() == Some(version)
            && payload["rollout_status"] == "active"
            && payload["chain"] == chain
            && payload["namespace"] == namespace
            && payload["source_family"] == family
            && payload["deployment_epoch"] == EPOCH
    }

    fn declares(&self, role: &str, address: &str, block: i64) -> bool {
        let Some(contracts) = self.after_state["manifest_payload"]["contracts"].as_array() else {
            return false;
        };
        let matches: Vec<_> = contracts
            .iter()
            .filter(|contract| {
                contract["role"] == role
                    && contract["address"]
                        .as_str()
                        .is_some_and(|value| value.eq_ignore_ascii_case(address))
            })
            .collect();
        let [contract] = matches.as_slice() else {
            return false;
        };
        contract["proxy_kind"] == "none"
            && contract["start_block"]
                .as_i64()
                .is_some_and(|start| start <= block)
    }
}

impl Declarations {
    pub(crate) async fn load(
        conn: &mut PgConnection,
        publication: &FamilyPublication,
    ) -> Result<Self> {
        let rows = sqlx::query_as(
            "/* storage:families.control.permissions.registry_support_declarations */
             SELECT DISTINCT ON (source_manifest_id) source_manifest_id, manifest_version,
                    namespace, source_family, after_state
             FROM bigname_phase.normalized_events
             WHERE chain_id = $1 AND event_kind = 'SourceManifestUpdated'
               AND source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1', 'ens_v2_migration_l1')
               AND source_manifest_id IS NOT NULL AND consumer_visibility = 'activated'
               AND canonicality_state IN ('canonical', 'safe', 'finalized')
               AND (block_number IS NULL OR block_number <= $2)
             ORDER BY source_manifest_id, normalized_event_id DESC",
        )
        .bind(&publication.chain_id)
        .bind(publication.block_number)
        .fetch_all(conn)
        .await
        .context("failed to read published registry declarations")?;
        Ok(Self(rows))
    }

    fn source(&self, id: Option<i64>) -> Option<&Declaration> {
        self.0
            .iter()
            .find(|source| Some(source.source_manifest_id) == id)
    }

    pub(crate) fn declared(
        &self,
        publication: &FamilyPublication,
        registry: &str,
        instance: Uuid,
    ) -> Option<SupportedRegistry> {
        // RootRegistry does not emit a watched RegistryCreated. Its actual instance comes
        // from the current Project entry fact; the retained manifest proves the exact code.
        // (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/RootRegistry.json:L2 @ ens_v2_sepolia_20261001@07e55a05)
        // (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/ETHRegistry.json:L2 @ ens_v2_sepolia_20261001@07e55a05)
        // The root is whichever address the active root manifest declares. `declares` below
        // matches the address, so no other registry passes as the root.
        let (family, role) = match registry {
            "0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4" => ("ens_v2_registry_l1", "registry"),
            _ => ("ens_v2_root_l1", "root_registry"),
        };
        let matching: Vec<_> = self
            .0
            .iter()
            .filter(|source| {
                source.active(
                    &publication.chain_id,
                    family,
                    &source.namespace,
                    source.manifest_version,
                ) && source.declares(role, registry, publication.block_number)
            })
            .collect();
        let [source] = matching.as_slice() else {
            return None;
        };
        Some(SupportedRegistry {
            root: ens_v2_registry_root_resource_id(&publication.chain_id, instance),
            namespace: source.namespace.clone(),
            model: Model::Declared,
        })
    }
}

#[derive(sqlx::FromRow)]
struct Origin {
    namespace: String,
    source_manifest_id: Option<i64>,
    manifest_version: i64,
    block_number: i64,
    implementation: Option<String>,
    emitter: Option<String>,
}

// At most two physical origins: ambiguous history is not proof. The initial Upgraded and
// RegistryCreated can precede the factory log in its transaction, or initialization can be later.
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/build-info/solc-0_8_25-b30e6dc9a03b37f6a0b89af5d02a73d3993944f7.json:L1561 @ ens_v2_sepolia_20261001@07e55a05)
const ORIGINS: &str = "/* storage:families.control.permissions.registry_support_origins */
SELECT namespace, source_manifest_id, manifest_version, block_number,
       lower(after_state ->> 'implementation') AS implementation,
       lower(raw_fact_ref ->> 'emitting_address') AS emitter
FROM bigname_phase.normalized_events
WHERE chain_id = $1 AND lower(after_state ->> 'proxy_address') = $2
  AND source_family = 'ens_v2_migration_l1' AND event_kind = 'ContractDiscovered'
  AND consumer_visibility = 'activated' AND after_state ->> 'source_event' = 'ContractDiscovered'
  AND canonicality_state IN ('canonical', 'safe', 'finalized') AND block_number <= $3
ORDER BY block_number, transaction_index, log_index, event_identity COLLATE \"C\"
LIMIT 2";

#[derive(sqlx::FromRow)]
struct Announcement {
    namespace: String,
    source_manifest_id: Option<i64>,
    manifest_version: i64,
    after_state: Value,
}

const ANNOUNCEMENTS: &str =
    "/* storage:families.control.permissions.registry_support_announcements */
SELECT namespace, source_manifest_id, manifest_version, after_state
FROM bigname_phase.normalized_events
WHERE lower(raw_fact_ref ->> 'emitting_address') = $2 AND chain_id = $1
  AND source_family = 'ens_v2_registry_l1' AND event_kind = 'RegistryCreated'
  AND consumer_visibility = 'activated' AND canonicality_state IN ('canonical', 'safe', 'finalized')
  AND block_number BETWEEN $3 AND $4
ORDER BY block_number, log_index, normalized_event_id
LIMIT 2";

// Literal predicates match the sparse indexes even for a generic plan. Any canonical
// physical departure disqualifies, irrespective of migration consumer visibility.
const DEPARTURES: &str = "/* storage:families.control.permissions.registry_support_departure */
SELECT EXISTS (
    SELECT 1 FROM bigname_phase.normalized_events event
    WHERE event.chain_id = $1 AND lower(event.after_state ->> 'proxy_address') = $2
      AND event.source_family = 'ens_v2_registry_l1' AND event.event_kind = 'Upgraded'
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND event.block_number BETWEEN $3 AND $4
      AND lower(event.after_state ->> 'implementation') IS DISTINCT FROM ";

pub(crate) async fn load(
    conn: &mut PgConnection,
    publication: &FamilyPublication,
    declarations: &Declarations,
    registry: &str,
    parent_instance: Option<Uuid>,
) -> Result<Option<SupportedRegistry>> {
    if let Some(declared) =
        parent_instance.and_then(|instance| declarations.declared(publication, registry, instance))
    {
        return Ok(Some(declared));
    }
    let origins: Vec<Origin> = sqlx::query_as(ORIGINS)
        .bind(&publication.chain_id)
        .bind(registry)
        .bind(publication.block_number)
        .fetch_all(&mut *conn)
        .await
        .context("failed to read the registry's factory origin")?;
    let [origin] = origins.as_slice() else {
        return Ok(None);
    };
    let (model, implementation, role) = match origin.implementation.as_deref() {
        Some(WRAPPER) => (Model::Wrapper, WRAPPER, "wrapper_registry_implementation"),
        Some(USER) => (Model::User, USER, "user_registry_implementation"),
        _ => return Ok(None),
    };
    let Some(source) = declarations.source(origin.source_manifest_id) else {
        return Ok(None);
    };
    if origin.emitter.as_deref() != Some(FACTORY)
        || !source.active(
            &publication.chain_id,
            "ens_v2_migration_l1",
            &origin.namespace,
            origin.manifest_version,
        )
        || !source.declares("verifiable_factory", FACTORY, origin.block_number)
        || !source.declares(role, implementation, origin.block_number)
    {
        return Ok(None);
    }
    let announcements: Vec<Announcement> = sqlx::query_as(ANNOUNCEMENTS)
        .bind(&publication.chain_id)
        .bind(registry)
        .bind(origin.block_number)
        .bind(publication.block_number)
        .fetch_all(&mut *conn)
        .await
        .context("failed to read the registry's ordinary announcement")?;
    let [announcement] = announcements.as_slice() else {
        return Ok(None);
    };
    let Some(source) = declarations.source(announcement.source_manifest_id) else {
        return Ok(None);
    };
    if announcement.namespace != origin.namespace
        || !source.active(
            &publication.chain_id,
            "ens_v2_registry_l1",
            &origin.namespace,
            announcement.manifest_version,
        )
        || announcement.after_state["source_event"] != "RegistryCreated"
        || announcement.after_state["registry"].as_str() != Some(registry)
    {
        return Ok(None);
    }
    let Some(instance) = announcement.after_state["contract_instance_id"]
        .as_str()
        .and_then(|id| id.parse::<Uuid>().ok())
    else {
        return Ok(None);
    };
    if parent_instance.is_some_and(|expected| expected != instance) {
        return Ok(None);
    }
    let departed: bool = sqlx::query_scalar(&format!("{DEPARTURES}'{implementation}')"))
        .bind(&publication.chain_id)
        .bind(registry)
        .bind(origin.block_number)
        .bind(publication.block_number)
        .fetch_one(conn)
        .await
        .context("failed to check the registry's implementation history")?;
    Ok((!departed).then(|| SupportedRegistry {
        root: ens_v2_registry_root_resource_id(&publication.chain_id, instance),
        namespace: origin.namespace.clone(),
        model,
    }))
}
