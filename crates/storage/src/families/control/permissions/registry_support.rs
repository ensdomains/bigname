//! Exact factory-origin and uninterrupted-code recognition from serving facts only.
use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::PgConnection;
use uuid::Uuid;

use crate::{families::name::FamilyPublication, identity::ens_v2_registry_root_resource_id};

// The reviewed deployment epochs. A manifest of any other epoch is not read, so its
// declarations prove no code. The epoch carries no address: the supported registry
// implementations are the active migration manifest's declarations under the two
// implementation roles (docs/manifests.md § Registry implementation declarations).
const REVIEWED_EPOCHS: [(&str, &str); 2] = [
    ("ethereum-sepolia", "ens_v2_sepolia_20261001"),
    ("ethereum-mainnet", "ens_v2_mainnet_20261002"),
];

// The migration family's two implementation roles. Storage does not depend on the manifest
// crate, so the names are repeated here and a test holds them equal to its constants.
const WRAPPER_ROLE: &str = "wrapper_registry_implementation";
const USER_ROLE: &str = "user_registry_implementation";

/// The ENS namespace every reviewed ENSv2 root, registry and resolver manifest declares under.
const NAMESPACE: &str = "ens";

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
            && self.reviewed_epoch(chain)
    }

    fn reviewed_epoch(&self, chain: &str) -> bool {
        let epoch = &self.after_state["manifest_payload"]["deployment_epoch"];
        REVIEWED_EPOCHS
            .iter()
            .any(|(reviewed_chain, reviewed)| *reviewed_chain == chain && epoch == reviewed)
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
    /// The ENSv2 declarations of the manifest set the publication was composed with
    /// ([`FamilyPublication::admission_manifests`]). A manifest update written after it
    /// applies from the redo that republishes.
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
               AND source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1', 'ens_v2_migration_l1',
                                     'ens_v2_resolver_l1')
               AND source_manifest_id IS NOT NULL AND consumer_visibility = 'activated'
               AND canonicality_state IN ('canonical', 'safe', 'finalized')
               AND normalized_event_id = ANY($2)
             ORDER BY source_manifest_id, normalized_event_id DESC",
        )
        .bind(&publication.chain_id)
        .bind(publication.manifest_event_ids()?)
        .fetch_all(conn)
        .await
        .context("failed to read published registry declarations")?;
        Ok(Self(rows))
    }

    /// The ENSv1 mirror resolver the publication's active ENSv2 resolver manifest declares for
    /// a reviewed deployment, as a non-proxy contract started by the publication's block.
    /// `None` unless exactly one is declared.
    /// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/ENSV1Resolver.json:L2 @ ens_v2_sepolia_20261001@07e55a05)
    /// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/ENSV1Resolver.json:L2 @ ens_v2_mainnet_20261002@b1f97b91)
    pub(crate) fn mirror_resolver(&self, publication: &FamilyPublication) -> Option<String> {
        let declared: Vec<String> = self
            .0
            .iter()
            .filter(|source| {
                source.active(
                    &publication.chain_id,
                    "ens_v2_resolver_l1",
                    NAMESPACE,
                    source.manifest_version,
                )
            })
            .filter_map(|source| source.after_state["manifest_payload"]["contracts"].as_array())
            .flatten()
            .filter(|contract| contract["role"] == "ensv1_mirror_resolver")
            .filter_map(|contract| contract["address"].as_str())
            .map(str::to_ascii_lowercase)
            .collect();
        let [address] = declared.as_slice() else {
            return None;
        };
        self.0
            .iter()
            .any(|source| {
                source.active(
                    &publication.chain_id,
                    "ens_v2_resolver_l1",
                    NAMESPACE,
                    source.manifest_version,
                ) && source.declares("ensv1_mirror_resolver", address, publication.block_number)
            })
            .then(|| address.clone())
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
        // The registry is whichever address the active root or registry manifest declares in its
        // root role. `declares` matches the address, so no other registry passes as either.
        // For these walk roles the reviewed deployment epoch and the active manifest's declaration
        // prove the code, as the e2e harness relies on. A declaration under an unreviewed epoch
        // is not read.
        let matching: Vec<_> = self
            .0
            .iter()
            .filter(|source| {
                [
                    ("ens_v2_registry_l1", "registry"),
                    ("ens_v2_root_l1", "root_registry"),
                ]
                .iter()
                .any(|(family, role)| {
                    source.active(
                        &publication.chain_id,
                        family,
                        NAMESPACE,
                        source.manifest_version,
                    ) && source.declares(role, registry, publication.block_number)
                })
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

// Any canonical physical departure from the origin's implementation disqualifies, irrespective
// of migration consumer visibility. The three arms are the ranges of
// `normalized_events_registry_departure_idx` either side of the implementation, plus a missing
// one, so same-code upgrades are never visited. The block bounds are a containment test, not
// an index range, so under default estimates no index keyed by block outranks the
// implementation key.
const DEPARTURES: &str = "/* storage:families.control.permissions.registry_support_departure */
SELECT EXISTS (
    SELECT 1 FROM bigname_phase.normalized_events event
    WHERE event.chain_id = $1 AND lower(event.after_state ->> 'proxy_address') = $2
      AND event.source_family = 'ens_v2_registry_l1' AND event.event_kind = 'Upgraded'
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lower(event.after_state ->> 'implementation') < $5
      AND int8range($3, $4, '[]') @> event.block_number
) OR EXISTS (
    SELECT 1 FROM bigname_phase.normalized_events event
    WHERE event.chain_id = $1 AND lower(event.after_state ->> 'proxy_address') = $2
      AND event.source_family = 'ens_v2_registry_l1' AND event.event_kind = 'Upgraded'
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lower(event.after_state ->> 'implementation') > $5
      AND int8range($3, $4, '[]') @> event.block_number
) OR EXISTS (
    SELECT 1 FROM bigname_phase.normalized_events event
    WHERE event.chain_id = $1 AND lower(event.after_state ->> 'proxy_address') = $2
      AND event.source_family = 'ens_v2_registry_l1' AND event.event_kind = 'Upgraded'
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lower(event.after_state ->> 'implementation') IS NULL
      AND int8range($3, $4, '[]') @> event.block_number
)";

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
    let Some(source) = declarations.source(origin.source_manifest_id) else {
        return Ok(None);
    };
    let (Some(implementation), Some(factory)) = (&origin.implementation, &origin.emitter) else {
        return Ok(None);
    };
    let declares = |role| source.declares(role, implementation, origin.block_number);
    // An address declared under both roles names no single code model.
    let model = match (declares(WRAPPER_ROLE), declares(USER_ROLE)) {
        (true, false) => Model::Wrapper,
        (false, true) => Model::User,
        _ => return Ok(None),
    };
    if !source.active(
        &publication.chain_id,
        "ens_v2_migration_l1",
        &origin.namespace,
        origin.manifest_version,
    ) || !source.declares("verifiable_factory", factory, origin.block_number)
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
    let departed: bool = sqlx::query_scalar(DEPARTURES)
        .bind(&publication.chain_id)
        .bind(registry)
        .bind(origin.block_number)
        .bind(publication.block_number)
        .bind(implementation)
        .fetch_one(conn)
        .await
        .context("failed to check the registry's implementation history")?;
    Ok((!departed).then(|| SupportedRegistry {
        root: ens_v2_registry_root_resource_id(&publication.chain_id, instance),
        namespace: origin.namespace.clone(),
        model,
    }))
}

#[cfg(test)]
#[path = "registry_support_tests.rs"]
mod tests;
