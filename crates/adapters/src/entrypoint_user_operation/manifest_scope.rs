use anyhow::{Context, Result};
use bigname_storage::sql_row;
use sqlx::PgPool;

use crate::adapter_manifest::{
    ActiveManifestEventTopic0sBySignature, load_required_active_manifest_event_topic0s_by_signature,
};
use crate::ens_v2_common::normalize_address;

use super::{
    ABI_EVENT_ANSWER_UPDATED_SIGNATURE, ABI_EVENT_USER_OPERATION_EVENT_SIGNATURE,
    CONTRACT_ROLE_ENTRYPOINT, CONTRACT_ROLE_ETH_USD_FEED, CONTRACT_ROLE_SPONSORING_PAYMASTER,
    SOURCE_FAMILY_ENS_GAS_SPONSORSHIP_L1,
};

/// The active gas-sponsorship manifest resolved to its role-tagged addresses
/// and derived event topics. `None` while the family has no active manifest on
/// the chain (draft/shadow rollout keeps the adapter inert).
#[derive(Clone, Debug)]
pub(super) struct GasSponsorshipManifestScope {
    pub(super) source_manifest_id: i64,
    pub(super) namespace: String,
    pub(super) manifest_version: i64,
    pub(super) entrypoint_addresses: Vec<String>,
    pub(super) sponsoring_paymaster_addresses: Vec<String>,
    pub(super) eth_usd_feed_addresses: Vec<String>,
    pub(super) event_topics: ActiveManifestEventTopic0sBySignature,
}

pub(super) async fn load_gas_sponsorship_manifest_scope(
    pool: &PgPool,
    chain: &str,
) -> Result<Option<GasSponsorshipManifestScope>> {
    let rows = sqlx::query(
        r#"
        SELECT
            versions.manifest_id,
            versions.namespace,
            versions.manifest_version,
            contracts.role,
            contracts.declared_address
        FROM manifest_versions AS versions
        JOIN manifest_contract_instances AS contracts
          ON contracts.manifest_id = versions.manifest_id
        WHERE versions.rollout_status = 'active'
          AND versions.chain = $1
          AND versions.source_family = $2
          AND contracts.declaration_kind = 'contract'
        ORDER BY versions.manifest_version DESC, versions.manifest_id DESC
        "#,
    )
    .bind(chain)
    .bind(SOURCE_FAMILY_ENS_GAS_SPONSORSHIP_L1)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load gas-sponsorship manifest contract roles for chain {chain}")
    })?;
    if rows.is_empty() {
        return Ok(None);
    }

    let mut selected_manifest: Option<(i64, String, i64)> = None;
    let mut entrypoint_addresses = Vec::new();
    let mut sponsoring_paymaster_addresses = Vec::new();
    let mut eth_usd_feed_addresses = Vec::new();
    for row in rows {
        let manifest_id: i64 = sql_row::get(&row, "manifest_id")?;
        let namespace: String = sql_row::get(&row, "namespace")?;
        let manifest_version: i64 = sql_row::get(&row, "manifest_version")?;
        match &selected_manifest {
            None => selected_manifest = Some((manifest_id, namespace, manifest_version)),
            // Rows are ordered latest-manifest-first; keep only that one.
            Some((selected_id, _, _)) if *selected_id != manifest_id => continue,
            Some(_) => {}
        }

        let role: String = sql_row::get(&row, "role")?;
        let address = normalize_address(&sql_row::get::<String>(&row, "declared_address")?);
        match role.as_str() {
            CONTRACT_ROLE_ENTRYPOINT => entrypoint_addresses.push(address),
            CONTRACT_ROLE_SPONSORING_PAYMASTER => sponsoring_paymaster_addresses.push(address),
            CONTRACT_ROLE_ETH_USD_FEED => eth_usd_feed_addresses.push(address),
            _ => {}
        }
    }
    let (source_manifest_id, namespace, manifest_version) =
        selected_manifest.expect("nonempty rows selected a manifest");

    let event_topics = load_required_active_manifest_event_topic0s_by_signature(
        pool,
        &[source_manifest_id],
        &[
            ABI_EVENT_USER_OPERATION_EVENT_SIGNATURE,
            ABI_EVENT_ANSWER_UPDATED_SIGNATURE,
        ],
        "gas sponsorship",
    )
    .await?;

    Ok(Some(GasSponsorshipManifestScope {
        source_manifest_id,
        namespace,
        manifest_version,
        entrypoint_addresses,
        sponsoring_paymaster_addresses,
        eth_usd_feed_addresses,
        event_topics,
    }))
}
