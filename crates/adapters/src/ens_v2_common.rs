use bigname_storage::sql_row;
use std::collections::HashMap;

use anyhow::{Context, Result};
use bigname_manifests::load_watched_contracts;
use sqlx::{PgPool, Row, types::Uuid};

use crate::adapter_manifest::{
    ActiveManifestMetadata, active_manifest_for_watched_contract,
    ensure_watched_contract_manifest_chain, load_active_manifest_metadata,
    load_latest_active_manifest_metadata_for_source_family, watched_contract_manifest_ids,
};
pub(crate) use crate::evm_abi::{keccak256_bytes, keccak256_hex};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ActiveEmitter {
    pub(crate) address: String,
    pub(crate) contract_instance_id: Uuid,
    pub(crate) source_manifest_id: i64,
    pub(crate) namespace: String,
    pub(crate) source_family: String,
    pub(crate) manifest_version: i64,
    pub(crate) active_from_block_number: Option<i64>,
    pub(crate) active_to_block_number: Option<i64>,
}

pub(crate) fn source_scope_bindings(
    source_scope: Option<&[(String, String, i64, i64)]>,
    source_family_filter: &str,
) -> (Vec<String>, Vec<i64>, Vec<i64>) {
    let mut addresses = Vec::new();
    let mut from_blocks = Vec::new();
    let mut to_blocks = Vec::new();
    for (source_family, address, from_block, to_block) in source_scope.unwrap_or(&[]) {
        if source_family != source_family_filter {
            continue;
        }
        addresses.push(address.to_ascii_lowercase());
        from_blocks.push(*from_block);
        to_blocks.push(*to_block);
    }
    (addresses, from_blocks, to_blocks)
}

/// Collapse multiple watched/discovery rows for one emitting address into a single envelope
/// interval. A discovered resolver carries one discovery edge per (name, registry) that referenced
/// it, each with its own activation block — the emitter is in scope from the EARLIEST activation
/// to the latest deactivation (open-ended when any edge is open). Keeping a single arbitrary
/// edge's interval instead silently dropped every raw log emitted before that edge's activation
/// block, which is how record events written around a name's registration went un-normalized.
fn merge_active_emitter(
    emitters_by_address: &mut HashMap<String, ActiveEmitter>,
    emitter: ActiveEmitter,
) {
    use std::collections::hash_map::Entry;
    match emitters_by_address.entry(emitter.address.clone()) {
        Entry::Vacant(entry) => {
            entry.insert(emitter);
        }
        Entry::Occupied(mut entry) => {
            let existing = entry.get_mut();
            existing.active_from_block_number = match (
                existing.active_from_block_number,
                emitter.active_from_block_number,
            ) {
                (Some(existing_from), Some(merged_from)) => Some(existing_from.min(merged_from)),
                _ => None,
            };
            existing.active_to_block_number = match (
                existing.active_to_block_number,
                emitter.active_to_block_number,
            ) {
                (Some(existing_to), Some(merged_to)) => Some(existing_to.max(merged_to)),
                _ => None,
            };
        }
    }
}

pub(crate) fn emitters_by_address(
    emitters: &[ActiveEmitter],
) -> HashMap<String, Vec<ActiveEmitter>> {
    let mut output = HashMap::<String, Vec<ActiveEmitter>>::new();
    for emitter in emitters.iter().cloned() {
        output
            .entry(emitter.address.clone())
            .or_default()
            .push(emitter);
    }
    output
}

pub(crate) fn active_emitter_for_block(
    emitters: &[ActiveEmitter],
    block_number: i64,
) -> Option<&ActiveEmitter> {
    emitters.iter().find(|emitter| {
        emitter
            .active_from_block_number
            .is_none_or(|active_from| block_number >= active_from)
            && emitter
                .active_to_block_number
                .is_none_or(|active_to| block_number < active_to)
    })
}

pub(crate) async fn load_active_emitters(
    pool: &PgPool,
    chain: &str,
    source_family: &str,
    resolver_edge_kind: &str,
    adapter_label: &str,
) -> Result<Vec<ActiveEmitter>> {
    let watched_contracts = load_watched_contracts(pool)
        .await
        .with_context(|| format!("failed to load watched contracts for {adapter_label} adapter"))?;
    let watched_contracts = watched_contracts
        .into_iter()
        .filter(|contract| contract.chain == chain)
        .collect::<Vec<_>>();
    if watched_contracts.is_empty() {
        return Ok(Vec::new());
    }

    let manifest_ids = watched_contract_manifest_ids(&watched_contracts)?;
    let context_label = format!("{adapter_label} emitters");
    let active_manifests =
        load_active_manifest_metadata(pool, &manifest_ids, &context_label).await?;

    let mut emitters_by_address = HashMap::<String, ActiveEmitter>::new();
    for watched_contract in watched_contracts {
        let (source_manifest_id, manifest) =
            active_manifest_for_watched_contract(&active_manifests, &watched_contract)?;
        if manifest.source_family != source_family {
            continue;
        }
        ensure_watched_contract_manifest_chain(&watched_contract, manifest, source_manifest_id)?;

        merge_active_emitter(
            &mut emitters_by_address,
            ActiveEmitter {
                address: watched_contract.address,
                contract_instance_id: watched_contract.contract_instance_id,
                source_manifest_id,
                namespace: manifest.namespace.clone(),
                source_family: manifest.source_family.clone(),
                manifest_version: manifest.manifest_version,
                active_from_block_number: watched_contract.active_from_block_number,
                active_to_block_number: watched_contract.active_to_block_number,
            },
        );
    }
    if let Some(manifest) =
        load_active_source_family_manifest_metadata(pool, chain, source_family).await?
    {
        for emitter in
            load_discovered_resolver_emitters(pool, chain, resolver_edge_kind, &manifest).await?
        {
            merge_active_emitter(&mut emitters_by_address, emitter);
        }
    }

    let mut emitters = emitters_by_address.into_values().collect::<Vec<_>>();
    emitters.sort_by(|left, right| {
        left.address
            .cmp(&right.address)
            .then(left.source_manifest_id.cmp(&right.source_manifest_id))
            .then(left.contract_instance_id.cmp(&right.contract_instance_id))
    });
    Ok(emitters)
}

async fn load_discovered_resolver_emitters(
    pool: &PgPool,
    chain: &str,
    resolver_edge_kind: &str,
    manifest: &ActiveManifestMetadata,
) -> Result<Vec<ActiveEmitter>> {
    let rows = sqlx::query(
        r#"
        SELECT
            cia.address,
            de.to_contract_instance_id,
            de.active_from_block_number,
            de.active_to_block_number
        FROM discovery_edges de
        JOIN manifest_versions source_mv
          ON source_mv.manifest_id = de.source_manifest_id
         AND source_mv.rollout_status = 'active'
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = de.to_contract_instance_id
        WHERE de.chain_id = $1
          AND de.edge_kind = $2
        ORDER BY lower(cia.address), de.active_from_block_number NULLS FIRST, de.discovery_edge_id
        "#,
    )
    .bind(chain)
    .bind(resolver_edge_kind)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load ENSv2 discovered resolver emitters for {chain}"))?;

    rows.into_iter()
        .map(|row| {
            let address = normalize_address(
                &row.try_get::<String, _>("address")
                    .context("missing discovered resolver address")?,
            );
            Ok(ActiveEmitter {
                address,
                contract_instance_id: row
                    .try_get("to_contract_instance_id")
                    .context("missing discovered resolver contract_instance_id")?,
                source_manifest_id: manifest.manifest_id,
                namespace: manifest.namespace.clone(),
                source_family: manifest.source_family.clone(),
                manifest_version: manifest.manifest_version,
                active_from_block_number: sql_row::get(&row, "active_from_block_number")?,
                active_to_block_number: sql_row::get(&row, "active_to_block_number")?,
            })
        })
        .collect()
}

async fn load_active_source_family_manifest_metadata(
    pool: &PgPool,
    chain: &str,
    source_family: &str,
) -> Result<Option<ActiveManifestMetadata>> {
    load_latest_active_manifest_metadata_for_source_family(
        pool,
        chain,
        source_family,
        "active ENSv2 resolver manifest",
    )
    .await
}

pub(crate) fn normalize_address(value: &str) -> String {
    value.to_ascii_lowercase()
}

pub(crate) fn dns_decode_optional(bytes: &[u8]) -> Result<Option<String>> {
    if bytes.is_empty() {
        Ok(None)
    } else {
        dns_decode(bytes).map(Some)
    }
}

pub(crate) fn dns_decode(bytes: &[u8]) -> Result<String> {
    bigname_domain::normalization::normalize_dns_encoded_name(bytes)
        .map(|name| name.normalized_name)
        .map_err(anyhow::Error::from)
}

pub(crate) fn hex_string(bytes: impl AsRef<[u8]>) -> String {
    crate::evm_abi::hex_string_without_prefix(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn emitter(active_from: Option<i64>, active_to: Option<i64>) -> ActiveEmitter {
        ActiveEmitter {
            address: "0xresolver".to_owned(),
            contract_instance_id: Uuid::from_u128(1),
            source_manifest_id: 1,
            namespace: "ens".to_owned(),
            source_family: "ens_v2_resolver_l1".to_owned(),
            manifest_version: 1,
            active_from_block_number: active_from,
            active_to_block_number: active_to,
        }
    }

    #[test]
    fn merge_active_emitter_unions_edge_intervals_per_address() {
        let mut by_address = HashMap::new();

        // The discovery order is arbitrary — a later-discovered edge must not shadow earlier
        // coverage (the bug this guards against: only the kept edge's interval was honoured, so
        // logs before its activation block were silently dropped).
        merge_active_emitter(&mut by_address, emitter(Some(10_710_418), Some(10_800_000)));
        merge_active_emitter(&mut by_address, emitter(Some(10_696_215), Some(10_750_000)));
        let merged = by_address.get("0xresolver").expect("merged emitter");
        assert_eq!(merged.active_from_block_number, Some(10_696_215));
        assert_eq!(merged.active_to_block_number, Some(10_800_000));

        // An open-ended edge (no activation / no deactivation bound) opens that side of the
        // envelope for the address.
        merge_active_emitter(&mut by_address, emitter(None, None));
        let merged = by_address.get("0xresolver").expect("merged emitter");
        assert_eq!(merged.active_from_block_number, None);
        assert_eq!(merged.active_to_block_number, None);
    }

    #[test]
    fn active_emitter_for_block_respects_the_merged_envelope() {
        let merged = [emitter(Some(10_696_215), None)];
        assert!(active_emitter_for_block(&merged, 10_696_215).is_some());
        assert!(active_emitter_for_block(&merged, 10_704_585).is_some());
        assert!(active_emitter_for_block(&merged, 10_696_214).is_none());
    }
}
