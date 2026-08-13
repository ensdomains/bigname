use alloy_primitives::{Address, B256, LogData, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use anyhow::Result;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

pub(super) const CHAIN: &str = "ethereum-mainnet";
pub(super) const HEAD: i64 = 16;
pub(super) const RESOLVER: &str = "0x0000000000000000000000000000000000000045";

const REGISTRAR: &str = "0x0000000000000000000000000000000000000042";
const REGISTRAR_ROLE: &str = "legacy_registrar_controller";
const SENDER: &str = "0x0000000000000000000000000000000000000043";
const REGISTRY: &str = "0x0000000000000000000000000000000000000044";
const NORMALIZER: &str = "ensip15@ens-normalize-0.1.1";
const REGISTRATION_EVENT_FRAGMENT: &str = "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 cost, uint256 expires)";

sol! {
    event NameRegistered(
        string name,
        bytes32 indexed label,
        address indexed owner,
        uint256 cost,
        uint256 expires
    );
    event NewResolver(bytes32 indexed node, address resolver);
    event TextChanged(
        bytes32 indexed node,
        string indexed indexedKey,
        string key,
        string value
    );
}

pub(super) async fn seed(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number, block_timestamp, canonicality_state)
         SELECT $1, $1 || '-block-' || number,
                CASE WHEN number = 0 THEN NULL ELSE $1 || '-block-' || (number - 1) END,
                number, to_timestamp(1700000000 + number), 'canonical'::canonicality_state
         FROM generate_series(0, $2::bigint) AS number",
    )
    .bind(CHAIN)
    .bind(HEAD)
    .execute(pool)
    .await?;

    insert_manifest(
        pool,
        "ens_v1_registrar_l1",
        REGISTRAR_ROLE,
        REGISTRAR,
        "NameRegistered",
        REGISTRATION_EVENT_FRAGMENT,
        &[REGISTRAR_ROLE],
        &["RegistrationGranted"],
    )
    .await?;
    insert_manifest(
        pool,
        "ens_v1_registry_l1",
        "registry",
        REGISTRY,
        "NewResolver",
        "event NewResolver(bytes32 indexed node, address resolver)",
        &["registry"],
        &["ResolverChanged", "PermissionChanged"],
    )
    .await?;
    insert_manifest(
        pool,
        "ens_v1_resolver_l1",
        "public_resolver",
        RESOLVER,
        "TextChanged",
        "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
        &[],
        &["RecordChanged"],
    )
    .await?;

    for (offset, (plain_label, bound_label)) in label_pairs().into_iter().enumerate() {
        insert_block_events(pool, offset as i64 + 1, &plain_label, &bound_label).await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_manifest(
    pool: &PgPool,
    source_family: &str,
    role: &str,
    address: &str,
    event_name: &str,
    event_fragment: &str,
    emitter_roles: &[&str],
    normalized_events: &[&str],
) -> Result<()> {
    let payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": source_family,
        "chain": CHAIN,
        "deployment_epoch": "benchmark-smoke",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [{
            "role": role,
            "address": address,
            "proxy_kind": "none",
            "implementation": null,
            "start_block": 0
        }],
        "discovery_rules": [],
        "abi": {"events": [{
            "name": event_name,
            "fragment": event_fragment,
            "emitter_roles": emitter_roles,
            "normalized_events": normalized_events
        }], "calls": []}
    });
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (
             manifest_version, namespace, source_family, chain_id, deployment_label,
             rollout_status, normalizer_version, file_path, manifest_payload
         ) VALUES (1, 'ens', $1, $2, 'benchmark-smoke', 'active', $3, $4, $5)
         RETURNING manifest_id",
    )
    .bind(source_family)
    .bind(CHAIN)
    .bind(NORMALIZER)
    .bind(format!("benchmarks/smoke-{source_family}.toml"))
    .bind(&payload)
    .fetch_one(pool)
    .await?;
    let instance_id = Uuid::new_v4();
    sqlx::query("INSERT INTO contract_instances VALUES ($1, $2, 'contract', '{}'::jsonb, now())")
        .bind(instance_id)
        .bind(CHAIN)
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances (
             manifest_id, chain_id, declaration_kind, declaration_name,
             contract_instance_id, declared_address, role, proxy_kind, start_block_number
         ) VALUES ($1, $2, 'contract', $3, $4, $5, $3, 'none', 0)",
    )
    .bind(manifest_id)
    .bind(CHAIN)
    .bind(role)
    .bind(instance_id)
    .bind(address)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (
             contract_instance_id, chain_id, address, active_from_block_number,
             source_manifest_id, provenance
         ) VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)",
    )
    .bind(instance_id)
    .bind(CHAIN)
    .bind(address)
    .bind(manifest_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, event_kind, source_family, manifest_version,
             source_manifest_id, chain_id, raw_fact_ref, derivation_kind,
             canonicality_state, before_state, after_state
         ) VALUES ($1, 'ens', 'SourceManifestUpdated', $2, 1, $3, $4, $5,
                   'manifest_sync', 'finalized', '{}'::jsonb, $6)",
    )
    .bind(format!(
        "benchmark-smoke:SourceManifestUpdated:{source_family}"
    ))
    .bind(source_family)
    .bind(manifest_id)
    .bind(CHAIN)
    .bind(json!({
        "manifest_id": manifest_id,
        "namespace": "ens",
        "source_family": source_family,
        "chain": CHAIN,
        "deployment_epoch": "benchmark-smoke"
    }))
    .bind(json!({
        "manifest_version": 1,
        "normalizer_version": NORMALIZER,
        "rollout_status": "active",
        "manifest_payload": payload
    }))
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_block_events(
    pool: &PgPool,
    block: i64,
    plain_label: &str,
    bound_label: &str,
) -> Result<()> {
    let transaction_hash = format!("{CHAIN}-transaction-{block}");
    sqlx::query(
        "INSERT INTO raw_transactions (
             chain_id, block_hash, block_number, transaction_hash, transaction_index,
             from_address, to_address
         ) VALUES ($1, $2, $3, $4, 0, $5, $6)",
    )
    .bind(CHAIN)
    .bind(block_hash(block))
    .bind(block)
    .bind(&transaction_hash)
    .bind(SENDER)
    .bind(REGISTRAR)
    .execute(pool)
    .await?;

    let mut owner_bytes = [0u8; 20];
    owner_bytes[12..].copy_from_slice(&(block as u64).to_be_bytes());
    insert_registration(pool, block, 0, plain_label, owner_bytes).await?;
    insert_registration(pool, block, 1, bound_label, owner_bytes).await?;

    let node = namehash(bound_label);
    let resolver = NewResolver {
        node,
        resolver: RESOLVER.parse::<Address>()?,
    }
    .encode_log_data();
    insert_log(pool, block, 2, REGISTRY, &resolver).await?;

    let record = TextChanged {
        node,
        indexedKey: keccak256(b"url"),
        key: "url".to_owned(),
        value: format!("https://{bound_label}.example"),
    }
    .encode_log_data();
    insert_log(pool, block, 3, RESOLVER, &record).await?;
    Ok(())
}

async fn insert_registration(
    pool: &PgPool,
    block: i64,
    log_index: i64,
    label: &str,
    owner_bytes: [u8; 20],
) -> Result<()> {
    let registration = NameRegistered {
        name: label.to_owned(),
        label: B256::from(keccak256(label.as_bytes())),
        owner: Address::from(owner_bytes),
        cost: U256::from(1_000_000_000_000_000u64),
        expires: U256::from(2_000_000_000u64 + block as u64),
    }
    .encode_log_data();
    insert_log(pool, block, log_index, REGISTRAR, &registration).await
}

async fn insert_log(
    pool: &PgPool,
    block: i64,
    log_index: i64,
    emitter: &str,
    event: &LogData,
) -> Result<()> {
    let topics = event
        .topics()
        .iter()
        .map(|topic| format!("{topic:#x}"))
        .collect::<Vec<_>>();
    sqlx::query(
        "INSERT INTO raw_logs (
             chain_id, block_hash, block_number, transaction_hash, transaction_index,
             log_index, emitting_address, topics, data
         ) VALUES ($1, $2, $3, $4, 0, $5, $6, $7, $8)",
    )
    .bind(CHAIN)
    .bind(block_hash(block))
    .bind(block)
    .bind(format!("{CHAIN}-transaction-{block}"))
    .bind(log_index)
    .bind(emitter)
    .bind(topics)
    .bind(event.data.to_vec())
    .execute(pool)
    .await?;
    Ok(())
}

fn namehash(label: &str) -> B256 {
    let mut input = [0u8; 64];
    input[32..].copy_from_slice(keccak256(b"eth").as_slice());
    let eth_node = keccak256(input);
    input[..32].copy_from_slice(eth_node.as_slice());
    input[32..].copy_from_slice(keccak256(label.as_bytes()).as_slice());
    keccak256(input)
}

fn label_pairs() -> Vec<(String, String)> {
    let mut candidates = (0..256)
        .map(|number| {
            let label = format!("bench{number:04}");
            (namehash(&label), label)
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(|(node, _)| *node);
    candidates
        .iter()
        .take(HEAD as usize)
        .zip(candidates.iter().rev().take(HEAD as usize))
        .map(|((_, plain), (_, bound))| (plain.clone(), bound.clone()))
        .collect()
}

pub(super) async fn seed_serving_state(pool: &PgPool) -> Result<()> {
    for table in ["name_current", "record_inventory_current"] {
        sqlx::query(&format!(
            "UPDATE {table}
             SET chain_positions = jsonb_set(
                 chain_positions,
                 '{{ethereum,timestamp}}',
                 to_jsonb(to_char(
                     (chain_positions #>> '{{ethereum,timestamp}}')::timestamptz AT TIME ZONE 'UTC',
                     'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'
                 ))
             )
             WHERE chain_positions #>> '{{ethereum,timestamp}}' IS NOT NULL"
        ))
        .execute(pool)
        .await?;
    }
    sqlx::query(
        "INSERT INTO chain_heads (chain_id, latest_block_hash, latest_block_number)
         VALUES ($1, $2, $3)",
    )
    .bind(CHAIN)
    .bind(block_hash(HEAD))
    .bind(HEAD)
    .execute(pool)
    .await?;
    for phase in ["ingest", "interpret", "project"] {
        let input_hash =
            (phase != "ingest").then_some(bigname_content_hash::INTERPRETER_CONTENT_HASH);
        sqlx::query(
            "INSERT INTO chain_phase_state (
                 chain_id, phase_name, phase_status, current_block_number, current_block_hash,
                 target_block_number, target_block_hash, input_content_hash, started_at, finished_at
             ) VALUES ($1, $2, 'completed', $3, $4, $3, $4, $5, now(), now())",
        )
        .bind(CHAIN)
        .bind(phase)
        .bind(HEAD)
        .bind(block_hash(HEAD))
        .bind(input_hash)
        .execute(pool)
        .await?;
    }
    Ok(())
}

pub(super) fn block_hash(number: i64) -> String {
    format!("{CHAIN}-block-{number}")
}

#[cfg(test)]
mod tests {
    use super::{REGISTRAR_ROLE, REGISTRATION_EVENT_FRAGMENT};

    #[test]
    fn production_manifest_admits_smoke_registration_fragment() {
        let manifest =
            include_str!("../../../../manifests/mainnet/ethereum/ens/ens_v1_registrar_l1/v1.toml");
        let parsed: toml::Value = toml::from_str(manifest).unwrap();
        let admitted = parsed["abi"]["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| {
                event["fragment"].as_str() == Some(REGISTRATION_EVENT_FRAGMENT)
                    && event["emitter_roles"].as_array().is_some_and(|roles| {
                        roles
                            .iter()
                            .any(|role| role.as_str() == Some(REGISTRAR_ROLE))
                    })
            });
        assert!(
            admitted,
            "smoke registrar fragment and role are not admitted together by the production ENSv1 manifest"
        );
    }
}
