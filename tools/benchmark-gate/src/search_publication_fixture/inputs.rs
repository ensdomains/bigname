use super::*;
use alloy_primitives::keccak256;
use serde_json::{Value, json};
use uuid::Uuid;

pub fn hash(chain: &str, block: i64) -> String {
    format!("gate1-{chain}-{block}")
}
pub async fn lineage(pool: &PgPool, chain: &str, block: i64, clock: i64) -> Result<()> {
    sqlx::query("INSERT INTO chain_lineage(chain_id,block_hash,block_number,block_timestamp,canonicality_state)
        VALUES($1,$2,$3,to_timestamp($4::bigint::double precision),'finalized') ON CONFLICT DO NOTHING")
        .bind(chain).bind(hash(chain,block)).bind(block).bind(clock).execute(pool).await?;
    Ok(())
}

#[derive(Clone)]
pub struct Name {
    pub id: String,
    pub node: String,
    pub resource: Uuid,
    pub namespace: String,
    pub chain: String,
}
/// Same identity-input path as the existing API fixture helper: normalize first, seed retained
/// identity and open binding facts, and let Project produce all serving state.
pub async fn name(
    pool: &PgPool,
    name: &str,
    namespace: &str,
    seed: u128,
    bound: bool,
    structural: bool,
) -> Result<Name> {
    let chain = if namespace == "basenames" { BASE } else { ETH };
    let normalized = bigname_domain::normalization::normalize_name(name)?;
    let node = bigname_lookup::ens_namehash_hex(&normalized.normalized_name)?;
    let id = format!("{namespace}:{node}");
    let labels = normalized
        .normalized_name
        .split('.')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let hashes = labels
        .iter()
        .map(|s| format!("{:#x}", keccak256(s.as_bytes())))
        .collect::<Vec<_>>();
    let resource = Uuid::from_u128(seed);
    let token = Uuid::from_u128(seed + 1);
    let block = 10;
    let block_hash = hash(chain, block);
    lineage(pool, chain, block, CLOCK - 90).await?;
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .execute(&mut *tx)
        .await?;
    bigname_storage::identity_search::prepare(
        &mut tx,
        &[],
        &[hashes.clone()],
        std::slice::from_ref(&id),
    )
    .await?;
    sqlx::query("INSERT INTO name_surfaces(logical_name_id,namespace,raw_name,raw_labels,dns_encoded_name,namehash,labelhashes,normalizer_version,visibility_state,chain_id,block_hash,block_number,canonicality_state)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,'active',$9,$10,$11,'canonical')")
        .bind(&id).bind(namespace).bind((!structural).then_some(&normalized.normalized_name))
        .bind((!structural).then_some(&labels)).bind((!structural).then_some(&normalized.dns_encoded_name))
        .bind(&node).bind(&hashes).bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
        .bind(chain).bind(&block_hash).bind(block).execute(&mut *tx).await?;
    bigname_storage::identity_search::refresh(&mut tx, std::slice::from_ref(&id), &[]).await?;
    tx.commit().await?;
    if structural {
        // Actual verified import helper, not a manufactured rendered/composed row.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS ens_names(hash text PRIMARY KEY,name text NOT NULL)",
        )
        .execute(pool)
        .await?;
        for (label, hash) in labels.iter().zip(&hashes) {
            sqlx::query("INSERT INTO ens_names VALUES($1,$2) ON CONFLICT DO NOTHING")
                .bind(hash)
                .bind(label)
                .execute(pool)
                .await?;
        }
        bigname_storage::import_label_preimages_from_ens_names_table(pool, Some(100), None).await?;
    }
    if bound {
        sqlx::query("INSERT INTO token_lineages(token_lineage_id,chain_id,block_hash,block_number,canonicality_state) VALUES($1,$2,$3,$4,'canonical')")
            .bind(token).bind(chain).bind(&block_hash).bind(block).execute(pool).await?;
        sqlx::query("INSERT INTO resources(resource_id,token_lineage_id,chain_id,block_hash,block_number,canonicality_state) VALUES($1,$2,$3,$4,$5,'canonical')")
            .bind(resource).bind(token).bind(chain).bind(&block_hash).bind(block).execute(pool).await?;
        sqlx::query("INSERT INTO surface_bindings(surface_binding_id,logical_name_id,resource_id,binding_kind,authority_arm,active_from,chain_id,block_hash,block_number,canonicality_state)
            VALUES($1,$2,$3,'declared_registry_path',$4,to_timestamp($5::bigint::double precision),$6,$7,$8,'canonical')")
            .bind(Uuid::from_u128(seed+2)).bind(&id).bind(resource).bind(if namespace=="basenames" {"basenames"}else{"ens_v1"})
            .bind(CLOCK-90).bind(chain).bind(&block_hash).bind(block).execute(pool).await?;
    }
    Ok(Name {
        id,
        node,
        resource,
        namespace: namespace.into(),
        chain: chain.into(),
    })
}

pub async fn event(
    pool: &PgPool,
    name: &Name,
    ordinal: i64,
    kind: &str,
    family: &str,
    logical: bool,
    after: Value,
) -> Result<()> {
    let block = 20;
    lineage(pool, &name.chain, block, CLOCK - 80).await?;
    sqlx::query("INSERT INTO normalized_events(event_identity,namespace,logical_name_id,resource_id,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,raw_fact_ref,derivation_kind,canonicality_state,after_state)
        VALUES($1,$2,$3,$4,$5,$6,1,$7,$8,$9,'gate1-tx',0,$10,$11,'ens_v1_unwrapped_authority','canonical',$12)")
        .bind(format!("gate1-{}-{ordinal}-{kind}",name.id)).bind(&name.namespace).bind(logical.then_some(&name.id)).bind(name.resource)
        .bind(kind).bind(family).bind(&name.chain).bind(block).bind(hash(&name.chain,block)).bind(ordinal)
        .bind(json!({"kind":"raw_log","transaction_index":0,"log_index":ordinal}))
        .bind(after).execute(pool).await?;
    Ok(())
}

pub async fn manifest(
    pool: &PgPool,
    namespace: &str,
    chain: &str,
    family: &str,
    version: i64,
    payload: Value,
) -> Result<i64> {
    let id=sqlx::query_scalar("INSERT INTO manifest_versions(manifest_version,namespace,source_family,chain_id,deployment_label,rollout_status,normalizer_version,file_path,manifest_payload)
        VALUES($1,$2,$3,$4,'basenames_v1','active',$5,$6,$7) RETURNING manifest_id")
        .bind(version).bind(namespace).bind(family).bind(chain).bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
        .bind(format!("gate1/{family}.toml")).bind(payload).fetch_one(pool).await?;
    Ok(id)
}
