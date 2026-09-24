//! `registry_generation` through the production phases: raw ENSv1 registry and controller logs
//! interpreted by the Interpret engine, then projected by Project. The name is recorded only in
//! the 2017 registry until the current registry writes its record, and the deployed registry
//! answers from the 2017 registry until then
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L18-L46 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L60-L84 @ ens_v1@91c966f).
//! A single Interpret batch and one batch per block (each restoring its state from the stored
//! events) must agree, and so must a full Project rebuild and incremental Project batches.

#[allow(dead_code)]
mod support;

use alloy_primitives::{B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use anyhow::Result;
use bigname_interpret::{
    BatchRequest as InterpretRequest, Engine as InterpretEngine, RunMode as InterpretRunMode,
};
use bigname_manifests::{load_repository, sync_schema_v2_repository};
use bigname_project::{BatchRequest, Engine, Marker, RunMode};
use serde_json::Value;
use sqlx::PgPool;

use support::ScratchDatabase;

const CHAIN: &str = "ethereum-mainnet";
const REGISTRY: &str = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E";
const OLD_REGISTRY: &str = "0x314159265dd8dbb310642f98f50c066173c1259b";
const CONTROLLER: &str = "0x283Af0B28c62C092C9727F1Ee09c02CA627EB7F5";
const SENDER: &str = "0x0000000000000000000000000000000000000043";
const OWNER: &str = "0x00000000000000000000000000000000000000a1";
const NEXT_OWNER: &str = "0x00000000000000000000000000000000000000a2";
/// The 2017 registry records `pointer.eth` and `hidden.eth`.
const OLD_RECORD: i64 = 0;
/// The legacy controller's label-bearing renewal materializes `pointer.eth`.
const SURFACED: i64 = 1;
/// The current registry writes `pointer.eth`'s first record, for the same owner.
const HANDOFF: i64 = 2;
/// A later current-registry `Transfer`.
const MOVED: i64 = 3;

sol! {
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    event Transfer(bytes32 indexed node, address owner);
    event NameRenewed(string name, bytes32 indexed label, uint256 cost, uint256 expires);
}

fn namehash(labels: &[&[u8]]) -> B256 {
    labels.iter().rev().fold(B256::ZERO, |node, label| {
        keccak256([node.as_slice(), keccak256(label).as_slice()].concat())
    })
}

fn block_hash(block: i64) -> String {
    format!("{CHAIN}-block-{block}")
}

/// The checked-in mainnet profile, with the registries and the legacy controller declared from
/// block zero so the fixture can use small block numbers.
async fn seed(pool: &PgPool) -> Result<()> {
    let profile = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/mainnet");
    sync_schema_v2_repository(pool, &load_repository(profile)?).await?;
    sqlx::query(
        "WITH declarations AS (
           UPDATE manifest_contract_instances SET start_block_number = 0
           WHERE chain_id = $1
             AND lower(declared_address) IN (lower($2), lower($3), lower($4))
           RETURNING contract_instance_id
         )
         UPDATE contract_instance_addresses SET active_from_block_number = 0
         WHERE contract_instance_id IN (SELECT contract_instance_id FROM declarations)",
    )
    .bind(CHAIN)
    .bind(REGISTRY)
    .bind(OLD_REGISTRY)
    .bind(CONTROLLER)
    .execute(pool)
    .await?;
    let eth = namehash(&[b"eth"]);
    let logs = [
        (
            OLD_RECORD,
            OLD_REGISTRY,
            NewOwner {
                node: eth,
                label: keccak256(b"pointer"),
                owner: OWNER.parse()?,
            }
            .encode_log_data(),
        ),
        (
            OLD_RECORD,
            OLD_REGISTRY,
            NewOwner {
                node: eth,
                label: keccak256(b"hidden"),
                owner: OWNER.parse()?,
            }
            .encode_log_data(),
        ),
        (
            SURFACED,
            CONTROLLER,
            NameRenewed {
                name: "pointer".into(),
                label: keccak256(b"pointer"),
                cost: U256::from(1),
                expires: U256::from(4_102_444_800_u64),
            }
            .encode_log_data(),
        ),
        (
            HANDOFF,
            REGISTRY,
            NewOwner {
                node: eth,
                label: keccak256(b"pointer"),
                owner: OWNER.parse()?,
            }
            .encode_log_data(),
        ),
        (
            MOVED,
            REGISTRY,
            Transfer {
                node: namehash(&[b"pointer", b"eth"]),
                owner: NEXT_OWNER.parse()?,
            }
            .encode_log_data(),
        ),
    ];
    for block in OLD_RECORD..=MOVED {
        sqlx::query(
            "INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number,
                                        block_timestamp, canonicality_state)
             VALUES ($1, $2, $3, $4, to_timestamp($4), 'canonical')",
        )
        .bind(CHAIN)
        .bind(block_hash(block))
        .bind((block > 0).then(|| block_hash(block - 1)))
        .bind(block)
        .execute(pool)
        .await?;
    }
    for (log_index, (block, emitter, log)) in logs.into_iter().enumerate() {
        let transaction = format!("{CHAIN}-transaction-{log_index}");
        sqlx::query(
            "INSERT INTO raw_transactions (chain_id, block_hash, block_number, transaction_hash,
                                           transaction_index, from_address, to_address)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(CHAIN)
        .bind(block_hash(block))
        .bind(block)
        .bind(&transaction)
        .bind(log_index as i64)
        .bind(SENDER)
        .bind(emitter)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO raw_logs (chain_id, block_hash, block_number, transaction_hash,
                                   transaction_index, log_index, emitting_address, topics, data)
             VALUES ($1, $2, $3, $4, $5, $5, $6, $7, $8)",
        )
        .bind(CHAIN)
        .bind(block_hash(block))
        .bind(block)
        .bind(&transaction)
        .bind(log_index as i64)
        .bind(emitter)
        .bind(
            log.topics()
                .iter()
                .map(|topic| format!("{topic:#x}"))
                .collect::<Vec<_>>(),
        )
        .bind(log.data.as_ref())
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn interpret(pool: &PgPool, from_block: i64, to_block: i64) -> Result<()> {
    let outcome = InterpretEngine::new(pool.clone())
        .run_batch(InterpretRequest {
            chain_id: CHAIN.into(),
            from_block,
            to_block,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    assert!(outcome.complete);
    Ok(())
}

async fn project(pool: &PgPool, target: i64, resume: Option<i64>) -> Result<()> {
    let outcome = Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.into(),
            target_block: target,
            affected_from_block: resume.map_or(OLD_RECORD, |previous| previous + 1),
            affected_to_block: target,
            resume_current: resume.map(|number| Marker {
                number,
                hash: block_hash(number),
            }),
            mode: RunMode::Normal,
        })
        .await?;
    assert!(outcome.complete);
    Ok(())
}

/// The interpreted events Project reads, without storage-assigned ids.
async fn interpreted(pool: &PgPool) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT jsonb_agg(jsonb_build_object(
                    'identity', event_identity, 'kind', event_kind, 'name', logical_name_id,
                    'resource', resource_id, 'after', after_state,
                    'visibility', consumer_visibility)
                ORDER BY event_identity)
         FROM normalized_events WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .fetch_one(pool)
    .await?)
}

/// `(raw_name, authority_selection, public authority)` for every projected ENS name.
async fn selections(pool: &PgPool) -> Result<Vec<(String, Value, Option<&'static str>)>> {
    let rows: Vec<(String, Value)> = sqlx::query_as(
        "SELECT raw_name, provenance FROM name_current WHERE namespace = 'ens' ORDER BY raw_name",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(name, provenance)| {
            let authority = bigname_storage::name_current_public_authority(&provenance);
            (name, provenance["authority_selection"].clone(), authority)
        })
        .collect())
}

#[tokio::test]
async fn registry_generation_matches_across_interpret_and_project_batching() -> Result<()> {
    let whole = ScratchDatabase::create("registry_generation_whole").await?;
    let split = ScratchDatabase::create("registry_generation_split").await?;
    seed(whole.pool()).await?;
    seed(split.pool()).await?;
    interpret(whole.pool(), OLD_RECORD, MOVED).await?;
    for block in OLD_RECORD..=MOVED {
        interpret(split.pool(), block, block).await?;
    }
    assert_eq!(
        interpreted(whole.pool()).await?,
        interpreted(split.pool()).await?,
        "a batch per block must interpret the handoff as one batch does"
    );

    let hidden = format!("{:#x}", namehash(&[b"hidden", b"eth"]));
    for block in OLD_RECORD..=MOVED {
        // A full rebuild at each position, and incremental batches that follow the chain.
        project(whole.pool(), block, None).await?;
        project(split.pool(), block, (block > OLD_RECORD).then(|| block - 1)).await?;
        let projected = selections(whole.pool()).await?;
        assert_eq!(projected, selections(split.pool()).await?, "block {block}");
        // A bare 2017-registry record creates no public name.
        let hidden_rows: i64 =
            sqlx::query_scalar("SELECT count(*) FROM name_current WHERE lower(namehash) = $1")
                .bind(&hidden)
                .fetch_one(whole.pool())
                .await?;
        assert_eq!(hidden_rows, 0, "block {block}");
        if block == OLD_RECORD {
            assert!(projected.is_empty(), "{projected:?}");
            continue;
        }
        let [(name, selection, authority)] = projected.as_slice() else {
            panic!("block {block}: {projected:?}");
        };
        assert_eq!(name, "pointer.eth");
        assert_eq!(selection["authority_arm"], "ens_v1", "block {block}");
        if block == SURFACED {
            assert_eq!(selection["registry_generation"], "old", "{selection}");
            assert!(selection.get("registry_handoff_block_number").is_none());
            assert_eq!(*authority, Some("ens_v0"));
        } else {
            assert_eq!(selection["registry_generation"], "current", "{selection}");
            // The first current record's block, unchanged by the later Transfer.
            assert_eq!(selection["registry_handoff_block_number"], HANDOFF);
            assert_eq!(*authority, Some("ens_v1"));
        }
    }
    split.cleanup().await?;
    whole.cleanup().await
}
