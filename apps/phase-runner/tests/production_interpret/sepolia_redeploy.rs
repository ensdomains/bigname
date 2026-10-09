//! TYR-183: the checked-in Sepolia profile replaces the 2026-09-15 ENSv2 deployment with the
//! 2026-10-01 redeploy over a database that already interpreted and projected the old set. The
//! previous profile is rebuilt from the checked-in one by mapping each redeployed contract back to
//! its 2026-09-15 address and receipt block. The Interpret redo runs on the engine directly: the
//! runner's watch-set attestation fence and the Ingest refetch it waits for are covered elsewhere.
use super::*;
use bigname_storage::families::control::cutover::load_cut_over_on;

const CHAIN: &str = "ethereum-sepolia";
const FIRST: i64 = 11_709_000;
const HEAD: i64 = 11_821_700;
const CLIENT_PROXY: &str = "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe";
const MANAGED_PROXY: &str = "0x6d80F2172CFdEc5730fE683860C33d26fC42e6F1";
const OLD_REGISTRY: &str = "0x657ea849311d3d5823348dded7c2aaafb3ede09e";
const NEW_REGISTRY: &str = "0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4";
const OLD_UNIVERSAL_RESOLVER: &str = "0x5d25c1d6acbb71b7a28aa7899618a3412a8303e3";
const NEW_UNIVERSAL_RESOLVER: &str = "0x24e1d8e068620b647ca097f961a61055f4f42d72";
const OLD_RESERVATION: i64 = 11_709_797;
const NEW_RESERVATION: i64 = 11_821_474;
const OLD_WRITE_AFTER_REPOINT: i64 = 11_821_690;
const REPOINT: i64 = 11_821_680;

/// (2026-10-01 address, 2026-09-15 address, 2026-10-01 start, 2026-09-15 start).
const REDEPLOYED: [(&str, &str, Option<u64>, Option<u64>); 15] = [
    (
        "0xb458d6a3a77919449d03e7a6903c26827c1ec43f",
        "0x9703dbd26dab89504490994138cf2c575251a9ce",
        Some(11_820_291),
        Some(11_708_988),
    ),
    (
        NEW_REGISTRY,
        OLD_REGISTRY,
        Some(11_820_399),
        Some(11_709_066),
    ),
    (
        "0xf633e7fc17e2bbe0d0965d18ec1821dcb754a3d3",
        "0xabe76f6c8dfced81aa5a2bb8034202a7136b94ca",
        Some(11_820_440),
        Some(11_709_083),
    ),
    (
        "0x322b7581ca210a69c6d0e0d7c88a7688d2789cb0",
        "0xb2bf4a9a86d29661ea93223582b9945943931e42",
        Some(11_820_288),
        Some(11_708_986),
    ),
    (
        "0xdc4a563d00f5c3012b699794eb9e13a561be386f",
        "0xd7e590ad0e92a6ac1d81f4483a9b951d3585a50f",
        Some(11_820_448),
        Some(11_709_089),
    ),
    (
        "0x2a35b94df22cc7354570be2284655e2cdc0e64a2",
        "0x7ed171bb143a905f56105e4ea146543ecb122f55",
        Some(11_820_436),
        Some(11_709_081),
    ),
    (
        "0x6029a063d69b09d23c52a754a90e4fe43adac3a8",
        "0xab1b57c6ee5e91e6090595c0af14cb9b8bc7773f",
        Some(11_820_452),
        Some(11_709_093),
    ),
    (
        "0xb58a90a39d13cce1d0e192b5da5c47640855b04d",
        "0x950b93885b33ce4c7e8571be2c88a1aa93d82f49",
        Some(11_820_435),
        Some(11_709_080),
    ),
    (
        "0x4a4c8b7cdab6b19dc2cdb417cdb53a2ccbaf5322",
        "0xbe68ff9afc7d5a1864ffef5c82de0a1c13e6b529",
        Some(11_820_450),
        Some(11_709_091),
    ),
    (
        "0xf2ece44980778966b8a0fccb3a9e339440f6e045",
        "0xd06e726e9bd8ac0f33a2a45f4cc28fe10d656a36",
        Some(11_820_442),
        Some(11_709_084),
    ),
    (
        "0xda70306c98e97ece36f997a21368e53298572991",
        "0x9e726eb570beb6bceb495ab8cda7df517d4e841c",
        Some(11_820_318),
        Some(11_708_995),
    ),
    (
        "0xa8f86ee5cdd28703bd876f3a8c10b1de70f36899",
        "0x58d12d60471b98f191856e4c2d56886e9c3ea573",
        Some(11_820_455),
        Some(11_709_095),
    ),
    (
        "0xbe768b63e5fbbfbb0ae97e9064e0002df8001880",
        "0x2741543c3b14640b97bc70a233318032f7e35bac",
        Some(11_820_449),
        Some(11_709_090),
    ),
    (
        "0x115eb53f0c60696633855f90b138178fb40b2b2c",
        "0x14f09fd05d4585759e54844dc9b00147131cf243",
        Some(11_820_406),
        Some(11_709_070),
    ),
    (NEW_UNIVERSAL_RESOLVER, OLD_UNIVERSAL_RESOLVER, None, None),
];

const REDEPLOYED_FAMILIES: [&str; 6] = [
    "ens_v2_root_l1",
    "ens_v2_registry_l1",
    "ens_v2_registrar_l1",
    "ens_v2_resolver_l1",
    "ens_v2_migration_l1",
    "ens_execution",
];

fn checked_in_profile() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia")
}

fn copy_dir(source: &std::path::Path, target: &std::path::Path) -> Result<()> {
    std::fs::create_dir_all(target)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            copy_dir(&path, &target.join(entry.file_name()))?;
        } else {
            std::fs::copy(&path, target.join(entry.file_name()))?;
        }
    }
    Ok(())
}

/// The profile the previous release synced: the six redeployed families as version 1 of the
/// 2026-09-15 deployment.
fn previous_profile() -> Result<std::path::PathBuf> {
    let root = std::env::temp_dir().join(format!("bigname-sepolia-20260915-{}", Uuid::new_v4()));
    copy_dir(&checked_in_profile(), &root)?;
    for family in REDEPLOYED_FAMILIES {
        let directory = root.join("ethereum/ens").join(family);
        let mut manifest = std::fs::read_to_string(directory.join("v2.toml"))?
            .replacen("manifest_version = 2", "manifest_version = 1", 1)
            .replacen(
                "deployment_epoch = \"ens_v2_sepolia_20261001\"",
                "deployment_epoch = \"ens_v2_sepolia_20260915\"",
                1,
            );
        for (new, old, new_start, old_start) in REDEPLOYED {
            manifest = manifest.replace(new, old);
            if let (Some(new_start), Some(old_start)) = (new_start, old_start) {
                manifest = manifest.replace(
                    &format!("start_block = {new_start}"),
                    &format!("start_block = {old_start}"),
                );
            }
        }
        std::fs::remove_file(directory.join("v2.toml"))?;
        std::fs::write(directory.join("v1.toml"), manifest)?;
    }
    Ok(root)
}

/// Interpret needs unbroken canonical lineage, so every block from `FIRST` to the head exists.
async fn lineage(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number, block_timestamp, canonicality_state
         )
         SELECT $1, $1 || '-block-' || height::text,
                CASE WHEN height > $2 THEN $1 || '-block-' || (height - 1)::text END,
                height, to_timestamp(height), 'canonical'::canonicality_state
         FROM generate_series($2::bigint, $3::bigint) AS height",
    )
    .bind(CHAIN)
    .bind(FIRST)
    .bind(HEAD)
    .execute(pool)
    .await?;
    Ok(())
}

async fn transaction(pool: &PgPool, number: i64, to: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO raw_transactions (
             chain_id, block_hash, block_number, transaction_hash, transaction_index,
             from_address, to_address
         ) VALUES ($1, $2, $3, $4, 0, $5, $6)",
    )
    .bind(CHAIN)
    .bind(block_hash(CHAIN, number))
    .bind(number)
    .bind(format!("{CHAIN}-transaction-{number}"))
    .bind(SENDER)
    .bind(to)
    .execute(pool)
    .await?;
    Ok(())
}

async fn log(
    pool: &PgPool,
    number: i64,
    emitter: &str,
    fact: alloy_primitives::LogData,
) -> Result<()> {
    insert_log_at(
        pool,
        CHAIN,
        number,
        &format!("{CHAIN}-transaction-{number}"),
        0,
        emitter,
        fact.topics(),
        fact.data.as_ref(),
    )
    .await
}

fn reservation(label: &str, expiry: u64) -> Result<alloy_primitives::LogData> {
    Ok(v2_registry_events::LabelReserved {
        tokenId: versioned_token(label, 0),
        labelHash: keccak256(label.as_bytes()),
        label: label.to_owned(),
        expiry,
        sender: SENDER.parse()?,
    }
    .encode_log_data())
}

/// Both generations' logs as Sepolia emitted them, except that the client proxy's repoint to the
/// managed proxy is moved up to `FIRST`: the old registry keeps receiving writes after the managed
/// proxy moves to the redeploy's implementation.
async fn seed_both_generations(pool: &PgPool) -> Result<()> {
    let upgraded = |implementation: &str| -> Result<alloy_primitives::LogData> {
        Ok(Upgraded {
            implementation: implementation.parse()?,
        }
        .encode_log_data())
    };
    let facts: [(i64, &str, alloy_primitives::LogData); 8] = [
        (FIRST, CLIENT_PROXY, upgraded(MANAGED_PROXY)?),
        (
            11_709_066,
            OLD_REGISTRY,
            RegistryCreated {}.encode_log_data(),
        ),
        (
            OLD_RESERVATION,
            OLD_REGISTRY,
            reservation("nick", 1_801_817_044)?,
        ),
        (11_710_193, MANAGED_PROXY, upgraded(OLD_UNIVERSAL_RESOLVER)?),
        (
            11_820_399,
            NEW_REGISTRY,
            RegistryCreated {}.encode_log_data(),
        ),
        (
            NEW_RESERVATION,
            NEW_REGISTRY,
            reservation("nick", 1_803_965_433)?,
        ),
        (REPOINT, MANAGED_PROXY, upgraded(NEW_UNIVERSAL_RESOLVER)?),
        (
            OLD_WRITE_AFTER_REPOINT,
            OLD_REGISTRY,
            reservation("later", 1_900_000_000)?,
        ),
    ];
    lineage(pool).await?;
    for (number, emitter, fact) in facts {
        transaction(pool, number, emitter).await?;
        log(pool, number, emitter, fact).await?;
    }
    sqlx::query(
        "INSERT INTO chain_heads (chain_id, latest_block_hash, latest_block_number)
         VALUES ($1, $2, $3)",
    )
    .bind(CHAIN)
    .bind(block_hash(CHAIN, HEAD))
    .bind(HEAD)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO ingest_cursors (
             chain_id, source_key, source_kind, seed_basis, start_block_number,
             next_block_number, target_block_number, last_processed_block_number,
             last_processed_block_hash
         ) VALUES ($1, 'intake', 'drpc', 'ethereum_head', 0, $2 + 1, $2, $2, $3)",
    )
    .bind(CHAIN)
    .bind(HEAD)
    .bind(block_hash(CHAIN, HEAD))
    .execute(pool)
    .await?;
    Ok(())
}

async fn interpret_through_head(
    pool: &PgPool,
    from_block: i64,
    mode: InterpretRunMode,
) -> Result<()> {
    interpret_through(pool, from_block, HEAD, mode).await
}

async fn interpret_through(
    pool: &PgPool,
    from_block: i64,
    to_block: i64,
    mode: InterpretRunMode,
) -> Result<()> {
    let engine = Engine::new(pool.clone())
        .with_blocks_per_batch(std::num::NonZeroU32::new(200_000).expect("non-zero"));
    let mut resume_current = None;
    loop {
        let outcome = engine
            .run_batch(BatchRequest {
                chain_id: CHAIN.to_owned(),
                from_block,
                to_block,
                resume_current,
                mode,
            })
            .await?;
        if outcome.complete {
            return Ok(());
        }
        resume_current = Some(outcome.current);
    }
}

/// The blocks of the reservations that name `nick.eth`.
async fn named_nick_reservations(pool: &PgPool) -> Result<Vec<i64>> {
    let nick = format!("ens:{:#x}", raw_namehash(&[b"nick", b"eth"]));
    Ok(sqlx::query_scalar(
        "SELECT block_number FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'RegistrationReserved' AND logical_name_id = $2
         ORDER BY block_number",
    )
    .bind(CHAIN)
    .bind(nick)
    .fetch_all(pool)
    .await?)
}

async fn managed_proxy_row(pool: &PgPool) -> Result<(String, String, i64)> {
    Ok(sqlx::query_as(
        "SELECT implementation, implementation_kind, block_number
         FROM project_universal_resolver_proxy
         WHERE chain_id = $1 AND lower(proxy_address) = lower($2)",
    )
    .bind(CHAIN)
    .bind(MANAGED_PROXY)
    .fetch_one(pool)
    .await?)
}

async fn cut_over(pool: &PgPool) -> Result<bool> {
    load_cut_over_on(&mut *pool.acquire().await?, CHAIN).await
}

#[tokio::test]
async fn the_sepolia_redeploy_replaces_the_dropped_set_through_the_attested_redo() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_sepolia_redeploy").await?;
    let pool = scratch.pool();
    let previous = previous_profile()?;
    sync_schema_v2_repository(pool, &load_repository(&previous)?).await?;
    seed_both_generations(pool).await?;
    PhaseStore::new(pool.clone())
        .initialize_chain(CHAIN)
        .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed', started_at = now(), finished_at = now(),
             current_block_number = $2,
             current_block_hash = $3,
             input_content_hash = CASE WHEN phase_name IN ('interpret', 'project')
                                       THEN $4 END
         WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .bind(HEAD)
    .bind(block_hash(CHAIN, HEAD))
    .bind(INTERPRETER_CONTENT_HASH)
    .execute(pool)
    .await?;
    interpret_through_head(pool, FIRST, InterpretRunMode::Normal).await?;
    run_project(pool, CHAIN, HEAD, 0, HEAD).await?;

    // Under the 2026-09-15 profile the old registry names nick.eth and the redeploy's
    // implementation is unlisted. That profile admits its own ENSv2 root registry, so the
    // chain is cut over whatever the proxy forwards to.
    assert_eq!(named_nick_reservations(pool).await?, [OLD_RESERVATION]);
    assert_eq!(
        managed_proxy_row(pool).await?,
        (
            NEW_UNIVERSAL_RESOLVER.to_owned(),
            "other".to_owned(),
            REPOINT
        )
    );
    assert!(cut_over(pool).await?);

    // The checked-in profile syncs over that state: the old declarations retire at the head,
    // derived phases need the attested redo, and the new registry's announcement rule stamps
    // Ingest from the earliest retained `RegistryCreated` through the head.
    sync_schema_v2_repository(pool, &load_repository(checked_in_profile())?).await?;
    let markers: Vec<String> = sqlx::query_scalar(
        "SELECT input_content_hash FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')",
    )
    .bind(CHAIN)
    .fetch_all(pool)
    .await?;
    assert_eq!(markers.len(), 2);
    assert!(
        markers
            .iter()
            .all(|marker| marker.starts_with("manifest-authority:")),
        "{markers:?}"
    );
    let ingest_redo: (bool, Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT redo_in_progress, redo_from_block_number, redo_to_block_number
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(CHAIN)
    .fetch_one(pool)
    .await?;
    assert_eq!(ingest_redo, (true, Some(11_709_066), Some(HEAD)));
    let old_registry: (Option<i64>, Option<bool>) = sqlx::query_as(
        "SELECT max(active_to_block_number), bool_and(deactivated_at IS NOT NULL)
         FROM contract_instance_addresses
         WHERE chain_id = $1 AND lower(address) = $2",
    )
    .bind(CHAIN)
    .bind(OLD_REGISTRY)
    .fetch_one(pool)
    .await?;
    assert_eq!(old_registry, (Some(HEAD), Some(true)));
    let dropped: Vec<(String, String)> = sqlx::query_as(
        "SELECT source_family, rollout_status FROM manifest_versions
         WHERE chain_id = $1 AND deployment_label = 'ens_v2_sepolia_20260915'
         ORDER BY source_family",
    )
    .bind(CHAIN)
    .fetch_all(pool)
    .await?;
    let mut expected: Vec<(String, String)> = REDEPLOYED_FAMILIES
        .iter()
        .map(|family| ((*family).to_owned(), "deprecated".to_owned()))
        .collect();
    expected.sort();
    assert_eq!(dropped, expected);

    interpret_through_head(pool, FIRST, InterpretRunMode::Redo).await?;
    run_project(pool, CHAIN, HEAD, 0, HEAD).await?;

    // Only the redeploy's reservation names nick.eth. The proxy row now classifies the
    // redeploy's implementation as listed, and the chain stays cut over.
    assert_eq!(named_nick_reservations(pool).await?, [NEW_RESERVATION]);
    assert_eq!(
        managed_proxy_row(pool).await?,
        (
            NEW_UNIVERSAL_RESOLVER.to_owned(),
            "admitted_universal_resolver".to_owned(),
            REPOINT
        )
    );
    assert!(cut_over(pool).await?);

    // Nothing stays admitted through the deprecated versions. The old registry still announces
    // itself through the active registry family's `RegistryCreated` rule, as it would on a fresh
    // corpus, so its reservations are re-derived, but it is no longer a declared `.eth` anchor and
    // they name nothing.
    let deprecated_edges: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM discovery_edges edge
         JOIN manifest_versions manifest ON manifest.manifest_id = edge.source_manifest_id
         WHERE edge.chain_id = $1 AND manifest.rollout_status = 'deprecated'
           AND edge.canonicality_state <> 'orphaned'",
    )
    .bind(CHAIN)
    .fetch_one(pool)
    .await?;
    assert_eq!(deprecated_edges, 0);
    let old_reservations: Vec<(i64, Option<String>, String)> = sqlx::query_as(
        "SELECT event.block_number, event.logical_name_id, manifest.rollout_status
         FROM normalized_events event
         JOIN manifest_versions manifest ON manifest.manifest_id = event.source_manifest_id
         WHERE event.chain_id = $1 AND event.event_kind = 'RegistrationReserved'
           AND lower(event.raw_fact_ref ->> 'emitting_address') = $2
         ORDER BY event.block_number",
    )
    .bind(CHAIN)
    .bind(OLD_REGISTRY)
    .fetch_all(pool)
    .await?;
    assert_eq!(
        old_reservations,
        [
            (OLD_RESERVATION, None, "active".to_owned()),
            (OLD_WRITE_AFTER_REPOINT, None, "active".to_owned()),
        ]
    );

    // After the synchronization head the retired declaration caps the old registry's
    // re-announced admission, so its next write derives nothing (TYR-195), while the
    // redeploy's registry keeps naming.
    for number in [HEAD + 1, HEAD + 2] {
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number, block_timestamp,
                 canonicality_state
             ) VALUES ($1, $1 || '-block-' || $2::text, $1 || '-block-' || ($2 - 1)::text, $2,
                       to_timestamp($2), 'canonical')",
        )
        .bind(CHAIN)
        .bind(number)
        .execute(pool)
        .await?;
    }
    transaction(pool, HEAD + 1, OLD_REGISTRY).await?;
    log(
        pool,
        HEAD + 1,
        OLD_REGISTRY,
        reservation("after", 1_900_000_000)?,
    )
    .await?;
    transaction(pool, HEAD + 2, NEW_REGISTRY).await?;
    log(
        pool,
        HEAD + 2,
        NEW_REGISTRY,
        reservation("fresh", 1_900_000_000)?,
    )
    .await?;
    sqlx::query(
        "UPDATE chain_heads SET latest_block_number = $2, latest_block_hash = $3
         WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .bind(HEAD + 2)
    .bind(block_hash(CHAIN, HEAD + 2))
    .execute(pool)
    .await?;
    interpret_through(pool, HEAD + 1, HEAD + 2, InterpretRunMode::Normal).await?;
    let after_head: Vec<(i64, bool)> = sqlx::query_as(
        "SELECT block_number, logical_name_id IS NOT NULL FROM normalized_events
         WHERE chain_id = $1 AND block_number > $2 AND event_kind = 'RegistrationReserved'
         ORDER BY block_number",
    )
    .bind(CHAIN)
    .bind(HEAD)
    .fetch_all(pool)
    .await?;
    assert_eq!(after_head, [(HEAD + 2, true)]);

    std::fs::remove_dir_all(previous)?;
    scratch.cleanup().await
}
