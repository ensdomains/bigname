//! Canonical registration deadlines through real Engine publications and time-only blocks.
use super::*;
use bigname_project::families::{self, FamilyMode, FamilyOptions};
use serde_json::{Value, json};

sol! {
    event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
    event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender);
    event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender);
    event Upgraded(address indexed implementation);
}

const LEASE: u64 = 2_000_000_000;
const RESERVED: u64 = LEASE + 62 * 86_400;
const EXTENDED: u64 = RESERVED + 30 * 86_400;
const GRACE: u64 = EXTENDED + 28 * 86_400;
const PROXY: &str = "0xeeeeeeee14d718c2b47d9923deab1335e144eeee";
const IMPLEMENTATION: &str = "0x24e1d8e068620b647ca097f961a61055f4f42d72";

pub(super) async fn publish(pool: &PgPool, target: i64, mode: FamilyMode) -> TestResult {
    let marker = bigname_project::Marker {
        number: target,
        hash: block_hash(target),
    };
    let token = families::input_token(pool, CHAIN).await?;
    let outcome = families::apply(
        pool,
        CHAIN,
        &marker,
        mode,
        &token,
        &FamilyOptions::new(bigname_content_hash::INTERPRETER_CONTENT_HASH),
    )
    .await?;
    assert_eq!(outcome.marker, Some(marker));
    Ok(())
}

#[tokio::test]
async fn canonical_reservation_deadline_survives_engine_restart_and_passive_expiry() -> TestResult {
    let db = database("interpret_canonical_registration_lifecycle").await?;
    let pool = db.pool();
    sync_schema_v2_repository(
        pool,
        &load_repository(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia"),
        )?,
    )
    .await?;
    let times = [
        LEASE - 100,
        LEASE - 90,
        LEASE - 80,
        EXTENDED - 1,
        EXTENDED,
        EXTENDED + 1,
        GRACE - 1,
        GRACE,
        GRACE + 1,
    ];
    for (offset, timestamp) in times.into_iter().enumerate() {
        let block = SETUP_BLOCK + offset as i64;
        sqlx::query("INSERT INTO chain_lineage(chain_id,block_hash,parent_hash,block_number,block_timestamp,canonicality_state)
            VALUES($1,$2,$3,$4,to_timestamp($5),'canonical')")
            .bind(CHAIN).bind(block_hash(block)).bind((offset>0).then(||block_hash(block-1)))
            .bind(block).bind(timestamp as f64).execute(pool).await?;
    }
    let label = "canonical-deadline";
    let hash = keccak256(label.as_bytes());
    let node = eth_namehash(hash);
    let token = U256::from_be_bytes(hash.0) >> 32 << 32;
    let owner: Address = OWNER.parse()?;
    for (offset, logs) in [
        vec![
            (
                BASE_REGISTRAR,
                base_registrar::Transfer {
                    from: Address::ZERO,
                    to: owner,
                    tokenId: U256::from_be_bytes(hash.0),
                }
                .encode_log_data(),
            ),
            (
                ENS_REGISTRY,
                ens_registry::NewOwner {
                    node: eth_node(),
                    label: hash,
                    owner,
                }
                .encode_log_data(),
            ),
            (
                BASE_REGISTRAR,
                NameRegistered {
                    id: U256::from_be_bytes(hash.0),
                    owner,
                    expires: U256::from(LEASE),
                }
                .encode_log_data(),
            ),
            (
                ETH_REGISTRY,
                LabelReserved {
                    tokenId: token,
                    labelHash: hash,
                    label: label.into(),
                    expiry: RESERVED,
                    sender: owner,
                }
                .encode_log_data(),
            ),
        ],
        vec![(
            PROXY,
            Upgraded {
                implementation: IMPLEMENTATION.parse()?,
            }
            .encode_log_data(),
        )],
        vec![(
            ETH_REGISTRY,
            ExpiryUpdated {
                tokenId: token,
                newExpiry: EXTENDED,
                sender: owner,
            }
            .encode_log_data(),
        )],
    ]
    .into_iter()
    .enumerate()
    {
        let block = SETUP_BLOCK + offset as i64;
        insert_transaction(pool, block, ETH_REGISTRY).await?;
        for (index, (emitter, data)) in logs.into_iter().enumerate() {
            insert_log(pool, block, index as i64, emitter, data).await?;
        }
    }
    let logical = format!("ens:{node:#x}");
    let mut previous = None;
    let mut registration_id = None;
    for (offset, &timestamp) in times.iter().enumerate() {
        let block = SETUP_BLOCK + offset as i64;
        // A fresh Engine at every committed boundary exercises normalized-state restoration.
        Engine::new(pool.clone())
            .run_batch(BatchRequest {
                chain_id: CHAIN.into(),
                from_block: block,
                to_block: block,
                resume_current: previous,
                mode: RunMode::Normal,
            })
            .await?;
        stamp_interpreter_hash(pool, bigname_content_hash::INTERPRETER_CONTENT_HASH).await?;
        publish(
            pool,
            block,
            if offset == 0 {
                FamilyMode::Rebuild
            } else {
                FamilyMode::Normal
            },
        )
        .await?;
        previous = Some(Marker {
            number: block,
            hash: block_hash(block),
        });
        if offset < 2 {
            continue;
        }
        let row = bigname_storage::families::name::load_family_name(pool, &logical)
            .await?
            .ok_or("published registration")?;
        let fields = &row.declared_summary["registration"];
        assert_eq!(
            fields["expiry"],
            json!(EXTENDED.to_string()),
            "canonical reservation expiry changed at t={}: {fields}",
            timestamp
        );
        assert_eq!(
            fields["grace_ends_at"],
            json!(GRACE.to_string()),
            "canonical reservation grace changed at t={}: {fields}",
            timestamp
        );
        let prepared: Value = sqlx::query_scalar(
            "SELECT search_fields FROM project_name_summary WHERE logical_name_id=$1",
        )
        .bind(&logical)
        .fetch_one(pool)
        .await?;
        assert_eq!(prepared["expires_at"], json!(EXTENDED.to_string()));
        assert_eq!(prepared["grace_ends_at"], json!(GRACE.to_string()));
        let expected = if timestamp < EXTENDED {
            "active"
        } else if timestamp < GRACE {
            "expired"
        } else {
            "released"
        };
        assert_eq!(fields["lifecycle_status"], expected);
        assert_eq!(prepared["status"], expected);
        // The ENSv1 lease is released after its own grace while the reservation's schedule
        // runs on. Its holder stays evidence of the past, never current ownership.
        if offset == 2 {
            assert!(fields.get("lapsed_registration").is_none(), "{fields}");
        } else {
            let lapsed = &fields["lapsed_registration"];
            assert_eq!(lapsed["owner"], OWNER, "t={timestamp}: {fields}");
            assert_eq!(lapsed["release_kind"], "expired", "{fields}");
            assert_eq!(lapsed["released_at"], json!(times[3]), "{fields}");
            assert!(row.declared_summary["control"]["owner"].is_null());
            assert!(!bigname_storage::public_name_fields::has_current_control(
                "ens",
                &row.declared_summary,
                row.resource_id.is_some()
            ));
        }
        assert!(prepared.get("registration_status").is_none());
        assert!(
            bigname_storage::public_name_fields::has_registration_identity(
                "ens",
                &row.declared_summary,
                row.resource_id.is_some()
            )
        );
        assert_eq!(
            bigname_storage::public_name_fields::declared_registered_at(&row.declared_summary),
            Some(times[0].to_string()),
            "continuous lease start: {fields}"
        );
        let public_id = fields["identity_resource_id"]
            .as_str()
            .map(str::to_owned)
            .expect("canonical reservation retains the public lease handle");
        if let Some(id) = &registration_id {
            assert_eq!(
                &public_id, id,
                "passive time changed public registration identity"
            );
        } else {
            registration_id = Some(public_id);
        }
        let stored_lookup: Value = sqlx::query_scalar("SELECT core->'declared_summary'->'registration' FROM project_lookup_name WHERE logical_name_id=$1")
            .bind(&logical).fetch_one(pool).await?;
        assert_eq!(
            &stored_lookup, fields,
            "prepared lookup missed time-only lifecycle refresh"
        );
        let next: Option<i64> = sqlx::query_scalar(
            "SELECT recompose_at FROM project_name_summary WHERE logical_name_id=$1",
        )
        .bind(&logical)
        .fetch_one(pool)
        .await?;
        if (EXTENDED..GRACE).contains(&timestamp) {
            assert_eq!(next, Some(GRACE as i64));
        }
        if timestamp >= GRACE {
            assert_eq!(next, None);
        }
    }
    let target = SETUP_BLOCK + times.len() as i64 - 1;
    let original: Value = sqlx::query_scalar(
        "SELECT search_fields FROM project_name_summary WHERE logical_name_id=$1",
    )
    .bind(&logical)
    .fetch_one(pool)
    .await?;
    // Undo the no-log grace cutoff, then replay from the retained E-1 publication.
    families::undo_to(pool, CHAIN, SETUP_BLOCK + 3).await?;
    publish(pool, target, FamilyMode::Normal).await?;
    for ranges in [
        families::RebuildRanges::Off,
        families::RebuildRanges::Through(target),
    ] {
        let token = families::input_token(pool, CHAIN).await?;
        let marker = bigname_project::Marker {
            number: target,
            hash: block_hash(target),
        };
        families::apply(
            pool,
            CHAIN,
            &marker,
            FamilyMode::Rebuild,
            &token,
            &FamilyOptions::new(bigname_content_hash::INTERPRETER_CONTENT_HASH)
                .with_rebuild_ranges(ranges),
        )
        .await?;
        let rebuilt: Value = sqlx::query_scalar(
            "SELECT search_fields FROM project_name_summary WHERE logical_name_id=$1",
        )
        .bind(&logical)
        .fetch_one(pool)
        .await?;
        assert_eq!(
            rebuilt, original,
            "incremental/rebuild/range lifecycle differed"
        );
    }
    // A canonical correction can change dates: orphan the actual extension block while
    // retaining its raw facts, then let full Interpret redo and Project rebuild select history.
    let changed = SETUP_BLOCK + 2;
    sqlx::query("UPDATE chain_lineage SET canonicality_state='orphaned' WHERE chain_id=$1 AND block_hash=$2")
        .bind(CHAIN).bind(block_hash(changed)).execute(pool).await?;
    sqlx::query("INSERT INTO chain_lineage(chain_id,block_hash,parent_hash,block_number,block_timestamp,canonicality_state) VALUES($1,'lifecycle-replacement',$2,$3,to_timestamp($4),'canonical')")
        .bind(CHAIN).bind(block_hash(changed-1)).bind(changed).bind((LEASE-80) as f64).execute(pool).await?;
    for corrected in [true, false] {
        if !corrected {
            sqlx::query("UPDATE chain_lineage SET canonicality_state='orphaned' WHERE chain_id=$1 AND block_hash='lifecycle-replacement'")
                .bind(CHAIN).execute(pool).await?;
            sqlx::query("UPDATE chain_lineage SET canonicality_state='canonical' WHERE chain_id=$1 AND block_hash=$2")
                .bind(CHAIN).bind(block_hash(changed)).execute(pool).await?;
        }
        Engine::new(pool.clone())
            .run_batch(BatchRequest {
                chain_id: CHAIN.into(),
                from_block: SETUP_BLOCK,
                to_block: target,
                resume_current: None,
                mode: RunMode::Redo,
            })
            .await?;
        stamp_interpreter_hash(pool, bigname_content_hash::INTERPRETER_CONTENT_HASH).await?;
        publish(pool, target, FamilyMode::Rebuild).await?;
        let rewritten: Value = sqlx::query_scalar(
            "SELECT search_fields FROM project_name_summary WHERE logical_name_id=$1",
        )
        .bind(&logical)
        .fetch_one(pool)
        .await?;
        if corrected {
            assert_eq!(
                rewritten["expires_at"],
                RESERVED.to_string(),
                "orphaned extension survived redo"
            );
            assert_eq!(
                rewritten["grace_ends_at"],
                (RESERVED + 28 * 86400).to_string()
            );
            assert_eq!(rewritten["status"], "released");
        } else {
            assert_eq!(
                rewritten, original,
                "restored canonical inputs changed the lifecycle"
            );
        }
    }
    db.cleanup().await?;
    Ok(())
}

sol! {
    event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender);
    event LabelUnregistered(uint256 indexed tokenId, address indexed sender);
    event TokenResource(uint256 indexed tokenId, uint256 indexed resource);
    event TokenRegenerated(uint256 indexed oldTokenId, uint256 indexed newTokenId);
}

/// Registry renewal can revive an expired reservation, but a short permitted extension can
/// leave it expired. Claim and token regeneration retain the same canonical registration;
/// renewal of the regenerated token changes its dates. Unregister ends it immediately while
/// retaining those dates; a new reservation is a new canonical instance.
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L226-L265 @ ens_v2_sepolia_20261001@07e55a05)
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L442-L508 @ ens_v2_sepolia_20261001@07e55a05)
#[tokio::test]
async fn canonical_mutations_preserve_schedule_until_actual_replacement() -> TestResult {
    use bigname_storage::families::control::lifecycle::{
        AuthoritySelection, Clock, NameInput, NamePlace, evaluate, load_name_facts,
    };

    let db = database("interpret_canonical_registration_mutations").await?;
    let pool = db.pool();
    sync_schema_v2_repository(
        pool,
        &load_repository(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia"),
        )?,
    )
    .await?;
    let label = "canonical-mutations";
    let hash = keccak256(label.as_bytes());
    let node = eth_namehash(hash);
    let logical = format!("ens:{node:#x}");
    let token = U256::from_be_bytes(hash.0) >> 32 << 32;
    let owner: Address = OWNER.parse()?;
    let regenerated_token = token + U256::from(1);
    let replacement_token = token + U256::from(2);
    let operator: Address = "0x00000000000000000000000000000000000000cc".parse()?;
    let e = RESERVED;
    let times = [
        LEASE - 100,
        e,
        e + 10,
        e + 11,
        e + 12,
        e + 13,
        e + 14,
        e + 15,
        e + 16,
        e + 2001,
    ];
    let logs = vec![
        vec![
            (
                BASE_REGISTRAR,
                base_registrar::Transfer {
                    from: Address::ZERO,
                    to: owner,
                    tokenId: U256::from_be_bytes(hash.0),
                }
                .encode_log_data(),
            ),
            (
                ENS_REGISTRY,
                ens_registry::NewOwner {
                    node: eth_node(),
                    label: hash,
                    owner,
                }
                .encode_log_data(),
            ),
            (
                BASE_REGISTRAR,
                NameRegistered {
                    id: U256::from_be_bytes(hash.0),
                    owner,
                    expires: U256::from(LEASE),
                }
                .encode_log_data(),
            ),
            (
                ETH_REGISTRY,
                LabelReserved {
                    tokenId: token,
                    labelHash: hash,
                    label: label.into(),
                    expiry: e,
                    sender: owner,
                }
                .encode_log_data(),
            ),
            (
                PROXY,
                Upgraded {
                    implementation: IMPLEMENTATION.parse()?,
                }
                .encode_log_data(),
            ),
        ],
        vec![],
        vec![(
            ETH_REGISTRY,
            ExpiryUpdated {
                tokenId: token,
                newExpiry: e + 5,
                sender: owner,
            }
            .encode_log_data(),
        )],
        vec![(
            ETH_REGISTRY,
            ExpiryUpdated {
                tokenId: token,
                newExpiry: e + 1000,
                sender: owner,
            }
            .encode_log_data(),
        )],
        vec![
            (
                ETH_REGISTRY,
                LabelRegistered {
                    tokenId: token,
                    labelHash: hash,
                    label: label.into(),
                    owner,
                    expiry: e + 1000,
                    sender: owner,
                }
                .encode_log_data(),
            ),
            (
                ETH_REGISTRY,
                TransferSingle {
                    operator: owner,
                    from: Address::ZERO,
                    to: owner,
                    id: token,
                    value: U256::from(1),
                }
                .encode_log_data(),
            ),
            (
                ETH_REGISTRY,
                TokenResource {
                    tokenId: token,
                    resource: token,
                }
                .encode_log_data(),
            ),
        ],
        // A role change regenerates the token but keeps the role resource and registration.
        // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L578-L588 @ ens_v2_sepolia_20261001@07e55a05)
        vec![
            (
                ETH_REGISTRY,
                EACRolesChanged {
                    resource: token,
                    account: operator,
                    oldRoleBitmap: U256::ZERO,
                    newRoleBitmap: U256::from(1),
                }
                .encode_log_data(),
            ),
            (
                ETH_REGISTRY,
                TransferSingle {
                    operator: owner,
                    from: owner,
                    to: Address::ZERO,
                    id: token,
                    value: U256::from(1),
                }
                .encode_log_data(),
            ),
            (
                ETH_REGISTRY,
                TokenRegenerated {
                    oldTokenId: token,
                    newTokenId: regenerated_token,
                }
                .encode_log_data(),
            ),
            (
                ETH_REGISTRY,
                TransferSingle {
                    operator: owner,
                    from: Address::ZERO,
                    to: owner,
                    id: regenerated_token,
                    value: U256::from(1),
                }
                .encode_log_data(),
            ),
        ],
        vec![(
            ETH_REGISTRY,
            ExpiryUpdated {
                tokenId: regenerated_token,
                newExpiry: e + 1200,
                sender: owner,
            }
            .encode_log_data(),
        )],
        vec![
            (
                ETH_REGISTRY,
                LabelUnregistered {
                    tokenId: regenerated_token,
                    sender: owner,
                }
                .encode_log_data(),
            ),
            (
                ETH_REGISTRY,
                TransferSingle {
                    operator: owner,
                    from: owner,
                    to: Address::ZERO,
                    id: regenerated_token,
                    value: U256::from(1),
                }
                .encode_log_data(),
            ),
        ],
        vec![(
            ETH_REGISTRY,
            LabelReserved {
                tokenId: replacement_token,
                labelHash: hash,
                label: label.into(),
                expiry: e + 2000,
                sender: owner,
            }
            .encode_log_data(),
        )],
        vec![(
            ETH_REGISTRY,
            LabelReserved {
                tokenId: replacement_token,
                labelHash: hash,
                label: label.into(),
                expiry: e + 1500,
                sender: owner,
            }
            .encode_log_data(),
        )],
    ];
    let expected = [
        (e, "active"),
        (e, "expired"),
        (e + 5, "expired"),
        (e + 1000, "active"),
        (e + 1000, "active"),
        (e + 1000, "active"),
        (e + 1200, "active"),
        (e + 1200, "released"),
        (e + 2000, "active"),
        (e + 1500, "expired"),
    ];
    let mut previous = None;
    let mut previous_origin: Option<(Value, Value)> = None;
    let mut claimed_resource = None;
    for (offset, block_logs) in logs.into_iter().enumerate() {
        let block = SETUP_BLOCK + offset as i64;
        sqlx::query("INSERT INTO chain_lineage(chain_id,block_hash,parent_hash,block_number,block_timestamp,canonicality_state)
            VALUES($1,$2,$3,$4,to_timestamp($5),'canonical')")
            .bind(CHAIN).bind(block_hash(block)).bind((offset>0).then(||block_hash(block-1)))
            .bind(block).bind(times[offset] as f64).execute(pool).await?;
        if !block_logs.is_empty() {
            insert_transaction(pool, block, ETH_REGISTRY).await?;
        }
        for (index, (emitter, data)) in block_logs.into_iter().enumerate() {
            insert_log(pool, block, index as i64, emitter, data).await?;
        }
        Engine::new(pool.clone())
            .run_batch(BatchRequest {
                chain_id: CHAIN.into(),
                from_block: block,
                to_block: block,
                resume_current: previous,
                mode: RunMode::Normal,
            })
            .await?;
        stamp_interpreter_hash(pool, bigname_content_hash::INTERPRETER_CONTENT_HASH).await?;
        publish(
            pool,
            block,
            if offset == 0 {
                FamilyMode::Rebuild
            } else {
                FamilyMode::Normal
            },
        )
        .await?;
        previous = Some(Marker {
            number: block,
            hash: block_hash(block),
        });
        let row = bigname_storage::families::name::load_family_name(pool, &logical)
            .await?
            .ok_or("mutated registration")?;
        let fields = &row.declared_summary["registration"];
        assert_eq!(
            fields["expiry"],
            expected[offset].0.to_string(),
            "mutation {offset}: {fields}"
        );
        assert_eq!(
            fields["grace_ends_at"],
            (expected[offset].0 + 28 * 86400).to_string()
        );
        assert_eq!(
            fields["lifecycle_status"], expected[offset].1,
            "mutation {offset}: {fields}"
        );
        // The origin is intentionally not a public name field. Read the same lifecycle fold
        // with the actual composed authority selection to inspect its retained identity.
        let facts = load_name_facts(
            pool,
            CHAIN,
            &[NameInput {
                logical_name_id: logical.clone(),
                namehash: row.namehash.clone(),
                selection: AuthoritySelection::from_provenance(&row.provenance),
                place: NamePlace::EthSecondLevel,
            }],
        )
        .await?;
        let shadow = evaluate(
            &facts[0],
            &Clock {
                block_number: block,
                timestamp_seconds: times[offset] as i64,
            },
        )?;
        for field in ["expiry", "grace_ends_at", "lifecycle_status"] {
            assert_eq!(
                shadow.registration[field], fields[field],
                "fold parity at {offset}"
            );
        }
        let origin = &shadow.trace["canonical_registration_origin"];
        assert!(
            origin["event_identity"].as_str().is_some(),
            "missing origin: {origin}"
        );
        assert!(
            origin["state_key"].as_str().is_some(),
            "missing origin key: {origin}"
        );
        let identity = (
            origin["event_identity"].clone(),
            origin["state_key"].clone(),
        );
        if let Some(previous) = &previous_origin {
            if offset < 8 {
                assert_eq!(
                    &identity, previous,
                    "registration origin changed at {offset}"
                );
            } else {
                assert_ne!(
                    identity.0, previous.0,
                    "replacement reused origin at {offset}"
                );
            }
        }
        previous_origin = Some(identity);
        assert_eq!(
            origin["terminal_event"].is_string(),
            offset == 7,
            "{origin}"
        );
        if offset == 4 {
            claimed_resource = Some(row.resource_id.ok_or("claimed registration resource")?);
        }
        if (4..=7).contains(&offset) {
            assert_eq!(
                fields["identity_resource_id"],
                claimed_resource.expect("claim handle").to_string()
            );
            assert_eq!(
                bigname_storage::public_name_fields::declared_registered_at(&row.declared_summary),
                Some(times[4].to_string()),
                "claim start remains through regeneration and release: {fields}"
            );
        }
        if offset >= 8 {
            assert!(fields["identity_resource_id"].is_null());
            assert!(
                bigname_storage::public_name_fields::declared_registered_at(&row.declared_summary)
                    .is_none()
            );
        }
        if (5..=6).contains(&offset) {
            assert_eq!(
                row.resource_id, claimed_resource,
                "regeneration changed resource"
            );
            assert_eq!(row.declared_summary["control"]["owner"], OWNER);
            let token_and_resource: (String, String) = sqlx::query_as(
                "SELECT token_id, upstream_resource FROM project_ens_v2_entry_owner
                 WHERE chain_id=$1 AND registry=$2 AND entry_key=$3",
            )
            .bind(CHAIN)
            .bind(ETH_REGISTRY)
            .bind(format!("0x{token:064x}"))
            .fetch_one(pool)
            .await?;
            assert_eq!(
                token_and_resource,
                (
                    format!("0x{regenerated_token:064x}"),
                    format!("0x{token:064x}")
                ),
                "token regeneration must keep the original role resource"
            );
        }
        if offset >= 8 {
            assert!(
                fields.get("lapsed_registration").is_none(),
                "replacement retained older holder: {fields}"
            );
        }
        if offset == 7 {
            assert_eq!(
                fields["lapsed_registration"]["release_kind"],
                "unregistered"
            );
            assert_eq!(fields["lapsed_registration"]["released_at"], times[offset]);
        }
    }
    // Replaying the real mutation inputs must reproduce the same final schedule.
    let target = SETUP_BLOCK + 9;
    publish(
        pool,
        target,
        FamilyMode::Redo {
            from: SETUP_BLOCK + 2,
            to: target,
        },
    )
    .await?;
    let row = bigname_storage::families::name::load_family_name(pool, &logical)
        .await?
        .ok_or("replayed registration")?;
    assert_eq!(
        row.declared_summary["registration"]["expiry"],
        (e + 1500).to_string()
    );
    assert_eq!(
        row.declared_summary["registration"]["grace_ends_at"],
        (e + 1500 + 28 * 86_400).to_string()
    );
    assert_eq!(
        row.declared_summary["registration"]["lifecycle_status"],
        "expired"
    );
    db.cleanup().await?;
    Ok(())
}
