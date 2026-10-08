//! Public lifecycle coordinates from complete registry registration/topology receipts.
//! The registries retain independent entries when the parent pointer changes.
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L148-L181 @ ens_v2_sepolia_20261001@07e55a05)
use super::*;
use alloy_primitives::{U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use bigname_adapters::schema_v2::{BatchOutput, RawLogInput};
const CHAIN: &str = "ethereum-mainnet";
const ROOT: &str = "0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4";
const A: &str = "0x00000000000000000000000000000000000000a1";
const B: &str = "0x00000000000000000000000000000000000000b1";
const HOLDER: &str = "0x0000000000000000000000000000000000000061";
const OTHER: &str = "0x0000000000000000000000000000000000000062";
const NAME: &str = "leaf.alice.eth";
const E: i64 = 1_700_000_124;
const B_E: i64 = 1_700_000_140;
sol! {
 event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender);
 event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender);
 event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
 event TokenResource(uint256 indexed tokenId, uint256 indexed resource);
 event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender);
 event ParentUpdated(address indexed parent, string label, address indexed sender);
 event LabelUnregistered(uint256 indexed tokenId, address indexed sender);
}
fn token(label: &str) -> U256 {
    U256::from_be_bytes(keccak256(label.as_bytes()).0) >> 32 << 32
}
fn raw(block: i64, index: i64, emitter: &str, data: alloy_primitives::LogData) -> RawLogInput {
    RawLogInput {
        chain_id: CHAIN.into(),
        block_hash: format!("0xhistory{block}"),
        block_number: block,
        block_timestamp: timestamp(1_700_000_000 + block),
        canonicality_state: "canonical".into(),
        transaction_hash: format!("0x{block:064x}"),
        transaction_index: 0,
        log_index: index,
        emitting_address: emitter.into(),
        topics: data.topics().iter().map(|t| format!("{t:#x}")).collect(),
        data: data.data.to_vec(),
    }
}
fn grant(label: &str, owner: &str, expiry: i64) -> Result<[alloy_primitives::LogData; 3]> {
    Ok([
        LabelRegistered {
            tokenId: token(label),
            labelHash: keccak256(label.as_bytes()),
            label: label.into(),
            owner: owner.parse()?,
            expiry: expiry as u64,
            sender: HOLDER.parse()?,
        }
        .encode_log_data(),
        TransferSingle {
            operator: HOLDER.parse()?,
            from: alloy_primitives::Address::ZERO,
            to: owner.parse()?,
            id: token(label),
            value: U256::from(1),
        }
        .encode_log_data(),
        TokenResource {
            tokenId: token(label),
            resource: token(label),
        }
        .encode_log_data(),
    ])
}
async fn fixture(database: &TestDatabase, reattach: bool, terminal: bool) -> Result<()> {
    let (manifest, rules) = v2_history_bounded_regeneration::manifest_and_rules();
    let mut logs = Vec::new();
    for data in grant("alice", HOLDER, 1_900_000_000)? {
        logs.push(raw(120, logs.len() as i64, ROOT, data));
    }
    for (emitter, data) in [
        (
            A,
            ParentUpdated {
                parent: ROOT.parse()?,
                label: "alice".into(),
                sender: HOLDER.parse()?,
            }
            .encode_log_data(),
        ),
        (
            ROOT,
            SubregistryUpdated {
                tokenId: token("alice"),
                subregistry: A.parse()?,
                sender: HOLDER.parse()?,
            }
            .encode_log_data(),
        ),
    ] {
        logs.push(raw(120, logs.len() as i64, emitter, data));
    }
    for data in grant("leaf", HOLDER, E)? {
        logs.push(raw(120, logs.len() as i64, A, data));
    }
    for (emitter, data) in [
        (
            ROOT,
            SubregistryUpdated {
                tokenId: token("alice"),
                subregistry: B.parse()?,
                sender: HOLDER.parse()?,
            }
            .encode_log_data(),
        ),
        (
            B,
            ParentUpdated {
                parent: ROOT.parse()?,
                label: "alice".into(),
                sender: HOLDER.parse()?,
            }
            .encode_log_data(),
        ),
    ] {
        logs.push(raw(120, logs.len() as i64, emitter, data));
    }
    if reattach {
        for (index, data) in grant("leaf", OTHER, B_E)?.into_iter().enumerate() {
            logs.push(raw(121, index as i64, B, data));
        }
        logs.push(raw(
            122,
            0,
            ROOT,
            SubregistryUpdated {
                tokenId: token("alice"),
                subregistry: A.parse()?,
                sender: HOLDER.parse()?,
            }
            .encode_log_data(),
        ));
        if terminal {
            // Unregister while A is still live, after the reattachment; an expired entry
            // cannot call unregister. This is distinct from the time-only boundary case.
            logs.push(raw(
                123,
                0,
                A,
                LabelUnregistered {
                    tokenId: token("leaf"),
                    sender: HOLDER.parse()?,
                }
                .encode_log_data(),
            ));
        }
    } else {
        logs.push(raw(
            121,
            0,
            B,
            LabelReserved {
                tokenId: token("leaf"),
                labelHash: keccak256(b"leaf"),
                label: "leaf".into(),
                expiry: B_E as u64,
                sender: HOLDER.parse()?,
            }
            .encode_log_data(),
        ));
    }
    seed_v2_history_blocks(database, 119..=141).await?;
    // Seed only admitted declarations and immutable raw facts; Engine owns every normalized,
    // binding and searchable-name write, including same-block binding closures.
    v2_history_bounded_rebinding::persist_with_manifests(
        &database.pool,
        std::slice::from_ref(&manifest),
        &BatchOutput::default(),
    )
    .await?;
    for rule in rules {
        sqlx::query("INSERT INTO manifest_discovery_rules(manifest_id,edge_kind,from_role,admission) VALUES($1,$2,$3,$4)")
            .bind(rule.manifest_id).bind(rule.edge_kind).bind(rule.from_role).bind(rule.admission).execute(&database.pool).await?;
    }
    let root_instance = Uuid::from_u128(0x262_9000);
    for (index, address) in [ROOT, A, B].into_iter().enumerate() {
        let instance = Uuid::from_u128(0x262_9000 + index as u128);
        sqlx::query("INSERT INTO contract_instances(contract_instance_id,chain_id,contract_kind) VALUES($1,$2,'contract')")
            .bind(instance).bind(CHAIN).execute(&database.pool).await?;
        sqlx::query("INSERT INTO contract_instance_addresses(contract_instance_id,chain_id,address,active_from_block_number,source_manifest_id) VALUES($1,$2,$3,0,$4)")
            .bind(instance).bind(CHAIN).bind(address).bind(manifest.manifest_id).execute(&database.pool).await?;
        if index == 0 {
            sqlx::query("INSERT INTO manifest_contract_instances(manifest_id,chain_id,declaration_kind,declaration_name,contract_instance_id,declared_address,role,proxy_kind,start_block_number) VALUES($1,$2,'contract','registry',$3,$4,'registry','none',0)")
                .bind(manifest.manifest_id).bind(CHAIN).bind(instance).bind(address).execute(&database.pool).await?;
        } else {
            sqlx::query("INSERT INTO discovery_edges(chain_id,edge_kind,from_contract_instance_id,to_contract_instance_id,discovery_source,admission_basis,source_manifest_id,active_from_block_number,active_from_block_hash,canonicality_state,provenance)
                VALUES($1,'registry_announcement',$2,$3,'RegistryCreated','reachable_from_root',$4,119,'0xhistory119','canonical',jsonb_build_object('observation_key',$5::text))")
                .bind(CHAIN).bind(root_instance).bind(instance).bind(manifest.manifest_id).bind(format!("fixture-registry-{index}")).execute(&database.pool).await?;
        }
    }
    for log in logs {
        sqlx::query("INSERT INTO raw_transactions(chain_id,block_hash,block_number,transaction_hash,transaction_index,from_address,to_address) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING")
            .bind(CHAIN).bind(&log.block_hash).bind(log.block_number).bind(&log.transaction_hash).bind(log.transaction_index).bind(HOLDER).bind(&log.emitting_address).execute(&database.pool).await?;
        sqlx::query("INSERT INTO raw_logs(chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,emitting_address,topics,data) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)")
            .bind(CHAIN).bind(&log.block_hash).bind(log.block_number).bind(&log.transaction_hash).bind(log.transaction_index).bind(log.log_index).bind(&log.emitting_address).bind(&log.topics).bind(&log.data).execute(&database.pool).await?;
    }
    let outcome = bigname_interpret::Engine::new(database.pool.clone())
        .run_batch(bigname_interpret::BatchRequest {
            chain_id: CHAIN.into(),
            from_block: 120,
            to_block: 141,
            resume_current: None,
            mode: bigname_interpret::RunMode::Normal,
        })
        .await?;
    assert!(outcome.complete);
    let surfaces: Vec<String> =
        sqlx::query_scalar("SELECT coalesce(raw_name,logical_name_id) FROM name_surfaces")
            .fetch_all(&database.pool)
            .await?;
    assert!(
        surfaces.iter().any(|name| name == NAME),
        "produced surfaces: {surfaces:?}; outcome={outcome:?}"
    );
    Ok(())
}
async fn publish_at(database: &TestDatabase, block: i64) -> Result<()> {
    publish_mode(
        database,
        block,
        bigname_project::families::FamilyMode::Normal,
    )
    .await
}
async fn publish_mode(
    database: &TestDatabase,
    block: i64,
    mode: bigname_project::families::FamilyMode,
) -> Result<()> {
    let hash:String=sqlx::query_scalar("SELECT block_hash FROM chain_lineage WHERE chain_id=$1 AND block_number=$2 AND canonicality_state IN ('canonical','safe','finalized')").bind(CHAIN).bind(block).fetch_one(&database.pool).await?;
    // Keep these latest blocks canonical: this fixture later exercises a legal reorg.
    let time = bigname_storage::UnixSeconds::from(OffsetDateTime::from_unix_timestamp(
        1_700_000_000 + block,
    )?)
    .internal_string();
    seed_schema_v2_ens_lookup_head(&database.pool, block, &hash, &time).await?;
    sqlx::query("INSERT INTO chain_phase_state(chain_id,phase_name,phase_status,current_block_number,current_block_hash,target_block_number,target_block_hash,input_content_hash,started_at,finished_at) VALUES($1,'interpret','completed',$2,$3,$2,$3,$4,now(),now()) ON CONFLICT(chain_id,phase_name) DO UPDATE SET phase_status='completed',current_block_number=$2,current_block_hash=$3,target_block_number=$2,target_block_hash=$3,input_content_hash=$4,finished_at=now()")
        .bind(CHAIN).bind(block).bind(&hash).bind(bigname_content_hash::INTERPRETER_CONTENT_HASH).execute(&database.pool).await?;
    let input = bigname_project::families::input_token(&database.pool, CHAIN).await?;
    let outcome = bigname_project::families::apply(
        &database.pool,
        CHAIN,
        &bigname_project::Marker {
            number: block,
            hash,
        },
        mode,
        &input,
        &bigname_project::families::FamilyOptions::new(
            bigname_content_hash::INTERPRETER_CONTENT_HASH,
        ),
    )
    .await?;
    assert_eq!(outcome.marker.map(|marker| marker.number), Some(block));
    Ok(())
}
fn named(rows: &Value) -> Option<&Value> {
    rows.as_array()?.iter().find(|row| row["name"] == NAME)
}
fn coordinates(
    row: &Value,
    reattach: bool,
    terminal: bool,
    block: i64,
    includes_start: bool,
) -> Result<()> {
    let expiry = if reattach { E } else { B_E }.to_string();
    assert_eq!(row["expires_at"], expiry, "t={block}: {row}");
    assert_eq!(row["grace_ends_at"], expiry, "t={block}: {row}");
    assert_eq!(
        row["status"],
        if (reattach && (terminal || block >= 124)) || (!reattach && block >= 140) {
            "released"
        } else {
            "active"
        },
        "{row}"
    );
    if reattach && includes_start {
        assert_eq!(row["registered_at"], "1700000120", "{row}");
    } else if !reattach {
        for key in ["registration_id", "registered_at", "lapsed_registration"] {
            assert!(row.get(key).is_none(), "replacement {key}: {row}");
        }
    }
    Ok(())
}
#[tokio::test]
async fn canonical_association_public_routes_keep_instance_coordinates() -> Result<()> {
    for (reattach, terminal) in [(false, false), (true, false), (true, true)] {
        let database = TestDatabase::new_migrated().await?;
        fixture(&database, reattach, terminal).await?;
        let mut handle = None;
        for block in [123, 124, 125, 126, 139, 140, 141] {
            publish_at(&database, block).await?;
            let detail = v2_names_payload(&database, &format!("/v1/names/{NAME}")).await?;
            coordinates(&detail["data"], reattach, terminal, block, true)?;
            assert_eq!(detail["data"]["read_status"], "ok");
            let logical = bigname_storage::logical_name_id_for_name("ens", NAME);
            let composed =
                bigname_storage::families::name::load_family_name(&database.pool, &logical)
                    .await?
                    .context("composed child")?;
            let prepared: (bool, bool, Value) = sqlx::query_as("SELECT expiry_listable,search_supported,search_fields FROM project_name_summary WHERE logical_name_id=$1")
                .bind(&logical).fetch_one(&database.pool).await?;
            assert!(
                prepared.0 && prepared.1,
                "prepared eligibility: {prepared:?}"
            );
            assert_eq!(
                prepared.2["expires_at"],
                if reattach { E } else { B_E }.to_string()
            );
            if !reattach && block == 123 {
                assert_eq!(composed.coverage["status"], "unsupported");
                assert_eq!(
                    composed.coverage["unsupported_reason"],
                    "current_authority_not_projected"
                );
                assert!(composed.surface_binding_id.is_none());
                let filter = bigname_storage::NameCurrentExpiringFilter {
                    deadline: bigname_storage::NameCurrentDeadline::Expiry,
                    namespace: "ens".into(),
                    windows: vec![bigname_storage::NameCurrentExpiryWindow {
                        expires_after: Some(B_E.to_string().parse()?),
                        expires_before: Some((B_E + 1).to_string().parse()?),
                    }],
                    authorities: None,
                    parent: None,
                };
                let page = bigname_storage::families::name::load_family_expiring_page(
                    &database.pool,
                    &filter,
                    bigname_storage::NameCurrentListOrder::Asc,
                    None,
                    10,
                    &[CHAIN.into()],
                )
                .await?;
                let listed = page
                    .rows
                    .iter()
                    .find(|row| row.row.logical_name_id == logical)
                    .context("composed deadline row")?;
                for key in ["status", "unsupported_reason"] {
                    assert_eq!(listed.row.coverage[key], composed.coverage[key]);
                }
            }
            if reattach {
                assert!(detail["data"]["registration_id"].is_string(), "{detail}");
                if let Some(ref id) = handle {
                    assert_eq!(&detail["data"]["registration_id"], id);
                } else {
                    handle = Some(detail["data"]["registration_id"].clone());
                }
            }
            let expiry = if reattach { E } else { B_E };
            for uri in [
                format!(
                    "/v1/names?namespace=ens&expires_after={expiry}&expires_before={}",
                    expiry + 1
                ),
                format!(
                    "/v1/names?namespace=ens&grace_ends_after={expiry}&grace_ends_before={}",
                    expiry + 1
                ),
                format!(
                    "/v1/names?namespace=ens&expires_window={expiry}..{}",
                    expiry + 1
                ),
                format!(
                    "/v1/names?namespace=ens&grace_ends_window={expiry}..{}",
                    expiry + 1
                ),
                "/v1/search?q=leaf&match=prefix".into(),
            ] {
                let body = v2_names_payload(&database, &uri).await?;
                let row = named(&body["data"]).with_context(|| format!("{uri}: {body}"))?;
                coordinates(row, reattach, terminal, block, true)?;
            }
            for profile in ["feed", "detail"] {
                let lookup = v2_lookup_json(
                    &database,
                    json!({"profile":profile,"inputs":[{"name":NAME}]}),
                )
                .await?;
                assert_eq!(lookup["data"][0]["record"]["read_status"], "ok");
                let record = &lookup["data"][0]["record"];
                coordinates(record, reattach, terminal, block, profile == "detail")?;
                if reattach && profile == "detail" {
                    assert_eq!(
                        record["registration_id"], detail["data"]["registration_id"],
                        "{lookup}"
                    );
                }
            }
            let former = v2_names_payload(
                &database,
                &format!("/v1/addresses/{HOLDER}/names?namespace=ens&relation=former_owner"),
            )
            .await?;
            assert_eq!(
                named(&former["data"]).is_some(),
                reattach && (terminal || block >= 124),
                "{former}"
            );
            if !reattach || terminal || block >= 124 {
                assert!(detail["data"].get("owner").is_none(), "{detail}");
                assert!(detail["data"].get("manager").is_none(), "{detail}");
            }
        }
        let uri = format!("/v1/names/{NAME}");
        let before = v2_names_payload(&database, &uri).await?["data"].clone();
        for mode in [
            bigname_project::families::FamilyMode::Rebuild,
            bigname_project::families::FamilyMode::Redo { from: 123, to: 141 },
        ] {
            publish_mode(&database, 141, mode).await?;
            assert_eq!(v2_names_payload(&database, &uri).await?["data"], before);
            let search = v2_names_payload(&database, "/v1/search?q=leaf&match=prefix").await?;
            coordinates(
                named(&search["data"]).context("recovered discovery")?,
                reattach,
                terminal,
                141,
                true,
            )?;
        }
        if !reattach {
            // Retracting the only raw allocation witnesses removes internal eligibility too.
            sqlx::query("UPDATE chain_heads SET latest_block_hash='0xhistory119',latest_block_number=119,safe_block_hash=NULL,safe_block_number=NULL,finalized_block_hash=NULL,finalized_block_number=NULL WHERE chain_id=$1").bind(CHAIN).execute(&database.pool).await?;
            sqlx::query("UPDATE chain_lineage SET canonicality_state='orphaned' WHERE chain_id=$1 AND block_number BETWEEN 120 AND 141").bind(CHAIN).execute(&database.pool).await?;
            sqlx::query("INSERT INTO chain_lineage(chain_id,block_hash,parent_hash,block_number,block_timestamp,canonicality_state) SELECT $1,'0xfork'||height,CASE WHEN height=120 THEN '0xhistory119' ELSE '0xfork'||(height-1) END,height,to_timestamp(1700000000+height),'canonical' FROM generate_series(120,141)height").bind(CHAIN).execute(&database.pool).await?;
            let outcome = bigname_interpret::Engine::new(database.pool.clone())
                .run_batch(bigname_interpret::BatchRequest {
                    chain_id: CHAIN.into(),
                    from_block: 120,
                    to_block: 141,
                    resume_current: None,
                    mode: bigname_interpret::RunMode::Redo,
                })
                .await?;
            assert!(outcome.complete);
            publish_mode(
                &database,
                141,
                bigname_project::families::FamilyMode::Redo { from: 120, to: 141 },
            )
            .await?;
            let logical = bigname_storage::logical_name_id_for_name("ens", NAME);
            assert!(
                bigname_storage::families::name::load_family_name(&database.pool, &logical)
                    .await?
                    .is_none()
            );
            let eligible:i64=sqlx::query_scalar("SELECT count(*) FROM project_name_summary WHERE logical_name_id=$1 AND (search_supported OR expiry_listable)").bind(&logical).fetch_one(&database.pool).await?;
            assert_eq!(eligible, 0);
            let search = v2_names_payload(&database, "/v1/search?q=leaf&match=prefix").await?;
            assert!(named(&search["data"]).is_none());
        }
        database.cleanup().await?;
    }
    Ok(())
}
