//! Actual admitted ENSv1 logs pass through Interpret, Project and public readers. Structural
//! child hashes follow NewOwner's parent/label pair; no fixture inserts an identity or binding.
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f)
use super::compatibility::{admit_family_from, role_address};
use super::*;
use alloy_primitives::B256;

mod events {
    use super::*;
    sol! {
        event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
        event Transfer(bytes32 indexed node, address owner);
        event NewResolver(bytes32 indexed node, address resolver);
        event AddressChanged(bytes32 indexed node, uint256 coinType, bytes newAddress);
        event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);
    }
}

fn node(labels: &[&[u8]]) -> B256 {
    labels.iter().rev().fold(B256::ZERO, |node, label| {
        keccak256([node.as_slice(), keccak256(label).as_slice()].concat())
    })
}

fn spelling(labels: &[&[u8]]) -> String {
    labels
        .iter()
        .map(|label| format!("[{:x}]", keccak256(label)))
        .collect::<Vec<_>>()
        .join(".")
}

fn tl_path(name: &str) -> String {
    name.replace('[', "%5B").replace(']', "%5D")
}

fn log(data: alloy_primitives::LogData, emitter: Address, block: i64, index: i64) -> RawLogInput {
    let mut row = raw(data, block, index);
    row.emitting_address = format!("{emitter:#x}");
    row
}

fn owner(
    parent: B256,
    label: &[u8],
    owner: Address,
    emitter: Address,
    block: i64,
    index: i64,
) -> RawLogInput {
    log(
        events::NewOwner {
            node: parent,
            label: keccak256(label),
            owner,
        }
        .encode_log_data(),
        emitter,
        block,
        index,
    )
}

fn wrapped(labels: &[&[u8]], emitter: Address, block: i64, index: i64) -> RawLogInput {
    let mut bytes = Vec::new();
    for label in labels {
        bytes.push(label.len() as u8);
        bytes.extend_from_slice(label);
    }
    bytes.push(0);
    log(
        events::NameWrapped {
            node: node(labels),
            name: bytes.into(),
            owner: HOLDER.parse().unwrap(),
            fuses: 0,
            expiry: 1_900_000_000,
        }
        .encode_log_data(),
        emitter,
        block,
        index,
    )
}

async fn intake(database: &TestDatabase, logs: &[RawLogInput], end: i64) -> Result<()> {
    let blocks = (120..=end)
        .map(|n| raw_block(CHAIN, &format!("0xhistory{n}"), None, n, 1_700_000_000 + n))
        .collect::<Vec<_>>();
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    seed_schema_v2_lookup_head(
        &database.pool,
        CHAIN,
        end,
        &format!("0xhistory{end}"),
        &bigname_storage::UnixSeconds::from(timestamp(1_700_000_000 + end)).internal_string(),
    )
    .await?;
    store_logs(database, logs).await
}

async fn store_logs(database: &TestDatabase, logs: &[RawLogInput]) -> Result<()> {
    for row in logs {
        sqlx::query("INSERT INTO raw_transactions(chain_id,block_hash,block_number,transaction_hash,transaction_index,from_address,to_address) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING")
            .bind(CHAIN).bind(&row.block_hash).bind(row.block_number).bind(&row.transaction_hash).bind(row.transaction_index).bind(HOLDER).bind(&row.emitting_address).execute(&database.pool).await?;
        sqlx::query("INSERT INTO raw_logs(chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,emitting_address,topics,data) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)")
            .bind(CHAIN).bind(&row.block_hash).bind(row.block_number).bind(&row.transaction_hash).bind(row.transaction_index).bind(row.log_index).bind(&row.emitting_address).bind(&row.topics).bind(&row.data).execute(&database.pool).await?;
    }
    Ok(())
}

async fn run(
    engine: &bigname_interpret::Engine,
    from: i64,
    to: i64,
    mode: bigname_interpret::RunMode,
) -> Result<()> {
    let outcome = engine
        .run_batch(bigname_interpret::BatchRequest {
            chain_id: CHAIN.into(),
            from_block: from,
            to_block: to,
            resume_current: None,
            mode,
        })
        .await?;
    assert!(
        outcome.complete,
        "small fixture must finish its actual range"
    );
    Ok(())
}

async fn canonical_hash(database: &TestDatabase, block: i64) -> Result<String> {
    Ok(sqlx::query_scalar("SELECT block_hash FROM chain_lineage WHERE chain_id=$1 AND block_number=$2 AND canonicality_state IN ('canonical','safe','finalized')")
        .bind(CHAIN).bind(block).fetch_one(&database.pool).await?)
}

// Preserve the old raw logs on orphaned lineage and replace the complete suffix with a real
// canonical branch. Interpret must exclude those logs itself, then rederive surviving input.
async fn replace_branch(
    database: &TestDatabase,
    from: i64,
    head: i64,
    generation: i64,
    logs: &[RawLogInput],
) -> Result<()> {
    let mut parent = canonical_hash(database, from - 1).await?;
    // Move the retained head to the common ancestor before orphaning the old suffix.
    sqlx::query(
        "UPDATE chain_heads SET latest_block_hash=$2,latest_block_number=$3 WHERE chain_id=$1",
    )
    .bind(CHAIN)
    .bind(&parent)
    .bind(from - 1)
    .execute(&database.pool)
    .await?;
    sqlx::query("UPDATE chain_lineage SET canonicality_state='orphaned' WHERE chain_id=$1 AND block_number >= $2")
        .bind(CHAIN).bind(from).execute(&database.pool).await?;
    for block in from..=head {
        let hash = format!("0xfork{generation}-{block}");
        upsert_phase_raw_blocks(
            &database.pool,
            &[raw_block(
                CHAIN,
                &hash,
                Some(&parent),
                block,
                1_700_000_000 + block,
            )],
        )
        .await?;
        parent = hash;
    }
    let mut replacement = logs.to_vec();
    for row in &mut replacement {
        row.block_hash = format!("0xfork{generation}-{}", row.block_number);
    }
    store_logs(database, &replacement).await?;
    seed_schema_v2_lookup_head(
        &database.pool,
        CHAIN,
        head,
        &parent,
        &bigname_storage::UnixSeconds::from(timestamp(1_700_000_000 + head)).internal_string(),
    )
    .await?;
    Ok(())
}

async fn publish(database: &TestDatabase, block: i64) -> Result<()> {
    let hash = canonical_hash(database, block).await?;
    publish_test_families_on(&database.pool, CHAIN, block).await?;
    // Keep the published head canonical so later tests can replace it by an ordinary reorg.
    // The generic selector seeder finalizes its head, which must never be orphaned.
    seed_schema_v2_lookup_head(
        &database.pool,
        CHAIN,
        block,
        &hash,
        &bigname_storage::UnixSeconds::from(timestamp(1_700_000_000 + block)).internal_string(),
    )
    .await?;
    sqlx::query("INSERT INTO chain_phase_state (chain_id,phase_name,phase_status,current_block_number,current_block_hash,target_block_number,target_block_hash,input_content_hash,started_at,finished_at) VALUES ($1,'interpret','completed',$2,$3,$2,$3,$4,now(),now()) ON CONFLICT (chain_id,phase_name) DO UPDATE SET phase_status='completed',current_block_number=$2,current_block_hash=$3,target_block_number=$2,target_block_hash=$3,input_content_hash=$4,finished_at=now()")
        .bind(CHAIN).bind(block).bind(hash).bind(bigname_content_hash::INTERPRETER_CONTENT_HASH).execute(&database.pool).await?;
    Ok(())
}

async fn redo(database: &TestDatabase, from: i64, to: i64, head: i64) -> Result<()> {
    database
        .simulate_interpret_redo_begin(CHAIN, "redo")
        .await?;
    run(
        &bigname_interpret::Engine::new(database.pool.clone()),
        from,
        to,
        bigname_interpret::RunMode::Redo,
    )
    .await?;
    database.simulate_interpret_redo_finish(CHAIN).await?;
    // Each distinct runner redo has a new Project attempt; reusing one would deliberately
    // resume its already-completed repair instead of replaying the changed fixture inputs.
    sqlx::query("UPDATE chain_phase_state SET redo_attempt_generation=redo_attempt_generation+1 WHERE chain_id=$1 AND phase_name='project'")
        .bind(CHAIN).execute(&database.pool).await?;
    let token = bigname_project::families::input_token(&database.pool, CHAIN).await?;
    bigname_project::families::apply(
        &database.pool,
        CHAIN,
        &bigname_project::Marker {
            number: head,
            hash: canonical_hash(database, head).await?,
        },
        bigname_project::families::FamilyMode::Redo { from, to: head },
        &token,
        &bigname_project::families::FamilyOptions::new(
            bigname_content_hash::INTERPRETER_CONTENT_HASH,
        ),
    )
    .await?;
    publish(database, head).await
}

async fn get(database: &TestDatabase, uri: &str) -> Result<Value> {
    let (status, body) = read_family_response(database, uri).await?;
    assert_eq!(status, StatusCode::OK, "{uri}: {body:#}");
    Ok(body)
}

async fn detail(database: &TestDatabase, labels: &[&[u8]]) -> Result<Value> {
    Ok(get(
        database,
        &format!("/v1/names/{}", tl_path(&spelling(labels))),
    )
    .await?["data"]
        .clone())
}

#[tokio::test]
async fn produced_node_names_agree_across_ranges_restarts_redo_and_public_routes() -> Result<()> {
    let mut expected = None;
    // One Interpret range, warm per-block continuation, and cold per-block restore.
    for shape in ["range", "warm", "cold"] {
        let database = TestDatabase::new_migrated().await?;
        let registry =
            admit_family_from(&database, "mainnet", CHAIN, "ens_v1_registry_l1", 2281).await?;
        let mut resolvers =
            admit_family_from(&database, "mainnet", CHAIN, "ens_v1_resolver_l1", 2282).await?;
        // The admission helper moves intake bounds to this fixture's block range. Project's
        // resolver classifier reads declaration bounds from the manifest-sync payload as well.
        for contract in resolvers["contracts"].as_array_mut().unwrap() {
            contract["start_block"] = json!(0);
        }
        sqlx::query("UPDATE manifest_versions SET manifest_payload=$2 WHERE manifest_id=$1")
            .bind(2282_i64)
            .bind(&resolvers)
            .execute(&database.pool)
            .await?;
        seed_fixture_manifest_update(
            &database.pool,
            2282,
            CHAIN,
            "ens",
            "ens_v1_resolver_l1",
            &resolvers,
        )
        .await?;
        let current = role_address(&registry, "registry");
        let old = role_address(&registry, "registry_old");
        let resolver = role_address(&resolvers, "public_resolver");
        let unrelated = role_address(&resolvers, "public_resolver_231b0ee");
        let holder = HOLDER.parse()?;
        let child = node(&[b"child", b"unknown", b"eth"]);
        let logs = vec![
            owner(B256::ZERO, b"eth", holder, old, 120, 0),
            owner(node(&[b"eth"]), b"unknown", holder, old, 121, 0),
            owner(node(&[b"unknown", b"eth"]), b"child", holder, old, 122, 0),
            log(
                events::NewResolver {
                    node: child,
                    resolver,
                }
                .encode_log_data(),
                old,
                123,
                0,
            ),
            log(
                events::AddressChanged {
                    node: child,
                    coinType: U256::from(60),
                    newAddress: GRANTEE.parse::<Address>()?.as_slice().to_vec().into(),
                }
                .encode_log_data(),
                resolver,
                123,
                1,
            ),
            log(
                events::AddressChanged {
                    node: child,
                    coinType: U256::from(60),
                    newAddress: HOLDER.parse::<Address>()?.as_slice().to_vec().into(),
                }
                .encode_log_data(),
                unrelated,
                123,
                2,
            ),
            owner(
                node(&[b"unknown", b"eth"]),
                b"child",
                holder,
                current,
                124,
                0,
            ),
            log(
                events::NewResolver {
                    node: child,
                    resolver,
                }
                .encode_log_data(),
                current,
                124,
                1,
            ),
            log(
                events::AddressChanged {
                    node: child,
                    coinType: U256::from(60),
                    newAddress: GRANTEE.parse::<Address>()?.as_slice().to_vec().into(),
                }
                .encode_log_data(),
                resolver,
                124,
                2,
            ),
            log(
                events::AddressChanged {
                    node: child,
                    coinType: U256::from(60),
                    newAddress: HOLDER.parse::<Address>()?.as_slice().to_vec().into(),
                }
                .encode_log_data(),
                unrelated,
                124,
                3,
            ),
            log(
                events::Transfer {
                    node: child,
                    owner: Address::ZERO,
                }
                .encode_log_data(),
                current,
                125,
                0,
            ),
            log(
                events::Transfer {
                    node: child,
                    owner: GRANTEE.parse()?,
                }
                .encode_log_data(),
                current,
                126,
                0,
            ),
        ];
        intake(&database, &logs, 126).await?;
        let engine = bigname_interpret::Engine::new(database.pool.clone());
        if shape == "range" {
            run(&engine, 120, 124, bigname_interpret::RunMode::Normal).await?;
        } else {
            for block in 120..=124 {
                if shape == "cold" {
                    run(
                        &bigname_interpret::Engine::new(database.pool.clone()),
                        block,
                        block,
                        bigname_interpret::RunMode::Normal,
                    )
                    .await?;
                } else {
                    run(&engine, block, block, bigname_interpret::RunMode::Normal).await?;
                }
            }
        }
        publish(&database, 122).await?;
        let earlier = detail(&database, &[b"child", b"unknown", b"eth"]).await?;
        assert_eq!(earlier["authority"], "ens_v0", "{earlier:#}");
        assert!(earlier.get("resolver").is_none());
        publish(&database, 124).await?;
        let labels = [b"child".as_slice(), b"unknown", b"eth"];
        let name = spelling(&labels);
        let row = detail(&database, &labels).await?;
        assert_eq!(row["registration_status"], "registered", "{row:#}");
        assert_eq!(row["owner"], HOLDER);
        assert_eq!(row["authority"], "ens_v1");
        assert_eq!(row["created_at"], "1700000122");
        let records = get(
            &database,
            &format!(
                "/v1/names/{}/records?source=indexed&keys=addr:60&include=inventory",
                tl_path(&name)
            ),
        )
        .await?;
        assert_eq!(
            row["records"]["addresses"]["60"], GRANTEE,
            "pointer selects only its own resolver: {row:#}; records: {records:#}"
        );
        assert!(row.get("registered_at").is_none());
        assert!(row.get("expires_at").is_none());
        assert!(row.get("registration_id").is_some());
        let stored: (Option<String>, Vec<String>, i64) = sqlx::query_as(
            "SELECT raw_name,labelhashes,block_number FROM name_surfaces WHERE logical_name_id=$1",
        )
        .bind(format!("ens:{child:#x}"))
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(stored.0, None);
        assert_eq!(stored.1.len(), 3);
        assert_eq!(stored.2, 122);
        let expiry_listing = get(
            &database,
            "/v1/names?namespace=ens&expires_before=2035-01-01T00:00:00Z&page_size=100",
        )
        .await?;
        assert!(
            !expiry_listing.to_string().contains(&name),
            "a structural child has no invented expiry: {expiry_listing:#}"
        );
        let mut views = vec![row.clone(), expiry_listing["data"].clone()];
        for uri in [
            format!(
                "/v1/names/{}/subnames?include=counts",
                tl_path(&spelling(&[b"unknown", b"eth"]))
            ),
            format!("/v1/addresses/{HOLDER}/names?namespace=ens&relation=owner&page_size=100"),
            format!("/v1/resolvers/1/{resolver:#x}?page_size=100"),
        ] {
            let body = get(&database, &uri).await?;
            assert!(body.to_string().contains(&name), "{uri}: {body:#}");
            views.push(body["data"].clone());
        }
        for scope in ["name", "registration", "both"] {
            let body = get(
                &database,
                &format!(
                    "/v1/names/{}/history?scope={scope}&include=data&page_size=100",
                    tl_path(&name)
                ),
            )
            .await?;
            assert!(!body["data"].as_array().unwrap().is_empty());
            views.push(body["data"].clone());
        }
        let search = get(
            &database,
            &format!("/v1/search?q={:x}&match=contains", keccak256(b"child")),
        )
        .await?;
        assert!(search.to_string().contains(&name), "{search:#}");
        assert_search_lean_pages_match(&database, &lean_filter("ens", "a"), 1).await?;
        let response = v2_lookup_response_for_database_with_public_namespaces(
            &database,
            "/v1/lookup",
            json!({"profile":"detail","inputs":[{"name":name}]}),
            &["ens"],
        )
        .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let lookup: Value = read_json(response).await?;
        assert_eq!(
            lookup["data"][0]["record"]["registration_status"],
            "registered"
        );
        if let Some(expected) = &expected {
            assert_eq!(&views, expected, "{shape}");
        } else {
            expected = Some(views);
        }
        // Genuine bounded/full Interpret+Project redo preserves the completed public row.
        for (from, to) in [(124, 124), (120, 124)] {
            redo(&database, from, to, 124).await?;
            assert_eq!(
                detail(&database, &labels).await?,
                row,
                "{shape}, redo {from}..{to}"
            );
        }
        run(
            &bigname_interpret::Engine::new(database.pool.clone()),
            125,
            125,
            bigname_interpret::RunMode::Normal,
        )
        .await?;
        publish(&database, 125).await?;
        let cleared = detail(&database, &labels).await?;
        assert_eq!(
            cleared["registration_status"], "unregistered",
            "{cleared:#}"
        );
        assert!(cleared.get("owner").is_none());
        assert_eq!(cleared["resolver"]["address"], format!("{resolver:#x}"));
        run(
            &bigname_interpret::Engine::new(database.pool.clone()),
            126,
            126,
            bigname_interpret::RunMode::Normal,
        )
        .await?;
        publish(&database, 126).await?;
        let moved = detail(&database, &labels).await?;
        assert_eq!(moved["registration_status"], "registered");
        assert_eq!(moved["owner"], GRANTEE);
        // Importing spellings changes presentation only, through the ordinary reader path.
        for label in labels {
            insert_family_label_preimage(&database.pool, label).await?;
        }
        let enriched = detail(&database, &labels).await?;
        assert_eq!(enriched["name"], "child.unknown.eth");
        for field in [
            "created_at",
            "registration_id",
            "owner",
            "manager",
            "registration_status",
        ] {
            assert_eq!(enriched[field], moved[field], "{field}");
        }
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn produced_byte_shadows_survive_reobservation_and_release_only_with_the_last_witness()
-> Result<()> {
    for label in [b"Invalid".as_slice(), &[0xff], b"nul\0"] {
        for bytes_first in [false, true] {
            let database = TestDatabase::new_migrated().await?;
            let registry =
                admit_family_from(&database, "mainnet", CHAIN, "ens_v1_registry_l1", 2281).await?;
            let wrapper =
                admit_family_from(&database, "mainnet", CHAIN, "ens_v1_wrapper_l1", 2282).await?;
            let registry = role_address(&registry, "registry");
            let wrapper = role_address(&wrapper, "name_wrapper");
            let labels = [label, b"eth".as_slice()];
            let child = node(&labels);
            let mut logs = vec![owner(B256::ZERO, b"eth", HOLDER.parse()?, registry, 120, 0)];
            let byte_block = if bytes_first { 121 } else { 122 };
            let node_block = if bytes_first { 122 } else { 121 };
            logs.extend([
                owner(
                    node(&[b"eth"]),
                    label,
                    HOLDER.parse()?,
                    registry,
                    node_block,
                    0,
                ),
                wrapped(&labels, wrapper, byte_block, 0),
                wrapped(&labels, wrapper, 123, 0),
                owner(node(&[b"eth"]), label, HOLDER.parse()?, registry, 124, 0),
            ]);
            logs.sort_by_key(|row| (row.block_number, row.log_index));
            intake(&database, &logs, 124).await?;
            for block in 120..=124 {
                run(
                    &bigname_interpret::Engine::new(database.pool.clone()),
                    block,
                    block,
                    bigname_interpret::RunMode::Normal,
                )
                .await?;
            }
            publish(&database, 124).await?;
            let surface: (String,Vec<String>,Option<String>,Option<time::OffsetDateTime>)=sqlx::query_as("SELECT visibility_state,labelhashes,preimage_event_identity,deactivated_at FROM name_surfaces WHERE logical_name_id=$1")
                .bind(format!("ens:{child:#x}")).fetch_one(&database.pool).await?;
            assert_eq!(surface.0, "shadow");
            assert_eq!(surface.1.len(), 2);
            assert!(surface.2.is_some());
            assert_eq!(surface.3, Some(timestamp(1_700_000_000 + byte_block)));
            let uri = format!("/v1/names/{}", tl_path(&spelling(&labels)));
            assert_eq!(
                read_family_response(&database, &uri).await?.0,
                StatusCode::NOT_FOUND
            );
            // Redo has to release the actual witness, retain the later one, and use its timestamp.
            sqlx::query("DELETE FROM raw_logs WHERE block_number=$1 AND emitting_address=$2")
                .bind(byte_block)
                .bind(format!("{wrapper:#x}"))
                .execute(&database.pool)
                .await?;
            redo(&database, byte_block, byte_block, 124).await?;
            let after: (String,Option<time::OffsetDateTime>)=sqlx::query_as("SELECT visibility_state,deactivated_at FROM name_surfaces WHERE logical_name_id=$1").bind(format!("ens:{child:#x}")).fetch_one(&database.pool).await?;
            assert_eq!(after, ("shadow".into(), Some(timestamp(1_700_000_123))));
            sqlx::query("DELETE FROM raw_logs WHERE block_number=123 AND emitting_address=$1")
                .bind(format!("{wrapper:#x}"))
                .execute(&database.pool)
                .await?;
            redo(&database, 123, 123, 124).await?;
            let after: (String,Option<String>,Option<String>)=sqlx::query_as("SELECT visibility_state,raw_name,preimage_event_identity FROM name_surfaces WHERE logical_name_id=$1").bind(format!("ens:{child:#x}")).fetch_one(&database.pool).await?;
            assert_eq!(after, ("active".into(), None, None));
            assert_eq!(
                get(&database, &uri).await?["data"]["created_at"],
                format!("{}", 1_700_000_000 + node_block)
            );
            database.cleanup().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn produced_identity_enriches_only_at_publication_and_reanchors_after_structural_reorg()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let registry =
        admit_family_from(&database, "mainnet", CHAIN, "ens_v1_registry_l1", 2281).await?;
    let wrapper = admit_family_from(&database, "mainnet", CHAIN, "ens_v1_wrapper_l1", 2282).await?;
    let registry = role_address(&registry, "registry");
    let wrapper = role_address(&wrapper, "name_wrapper");
    let labels = [b"wrappedchild".as_slice(), b"eth"];
    let logs = vec![
        owner(B256::ZERO, b"eth", HOLDER.parse()?, registry, 120, 0),
        owner(node(&[b"eth"]), labels[0], wrapper, registry, 121, 0),
        wrapped(&labels, wrapper, 122, 0),
    ];
    intake(&database, &logs, 122).await?;
    let engine = bigname_interpret::Engine::new(database.pool.clone());
    run(&engine, 120, 121, bigname_interpret::RunMode::Normal).await?;
    publish(&database, 121).await?;
    let before = detail(&database, &labels).await?;
    assert_eq!(before["name"], spelling(&labels));
    assert_eq!(before["created_at"], "1700000121");
    run(&engine, 122, 122, bigname_interpret::RunMode::Normal).await?;
    // Interpret has observed bytes, while the compatible Project publication is still 121.
    // The reader may hold the earlier spelling or refuse stale data; it cannot expose new custody.
    let (status, held) = read_family_response(
        &database,
        &format!("/v1/names/{}", tl_path(&spelling(&labels))),
    )
    .await?;
    if status == StatusCode::OK {
        assert_ne!(held["data"]["registration_status"], "wrapped", "{held:#}");
    } else {
        assert_eq!(status, StatusCode::CONFLICT, "{held:#}");
    }
    publish(&database, 122).await?;
    let after = detail(&database, &labels).await?;
    assert_eq!(after["name"], "wrappedchild.eth");
    assert_eq!(after["created_at"], before["created_at"]);
    assert_eq!(after["registration_status"], "wrapped");
    assert_eq!(after["owner"], HOLDER);
    assert_search_lean_pages_match(&database, &lean_filter("ens", "eth"), 1).await?;
    replace_branch(&database, 121, 122, 1, &logs[2..]).await?;
    redo(&database, 121, 122, 122).await?;
    let reanchored = detail(&database, &labels).await?;
    assert_eq!(reanchored["created_at"], "1700000122");
    assert_eq!(reanchored["registration_status"], "wrapped");
    replace_branch(&database, 122, 122, 2, &[]).await?;
    redo(&database, 122, 122, 122).await?;
    let retained_orphans: i64 = sqlx::query_scalar("SELECT count(*) FROM raw_logs raw JOIN chain_lineage lineage USING(chain_id,block_hash) WHERE lineage.canonicality_state='orphaned'").fetch_one(&database.pool).await?;
    assert_eq!(
        retained_orphans, 3,
        "old structural and both old byte logs remain stored"
    );
    assert_eq!(
        read_family_response(&database, "/v1/names/wrappedchild.eth")
            .await?
            .0,
        StatusCode::NOT_FOUND
    );
    database.cleanup().await?;
    Ok(())
}

mod recovery {
    use super::*;
    sol! {
        event RegistryCreated();
        event RawParentUpdated(address indexed parent, bytes label, address indexed sender);
    }
}

// Synthetic repetitions of a known reachable old-registry unmasked-owner event shape
// leave lexical candidates without projected authority. Put them between two supported
// children so the smaller search batch must keep walking; these are not 32 historical logs.
#[tokio::test]
async fn produced_search_lean_walks_an_unsupported_interval() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let manifest =
        admit_family_from(&database, "mainnet", CHAIN, "ens_v1_registry_l1", 2281).await?;
    let registry = role_address(&manifest, "registry");
    let old_registry = role_address(&manifest, "registry_old");
    let unmasked_owner = alloy_primitives::hex::decode(
        "0x6330363834636235336331363831343865616130313363333864316330663339",
    )?;
    let holder = HOLDER.parse()?;
    let mut logs = vec![owner(B256::ZERO, b"eth", holder, registry, 120, 0)];
    let mut labels = vec!["eth".to_owned(), "lean000".to_owned(), "lean999".to_owned()];
    for index in 0..32 {
        let label = format!("lean1{index:02}");
        let mut observation = owner(
            B256::ZERO,
            label.as_bytes(),
            holder,
            old_registry,
            120,
            index + 1,
        );
        observation.data = unmasked_owner.clone();
        logs.push(observation);
        labels.push(label);
    }
    for (index, label) in [b"lean000", b"lean999"].iter().enumerate() {
        logs.push(owner(
            node(&[b"eth"]),
            *label,
            holder,
            registry,
            121,
            index as i64,
        ));
    }
    intake(&database, &logs, 121).await?;
    run(
        &bigname_interpret::Engine::new(database.pool.clone()),
        120,
        121,
        bigname_interpret::RunMode::Normal,
    )
    .await?;
    publish(&database, 121).await?;
    for label in &labels {
        insert_family_label_preimage(&database.pool, label.as_bytes()).await?;
    }
    for index in 0..32 {
        let label = format!("lean1{index:02}");
        let namehash = format!("{:#x}", node(&[label.as_bytes()]));
        let logical = format!("ens:{namehash}");
        // Unmasked authority has no logical identity; the sibling SubregistryChanged
        // observation carries the structural identity for this raw NewOwner event.
        let unmasked: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM normalized_events WHERE chain_id=$1
             AND namespace='ens' AND source_family='ens_v1_registry_l1'
             AND event_kind='AuthorityTransferred'
             AND after_state->>'child_node'=$2
             AND after_state @> '{\"source_event\":\"NewOwner\",\"emitter_role\":\"registry_old\",\"owner_word_unmasked\":true}'
             AND logical_name_id IS NULL AND resource_id IS NULL",
        )
        .bind(CHAIN)
        .bind(&namehash)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(unmasked, 1, "{label}: admitted unmasked owner observation");
        let bindings: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM project_binding_candidate WHERE logical_name_id=$1",
        )
        .bind(&logical)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(bindings, 0, "{label}: no projected binding candidate");
        let tld = bigname_storage::families::name::load_family_name(&database.pool, &logical)
            .await?
            .context("the old-registry observation must compose")?;
        assert!(tld.surface_binding_id.is_none(), "{label}: {tld:?}");
        assert!(
            tld.provenance["authority_selection"]["authority_arm"].is_null(),
            "{label}: {tld:?}"
        );
        assert_eq!(tld.coverage["status"], "unsupported", "{label}: {tld:?}");
        assert_eq!(
            tld.coverage["unsupported_reason"], "current_authority_not_projected",
            "{label}: {tld:?}"
        );
    }
    let filter = lean_filter("ens", "lean");
    let rows = assert_search_lean_pages_match(&database, &filter, 1).await?;
    assert_eq!(
        rows.iter()
            .map(|row| row["name"].clone())
            .collect::<Vec<_>>(),
        [json!("lean000.eth"), json!("lean999.eth")]
    );
    let pages = read_family_pages(&database, "/v1/search?q=lean&namespace=ens&page_size=1").await?;
    assert_eq!(pages.len(), 2);
    assert_eq!(
        pages
            .iter()
            .flat_map(|page| page["data"].as_array().unwrap().iter().cloned())
            .collect::<Vec<_>>(),
        rows
    );
    database.cleanup().await
}

#[tokio::test]
async fn recovered_parent_updated_before_registry_created_keeps_earliest_same_block_witness()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let manifest =
        admit_family_from(&database, "sepolia", CHAIN, "ens_v2_registry_l1", 2284).await?;
    for rule in manifest["discovery_rules"].as_array().unwrap() {
        sqlx::query("INSERT INTO manifest_discovery_rules(manifest_id,edge_kind,from_role,admission,rule_payload) VALUES (2284,$1,$2,$3,$4)")
            .bind(rule["edge_kind"].as_str().unwrap()).bind(rule["from_role"].as_str()).bind(rule["admission"].as_str().unwrap()).bind(rule).execute(&database.pool).await?;
    }
    let parent = role_address(&manifest, "registry");
    let child: Address = "0x0000000000000000000000000000000000000abc".parse()?;
    let label = vec![0xff; 256]; // Raw string bytes cannot be encoded as a DNS label or PostgreSQL text.
    let parent_log = |index| {
        let mut row = log(
            recovery::RawParentUpdated {
                parent,
                label: label.clone().into(),
                sender: HOLDER.parse().unwrap(),
            }
            .encode_log_data(),
            child,
            120,
            index,
        );
        row.topics[0] = format!("{:#x}", keccak256(b"ParentUpdated(address,string,address)"));
        row
    };
    intake(
        &database,
        &[
            parent_log(0),
            log(
                recovery::RegistryCreated {}.encode_log_data(),
                child,
                120,
                1,
            ),
            parent_log(2),
        ],
        120,
    )
    .await?;
    let engine = bigname_interpret::Engine::new(database.pool.clone());
    run(&engine, 120, 120, bigname_interpret::RunMode::Normal).await?;
    let id = format!("ens:{:#x}", node(&[&label, b"eth"]));
    let read = || async {
        sqlx::query_as::<_,(String,Vec<String>,Option<String>,Option<time::OffsetDateTime>,Option<i64>)>("SELECT surface.visibility_state,surface.labelhashes,surface.preimage_event_identity,surface.deactivated_at,event.log_index FROM name_surfaces surface LEFT JOIN normalized_events event ON event.event_identity=surface.preimage_event_identity WHERE surface.logical_name_id=$1")
            .bind(&id).fetch_one(&database.pool).await
    };
    let first = read().await?;
    assert_eq!(first.0, "shadow");
    assert_eq!(first.1.len(), 2);
    assert_eq!(first.3, Some(timestamp(1_700_000_120)));
    assert_eq!(first.4, Some(0));
    run(
        &bigname_interpret::Engine::new(database.pool.clone()),
        120,
        120,
        bigname_interpret::RunMode::Redo,
    )
    .await?;
    assert_eq!(read().await?, first);
    database.cleanup().await?;
    Ok(())
}

mod numeric {
    use super::*;
    sol! { event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires); }
}

#[tokio::test]
async fn produced_identity_names_numeric_registrar_setup_in_either_log_order() -> Result<()> {
    for registry_first in [true, false] {
        let database = TestDatabase::new_migrated().await?;
        let registry =
            admit_family_from(&database, "sepolia", CHAIN, "ens_v1_registry_l1", 2281).await?;
        let registrar =
            admit_family_from(&database, "sepolia", CHAIN, "ens_v1_registrar_l1", 2282).await?;
        let registry = role_address(&registry, "registry");
        let registrar = role_address(&registrar, "registrar");
        let labels = [b"numeric-child".as_slice(), b"eth"];
        let mut logs = vec![
            owner(B256::ZERO, b"eth", registrar, registry, 120, 0),
            owner(
                node(&[b"eth"]),
                labels[0],
                HOLDER.parse()?,
                registry,
                121,
                i64::from(!registry_first),
            ),
            log(
                numeric::NameRegistered {
                    id: U256::from_be_bytes(*keccak256(labels[0])),
                    owner: HOLDER.parse()?,
                    expires: U256::from(1_900_000_000u64),
                }
                .encode_log_data(),
                registrar,
                121,
                i64::from(registry_first),
            ),
        ];
        logs.sort_by_key(|row| (row.block_number, row.log_index));
        intake(&database, &logs, 121).await?;
        run(
            &bigname_interpret::Engine::new(database.pool.clone()),
            120,
            121,
            bigname_interpret::RunMode::Normal,
        )
        .await?;
        publish(&database, 121).await?;
        let row = detail(&database, &labels).await?;
        assert_eq!(row["registration_status"], "active", "{row:#}");
        assert_eq!(row["owner"], HOLDER);
        assert_eq!(row["expires_at"], "1900000000");
        let id = format!("ens:{:#x}", node(&labels));
        let resources:Vec<Uuid>=sqlx::query_scalar("SELECT resource_id FROM surface_bindings WHERE logical_name_id=$1 AND active_to IS NULL AND canonicality_state IN ('canonical','safe','finalized')").bind(&id).fetch_all(&database.pool).await?;
        let registrar:Uuid=sqlx::query_scalar("SELECT resource_id FROM normalized_events WHERE logical_name_id=$1 AND event_kind='RegistrationGranted'").bind(&id).fetch_one(&database.pool).await?;
        assert_eq!(resources, vec![registrar]);
        redo(&database, 120, 121, 121).await?;
        assert_eq!(detail(&database, &labels).await?, row);
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn v2_named_byte_shadow_masks_later_v1_structural_control_in_live_and_cold_interpret()
-> Result<()> {
    for cold in [false, true] {
        let database = TestDatabase::new_migrated().await?;
        let registry =
            admit_family_from(&database, "mainnet", CHAIN, "ens_v1_registry_l1", 2281).await?;
        let v2 = admit_family_from(&database, "sepolia", CHAIN, "ens_v2_registry_l1", 2284).await?;
        for rule in v2["discovery_rules"].as_array().unwrap() {
            sqlx::query("INSERT INTO manifest_discovery_rules(manifest_id,edge_kind,from_role,admission,rule_payload) VALUES (2284,$1,$2,$3,$4)")
                .bind(rule["edge_kind"].as_str().unwrap()).bind(rule["from_role"].as_str()).bind(rule["admission"].as_str().unwrap()).bind(rule).execute(&database.pool).await?;
        }
        let registry = role_address(&registry, "registry");
        let v2_parent = role_address(&v2, "registry");
        let v2_child: Address = "0x0000000000000000000000000000000000000abc".parse()?;
        let labels = [b"\xff".as_slice(), b"eth"];
        let mut parent_log = log(
            recovery::RawParentUpdated {
                parent: v2_parent,
                label: vec![0xff].into(),
                sender: HOLDER.parse()?,
            }
            .encode_log_data(),
            v2_child,
            121,
            0,
        );
        parent_log.topics[0] =
            format!("{:#x}", keccak256(b"ParentUpdated(address,string,address)"));
        let mut repeated = parent_log.clone();
        repeated.block_number = 123;
        repeated.block_hash = "0xhistory123".into();
        repeated.block_timestamp = timestamp(1_700_000_123);
        repeated.transaction_hash = format!("0x{:064x}", 123);
        let logs = vec![
            owner(B256::ZERO, b"eth", HOLDER.parse()?, registry, 120, 0),
            log(
                recovery::RegistryCreated {}.encode_log_data(),
                v2_child,
                120,
                1,
            ),
            parent_log,
            owner(
                node(&[b"eth"]),
                labels[0],
                HOLDER.parse()?,
                registry,
                122,
                0,
            ),
            repeated,
            owner(
                node(&[b"eth"]),
                labels[0],
                HOLDER.parse()?,
                registry,
                124,
                0,
            ),
        ];
        intake(&database, &logs, 124).await?;
        if cold {
            for block in 120..=124 {
                run(
                    &bigname_interpret::Engine::new(database.pool.clone()),
                    block,
                    block,
                    bigname_interpret::RunMode::Normal,
                )
                .await?;
            }
        } else {
            run(
                &bigname_interpret::Engine::new(database.pool.clone()),
                120,
                124,
                bigname_interpret::RunMode::Normal,
            )
            .await?;
        }
        let id = format!("ens:{:#x}", node(&labels));
        let v2_witnesses:i64=sqlx::query_scalar("SELECT count(*) FROM normalized_events WHERE logical_name_id=$1 AND event_kind='PreimageObserved' AND source_family='ens_v2_registry_l1' AND after_state->>'visibility_state'='shadow'").bind(&id).fetch_one(&database.pool).await?;
        assert_eq!(
            v2_witnesses, 2,
            "the actual V2 log must establish this exact name's shadow"
        );
        let active_claims:i64=sqlx::query_scalar("SELECT count(*) FROM normalized_events WHERE logical_name_id=$1 AND block_number BETWEEN 122 AND 124 AND event_kind='SurfaceBound'").bind(&id).fetch_one(&database.pool).await?;
        assert_eq!(
            active_claims, 0,
            "a known shadow must not produce an active V1 binding; cold={cold}"
        );
        let row: (String, Vec<String>) = sqlx::query_as(
            "SELECT visibility_state,labelhashes FROM name_surfaces WHERE logical_name_id=$1",
        )
        .bind(&id)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(row.0, "shadow");
        assert_eq!(row.1.len(), 2);
        publish(&database, 124).await?;
        sqlx::query("DELETE FROM raw_logs WHERE block_number=121 AND emitting_address=$1")
            .bind(format!("{v2_child:#x}"))
            .execute(&database.pool)
            .await?;
        redo(&database, 121, 124, 124).await?;
        let surviving: String = sqlx::query_scalar(
            "SELECT visibility_state FROM name_surfaces WHERE logical_name_id=$1",
        )
        .bind(&id)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(surviving, "shadow");
        let rebound:i64=sqlx::query_scalar("SELECT count(*) FROM normalized_events WHERE logical_name_id=$1 AND block_number=124 AND event_kind='SurfaceBound'").bind(&id).fetch_one(&database.pool).await?;
        assert_eq!(
            rebound, 0,
            "the second byte witness still suppresses later structural binding"
        );
        sqlx::query("DELETE FROM raw_logs WHERE block_number=123 AND emitting_address=$1")
            .bind(format!("{v2_child:#x}"))
            .execute(&database.pool)
            .await?;
        redo(&database, 121, 124, 124).await?;
        let surviving: (String, Option<String>) = sqlx::query_as(
            "SELECT visibility_state,raw_name FROM name_surfaces WHERE logical_name_id=$1",
        )
        .bind(&id)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(surviving, ("active".into(), None));
        let recovered = detail(&database, &labels).await?;
        assert_eq!(
            recovered["registration_status"], "registered",
            "{recovered:#}"
        );
        database.cleanup().await?;
    }
    Ok(())
}
