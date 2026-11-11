// The history `record_key` filter (docs/api-v1-routes.md, "Shared Route Rules"). Text records
// are stored as `text:<key>`, and a key with no colon that no producer stores bare is read as
// that text record. Every other key is exact. Cursors bind the spelling the request reads.

const RK_NAME: &str = "record-key.eth";
const RK_RESOLVER: &str = "0x00000000000000000000000000000000000000d7";
const RK_ADDRESS: &str = "0x0000000000000000000000000000000000007517";
/// The keys producers store without a family prefix.
const RK_UNALIASED: [&str; 3] = ["content", "contenthash", "name"];

/// `record-key.eth` with a resolver pointer and one write of each stored key shape, plus four
/// `text:avatar` writes and one record-version reset of the name's node.
async fn rk_seed(database: &TestDatabase) -> Result<()> {
    let logical = bigname_storage::logical_name_id_for_name("ens", RK_NAME);
    let resource = Uuid::from_u128(0x7517);
    seed_identity_name(
        database,
        &logical,
        RK_NAME,
        RK_NAME,
        "node:record-key.eth",
        resource,
        Uuid::from_u128(0x8517),
        Uuid::from_u128(0x9517),
        RK_ADDRESS,
        bigname_storage::AddressNameRelation::EffectiveController,
        80,
    )
    .await?;
    seed_v2_history_blocks(database, 130..=150).await?;
    let node = bigname_lookup::ens_namehash_hex(RK_NAME)?;
    let mut pointer = v2_history_event(
        "rk-pointer",
        Some(&logical),
        Some(resource),
        "ResolverChanged",
        130,
    );
    pointer.source_family = "ens_v1_registry_l1".to_owned();
    pointer.after_state = json!({"node":node, "resolver":RK_RESOLVER});
    pointer.log_index = Some(1);
    let record = |identity: &str, block: i64, after: Value| {
        let kind = if after["source_event"] == "VersionChanged" {
            "RecordVersionChanged"
        } else {
            "RecordChanged"
        };
        event_data_event(
            identity,
            None,
            None,
            kind,
            "ens_v1_resolver_l1",
            block,
            &format!("0xtx{block}"),
            0,
            RK_RESOLVER,
            after,
        )
    };
    let write = |source: &str, key: &str, family: &str| {
        json!({"source_event":source, "resolver":RK_RESOLVER, "node":node,
        "record_key":key, "record_family":family, "selector_key":Value::Null,
        "value_retained":false})
    };
    let events = vec![
        v2_history_event(
            "rk-grant",
            Some(&logical),
            Some(resource),
            "RegistrationGranted",
            130,
        ),
        pointer,
        record(
            "rk-avatar-1",
            131,
            write("TextChanged", "text:avatar", "text"),
        ),
        record("rk-addr", 132, write("AddressChanged", "addr:60", "addr")),
        record(
            "rk-avatar-2",
            133,
            write("TextChanged", "text:avatar", "text"),
        ),
        record("rk-reset", 134, version_after(&node, RK_RESOLVER, 1)),
        record(
            "rk-content",
            135,
            write("ContentChanged", "content", "content"),
        ),
        record(
            "rk-avatar-3",
            136,
            write("TextChanged", "text:avatar", "text"),
        ),
        record(
            "rk-text-content",
            137,
            write("TextChanged", "text:content", "text"),
        ),
        record(
            "rk-contenthash",
            138,
            write("ContenthashChanged", "contenthash", "contenthash"),
        ),
        record("rk-name", 139, write("NameChanged", "name", "name")),
        record(
            "rk-zonehash",
            140,
            write("DNSZonehashChanged", "dns:zonehash", "dns"),
        ),
        record(
            "rk-literal",
            141,
            write("TextChanged", "text:Case, and space ", "text"),
        ),
        record(
            "rk-avatar-4",
            142,
            write("TextChanged", "text:avatar", "text"),
        ),
    ];
    publish_event_data(database, &events, 150).await
}

/// The three history routes that take the filter, with `/v1/events` unanchored, anchored by
/// the name, and scoped by the emitting resolver both ways.
fn rk_routes() -> Vec<String> {
    vec![
        "/v1/events?namespace=ens".to_owned(),
        format!("/v1/events?name={RK_NAME}"),
        format!("/v1/events?contract_address={RK_RESOLVER}"),
        format!("/v1/events?resolver=1:{RK_RESOLVER}"),
        format!("/v1/names/{RK_NAME}/history?scope=both"),
        format!("/v1/addresses/{RK_ADDRESS}/history?scope=both"),
    ]
}

fn rk_ids(identities: &[&str]) -> Vec<String> {
    identities.iter().map(|identity| hkw_id(identity)).collect()
}

/// Every page of `base` one row at a time: the row ids and each page's next cursor.
async fn rk_walk(database: &TestDatabase, base: &str) -> Result<(Vec<String>, Vec<String>)> {
    let mut ids = Vec::new();
    let mut cursors = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let uri = match cursor.as_ref() {
            Some(cursor) => format!("{base}&page_size=1&cursor={cursor}"),
            None => format!("{base}&page_size=1"),
        };
        let page = hk_ok(database, &uri).await?;
        ids.extend(hk_ids(&page));
        match page["page"]["next_cursor"].as_str() {
            Some(next) => {
                cursors.push(next.to_owned());
                cursor = Some(next.to_owned());
            }
            None => return Ok((ids, cursors)),
        }
    }
}

#[tokio::test]
async fn bare_text_record_keys_read_the_text_record_on_every_route() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    rk_seed(&database).await?;
    let expected = rk_ids(&[
        "rk-avatar-1",
        "rk-avatar-2",
        "rk-reset",
        "rk-avatar-3",
        "rk-avatar-4",
    ]);
    for route in rk_routes() {
        let stored = format!("{route}&record_key=text:avatar&order=asc");
        let bare = format!("{route}&record_key=avatar&order=asc");
        let (ids, _) = hkw_baseline(&database, &stored).await?;
        assert_eq!(ids, expected, "{stored}");
        let (bare_ids, _) = hkw_baseline(&database, &bare).await?;
        assert_eq!(bare_ids, expected, "{bare}");
        assert_eq!(
            rk_walk(&database, &bare).await?,
            rk_walk(&database, &stored).await?,
            "{bare}: pages and cursors"
        );
    }
    database.cleanup().await
}

#[tokio::test]
async fn record_key_spellings_stay_exact_apart_from_the_text_alias() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    rk_seed(&database).await?;
    let cases: [(&str, &[&str]); 13] = [
        (
            "text:avatar",
            &[
                "rk-avatar-1",
                "rk-avatar-2",
                "rk-reset",
                "rk-avatar-3",
                "rk-avatar-4",
            ],
        ),
        ("Avatar", &["rk-reset"]),
        ("text:Avatar", &["rk-reset"]),
        ("contenthash", &["rk-reset", "rk-contenthash"]),
        ("name", &["rk-reset", "rk-name"]),
        ("content", &["rk-reset", "rk-content"]),
        ("text:content", &["rk-reset", "rk-text-content"]),
        ("addr:60", &["rk-addr", "rk-reset"]),
        ("dns:zonehash", &["rk-reset", "rk-zonehash"]),
        ("a:b", &["rk-reset"]),
        ("%20avatar", &["rk-reset"]),
        ("text:%20avatar", &["rk-reset"]),
        ("text:Case%2C%20and%20space%20", &["rk-reset", "rk-literal"]),
    ];
    for route in rk_routes() {
        for (key, identities) in cases {
            let uri = format!("{route}&record_key={key}&order=asc");
            let (ids, _) = hkw_baseline(&database, &uri).await?;
            assert_eq!(ids, rk_ids(identities), "{uri}");
        }
    }
    database.cleanup().await
}

#[tokio::test]
async fn record_key_cursors_bind_the_canonical_spelling() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    rk_seed(&database).await?;
    let second = rk_ids(&["rk-avatar-2"]);
    for route in rk_routes() {
        let stored = format!("{route}&record_key=text:avatar&order=asc&page_size=1");
        let bare = format!("{route}&record_key=avatar&order=asc&page_size=1");
        // A cursor issued under either spelling continues under the other.
        for (issued, continued) in [(&bare, &stored), (&stored, &bare)] {
            let cursor = hk_next_cursor(&hk_ok(&database, issued).await?)?;
            let page = hk_ok(&database, &format!("{continued}&cursor={cursor}")).await?;
            assert_eq!(hk_ids(&page), second, "{issued} continued as {continued}");
        }
        // The cursor binds the stored spelling, so one issued before the alias under
        // `text:avatar` is byte-identical and continues.
        let cursor = hk_next_cursor(&hk_ok(&database, &stored).await?)?;
        let mut payload: Value = serde_json::from_slice(&hex::decode(&cursor)?)?;
        assert_eq!(
            payload["filters"]["record_key"],
            json!("text:avatar"),
            "{stored}"
        );
        let page = hk_ok(&database, &format!("{stored}&cursor={cursor}")).await?;
        assert_eq!(hk_ids(&page), second, "{stored}");
        // A cursor issued before the alias under the bare key bound `avatar`. Its earlier
        // pages held resets only, so it is refused rather than continued.
        payload["filters"]["record_key"] = json!("avatar");
        let legacy = hex::encode(serde_json::to_vec(&payload)?);
        let (status, body) = hk_get(&database, &format!("{bare}&cursor={legacy}")).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bare}: {body}");
        assert_eq!(
            body["error"]["message"],
            json!("cursor must be a valid pagination cursor"),
            "{bare}"
        );
    }
    database.cleanup().await
}

/// The rows the plain statement returns for an unanchored `/v1/events` key read on the fixture
/// chain, oldest first, as row ids.
async fn rk_plain_ids(
    database: &TestDatabase,
    key: &str,
    emitter: Option<&str>,
) -> Result<Vec<String>> {
    let identities: Vec<String> = sqlx::query_scalar(
        "SELECT ne.event_identity FROM bigname_phase.normalized_events ne
         LEFT JOIN bigname_phase.chain_lineage rb
           ON rb.chain_id = ne.chain_id AND rb.block_hash = ne.block_hash
         WHERE ne.consumer_visibility = 'activated' AND ne.namespace = 'ens'
           AND ne.chain_id = 'ethereum-mainnet' AND ne.block_number <= 150
           AND (ne.event_kind = 'RecordVersionChanged'
                OR (ne.event_kind = 'RecordChanged' AND ne.after_state ->> 'record_key' = $1))
           AND ($2::text IS NULL OR lower(ne.raw_fact_ref ->> 'emitting_address') = $2)
           AND ne.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND (ne.block_hash IS NULL OR rb.canonicality_state IN ('canonical', 'safe', 'finalized'))
         ORDER BY ne.block_number ASC NULLS FIRST, ne.chain_id DESC NULLS FIRST,
                  ne.block_hash ASC NULLS FIRST, ne.transaction_index ASC NULLS FIRST,
                  ne.log_index ASC NULLS FIRST, ne.event_identity ASC",
    )
    .bind(key)
    .bind(emitter)
    .fetch_all(&database.pool)
    .await?;
    Ok(identities.iter().map(|identity| hkw_id(identity)).collect())
}

#[tokio::test]
async fn unanchored_record_key_events_match_the_plain_statement() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    rk_seed(&database).await?;
    for (key, stored) in [
        ("text:avatar", "text:avatar"),
        ("avatar", "text:avatar"),
        ("content", "content"),
        ("text:nosuch", "text:nosuch"),
    ] {
        for (scope, emitter) in [
            (String::new(), None),
            (
                format!("&contract_address={RK_RESOLVER}"),
                Some(RK_RESOLVER),
            ),
            (format!("&resolver=1:{RK_RESOLVER}"), Some(RK_RESOLVER)),
        ] {
            let mut expected = rk_plain_ids(&database, stored, emitter).await?;
            assert!(!expected.is_empty(), "{key}{scope}: resets match every key");
            for order in ["asc", "desc"] {
                if order == "desc" {
                    expected.reverse();
                }
                let base =
                    format!("/v1/events?namespace=ens{scope}&record_key={key}&order={order}");
                let (ids, _) = hkw_baseline(&database, &base).await?;
                assert_eq!(ids, expected, "{base}");
                assert_eq!(
                    rk_walk(&database, &base).await?.0,
                    expected,
                    "{base}: pages of one"
                );
            }
        }
    }
    database.cleanup().await
}

/// Every record key a producer stores without a colon is one the bare-key alias leaves alone,
/// and every key the alias leaves alone is one a producer stores. Each event a checked-in
/// manifest declares as `RecordChanged` is decoded from a synthetic log through the adapters,
/// from each declared contract the event may come from and from an undeclared address.
#[test]
fn stored_bare_record_keys_are_the_unaliased_set() -> Result<()> {
    use std::collections::BTreeSet;

    use alloy_primitives::keccak256;
    use bigname_adapters::schema_v2::{
        AddressAdmissionInput, BatchInput, ManifestInput, RawBlockInput, RawLogInput,
        StateCacheCapacity, prepare_schema_v2_batch_incremental,
    };
    const BLOCK: i64 = 40_000_000;
    const UNDECLARED: &str = "0x00000000000000000000000000000000000c0571";
    // Every string and bytes argument carries this value, so an indexed key's topic is its hash.
    const TEXT: &[u8] = b"avatar";
    let word = |ty: &str| -> Result<[u8; 32]> {
        let mut word = [0u8; 32];
        if ty == "address" {
            word[31] = 0xa1;
        } else if ty == "bool" {
            word[31] = 1;
        } else if ty.starts_with("uint") || ty.starts_with("int") {
            word[31] = 60;
        } else if let Some(size) = ty
            .strip_prefix("bytes")
            .and_then(|n| n.parse::<usize>().ok())
        {
            word[..size].fill(0x11);
        } else {
            anyhow::bail!("no synthetic value for {ty}");
        }
        Ok(word)
    };
    let dynamic = |ty: &str| ty == "string" || ty == "bytes";
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests");
    let mut bare = BTreeSet::new();
    let mut decoded = 0usize;
    let mut silent = Vec::new();
    for profile in ["mainnet", "sepolia"] {
        let repository = bigname_manifests::load_repository(root.join(profile))?;
        let loaded = repository.manifests();
        let chains: BTreeSet<String> = loaded
            .iter()
            .map(|loaded| loaded.manifest.chain.clone())
            .collect();
        for chain in chains {
            let on_chain: Vec<_> = loaded
                .iter()
                .filter(|loaded| loaded.manifest.chain == chain)
                .map(|loaded| &loaded.manifest)
                .collect();
            let mut manifests = Vec::new();
            let mut admissions = Vec::new();
            for (index, manifest) in on_chain.iter().enumerate() {
                let manifest_id = 7_000 + i64::try_from(index)?;
                manifests.push(ManifestInput {
                    manifest_id,
                    manifest_version: i64::try_from(manifest.manifest_version)?,
                    namespace: manifest.namespace.clone(),
                    source_family: manifest.source_family.clone(),
                    chain_id: chain.clone(),
                    deployment_label: manifest.deployment_epoch.clone(),
                    normalizer_version: manifest.normalizer_version.clone(),
                    payload_json: serde_json::to_string(manifest)?,
                });
                for (ordinal, contract) in manifest.contracts.iter().enumerate() {
                    admissions.push(AddressAdmissionInput {
                        address: contract.address.to_ascii_lowercase(),
                        contract_instance_id: Uuid::from_u128(
                            u128::try_from(manifest_id)? * 100 + u128::try_from(ordinal)?,
                        ),
                        source_manifest_id: Some(manifest_id),
                        role: Some(contract.role.clone()),
                        discovery_edge_kind: None,
                        discovery_from_contract_instance_id: None,
                        discovery_observation_key: None,
                        active_from_block: Some(0),
                        active_to_block: None,
                    });
                }
            }
            for manifest in &on_chain {
                for declared in &manifest.abi.events {
                    if !declared
                        .normalized_events
                        .iter()
                        .any(|kind| kind == "RecordChanged")
                    {
                        continue;
                    }
                    let event = declared.parsed_event()?;
                    let mut topics = vec![format!("{:#x}", event.selector())];
                    let mut head = Vec::new();
                    let mut tail = Vec::new();
                    let data_params = event.inputs.iter().filter(|input| !input.indexed).count();
                    for input in &event.inputs {
                        if input.indexed {
                            let topic = if dynamic(&input.ty) {
                                keccak256(TEXT).0
                            } else {
                                word(&input.ty)?
                            };
                            topics.push(format!("0x{}", hex::encode(topic)));
                        } else if dynamic(&input.ty) {
                            let offset = 32 * data_params + tail.len();
                            let mut offset_word = [0u8; 32];
                            offset_word[24..].copy_from_slice(&(offset as u64).to_be_bytes());
                            head.extend_from_slice(&offset_word);
                            let mut length = [0u8; 32];
                            length[24..].copy_from_slice(&(TEXT.len() as u64).to_be_bytes());
                            tail.extend_from_slice(&length);
                            let mut padded = TEXT.to_vec();
                            padded.resize(TEXT.len().div_ceil(32) * 32, 0);
                            tail.extend_from_slice(&padded);
                        } else {
                            head.extend_from_slice(&word(&input.ty)?);
                        }
                    }
                    head.extend_from_slice(&tail);
                    let mut emitters: Vec<String> = manifest
                        .contracts
                        .iter()
                        .filter(|contract| {
                            declared.emitter_roles.is_empty()
                                || declared.emitter_roles.contains(&contract.role)
                        })
                        .map(|contract| contract.address.to_ascii_lowercase())
                        .collect();
                    emitters.push(UNDECLARED.to_owned());
                    let mut keys = BTreeSet::new();
                    for emitter in emitters {
                        let (output, _) = prepare_schema_v2_batch_incremental(
                            BatchInput {
                                chain_id: chain.clone(),
                                manifests: manifests.clone(),
                                admissions: admissions.clone(),
                                discovery_rules: vec![],
                                prior_events: vec![],
                                blocks: vec![RawBlockInput {
                                    chain_id: chain.clone(),
                                    block_hash: "0xrecord-key-producers".into(),
                                    block_number: BLOCK,
                                    block_timestamp: timestamp(1_800_000_000),
                                    canonicality_state: "canonical".into(),
                                }],
                                raw_logs: vec![RawLogInput {
                                    chain_id: chain.clone(),
                                    block_hash: "0xrecord-key-producers".into(),
                                    block_number: BLOCK,
                                    block_timestamp: timestamp(1_800_000_000),
                                    canonicality_state: "canonical".into(),
                                    transaction_hash: "0xrecord-key-producer".into(),
                                    transaction_index: 0,
                                    log_index: 0,
                                    emitting_address: emitter,
                                    topics: topics.clone(),
                                    data: head.clone(),
                                }],
                            },
                            None,
                            StateCacheCapacity::Unlimited,
                        )?
                        .finish(vec![])?;
                        for row in &output.normalized_events {
                            if row.event_kind == "RecordChanged"
                                && let Some(key) = row.after_state["record_key"].as_str()
                            {
                                keys.insert(key.to_owned());
                            }
                        }
                    }
                    if keys.is_empty() {
                        silent.push(format!(
                            "{chain} {} {}",
                            manifest.source_family, declared.fragment
                        ));
                    }
                    decoded += 1;
                    bare.extend(keys.into_iter().filter(|key| !key.contains(':')));
                }
            }
        }
    }
    assert!(
        silent.is_empty(),
        "these declared record events wrote no record: {silent:#?}"
    );
    assert!(
        decoded > 20,
        "only {decoded} declared record events were decoded"
    );
    assert_eq!(
        bare,
        RK_UNALIASED
            .iter()
            .map(|key| (*key).to_owned())
            .collect::<BTreeSet<_>>(),
        "the producers' bare keys must be exactly the keys the alias leaves alone"
    );
    Ok(())
}
