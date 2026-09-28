// PublicResolverV2 `NameChanged` and Basenames `ContenthashChanged` count like the same events on an
// ENSv1 resolver (TYR-89 follow-up). Decoding per resolver family is covered in
// crates/adapters/src/schema_v2/tests/node_record_events.rs; these cases pin what the routes serve
// from the adapter-shaped events: a write, a clear, a record-version reset and a resolver switch.

/// One resolver-side event at the head of `chain`, as the named adapter stores it, then the family
/// rebuild there.
#[allow(clippy::too_many_arguments)]
async fn publish_node_record_event(
    database: &TestDatabase,
    chain: &str,
    namespace: &str,
    kind: &str,
    family: &str,
    manifest: Option<i64>,
    emitter: &str,
    identity: (Option<&str>, Option<Uuid>),
    after: Value,
) -> Result<()> {
    let (hash, number) = abi_chain_head(database, chain).await?;
    // `publish_primary_claim` places its events at `id * 4 + offset` for an id from the same
    // process-wide counter, so `id * 4 + 3` orders every later call after them in the block, also
    // when other tests in this process have advanced the counter.
    let ordinal = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as i64 * 4 + 3;
    let mut event = history_event(
        &format!("node-record:{family}:{ordinal}"),
        identity.0,
        identity.1,
        Some(chain),
        Some(number),
        Some(&hash),
        Some(&format!("0x{:064x}", 0xc4_u64)),
        Some(ordinal),
        CanonicalityState::Canonical,
    );
    event.namespace = namespace.into();
    event.event_kind = kind.into();
    event.source_family = family.into();
    event.manifest_version = 1;
    event.source_manifest_id = manifest;
    event.derivation_kind = "ens_v1_unwrapped_authority".into();
    event.raw_fact_ref =
        json!({"kind":"raw_log", "emitting_address":emitter, "transaction_index":0});
    event.before_state = json!({});
    event.after_state = after;
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event]).await?;
    rebuild_fixture_families(&database.pool, chain, number, &hash).await
}

/// A `ContenthashChanged` write as the ENSv1 node decoder stores it (`record_after` with
/// `contenthash_hex`).
fn contenthash_after(node: &str, resolver: &str, hash: &str) -> Value {
    json!({"source_event":"ContenthashChanged", "resolver":resolver, "node":node,
        "record_key":"contenthash", "record_family":"contenthash", "selector_key":null,
        "value_retained":false, "contenthash_hex":hash})
}

fn version_after(node: &str, resolver: &str, version: i64) -> Value {
    json!({"source_event":"VersionChanged", "resolver":resolver, "node":node,
        "record_version":version})
}

async fn contenthash_record(database: &TestDatabase, name: &str) -> Result<Value> {
    let payload = v2_get_json(database, &format!("/v1/names/{name}/records?keys=contenthash")).await?;
    Ok(payload["data"]["records"]["contenthash"].clone())
}

// The Basenames resolver's contenthash is served like an ENSv1 resolver's: a write, the ENS clear
// (empty bytes), a record-version reset, and a switch to another declared resolver and back.
#[tokio::test]
async fn basenames_contenthash_follows_writes_clears_resets_and_resolver_switches() -> Result<()> {
    const BASE: &str = "base-mainnet";
    const RESOLVER: &str = "0x00000000000000000000000000000000000ba5e1";
    const OTHER: &str = "0x00000000000000000000000000000000000ba5e2";
    let database = TestDatabase::new_migrated().await?;
    database.seed_default_ens_snapshot_selector_position().await?;
    let name = "hash.base.eth";
    let resource = Uuid::from_u128(0x5ac100);
    seed_identity_name(
        &database,
        "basenames:hash.base.eth",
        name,
        name,
        "namehash:hash.base.eth",
        resource,
        Uuid::from_u128(0x5ac101),
        Uuid::from_u128(0x5ac102),
        RESOLVER,
        bigname_storage::AddressNameRelation::TokenHolder,
        40,
    )
    .await?;
    let node = bigname_lookup::ens_namehash_hex(name)?;
    let family = "basenames_base_resolver";
    let manifest =
        declare_family_fixture_resolver(&database.pool, "basenames", BASE, family, RESOLVER)
            .await?;
    let write = |resolver: &'static str, manifest: i64, after: Value| {
        let database = &database;
        async move {
            publish_node_record_event(
                database,
                BASE,
                "basenames",
                if after["source_event"] == "VersionChanged" {
                    "RecordVersionChanged"
                } else {
                    "RecordChanged"
                },
                family,
                Some(manifest),
                resolver,
                (None, None),
                after,
            )
            .await
        }
    };

    write(RESOLVER, manifest, contenthash_after(&node, RESOLVER, "0xe30101701220aa")).await?;
    let set = contenthash_record(&database, name).await?;
    assert_eq!(set["status"], json!("ok"), "{set}");
    // A clear stores empty bytes and emits the same event: the value is gone.
    write(RESOLVER, manifest, contenthash_after(&node, RESOLVER, "0x")).await?;
    assert_eq!(contenthash_record(&database, name).await?["status"], json!("not_found"));
    // A reset drops the earlier write; a later write counts again.
    write(RESOLVER, manifest, contenthash_after(&node, RESOLVER, "0xe30101701220bb")).await?;
    let rewritten = contenthash_record(&database, name).await?;
    assert_eq!(rewritten["status"], json!("ok"), "{rewritten}");
    assert_ne!(rewritten, set);
    write(RESOLVER, manifest, version_after(&node, RESOLVER, 1)).await?;
    assert_eq!(contenthash_record(&database, name).await?["status"], json!("not_found"));
    write(RESOLVER, manifest, contenthash_after(&node, RESOLVER, "0xe30101701220cc")).await?;
    let after_reset = contenthash_record(&database, name).await?;
    assert_eq!(after_reset["status"], json!("ok"), "{after_reset}");

    // Another declared resolver's write counts only while the name points at it.
    let other = declare_family_fixture_resolver(&database.pool, "basenames", BASE, family, OTHER)
        .await?;
    write(OTHER, other, contenthash_after(&node, OTHER, "0xe30101701220dd")).await?;
    assert_eq!(contenthash_record(&database, name).await?, after_reset);
    let logical = bigname_storage::logical_name_id_for_name("basenames", name);
    let point = |resolver: &'static str| {
        let (database, node, logical) = (&database, node.clone(), logical.clone());
        async move {
            publish_node_record_event(
                database,
                BASE,
                "basenames",
                "ResolverChanged",
                "basenames_base_registry",
                None,
                resolver,
                (Some(logical.as_str()), Some(resource)),
                json!({"source_event":"NewResolver", "node":node, "resolver":resolver}),
            )
            .await
        }
    };
    point(OTHER).await?;
    let switched = contenthash_record(&database, name).await?;
    assert_eq!(switched["status"], json!("ok"), "{switched}");
    assert_ne!(switched, after_reset);
    point(RESOLVER).await?;
    assert_eq!(contenthash_record(&database, name).await?, after_reset);
    database.cleanup().await
}

// PublicResolverV2's contenthash was already served; the same write, clear and reset hold for it
// alongside its now-admitted ABI and name writes.
#[tokio::test]
async fn public_resolver_v2_contenthash_follows_writes_clears_and_resets() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.seed_default_ens_snapshot_selector_position().await?;
    let name = "hash-v2.eth";
    seed_abi_public_resolver_v2_name(&database, name, &[]).await?;
    let node = bigname_lookup::ens_namehash_hex(name)?;
    let manifest: i64 = sqlx::query_scalar(
        "SELECT manifest_id FROM manifest_versions WHERE file_path = 'fixture/abi-public-resolver-v2.toml'",
    )
    .fetch_one(&database.pool)
    .await?;
    let write = |kind: &'static str, after: Value| {
        let database = &database;
        async move {
            publish_node_record_event(
                database,
                ABI_CHAIN,
                "ens",
                kind,
                "ens_v2_resolver_l1",
                Some(manifest),
                PUBLIC_RESOLVER_V2,
                (None, None),
                after,
            )
            .await
        }
    };
    write("RecordChanged", contenthash_after(&node, PUBLIC_RESOLVER_V2, "0xe30101701220aa")).await?;
    assert_eq!(contenthash_record(&database, name).await?["status"], json!("ok"));
    write("RecordChanged", contenthash_after(&node, PUBLIC_RESOLVER_V2, "0x")).await?;
    assert_eq!(contenthash_record(&database, name).await?["status"], json!("not_found"));
    write("RecordChanged", contenthash_after(&node, PUBLIC_RESOLVER_V2, "0xe30101701220bb")).await?;
    assert_eq!(contenthash_record(&database, name).await?["status"], json!("ok"));
    write("RecordVersionChanged", version_after(&node, PUBLIC_RESOLVER_V2, 1)).await?;
    assert_eq!(contenthash_record(&database, name).await?["status"], json!("not_found"));
    database.cleanup().await
}

async fn is_primary(database: &TestDatabase, name: &str) -> Result<bool> {
    let payload = v2_address_names_payload_for_database(
        database,
        &format!("/v1/addresses/{V2_ADDRESS}/names"),
    )
    .await?;
    let rows = payload["data"].as_array().context("address names")?;
    Ok(rows
        .iter()
        .find(|row| row["name"] == json!(name))
        .with_context(|| format!("{name} row"))?["is_primary"]
        == json!(true))
}

// A reverse node whose registry resolver is the declared PublicResolverV2 now reads that
// resolver's `NameChanged`, as ENS reverse resolution does: the registry's resolver for the
// reverse node answers `name(node)` from the value `setName` stored
// (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L13-L29 @ ens_v1@91c966f).
#[tokio::test]
async fn public_resolver_v2_name_changed_answers_a_reverse_claim_through_its_node() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    publish_primary_claim(&database.pool, "ens", V2_ADDRESS, b"alpha.eth").await?;
    assert!(is_primary(&database, "alpha.eth").await?);

    let payload = json!({"contracts":[{"role":"public_resolver_v2", "address":PUBLIC_RESOLVER_V2,
        "proxy_kind":"none", "start_block":0, "read_features":[]}]});
    let manifest: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (manifest_version, namespace, source_family, chain_id,
            deployment_label, rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES (1, 'ens', 'ens_v2_resolver_l1', $1, 'fixture', 'active', $2,
            'fixture/reverse-public-resolver-v2.toml', $3) RETURNING manifest_id",
    )
    .bind(ABI_CHAIN)
    .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
    .bind(&payload)
    .fetch_one(&database.pool)
    .await?;
    let address = V2_ADDRESS.to_ascii_lowercase();
    let reverse_node = bigname_lookup::ens_namehash_hex(&format!(
        "{}.addr.reverse",
        address.strip_prefix("0x").unwrap_or(&address)
    ))?;
    let point = |resolver: &'static str| {
        let (database, node) = (&database, reverse_node.clone());
        async move {
            publish_node_record_event(
                database,
                ABI_CHAIN,
                "ens",
                "ResolverChanged",
                "ens_v1_registry_l1",
                None,
                ENS_REGISTRY,
                (None, None),
                json!({"source_event":"NewResolver", "node":node, "resolver":resolver,
                    "emitter_role":"registry"}),
            )
            .await
        }
    };
    let name_write = |name: &'static str| {
        let (database, node) = (&database, reverse_node.clone());
        async move {
            publish_node_record_event(
                database,
                ABI_CHAIN,
                "ens",
                "RecordChanged",
                "ens_v2_resolver_l1",
                Some(manifest),
                PUBLIC_RESOLVER_V2,
                (None, None),
                json!({"source_event":"NameChanged", "resolver":PUBLIC_RESOLVER_V2, "node":node,
                    "record_key":"name", "record_family":"name", "selector_key":null,
                    "value_retained":false, "raw_name":name}),
            )
            .await
        }
    };

    // The reverse node moves to PublicResolverV2, which holds no name yet.
    point(PUBLIC_RESOLVER_V2).await?;
    assert!(!is_primary(&database, "alpha.eth").await?);
    name_write("alpha.eth").await?;
    assert!(is_primary(&database, "alpha.eth").await?);
    // A clear (empty name) and a record-version reset each leave no claimed name.
    name_write("").await?;
    assert!(!is_primary(&database, "alpha.eth").await?);
    name_write("alpha.eth").await?;
    assert!(is_primary(&database, "alpha.eth").await?);
    publish_node_record_event(
        &database,
        ABI_CHAIN,
        "ens",
        "RecordVersionChanged",
        "ens_v2_resolver_l1",
        Some(manifest),
        PUBLIC_RESOLVER_V2,
        (None, None),
        version_after(&reverse_node, PUBLIC_RESOLVER_V2, 1),
    )
    .await?;
    assert!(!is_primary(&database, "alpha.eth").await?);
    // Switching back to the original reverse resolver restores its name record.
    point(ENS_REVERSE_RESOLVER).await?;
    assert!(is_primary(&database, "alpha.eth").await?);
    database.cleanup().await
}
