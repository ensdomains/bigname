// A history cursor across a resolver declaration that starts above its bound. Project picks a
// resolver's classification among its manifest's declarations by `start_block` at the Project
// target, so ordinary advancement past a later-start declaration changes the classification the
// bounded record attribution joins, with no manifest change, redo, or reorg. The manifest comes
// from the manifest producer, the classification from real Project batches, and the events have
// the shape the ENSv2 registry and resolver adapters write.

const CH_NAME: &str = "horizon-v2.eth";
const CH_RESOLVER: &str = "0x00000000000000000000000000000000000c0a2c";
const CH_BOUND: i64 = 240;

/// The Sepolia ENSv2 resolver manifest moved to the test chain, declaring `CH_RESOLVER` as
/// `public_resolver_v2` from block 200 and as the ENSv1 mirror resolver from `mirror_start`.
fn ch_manifest(mirror_start: i64) -> Result<String> {
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../manifests/sepolia/ethereum/ens/ens_v2_resolver_l1/v1.toml"),
    )?;
    let (header, rest) = source
        .split_once("\n[[contracts]]")
        .context("manifest contracts")?;
    let abi = rest.find("\n[[abi.events]]").context("manifest abi")?;
    Ok(format!(
        r#"{header}
[[contracts]]
role = "public_resolver_v2"
address = "{CH_RESOLVER}"
proxy_kind = "none"
start_block = 200

[[contracts]]
role = "ensv1_mirror_resolver"
address = "{CH_RESOLVER}"
proxy_kind = "none"
start_block = {mirror_start}
{abi}"#,
        header = header.replace(
            r#"chain = "ethereum-sepolia""#,
            &format!(r#"chain = "{BOUNDED_CHAIN}""#)
        ),
        abi = &rest[abi..],
    ))
}

/// Blocks 200..=240, the synced manifest, the name with its ENSv2 pointer to `CH_RESOLVER`, and
/// node-keyed writes at 220 and 225 that only the resolver's classification attributes to it.
/// Project runs to the bound and the bound is published.
async fn ch_seed(database: &TestDatabase, mirror_start: i64) -> Result<Uuid> {
    for block in 200..=CH_BOUND {
        ch_head_to(database, block).await?;
    }
    publish_bounded_membership_at(database, CH_BOUND).await?;
    ch_sync(database, &[("v1.toml", ch_manifest(mirror_start)?)]).await?;
    let manifest_id: i64 = sqlx::query_scalar(
        "SELECT manifest_id FROM bigname_phase.manifest_versions
         WHERE source_family = 'ens_v2_resolver_l1' AND chain_id = $1 AND manifest_version = 1",
    )
    .bind(BOUNDED_CHAIN)
    .fetch_one(&database.pool)
    .await?;

    let (logical_name_id, resource) = seed_bounded_name(
        database,
        CH_NAME,
        0xc0a_7000,
        "0x00000000000000000000000000000000000c0a07",
        bigname_storage::AddressNameRelation::EffectiveController,
        205,
    )
    .await?;
    let mut pointer = v2_history_event(
        "ch-pointer-210",
        Some(&logical_name_id),
        Some(resource),
        "ResolverChanged",
        210,
    );
    pointer.source_family = "ens_v2_registry_l1".to_owned();
    pointer.derivation_kind = "ens_v2_registry_resource_surface".to_owned();
    pointer.after_state = json!({"resolver": CH_RESOLVER});
    pointer.log_index = Some(1);
    let node_write = |identity: &str, block: i64| -> Result<NormalizedEvent> {
        let mut event = v2_history_event(identity, None, None, "RecordChanged", block);
        event.source_family = "ens_v2_resolver_l1".to_owned();
        event.derivation_kind = "ens_v2_resolver".to_owned();
        event.source_manifest_id = Some(manifest_id);
        event.manifest_version = 1;
        event.after_state = json!({
            "source_event": "TextChanged",
            "node": bigname_lookup::ens_namehash_hex(CH_NAME)?,
            "resolver": CH_RESOLVER,
            "record_key": "text:url",
            "record_family": "text",
            "selector_key": "url",
            "value_retained": true,
            "value": identity,
        });
        Ok(event)
    };
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            v2_history_event(
                "ch-grant",
                Some(&logical_name_id),
                Some(resource),
                "RegistrationGranted",
                205,
            ),
            pointer,
            node_write("ch-write-220", 220)?,
            node_write("ch-write-225", 225)?,
        ],
    )
    .await?;
    ch_project_to(database, CH_BOUND).await?;
    Ok(resource)
}

/// Run the manifest producer over a repository holding exactly `files` of the ENSv2 resolver
/// family.
async fn ch_sync(database: &TestDatabase, files: &[(&str, String)]) -> Result<()> {
    let root =
        std::env::temp_dir().join(format!("bigname-classification-horizon-{}", Uuid::new_v4()));
    let directory = root.join("ethereum/ens/ens_v2_resolver_l1");
    std::fs::create_dir_all(&directory)?;
    for (file, text) in files {
        std::fs::write(directory.join(file), text)?;
    }
    let synced = match bigname_manifests::load_repository(&root) {
        Ok(repository) => {
            bigname_manifests::sync_schema_v2_repository(&database.pool, &repository)
                .await
                .map(|_| ())
        }
        Err(error) => Err(error),
    };
    std::fs::remove_dir_all(&root)?;
    synced
}

/// `manifest` with one more ENSv1 mirror resolver declaration, for another address, starting
/// at `start`.
fn ch_with_declaration(manifest: &str, start: i64) -> Result<String> {
    let abi = manifest.find("\n[[abi.events]]").context("manifest abi")?;
    Ok(format!(
        r#"{head}
[[contracts]]
role = "ensv1_mirror_resolver"
address = "0x00000000000000000000000000000000000c0a3c"
proxy_kind = "none"
start_block = {start}
{tail}"#,
        head = &manifest[..abi],
        tail = &manifest[abi..],
    ))
}

/// Ingest adds the canonical block `block` on top of `block - 1`: the chain's readable head.
async fn ch_head_to(database: &TestDatabase, block: i64) -> Result<()> {
    let parent = (block > 200).then(|| format!("0xhistory{}", block - 1));
    upsert_phase_raw_blocks(
        &database.pool,
        &[raw_block(
            BOUNDED_CHAIN,
            &format!("0xhistory{block}"),
            parent.as_deref(),
            block,
            1_700_000_000 + block,
        )],
    )
    .await?;
    Ok(())
}

/// One ordinary Project batch to `target`, then its publication.
async fn ch_project_to(database: &TestDatabase, target: i64) -> Result<()> {
    ch_swap_to(database, target).await?;
    publish_bounded_membership_at(database, target).await
}

/// The head reaches `target` and Project's batch commits its projection swap there, but the
/// phase runner has not yet recorded the new position, which it writes afterwards in its own
/// transaction (bigname: `crates/project/src/engine.rs:44-63`,
/// `apps/phase-runner/src/runner_batch.rs:160-163`).
async fn ch_swap_to(database: &TestDatabase, target: i64) -> Result<()> {
    ch_head_to(database, target).await?;
    bigname_project::Engine::new(database.pool.clone())
        .run_batch(bigname_project::BatchRequest {
            chain_id: BOUNDED_CHAIN.into(),
            target_block: target,
            affected_from_block: 200,
            affected_to_block: target,
            resume_current: None,
            mode: bigname_project::RunMode::Normal,
        })
        .await?;
    Ok(())
}

async fn ch_get(database: &TestDatabase, uri: &str) -> Result<(StatusCode, Value)> {
    let state = AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    );
    let response = app_router(state)
        .oneshot(Request::builder().uri(uri).body(Body::empty())?)
        .await?;
    let status = response.status();
    Ok((status, read_json(response).await?))
}

async fn ch_ok(database: &TestDatabase, uri: &str) -> Result<Value> {
    let (status, payload) = ch_get(database, uri).await?;
    anyhow::ensure!(status == StatusCode::OK, "{uri}: {status} {payload}");
    Ok(payload)
}

/// The cursor's manifest digest and the redo counters of every chain it binds.
fn ch_binding_identity(cursor: &str) -> Result<Value> {
    let payload: Value = serde_json::from_slice(&hex::decode(cursor)?)?;
    let binding = &payload["binding"];
    let counters = binding["chains"]
        .as_object()
        .context("cursor chains")?
        .iter()
        .map(|(chain, bound)| {
            (
                chain.clone(),
                json!([bound["interpret_generation"], bound["project_generation"]]),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    Ok(json!({"manifests": binding["manifests"], "counters": counters}))
}

fn ch_routes(resource: Uuid) -> [String; 2] {
    [
        format!("/v1/names/{CH_NAME}/history?scope=registration&include=total_count"),
        format!("/v1/events?registration_id={resource}&include=total_count"),
    ]
}

/// Crossing the later declaration's start flips the resolver's classification during ordinary
/// Project advancement. Neither the manifests nor the redo counters change, so without the
/// classification horizon a saved cursor would continue with the attributed writes gone: a
/// changed page and count when its anchor is unaffected, `400` when its anchor is such a write.
#[tokio::test]
async fn v2_history_cursor_expires_at_the_classification_horizon() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resource = ch_seed(&database, CH_BOUND + 1).await?;
    let mut saved = Vec::new();
    for route in ch_routes(resource) {
        // Newest first: the write at 225 anchors page size 1; page size 3 anchors the grant
        // side of the walk on the pointer at 210.
        for page_size in [1, 3] {
            let base = format!("{route}&page_size={page_size}");
            let first = ch_ok(&database, &base).await?;
            assert_eq!(
                bounded_route_hashes(&first)[0],
                "0xtx225",
                "{base}: {first}"
            );
            assert_eq!(first["page"]["total_count"], json!(4), "{base}: {first}");
            saved.push((route.clone(), base, hb_next_cursor(&first)?));
        }
    }

    ch_project_to(&database, CH_BOUND + 1).await?;
    for (route, base, cursor) in &saved {
        let fresh = ch_ok(&database, &format!("{route}&page_size=1")).await?;
        assert!(
            !bounded_route_hashes(&fresh).contains(&"0xtx225".to_owned()),
            "the later declaration must change the classification: {base}: {fresh}"
        );
        assert_eq!(
            ch_binding_identity(&hb_next_cursor(&fresh)?)?,
            ch_binding_identity(cursor)?,
            "{base}: the manifests and redo counters must not change"
        );
        let (status, payload) = ch_get(&database, &format!("{base}&cursor={cursor}")).await?;
        assert_eq!(status, StatusCode::CONFLICT, "{base}: {payload}");
        assert_eq!(payload["error"]["message"], json!(HB_RESTART), "{base}");
    }
    database.cleanup().await
}

/// A later declaration that Project has not reached leaves the classification alone, so the
/// saved cursors walk the same rows and count across the advance.
#[tokio::test]
async fn v2_history_cursor_continues_below_the_classification_horizon() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resource = ch_seed(&database, 10_000).await?;
    let mut walks = Vec::new();
    for route in ch_routes(resource) {
        for page_size in [1, 3] {
            let base = format!("{route}&page_size={page_size}");
            let first = ch_ok(&database, &base).await?;
            let cursor = hb_next_cursor(&first)?;
            let second = ch_ok(&database, &format!("{base}&cursor={cursor}")).await?;
            walks.push((base, cursor, second));
        }
    }

    ch_project_to(&database, CH_BOUND + 1).await?;
    for (base, cursor, second) in walks {
        let continued = ch_ok(&database, &format!("{base}&cursor={cursor}")).await?;
        assert_eq!(continued["data"], second["data"], "{base}");
        assert_eq!(continued["page"], second["page"], "{base}");
        assert_eq!(continued["meta"]["as_of"], second["meta"]["as_of"], "{base}");
    }
    database.cleanup().await
}

/// The cursor carries the horizon it was issued under. A horizon at or below the bound block is
/// malformed; any other value that differs from the one the manifests give for the bound
/// expires the cursor, as an edited redo counter does.
#[tokio::test]
async fn v2_history_cursor_checks_its_classification_horizon() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resource = ch_seed(&database, CH_BOUND + 1).await?;
    for route in ch_routes(resource) {
        let base = format!("{route}&page_size=1");
        let cursor = hb_next_cursor(&ch_ok(&database, &base).await?)?;
        let horizon = |value: Value| {
            hb_edit_cursor(&cursor, |payload| {
                payload["binding"]["chains"][BOUNDED_CHAIN]["classification_horizon"] = value;
            })
        };
        assert_eq!(
            serde_json::from_slice::<Value>(&hex::decode(&cursor)?)?["binding"]["chains"]
                [BOUNDED_CHAIN]["classification_horizon"],
            json!(CH_BOUND + 1),
            "{base}"
        );
        let (status, payload) =
            ch_get(&database, &format!("{base}&cursor={}", horizon(json!(CH_BOUND)))).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{base}: {payload}");
        for edited in [json!(null), json!(CH_BOUND + 2)] {
            let (status, payload) =
                ch_get(&database, &format!("{base}&cursor={}", horizon(edited))).await?;
            assert_eq!(status, StatusCode::CONFLICT, "{base}: {payload}");
            assert_eq!(payload["error"]["message"], json!(HB_RESTART), "{base}");
        }
        ch_ok(&database, &format!("{base}&cursor={cursor}")).await?;
    }
    database.cleanup().await
}

/// The horizon counts only what Project stages: the latest update of each active manifest. A
/// shadow manifest whose declaration starts between the bound and the recorded horizon leaves
/// the digest alone and must leave a saved cursor walking the same pages. (A superseded revision
/// that dropped such a declaration cannot be produced: manifest sync refuses to retire a
/// declaration before its start block.)
#[tokio::test]
async fn v2_history_horizon_ignores_manifests_project_does_not_stage() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resource = ch_seed(&database, 10_000).await?;
    let active = ch_manifest(10_000)?;
    let shadow = ch_with_declaration(&active, 5_000)?
        .replace("manifest_version = 1", "manifest_version = 2")
        .replace(r#"rollout_status = "active""#, r#"rollout_status = "shadow""#);

    let mut walks = Vec::new();
    for route in ch_routes(resource) {
        let base = format!("{route}&page_size=1");
        let cursor = hb_next_cursor(&ch_ok(&database, &base).await?)?;
        let second = ch_ok(&database, &format!("{base}&cursor={cursor}")).await?;
        walks.push((base, cursor, second));
    }
    ch_sync(&database, &[("v1.toml", active), ("v2.toml", shadow)]).await?;
    let shadow_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM bigname_phase.normalized_events
         WHERE event_kind = 'SourceManifestUpdated'
           AND after_state ->> 'rollout_status' = 'shadow'",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(shadow_events, 1, "the producer must record the shadow manifest");
    for (base, cursor, second) in &walks {
        let continued = ch_ok(&database, &format!("{base}&cursor={cursor}")).await?;
        assert_eq!(continued["data"], second["data"], "{base}");
        assert_eq!(continued["page"], second["page"], "{base}");
    }
    database.cleanup().await
}

/// Project's swap to the horizon is visible before its recorded position moves: while the phase
/// row still says `running` at the bound, and after a crash in that gap, when recovery labels the
/// row `completed` at the bound again (bigname: `apps/phase-runner/src/runner_recovery.rs:96-135`).
/// The attributed writes are already gone from the live classification, so a continuation must
/// restart rather than return changed rows, both when the swap lands before the request and
/// when it lands during the read.
#[tokio::test]
async fn v2_history_cursor_expires_when_the_swap_precedes_the_position() -> Result<()> {
    for crashed in [false, true] {
        let database = TestDatabase::new_migrated().await?;
        let resource = ch_seed(&database, CH_BOUND + 1).await?;
        let mut saved = Vec::new();
        for route in ch_routes(resource) {
            let base = format!("{route}&page_size=3");
            let first = ch_ok(&database, &base).await?;
            saved.push(format!("{base}&cursor={}", hb_next_cursor(&first)?));
        }

        // The swap lands during the read of the first continuation, after its admission.
        let (guard, control) = bigname_storage::history_anchor_read_test_hooks::install(
            &database.lookup_pool,
            bigname_storage::history_anchor_read_test_hooks::HistoryReadHookPoint::AfterAnchors,
        )
        .await?;
        let request = {
            let state = AppState::new_with_rpc_urls(
                database.lookup_pool.clone(),
                bigname_lookup::ChainRpcUrls::default(),
            );
            let uri = saved[0].clone();
            tokio::spawn(async move {
                let response = app_router(state)
                    .oneshot(Request::builder().uri(uri).body(Body::empty())?)
                    .await?;
                let status = response.status();
                anyhow::Ok((status, read_json::<Value>(response).await?))
            })
        };
        tokio::time::timeout(std::time::Duration::from_secs(10), control.wait_until_reached())
            .await
            .context("history request did not reach its read hook")?;
        ch_swap_to(&database, CH_BOUND + 1).await?;
        if !crashed {
            hb_project_running(&database).await?;
        }
        control.resume().await;
        let (status, payload) = request.await.context("request task")??;
        drop(guard);
        let shape = if crashed { "crashed" } else { "running" };
        assert_eq!(status, StatusCode::CONFLICT, "{shape} during the read: {payload}");
        assert_eq!(payload["error"]["message"], json!(HB_RESTART), "{shape}");

        // The swap already landed when the continuation arrives.
        for uri in &saved {
            let (status, payload) = ch_get(&database, uri).await?;
            assert_eq!(status, StatusCode::CONFLICT, "{shape}: {uri}: {payload}");
            assert_eq!(payload["error"]["message"], json!(HB_RESTART), "{shape}");
        }
        database.cleanup().await?;
    }
    Ok(())
}
