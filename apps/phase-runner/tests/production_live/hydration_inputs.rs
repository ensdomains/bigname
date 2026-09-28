//! Event-driven hydration fixtures. Publication uses the production Project phase.
use super::*;

pub(super) const TEXT_RESOLVER: &str = "0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41";
pub(super) const TEXT_NODE: &str =
    "0x1111111111111111111111111111111111111111111111111111111111111111";

pub(super) fn reverse_node(address: &str) -> String {
    let labels = [address.trim_start_matches("0x"), "addr", "reverse"];
    let hash = labels
        .iter()
        .rev()
        .fold(alloy_primitives::B256::ZERO, |parent, label| {
            let label = alloy_primitives::keccak256(label.as_bytes());
            alloy_primitives::keccak256([parent.as_slice(), label.as_slice()].concat())
        });
    format!("{hash:#x}")
}

pub(super) fn urls(endpoint: &str) -> Result<ChainRpcUrls> {
    ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={endpoint}")])
}

pub(super) async fn setup(label: &str, through: i64) -> Result<ScratchDatabase> {
    let db = ScratchDatabase::create(label).await?;
    seed_branch(db.pool(), ETHEREUM, 1, through, None).await?;
    publish(db.pool(), ETHEREUM, 1, 0, 0, 0).await?;
    project(db.pool(), None, 1, 0, false).await?;
    Ok(db)
}

pub(super) async fn project(
    pool: &PgPool,
    rpc: Option<ChainRpcUrls>,
    branch: u64,
    block: i64,
    rebuild: bool,
) -> Result<()> {
    let current: Option<(i64, String)> = sqlx::query_as("SELECT current_block_number, current_block_hash FROM project_family_marker WHERE chain_id = $1")
        .bind(ETHEREUM).fetch_optional(pool).await?;
    let phase = match rpc {
        Some(urls) => ProjectPhase::with_hydration(pool.clone(), urls),
        None => ProjectPhase::new(pool.clone()),
    };
    let outcome = phase
        .run_batch(PhaseContext {
            chain_id: ETHEREUM.into(),
            phase: PhaseName::Project,
            mode: RunMode::Normal,
            redo_attempt: None,
            sources: Arc::from([]),
            available_heads: Some(HeadMarkers {
                latest: BlockMarker::new(block, block_hash(branch, block))?,
                safe: None,
                finalized: None,
            }),
            live_handoff: None,
            resume: PhaseResume {
                current: if rebuild {
                    None
                } else {
                    current.map(|(number, hash)| BlockMarker { number, hash })
                },
                ..Default::default()
            },
        })
        .await?;
    let PhaseBatchOutcome::Complete(progress) = outcome else {
        anyhow::bail!("bounded fixture publication did not finish")
    };
    assert_eq!(
        progress.current,
        Some(BlockMarker::new(block, block_hash(branch, block))?)
    );
    Ok(())
}

pub(super) async fn follow(pool: &PgPool, rpc: &str, branch: u64, block: i64) -> Result<()> {
    publish(pool, ETHEREUM, branch, block, 0, 0).await?;
    project(pool, Some(urls(rpc)?), branch, block, false).await
}

pub(super) async fn event(
    pool: &PgPool,
    block: i64,
    kind: &str,
    family: &str,
    after: Value,
    name: Option<&str>,
    resource: Option<Uuid>,
) -> Result<()> {
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,derivation_kind,canonicality_state,after_state,raw_fact_ref,logical_name_id,resource_id) VALUES ($1,'ens',$2,$3,1,$4,$5,$6,'ens_v1_unwrapped_authority','canonical',$7,$8,$9,$10)")
        .bind(format!("hydration:{block}:{kind}:{}", Uuid::new_v4())).bind(kind).bind(family).bind(ETHEREUM).bind(block).bind(block_hash(1,block))
        .bind(&after).bind(json!({"emitting_address":after["resolver"]})).bind(name).bind(resource).execute(pool).await?;
    Ok(())
}

pub(super) async fn seed_reverse(pool: &PgPool, address: &str) -> Result<()> {
    let node = reverse_node(address);
    event(pool, 1, "ReverseChanged", "ens_v1_reverse_l1", json!({"source_event":"ReverseClaimed","address":address,"coin_type":"60","namespace":"ens","reverse_node":node}), None, None).await?;
    event(
        pool,
        1,
        "ResolverChanged",
        "ens_v1_registry_l1",
        json!({"source_event":"NewResolver","node":node,"resolver":REVERSE_RESOLVER}),
        None,
        None,
    )
    .await
}

pub(super) async fn seed_page(pool: &PgPool) -> Result<()> {
    for index in 1..=251 {
        seed_reverse(pool, &format!("0x{index:040x}")).await?;
    }
    publish(pool, ETHEREUM, 1, 1, 0, 0).await?;
    project(pool, None, 1, 1, true).await
}

pub(super) async fn primary(
    pool: &PgPool,
    address: &str,
) -> Result<bigname_storage::PrimaryNameCurrentSnapshot> {
    load_primary_name_current_snapshot(pool, address, "ens", "60")
        .await?
        .context("published reverse tuple")
}

pub(super) async fn assert_primary(
    pool: &PgPool,
    address: &str,
    status: &str,
    name: Option<&str>,
    hash: Option<&str>,
) -> Result<()> {
    let claim = primary(pool, address).await?;
    assert_eq!(claim.row.claim_status.as_str(), status);
    assert_eq!(claim.row.raw_claim_name.as_deref(), name);
    assert_eq!(
        claim
            .row
            .claim_provenance
            .pointer("/canonical_head_multicall_hydration/block_hash")
            .and_then(Value::as_str),
        hash
    );
    Ok(())
}

pub(super) async fn seed_text(pool: &PgPool) -> Result<Uuid> {
    let resource = Uuid::new_v4();
    let name = format!("ens:{TEXT_NODE}");
    sqlx::query("INSERT INTO resources (resource_id,chain_id,block_hash,block_number,canonicality_state) VALUES ($1,$2,$3,1,'canonical')")
        .bind(resource).bind(ETHEREUM).bind(block_hash(1,1)).execute(pool).await?;
    sqlx::query("INSERT INTO name_surfaces (logical_name_id,namespace,raw_name,raw_labels,dns_encoded_name,namehash,labelhashes,normalizer_version,visibility_state,chain_id,block_hash,block_number,canonicality_state) VALUES ($1,'ens','alice.eth',ARRAY['alice','eth'],decode('05616c6963650365746800','hex'),$2,ARRAY['0xalice','0xeth'],'ensip15@ens-normalize-0.1.1','active',$3,$4,1,'canonical')")
        .bind(&name).bind(TEXT_NODE).bind(ETHEREUM).bind(block_hash(1,1)).execute(pool).await?;
    let payload = json!({"contracts":[{"address":TEXT_RESOLVER,"role":"public_resolver","read_features":["text"]}]});
    let id: i64 = sqlx::query_scalar("INSERT INTO manifest_versions (manifest_version,namespace,source_family,chain_id,deployment_label,rollout_status,normalizer_version,file_path,manifest_payload) VALUES (1,'ens','ens_v1_resolver_l1',$1,'fixture','active','fixture','fixture/text.yaml',$2) RETURNING manifest_id")
        .bind(ETHEREUM).bind(&payload).fetch_one(pool).await?;
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,source_manifest_id,chain_id,block_number,block_hash,derivation_kind,canonicality_state,after_state) VALUES ('text-manifest','ens','SourceManifestUpdated','ens_v1_resolver_l1',1,$1,$2,1,$3,'manifest_sync','canonical',$4)")
        .bind(id).bind(ETHEREUM).bind(block_hash(1,1)).bind(json!({"rollout_status":"active","manifest_payload":payload})).execute(pool).await?;
    event(
        pool,
        1,
        "ResolverChanged",
        "ens_v1_registry_l1",
        json!({"node":TEXT_NODE,"resolver":TEXT_RESOLVER}),
        Some(&name),
        Some(resource),
    )
    .await?;
    text_change(pool, 1).await?;
    Ok(resource)
}

pub(super) async fn text_change(pool: &PgPool, block: i64) -> Result<()> {
    event(pool,block,"RecordChanged","ens_v1_resolver_l1",json!({"node":TEXT_NODE,"resolver":TEXT_RESOLVER,"record_key":"text:url","record_family":"text","selector_key":"url","source_event":"TextChanged"}),None,None).await
}

pub(super) async fn text_entry(pool: &PgPool, resource: Uuid) -> Result<Value> {
    let inventory = bigname_storage::families::records::load_family_record_inventory_detail(
        pool,
        ETHEREUM,
        resource,
        bigname_storage::families::records::FamilyAttribution::Given(Default::default()),
    )
    .await?
    .context("published text inventory")?;
    inventory
        .row
        .entries
        .as_array()
        .and_then(|entries| entries.first())
        .cloned()
        .context("text entry")
}
