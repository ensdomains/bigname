#[tokio::test]
async fn registry_history_survives_real_manifest_child_replacement() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let root = std::env::temp_dir().join(format!("bigname-registry-history-{}", Uuid::new_v4()));
    let directory = root.join("ethereum/ens/ens_v2_registry_l1");
    std::fs::create_dir_all(&directory)?;
    let path = directory.join("v1.toml");
    registry_history_head(&database.pool, 60).await?;
    sync_registry_history_manifest(&database.pool, &root, &path, Some(55), "registry").await?;
    assert_registry_history(&database.pool, &[(54, false), (55, true), (60, true)]).await?;

    // Actual same-version synchronization removes both the root and contract children,
    // preserving one unrelated declaration and the removed address's finite interval.
    sync_registry_history_manifest(&database.pool, &root, &path, None, "registry").await?;
    let children: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM bigname_phase.manifest_contract_instances WHERE declared_address = $1",
    ).bind(DECLARED_REGISTRY).fetch_one(&database.pool).await?;
    assert_eq!(
        children, 0,
        "fixture must exercise deleted declaration children"
    );
    let retired: (Option<i64>, Option<i64>, bool) = sqlx::query_as(
        "SELECT active_from_block_number, active_to_block_number, deactivated_at IS NOT NULL
         FROM bigname_phase.contract_instance_addresses WHERE address = $1",
    )
    .bind(DECLARED_REGISTRY)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(retired, (Some(55), Some(60), true));
    assert_registry_history(
        &database.pool,
        &[(54, false), (55, true), (60, true), (61, false)],
    )
    .await?;
    assert!(
        bigname_storage::load_registry_contract(
            &database.pool,
            REGISTRY_CHAIN_ID,
            DECLARED_REGISTRY,
            None
        )
        .await?
        .is_none()
    );
    assert!(
        bigname_storage::load_registry_contract(
            &database.pool,
            REGISTRY_CHAIN_ID,
            OTHER_EMITTER,
            Some(60)
        )
        .await?
        .is_none(),
        "a non-registry role is not a registry"
    );

    // A retraction without a retained end is not historical admission, even though the
    // manifest event still contains the old declaration.
    sqlx::query(
        "UPDATE bigname_phase.contract_instance_addresses SET active_to_block_number = NULL,
                    active_to_block_hash = NULL WHERE address = $1 AND deactivated_at IS NOT NULL",
    )
    .bind(DECLARED_REGISTRY)
    .execute(&database.pool)
    .await?;
    assert_registry_history(&database.pool, &[(55, false), (60, false)]).await?;
    sqlx::query("UPDATE bigname_phase.contract_instance_addresses SET active_to_block_number = 60 WHERE address = $1 AND deactivated_at IS NOT NULL")
        .bind(DECLARED_REGISTRY).execute(&database.pool).await?;
    registry_history_head(&database.pool, 70).await?;
    sync_registry_history_manifest(&database.pool, &root, &path, Some(70), "registry").await?;
    assert_registry_history(
        &database.pool,
        &[
            (54, false),
            (55, true),
            (60, true),
            (61, false),
            (69, false),
            (70, true),
        ],
    )
    .await?;
    std::fs::remove_dir_all(root)?;
    database.cleanup().await
}

#[tokio::test]
async fn registry_history_does_not_reclassify_an_earlier_non_registry_admission() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let root =
        std::env::temp_dir().join(format!("bigname-registry-role-history-{}", Uuid::new_v4()));
    let directory = root.join("ethereum/ens/ens_v2_registry_l1");
    std::fs::create_dir_all(&directory)?;
    let path = directory.join("v1.toml");
    registry_history_head(&database.pool, 60).await?;
    sync_registry_history_manifest(&database.pool, &root, &path, Some(55), "unrelated").await?;
    // Keep only the root briefly, so retirement snapshots its name, not the unrelated
    // contract role. Reusing this root name later must still not reclassify the old interval.
    let manifest = std::fs::read_to_string(&path)?;
    let (root_fields, contracts) = manifest
        .split_once("[[contracts]]")
        .expect("target contract");
    let (_, control) = contracts
        .split_once("[[contracts]]")
        .expect("control contract");
    std::fs::write(&path, format!("{root_fields}[[contracts]]{control}"))?;
    bigname_manifests::sync_schema_v2_repository(
        &database.pool,
        &bigname_manifests::load_repository(&root)?,
    )
    .await?;
    sync_registry_history_manifest(&database.pool, &root, &path, None, "unrelated").await?;
    let kind: String = sqlx::query_scalar(
        "SELECT provenance ->> 'declaration_kind'
        FROM bigname_phase.contract_instance_addresses WHERE address = $1",
    )
    .bind(DECLARED_REGISTRY)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(kind, "root");
    registry_history_head(&database.pool, 70).await?;
    sync_registry_history_manifest(&database.pool, &root, &path, Some(70), "registry").await?;
    assert_registry_history(
        &database.pool,
        &[(55, false), (60, false), (69, false), (70, true)],
    )
    .await?;
    std::fs::remove_dir_all(root)?;
    database.cleanup().await
}

async fn assert_registry_history(pool: &PgPool, expectations: &[(i64, bool)]) -> Result<()> {
    for &(block, expected) in expectations {
        let row = bigname_storage::load_registry_contract(
            pool,
            REGISTRY_CHAIN_ID,
            DECLARED_REGISTRY,
            Some(block),
        )
        .await?;
        assert_eq!(
            row.is_some(),
            expected,
            "registry declaration at block {block}"
        );
    }
    Ok(())
}

async fn registry_history_head(pool: &PgPool, number: i64) -> Result<()> {
    let hash = format!("registry-history-{number}");
    sqlx::query(
        "INSERT INTO bigname_phase.chain_lineage
        (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
        VALUES ($1, $2, $3, to_timestamp($3), 'canonical')",
    )
    .bind(REGISTRY_CHAIN_ID)
    .bind(&hash)
    .bind(number)
    .execute(pool)
    .await?;
    sqlx::query("INSERT INTO bigname_phase.chain_heads (chain_id, latest_block_hash, latest_block_number)
        VALUES ($1, $2, $3) ON CONFLICT (chain_id) DO UPDATE
        SET latest_block_hash = EXCLUDED.latest_block_hash, latest_block_number = EXCLUDED.latest_block_number")
        .bind(REGISTRY_CHAIN_ID).bind(hash).bind(number).execute(pool).await?;
    Ok(())
}

async fn sync_registry_history_manifest(
    pool: &PgPool,
    root: &std::path::Path,
    path: &std::path::Path,
    start: Option<i64>,
    role: &str,
) -> Result<()> {
    let registry = start
        .map(|start| {
            format!(
                r#"
[[roots]]
name = "RootRegistry"
address = "{DECLARED_REGISTRY}"
start_block = {start}
[[contracts]]
role = "{role}"
address = "{DECLARED_REGISTRY}"
proxy_kind = "none"
start_block = {start}
"#
            )
        })
        .unwrap_or_default();
    let roots = if start.is_none() { "roots = []" } else { "" };
    std::fs::write(
        path,
        format!(
            r#"manifest_version = 1
namespace = "ens"
source_family = "ens_v2_registry_l1"
chain = "{REGISTRY_CHAIN_ID}"
deployment_epoch = "fixture"
rollout_status = "active"
normalizer_version = "ensip15@ens-normalize-0.1.1"
discovery_rules = []
{roots}
[capability_flags]
{registry}
[[contracts]]
role = "control"
address = "{OTHER_EMITTER}"
proxy_kind = "none"
start_block = 0
[[abi.events]]
name = "LabelRegistered"
fragment = "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)"
emitter_roles = ["control"]
normalized_events = ["RegistrationGranted"]
status = "supported"
"#
        ),
    )?;
    let repository = bigname_manifests::load_repository(root)?;
    bigname_manifests::sync_schema_v2_repository(pool, &repository).await?;
    Ok(())
}
