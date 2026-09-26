use super::*;

// The child dual-current assertion compares an ENSv1 parent-child relation against the child's
// activated `MigrationApplied` position, not against the published authority epoch start, which
// is the child's ENSv2 binding (Pro review of PR 953, question 4). These cases put the relation
// before, at, between and after the two positions, in both orders, at block boundaries without
// transaction or log indices, and through full and incremental rebuilds.

const REGISTRY_ADDRESS: &str = "0x0000000000000000000000000000000000000590";
const LATER_HASH: &str = "0x50311";

type Position = (i64, Option<i64>, Option<i64>);
/// (case, binding, migration, relation, halts)
type CutoffCase = (&'static str, (i64, i64, i64), Position, Position, bool);

fn block_hash_of(block: i64) -> &'static str {
    match block {
        9 => EARLIER_HASH,
        10 => HASH,
        _ => LATER_HASH,
    }
}

/// Moves a fixture event to `position`, before Project reads it.
async fn place(pool: &PgPool, identity: &str, (block, tx, log): Position) -> Result<()> {
    sqlx::query("UPDATE normalized_events SET block_number = $2, block_hash = $3, transaction_hash = CASE WHEN $4::bigint IS NULL THEN NULL ELSE transaction_hash END, transaction_index = $4, log_index = $5 WHERE event_identity = $1")
        .bind(identity).bind(block).bind(block_hash_of(block)).bind(tx).bind(log).execute(pool).await?;
    Ok(())
}

async fn later_block(pool: &PgPool) -> Result<()> {
    sqlx::query("INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state) VALUES ($1, $2, 11, '2026-08-26T00:00:12Z', 'canonical')")
        .bind(CHAIN).bind(LATER_HASH).execute(pool).await?;
    Ok(())
}

/// A parent whose ENSv2 subregistry is an admitted registry, and a child registered there with its
/// ENSv2 binding at `binding` and an activated migration at `migration`. Returns (parent, child).
async fn seed(
    pool: &PgPool,
    binding: (i64, i64, i64),
    migration: Position,
) -> Result<(String, String)> {
    earlier_block(pool).await?;
    let parent = surface(pool, 90, "cutoff.eth", &["ens_v2"]).await?;
    let child = surface(pool, 91, "child.cutoff.eth", &[]).await?;
    let registry = uuid(8, 90);
    sqlx::query("INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind) VALUES ($1::uuid, $2, 'contract')")
        .bind(&registry).bind(CHAIN).execute(pool).await?;
    sqlx::query("INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address, active_from_block_number) VALUES ($1::uuid, $2, $3, 9)")
        .bind(&registry).bind(CHAIN).bind(REGISTRY_ADDRESS).execute(pool).await?;
    let (block, tx, log) = binding;
    let resource = uuid(1, 91);
    sqlx::query("INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid, $2, $3, $4, 'canonical')")
        .bind(&resource).bind(CHAIN).bind(block_hash_of(block)).bind(block).execute(pool).await?;
    sqlx::query("INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id, binding_kind, authority_arm, active_from, chain_id, block_hash, block_number, provenance, canonicality_state) VALUES ($1::uuid, $2, $3::uuid, 'declared_registry_path', 'ens_v2', '2026-08-25T00:00:00Z', $4, $5, $6, jsonb_build_object('transaction_index', $7::bigint, 'log_index', $8::bigint), 'canonical')")
        .bind(uuid(3, 91)).bind(&child).bind(&resource).bind(CHAIN).bind(block_hash_of(block)).bind(block).bind(tx).bind(log).execute(pool).await?;
    event(
        pool,
        "cutoff-parent-registry",
        &parent,
        None,
        Event {
            family: "ens_v2_registry_l1",
            kind: "SubregistryChanged",
            log: 0,
            after: json!({"subregistry":REGISTRY_ADDRESS}),
        },
    )
    .await?;
    place(pool, "cutoff-parent-registry", (9, Some(0), Some(0))).await?;
    event(
        pool,
        "cutoff-child-registration",
        &child,
        Some(&resource),
        Event {
            family: "ens_v2_registry_l1",
            kind: "RegistrationGranted",
            log,
            after: json!({"registry_contract_instance_id":registry,"status":"registered","registrant":"0x0000000000000000000000000000000000000001"}),
        },
    )
    .await?;
    place(
        pool,
        "cutoff-child-registration",
        (block, Some(tx), Some(log)),
    )
    .await?;
    event(
        pool,
        "cutoff-child-migration",
        &child,
        None,
        Event {
            family: "ens_v2_migration_l1",
            kind: "MigrationApplied",
            log: 0,
            after: json!({"migration_path":"locked_wrapped","successor_binding":{"binding_id":uuid(3, 91),"resource_id":resource}}),
        },
    )
    .await?;
    place(pool, "cutoff-child-migration", migration).await?;
    Ok((parent, child))
}

/// The child's ENSv1 relation under the parent, at `position`.
async fn relation(pool: &PgPool, parent: &str, child: &str, position: Position) -> Result<()> {
    event(
        pool,
        "cutoff-v1-relation",
        child,
        None,
        Event {
            family: "ens_v1_registry_l1",
            kind: "SubregistryChanged",
            log: 0,
            after: json!({"node":parent.trim_start_matches("ens:"),"child_node":child.trim_start_matches("ens:"),"labelhash":labelhash("child"),"owner":"0x0000000000000000000000000000000000000002"}),
        },
    )
    .await?;
    place(pool, "cutoff-v1-relation", position).await
}

async fn project(
    pool: &PgPool,
    target: i64,
    previous: Option<bigname_project::Marker>,
) -> bigname_project::Result<bigname_project::Marker> {
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.into(),
            target_block: target,
            affected_from_block: previous.as_ref().map_or(9, |_| 11),
            affected_to_block: target,
            resume_current: previous,
            mode: RunMode::Normal,
        })
        .await
        .map(|batch| batch.current)
}

fn position_json((block, tx, log): Position) -> Value {
    json!({"block_number": block, "transaction_index": tx.unwrap_or(-1), "log_index": log.unwrap_or(-1)})
}

/// The published child relation's arm for the pair.
async fn published_arm(pool: &PgPool, parent: &str, child: &str) -> Result<Option<String>> {
    Ok(sqlx::query_scalar("SELECT CASE WHEN registrant IS NULL THEN 'ens_v1' ELSE 'ens_v2' END FROM children_current WHERE parent_logical_name_id = $1 AND child_logical_name_id = $2")
        .bind(parent).bind(child).fetch_optional(pool).await?)
}

fn assert_halt(
    case: &str,
    outcome: bigname_project::Result<bigname_project::Marker>,
    child: &str,
    cutoff: Position,
    epoch: Position,
) -> Result<()> {
    let error = match outcome {
        Ok(_) => anyhow::bail!("{case}: a post-migration ENSv1 relation must halt"),
        Err(error) => error,
    };
    let evidence = error
        .generation_failure_evidence()
        .with_context(|| format!("{case}: failure evidence"))?;
    assert_eq!(
        evidence.failure_kind, DUAL_CURRENT_CHILD_AUTHORITY,
        "{case}"
    );
    assert_eq!(evidence.logical_name_id, child, "{case}");
    assert_eq!(
        evidence.payload["integrity_cutoff_position"],
        position_json(cutoff),
        "{case}: the cutoff is the migration position"
    );
    assert_eq!(
        evidence.payload["authority_epoch_start_position"],
        position_json(epoch),
        "{case}: the published epoch stays at the binding"
    );
    assert_eq!(
        evidence.payload["predecessor"]["event_identity"], "cutoff-v1-relation",
        "{case}"
    );
    Ok(())
}

#[tokio::test]
async fn child_integrity_cutoff_is_the_migration_position_in_full_rebuilds() -> Result<()> {
    let some = |block: i64, tx: i64, log: i64| (block, Some(tx), Some(log));
    let cases: [CutoffCase; 9] = [
        (
            "before both",
            (10, 0, 2),
            some(10, 0, 5),
            some(10, 0, 1),
            false,
        ),
        (
            "at the binding",
            (10, 0, 2),
            some(10, 0, 5),
            some(10, 0, 2),
            false,
        ),
        // A binding-based cutoff would halt here.
        (
            "between binding and migration",
            (10, 0, 2),
            some(10, 0, 5),
            some(10, 0, 3),
            false,
        ),
        (
            "at the migration",
            (10, 0, 2),
            some(10, 0, 5),
            some(10, 0, 5),
            false,
        ),
        (
            "after both",
            (10, 0, 2),
            some(10, 0, 5),
            some(10, 0, 7),
            true,
        ),
        // A binding-based cutoff would publish these two.
        (
            "between migration and binding",
            (10, 0, 5),
            some(10, 0, 2),
            some(10, 0, 3),
            true,
        ),
        (
            "at the later binding",
            (10, 0, 5),
            some(10, 0, 2),
            some(10, 0, 5),
            true,
        ),
        (
            "block boundary in the migration block",
            (9, 0, 2),
            some(9, 0, 5),
            (9, None, None),
            false,
        ),
        (
            "block boundary after the migration block",
            (9, 0, 2),
            some(9, 0, 5),
            (10, None, None),
            true,
        ),
    ];
    for (index, (case, binding, migration, at, halts)) in cases.into_iter().enumerate() {
        let (db, pool) = database(&format!("child_cutoff_full_{index}")).await?;
        let (parent, child) = seed(&pool, binding, migration).await?;
        relation(&pool, &parent, &child, at).await?;
        let outcome = project(&pool, 10, None).await;
        if halts {
            let epoch = (binding.0, Some(binding.1), Some(binding.2));
            assert_halt(case, outcome, &child, migration, epoch)?;
        } else {
            outcome.with_context(|| format!("{case}: pre-migration residue must publish"))?;
            assert_eq!(
                authority(&pool, &child).await?.0.as_deref(),
                Some("ens_v2"),
                "{case}"
            );
            assert_eq!(
                published_arm(&pool, &parent, &child).await?.as_deref(),
                Some("ens_v2"),
                "{case}: the ENSv2 relation is published and the ENSv1 one is residue"
            );
        }
        db.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn child_integrity_cutoff_is_the_migration_position_in_incremental_rebuilds() -> Result<()> {
    // A relation between the binding and the migration is residue; a later incremental block that
    // touches the child rebuilds it with the same cutoff and still publishes, as a full rebuild at
    // the same head does.
    let (db, pool) = database("child_cutoff_incremental_residue").await?;
    let (parent, child) = seed(&pool, (9, 0, 2), (9, Some(0), Some(5))).await?;
    relation(&pool, &parent, &child, (9, Some(0), Some(3))).await?;
    later_block(&pool).await?;
    let prefix = project(&pool, 10, None).await?;
    let renewal = event(
        &pool,
        "cutoff-child-renewal",
        &child,
        Some(&uuid(1, 91)),
        Event {
            family: "ens_v2_registry_l1",
            kind: "RegistrationRenewed",
            log: 0,
            after: json!({"status":"registered","expiry":4_000_000_000_i64}),
        },
    )
    .await?;
    sqlx::query("UPDATE normalized_events SET block_number = 11, block_hash = $2, transaction_index = 0, log_index = 1 WHERE normalized_event_id = $1")
        .bind(renewal).bind(LATER_HASH).execute(&pool).await?;
    project(&pool, 11, Some(prefix)).await?;
    assert_eq!(
        published_arm(&pool, &parent, &child).await?.as_deref(),
        Some("ens_v2")
    );
    project(&pool, 11, None).await?;
    assert_eq!(
        published_arm(&pool, &parent, &child).await?.as_deref(),
        Some("ens_v2")
    );
    db.cleanup().await?;

    // An ENSv1 relation that arrives in a later block at the block boundary halts the incremental
    // rebuild and the full rebuild at the same head alike.
    let (db, pool) = database("child_cutoff_incremental_halt").await?;
    let (parent, child) = seed(&pool, (9, 0, 2), (9, Some(0), Some(5))).await?;
    later_block(&pool).await?;
    let prefix = project(&pool, 10, None).await?;
    relation(&pool, &parent, &child, (11, None, None)).await?;
    let epoch = (9, Some(0), Some(2));
    let cutoff = (9, Some(0), Some(5));
    assert_halt(
        "incremental",
        project(&pool, 11, Some(prefix)).await,
        &child,
        cutoff,
        epoch,
    )?;
    assert_halt(
        "full",
        project(&pool, 11, None).await,
        &child,
        cutoff,
        epoch,
    )?;
    db.cleanup().await?;
    Ok(())
}
