//! Database tests for a registry named by its mount path: a registry with two mounts and no
//! parent claim, and what a batch that touches a registry loads as more tokens point at it.
use super::*;

const TWO_MOUNTS: &str = "0x0000000000000000000000000000000000000774";
/// Block 2 is quiet and falls after mount `a` expires at `START + 15`.
const TWO_MOUNT_OFFSETS: [i64; 4] = [0, 10, 20, 30];

async fn point(seed: &mut Seeder<'_>, label: &str, target: &str) -> TestResult {
    let pointed = v2::SubregistryUpdated {
        tokenId: v2_token(label),
        subregistry: target.parse()?,
        sender: OWNER.parse()?,
    };
    seed.log(ETH_REGISTRY, pointed.encode_log_data()).await
}

async fn anchor_eth_registry(seed: &mut Seeder<'_>) -> TestResult {
    let parent = v2::ParentUpdated {
        parent: ROOT_REGISTRY.parse()?,
        label: "eth".to_owned(),
        sender: OWNER.parse()?,
    };
    seed.log(ETH_REGISTRY, parent.encode_log_data()).await
}

/// Block 0: the ETH registry's tokens `a` and `b` both point at one registry, and `a` expires
/// first. Block 1: that registry announces itself and registers `kid` with no parent claim.
/// Block 2: quiet, after `a` expired. Block 3: the registry registers `late`.
async fn seed_two_mounts(pool: &PgPool) -> TestResult {
    seed_lineage(pool, CHAIN, &TWO_MOUNT_OFFSETS).await?;
    let mut seed = seeder(pool);
    seed.block(FIRST_BLOCK).await?;
    anchor_eth_registry(&mut seed).await?;
    seed.register_v2("a", START + 15, OWNER, MIGRATED_ROLES)
        .await?;
    seed.register_v2("b", START + 10 * GRACE, OWNER, MIGRATED_ROLES)
        .await?;
    point(&mut seed, "a", TWO_MOUNTS).await?;
    point(&mut seed, "b", TWO_MOUNTS).await?;

    seed.block(FIRST_BLOCK + 1).await?;
    seed.log(TWO_MOUNTS, v2::RegistryCreated {}.encode_log_data())
        .await?;
    seed.register_v2_in(TWO_MOUNTS, "kid", START + 10 * GRACE, OWNER, MIGRATED_ROLES)
        .await?;

    seed.block(FIRST_BLOCK + 2).await?;
    seed.block(FIRST_BLOCK + 3).await?;
    seed.register_v2_in(
        TWO_MOUNTS,
        "late",
        START + 10 * GRACE,
        OWNER,
        MIGRATED_ROLES,
    )
    .await
}

/// A registry with two mounts of equal length and no claim is named by the mount with the
/// smaller label. When that mount expires in a block with no log, the name moves to the
/// other mount. Both loaders store the same rows at every batch size.
#[tokio::test]
async fn a_two_mount_registry_moves_to_the_other_mount_under_both_loaders() -> TestResult {
    let mut grids = Vec::new();
    for (blocks_per_batch, force_full_state) in [(1, false), (2, false), (500, false), (1, true)] {
        let database =
            database_with_manifests("interpret_lookahead_ensv2_two_mounts", "sepolia").await?;
        seed_two_mounts(database.pool()).await?;
        stamp_interpreter_hash(database.pool()).await?;
        let walk = walk_seeded(
            database.pool(),
            CHAIN,
            &TWO_MOUNT_OFFSETS,
            blocks_per_batch,
            force_full_state,
        )
        .await?;
        database.cleanup().await?;
        grids.push((blocks_per_batch, walk.stored));
    }
    let (_, full_state) = grids.pop().expect("forced full-state run");
    let rows: Vec<serde_json::Value> = full_state
        .iter()
        .map(|row| serde_json::from_str(row))
        .collect::<Result<_, _>>()?;
    let under =
        |mount: &str, label: &str| format!("ens:{:#x}", child(child(eth_node(), mount), label));
    let blocks = |name: &str, kind: &str| {
        rows.iter()
            .filter(|row| row["event_kind"] == kind && row["logical_name_id"] == name)
            .map(|row| row["block_number"].as_i64().expect("block number") - FIRST_BLOCK)
            .collect::<BTreeSet<_>>()
    };
    let granted = "RegistrationGranted";
    assert_eq!(blocks(&under("a", "kid"), granted), BTreeSet::from([1]));
    assert_eq!(
        blocks(&under("a", "kid"), "RegistrationReleased"),
        BTreeSet::from([2])
    );
    assert_eq!(blocks(&under("b", "kid"), granted), BTreeSet::from([2]));
    assert_eq!(blocks(&under("b", "late"), granted), BTreeSet::from([3]));
    assert!(blocks(&under("a", "late"), granted).is_empty());
    for (blocks_per_batch, stored) in grids {
        assert_eq!(
            stored, full_state,
            "lookahead at {blocks_per_batch} blocks per batch differs from full state"
        );
    }
    Ok(())
}

const POINTED_AT: &str = "0x0000000000000000000000000000000000000775";
const ELSEWHERE: &str = "0x0000000000000000000000000000000000000776";
const POINTER_OFFSETS: [i64; 3] = [0, 10, 20];

/// Block 0: `tokens` tokens in the ETH registry, each pointed at one registry and then
/// pointed away and back `repoints` times. Block 1: that registry announces itself and
/// registers `kid`. Block 2: it registers `late`, which touches the registry alone.
async fn seed_pointers(pool: &PgPool, tokens: usize, repoints: usize) -> TestResult {
    seed_lineage(pool, CHAIN, &POINTER_OFFSETS).await?;
    let mut seed = seeder(pool);
    seed.block(FIRST_BLOCK).await?;
    anchor_eth_registry(&mut seed).await?;
    seed.register_v2_labels(ETH_REGISTRY, "tok", tokens).await?;
    for index in 0..tokens {
        let label = format!("tok{index}");
        point(&mut seed, &label, POINTED_AT).await?;
        for _ in 0..repoints {
            point(&mut seed, &label, ELSEWHERE).await?;
            point(&mut seed, &label, POINTED_AT).await?;
        }
    }
    seed.block(FIRST_BLOCK + 1).await?;
    seed.log(POINTED_AT, v2::RegistryCreated {}.encode_log_data())
        .await?;
    seed.register_v2_in(POINTED_AT, "kid", START + 10 * GRACE, OWNER, MIGRATED_ROLES)
        .await?;
    seed.block(FIRST_BLOCK + 2).await?;
    seed.register_v2_in(
        POINTED_AT,
        "late",
        START + 10 * GRACE,
        OWNER,
        MIGRATED_ROLES,
    )
    .await
}

/// The events a batch restores when it touches a registry that `tokens` tokens point at,
/// each with `repoints` earlier pointer changes.
async fn restored_for_a_touch(tokens: usize, repoints: usize) -> TestResult<usize> {
    let database = database_with_manifests("interpret_lookahead_ensv2_pointers", "sepolia").await?;
    seed_pointers(database.pool(), tokens, repoints).await?;
    stamp_interpreter_hash(database.pool()).await?;
    super::super::LOADED_BATCHES.take();
    walk_seeded(database.pool(), CHAIN, &POINTER_OFFSETS, 1, false).await?;
    database.cleanup().await?;
    let (_, kinds) = super::super::LOADED_BATCHES
        .take()
        .remove(&(FIRST_BLOCK + 2))
        .expect("the touched batch loads through lookahead");
    Ok(kinds.len())
}

/// A pointer event is filed under the registry it names, so a batch that touches a registry
/// also restores the tokens that point at it. What it restores grows in step with the number
/// of those tokens: each one adds the same number of restored events. Earlier pointer changes
/// on the same token add nothing, because a restore keeps the latest state of each key.
#[tokio::test]
async fn a_touched_registry_restores_in_step_with_the_tokens_that_point_at_it() -> TestResult {
    let mut measured = Vec::new();
    for (tokens, repoints) in [(1, 0), (5, 0), (25, 0), (5, 2), (5, 10)] {
        let restored = restored_for_a_touch(tokens, repoints).await?;
        measured.push(((tokens, repoints), restored));
    }
    let restored = |at: (usize, usize)| {
        let found = measured.iter().find(|(key, _)| *key == at);
        i64::try_from(found.expect("measured").1).expect("count fits")
    };
    // Twenty more tokens cost five times what four more tokens cost.
    let per_four = restored((5, 0)) - restored((1, 0));
    assert!(per_four > 0, "{measured:?}");
    assert_eq!(
        restored((25, 0)) - restored((5, 0)),
        5 * per_four,
        "{measured:?}"
    );
    assert_eq!(restored((5, 2)), restored((5, 0)), "{measured:?}");
    assert_eq!(restored((5, 10)), restored((5, 0)), "{measured:?}");
    Ok(())
}
