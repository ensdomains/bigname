//! A served mirror name that loses its exact ENSv1 resolver and falls back to a non-extended
//! ancestor stops serving records, including its address record, and gets them back when the
//! exact node is pointed at a resolver again. Every execution shape agrees on each state, and
//! the bounded history reader follows the same transitions.
use std::collections::BTreeSet;

use super::ancestor_gate::assert_unsupported_mirror_row;
use super::*;

/// Blocks, with `base = 10`:
/// - 10: the ENSv1 registry points `mirror.fixture` at `V1_RESOLVER` and `fixture` at the
///   declared, non-extended `PARENT_RESOLVER`; the ENSv2 resource of `mirror.fixture` points at
///   the mirror and is the name's serving resource.
/// - 11: `V1_RESOLVER` stores `text:url` and `addr:60` for `mirror.fixture`; `PARENT_RESOLVER`
///   stores `text:description` for it too.
/// - 12: the registry clears `mirror.fixture`, so the walk selects `fixture`, which the mirror
///   rejects.
/// - 13: `V1_RESOLVER` rewrites `addr:60` for `mirror.fixture` while nothing points there.
/// - 14: the registry points `mirror.fixture` at `V1_RESOLVER` again.
const LAST: i64 = 14;

async fn seed_transition(pool: &PgPool, fixture: &Fixture) -> Result<()> {
    seed(pool, fixture).await?;
    extend_chain(pool, fixture.base + 4, LAST).await?;
    let node = bigname_lookup::ens_namehash_hex(NAME)?;
    let logical_name_id = format!("ens:{node}");
    insert_event(
        pool,
        fixture,
        record(
            "v1-addr-early",
            fixture.base + 1,
            2,
            &node,
            V1_RESOLVER,
            manifest_id(pool, "ens_v1_resolver_l1").await?,
            json!({"record_key": "addr:60", "record_family": "addr", "selector_key": "60",
                   "source_event": "AddrChanged", "value": ADDRESS}),
        ),
    )
    .await?;
    insert_event(
        pool,
        fixture,
        Event {
            identity: "v1-repoint",
            logical_name_id: Some(logical_name_id),
            resource_id: Some(V1_RESOURCE),
            kind: "ResolverChanged",
            source_family: "ens_v1_registry_l1",
            manifest_id: None,
            block: LAST,
            log_index: 0,
            emitter: V1_REGISTRY,
            after_state: json!({"node": node, "resolver": V1_RESOLVER}),
        },
    )
    .await?;
    serve_through_ensv2_arm(pool, fixture).await
}

/// What each target must publish for the mirrored row, its address index row and history.
async fn assert_state(pool: &PgPool, fixture: &Fixture, target: i64, label: &str) -> Result<()> {
    let v2 = inventory(pool, V2_RESOURCE).await?;
    let logical_name_id = format!("ens:{}", bigname_lookup::ens_namehash_hex(NAME)?);
    let name = name_current(pool, &logical_name_id)
        .await?
        .context("served name")?;
    assert_eq!(name["resource_id"], V2_RESOURCE, "{label}: {name}");
    assert_eq!(
        name["declared_summary"]["resolver"]["address"], MIRROR,
        "{label}: the name keeps the mirror as its resolver"
    );
    let addresses: Vec<Value> = sqlx::query_scalar(
        "SELECT jsonb_build_object('address', address, 'coin_type', coin_type,
                                   'record_resource_id', record_resource_id)
         FROM address_records_current WHERE logical_name_id = $1 ORDER BY 1::text",
    )
    .bind(&logical_name_id)
    .fetch_all(pool)
    .await?;
    let history = history_attribution(pool, V2_RESOURCE, target).await?;
    if target == fixture.base + 2 || target == fixture.base + 3 {
        assert_unsupported_mirror_row(&v2);
        let mirror = &v2["provenance"]["mirror"];
        assert_eq!(mirror["ancestor_depth"], 1, "{label}: {v2}");
        assert_eq!(mirror["mirrored_resolver_address"], PARENT_RESOLVER);
        assert_eq!(
            mirror["mirrored_unsupported_reason"], "ancestor_resolver_not_extended",
            "{label}: {v2}"
        );
        assert_eq!(addresses, Vec::<Value>::new(), "{label}: address withdrawn");
        assert_eq!(
            history,
            BTreeSet::new(),
            "{label}: no followed ancestor writes"
        );
        return Ok(());
    }
    assert_eq!(v2["support_status"], "supported", "{label}: {v2}");
    assert_eq!(v2["provenance"]["mirror"]["ancestor_depth"], 0);
    let keys: Vec<_> = v2["entries"]
        .as_array()
        .context("entries")?
        .iter()
        .map(|entry| entry["record_key"].as_str().unwrap_or_default().to_owned())
        .collect();
    assert_eq!(keys, ["addr:60", "text:url"], "{label}: {v2}");
    assert_eq!(
        addresses,
        vec![json!({"address": ADDRESS, "coin_type": "60", "record_resource_id": V2_RESOURCE})],
        "{label}: the address record is served from the mirror resource"
    );
    let expected = if target == LAST {
        event_ids(pool, fixture, &["v1-text", "v1-addr-early", "v1-addr"]).await?
    } else {
        event_ids(pool, fixture, &["v1-text", "v1-addr-early"]).await?
    };
    assert_eq!(history, expected, "{label}: history attribution");
    Ok(())
}

async fn event_ids(pool: &PgPool, fixture: &Fixture, identities: &[&str]) -> Result<BTreeSet<i64>> {
    let mut ids = BTreeSet::new();
    for identity in identities {
        ids.insert(event_id(pool, fixture, identity).await?);
    }
    Ok(ids)
}

#[tokio::test]
async fn exact_pointer_loss_withdraws_and_restores_served_mirror_records() -> Result<()> {
    let fixture = Fixture::declared("mirror_gate_transition", V1Side::Cleared).with_ancestor(
        Ancestor::Direct {
            pointer_block_offset: 0,
        },
    );
    let checkpoints = [fixture.base + 1, fixture.base + 2, fixture.base + 3, LAST];

    // Full rebuilds at each checkpoint.
    let mut full = Vec::new();
    for target in checkpoints {
        let (database, pool) = database(&format!("{}_full_{target}", fixture.id)).await?;
        seed_transition(&pool, &fixture).await?;
        run(&pool, target, 0, target, None, RunMode::Normal).await?;
        assert_state(&pool, &fixture, target, &format!("full {target}")).await?;
        full.push(content(inventory(&pool, V2_RESOURCE).await?));
        database.cleanup().await?;
    }

    // Incremental, one block at a time, then a repeated run of the last block and a redo of it.
    let (database, pool) = database(&format!("{}_incremental", fixture.id)).await?;
    seed_transition(&pool, &fixture).await?;
    run(&pool, fixture.base, 0, fixture.base, None, RunMode::Normal).await?;
    let mut parent_after_own_write = None;
    for (index, block) in checkpoints.into_iter().enumerate() {
        run(&pool, block, block, block, Some(block - 1), RunMode::Normal).await?;
        assert_state(&pool, &fixture, block, &format!("incremental {block}")).await?;
        assert_eq!(
            content(inventory(&pool, V2_RESOURCE).await?),
            full[index],
            "incremental {block} drifted from the full rebuild"
        );
        // The parent's own row is unaffected by the child's pointer changes: it keeps the
        // publication target of its own last write.
        let parent = inventory(&pool, PARENT_V1_RESOURCE).await?;
        match &parent_after_own_write {
            None => parent_after_own_write = Some(parent),
            Some(previous) => assert_eq!(&parent, previous, "parent row changed at {block}"),
        }
    }
    let incremental = inventory(&pool, V2_RESOURCE).await?;
    run(&pool, LAST, LAST, LAST, Some(LAST - 1), RunMode::Normal).await?;
    assert_state(&pool, &fixture, LAST, "repeated").await?;
    assert_eq!(
        inventory(&pool, V2_RESOURCE).await?,
        incremental,
        "repeated"
    );
    run(&pool, LAST, LAST, LAST, Some(LAST), RunMode::Redo).await?;
    assert_state(&pool, &fixture, LAST, "redo").await?;
    assert_eq!(
        content(inventory(&pool, V2_RESOURCE).await?),
        full[3],
        "redo drifted from the full rebuild"
    );
    database.cleanup().await?;
    Ok(())
}
