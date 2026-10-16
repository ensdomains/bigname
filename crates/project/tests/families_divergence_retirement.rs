//! A successful null-resolver family publication retires earlier direct observations in the
//! same transaction. The evidence remains durable and a failed publication retires nothing.
#[path = "families_support/mod.rs"]
mod support;
use anyhow::{Result, ensure};
use bigname_project::families::{self, FamilyMode, FamilyOptions, RebuildRanges};
use serde_json::json;
use support::{CONTENT_HASH, Event, Fixture, marker};
const CHAIN: &str = "ethereum-mainnet";
const RESOLVER: &str = "0x00000000000000000000000000000000000000a1";
const ZERO: &str = "0x0000000000000000000000000000000000000000";

#[tokio::test]
async fn null_resolver_publication_retires_evidence_atomically() -> Result<()> {
    let fixture = Fixture::new("families_divergence_retirement", 3).await?;
    fixture.lineage(CHAIN, 3).await?;
    let node = format!("0x{:064x}", 1);
    let name = format!("ens:{node}");
    fixture.surface_on(CHAIN, &name, &node).await?;
    for (identity, kind, after) in [
        (
            "owner:1",
            "AuthorityTransferred",
            json!({"owner":RESOLVER,"owner_getter":RESOLVER,"source_event":"Transfer", "node":node}),
        ),
        (
            "resolver:1",
            "ResolverChanged",
            json!({"resolver":RESOLVER,"node":node}),
        ),
    ] {
        fixture
            .event(
                Event::new(
                    identity,
                    1,
                    i64::from(kind == "ResolverChanged"),
                    kind,
                    "ens_v1_registry_l1",
                )
                .on(CHAIN)
                .name(&name)
                .after(after),
            )
            .await?;
    }
    fixture.apply_on(CHAIN, 1).await?;
    sqlx::query(
        "INSERT INTO resolution_divergences (logical_name_id, resolver_chain_id,
        resolver_address, request_kind, observed_positions, indexed_result, live_result)
        SELECT $1, $2, $3, 'text:avatar', jsonb_build_object('ethereum', jsonb_build_object(
          'chain_id', chain_id, 'block_number', block_number, 'block_hash', block_hash,
          'timestamp', to_jsonb(block_timestamp))), '\"before\"'::jsonb, '\"live\"'::jsonb
        FROM chain_lineage WHERE chain_id=$2 AND block_number=1",
    )
    .bind(&name)
    .bind(CHAIN)
    .bind(RESOLVER)
    .execute(&fixture.pool)
    .await?;
    fixture
        .event(
            Event::new("resolver:2", 2, 0, "ResolverChanged", "ens_v1_registry_l1")
                .on(CHAIN)
                .name(&name)
                .after(json!({"resolver":ZERO,"node":node})),
        )
        .await?;
    sqlx::raw_sql(
        "CREATE FUNCTION refuse_marker() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN IF NEW.current_block_number = 2 THEN RAISE EXCEPTION 'publication refused'; END IF;
        RETURN NEW; END $$;
      CREATE TRIGGER refuse_marker BEFORE UPDATE ON project_family_marker
        FOR EACH ROW EXECUTE FUNCTION refuse_marker();",
    )
    .execute(&fixture.pool)
    .await?;
    assert!(fixture.apply_on(CHAIN, 2).await.is_err());
    assert_eq!(active(&fixture).await?, (1, 1));
    sqlx::raw_sql("DROP TRIGGER refuse_marker ON project_family_marker")
        .execute(&fixture.pool)
        .await?;
    fixture.apply_on(CHAIN, 2).await?;
    let row = bigname_storage::families::name::load_family_name(&fixture.pool, &name)
        .await?
        .expect("name");
    assert!(row.declared_summary["resolver"]["address"].is_null());
    assert_eq!(active(&fixture).await?, (1, 0));
    fixture.cleanup().await
}
async fn active(fixture: &Fixture) -> Result<(i64, i64)> {
    Ok(sqlx::query_as(
        "SELECT count(*), count(*) FILTER (WHERE cleared_at IS NULL) FROM resolution_divergences",
    )
    .fetch_one(&fixture.pool)
    .await?)
}

/// Direct evidence against `name` of `chain` at block 1, active.
async fn observe(fixture: &Fixture, chain: &str, name: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO resolution_divergences (logical_name_id, resolver_chain_id,
        resolver_address, request_kind, observed_positions, indexed_result, live_result)
        SELECT $1, $2, $3, 'text:avatar', jsonb_build_object('ethereum', jsonb_build_object(
          'chain_id', chain_id, 'block_number', block_number, 'block_hash', block_hash,
          'timestamp', to_jsonb(block_timestamp))), '\"before\"'::jsonb, '\"live\"'::jsonb
        FROM chain_lineage WHERE chain_id=$2 AND block_number=1",
    )
    .bind(name)
    .bind(chain)
    .bind(RESOLVER)
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

async fn active_for(fixture: &Fixture, name: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM resolution_divergences
         WHERE logical_name_id = $1 AND cleared_at IS NULL",
    )
    .bind(name)
    .fetch_one(&fixture.pool)
    .await?)
}

// A rebuild in ranges through 7, then single blocks to the target 10: ranges [1], [2, 3] and
// [4 to 7], the composition pass at 7. Each of three registered names has its resolver pointer
// on its own resource from block 1. Name 1's pointer is zero at 3, a range's last block, and set
// again at 5: the ranges compose nothing, so the rebuild does not retire its evidence, where a
// range composing at 3 would. Name 2's pointer turns zero at 6 and stays so at the pass block: the
// pass retires its evidence. Name 3's turns zero at 9, a single block above the switch point:
// that block retires its evidence, as a follow block does.
#[tokio::test]
async fn a_rebuild_in_ranges_retires_evidence_from_its_pass_and_later_blocks() -> Result<()> {
    let fixture = Fixture::new("families_divergence_retirement_ranges", 10).await?;
    let chain = support::CHAIN;
    let registrar = "0x00000000000000000000000000000000000000e3";
    let registry = "0x00000000000000000000000000000000000000e1";
    let names: Vec<(String, String, String)> = (1..=3)
        .map(|n| {
            let node = format!("0x{n:064x}");
            (format!("ens:{node}"), node, support::uuid(n))
        })
        .collect();
    for (index, (name, _, resource)) in names.iter().enumerate() {
        let n = u32::try_from(index)?;
        fixture
            .binding(&support::uuid(100 + n), name, resource, "ens_v1", 1, 0, None)
            .await?;
        fixture
            .write(
                1,
                10 + i64::from(n),
                "RegistrationGranted",
                "ens_v1_registrar_l1",
                Some(name),
                Some(resource),
                json!({"authority_kind": "registrar", "status": "registered",
                       "registrant": RESOLVER, "expiry": 2_000_000_000u64}),
                registrar,
            )
            .await?;
        observe(&fixture, chain, name).await?;
    }
    for (block, index, resolver) in [
        (1, 0, RESOLVER),
        (1, 1, RESOLVER),
        (1, 2, RESOLVER),
        (2, 0, RESOLVER),
        (3, 0, ZERO),
        (4, 1, RESOLVER),
        (5, 0, RESOLVER),
        (6, 1, ZERO),
        (7, 2, RESOLVER),
        (9, 2, ZERO),
    ] {
        let (name, node, resource) = &names[index];
        fixture
            .write(
                block,
                20 + i64::try_from(index)?,
                "ResolverChanged",
                "ens_v1_registry_l1",
                Some(name),
                Some(resource),
                json!({"node": node, "resolver": resolver}),
                registry,
            )
            .await?;
    }
    let token = families::input_token(&fixture.pool, chain).await?;
    let outcome = families::apply(
        &fixture.pool,
        chain,
        &marker(10),
        FamilyMode::Rebuild,
        &token,
        &FamilyOptions::new(CONTENT_HASH).with_rebuild_ranges(RebuildRanges::Through(7)),
    )
    .await?;
    ensure!(
        outcome.ranges == 3 && outcome.compose_chunks > 0,
        "ranges [1], [2, 3] and [4 to 7], then the pass: {outcome:?}"
    );
    let mut active = Vec::new();
    let mut resolvers = Vec::new();
    for (name, _, _) in &names {
        active.push(active_for(&fixture, name).await?);
        let row = bigname_storage::families::name::load_family_name(&fixture.pool, name).await?;
        resolvers.push(row.map(|row| row.declared_summary["resolver"]["address"].clone()));
    }
    ensure!(
        active == [1, 0, 0],
        "name 1 kept, names 2 and 3 retired: {active:?}, resolvers at the target {resolvers:?}"
    );
    ensure!(
        resolvers[0] == Some(json!(RESOLVER)),
        "name 1 resolves at the target: {resolvers:?}"
    );
    fixture.cleanup().await
}
