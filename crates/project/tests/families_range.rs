//! Rebuild ranges (docs/projections.md, "Owned key families"): a rebuild applies the work blocks
//! below its switch point several to a transaction, folding them block by block, and must leave
//! exactly the rows a block-by-block follow leaves. Each case builds the families incrementally,
//! one run per block, then rebuilds them in ranges and compares every family table and the
//! marker. The cases put the facts one range must carry across its own blocks into one range:
//! ranges double from one work block, so the leading filler blocks decide which blocks share a
//! range.
mod families_support;

use anyhow::{Result, ensure};
use bigname_project::families::{self, FamilyMode, FamilyOptions, FamilyOutcome, RebuildRanges};
use families_support::{CHAIN, CONTENT_HASH, Event, Fixture, hash, uuid};
use serde_json::{Value, json};

const REGISTRY: &str = "0x00000000000000000000000000000000000000e1";
const REGISTRAR: &str = "0x00000000000000000000000000000000000000e2";
const WRAPPER: &str = "0x00000000000000000000000000000000000000e3";
const OWNER: &str = "0x00000000000000000000000000000000000000b1";
const R1: &str = "0x00000000000000000000000000000000000000a1";
const R2: &str = "0x00000000000000000000000000000000000000a2";
const R3: &str = "0x00000000000000000000000000000000000000a3";

fn node(n: u64) -> String {
    format!("0x{n:064x}")
}

fn name(n: u64) -> String {
    format!("ens:{}", node(n))
}

fn in_ranges(through: i64) -> FamilyOptions {
    FamilyOptions::new(CONTENT_HASH).with_rebuild_ranges(RebuildRanges::Through(through))
}

/// A block that carries family work and touches no row the cases read: an event no reducer
/// keys.
async fn filler(fixture: &Fixture, block: i64) -> Result<()> {
    let identity = format!("filler:{block}");
    fixture
        .event(Event::new(
            &identity,
            block,
            90,
            "PreimageObserved",
            "ens_v1_registry_l1",
        ))
        .await?;
    Ok(())
}

/// Follow block by block from `first` to `target`, one run per block.
async fn follow(fixture: &Fixture, first: i64, target: i64) -> Result<()> {
    for block in first..=target {
        fixture.apply(block, FamilyMode::Normal).await?;
    }
    Ok(())
}

/// Rebuild at `target` with `options` and require every family table and the marker (without
/// its generation) as the incremental follow left them.
async fn rebuild_equal(
    fixture: &Fixture,
    target: i64,
    options: &FamilyOptions,
) -> Result<FamilyOutcome> {
    let incremental = fixture.exact().await?;
    let outcome = fixture
        .apply_with(target, FamilyMode::Rebuild, options)
        .await?;
    let rebuilt = fixture.exact().await?;
    for ((table, was), (_, now)) in incremental.iter().zip(&rebuilt) {
        ensure!(
            was == now,
            "a rebuild in ranges at {target} left {table} as {now}, not {was}"
        );
    }
    Ok(outcome)
}

async fn marker_state(fixture: &Fixture) -> Result<String> {
    Ok(
        sqlx::query_scalar("SELECT state FROM project_family_marker WHERE chain_id = $1")
            .bind(CHAIN)
            .fetch_one(&fixture.pool)
            .await?,
    )
}

// One key written in consecutive blocks of one range keeps the last block's row, and the range
// journals it once, under the range's last block.
#[tokio::test]
async fn a_key_written_in_consecutive_blocks_of_a_range_keeps_the_last_write() -> Result<()> {
    let fixture = Fixture::new("families_range_same_key", 20).await?;
    filler(&fixture, 10).await?;
    fixture.resolver_changed(11, 1, 1, R1).await?;
    fixture.resolver_changed(12, 1, 1, R2).await?;
    fixture.resolver_changed(13, 1, 1, R1).await?;
    fixture.resolver_changed(13, 2, 2, R2).await?;
    fixture.resolver_changed(14, 1, 1, R2).await?;
    follow(&fixture, 10, 14).await?;
    let outcome = rebuild_equal(&fixture, 14, &in_ranges(14)).await?;
    assert_eq!(
        (outcome.blocks, outcome.ranges),
        (5, 3),
        "ranges [10], [11, 12] and [13], then the target on its own"
    );
    assert_eq!(fixture.journalled_blocks().await?, vec![10, 12, 13, 14]);
    let undo_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_family_undo
         WHERE chain_id = $1 AND block_number = 12 AND family = 'project_resource_pointer'",
    )
    .bind(CHAIN)
    .fetch_one(&fixture.pool)
    .await?;
    let registry_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_family_undo
         WHERE chain_id = $1 AND block_number = 12 AND family = 'project_registry_pointer'",
    )
    .bind(CHAIN)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        (undo_rows, registry_rows),
        (0, 1),
        "name 1's pointer, written at 11 and 12, is journalled once with its pre-range image"
    );
    fixture.cleanup().await
}

/// Opens an ens_v1 binding of name 1 on `resource` at `block`, log 1, with its SurfaceBound:
/// a registry-only one for `registry`, a registrar one otherwise.
async fn bound(
    fixture: &Fixture,
    id: u32,
    resource: &str,
    registry: bool,
    block: i64,
    closed_at: Option<i64>,
) -> Result<()> {
    fixture
        .binding(&uuid(id), &name(1), resource, "ens_v1", block, 1, closed_at)
        .await?;
    let (family, after, emitter) = if registry {
        (
            "ens_v1_registry_l1",
            json!({"authority_kind": "registry_only", "state_derived": true}),
            REGISTRY,
        )
    } else {
        (
            "ens_v1_registrar_l1",
            json!({"authority_kind": "registrar"}),
            REGISTRAR,
        )
    };
    fixture
        .write(
            block,
            1,
            "SurfaceBound",
            family,
            Some(&name(1)),
            Some(resource),
            after,
            emitter,
        )
        .await?;
    Ok(())
}

async fn registrar_event(
    fixture: &Fixture,
    block: i64,
    log: i64,
    kind: &str,
    resource: &str,
) -> Result<()> {
    fixture
        .write(
            block,
            log,
            kind,
            "ens_v1_registrar_l1",
            Some(&name(1)),
            Some(resource),
            json!({"namehash": node(1), "registrant": OWNER}),
            REGISTRAR,
        )
        .await?;
    Ok(())
}

// F1 across the blocks of one range: the lease binding and its release at 10, the registry-only
// binding at 11, a registrar grant at 12 and the epoch that turns the binding registry-only at
// 13. The epoch reads the name's earlier candidates, the grants retained since the binding and
// the predecessor's release, all written by earlier blocks of the same range.
#[tokio::test]
async fn a_binding_and_its_registry_only_epoch_in_one_range_take_the_retained_grant() -> Result<()>
{
    let fixture = Fixture::new("families_range_epoch", 20).await?;
    let (lease, registry, successor) = (uuid(1), uuid(2), uuid(3));
    for block in 6..=8 {
        filler(&fixture, block).await?;
    }
    bound(&fixture, 101, &lease, false, 10, Some(11)).await?;
    registrar_event(&fixture, 10, 2, "RegistrationReleased", &lease).await?;
    bound(&fixture, 102, &registry, true, 11, None).await?;
    registrar_event(&fixture, 12, 1, "RegistrationGranted", &successor).await?;
    fixture
        .write(
            13,
            1,
            "AuthorityEpochChanged",
            "ens_v1_registry_l1",
            Some(&name(1)),
            Some(&registry),
            json!({"authority_kind": "registry_only"}),
            REGISTRY,
        )
        .await?;
    filler(&fixture, 14).await?;
    follow(&fixture, 6, 15).await?;
    let handoff = fixture
        .rows("project_binding_candidate")
        .await?
        .into_iter()
        .find(|row| row["registry_only"] == json!(true))
        .map(|row| {
            json!([
                row["predecessor_resource_id"],
                row["lease_resource_id"],
                row["lease_position"]["block_number"]
            ])
        });
    assert_eq!(handoff, Some(json!([lease, successor, 12])));
    let outcome = rebuild_equal(&fixture, 15, &in_ranges(15)).await?;
    assert_eq!(
        fixture.journalled_blocks().await?,
        vec![6, 8, 13, 14, 15],
        "blocks 10 to 13 share one range, 14 is the next"
    );
    assert_eq!(outcome.ranges, 4);
    fixture.cleanup().await
}

/// A manifest of `family` and its SourceManifestUpdated at `block`; the same `manifest_id`
/// publishes an update.
async fn manifest(
    fixture: &Fixture,
    manifest_id: Option<i64>,
    family: &str,
    block: i64,
    payload: Value,
) -> Result<i64> {
    let id = match manifest_id {
        Some(id) => id,
        None => {
            sqlx::query_scalar(
                "INSERT INTO manifest_versions (manifest_version, namespace, source_family,
                     chain_id, deployment_label, rollout_status, normalizer_version, file_path,
                     manifest_payload)
                 VALUES (1, 'ens', $1, $2, 'fixture', 'active', 'fixture', $3, $4)
                 RETURNING manifest_id",
            )
            .bind(family)
            .bind(CHAIN)
            .bind(format!("fixture/{family}.yaml"))
            .bind(&payload)
            .fetch_one(&fixture.pool)
            .await?
        }
    };
    sqlx::query(
        "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, source_manifest_id, chain_id, block_number, block_hash,
             derivation_kind, canonicality_state, before_state, after_state, raw_fact_ref)
         VALUES ($1, 'ens', 'SourceManifestUpdated', $2, 1, $3, $4, $5, $6,
                 'ens_v2_registry_resource_surface', 'canonical', '{}'::jsonb,
                 jsonb_build_object('rollout_status', 'active', 'manifest_payload', $7::jsonb),
                 '{}'::jsonb)",
    )
    .bind(format!("manifest:{id}:{block}"))
    .bind(family)
    .bind(id)
    .bind(CHAIN)
    .bind(block)
    .bind(hash(block))
    .bind(&payload)
    .execute(&fixture.pool)
    .await?;
    Ok(id)
}

/// A resolver discovery edge that manifest `origin` admitted, to a contract at `address`,
/// active from `block` and, with `until`, up to that block.
async fn resolver_edge(
    fixture: &Fixture,
    address: &str,
    origin: i64,
    block: i64,
    until: Option<i64>,
) -> Result<()> {
    let (from, to) = (uuid(0xf001), uuid(0xf002));
    for instance in [&from, &to] {
        sqlx::query(
            "INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
             VALUES ($1::uuid, $2, 'contract')",
        )
        .bind(instance)
        .bind(CHAIN)
        .execute(&fixture.pool)
        .await?;
    }
    sqlx::query(
        "INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address)
         VALUES ($1::uuid, $2, $3)",
    )
    .bind(&to)
    .bind(CHAIN)
    .bind(address)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(
        "INSERT INTO discovery_edges (chain_id, edge_kind, from_contract_instance_id,
             to_contract_instance_id, discovery_source, admission_basis, source_manifest_id,
             active_from_block_number, active_from_block_hash, active_to_block_number,
             active_to_block_hash, canonicality_state)
         VALUES ($1, 'resolver', $2::uuid, $3::uuid, 'NewResolver', 'fixture', $4, $5, $6, $7,
                 $8, 'canonical')",
    )
    .bind(CHAIN)
    .bind(&from)
    .bind(&to)
    .bind(origin)
    .bind(block)
    .bind(hash(block))
    .bind(until)
    .bind(until.map(hash))
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

// F3 inside one range: a pointer at 10 names R2, which no manifest declares; a manifest update
// at 12 declares it, so every stored resolver, R2 among them though it exists only in the
// range, is classified again at 12; a resolver edge that starts at 13 classifies R3 there. Each
// block classifies under its own manifest set and its own activations.
#[tokio::test]
async fn a_manifest_change_and_an_activation_inside_a_range_classify_at_their_block() -> Result<()>
{
    let fixture = Fixture::new("families_range_classification", 20).await?;
    let resolvers = manifest(
        &fixture,
        None,
        "ens_v1_resolver_l1",
        1,
        json!({"contracts": [{"address": R1, "role": "public_resolver"},
                             {"address": R3, "role": "public_resolver"}]}),
    )
    .await?;
    let registry = manifest(
        &fixture,
        None,
        "ens_v1_registry_l1",
        1,
        json!({"contracts": []}),
    )
    .await?;
    for block in 2..=3 {
        filler(&fixture, block).await?;
    }
    fixture
        .write(
            10,
            1,
            "ResolverChanged",
            "ens_v1_registry_l1",
            None,
            None,
            json!({"node": node(2), "resolver": R2}),
            REGISTRY,
        )
        .await?;
    manifest(
        &fixture,
        Some(resolvers),
        "ens_v1_resolver_l1",
        12,
        json!({"contracts": [{"address": R1, "role": "public_resolver"},
                             {"address": R2, "role": "public_resolver"},
                             {"address": R3, "role": "public_resolver"}]}),
    )
    .await?;
    resolver_edge(&fixture, R3, registry, 13, None).await?;
    filler(&fixture, 14).await?;
    follow(&fixture, 1, 15).await?;
    let mut classified: Vec<String> = fixture
        .rows("project_resolver_classification")
        .await?
        .iter()
        .map(|row| {
            json!([
                row["resolver_address"],
                row["support_status"],
                row["event_identity"]
            ])
            .to_string()
        })
        .collect();
    classified.sort();
    assert_eq!(
        classified,
        vec![
            json!([R2, "supported", "ResolverChanged:10:1"]).to_string(),
            json!([R3, "supported", "activation:13"]).to_string(),
        ]
    );
    rebuild_equal(&fixture, 15, &in_ranges(15)).await?;
    assert_eq!(
        fixture.journalled_blocks().await?,
        vec![1, 3, 14, 15],
        "blocks 10 to 14 share one range"
    );
    fixture.cleanup().await
}

/// The resolvers `project_resolver_classification` holds.
async fn classified(fixture: &Fixture) -> Result<Vec<Value>> {
    Ok(fixture
        .rows("project_resolver_classification")
        .await?
        .iter()
        .map(|row| row["resolver_address"].clone())
        .collect())
}

// F3 across a deletion inside one range: a resolver edge alone classifies R3 at 3, in the range
// [2, 3], and ends at 5, which deletes R3's row inside the range [4 to 7]; a manifest update at 6
// then declares R3. The update classifies every stored resolver again, and the range still reads
// R3 from the table, where the range before committed it; with no edge left it has no candidate
// and nothing is written for it. Block by block, 6 never sees R3. The manifest's declaration
// alone must not classify R3 in the range either.
#[tokio::test]
async fn a_resolver_deleted_earlier_in_a_range_stays_deleted_through_a_manifest_change()
-> Result<()> {
    let fixture = Fixture::new("families_range_deleted_resolver", 20).await?;
    let resolvers = manifest(
        &fixture,
        None,
        "ens_v1_resolver_l1",
        1,
        json!({"contracts": [{"address": R1, "role": "public_resolver"}]}),
    )
    .await?;
    let registry = manifest(
        &fixture,
        None,
        "ens_v1_registry_l1",
        1,
        json!({"contracts": []}),
    )
    .await?;
    filler(&fixture, 2).await?;
    resolver_edge(&fixture, R3, registry, 3, Some(5)).await?;
    filler(&fixture, 4).await?;
    manifest(
        &fixture,
        Some(resolvers),
        "ens_v1_resolver_l1",
        6,
        json!({"contracts": [{"address": R1, "role": "public_resolver"},
                             {"address": R3, "role": "public_resolver"}]}),
    )
    .await?;
    filler(&fixture, 7).await?;
    follow(&fixture, 1, 4).await?;
    assert_eq!(classified(&fixture).await?, vec![json!(R3)]);
    follow(&fixture, 5, 8).await?;
    assert_eq!(classified(&fixture).await?, Vec::<Value>::new());
    rebuild_equal(&fixture, 8, &in_ranges(8)).await?;
    assert_eq!(
        fixture.journalled_blocks().await?,
        vec![1, 3, 7, 8],
        "ranges [1], [2, 3] and [4 to 7], then the target"
    );
    fixture.assert_rebuild_equal(8).await?;
    fixture.cleanup().await
}

// F13 inside one range: name 1's grant to OWNER at 3 is committed by the range [3, 4]; the grant
// to OTHER at 5 is the name's latest reporting row, and the renewal at 6, in the same range, is
// the only row block 6 changes and reports no registrant. Refolding at 6 must read the grant at 5
// from the range, not only the table's grant at 3, so the registrant stays OTHER.
#[tokio::test]
async fn a_registrant_refold_reads_the_latest_grant_from_earlier_in_the_range() -> Result<()> {
    const OTHER: &str = "0x00000000000000000000000000000000000000b2";
    let fixture = Fixture::new("families_range_registrant", 20).await?;
    let lease = uuid(1);
    filler(&fixture, 2).await?;
    registrar_event(&fixture, 3, 1, "RegistrationGranted", &lease).await?;
    filler(&fixture, 4).await?;
    fixture
        .write(
            5,
            1,
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            Some(&name(1)),
            Some(&lease),
            json!({"namehash": node(1), "registrant": OTHER}),
            REGISTRAR,
        )
        .await?;
    fixture
        .write(
            6,
            1,
            "RegistrationRenewed",
            "ens_v1_registrar_l1",
            Some(&name(1)),
            Some(&lease),
            json!({"namehash": node(1), "expiry": 900}),
            REGISTRAR,
        )
        .await?;
    for block in 7..=8 {
        filler(&fixture, block).await?;
    }
    follow(&fixture, 2, 9).await?;
    let registrant: Vec<Value> = fixture
        .rows("project_address_name_fold")
        .await?
        .iter()
        .map(|row| {
            json!([
                row["registrant"],
                row["registrant_position"]["block_number"]
            ])
        })
        .collect();
    assert_eq!(registrant, vec![json!([OTHER, 5])]);
    rebuild_equal(&fixture, 9, &in_ranges(9)).await?;
    assert_eq!(
        fixture.journalled_blocks().await?,
        vec![2, 4, 8, 9],
        "ranges [2], [3, 4] and [5 to 8], then the target"
    );
    fixture.assert_rebuild_equal(9).await?;
    fixture.cleanup().await
}

// F2c inside one range: name 1's binding on resource X opens at 10 and closes at 13, where no
// event of the name arrives. The named AuthorityTransferred at 12 reaches the name's current
// resource at 12, X, even though the range ends at 13, after X closed.
#[tokio::test]
async fn an_observation_reaches_the_resource_current_at_its_own_block() -> Result<()> {
    let fixture = Fixture::new("families_range_rebound", 20).await?;
    let (bound_resource, own) = (uuid(1), uuid(2));
    for block in 6..=8 {
        filler(&fixture, block).await?;
    }
    fixture
        .binding(
            &uuid(101),
            &name(1),
            &bound_resource,
            "ens_v1",
            10,
            1,
            Some(13),
        )
        .await?;
    fixture
        .write(
            12,
            1,
            "AuthorityTransferred",
            "ens_v1_registry_l1",
            Some(&name(1)),
            Some(&own),
            json!({"node": node(1), "owner": OWNER, "owner_getter": OWNER}),
            REGISTRY,
        )
        .await?;
    filler(&fixture, 13).await?;
    follow(&fixture, 6, 14).await?;
    let target: Vec<Value> = fixture
        .rows("project_registry_binding_observation")
        .await?
        .iter()
        .map(|row| row["target_resource_id"].clone())
        .collect();
    assert_eq!(target, vec![json!(bound_resource)]);
    rebuild_equal(&fixture, 14, &in_ranges(14)).await?;
    assert_eq!(fixture.journalled_blocks().await?, vec![6, 8, 13, 14]);
    fixture.cleanup().await
}

// F2a inside one range: an unnamed registrar grant on lease L at 10 and, at 12, a wrapper
// binding whose SurfaceBound records L. The wrapper candidate names the grant, which only the
// range holds.
#[tokio::test]
async fn an_unnamed_grant_and_its_wrapper_candidate_in_one_range_name_the_grant() -> Result<()> {
    let fixture = Fixture::new("families_range_decode", 20).await?;
    let (lease, wrapper) = (uuid(1), uuid(2));
    for block in 6..=8 {
        filler(&fixture, block).await?;
    }
    fixture
        .write(
            10,
            1,
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            None,
            Some(&lease),
            json!({"registrant": OWNER, "namehash": node(1), "expiry": 900}),
            REGISTRAR,
        )
        .await?;
    fixture
        .binding(&uuid(101), &name(1), &wrapper, "ens_v1", 12, 1, None)
        .await?;
    fixture
        .write(
            12,
            1,
            "SurfaceBound",
            "ens_v1_wrapper_l1",
            Some(&name(1)),
            Some(&wrapper),
            json!({"authority_kind": "wrapper", "wrapped_registrar_resource_id": lease,
                   "node": node(1)}),
            WRAPPER,
        )
        .await?;
    filler(&fixture, 13).await?;
    follow(&fixture, 6, 14).await?;
    let decoded: Vec<Value> = fixture
        .rows("project_lifecycle_event")
        .await?
        .iter()
        .map(|row| row["decoded_logical_name_id"].clone())
        .collect();
    assert_eq!(decoded, vec![json!(name(1))]);
    rebuild_equal(&fixture, 14, &in_ranges(14)).await?;
    assert_eq!(fixture.journalled_blocks().await?, vec![6, 8, 13, 14]);
    fixture.cleanup().await
}

// One event identity delivered in two blocks is two events: each block keeps its own delivery,
// in a range as block by block, and neither counts as a disagreeing duplicate.
#[tokio::test]
async fn an_event_identity_in_two_blocks_of_a_range_is_kept_in_each() -> Result<()> {
    let fixture = Fixture::new("families_range_identity", 20).await?;
    sqlx::query(
        "ALTER TABLE normalized_events DROP CONSTRAINT normalized_events_event_identity_key",
    )
    .execute(&fixture.pool)
    .await?;
    filler(&fixture, 10).await?;
    fixture.surface(&name(1), &node(1)).await?;
    for (block, resolver) in [(11, R1), (12, R2)] {
        fixture
            .event(
                Event::new(
                    "repeated",
                    block,
                    1,
                    "ResolverChanged",
                    "ens_v1_registry_l1",
                )
                .name(&name(1))
                .after(json!({"resolver": resolver, "node": node(1)})),
            )
            .await?;
    }
    follow(&fixture, 10, 13).await?;
    let outcome = rebuild_equal(&fixture, 13, &in_ranges(13)).await?;
    assert_eq!(outcome.duplicate_anomalies, 0);
    assert_eq!(fixture.journalled_blocks().await?, vec![10, 12, 13]);
    let pointer: Vec<Value> = fixture
        .rows("project_registry_pointer")
        .await?
        .iter()
        .map(|row| json!([row["resolver_address"], row["block_number"]]))
        .collect();
    assert_eq!(pointer, vec![json!([R2, 12])]);
    fixture.cleanup().await
}

/// Resolver pointers at every block of `blocks`, name `block % 3`.
async fn pointers(fixture: &Fixture, blocks: std::ops::RangeInclusive<i64>) -> Result<()> {
    for block in blocks {
        let resolver = if block % 2 == 0 { R1 } else { R2 };
        fixture
            .resolver_changed(block, 1, block.unsigned_abs() % 3, resolver)
            .await?;
    }
    Ok(())
}

// A range spends one block of the run's budget per work block it applies and never more than
// the budget holds; the marker stands on a range's last block, still bootstrap_pending, and the
// rebuild's journal has one marker row per range.
#[tokio::test]
async fn a_budgeted_range_rebuild_stops_on_a_range_end_in_bootstrap() -> Result<()> {
    let fixture = Fixture::new("families_range_budget", 40).await?;
    pointers(&fixture, 10..=29).await?;
    let options = in_ranges(30).with_max_blocks_per_run(5);
    let first = fixture
        .apply_with(30, FamilyMode::Rebuild, &options)
        .await?;
    assert_eq!(
        (first.blocks, first.ranges, first.budget_exhausted),
        (5, 3, true),
        "ranges [10], [11, 12] and [13, 14], the last cut to the budget"
    );
    let (block, _, _) = fixture.marker().await?;
    assert_eq!(block, Some(14));
    assert_eq!(marker_state(&fixture).await?, "bootstrap_pending");
    assert_eq!(fixture.journalled_blocks().await?, vec![10, 12, 14]);
    fixture.cleanup().await
}

// A rebuild stopped between two ranges resumes from the last range's block on the next run, and
// the finished families equal the block-by-block follow.
#[tokio::test]
async fn a_range_rebuild_resumes_after_a_stop_and_equals_the_follow() -> Result<()> {
    let fixture = Fixture::new("families_range_resume", 40).await?;
    pointers(&fixture, 10..=29).await?;
    follow(&fixture, 10, 30).await?;
    let incremental = fixture.exact().await?;
    let options = in_ranges(30).with_max_blocks_per_run(5);
    let mut mode = FamilyMode::Rebuild;
    let mut runs = 0;
    loop {
        let outcome = fixture.apply_with(30, mode.clone(), &options).await?;
        ensure!(
            !outcome.reset || runs == 0,
            "run {runs} restarted the rebuild"
        );
        runs += 1;
        mode = FamilyMode::Normal;
        if outcome.marker.as_ref().map(|marker| marker.number) == Some(30) {
            break;
        }
        ensure!(runs < 10, "the rebuild did not finish");
    }
    assert_eq!(runs, 5, "21 work blocks at five a run");
    assert_eq!(marker_state(&fixture).await?, "live");
    let rebuilt = fixture.exact().await?;
    for ((table, was), (_, now)) in incremental.iter().zip(&rebuilt) {
        assert_eq!(was, now, "{table}");
    }
    fixture.cleanup().await
}

// Ranges double from one block only right after the reset: a run that resumes a rebuild starts
// with a range as large as its budget. With a budget of eight, the first run commits [10],
// [11, 12], [13 to 16] and [17]; the second commits [18 to 25] as one range.
#[tokio::test]
async fn a_resumed_range_rebuild_starts_with_a_range_of_its_whole_budget() -> Result<()> {
    let fixture = Fixture::new("families_range_resume_size", 40).await?;
    pointers(&fixture, 10..=29).await?;
    follow(&fixture, 10, 30).await?;
    let incremental = fixture.exact().await?;
    let options = in_ranges(30).with_max_blocks_per_run(8);
    let first = fixture
        .apply_with(30, FamilyMode::Rebuild, &options)
        .await?;
    assert_eq!(
        (first.blocks, first.ranges, first.budget_exhausted),
        (8, 4, true)
    );
    assert_eq!(fixture.journalled_blocks().await?, vec![10, 12, 16, 17]);
    let second = fixture.apply_with(30, FamilyMode::Normal, &options).await?;
    assert_eq!(
        (second.blocks, second.ranges, second.reset),
        (8, 1, false),
        "the resumed run applies its whole budget in one range"
    );
    assert_eq!(fixture.marker().await?.0, Some(25));
    let third = fixture.apply_with(30, FamilyMode::Normal, &options).await?;
    assert_eq!(
        (third.blocks, third.ranges),
        (5, 1),
        "[26 to 29], then the target on its own"
    );
    assert_eq!(
        fixture.journalled_blocks().await?,
        vec![10, 12, 16, 17, 25, 29, 30]
    );
    assert_eq!(marker_state(&fixture).await?, "live");
    let rebuilt = fixture.exact().await?;
    for ((table, was), (_, now)) in incremental.iter().zip(&rebuilt) {
        assert_eq!(was, now, "{table}");
    }
    fixture.cleanup().await
}

// A range cap of zero, set on the option field rather than through `with_range_caps`, counts as
// one block: the rebuild completes with every work block a range of its own, then the target,
// and equals both the block-by-block follow and a rebuild with ranges off.
#[tokio::test]
async fn a_range_cap_of_zero_counts_as_one_block() -> Result<()> {
    let fixture = Fixture::new("families_range_zero_cap", 20).await?;
    pointers(&fixture, 10..=13).await?;
    follow(&fixture, 10, 14).await?;
    let mut options = in_ranges(14);
    options.max_range_blocks = 0;
    let outcome = rebuild_equal(&fixture, 14, &options).await?;
    assert_eq!((outcome.blocks, outcome.ranges), (5, 4));
    assert_eq!(marker_state(&fixture).await?, "live");
    assert_eq!(fixture.journalled_blocks().await?, vec![10, 11, 12, 13, 14]);
    let off = FamilyOptions::new(CONTENT_HASH).with_rebuild_ranges(RebuildRanges::Off);
    let outcome = rebuild_equal(&fixture, 14, &off).await?;
    assert_eq!((outcome.blocks, outcome.ranges), (5, 0));
    fixture.cleanup().await
}

// Undo to a block inside a range lands on the range's predecessor, since the range journals as
// one step; replaying from there block by block equals a fresh rebuild.
#[tokio::test]
async fn an_undo_into_a_range_lands_on_its_predecessor_and_replays_to_a_rebuild() -> Result<()> {
    let fixture = Fixture::new("families_range_undo", 40).await?;
    pointers(&fixture, 10..=20).await?;
    fixture
        .apply_with(21, FamilyMode::Rebuild, &in_ranges(21))
        .await?;
    assert_eq!(
        fixture.journalled_blocks().await?,
        vec![10, 12, 16, 20, 21],
        "ranges [10], [11, 12], [13 to 16] and [17 to 20], then 21"
    );
    let undone = families::undo_to(&fixture.pool, CHAIN, 14).await?;
    assert_eq!(
        undone, 3,
        "21, the range ending at 20 and the range ending at 16"
    );
    assert_eq!(fixture.marker().await?.0, Some(12));
    let replayed = fixture.apply(21, FamilyMode::Normal).await?;
    assert_eq!(replayed.blocks, 9, "13 to 21, block by block");
    let incremental = fixture.exact().await?;
    fixture
        .apply_with(
            21,
            FamilyMode::Rebuild,
            &FamilyOptions::new(CONTENT_HASH).with_rebuild_ranges(RebuildRanges::Off),
        )
        .await?;
    let rebuilt = fixture.exact().await?;
    for ((table, was), (_, now)) in incremental.iter().zip(&rebuilt) {
        assert_eq!(was, now, "{table}");
    }
    fixture.cleanup().await
}

// The default switch point: work blocks at or below the chain's safe block minus 5 go in
// ranges, those above it and the target one block to a transaction.
#[tokio::test]
async fn ranges_stop_five_blocks_below_the_safe_block() -> Result<()> {
    let fixture = Fixture::new("families_range_switch_safe", 40).await?;
    pointers(&fixture, 10..=29).await?;
    fixture.heads(40, 25, 20).await?;
    follow(&fixture, 10, 30).await?;
    rebuild_equal(&fixture, 30, &FamilyOptions::new(CONTENT_HASH)).await?;
    let mut expected = vec![10, 12, 16, 20];
    expected.extend(21..=30);
    assert_eq!(fixture.journalled_blocks().await?, expected);
    fixture.cleanup().await
}

// With no safe block published, the switch point is 256 blocks below the target.
#[tokio::test]
async fn without_a_safe_block_ranges_stop_256_blocks_below_the_target() -> Result<()> {
    let fixture = Fixture::new("families_range_switch_target", 300).await?;
    for block in (10..=290).step_by(10) {
        fixture.resolver_changed(block, 1, 1, R1).await?;
    }
    let rebuilt = fixture
        .apply_with(300, FamilyMode::Rebuild, &FamilyOptions::new(CONTENT_HASH))
        .await?;
    let mut expected = vec![10, 30, 40];
    expected.extend((50..=290).step_by(10));
    expected.push(300);
    assert_eq!(
        fixture.journalled_blocks().await?,
        expected,
        "ranges [10], [20, 30] and [40] at or below 44, then block by block"
    );
    assert_eq!(rebuilt.ranges, 3);
    fixture.cleanup().await
}

// A range ends before the block that would take its events past the event cap, and a block
// holding more events than the cap still makes a range of its own; the families equal the
// block-by-block follow.
#[tokio::test]
async fn a_range_ends_before_the_block_past_its_event_cap() -> Result<()> {
    let fixture = Fixture::new("families_range_event_cap", 40).await?;
    pointers(&fixture, 10..=29).await?;
    fixture.resolver_changed(17, 2, 5, R1).await?;
    filler(&fixture, 17).await?;
    follow(&fixture, 10, 30).await?;
    let rebuilt = rebuild_equal(&fixture, 30, &in_ranges(30).with_range_caps(1024, 2)).await?;
    assert_eq!(rebuilt.ranges, 11);
    assert_eq!(
        fixture.journalled_blocks().await?,
        vec![10, 12, 14, 16, 17, 19, 21, 23, 25, 27, 29, 30],
        "two events a range: [10], then pairs, and block 17's three events alone"
    );
    fixture.cleanup().await
}
