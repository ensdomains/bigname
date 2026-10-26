//! The address history catalogue build of a rebuild's composition pass
//! (`history_catalogue/build`, docs/projections.md, "Owned key families"). Each key-rule case
//! writes one shape the build must find its keys for, checks the shape in a rebuild block by
//! block, then rebuilds in ranges with the build under one block span and one key partition and
//! under three spans and two partitions, and requires every family table as block by block left
//! it. The other cases pin the build's steps: its partitions, a crash inside them, its start
//! gate, a changed split, its `work_mem`, its statistics, its readers and a hot address.
mod families_support;

use anyhow::{Result, ensure};
use bigname_project::families::{self, FamilyMode, FamilyOptions, FamilyOutcome, RebuildRanges};
use bigname_storage::history_catalogue_contract::{
    event_kind_mask, relation_mask, relation_mask_sql,
};
use families_support::{
    CHAIN, CONTENT_HASH, Event, Fixture,
    compose::{build_order, build_step, install_probe, probe_build, reset_probe, seeded},
    hash, marker, uuid,
};
use serde_json::{Value, json};
use sqlx::raw_sql;

const TARGET: i64 = 40;
const ALICE: &str = "0x00000000000000000000000000000000000000aa";
const BOB: &str = "0x00000000000000000000000000000000000000bb";
const CAROL: &str = "0x00000000000000000000000000000000000000cc";
const R1: &str = "0x00000000000000000000000000000000000000a1";
const R2: &str = "0x00000000000000000000000000000000000000a2";
const REGISTRAR: &str = "0x00000000000000000000000000000000000000e2";
const REGISTRY: &str = "0x00000000000000000000000000000000000000e1";
const V1_REGISTRAR: &str = "ens_v1_registrar_l1";
const V1_REGISTRY: &str = "ens_v1_registry_l1";
const ZERO_NODE: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";
const ETH_NODE: &str = "0x93cdeb708b7545dc668eb9280176169d1c33cfd8ed6f04690a0bcc88a93fc4ae";
/// The splits every key-rule case is built under, with the names chunk size of each.
const LAYOUTS: [((u16, u16), usize); 2] = [((1, 1), 1), ((3, 2), 5)];

fn node(n: u64) -> String {
    format!("0x{n:064x}")
}

fn name(n: u64) -> String {
    format!("ens:{}", node(n))
}

fn resource(n: u64) -> String {
    uuid(u32::try_from(n).expect("a small fixture number"))
}

/// A fixture with canonical blocks to `TARGET` and the probe installed.
async fn fixture(prefix: &str) -> Result<Fixture> {
    let fixture = Fixture::new(prefix, TARGET).await?;
    install_probe(&fixture).await?;
    Ok(fixture)
}

/// A rebuild in ranges through the target with the build split `layout`, names in chunks of
/// `chunk`.
fn ranged(layout: (u16, u16), chunk: usize) -> FamilyOptions {
    FamilyOptions::new(CONTENT_HASH)
        .with_max_blocks_per_run(u64::MAX)
        .with_rebuild_ranges(RebuildRanges::Through(TARGET))
        .with_compose_chunk_keys(chunk)
        .with_compose_builder_partitions(layout.0, layout.1)
}

/// Rebuild block by block and return every family table and the marker.
async fn by_block(fixture: &Fixture) -> Result<Vec<(String, String)>> {
    let off = FamilyOptions::new(CONTENT_HASH)
        .with_max_blocks_per_run(u64::MAX)
        .with_rebuild_ranges(RebuildRanges::Off);
    fixture
        .apply_with(TARGET, FamilyMode::Rebuild, &off)
        .await?;
    fixture.exact().await
}

fn require_equal(
    now: &[(String, String)],
    expected: &[(String, String)],
    label: &str,
) -> Result<()> {
    for ((table, was), (_, rebuilt)) in expected.iter().zip(now) {
        ensure!(
            was == rebuilt,
            "{label}: {table} is {rebuilt}, block by block it is {was}"
        );
    }
    Ok(())
}

/// One rebuild in ranges under `options`, which must range and run the whole build.
async fn build(fixture: &Fixture, options: &FamilyOptions, label: &str) -> Result<FamilyOutcome> {
    reset_probe(fixture).await?;
    let outcome = fixture
        .apply_with(TARGET, FamilyMode::Rebuild, options)
        .await?;
    let built = probe_build(fixture).await?;
    ensure!(
        outcome.ranges > 0 && built.last().is_some_and(|(step, _)| step == "done"),
        "{label}: a rebuild in ranges that runs the build: {outcome:?}, {built:?}"
    );
    Ok(outcome)
}

/// Rebuild in ranges under each of `LAYOUTS` and require the block-by-block families.
async fn require_built(
    fixture: &Fixture,
    expected: &[(String, String)],
    label: &str,
) -> Result<()> {
    for (layout, chunk) in LAYOUTS {
        let label = format!("{label}, built under {layout:?}");
        build(fixture, &ranged(layout, chunk), &label).await?;
        require_equal(&fixture.exact().await?, expected, &label)?;
    }
    Ok(())
}

async fn rows(fixture: &Fixture, query: &str, binds: &[&str]) -> Result<Vec<Value>> {
    let mut query = sqlx::query_scalar::<_, Value>(query);
    for bind in binds {
        query = query.bind(*bind);
    }
    Ok(query.fetch_all(&fixture.pool).await?)
}

/// The catalogue source of `key`, kind `kind`.
async fn source(fixture: &Fixture, kind: i16, key: &str) -> Result<Option<Value>> {
    Ok(sqlx::query_scalar(
        "SELECT to_jsonb(source) - 'chain_id' FROM project_history_source source
         WHERE source_kind = $1 AND source_key = $2",
    )
    .bind(kind)
    .bind(key)
    .fetch_optional(&fixture.pool)
    .await?)
}

/// The address memberships of anchor `id`, kind `kind`.
async fn anchors(fixture: &Fixture, kind: i16, id: &str) -> Result<Vec<Value>> {
    Ok(sqlx::query_scalar(
        "SELECT to_jsonb(anchor) - 'chain_id' FROM project_address_history_anchor anchor
         WHERE anchor_kind = $1 AND anchor_id = $2 ORDER BY to_jsonb(anchor)::text",
    )
    .bind(kind)
    .bind(id)
    .fetch_all(&fixture.pool)
    .await?)
}

/// The resolver source edges of `resource`.
async fn edges(fixture: &Fixture, resource: &str) -> Result<Vec<Value>> {
    rows(
        fixture,
        "SELECT to_jsonb(edge) - 'chain_id' FROM project_history_source_edge edge
         WHERE resource_id = $1::uuid ORDER BY to_jsonb(edge)::text",
        &[resource],
    )
    .await
}

/// Whether the last build's work table marked resource `key` as a work resource.
async fn work(fixture: &Fixture, key: &str) -> Result<Option<bool>> {
    Ok(sqlx::query_scalar(
        "SELECT bool_or(work) FROM compose_probe_work WHERE source_kind = 1 AND source_key = $1",
    )
    .bind(key)
    .fetch_one(&fixture.pool)
    .await?)
}

/// Whether a source row is empty: no event the catalogue reads.
fn empty(source: &Option<Value>) -> bool {
    source.as_ref().is_some_and(|source| {
        source["event_mask"] == json!(0)
            && source["first_bucket"].is_null()
            && source["bucket_range"] == json!("empty")
    })
}

fn bucket(source: &Option<Value>) -> Option<(i64, i64)> {
    let source = source.as_ref()?;
    Some((
        source["first_bucket"].as_i64()?,
        source["last_bucket"].as_i64()?,
    ))
}

/// A bloom with every bit set, as an unknown kind and a version bump write it.
fn full_bloom(source: &Option<Value>) -> bool {
    source
        .as_ref()
        .and_then(|source| source["key_bloom"].as_str())
        .is_some_and(|bits| bits.len() == 256 && bits.bytes().all(|bit| bit == b'1'))
}

fn some_bloom(source: &Option<Value>) -> bool {
    source
        .as_ref()
        .and_then(|source| source["key_bloom"].as_str())
        .is_some_and(|bits| bits.contains('1') && bits.contains('0'))
}

/// Write `event` and take it off every block: an unpositioned event, as manifest history and
/// other blockless facts are.
async fn unpositioned(fixture: &Fixture, event: Event<'_>) -> Result<i64> {
    let id = fixture.event(event.synthesised()).await?;
    sqlx::query(
        "UPDATE normalized_events SET block_number = NULL, block_hash = NULL
         WHERE normalized_event_id = $1",
    )
    .bind(id)
    .execute(&fixture.pool)
    .await?;
    Ok(id)
}

async fn set(fixture: &Fixture, id: i64, assignment: &str) -> Result<()> {
    raw_sql(&format!(
        "UPDATE normalized_events SET {assignment} WHERE normalized_event_id = {id}"
    ))
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

/// Name `n` registered to `registrant` at `block` on its own resource `n`, bound there.
async fn registered(fixture: &Fixture, n: u64, block: i64, registrant: &str) -> Result<i64> {
    let log = i64::try_from(n)?;
    fixture
        .binding(
            &resource(1000 + n),
            &name(n),
            &resource(n),
            "ens_v1",
            block,
            log,
            None,
        )
        .await?;
    fixture
        .write(
            block,
            log,
            "RegistrationGranted",
            V1_REGISTRAR,
            Some(&name(n)),
            Some(&resource(n)),
            json!({"authority_kind": "registrar", "status": "registered",
                   "registrant": registrant, "expiry": 2_000_000_000u64}),
            REGISTRAR,
        )
        .await
}

/// An unnamed record write by node on `family`.
async fn node_record(
    fixture: &Fixture,
    block: i64,
    log: i64,
    family: &str,
    at: &str,
) -> Result<i64> {
    fixture
        .write(
            block,
            log,
            "RecordChanged",
            family,
            None,
            None,
            json!({"node": at, "resolver": R1, "record_key": "addr:60", "record_family": "addr",
                   "selector_key": "60", "value": ALICE, "source_event": "AddressChanged"}),
            R1,
        )
        .await
}

/// A record write by record id `id` of resolver `R1`.
async fn record_id_record(fixture: &Fixture, block: i64, log: i64, id: &str) -> Result<i64> {
    fixture
        .write(
            block,
            log,
            "RecordChanged",
            "ens_v2_resolver_l1",
            None,
            None,
            json!({"resolver": R1, "resolver_record_id": id, "storage_model": "resolver_record_id",
                   "record_key": "text:url", "record_family": "text", "selector_key": "url",
                   "value": "https://example.com", "source_event": "TextChanged"}),
            R1,
        )
        .await
}

/// A registrar token transfer of the unsurfaced name `namehash` on `on` to `to`, with no name.
async fn unnamed_transfer(
    fixture: &Fixture,
    block: i64,
    log: i64,
    on: &str,
    namehash: &str,
    to: &str,
) -> Result<i64> {
    fixture
        .write(
            block,
            log,
            "TokenControlTransferred",
            V1_REGISTRAR,
            None,
            Some(on),
            json!({"namehash": namehash, "to": to, "source_event": "Transfer",
                   "authority_kind": "registrar"}),
            REGISTRAR,
        )
        .await
}

// B2. An unpositioned event naming a composed name and a resource, activated and canonical:
// both sources start in the unpositioned bucket, and the resource is a work resource because a
// composed name's blockless event reaches it (rule R4).
#[tokio::test]
async fn an_unpositioned_event_sources_its_name_and_resource_unpositioned() -> Result<()> {
    let fixture = fixture("families_builder_unpositioned").await?;
    registered(&fixture, 1, 5, ALICE).await?;
    registered(&fixture, 2, 6, BOB).await?;
    fixture.resource(&resource(52)).await?;
    unpositioned(
        &fixture,
        Event::new(
            "unpositioned-b2",
            0,
            0,
            "SourceManifestUpdated",
            V1_REGISTRAR,
        )
        .name(&name(2))
        .resource(&resource(52)),
    )
    .await?;
    let expected = by_block(&fixture).await?;
    let named = source(&fixture, 0, &name(2)).await?;
    let held = source(&fixture, 1, &resource(52)).await?;
    ensure!(
        bucket(&named).is_some_and(|(first, _)| first == -1)
            && bucket(&held) == Some((-1, -1))
            && held.as_ref().is_some_and(|source| {
                source["event_mask"] == json!(event_kind_mask("SourceManifestUpdated"))
            }),
        "block by block, both sources start unpositioned: {named:?} {held:?}"
    );
    require_built(&fixture, &expected, "an unpositioned event").await?;
    ensure!(
        work(&fixture, &resource(52)).await? == Some(true),
        "a composed name's blockless event makes its resource a work resource"
    );
    fixture.cleanup().await
}

// B3. Unnamed record writes by node on each of the three node-record families and one by record
// id: their node and record-id sources exist with the masks and blooms of their writes, though no
// composed name reaches them.
#[tokio::test]
async fn unnamed_record_writes_source_their_nodes_and_record_ids() -> Result<()> {
    let fixture = fixture("families_builder_records").await?;
    registered(&fixture, 1, 5, ALICE).await?;
    for (block, family, at) in [
        (10, "ens_v1_resolver_l1", 81),
        (11, "ens_v2_resolver_l1", 82),
        (12, "basenames_base_resolver", 83),
    ] {
        node_record(&fixture, block, 1, family, &node(at)).await?;
    }
    record_id_record(&fixture, 13, 1, "7").await?;
    let expected = by_block(&fixture).await?;
    let changed = json!(event_kind_mask("RecordChanged"));
    for at in [81, 82, 83] {
        let row = source(&fixture, 2, &node(at)).await?;
        ensure!(
            row.as_ref().is_some_and(|row| row["event_mask"] == changed) && some_bloom(&row),
            "block by block, node {at} has its source: {row:?}"
        );
    }
    let by_id: Vec<Value> = rows(
        &fixture,
        "SELECT to_jsonb(source) - 'chain_id' FROM project_history_source source
         WHERE source_kind = 3 AND source_key = '7' AND resolver_address = $1",
        &[R1],
    )
    .await?;
    ensure!(
        by_id.len() == 1 && by_id[0]["event_mask"] == changed,
        "block by block, the record id has its source: {by_id:?}"
    );
    require_built(&fixture, &expected, "unnamed record writes").await?;
    fixture.cleanup().await
}

// B4. A resource whose only events name no name: its resource anchor holds a historical mask and
// no name anchor exists for it. Its edges equal block by block, and are none: discovery reads a
// pointer's name surface, so an unnamed pointer discovers no edge, and the edge part of the row
// cannot be built.
#[tokio::test]
async fn a_resource_with_only_unnamed_events_anchors_alone() -> Result<()> {
    let fixture = fixture("families_builder_unnamed_resource").await?;
    registered(&fixture, 1, 5, ALICE).await?;
    let lease = resource(61);
    unnamed_transfer(&fixture, 10, 1, &lease, &node(91), ALICE).await?;
    unnamed_transfer(&fixture, 20, 1, &lease, &node(91), BOB).await?;
    fixture
        .write(
            21,
            1,
            "ResolverChanged",
            "ens_v2_registry_l1",
            None,
            Some(&lease),
            json!({"node": node(91), "resolver": R1}),
            R1,
        )
        .await?;
    let expected = by_block(&fixture).await?;
    let held = anchors(&fixture, 1, &lease).await?;
    let named = rows(
        &fixture,
        "SELECT to_jsonb(anchor) FROM project_address_history_anchor anchor
         WHERE anchor_kind = 0 AND anchor_id LIKE '%' || $1",
        &[&node(91)],
    )
    .await?;
    ensure!(
        !held.is_empty()
            && held
                .iter()
                .all(|row| row["historical_mask"] != json!(0) && row["current_mask"] == json!(0))
            && named.is_empty()
            && edges(&fixture, &lease).await?.is_empty(),
        "block by block, the resource anchors with a historical mask alone: {held:?} {named:?}"
    );
    require_built(&fixture, &expected, "a resource of unnamed events").await?;
    ensure!(work(&fixture, &lease).await? == Some(true), "rule R1");
    fixture.cleanup().await
}

/// Names 11 and 12 bound to one resource 200 from block 5: 11 registered to ALICE, 12 to BOB
/// with ALICE its resource controller.
async fn shared(fixture: &Fixture) -> Result<String> {
    let shared = resource(200);
    for (n, log) in [(11, 0), (12, 1)] {
        fixture
            .binding(
                &resource(1000 + n),
                &name(n),
                &shared,
                "ens_v1",
                5,
                log,
                None,
            )
            .await?;
    }
    for (n, block, registrant) in [(11, 5, ALICE), (12, 6, BOB)] {
        fixture
            .write(
                block,
                0,
                "RegistrationGranted",
                V1_REGISTRAR,
                Some(&name(n)),
                Some(&shared),
                json!({"authority_kind": "registrar", "status": "registered",
                       "registrant": registrant, "expiry": 2_000_000_000u64}),
                REGISTRAR,
            )
            .await?;
    }
    fixture
        .event(
            Event::new("controller:12", 7, 0, "PermissionChanged", V1_REGISTRAR)
                .name(&name(12))
                .resource(&shared)
                .after(json!({"subject": ALICE, "scope": {"kind": "resource"},
                              "effective_powers": ["resource_control"]})),
        )
        .await?;
    Ok(shared)
}

// B5. One resource shared by two names composed in different chunks: the resource anchor's
// current mask for an address is the union of both names' current masks for it.
#[tokio::test]
async fn a_shared_resource_unions_its_names_current_masks() -> Result<()> {
    let fixture = fixture("families_builder_shared").await?;
    let shared = shared(&fixture).await?;
    let expected = by_block(&fixture).await?;
    let unions: Vec<(String, i64, i64, i64)> = sqlx::query_as(
        "SELECT resource.address, resource.current_mask::bigint,
                coalesce(bit_or(name.current_mask), 0)::bigint, count(name.anchor_id)
         FROM project_address_history_anchor resource
         LEFT JOIN project_address_history_anchor name ON name.anchor_kind = 0
          AND name.address = resource.address AND name.current_resource_id = $1::uuid
         WHERE resource.anchor_kind = 1 AND resource.anchor_id = $1
         GROUP BY resource.address, resource.current_mask",
    )
    .bind(&shared)
    .fetch_all(&fixture.pool)
    .await?;
    ensure!(
        unions
            .iter()
            .any(|(_, mask, _, names)| *mask != 0 && *names == 2)
            && unions.iter().all(|(_, mask, union, _)| mask == union),
        "block by block, the resource's current masks are its names' unions: {unions:?}"
    );
    require_built(&fixture, &expected, "a shared resource").await?;
    fixture.cleanup().await
}

// B6. A name another name's resource reaches first (12, after 11 in the work list, one a
// chunk) holds both its masks and its current resource once it is composed.
#[tokio::test]
async fn a_name_reached_through_a_shared_resource_keeps_both_masks() -> Result<()> {
    let fixture = fixture("families_builder_widened").await?;
    let shared = shared(&fixture).await?;
    let expected = by_block(&fixture).await?;
    let rows = anchors(&fixture, 0, &name(12)).await?;
    ensure!(
        rows.iter().any(|row| {
            row["historical_mask"] != json!(0)
                && row["current_mask"] != json!(0)
                && row["current_resource_id"] == json!(shared)
        }),
        "block by block, name 12 holds both masks: {rows:?}"
    );
    require_built(&fixture, &expected, "a widened name").await?;
    fixture.cleanup().await
}

/// Name 1 on resource 1 points at `R1` at block 8, with record-id links of `R1` at its node
/// (block 12, record id 7) and at the zero node (block 8, record id 8), node records at block 15
/// and record id 7's record at 16. Name 2 on resource 2 points at `R2`, which nothing links.
async fn edged(fixture: &Fixture) -> Result<()> {
    registered(fixture, 1, 5, ALICE).await?;
    registered(fixture, 2, 6, BOB).await?;
    for (n, block, resolver) in [(1, 8, R1), (2, 9, R2)] {
        fixture
            .write(
                block,
                1,
                "ResolverChanged",
                V1_REGISTRY,
                Some(&name(n)),
                Some(&resource(n)),
                json!({"node": node(n), "resolver": resolver}),
                REGISTRY,
            )
            .await?;
    }
    for (block, at, id) in [(12, node(1), "7"), (8, ZERO_NODE.to_owned(), "8")] {
        fixture
            .write(
                block,
                2,
                "ResolverRecordLinked",
                "ens_v2_resolver_l1",
                None,
                None,
                json!({"resolver": R1, "node": at, "storage_model": "resolver_record_id",
                       "resolver_record_id": id, "source_event": "RecordLinked"}),
                R1,
            )
            .await?;
    }
    node_record(fixture, 15, 1, "ens_v1_resolver_l1", &node(1)).await?;
    record_id_record(fixture, 16, 1, "7").await?;
    Ok(())
}

// B7. Edges: a node edge with no link, a record-id edge with a link at the pointer's node in a
// later block than the pointer, and one with a zero-node link. Each has its bounds, the link bit
// in its event mask and its source's bloom.
#[tokio::test]
async fn resolver_edges_carry_their_links_and_bounds() -> Result<()> {
    let fixture = fixture("families_builder_edges").await?;
    edged(&fixture).await?;
    let expected = by_block(&fixture).await?;
    let first = edges(&fixture, &resource(1)).await?;
    let link = event_kind_mask("ResolverRecordLinked");
    let of = |kind: i64, key: &str| {
        first
            .iter()
            .find(|edge| edge["source_kind"] == json!(kind) && edge["source_key"] == json!(key))
            .cloned()
    };
    let by_node = of(2, &node(1));
    let linked = of(3, "7");
    let zero = of(3, "8");
    ensure!(
        by_node
            .as_ref()
            .is_some_and(|edge| edge["link_event_identity"] == json!("") && some_bloom(&by_node))
            && linked.as_ref().is_some_and(|edge| {
                edge["link_block_number"] == json!(12)
                    && edge["pointer_block_number"] == json!(8)
                    && edge["event_mask"]
                        .as_i64()
                        .is_some_and(|mask| mask & link != 0)
            })
            && zero.as_ref().is_some_and(|edge| {
                edge["node"] == json!(node(1))
                    && edge["event_mask"]
                        .as_i64()
                        .is_some_and(|mask| mask & link != 0)
            }),
        "block by block, the edges of resource 1: {first:?}"
    );
    let second = edges(&fixture, &resource(2)).await?;
    ensure!(
        second.len() == 1 && second[0]["source_kind"] == json!(2),
        "block by block, resource 2 has its node edge alone: {second:?}"
    );
    require_built(&fixture, &expected, "resolver edges").await?;
    fixture.cleanup().await
}

// B8. A source key only an edge reaches, with no event of its own: an empty source row, for a
// record id (the zero-node link's) and for a node (name 2's pointer).
#[tokio::test]
async fn a_key_only_an_edge_reaches_has_an_empty_source() -> Result<()> {
    let fixture = fixture("families_builder_edge_keys").await?;
    edged(&fixture).await?;
    let expected = by_block(&fixture).await?;
    let by_id = rows(
        &fixture,
        "SELECT to_jsonb(source) - 'chain_id' FROM project_history_source source
         WHERE source_kind = 3 AND source_key = '8' AND resolver_address = $1",
        &[R1],
    )
    .await?;
    let by_node = source(&fixture, 2, &node(2)).await?;
    ensure!(
        by_id.len() == 1 && empty(&Some(by_id[0].clone())) && empty(&by_node),
        "block by block, the keys only edges reach have empty sources: {by_id:?} {by_node:?}"
    );
    require_built(&fixture, &expected, "keys only edges reach").await?;
    fixture.cleanup().await
}

// B9. Bloom classes: a record write with a key sets that key's bits, a version bump and an
// event kind outside the catalogue's vocabulary set every bit, and the unknown kind every mask
// bit. The vocabulary check on `normalized_events` is dropped in this database so the unknown
// kind can be written. The event has no block, so no fold reads it.
#[tokio::test]
async fn record_blooms_follow_their_event_kinds() -> Result<()> {
    let fixture = fixture("families_builder_blooms").await?;
    registered(&fixture, 1, 5, ALICE).await?;
    node_record(&fixture, 10, 1, "ens_v1_resolver_l1", &node(81)).await?;
    fixture
        .write(
            11,
            1,
            "RecordVersionChanged",
            "ens_v1_resolver_l1",
            None,
            None,
            json!({"node": node(82), "resolver": R1, "source_event": "VersionChanged"}),
            R1,
        )
        .await?;
    raw_sql("ALTER TABLE normalized_events DROP CONSTRAINT normalized_events_event_kind_check")
        .execute(&fixture.pool)
        .await?;
    fixture.resource(&resource(95)).await?;
    unpositioned(
        &fixture,
        Event::new("unknown-kind", 0, 0, "FutureVocabularyEvent", V1_REGISTRAR)
            .resource(&resource(95)),
    )
    .await?;
    let expected = by_block(&fixture).await?;
    let keyed = source(&fixture, 2, &node(81)).await?;
    let bumped = source(&fixture, 2, &node(82)).await?;
    let unknown = source(&fixture, 1, &resource(95)).await?;
    ensure!(
        some_bloom(&keyed)
            && full_bloom(&bumped)
            && full_bloom(&unknown)
            && unknown
                .as_ref()
                .is_some_and(|source| source["event_mask"] == json!(i64::MAX)),
        "block by block, the blooms: {keyed:?} {bumped:?} {unknown:?}"
    );
    require_built(&fixture, &expected, "bloom classes").await?;
    fixture.cleanup().await
}

/// An ENSv1 name 0x61 whose registry record moves off its registrar lease (resource 1) by a
/// registry transfer to BOB at block 11 (the registry-only binding on resource 2, the
/// handoff), then to CAROL at 13. With `lease`, the registrar's grant on the lease precedes it,
/// so the matcher's registry-only handoff exclusion applies to the later owner writes.
async fn handoff(fixture: &Fixture, lease: bool) -> Result<()> {
    let at = node(0x61);
    let named = format!("ens:{at}");
    let (held, registry_only) = (resource(1), resource(2));
    let write = |block: i64,
                 log: i64,
                 kind: &'static str,
                 family: &'static str,
                 on: String,
                 after: Value| {
        let named = named.clone();
        async move {
            let emitter = if family == V1_REGISTRAR {
                REGISTRAR
            } else {
                REGISTRY
            };
            fixture
                .write(
                    block,
                    log,
                    kind,
                    family,
                    Some(&named),
                    Some(&on),
                    after,
                    emitter,
                )
                .await
        }
    };
    fixture
        .binding(&uuid(100), &named, &held, "ens_v1", 10, 2, Some(11))
        .await?;
    let id = write(
        10,
        1,
        "AuthorityTransferred",
        V1_REGISTRY,
        held.clone(),
        json!({"source_event": "NewOwner", "node": ETH_NODE, "child_node": at,
               "owner": ALICE, "owner_getter": ALICE, "emitter_role": "registry"}),
    )
    .await?;
    set(
        fixture,
        id,
        "derivation_kind = 'ens_v1_unwrapped_authority'",
    )
    .await?;
    if lease {
        let grant = json!({"authority_kind": "registrar", "authority_key": "lease",
                           "namehash": at, "registrant": ALICE, "expiry": 2_100_000_000u64,
                           "source_event": "NameRegistered"});
        let mut bound = grant.clone();
        bound["owner_getter"] = json!(ALICE);
        bound["binding_kind"] = json!("declared_registry_path");
        bound["registry_contract"] = json!(REGISTRY);
        let mut granted = grant.clone();
        granted["status"] = json!("registered");
        for (kind, after) in [
            ("SurfaceBound", bound),
            ("AuthorityEpochChanged", grant.clone()),
            ("RegistrationGranted", granted),
            ("ExpiryChanged", grant),
        ] {
            write(10, 2, kind, V1_REGISTRAR, held.clone(), after).await?;
        }
    }
    for (binding, block, owner, closed) in [(101, 11, BOB, Some(13)), (102, 13, CAROL, None)] {
        fixture
            .binding(
                &uuid(binding),
                &named,
                &registry_only,
                "ens_v1",
                block,
                1,
                closed,
            )
            .await?;
        let after = json!({"source_event": "Transfer", "node": at, "owner": owner,
                           "owner_getter": owner, "emitter_role": "registry",
                           "authority_kind": "registry_only", "authority_key": "registry-only"});
        let mut bound = after.clone();
        bound["binding_kind"] = json!("declared_registry_path");
        bound["registry_contract"] = json!(REGISTRY);
        for (kind, after) in [
            ("SurfaceBound", bound),
            ("AuthorityEpochChanged", after.clone()),
            ("AuthorityTransferred", after),
        ] {
            write(block, 1, kind, V1_REGISTRY, registry_only.clone(), after).await?;
        }
    }
    Ok(())
}

// B10. The registry-only handoff exclusion: owners before and after a handoff, with the lease's
// registrar events and without them. The historical masks equal block by block in both, and
// the exclusion is live in the first: its owner masks on the registry-only resource differ
// from the second's.
#[tokio::test]
async fn the_registry_only_handoff_exclusion_holds_in_the_build() -> Result<()> {
    let mut masks = Vec::new();
    for lease in [true, false] {
        let fixture = fixture("families_builder_handoff").await?;
        handoff(&fixture, lease).await?;
        let expected = by_block(&fixture).await?;
        let qualifying: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM project_binding_candidate handoff
             WHERE handoff.registry_only AND EXISTS (
                 SELECT 1 FROM normalized_events lease
                 WHERE lease.resource_id = handoff.lease_resource_id
                   AND lease.source_family = 'ens_v1_registrar_l1'
                   AND lease.canonicality_state <> 'orphaned')",
        )
        .fetch_one(&fixture.pool)
        .await?;
        let held = anchors(&fixture, 1, &resource(2)).await?;
        ensure!(
            (qualifying > 0) == lease && !held.is_empty(),
            "lease {lease}: {qualifying} qualifying handoffs, registry-only anchors {held:?}"
        );
        masks.push(
            held.iter()
                .map(|row| (row["address"].clone(), row["historical_mask"].clone()))
                .collect::<Vec<_>>(),
        );
        require_built(&fixture, &expected, &format!("a handoff, lease {lease}")).await?;
        fixture.cleanup().await?;
    }
    ensure!(
        masks[0] != masks[1],
        "the exclusion changes the owner masks: {masks:?}"
    );
    Ok(())
}

// C2. A resource whose only event has no block, activated and canonical, which no composed
// name reaches (rule R5 without work): a source that starts unpositioned, no edge, no resource
// anchor, and not a work resource.
#[tokio::test]
async fn an_unpositioned_resource_no_name_reaches_is_a_source_alone() -> Result<()> {
    let fixture = fixture("families_builder_source_only").await?;
    registered(&fixture, 1, 5, ALICE).await?;
    let alone = resource(70);
    fixture.resource(&alone).await?;
    unpositioned(
        &fixture,
        Event::new(
            "unpositioned-c2",
            0,
            0,
            "TokenControlTransferred",
            V1_REGISTRAR,
        )
        .resource(&alone)
        .after(
            json!({"namehash": node(92), "to": ALICE, "source_event": "Transfer",
                          "authority_kind": "registrar"}),
        ),
    )
    .await?;
    let expected = by_block(&fixture).await?;
    let row = source(&fixture, 1, &alone).await?;
    ensure!(
        bucket(&row) == Some((-1, -1))
            && edges(&fixture, &alone).await?.is_empty()
            && anchors(&fixture, 1, &alone).await?.is_empty(),
        "block by block, a source alone: {row:?}"
    );
    require_built(&fixture, &expected, "an unpositioned resource").await?;
    ensure!(
        work(&fixture, &alone).await? == Some(false),
        "not a work resource"
    );
    fixture.cleanup().await
}

// C3. A resource only an orphaned event touches and one only a pending (candidate) event
// touches, both in a block at or below the pass block (rule R1 reads any event): each has an
// empty source and is a work resource, with no edge and no resource anchor.
#[tokio::test]
async fn resources_of_orphaned_or_pending_events_have_empty_sources() -> Result<()> {
    let fixture = fixture("families_builder_unread").await?;
    registered(&fixture, 1, 5, ALICE).await?;
    registered(&fixture, 2, 14, BOB).await?;
    let (orphaned, pending) = (resource(71), resource(72));
    let id = unnamed_transfer(&fixture, 10, 1, &orphaned, &node(93), ALICE).await?;
    set(&fixture, id, "canonicality_state = 'orphaned'").await?;
    let id = unnamed_transfer(&fixture, 11, 1, &pending, &node(94), ALICE).await?;
    set(
        &fixture,
        id,
        "consumer_visibility = 'candidate', migration_correlation_ids = ARRAY['probe']",
    )
    .await?;
    let expected = by_block(&fixture).await?;
    for held in [&orphaned, &pending] {
        let row = source(&fixture, 1, held).await?;
        ensure!(
            empty(&row)
                && edges(&fixture, held).await?.is_empty()
                && anchors(&fixture, 1, held).await?.is_empty(),
            "block by block, {held} has an empty source alone: {row:?}"
        );
    }
    require_built(&fixture, &expected, "unread events").await?;
    for held in [&orphaned, &pending] {
        ensure!(
            work(&fixture, held).await? == Some(true),
            "{held} is a work resource"
        );
    }
    fixture.cleanup().await
}

// C4. A name whose only event is orphaned and whose surface comes after the pass block, so only
// rule N3 names it, and a registry child no event names that the work list holds (rule N1, an
// id of `project_registry_node_state`): each has an empty name source and no name anchor.
#[tokio::test]
async fn names_of_no_read_event_have_empty_sources() -> Result<()> {
    let fixture = fixture("families_builder_unread_names").await?;
    registered(&fixture, 1, 5, ALICE).await?;
    registered(&fixture, 2, 14, BOB).await?;
    let late = name(31);
    let id = fixture
        .write(
            12,
            1,
            "TokenControlTransferred",
            V1_REGISTRAR,
            Some(&late),
            None,
            json!({"namehash": node(31), "to": ALICE, "source_event": "Transfer",
                   "authority_kind": "registrar"}),
            REGISTRAR,
        )
        .await?;
    set(&fixture, id, "canonicality_state = 'orphaned'").await?;
    sqlx::query(
        "UPDATE name_surfaces SET block_number = 35, block_hash = $2 WHERE logical_name_id = $1",
    )
    .bind(&late)
    .bind(hash(35))
    .execute(&fixture.pool)
    .await?;
    let child = name(33);
    fixture.resource(&resource(32)).await?;
    let id = fixture
        .event(
            Event::new("child:7", 7, 1, "SubregistryChanged", V1_REGISTRY)
                .resource(&resource(32))
                .after(json!({"source_event": "NewOwner", "node": node(1),
                              "child_node": node(33), "labelhash": node(34), "owner": ALICE,
                              "owner_getter": ALICE, "emitter_role": "registry"}))
                .raw(json!({"emitting_address": REGISTRY})),
        )
        .await?;
    set(
        &fixture,
        id,
        "derivation_kind = 'ens_v1_unwrapped_authority'",
    )
    .await?;
    let expected = by_block(&fixture).await?;
    let state: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_registry_node_state
         WHERE namespace || ':' || lower(node) = $1",
    )
    .bind(&child)
    .fetch_one(&fixture.pool)
    .await?;
    for id in [&late, &child] {
        let row = source(&fixture, 0, id).await?;
        ensure!(
            empty(&row) && anchors(&fixture, 0, id).await?.is_empty(),
            "block by block, {id} has an empty name source alone: {row:?}"
        );
    }
    ensure!(state == 1, "the child is a registry node: {state}");
    require_built(&fixture, &expected, "names of no read event").await?;
    fixture.cleanup().await
}

// C5. A composed name's unpositioned pointer on a resource no positioned event touches (rule
// R4's blockless part): the resource is a work resource and its pointer discovers edges. The
// matcher reads positioned events only, so it gets no resource anchor.
#[tokio::test]
async fn a_composed_names_unpositioned_pointer_discovers_edges() -> Result<()> {
    let fixture = fixture("families_builder_unpositioned_pointer").await?;
    registered(&fixture, 1, 5, ALICE).await?;
    registered(&fixture, 3, 6, BOB).await?;
    let pointed = resource(75);
    fixture.resource(&pointed).await?;
    unpositioned(
        &fixture,
        Event::new(
            "unpositioned-c5",
            0,
            0,
            "ResolverChanged",
            "ens_v2_registry_l1",
        )
        .name(&name(3))
        .resource(&pointed)
        .after(json!({"node": node(3), "resolver": R1})),
    )
    .await?;
    let expected = by_block(&fixture).await?;
    let found = edges(&fixture, &pointed).await?;
    let row = source(&fixture, 1, &pointed).await?;
    ensure!(
        !found.is_empty()
            && bucket(&row) == Some((-1, -1))
            && anchors(&fixture, 1, &pointed).await?.is_empty(),
        "block by block, the blockless pointer's edges: {found:?} {row:?}"
    );
    require_built(&fixture, &expected, "an unpositioned pointer").await?;
    ensure!(
        work(&fixture, &pointed).await? == Some(true),
        "rule R4 makes it a work resource"
    );
    fixture.cleanup().await
}

// C15. A resource with no event the catalogue reads and no edge: its source is empty, and its
// resource anchor, whose envelope those two would give, is written only for a nonzero mask, so
// there is none. Here a name is bound to the resource and registered by an event that carries
// no resource, and a token transfer on the resource is orphaned. A resource anchor with a mask
// and an empty envelope cannot be built: a resource's historical mask needs a read event on it,
// which fills its source, and block by block gives this binding's resource no current relation.
#[tokio::test]
async fn a_resource_with_an_empty_envelope_has_no_anchor() -> Result<()> {
    let fixture = fixture("families_builder_empty_envelope").await?;
    registered(&fixture, 1, 5, ALICE).await?;
    let (bound, granted) = (resource(141), name(41));
    fixture
        .binding(&resource(1041), &granted, &bound, "ens_v1", 6, 0, None)
        .await?;
    fixture
        .write(
            6,
            0,
            "RegistrationGranted",
            V1_REGISTRAR,
            Some(&granted),
            None,
            json!({"authority_kind": "registrar", "status": "registered", "registrant": BOB,
                   "expiry": 2_000_000_000u64}),
            REGISTRAR,
        )
        .await?;
    let id = unnamed_transfer(&fixture, 7, 1, &bound, &node(41), BOB).await?;
    set(&fixture, id, "canonicality_state = 'orphaned'").await?;
    let expected = by_block(&fixture).await?;
    let row = source(&fixture, 1, &bound).await?;
    ensure!(
        empty(&row)
            && edges(&fixture, &bound).await?.is_empty()
            && anchors(&fixture, 1, &bound).await?.is_empty(),
        "block by block, an empty source and no anchor: {row:?}"
    );
    require_built(&fixture, &expected, "an empty envelope").await?;
    ensure!(
        work(&fixture, &bound).await? == Some(true),
        "the bound resource is a work resource"
    );
    fixture.cleanup().await
}

/// The families of `edged` and `shared` together, for the cases about the build's steps.
async fn mixed(prefix: &str) -> Result<Fixture> {
    let fixture = fixture(prefix).await?;
    edged(&fixture).await?;
    shared(&fixture).await?;
    unnamed_transfer(&fixture, 20, 1, &resource(61), &node(91), CAROL).await?;
    Ok(fixture)
}

// C1, C6 and C12, with the risk-6 check. The compose fixture builds to the same families under
// one span and one partition, three and two, and the default sixty-four and sixteen, with names
// one and five a chunk, and the build's key set is the same under each. Two partitions split
// names from their current resources. No resource's events carry two namespaces, which the
// anchor key relies on. The address history pages of three addresses read the same from the
// built catalogue as from the block-by-block one.
#[tokio::test]
async fn the_build_is_independent_of_its_split() -> Result<()> {
    const SEEDED: i64 = 300;
    let fixture = seeded("families_builder_split").await?;
    install_probe(&fixture).await?;
    // The catalogue reader serves a publication within its lag tolerance of the head, with
    // Interpret out of redo.
    fixture.interpret_row("h1", 1, false).await?;
    sqlx::query(
        "INSERT INTO chain_heads (chain_id, latest_block_hash, latest_block_number)
         VALUES ($1, $2, $3)",
    )
    .bind(CHAIN)
    .bind(hash(SEEDED))
    .bind(SEEDED)
    .execute(&fixture.pool)
    .await?;
    let off = FamilyOptions::new(CONTENT_HASH)
        .with_max_blocks_per_run(u64::MAX)
        .with_rebuild_ranges(RebuildRanges::Off);
    fixture
        .apply_with(SEEDED, FamilyMode::Rebuild, &off)
        .await?;
    let expected = fixture.exact().await?;
    let addresses: Vec<String> = sqlx::query_scalar(
        "SELECT address FROM project_address_history_anchor
         GROUP BY address ORDER BY count(*) DESC, address LIMIT 3",
    )
    .fetch_all(&fixture.pool)
    .await?;
    let pages = history_pages(&fixture, &addresses, SEEDED).await?;
    ensure!(
        addresses.len() == 3 && pages.iter().all(|page| !page.is_empty()),
        "three addresses with history: {addresses:?} {pages:?}"
    );
    let mut keys = Vec::new();
    for ((spans, partitions), chunk) in [((1, 1), 1), ((3, 2), 5), ((64, 16), 1), ((64, 16), 5)] {
        let label = format!("({spans}, {partitions}), {chunk} a chunk");
        let options = FamilyOptions::new(CONTENT_HASH)
            .with_max_blocks_per_run(u64::MAX)
            .with_rebuild_ranges(RebuildRanges::Through(SEEDED))
            .with_compose_chunk_keys(chunk)
            .with_compose_builder_partitions(spans, partitions);
        reset_probe(&fixture).await?;
        fixture
            .apply_with(SEEDED, FamilyMode::Rebuild, &options)
            .await?;
        ensure!(
            probe_build(&fixture).await? == build_order(spans.into(), partitions.into()),
            "{label}: the build ran each partition once"
        );
        require_equal(&fixture.exact().await?, &expected, &label)?;
        keys.push(
            rows(
                &fixture,
                "SELECT to_jsonb(key) FROM (
                     SELECT source_kind, source_key, resolver_address, bool_or(work) AS work
                     FROM compose_probe_work GROUP BY 1, 2, 3) key
                 ORDER BY to_jsonb(key)::text",
                &[],
            )
            .await?,
        );
        if (spans, partitions) == (3, 2) {
            let split: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM project_address_history_anchor anchor
                 WHERE anchor.anchor_kind = 0 AND anchor.current_resource_id IS NOT NULL
                   AND (hashtext(anchor.anchor_id) & 1)
                       <> (hashtext(anchor.current_resource_id::text) & 1)",
            )
            .fetch_one(&fixture.pool)
            .await?;
            ensure!(
                split > 0,
                "two partitions split a name from its current resource"
            );
        }
    }
    ensure!(
        !keys[0].is_empty() && keys.iter().all(|set| *set == keys[0]),
        "the same key set under every split"
    );
    let shared_namespaces: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM (SELECT resource_id FROM normalized_events
             WHERE resource_id IS NOT NULL GROUP BY 1 HAVING count(DISTINCT namespace) > 1) shared",
    )
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(
        shared_namespaces == 0,
        "{shared_namespaces} resources with events of two namespaces"
    );
    ensure!(
        history_pages(&fixture, &addresses, SEEDED).await? == pages,
        "the built catalogue serves the block-by-block pages"
    );
    fixture.cleanup().await
}

/// The first page of each address's history in `ens`, served from the catalogue at the family
/// marker's publication.
async fn history_pages(
    fixture: &Fixture,
    addresses: &[String],
    through: i64,
) -> Result<Vec<Vec<String>>> {
    use bigname_storage::{
        HistoryCataloguePublication, HistoryCataloguePublicationFence, HistoryPageOptions,
        HistoryScope, HistorySummaryMode,
    };
    let (number, block_hash, generation): (i64, String, String) = sqlx::query_as(
        "SELECT current_block_number, current_block_hash, sequence::text
         FROM project_family_marker WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(number == through, "the marker is at {number}");
    let options = HistoryPageOptions {
        publication_block_bounds: Some([(CHAIN.to_owned(), number)].into()),
        catalogue_publication: Some(HistoryCataloguePublicationFence::Captured {
            publications: vec![HistoryCataloguePublication {
                chain_id: CHAIN.to_owned(),
                block_number: number,
                block_hash,
                project_generation: generation,
            }],
            lag_tolerance_blocks: 10,
            captured_at: std::time::Instant::now(),
        }),
        ..Default::default()
    };
    let state: Vec<Value> = rows(
        fixture,
        "SELECT to_jsonb(m) - 'admission_manifests' FROM project_family_marker m
         UNION ALL SELECT to_jsonb(p) FROM chain_phase_state p",
        &[],
    )
    .await?;
    let mut pages = Vec::new();
    for address in addresses {
        let page = bigname_storage::load_address_history_page_for_relations(
            &fixture.pool,
            address,
            Some("ens"),
            None,
            HistoryScope::Both,
            true,
            None,
            200,
            HistorySummaryMode::Count,
            &options,
            false,
        )
        .await
        .map_err(|error| anyhow::anyhow!("{error:#}: {state:?}"))?;
        pages.push(
            page.rows
                .into_iter()
                .map(|row| row.event_identity)
                .collect(),
        );
    }
    Ok(pages)
}

/// The record's pass stream and cursor.
async fn pass_cursor(fixture: &Fixture) -> Result<(Value, Value)> {
    let record = fixture.repair_record().await?.unwrap_or_default();
    Ok((
        record["compose_stream"].clone(),
        record["compose_cursor"].clone(),
    ))
}

/// Run budgets of one block, the first a rebuild, until `until` holds for the record's stream
/// and cursor.
async fn run_until(
    fixture: &Fixture,
    options: &FamilyOptions,
    until: impl Fn(&Value, &Value) -> bool,
) -> Result<()> {
    let one = options.clone().with_max_blocks_per_run(1);
    for run in 0..500 {
        let mode = if run == 0 {
            FamilyMode::Rebuild
        } else {
            FamilyMode::Normal
        };
        fixture.apply_with(TARGET, mode, &one).await?;
        let (stream, cursor) = pass_cursor(fixture).await?;
        if until(&stream, &cursor) {
            return Ok(());
        }
    }
    anyhow::bail!("the pass never reached the point")
}

// B12. A crash inside the build: the transaction of the nth partition fails once, for a
// partition inside the spans, the sources, the edge bounds and the resource anchors. The run
// returns the error and the cursor names the partition before, the next run commits that
// partition and goes on, every partition commits exactly once over both runs, and the families
// equal block by block.
#[tokio::test]
async fn a_build_partition_that_fails_is_built_again_from_the_cursor() -> Result<()> {
    let fixture = mixed("families_builder_crash").await?;
    let expected = by_block(&fixture).await?;
    let options = ranged((3, 2), 5);
    let order = build_order(3, 2);
    for (step, part) in [
        ("spans", 1),
        ("sources", 1),
        ("edge_bounds", 0),
        ("resource_anchors", 1),
    ] {
        raw_sql(&format!(
            "CREATE SEQUENCE fail_partition_once;
             CREATE FUNCTION fail_partition() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 IF NEW.compose_stream = 3 AND NEW.compose_cursor IS NOT NULL THEN
                     IF NEW.compose_cursor::jsonb ->> 'step' = '{step}'
                        AND (NEW.compose_cursor::jsonb ->> 'part')::int = {part} THEN
                         IF nextval('fail_partition_once') = 1 THEN
                             RAISE EXCEPTION 'injected partition failure';
                         END IF;
                     END IF;
                 END IF;
                 RETURN NEW;
             END $$;
             CREATE TRIGGER fail_partition BEFORE UPDATE ON project_repair_record
             FOR EACH ROW EXECUTE FUNCTION fail_partition();"
        ))
        .execute(&fixture.pool)
        .await?;
        reset_probe(&fixture).await?;
        let error = fixture
            .apply_with(TARGET, FamilyMode::Rebuild, &options)
            .await
            .expect_err("the partition fails");
        ensure!(
            error.to_string().contains("injected partition failure"),
            "{error}"
        );
        let failed = order
            .iter()
            .position(|at| *at == (step.to_owned(), part))
            .expect("a partition of the order");
        let (stream, cursor) = pass_cursor(&fixture).await?;
        ensure!(
            stream == json!(3)
                && cursor
                    .as_str()
                    .and_then(build_step)
                    .is_some_and(|(at, _)| at == order[failed - 1]),
            "{step} {part}: the cursor names the partition before: {cursor}"
        );
        raw_sql(
            "DROP TRIGGER fail_partition ON project_repair_record;
             DROP FUNCTION fail_partition(); DROP SEQUENCE fail_partition_once;",
        )
        .execute(&fixture.pool)
        .await?;
        let resumed = fixture
            .apply_with(TARGET, FamilyMode::Normal, &options)
            .await?;
        ensure!(!resumed.reset, "{step} {part}: resumed, {resumed:?}");
        ensure!(
            probe_build(&fixture).await? == order,
            "{step} {part}: each partition committed once: {:?}",
            probe_build(&fixture).await?
        );
        require_equal(
            &fixture.exact().await?,
            &expected,
            &format!("after a failed {step} {part}"),
        )?;
    }
    fixture.cleanup().await
}

// B13. The build's start gate: a work list row left uncomposed when the build starts fails the
// run with the gate's error naming one row, and no catalogue row is written.
#[tokio::test]
async fn the_build_refuses_an_uncomposed_name() -> Result<()> {
    let fixture = mixed("families_builder_gate").await?;
    let options = ranged((3, 2), 5);
    run_until(&fixture, &options, |stream, cursor| {
        *stream == json!(3) && cursor.is_null()
    })
    .await?;
    sqlx::query("UPDATE project_compose_name_work SET composed = false WHERE serial = 1")
        .execute(&fixture.pool)
        .await?;
    let error = fixture
        .apply_with(TARGET, FamilyMode::Normal, &options)
        .await
        .expect_err("the gate refuses");
    ensure!(
        error
            .to_string()
            .contains("found 1 names of the work list not composed"),
        "{error}"
    );
    let written: i64 = sqlx::query_scalar(
        "SELECT (SELECT count(*) FROM project_history_source)
              + (SELECT count(*) FROM project_history_source_edge)
              + (SELECT count(*) FROM project_address_history_anchor)
              + (SELECT count(*) FROM project_compose_history_work)",
    )
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(written == 0, "{written} catalogue rows written");
    fixture.cleanup().await
}

// C8. The names stream's chunks mark exactly their own work rows composed, and persist the
// current relations of each composed name: none below reach 2 or for a name without one, and for
// the others the relations the built anchors carry, mask and current resource.
#[tokio::test]
async fn a_names_chunk_marks_its_rows_and_persists_their_relations() -> Result<()> {
    let fixture = mixed("families_builder_marks").await?;
    let options = ranged((3, 2), 2);
    run_until(&fixture, &options, |stream, cursor| {
        *stream == json!(0) && cursor.is_string()
    })
    .await?;
    let (rows, composed): (i64, Vec<i64>) = sqlx::query_as(
        "SELECT count(*), coalesce(array_agg(serial ORDER BY serial) FILTER (WHERE composed), '{}')
         FROM project_compose_name_work",
    )
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(
        rows > 2 && composed == vec![1, 2],
        "one chunk of two names marks its two rows: {rows} rows, composed {composed:?}"
    );
    fixture
        .apply_with(TARGET, FamilyMode::Normal, &options)
        .await?;
    let unmarked: (i64, i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE NOT composed),
                count(*) FILTER (WHERE reach < 2 AND relations IS NOT NULL),
                count(*) FILTER (WHERE relations = '[]'::jsonb)
         FROM compose_probe_names",
    )
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(
        unmarked == (0, 0, 0),
        "every row composed, relations only above reach 1 and never empty: {unmarked:?}"
    );
    let (persisted, differing): (i64, i64) = sqlx::query_as(&format!(
        "WITH persisted AS (
             SELECT work.logical_name_id AS anchor_id, relation.address,
                    bit_or({mask}) AS current_mask, max(relation.resource_id::text) AS resource
             FROM compose_probe_names work
             CROSS JOIN LATERAL jsonb_to_recordset(work.relations)
                 relation(address text, namespace text, resource_id uuid, relation text)
             GROUP BY 1, 2
             HAVING bit_or({mask}) <> 0)
         SELECT count(*), count(*) FILTER (WHERE anchor.anchor_id IS NULL
                    OR anchor.current_mask <> persisted.current_mask
                    OR anchor.current_resource_id::text <> persisted.resource)
         FROM persisted
         LEFT JOIN project_address_history_anchor anchor ON anchor.anchor_kind = 0
          AND anchor.anchor_id = persisted.anchor_id AND anchor.address = persisted.address",
        mask = relation_mask_sql("relation.relation"),
    ))
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(
        persisted > 0 && differing == 0,
        "{differing} of {persisted} persisted relations differ from the built anchors"
    );
    fixture.cleanup().await
}

// C10. The split changes between two runs of one build, or the server version the cursor
// recorded is not the server's: the next run fails naming both, and writes nothing.
#[tokio::test]
async fn a_build_refuses_another_split_or_server_version() -> Result<()> {
    let fixture = mixed("families_builder_resplit").await?;
    run_until(&fixture, &ranged((3, 2), 5), |stream, cursor| {
        *stream == json!(3)
            && cursor
                .as_str()
                .and_then(build_step)
                .is_some_and(|(at, _)| at.0 == "spans")
    })
    .await?;
    let before = fixture.exact().await?;
    let work: i64 = sqlx::query_scalar("SELECT count(*) FROM project_compose_history_work")
        .fetch_one(&fixture.pool)
        .await?;
    let error = fixture
        .apply_with(TARGET, FamilyMode::Normal, &ranged((2, 2), 5))
        .await
        .expect_err("another split is refused");
    let text = error.to_string();
    ensure!(
        text.contains("= (3, 2)") && text.contains("this run has (2, 2)"),
        "{text}"
    );
    let version: i64 = sqlx::query_scalar("SELECT current_setting('server_version_num')::bigint")
        .fetch_one(&fixture.pool)
        .await?;
    sqlx::query(
        "UPDATE project_repair_record SET compose_cursor =
             jsonb_set(compose_cursor::jsonb, '{server_version_num}', to_jsonb($1::bigint))::text",
    )
    .bind(version - 1)
    .execute(&fixture.pool)
    .await?;
    let error = fixture
        .apply_with(TARGET, FamilyMode::Normal, &ranged((3, 2), 5))
        .await
        .expect_err("another server version is refused");
    let text = error.to_string();
    ensure!(
        text.contains(&format!("server version {}", version - 1))
            && text.contains(&format!("server version {version}")),
        "{text}"
    );
    let after: i64 = sqlx::query_scalar("SELECT count(*) FROM project_compose_history_work")
        .fetch_one(&fixture.pool)
        .await?;
    ensure!(
        fixture.exact().await? == before && after == work,
        "the refused runs wrote nothing"
    );
    fixture.cleanup().await
}

// B18 and C11. The build under the `work_mem` option and without it projects the same families;
// inside each build transaction the setting is the option, inside the names stream's it is the
// server's, and after the run the pool's connection has the server's again.
#[tokio::test]
async fn the_build_work_mem_is_local_to_its_transactions() -> Result<()> {
    let fixture = mixed("families_builder_work_mem").await?;
    let expected = by_block(&fixture).await?;
    let server: String = sqlx::query_scalar("SELECT current_setting('work_mem')")
        .fetch_one(&fixture.pool)
        .await?;
    ensure!(server != "64MB", "the server default is not the option");
    for work_mem in [None, Some("64MB")] {
        let options = ranged((3, 2), 5).with_compose_builder_work_mem(work_mem.map(str::to_owned));
        build(&fixture, &options, &format!("work_mem {work_mem:?}")).await?;
        require_equal(
            &fixture.exact().await?,
            &expected,
            &format!("work_mem {work_mem:?}"),
        )?;
        let seen: Vec<(Option<i16>, String)> =
            sqlx::query_as("SELECT stream, work_mem FROM compose_probe_log ORDER BY seen")
                .fetch_all(&fixture.pool)
                .await?;
        let wanted = work_mem.unwrap_or(&server);
        ensure!(
            seen.iter().any(|(stream, _)| *stream == Some(0))
                && seen.iter().all(|(stream, setting)| {
                    if *stream == Some(3) {
                        setting == wanted
                    } else {
                        *setting == server
                    }
                }),
            "work_mem {work_mem:?}: {seen:?}"
        );
        let after: String = sqlx::query_scalar("SELECT current_setting('work_mem')")
            .fetch_one(&fixture.pool)
            .await?;
        ensure!(after == server, "after the run the session has {after}");
    }
    fixture.cleanup().await
}

// C16. The build refreshes the planner statistics of what it wrote: after the pass the three
// catalogue tables and the build's work table were analysed after the build began.
#[tokio::test]
async fn the_build_analyses_its_tables() -> Result<()> {
    let fixture = mixed("families_builder_analyse").await?;
    build(&fixture, &ranged((3, 2), 5), "analysed").await?;
    let began: time::OffsetDateTime =
        sqlx::query_scalar("SELECT min(at) FROM compose_probe_log WHERE stream = 3")
            .fetch_one(&fixture.pool)
            .await?;
    let tables = [
        "project_compose_history_work",
        "project_history_source",
        "project_history_source_edge",
        "project_address_history_anchor",
    ];
    let mut analysed = Vec::new();
    // Table statistics reach the view once the backend flushes them.
    for _ in 0..50 {
        // Postgres 15 and later. An older server flushes on its own.
        let _ = raw_sql("SELECT pg_stat_force_next_flush()")
            .execute(&fixture.pool)
            .await;
        analysed = sqlx::query_as(
            "SELECT relname::text, last_analyze FROM pg_stat_user_tables
             WHERE schemaname = 'bigname_phase' AND relname = ANY($1) ORDER BY 1",
        )
        .bind(&tables[..])
        .fetch_all(&fixture.pool)
        .await?;
        if analysed.len() == tables.len()
            && analysed
                .iter()
                .all(|(_, at): &(String, Option<time::OffsetDateTime>)| {
                    at.is_some_and(|at| at >= began)
                })
        {
            return fixture.cleanup().await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    anyhow::bail!("tables not analysed after {began}: {analysed:?}")
}

// B15. A hot address: one address holding ten thousand names, registered over twenty blocks.
// The build's anchors equal block by block, and every name anchors the address.
#[tokio::test]
async fn a_hot_address_builds_as_block_by_block() -> Result<()> {
    const HOT: &str = "0x00000000000000000000000000000000000000ff";
    let fixture = fixture("families_builder_hot").await?;
    raw_sql(&format!(
        "CREATE TEMP TABLE hot AS
         SELECT i, '0x' || md5('h' || i) || md5('k' || i) AS namehash,
                'ens:0x' || md5('h' || i) || md5('k' || i) AS logical_name_id,
                md5('q' || i)::uuid AS lease, 1 + (i % 20) AS block
         FROM generate_series(1, 10000) i;
         INSERT INTO resources (resource_id, chain_id, block_hash, block_number,
             canonicality_state)
         SELECT lease, '{CHAIN}', '0x' || lpad(to_hex(block), 64, '0'), block, 'canonical'
         FROM hot;
         INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state)
         SELECT logical_name_id, 'ens', 'h' || i || '.eth', ARRAY['h' || i, 'eth'], '\\x00',
                namehash, ARRAY['0x' || md5('a' || i) || md5('b' || i), '0x' || lpad('2', 64, '0')],
                'fixture', 'active', '{CHAIN}', '0x' || lpad(to_hex(block), 64, '0'), block,
                'canonical'
         FROM hot;
         INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id,
             binding_kind, authority_arm, active_from, chain_id, block_hash, block_number,
             canonicality_state, provenance)
         SELECT md5('s' || i)::uuid, logical_name_id, lease, 'declared_registry_path', 'ens_v1',
                to_timestamp(1800000000 + block * 12), '{CHAIN}',
                '0x' || lpad(to_hex(block), 64, '0'), block, 'canonical',
                jsonb_build_object('transaction_index', 0, 'log_index', i)
         FROM hot;
         INSERT INTO normalized_events (event_identity, namespace, logical_name_id, resource_id,
             event_kind, source_family, manifest_version, chain_id, block_number, block_hash,
             transaction_hash, transaction_index, log_index, derivation_kind,
             canonicality_state, after_state, raw_fact_ref)
         SELECT 'hot:' || i, 'ens', logical_name_id, lease, 'RegistrationGranted',
                'ens_v1_registrar_l1', 1, '{CHAIN}', block, '0x' || lpad(to_hex(block), 64, '0'),
                '0x' || md5('t' || i), 0, i, 'ens_v2_registry_resource_surface', 'canonical',
                jsonb_build_object('authority_kind', 'registrar', 'status', 'registered',
                    'registrant', '{HOT}', 'expiry', 2000000000, 'namehash', namehash),
                jsonb_build_object('emitting_address', '{REGISTRAR}')
         FROM hot;"
    ))
    .execute(&fixture.pool)
    .await?;
    let expected = by_block(&fixture).await?;
    let held: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_address_history_anchor
         WHERE address = $1 AND anchor_kind = 0 AND historical_mask <> 0",
    )
    .bind(HOT)
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(
        held == 10_000,
        "block by block, the hot address anchors {held} names"
    );
    // One split only: the block-by-block rebuild of ten thousand names is most of the time.
    build(&fixture, &ranged((3, 2), 1000), "a hot address").await?;
    require_equal(&fixture.exact().await?, &expected, "a hot address")?;
    fixture.cleanup().await
}

// B11. A Basenames history on its own chain: names granted on their leases and an unnamed record
// write by node on the Basenames resolver build as block by block.
#[tokio::test]
async fn a_basenames_chain_builds_as_block_by_block() -> Result<()> {
    const BASE: &str = "base-sepolia";
    let fixture = fixture("families_builder_basenames").await?;
    fixture.lineage(BASE, TARGET).await?;
    for n in 1..=3u64 {
        let other = format!("basenames:{}", node(n));
        fixture.surface_on(BASE, &other, &node(n)).await?;
        let lease = resource(n);
        sqlx::query(
            "INSERT INTO resources (resource_id, chain_id, block_hash, block_number,
                 canonicality_state)
             VALUES ($1::uuid, $2, $3, 0, 'canonical')",
        )
        .bind(&lease)
        .bind(BASE)
        .bind(hash(0))
        .execute(&fixture.pool)
        .await?;
        let identity = format!("base-grant-{n}");
        let block = 4 + i64::try_from(n)?;
        fixture
            .event(
                Event::new(&identity, block, 0, "RegistrationGranted", V1_REGISTRAR)
                    .on(BASE)
                    .name(&other)
                    .resource(&lease)
                    .after(
                        json!({"authority_kind": "registrar", "status": "registered",
                                  "registrant": ALICE, "expiry": 2_000_000_000u64}),
                    ),
            )
            .await?;
    }
    fixture
        .event(
            Event::new(
                "base-record",
                9,
                0,
                "RecordChanged",
                "basenames_base_resolver",
            )
            .on(BASE)
            .after(
                json!({"node": node(1), "resolver": R1, "record_key": "addr:60",
                              "record_family": "addr", "selector_key": "60", "value": ALICE,
                              "source_event": "AddressChanged"}),
            )
            .raw(json!({"emitting_address": R1})),
        )
        .await?;
    let run = |mode: FamilyMode, options: FamilyOptions| {
        let pool = fixture.pool.clone();
        async move {
            let token = families::input_token(&pool, BASE).await?;
            families::apply(&pool, BASE, &marker(TARGET), mode, &token, &options).await
        }
    };
    let off = FamilyOptions::new(CONTENT_HASH)
        .with_max_blocks_per_run(u64::MAX)
        .with_rebuild_ranges(RebuildRanges::Off);
    run(FamilyMode::Rebuild, off).await?;
    let expected = fixture.exact().await?;
    let sources: i64 =
        sqlx::query_scalar("SELECT count(*) FROM project_history_source WHERE chain_id = $1")
            .bind(BASE)
            .fetch_one(&fixture.pool)
            .await?;
    ensure!(sources > 0, "block by block, Basenames sources");
    for (layout, chunk) in LAYOUTS {
        reset_probe(&fixture).await?;
        let outcome = run(FamilyMode::Rebuild, ranged(layout, chunk)).await?;
        ensure!(
            outcome.ranges > 0 && probe_build(&fixture).await?.len() > 2,
            "{layout:?}: built in a pass: {outcome:?}"
        );
        require_equal(
            &fixture.exact().await?,
            &expected,
            &format!("Basenames under {layout:?}"),
        )?;
    }
    fixture.cleanup().await
}

// C7 in the database. The relation mask SQL packs every relation, and an unknown one, as the
// Rust one does.
#[tokio::test]
async fn the_sql_relation_mask_evaluates_as_the_rust_one() -> Result<()> {
    let fixture = Fixture::new("families_builder_relation_mask", 0).await?;
    for relation in [
        "token_holder",
        "effective_controller",
        "role_holder",
        "manager",
        "unknown",
    ] {
        let packed: i16 = sqlx::query_scalar(&format!("SELECT {}", relation_mask_sql("$1::text")))
            .bind(relation)
            .fetch_one(&fixture.pool)
            .await?;
        ensure!(
            packed == relation_mask(relation),
            "{relation}: {packed} in SQL"
        );
    }
    fixture.cleanup().await
}
