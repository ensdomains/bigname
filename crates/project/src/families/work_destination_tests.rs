//! The work list's two address sources keep the addresses of resolver edge destinations. Each
//! test here pins one class of input and compares the work list with two frozen statements: the
//! one it replaced, which asked for each address whether a resolver edge pointed at it, and the
//! one before the sources read their boundary indexes. All three must return the same blocks at
//! every limit.
use anyhow::Result;

use super::{
    guard_tests::{CHAIN, database, executed_scans, hash, indexes_read, reads_through},
    input, manifests,
    work_tests::WORK_BLOCKS_BEFORE,
};

/// The work list as it stood before its address sources took an ordered step of the destination
/// index: each asked, address by address, whether any resolver edge pointed at the address's
/// instance. A frozen copy, never edited to follow `input::work_blocks`.
const WORK_BLOCKS_PROBED: &str = "
     SELECT DISTINCT work.block_number FROM (
         (SELECT DISTINCT event.block_number
          FROM normalized_events event
          JOIN chain_lineage lineage
            ON lineage.chain_id = event.chain_id
           AND lineage.block_number = event.block_number
           AND lineage.block_hash = event.block_hash
          WHERE event.chain_id = $1 AND event.block_number BETWEEN $3 AND $2
            AND event.consumer_visibility = 'activated'
            AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
            AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
          ORDER BY 1
          LIMIT $5)
         UNION ALL
         (SELECT DISTINCT binding.block_number
          FROM surface_bindings binding
          JOIN chain_lineage lineage
            ON lineage.chain_id = binding.chain_id
           AND lineage.block_number = binding.block_number
           AND lineage.block_hash = binding.block_hash
          WHERE binding.chain_id = $1 AND binding.block_number BETWEEN $3 AND $2
            AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')
            AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
          ORDER BY 1
          LIMIT $5)
         UNION ALL
         (SELECT boundary.block_number
          FROM (SELECT DISTINCT edge.active_from_block_number AS block_number
                FROM discovery_edges edge
                WHERE edge.chain_id = $1 AND edge.edge_kind = 'resolver'
                  AND edge.active_from_block_number BETWEEN $3 AND $2) boundary
          -- An activation below the retained lineage has no block to apply; the first
          -- readable work block classifies under it.
          WHERE EXISTS (
              SELECT 1 FROM chain_lineage lineage
              WHERE lineage.chain_id = $1 AND lineage.block_number = boundary.block_number
                AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized'))
          ORDER BY 1
          LIMIT $5)
         UNION ALL
         (SELECT boundary.block_number
          FROM (SELECT DISTINCT edge.active_to_block_number AS block_number
                FROM discovery_edges edge
                WHERE edge.chain_id = $1 AND edge.edge_kind = 'resolver'
                  AND edge.active_to_block_number BETWEEN $3 AND $2) boundary
          WHERE EXISTS (
              SELECT 1 FROM chain_lineage lineage
              WHERE lineage.chain_id = $1 AND lineage.block_number = boundary.block_number
                AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized'))
          ORDER BY 1
          LIMIT $5)
         UNION ALL
         (SELECT boundary.block_number
          FROM (SELECT DISTINCT address.active_from_block_number AS block_number
                FROM contract_instance_addresses address
                WHERE address.chain_id = $1
                  AND address.active_from_block_number BETWEEN $3 AND $2
                  -- The address of a resolver edge's destination. OFFSET 0 keeps this
                  -- one index probe for each address that starts in the range.
                  AND EXISTS (
                      SELECT 1 FROM discovery_edges edge
                      WHERE edge.chain_id = address.chain_id
                        AND edge.to_contract_instance_id = address.contract_instance_id
                        AND edge.edge_kind = 'resolver'
                      OFFSET 0)) boundary
          WHERE EXISTS (
              SELECT 1 FROM chain_lineage lineage
              WHERE lineage.chain_id = $1 AND lineage.block_number = boundary.block_number
                AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized'))
          ORDER BY 1
          LIMIT $5)
         UNION ALL
         (SELECT boundary.block_number
          FROM (SELECT DISTINCT address.active_to_block_number AS block_number
                FROM contract_instance_addresses address
                WHERE address.chain_id = $1
                  AND address.active_to_block_number BETWEEN $3 AND $2
                  AND EXISTS (
                      SELECT 1 FROM discovery_edges edge
                      WHERE edge.chain_id = address.chain_id
                        AND edge.to_contract_instance_id = address.contract_instance_id
                        AND edge.edge_kind = 'resolver'
                      OFFSET 0)) boundary
          WHERE EXISTS (
              SELECT 1 FROM chain_lineage lineage
              WHERE lineage.chain_id = $1 AND lineage.block_number = boundary.block_number
                AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized'))
          ORDER BY 1
          LIMIT $5)
         UNION ALL
         (SELECT DISTINCT start.block_number
          FROM unnest($4::bigint[]) start (block_number)
          WHERE EXISTS (
              SELECT 1 FROM chain_lineage lineage
              WHERE lineage.chain_id = $1 AND lineage.block_number = start.block_number
                AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized'))
          ORDER BY 1
          LIMIT $5)
     ) work
     ORDER BY 1
     LIMIT $5";

/// A second chain, with its own instances and lineage.
const OTHER: &str = "ethereum-mainnet";

/// Resolver edges that point at the one hot destination.
const HOT: i64 = 50_000;

/// Addresses outside the range, of instances no edge names.
const FILLER: i64 = 2_000;

fn id(n: u32) -> String {
    format!("00000000-0000-0000-0000-{n:012x}")
}

/// The instance every fixture edge starts from on `chain`.
fn registry(chain: &str) -> u32 {
    if chain == CHAIN { 0 } else { 0x10_0000 }
}

fn number(value: Option<i64>) -> String {
    value.map_or_else(|| "NULL".to_owned(), |value| value.to_string())
}

fn block_hash(value: Option<i64>) -> String {
    value.map_or_else(|| "NULL".to_owned(), |value| format!("'{}'", hash(value)))
}

/// One fixture's rows, written in one batch.
#[derive(Default)]
struct Fixture {
    sql: String,
    rows: u32,
}

impl Fixture {
    fn lineage(&mut self, chain: &str, block: i64, block_hash: &str, state: &str) -> &mut Self {
        self.sql += &format!(
            "INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp,
                 canonicality_state)
             VALUES ('{chain}', '{block_hash}', {block}, to_timestamp(1800000000 + {block} * 12),
                     '{state}');\n"
        );
        self
    }

    fn instance(&mut self, chain: &str, n: u32) -> &mut Self {
        self.sql += &format!(
            "INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
             VALUES ('{}', '{chain}', 'contract') ON CONFLICT DO NOTHING;\n",
            id(n)
        );
        self
    }

    /// An edge of `kind` from the chain's registry to instance `to`, canonical and active.
    fn edge(
        &mut self,
        chain: &str,
        kind: &str,
        to: u32,
        from_block: Option<i64>,
        to_block: Option<i64>,
    ) -> &mut Self {
        self.edge_from(
            chain,
            kind,
            registry(chain),
            to,
            from_block,
            to_block,
            "canonical",
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn edge_from(
        &mut self,
        chain: &str,
        kind: &str,
        from: u32,
        to: u32,
        from_block: Option<i64>,
        to_block: Option<i64>,
        state: &str,
        deactivated: bool,
    ) -> &mut Self {
        self.instance(chain, from).instance(chain, to);
        self.sql += &format!(
            "INSERT INTO discovery_edges (chain_id, edge_kind, from_contract_instance_id,
                 to_contract_instance_id, discovery_source, admission_basis,
                 active_from_block_number, active_from_block_hash, active_to_block_number,
                 active_to_block_hash, canonicality_state, deactivated_at)
             VALUES ('{chain}', '{kind}', '{}', '{}', 'NewResolver', 'fixture', {}, {}, {}, {},
                     '{state}', {});\n",
            id(from),
            id(to),
            number(from_block),
            block_hash(from_block),
            number(to_block),
            block_hash(to_block),
            if deactivated { "now()" } else { "NULL" },
        );
        self
    }

    fn address(
        &mut self,
        chain: &str,
        n: u32,
        from_block: Option<i64>,
        to_block: Option<i64>,
        deactivated: bool,
    ) -> &mut Self {
        self.instance(chain, n);
        self.sql += &format!(
            "INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address,
                 active_from_block_number, active_to_block_number, deactivated_at)
             VALUES ('{}', '{chain}', '0x{n:040x}', {}, {}, {});\n",
            id(n),
            number(from_block),
            number(to_block),
            if deactivated { "now()" } else { "NULL" },
        );
        self
    }

    /// Two activated canonical events at `block`, recorded under `block_hash`.
    fn events(&mut self, chain: &str, block: i64, block_hash: &str) -> &mut Self {
        self.sql += &format!(
            "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
                 manifest_version, chain_id, block_number, block_hash, transaction_hash,
                 transaction_index, log_index, derivation_kind, canonicality_state, before_state,
                 after_state, raw_fact_ref)
             SELECT 'work:{chain}:{block_hash}:' || log, 'ens', 'PreimageObserved',
                    'ens_v1_registry_l1', 1, '{chain}', {block}, '{block_hash}', '0xfeed', 0, log,
                    'ens_v2_registry_resource_surface', 'canonical', '{{}}'::jsonb, '{{}}'::jsonb,
                    '{{}}'::jsonb
             FROM generate_series(0, 1) log;\n"
        );
        self
    }

    /// A canonical surface binding at `block`, recorded under `block_hash`, for a name and a
    /// resource of its own.
    fn binding(&mut self, chain: &str, block: i64, block_hash: &str) -> &mut Self {
        self.rows += 1;
        let (name, resource) = (format!("0x{:064x}", self.rows), id(0x20_0000 + self.rows));
        self.sql += &format!(
            "INSERT INTO name_surfaces (logical_name_id, namespace, namehash, labelhashes,
                 normalizer_version, visibility_state, chain_id, block_hash, block_number,
                 canonicality_state)
             VALUES ('ens:{name}', 'ens', '{name}', ARRAY['{name}'], 'fixture', 'active',
                     '{chain}', '{}', 0, 'canonical');
             INSERT INTO resources (resource_id, chain_id, block_hash, block_number,
                 canonicality_state)
             VALUES ('{resource}', '{chain}', '{}', 0, 'canonical');
             INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id,
                 binding_kind, authority_arm, active_from, chain_id, block_hash, block_number,
                 canonicality_state)
             VALUES (gen_random_uuid(), 'ens:{name}', '{resource}', 'declared_registry_path',
                     'ens_v1', to_timestamp(1800000000 + {block} * 12), '{chain}',
                     '{block_hash}', {block}, 'canonical');\n",
            hash(0),
            hash(0),
        );
        self
    }

    /// One blockless manifest update declaring a resolver at each of `starts`.
    fn declarations(&mut self, chain: &str, starts: &[i64]) -> &mut Self {
        let contracts: Vec<_> = (1..)
            .zip(starts)
            .map(|(n, start)| {
                serde_json::json!({"address": format!("0x{n:040x}"), "role": "public_resolver",
                                   "start_block": start})
            })
            .collect();
        let payload = serde_json::json!({ "contracts": contracts });
        self.sql += &format!(
            "WITH manifest AS (
                 INSERT INTO manifest_versions (manifest_version, namespace, source_family,
                     chain_id, deployment_label, rollout_status, normalizer_version, file_path,
                     manifest_payload)
                 VALUES (1, 'ens', 'ens_v1_resolver_l1', '{chain}', 'fixture', 'active',
                         'fixture', 'fixture/{chain}/resolver.yaml', '{payload}')
                 RETURNING manifest_id)
             INSERT INTO normalized_events (event_identity, namespace, event_kind,
                 source_family, manifest_version, source_manifest_id, chain_id, derivation_kind,
                 canonicality_state, before_state, after_state, raw_fact_ref)
             SELECT 'manifest:' || manifest_id, 'ens', 'SourceManifestUpdated',
                    'ens_v1_resolver_l1', 1, manifest_id, '{chain}',
                    'ens_v2_registry_resource_surface', 'canonical', '{{}}'::jsonb,
                    jsonb_build_object('rollout_status', 'active',
                                       'manifest_payload', '{payload}'::jsonb),
                    '{{}}'::jsonb
             FROM manifest;\n"
        );
        self
    }

    fn analyze(&mut self) -> &mut Self {
        self.sql += "ANALYZE;\n";
        self
    }

    async fn write(&mut self, pool: &sqlx::PgPool) -> Result<()> {
        sqlx::raw_sql(&std::mem::take(&mut self.sql))
            .execute(pool)
            .await?;
        Ok(())
    }
}

/// The blocks each source has work at, on one chain.
#[derive(Default)]
struct Sources<'a> {
    declarations: &'a [i64],
    bindings: &'a [i64],
    events: &'a [i64],
    edge_from: &'a [i64],
    edge_to: &'a [i64],
    address_from: &'a [i64],
    address_to: &'a [i64],
}

/// Rows on `chain` that give each source work at its blocks and nowhere else in `1..=12`.
/// Instance numbers start at `base`. The edges of the edge sources point at an instance with
/// no address. The address sources' instances are reached by blockless resolver edges.
fn sources(fixture: &mut Fixture, chain: &str, base: u32, sources: &Sources) {
    if !sources.declarations.is_empty() {
        fixture.declarations(chain, sources.declarations);
    }
    for &block in sources.bindings {
        fixture.binding(chain, block, &hash(block));
    }
    for &block in sources.events {
        fixture.events(chain, block, &hash(block));
    }
    for &block in sources.edge_from {
        fixture.edge(chain, "resolver", base + 1, Some(block), None);
    }
    for &block in sources.edge_to {
        fixture.edge(chain, "resolver", base + 2, Some(0), Some(block));
    }
    for (n, &block) in (base + 10..).zip(sources.address_from) {
        fixture
            .edge(chain, "resolver", n, None, None)
            .address(chain, n, Some(block), None, false);
    }
    for (n, &block) in (base + 20..).zip(sources.address_to) {
        fixture.edge(chain, "resolver", n, None, None).address(
            chain,
            n,
            Some(0),
            Some(block),
            false,
        );
    }
}

/// Every source at once, on distinct blocks, with nothing at 1, 5 or 12.
const COMBINED: Sources<'static> = Sources {
    declarations: &[2, 11],
    bindings: &[3],
    events: &[4, 10],
    edge_from: &[6],
    edge_to: &[7],
    address_from: &[8],
    address_to: &[9],
};
const COMBINED_BLOCKS: [i64; 9] = [2, 3, 4, 6, 7, 8, 9, 10, 11];

/// Instance 1 is the destination of `HOT` resolver edges, and instance 2 of five more, all
/// outside `1..=12`. Instance 1's address starts at 7 and instance 2's stops at 9. Instances 3
/// and 5, which no edge names, have addresses that start at 5 and 3, and instance 4, reached
/// only by a subregistry edge, one that starts at 6. `FILLER` more instances no edge names have
/// addresses outside the range, so the address table is not small enough to read whole.
fn hot(fixture: &mut Fixture) {
    fixture.instance(CHAIN, 0).instance(CHAIN, 1);
    fixture.sql += &format!(
        "INSERT INTO discovery_edges (chain_id, edge_kind, from_contract_instance_id,
             to_contract_instance_id, discovery_source, admission_basis,
             active_from_block_number, active_from_block_hash, canonicality_state)
         SELECT '{CHAIN}', 'resolver', '{}', '{}', 'NewResolver', 'fixture', 100 + n,
                '0x' || lpad(to_hex(100 + n), 64, '0'), 'canonical'
         FROM generate_series(1, {HOT}) n;
         INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
         SELECT ('00000000-0000-0000-0000-' || lpad(to_hex(n), 12, '0'))::uuid, '{CHAIN}',
                'contract'
         FROM generate_series(4096, 4095 + {FILLER}) n;
         INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address,
             active_from_block_number)
         SELECT ('00000000-0000-0000-0000-' || lpad(to_hex(n), 12, '0'))::uuid, '{CHAIN}',
                '0x' || lpad(to_hex(n), 40, '0'), 100 + n
         FROM generate_series(4096, 4095 + {FILLER}) n;\n",
        id(0),
        id(1)
    );
    for _ in 0..5 {
        fixture.edge(CHAIN, "resolver", 2, Some(50), None);
    }
    fixture
        .edge(CHAIN, "subregistry", 4, Some(50), None)
        .address(CHAIN, 1, Some(7), None, false)
        .address(CHAIN, 2, Some(0), Some(9), false)
        .address(CHAIN, 3, Some(5), None, false)
        .address(CHAIN, 4, Some(6), None, false)
        .address(CHAIN, 5, Some(3), None, false);
}

async fn frozen(
    pool: &sqlx::PgPool,
    statement: &'static str,
    chain: &str,
    (from, to): (i64, i64),
    starts: &[i64],
    limit: i64,
) -> Result<Vec<i64>> {
    Ok(sqlx::query_scalar(statement)
        .bind(chain)
        .bind(to)
        .bind(from)
        .bind(starts)
        .bind(limit)
        .fetch_all(pool)
        .await?)
}

/// The work list over `from..=to` on `chain` is `expected`, cut at each limit around its
/// length, and both frozen statements return the same.
async fn returns(
    pool: &sqlx::PgPool,
    chain: &str,
    (from, to): (i64, i64),
    expected: &[i64],
) -> Result<()> {
    let history = manifests::History::read(pool, chain, to).await?;
    let starts = history.declaration_starts(from, to);
    let length = i64::try_from(expected.len())?;
    let mut limits = vec![1, 2, length - 1, length, length + 1, i64::MAX];
    limits.retain(|limit| *limit >= 1);
    limits.sort_unstable();
    limits.dedup();
    for limit in limits {
        let want = &expected[..expected.len().min(usize::try_from(limit)?)];
        let new = input::work_blocks(pool, chain, from, to, &history, limit).await?;
        let probed = frozen(pool, WORK_BLOCKS_PROBED, chain, (from, to), &starts, limit).await?;
        let before = frozen(pool, WORK_BLOCKS_BEFORE, chain, (from, to), &starts, limit).await?;
        assert_eq!(new, want, "work list, {chain} {from}..={to}, limit {limit}");
        assert_eq!(
            probed, new,
            "probed statement, {chain} {from}..={to}, limit {limit}"
        );
        assert_eq!(
            before, new,
            "statement before, {chain} {from}..={to}, limit {limit}"
        );
    }
    Ok(())
}

async fn fixture_returns(fixture: &mut Fixture, expected: &[i64]) -> Result<()> {
    let (database, pool) = database().await?;
    fixture.write(&pool).await?;
    returns(&pool, CHAIN, (1, 12), expected).await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn events_alone_are_work_once_per_block() -> Result<()> {
    let mut fixture = Fixture::default();
    for block in 4..=10 {
        fixture.events(CHAIN, block, &hash(block));
    }
    fixture_returns(&mut fixture, &[4, 5, 6, 7, 8, 9, 10]).await
}

// One binding at 5 and the binding at 11 are recorded under an orphaned block hash.
#[tokio::test]
async fn bindings_alone_are_work_where_their_block_is_readable() -> Result<()> {
    let mut fixture = Fixture::default();
    fixture
        .lineage(CHAIN, 5, "0xorphaned5", "orphaned")
        .lineage(CHAIN, 11, "0xorphaned11", "orphaned")
        .binding(CHAIN, 3, &hash(3))
        .binding(CHAIN, 5, &hash(5))
        .binding(CHAIN, 5, "0xorphaned5")
        .binding(CHAIN, 9, &hash(9))
        .binding(CHAIN, 11, "0xorphaned11");
    fixture_returns(&mut fixture, &[3, 5, 9]).await
}

#[tokio::test]
async fn resolver_edges_that_start_in_the_range_are_work() -> Result<()> {
    let mut fixture = Fixture::default();
    for block in [2, 6, 6, 8] {
        fixture.edge(CHAIN, "resolver", 1, Some(block), None);
    }
    fixture_returns(&mut fixture, &[2, 6, 8]).await
}

#[tokio::test]
async fn resolver_edges_that_stop_in_the_range_are_work() -> Result<()> {
    let mut fixture = Fixture::default();
    for block in [4, 7] {
        fixture.edge(CHAIN, "resolver", 1, Some(0), Some(block));
    }
    fixture_returns(&mut fixture, &[4, 7]).await
}

#[tokio::test]
async fn destination_addresses_that_start_in_the_range_are_work() -> Result<()> {
    let mut fixture = Fixture::default();
    sources(
        &mut fixture,
        CHAIN,
        0,
        &Sources {
            address_from: &[3, 5],
            ..Sources::default()
        },
    );
    fixture_returns(&mut fixture, &[3, 5]).await
}

#[tokio::test]
async fn destination_addresses_that_stop_in_the_range_are_work() -> Result<()> {
    let mut fixture = Fixture::default();
    sources(
        &mut fixture,
        CHAIN,
        0,
        &Sources {
            address_to: &[6, 9],
            ..Sources::default()
        },
    );
    fixture_returns(&mut fixture, &[6, 9]).await
}

#[tokio::test]
async fn a_declaration_update_with_no_contracts_is_no_work() -> Result<()> {
    let mut fixture = Fixture::default();
    fixture.declarations(CHAIN, &[]);
    fixture_returns(&mut fixture, &[]).await
}

#[tokio::test]
async fn declaration_starts_in_the_range_are_work() -> Result<()> {
    let mut fixture = Fixture::default();
    fixture.declarations(CHAIN, &[2, 7, 11, 20]);
    fixture_returns(&mut fixture, &[2, 7, 11]).await
}

#[tokio::test]
async fn every_source_together_gives_the_lowest_blocks_across_them() -> Result<()> {
    let (database, pool) = database().await?;
    let mut fixture = Fixture::default();
    sources(&mut fixture, CHAIN, 0, &COMBINED);
    fixture.write(&pool).await?;
    returns(&pool, CHAIN, (1, 12), &COMBINED_BLOCKS).await?;
    let history = manifests::History::read(&pool, CHAIN, 12).await?;
    assert_eq!(
        input::work_blocks(&pool, CHAIN, 1, 12, &history, 4).await?,
        [2, 3, 4, 6]
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn an_empty_range_is_no_work() -> Result<()> {
    let (database, pool) = database().await?;
    let mut fixture = Fixture::default();
    sources(&mut fixture, CHAIN, 0, &COMBINED);
    fixture.write(&pool).await?;
    returns(&pool, CHAIN, (9, 3), &[]).await?;
    returns(&pool, CHAIN, (5, 5), &[]).await?;
    returns(&pool, CHAIN, (8, 8), &[8]).await?;
    database.cleanup().await?;
    Ok(())
}

// Instance 1's address starts at 4 and no edge names it.
#[tokio::test]
async fn an_address_no_edge_points_at_is_no_work() -> Result<()> {
    let mut fixture = Fixture::default();
    fixture.address(CHAIN, 1, Some(4), None, false);
    sources(
        &mut fixture,
        CHAIN,
        0,
        &Sources {
            address_from: &[6],
            ..Sources::default()
        },
    );
    fixture_returns(&mut fixture, &[6]).await
}

// Instance 1's address starts at 4 and only a subregistry edge, outside the range, names it.
#[tokio::test]
async fn an_address_only_a_subregistry_edge_points_at_is_no_work() -> Result<()> {
    let mut fixture = Fixture::default();
    fixture
        .edge(CHAIN, "subregistry", 1, Some(50), None)
        .address(CHAIN, 1, Some(4), None, false);
    sources(
        &mut fixture,
        CHAIN,
        0,
        &Sources {
            address_from: &[6],
            ..Sources::default()
        },
    );
    fixture_returns(&mut fixture, &[6]).await
}

#[tokio::test]
async fn a_destination_most_resolver_edges_point_at_is_work_once() -> Result<()> {
    let mut fixture = Fixture::default();
    hot(&mut fixture);
    fixture.analyze();
    fixture_returns(&mut fixture, &[7, 9]).await
}

// Five registries and a subregistry edge point at instance 1. Its active address starts at 4,
// and a deactivated one held it at block 0 only.
#[tokio::test]
async fn a_destination_many_edges_share_is_work_once() -> Result<()> {
    let mut fixture = Fixture::default();
    for from in 2..7 {
        fixture.edge_from(
            CHAIN,
            "resolver",
            from,
            1,
            Some(50),
            None,
            "canonical",
            false,
        );
    }
    fixture
        .edge(CHAIN, "subregistry", 1, Some(50), None)
        .address(CHAIN, 1, Some(0), Some(0), true)
        .address(CHAIN, 1, Some(4), None, false);
    fixture_returns(&mut fixture, &[4]).await
}

// Instance 1 is reached only by a deactivated edge, instance 2 only by an orphaned one and
// instance 3 only by a blockless one.
#[tokio::test]
async fn deactivated_orphaned_and_blockless_edges_still_make_a_destination() -> Result<()> {
    let mut fixture = Fixture::default();
    fixture
        .edge_from(
            CHAIN,
            "resolver",
            0,
            1,
            Some(50),
            Some(60),
            "canonical",
            true,
        )
        .edge_from(CHAIN, "resolver", 0, 2, Some(50), None, "orphaned", false)
        .edge(CHAIN, "resolver", 3, None, None)
        .address(CHAIN, 1, Some(3), None, false)
        .address(CHAIN, 2, Some(6), None, false)
        .address(CHAIN, 3, Some(9), None, false);
    fixture_returns(&mut fixture, &[3, 6, 9]).await
}

// Block 7 has an event, a binding, an edge that starts there and a destination address that
// starts there. Block 9 has a declaration.
#[tokio::test]
async fn a_block_several_sources_name_is_work_once() -> Result<()> {
    let mut fixture = Fixture::default();
    sources(
        &mut fixture,
        CHAIN,
        0,
        &Sources {
            declarations: &[9],
            bindings: &[7],
            events: &[7],
            edge_from: &[7],
            address_from: &[7],
            ..Sources::default()
        },
    );
    fixture_returns(&mut fixture, &[7, 9]).await
}

#[tokio::test]
async fn the_limit_cuts_the_union_of_the_sources() -> Result<()> {
    let mut fixture = Fixture::default();
    sources(
        &mut fixture,
        CHAIN,
        0,
        &Sources {
            declarations: &[11],
            events: &[2],
            edge_from: &[3],
            edge_to: &[5],
            address_from: &[7],
            address_to: &[9],
            ..Sources::default()
        },
    );
    fixture_returns(&mut fixture, &[2, 3, 5, 7, 9, 11]).await
}

// Above block 12 lineage is recorded for 13 to 20, except 15, whose only row is orphaned, and
// 17, which has none. Every source has work at 15, and every source that does not need a
// lineage row of its own at 17.
#[tokio::test]
async fn work_at_an_unreadable_block_is_dropped() -> Result<()> {
    let (database, pool) = database().await?;
    let mut fixture = Fixture::default();
    for block in [13, 14, 16, 18, 19, 20] {
        fixture.lineage(CHAIN, block, &hash(block), "canonical");
    }
    fixture.lineage(CHAIN, 15, &hash(15), "orphaned");
    sources(
        &mut fixture,
        CHAIN,
        0,
        &Sources {
            declarations: &[14, 15, 17],
            bindings: &[15],
            events: &[15],
            edge_from: &[15, 17],
            edge_to: &[15, 17],
            address_from: &[15, 17, 16],
            address_to: &[15, 17],
        },
    );
    fixture.write(&pool).await?;
    returns(&pool, CHAIN, (13, 20), &[14, 16]).await?;
    database.cleanup().await?;
    Ok(())
}

// Instance 1's edge has no blocks and its address stops at 8, with no start. Instance 2's edge
// starts at 5 and never stops, and its address has neither block.
#[tokio::test]
async fn a_missing_boundary_is_never_in_a_range() -> Result<()> {
    let mut fixture = Fixture::default();
    fixture
        .edge(CHAIN, "resolver", 1, None, None)
        .address(CHAIN, 1, None, Some(8), false)
        .edge(CHAIN, "resolver", 2, Some(5), None)
        .address(CHAIN, 2, None, None, false);
    fixture_returns(&mut fixture, &[5, 8]).await
}

// The destination with the larger id has the lower block.
#[tokio::test]
async fn the_blocks_follow_block_order_not_destination_order() -> Result<()> {
    let mut fixture = Fixture::default();
    fixture
        .edge(CHAIN, "resolver", 0xa1, None, None)
        .edge(CHAIN, "resolver", 0xa2, None, None)
        .address(CHAIN, 0xa1, Some(9), None, false)
        .address(CHAIN, 0xa2, Some(4), None, false);
    fixture_returns(&mut fixture, &[4, 9]).await
}

// The second chain has work at 1, 5 and 12, where the first has none, and its address at 5
// belongs to a destination of its own.
#[tokio::test]
async fn each_chain_reads_only_its_own_work() -> Result<()> {
    let (database, pool) = database().await?;
    let mut fixture = Fixture::default();
    for block in 0..=12 {
        fixture.lineage(OTHER, block, &hash(block), "canonical");
    }
    sources(&mut fixture, CHAIN, 0, &COMBINED);
    sources(
        &mut fixture,
        OTHER,
        registry(OTHER),
        &Sources {
            declarations: &[8],
            bindings: &[6],
            events: &[4],
            edge_from: &[1],
            edge_to: &[3],
            address_from: &[5],
            address_to: &[12],
        },
    );
    fixture.write(&pool).await?;
    returns(&pool, CHAIN, (1, 12), &COMBINED_BLOCKS).await?;
    returns(&pool, OTHER, (1, 12), &[1, 3, 4, 5, 6, 8, 12]).await?;
    database.cleanup().await?;
    Ok(())
}

/// Runs the work list and both frozen statements under the default plan cache mode until
/// PostgreSQL may switch to a generic plan, then under each forced mode. The pool holds one
/// session, so sqlx keeps each statement prepared on it.
async fn plans_agree(fixture: &mut Fixture, expected: &[i64]) -> Result<()> {
    let (database, pool) = database().await?;
    fixture.analyze().write(&pool).await?;
    for mode in ["auto", "auto", "force_custom_plan", "force_generic_plan"] {
        sqlx::raw_sql(&format!("SET plan_cache_mode = {mode}"))
            .execute(&pool)
            .await?;
        returns(&pool, CHAIN, (1, 12), expected).await?;
    }
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn generic_and_custom_plans_return_the_same_blocks() -> Result<()> {
    let mut fixture = Fixture::default();
    sources(&mut fixture, CHAIN, 0, &COMBINED);
    plans_agree(&mut fixture, &COMBINED_BLOCKS).await?;
    let mut fixture = Fixture::default();
    hot(&mut fixture);
    plans_agree(&mut fixture, &[7, 9]).await
}

const EDGE_INDEXES: [&str; 3] = [
    "project_families_discovery_edges_resolver_from_block_idx",
    "project_families_discovery_edges_resolver_to_block_idx",
    DESTINATION_INDEX,
];
const DESTINATION_INDEX: &str = "project_families_discovery_edges_resolver_destination_idx";
const ADDRESS_INDEXES: [&str; 3] = [
    "project_families_contract_instance_addresses_from_block_idx",
    "project_families_contract_instance_addresses_to_block_idx",
    "contract_instance_addresses_instance_idx",
];

/// How the prepared work list over `from..=to` reads the edges and addresses, under a custom and
/// a generic plan: the failures, each named with its plan mode. Prints both plans. The probes may
/// number at most one per address boundary in the range, plus one, however many destinations the
/// chain has. A probe of the
/// destination index is any scan of it under the `destination` list, or one with the destination
/// in its index condition, which only the address side has. Each probe returns at most one row.
/// Without statistics the planner may serve the unchanged edge boundary sources from the
/// destination index with a filter, so their filter removals and boundary indexes are checked
/// only on `analyzed` tables.
async fn access_path_failures(
    pool: &sqlx::PgPool,
    label: &str,
    analyzed: bool,
    (from, to): (i64, i64),
) -> Result<Vec<String>> {
    let history = manifests::History::read(pool, CHAIN, to).await?;
    input::work_blocks(pool, CHAIN, from, to, &history, 257).await?;
    let prepared: String = sqlx::query_scalar(
        "SELECT name FROM pg_prepared_statements
         WHERE statement LIKE '/* project:families.input.work_blocks */%'",
    )
    .fetch_one(pool)
    .await?;
    let destinations: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT to_contract_instance_id) FROM discovery_edges
         WHERE chain_id = $1 AND edge_kind = 'resolver'",
    )
    .bind(CHAIN)
    .fetch_one(pool)
    .await?;
    let boundaries: i64 = sqlx::query_scalar(
        "SELECT count(*) FILTER (WHERE active_from_block_number BETWEEN $2 AND $3)
              + count(*) FILTER (WHERE active_to_block_number BETWEEN $2 AND $3)
         FROM contract_instance_addresses WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .bind(from)
    .bind(to)
    .fetch_one(pool)
    .await?;
    let max_probes = boundaries + 1;
    let mut failures = Vec::new();
    for mode in ["force_custom_plan", "force_generic_plan"] {
        sqlx::raw_sql(&format!("SET plan_cache_mode = {mode}"))
            .execute(pool)
            .await?;
        let execute = format!("EXECUTE {prepared} ('{CHAIN}', {to}, {from}, '{{}}', 257)");
        let text: Vec<String> =
            sqlx::query_scalar(&format!("EXPLAIN (ANALYZE, FORMAT TEXT) {execute}"))
                .fetch_all(pool)
                .await?;
        println!("{label}, {mode}:\n{}\n", text.join("\n"));
        let plan: serde_json::Value =
            sqlx::query_scalar(&format!("EXPLAIN (ANALYZE, FORMAT JSON) {execute}"))
                .fetch_one(pool)
                .await?;
        let mut scans = Vec::new();
        executed_scans(&plan[0]["Plan"], "", &mut scans);
        let (mut read, mut probes) = (std::collections::BTreeSet::new(), 0);
        for (cte, node) in scans {
            let indexes = indexes_read(node);
            read.extend(indexes.iter().copied());
            let relation = node["Relation Name"].as_str();
            let allowed = match relation {
                Some("discovery_edges") => &EDGE_INDEXES[..],
                Some("contract_instance_addresses") => &ADDRESS_INDEXES[..],
                _ => continue,
            };
            let kind = node["Node Type"].as_str().unwrap_or_default();
            let loops = node["Actual Loops"].as_i64().unwrap_or(1);
            if !reads_through(node, allowed) {
                failures.push(format!(
                    "{label}, {mode}: {kind} of {} by {}",
                    relation.unwrap_or_default(),
                    node["Index Name"]
                ));
            }
            let condition = node["Index Cond"]
                .as_str()
                .or(node["Recheck Cond"].as_str())
                .unwrap_or_default();
            if indexes.contains(&DESTINATION_INDEX)
                && (cte == "destination" || condition.contains("to_contract_instance_id"))
            {
                probes += loops;
                if node["Actual Rows"].as_f64().unwrap_or_default() > 1.0 {
                    failures.push(format!(
                        "{label}, {mode}: a probe of the destination index returns {} rows",
                        node["Actual Rows"]
                    ));
                }
            }
            if let Some(removed) = node["Rows Removed by Filter"].as_i64()
                && analyzed
                && removed * loops > 16
            {
                failures.push(format!(
                    "{label}, {mode}: a scan of {} filters out {removed} rows",
                    relation.unwrap_or_default()
                ));
            }
        }
        if probes > max_probes {
            failures.push(format!(
                "{label}, {mode}: {probes} probes of the destination index for {boundaries} \
                 address boundaries and {destinations} destinations"
            ));
        }
        for index in &EDGE_INDEXES[..2] {
            if analyzed && !read.contains(index) {
                failures.push(format!("{label}, {mode}: {index} is not read"));
            }
        }
    }
    sqlx::raw_sql("SET plan_cache_mode = auto")
        .execute(pool)
        .await?;
    Ok(failures)
}

// When most resolver edges point at one destination, the planner must still read no edge
// outside the range and probe the destination index at most once for each address boundary in
// the range, plus one, whatever the statistics say: before ANALYZE and after it, under a custom
// and a generic plan.
#[tokio::test]
async fn a_hot_destination_does_not_make_the_address_sources_scan_every_edge() -> Result<()> {
    let (database, pool) = database().await?;
    let mut fixture = Fixture::default();
    hot(&mut fixture);
    fixture.write(&pool).await?;
    let mut failures = access_path_failures(&pool, "before ANALYZE", false, (1, 12)).await?;
    fixture.analyze().write(&pool).await?;
    failures.extend(access_path_failures(&pool, "after ANALYZE", true, (1, 12)).await?);
    assert!(
        failures.is_empty(),
        "the work list's access paths:\n{}",
        failures.join("\n")
    );
    database.cleanup().await?;
    Ok(())
}

/// Distinct resolver destinations: one resolver contract each, as ENSv2 gives every
/// `PermissionedResolver` proxy its own instance.
const SPREAD: i64 = 50_000;

/// `SPREAD` resolver edges at block 2, each to an instance of its own whose address starts above
/// 100. One more destination has an address that starts at 9. So `5..=12` holds one address
/// boundary.
fn spread(fixture: &mut Fixture) {
    fixture.instance(CHAIN, 0);
    fixture.sql += &format!(
        "INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
         SELECT ('00000000-0000-0000-0000-' || lpad(to_hex(n), 12, '0'))::uuid, '{CHAIN}',
                'contract'
         FROM generate_series(1, {SPREAD}) n;
         INSERT INTO discovery_edges (chain_id, edge_kind, from_contract_instance_id,
             to_contract_instance_id, discovery_source, admission_basis,
             active_from_block_number, active_from_block_hash, canonicality_state)
         SELECT '{CHAIN}', 'resolver', '{}',
                ('00000000-0000-0000-0000-' || lpad(to_hex(n), 12, '0'))::uuid, 'NewResolver',
                'fixture', 2, '{}', 'canonical'
         FROM generate_series(1, {SPREAD}) n;
         INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address,
             active_from_block_number)
         SELECT ('00000000-0000-0000-0000-' || lpad(to_hex(n), 12, '0'))::uuid, '{CHAIN}',
                '0x' || lpad(to_hex(n), 40, '0'), 100 + n
         FROM generate_series(1, {SPREAD}) n;\n",
        id(0),
        hash(2)
    );
    let last = u32::try_from(SPREAD + 1).expect("the fixture fits in u32");
    fixture
        .edge(CHAIN, "resolver", last, Some(2), None)
        .address(CHAIN, last, Some(9), None, false);
}

#[tokio::test]
async fn many_destinations_return_the_one_boundary_in_the_range() -> Result<()> {
    let (database, pool) = database().await?;
    let mut fixture = Fixture::default();
    spread(&mut fixture);
    fixture.analyze().write(&pool).await?;
    returns(&pool, CHAIN, (5, 12), &[9]).await?;
    database.cleanup().await?;
    Ok(())
}

// When every resolver contract is a destination of its own, a range with one address boundary
// probes the destination index at most once for that address, plus one, not once per
// destination: before ANALYZE and after it, under a custom and a generic plan.
#[tokio::test]
async fn many_destinations_do_not_make_a_range_probe_each_one() -> Result<()> {
    let (database, pool) = database().await?;
    let mut fixture = Fixture::default();
    spread(&mut fixture);
    fixture.write(&pool).await?;
    let mut failures = access_path_failures(&pool, "before ANALYZE", false, (5, 12)).await?;
    fixture.analyze().write(&pool).await?;
    failures.extend(access_path_failures(&pool, "after ANALYZE", true, (5, 12)).await?);
    assert!(
        failures.is_empty(),
        "the work list's access paths:\n{}",
        failures.join("\n")
    );
    database.cleanup().await?;
    Ok(())
}
