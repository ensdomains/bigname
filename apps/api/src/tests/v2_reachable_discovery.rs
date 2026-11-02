//! TYR-400 through Engine, Project and the public routes on the checked-in mainnet profile. A
//! registry that announces itself with `RegistryCreated()` is indexed, but its resolver pointers
//! become discovery edges only while it is reachable from the anchored ETHRegistry. A registry
//! mounted later serves the entries it held before the mount.
use super::v2_mainnet_admission::{DAY, OWNER, PUBLIC_RESOLVER, REGISTERED, lease, publish, text};
use super::v2_sepolia_redeploy::{get, token};
use super::*;
use alloy_primitives::{Address, keccak256};
use alloy_sol_types::{SolEvent, sol};
use axum::http::StatusCode;

sol! {
    event RegistryCreated();
    event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender);
    event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender);
    event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender);
    event Upgraded(address indexed implementation);
}

/// The declared `PermissionedResolver` implementation on the mainnet profile.
const RESOLVER_IMPLEMENTATION: &str = "0x565b0f456a04ac9b48758190e90030623cff57c5";

/// The values `publish` uses (v2_mainnet_admission.rs).
const CHAIN: &str = "ethereum-mainnet";
const HEAD: i64 = 26_216_500;
/// The declared ETHRegistry, the suffix anchor named `eth`.
const ETH_REGISTRY: &str = "0xf60aba43e1f7f3dab1186ff03b561401c9df85e2";
const REGISTRY: &str = "0x00000000000000000000000000000000000000d1";
const LOWER_REGISTRY: &str = "0x00000000000000000000000000000000000000d2";
const OTHER_REGISTRY: &str = "0x00000000000000000000000000000000000000d3";
const RESOLVER_X: &str = "0x00000000000000000000000000000000000000e1";
const RESOLVER_Y: &str = "0x00000000000000000000000000000000000000e2";
const FOREVER: u64 = u64::MAX;

type Logs = super::v2_mainnet_admission::Logs;

fn announce(registry: &'static str) -> (&'static str, alloy_primitives::LogData) {
    (registry, RegistryCreated {}.encode_log_data())
}

fn register(
    registry: &'static str,
    label: &str,
    expiry: u64,
) -> Result<(&'static str, alloy_primitives::LogData)> {
    let owner: Address = OWNER.parse()?;
    Ok((
        registry,
        LabelRegistered {
            tokenId: token(label),
            labelHash: keccak256(label.as_bytes()),
            label: label.into(),
            owner,
            expiry,
            sender: owner,
        }
        .encode_log_data(),
    ))
}

fn set_resolver(
    registry: &'static str,
    label: &str,
    resolver: &str,
) -> Result<(&'static str, alloy_primitives::LogData)> {
    Ok((
        registry,
        ResolverUpdated {
            tokenId: token(label),
            resolver: resolver.parse()?,
            sender: OWNER.parse()?,
        }
        .encode_log_data(),
    ))
}

fn mount(
    parent: &'static str,
    label: &str,
    child: &str,
) -> Result<(&'static str, alloy_primitives::LogData)> {
    Ok((
        parent,
        SubregistryUpdated {
            tokenId: token(label),
            subregistry: child.parse()?,
            sender: OWNER.parse()?,
        }
        .encode_log_data(),
    ))
}

/// The block number `publish` gives the block at `index` of `count`.
fn block(index: usize, count: usize) -> i64 {
    HEAD + 1 - count as i64 + index as i64
}

/// The key the registry's own `ResolverUpdated` for `label` writes.
fn key(registry: &str, label: &str) -> String {
    format!("resolver:{registry}:{:#x}", token(label))
}

/// Each resolver edge from `registry`: target address, first block, end block and key.
async fn resolver_edges(
    database: &TestDatabase,
    registry: &str,
) -> Result<Vec<(String, i64, Option<i64>, String)>> {
    Ok(sqlx::query_as(
        "SELECT lower(target.address), edge.active_from_block_number,
                edge.active_to_block_number, edge.provenance ->> 'observation_key'
         FROM discovery_edges edge
         JOIN contract_instance_addresses source
           ON source.contract_instance_id = edge.from_contract_instance_id
          AND source.chain_id = edge.chain_id
         JOIN contract_instance_addresses target
           ON target.contract_instance_id = edge.to_contract_instance_id
          AND target.chain_id = edge.chain_id
         WHERE edge.chain_id = $1 AND edge.edge_kind = 'resolver'
           AND lower(source.address) = $2 AND edge.canonicality_state <> 'orphaned'
         ORDER BY edge.active_from_block_number, lower(target.address)",
    )
    .bind(CHAIN)
    .bind(registry)
    .fetch_all(&database.pool)
    .await?)
}

/// The resolver's classification row, without its surrogate event id.
async fn classification(database: &TestDatabase, resolver: &str) -> Result<Option<Value>> {
    Ok(sqlx::query_scalar(
        "SELECT to_jsonb(classification) - 'normalized_event_id'
         FROM project_resolver_classification classification
         WHERE chain_id = $1 AND resolver_address = $2",
    )
    .bind(CHAIN)
    .bind(resolver)
    .fetch_optional(&database.pool)
    .await?)
}

async fn ok(database: &TestDatabase, uri: &str) -> Result<Value> {
    let (status, body) = get(database, uri).await?;
    assert_eq!(status, StatusCode::OK, "{uri}: {body:#}");
    Ok(body)
}

/// `REGISTRY` announces itself, registers `a` and points it at `RESOLVER_X`.
fn unmounted_registry() -> Result<Vec<(u64, Logs)>> {
    Ok(vec![
        (REGISTERED, vec![announce(REGISTRY)]),
        (
            REGISTERED + 60,
            vec![
                register(REGISTRY, "a", FOREVER)?,
                set_resolver(REGISTRY, "a", RESOLVER_X)?,
            ],
        ),
        (
            REGISTERED + 120,
            vec![register(ETH_REGISTRY, "n", FOREVER)?],
        ),
    ])
}

/// N1 and F2's route half: a registry nothing reachable mounts is known to the registries route
/// with no name, opens no resolver edge, and gives the resolver it points at no supported
/// classification. Its `eth` entry and subregistry name nothing.
#[tokio::test]
async fn n1_an_unmounted_registry_adds_no_resolver_edge_or_supported_classification() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    publish(
        &database,
        vec![
            (REGISTERED, vec![announce(REGISTRY)]),
            (
                REGISTERED + 60,
                vec![
                    register(REGISTRY, "eth", FOREVER)?,
                    set_resolver(REGISTRY, "eth", RESOLVER_X)?,
                    mount(REGISTRY, "eth", LOWER_REGISTRY)?,
                ],
            ),
            (REGISTERED + DAY, vec![]),
        ],
    )
    .await?;

    let registry = ok(&database, &format!("/v1/registries/1/{REGISTRY}")).await?;
    assert_eq!(registry["data"]["name"], Value::Null, "{registry:#}");
    assert_eq!(
        registry["data"]["created_basis"],
        json!("registry_created"),
        "{registry:#}"
    );
    let (_, eth) = get(&database, "/v1/names/eth?namespace=ens").await?;
    assert!(!eth.to_string().contains(RESOLVER_X), "{eth:#}");

    assert_eq!(resolver_edges(&database, REGISTRY).await?, Vec::new());
    let row = classification(&database, RESOLVER_X).await?;
    assert!(
        row.as_ref()
            .is_none_or(|row| row["support_status"] == json!("unsupported")),
        "{row:#?}"
    );
    database.cleanup().await
}

/// C1: an unmounted registry pointing at a popular ENSv1 resolver leaves that resolver's
/// classification row as it is in a corpus without the registry, position included, and the
/// name it serves keeps its resolver.
#[tokio::test]
async fn c1_an_unmounted_registry_leaves_a_v1_resolver_classification_alone() -> Result<()> {
    let spam = vec![
        announce(REGISTRY),
        register(REGISTRY, "a", FOREVER)?,
        set_resolver(REGISTRY, "a", PUBLIC_RESOLVER)?,
    ];
    let corpus = |registry_logs: Logs| -> Result<Vec<(u64, Logs)>> {
        Ok(vec![
            (REGISTERED, lease()?),
            (REGISTERED + 60, text()?),
            (REGISTERED + 120, registry_logs),
            (REGISTERED + DAY, vec![]),
        ])
    };
    let with = TestDatabase::new_migrated().await?;
    publish(&with, corpus(spam)?).await?;
    let without = TestDatabase::new_migrated().await?;
    publish(&without, corpus(vec![])?).await?;

    let name = format!(
        "/v1/names/{}?namespace=ens",
        super::v2_mainnet_admission::NAME
    );
    assert_eq!(
        ok(&with, &name).await?["data"]["resolver"],
        ok(&without, &name).await?["data"]["resolver"]
    );
    assert_eq!(resolver_edges(&with, REGISTRY).await?, Vec::new());
    let expected = classification(&without, PUBLIC_RESOLVER).await?;
    assert!(expected.is_some(), "the ENSv1 resolver is classified");
    assert_eq!(classification(&with, PUBLIC_RESOLVER).await?, expected);
    with.cleanup().await?;
    without.cleanup().await
}

/// M1: `n.eth` mounts the registry two blocks after its entry pointed at a resolver. The entry
/// serves under `a.n.eth` with that resolver, the registries route names the registry, and the
/// resolver edge runs from the mount, under the registry's own key.
#[tokio::test]
async fn m1_a_registry_mounted_later_serves_its_entry_and_opens_its_edge_at_the_mount() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    let mut blocks = unmounted_registry()?;
    // The routes serve only a resolver they know, so `b` points at the declared public
    // resolver. `a`'s fresh resolver shows the edge and the classification.
    blocks.push((
        REGISTERED + 150,
        vec![
            register(REGISTRY, "b", FOREVER)?,
            set_resolver(REGISTRY, "b", PUBLIC_RESOLVER)?,
        ],
    ));
    blocks.push((REGISTERED + 180, vec![mount(ETH_REGISTRY, "n", REGISTRY)?]));
    blocks.push((REGISTERED + DAY, vec![]));
    let count = blocks.len();
    publish(&database, blocks).await?;

    let detail = ok(&database, "/v1/names/a.n.eth?namespace=ens").await?;
    assert_eq!(detail["data"]["status"], json!("active"), "{detail:#}");
    let detail = ok(&database, "/v1/names/b.n.eth?namespace=ens").await?;
    assert_eq!(
        detail["data"]["resolver"]["address"],
        json!(PUBLIC_RESOLVER),
        "{detail:#}"
    );
    let registry = ok(&database, &format!("/v1/registries/1/{REGISTRY}")).await?;
    assert_eq!(
        registry["data"]["name"]["name"],
        json!("n.eth"),
        "{registry:#}"
    );

    let mounted = block(4, count);
    assert_eq!(
        resolver_edges(&database, REGISTRY).await?,
        vec![
            (RESOLVER_X.to_owned(), mounted, None, key(REGISTRY, "a")),
            (
                PUBLIC_RESOLVER.to_owned(),
                mounted,
                None,
                key(REGISTRY, "b")
            ),
        ]
    );
    let row = classification(&database, RESOLVER_X)
        .await?
        .context("the mounted registry's resolver is classified")?;
    assert_eq!(row["block_number"], json!(mounted), "{row:#}");
    database.cleanup().await
}

/// M3: the entry's resolver changed while the registry was unreachable. Only the current one
/// serves and gets an edge, from the mount.
#[tokio::test]
async fn m3_a_resolver_changed_before_the_mount_serves_only_the_current_one() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let mut blocks = unmounted_registry()?;
    blocks.push((
        REGISTERED + 150,
        vec![set_resolver(REGISTRY, "a", PUBLIC_RESOLVER)?],
    ));
    blocks.push((REGISTERED + 180, vec![mount(ETH_REGISTRY, "n", REGISTRY)?]));
    blocks.push((REGISTERED + DAY, vec![]));
    let count = blocks.len();
    publish(&database, blocks).await?;

    let detail = ok(&database, "/v1/names/a.n.eth?namespace=ens").await?;
    assert_eq!(
        detail["data"]["resolver"]["address"],
        json!(PUBLIC_RESOLVER),
        "{detail:#}"
    );
    assert_eq!(
        resolver_edges(&database, REGISTRY).await?,
        vec![(
            PUBLIC_RESOLVER.to_owned(),
            block(4, count),
            None,
            key(REGISTRY, "a")
        )]
    );
    database.cleanup().await
}

/// M5: the entry's pointer log comes before the mount's in one transaction. The name serves
/// the resolver from that block, and the edge opens at the mount.
#[tokio::test]
async fn m5_a_pointer_before_the_mount_in_one_transaction_opens_at_the_mount() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let blocks = vec![
        (REGISTERED, vec![announce(REGISTRY)]),
        (
            REGISTERED + 60,
            vec![
                register(REGISTRY, "a", FOREVER)?,
                register(ETH_REGISTRY, "n", FOREVER)?,
            ],
        ),
        (
            REGISTERED + 120,
            vec![
                set_resolver(REGISTRY, "a", PUBLIC_RESOLVER)?,
                mount(ETH_REGISTRY, "n", REGISTRY)?,
            ],
        ),
        (REGISTERED + DAY, vec![]),
    ];
    let count = blocks.len();
    publish(&database, blocks).await?;

    let detail = ok(&database, "/v1/names/a.n.eth?namespace=ens").await?;
    assert_eq!(
        detail["data"]["resolver"]["address"],
        json!(PUBLIC_RESOLVER),
        "{detail:#}"
    );
    let log: Option<i64> = sqlx::query_scalar(
        "SELECT (provenance ->> 'log_index')::bigint FROM discovery_edges
         WHERE chain_id = $1 AND edge_kind = 'resolver'
           AND provenance ->> 'observation_key' = $2",
    )
    .bind(CHAIN)
    .bind(key(REGISTRY, "a"))
    .fetch_optional(&database.pool)
    .await?;
    assert_eq!(
        resolver_edges(&database, REGISTRY).await?,
        vec![(
            PUBLIC_RESOLVER.to_owned(),
            block(2, count),
            None,
            key(REGISTRY, "a")
        )]
    );
    assert_eq!(log, Some(1));
    database.cleanup().await
}

/// U3: the mounting `n.eth` entry expires between blocks. At the next block the name is no
/// longer served with the resolver, and the registry's edge ends there, with no log.
#[tokio::test]
async fn u3_an_expired_mount_closes_the_registry_edge_at_the_next_block() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let expiry = REGISTERED + 300;
    let blocks = vec![
        (REGISTERED, vec![announce(REGISTRY)]),
        (
            REGISTERED + 60,
            vec![
                register(REGISTRY, "a", FOREVER)?,
                set_resolver(REGISTRY, "a", RESOLVER_X)?,
            ],
        ),
        (REGISTERED + 120, vec![register(ETH_REGISTRY, "n", expiry)?]),
        (REGISTERED + 180, vec![mount(ETH_REGISTRY, "n", REGISTRY)?]),
        (expiry + 60, vec![]),
        (expiry + DAY, vec![]),
    ];
    let count = blocks.len();
    publish(&database, blocks).await?;

    let (status, detail) = get(&database, "/v1/names/a.n.eth?namespace=ens").await?;
    assert!(
        status != StatusCode::OK || detail["data"]["resolver"]["address"] != json!(RESOLVER_X),
        "{detail:#}"
    );
    assert_eq!(
        resolver_edges(&database, REGISTRY).await?,
        vec![(
            RESOLVER_X.to_owned(),
            block(3, count),
            Some(block(4, count)),
            key(REGISTRY, "a")
        )]
    );
    database.cleanup().await
}

/// T1: `LOWER_REGISTRY` sits under the registry's `b`, and `n.eth` mounts the registry last.
/// Both subtrees serve from that block, and both registries' edges open there.
#[tokio::test]
async fn t1_mounting_an_ancestor_serves_and_opens_both_registries() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let blocks = vec![
        (
            REGISTERED,
            vec![announce(LOWER_REGISTRY), announce(REGISTRY)],
        ),
        (
            REGISTERED + 60,
            vec![
                register(LOWER_REGISTRY, "c", FOREVER)?,
                set_resolver(LOWER_REGISTRY, "c", PUBLIC_RESOLVER)?,
                register(REGISTRY, "b", FOREVER)?,
                mount(REGISTRY, "b", LOWER_REGISTRY)?,
                set_resolver(REGISTRY, "b", RESOLVER_Y)?,
            ],
        ),
        (
            REGISTERED + 120,
            vec![register(ETH_REGISTRY, "n", FOREVER)?],
        ),
        (REGISTERED + 180, vec![mount(ETH_REGISTRY, "n", REGISTRY)?]),
        (REGISTERED + DAY, vec![]),
    ];
    let count = blocks.len();
    publish(&database, blocks).await?;

    let detail = ok(&database, "/v1/names/c.b.n.eth?namespace=ens").await?;
    assert_eq!(
        detail["data"]["resolver"]["address"],
        json!(PUBLIC_RESOLVER),
        "{detail:#}"
    );
    let mounted = block(3, count);
    assert_eq!(
        resolver_edges(&database, REGISTRY).await?,
        vec![(RESOLVER_Y.to_owned(), mounted, None, key(REGISTRY, "b"))]
    );
    assert_eq!(
        resolver_edges(&database, LOWER_REGISTRY).await?,
        vec![(
            PUBLIC_RESOLVER.to_owned(),
            mounted,
            None,
            key(LOWER_REGISTRY, "c")
        )]
    );
    database.cleanup().await
}

/// A per-registry announcement report, run against a corpus with one mounted and one unmounted
/// announced registry. Per
/// announced address: its announcement block, the first block a named registry-family
/// `SubregistryChanged` targets it, its `ResolverUpdated` logs before that block, and its
/// resolver edges, all and still open.
#[tokio::test]
async fn the_per_registry_announcement_report_reads_mounts_and_edges() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let mut blocks = unmounted_registry()?;
    blocks.push((
        REGISTERED + 150,
        vec![
            announce(OTHER_REGISTRY),
            register(OTHER_REGISTRY, "z", FOREVER)?,
            set_resolver(OTHER_REGISTRY, "z", RESOLVER_Y)?,
        ],
    ));
    blocks.push((REGISTERED + 180, vec![mount(ETH_REGISTRY, "n", REGISTRY)?]));
    blocks.push((REGISTERED + DAY, vec![]));
    let count = blocks.len();
    publish(&database, blocks).await?;

    let report: Vec<(String, i64, Option<i64>, i64, i64, i64)> = sqlx::query_as(
        "WITH announced AS (
             SELECT edge.from_contract_instance_id AS instance,
                    lower(address.address) AS address,
                    min(edge.active_from_block_number) AS announced_at
             FROM discovery_edges edge
             JOIN contract_instance_addresses address
               ON address.contract_instance_id = edge.from_contract_instance_id
              AND address.chain_id = edge.chain_id
             WHERE edge.chain_id = $1 AND edge.edge_kind = 'registry_announcement'
               AND edge.canonicality_state IN ('canonical', 'safe', 'finalized')
             GROUP BY 1, 2
         ),
         named_mounts AS (
             SELECT announced.address, min(edge.active_from_block_number) AS first_named_mount
             FROM announced
             JOIN discovery_edges edge
               ON edge.chain_id = $1 AND edge.edge_kind = 'subregistry'
              AND edge.to_contract_instance_id = announced.instance
              AND edge.canonicality_state IN ('canonical', 'safe', 'finalized')
             WHERE EXISTS (
                 SELECT 1 FROM normalized_events event
                 WHERE event.chain_id = $1
                   AND event.block_number = edge.active_from_block_number
                   AND event.event_kind = 'SubregistryChanged'
                   AND event.source_family IN ('ens_v2_registry_l1', 'ens_v2_root_l1')
                   AND event.logical_name_id IS NOT NULL
                   AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
                   AND lower(event.after_state ->> 'subregistry') = announced.address
             )
             GROUP BY announced.address
         )
         SELECT announced.address,
                announced.announced_at,
                named_mounts.first_named_mount,
                (SELECT count(*) FROM raw_logs log
                 JOIN chain_lineage lineage
                   ON lineage.chain_id = log.chain_id AND lineage.block_hash = log.block_hash
                  AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                 WHERE log.chain_id = $1
                   AND lower(log.emitting_address) = announced.address
                   AND lower(log.topics[1]) = $2
                   AND log.block_number
                       < COALESCE(named_mounts.first_named_mount, 9223372036854775807)),
                (SELECT count(*) FROM discovery_edges edge
                 WHERE edge.chain_id = $1 AND edge.edge_kind = 'resolver'
                   AND edge.from_contract_instance_id = announced.instance
                   AND edge.canonicality_state IN ('canonical', 'safe', 'finalized')),
                (SELECT count(*) FROM discovery_edges edge
                 WHERE edge.chain_id = $1 AND edge.edge_kind = 'resolver'
                   AND edge.from_contract_instance_id = announced.instance
                   AND edge.canonicality_state IN ('canonical', 'safe', 'finalized')
                   AND edge.deactivated_at IS NULL)
         FROM announced
         LEFT JOIN named_mounts USING (address)
         ORDER BY announced.announced_at, announced.address",
    )
    .bind(CHAIN)
    .bind(format!("{:#x}", ResolverUpdated::SIGNATURE_HASH))
    .fetch_all(&database.pool)
    .await?;
    let row = |address: &str| {
        report
            .iter()
            .find(|row| row.0 == address)
            .cloned()
            .with_context(|| format!("{address} is not in the report: {report:?}"))
    };
    assert_eq!(
        row(REGISTRY)?,
        (
            REGISTRY.to_owned(),
            block(0, count),
            Some(block(4, count)),
            1,
            1,
            1
        )
    );
    assert_eq!(
        row(OTHER_REGISTRY)?,
        (OTHER_REGISTRY.to_owned(), block(3, count), None, 1, 0, 0)
    );
    database.cleanup().await
}

/// Replaces every block from `from` with `replacement`, one block each, then reruns Interpret over
/// them as a redo and rebuilds Project at the new head. The replaced blocks are orphaned, as an
/// Ingest rewind leaves them.
async fn reorganize(
    database: &TestDatabase,
    from: i64,
    replacement: Vec<(u64, Logs)>,
) -> Result<()> {
    let pool = &database.pool;
    let original = |block: i64| format!("{CHAIN}-block-{block}");
    let replaced = |block: i64| format!("{CHAIN}-block-{block}-reorganized");
    let head = from + replacement.len() as i64 - 1;
    assert_eq!(head, HEAD, "the replacement ends at the published head");
    let mut transaction = pool.begin().await?;
    for (offset, (time, logs)) in replacement.into_iter().enumerate() {
        let block = from + offset as i64;
        let parent = if offset == 0 {
            original(block - 1)
        } else {
            replaced(block - 1)
        };
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number, block_timestamp,
                 canonicality_state
             ) VALUES ($1, $2, $3, $4, to_timestamp($5), 'observed')",
        )
        .bind(CHAIN)
        .bind(replaced(block))
        .bind(parent)
        .bind(block)
        .bind(time as f64)
        .execute(&mut *transaction)
        .await?;
        let transaction_hash = format!("{CHAIN}-transaction-{block}-reorganized");
        if let Some((to, _)) = logs.last() {
            sqlx::query(
                "INSERT INTO raw_transactions (
                     chain_id, block_hash, block_number, transaction_hash, transaction_index,
                     from_address, to_address
                 ) VALUES ($1, $2, $3, $4, 0, $5, $6)",
            )
            .bind(CHAIN)
            .bind(replaced(block))
            .bind(block)
            .bind(&transaction_hash)
            .bind(OWNER)
            .bind(*to)
            .execute(&mut *transaction)
            .await?;
        }
        for (index, (emitter, data)) in logs.into_iter().enumerate() {
            let topics: Vec<String> = data
                .topics()
                .iter()
                .map(|topic| format!("{topic:#x}"))
                .collect();
            sqlx::query(
                "INSERT INTO raw_logs (
                     chain_id, block_hash, block_number, transaction_hash, transaction_index,
                     log_index, emitting_address, topics, data
                 ) VALUES ($1, $2, $3, $4, 0, $5, $6, $7, $8)",
            )
            .bind(CHAIN)
            .bind(replaced(block))
            .bind(block)
            .bind(&transaction_hash)
            .bind(index as i64)
            .bind(emitter)
            .bind(topics)
            .bind(data.data.as_ref())
            .execute(&mut *transaction)
            .await?;
        }
    }
    sqlx::query("UPDATE chain_heads SET latest_block_hash = $2, latest_block_number = $3 WHERE chain_id = $1")
        .bind(CHAIN)
        .bind(original(from - 1))
        .bind(from - 1)
        .execute(&mut *transaction)
        .await?;
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND block_number >= $2 AND block_hash NOT LIKE '%-reorganized'",
    )
    .bind(CHAIN)
    .bind(from)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'canonical'
         WHERE chain_id = $1 AND block_hash LIKE '%-reorganized'",
    )
    .bind(CHAIN)
    .execute(&mut *transaction)
    .await?;
    sqlx::query("UPDATE chain_heads SET latest_block_hash = $2, latest_block_number = $3 WHERE chain_id = $1")
        .bind(CHAIN)
        .bind(replaced(head))
        .bind(head)
        .execute(&mut *transaction)
        .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET current_block_hash = $2,
             live_handoff_block_hash = CASE WHEN phase_name = 'ingest' THEN $2 END
         WHERE chain_id = $1 AND current_block_hash = $3",
    )
    .bind(CHAIN)
    .bind(replaced(head))
    .bind(original(head))
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    bigname_interpret::Engine::new(pool.clone())
        .run_batch(bigname_interpret::BatchRequest {
            chain_id: CHAIN.into(),
            from_block: from,
            to_block: head,
            resume_current: None,
            mode: bigname_interpret::RunMode::Redo,
        })
        .await?;
    let input = bigname_project::families::input_token(pool, CHAIN).await?;
    let publication = bigname_project::families::apply(
        pool,
        CHAIN,
        &bigname_project::Marker {
            number: head,
            hash: replaced(head),
        },
        bigname_project::families::FamilyMode::Rebuild,
        &input,
        &bigname_project::families::FamilyOptions::new(
            bigname_content_hash::PROJECTION_CONTENT_HASH,
        ),
    )
    .await?;
    assert_eq!(publication.marker.map(|marker| marker.number), Some(head));
    Ok(())
}

/// O2: M1's mount block is reorganized away and the mount lands one block later. The edge the
/// orphaned mount opened is orphaned with it, and the registry's edge opens at the new mount,
/// where its entry serves from.
#[tokio::test]
async fn o2_a_reorganized_mount_orphans_its_edge_and_reopens_at_the_new_mount() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let mut blocks = unmounted_registry()?;
    blocks.push((REGISTERED + 180, vec![mount(ETH_REGISTRY, "n", REGISTRY)?]));
    blocks.push((REGISTERED + DAY, vec![]));
    let count = blocks.len();
    publish(&database, blocks).await?;
    let first_mount = block(3, count);
    assert_eq!(
        resolver_edges(&database, REGISTRY).await?,
        vec![(RESOLVER_X.to_owned(), first_mount, None, key(REGISTRY, "a"))]
    );

    reorganize(
        &database,
        first_mount,
        vec![
            (REGISTERED + 180, vec![]),
            (REGISTERED + DAY, vec![mount(ETH_REGISTRY, "n", REGISTRY)?]),
        ],
    )
    .await?;

    let second_mount = block(4, count);
    assert_eq!(
        resolver_edges(&database, REGISTRY).await?,
        vec![(
            RESOLVER_X.to_owned(),
            second_mount,
            None,
            key(REGISTRY, "a")
        )]
    );
    let orphaned: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM discovery_edges
         WHERE chain_id = $1 AND edge_kind = 'resolver' AND active_from_block_number = $2
           AND canonicality_state = 'orphaned'",
    )
    .bind(CHAIN)
    .bind(first_mount)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(orphaned, 1, "the orphaned mount's edge is orphaned with it");
    let detail = ok(&database, "/v1/names/a.n.eth?namespace=ens").await?;
    assert_eq!(detail["data"]["status"], json!("active"), "{detail:#}");
    let row = classification(&database, RESOLVER_X)
        .await?
        .context("the mounted registry's resolver is classified")?;
    assert_eq!(row["block_number"], json!(second_mount), "{row:#}");
    database.cleanup().await
}

/// O1: the registry's announcement block, and the block where `n.eth` mounts it, are
/// reorganized away. The replacement mounts it before it announces again at the head, and its
/// own entry and pointer are gone. Nothing from the orphaned blocks stays live: one announcement
/// edge from the new block, and no resolver edge, where the mount had opened one.
#[tokio::test]
async fn o1_a_reorganized_announcement_leaves_nothing_live_from_the_orphaned_block() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let blocks = vec![
        (REGISTERED - 60, vec![]),
        (REGISTERED, vec![announce(REGISTRY)]),
        (
            REGISTERED + 60,
            vec![
                register(REGISTRY, "a", FOREVER)?,
                set_resolver(REGISTRY, "a", RESOLVER_X)?,
                register(ETH_REGISTRY, "n", FOREVER)?,
                mount(ETH_REGISTRY, "n", REGISTRY)?,
            ],
        ),
        (REGISTERED + DAY, vec![]),
    ];
    let count = blocks.len();
    publish(&database, blocks).await?;
    let announced = block(1, count);
    assert_eq!(
        resolver_edges(&database, REGISTRY).await?.len(),
        1,
        "the mount opened the registry's edge"
    );

    reorganize(
        &database,
        announced,
        vec![
            (REGISTERED, vec![]),
            (
                REGISTERED + 60,
                vec![
                    register(ETH_REGISTRY, "n", FOREVER)?,
                    mount(ETH_REGISTRY, "n", REGISTRY)?,
                ],
            ),
            (REGISTERED + DAY, vec![announce(REGISTRY)]),
        ],
    )
    .await?;

    let announcements: Vec<(i64, String)> = sqlx::query_as(
        "SELECT edge.active_from_block_number, edge.canonicality_state::text
         FROM discovery_edges edge
         JOIN contract_instance_addresses address
           ON address.contract_instance_id = edge.from_contract_instance_id
          AND address.chain_id = edge.chain_id
         WHERE edge.chain_id = $1 AND edge.edge_kind = 'registry_announcement'
           AND lower(address.address) = $2
         ORDER BY 1",
    )
    .bind(CHAIN)
    .bind(REGISTRY)
    .fetch_all(&database.pool)
    .await?;
    assert_eq!(
        announcements,
        vec![
            (announced, "orphaned".to_owned()),
            (block(3, count), "canonical".to_owned())
        ]
    );
    assert_eq!(resolver_edges(&database, REGISTRY).await?, vec![]);
    database.cleanup().await
}

/// A contract that announces itself as a resolver, through `Upgraded` naming the declared
/// implementation, before it announces itself as a registry in the same transaction. The edge
/// its mount opens runs under the registry manifest and rule its own `ResolverUpdated` selects,
/// not the resolver admission that came first.
#[tokio::test]
async fn m1_a_registry_that_announced_itself_as_a_resolver_first_opens_under_the_registry_rule()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let blocks = vec![
        (
            REGISTERED,
            vec![
                (
                    REGISTRY,
                    Upgraded {
                        implementation: RESOLVER_IMPLEMENTATION.parse()?,
                    }
                    .encode_log_data(),
                ),
                announce(REGISTRY),
                register(REGISTRY, "a", FOREVER)?,
                set_resolver(REGISTRY, "a", PUBLIC_RESOLVER)?,
            ],
        ),
        (
            REGISTERED + 60,
            vec![
                register(ETH_REGISTRY, "n", FOREVER)?,
                mount(ETH_REGISTRY, "n", REGISTRY)?,
            ],
        ),
        (REGISTERED + DAY, vec![]),
    ];
    let count = blocks.len();
    publish(&database, blocks).await?;

    let mounted = block(1, count);
    assert_eq!(
        resolver_edges(&database, REGISTRY).await?,
        vec![(
            PUBLIC_RESOLVER.to_owned(),
            mounted,
            None,
            key(REGISTRY, "a")
        )]
    );
    let (family, basis): (String, String) = sqlx::query_as(
        "SELECT manifest.source_family, edge.admission_basis
         FROM discovery_edges edge
         JOIN manifest_versions manifest ON manifest.manifest_id = edge.source_manifest_id
         JOIN contract_instance_addresses source
           ON source.contract_instance_id = edge.from_contract_instance_id
          AND source.chain_id = edge.chain_id
         WHERE edge.chain_id = $1 AND edge.edge_kind = 'resolver'
           AND lower(source.address) = $2",
    )
    .bind(CHAIN)
    .bind(REGISTRY)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(
        (family.as_str(), basis.as_str()),
        ("ens_v2_registry_l1", "reachable_from_root")
    );
    let detail = ok(&database, "/v1/names/a.n.eth?namespace=ens").await?;
    assert_eq!(
        detail["data"]["resolver"]["address"],
        json!(PUBLIC_RESOLVER),
        "{detail:#}"
    );
    database.cleanup().await
}
