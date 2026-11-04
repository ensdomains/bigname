//! Plan and row tests for registration-anchored history reads (`registration_identity.rs`).
//!
//! The registration filter compares binding windows with the event's block timestamp. When it
//! read that timestamp through the statement's `LEFT JOIN chain_lineage rb`, the whole filter sat
//! on the join, subqueries included. PostgreSQL then costed the subqueries once per joined row
//! and hashed every lineage row of the chain instead of probing the key per candidate row. The
//! filter now reads the event's block through its own keyed probe
//! (`lineage::event_block_timestamp`).
//!
//! The plan test checks every statement of the read in both plan modes. It runs on the fixture
//! as built and again with the chain's lineage inflated. No lineage join may filter on a
//! subquery, and no plan may be parallel. On the inflated lineage, the lineage must be joined by
//! probes keyed on `(chain_id, block_hash)`. The previous text is the probe spelled
//! `rb.block_timestamp` again. The test checks that it still puts subqueries on the join and
//! still scans the inflated lineage, so the fixture keeps proving something.
//!
//! The row test runs both texts on every input class, in both plan modes, and compares the
//! counts, the page rows, the next cursors, the cursor's anchor block and the registration
//! precheck. Three altered probes (no timestamp, a timestamp three microseconds late, the
//! canonical block's timestamp at the event's height) must each change some answer, so the
//! fixture holds cells that the probe's value decides.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::Value;
use sqlx::{
    Arguments, Connection, Execute, PgConnection, Postgres, QueryBuilder, Row, postgres::PgRow,
    raw_sql,
};
use uuid::Uuid;

use super::{
    ChainBlockRange, EventHistoryReadFilter, HistoryBlockWindow, HistoryCursor, HistoryEvent,
    HistoryOrder, HistoryPosition,
    attribution::AttributedRecords,
    decoders::decode_history_event,
    keyset::{
        history_cursor_from_row, history_keyset_filter, load_history_keyset,
        push_history_keyset_query,
    },
    lineage::event_block_timestamp,
    paging::push_history_page_query,
    registration_identity::push_is_public_registration_id_query,
    selectors::HistorySelector,
    summary::push_history_count_query,
};
use crate::projection_helpers::split_keyset_page;

const PLAN_MODES: [&str; 2] = ["force_custom_plan", "force_generic_plan"];
/// The `chain_lineage` indexes that lead with `(chain_id, block_hash)`.
const KEYED_LINEAGE_INDEXES: [&str; 2] = [
    "chain_lineage_pkey",
    "chain_lineage_chain_id_block_hash_block_number_key",
];
const CHAIN: &str = "ethereum-mainnet";
const PUBLISHED_BLOCK: i64 = 5_000;
const PERMISSION_KINDS: [&str; 6] = [
    "PermissionChanged",
    "RootPermissionChanged",
    "PermissionScopeChanged",
    "RolesChanged",
    "EACRolesChanged",
    "AccountPermissionChanged",
];

/// The fixture's name and resources, by the number their namehash or uuid encodes.
const NAME: u128 = 0xa1;
const LEASE: u128 = 0xa10;
const NEXT_LEASE: u128 = 0xa11;
const WRAPPER: u128 = 0xa12;
const MIGRATION: u128 = 0xa13;
const NAMELESS_LEASE: u128 = 0xa20;
const NOT_A_REGISTRATION: u128 = 0xa30;
const HOT_LEASE: u128 = 0xa40;

#[tokio::test]
async fn registration_reads_keep_subqueries_off_the_lineage_join() -> Result<()> {
    with_fixture("registration_plan", true, async |connection| {
        let cells = plan_cells(connection).await?;
        let probe = event_block_timestamp("ne");
        let mut failures = Vec::new();
        for lineage in [Lineage::AsBuilt, Lineage::Inflated] {
            if lineage == Lineage::Inflated {
                raw_sql(INFLATE_LINEAGE_SQL)
                    .execute(&mut *connection)
                    .await
                    .context("inflate the lineage")?;
            }
            raw_sql("DEALLOCATE ALL").execute(&mut *connection).await?;
            // sqlx's statement cache still names the statements DEALLOCATE dropped.
            connection.clear_cached_statements().await?;
            for (cell_index, cell) in cells.iter().enumerate() {
                for (statement_index, statement) in cell.statements.iter().enumerate() {
                    let label = format!("{lineage:?}, {}, {}", cell.label, statement.label());
                    let bound = bind_statement(connection, &cell.filter, true, statement).await?;
                    if !bound.sql.contains(&probe) {
                        failures.push(format!(
                            "{label}: the statement does not read the event block through its probe"
                        ));
                    }
                    let name = format!("plan_{cell_index}_{statement_index}");
                    let before_name = format!("{name}_before");
                    prepare(connection, &name, &bound.sql).await?;
                    prepare(
                        connection,
                        &before_name,
                        &bound.sql.replace(&probe, "rb.block_timestamp"),
                    )
                    .await?;
                    for mode in PLAN_MODES {
                        let cell_label = format!("{label}, {mode}");
                        let analyze = cell.bound_lineage_reads && lineage == Lineage::Inflated;
                        let after =
                            explain(connection, mode, &name, &bound.literals, analyze).await?;
                        let before =
                            explain(connection, mode, &before_name, &bound.literals, false).await?;
                        println!(
                            "{cell_label}\n  probe text: {}\n  previous text: {}",
                            outline(&after),
                            outline(&before)
                        );
                        if cell.folds_in_custom_mode && mode == "force_custom_plan" {
                            for (text, plan) in [("probe", &after), ("previous", &before)] {
                                if !lineage_scans(plan).is_empty() {
                                    failures.push(format!(
                                        "{cell_label}: the {text} text reads lineage although \
                                         its registration filter is constant FALSE\n{}",
                                        outline(plan)
                                    ));
                                }
                            }
                            continue;
                        }
                        failures.extend(prefixed(&cell_label, subquery_join_failures(&after)));
                        failures.extend(prefixed(&cell_label, gather_failures(&after)));
                        if !subquery_on_lineage_join(&before) {
                            failures.push(format!(
                                "{cell_label}: the previous text no longer puts a subquery on \
                                 the lineage join, so this cell proves nothing about it\n{}",
                                outline(&before)
                            ));
                        }
                        if let Some(order) = cell.order {
                            failures.extend(prefixed(&cell_label, order_failures(&after, order)));
                        }
                        if lineage == Lineage::Inflated {
                            failures.extend(prefixed(&cell_label, keyed_failures(&after)));
                            if cell.previous_hashes && !lineage_scanned(&before) {
                                failures.push(format!(
                                    "{cell_label}: the previous text probes the inflated lineage, \
                                     so this cell proves nothing about the hash\n{}",
                                    outline(&before)
                                ));
                            }
                            if analyze {
                                failures
                                    .extend(prefixed(&cell_label, lineage_read_failures(&after)));
                            }
                        }
                    }
                }
            }
        }
        ensure!(
            failures.is_empty(),
            "registration read plans:\n{}",
            failures.join("\n\n")
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn registration_rows_match_the_previous_text() -> Result<()> {
    with_fixture("registration_rows", false, async |connection| {
        let cells = row_cells(connection).await?;
        let probe = event_block_timestamp("ne");
        let mutants = [
            ("no timestamp", "NULL::timestamptz".to_owned(), false),
            (
                "three microseconds late",
                format!("({probe} + interval '3 microseconds')"),
                false,
            ),
            (
                "the canonical block at the height",
                CANONICAL_HEIGHT_TIMESTAMP.to_owned(),
                true,
            ),
        ];
        let mut changed = [false; 3];
        let mut failures = Vec::new();
        for (cell_index, cell) in cells.iter().enumerate() {
            for (statement_index, statement) in cell.statements.iter().enumerate() {
                let label = format!("{}, {}", cell.label, statement.label());
                let bound =
                    bind_statement(connection, &cell.filter, cell.canonical_only, statement)
                        .await?;
                if !bound.sql.contains(&probe) {
                    failures.push(format!(
                        "{label}: the statement does not read the event block through its probe"
                    ));
                }
                let name = format!("rows_{cell_index}_{statement_index}");
                prepare(connection, &name, &bound.sql).await?;
                prepare(
                    connection,
                    &format!("{name}_before"),
                    &bound.sql.replace(&probe, "rb.block_timestamp"),
                )
                .await?;
                for (index, (_, expression, _)) in mutants.iter().enumerate() {
                    prepare(
                        connection,
                        &format!("{name}_mutant_{index}"),
                        &bound.sql.replace(&probe, expression),
                    )
                    .await?;
                }
                for mode in PLAN_MODES {
                    let cell_label = format!("{label}, {mode}");
                    let after = output(connection, mode, &name, &bound.literals, statement).await?;
                    let before = output(
                        connection,
                        mode,
                        &format!("{name}_before"),
                        &bound.literals,
                        statement,
                    )
                    .await?;
                    if after != before {
                        failures.push(format!(
                            "{cell_label}: the texts disagree\nprobe {after:#?}\nprevious \
                             {before:#?}"
                        ));
                    }
                    for (index, (_, _, noncanonical)) in mutants.iter().enumerate() {
                        if *noncanonical == cell.canonical_only {
                            continue;
                        }
                        let mutant = output(
                            connection,
                            mode,
                            &format!("{name}_mutant_{index}"),
                            &bound.literals,
                            statement,
                        )
                        .await?;
                        changed[index] |= mutant != after;
                    }
                    // Only a page that holds every row of the read can pin one.
                    if let Output::Page {
                        rows,
                        next_cursor: None,
                    } = &after
                    {
                        let listed = rows
                            .iter()
                            .map(|row| row.event_identity.as_str())
                            .collect::<Vec<_>>();
                        for (identity, expected) in &cell.pins {
                            if listed.contains(identity) != *expected {
                                failures.push(format!(
                                    "{cell_label}: {identity} listed {}, expected {expected}\n\
                                     {listed:?}",
                                    !expected
                                ));
                            }
                        }
                    }
                }
            }
        }
        for ((label, _, _), changed) in mutants.iter().zip(changed) {
            if !changed {
                failures.push(format!(
                    "a probe that reads {label} changes no answer, so the fixture does not \
                     exercise the event block"
                ));
            }
        }
        ensure!(
            failures.is_empty(),
            "registration read rows:\n{}",
            failures.join("\n\n")
        );
        Ok(())
    })
    .await
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Lineage {
    AsBuilt,
    Inflated,
}

/// One statement of a registration read.
enum Statement {
    Count,
    Page {
        page_size: i64,
        cursor: Option<HistoryCursor>,
    },
    /// The validation of a cursor without its position: the anchor row's block.
    Keyset(HistoryCursor),
    /// `is_public_registration_id`, the precheck that sets `registration_id_is_public`.
    IsPublic(Uuid),
}

impl Statement {
    fn label(&self) -> String {
        match self {
            Self::Count => "count".to_owned(),
            Self::Page {
                page_size,
                cursor: None,
            } => format!("page of {page_size}"),
            Self::Page {
                page_size,
                cursor: Some(cursor),
            } => format!(
                "page of {page_size} after {} {}",
                cursor.event_identity,
                if cursor.position.is_some() {
                    "by position"
                } else {
                    "through its anchor row"
                }
            ),
            Self::Keyset(cursor) => format!("cursor anchor {}", cursor.event_identity),
            Self::IsPublic(id) => format!("registration precheck of {id}"),
        }
    }
}

struct PlanCell {
    label: &'static str,
    filter: EventHistoryReadFilter,
    statements: Vec<Statement>,
    /// The page's order, checked against the plan's sort keys.
    order: Option<HistoryOrder>,
    /// The previous text must scan the inflated lineage here.
    previous_hashes: bool,
    /// The registration filter is a FALSE parameter, which a custom plan folds away.
    folds_in_custom_mode: bool,
    /// Run with `ANALYZE` and bound the lineage rows read by the candidate rows.
    bound_lineage_reads: bool,
}

struct RowCell {
    label: &'static str,
    filter: EventHistoryReadFilter,
    canonical_only: bool,
    statements: Vec<Statement>,
    /// Event identities a page holding every row must list (`true`) or leave out (`false`).
    pins: Vec<(&'static str, bool)>,
}

#[derive(Debug, PartialEq)]
enum Output {
    Count(i64),
    Page {
        rows: Vec<HistoryEvent>,
        next_cursor: Option<HistoryCursor>,
    },
    AnchorBlock(Option<Option<i64>>),
    IsPublic(bool),
}

struct Registration {
    id: u128,
    names: Vec<u128>,
    resources: Vec<u128>,
    public: bool,
}

fn rid(k: u128) -> Uuid {
    Uuid::from_u128(k)
}

fn name(k: u128) -> String {
    format!("ens:0x{k:064x}")
}

fn lease() -> Registration {
    Registration {
        id: LEASE,
        names: vec![NAME],
        resources: vec![LEASE, NEXT_LEASE, WRAPPER],
        public: true,
    }
}

fn next_lease() -> Registration {
    Registration {
        id: NEXT_LEASE,
        names: vec![NAME],
        resources: vec![LEASE, NEXT_LEASE, WRAPPER],
        public: true,
    }
}

fn one_arm(id: u128, public: bool) -> Registration {
    Registration {
        id,
        names: Vec::new(),
        resources: vec![id],
        public,
    }
}

fn read_filter(
    registration: &Registration,
    attributed: &AttributedRecords,
) -> EventHistoryReadFilter {
    EventHistoryReadFilter {
        selectors: vec![HistorySelector::ProductRegistration {
            logical_name_ids: registration.names.iter().map(|k| name(*k)).collect(),
            resource_ids: registration.resources.iter().map(|k| rid(*k)).collect(),
        }],
        registration_id: Some(rid(registration.id)),
        registration_id_is_public: registration.public,
        publication_block_bounds: Some(BTreeMap::from([(CHAIN.to_owned(), PUBLISHED_BLOCK)])),
        namespace: Some("ens".to_owned()),
        attributed_records: attributed.clone(),
        ..EventHistoryReadFilter::default()
    }
}

/// The block window `/v1/events` applies: every row of the chain at or below the published block.
fn windowed(mut filter: EventHistoryReadFilter) -> EventHistoryReadFilter {
    filter.block_window = Some(HistoryBlockWindow {
        ranges: vec![ChainBlockRange {
            chain_id: CHAIN.to_owned(),
            from_block: None,
            to_block: Some(PUBLISHED_BLOCK),
        }],
    });
    filter
}

fn with_kinds(mut filter: EventHistoryReadFilter, kinds: &[&str]) -> EventHistoryReadFilter {
    filter.event_kinds = kinds.iter().map(|kind| (*kind).to_owned()).collect();
    filter
}

fn with_order(mut filter: EventHistoryReadFilter, order: HistoryOrder) -> EventHistoryReadFilter {
    filter.order = order;
    filter
}

/// `name=` and `address=` add their own selector. `/v1/events` lists the name's first.
fn with_selector(
    mut filter: EventHistoryReadFilter,
    selector: HistorySelector,
) -> EventHistoryReadFilter {
    filter.selectors.insert(0, selector);
    filter
}

fn name_selector() -> HistorySelector {
    HistorySelector::LogicalNamesOrResources {
        logical_name_ids: vec![name(NAME)],
        resource_ids: vec![rid(LEASE), rid(NEXT_LEASE), rid(WRAPPER)],
    }
}

/// What `load_address_history_selector` builds for a `/v1/events` product read.
fn address_selector() -> HistorySelector {
    let address = "0x00000000000000000000000000000000000000ad";
    HistorySelector::OrAccountParticipants {
        anchors: Box::new(HistorySelector::OrRootPermissionSubject {
            anchors: Box::new(HistorySelector::LogicalNamesOrResources {
                logical_name_ids: vec![name(NAME)],
                resource_ids: vec![rid(LEASE)],
            }),
            subject: address.to_owned(),
            namespace: Some("ens".to_owned()),
        }),
        address: address.to_owned(),
        namespace: Some("ens".to_owned()),
        relations: None,
    }
}

async fn event_ids(
    connection: &mut PgConnection,
    identities: &[String],
) -> Result<BTreeMap<String, i64>> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT event_identity, normalized_event_id FROM normalized_events
         WHERE event_identity = ANY($1)",
    )
    .bind(identities)
    .fetch_all(&mut *connection)
    .await?;
    ensure!(
        rows.len() == identities.len(),
        "missing fixture rows in {identities:?}"
    );
    Ok(rows.into_iter().collect())
}

/// The resolver record writes attributed to each resource, as `with_attributed_records` loads
/// them: the lease's own writes, the wrapper's, and the next lease's.
async fn lease_attribution(connection: &mut PgConnection) -> Result<AttributedRecords> {
    let mut identities = (1..=10)
        .map(|n| format!("story:attr:{n}"))
        .collect::<Vec<_>>();
    identities.push("story:attr:null".to_owned());
    let ids = event_ids(connection, &identities).await?;
    let id = |identity: &str| ids[identity];
    let set = |range: std::ops::RangeInclusive<u32>| {
        range
            .map(|n| id(&format!("story:attr:{n}")))
            .collect::<BTreeSet<_>>()
    };
    let mut own = set(1..=4);
    own.insert(id("story:attr:null"));
    Ok(AttributedRecords::from_map(BTreeMap::from([
        (rid(LEASE), own),
        (rid(WRAPPER), set(5..=7)),
        (rid(NEXT_LEASE), set(8..=10)),
    ])))
}

async fn nameless_lease_attribution(connection: &mut PgConnection) -> Result<AttributedRecords> {
    let identities = (1..=30)
        .map(|n| format!("nameless:attr:{n}"))
        .collect::<Vec<_>>();
    let ids = event_ids(connection, &identities).await?;
    Ok(AttributedRecords::from_map(BTreeMap::from([(
        rid(NAMELESS_LEASE),
        ids.into_values().collect(),
    )])))
}

/// A cursor at the row `identity`, with its position or, without one, through the row itself.
async fn cursor_at(
    connection: &mut PgConnection,
    identity: &str,
    with_position: bool,
) -> Result<HistoryCursor> {
    let row = sqlx::query(
        "SELECT normalized_event_id, block_number, chain_id, block_hash, transaction_index,
                transaction_hash, log_index
         FROM normalized_events WHERE event_identity = $1",
    )
    .bind(identity)
    .fetch_one(&mut *connection)
    .await?;
    Ok(HistoryCursor {
        normalized_event_id: Some(row.try_get(0)?),
        event_identity: identity.to_owned(),
        position: with_position
            .then(|| -> Result<_> {
                Ok(HistoryPosition {
                    block_number: row.try_get(1)?,
                    chain_id: row.try_get(2)?,
                    block_hash: row.try_get(3)?,
                    transaction_index: row.try_get(4)?,
                    transaction_hash: row.try_get(5)?,
                    log_index: row.try_get(6)?,
                })
            })
            .transpose()?,
    })
}

async fn plan_cells(connection: &mut PgConnection) -> Result<Vec<PlanCell>> {
    let attributed = lease_attribution(connection).await?;
    let nameless = nameless_lease_attribution(connection).await?;
    let none = AttributedRecords::default();
    let lease = windowed(read_filter(&lease(), &attributed));
    let positioned = cursor_at(connection, "story:name:150:0", true).await?;
    let anchored = cursor_at(connection, "story:name:150:0", false).await?;
    let cell = |label, filter, statements| PlanCell {
        label,
        filter,
        statements,
        order: None,
        previous_hashes: false,
        folds_in_custom_mode: false,
        bound_lineage_reads: false,
    };
    let count_and_page = |page_size| {
        vec![
            Statement::Count,
            Statement::Page {
                page_size,
                cursor: None,
            },
        ]
    };
    let page = |page_size| {
        vec![Statement::Page {
            page_size,
            cursor: None,
        }]
    };
    Ok(vec![
        PlanCell {
            previous_hashes: true,
            ..cell(
                "P1 three arms, permission events, oldest first",
                with_order(
                    with_kinds(lease.clone(), &PERMISSION_KINDS),
                    HistoryOrder::Asc,
                ),
                count_and_page(200),
            )
        },
        cell(
            "P2 one arm, a lease with no name",
            windowed(read_filter(&one_arm(NAMELESS_LEASE, true), &none)),
            count_and_page(200),
        ),
        PlanCell {
            folds_in_custom_mode: true,
            ..cell(
                "P2 one arm, not a registration",
                windowed(read_filter(&one_arm(NOT_A_REGISTRATION, false), &none)),
                count_and_page(200),
            )
        },
        PlanCell {
            bound_lineage_reads: true,
            ..cell(
                "P3 a hot resource",
                windowed(read_filter(&one_arm(HOT_LEASE, true), &none)),
                page(20),
            )
        },
        cell(
            "P4 more attributed writes than the page",
            windowed(read_filter(&one_arm(NAMELESS_LEASE, true), &nameless)),
            count_and_page(20),
        ),
        cell(
            "P7 one event kind",
            with_kinds(lease.clone(), &["RecordChanged"]),
            page(200),
        ),
        cell(
            "P7 three event kinds",
            with_kinds(
                lease.clone(),
                &["RecordChanged", "ResolverChanged", "RegistrationGranted"],
            ),
            page(200),
        ),
        PlanCell {
            order: Some(HistoryOrder::Desc),
            ..cell(
                "P7, P8 no event kind filter, newest first",
                lease.clone(),
                page(200),
            )
        },
        PlanCell {
            order: Some(HistoryOrder::Asc),
            ..cell(
                "P8 oldest first",
                with_order(lease.clone(), HistoryOrder::Asc),
                page(200),
            )
        },
        cell(
            "P9 a cursor with its position",
            lease.clone(),
            vec![Statement::Page {
                page_size: 5,
                cursor: Some(positioned),
            }],
        ),
        cell(
            "P9 a cursor through its anchor row",
            lease.clone(),
            vec![
                Statement::Page {
                    page_size: 5,
                    cursor: Some(anchored.clone()),
                },
                Statement::Keyset(anchored),
            ],
        ),
        cell(
            "P10 with name=",
            with_selector(lease.clone(), name_selector()),
            count_and_page(200),
        ),
        cell(
            "P10 with contract_address=",
            {
                let mut filter = lease.clone();
                filter.contract_address =
                    Some("0x00000000000000000000000000000000000000ca".to_owned());
                filter
            },
            count_and_page(200),
        ),
        cell(
            "P10 with address=",
            with_selector(lease.clone(), address_selector()),
            page(200),
        ),
        cell(
            "P11 the registration precheck",
            lease,
            vec![
                Statement::IsPublic(rid(LEASE)),
                Statement::IsPublic(rid(NAMELESS_LEASE)),
                Statement::IsPublic(rid(NOT_A_REGISTRATION)),
            ],
        ),
    ])
}

async fn row_cells(connection: &mut PgConnection) -> Result<Vec<RowCell>> {
    let attributed = lease_attribution(connection).await?;
    let nameless = nameless_lease_attribution(connection).await?;
    let none = AttributedRecords::default();
    let lease_filter = read_filter(&lease(), &attributed);
    let next_lease_filter = read_filter(&next_lease(), &attributed);
    let positioned = cursor_at(connection, "story:name:150:0", true).await?;
    let anchored = cursor_at(connection, "story:name:150:0", false).await?;
    let counted_pages = |page_size| {
        vec![
            Statement::Count,
            Statement::Page {
                page_size,
                cursor: None,
            },
            Statement::Page {
                page_size: 3,
                cursor: None,
            },
        ]
    };
    let prechecks = |ids: &[u128]| {
        ids.iter()
            .map(|id| Statement::IsPublic(rid(*id)))
            .collect::<Vec<_>>()
    };
    let all_resources = [
        LEASE,
        NEXT_LEASE,
        WRAPPER,
        MIGRATION,
        NAMELESS_LEASE,
        NOT_A_REGISTRATION,
        HOT_LEASE,
    ];
    // The binding to the next lease starts five microseconds into block 200, so log index 3
    // still belongs to the wrapped lease and log index 7 to the next one.
    let lease_pins = vec![
        ("story:name:200:3", true),
        ("story:name:200:7", false),
        ("story:registry:130:2", true),
        ("story:registry:150:0", false),
        ("story:attr:5", true),
        ("story:attr:8", false),
        ("story:migration", true),
    ];
    let next_lease_pins = vec![
        ("story:name:200:3", false),
        ("story:name:200:7", true),
        ("story:attr:8", true),
    ];
    let mut lease_statements = counted_pages(200);
    lease_statements.extend([
        Statement::Page {
            page_size: 3,
            cursor: Some(positioned),
        },
        Statement::Page {
            page_size: 3,
            cursor: Some(anchored.clone()),
        },
        Statement::Keyset(anchored),
    ]);
    lease_statements.extend(prechecks(&all_resources[..]));
    Ok(vec![
        RowCell {
            label: "R1-R5 the lease, newest first",
            filter: lease_filter.clone(),
            canonical_only: true,
            statements: lease_statements,
            pins: lease_pins.clone(),
        },
        RowCell {
            label: "R1-R5 the lease, oldest first",
            filter: with_order(lease_filter.clone(), HistoryOrder::Asc),
            canonical_only: true,
            statements: counted_pages(200),
            pins: lease_pins.clone(),
        },
        RowCell {
            label: "R1-R5 the lease, in the /v1/events block window",
            filter: windowed(lease_filter.clone()),
            canonical_only: true,
            statements: counted_pages(200),
            pins: lease_pins.clone(),
        },
        RowCell {
            label: "R1-R5 the lease with name=",
            filter: with_selector(lease_filter.clone(), name_selector()),
            canonical_only: true,
            statements: counted_pages(200),
            pins: lease_pins,
        },
        RowCell {
            label: "R4 the next lease",
            filter: next_lease_filter.clone(),
            canonical_only: true,
            statements: counted_pages(200),
            pins: next_lease_pins,
        },
        RowCell {
            label: "R7 the lease, noncanonical",
            filter: lease_filter,
            canonical_only: false,
            statements: {
                let mut statements = counted_pages(200);
                statements.extend(prechecks(&all_resources[..]));
                statements
            },
            pins: Vec::new(),
        },
        RowCell {
            label: "R7 the next lease, noncanonical",
            filter: next_lease_filter,
            canonical_only: false,
            statements: counted_pages(200),
            pins: Vec::new(),
        },
        RowCell {
            label: "one arm, a lease with no name and its attributed writes",
            filter: read_filter(&one_arm(NAMELESS_LEASE, true), &nameless),
            canonical_only: true,
            statements: counted_pages(40),
            pins: vec![("nameless:attr:1", true), ("nameless:grant", true)],
        },
        RowCell {
            label: "one arm, not a registration",
            filter: read_filter(&one_arm(NOT_A_REGISTRATION, false), &none),
            canonical_only: true,
            statements: counted_pages(20),
            pins: Vec::new(),
        },
        RowCell {
            label: "a hot resource",
            filter: read_filter(&one_arm(HOT_LEASE, true), &none),
            canonical_only: true,
            statements: counted_pages(20),
            pins: Vec::new(),
        },
    ])
}

/// A statement's text and its bound values as SQL literals, ready for `EXECUTE`.
struct Bound {
    sql: String,
    literals: String,
}

async fn bind_statement(
    connection: &mut PgConnection,
    filter: &EventHistoryReadFilter,
    canonical_only: bool,
    statement: &Statement,
) -> Result<Bound> {
    match statement {
        Statement::Count => {
            let mut builder = QueryBuilder::<Postgres>::new("");
            push_history_count_query(&mut builder, filter, canonical_only, None);
            bind(connection, builder).await
        }
        Statement::Page { page_size, cursor } => {
            // The cursor's block, as the page read loads it before the page.
            let keyset = match cursor {
                Some(cursor) => Some(
                    load_history_keyset(connection, filter, canonical_only, cursor, false).await?,
                ),
                None => None,
            };
            let mut builder = QueryBuilder::<Postgres>::new("");
            push_history_page_query(
                &mut builder,
                filter,
                canonical_only,
                keyset.as_ref(),
                false,
                page_size + 1,
            );
            bind(connection, builder).await
        }
        Statement::Keyset(cursor) => {
            let cursor_filter = history_keyset_filter(filter);
            let mut builder = QueryBuilder::<Postgres>::new("");
            push_history_keyset_query(&mut builder, &cursor_filter, canonical_only, cursor, false);
            bind(connection, builder).await
        }
        Statement::IsPublic(id) => {
            let mut builder = QueryBuilder::<Postgres>::new("");
            push_is_public_registration_id_query(
                &mut builder,
                *id,
                canonical_only,
                filter.publication_block_bounds.as_ref(),
            );
            bind(connection, builder).await
        }
    }
}

async fn bind(
    connection: &mut PgConnection,
    mut builder: QueryBuilder<'_, Postgres>,
) -> Result<Bound> {
    let sql = builder.sql().to_owned();
    let mut built = builder.build();
    let arguments = built
        .take_arguments()
        .map_err(anyhow::Error::from_boxed)?
        .unwrap_or_default();
    let count = arguments.len();
    if count == 0 {
        return Ok(Bound {
            sql,
            literals: String::new(),
        });
    }
    // EXECUTE takes no protocol parameters, so PostgreSQL renders each bound value as a quoted
    // literal for it.
    let placeholders = (1..=count)
        .map(|index| format!("quote_nullable(${index})"))
        .collect::<Vec<_>>()
        .join(", ");
    let literals: Vec<String> =
        sqlx::query_scalar_with(&format!("SELECT ARRAY[{placeholders}]"), arguments)
            .persistent(false)
            .fetch_one(&mut *connection)
            .await?;
    Ok(Bound {
        sql,
        literals: format!("({})", literals.join(", ")),
    })
}

async fn prepare(connection: &mut PgConnection, statement: &str, sql: &str) -> Result<()> {
    raw_sql(&format!("PREPARE {statement} AS {sql}"))
        .execute(&mut *connection)
        .await
        .with_context(|| format!("prepare {statement}"))?;
    Ok(())
}

async fn plan_counts(connection: &mut PgConnection, statement: &str) -> Result<(i64, i64)> {
    // Not cached: `DEALLOCATE ALL` would drop it.
    Ok(sqlx::query_as(
        "SELECT generic_plans, custom_plans FROM pg_prepared_statements WHERE name = $1",
    )
    .bind(statement)
    .persistent(false)
    .fetch_one(&mut *connection)
    .await?)
}

/// `EXPLAIN (FORMAT JSON)` of one execution in `mode`, checked against the plan counters.
async fn explain(
    connection: &mut PgConnection,
    mode: &str,
    statement: &str,
    literals: &str,
    analyze: bool,
) -> Result<Value> {
    raw_sql(&format!("SET plan_cache_mode = {mode}"))
        .execute(&mut *connection)
        .await?;
    let (generic, custom) = plan_counts(connection, statement).await?;
    let options = if analyze {
        "ANALYZE, FORMAT JSON"
    } else {
        "FORMAT JSON"
    };
    let plan: Value = sqlx::query_scalar(&format!(
        "EXPLAIN ({options}) EXECUTE {statement}{literals}"
    ))
    .persistent(false)
    .fetch_one(&mut *connection)
    .await
    .with_context(|| format!("explain {statement} under {mode}"))?;
    let expected = if mode == "force_generic_plan" {
        (generic + 1, custom)
    } else {
        (generic, custom + 1)
    };
    let counts = plan_counts(connection, statement).await?;
    ensure!(
        counts == expected,
        "{statement} under {mode}: plan counts {counts:?}, expected {expected:?}"
    );
    Ok(plan)
}

async fn execute(
    connection: &mut PgConnection,
    mode: &str,
    statement: &str,
    literals: &str,
) -> Result<Vec<PgRow>> {
    raw_sql(&format!("SET plan_cache_mode = {mode}"))
        .execute(&mut *connection)
        .await?;
    sqlx::query(&format!("EXECUTE {statement}{literals}"))
        .persistent(false)
        .fetch_all(&mut *connection)
        .await
        .with_context(|| format!("execute {statement} under {mode}"))
}

async fn output(
    connection: &mut PgConnection,
    mode: &str,
    name: &str,
    literals: &str,
    statement: &Statement,
) -> Result<Output> {
    let rows = execute(connection, mode, name, literals).await?;
    Ok(match statement {
        Statement::Count => Output::Count(rows[0].try_get(0)?),
        Statement::Page { page_size, .. } => {
            let rows = rows
                .into_iter()
                .map(decode_history_event)
                .collect::<Result<Vec<_>>>()?;
            let (rows, next_cursor) =
                split_keyset_page(rows, usize::try_from(*page_size)?, history_cursor_from_row);
            Output::Page { rows, next_cursor }
        }
        Statement::Keyset(_) => Output::AnchorBlock(
            rows.first()
                .map(|row| row.try_get::<Option<i64>, _>(0))
                .transpose()?,
        ),
        Statement::IsPublic(_) => Output::IsPublic(rows[0].try_get(0)?),
    })
}

fn prefixed(cell: &str, failures: Vec<String>) -> Vec<String> {
    failures
        .into_iter()
        .map(|failure| format!("{cell}: {failure}"))
        .collect()
}

fn root(plan: &Value) -> &Value {
    &plan[0]["Plan"]
}

fn inputs(node: &Value) -> impl Iterator<Item = &Value> {
    node["Plans"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|child| {
            matches!(
                child["Parent Relationship"].as_str(),
                Some("Outer" | "Inner")
            )
        })
}

fn all_nodes(plan: &Value) -> Vec<&Value> {
    fn walk<'a>(node: &'a Value, nodes: &mut Vec<&'a Value>) {
        nodes.push(node);
        for child in node["Plans"].as_array().into_iter().flatten() {
            walk(child, nodes);
        }
    }
    let mut nodes = Vec::new();
    walk(root(plan), &mut nodes);
    nodes
}

/// The statement's own lineage join is aliased `rb`. EXPLAIN numbers the copies nested in its
/// subqueries `rb_1`, `rb_2`, and so on.
fn is_statement_lineage(node: &Value) -> bool {
    node["Relation Name"] == "chain_lineage"
        && node["Alias"].as_str().is_some_and(|alias| {
            alias == "rb"
                || alias
                    .strip_prefix("rb_")
                    .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
        })
}

fn lineage_scans(plan: &Value) -> Vec<&Value> {
    all_nodes(plan)
        .into_iter()
        .filter(|node| is_statement_lineage(node))
        .collect()
}

/// The lineage scan a join input reads, through a memoize, hash or materialize node.
fn lineage_input(node: &Value) -> Option<&Value> {
    if node.get("Relation Name").is_some() {
        return is_statement_lineage(node).then_some(node);
    }
    match node["Node Type"].as_str() {
        Some(
            "Memoize" | "Hash" | "Materialize" | "Gather" | "Gather Merge" | "Sort"
            | "Incremental Sort",
        ) => {
            let mut children = inputs(node);
            let only = children.next()?;
            if children.next().is_some() {
                return None;
            }
            lineage_input(only)
        }
        _ => None,
    }
}

/// Every join of a statement lineage alias: the join, the lineage scan, and which input it is.
fn lineage_joins(plan: &Value) -> Vec<(&Value, &Value, &str)> {
    all_nodes(plan)
        .into_iter()
        .filter(|node| {
            matches!(
                node["Node Type"].as_str(),
                Some("Nested Loop" | "Hash Join" | "Merge Join")
            )
        })
        .flat_map(|join| {
            inputs(join).filter_map(move |input| {
                lineage_input(input).map(|scan| {
                    (
                        join,
                        scan,
                        input["Parent Relationship"].as_str().unwrap_or_default(),
                    )
                })
            })
        })
        .collect()
}

/// S: no lineage join filters on a subquery, and the statement's own join exists.
fn subquery_join_failures(plan: &Value) -> Vec<String> {
    let joins = lineage_joins(plan);
    let mut failures = Vec::new();
    if !joins.iter().any(|(_, scan, _)| scan["Alias"] == "rb") {
        failures.push(format!(
            "no join of the statement's lineage\n{}",
            outline(plan)
        ));
    }
    for (join, scan, _) in joins {
        for key in ["Filter", "Join Filter"] {
            if join[key]
                .as_str()
                .is_some_and(|text| text.contains("SubPlan"))
            {
                failures.push(format!(
                    "the {} of {} carries a subquery: {}",
                    scan["Alias"],
                    join["Node Type"],
                    join[key].as_str().unwrap_or_default()
                ));
            }
        }
    }
    failures
}

/// Whether a join of a statement lineage alias filters on a subquery.
fn subquery_on_lineage_join(plan: &Value) -> bool {
    lineage_joins(plan).iter().any(|(join, _, _)| {
        ["Filter", "Join Filter"].iter().any(|key| {
            join[*key]
                .as_str()
                .is_some_and(|text| text.contains("SubPlan"))
        })
    })
}

/// Whether the statement's lineage is read by a scan of the table rather than a key probe.
fn lineage_scanned(plan: &Value) -> bool {
    all_nodes(plan).into_iter().any(|node| {
        node["Relation Name"] == "chain_lineage"
            && node["Alias"] == "rb"
            && matches!(
                node["Node Type"].as_str(),
                Some("Seq Scan" | "Bitmap Heap Scan")
            )
    })
}

/// K: the statement's lineage is read by probes keyed on `(chain_id, block_hash)` from the inner
/// side of a nested loop, never scanned or hashed. Both the primary key and the unique key that
/// adds `block_number` lead with those columns, and the planner picks either.
fn keyed_failures(plan: &Value) -> Vec<String> {
    let mut failures = Vec::new();
    let probed = lineage_joins(plan).iter().any(|(join, scan, side)| {
        scan["Alias"] == "rb"
            && join["Node Type"] == "Nested Loop"
            && *side == "Inner"
            && scan["Node Type"] == "Index Scan"
            && scan["Index Name"]
                .as_str()
                .is_some_and(|index| KEYED_LINEAGE_INDEXES.contains(&index))
            && scan["Index Cond"]
                .as_str()
                .is_some_and(|condition| condition.contains("block_hash"))
    });
    if !probed {
        failures.push("rb is not probed by its key from a nested loop".to_owned());
    }
    for node in all_nodes(plan) {
        if node["Relation Name"] == "chain_lineage"
            && node["Alias"] == "rb"
            && node["Node Type"] != "Index Scan"
        {
            failures.push(format!("rb is read by a {}", node["Node Type"]));
        }
        if node["Node Type"] == "Hash"
            && inputs(node)
                .filter_map(lineage_input)
                .any(|scan| scan["Alias"] == "rb")
        {
            failures.push("rb is hashed".to_owned());
        }
    }
    if failures.is_empty() {
        return failures;
    }
    vec![format!("{}\n{}", failures.join("; "), outline(plan))]
}

/// G: no parallel plan.
fn gather_failures(plan: &Value) -> Vec<String> {
    all_nodes(plan)
        .into_iter()
        .filter_map(|node| node["Node Type"].as_str())
        .filter(|node_type| node_type.starts_with("Gather"))
        .map(|node_type| format!("a {node_type} node\n{}", outline(plan)))
        .collect()
}

/// P3: the statement reads no more lineage rows than its candidate rows.
fn lineage_read_failures(plan: &Value) -> Vec<String> {
    let rows = |node: &Value| {
        node["Actual Rows"].as_f64().unwrap_or_default()
            * node["Actual Loops"].as_f64().unwrap_or_default()
    };
    lineage_joins(plan)
        .into_iter()
        .filter(|(_, scan, _)| scan["Alias"] == "rb")
        .filter_map(|(join, scan, side)| {
            let candidates = inputs(join)
                .find(|input| input["Parent Relationship"].as_str() != Some(side))
                .map(rows)
                .unwrap_or_default();
            let read = rows(scan)
                + scan["Rows Removed by Filter"].as_f64().unwrap_or_default()
                    * scan["Actual Loops"].as_f64().unwrap_or_default();
            (read > candidates).then(|| {
                format!(
                    "read {read} lineage rows for {candidates} candidate rows\n{}",
                    outline(plan)
                )
            })
        })
        .collect()
}

/// P8: the page is sorted by the history order.
fn order_failures(plan: &Value, order: HistoryOrder) -> Vec<String> {
    fn first_sort_keys(node: &Value) -> Option<Vec<String>> {
        if let Some(keys) = node["Sort Key"].as_array() {
            return Some(
                keys.iter()
                    .map(|key| key.as_str().unwrap_or_default().to_owned())
                    .collect(),
            );
        }
        inputs(node).find_map(first_sort_keys)
    }
    let Some(keys) = first_sort_keys(root(plan)) else {
        return vec![format!("no sort keys\n{}", outline(plan))];
    };
    let first = keys.first().map(String::as_str).unwrap_or_default();
    let descending = first.contains("DESC");
    let in_order = first.contains("block_number")
        && descending == (order == HistoryOrder::Desc)
        && keys
            .last()
            .is_some_and(|key| key.contains("event_identity"));
    if in_order {
        Vec::new()
    } else {
        vec![format!("sort keys {keys:?} do not follow {order:?}")]
    }
}

/// The plan's node types, one per line, with each scan's alias and index.
fn outline(plan: &Value) -> String {
    fn walk(node: &Value, depth: usize, lines: &mut Vec<String>) {
        let mut line = format!(
            "{}{}",
            "  ".repeat(depth),
            node["Node Type"].as_str().unwrap_or("?")
        );
        if let Some(relationship) = node["Parent Relationship"].as_str() {
            line.push_str(&format!(" [{relationship}]"));
        }
        if let Some(name) = node["Subplan Name"].as_str() {
            line.push_str(&format!(" {name}"));
        }
        if let Some(relation) = node["Relation Name"].as_str() {
            line.push_str(&format!(
                " on {relation} {}",
                node["Alias"].as_str().unwrap_or_default()
            ));
        }
        if let Some(index) = node["Index Name"].as_str() {
            line.push_str(&format!(" using {index}"));
        }
        for key in ["Filter", "Join Filter"] {
            if node[key]
                .as_str()
                .is_some_and(|text| text.contains("SubPlan"))
            {
                line.push_str(&format!(" ({key} has a subquery)"));
            }
        }
        lines.push(line);
        for child in node["Plans"].as_array().into_iter().flatten() {
            walk(child, depth + 1, lines);
        }
    }
    let mut lines = Vec::new();
    walk(root(plan), 2, &mut lines);
    lines.join("\n")
}

async fn with_fixture(
    prefix: &str,
    noise: bool,
    check: impl AsyncFnOnce(&mut PgConnection) -> Result<()>,
) -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new(prefix).pool_max_connections(1)).await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        install_schema(&mut connection).await?;
        raw_sql(FIXTURE_SQL)
            .execute(&mut *connection)
            .await
            .context("install the registration fixture")?;
        if noise {
            raw_sql(NOISE_SQL)
                .execute(&mut *connection)
                .await
                .context("install the unrelated rows")?;
        }
        raw_sql(
            "ANALYZE normalized_events, chain_lineage, surface_bindings, resources, name_surfaces",
        )
        .execute(&mut *connection)
        .await?;
        check(&mut connection).await
    }
    .await;
    database.cleanup().await?;
    result
}

async fn install_schema(connection: &mut PgConnection) -> Result<()> {
    raw_sql("CREATE SCHEMA bigname_phase; SET search_path TO bigname_phase, public")
        .execute(&mut *connection)
        .await?;
    for baseline in [
        include_str!("../../schema/baseline/01_chain.sql"),
        include_str!("../../schema/baseline/02_raw_facts.sql"),
        include_str!("../../schema/baseline/03_identity.sql"),
        include_str!("../../schema/baseline/04_manifests.sql"),
        include_str!("../../schema/baseline/05_normalized_events.sql"),
    ] {
        raw_sql(baseline).execute(&mut *connection).await?;
    }
    raw_sql("SET jit = off").execute(&mut *connection).await?;
    Ok(())
}

/// The canonical block's timestamp at the event's height, for the third altered probe. It
/// differs from the event's own block only on a fork.
const CANONICAL_HEIGHT_TIMESTAMP: &str = "(SELECT height_block.block_timestamp
    FROM bigname_phase.chain_lineage height_block
    WHERE height_block.chain_id = ne.chain_id
      AND height_block.block_number = ne.block_number
      AND height_block.canonicality_state IN ('canonical', 'safe', 'finalized'))";

/// Lineage rows on the fixture chain far above every event, so a hash of the chain's lineage
/// costs more than probing it once per candidate row.
const INFLATE_LINEAGE_SQL: &str = r#"
INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
SELECT 'ethereum-mainnet', 'pad:' || n, 100000 + n, pg_temp.ts(100000 + n), 'canonical'
FROM generate_series(1, 200000) n;
ANALYZE chain_lineage;
"#;

/// Blocks 1..=2000 on one chain, each the parent of the next, and an orphaned sibling of block
/// 200 one second later. Name k has namehash k as 32 bytes of hex and resource k is uuid k.
///
/// The name's bindings: the lease from block 100, its NameWrapper resource from five microseconds
/// into block 130, and the next lease from five microseconds into block 200. Resource-less rows of
/// the name belong to whichever lease a binding active at the row's block timestamp, plus its log
/// index in microseconds, witnesses. Registry rows on the lease belong to it only while its own
/// binding is active. The fixture has no foreign key from events to lineage, so one row can sit
/// on a block with no lineage row. Autovacuum is off so the statistics are only the ones the test
/// collects.
const FIXTURE_SQL: &str = r#"
ALTER TABLE normalized_events SET (autovacuum_enabled = false);
ALTER TABLE chain_lineage SET (autovacuum_enabled = false);
ALTER TABLE surface_bindings SET (autovacuum_enabled = false);
DO $$
DECLARE constraint_name text;
BEGIN
    FOR constraint_name IN
        SELECT conname FROM pg_constraint
        WHERE conrelid = 'normalized_events'::regclass
          AND confrelid = 'chain_lineage'::regclass
    LOOP
        EXECUTE format('ALTER TABLE normalized_events DROP CONSTRAINT %I', constraint_name);
    END LOOP;
END
$$;
CREATE FUNCTION pg_temp.node(k bigint) RETURNS text LANGUAGE sql IMMUTABLE
    AS $$ SELECT '0x' || lpad(to_hex(k), 64, '0') $$;
CREATE FUNCTION pg_temp.name(k bigint) RETURNS text LANGUAGE sql IMMUTABLE
    AS $$ SELECT 'ens:' || pg_temp.node(k) $$;
CREATE FUNCTION pg_temp.rid(k bigint) RETURNS uuid LANGUAGE sql IMMUTABLE
    AS $$ SELECT lpad(to_hex(k), 32, '0')::uuid $$;
CREATE FUNCTION pg_temp.ts(block bigint) RETURNS timestamptz LANGUAGE sql IMMUTABLE
    AS $$ SELECT to_timestamp(1700000000 + 12 * block) $$;
CREATE FUNCTION pg_temp.ev(
    identity text, block bigint, family text, kind text, resource uuid, name text,
    state jsonb DEFAULT '{}', log bigint DEFAULT 0, canon canonicality_state DEFAULT 'canonical',
    hash text DEFAULT NULL
) RETURNS void LANGUAGE sql AS $$
    INSERT INTO normalized_events (event_identity, namespace, logical_name_id, resource_id,
        event_kind, source_family, manifest_version, chain_id, block_number, block_hash,
        transaction_hash, transaction_index, log_index, derivation_kind, canonicality_state,
        after_state)
    VALUES (identity, 'ens', name, resource, kind, family, 1, 'ethereum-mainnet', block,
        CASE WHEN block IS NOT NULL THEN COALESCE(hash, 'b:' || block) END,
        CASE WHEN block IS NOT NULL THEN 'tx:' || identity END,
        CASE WHEN block IS NOT NULL THEN 0 END,
        CASE WHEN block IS NOT NULL THEN log END,
        'ens_v1_unwrapped_authority', canon, state);
$$;
CREATE FUNCTION pg_temp.bind(binding bigint, resource bigint, block bigint,
    active_from timestamptz, active_to timestamptz) RETURNS void LANGUAGE sql AS $$
    INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id, binding_kind,
        authority_arm, active_from, active_to, chain_id, block_hash, block_number,
        canonicality_state)
    VALUES (pg_temp.rid(binding), pg_temp.name(161), pg_temp.rid(resource),
        'declared_registry_path', 'ens_v1', active_from, active_to, 'ethereum-mainnet',
        'b:' || block, block, 'canonical');
$$;

INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number, block_timestamp,
    canonicality_state)
SELECT 'ethereum-mainnet', 'b:' || n, CASE WHEN n > 1 THEN 'b:' || (n - 1) END, n,
    pg_temp.ts(n), 'canonical'
FROM generate_series(1, 2000) n;
INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number, block_timestamp,
    canonicality_state)
VALUES ('ethereum-mainnet', 'orphan:200', 'b:199', 200, pg_temp.ts(200) + interval '1 second',
    'orphaned');

INSERT INTO name_surfaces (logical_name_id, namespace, namehash, labelhashes, normalizer_version,
    visibility_state, chain_id, block_hash, block_number, canonicality_state)
VALUES (pg_temp.name(161), 'ens', pg_temp.node(161), ARRAY[pg_temp.node(161)], 'fixture',
    'active', 'ethereum-mainnet', 'b:1', 1, 'canonical');
INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state)
SELECT pg_temp.rid(k), 'ethereum-mainnet', 'b:1', 1, 'canonical'
FROM unnest(ARRAY[2576, 2577, 2578, 2579, 2592, 2608, 2624]) k;

-- The lease (0xa10), its NameWrapper resource (0xa12) and the next lease (0xa11).
SELECT pg_temp.bind(177, 2576, 100, pg_temp.ts(100),
    pg_temp.ts(130) + interval '5 microseconds');
SELECT pg_temp.bind(178, 2578, 130, pg_temp.ts(130) + interval '5 microseconds',
    pg_temp.ts(200) + interval '5 microseconds');
SELECT pg_temp.bind(179, 2577, 190, pg_temp.ts(200) + interval '5 microseconds', NULL);
SELECT pg_temp.ev('story:lease:grant', 100, 'ens_v1_registrar_l1', 'RegistrationGranted',
    pg_temp.rid(2576), pg_temp.name(161), jsonb_build_object('namehash', pg_temp.node(161)));
SELECT pg_temp.ev('story:lease:renew', 160, 'ens_v1_registrar_l1', 'RegistrationRenewed',
    pg_temp.rid(2576), pg_temp.name(161), jsonb_build_object('namehash', pg_temp.node(161)));
SELECT pg_temp.ev('story:wrapper:bound', 130, 'ens_v1_wrapper_l1', 'SurfaceBound',
    pg_temp.rid(2578), pg_temp.name(161),
    jsonb_build_object('node', pg_temp.node(161),
        'wrapped_registrar_resource_id', pg_temp.rid(2576)), 5);
SELECT pg_temp.ev('story:wrapper:transfer', 131, 'ens_v1_wrapper_l1',
    'TokenControlTransferred', pg_temp.rid(2578), pg_temp.name(161));
SELECT pg_temp.ev('story:next-lease:grant', 190, 'ens_v1_registrar_l1', 'RegistrationGranted',
    pg_temp.rid(2577), pg_temp.name(161), jsonb_build_object('namehash', pg_temp.node(161)));
SELECT pg_temp.ev('story:migration', 300, 'ens_v2_migration_l1', 'MigrationApplied',
    pg_temp.rid(2579), pg_temp.name(161),
    jsonb_build_object('successor_binding', jsonb_build_object('resource_id', pg_temp.rid(2576))));

-- Resource-less rows of the name, decided by the binding active at their block.
SELECT pg_temp.ev('story:name:' || label, block, 'ens_v1_resolver_l1', kind, NULL,
    pg_temp.name(161), '{}', log, canon, hash)
FROM (VALUES
    ('50:0', 50, 0, 'RecordChanged', 'canonical'::canonicality_state, NULL),
    ('130:1', 130, 1, 'RecordChanged', 'canonical', NULL),
    ('130:6', 130, 6, 'RecordChanged', 'canonical', NULL),
    ('150:0', 150, 0, 'RecordChanged', 'canonical', NULL),
    ('160:1', 160, 1, 'PermissionChanged', 'canonical', NULL),
    ('200:3', 200, 3, 'RecordChanged', 'canonical', NULL),
    ('200:7', 200, 7, 'RecordChanged', 'canonical', NULL),
    ('250:0', 250, 0, 'RecordChanged', 'canonical', NULL),
    ('null', NULL, NULL, 'RecordChanged', 'canonical', NULL),
    ('orphan:0', 200, 0, 'RecordChanged', 'canonical', 'orphan:200'),
    ('orphan:1', 200, 1, 'RecordChanged', 'orphaned', 'orphan:200'),
    ('missing', 150, 2, 'RecordChanged', 'canonical', 'missing:150')
) row(label, block, log, kind, canon, hash);

-- Registry rows on the lease, decided by the lease's own binding.
SELECT pg_temp.ev('story:registry:' || label, block, 'ens_v1_registry_l1', 'ResolverChanged',
    pg_temp.rid(2576), pg_temp.name(161), jsonb_build_object('node', pg_temp.node(161)), log,
    hash => hash)
FROM (VALUES
    ('110:0', 110, 0, NULL),
    ('130:2', 130, 2, NULL),
    ('150:0', 150, 0, NULL),
    ('null', NULL, NULL, NULL),
    ('missing', 150, 3, 'missing:150')
) row(label, block, log, hash);

-- Node-keyed record writes, attributed by the test to the lease, the wrapper or the next lease.
SELECT pg_temp.ev('story:attr:' || n, 139 + n, 'ens_v1_resolver_l1', 'RecordChanged', NULL, NULL)
FROM generate_series(1, 10) n;
SELECT pg_temp.ev('story:attr:null', NULL, 'ens_v1_resolver_l1', 'RecordChanged', NULL, NULL,
    log => NULL);

-- A lease with no name (0xa20), and its 30 attributed record writes.
SELECT pg_temp.ev('nameless:grant', 400, 'ens_v1_registrar_l1', 'RegistrationGranted',
    pg_temp.rid(2592), NULL, jsonb_build_object('namehash', pg_temp.node(187)));
SELECT pg_temp.ev('nameless:renew', 410, 'ens_v1_registrar_l1', 'RegistrationRenewed',
    pg_temp.rid(2592), NULL, jsonb_build_object('namehash', pg_temp.node(187)));
SELECT pg_temp.ev('nameless:attr:' || n, 419 + n, 'ens_v1_resolver_l1', 'RecordChanged', NULL,
    NULL)
FROM generate_series(1, 30) n;

-- A registry resource with no lease (0xa30): not a registration.
SELECT pg_temp.ev('not-a-registration:resolver', 500, 'ens_v1_registry_l1', 'ResolverChanged',
    pg_temp.rid(2608), NULL, jsonb_build_object('node', pg_temp.node(204)));

-- A hot lease (0xa40) with 500 rows.
SELECT pg_temp.ev('hot:grant', 600, 'ens_v1_registrar_l1', 'RegistrationGranted',
    pg_temp.rid(2624), NULL, jsonb_build_object('namehash', pg_temp.node(221)));
SELECT pg_temp.ev('hot:renew:' || n, 600 + n, 'ens_v1_registrar_l1', 'RegistrationRenewed',
    pg_temp.rid(2624), NULL, jsonb_build_object('namehash', pg_temp.node(221)))
FROM generate_series(1, 499) n;
"#;

/// Unrelated names and resources for the planner's estimates: 400 names with one binding each,
/// 200 resources with 100 rows each.
const NOISE_SQL: &str = r#"
INSERT INTO name_surfaces (logical_name_id, namespace, namehash, labelhashes, normalizer_version,
    visibility_state, chain_id, block_hash, block_number, canonicality_state)
SELECT pg_temp.name(1048576 + n), 'ens', pg_temp.node(1048576 + n),
    ARRAY[pg_temp.node(1048576 + n)], 'fixture', 'active', 'ethereum-mainnet', 'b:1', 1,
    'canonical'
FROM generate_series(0, 399) n;
INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state)
SELECT pg_temp.rid(16777216 + n), 'ethereum-mainnet', 'b:1', 1, 'canonical'
FROM generate_series(0, 199) n;
INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id, binding_kind,
    authority_arm, active_from, active_to, chain_id, block_hash, block_number, canonicality_state)
SELECT pg_temp.rid(33554432 + n), pg_temp.name(1048576 + n), pg_temp.rid(16777216 + n % 200),
    'declared_registry_path', 'ens_v1', pg_temp.ts(1), NULL, 'ethereum-mainnet', 'b:1', 1,
    'canonical'
FROM generate_series(0, 399) n;
INSERT INTO normalized_events (event_identity, namespace, logical_name_id, resource_id,
    event_kind, source_family, manifest_version, chain_id, block_number, block_hash,
    transaction_hash, transaction_index, log_index, derivation_kind, canonicality_state,
    after_state)
SELECT 'noise:' || n, 'ens', pg_temp.name(1048576 + n % 400), pg_temp.rid(16777216 + n % 200),
    CASE n % 4 WHEN 0 THEN 'RegistrationGranted' WHEN 1 THEN 'ResolverChanged'
        WHEN 2 THEN 'RecordChanged' ELSE 'TokenControlTransferred' END,
    CASE n % 4 WHEN 1 THEN 'ens_v1_registry_l1' WHEN 2 THEN 'ens_v1_resolver_l1'
        ELSE 'ens_v1_registrar_l1' END,
    1, 'ethereum-mainnet', 1 + n % 2000, 'b:' || (1 + n % 2000), 'tx:noise:' || n, 0, n / 2000,
    'ens_v1_unwrapped_authority', 'canonical',
    jsonb_build_object('namehash', pg_temp.node(1048576 + n % 400))
FROM generate_series(0, 19999) n;
"#;
