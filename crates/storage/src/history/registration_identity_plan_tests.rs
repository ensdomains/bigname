//! Plan and row tests for how a registration read reaches `chain_lineage`: the history source's
//! own lineage row `rb`, the binding active at a resource-less event
//! (`push_registration_binding_at_event`), the binding covering a registry-only resolver write
//! (the `binding` lookup of the registration identity), and the identity's NameWrapper lookup.
//!
//! The two binding lookups used to read the event's block timestamp from `rb`. A predicate that
//! references `rb` can only run on that join. PostgreSQL charges it per joined row in a nested
//! loop and almost never in a hash join, so a registration read hashed every `chain_lineage` row
//! of the chain to join a handful of candidate rows. Each lookup now probes the event's lineage
//! row by key. `rb` itself is a correlated `LIMIT 1` lookup, so no planner state can hash or walk
//! the lineage for it: the plan test also plans each statement with nested loops disabled and
//! still expects the probe. The NameWrapper lookup must stay keyed by resource. The fixture's few
//! SurfaceBound rows are what let a plain scan key it by event kind.
//!
//! The fixture needs enough lineage and enough events that the previous statements' hash of the
//! lineage is the cheaper plan on paper. The plan test checks that it still is in every cell, so
//! the fixture keeps proving something. The row test pins each input class to its rows and to the
//! previous statements' rows.

use std::collections::BTreeMap;

use anyhow::{Context, Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::Value;
use sqlx::{Execute, PgConnection, Postgres, QueryBuilder, Row, postgres::PgArguments, raw_sql};
use uuid::Uuid;

use super::super::{
    ChainBlockRange, EventHistoryReadFilter, HistoryBlockWindow, HistoryCursor, HistoryOrder,
    HistoryPosition, keyset::HistoryKeyset, paging::push_history_page_query,
    selectors::HistorySelector, summary::push_history_count_query,
};
use super::push_public_registration_id_query;

const MAINNET: &str = "ethereum-mainnet";
const PUBLISHED: i64 = 1_000_000;
const COUNT_LIMIT: i64 = 10_001;
const PAGE_LIMIT: i64 = 201;

/// The fixture's names, by the number their namehash encodes: N carries the case rows, H a
/// registration with 2,000 permission changes.
const N: u32 = 0xa1;
const H: u32 = 0xb1;
/// Resources, by the number their uuid encodes. L1, L2 and L3 are leases of N, X a registry-only
/// resource of N, LH the lease of H.
const L1: u128 = 0xa10;
const L2: u128 = 0xa11;
const L3: u128 = 0xa12;
const X: u128 = 0xa13;
const LH: u128 = 0xb10;
const ADDRESS: &str = "0x00000000000000000000000000000000000000aa";

/// The product event types and the event kinds each one reads.
const EVENT_TYPES: [(&str, &[&str]); 15] = [
    ("registration", &["RegistrationGranted", "LabelRegistered"]),
    ("renewal", &["RegistrationRenewed"]),
    ("release", &["RegistrationReleased"]),
    ("expiry", &["ExpiryChanged"]),
    ("transfer", &["TokenControlTransferred"]),
    (
        "authority",
        &["AuthorityTransferred", "AuthorityEpochChanged"],
    ),
    ("resolver", &["ResolverChanged", "ResolverRecordLinked"]),
    ("record", &["RecordChanged", "RecordVersionChanged"]),
    ("primary_name", &["ReverseChanged"]),
    (
        "permission",
        &[
            "PermissionChanged",
            "RootPermissionChanged",
            "PermissionScopeChanged",
            "RolesChanged",
            "EACRolesChanged",
            "AccountPermissionChanged",
        ],
    ),
    ("subregistry", &["SubregistryChanged"]),
    ("migration", &["MigrationApplied"]),
    ("reservation", &["RegistrationReserved"]),
    ("token", &["TokenRegenerated"]),
    (
        "contract",
        &["RegistryCreated", "ParentChanged", "Upgraded"],
    ),
];

/// Every row of N's registration L1 in page order.
const L1_ROWS: [&str; 10] = [
    "l1:grant",
    "n:perm:10:0",
    "n:perm:20:0",
    "x:authority",
    "l1:renew",
    "x:resolver:30:1",
    "n:record:40:0",
    "n:perm:50:none",
    "n:scope:50:0",
    "n:scope:50:1",
];
const L2_ROWS: [&str; 5] = [
    "l2:grant",
    "n:perm:50:2",
    "n:scope:50:3",
    "n:perm:60:0",
    "x:resolver:70:0",
];

/// The registration identity each row of the fixture carries. Every other row carries none.
const REGISTRATION_IDS: [(&str, u128); 8] = [
    ("l1:grant", L1),
    ("l1:renew", L1),
    ("x:authority", L1),
    ("x:resolver:30:1", L1),
    ("l2:grant", L2),
    ("x:resolver:70:0", L2),
    ("l3:grant", L3),
    ("h:grant", LH),
];

/// The lookups' text before this change and now. The previous statement is the current one with
/// each text now put back as it was, so the two share every other byte.
const LOOKUPS: [(&str, &str); 6] = [
    (A_SOURCE_NOW, A_SOURCE_BEFORE),
    (A_BOUNDS_NOW, A_BOUNDS_BEFORE),
    (B_SOURCE_NOW, B_SOURCE_BEFORE),
    (B_BOUNDS_NOW, B_BOUNDS_BEFORE),
    (LINEAGE_NOW, LINEAGE_BEFORE),
    (WRAPPER_NOW, WRAPPER_BEFORE),
];
const LINEAGE_BEFORE: &str = r#"        LEFT JOIN bigname_phase.chain_lineage rb
          ON rb.chain_id = ne.chain_id
         AND rb.block_hash = ne.block_hash
"#;
const LINEAGE_NOW: &str = r#"        LEFT JOIN LATERAL (
            SELECT rb.block_timestamp, rb.canonicality_state
            FROM bigname_phase.chain_lineage rb
            WHERE rb.chain_id = ne.chain_id
              AND rb.block_hash = ne.block_hash
            LIMIT 1
        ) rb ON TRUE
"#;
const WRAPPER_BEFORE: &str = r#"                    FROM bigname_phase.normalized_events wrapper_binding
                    LEFT JOIN bigname_phase.chain_lineage wrapper_lineage
"#;
const WRAPPER_NOW: &str = r#"                    FROM (
                        SELECT * FROM bigname_phase.normalized_events wrapper_binding
                        WHERE wrapper_binding.resource_id = ne.resource_id
                          AND wrapper_binding.canonicality_state IN (
                              'canonical'::bigname_phase.canonicality_state,
                              'safe'::bigname_phase.canonicality_state,
                              'finalized'::bigname_phase.canonicality_state
                          )
                        OFFSET 0
                    ) wrapper_binding
                    LEFT JOIN bigname_phase.chain_lineage wrapper_lineage
"#;
const A_SOURCE_BEFORE: &str = r#"            FROM (SELECT ne.chain_id, ne.block_hash, ne.block_number, ne.log_index,
                         ne.logical_name_id, ne.namespace, ne.after_state) history_event
"#;
const A_SOURCE_NOW: &str = r#"            FROM (SELECT ne.chain_id, ne.block_hash, ne.block_number, ne.log_index,
                     ne.logical_name_id, ne.namespace, ne.after_state,
                     event_lineage.block_timestamp
                    + GREATEST(COALESCE(ne.log_index, 0), 0) * interval '1 microsecond'
                    AS event_time
              FROM bigname_phase.chain_lineage event_lineage
              WHERE event_lineage.chain_id = ne.chain_id
                AND event_lineage.block_hash = ne.block_hash) history_event
"#;
const A_BOUNDS_BEFORE: &str = r#"              AND history_binding.active_from <= rb.block_timestamp
                  + GREATEST(COALESCE(ne.log_index, 0), 0) * interval '1 microsecond'
              AND (history_binding.active_to IS NULL
                   OR history_binding.active_to > rb.block_timestamp
                       + GREATEST(COALESCE(ne.log_index, 0), 0)
                         * interval '1 microsecond')"#;
const A_BOUNDS_NOW: &str = r#"              AND history_binding.active_from <= history_event.event_time
              AND (history_binding.active_to IS NULL
                   OR history_binding.active_to > history_event.event_time)"#;
const B_SOURCE_BEFORE: &str = r#"                                SELECT 1
                                FROM bigname_phase.surface_bindings binding
"#;
const B_SOURCE_NOW: &str = r#"                                SELECT 1
                                FROM (SELECT event_lineage.block_timestamp
                    + GREATEST(COALESCE(ne.log_index, 0), 0) * interval '1 microsecond'
                    AS event_time
              FROM bigname_phase.chain_lineage event_lineage
              WHERE event_lineage.chain_id = ne.chain_id
                AND event_lineage.block_hash = ne.block_hash) binding_event
                                CROSS JOIN bigname_phase.surface_bindings binding
"#;
const B_BOUNDS_BEFORE: &str = r#"                                  AND binding.active_from <= rb.block_timestamp
                                      + GREATEST(COALESCE(ne.log_index, 0), 0)
                                        * interval '1 microsecond'
                                  AND (
                                      binding.active_to IS NULL
                                      OR binding.active_to > rb.block_timestamp
                                          + GREATEST(COALESCE(ne.log_index, 0), 0)
                                            * interval '1 microsecond'
                                  )
"#;
const B_BOUNDS_NOW: &str = r#"                                  AND binding.active_from <= binding_event.event_time
                                  AND (
                                      binding.active_to IS NULL
                                      OR binding.active_to > binding_event.event_time
                                  )
"#;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Custom,
    Generic,
}

#[tokio::test]
async fn binding_lookups_probe_the_event_lineage_by_key() -> Result<()> {
    with_fixture("registration_lineage_plan", async |connection| {
        let mut failures = Vec::new();
        let permission = event_kinds("permission");
        let shapes = [
            ("one candidate", registration_read(N, L1, &permission)),
            ("many candidates", registration_read(H, LH, &permission)),
        ];
        for (shape, filter) in &shapes {
            let count = count_statement(filter, true)?;
            let page = page_statement(filter, None, true, false, PAGE_LIMIT)?;
            for (label, statement, timestamps) in [("count", &count, 0), ("page", &page, 1)] {
                // Only the page's `block_timestamp` column reads the lineage row's time.
                let read = statement
                    .sql
                    .replace(LINEAGE_NOW, "")
                    .matches("rb.block_timestamp")
                    .count();
                if read != timestamps {
                    failures.push(format!(
                        "{shape}, {label}: {read} reads of rb.block_timestamp"
                    ));
                }
                statement.check_previous(&format!("{shape}, {label}"), &mut failures);
                for mode in [Mode::Custom, Mode::Generic] {
                    let cell = format!("{shape}, {label}, {mode:?}");
                    let plan = explain(connection, mode, &statement.sql, &statement.arguments)
                        .await
                        .with_context(|| cell.clone())?;
                    failures.extend(lineage_failures(&cell, &plan));
                    failures.extend(wrapper_failures(&cell, &plan));
                    let before =
                        explain(connection, mode, &statement.previous, &statement.arguments)
                            .await
                            .with_context(|| format!("{cell}, previous statement"))?;
                    if !hashes_lineage(&before) {
                        failures.push(format!(
                            "{cell}: the previous statement no longer hashes chain_lineage, so \
                             this fixture proves nothing about it\n{before:#}"
                        ));
                    }
                    // With nested loops disabled a plain join to `rb` turns into a hash join.
                    // The correlated lookup has no plan but the probe.
                    let cell = format!("{cell}, nested loops off");
                    raw_sql("SET enable_nestloop = off")
                        .execute(&mut *connection)
                        .await?;
                    let plan =
                        explain(connection, mode, &statement.sql, &statement.arguments).await;
                    raw_sql("RESET enable_nestloop")
                        .execute(&mut *connection)
                        .await?;
                    let plan = plan.with_context(|| cell.clone())?;
                    failures.extend(probe_failures(&cell, &plan));
                }
            }
        }
        for registration in [L1, LH] {
            let precheck = precheck_statement(registration)?;
            precheck.check_previous(&format!("precheck of {registration:x}"), &mut failures);
            for mode in [Mode::Custom, Mode::Generic] {
                let cell = format!("precheck of {registration:x}, {mode:?}");
                let plan = explain(connection, mode, &precheck.sql, &precheck.arguments)
                    .await
                    .with_context(|| cell.clone())?;
                failures.extend(lineage_failures(&cell, &plan));
                failures.extend(wrapper_failures(&cell, &plan));
            }
        }
        ensure!(
            failures.is_empty(),
            "registration lineage plans:\n{}",
            failures.join("\n\n")
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn binding_lookups_keep_every_row() -> Result<()> {
    with_fixture("registration_lineage_rows", async |connection| {
        let mut failures = Vec::new();
        let all: &[&str] = &[];

        // Each event type of L1, L2 and L3, and every row with no type. L3's binding lies on an
        // orphaned block, so the row its window covers belongs to no registration.
        for (registration, rows) in [
            (L1, &L1_ROWS[..]),
            (L2, &L2_ROWS[..]),
            (L3, &["l3:grant"][..]),
        ] {
            for (event_type, kinds) in EVENT_TYPES {
                let expected = rows
                    .iter()
                    .copied()
                    .filter(|identity| kinds.contains(&kind_of(identity)))
                    .collect::<Vec<_>>();
                let filter = registration_read(N, registration, kinds);
                let read = format!("registration {registration:x}, type {event_type}");
                check_registration(connection, &read, &filter, &expected, &mut failures).await?;
            }
            let filter = registration_read(N, registration, all);
            let read = format!("registration {registration:x}, every type");
            check_registration(connection, &read, &filter, rows, &mut failures).await?;
        }

        // A handle that is not a public registration matches nothing.
        let mut filter = registration_read(N, X, all);
        filter.registration_id_is_public = false;
        check_registration(
            connection,
            "registry-only handle",
            &filter,
            &[],
            &mut failures,
        )
        .await?;

        // Without a block window the rows with no block reach the lookups and stay out.
        let mut filter = registration_read(N, L1, all);
        filter.block_window = None;
        check_registration(
            connection,
            "no block window",
            &filter,
            &L1_ROWS,
            &mut failures,
        )
        .await?;

        // A registration with more rows than a page.
        let filter = registration_read(H, LH, all);
        let page = page_statement(&filter, None, true, false, PAGE_LIMIT)?;
        let rows = compare_page(connection, "many rows", &page, &mut failures).await?;
        if rows.len() != PAGE_LIMIT as usize || rows.first().map(String::as_str) != Some("h:grant")
        {
            failures.push(format!("many rows: page {rows:?}"));
        }
        let count = count_statement(&filter, true)?;
        compare_count(connection, "many rows", &count, 2_001, &mut failures).await?;

        // A page boundary: the first page and the page after its last row.
        let filter = registration_read(N, L1, all);
        let first = page_statement(&filter, None, true, false, 3)?;
        let rows = compare_page(connection, "first page", &first, &mut failures).await?;
        expect_rows("first page", &rows, &L1_ROWS[..3], &mut failures);
        let cursor = HistoryCursor {
            normalized_event_id: None,
            event_identity: "n:perm:20:0".to_owned(),
            position: Some(HistoryPosition {
                block_number: Some(20),
                chain_id: Some(MAINNET.to_owned()),
                block_hash: Some("m:20".to_owned()),
                transaction_index: Some(0),
                transaction_hash: None,
                log_index: Some(0),
            }),
        };
        let keyset = HistoryKeyset {
            cursor: &cursor,
            block_number: Some(20),
        };
        let next = page_statement(&filter, Some(&keyset), true, false, 3)?;
        let rows = compare_page(connection, "next page", &next, &mut failures).await?;
        expect_rows("next page", &rows, &L1_ROWS[3..6], &mut failures);

        // A name's history and an address's history select the registration identity of every
        // row, with no registration filter.
        let filter = EventHistoryReadFilter {
            selectors: vec![HistorySelector::LogicalNamesOrResources {
                logical_name_ids: vec![logical_name(N)],
                resource_ids: [L1, L2, L3, X].map(Uuid::from_u128).to_vec(),
            }],
            block_window: None,
            ..product_read()
        };
        let page = page_statement(&filter, None, true, false, PAGE_LIMIT)?;
        let rows = compare_page(connection, "name history", &page, &mut failures).await?;
        expect_rows("name history", &rows, &NAME_ROWS, &mut failures);

        let filter = EventHistoryReadFilter {
            selectors: vec![HistorySelector::OrAccountParticipants {
                anchors: Box::new(HistorySelector::Resources(vec![Uuid::from_u128(X)])),
                address: ADDRESS.to_owned(),
                namespace: Some("ens".to_owned()),
                relations: None,
            }],
            ..product_read()
        };
        let page = page_statement(&filter, None, true, false, PAGE_LIMIT)?;
        let rows = compare_page(connection, "address history", &page, &mut failures).await?;
        expect_rows(
            "address history",
            &rows,
            &[
                "x:authority",
                "x:resolver:30:0",
                "x:resolver:30:1",
                "acct:operator",
                "x:resolver:70:0",
                "x:resolver:70:1",
            ],
            &mut failures,
        );

        // A diagnostics read also lists the candidate row.
        let filter = EventHistoryReadFilter {
            selectors: vec![HistorySelector::LogicalNamesOrResources {
                logical_name_ids: vec![logical_name(N)],
                resource_ids: vec![Uuid::from_u128(L1)],
            }],
            diagnostics: true,
            ..product_read()
        };
        let page = page_statement(&filter, None, true, true, PAGE_LIMIT)?;
        let rows = compare_page(connection, "diagnostics", &page, &mut failures).await?;
        let mut expected = NAME_ROWS[2..].to_vec();
        let record = expected.iter().position(|row| *row == "n:record:40:0");
        expected.insert(
            record.context("the fixture's record write")?,
            "n:perm:candidate",
        );
        expect_rows("diagnostics", &rows, &expected, &mut failures);

        // The public-registration precheck.
        for (registration, public) in [
            (L1, true),
            (L2, true),
            (L3, true),
            (LH, true),
            (X, false),
            (0xdead, false),
        ] {
            let precheck = precheck_statement(registration)?;
            precheck.check_previous(&format!("precheck of {registration:x}"), &mut failures);
            let now = exists(connection, &precheck.sql, &precheck.arguments).await?;
            let before = exists(connection, &precheck.previous, &precheck.arguments).await?;
            if now != public || before != public {
                failures.push(format!(
                    "precheck of {registration:x}: {now}, previous {before}, expected {public}"
                ));
            }
        }

        // Without `canonical_only`, which no reader passes, the lookups also prove that the
        // binding and the event lie on one fork.
        for registration in [L1, L2, L3] {
            let filter = registration_read(N, registration, all);
            let read = format!("registration {registration:x}, every fork");
            let page = page_statement(&filter, None, false, false, PAGE_LIMIT)?;
            let rows = compare_page(connection, &read, &page, &mut failures).await?;
            if rows.is_empty() {
                failures.push(format!("{read}: no rows"));
            }
            let count = count_statement(&filter, false)?;
            compare_count(connection, &read, &count, rows.len() as i64, &mut failures).await?;
        }

        ensure!(
            failures.is_empty(),
            "registration lineage rows:\n{}",
            failures.join("\n\n")
        );
        Ok(())
    })
    .await
}

/// N's history in page order: rows with no block first, then by block and log index.
const NAME_ROWS: [&str; 21] = [
    "n:record:no-block",
    "x:resolver:no-block",
    "l3:grant",
    "n:perm:9:5",
    "l1:grant",
    "n:perm:10:0",
    "n:perm:20:0",
    "x:authority",
    "l1:renew",
    "x:resolver:30:0",
    "x:resolver:30:1",
    "n:record:40:0",
    "n:perm:50:none",
    "n:scope:50:0",
    "n:scope:50:1",
    "l2:grant",
    "n:perm:50:2",
    "n:scope:50:3",
    "n:perm:60:0",
    "x:resolver:70:0",
    "x:resolver:70:1",
];

/// A statement as the builders write it now and as it read before, with its arguments.
struct Statement {
    sql: String,
    previous: String,
    arguments: PgArguments,
}

impl Statement {
    fn new(mut builder: QueryBuilder<'_, Postgres>) -> Result<Self> {
        let sql = builder.sql().to_owned();
        let mut previous = sql.clone();
        for (now, before) in LOOKUPS {
            previous = previous.replace(now, before);
        }
        let arguments = builder
            .build()
            .take_arguments()
            .map_err(anyhow::Error::from_boxed)?
            .unwrap_or_default();
        Ok(Self {
            sql,
            previous,
            arguments,
        })
    }

    /// Whether every lookup was put back, so the previous statement is not the current one.
    fn check_previous(&self, read: &str, failures: &mut Vec<String>) {
        if self.previous == self.sql
            || self.previous.contains("event_lineage")
            || self.previous.contains("binding_event")
            || LOOKUPS.iter().any(|(now, _)| self.previous.contains(now))
        {
            failures.push(format!(
                "{read}: the lookups no longer read as this test expects, so the previous \
                 statement could not be rebuilt"
            ));
        }
    }
}

fn count_statement(filter: &EventHistoryReadFilter, canonical_only: bool) -> Result<Statement> {
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_history_count_query(&mut builder, filter, canonical_only, Some(COUNT_LIMIT));
    Statement::new(builder)
}

fn page_statement<'a>(
    filter: &'a EventHistoryReadFilter,
    keyset: Option<&HistoryKeyset<'a>>,
    canonical_only: bool,
    include_candidates: bool,
    limit: i64,
) -> Result<Statement> {
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_history_page_query(
        &mut builder,
        filter,
        canonical_only,
        keyset,
        include_candidates,
        limit,
    );
    Statement::new(builder)
}

fn precheck_statement(registration: u128) -> Result<Statement> {
    let published = publication_bounds();
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_public_registration_id_query(
        &mut builder,
        Uuid::from_u128(registration),
        true,
        Some(&published),
    );
    Statement::new(builder)
}

fn logical_name(k: u32) -> String {
    format!("ens:0x{k:064x}")
}

fn publication_bounds() -> BTreeMap<String, i64> {
    BTreeMap::from([(MAINNET.to_owned(), PUBLISHED)])
}

fn event_kinds(event_type: &str) -> Vec<&'static str> {
    EVENT_TYPES
        .iter()
        .find(|(name, _)| *name == event_type)
        .map(|(_, kinds)| kinds.to_vec())
        .unwrap_or_default()
}

/// The event kind of a fixture row, from its identity.
fn kind_of(identity: &str) -> &'static str {
    let mut parts = identity.split(':');
    match (parts.next(), parts.next()) {
        (_, Some("grant")) => "RegistrationGranted",
        (_, Some("renew")) => "RegistrationRenewed",
        (Some("x"), Some("authority")) => "AuthorityTransferred",
        (Some("x"), Some("resolver")) => "ResolverChanged",
        (Some("n"), Some("perm")) => "PermissionChanged",
        (Some("n"), Some("scope")) => "PermissionScopeChanged",
        (Some("n"), Some("record")) => "RecordChanged",
        _ => "",
    }
}

/// The settings every read here shares: one namespace, oldest first, bound to the published block.
fn product_read() -> EventHistoryReadFilter {
    EventHistoryReadFilter {
        publication_block_bounds: Some(publication_bounds()),
        namespace: Some("ens".to_owned()),
        order: HistoryOrder::Asc,
        block_window: Some(HistoryBlockWindow {
            ranges: vec![ChainBlockRange {
                chain_id: MAINNET.to_owned(),
                from_block: None,
                to_block: Some(PUBLISHED),
            }],
        }),
        ..EventHistoryReadFilter::default()
    }
}

/// A registration read whose candidates are the registration's name and the registration itself.
fn registration_read(name: u32, registration: u128, kinds: &[&str]) -> EventHistoryReadFilter {
    EventHistoryReadFilter {
        selectors: vec![HistorySelector::ProductRegistration {
            logical_name_ids: vec![logical_name(name)],
            resource_ids: vec![Uuid::from_u128(registration)],
        }],
        registration_id: Some(Uuid::from_u128(registration)),
        registration_id_is_public: true,
        event_kinds: kinds.iter().map(|kind| (*kind).to_owned()).collect(),
        ..product_read()
    }
}

/// The page and the count of a registration read: `expected` in page order, from the current and
/// the previous statements alike.
async fn check_registration(
    connection: &mut PgConnection,
    read: &str,
    filter: &EventHistoryReadFilter,
    expected: &[&str],
    failures: &mut Vec<String>,
) -> Result<()> {
    let page = page_statement(filter, None, true, false, PAGE_LIMIT)?;
    let rows = compare_page(connection, read, &page, failures).await?;
    expect_rows(read, &rows, expected, failures);
    let count = count_statement(filter, true)?;
    compare_count(connection, read, &count, expected.len() as i64, failures).await
}

/// Run a page statement now and as it read before. Every row must carry the fixture's
/// registration identity and both must return the same rows. Returns the rows' identities.
async fn compare_page(
    connection: &mut PgConnection,
    read: &str,
    page: &Statement,
    failures: &mut Vec<String>,
) -> Result<Vec<String>> {
    page.check_previous(read, failures);
    let now = page_rows(connection, &page.sql, &page.arguments)
        .await
        .with_context(|| format!("{read}: page"))?;
    let before = page_rows(connection, &page.previous, &page.arguments)
        .await
        .with_context(|| format!("{read}: previous page"))?;
    if now != before {
        failures.push(format!("{read}: rows {now:?}\nprevious {before:?}"));
    }
    for (identity, registration_id) in &now {
        let expected = REGISTRATION_IDS
            .iter()
            .find(|(row, _)| row == identity)
            .map(|(_, registration)| Uuid::from_u128(*registration));
        if *registration_id != expected {
            failures.push(format!(
                "{read}: {identity} carries registration {registration_id:?}, expected {expected:?}"
            ));
        }
    }
    Ok(now.into_iter().map(|(identity, _)| identity).collect())
}

async fn compare_count(
    connection: &mut PgConnection,
    read: &str,
    count: &Statement,
    expected: i64,
    failures: &mut Vec<String>,
) -> Result<()> {
    count.check_previous(read, failures);
    let now = total(connection, &count.sql, &count.arguments).await?;
    let before = total(connection, &count.previous, &count.arguments).await?;
    if now != expected || before != expected {
        failures.push(format!(
            "{read}: count {now}, previous {before}, expected {expected}"
        ));
    }
    Ok(())
}

fn expect_rows(read: &str, rows: &[String], expected: &[&str], failures: &mut Vec<String>) {
    if rows.iter().map(String::as_str).ne(expected.iter().copied()) {
        failures.push(format!("{read}: rows {rows:?}\nexpected {expected:?}"));
    }
}

async fn page_rows(
    connection: &mut PgConnection,
    sql: &str,
    arguments: &PgArguments,
) -> Result<Vec<(String, Option<Uuid>)>> {
    sqlx::query_with(sql, arguments.clone())
        .persistent(false)
        .fetch_all(&mut *connection)
        .await?
        .iter()
        .map(|row| -> Result<(String, Option<Uuid>)> {
            Ok((
                row.try_get("event_identity")?,
                row.try_get("registration_id")?,
            ))
        })
        .collect()
}

async fn total(connection: &mut PgConnection, sql: &str, arguments: &PgArguments) -> Result<i64> {
    Ok(sqlx::query_scalar_with(sql, arguments.clone())
        .persistent(false)
        .fetch_one(&mut *connection)
        .await?)
}

async fn exists(connection: &mut PgConnection, sql: &str, arguments: &PgArguments) -> Result<bool> {
    Ok(sqlx::query_scalar_with(sql, arguments.clone())
        .persistent(false)
        .fetch_one(&mut *connection)
        .await?)
}

/// The plan PostgreSQL picks with the values bound, or its generic plan for a prepared
/// statement, which sqlx's statement cache can reach.
async fn explain(
    connection: &mut PgConnection,
    mode: Mode,
    sql: &str,
    arguments: &PgArguments,
) -> Result<Value> {
    Ok(match mode {
        Mode::Custom => {
            sqlx::query_scalar_with(&format!("EXPLAIN (FORMAT JSON) {sql}"), arguments.clone())
                .persistent(false)
                .fetch_one(&mut *connection)
                .await?
        }
        Mode::Generic => raw_sql(&format!("EXPLAIN (GENERIC_PLAN, FORMAT JSON) {sql}"))
            .fetch_one(&mut *connection)
            .await?
            .try_get(0)?,
    })
}

/// Every node of the plan, with whether it lies in a sub-plan.
fn plan_nodes(plan: &Value) -> Vec<(&Value, bool)> {
    fn walk<'a>(node: &'a Value, nested: bool, nodes: &mut Vec<(&'a Value, bool)>) {
        nodes.push((node, nested));
        for child in node["Plans"].as_array().into_iter().flatten() {
            let nested = nested
                || child["Parent Relationship"] == "SubPlan"
                || child["Parent Relationship"] == "InitPlan";
            walk(child, nested, nodes);
        }
    }
    let mut nodes = Vec::new();
    walk(&plan[0]["Plan"], false, &mut nodes);
    nodes
}

fn reads_lineage(node: &Value) -> bool {
    node["Relation Name"] == "chain_lineage"
        || node["Plans"]
            .as_array()
            .into_iter()
            .flatten()
            .any(reads_lineage)
}

/// The hash or merge joins, anywhere in the plan, whose inner side reads `chain_lineage`.
fn lineage_joins(plan: &Value) -> Vec<String> {
    plan_nodes(plan)
        .into_iter()
        .filter(|(node, _)| node["Node Type"] == "Hash Join" || node["Node Type"] == "Merge Join")
        .filter(|(node, _)| {
            node["Plans"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|child| child["Parent Relationship"] == "Inner" && reads_lineage(child))
        })
        .map(|(node, nested)| {
            format!(
                "{} over chain_lineage{}",
                node["Node Type"].as_str().unwrap_or_default(),
                if nested { " in a sub-plan" } else { "" }
            )
        })
        .collect()
}

fn hashes_lineage(plan: &Value) -> bool {
    lineage_joins(plan)
        .iter()
        .any(|join| join == "Hash Join over chain_lineage")
}

/// `chain_lineage` must never be scanned whole or joined by hash or merge, and the event's own
/// lineage row `rb` must be read by an index probe.
fn lineage_failures(cell: &str, plan: &Value) -> Vec<String> {
    let mut failures = lineage_joins(plan);
    for (node, _) in plan_nodes(plan) {
        if node["Node Type"] == "Seq Scan" && node["Relation Name"] == "chain_lineage" {
            failures.push(format!(
                "a sequential scan of chain_lineage {}",
                node["Alias"].as_str().unwrap_or_default()
            ));
        }
    }
    failures.extend(rb_failures(plan));
    report(cell, plan, failures)
}

/// The event's own lineage row `rb` alone: one index scan keyed by the lineage key, never the
/// inner side of a hash or merge join.
fn probe_failures(cell: &str, plan: &Value) -> Vec<String> {
    report(cell, plan, rb_failures(plan))
}

fn rb_failures(plan: &Value) -> Vec<String> {
    /// Each `rb` scan, with whether a hash or merge join reads it as its inner side.
    fn walk<'a>(node: &'a Value, hashed: bool, scans: &mut Vec<(&'a Value, bool)>) {
        if node["Alias"] == "rb" && node["Relation Name"] == "chain_lineage" {
            scans.push((node, hashed));
        }
        let joins = node["Node Type"] == "Hash Join" || node["Node Type"] == "Merge Join";
        for child in node["Plans"].as_array().into_iter().flatten() {
            let inner = joins && child["Parent Relationship"] == "Inner";
            walk(child, hashed || inner, scans);
        }
    }
    let mut scans = Vec::new();
    walk(&plan[0]["Plan"], false, &mut scans);
    let mut failures = Vec::new();
    if scans.len() != 1 {
        failures.push(format!("{} scans of rb", scans.len()));
    }
    for (node, hashed) in scans {
        let condition = node["Index Cond"].as_str().unwrap_or_default();
        if node["Node Type"] != "Index Scan"
            || !condition.contains("chain_id =")
            || !condition.contains("block_hash =")
        {
            failures.push(format!(
                "rb is read by {} with index condition {condition:?}",
                node["Node Type"].as_str().unwrap_or_default()
            ));
        }
        if hashed {
            failures.push("rb is the inner side of a hash or merge join".to_owned());
        }
    }
    failures
}

/// Every NameWrapper lookup of `normalized_events` must be keyed by resource, by an index scan
/// or a bitmap scan.
fn wrapper_failures(cell: &str, plan: &Value) -> Vec<String> {
    let mut failures = Vec::new();
    let mut lookups = 0;
    for (node, _) in plan_nodes(plan) {
        if node["Relation Name"] != "normalized_events"
            || !node["Alias"]
                .as_str()
                .is_some_and(|alias| alias.starts_with("wrapper_binding"))
        {
            continue;
        }
        lookups += 1;
        let condition = match node["Node Type"].as_str().unwrap_or_default() {
            "Index Scan" | "Index Only Scan" => {
                node["Index Cond"].as_str().unwrap_or_default().to_owned()
            }
            "Bitmap Heap Scan" => bitmap_conditions(node),
            _ => String::new(),
        };
        if !condition.contains("resource_id =") {
            failures.push(format!(
                "{} is read by {} with index condition {condition:?}",
                node["Alias"].as_str().unwrap_or_default(),
                node["Node Type"].as_str().unwrap_or_default()
            ));
        }
    }
    if lookups == 0 {
        failures.push("no NameWrapper lookup in the plan".to_owned());
    }
    report(cell, plan, failures)
}

/// The index conditions of the bitmap index scans under a bitmap heap scan, through any
/// BitmapAnd or BitmapOr.
fn bitmap_conditions(node: &Value) -> String {
    node["Plans"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|child| {
            format!(
                "{} {}",
                child["Index Cond"].as_str().unwrap_or_default(),
                bitmap_conditions(child)
            )
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn report(cell: &str, plan: &Value, failures: Vec<String>) -> Vec<String> {
    if failures.is_empty() {
        return failures;
    }
    vec![format!("{cell}: {}\n{plan:#}", failures.join("; "))]
}

async fn with_fixture(
    prefix: &str,
    check: impl AsyncFnOnce(&mut PgConnection) -> Result<()>,
) -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new(prefix).pool_max_connections(1)).await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        raw_sql("CREATE SCHEMA bigname_phase; SET search_path TO bigname_phase, public")
            .execute(&mut *connection)
            .await?;
        for baseline in [
            include_str!("../../schema/baseline/01_chain.sql"),
            include_str!("../../schema/baseline/02_raw_facts.sql"),
            include_str!("../../schema/baseline/03_identity.sql"),
            include_str!("../../schema/baseline/04_manifests.sql"),
            include_str!("../../schema/baseline/05_normalized_events.sql"),
            include_str!("../../schema/baseline/06_projections.sql"),
        ] {
            raw_sql(baseline).execute(&mut *connection).await?;
        }
        raw_sql("SET jit = off").execute(&mut *connection).await?;
        raw_sql(FIXTURE_SQL)
            .execute(&mut *connection)
            .await
            .context("install the registration lineage fixture")?;
        check(&mut connection).await
    }
    .await;
    database.cleanup().await?;
    result
}

/// Lineage for blocks 1..=200,000 of Ethereum Mainnet, block n hashed `m:n` at 12 s a block, and
/// one orphaned block at 45. Name k has namehash k as 32 bytes of hex and resource k is uuid k.
/// Bindings open and close at a block's timestamp plus one microsecond per log index, as
/// Interpret writes them. Autovacuum is off so the statistics are only the ones `ANALYZE`
/// collects.
const FIXTURE_SQL: &str = r#"
ALTER TABLE normalized_events SET (autovacuum_enabled = false);
ALTER TABLE chain_lineage SET (autovacuum_enabled = false);
ALTER TABLE surface_bindings SET (autovacuum_enabled = false);
CREATE FUNCTION pg_temp.node(k bigint) RETURNS text LANGUAGE sql IMMUTABLE
    AS $$ SELECT '0x' || lpad(to_hex(k), 64, '0') $$;
CREATE FUNCTION pg_temp.rid(k bigint) RETURNS uuid LANGUAGE sql IMMUTABLE
    AS $$ SELECT lpad(to_hex(k), 32, '0')::uuid $$;
CREATE FUNCTION pg_temp.at(block bigint, micros bigint DEFAULT 0) RETURNS timestamptz
    LANGUAGE sql IMMUTABLE
    AS $$ SELECT to_timestamp(1500000000 + block * 12) + micros * interval '1 microsecond' $$;
CREATE FUNCTION pg_temp.ev(
    identity text, kind text, family text, resource uuid, name bigint, block bigint,
    log bigint, state jsonb DEFAULT '{}', visibility text DEFAULT 'activated',
    hash text DEFAULT NULL
) RETURNS void LANGUAGE sql AS $$
    INSERT INTO normalized_events (event_identity, namespace, logical_name_id, resource_id,
        event_kind, source_family, manifest_version, chain_id, block_number, block_hash,
        transaction_hash, transaction_index, log_index, derivation_kind, canonicality_state,
        consumer_visibility, migration_correlation_ids, after_state)
    VALUES (identity, 'ens', 'ens:' || pg_temp.node(name), resource, kind, family, 1,
        'ethereum-mainnet', block,
        CASE WHEN block IS NOT NULL THEN COALESCE(hash, 'm:' || block) END,
        CASE WHEN log IS NOT NULL THEN 'tx:' || identity END,
        CASE WHEN log IS NOT NULL THEN 0 END, log,
        'ens_v1_unwrapped_authority', 'canonical', visibility,
        CASE visibility WHEN 'candidate' THEN ARRAY['fixture'] ELSE ARRAY[]::text[] END,
        state);
$$;
CREATE FUNCTION pg_temp.bind(name bigint, resource uuid, arm text, opened timestamptz,
    closed timestamptz, block bigint, hash text DEFAULT NULL) RETURNS void LANGUAGE sql AS $$
    INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id, binding_kind,
        authority_arm, active_from, active_to, chain_id, block_hash, block_number,
        canonicality_state)
    VALUES (gen_random_uuid(), 'ens:' || pg_temp.node(name), resource, 'declared_registry_path',
        arm, opened, closed, 'ethereum-mainnet', COALESCE(hash, 'm:' || block), block, 'canonical');
$$;

INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number, block_timestamp,
    canonicality_state)
SELECT 'ethereum-mainnet', 'm:' || n, CASE WHEN n > 1 THEN 'm:' || (n - 1) END, n, pg_temp.at(n),
    'canonical'::canonicality_state
FROM generate_series(1, 200000) n
UNION ALL
SELECT 'ethereum-mainnet', 'orphan:45', 'm:44', 45, pg_temp.at(45), 'orphaned';

INSERT INTO name_surfaces (logical_name_id, namespace, namehash, labelhashes, normalizer_version,
    visibility_state, chain_id, block_hash, block_number, canonicality_state)
SELECT 'ens:' || pg_temp.node(k), 'ens', pg_temp.node(k), ARRAY[pg_temp.node(k)], 'fixture',
    'active', 'ethereum-mainnet', 'm:1', 1, 'canonical'
FROM (SELECT unnest(ARRAY[161, 162, 177]) k UNION ALL SELECT 65536 + n FROM generate_series(0, 1000 - 1) n) names;
INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state)
SELECT pg_temp.rid(k), 'ethereum-mainnet', 'm:1', 1, 'canonical'
FROM (SELECT unnest(ARRAY[2576, 2577, 2578, 2579, 2832]) k UNION ALL SELECT 1048576 + n FROM generate_series(0, 100 - 1) n) resources;

-- Unrelated leases: 100 resources of 50 rows each, a grant and then renewals and transfers,
-- on 1,000 names and blocks 1 to 5,000.
INSERT INTO normalized_events (event_identity, namespace, logical_name_id, resource_id, event_kind,
    source_family, manifest_version, chain_id, block_number, block_hash, derivation_kind,
    canonicality_state, after_state)
SELECT 'noise:' || n, 'ens', 'ens:' || pg_temp.node(65536 + n % 1000),
    pg_temp.rid(1048576 + n % 100),
    CASE WHEN n < 100 THEN 'RegistrationGranted' WHEN n % 2 = 0 THEN 'RegistrationRenewed'
         ELSE 'TokenControlTransferred' END,
    'ens_v1_registrar_l1', 1, 'ethereum-mainnet', 1 + n % 200000, 'm:' || (1 + n % 200000),
    'ens_v1_unwrapped_authority', 'canonical', '{}'
FROM generate_series(0, 5000 - 1) n;
SELECT pg_temp.bind(65536 + n, pg_temp.rid(1048576 + n % 100), 'ens_v1', pg_temp.at(1), NULL, 1)
FROM generate_series(0, 1000 - 1) n;

-- N (161): lease L1 (2576) bound from block 10, lease L2 (2577) bound from block 50 at
-- log 2. L3 (2578) is bound on an orphaned block. X (2579) is a registry-only resource of N
-- bound to another name, M (162), from block 30 at log 1 to block 70 at log 1.
SELECT pg_temp.ev('l1:grant', 'RegistrationGranted', 'ens_v1_registrar_l1', pg_temp.rid(2576), 161, 10, 0, jsonb_build_object('namehash', pg_temp.node(161)));
SELECT pg_temp.ev('l1:renew', 'RegistrationRenewed', 'ens_v1_registrar_l1', pg_temp.rid(2576), 161, 30, 0, jsonb_build_object('namehash', pg_temp.node(161)));
SELECT pg_temp.ev('l2:grant', 'RegistrationGranted', 'ens_v1_registrar_l1', pg_temp.rid(2577), 161, 50, 2, jsonb_build_object('namehash', pg_temp.node(161)));
SELECT pg_temp.ev('l3:grant', 'RegistrationGranted', 'ens_v1_registrar_l1', pg_temp.rid(2578), 161, 5, 0, jsonb_build_object('namehash', pg_temp.node(161)));
SELECT pg_temp.bind(161, pg_temp.rid(2576), 'ens_v1', pg_temp.at(10), pg_temp.at(50, 2), 10);
SELECT pg_temp.bind(161, pg_temp.rid(2577), 'ens_v1', pg_temp.at(50, 2), NULL, 50);
SELECT pg_temp.bind(161, pg_temp.rid(2578), 'ens_v2', pg_temp.at(5), pg_temp.at(10), 45, 'orphan:45');
SELECT pg_temp.bind(162, pg_temp.rid(2579), 'ens_v1', pg_temp.at(30, 1), pg_temp.at(70, 1), 30);
SELECT pg_temp.ev('x:authority', 'AuthorityTransferred', 'ens_v1_registry_l1', pg_temp.rid(2579), 161, 25, 0, jsonb_build_object('authority_kind', 'registry_only', 'node', pg_temp.node(161)));
SELECT pg_temp.ev('x:resolver:' || block || ':' || log, 'ResolverChanged', 'ens_v1_registry_l1', pg_temp.rid(2579), 161, block, log,
    jsonb_build_object('surface_materialization', 'true', 'authority_kind', 'registry_only', 'node', pg_temp.node(161)))
FROM (VALUES (30, 0), (30, 1), (70, 0), (70, 1)) x(block, log);
SELECT pg_temp.ev('x:resolver:no-block', 'ResolverChanged', 'ens_v1_registry_l1', pg_temp.rid(2579), 161, NULL, NULL,
    jsonb_build_object('surface_materialization', 'true', 'authority_kind', 'registry_only', 'node', pg_temp.node(161)));
SELECT pg_temp.ev('n:' || label, kind, 'ens_v1_registry_l1', NULL, 161, block, log, '{}', visibility, hash)
FROM (VALUES
    ('perm:9:5', 'PermissionChanged', 9, 5, 'activated', NULL),
    ('perm:10:0', 'PermissionChanged', 10, 0, 'activated', NULL),
    ('perm:20:0', 'PermissionChanged', 20, 0, 'activated', NULL),
    ('scope:50:0', 'PermissionScopeChanged', 50, 0, 'activated', NULL),
    ('scope:50:1', 'PermissionScopeChanged', 50, 1, 'activated', NULL),
    ('perm:50:2', 'PermissionChanged', 50, 2, 'activated', NULL),
    ('scope:50:3', 'PermissionScopeChanged', 50, 3, 'activated', NULL),
    ('perm:50:none', 'PermissionChanged', 50, NULL, 'activated', NULL),
    ('perm:60:0', 'PermissionChanged', 60, 0, 'activated', NULL),
    ('perm:orphan', 'PermissionChanged', 45, 0, 'activated', 'orphan:45'),
    ('perm:candidate', 'PermissionChanged', 40, 0, 'candidate', NULL),
    ('record:40:0', 'RecordChanged', 40, 0, 'activated', NULL),
    ('record:no-block', 'RecordChanged', NULL, NULL, 'activated', NULL)
) n(label, kind, block, log, visibility, hash);

-- An account-scoped operator approval by the address the address read asks for.
SELECT pg_temp.ev('acct:operator', 'AccountPermissionChanged', 'ens_v2_registry_l1', NULL, NULL, 40, 1,
    jsonb_build_object('scope', jsonb_build_object('kind', 'account', 'owner', '0x00000000000000000000000000000000000000aa'),
        'relation_kind', 'operator', 'subject', '0x00000000000000000000000000000000000000bb'));

-- H (177): lease LH (2832) with 2000 permission changes.
SELECT pg_temp.ev('h:grant', 'RegistrationGranted', 'ens_v1_registrar_l1', pg_temp.rid(2832), 177, 10, 0, jsonb_build_object('namehash', pg_temp.node(177)));
SELECT pg_temp.bind(177, pg_temp.rid(2832), 'ens_v1', pg_temp.at(10), NULL, 10);
SELECT pg_temp.ev('h:perm:' || n, 'PermissionChanged', 'ens_v1_registry_l1', NULL, 177, 11 + n % 9000, n / 9000, '{}')
FROM generate_series(0, 2000 - 1) n;
ANALYZE;"#;
