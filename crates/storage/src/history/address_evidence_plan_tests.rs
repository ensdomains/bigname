//! Plan and row tests for the historical address membership statement
//! (`push_historical_address_matches_query`), which `GET /v1/events?address=`,
//! `GET /v1/diagnostics/events?address=` and the address history walker send.
//!
//! The address-match indexes are partial, and the planner never reads a partial index's
//! expression statistics, so it estimates every address at the default equality selectivity.
//! Against that estimate the previous statement ANDed the address probes with an index keyed by
//! event kind or block over every grant and transfer of the chain. The fixture reproduces that:
//! the plan test requires the previous statement to take the `BitmapAnd` here before `ANALYZE`
//! and under plain statistics, so the fixture keeps proving something, and requires the fenced
//! statement to read each address key through its own index alone in every cell. Sequential and
//! bitmap scans stay enabled: the shape is chosen on cost. The row test pins every input class
//! to its rows and to the previous statement's rows.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::Value;
use sqlx::{PgConnection, PgPool, Postgres, QueryBuilder, Row, raw_sql};
use uuid::Uuid;

use super::address_evidence::push_historical_address_matches_query;
use super::source::{
    push_history_canonicality_filter, push_history_lineage_join, push_readable_anchored_row_filter,
};
use crate::AddressNameRelation;

const PLAN_MODES: [&str; 2] = ["force_custom_plan", "force_generic_plan"];
const MAINNET: &str = "ethereum-mainnet";
const SEPOLIA: &str = "ethereum-sepolia";
const BOUND: i64 = 1900;
const SEPOLIA_BOUND: i64 = 100;
/// The kind-free index of each address key.
const KEY_INDEXES: [(&str, &str); 3] = [
    ("registrant", "normalized_events_address_registrant_idx"),
    ("to", "normalized_events_address_to_idx"),
    ("owner", "normalized_events_address_owner_idx"),
];

const TOKEN_HOLDER: &[AddressNameRelation] = &[AddressNameRelation::TokenHolder];
const CONTROLLER: &[AddressNameRelation] = &[AddressNameRelation::EffectiveController];
const ROLE_HOLDER: &[AddressNameRelation] = &[AddressNameRelation::RoleHolder];

/// The fixture's addresses, by the number they encode.
const TYPICAL: u32 = 0x7001;
const HEAVY: u32 = 0x7002;
const ZERO: u32 = 0x7003;
const MULTI_ARM: u32 = 0x7004;
const MULTI_KEY: u32 = 0x7005;
const OTHER_KIND: u32 = 0x7006;
const WRAPPER: u32 = 0x7007;
const LEASE: u32 = 0x7008;
const LINEAGE_EDGES: u32 = 0x7009;
const READABILITY: u32 = 0x700a;
const SEPOLIA_HOLDER: u32 = 0x700b;
const CONTRACT: u32 = 0x700c;
/// Stored in upper case as a registrant.
const UPPER_CASE: &str = "0x0000000000000000000000000000000000abcdef";
/// Stored with mixed case, as a checksummed address prints, as a token transfer's recipient.
const MIXED_CASE: &str = "0x00000000000000000000000000000000abcdef12";
/// Stored as a token transfer's recipient without the `0x` prefix.
const BARE_HEX: &str = "0x00000000000000000000000000000000000fedcb";

type Anchor = (Option<String>, Option<Uuid>);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Bound {
    Unbounded,
    Mainnet,
    BothChains,
    Empty,
}

/// One combination of the statement's flags. The binaries reach `EVENTS` (the events route, and
/// the address history walker with no relation), the relation forms (the walker), `DIAGNOSTICS`
/// and the unbounded form (the history loaders).
#[derive(Clone, Copy)]
struct Form {
    label: &'static str,
    canonical_only: bool,
    include_candidates: bool,
    namespace: Option<&'static str>,
    bound: Bound,
    relations: Option<&'static [AddressNameRelation]>,
}

const EVENTS: Form = Form {
    label: "events",
    canonical_only: true,
    include_candidates: false,
    namespace: Some("ens"),
    bound: Bound::Mainnet,
    relations: None,
};
const DIAGNOSTICS: Form = Form {
    label: "diagnostics",
    include_candidates: true,
    ..EVENTS
};
const UNBOUNDED: Form = Form {
    label: "unbounded",
    bound: Bound::Unbounded,
    ..EVENTS
};
const ANY_NAMESPACE: Form = Form {
    label: "any namespace",
    namespace: None,
    ..EVENTS
};
const NONCANONICAL: Form = Form {
    label: "not canonical only",
    canonical_only: false,
    ..EVENTS
};
const EMPTY_BOUND: Form = Form {
    label: "empty bound",
    bound: Bound::Empty,
    ..EVENTS
};
const BOTH_CHAINS: Form = Form {
    label: "both chains",
    bound: Bound::BothChains,
    ..EVENTS
};
const TOKEN_HOLDER_FORM: Form = Form {
    label: "token holder",
    relations: Some(TOKEN_HOLDER),
    ..EVENTS
};
const CONTROLLER_FORM: Form = Form {
    label: "controller",
    relations: Some(CONTROLLER),
    ..EVENTS
};
const ROLE_HOLDER_FORM: Form = Form {
    label: "role holder",
    relations: Some(ROLE_HOLDER),
    ..EVENTS
};

impl Form {
    fn published(self) -> Option<BTreeMap<String, i64>> {
        match self.bound {
            Bound::Unbounded => None,
            Bound::Mainnet => Some(BTreeMap::from([(MAINNET.to_owned(), BOUND)])),
            Bound::BothChains => Some(BTreeMap::from([
                (MAINNET.to_owned(), BOUND),
                (SEPOLIA.to_owned(), SEPOLIA_BOUND),
            ])),
            Bound::Empty => Some(BTreeMap::new()),
        }
    }

    /// The address keys the statement must probe: one per key an arm of these relations reads.
    fn probed_keys(self) -> BTreeSet<&'static str> {
        let token_holder = self
            .relations
            .is_none_or(|relations| relations.contains(&AddressNameRelation::TokenHolder));
        let controller = self
            .relations
            .is_none_or(|relations| relations.contains(&AddressNameRelation::EffectiveController));
        let mut keys = BTreeSet::new();
        if token_holder {
            keys.extend(["registrant", "to"]);
        }
        if token_holder || controller {
            keys.insert("owner");
        }
        if controller {
            keys.insert("to");
        }
        if self.bound == Bound::Empty {
            keys.clear();
        }
        keys
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Statement {
    Fenced,
    Before,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Statistics {
    /// The copy of the template has never been analyzed.
    Never,
    /// A plain `ANALYZE`. The partial indexes' expression statistics never reach the planner.
    Plain,
    /// Whole-table extended statistics on the three expressions, which the planner does read.
    Expressions,
}

impl Statistics {
    fn label(self) -> &'static str {
        match self {
            Self::Never => "before ANALYZE",
            Self::Plain => "plain statistics",
            Self::Expressions => "expression statistics",
        }
    }

    async fn apply(self, connection: &mut PgConnection) -> Result<()> {
        let sql = match self {
            Self::Never => return Ok(()),
            Self::Plain => {
                "DROP STATISTICS IF EXISTS address_key_registrant_stats, address_key_to_stats,
                     address_key_owner_stats;
                 ANALYZE normalized_events, resources, chain_lineage, name_surfaces,
                     token_lineages, project_binding_candidate;"
            }
            Self::Expressions => {
                "CREATE STATISTICS IF NOT EXISTS address_key_registrant_stats
                     ON (lower(COALESCE(after_state ->> 'registrant', ''))) FROM normalized_events;
                 CREATE STATISTICS IF NOT EXISTS address_key_to_stats
                     ON (lower(COALESCE(after_state ->> 'to', ''))) FROM normalized_events;
                 CREATE STATISTICS IF NOT EXISTS address_key_owner_stats
                     ON (lower(COALESCE(after_state ->> 'owner', ''))) FROM normalized_events;
                 ANALYZE normalized_events, resources, chain_lineage, name_surfaces,
                     token_lineages, project_binding_candidate;"
            }
        };
        raw_sql(sql).execute(&mut *connection).await?;
        Ok(())
    }
}

fn address(k: u32) -> String {
    format!("0x{k:040x}")
}

fn named(k: u128) -> Anchor {
    (Some(format!("ens:0x{k:064x}")), Some(Uuid::from_u128(k)))
}

fn name_only(k: u128) -> Anchor {
    (Some(format!("ens:0x{k:064x}")), None)
}

fn resource_only(k: u128) -> Anchor {
    (None, Some(Uuid::from_u128(k)))
}

fn push_statement<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    statement: Statement,
    address: &'a str,
    form: Form,
    published: Option<&'a BTreeMap<String, i64>>,
) {
    let (namespace, relations) = (form.namespace, form.relations);
    let (canonical_only, include_candidates) = (form.canonical_only, form.include_candidates);
    match statement {
        Statement::Fenced => push_historical_address_matches_query(
            builder,
            address,
            namespace,
            relations,
            canonical_only,
            include_candidates,
            published,
        ),
        Statement::Before => push_before_query(
            builder,
            address,
            namespace,
            relations,
            canonical_only,
            include_candidates,
            published,
        ),
    }
}

async fn explain(
    connection: &mut PgConnection,
    mode: &str,
    statement: Statement,
    address: &str,
    form: Form,
) -> Result<Value> {
    let published = form.published();
    if mode == "force_generic_plan" {
        let mut builder = QueryBuilder::<Postgres>::new("");
        push_statement(&mut builder, statement, address, form, published.as_ref());
        let sql = format!("EXPLAIN (GENERIC_PLAN, FORMAT JSON) {}", builder.sql());
        return raw_sql(&sql)
            .fetch_one(&mut *connection)
            .await
            .context("generic plan")?
            .try_get::<Value, _>(0)
            .map_err(Into::into);
    }
    raw_sql(&format!("SET plan_cache_mode = {mode}"))
        .execute(&mut *connection)
        .await?;
    let mut builder = QueryBuilder::<Postgres>::new("EXPLAIN (FORMAT JSON) ");
    push_statement(&mut builder, statement, address, form, published.as_ref());
    builder
        .build_query_scalar::<Value>()
        .fetch_one(&mut *connection)
        .await
        .context("custom plan")
}

async fn anchors(
    connection: &mut PgConnection,
    mode: &str,
    statement: Statement,
    address: &str,
    form: Form,
) -> Result<Vec<Anchor>> {
    raw_sql(&format!("SET plan_cache_mode = {mode}"))
        .execute(&mut *connection)
        .await?;
    let published = form.published();
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_statement(&mut builder, statement, address, form, published.as_ref());
    let mut rows = builder
        .build()
        .fetch_all(&mut *connection)
        .await?
        .iter()
        .map(|row| Ok((row.try_get(0)?, row.try_get(1)?)))
        .collect::<Result<Vec<Anchor>>>()?;
    rows.sort();
    Ok(rows)
}

#[tokio::test]
async fn address_matches_probe_only_the_key_indexes() -> Result<()> {
    with_fixture("address_key_plan", async |connection| {
        let inputs = [TYPICAL, HEAVY, ZERO, CONTRACT];
        let mut cells: Vec<(Form, u32)> = Vec::new();
        for form in [EVENTS, DIAGNOSTICS, UNBOUNDED] {
            cells.extend(inputs.iter().map(|&input| (form, input)));
        }
        for form in [
            TOKEN_HOLDER_FORM,
            CONTROLLER_FORM,
            ROLE_HOLDER_FORM,
            EMPTY_BOUND,
        ] {
            cells.push((form, TYPICAL));
        }
        let mut failures = Vec::new();
        for statistics in [
            Statistics::Never,
            Statistics::Plain,
            Statistics::Expressions,
        ] {
            statistics.apply(connection).await?;
            for mode in PLAN_MODES {
                for &(form, input) in &cells {
                    let holder = address(input);
                    let cell = format!(
                        "{}, {mode}, {} form, address {holder}",
                        statistics.label(),
                        form.label
                    );
                    let plan = explain(connection, mode, Statement::Fenced, &holder, form).await?;
                    failures.extend(fenced_plan_failures(&cell, &plan, form));
                    let before =
                        explain(connection, mode, Statement::Before, &holder, form).await?;
                    println!("{cell}, previous statement:\n{before}\n");
                    let red_cell =
                        statistics != Statistics::Expressions && form.label == EVENTS.label;
                    if red_cell && !ands_an_unkeyed_index(&before) {
                        failures.push(format!(
                            "{cell}: the previous statement no longer ANDs an index keyed by \
                             event kind or block, so this fixture proves nothing about it\n\
                             {before}"
                        ));
                    }
                }
            }
        }
        ensure!(
            failures.is_empty(),
            "address match plans:\n{}",
            failures.join("\n\n")
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn address_matches_rows_and_oracle_parity() -> Result<()> {
    with_fixture("address_key_rows", async |connection| {
        let typical = [named(0x7101), named(0x7102), named(0x7103)];
        let heavy = (0x20001..=0x205dc).map(resource_only).collect::<Vec<_>>();
        let contract = (0x40001..=0x44e20).map(resource_only).collect::<Vec<_>>();
        let multi_arm = [named(0x7401), named(0x7402), resource_only(0x7403)];
        let lease_all = [named(0x7801), named(0x7802), named(0x7803), named(0x7804)];
        let readable_noncanonical = [
            named(0x7a01),
            named(0x7a02),
            named(0x7a03),
            named(0x7a04),
            name_only(0x7a05),
        ];
        let cases: Vec<(&str, String, Form, Vec<Anchor>)> = vec![
            ("R1 typical", address(TYPICAL), EVENTS, typical.to_vec()),
            ("R2 heavy", address(HEAVY), EVENTS, heavy.clone()),
            ("R3 zero", address(ZERO), EVENTS, vec![]),
            (
                "R4 candidate rows under diagnostics",
                address(READABILITY),
                DIAGNOSTICS,
                vec![name_only(0x7a05), named(0x7a06)],
            ),
            (
                "R5 not canonical only",
                address(READABILITY),
                NONCANONICAL,
                readable_noncanonical.to_vec(),
            ),
            (
                "R6 unbounded",
                address(TYPICAL),
                UNBOUNDED,
                typical.iter().cloned().chain([named(0x7104)]).collect(),
            ),
            (
                "R7 any namespace",
                address(TYPICAL),
                ANY_NAMESPACE,
                typical
                    .iter()
                    .cloned()
                    .chain([(
                        Some(format!("basenames:0x{:064x}", 0x7105)),
                        Some(Uuid::from_u128(0x7105)),
                    )])
                    .collect(),
            ),
            ("R8 empty bound", address(TYPICAL), EMPTY_BOUND, vec![]),
            (
                "R9 both chains",
                address(SEPOLIA_HOLDER),
                BOTH_CHAINS,
                vec![named(0x7b02), named(0x7b03)],
            ),
            (
                "R9 one chain",
                address(SEPOLIA_HOLDER),
                EVENTS,
                vec![named(0x7b02)],
            ),
            (
                "R10 multi-arm",
                address(MULTI_ARM),
                EVENTS,
                multi_arm.to_vec(),
            ),
            (
                "R10' one row under every key",
                address(MULTI_KEY),
                EVENTS,
                vec![named(0x7501)],
            ),
            (
                "R11 controller",
                address(MULTI_ARM),
                CONTROLLER_FORM,
                vec![named(0x7401), named(0x7402)],
            ),
            (
                "R11 token holder",
                address(MULTI_ARM),
                TOKEN_HOLDER_FORM,
                multi_arm.to_vec(),
            ),
            (
                "R12 lease handoff, token holder",
                address(LEASE),
                TOKEN_HOLDER_FORM,
                vec![named(0x7801)],
            ),
            (
                "R13 lease handoff",
                address(LEASE),
                EVENTS,
                lease_all.to_vec(),
            ),
            (
                "R14 role holder",
                address(TYPICAL),
                ROLE_HOLDER_FORM,
                vec![],
            ),
            (
                "R15 wrapper",
                address(WRAPPER),
                EVENTS,
                vec![named(0x7702), named(0x7704)],
            ),
            (
                "R16 token lineage edges",
                address(LINEAGE_EDGES),
                EVENTS,
                vec![],
            ),
            (
                "R17 readability",
                address(READABILITY),
                EVENTS,
                vec![name_only(0x7a05)],
            ),
            (
                "R18 upper-case registrant",
                UPPER_CASE.to_owned(),
                EVENTS,
                vec![named(0x7d01)],
            ),
            ("R19 contract", address(CONTRACT), EVENTS, contract.clone()),
            ("R20 other kind", address(OTHER_KIND), EVENTS, vec![]),
            (
                "R20 other kind, diagnostics",
                address(OTHER_KIND),
                DIAGNOSTICS,
                vec![],
            ),
            ("R2 heavy, diagnostics", address(HEAVY), DIAGNOSTICS, heavy),
            (
                "R19 contract, diagnostics",
                address(CONTRACT),
                DIAGNOSTICS,
                contract,
            ),
            (
                "R1 typical, diagnostics",
                address(TYPICAL),
                DIAGNOSTICS,
                typical.to_vec(),
            ),
            (
                "R22 mixed-case recipient",
                MIXED_CASE.to_owned(),
                EVENTS,
                vec![named(0x7f21)],
            ),
            (
                "R23 recipient without 0x",
                BARE_HEX.to_owned(),
                EVENTS,
                vec![],
            ),
            (
                "R23 recipient without 0x, diagnostics",
                BARE_HEX.to_owned(),
                DIAGNOSTICS,
                vec![],
            ),
        ];
        // The rows whose key is JSON null, a 3,000-character string or 40 hex digits without
        // `0x` hold no address, so no probe may return them.
        let never = [
            named(0x7e01),
            named(0x7e02),
            named(0x7e03),
            named(0x7f01),
            named(0x7f02),
            named(0x7f03),
            named(0x7f11),
            named(0x7f12),
            named(0x7f13),
        ];
        // Only the mixed-case recipient's value has an address's length, so it alone is in a key
        // index, and the long values were inserted with the indexes present.
        let indexed: Vec<(String, bool)> = sqlx::query_as(
            "SELECT event_identity, length(COALESCE(after_state ->> 'to', after_state ->> \
             'registrant', after_state ->> 'owner')) = 42 IS TRUE
             FROM normalized_events
             WHERE event_identity LIKE 'n:%' OR event_identity LIKE 'long:%'
                OR event_identity LIKE 'bare:%' OR event_identity = 'mixed:transfer'
             ORDER BY event_identity COLLATE \"C\"",
        )
        .fetch_all(&mut *connection)
        .await?;
        let expected_indexed = [
            ("bare:grant", false),
            ("bare:owner", false),
            ("bare:transfer", false),
            ("long:owner", false),
            ("long:registrant", false),
            ("long:to", false),
            ("mixed:transfer", true),
            ("n:grant", false),
            ("n:owner", false),
            ("n:transfer", false),
        ]
        .map(|(identity, indexed)| (identity.to_owned(), indexed));
        ensure!(
            indexed == expected_indexed,
            "key index membership {indexed:?}"
        );
        // The earlier kind-partial match indexes are unbounded: the same long value as a token
        // transfer's recipient fails its insert there. No writer produces one.
        let refused = raw_sql(
            "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
                 manifest_version, chain_id, derivation_kind, canonicality_state, after_state)
             SELECT 'long:old-index', 'ens', 'TokenControlTransferred', 'ens_v2_registry_l1', 1,
                 'ethereum-mainnet', 'ens_v2_registry_resource_surface', 'canonical',
                 jsonb_build_object('to', after_state ->> 'to')
             FROM normalized_events WHERE event_identity = 'long:to'",
        )
        .execute(&mut *connection)
        .await;
        ensure!(
            refused.as_ref().is_err_and(|error| error
                .to_string()
                .contains("normalized_events_address_token_holder_match_idx")),
            "a long recipient on a token transfer was not refused by the kind-partial index: \
             {refused:?}"
        );
        let mut failures = Vec::new();
        for statistics in [Statistics::Plain, Statistics::Expressions] {
            statistics.apply(connection).await?;
            for mode in PLAN_MODES {
                for (label, address, form, expected) in &cases {
                    let cell = format!("{}, {mode}, {label}", statistics.label());
                    let mut expected = expected.clone();
                    expected.sort();
                    let rows = anchors(connection, mode, Statement::Fenced, address, *form).await?;
                    let before =
                        anchors(connection, mode, Statement::Before, address, *form).await?;
                    if rows != expected || rows != before {
                        failures.push(format!(
                            "{cell}: {} rows {rows:?}\nexpected {} {expected:?}\nprevious {} \
                             {before:?}",
                            rows.len(),
                            expected.len(),
                            before.len()
                        ));
                    }
                    if let Some(row) = rows.iter().find(|row| never.contains(row)) {
                        failures.push(format!("{cell}: a key holding no address matched: {row:?}"));
                    }
                }
            }
        }
        ensure!(
            failures.is_empty(),
            "address match rows:\n{}",
            failures.join("\n\n")
        );
        Ok(())
    })
    .await
}

/// The publication statement as `historical_history_relations` sends it at ccc39d88a, by the
/// md5 of its text. The fence leaves Project's statement byte-identical: it shares only the
/// arm and eligibility filters, which do not move.
const PUBLICATION_STATEMENT_MD5: &str = "unpinned";

/// M1: Project's publication statement keeps its text, and on this fixture its rows. The run
/// log carries the rows' digest, which the base and head runs must print alike.
#[tokio::test]
async fn publication_statement_is_unchanged() -> Result<()> {
    with_fixture("address_key_publication", async |connection| {
        Statistics::Plain.apply(connection).await?;
        let names = [
            0x7101_u128,
            0x7401,
            0x7402,
            0x7501,
            0x7601,
            0x7702,
            0x7802,
            0x7a05,
        ]
        .map(|k| format!("ens:0x{k:064x}"))
        .to_vec();
        let resources = [
            0x7101_u128,
            0x7102,
            0x7403,
            0x7701,
            0x7704,
            0x7801,
            0x7810,
            0x20001,
        ]
        .map(Uuid::from_u128)
        .to_vec();
        let mut rows = super::historical_history_relations(
            &mut *connection,
            MAINNET,
            BOUND,
            &names,
            &resources,
        )
        .await?
        .into_iter()
        .map(|row| {
            format!(
                "{} {} {:?} {:?} {}",
                row.address, row.namespace, row.logical_name_id, row.resource_id, row.relation_mask
            )
        })
        .collect::<Vec<_>>();
        rows.sort();
        ensure!(
            !rows.is_empty(),
            "the publication statement found no holding"
        );
        let (statement, digest): (String, String) = sqlx::query_as(
            "SELECT statement, md5(statement) FROM pg_prepared_statements
             WHERE statement LIKE '/* storage:history.publication_memberships */%'",
        )
        .fetch_one(&mut *connection)
        .await
        .context("the publication statement is not prepared on this connection")?;
        println!("PUBLICATION_STATEMENT_MD5 {digest}\n{statement}\nEND_PUBLICATION_STATEMENT");
        let rows_digest: String = sqlx::query_scalar("SELECT md5($1)")
            .bind(rows.join("\n"))
            .fetch_one(&mut *connection)
            .await?;
        println!(
            "PUBLICATION_ROWS_MD5 {rows_digest} {}\n{}",
            rows.len(),
            rows.join("\n")
        );
        ensure!(
            digest == PUBLICATION_STATEMENT_MD5,
            "the publication statement changed: md5 {digest}\n{statement}"
        );
        Ok(())
    })
    .await
}

/// M1: a Project publication of a small fixture writes the same `project_address_history_anchor`
/// rows before and after the fence. The run log carries their digest.
#[tokio::test]
async fn project_address_history_anchor_rows_digest() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("address_key_project").pool_max_connections(1),
    )
    .await?;
    let result = async {
        // One pooled connection, so the publication reads with this search_path.
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
        raw_sql(PROJECT_FIXTURE_SQL)
            .execute(&mut *connection)
            .await
            .context("install the Project fixture")?;
        drop(connection);
        super::family_test_support::publish(database.pool(), MAINNET, 1000).await?;
        let (digest, count): (String, i64) = sqlx::query_as(
            "SELECT md5(COALESCE(string_agg(row_text, E'\\n' ORDER BY row_text), '')), count(*)
             FROM (SELECT to_jsonb(anchor)::text AS row_text
                   FROM bigname_phase.project_address_history_anchor anchor) rows",
        )
        .fetch_one(database.pool())
        .await?;
        println!("PROJECT_ANCHOR_DIGEST {digest} {count}");
        ensure!(count > 0, "the publication wrote no address history anchor");
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

/// Every node outside correlated sub-plans and init plans; those run per row or once, and
/// are keyed there.
fn main_plan_nodes(plan: &Value) -> Vec<&Value> {
    fn walk<'a>(node: &'a Value, output: &mut Vec<&'a Value>) {
        if matches!(
            node["Parent Relationship"].as_str(),
            Some("SubPlan" | "InitPlan")
        ) {
            return;
        }
        output.push(node);
        for child in node["Plans"].as_array().into_iter().flatten() {
            walk(child, output);
        }
    }
    let mut output = Vec::new();
    walk(&plan[0]["Plan"], &mut output);
    output
}

fn text(node: &Value, key: &str) -> String {
    node[key].as_str().unwrap_or_default().to_owned()
}

/// The previous statement's failure: a `BitmapAnd` that combines the address probes with an
/// index keyed by event kind or by block.
fn ands_an_unkeyed_index(plan: &Value) -> bool {
    let nodes = main_plan_nodes(plan);
    nodes.iter().any(|node| node["Node Type"] == "BitmapAnd")
        && nodes.iter().any(|node| {
            matches!(
                node["Index Name"].as_str(),
                Some(
                    "normalized_events_projection_idx" | "normalized_events_chain_block_number_idx"
                )
            )
        })
}

/// The fenced statement reads `normalized_events` once per probed key, each through that key's
/// index alone with the key's expression in the index condition and nothing left to filter, and
/// reads `resources` by key per probe row. With no probe there is no scan of `normalized_events`
/// at all.
fn fenced_plan_failures(cell: &str, plan: &Value, form: Form) -> Vec<String> {
    let nodes = main_plan_nodes(plan);
    let mut failures = Vec::new();
    let mut probed = BTreeSet::new();
    for node in &nodes {
        let node_type = text(node, "Node Type");
        if node_type == "BitmapAnd" || node_type == "BitmapOr" {
            failures.push(format!("a combined bitmap ({node_type})"));
        }
        match node["Relation Name"].as_str() {
            Some("normalized_events") => {
                let (index, condition) = match node_type.as_str() {
                    "Index Scan" | "Index Only Scan" => {
                        (text(node, "Index Name"), text(node, "Index Cond"))
                    }
                    "Bitmap Heap Scan" => {
                        let children = node["Plans"].as_array().cloned().unwrap_or_default();
                        match children.as_slice() {
                            [child] if child["Node Type"] == "Bitmap Index Scan" => {
                                (text(child, "Index Name"), text(child, "Index Cond"))
                            }
                            _ => (String::new(), String::new()),
                        }
                    }
                    _ => (String::new(), String::new()),
                };
                match KEY_INDEXES.iter().find(|(_, name)| *name == index) {
                    Some((key, _)) => {
                        probed.insert(*key);
                        if !condition.contains(&format!("(after_state ->> '{key}'::text)")) {
                            failures.push(format!(
                                "{index} is not read by its key: Index Cond {condition}"
                            ));
                        }
                        // The value's length is the index predicate, so it must not be left as a
                        // filter on the rows the index returns.
                        if text(node, "Filter").contains("length(") {
                            failures.push(format!(
                                "{index}: the value's length is a filter, not the index \
                                 predicate: {}",
                                text(node, "Filter")
                            ));
                        }
                    }
                    None => failures.push(format!(
                        "normalized_events read by {node_type} {index} ({})",
                        text(node, "Alias")
                    )),
                }
            }
            Some("resources") => {
                let keyed = matches!(node_type.as_str(), "Index Scan" | "Index Only Scan")
                    && text(node, "Index Cond").contains("resource_id");
                if !keyed {
                    failures.push(format!(
                        "resources read by {node_type} {} without a probe row's key",
                        text(node, "Index Name")
                    ));
                }
            }
            _ => {}
        }
    }
    let expected = form.probed_keys();
    if probed != expected {
        failures.push(format!("probed keys {probed:?}, expected {expected:?}"));
    }
    if expected.is_empty()
        && !nodes
            .iter()
            .any(|node| node["Node Type"] == "Result" && text(node, "One-Time Filter") == "false")
    {
        failures.push("no one-time false Result".to_owned());
    }
    if failures.is_empty() {
        return failures;
    }
    vec![format!("{cell}: {}\n{plan}", failures.join("; "))]
}

async fn with_fixture(
    prefix: &str,
    check: impl AsyncFnOnce(&mut PgConnection) -> Result<()>,
) -> Result<()> {
    let database = TestDatabase::create_from_template(
        TestDatabaseConfig::new(prefix).pool_max_connections(1),
        "address_key_fixture",
        &[
            include_str!("../../schema/baseline/01_chain.sql").as_bytes(),
            include_str!("../../schema/baseline/02_raw_facts.sql").as_bytes(),
            include_str!("../../schema/baseline/03_identity.sql").as_bytes(),
            include_str!("../../schema/baseline/04_manifests.sql").as_bytes(),
            include_str!("../../schema/baseline/05_normalized_events.sql").as_bytes(),
            include_str!("../../schema/baseline/06_projections.sql").as_bytes(),
            FIXTURE_SQL.as_bytes(),
        ],
        build_template,
    )
    .await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        raw_sql("SET search_path TO bigname_phase, public; SET jit = off")
            .execute(&mut *connection)
            .await?;
        check(&mut connection).await
    }
    .await;
    database.cleanup().await?;
    result
}

async fn build_template(pool: PgPool) -> Result<()> {
    let mut connection = pool.acquire().await?;
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
    raw_sql(FIXTURE_SQL)
        .execute(&mut *connection)
        .await
        .context("install the address match fixture")?;
    Ok(())
}

/// Lineage for Mainnet blocks 1..=2000 and Sepolia blocks 1..=200, with one orphaned hash per
/// chain at block 120. Address k is k as 20 bytes of hex, name k has namehash k as 32 bytes and
/// resource k is uuid k. Autovacuum is off on every table, so the statistics are only the ones a
/// test collects.
const FIXTURE_SQL: &str = r#"
ALTER TABLE normalized_events SET (autovacuum_enabled = false);
ALTER TABLE resources SET (autovacuum_enabled = false);
ALTER TABLE chain_lineage SET (autovacuum_enabled = false);
ALTER TABLE name_surfaces SET (autovacuum_enabled = false);
ALTER TABLE token_lineages SET (autovacuum_enabled = false);
ALTER TABLE project_binding_candidate SET (autovacuum_enabled = false);
CREATE FUNCTION pg_temp.addr(k bigint) RETURNS text LANGUAGE sql IMMUTABLE
    AS $$ SELECT '0x' || lpad(to_hex(k), 40, '0') $$;
CREATE FUNCTION pg_temp.node(k bigint) RETURNS text LANGUAGE sql IMMUTABLE
    AS $$ SELECT '0x' || lpad(to_hex(k), 64, '0') $$;
CREATE FUNCTION pg_temp.rid(k bigint) RETURNS uuid LANGUAGE sql IMMUTABLE
    AS $$ SELECT lpad(to_hex(k), 32, '0')::uuid $$;
CREATE FUNCTION pg_temp.hash(chain text, block bigint) RETURNS text LANGUAGE sql IMMUTABLE
    AS $$ SELECT chain || ':' || block $$;
CREATE FUNCTION pg_temp.ev(
    identity text, block bigint, kind text, name bigint, resource bigint, state jsonb,
    family text DEFAULT 'ens_v2_registry_l1',
    derivation text DEFAULT 'ens_v2_registry_resource_surface',
    canon canonicality_state DEFAULT 'canonical',
    visibility text DEFAULT 'activated',
    chain text DEFAULT 'ethereum-mainnet',
    hash text DEFAULT NULL,
    space text DEFAULT 'ens',
    tx bigint DEFAULT NULL,
    raw jsonb DEFAULT '{}',
    lineage boolean DEFAULT false,
    resource_canon canonicality_state DEFAULT 'canonical'
) RETURNS void LANGUAGE sql AS $$
    INSERT INTO token_lineages (token_lineage_id, chain_id, block_hash, block_number,
        canonicality_state)
    SELECT pg_temp.rid(resource), chain, pg_temp.hash(chain, 1), 1, 'canonical'
    WHERE resource IS NOT NULL AND lineage
    ON CONFLICT DO NOTHING;
    INSERT INTO resources (resource_id, token_lineage_id, chain_id, block_hash, block_number,
        canonicality_state)
    SELECT pg_temp.rid(resource), CASE WHEN lineage THEN pg_temp.rid(resource) END, chain,
        pg_temp.hash(chain, 1), 1, resource_canon
    WHERE resource IS NOT NULL
    ON CONFLICT DO NOTHING;
    INSERT INTO name_surfaces (logical_name_id, namespace, namehash, labelhashes,
        normalizer_version, visibility_state, chain_id, block_hash, block_number,
        canonicality_state)
    SELECT space || ':' || pg_temp.node(name), space, pg_temp.node(name),
        ARRAY[pg_temp.node(name)], 'fixture', 'active', chain, pg_temp.hash(chain, 1), 1,
        'canonical'
    WHERE name IS NOT NULL
    ON CONFLICT DO NOTHING;
    INSERT INTO normalized_events (event_identity, namespace, logical_name_id, resource_id,
        event_kind, source_family, manifest_version, chain_id, block_number, block_hash,
        transaction_hash, transaction_index, log_index, raw_fact_ref, derivation_kind,
        canonicality_state, consumer_visibility, migration_correlation_ids, after_state)
    VALUES (identity, space,
        CASE WHEN name IS NOT NULL THEN space || ':' || pg_temp.node(name) END,
        pg_temp.rid(resource), kind, family, 1, chain, block,
        COALESCE(hash, pg_temp.hash(chain, block)),
        CASE WHEN tx IS NOT NULL THEN 'tx:' || identity END, tx,
        CASE WHEN tx IS NOT NULL THEN 0 END, raw, derivation, canon, visibility,
        CASE visibility WHEN 'candidate' THEN ARRAY['fixture'] ELSE ARRAY[]::text[] END,
        state);
$$;

INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
SELECT 'ethereum-mainnet', pg_temp.hash('ethereum-mainnet', n), n, to_timestamp(n),
    'canonical'::canonicality_state
FROM generate_series(1, 2000) n
UNION ALL
SELECT 'ethereum-sepolia', pg_temp.hash('ethereum-sepolia', n), n, to_timestamp(n), 'canonical'
FROM generate_series(1, 200) n
UNION ALL
SELECT chain, chain || ':orphan', 120, to_timestamp(120), 'orphaned'
FROM unnest(ARRAY['ethereum-mainnet', 'ethereum-sepolia']) chain;

-- Unrelated holdings: 400,000 rows across 100,000 holders and 60,000 resources, half of them
-- with a token lineage. A quarter are grants (registrant), a quarter token transfers (to), a
-- quarter registry owner transfers (owner) and a quarter surface bindings that also carry an
-- owner, as wrapper rows do. Grants and transfers alternate between the ENSv2 registry and
-- the ENSv1 registrar, so the wrap probe applies to half the grants. The padding gives the heap
-- the width of a live row.
INSERT INTO token_lineages (token_lineage_id, chain_id, block_hash, block_number,
    canonicality_state)
SELECT pg_temp.rid(16777216 + n), 'ethereum-mainnet', 'ethereum-mainnet:1', 1, 'canonical'
FROM generate_series(0, 29999) n;
INSERT INTO resources (resource_id, token_lineage_id, chain_id, block_hash, block_number,
    canonicality_state)
SELECT pg_temp.rid(16777216 + n), CASE WHEN n < 30000 THEN pg_temp.rid(16777216 + n) END,
    'ethereum-mainnet', 'ethereum-mainnet:1', 1, 'canonical'
FROM generate_series(0, 59999) n;
INSERT INTO normalized_events (event_identity, namespace, resource_id, event_kind, source_family,
    manifest_version, chain_id, block_number, block_hash, derivation_kind, canonicality_state,
    after_state)
SELECT 'noise:' || n, 'ens', pg_temp.rid(16777216 + n % 60000),
    (ARRAY['RegistrationGranted', 'TokenControlTransferred', 'AuthorityTransferred',
        'SurfaceBound'])[1 + n % 4],
    CASE WHEN n / 4 % 2 = 0 THEN 'ens_v2_registry_l1' ELSE 'ens_v1_registrar_l1' END,
    1, 'ethereum-mainnet', 1 + n % 2000, pg_temp.hash('ethereum-mainnet', 1 + n % 2000),
    CASE WHEN n / 4 % 2 = 0 THEN 'ens_v2_registry_resource_surface'
         ELSE 'ens_v1_unwrapped_authority' END,
    'canonical',
    jsonb_build_object(
        CASE n % 4 WHEN 0 THEN 'registrant' WHEN 1 THEN 'to' ELSE 'owner' END,
        '0x' || left(md5((n % 100000)::text), 40),
        'padding', repeat('p', 200))
FROM generate_series(0, 399999) n;

-- T: a grant, a token transfer and an owner transfer below the bound, a grant above it, and a
-- grant on Basenames.
SELECT pg_temp.ev('t:grant', 100, 'RegistrationGranted', 28929, 28929,
    jsonb_build_object('registrant', pg_temp.addr(28673)));
SELECT pg_temp.ev('t:transfer', 200, 'TokenControlTransferred', 28930, 28930,
    jsonb_build_object('to', pg_temp.addr(28673)));
SELECT pg_temp.ev('t:owner', 300, 'AuthorityTransferred', 28931, 28931,
    jsonb_build_object('owner', pg_temp.addr(28673)));
SELECT pg_temp.ev('t:above-bound', 1950, 'RegistrationGranted', 28932, 28932,
    jsonb_build_object('registrant', pg_temp.addr(28673)));
SELECT pg_temp.ev('t:basenames', 400, 'RegistrationGranted', 28933, 28933,
    jsonb_build_object('registrant', pg_temp.addr(28673)), space => 'basenames');

-- H: 700 grants, 150 token transfers and 650 owner transfers on 1,500 resources.
INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state)
SELECT pg_temp.rid(131072 + n), 'ethereum-mainnet', 'ethereum-mainnet:1', 1, 'canonical'
FROM generate_series(1, 1500) n;
INSERT INTO normalized_events (event_identity, namespace, resource_id, event_kind, source_family,
    manifest_version, chain_id, block_number, block_hash, derivation_kind, canonicality_state,
    after_state)
SELECT 'h:' || n, 'ens', pg_temp.rid(131072 + n), kind, 'ens_v2_registry_l1', 1,
    'ethereum-mainnet', 1 + n % 1800, pg_temp.hash('ethereum-mainnet', 1 + n % 1800),
    'ens_v2_registry_resource_surface', 'canonical',
    jsonb_build_object(key, pg_temp.addr(28674), 'padding', repeat('p', 200))
FROM generate_series(1, 1500) n,
    LATERAL (SELECT CASE WHEN n <= 700 THEN 'RegistrationGranted'
                         WHEN n <= 850 THEN 'TokenControlTransferred'
                         ELSE 'AuthorityTransferred' END AS kind,
                    CASE WHEN n <= 700 THEN 'registrant' WHEN n <= 850 THEN 'to'
                         ELSE 'owner' END AS key) arm;

-- M: a grant, a token transfer and an owner transfer on one name, an ENSv2 registry transfer of
-- a second name, and an ENSv2 root transfer of a resource with no name.
SELECT pg_temp.ev('m:grant', 100, 'RegistrationGranted', 29697, 29697,
    jsonb_build_object('registrant', pg_temp.addr(28676)));
SELECT pg_temp.ev('m:transfer', 101, 'TokenControlTransferred', 29697, 29697,
    jsonb_build_object('to', pg_temp.addr(28676)));
SELECT pg_temp.ev('m:owner', 102, 'AuthorityTransferred', 29697, 29697,
    jsonb_build_object('owner', pg_temp.addr(28676)));
SELECT pg_temp.ev('m:registry-transfer', 103, 'TokenControlTransferred', 29698, 29698,
    jsonb_build_object('to', pg_temp.addr(28676)));
SELECT pg_temp.ev('m:root-transfer', 104, 'TokenControlTransferred', NULL, 29699,
    jsonb_build_object('to', pg_temp.addr(28676)), family => 'ens_v2_root_l1');

-- M': one token transfer whose registrant, recipient and owner are all the address.
SELECT pg_temp.ev('m-prime:transfer', 110, 'TokenControlTransferred', 29953, 29953,
    jsonb_build_object('registrant', pg_temp.addr(28677), 'to', pg_temp.addr(28677),
        'owner', pg_temp.addr(28677)));

-- O: a surface binding that carries the address under every key. No arm reads its kind.
SELECT pg_temp.ev('o:surface', 120, 'SurfaceBound', 30209, 30209,
    jsonb_build_object('registrant', pg_temp.addr(28678), 'to', pg_temp.addr(28678),
        'owner', pg_temp.addr(28678)));

-- W: registrar grants to the NameWrapper's address. A same-transaction wrap linked to the
-- lease excludes the first, an orphaned wrap does not exclude the second, a wrap matched by
-- node excludes the third, the fourth has no wrap and an observed wrap excludes the fifth.
SELECT pg_temp.ev('w:grant:' || n, 500 + n, 'RegistrationGranted', 30464 + n, 30464 + n,
    jsonb_build_object('registrant', pg_temp.addr(28679), 'namehash', pg_temp.node(30464 + n)),
    family => 'ens_v1_registrar_l1', derivation => 'ens_v1_unwrapped_authority', tx => 1,
    lineage => true)
FROM generate_series(1, 5) n;
SELECT pg_temp.ev('w:wrap:' || n, 500 + n, 'SurfaceBound', NULL, 30480 + n, state,
    family => 'ens_v1_wrapper_l1', derivation => 'ens_v1_unwrapped_authority', canon => canon,
    tx => 1, raw => jsonb_build_object('emitting_address', pg_temp.addr(28679)))
FROM (VALUES
    (1, jsonb_build_object('wrapped_registrar_resource_id', pg_temp.rid(30465)::text),
        'canonical'::canonicality_state),
    (2, jsonb_build_object('wrapped_registrar_resource_id', pg_temp.rid(30466)::text),
        'orphaned'),
    (3, jsonb_build_object('source_event', 'NameWrapped', 'node', pg_temp.node(30467)),
        'canonical'),
    (5, jsonb_build_object('wrapped_registrar_resource_id', pg_temp.rid(30469)::text),
        'observed')
) wrap(n, state, canon);

-- L: owner transfers of registry-only resources. The second is handed to a BaseRegistrar
-- lease, the third is held by the Graveyard and the fourth reads a zero owner from the getter.
SELECT pg_temp.ev('l:owner:' || n, 600 + 10 * n, 'AuthorityTransferred', 30720 + n, 30720 + n,
    jsonb_build_object('owner', pg_temp.addr(28680)) || extra,
    family => 'ens_v1_registry_l1', derivation => 'ens_v1_unwrapped_authority')
FROM (VALUES
    (1, '{}'::jsonb),
    (2, '{}'),
    (3, '{"owner_getter_reason": "graveyard"}'),
    (4, jsonb_build_object('owner_getter', '0x' || repeat('0', 40)))
) owner(n, extra);
SELECT pg_temp.ev('l:lease', 590, 'RegistrationGranted', NULL, 30736,
    jsonb_build_object('registrant', pg_temp.addr(1)), family => 'ens_v1_registrar_l1',
    derivation => 'ens_v1_unwrapped_authority', lineage => true);
INSERT INTO project_binding_candidate (surface_binding_id, logical_name_id, namespace, chain_id,
    authority_arm, resource_id, binding_kind, canonicality_state, block_number, event_identity,
    registry_only, lease_resource_id)
VALUES (pg_temp.rid(30752), 'ens:' || pg_temp.node(30722), 'ens', 'ethereum-mainnet', 'ens_v1',
    pg_temp.rid(30722), 'declared_registry_path', 'canonical', 615, 'l:handoff', true,
    pg_temp.rid(30736));

-- K: a registrar grant of a resource with no token lineage, a registry owner transfer of a
-- resource with one, and an owner transfer whose owner word is unmasked.
SELECT pg_temp.ev('k:grant', 650, 'RegistrationGranted', 30977, 30977,
    jsonb_build_object('registrant', pg_temp.addr(28681)), family => 'ens_v1_registrar_l1',
    derivation => 'ens_v1_unwrapped_authority');
SELECT pg_temp.ev('k:owner', 651, 'AuthorityTransferred', 30978, 30978,
    jsonb_build_object('owner', pg_temp.addr(28681)), family => 'ens_v1_registry_l1',
    derivation => 'ens_v1_unwrapped_authority', lineage => true);
SELECT pg_temp.ev('k:unmasked', 652, 'AuthorityTransferred', 30979, 30979,
    jsonb_build_object('owner', pg_temp.addr(28681), 'owner_word_unmasked', 'true'));

-- R: grants on an orphaned row, an observed row, a canonical row on an orphaned block, a row
-- whose resource is orphaned, a row with no resource and a candidate row.
SELECT pg_temp.ev('r:orphaned', 700, 'RegistrationGranted', 31233, 31233,
    jsonb_build_object('registrant', pg_temp.addr(28682)), canon => 'orphaned');
SELECT pg_temp.ev('r:observed', 701, 'RegistrationGranted', 31234, 31234,
    jsonb_build_object('registrant', pg_temp.addr(28682)), canon => 'observed');
SELECT pg_temp.ev('r:orphaned-block', 120, 'RegistrationGranted', 31235, 31235,
    jsonb_build_object('registrant', pg_temp.addr(28682)), hash => 'ethereum-mainnet:orphan');
SELECT pg_temp.ev('r:orphaned-resource', 703, 'RegistrationGranted', 31236, 31236,
    jsonb_build_object('registrant', pg_temp.addr(28682)), resource_canon => 'orphaned');
SELECT pg_temp.ev('r:no-resource', 704, 'RegistrationGranted', 31237, NULL,
    jsonb_build_object('registrant', pg_temp.addr(28682)));
SELECT pg_temp.ev('r:candidate', 705, 'RegistrationGranted', 31238, 31238,
    jsonb_build_object('registrant', pg_temp.addr(28682)), visibility => 'candidate');

-- S: grants on Sepolia at blocks 150 and 50 and on Mainnet at block 100.
SELECT pg_temp.ev('s:sepolia-late', 150, 'RegistrationGranted', 31489, 31489,
    jsonb_build_object('registrant', pg_temp.addr(28683)), chain => 'ethereum-sepolia');
SELECT pg_temp.ev('s:mainnet', 100, 'RegistrationGranted', 31490, 31490,
    jsonb_build_object('registrant', pg_temp.addr(28683)));
SELECT pg_temp.ev('s:sepolia-early', 50, 'RegistrationGranted', 31491, 31491,
    jsonb_build_object('registrant', pg_temp.addr(28683)), chain => 'ethereum-sepolia');

-- X: 20,000 token transfers to one contract.
INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state)
SELECT pg_temp.rid(262144 + n), 'ethereum-mainnet', 'ethereum-mainnet:1', 1, 'canonical'
FROM generate_series(1, 20000) n;
INSERT INTO normalized_events (event_identity, namespace, resource_id, event_kind, source_family,
    manifest_version, chain_id, block_number, block_hash, derivation_kind, canonicality_state,
    after_state)
SELECT 'x:' || n, 'ens', pg_temp.rid(262144 + n), 'TokenControlTransferred',
    'ens_v2_registry_l1', 1, 'ethereum-mainnet', 1 + n % 1800,
    pg_temp.hash('ethereum-mainnet', 1 + n % 1800), 'ens_v2_registry_resource_surface',
    'canonical', jsonb_build_object('to', pg_temp.addr(28684), 'padding', repeat('p', 200))
FROM generate_series(1, 20000) n;

-- U: a grant whose registrant is stored in upper case.
SELECT pg_temp.ev('u:grant', 800, 'RegistrationGranted', 32001, 32001,
    jsonb_build_object('registrant', '0x' || upper('0000000000000000000000000000000000abcdef')));

-- N: one row of each arm kind whose key is present as JSON null.
SELECT pg_temp.ev('n:transfer', 810, 'TokenControlTransferred', 32257, 32257,
    '{"to": null}');
SELECT pg_temp.ev('n:grant', 811, 'RegistrationGranted', 32258, 32258, '{"registrant": null}');
SELECT pg_temp.ev('n:owner', 812, 'AuthorityTransferred', 32259, 32259, '{"owner": null}');

-- Long: a 3,000-character string that does not compress under each key. An unbounded key index
-- would refuse these inserts. They sit on surface bindings: the earlier kind-partial match
-- indexes, which stay installed, are unbounded too and would refuse one on their own kinds.
CREATE FUNCTION pg_temp.long(seed text) RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT left(string_agg(replace(encode(sha512((seed || ':' || i)::bytea), 'base64'), E'\n', ''),
        '' ORDER BY i), 3000)
    FROM generate_series(1, 40) i
$$;
SELECT pg_temp.ev('long:' || label, 820 + n, kind, 32512 + n, 32512 + n,
    jsonb_build_object(key, pg_temp.long(label)))
FROM (VALUES
    (1, 'to', 'SurfaceBound', 'to'),
    (2, 'registrant', 'SurfaceBound', 'registrant'),
    (3, 'owner', 'SurfaceBound', 'owner')
) long(n, label, kind, key);

-- Bare: 40 hex digits without 0x under each key.
SELECT pg_temp.ev('bare:' || label, 830 + n, kind, 32528 + n, 32528 + n,
    jsonb_build_object(key, '00000000000000000000000000000000000fedcb'))
FROM (VALUES
    (1, 'transfer', 'TokenControlTransferred', 'to'),
    (2, 'grant', 'RegistrationGranted', 'registrant'),
    (3, 'owner', 'AuthorityTransferred', 'owner')
) bare(n, label, kind, key);

-- Mixed: a recipient stored with mixed case, as a checksummed address prints.
SELECT pg_temp.ev('mixed:transfer', 840, 'TokenControlTransferred', 32545, 32545,
    jsonb_build_object('to', '0x00000000000000000000000000000000AbCdEf12'));
"#;

/// A small fixture for a Project publication: ENSv2 registry rows of the typical, multi-arm,
/// multi-key, other-kind and readability classes on Mainnet blocks 1..=1000, with raw names
/// the publication normalizes.
const PROJECT_FIXTURE_SQL: &str = r#"
CREATE FUNCTION pg_temp.addr(k bigint) RETURNS text LANGUAGE sql IMMUTABLE
    AS $$ SELECT '0x' || lpad(to_hex(k), 40, '0') $$;
CREATE FUNCTION pg_temp.node(k bigint) RETURNS text LANGUAGE sql IMMUTABLE
    AS $$ SELECT '0x' || lpad(to_hex(k), 64, '0') $$;
CREATE FUNCTION pg_temp.rid(k bigint) RETURNS uuid LANGUAGE sql IMMUTABLE
    AS $$ SELECT lpad(to_hex(k), 32, '0')::uuid $$;
INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
SELECT 'ethereum-mainnet', 'block-' || n, n, to_timestamp(n), 'canonical'::canonicality_state
FROM generate_series(1, 1000) n;
INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
    namehash, labelhashes, normalizer_version, visibility_state, chain_id, block_hash,
    block_number, canonicality_state)
SELECT 'ens:' || pg_temp.node(k), 'ens', 'name-' || k || '.eth', ARRAY['name-' || k, 'eth'],
    '\x00'::bytea, pg_temp.node(k), ARRAY[pg_temp.node(k), 'eth'], 'test', 'active',
    'ethereum-mainnet', 'block-1', 1, 'canonical'::canonicality_state
FROM unnest(ARRAY[28929, 28930, 28931, 29697, 29698, 29953, 30209, 31237, 31238]) k;
INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state)
SELECT pg_temp.rid(k), 'ethereum-mainnet', 'block-1', 1, 'canonical'::canonicality_state
FROM unnest(ARRAY[28929, 28930, 28931, 29697, 29698, 29699, 29953, 30209, 31238]) k;
INSERT INTO normalized_events (event_identity, namespace, logical_name_id, resource_id,
    event_kind, source_family, manifest_version, chain_id, block_hash, block_number,
    transaction_hash, transaction_index, log_index, derivation_kind, canonicality_state,
    consumer_visibility, migration_correlation_ids, after_state)
SELECT identity, 'ens', CASE WHEN name IS NOT NULL THEN 'ens:' || pg_temp.node(name) END,
    pg_temp.rid(resource), kind, family, 1, 'ethereum-mainnet', 'block-' || block, block,
    'tx-' || identity, 0, 0, 'ens_v2_registry_resource_surface', 'canonical'::canonicality_state,
    visibility, CASE visibility WHEN 'candidate' THEN ARRAY['fixture'] ELSE ARRAY[]::text[] END,
    state || jsonb_build_object('expiry', 1900000000)
FROM (VALUES
    ('t:grant', 28929, 28929, 'RegistrationGranted', 'ens_v2_registry_l1', 100, 'activated',
        jsonb_build_object('registrant', pg_temp.addr(28673))),
    ('t:transfer', 28930, 28930, 'TokenControlTransferred', 'ens_v2_registry_l1', 200,
        'activated', jsonb_build_object('to', pg_temp.addr(28673))),
    ('t:owner', 28931, 28931, 'AuthorityTransferred', 'ens_v2_registry_l1', 300, 'activated',
        jsonb_build_object('owner', pg_temp.addr(28673))),
    ('m:grant', 29697, 29697, 'RegistrationGranted', 'ens_v2_registry_l1', 400, 'activated',
        jsonb_build_object('registrant', pg_temp.addr(28676))),
    ('m:owner', 29697, 29697, 'AuthorityTransferred', 'ens_v2_registry_l1', 401, 'activated',
        jsonb_build_object('owner', pg_temp.addr(28676))),
    ('m:registry-transfer', 29698, 29698, 'TokenControlTransferred', 'ens_v2_registry_l1', 402,
        'activated', jsonb_build_object('to', pg_temp.addr(28676))),
    ('m:root-transfer', NULL, 29699, 'TokenControlTransferred', 'ens_v2_root_l1', 403,
        'activated', jsonb_build_object('to', pg_temp.addr(28676))),
    ('m-prime:transfer', 29953, 29953, 'TokenControlTransferred', 'ens_v2_registry_l1', 500,
        'activated', jsonb_build_object('registrant', pg_temp.addr(28677),
            'to', pg_temp.addr(28677), 'owner', pg_temp.addr(28677))),
    ('o:surface', 30209, 30209, 'SurfaceBound', 'ens_v2_registry_l1', 600, 'activated',
        jsonb_build_object('owner', pg_temp.addr(28678))),
    ('r:no-resource', 31237, NULL, 'RegistrationGranted', 'ens_v2_registry_l1', 700,
        'activated', jsonb_build_object('registrant', pg_temp.addr(28682))),
    ('r:candidate', 31238, 31238, 'RegistrationGranted', 'ens_v2_registry_l1', 701,
        'candidate', jsonb_build_object('registrant', pg_temp.addr(28682)))
) event(identity, name, resource, kind, family, block, visibility, state);
"#;

// The previous statement, frozen. Everything below is `address_evidence.rs` at ccc39d88a with
// the shared functions renamed and the bound address bound directly.
const ENS_V1_AUTHORITY_DERIVATION_KIND: &str = "ens_v1_unwrapped_authority";
const ENS_V2_REGISTRY_DERIVATION_KIND: &str = "ens_v2_registry_resource_surface";
const ADDRESS_HISTORY_MATCH_DERIVATION_KINDS: &[&str] = &[
    ENS_V1_AUTHORITY_DERIVATION_KIND,
    ENS_V2_REGISTRY_DERIVATION_KIND,
];
const ADDRESS_HISTORY_MATCH_EVENT_KINDS: &[&str] = &[
    "RegistrationGranted",
    "TokenControlTransferred",
    "AuthorityTransferred",
];

fn before_eligibility(
    builder: &mut QueryBuilder<'_, Postgres>,
    canonical_only: bool,
    include_candidates: bool,
) {
    push_history_lineage_join(builder);
    if include_candidates {
        builder.push(" WHERE ne.derivation_kind IN (");
    } else {
        builder.push(" WHERE ne.consumer_visibility = 'activated' AND ne.derivation_kind IN (");
    }
    let mut separated = builder.separated(", ");
    for derivation_kind in ADDRESS_HISTORY_MATCH_DERIVATION_KINDS {
        separated.push_bind(*derivation_kind);
    }
    separated.push_unseparated(") AND ne.event_kind IN (");
    let mut separated = builder.separated(", ");
    for event_kind in ADDRESS_HISTORY_MATCH_EVENT_KINDS {
        separated.push_bind(*event_kind);
    }
    separated.push_unseparated(")");

    push_history_canonicality_filter(builder, canonical_only);
    if canonical_only {
        builder.push(" AND (ne.resource_id IS NULL OR (TRUE ");
        push_readable_anchored_row_filter(builder, "r", "resource_lineage");
        builder.push("))");
    }
}

/// The statement as `address_evidence.rs` built it at ccc39d88a, before the fence: one scan of
/// `normalized_events` filtered by every arm.
fn push_before_query<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    address: &'a str,
    namespace: Option<&'a str>,
    relations: Option<&'a [AddressNameRelation]>,
    canonical_only: bool,
    include_candidates: bool,
    published: Option<&'a std::collections::BTreeMap<String, i64>>,
) {
    builder.push(
        r#"
        SELECT DISTINCT
            ne.logical_name_id,
            ne.resource_id
        FROM normalized_events ne
        LEFT JOIN resources r
          ON r.resource_id = ne.resource_id
        LEFT JOIN bigname_phase.chain_lineage resource_lineage
          ON resource_lineage.chain_id = r.chain_id
         AND resource_lineage.block_hash = r.block_hash
        "#,
    );
    before_eligibility(builder, canonical_only, include_candidates);

    if let Some(namespace) = namespace {
        builder.push(" AND ne.namespace = ");
        builder.push_bind(namespace);
    }

    if let Some(bounds) = published {
        if bounds.is_empty() {
            builder.push(" AND FALSE");
        } else {
            builder.push(" AND (");
            for (index, (chain, block)) in bounds.iter().enumerate() {
                if index > 0 {
                    builder.push(" OR ");
                }
                builder.push("(ne.chain_id = ");
                builder.push_bind(chain);
                builder.push(" AND ne.block_number <= ");
                builder.push_bind(*block);
                builder.push(")");
            }
            builder.push(")");
        }
    }
    builder.push(" AND ");
    before_match_filter(builder, address, relations);
}

fn before_match_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    address: &'a str,
    relations: Option<&'a [AddressNameRelation]>,
) {
    let include_token_holder =
        relations.is_none_or(|relations| relations.contains(&AddressNameRelation::TokenHolder));
    let include_controller = relations
        .is_none_or(|relations| relations.contains(&AddressNameRelation::EffectiveController));

    builder.push("(");
    let mut needs_or = false;
    if include_token_holder {
        before_registrant_filter(builder, address);
        builder.push(" OR ");
        before_token_holder_filter(builder, address);
        needs_or = true;
    }
    // A name with no token is owned by its registry owner, so `owner` also matches the registry
    // ownership transfers `manager` does, except from the position a registry-only binding that
    // stands for a BaseRegistrar lease opens: the lease's holder owns the name after a transfer
    // without `reclaim`, and a released lease has no owner. The registry-only resource is one
    // per node, so an earlier tokenless owner of it keeps its history.
    if include_token_holder || include_controller {
        if needs_or {
            builder.push(" OR ");
        }
        before_registry_owner_filter(builder, address, !include_controller);
        needs_or = true;
    }
    if include_controller {
        builder.push(" OR ");
        before_controller_filter(builder, address);
    }
    if !needs_or {
        builder.push("FALSE");
    }
    builder.push(")");
}

fn before_registrant_filter<'a>(builder: &mut QueryBuilder<'a, Postgres>, address: &'a str) {
    builder.push(
        r#"
        (
            (
                r.token_lineage_id IS NOT NULL
                OR ne.namespace =
        "#,
    );
    builder.push_bind("basenames");
    builder.push(" OR ne.derivation_kind = ");
    builder.push_bind(ENS_V2_REGISTRY_DERIVATION_KIND);
    builder.push(
        r#"
            )
            AND (
                (
                    ne.event_kind = 'RegistrationGranted'
                    AND LOWER(COALESCE(ne.after_state ->> 'registrant', '')) =
        "#,
    );
    builder.push_bind(address);
    // A BaseRegistrar lease registered straight into the NameWrapper names it as registrant, but
    // the wrapped token's holder owns the name; `registerAndWrapETH2LD` wraps it in the same
    // transaction. A receiver that unwraps in its mint callback leaves that wrap with no registrar
    // link, so the wrap's node also identifies it. Grants of other families are not this custody.
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289-L304 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L894-L902 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L257-L265 @ ens_v1@91c966f)
    builder.push(
        r#"
                    AND NOT EXISTS (
                        SELECT 1 FROM normalized_events wrap
                        WHERE ne.source_family = 'ens_v1_registrar_l1'
                          AND wrap.chain_id = ne.chain_id
                          AND wrap.block_number = ne.block_number
                          AND wrap.transaction_index IS NOT DISTINCT FROM ne.transaction_index
                          AND wrap.source_family = 'ens_v1_wrapper_l1'
                          AND (
                              wrap.after_state ->> 'wrapped_registrar_resource_id'
                                  = ne.resource_id::text
                              OR (
                                  wrap.after_state ->> 'source_event' = 'NameWrapped'
                                  AND LOWER(wrap.after_state ->> 'node')
                                      = LOWER(ne.after_state ->> 'namehash')
                              )
                          )
                          AND LOWER(wrap.raw_fact_ref ->> 'emitting_address')
                              = LOWER(ne.after_state ->> 'registrant')
                          AND wrap.canonicality_state
                              <> 'orphaned'::bigname_phase.canonicality_state
                    )
                )
            )
        )
        "#,
    );
}

fn before_token_holder_filter<'a>(builder: &mut QueryBuilder<'a, Postgres>, address: &'a str) {
    builder.push(
        r#"
        (
            (
                r.token_lineage_id IS NOT NULL
                OR ne.namespace =
        "#,
    );
    builder.push_bind("basenames");
    builder.push(" OR ne.derivation_kind = ");
    builder.push_bind(ENS_V2_REGISTRY_DERIVATION_KIND);
    builder.push(
        r#"
            )
            AND (
                (
                    ne.event_kind = 'TokenControlTransferred'
                    AND LOWER(COALESCE(ne.after_state ->> 'to', '')) =
        "#,
    );
    builder.push_bind(address);
    builder.push(
        r#"
                )
            )
        )
        "#,
    );
}

/// An ENSv2 registry moves the sender's roles on a name to the recipient with its token, so the
/// recipient of an ENSv2 registry or root registry transfer is the name's manager. A transfer
/// with no name, such as one inside a registry with no path to a root, moves no `manager`, as a
/// controller candidate needs a name. The arm has the event kind and the expression of the
/// token-holder match index, so it reads that index.
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L517-L547 @ ens_v2_sepolia_20261001@07e55a05)
fn before_controller_filter<'a>(builder: &mut QueryBuilder<'a, Postgres>, address: &'a str) {
    builder.push(
        r#"
        (
            ne.event_kind = 'TokenControlTransferred'
            AND ne.source_family IN ('ens_v2_registry_l1', 'ens_v2_root_l1')
            AND ne.logical_name_id IS NOT NULL
            AND LOWER(COALESCE(ne.after_state ->> 'to', '')) =
        "#,
    );
    builder.push_bind(address);
    builder.push(")");
}

fn before_registry_owner_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    address: &'a str,
    owner_only: bool,
) {
    builder.push(
        r#"
        (
            (
                r.token_lineage_id IS NULL
                OR ne.derivation_kind =
        "#,
    );
    builder.push_bind(ENS_V2_REGISTRY_DERIVATION_KIND);
    builder.push(
        r#"
            )
            AND ne.event_kind = 'AuthorityTransferred'
            AND (ne.after_state ->> 'owner_word_unmasked' = 'true') IS NOT TRUE
            AND LOWER(COALESCE(ne.after_state ->> 'owner', '')) =
        "#,
    );
    builder.push_bind(address);
    if owner_only {
        // As the served owner: a write the admitted Graveyard holds, or one the registry getter
        // reports as zero, names no owner.
        builder.push(
            r#"
            AND (ne.after_state ->> 'owner_getter_reason') IS DISTINCT FROM 'graveyard'
            AND LOWER(COALESCE(ne.after_state ->> 'owner_getter', ''))
                <> '0x0000000000000000000000000000000000000000'
            AND NOT EXISTS (
                SELECT 1
                FROM bigname_phase.project_binding_candidate handoff
                WHERE handoff.chain_id = ne.chain_id
                  AND handoff.resource_id = ne.resource_id
                  AND handoff.registry_only
                  AND ROW(handoff.block_number, COALESCE(handoff.transaction_index, -1),
                          COALESCE(handoff.log_index, -1))
                      <= ROW(ne.block_number, COALESCE(ne.transaction_index, -1),
                             COALESCE(ne.log_index, -1))
                  AND EXISTS (
                      SELECT 1 FROM normalized_events lease
                      WHERE lease.resource_id = handoff.lease_resource_id
                        AND lease.source_family = 'ens_v1_registrar_l1'
                        AND lease.canonicality_state <> 'orphaned'::bigname_phase.canonicality_state
                  )
            )
            "#,
        );
    }
    builder.push(")");
}
