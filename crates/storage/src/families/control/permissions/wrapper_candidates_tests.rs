//! The WrapperRegistry sources of the candidate-key statements. A read bound to one published
//! resource, and the read of a page's resources, start from the resources' root grants. They
//! reach each WrapperRegistry's parent row, the parent entry and that entry owner's approvals
//! by key, whatever the approvals' statistics say. Every other read sends the text it sent
//! before.
//!
//! The fixture is a chain just after the ENSv2 activation: 700 WrapperRegistries under one
//! parent registry, whose entries 148 owners hold but for three special ones, beside 100,000
//! entries of other registries and 100,000 other grants. The fixture is analyzed with a
//! statistics target whose sample holds every row of every table, so the estimates are the
//! same on every run. The approvals table was analyzed before its first `ens_v2_registry` row,
//! and autovacuum is off on it, so the planner estimates those approvals at about none. The
//! fixture checks that in `pg_stats` before anything is planned. Sequential scans stay enabled
//! and no `enable_*` switch is set, so every plan is the planner's own choice.
//!
//! Each source is planned on its own, cut out of the statement the reader sends: a source's
//! plan is that of its leg of the union. Each statement as sent is planned as well.

use std::collections::BTreeMap;

use anyhow::{Context, Result, ensure};
use sqlx::{PgConnection, raw_sql};
use uuid::Uuid;

use super::{Key, TokenRead, page, resource_page_statement, resource_pages, statement};
use crate::{
    PermissionsCurrentAccountResourceCursor as Cursor,
    families::id_index_plan_tests::{PLAN_MODES, explain_execute, with_database},
};

const CHAIN: &str = "ethereum-sepolia";
const OTHER_CHAIN: &str = "base-sepolia";
/// The parent registry of every WrapperRegistry of the fixture.
const PARENT: &str = "0x00000000000000000000000000000000000000ee";
const REGISTRY_A: &str = "0x00000000000000000000000000000000000000aa";
const WRAPPERS: i64 = 700;
const OTHER_WRAPPERS: i64 = 10;
const OWNERS: i64 = 148;

const WRAPPER_BASE: i64 = 0x10_0000;
const OWNER_BASE: i64 = 0x20_0000;
const OPERATOR_BASE: i64 = 0x30_0000;
const ROOT: u128 = 4;
const TOKEN: u128 = 5;
const OTHER_ROOT: u128 = 7;

/// The parameter types of a statement for one resource, and for the resources of a page.
const BOUND_TYPES: &str = "text, uuid, text, text, uuid, text, bigint, uuid, text";
const PAGE_TYPES: &str = "text, uuid[], text, text, uuid, text, bigint, uuid, text";

/// The WrapperRegistry sources of a read bound to one published resource: the owner source,
/// then the operator source.
fn bound_sources(_subject: bool) -> [String; 2] {
    sources_before()
}

/// The WrapperRegistry sources of the statement for a page's resources.
fn page_sources() -> [String; 2] {
    [
        super::super::wrapper_registry::page_owner_candidates(),
        super::super::wrapper_registry::page_operator_candidates(),
    ]
}

/// The WrapperRegistry sources every read sent before a read bound to a resource started from
/// it. Reads that are not bound to a published resource still send them.
fn sources_before() -> [String; 2] {
    [
        super::super::wrapper_registry::owner_candidates(),
        super::super::wrapper_registry::operator_candidates(),
    ]
}

/// The page sources before they started from the page's root grants: the approvals or the
/// entries first, with the page's resources bound only by the statement around them.
fn page_sources_before() -> [String; 2] {
    let parent_root = "JOIN bigname_phase.project_ens_v2_registry_parent parent
      ON parent.chain_id = entry.chain_id AND parent.parent = entry.registry
     AND parent.parent_entry_key = entry.entry_key
    JOIN bigname_phase.project_grant virtual_grant
      ON virtual_grant.chain_id = parent.chain_id AND virtual_grant.subject = parent.parent
     AND virtual_grant.scope = 'root'
     AND virtual_grant.scope_detail ->> 'registry_address' = parent.registry
    JOIN bigname_phase.project_family_marker marker ON marker.chain_id = entry.chain_id
    WHERE entry.status = 'registered' AND entry.owner IS NOT NULL
      AND entry.expiry > extract(epoch FROM marker.block_timestamp)";
    [
        format!(
            "SELECT entry.owner AS subject, virtual_grant.resource_id, 'root'::text AS scope
             FROM bigname_phase.project_ens_v2_entry_owner entry {parent_root}
             AND entry.chain_id = $9"
        ),
        format!(
            "SELECT approval.subject, virtual_grant.resource_id,
             concat('account:', approval.chain_id, ':ens_v2_registry:',
                 approval.authority_contract, ':', approval.owner) AS scope
             FROM bigname_phase.project_account_approval approval
             JOIN bigname_phase.project_ens_v2_entry_owner entry
               ON entry.chain_id = approval.chain_id
              AND entry.registry = approval.authority_contract AND entry.owner = approval.owner
             {parent_root}
             AND entry.chain_id = $9
             AND approval.authority_kind = 'ens_v2_registry'
             AND approval.relation_kind = 'operator' AND approval.approved
             AND approval.subject <> approval.owner"
        ),
    ]
}

/// `sql` with the sources a read bound to a published resource sends, or those of a page,
/// replaced by the ones it sent before.
fn before_statement(sql: &str, subject: bool) -> String {
    let mut sql = sql.to_owned();
    for (now, before) in bound_sources(subject)
        .into_iter()
        .zip(sources_before())
        .chain(page_sources().into_iter().zip(page_sources_before()))
    {
        sql = sql.replace(&now, &before);
    }
    sql
}

/// The leg of the union in `sql` whose source is `source`, as the statement sends it.
fn arm_of(sql: &str, source: &str) -> Result<String> {
    let at = sql
        .find(source)
        .context("the statement does not send the source")?;
    let start = sql[..at]
        .rfind("(SELECT DISTINCT candidate.")
        .context("no leg opens before the source")?;
    let end = at
        + source.len()
        + sql[at + source.len()..]
            .find("LIMIT $7)")
            .context("no leg closes after the source")?
        + "LIMIT $7)".len();
    Ok(sql[start..end].to_owned())
}

/// A lookup a plan must make: a scan of one of `indexes` whose index condition compares
/// everything in `has` and nothing in `lacks`.
#[derive(Clone, Copy)]
struct Step {
    label: &'static str,
    indexes: &'static [&'static str],
    has: &'static [&'static str],
    lacks: &'static [&'static str],
}

/// The read's root grants, found by the resource. A lookup that also compares the subject
/// is reached from a parent row instead.
const ROOT_GRANTS: Step = Step {
    label: "root grants by resource",
    indexes: &["project_grant_pkey", "project_grant_resource_subject_idx"],
    has: &["(resource_id = "],
    lacks: &["(subject = "],
};
const PAGE_ROOT_GRANTS: Step = Step {
    label: "root grants by the page's resources",
    indexes: &["project_grant_pkey", "project_grant_resource_subject_idx"],
    has: &["(resource_id = ANY "],
    lacks: &["(subject = "],
};
const PARENT_ROW: Step = Step {
    label: "parent row by registry",
    indexes: &["project_ens_v2_registry_parent_pkey"],
    has: &["(registry = "],
    lacks: &[],
};
const PARENT_ENTRY: Step = Step {
    label: "parent entry by key",
    indexes: &["project_ens_v2_entry_owner_pkey"],
    has: &["(registry = ", "(entry_key = "],
    lacks: &[],
};
const OWNER_APPROVALS: Step = Step {
    label: "approvals by the entry owner",
    indexes: &["project_account_approval_pkey"],
    has: &[
        "(authority_kind = 'ens_v2_registry'",
        "(authority_contract = ",
        "(owner = ",
    ],
    lacks: &[],
};
/// With a subject, the approvals of that subject by the entry owner: through the primary key,
/// or through the subject's own index.
const SUBJECT_APPROVAL: Step = Step {
    label: "the subject's approvals by key",
    indexes: &[
        "project_account_approval_pkey",
        "project_account_approval_subject_idx",
    ],
    has: &["(subject = ", "(authority_kind = 'ens_v2_registry'"],
    lacks: &[],
};

const WRAPPER_TABLES: [&str; 4] = [
    "project_grant",
    "project_ens_v2_registry_parent",
    "project_ens_v2_entry_owner",
    "project_account_approval",
];

fn missing_steps(plan: &[String], steps: &[Step]) -> Vec<&'static str> {
    steps
        .iter()
        .filter(|step| {
            !plan.windows(2).any(|lines| {
                step.indexes.iter().any(|index| {
                    lines[0].contains(&format!("using {index} on"))
                        || lines[0].ends_with(&format!("on {index}"))
                }) && lines[1].contains("Index Cond:")
                    && step.has.iter().all(|value| lines[1].contains(value))
                    && !step.lacks.iter().any(|value| lines[1].contains(value))
            })
        })
        .map(|step| step.label)
        .collect()
}

/// The scans of the entry owner index whose condition does not compare the owner: each one
/// walks the chain's whole index.
fn unkeyed_owner_scans(plan: &[String]) -> Vec<String> {
    plan.iter()
        .enumerate()
        .filter(|(_, line)| line.contains("project_ens_v2_entry_owner_owner_idx"))
        .filter(|(index, _)| {
            !plan.get(index + 1).is_some_and(|condition| {
                condition.contains("Index Cond:") && condition.contains("(owner = ")
            })
        })
        .map(|(_, line)| line.trim().to_owned())
        .collect()
}

/// For each wrapper table, the index conditions that bound a scan of it to keyed rows. A scan
/// that compares only the chain, or the chain and a constant kind or scope, reads the chain's
/// whole index.
const TABLE_KEYS: [(&str, &[&str]); 4] = [
    ("project_grant", &["(resource_id = ", "(subject = "]),
    (
        "project_ens_v2_entry_owner",
        &["(entry_key = ", "(owner = ", "(resource_id = "],
    ),
    ("project_ens_v2_registry_parent", &["(registry = "]),
    ("project_account_approval", &["(owner = ", "(subject = "]),
];

/// The steps of `plan`, outside its subplans, that are not bounded by a key: a merge or hash
/// join, which the keyed shape never needs, a sequential scan of a wrapper table, and an index
/// scan of one whose condition compares none of its keys. An entry primary-key probe by the
/// registry alone is one. The subplans are the namespace check of the statement around a
/// source, which no wrapper table is read in.
fn unbounded_steps(plan: &[String]) -> Vec<String> {
    let mut steps = Vec::new();
    let mut subplan: Option<usize> = None;
    for (index, line) in plan.iter().enumerate() {
        let trimmed = line.trim_start();
        let depth = line.len() - trimmed.len();
        if subplan.is_some_and(|start| depth > start) {
            continue;
        }
        subplan = None;
        if trimmed.starts_with("SubPlan") || trimmed.starts_with("hashed SubPlan") {
            subplan = Some(depth);
            continue;
        }
        let node = trimmed.trim_start_matches("->  ");
        if (node.starts_with("Hash ") || node.starts_with("Merge ")) && node.contains(" Join") {
            steps.push(trimmed.to_owned());
            continue;
        }
        let scanned = if trimmed.contains("Index") && trimmed.contains(" using ") {
            trimmed.split_once(" using ").map(|(_, rest)| rest)
        } else {
            trimmed
                .split_once("Bitmap Index Scan on ")
                .map(|(_, rest)| rest)
        }
        .and_then(|rest| rest.split(' ').next());
        for (table, keys) in TABLE_KEYS {
            if trimmed.contains(&format!("Seq Scan on {table} ")) {
                steps.push(trimmed.to_owned());
            }
            if scanned.is_some_and(|name| name.starts_with(&format!("{table}_")))
                && !plan.get(index + 1).is_some_and(|condition| {
                    condition.contains("Index Cond:")
                        && keys.iter().any(|key| condition.contains(key))
                })
            {
                steps.push(trimmed.to_owned());
            }
        }
    }
    steps
}

/// What a source keyed from its resources must not do: miss a step, read one of its tables
/// whole, or use the entry owner index at all.
fn unkeyed(cell: &str, plan: &[String], steps: &[Step]) -> Vec<String> {
    let mut failures: Vec<String> = missing_steps(plan, steps)
        .into_iter()
        .map(|step| format!("{cell}: no lookup of {step}"))
        .collect();
    for table in WRAPPER_TABLES {
        if plan
            .iter()
            .any(|line| line.contains(&format!("Seq Scan on {table} ")))
        {
            failures.push(format!("{cell}: reads {table} whole"));
        }
    }
    if plan
        .iter()
        .any(|line| line.contains("project_ens_v2_entry_owner_owner_idx"))
    {
        failures.push(format!("{cell}: walks the entry owner index"));
    }
    let unbounded = unbounded_steps(plan);
    if !unbounded.is_empty() {
        failures.push(format!("{cell}: steps not bounded by a key: {unbounded:?}"));
    }
    if failures.is_empty() {
        return failures;
    }
    vec![format!("{}\n{}", failures.join("\n"), shown(plan))]
}

/// The lookups of `steps` the plan of a source before makes. Each one would mean the fixture
/// does not tell that source and its replacement apart.
fn made_before(cell: &str, plan: &[String], steps: &[Step]) -> Option<String> {
    let missed = missing_steps(plan, steps);
    let made: Vec<&str> = steps
        .iter()
        .map(|step| step.label)
        .filter(|label| !missed.contains(label))
        .collect();
    (!made.is_empty()).then(|| {
        format!(
            "{cell}: the source before also makes the lookups of {made:?}\n{}",
            shown(plan)
        )
    })
}

/// A plan for a failure message, its lines cut short: the bound ids make them long.
fn shown(plan: &[String]) -> String {
    plan.iter()
        .map(|line| line.chars().take(220).collect::<String>())
        .collect::<Vec<_>>()
        .join("\n")
}

/// Plans `sql` prepared with `types`, executed with `values` in `mode`.
async fn explain(
    connection: &mut PgConnection,
    count: &mut usize,
    sql: &str,
    types: &str,
    values: &str,
    mode: &str,
) -> Result<Vec<String>> {
    *count += 1;
    let name = format!("wrapper_plan_{count}");
    raw_sql(&format!("PREPARE {name} ({types}) AS {sql}"))
        .execute(&mut *connection)
        .await
        .with_context(|| format!("prepare {name}"))?;
    let plan = explain_execute(connection, mode, &name, values).await?;
    raw_sql(&format!("DEALLOCATE {name}"))
        .execute(&mut *connection)
        .await?;
    Ok(plan)
}

#[derive(Clone, Debug)]
struct Read {
    label: &'static str,
    subject: Option<String>,
    resource: Uuid,
    /// The registry root of a token resource.
    root: Option<Uuid>,
    chain: &'static str,
}

impl Read {
    fn new(label: &'static str, resource: Uuid) -> Self {
        Self {
            label,
            subject: None,
            resource,
            root: None,
            chain: CHAIN,
        }
    }

    fn token(&self) -> TokenRead {
        TokenRead {
            chain_id: self.chain.to_owned(),
            root: self.root,
        }
    }

    fn values(&self) -> String {
        let quoted = |value: Option<String>| value.map_or("NULL".to_owned(), |v| format!("'{v}'"));
        format!(
            "{}, '{}', NULL, NULL, NULL, NULL, 64, {}, '{}'",
            quoted(self.subject.clone()),
            self.resource,
            quoted(self.root.map(|root| root.to_string())),
            self.chain
        )
    }
}

/// The reads bound to one resource the plans are checked for. Root 1 has 300 root holders and
/// 1,700 other grants. Entry 3's owner approved nobody and entry 4's owner approved 2,000
/// operators.
fn bound_reads() -> Vec<Read> {
    vec![
        Read::new("a resource with one root grant", root(2)),
        Read::new("a registry root with 300 root holders", root(1)),
        Read::new("a parent owner who approved nobody", root(3)),
        Read::new("a parent owner with 2,000 operators", root(4)),
        Read {
            subject: Some(operator(2)),
            ..Read::new("one operator on one resource", root(2))
        },
        Read {
            root: Some(root(2)),
            ..Read::new("a token resource of a registry root", token(2))
        },
    ]
}

#[tokio::test]
async fn wrapper_sources_reach_each_row_by_key_from_the_resources() -> Result<()> {
    with_database("wrapper_candidate_plan", async |connection| {
        install_wrapper_fixture(connection).await?;
        let mut count = 0;
        let mut failures = Vec::new();
        for read in bound_reads() {
            let subject = read.subject.as_deref();
            let token = read.token();
            let sql = statement(subject, Some(read.resource), None, Some(&token));
            let before = before_statement(&sql, subject.is_some());
            let approvals = if subject.is_some() {
                SUBJECT_APPROVAL
            } else {
                OWNER_APPROVALS
            };
            let [owners, operators] = bound_sources(subject.is_some());
            let [owners_before, operators_before] = sources_before();
            let owner_steps = [ROOT_GRANTS, PARENT_ROW, PARENT_ENTRY];
            let operator_steps = [ROOT_GRANTS, PARENT_ROW, PARENT_ENTRY, approvals];
            // With a subject, the operator source before also reads that subject's approvals
            // through the subject's index, which is a lookup by key as well.
            let operator_before_misses = if subject.is_some() {
                &operator_steps[..3]
            } else {
                &operator_steps[..]
            };
            let values = read.values();
            for mode in PLAN_MODES {
                // The lookups the source before misses on this fixture, which tell the two
                // sources apart. The operator source before starts from the approvals and
                // misses every lookup from the resource. The owner source before has no key
                // for the resource in a generic plan. A custom plan folds its `$2 IS NULL`, so
                // it starts from the root grant too, except at a root with 300 holders, where it
                // merge-joins every entry of the chain with every parent row.
                let owner_before_misses: &[Step] = match (mode, read.resource == root(1)) {
                    ("force_generic_plan", _) => &[ROOT_GRANTS],
                    (_, true) => &[PARENT_ROW, PARENT_ENTRY],
                    _ => &[],
                };
                for (arm, source, source_before, steps, before_misses) in [
                    (
                        "owner",
                        &owners,
                        &owners_before,
                        &owner_steps[..],
                        owner_before_misses,
                    ),
                    (
                        "operator",
                        &operators,
                        &operators_before,
                        &operator_steps[..],
                        operator_before_misses,
                    ),
                ] {
                    let cell = format!("{}, {arm} source, {mode}", read.label);
                    let plan = explain(
                        connection,
                        &mut count,
                        &arm_of(&sql, source)?,
                        BOUND_TYPES,
                        &values,
                        mode,
                    )
                    .await?;
                    failures.extend(unkeyed(&cell, &plan, steps));
                    let plan = explain(
                        connection,
                        &mut count,
                        &arm_of(&before, source_before)?,
                        BOUND_TYPES,
                        &values,
                        mode,
                    )
                    .await?;
                    failures.extend(made_before(&cell, &plan, before_misses));
                    // At the root with 300 holders both sources before read rows no key
                    // bounds, in both plan modes: the owner source every root grant or every
                    // entry of the chain, the operator source every ENSv2 registry approval.
                    if read.resource == root(1) && unbounded_steps(&plan).is_empty() {
                        failures.push(format!(
                            "{cell}: the source before reads only keyed rows\n{}",
                            shown(&plan)
                        ));
                    }
                }
                // No source of the statement as sent walks the entry owner index unkeyed.
                let plan =
                    explain(connection, &mut count, &sql, BOUND_TYPES, &values, mode).await?;
                let scans = unkeyed_owner_scans(&plan);
                if !scans.is_empty() {
                    failures.push(format!(
                        "{}, {mode}: the statement walks the entry owner index without the \
                         owner: {scans:?}\n{}",
                        read.label,
                        shown(&plan)
                    ));
                }
            }
        }

        // The page's resources: 200 roots, among them every special entry.
        let resources: Vec<String> = (1..=200).map(|n| root(n).to_string()).collect();
        let values = format!(
            "NULL, '{{{}}}', NULL, NULL, NULL, NULL, 1001, NULL, '{CHAIN}'",
            resources.join(",")
        );
        let sql = resource_page_statement();
        let before = before_statement(&sql, false);
        let [owners, operators] = page_sources();
        let [owners_before, operators_before] = page_sources_before();
        let owner_steps = [PAGE_ROOT_GRANTS, PARENT_ROW, PARENT_ENTRY];
        let operator_steps = [PAGE_ROOT_GRANTS, PARENT_ROW, PARENT_ENTRY, OWNER_APPROVALS];
        for mode in PLAN_MODES {
            // The owner source before reaches the root grants through the statement's own
            // bound on the ids in a generic plan, and reads every parent row in a custom one.
            let owner_before_misses: &[Step] = if mode == "force_custom_plan" {
                &[PAGE_ROOT_GRANTS]
            } else {
                &[]
            };
            for (arm, source, source_before, steps, before_misses) in [
                (
                    "owner",
                    &owners,
                    &owners_before,
                    &owner_steps[..],
                    owner_before_misses,
                ),
                (
                    "operator",
                    &operators,
                    &operators_before,
                    &operator_steps[..],
                    &[PAGE_ROOT_GRANTS, OWNER_APPROVALS][..],
                ),
            ] {
                let cell = format!("200 resources, {arm} source, {mode}");
                let plan = explain(
                    connection,
                    &mut count,
                    &arm_of(&sql, source)?,
                    PAGE_TYPES,
                    &values,
                    mode,
                )
                .await?;
                failures.extend(unkeyed(&cell, &plan, steps));
                let plan = explain(
                    connection,
                    &mut count,
                    &arm_of(&before, source_before)?,
                    PAGE_TYPES,
                    &values,
                    mode,
                )
                .await?;
                failures.extend(made_before(&cell, &plan, before_misses));
            }
            let plan = explain(connection, &mut count, &sql, PAGE_TYPES, &values, mode).await?;
            let scans = unkeyed_owner_scans(&plan);
            if !scans.is_empty() {
                failures.push(format!(
                    "200 resources, {mode}: the statement walks the entry owner index without \
                     the owner: {scans:?}\n{}",
                    shown(&plan)
                ));
            }
        }

        // A read not bound to a published resource keeps its text. Its plan is the planner's
        // choice over that unchanged text, which moves with the sampled statistics, so it is
        // not asserted here.
        for (label, subject, resource) in [
            ("an address", Some(operator(2)), None),
            ("an unpublished resource", None, Some(root(2))),
            (
                "an address on an unpublished resource",
                Some(operator(2)),
                Some(root(2)),
            ),
        ] {
            let sql = statement(subject.as_deref(), resource, None, None);
            for source in sources_before() {
                ensure!(
                    sql.contains(&source),
                    "{label}: the statement no longer sends\n{source}"
                );
            }
        }
        ensure!(failures.is_empty(), "{}", failures.join("\n\n"));
        Ok(())
    })
    .await
}

#[tokio::test]
async fn wrapper_sources_keep_every_key_of_the_resources() -> Result<()> {
    with_database("wrapper_candidate_rows", async |connection| {
        install_wrapper_fixture(connection).await?;
        let scope =
            |chain: &str, owner: &str| format!("account:{chain}:ens_v2_registry:{PARENT}:{owner}");
        let key = |subject: &str, resource: Uuid, scope: &str| Key {
            subject: subject.to_owned(),
            resource_id: resource,
            scope: scope.to_owned(),
        };

        // Every read returns, page by page, the keys the sources before return.
        let mut reads = bound_reads();
        reads.extend([
            Read::new("an unregistered parent entry", root(5)),
            Read::new("an expired parent entry", root(6)),
            Read {
                subject: Some(owner(1)),
                ..Read::new("the parent owner on a registry root", root(1))
            },
            Read {
                subject: Some(operator(10_001)),
                ..Read::new("one of 2,000 operators", root(4))
            },
            Read {
                chain: OTHER_CHAIN,
                ..Read::new("a resource of another chain", other_root(1))
            },
        ]);
        reads.extend(
            (10..=WRAPPERS)
                .step_by(70)
                .map(|n| Read::new("a root", root(n))),
        );
        let mut read_keys = BTreeMap::new();
        for read in &reads {
            // 64 is the reader's batch. 1 continues every read from a cursor, and 501 cuts
            // inside those of the two roots with 2,000 keys or more.
            let heavy = [root(1), root(4)].contains(&read.resource);
            for limit in if heavy { [64, 501] } else { [64, 1] } {
                let mut all = Vec::new();
                let mut after: Option<Cursor> = None;
                loop {
                    let keys = page(
                        connection,
                        read.subject.as_deref(),
                        Some(read.resource),
                        None,
                        after.as_ref(),
                        limit,
                        Some(&read.token()),
                    )
                    .await?;
                    let before = keys_before(connection, read, after.as_ref(), limit).await?;
                    ensure!(
                        keys == before,
                        "{read:?}, limit {limit}, after {after:?}: keys differ from the \
                         sources before\n{keys:?}\n{before:?}"
                    );
                    let last = (keys.len() as i64) < limit;
                    after = keys.last().map(Cursor::from);
                    all.extend(keys);
                    if last {
                        break;
                    }
                }
                ensure!(
                    all.windows(2).all(|pair| pair[0] < pair[1]),
                    "{read:?}, limit {limit}: keys are not in order"
                );
                read_keys.insert((read.label, read.resource, limit), all);
            }
        }
        let keys_of =
            |label: &'static str, resource: Uuid| read_keys[&(label, resource, 64)].clone();

        let mut expected = vec![
            key(PARENT, root(2), "root"),
            key(&owner(2), root(2), "root"),
            key(&operator(2), root(2), &scope(CHAIN, &owner(2))),
            key(&operator(150), root(2), &scope(CHAIN, &owner(2))),
        ];
        expected.sort();
        ensure!(
            keys_of("a resource with one root grant", root(2)) == expected,
            "one root grant: {:?}",
            keys_of("a resource with one root grant", root(2))
        );
        let expected = vec![
            key(PARENT, root(3), "root"),
            key(&owner(500), root(3), "root"),
        ];
        ensure!(
            keys_of("a parent owner who approved nobody", root(3)) == expected,
            "no approvals: {:?}",
            keys_of("a parent owner who approved nobody", root(3))
        );
        let heavy = keys_of("a parent owner with 2,000 operators", root(4));
        let operators = heavy
            .iter()
            .filter(|key| key.scope == scope(CHAIN, &owner(600)))
            .count();
        ensure!(
            heavy.len() == 2_002 && operators == 2_000,
            "2,000 operators: {} keys, {operators} of them operators",
            heavy.len()
        );
        let expected = vec![key(&operator(2), root(2), &scope(CHAIN, &owner(2)))];
        ensure!(
            keys_of("one operator on one resource", root(2)) == expected,
            "one operator: {:?}",
            keys_of("one operator on one resource", root(2))
        );
        // A token resource lists its registry root's keys, and the root's holders.
        let mut expected = vec![
            key(PARENT, token(2), "root"),
            key(&owner(2), token(2), "root"),
            key(&operator(2), token(2), &scope(CHAIN, &owner(2))),
            key(&operator(150), token(2), &scope(CHAIN, &owner(2))),
        ];
        expected.sort();
        ensure!(
            keys_of("a token resource of a registry root", token(2)) == expected,
            "token resource: {:?}",
            keys_of("a token resource of a registry root", token(2))
        );
        let holders = keys_of("a registry root with 300 root holders", root(1));
        for wanted in [
            key(&owner(1), root(1), "root"),
            key(&operator(1), root(1), &scope(CHAIN, &owner(1))),
            key(&operator(149), root(1), &scope(CHAIN, &owner(1))),
        ] {
            ensure!(holders.contains(&wanted), "300 root holders: no {wanted:?}");
        }
        ensure!(
            holders.len() == 2_004,
            "300 root holders: {} keys",
            holders.len()
        );
        for (label, resource) in [
            ("an unregistered parent entry", root(5)),
            ("an expired parent entry", root(6)),
        ] {
            ensure!(
                keys_of(label, resource) == vec![key(PARENT, resource, "root")],
                "{label}: {:?}",
                keys_of(label, resource)
            );
        }
        ensure!(
            keys_of("a resource of another chain", other_root(1)).contains(&key(
                &operator(5_001),
                other_root(1),
                &scope(OTHER_CHAIN, &owner(1))
            )),
            "another chain: {:?}",
            keys_of("a resource of another chain", other_root(1))
        );

        // The statement for a page's resources returns the keys of the reads of one resource,
        // in resource order, on each chain and however the page is cut.
        let chains = BTreeMap::from([
            (CHAIN, (1..=40).map(root).collect::<Vec<_>>()),
            (OTHER_CHAIN, (1..=OTHER_WRAPPERS).map(other_root).collect()),
        ]);
        let mut expected = Vec::new();
        for (chain, resources) in &chains {
            for resource in resources {
                let read = Read {
                    chain,
                    ..Read::new("a page resource", *resource)
                };
                let mut after: Option<Cursor> = None;
                loop {
                    let keys = page(
                        connection,
                        None,
                        Some(*resource),
                        None,
                        after.as_ref(),
                        64,
                        Some(&read.token()),
                    )
                    .await?;
                    let last = keys.len() < 64;
                    after = keys.last().map(Cursor::from);
                    expected.extend(keys);
                    if last {
                        break;
                    }
                }
            }
        }
        expected.sort_by(|left, right| {
            (left.resource_id, &left.subject, &left.scope).cmp(&(
                right.resource_id,
                &right.subject,
                &right.scope,
            ))
        });
        ensure!(
            expected
                .iter()
                .any(|key| key.scope == scope(OTHER_CHAIN, &owner(1))),
            "the page has no operator key of another chain"
        );
        for limit in [1001, 64] {
            let mut all: Vec<Key> = Vec::new();
            loop {
                let keys = resource_pages(connection, &chains, all.last(), limit).await?;
                let last = keys.len() < limit;
                all.extend(keys);
                if last {
                    break;
                }
            }
            ensure!(
                all == expected,
                "page limit {limit}: keys differ from the per-resource reads"
            );
        }
        // The page statement before returns them too, chain by chain.
        let sql = resource_page_statement();
        let sql_before = before_statement(&sql, false);
        ensure!(sql_before != sql, "the page statement sends no page source");
        let mut before = Vec::new();
        for (chain, resources) in &chains {
            let mut after: Option<Key> = None;
            loop {
                let keys: Vec<Key> = sqlx::query_as(&sql_before)
                    .persistent(false)
                    .bind(None::<&str>)
                    .bind(resources)
                    .bind(None::<&str>)
                    .bind(after.as_ref().map(|key| key.subject.as_str()))
                    .bind(after.as_ref().map(|key| key.resource_id))
                    .bind(after.as_ref().map(|key| key.scope.as_str()))
                    .bind(1001_i64)
                    .bind(None::<Uuid>)
                    .bind(*chain)
                    .fetch_all(&mut *connection)
                    .await?;
                let last = keys.len() < 1001;
                after = keys.last().cloned();
                before.extend(keys);
                if last {
                    break;
                }
            }
        }
        before.sort_by(|left, right| {
            (left.resource_id, &left.subject, &left.scope).cmp(&(
                right.resource_id,
                &right.subject,
                &right.scope,
            ))
        });
        ensure!(
            before == expected,
            "the page statement before returns other keys"
        );
        Ok(())
    })
    .await
}

async fn keys_before(
    connection: &mut PgConnection,
    read: &Read,
    after: Option<&Cursor>,
    limit: i64,
) -> Result<Vec<Key>> {
    let subject = read.subject.as_deref();
    let sql = statement(subject, Some(read.resource), after, Some(&read.token()));
    let sql_before = before_statement(&sql, subject.is_some());
    ensure!(
        sql_before != sql,
        "{read:?}: the statement sends no bound source"
    );
    sqlx::query_as(&sql_before)
        .persistent(false)
        .bind(subject)
        .bind(Some(read.resource))
        .bind(None::<&str>)
        .bind(after.map(|key| key.subject.as_str()))
        .bind(after.map(|key| key.resource_id))
        .bind(after.map(|key| key.scope.as_str()))
        .bind(limit)
        .bind(read.root)
        .bind(read.chain)
        .fetch_all(connection)
        .await
        .map_err(Into::into)
}

fn address(base: i64, n: i64) -> String {
    format!("0x{:040x}", base + n)
}

fn owner(k: i64) -> String {
    address(OWNER_BASE, k)
}

fn operator(j: i64) -> String {
    address(OPERATOR_BASE, j)
}

fn id(prefix: u128, n: i64) -> Uuid {
    Uuid::from_u128((prefix << 64) | n as u128)
}

/// The registry root resource of WrapperRegistry `n`.
fn root(n: i64) -> Uuid {
    id(ROOT, n)
}

/// The resource of the parent entry of WrapperRegistry `n`.
fn token(n: i64) -> Uuid {
    id(TOKEN, n)
}

fn other_root(n: i64) -> Uuid {
    id(OTHER_ROOT, n)
}

/// WrapperRegistry `i` (`WRAPPER_BASE + i`) has parent registry `PARENT`, whose entry
/// `entry:i` has resource `token(i)` and owner `i % 148`, and a root resource `root(i)` with
/// `PARENT`'s virtual root grant. Entry 3's owner is 500, who approved nobody, entry 4's 600,
/// who approved 2,000 operators. Entry 5 is unregistered and entry 6 expired. Root 1 also has
/// 300 root holders and 1,700 other grants. Owner `i % 148` approved operator `i` for `i` up
/// to 170, and some approvals are revoked, self-approvals, not operators or of another
/// registry. The other chain has ten WrapperRegistries the same way, owner k approving
/// operator 5000 + k. The other registries' 100,000 entries are held by 5,000 owners, and
/// every seventh by one of the 148.
async fn install_wrapper_fixture(connection: &mut PgConnection) -> Result<()> {
    raw_sql(&format!(
        "ALTER TABLE project_account_approval SET (autovacuum_enabled = false);
         CREATE FUNCTION pg_temp.address(base bigint, n bigint) RETURNS text LANGUAGE sql
             IMMUTABLE AS $$ SELECT '0x' || lpad(to_hex(base + n), 40, '0') $$;
         CREATE FUNCTION pg_temp.id(prefix bigint, n bigint) RETURNS uuid LANGUAGE sql
             IMMUTABLE AS $$ SELECT (lpad(to_hex(prefix), 16, '0') || lpad(to_hex(n), 16, '0'))::uuid $$;

         INSERT INTO project_family_marker
             (chain_id, current_block_number, current_block_hash, block_timestamp, state)
         VALUES ('{CHAIN}', 100, 'block-100', to_timestamp(1800000000), 'live'),
                ('{OTHER_CHAIN}', 100, 'block-100', to_timestamp(1800000000), 'live');

         INSERT INTO project_ens_v2_registry_parent
             (chain_id, registry, parent, raw_label_hex, parent_entry_key, block_number,
              event_identity)
         SELECT chain, pg_temp.address({WRAPPER_BASE}, i), '{PARENT}', '0x' || to_hex(i),
                'entry:' || i, i, 'parent:' || chain || ':' || i
         FROM (VALUES ('{CHAIN}', {WRAPPERS}), ('{OTHER_CHAIN}', {OTHER_WRAPPERS}))
             chains (chain, wrappers),
             generate_series(1, wrappers) i;

         INSERT INTO project_ens_v2_entry_owner
             (chain_id, registry, entry_key, token_id, resource_id, status, owner, expiry,
              block_number, event_identity)
         SELECT chain, '{PARENT}', 'entry:' || i, i::text, pg_temp.id(token_prefix, i),
                CASE WHEN i = 5 THEN 'unregistered' ELSE 'registered' END,
                CASE i WHEN 3 THEN pg_temp.address({OWNER_BASE}, 500)
                       WHEN 4 THEN pg_temp.address({OWNER_BASE}, 600)
                       WHEN 5 THEN NULL
                       ELSE pg_temp.address({OWNER_BASE}, i % {OWNERS}) END,
                CASE WHEN i = 6 THEN 1 ELSE 4102444800 END, i, 'entry:' || chain || ':' || i
         FROM (VALUES ('{CHAIN}', {WRAPPERS}, {TOKEN}), ('{OTHER_CHAIN}', {OTHER_WRAPPERS}, 8))
             chains (chain, wrappers, token_prefix),
             generate_series(1, wrappers) i;

         INSERT INTO project_ens_v2_entry_owner
             (chain_id, registry, entry_key, token_id, resource_id, status, owner, expiry,
              block_number, event_identity)
         SELECT '{CHAIN}', pg_temp.address(5242880, n % 50), 'other:' || n, n::text,
                pg_temp.id(6, n), 'registered',
                CASE WHEN n % 7 = 0 THEN pg_temp.address({OWNER_BASE}, n % {OWNERS})
                     ELSE pg_temp.address(6291456, n % 5000) END,
                4102444800, n, 'other-entry:' || n
         FROM generate_series(1, 100000) n;

         INSERT INTO project_grant
             (chain_id, resource_id, subject, scope, scope_detail, block_number,
              event_identity, event_kind, effective_powers, revoked)
         SELECT chain, pg_temp.id(root_prefix, i), '{PARENT}', 'root',
                jsonb_build_object('registry_address', pg_temp.address({WRAPPER_BASE}, i)), i,
                'virtual:' || chain || ':' || i, 'RoleGranted', '[]'::jsonb, false
         FROM (VALUES ('{CHAIN}', {WRAPPERS}, {ROOT}), ('{OTHER_CHAIN}', {OTHER_WRAPPERS}, {OTHER_ROOT}))
             chains (chain, wrappers, root_prefix),
             generate_series(1, wrappers) i;

         INSERT INTO project_grant
             (chain_id, resource_id, subject, scope, scope_detail, block_number,
              event_identity, event_kind, effective_powers, revoked)
         SELECT '{CHAIN}', pg_temp.id({ROOT}, 1), pg_temp.address(4194304, k),
                CASE WHEN k <= 300 THEN 'root' ELSE 'registry' END,
                CASE WHEN k <= 300 THEN jsonb_build_object('registry_address',
                    pg_temp.address({WRAPPER_BASE}, 1)) END,
                k, 'holder:' || k, 'RoleGranted', '[]'::jsonb, false
         FROM generate_series(1, 2000) k;

         INSERT INTO project_grant
             (chain_id, resource_id, subject, scope, block_number, event_identity, event_kind,
              effective_powers, revoked)
         SELECT '{CHAIN}', pg_temp.id(0, n), pg_temp.address(0, n % 5000), 'owner', n,
                'grant:' || n, 'PermissionChanged', '[]'::jsonb, false
         FROM generate_series(1, 100000) n;

         INSERT INTO project_account_approval
             (chain_id, authority_kind, authority_contract, owner, subject, relation_kind,
              block_number, event_identity, approved)
         SELECT '{CHAIN}', CASE WHEN i <= 700 THEN 'registry' ELSE 'wrapper' END,
                '{REGISTRY_A}', pg_temp.address(0, i % {OWNERS}),
                pg_temp.address(50000, i), 'operator', i, 'approval:' || i,
                i <= 330 OR i > 700
         FROM generate_series(1, 1300) i;

         SET default_statistics_target = 1000;
         ANALYZE;

         INSERT INTO project_account_approval
             (chain_id, authority_kind, authority_contract, owner, subject, relation_kind,
              block_number, event_identity, approved)
         SELECT chain, 'ens_v2_registry', contract, owner, subject, relation, 1,
                concat_ws(':', 'ens-v2', chain, contract, owner, subject, relation), approved
         FROM (
             SELECT '{CHAIN}' AS chain, '{PARENT}' AS contract,
                    pg_temp.address({OWNER_BASE}, i % {OWNERS}) AS owner,
                    pg_temp.address({OPERATOR_BASE}, i) AS subject, 'operator' AS relation,
                    true AS approved
             FROM generate_series(1, 170) i
             UNION ALL
             SELECT '{CHAIN}', '{PARENT}', pg_temp.address({OWNER_BASE}, k),
                    pg_temp.address({OPERATOR_BASE}, 900 + k), 'operator', false
             FROM generate_series(1, 5) k
             UNION ALL
             SELECT '{CHAIN}', '{PARENT}', pg_temp.address({OWNER_BASE}, k),
                    pg_temp.address({OWNER_BASE}, k), 'operator', true
             FROM generate_series(1, 3) k
             UNION ALL
             SELECT '{CHAIN}', '{PARENT}', pg_temp.address({OWNER_BASE}, k),
                    pg_temp.address({OPERATOR_BASE}, 950 + k), 'manager', true
             FROM generate_series(1, 3) k
             UNION ALL
             SELECT '{CHAIN}', pg_temp.address({WRAPPER_BASE}, 1),
                    pg_temp.address({OWNER_BASE}, k),
                    pg_temp.address({OPERATOR_BASE}, 1000 + k), 'operator', true
             FROM generate_series(1, 20) k
             UNION ALL
             SELECT '{CHAIN}', '{PARENT}', pg_temp.address({OWNER_BASE}, 600),
                    pg_temp.address({OPERATOR_BASE}, 10000 + j), 'operator', true
             FROM generate_series(1, 2000) j
             UNION ALL
             SELECT '{OTHER_CHAIN}', '{PARENT}', pg_temp.address({OWNER_BASE}, k),
                    pg_temp.address({OPERATOR_BASE}, 5000 + k), 'operator', true
             FROM generate_series(1, {OTHER_WRAPPERS}) k
         ) approvals"
    ))
    .execute(&mut *connection)
    .await?;
    // The approvals' statistics predate their ENSv2 registry rows, as on a deployment analyzed
    // before the activation: a value outside the most common values estimates at about none.
    let (common, rows): (Option<String>, i64) = sqlx::query_as(
        "SELECT (SELECT most_common_vals::text FROM pg_stats
                 WHERE schemaname = 'bigname_phase' AND tablename = 'project_account_approval'
                   AND attname = 'authority_kind'),
                (SELECT count(*) FROM project_account_approval
                 WHERE authority_kind = 'ens_v2_registry')",
    )
    .fetch_one(&mut *connection)
    .await?;
    ensure!(
        common
            .as_deref()
            .is_some_and(|values| values.contains("registry") && !values.contains("ens_v2")),
        "the approvals' statistics know the ENSv2 registry rows: {common:?}"
    );
    ensure!(rows > 2_000, "{rows} ENSv2 registry approvals");
    Ok(())
}
