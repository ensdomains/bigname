//! The candidate-key statement of a permission page: which indexes a read bound to one resource
//! probes, and that starting its registry sources from the resource drops no served key.
//!
//! The statement for the resources of an address page is checked the same way: which indexes it
//! probes at 200 resources, and that it returns the keys the reads of one resource return, in
//! resource order, however the page is cut.
//!
//! The plan test sets `plan_cache_mode` to force the custom plan, which is how the unprepared
//! statement is planned, and the schema install turns `jit` off. No `enable_*` switch is set.
//! Sequential scans stay enabled, so a plan that reads a table whole or walks an index without
//! its leading chain is the planner's own choice. On the read's chain the plan fixture has
//! 100,000 observations, 190,000 binding candidates, 100,000 grants and 1,300 account
//! approvals, 330 of them approved registry operators of 148 owners. Another chain adds 2,000
//! observations and 2,000 binding candidates.

use std::collections::BTreeMap;

use anyhow::{Result, ensure};
use sqlx::{PgConnection, raw_sql};
use uuid::Uuid;

use super::{Key, TokenRead, page, resource_page_statement, resource_pages, statement};
use crate::{
    PermissionsCurrentAccountResourceCursor as Cursor,
    families::id_index_plan_tests::{Probe, explain_execute, missing_probes, with_database},
};

const CHAIN: &str = "ethereum-sepolia";
const OTHER_CHAIN: &str = "base-sepolia";
const REGISTRY_A: &str = "0x00000000000000000000000000000000000000aa";
const REGISTRY_B: &str = "0x00000000000000000000000000000000000000bb";
const WRAPPER_CONTRACT: &str = "0x00000000000000000000000000000000000000cc";
/// The index condition of a lookup keyed by the bound chain.
const ON_CHAIN: &str = "(chain_id = 'ethereum-sepolia'::text)";
const OBSERVATIONS: i64 = 100_000;
const NAMES: i64 = 40;

const DIRECT_BEFORE: &str = "SELECT grant_row.subject, grant_row.resource_id, grant_row.scope
    FROM bigname_phase.project_grant grant_row";
const WRAPPER_BEFORE: &str = "SELECT approval.subject, grant_row.resource_id, grant_row.scope
    FROM bigname_phase.project_account_approval approval
    JOIN bigname_phase.project_grant grant_row
      ON grant_row.chain_id = approval.chain_id AND grant_row.subject = approval.owner
    WHERE approval.authority_kind = 'wrapper' AND approval.approved
      AND grant_row.grant_source ->> 'authority_kind' = 'wrapper'
      AND grant_row.grant_source ->> 'relation_kind' = 'holder'
      AND lower(grant_row.grant_source ->> 'authority_contract') = approval.authority_contract";
const REGISTRY_BEFORE: &str = "FROM bigname_phase.project_account_approval approval
    JOIN bigname_phase.project_registry_binding_observation observation
      ON observation.chain_id = approval.chain_id
     AND observation.registry_contract = approval.authority_contract
     AND observation.registry_owner = approval.owner AND observation.applicable";
const APPROVED_BEFORE: &str = "approval.authority_kind = 'registry'
    AND approval.relation_kind = 'operator' AND approval.approved";
const ACCOUNT_SCOPE_BEFORE: &str = "concat('account:', approval.chain_id, ':registry:',
    approval.authority_contract, ':', approval.owner) AS scope";

/// The statement before the registry sources of a read bound to one resource started from
/// that resource: the text every read sent, with the ENSv2 and wrapper-registry sources it
/// still shares.
fn statement_before(
    subject: Option<&str>,
    resource_id: Option<Uuid>,
    after: Option<&Cursor>,
) -> String {
    let registry_target = format!(
        "SELECT approval.subject, observation.target_resource_id AS resource_id,
         {ACCOUNT_SCOPE_BEFORE} {REGISTRY_BEFORE} WHERE {APPROVED_BEFORE}"
    );
    let registry_resource = format!(
        "SELECT approval.subject, observation.resource_id,
         {ACCOUNT_SCOPE_BEFORE} {REGISTRY_BEFORE} WHERE {APPROVED_BEFORE}"
    );
    let registry_name = format!(
        "SELECT approval.subject, candidate.resource_id, {ACCOUNT_SCOPE_BEFORE} {REGISTRY_BEFORE}
         JOIN bigname_phase.project_binding_candidate candidate
           ON candidate.logical_name_id = observation.logical_name_id
         WHERE {APPROVED_BEFORE} AND observation.attributed_via = 'name'"
    );
    let seek = if subject
        .zip(after)
        .is_some_and(|(subject, after)| subject == after.subject)
    {
        "AND candidate.resource_id >= $5::uuid"
    } else if resource_id.is_some() && after.is_some() {
        "AND candidate.subject COLLATE \"C\" >= $4 COLLATE \"C\""
    } else {
        ""
    };
    let wrapper_owners = super::super::wrapper_registry::owner_candidates();
    let wrapper_operators = super::super::wrapper_registry::operator_candidates();
    let arms = [
        DIRECT_BEFORE, WRAPPER_BEFORE, &registry_target, &registry_resource, &registry_name,
        super::super::ens_v2::OPERATOR_CANDIDATES, super::super::ens_v2::ROOT_HOLDER_CANDIDATES,
        &wrapper_owners, &wrapper_operators,
    ]
        .into_iter().map(|source| format!(
            "(SELECT DISTINCT candidate.subject COLLATE \"C\" AS subject,
                    candidate.resource_id, candidate.scope COLLATE \"C\" AS scope
              FROM ({source}) candidate
              WHERE ($1::text IS NULL OR candidate.subject COLLATE \"C\" = $1 COLLATE \"C\")
                AND ($2::uuid IS NULL OR candidate.resource_id = $2) {seek}
                AND ($4::text IS NULL OR
                    (candidate.subject COLLATE \"C\", candidate.resource_id, candidate.scope COLLATE \"C\")
                        > ($4 COLLATE \"C\", $5::uuid, $6 COLLATE \"C\"))
                AND ($3::text IS NULL OR EXISTS (
                    SELECT 1 FROM bigname_phase.normalized_events event
                    JOIN bigname_phase.chain_lineage lineage
                      ON lineage.chain_id = event.chain_id AND lineage.block_hash = event.block_hash
                    WHERE event.resource_id = candidate.resource_id AND event.namespace = $3
                      AND event.consumer_visibility = 'activated'
                      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
                      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')))
              ORDER BY subject, resource_id, scope LIMIT $7)"
        )).collect::<Vec<_>>().join(" UNION ");
    format!(
        "/* storage:families.control.permissions.candidate_page */
         SELECT subject, resource_id, scope FROM ({arms}) candidates
         ORDER BY subject COLLATE \"C\", resource_id, scope COLLATE \"C\" LIMIT $7"
    )
}

#[tokio::test]
async fn resource_bound_read_probes_each_registry_source_by_resource() -> Result<()> {
    with_database("candidate_page_plan", async |connection| {
        install_plan_fixture(connection).await?;
        let probes = [
            Probe {
                index: "project_registry_binding_observation_target_idx",
                conditions: &[ON_CHAIN, "(target_resource_id = '"],
            },
            Probe {
                index: "project_registry_binding_observation_resource_idx",
                conditions: &[ON_CHAIN, "(resource_id = '"],
            },
            Probe {
                index: "project_binding_candidate_resource_idx",
                conditions: &[ON_CHAIN, "(resource_id = '"],
            },
            Probe {
                index: "project_registry_binding_observation_pkey",
                conditions: &["chain_id = ", "(observation_identity = "],
            },
        ];
        let token = TokenRead {
            chain_id: CHAIN.to_owned(),
            root: None,
        };
        // Observation 20 is named, of resource 20, and reaches another resource. Name 22 is
        // also bound to a second resource. Approval 20 is of observation 20's registry owner.
        let operator = account(50_000 + 20);
        let reads = [
            ("an observation's own resource", None, resource(20), true),
            ("the resource an observation reaches", None, moved(20), true),
            ("a second resource of a name", None, alias(22), true),
            // A subject narrows the approvals as well, so the planner may reach the
            // observations from them through an index that leads with the chain.
            (
                "one subject on one resource",
                Some(operator),
                resource(20),
                false,
            ),
        ];
        let mut failures = Vec::new();
        for (index, (label, subject, resource, from_resource)) in reads.iter().enumerate() {
            let subject = subject.as_deref();
            let sql = statement(subject, Some(*resource), None, Some(&token));
            let name = format!("candidate_page_{index}");
            let plan = custom_plan(connection, &name, &sql, subject, *resource).await?;
            let rendered = plan.join("\n");
            for table in [
                "project_registry_binding_observation",
                "project_binding_candidate",
            ] {
                if plan
                    .iter()
                    .any(|line| line.contains(&format!("Seq Scan on {table}")))
                {
                    failures.push(format!("{label}: reads {table} whole\n{rendered}"));
                }
            }
            failures.extend(
                chainless_index_scans(&plan)
                    .into_iter()
                    .map(|scan| format!("{label}: no chain in the condition of{scan}\n{rendered}")),
            );
            if !*from_resource {
                continue;
            }
            failures.extend(missing_probes(label, &plan, &probes));
            // The statement before has no lookup keyed by the bound chain to plan, so on the
            // same fixture it must miss every probe. A fixture or a probe that let it pass
            // would no longer tell the two statements apart.
            let sql = statement_before(subject, Some(*resource), None);
            let name = format!("candidate_page_before_{index}");
            let plan = custom_plan(connection, &name, &sql, subject, *resource).await?;
            let missed = missing_probes(label, &plan, &probes).len();
            if missed != probes.len() {
                failures.push(format!(
                    "{label}: the statement before misses {missed} of {} probes\n{}",
                    probes.len(),
                    plan.join("\n")
                ));
            }
        }
        ensure!(failures.is_empty(), "{}", failures.join("\n\n"));
        Ok(())
    })
    .await
}

/// The plan of a candidate-key statement for a read of one resource on the main chain, with no
/// namespace and no cursor. The API sends the statement unprepared, so it is planned with its
/// values on every call: the custom plan of a prepared statement.
async fn custom_plan(
    connection: &mut PgConnection,
    name: &str,
    sql: &str,
    subject: Option<&str>,
    resource: Uuid,
) -> Result<Vec<String>> {
    raw_sql(&format!(
        "PREPARE {name} (text, uuid, text, text, uuid, text, bigint, uuid, text) AS {sql}"
    ))
    .execute(&mut *connection)
    .await?;
    let subject = subject.map_or("NULL".to_owned(), |subject| format!("'{subject}'"));
    let values = format!("{subject}, '{resource}', NULL, NULL, NULL, NULL, 64, NULL, '{CHAIN}'");
    explain_execute(connection, "force_custom_plan", name, &values).await
}

/// The scans of an observation or binding-candidate index whose index condition does not
/// compare the chain. Every index of the two tables leads with the chain, except the candidate
/// table's primary key, which no source reads. So such a scan walks a whole index.
fn chainless_index_scans(plan: &[String]) -> Vec<String> {
    plan.iter()
        .enumerate()
        .filter(|(_, line)| {
            line.contains("Index")
                && line.contains("Scan")
                && (line.contains("project_registry_binding_observation_")
                    || line.contains("project_binding_candidate_"))
        })
        .filter(|(index, _)| {
            !plan.get(index + 1).is_some_and(|condition| {
                condition.contains("Index Cond:") && condition.contains("chain_id = ")
            })
        })
        .map(|(_, line)| line.clone())
        .collect()
}

#[tokio::test]
async fn resource_bound_candidate_page_drops_no_served_key() -> Result<()> {
    // The statement before is the oracle. Its name source joined on the name alone, so it
    // could also return a key scoped to another chain, which composition drops. No name of the
    // fixture is on two chains, so here the two statements must return the same keys.
    with_database("candidate_page_rows", async |connection| {
        install_row_fixture(connection).await?;
        let on = |chain: &str| TokenRead {
            chain_id: chain.to_owned(),
            root: None,
        };
        // Each resource with the chain the reader resolved for it, none when it is not published.
        let mut resources: Vec<(Uuid, Option<TokenRead>)> = (1..=NAMES)
            .map(|n| (resource(n), Some(on(CHAIN))))
            .collect();
        resources.extend((5..=NAMES).step_by(5).map(|n| (alias(n), Some(on(CHAIN)))));
        resources.extend(
            (10..=NAMES)
                .step_by(10)
                .map(|n| (moved(n), Some(on(CHAIN)))),
        );
        resources.extend((1..=6).map(|n| (other(n), Some(on(OTHER_CHAIN)))));
        resources.push((resource(NAMES + 5), Some(on(CHAIN))));
        resources.extend([resource(20), alias(20), moved(20)].map(|id| (id, None)));
        let subjects: Vec<String> = (0..8).map(account).chain((100..105).map(account)).collect();

        let mut reads: Vec<Read> = vec![Read::default()];
        reads.extend(resources.iter().map(|(resource, token)| Read {
            resource: Some(*resource),
            token: token.clone(),
            ..Read::default()
        }));
        reads.extend(subjects.iter().map(|subject| Read {
            subject: Some(subject.clone()),
            ..Read::default()
        }));
        // Every fourth resource, all unnamed, and a second resource of name 20, which only the
        // name source reaches.
        let named = (alias(20), Some(on(CHAIN)));
        for (resource, token) in resources.iter().step_by(4).chain([&named]) {
            reads.extend(subjects.iter().take(3).map(|subject| Read {
                subject: Some(subject.clone()),
                resource: Some(*resource),
                token: token.clone(),
            }));
        }

        let mut resource_bound_keys = 0;
        for read in &reads {
            // A read that is not bound to a published resource sends the text it sent before,
            // so its plan cannot have changed.
            if read.token.is_none() {
                ensure!(
                    statement(read.subject.as_deref(), read.resource, None, None)
                        == statement_before(read.subject.as_deref(), read.resource, None),
                    "{read:?}: the statement text changed"
                );
            }
            for namespace in [None, Some("ens"), Some("basenames")] {
                // 64 is the reader's batch. 3 makes most reads continue from a cursor.
                for limit in [64, 3] {
                    let mut after: Option<Cursor> = None;
                    loop {
                        let keys = page(
                            connection,
                            read.subject.as_deref(),
                            read.resource,
                            namespace,
                            after.as_ref(),
                            limit,
                            read.token.as_ref(),
                        )
                        .await?;
                        let before =
                            keys_before(connection, read, namespace, after.as_ref(), limit).await?;
                        ensure!(
                            keys == before,
                            "{read:?}, namespace {namespace:?}, limit {limit}, after {after:?}: \
                             keys differ from the statement before\n{keys:?}\n{before:?}"
                        );
                        if read.token.is_some() {
                            resource_bound_keys += keys.len();
                        }
                        if (keys.len() as i64) < limit {
                            break;
                        }
                        after = keys.last().map(Cursor::from);
                    }
                }
            }
        }
        ensure!(
            resource_bound_keys > 0,
            "no resource-bound read returned a key"
        );

        // The resource observation 20 reaches comes from the target source alone, and a second
        // resource of its name from the name source alone. Its own resource comes from the
        // resource source and the name source, which return the same keys, and also has a
        // direct grant.
        let operators = [account(0), account(1)];
        for (label, id, direct) in [
            ("reached", moved(20), false),
            ("through the name", alias(20), false),
            ("own", resource(20), true),
        ] {
            let keys = page(connection, None, Some(id), None, None, 64, Some(&on(CHAIN))).await?;
            let scope = format!("account:{CHAIN}:registry:{REGISTRY_A}:{}", account(0));
            let mut expected: Vec<(String, String)> = operators
                .iter()
                .map(|subject| (subject.clone(), scope.clone()))
                .collect();
            if direct {
                expected.insert(1, (account(0), "owner".to_owned()));
            }
            let found: Vec<(String, String)> = keys
                .iter()
                .map(|key| (key.subject.clone(), key.scope.clone()))
                .collect();
            ensure!(
                found == expected,
                "{label}: {found:?}, expected {expected:?}"
            );
        }
        // With a subject as well, the name source still returns that operator's key.
        let subject = account(1);
        let chain = on(CHAIN);
        let keys = page(
            connection,
            Some(&subject),
            Some(alias(20)),
            None,
            None,
            64,
            Some(&chain),
        )
        .await?;
        ensure!(
            keys.len() == 1 && keys[0].scope.starts_with("account:"),
            "one subject through the name: {keys:?}"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn page_candidates_probe_an_index_in_every_arm_at_200_resources() -> Result<()> {
    with_database("resource_candidate_page_plan", async |connection| {
        install_plan_fixture(connection).await?;
        // 160 resources with an observation and a name of their own, 20 second resources of a
        // name, and 20 resources a named observation reaches.
        let resources: Vec<String> = (1..=160)
            .map(resource)
            .chain((1..=20).map(alias))
            .chain((1..=20).map(|n| moved(n * 10)))
            .map(|id| id.to_string())
            .collect();
        let resources = format!("'{{{}}}'", resources.join(","));
        raw_sql(&format!(
            "PREPARE resource_candidates (text, uuid[], text, text, uuid, text, bigint, uuid, text)
             AS {}",
            resource_page_statement()
        ))
        .execute(&mut *connection)
        .await?;
        let probes = [
            Probe {
                index: "project_registry_binding_observation_target_idx",
                conditions: &[ON_CHAIN, "(target_resource_id = ANY "],
            },
            Probe {
                index: "project_registry_binding_observation_resource_idx",
                conditions: &[ON_CHAIN, "(resource_id = ANY "],
            },
            Probe {
                index: "project_binding_candidate_resource_idx",
                conditions: &[ON_CHAIN, "(resource_id = ANY "],
            },
            Probe {
                index: "project_registry_binding_observation_pkey",
                conditions: &["chain_id = ", "(observation_identity = "],
            },
        ];
        // The first read of a page, and one that continues after a key.
        let after = format!("'{}', '{}', 'owner'", account(80), resource(80));
        let mut failures = Vec::new();
        for (label, after) in [
            ("first read", "NULL, NULL, NULL"),
            ("after a key", after.as_str()),
        ] {
            let values = format!("NULL, {resources}, NULL, {after}, 1001, NULL, '{CHAIN}'");
            // The reader sends the statement unprepared, so it is planned with its values.
            let plan = explain_execute(
                connection,
                "force_custom_plan",
                "resource_candidates",
                &values,
            )
            .await?;
            // The bound ids make the lines long. A failure needs the node and the head of its
            // condition.
            let plan: Vec<String> = plan
                .iter()
                .map(|line| line.chars().take(160).collect())
                .collect();
            let rendered = plan.join("\n");
            for table in [
                "project_registry_binding_observation",
                "project_binding_candidate",
                "project_grant",
            ] {
                if plan
                    .iter()
                    .any(|line| line.contains(&format!("Seq Scan on {table}")))
                {
                    failures.push(format!("{label}: reads {table} whole\n{rendered}"));
                }
            }
            failures.extend(
                chainless_index_scans(&plan)
                    .into_iter()
                    .map(|scan| format!("{label}: no chain in the condition of{scan}\n{rendered}")),
            );
            failures.extend(missing_probes(label, &plan, &probes));
        }
        ensure!(failures.is_empty(), "{}", failures.join("\n\n"));
        Ok(())
    })
    .await
}

/// The resources of the row fixture on each chain.
fn fixture_chains() -> BTreeMap<&'static str, Vec<Uuid>> {
    let main: Vec<Uuid> = (1..=NAMES + 5)
        .map(resource)
        .chain((5..=NAMES).step_by(5).map(alias))
        .chain((10..=NAMES).step_by(10).map(moved))
        .collect();
    BTreeMap::from([(CHAIN, main), (OTHER_CHAIN, (1..=6).map(other).collect())])
}

/// The keys a read of one resource at a time returns for every resource of `chains`, in
/// resource order: what the statement for a page's resources must return.
async fn keys_per_resource(
    connection: &mut PgConnection,
    chains: &BTreeMap<&str, Vec<Uuid>>,
) -> Result<Vec<Key>> {
    let mut resources: Vec<(Uuid, &str)> = chains
        .iter()
        .flat_map(|(chain, resources)| resources.iter().map(|resource| (*resource, *chain)))
        .collect();
    resources.sort();
    let mut all = Vec::new();
    for (resource, chain) in resources {
        let token = TokenRead {
            chain_id: chain.to_owned(),
            root: None,
        };
        let mut after: Option<Cursor> = None;
        loop {
            let keys = page(
                connection,
                None,
                Some(resource),
                None,
                after.as_ref(),
                64,
                Some(&token),
            )
            .await?;
            let last = keys.len() < 64;
            after = keys.last().map(Cursor::from);
            all.extend(keys);
            if last {
                break;
            }
        }
    }
    Ok(all)
}

/// Every key of `chains`, read `limit` at a time, each read continuing after the last key.
async fn keys_by_page(
    connection: &mut PgConnection,
    chains: &BTreeMap<&str, Vec<Uuid>>,
    limit: usize,
) -> Result<Vec<Key>> {
    let mut all: Vec<Key> = Vec::new();
    loop {
        let keys = resource_pages(connection, chains, all.last(), limit).await?;
        ensure!(
            keys.len() <= limit,
            "{} keys for a limit of {limit}",
            keys.len()
        );
        let last = keys.len() < limit;
        all.extend(keys);
        if last {
            return Ok(all);
        }
    }
}

#[tokio::test]
async fn page_candidates_match_the_per_resource_keys() -> Result<()> {
    with_database("resource_candidate_page_rows", async |connection| {
        install_row_fixture(connection).await?;
        let both = fixture_chains();
        let main = BTreeMap::from([(CHAIN, both[CHAIN].clone())]);
        // Every third resource: the statement must not return a key of a resource left out.
        let some = BTreeMap::from([(CHAIN, both[CHAIN].iter().copied().step_by(3).collect())]);
        for (label, chains) in [
            ("one chain", &main),
            ("a subset", &some),
            ("two chains", &both),
        ] {
            let expected = keys_per_resource(connection, chains).await?;
            ensure!(expected.len() > 20, "{label}: only {} keys", expected.len());
            // 1,001 is the reader's first read, which here takes every key at once.
            let keys = keys_by_page(connection, chains, 1001).await?;
            ensure!(
                keys == expected,
                "{label}: keys differ from the per-resource reads\n{keys:?}\n{expected:?}"
            );
        }
        // The fixture reaches a resource through each source: a direct grant, a wrapper
        // holder's operator, and an observation's target, own resource and name.
        let expected = keys_per_resource(connection, &main).await?;
        for (label, id, subject, scope) in [
            ("a direct grant", resource(1), account(1), "owner"),
            ("a wrapper operator", resource(7), account(4), "wrapped"),
            ("an observation's target", moved(20), account(1), "account:"),
            (
                "a second resource of a name",
                alias(20),
                account(1),
                "account:",
            ),
            (
                "an observation's own resource",
                resource(2),
                account(3),
                "account:",
            ),
        ] {
            ensure!(
                expected.iter().any(|key| {
                    key.resource_id == id && key.subject == subject && key.scope.starts_with(scope)
                }),
                "{label}: the fixture has no such key"
            );
        }
        Ok(())
    })
    .await
}

#[tokio::test]
async fn page_candidates_seek_continues_in_resource_order() -> Result<()> {
    with_database("resource_candidate_page_seek", async |connection| {
        install_row_fixture(connection).await?;
        let chains = fixture_chains();
        let expected = keys_per_resource(connection, &chains).await?;
        // Each limit cuts inside a resource's keys, and the small ones inside the other chain's.
        for limit in [1, 2, 7, 64] {
            let keys = keys_by_page(connection, &chains, limit).await?;
            ensure!(
                keys.windows(2).all(|pair| {
                    (pair[0].resource_id, &pair[0].subject, &pair[0].scope)
                        < (pair[1].resource_id, &pair[1].subject, &pair[1].scope)
                }),
                "limit {limit}: keys are not in resource, subject and scope order\n{keys:?}"
            );
            ensure!(
                keys == expected,
                "limit {limit}: keys differ from the per-resource reads\n{keys:?}\n{expected:?}"
            );
        }
        Ok(())
    })
    .await
}

#[derive(Clone, Debug, Default)]
struct Read {
    subject: Option<String>,
    resource: Option<Uuid>,
    token: Option<TokenRead>,
}

async fn keys_before(
    connection: &mut PgConnection,
    read: &Read,
    namespace: Option<&str>,
    after: Option<&Cursor>,
    limit: i64,
) -> Result<Vec<Key>> {
    let sql = statement_before(read.subject.as_deref(), read.resource, after);
    sqlx::query_as(&sql)
        .persistent(false)
        .bind(read.subject.as_deref())
        .bind(read.resource)
        .bind(namespace)
        .bind(after.map(|key| key.subject.as_str()))
        .bind(after.map(|key| key.resource_id))
        .bind(after.map(|key| key.scope.as_str()))
        .bind(limit)
        .bind(read.token.as_ref().and_then(|token| token.root))
        .bind(read.token.as_ref().map(|token| token.chain_id.as_str()))
        .fetch_all(connection)
        .await
        .map_err(Into::into)
}

/// Resource `n` of the fixtures.
fn resource(n: i64) -> Uuid {
    Uuid::from_u128(n as u128)
}

/// A second resource bound to name `n`.
fn alias(n: i64) -> Uuid {
    Uuid::from_u128((1_u128 << 64) | n as u128)
}

/// The resource the observation of name `n` reaches, on every tenth name.
fn moved(n: i64) -> Uuid {
    Uuid::from_u128((2_u128 << 64) | n as u128)
}

/// Resource `n` of the other chain.
fn other(n: i64) -> Uuid {
    Uuid::from_u128((3_u128 << 64) | n as u128)
}

fn account(n: i64) -> String {
    format!("0x{n:040x}")
}

async fn install_plan_fixture(connection: &mut PgConnection) -> Result<()> {
    // Observation n is of resource n. An even n is a named observation of name n, which reaches
    // resource 2 * 2^64 + n instead of its own on every tenth name. An odd n is unnamed. Three
    // in four observations are of one of 120 registry owners, the rest of an owner of their
    // own. Name n is bound to resource n, and unless n is a multiple of ten also to resource
    // 2^64 + n. Resource n has one direct grant. Approval i is of owner i % 148: the first 330
    // are approved registry operators, the next 370 revoked ones, the rest of other authority
    // kinds. Another chain holds 2,000 observations and candidates of its own resources.
    raw_sql(&format!(
        "INSERT INTO project_registry_binding_observation
             (chain_id, observation_identity, logical_name_id, resource_id, attributed_via,
              target_resource_id, block_number, event_identity, event_kind, registry_owner,
              registry_contract, applicable)
         SELECT '{CHAIN}',
                CASE WHEN n % 2 = 0 THEN 'ens:' || n
                     ELSE lpad(to_hex(n), 32, '0')::uuid::text END,
                CASE WHEN n % 2 = 0 THEN 'ens:' || n END,
                lpad(to_hex(n), 32, '0')::uuid,
                CASE WHEN n % 2 = 0 THEN 'name' ELSE 'own' END,
                CASE WHEN n % 10 = 0
                     THEN ('0000000000000002' || lpad(to_hex(n), 16, '0'))::uuid
                     ELSE lpad(to_hex(n), 32, '0')::uuid END,
                n, 'observation:' || n, 'AuthorityTransferred',
                '0x' || lpad(to_hex(CASE WHEN n % 4 = 3 THEN 1000 + n ELSE n % 120 END), 40, '0'),
                '{REGISTRY_A}', true
         FROM generate_series(1, {OBSERVATIONS}) n;

         INSERT INTO project_registry_binding_observation
             (chain_id, observation_identity, logical_name_id, resource_id, attributed_via,
              target_resource_id, block_number, event_identity, event_kind, registry_owner,
              registry_contract, applicable)
         SELECT '{OTHER_CHAIN}', 'basenames:' || n, 'basenames:' || n,
                ('0000000000000003' || lpad(to_hex(n), 16, '0'))::uuid, 'name',
                ('0000000000000003' || lpad(to_hex(n), 16, '0'))::uuid,
                n, 'observation:other:' || n, 'AuthorityTransferred',
                '0x' || lpad(to_hex(n % 120), 40, '0'), '{REGISTRY_A}', true
         FROM generate_series(1, 2000) n;

         INSERT INTO project_binding_candidate
             (surface_binding_id, logical_name_id, namespace, chain_id, authority_arm,
              resource_id, binding_kind, canonicality_state, block_number, event_identity)
         SELECT (lpad(to_hex(copy), 16, '0') || lpad(to_hex(n), 16, '0'))::uuid, 'ens:' || n,
                'ens', '{CHAIN}', 'ens_v1',
                (lpad(to_hex(copy), 16, '0') || lpad(to_hex(n), 16, '0'))::uuid,
                'registration', 'canonical', n, 'candidate:' || copy || ':' || n
         FROM generate_series(1, {OBSERVATIONS}) n
         CROSS JOIN generate_series(0, 1) copy
         WHERE copy = 0 OR n % 10 <> 0;

         INSERT INTO project_binding_candidate
             (surface_binding_id, logical_name_id, namespace, chain_id, authority_arm,
              resource_id, binding_kind, canonicality_state, block_number, event_identity)
         SELECT ('0000000000000003' || lpad(to_hex(n), 16, '0'))::uuid, 'basenames:' || n,
                'basenames', '{OTHER_CHAIN}', 'basenames',
                ('0000000000000003' || lpad(to_hex(n), 16, '0'))::uuid,
                'registration', 'canonical', n, 'candidate:other:' || n
         FROM generate_series(1, 2000) n;

         INSERT INTO project_account_approval
             (chain_id, authority_kind, authority_contract, owner, subject, relation_kind,
              block_number, event_identity, approved)
         SELECT '{CHAIN}',
                CASE WHEN i <= 700 THEN 'registry' WHEN i <= 1000 THEN 'wrapper'
                     ELSE 'ens_v2_registry' END,
                '{REGISTRY_A}', '0x' || lpad(to_hex(i % 148), 40, '0'),
                '0x' || lpad(to_hex(50000 + i), 40, '0'), 'operator', i, 'approval:' || i,
                i <= 330 OR i > 700
         FROM generate_series(1, 1300) i;

         INSERT INTO project_grant
             (chain_id, resource_id, subject, scope, block_number, event_identity, event_kind,
              effective_powers, revoked)
         SELECT '{CHAIN}', lpad(to_hex(n), 32, '0')::uuid,
                '0x' || lpad(to_hex(n % 5000), 40, '0'), 'owner', n, 'grant:' || n,
                'PermissionChanged', '{{}}'::jsonb, false
         FROM generate_series(1, {OBSERVATIONS}) n;

         ANALYZE"
    ))
    .execute(&mut *connection)
    .await?;
    Ok(())
}

async fn install_row_fixture(connection: &mut PgConnection) -> Result<()> {
    // Forty names on the main chain. Observation n is of resource n and registry owner n % 5,
    // on registry A, or on registry B when n is a multiple of three. A multiple of four is a
    // named observation that reaches the name's current resource, any other even n a named
    // observation of its own resource, an odd n unnamed. Every tenth reaches resource
    // 2 * 2^64 + n. Observation 7 has no owner and every eleventh is not applicable. Name n is
    // bound to resource n, and every fifth also to resource 2^64 + n.
    //
    // Owner k approved accounts k and k + 1 as operators on registry A, and account k on
    // registry B, which stands only for an even k. Owner 1 also has a non-operator relation.
    // Resource n has a direct grant to owner n % 5, and every seventh also a wrapper holder
    // grant, whose holder k approved account k + 2 on the wrapper.
    //
    // Even resources have an event in namespace ens and multiples of three one in basenames.
    // A multiple of five also has an ens event that is only observed, which does not count.
    //
    // The other chain has six named observations of its own resources and names, with the same
    // owner addresses and registry A, each owner approving account 100 + k there.
    raw_sql(&format!(
        "INSERT INTO chain_lineage
             (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         VALUES ('{CHAIN}', 'block-1', 1, to_timestamp(1), 'canonical');

         INSERT INTO resources (resource_id, chain_id, block_hash, block_number,
             canonicality_state)
         SELECT lpad(to_hex(n), 32, '0')::uuid, '{CHAIN}', 'block-1', 1, 'canonical'
         FROM generate_series(1, {NAMES}) n;

         INSERT INTO normalized_events (event_identity, namespace, resource_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash,
             derivation_kind, canonicality_state)
         SELECT namespace || ':' || state || ':' || n, namespace,
                lpad(to_hex(n), 32, '0')::uuid, 'SurfaceBound', 'ens_v1_registry_l1', 1,
                '{CHAIN}', 1, 'block-1', 'ens_v1_unwrapped_authority',
                state::canonicality_state
         FROM generate_series(1, {NAMES}) n
         CROSS JOIN (VALUES ('ens', 'canonical', 2), ('basenames', 'finalized', 3),
                            ('ens', 'observed', 5)) events (namespace, state, every)
         WHERE n % every = 0;

         INSERT INTO project_registry_binding_observation
             (chain_id, observation_identity, logical_name_id, resource_id, attributed_via,
              target_resource_id, block_number, event_identity, event_kind, registry_owner,
              registry_contract, applicable)
         SELECT '{CHAIN}',
                CASE WHEN n % 2 = 0 THEN 'ens:' || n
                     ELSE lpad(to_hex(n), 32, '0')::uuid::text END,
                CASE WHEN n % 2 = 0 THEN 'ens:' || n END,
                lpad(to_hex(n), 32, '0')::uuid,
                CASE WHEN n % 4 = 0 THEN 'name' ELSE 'own' END,
                CASE WHEN n % 10 = 0
                     THEN ('0000000000000002' || lpad(to_hex(n), 16, '0'))::uuid
                     ELSE lpad(to_hex(n), 32, '0')::uuid END,
                n, 'observation:' || n, 'AuthorityTransferred',
                CASE WHEN n <> 7 THEN '0x' || lpad(to_hex(n % 5), 40, '0') END,
                CASE WHEN n = 7 THEN NULL WHEN n % 3 = 0 THEN '{REGISTRY_B}'
                     ELSE '{REGISTRY_A}' END,
                n <> 7 AND n % 11 <> 0
         FROM generate_series(1, {NAMES}) n;

         INSERT INTO project_registry_binding_observation
             (chain_id, observation_identity, logical_name_id, resource_id, attributed_via,
              target_resource_id, block_number, event_identity, event_kind, registry_owner,
              registry_contract, applicable)
         SELECT '{OTHER_CHAIN}', 'basenames:' || n, 'basenames:' || n,
                ('0000000000000003' || lpad(to_hex(n), 16, '0'))::uuid, 'name',
                ('0000000000000003' || lpad(to_hex(n), 16, '0'))::uuid,
                n, 'observation:other:' || n, 'AuthorityTransferred',
                '0x' || lpad(to_hex(n % 5), 40, '0'), '{REGISTRY_A}', true
         FROM generate_series(1, 6) n;

         INSERT INTO project_binding_candidate
             (surface_binding_id, logical_name_id, namespace, chain_id, authority_arm,
              resource_id, binding_kind, canonicality_state, block_number, event_identity)
         SELECT (lpad(to_hex(copy), 16, '0') || lpad(to_hex(n), 16, '0'))::uuid, 'ens:' || n,
                'ens', '{CHAIN}', 'ens_v1',
                (lpad(to_hex(copy), 16, '0') || lpad(to_hex(n), 16, '0'))::uuid,
                'registration', 'canonical', n, 'candidate:' || copy || ':' || n
         FROM generate_series(1, {NAMES}) n
         CROSS JOIN generate_series(0, 1) copy
         WHERE copy = 0 OR n % 5 = 0;

         INSERT INTO project_binding_candidate
             (surface_binding_id, logical_name_id, namespace, chain_id, authority_arm,
              resource_id, binding_kind, canonicality_state, block_number, event_identity)
         SELECT ('0000000000000003' || lpad(to_hex(n), 16, '0'))::uuid, 'basenames:' || n,
                'basenames', '{OTHER_CHAIN}', 'basenames',
                ('0000000000000003' || lpad(to_hex(n), 16, '0'))::uuid,
                'registration', 'canonical', n, 'candidate:other:' || n
         FROM generate_series(1, 6) n;

         INSERT INTO project_account_approval
             (chain_id, authority_kind, authority_contract, owner, subject, relation_kind,
              block_number, event_identity, approved)
         SELECT chain, kind, contract, '0x' || lpad(to_hex(k), 40, '0'),
                '0x' || lpad(to_hex(k + step), 40, '0'), relation, 1,
                concat_ws(':', 'approval', chain, kind, contract, k, step, relation),
                contract <> '{REGISTRY_B}' OR k % 2 = 0
         FROM generate_series(0, 4) k
         CROSS JOIN (VALUES
             ('{CHAIN}', 'registry', '{REGISTRY_A}', 0, 'operator'),
             ('{CHAIN}', 'registry', '{REGISTRY_A}', 1, 'operator'),
             ('{CHAIN}', 'registry', '{REGISTRY_B}', 0, 'operator'),
             ('{CHAIN}', 'wrapper', '{WRAPPER_CONTRACT}', 2, 'operator'),
             ('{OTHER_CHAIN}', 'registry', '{REGISTRY_A}', 100, 'operator')
         ) approvals (chain, kind, contract, step, relation)
         UNION ALL
         SELECT '{CHAIN}', 'registry', '{REGISTRY_A}', '0x' || lpad(to_hex(1), 40, '0'),
                '0x' || lpad(to_hex(6), 40, '0'), 'manager', 1, 'approval:manager', true;

         INSERT INTO project_grant
             (chain_id, resource_id, subject, scope, block_number, event_identity, event_kind,
              effective_powers, grant_source, revoked)
         SELECT '{CHAIN}', lpad(to_hex(n), 32, '0')::uuid,
                '0x' || lpad(to_hex(n % 5), 40, '0'), scope, n, 'grant:' || scope || ':' || n,
                'PermissionChanged', '{{}}'::jsonb,
                CASE WHEN scope = 'wrapped' THEN jsonb_build_object(
                    'authority_kind', 'wrapper', 'relation_kind', 'holder',
                    'authority_contract', '{WRAPPER_CONTRACT}') END,
                false
         FROM generate_series(1, {NAMES}) n
         CROSS JOIN (VALUES ('owner'), ('wrapped')) scopes (scope)
         WHERE scope = 'owner' OR n % 7 = 0;

         ANALYZE"
    ))
    .execute(&mut *connection)
    .await?;
    Ok(())
}
