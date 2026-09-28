//! Shadow comparison for TYR-36 step 7b slice 4: at one publication, the reads
//! `GET /v1/permissions` and the resolver routes serve must be the same with the publication
//! switch off (the served tables) and on (the owned key families). Every read goes through the
//! storage function the route calls, once under each switch state
//! (`publication_source::with_serve_from_families`), so both states are exercised as served.
//!
//! What is compared:
//! - permission pages: for every subject the served rows, the families or the approvals name,
//!   with no namespace filter and with the chain's namespace, and for every resource of the
//!   chain, the effective-permission page (`load_serving_effective_permissions_page`) walked
//!   cursor by cursor at `page_size`: every page's rows in every semantic column the route reads
//!   (subject, resource, scope, record selector, operator relation, powers, grant and revocation
//!   sources, inheritance path, transfer behaviour) and its next cursor, which also gives the
//!   exact total of every walk;
//! - resource summaries (`load_serving_permission_summaries`): authority kind, registry root,
//!   coverage and restriction block of every resource of the chain;
//! - resolvers: for every resolver `resolver_current` or F3 names, the overview row
//!   (`load_phase_resolver_current`): whether it exists, its classification and the support of
//!   every section the routes gate on; and the `/aliases`, `/links` and `/roles` pages walked by
//!   key, each side read the way the route reads it (the served statements of
//!   apps/api/src/v2/resolvers/collections/reads.rs, the family collection readers), with totals.
//!   `/roles` is compared after the route's enrichment: each row's evidence ids are replaced by
//!   the `grant_event` the route picks from them (the earliest permission event of the row's
//!   subject at or below the publication), so the masks, the evidence and the pick are all
//!   compared.
//!
//! A resource the control comparison excuses (a same-block ordering delta or a named served-side
//! cause, `shadow.rs`) can differ for that cause: a walk that lists one is compared as one
//! sequence without its rows, counted as `excused_walks`.
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use bigname_storage::{
    DEFAULT_PERMISSIONS_CURRENT_READ_FILTER, EffectivePermissionRow,
    PermissionsCurrentAccountResourceCursor, PermissionsCurrentResourceSummary,
    families::topology as family, load_history_events_by_ids, load_phase_resolver_current,
    load_serving_effective_permissions_page, load_serving_permission_summaries,
    publication_source::with_serve_from_families,
};
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

/// What one comparison saw.
#[derive(Debug, Default)]
pub struct Report {
    pub target: i64,
    pub subjects: usize,
    pub resources: usize,
    pub pages: usize,
    pub rows: usize,
    pub summaries: usize,
    pub resolvers: usize,
    pub collection_rows: usize,
    pub excused_walks: usize,
    pub mismatches: Vec<String>,
    /// Per read kind: reads, served time, family time.
    pub timings: BTreeMap<&'static str, (u32, Duration, Duration)>,
}

impl Report {
    fn mismatch(&mut self, key: String, detail: String) {
        self.mismatches.push(format!("{key}: {detail}"));
    }

    fn time(&mut self, kind: &'static str, served: Duration, family: Duration) {
        let entry = self.timings.entry(kind).or_default();
        entry.0 += 1;
        entry.1 += served;
        entry.2 += family;
    }

    pub fn line(&self) -> String {
        let timings: Vec<String> = self
            .timings
            .iter()
            .map(|(kind, (reads, served, family))| {
                let each = |total: &Duration| total.as_secs_f64() * 1000.0 / f64::from(*reads);
                format!(
                    "{kind}:reads={reads},served_ms={:.2},family_ms={:.2}",
                    each(served),
                    each(family)
                )
            })
            .collect();
        format!(
            "SEPOLIA_END_TO_END_PERMISSIONS target={} subjects={} resources={} pages={} rows={} \
             summaries={} resolvers={} collection_rows={} excused_walks={} mismatches={} \
             timings={}",
            self.target,
            self.subjects,
            self.resources,
            self.pages,
            self.rows,
            self.summaries,
            self.resolvers,
            self.collection_rows,
            self.excused_walks,
            self.mismatches.len(),
            timings.join(";")
        )
    }

    pub fn require_clean(&self) -> Result<()> {
        ensure!(
            self.mismatches.is_empty(),
            "the permission and resolver reads differ between the switch states at {}: {:#?}",
            self.target,
            self.mismatches
        );
        Ok(())
    }
}

/// The served Project row and the family marker must name the same block; returns it.
async fn publication(pool: &PgPool, chain: &str) -> Result<(i64, String)> {
    let served: (Option<i64>, Option<String>) = sqlx::query_as(
        "SELECT current_block_number, current_block_hash FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(chain)
    .fetch_one(pool)
    .await?;
    let family: Option<(Option<i64>, Option<String>)> = sqlx::query_as(
        "SELECT current_block_number, current_block_hash FROM project_family_marker
         WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_optional(pool)
    .await?;
    ensure!(
        served.0.is_some() && family.as_ref() == Some(&served),
        "the families are at {family:?}, the served publication at {served:?}"
    );
    Ok((served.0.unwrap_or_default(), served.1.unwrap_or_default()))
}

/// Compare every permission and resolver read of `chain` at its current publication, walking
/// pages of `page_size`. `excused` holds the resources the control comparison excused.
pub async fn compare(
    pool: &PgPool,
    chain: &str,
    page_size: u64,
    excused: &BTreeSet<String>,
) -> Result<Report> {
    let (target, hash) = publication(pool, chain).await?;
    let mut report = Report {
        target,
        ..Report::default()
    };
    let namespace = if chain.starts_with("base-") {
        "basenames"
    } else {
        "ens"
    };
    let subjects: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT pc.subject FROM permissions_current pc
         WHERE pc.provenance ->> 'chain_id' = $1 {DEFAULT_PERMISSIONS_CURRENT_READ_FILTER}
         UNION SELECT subject FROM project_grant WHERE chain_id = $1
         UNION SELECT subject FROM account_permission_state_current WHERE chain_id = $1
         UNION SELECT subject FROM project_account_approval WHERE chain_id = $1
         ORDER BY 1"
    ))
    .bind(chain)
    .fetch_all(pool)
    .await
    .context("permission subjects")?;
    let resources: Vec<Uuid> = sqlx::query_scalar(
        "SELECT resource_id FROM permissions_current_resource_summary
         WHERE provenance ->> 'chain_id' = $1
         UNION SELECT resource_id FROM permissions_current WHERE provenance ->> 'chain_id' = $1
         UNION SELECT resource_id FROM project_grant WHERE chain_id = $1
         UNION SELECT resource_id FROM resources WHERE chain_id = $1 AND block_number <= $2
         ORDER BY 1",
    )
    .bind(chain)
    .bind(target)
    .fetch_all(pool)
    .await
    .context("permission resources")?;
    for subject in &subjects {
        report.subjects += 1;
        for filter in [None, Some(namespace)] {
            walk(
                pool,
                (Some(subject.as_str()), None, filter),
                page_size,
                excused,
                &mut report,
            )
            .await?;
        }
    }
    for resource in &resources {
        report.resources += 1;
        walk(
            pool,
            (None, Some(*resource), None),
            page_size,
            excused,
            &mut report,
        )
        .await?;
    }
    summaries(pool, &resources, excused, &mut report).await?;
    resolvers(pool, chain, namespace, target, excused, &mut report).await?;
    ensure!(
        publication(pool, chain).await? == (target, hash),
        "the publication moved during the permission comparison"
    );
    Ok(report)
}

/// A row in the columns the route reads.
fn row_json(row: &EffectivePermissionRow) -> Value {
    json!({
        "subject": row.subject, "resource_id": row.resource_id,
        "scope": row.scope.storage_key(), "record_resource_selector": row.record_resource_selector,
        "operator": row.grant_relation.is_some(), "effective_powers": row.effective_powers,
        "grant_source": row.grant_source, "revocation_source": row.revocation_source,
        "inheritance_path": row.inheritance_path, "transfer_behavior": row.transfer_behavior,
    })
}

fn cursor_json(cursor: Option<&PermissionsCurrentAccountResourceCursor>) -> Value {
    cursor.map_or(Value::Null, |cursor| {
        json!([cursor.subject, cursor.resource_id, cursor.scope])
    })
}

type Walk = Vec<(Vec<Value>, Value)>;

/// Every page of one filter under one switch state, following that side's cursors.
async fn pages(
    pool: &PgPool,
    on: bool,
    (subject, resource, namespace): (Option<&str>, Option<Uuid>, Option<&str>),
    page_size: u64,
) -> Result<(Walk, Duration)> {
    let started = Instant::now();
    let mut pages = Vec::new();
    let mut cursor: Option<PermissionsCurrentAccountResourceCursor> = None;
    loop {
        let page = with_serve_from_families(
            on,
            load_serving_effective_permissions_page(
                pool,
                subject,
                resource,
                namespace,
                cursor.as_ref(),
                page_size,
            ),
        )
        .await
        .with_context(|| format!("switch {on}: page of {subject:?} {resource:?} {namespace:?}"))?;
        pages.push((
            page.rows.iter().map(row_json).collect(),
            cursor_json(page.next_cursor.as_ref()),
        ));
        cursor = page.next_cursor;
        if cursor.is_none() {
            return Ok((pages, started.elapsed()));
        }
        ensure!(pages.len() < 100_000, "the page walk does not end");
    }
}

async fn walk(
    pool: &PgPool,
    filter: (Option<&str>, Option<Uuid>, Option<&str>),
    page_size: u64,
    excused: &BTreeSet<String>,
    report: &mut Report,
) -> Result<()> {
    let (served, served_time) = pages(pool, false, filter, page_size).await?;
    let (shadow, family_time) = pages(pool, true, filter, page_size).await?;
    report.time(
        if filter.1.is_some() {
            "resource_page_walk"
        } else {
            "subject_page_walk"
        },
        served_time,
        family_time,
    );
    report.pages += served.len();
    report.rows += served.iter().map(|(rows, _)| rows.len()).sum::<usize>();
    if served == shadow {
        return Ok(());
    }
    let without = |walk: &Walk| -> Vec<Value> {
        walk.iter()
            .flat_map(|(rows, _)| rows.iter())
            .filter(|row| {
                row["resource_id"]
                    .as_str()
                    .is_none_or(|resource| !excused.contains(resource))
            })
            .cloned()
            .collect()
    };
    let touches_excused = |walk: &Walk| {
        walk.iter().flat_map(|(rows, _)| rows.iter()).any(|row| {
            row["resource_id"]
                .as_str()
                .is_some_and(|resource| excused.contains(resource))
        })
    };
    if (touches_excused(&served) || touches_excused(&shadow))
        && without(&served) == without(&shadow)
    {
        report.excused_walks += 1;
        return Ok(());
    }
    report.mismatch(
        format!("permissions {filter:?}"),
        format!("served {served:?}; families {shadow:?}"),
    );
    Ok(())
}

fn summary_json(summary: &PermissionsCurrentResourceSummary) -> Value {
    json!({
        "authority_kind": summary.authority_kind, "root_resource_id": summary.root_resource_id,
        "coverage": summary.coverage, "resource_restrictions": summary.resource_restrictions,
    })
}

async fn summaries(
    pool: &PgPool,
    resources: &[Uuid],
    excused: &BTreeSet<String>,
    report: &mut Report,
) -> Result<()> {
    for chunk in resources.chunks(200) {
        let started = Instant::now();
        let served =
            with_serve_from_families(false, load_serving_permission_summaries(pool, chunk)).await?;
        let served_time = started.elapsed();
        let started = Instant::now();
        let shadow =
            with_serve_from_families(true, load_serving_permission_summaries(pool, chunk)).await?;
        report.time("summaries_chunk", served_time, started.elapsed());
        for resource in chunk {
            let pair = (
                served.get(resource).map(summary_json),
                shadow.get(resource).map(summary_json),
            );
            report.summaries += usize::from(pair.0.is_some());
            if pair.0 != pair.1 && !excused.contains(&resource.to_string()) {
                report.mismatch(
                    format!("summary of {resource}"),
                    format!("served {:?}; families {:?}", pair.0, pair.1),
                );
            }
        }
    }
    Ok(())
}

/// The sections the resolver routes gate on, with their support.
const SECTIONS: [&str; 5] = [
    "bindings",
    "aliases",
    "links",
    "permissions",
    "role_holders",
];

async fn resolvers(
    pool: &PgPool,
    chain: &str,
    namespace: &str,
    target: i64,
    excused: &BTreeSet<String>,
    report: &mut Report,
) -> Result<()> {
    let addresses: Vec<String> = sqlx::query_scalar(
        "SELECT lower(resolver_address) FROM resolver_current WHERE chain_id = $1
         UNION SELECT resolver_address FROM project_resolver_classification WHERE chain_id = $1
         UNION SELECT split_part(scope, ':', 3) FROM project_grant
               WHERE chain_id = $1 AND scope LIKE 'resolver:%'
         ORDER BY 1",
    )
    .bind(chain)
    .fetch_all(pool)
    .await?;
    for address in addresses {
        report.resolvers += 1;
        let started = Instant::now();
        let served =
            with_serve_from_families(false, load_phase_resolver_current(pool, chain, &address))
                .await?;
        let served_time = started.elapsed();
        let started = Instant::now();
        let shadow =
            with_serve_from_families(true, load_phase_resolver_current(pool, chain, &address))
                .await?;
        report.time("resolver_overview", served_time, started.elapsed());
        let overview = |row: &bigname_storage::ResolverCurrentRow| {
            let mut sections = serde_json::Map::new();
            for section in SECTIONS {
                let summary = &row.declared_summary[section];
                sections.insert(
                    section.to_owned(),
                    json!([summary["status"], summary["unsupported_reason"]]),
                );
            }
            json!({"classification": row.declared_summary["classification"],
                   "sections": sections, "coverage": row.coverage})
        };
        let pair = (served.as_ref().map(overview), shadow.as_ref().map(overview));
        if pair.0 != pair.1 {
            report.mismatch(
                format!("resolver {address}"),
                format!("served {:?}; families {:?}", pair.0, pair.1),
            );
        }
        for section in ["aliases", "links", "roles"] {
            collection(
                pool,
                (chain, namespace),
                &address,
                section,
                target,
                excused,
                report,
            )
            .await?;
        }
    }
    Ok(())
}

/// The served collection statement of apps/api/src/v2/resolvers/collections/reads.rs, from the
/// API's own SQL files; the roles statement is inline there and copied here.
fn served_collection_sql(section: &str) -> String {
    let source = match section {
        "links" => include_str!("../../../api/src/v2/resolvers/collections/links.sql").to_owned(),
        "roles" => format!(
            r#"WITH items AS (
            SELECT pc.subject AS key1, pc.resource_id::text AS key2,
                jsonb_strip_nulls(jsonb_build_object('address', pc.subject,
                    'registration_id', pc.resource_id, 'powers', pc.effective_powers,
                    'record_resource_selector', pc.scope_detail -> 'resource_selector',
                    'event_ids',
                    COALESCE(pc.provenance -> 'normalized_event_ids', '[]'::jsonb))) AS item
            FROM bigname_phase.permissions_current pc
            WHERE pc.scope_kind = 'resolver'
              AND pc.scope_detail ->> 'chain_id' = $1
              AND lower(pc.scope_detail ->> 'resolver_address') = $2
              AND (pc.chain_positions ->> 'target_block_number')::bigint <= $3
              AND jsonb_array_length(pc.effective_powers) > 0
              {DEFAULT_PERMISSIONS_CURRENT_READ_FILTER}
        )"#
        ),
        _ => include_str!("../../../api/src/v2/resolvers/collections/aliases.sql")
            .replace(
                "{{name_lineage_joins}}",
                bigname_storage::DEFAULT_NAME_CURRENT_LINEAGE_JOINS,
            )
            .replace(
                "{{name_read_filter}}",
                bigname_storage::DEFAULT_NAME_CURRENT_READ_FILTER,
            ),
    };
    format!(
        r#"{source}, selected_page AS (
        SELECT key1, key2, item FROM items
        WHERE $4::text IS NULL OR (key1, key2) > ($4, $5)
        ORDER BY key1, key2 LIMIT $6
    ) SELECT (SELECT count(*) FROM items) AS total,
        COALESCE((SELECT jsonb_agg(jsonb_build_object('key1', key1, 'key2', key2, 'item', item)
                    ORDER BY key1, key2) FROM selected_page), '[]'::jsonb) AS rows"#
    )
}

type CollectionPage = (u64, Vec<(String, String, Value)>);

async fn served_collection(
    pool: &PgPool,
    (chain, namespace): (&str, &str),
    address: &str,
    section: &str,
    height: i64,
    after: Option<&(String, String)>,
    limit: i64,
) -> Result<CollectionPage> {
    let sql = served_collection_sql(section);
    let mut statement = sqlx::query_as::<_, (i64, Value)>(&sql)
        .bind(chain)
        .bind(address)
        .bind(height)
        .bind(after.map(|key| key.0.as_str()))
        .bind(after.map(|key| key.1.as_str()))
        .bind(limit);
    if section == "links" {
        statement = statement.bind(namespace);
    }
    let (total, rows) = statement
        .fetch_one(pool)
        .await
        .with_context(|| format!("served {section} of {address}"))?;
    let rows = rows
        .as_array()
        .context("served collection rows")?
        .iter()
        .map(|row| {
            Ok((
                row["key1"].as_str().context("key1")?.to_owned(),
                row["key2"].as_str().context("key2")?.to_owned(),
                row["item"].clone(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((u64::try_from(total)?, rows))
}

async fn family_collection(
    pool: &PgPool,
    (chain, namespace): (&str, &str),
    address: &str,
    section: &str,
    after: Option<&(String, String)>,
    limit: i64,
) -> Result<CollectionPage> {
    let page = match section {
        "links" => {
            family::load_resolver_links_shadow(pool, chain, address, namespace, after, limit)
                .await?
        }
        "roles" => family::load_resolver_roles_shadow(pool, chain, address, after, limit).await?,
        _ => family::load_resolver_aliases_shadow(pool, chain, address, after, limit).await?,
    };
    Ok((page.total_count, page.rows))
}

/// `/roles` rows as the route serves them: each row's evidence ids replaced by the event the
/// route attaches as `grant_event` (apps/api/src/v2/resolvers/collections/reads.rs
/// `attach_grants`): the earliest, by block, log index and id, of the row's evidence events that
/// is a permission event of the row's subject at or below the publication.
async fn enriched(
    pool: &PgPool,
    rows: &[(String, String, Value)],
    height: i64,
) -> Result<Vec<(String, String, Value)>> {
    let ids: Vec<i64> = rows
        .iter()
        .flat_map(|(_, _, item)| {
            item["event_ids"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_i64)
        })
        .collect();
    let events = load_history_events_by_ids(pool, &ids).await?;
    Ok(rows
        .iter()
        .map(|(key1, key2, item)| {
            let mut item = item.clone();
            let ids = item
                .as_object_mut()
                .and_then(|object| object.remove("event_ids"))
                .unwrap_or(json!([]));
            let address = item["address"].as_str().unwrap_or_default().to_owned();
            let grant = events
                .iter()
                .filter(|event| {
                    ids.as_array()
                        .is_some_and(|ids| ids.contains(&json!(event.normalized_event_id)))
                        && matches!(
                            event.event_kind.as_str(),
                            "PermissionChanged"
                                | "PermissionScopeChanged"
                                | "RolesChanged"
                                | "EACRolesChanged"
                        )
                        && event
                            .after_state
                            .get("subject")
                            .and_then(Value::as_str)
                            .is_some_and(|subject| subject.eq_ignore_ascii_case(&address))
                        && event.block_number.is_some_and(|number| number <= height)
                })
                .min_by_key(|event| {
                    (
                        event.block_number,
                        event.log_index,
                        event.normalized_event_id,
                    )
                });
            item["grant_event"] = grant.map_or(Value::Null, |event| json!(event.event_identity));
            (key1.clone(), key2.clone(), item)
        })
        .collect())
}

async fn collection(
    pool: &PgPool,
    scope: (&str, &str),
    address: &str,
    section: &'static str,
    height: i64,
    excused: &BTreeSet<String>,
    report: &mut Report,
) -> Result<()> {
    let limit = 3;
    let mut after: Option<(String, String)> = None;
    loop {
        let started = Instant::now();
        let served =
            served_collection(pool, scope, address, section, height, after.as_ref(), limit).await?;
        let served_time = started.elapsed();
        let started = Instant::now();
        let shadow =
            family_collection(pool, scope, address, section, after.as_ref(), limit).await?;
        report.time(section, served_time, started.elapsed());
        let (served, shadow) = if section == "roles" {
            (
                (served.0, enriched(pool, &served.1, height).await?),
                (shadow.0, enriched(pool, &shadow.1, height).await?),
            )
        } else {
            (served, shadow)
        };
        if served != shadow {
            let resources: BTreeSet<&str> = served
                .1
                .iter()
                .chain(&shadow.1)
                .map(|(_, key2, _)| key2.as_str())
                .collect();
            if section == "roles" && resources.iter().any(|key| excused.contains(*key)) {
                report.excused_walks += 1;
                return Ok(());
            }
            report.mismatch(
                format!("{section} of {address}"),
                format!("served {served:?}; families {shadow:?}"),
            );
            return Ok(());
        }
        report.collection_rows += served.1.len().min(2);
        if served.1.len() <= 2 {
            return Ok(());
        }
        let (key1, key2, _) = &served.1[1];
        after = Some((key1.clone(), key2.clone()));
    }
}
