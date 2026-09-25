//! Shadow comparison for TYR-36 step 5: at one publication, every family reader of
//! `bigname_storage::families::topology` must serve what today's reader serves.
//!
//! The family loop runs after the served batch commits, in its own transactions, so the
//! comparison first requires the family marker to equal the served Project marker and fails
//! loudly otherwise: a lagging family is not a mismatch, and it is not compared.
//!
//! What is compared, key by key:
//! - subnames: every parent the served table or the edge families name, paged through
//!   `load_children_current_page_filtered` and `load_children_shadow_page` with the same filter,
//!   page size and cursors; rows over the wire fields, next cursors and totals;
//! - topology: the alias and wildcard arms of `declared_summary.topology` for every name whose
//!   selected binding is on those arms, or that the shadow gives a topology;
//! - resolvers: for every resolver the served table or the families name, the overview's mirror
//!   and support against the F3 row, `bound_names` paged through both readers, and the `/aliases`,
//!   `/links` and `/roles` pages and totals against the served collection statements (the API's
//!   own SQL files, and a copy of its inline roles statement).
//!
//! Excluded, by design: `children_current`'s provenance, chain positions, canonicality summary
//! and manifest version (per-row target blocks the families do not keep), and `/roles` evidence
//! ids (the evidence arrays leave the F8 row). Named expected deltas are counted, not failed.
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};

use anyhow::{Context, Result, ensure};
use bigname_storage::{
    ChildrenCurrentKeysetCursor, ChildrenCurrentOrder, ChildrenCurrentPageFilter,
    ChildrenCurrentRow, ChildrenCurrentSort, NameCurrentListCursor, NameCurrentListCursorValue,
    NameCurrentRow,
    families::topology::{
        self as family, ClassificationSource, FamilyChildRow, FamilyCollectionPage,
    },
    load_children_current_page_filtered, load_phase_resolver_bound_name_rows,
    load_phase_resolver_current,
};
use serde_json::{Value, json};
use sqlx::{PgPool, types::time::OffsetDateTime};

/// How far the comparison walks.
#[derive(Clone, Copy, Debug)]
pub struct Settings {
    pub children_page: u64,
    pub collection_page: u64,
    /// Compare the timestamp sorts and the expiry fence too, not only the default page.
    pub every_child_filter: bool,
}

/// One difference between a served read and its shadow. `key` names the read exactly (for
/// example `children of ens:0x.. filter 2` or `links of 0x..`), so a named expected difference
/// matches one key and never a family of keys.
#[derive(Clone, Debug, PartialEq)]
pub struct Mismatch {
    pub key: String,
    pub detail: String,
}

impl std::fmt::Display for Mismatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.key, self.detail)
    }
}

/// What the comparison saw.
#[derive(Debug, Default)]
pub struct Report {
    pub target: i64,
    pub parents: usize,
    pub child_rows: usize,
    pub child_pages: usize,
    pub topology_names: usize,
    pub resolvers: usize,
    pub bound_names: usize,
    pub aliases: usize,
    pub links: usize,
    pub roles: usize,
    /// Served resolvers the shadow classified from the declaration manifest because F3 is
    /// unfilled, and the subset whose served mirror that fallback does not reproduce.
    pub f3_unfilled: usize,
    pub f3_unfilled_mirror_differs: usize,
    pub mismatches: Vec<Mismatch>,
    /// Shadow time per reader: total microseconds and keys read.
    pub timings: BTreeMap<&'static str, (u128, usize)>,
}

impl Report {
    fn time(&mut self, reader: &'static str, started: Instant) {
        let entry = self.timings.entry(reader).or_default();
        entry.0 += started.elapsed().as_micros();
        entry.1 += 1;
    }

    fn mismatch(&mut self, key: String, detail: String) {
        self.mismatches.push(Mismatch { key, detail });
    }

    /// One line for the pull request: counts, mismatches and mean shadow time per key.
    pub fn line(&self) -> String {
        let timings = self
            .timings
            .iter()
            .map(|(reader, (micros, keys))| {
                format!("{reader}_us_per_key={}", micros / (*keys).max(1) as u128)
            })
            .collect::<Vec<_>>()
            .join(" ");
        format!(
            "SEPOLIA_END_TO_END_SHADOW target={} parents={} child_rows={} child_pages={} \
             topology_names={} resolvers={} bound_names={} aliases={} links={} roles={} \
             f3_unfilled={} f3_unfilled_mirror_differs={} mismatches={} {timings}",
            self.target,
            self.parents,
            self.child_rows,
            self.child_pages,
            self.topology_names,
            self.resolvers,
            self.bound_names,
            self.aliases,
            self.links,
            self.roles,
            self.f3_unfilled,
            self.f3_unfilled_mirror_differs,
            self.mismatches.len(),
        )
    }
}

/// The served and family markers must name the same block: that block, and the family marker's
/// block timestamp, which is the clock the time-dependent filters read.
pub async fn publication(pool: &PgPool, chain: &str) -> Result<(i64, OffsetDateTime)> {
    let served: (Option<i64>, Option<String>) = sqlx::query_as(
        "SELECT current_block_number, current_block_hash FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(chain)
    .fetch_one(pool)
    .await?;
    let family: Option<(Option<i64>, Option<String>, Option<OffsetDateTime>)> = sqlx::query_as(
        "SELECT current_block_number, current_block_hash, block_timestamp
         FROM project_family_marker WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_optional(pool)
    .await?;
    let family = family.unwrap_or_default();
    ensure!(
        served.0.is_some() && (family.0, family.1.as_ref()) == (served.0, served.1.as_ref()),
        "the families are at {:?}, the served publication at {:?}: the shadow comparison runs \
         at one publication only",
        family.0,
        served.0
    );
    Ok((
        served.0.unwrap_or_default(),
        family
            .2
            .context("the family marker has no block timestamp")?,
    ))
}

pub async fn compare(pool: &PgPool, chain: &str, settings: Settings) -> Result<Report> {
    let (target, clock) = publication(pool, chain).await?;
    let mut report = Report {
        target,
        ..Report::default()
    };
    children(pool, chain, settings, clock, &mut report).await?;
    topology(pool, chain, &mut report).await?;
    resolvers(pool, chain, target, settings, &mut report).await?;
    // The comparison read one publication throughout.
    ensure!(
        publication(pool, chain).await?.0 == target,
        "the publication moved during the shadow comparison"
    );
    Ok(report)
}

/// The subnames filters the comparison reads: the default page, and with `every` also the expiry
/// sort behind the expiry fence at `clock`, the registration sort descending, and the fence alone
/// descending. A filter's index in this list is the `filter` number in a mismatch key.
pub fn child_filters(
    clock: OffsetDateTime,
    every: bool,
) -> Vec<ChildrenCurrentPageFilter<'static>> {
    let default = ChildrenCurrentPageFilter::default();
    let mut filters = vec![default];
    if every {
        filters.extend([
            ChildrenCurrentPageFilter {
                include_expired: false,
                evaluated_at: Some(clock),
                sort: ChildrenCurrentSort::ExpiresAt,
                ..default
            },
            ChildrenCurrentPageFilter {
                sort: ChildrenCurrentSort::RegisteredAt,
                order: ChildrenCurrentOrder::Desc,
                ..default
            },
            ChildrenCurrentPageFilter {
                include_expired: false,
                evaluated_at: Some(clock),
                order: ChildrenCurrentOrder::Desc,
                ..default
            },
        ]);
    }
    filters
}

/// Every row both subnames readers serve for `parent` under `filter`, each reader walking its own
/// cursors: `(served total, served rows, shadow total, shadow rows)`.
pub async fn walk_children(
    pool: &PgPool,
    parent: &str,
    filter: &ChildrenCurrentPageFilter<'_>,
    page: u64,
) -> Result<(u64, Vec<FamilyChildRow>, u64, Vec<FamilyChildRow>)> {
    let mut served_rows = Vec::new();
    let mut served_total = None;
    let mut cursor: Option<ChildrenCurrentKeysetCursor> = None;
    loop {
        let served =
            load_children_current_page_filtered(pool, parent, filter, cursor.as_ref(), page)
                .await?;
        served_total.get_or_insert(served.total_count);
        served_rows.extend(served.rows.iter().map(wire));
        match served.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    let mut shadow_rows = Vec::new();
    let mut shadow_total = None;
    let mut cursor: Option<ChildrenCurrentKeysetCursor> = None;
    loop {
        let shadow =
            family::load_children_shadow_page(pool, parent, filter, cursor.as_ref(), page).await?;
        shadow_total.get_or_insert(shadow.total_count);
        shadow_rows.extend(shadow.rows);
        match shadow.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    Ok((
        served_total.unwrap_or_default(),
        served_rows,
        shadow_total.unwrap_or_default(),
        shadow_rows,
    ))
}

fn wire(row: &ChildrenCurrentRow) -> FamilyChildRow {
    FamilyChildRow {
        parent_logical_name_id: row.parent_logical_name_id.clone(),
        child_logical_name_id: row.child_logical_name_id.clone(),
        namespace: row.namespace.clone(),
        canonical_display_name: row.canonical_display_name.clone(),
        namehash: row.namehash.clone(),
        labelhash: row.labelhash.clone(),
        owner: row.owner.clone(),
        registrant: row.registrant.clone(),
    }
}

async fn children(
    pool: &PgPool,
    chain: &str,
    settings: Settings,
    clock: OffsetDateTime,
    report: &mut Report,
) -> Result<()> {
    let parents: Vec<String> = sqlx::query_scalar(
        "SELECT parent_logical_name_id FROM children_current
         UNION
         SELECT surface.logical_name_id FROM project_child_edge_candidate edge
         JOIN name_surfaces surface
           ON surface.chain_id = edge.chain_id AND surface.namespace = edge.namespace
          AND lower(surface.namehash) = edge.parent_node
         WHERE edge.chain_id = $1
         UNION
         SELECT logical_name_id FROM project_parent_subregistry WHERE chain_id = $1
         ORDER BY 1",
    )
    .bind(chain)
    .fetch_all(pool)
    .await?;
    report.parents = parents.len();
    let filters = child_filters(clock, settings.every_child_filter);
    for parent in &parents {
        for (index, filter) in filters.iter().enumerate() {
            let mut served_cursor: Option<ChildrenCurrentKeysetCursor> = None;
            let mut shadow_cursor: Option<ChildrenCurrentKeysetCursor> = None;
            loop {
                let served = load_children_current_page_filtered(
                    pool,
                    parent,
                    filter,
                    served_cursor.as_ref(),
                    settings.children_page,
                )
                .await?;
                let started = Instant::now();
                let shadow = family::load_children_shadow_page(
                    pool,
                    parent,
                    filter,
                    shadow_cursor.as_ref(),
                    settings.children_page,
                )
                .await?;
                report.time("children", started);
                report.child_pages += 1;
                let served_rows: Vec<FamilyChildRow> = served.rows.iter().map(wire).collect();
                if index == 0 {
                    report.child_rows += served_rows.len();
                }
                if served_rows != shadow.rows
                    || served.total_count != shadow.total_count
                    || served.next_cursor != shadow.next_cursor
                {
                    report.mismatch(
                        format!("children of {parent} filter {index}"),
                        format!(
                            "served total {} rows {served_rows:?} next {:?}; \
                             shadow total {} rows {:?} next {:?}",
                            served.total_count,
                            served.next_cursor,
                            shadow.total_count,
                            shadow.rows,
                            shadow.next_cursor
                        ),
                    );
                    break;
                }
                match served.next_cursor {
                    Some(next) => {
                        served_cursor = Some(next.clone());
                        shadow_cursor = Some(next);
                    }
                    None => break,
                }
            }
        }
    }
    Ok(())
}

const TOPOLOGY_KINDS: [&str; 2] = ["resolver_alias_path", "observed_wildcard_path"];

async fn topology(pool: &PgPool, chain: &str, report: &mut Report) -> Result<()> {
    let names: Vec<(String, Option<Value>, bool)> = sqlx::query_as(
        "SELECT nc.logical_name_id, nc.declared_summary -> 'topology',
                nc.binding_kind = ANY($2)
         FROM name_current nc
         WHERE nc.binding_kind = ANY($2)
            OR nc.logical_name_id IN (
                SELECT logical_name_id FROM project_name_alias WHERE chain_id = $1
                UNION
                SELECT logical_name_id FROM project_binding_candidate
                WHERE chain_id = $1 AND binding_kind = ANY($2))
         ORDER BY 1",
    )
    .bind(chain)
    .bind(TOPOLOGY_KINDS.as_slice())
    .fetch_all(pool)
    .await?;
    for (name, served, on_arm) in names {
        report.topology_names += 1;
        let started = Instant::now();
        let shadow = family::load_name_topology_shadow(pool, &name).await?;
        report.time("topology", started);
        let served = served.filter(|_| on_arm);
        if served != shadow {
            report.mismatch(
                format!("topology of {name}"),
                format!("served {served:?}, shadow {shadow:?}"),
            );
        }
    }
    Ok(())
}

fn namespace_of(chain: &str) -> &'static str {
    if chain.starts_with("base-") {
        "basenames"
    } else {
        "ens"
    }
}

async fn resolvers(
    pool: &PgPool,
    chain: &str,
    target: i64,
    settings: Settings,
    report: &mut Report,
) -> Result<()> {
    let addresses: Vec<String> = sqlx::query_scalar(
        "SELECT lower(resolver_address) FROM resolver_current WHERE chain_id = $1
         UNION SELECT resolver_address FROM project_resolver_alias WHERE chain_id = $1
         UNION SELECT resolver_address FROM project_resolver_link WHERE chain_id = $1
         UNION SELECT resolver_address FROM project_resource_pointer
               WHERE chain_id = $1 AND resolver_address IS NOT NULL
                 AND resolver_address NOT IN ('', '0x0000000000000000000000000000000000000000')
         UNION SELECT split_part(scope, ':', 3) FROM project_grant
               WHERE chain_id = $1 AND scope LIKE 'resolver:%'
         ORDER BY 1",
    )
    .bind(chain)
    .fetch_all(pool)
    .await?;
    for address in addresses {
        report.resolvers += 1;
        classification(pool, chain, &address, report).await?;
        bound_names(pool, chain, &address, settings, report).await?;
        for section in ["aliases", "links", "roles"] {
            collection(pool, chain, &address, section, target, settings, report).await?;
        }
    }
    Ok(())
}

async fn classification(
    pool: &PgPool,
    chain: &str,
    address: &str,
    report: &mut Report,
) -> Result<()> {
    let served = load_phase_resolver_current(pool, chain, address).await?;
    let started = Instant::now();
    let shadow = family::load_resolver_shadow(pool, chain, address).await?;
    report.time("resolver", started);
    let Some(served) = served else {
        if shadow
            .as_ref()
            .is_some_and(|shadow| shadow.source == ClassificationSource::Family)
        {
            report.mismatch(
                format!("resolver {address}"),
                "a classification row with no served row".to_owned(),
            );
        }
        return Ok(());
    };
    let served_mirror = served
        .declared_summary
        .pointer("/classification/mirror/mirrored_registry_address")
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase);
    let support = served
        .coverage
        .get("status")
        .and_then(Value::as_str)
        .map(|status| {
            if status == "projected" {
                "supported"
            } else {
                "unsupported"
            }
        });
    match shadow {
        Some(shadow) if shadow.source == ClassificationSource::Family => {
            if shadow.mirrored_registry_address() != served_mirror
                || shadow.support_status.as_deref() != support
            {
                report.mismatch(
                    format!("resolver {address}"),
                    format!(
                        "served mirror {served_mirror:?} support {support:?}, \
                         classification row {shadow:?}"
                    ),
                );
            }
        }
        fallback => {
            // project_resolver_classification is not filled yet, so the shadow classifies from
            // the declaration manifest. A declaration carries no support status, so support is
            // not compared on this path until the table is filled; the mirror is, and every
            // caller asserts `f3_unfilled_mirror_differs == 0`.
            report.f3_unfilled += 1;
            let mirror = fallback.and_then(|shadow| shadow.mirrored_registry_address());
            if mirror != served_mirror {
                report.f3_unfilled_mirror_differs += 1;
            }
        }
    }
    Ok(())
}

fn bound_cursor(row: &NameCurrentRow) -> NameCurrentListCursor {
    NameCurrentListCursor {
        sort_value: NameCurrentListCursorValue::Name(row.canonical_display_name.clone()),
        namespace: row.namespace.clone(),
        normalized_name: row.normalized_name.clone(),
        namehash: row.namehash.clone(),
    }
}

async fn bound_names(
    pool: &PgPool,
    chain: &str,
    address: &str,
    settings: Settings,
    report: &mut Report,
) -> Result<()> {
    let limit = i64::try_from(settings.collection_page)? + 1;
    let mut cursor: Option<NameCurrentListCursor> = None;
    loop {
        let served =
            load_phase_resolver_bound_name_rows(pool, chain, address, None, cursor.as_ref(), limit)
                .await?;
        let started = Instant::now();
        let shadow =
            family::load_bound_names_shadow(pool, chain, address, None, cursor.as_ref(), limit)
                .await?;
        report.time("bound_names", started);
        let served_ids: Vec<&str> = served
            .iter()
            .map(|row| row.logical_name_id.as_str())
            .collect();
        let shadow_ids: Vec<&str> = shadow
            .iter()
            .map(|row| row.logical_name_id.as_str())
            .collect();
        if served_ids != shadow_ids {
            report.mismatch(
                format!("bound_names of {address}"),
                format!("served {served_ids:?}, shadow {shadow_ids:?}"),
            );
            return Ok(());
        }
        let page = usize::try_from(settings.collection_page)?;
        report.bound_names += served.len().min(page);
        if served.len() <= page {
            return Ok(());
        }
        cursor = Some(bound_cursor(&served[page - 1]));
    }
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
              {}
        )"#,
            bigname_storage::DEFAULT_PERMISSIONS_CURRENT_READ_FILTER
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

pub async fn served_collection(
    pool: &PgPool,
    chain: &str,
    address: &str,
    section: &str,
    height: i64,
    after: Option<&(String, String)>,
    limit: i64,
) -> Result<FamilyCollectionPage> {
    let sql = served_collection_sql(section);
    let mut statement = sqlx::query_as::<_, (i64, Value)>(&sql)
        .bind(chain)
        .bind(address)
        .bind(height)
        .bind(after.map(|key| key.0.as_str()))
        .bind(after.map(|key| key.1.as_str()))
        .bind(limit);
    if section == "links" {
        statement = statement.bind(namespace_of(chain));
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
    Ok(FamilyCollectionPage {
        rows,
        total_count: u64::try_from(total)?,
    })
}

/// `/roles` evidence ids are excluded: F8 drops the evidence arrays from the row.
fn comparable(section: &str, page: &FamilyCollectionPage) -> Vec<(String, String, Value)> {
    page.rows
        .iter()
        .map(|(key1, key2, item)| {
            let mut item = item.clone();
            if section == "roles"
                && let Some(object) = item.as_object_mut()
            {
                object.remove("event_ids");
            }
            (key1.clone(), key2.clone(), item)
        })
        .collect()
}

async fn collection(
    pool: &PgPool,
    chain: &str,
    address: &str,
    section: &'static str,
    height: i64,
    settings: Settings,
    report: &mut Report,
) -> Result<()> {
    let page = usize::try_from(settings.collection_page)?;
    let limit = i64::try_from(settings.collection_page)? + 1;
    let mut after: Option<(String, String)> = None;
    loop {
        let served =
            served_collection(pool, chain, address, section, height, after.as_ref(), limit).await?;
        let started = Instant::now();
        let shadow = match section {
            "aliases" => {
                family::load_resolver_aliases_shadow(pool, chain, address, after.as_ref(), limit)
                    .await?
            }
            "links" => {
                family::load_resolver_links_shadow(
                    pool,
                    chain,
                    address,
                    namespace_of(chain),
                    after.as_ref(),
                    limit,
                )
                .await?
            }
            _ => {
                family::load_resolver_roles_shadow(pool, chain, address, after.as_ref(), limit)
                    .await?
            }
        };
        report.time(section, started);
        let served_rows = comparable(section, &served);
        if served_rows != comparable(section, &shadow) || served.total_count != shadow.total_count {
            report.mismatch(
                format!("{section} of {address}"),
                format!(
                    "served total {} {served_rows:?}; shadow total {} {:?}",
                    served.total_count, shadow.total_count, shadow.rows
                ),
            );
            return Ok(());
        }
        let counted = served.rows.len().min(page);
        match section {
            "aliases" => report.aliases += counted,
            "links" => report.links += counted,
            _ => report.roles += counted,
        }
        if served.rows.len() <= page {
            return Ok(());
        }
        let (key1, key2, _) = &served.rows[page - 1];
        after = Some((key1.clone(), key2.clone()));
    }
}

/// Keys a shadow page served, for fixture assertions.
pub async fn shadow_children(pool: &PgPool, parent: &str) -> Result<BTreeSet<String>> {
    let page = family::load_children_shadow_page(
        pool,
        parent,
        &ChildrenCurrentPageFilter::default(),
        None,
        10_000,
    )
    .await?;
    Ok(page
        .rows
        .into_iter()
        .map(|row| row.child_logical_name_id)
        .collect())
}

/// The report as JSON, for a failure message.
pub fn describe(report: &Report) -> Value {
    json!({
        "line": report.line(),
        "mismatches": report
            .mismatches
            .iter()
            .take(20)
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
    })
}
