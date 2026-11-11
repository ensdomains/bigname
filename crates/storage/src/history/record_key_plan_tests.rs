//! Pages filtered by one record key. An unanchored page, scoped by an emitter or not, reads one
//! `RecordChanged` arm and one `RecordVersionChanged` arm per chain of its window. Each arm reads
//! its own partial index with the chain, the key and, when scoped, the emitter in its index
//! condition, and stops after one page. The pages return the plain statement's rows in its
//! order, with or without the indexes.
use super::super::super::{
    duplicates::push_product_history_duplicate_filter,
    paging::{push_history_filters, push_history_order},
    resolver_source::ResolverSource,
    source,
};
use super::*;
use std::io::Write;

const KEY_INDEX: &str = "normalized_events_record_key_history_idx";
const EMITTER_KEY_INDEX: &str = "normalized_events_record_emitter_key_history_idx";
/// The reset arm, pinned by what it reads rather than by an index: only `RecordVersionChanged`
/// entries of its chain, at most the chain's resets.
const RESET_ARM: &str = "reset arm";
const RECORD_INDEXES: [&str; 2] = [KEY_INDEX, EMITTER_KEY_INDEX];
/// The fixture's resets on the key chain, the most any reset arm may read.
const KEY_CHAIN_RESETS: f64 = 9.0;
/// The key equality only a `RecordChanged` arm carries.
const RECORD_ARM_SQL: &str = "(candidate.after_state ->> 'record_key') = ";

const KEY_CHAIN: &str = "ethereum-sepolia";
const KEY_OTHER_CHAIN: &str = "other-chain";
const KEY_TOP: i64 = 20_000;
const KEY_LIMIT: i64 = 21;
/// Holds most writes of the common keys, like the PublicResolver.
const LARGE: &str = "0x00000000000000000000000000000000000000a7";
/// Writes only `text:url`, so it has no row of the rare or common keys.
const SMALL_NONE: &str = "0x00000000000000000000000000000000000000b7";
/// A few writes of each common key and of the rare key.
const SMALL_FEW: &str = "0x00000000000000000000000000000000000000c7";
/// A deep cursor position: the large emitter's `text:avatar` write at block 10,000.
const DEEP_AVATAR: &str = "key:ethereum-sepolia:large:text:avatar:10000";
/// The same position seen from the oldest end.
const SHALLOW_AVATAR: &str = "key:ethereum-sepolia:large:text:avatar:2000";

/// A pointer change in every block of two chains, and record writes from four emitters:
/// - the large emitter writes `text:avatar` every 10th block (1,998 rows) and `addr:60` every
///   7th (2,854), the rare `text:links` 5 times and `text:Case, and space ` 3 times;
/// - the small emitter with a few rows writes each of `text:avatar`, `addr:60` 3 times and
///   `text:links` twice; the other small emitter writes only `text:url`;
/// - a noise emitter writes `text:noise` every 4th block;
/// - nine record-version resets across the emitters on the key chain, one on the other chain;
/// - on the other chain, the large emitter's address writes `text:avatar` 50 times and
///   `text:links` 3 times;
/// - one `text:avatar` write with no block;
/// - at the newest end, `text:avatar` writes the page must never return: orphaned rows,
///   candidate rows, rows in orphaned lineage blocks, rows of another namespace and
///   registry read copies. An arm that applied any of these conditions only after its limit
///   would lose rows behind them.
const KEY_FIXTURE: &str = r#"
    INSERT INTO chain_lineage
        (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
    SELECT chain, chain || '-block-' || n, n, to_timestamp(n),
           CASE WHEN n BETWEEN 19991 AND 19993 THEN 'orphaned'::canonicality_state
                ELSE 'canonical'::canonicality_state END
    FROM unnest(ARRAY['ethereum-sepolia', 'other-chain']) chain
    CROSS JOIN generate_series(1, 20000) n;
    INSERT INTO normalized_events
        (event_identity, namespace, event_kind, source_family, manifest_version,
         chain_id, block_hash, block_number, transaction_hash, transaction_index,
         log_index, derivation_kind, canonicality_state, after_state, raw_fact_ref)
    SELECT 'noise:' || chain || ':' || n, 'ens', 'ResolverChanged', 'ens_v1_registry_l1', 1,
           chain, chain || '-block-' || n, n, 'tx-' || n, 0, 0,
           'ens_v1_unwrapped_authority', 'canonical',
           jsonb_build_object('node', 'node-' || n,
                              'resolver', '0x0000000000000000000000000000000000000099'),
           '{}'::jsonb
    FROM unnest(ARRAY['ethereum-sepolia', 'other-chain']) chain
    CROSS JOIN generate_series(1, 20000) n;
    INSERT INTO normalized_events
        (event_identity, namespace, event_kind, source_family, manifest_version,
         chain_id, block_hash, block_number, transaction_hash, transaction_index,
         log_index, derivation_kind, canonicality_state, after_state, raw_fact_ref)
    SELECT 'key:' || chain || ':' || emitter_name || ':' || record_key || ':' || n, 'ens',
           'RecordChanged', 'ens_v1_resolver_l1', 1,
           chain, chain || '-block-' || n, n, 'tx-' || n, 0, log_index,
           'ens_v1_unwrapped_authority', 'canonical',
           jsonb_build_object('node', 'node-key', 'record_key', record_key),
           jsonb_build_object('emitting_address', emitter)
    FROM (VALUES
            ('ethereum-sepolia', 'large', '0x00000000000000000000000000000000000000a7',
             'text:avatar', 10, 10, 19980, 1),
            ('ethereum-sepolia', 'large', '0x00000000000000000000000000000000000000a7',
             'addr:60', 7, 7, 19980, 2),
            ('ethereum-sepolia', 'large', '0x00000000000000000000000000000000000000a7',
             'text:links', 1000, 4000, 17000, 3),
            ('ethereum-sepolia', 'large', '0x00000000000000000000000000000000000000a7',
             'text:Case, and space ', 3001, 8000, 19001, 4),
            ('ethereum-sepolia', 'few', '0x00000000000000000000000000000000000000c7',
             'text:avatar', 2003, 6000, 14003, 5),
            ('ethereum-sepolia', 'few', '0x00000000000000000000000000000000000000c7',
             'addr:60', 4005, 6000, 16005, 6),
            ('ethereum-sepolia', 'few', '0x00000000000000000000000000000000000000c7',
             'text:links', 7007, 5000, 12007, 7),
            ('ethereum-sepolia', 'none', '0x00000000000000000000000000000000000000b7',
             'text:url', 1509, 5000, 16509, 8),
            ('ethereum-sepolia', 'noise', '0x00000000000000000000000000000000000000f7',
             'text:noise', 4, 4, 19980, 9),
            ('other-chain', 'large', '0x00000000000000000000000000000000000000a7',
             'text:avatar', 400, 400, 20000, 1),
            ('other-chain', 'large', '0x00000000000000000000000000000000000000a7',
             'text:links', 6000, 6000, 18000, 3))
         writes(chain, emitter_name, emitter, record_key, first_block, step, last_block, log_index)
    CROSS JOIN LATERAL generate_series(first_block, last_block, step) n;
    INSERT INTO normalized_events
        (event_identity, namespace, event_kind, source_family, manifest_version,
         chain_id, block_hash, block_number, transaction_hash, transaction_index,
         log_index, derivation_kind, canonicality_state, after_state, raw_fact_ref)
    SELECT 'reset:' || chain || ':' || emitter_name || ':' || n, 'ens',
           'RecordVersionChanged', 'ens_v1_resolver_l1', 1,
           chain, chain || '-block-' || n, n, 'tx-' || n, 0, 10,
           'ens_v1_unwrapped_authority', 'canonical',
           jsonb_build_object('node', 'node-key', 'record_version', 1),
           jsonb_build_object('emitting_address', emitter)
    FROM (VALUES
            ('ethereum-sepolia', 'large', '0x00000000000000000000000000000000000000a7', 2500),
            ('ethereum-sepolia', 'large', '0x00000000000000000000000000000000000000a7', 7500),
            ('ethereum-sepolia', 'large', '0x00000000000000000000000000000000000000a7', 12500),
            ('ethereum-sepolia', 'large', '0x00000000000000000000000000000000000000a7', 17500),
            ('ethereum-sepolia', 'few', '0x00000000000000000000000000000000000000c7', 6500),
            ('ethereum-sepolia', 'few', '0x00000000000000000000000000000000000000c7', 15500),
            ('ethereum-sepolia', 'none', '0x00000000000000000000000000000000000000b7', 3500),
            ('ethereum-sepolia', 'none', '0x00000000000000000000000000000000000000b7', 9500),
            ('ethereum-sepolia', 'noise', '0x00000000000000000000000000000000000000f7', 11500),
            ('other-chain', 'large', '0x00000000000000000000000000000000000000a7', 9000))
         resets(chain, emitter_name, emitter, n);
    INSERT INTO normalized_events
        (event_identity, namespace, event_kind, source_family, manifest_version,
         chain_id, derivation_kind, canonicality_state, after_state, raw_fact_ref)
    VALUES
        ('key:null:large:text:avatar', 'ens', 'RecordChanged', 'ens_v1_resolver_l1', 1,
         'ethereum-sepolia', 'ens_v1_unwrapped_authority', 'canonical',
         jsonb_build_object('node', 'node-key', 'record_key', 'text:avatar'),
         jsonb_build_object('emitting_address', '0x00000000000000000000000000000000000000a7'));
    INSERT INTO normalized_events
        (event_identity, namespace, event_kind, source_family, manifest_version,
         chain_id, block_hash, block_number, transaction_hash, transaction_index,
         log_index, derivation_kind, canonicality_state, consumer_visibility,
         migration_correlation_ids, after_state, raw_fact_ref)
    SELECT identity_ || n, namespace_, 'RecordChanged', 'ens_v1_resolver_l1', 1,
           'ethereum-sepolia', 'ethereum-sepolia-block-' || n, n, 'tx-' || n, 0, 11,
           'ens_v1_unwrapped_authority', state_::canonicality_state, visibility_,
           CASE WHEN visibility_ = 'candidate' THEN ARRAY['candidate-1'] ELSE ARRAY[]::text[] END,
           jsonb_build_object('node', 'node-key', 'record_key', 'text:avatar'),
           jsonb_build_object('emitting_address', '0x00000000000000000000000000000000000000a7')
    FROM (VALUES
            ('excluded:namespace:', 'basenames', 'canonical', 'activated', 19985, 19987),
            ('excluded:dup:ResolverChanged:registry-read:', 'ens', 'canonical', 'activated',
             19988, 19990),
            ('excluded:lineage:', 'ens', 'canonical', 'activated', 19991, 19993),
            ('excluded:orphaned:', 'ens', 'orphaned', 'activated', 19994, 19996),
            ('excluded:candidate:', 'ens', 'canonical', 'candidate', 19997, 19999))
         excluded(identity_, namespace_, state_, visibility_, first_block, last_block)
    CROSS JOIN LATERAL generate_series(first_block, last_block) n;
"#;

const KEY_ANALYZE: &str = "ANALYZE normalized_events; ANALYZE chain_lineage;";

#[derive(Clone, Copy, Debug)]
enum KeyScope {
    Unscoped,
    Contract(&'static str),
    Resolver(&'static str, &'static str),
}

fn key_filter(
    key: &str,
    order: HistoryOrder,
    scope: KeyScope,
    block_window: Option<HistoryBlockWindow>,
) -> EventHistoryReadFilter {
    let mut filter = EventHistoryReadFilter {
        namespace: Some("ens".to_owned()),
        event_kinds: vec![
            "RecordChanged".to_owned(),
            "RecordVersionChanged".to_owned(),
        ],
        record_key: Some(key.to_owned()),
        bind_cursor_anchor_to_event_kinds: true,
        order,
        block_window,
        ..EventHistoryReadFilter::default()
    };
    match scope {
        KeyScope::Unscoped => {}
        KeyScope::Contract(address) => filter.contract_address = Some(address.to_owned()),
        KeyScope::Resolver(chain_id, address) => {
            filter.resolver = Some(EventHistoryResolverFilter {
                chain_id: chain_id.to_owned(),
                address: address.to_owned(),
            });
        }
    }
    filter
}

fn key_window() -> Option<HistoryBlockWindow> {
    Some(window(&[(KEY_CHAIN, None, Some(KEY_TOP))]))
}

fn two_chain_window() -> Option<HistoryBlockWindow> {
    Some(window(&[
        (KEY_CHAIN, None, Some(KEY_TOP)),
        (KEY_OTHER_CHAIN, None, Some(KEY_TOP)),
    ]))
}

/// One page read to plan, and the indexes its arms must read, one entry per arm.
struct KeyShape {
    name: String,
    filter: EventHistoryReadFilter,
    cursor: Option<&'static str>,
    limit: i64,
    arms: Vec<&'static str>,
}

fn kinds(filter: &mut EventHistoryReadFilter, kinds: &[&str]) {
    filter.event_kinds = kinds.iter().map(|kind| (*kind).to_owned()).collect();
}

/// The step-2 rows on an unscoped read and the scoped grid, in both orders where the row has
/// one.
fn plan_shapes() -> Vec<KeyShape> {
    let mut shapes = Vec::new();
    let both = [KEY_INDEX, RESET_ARM];
    for order in [HistoryOrder::Desc, HistoryOrder::Asc] {
        let mut add =
            |name: &str, filter: EventHistoryReadFilter, cursor, arms: &[&'static str]| {
                shapes.push(KeyShape {
                    name: format!("{name} {order:?}"),
                    filter,
                    cursor,
                    limit: KEY_LIMIT,
                    arms: arms.to_vec(),
                });
            };
        let unscoped = |key: &str| key_filter(key, order, KeyScope::Unscoped, key_window());
        add("row 2/3 common", unscoped("text:avatar"), None, &both);
        let mut deep = unscoped("text:avatar");
        deep.to_block = Some(5);
        add("row 4 to_block below every match", deep, None, &both);
        let cursor = match order {
            HistoryOrder::Desc => DEEP_AVATAR,
            HistoryOrder::Asc => SHALLOW_AVATAR,
        };
        add(
            "row 5 deep cursor",
            unscoped("text:avatar"),
            Some(cursor),
            &both,
        );
        add("row 6 rare", unscoped("text:links"), None, &both);
        add("row 7 unknown", unscoped("text:nosuch"), None, &both);
        let mut written = unscoped("text:avatar");
        kinds(&mut written, &["RecordChanged"]);
        add("row 9 writes only", written, None, &[KEY_INDEX]);
        let mut unknown_written = unscoped("text:nosuch");
        kinds(&mut unknown_written, &["RecordChanged"]);
        add(
            "row 10 unknown writes only",
            unknown_written,
            None,
            &[KEY_INDEX],
        );
        let mut resets = unscoped("text:avatar");
        kinds(&mut resets, &["RecordVersionChanged"]);
        add("row 11 resets only", resets, None, &[RESET_ARM]);
        let mut nothing = unscoped("text:avatar");
        nothing.match_no_events = true;
        add("row 12 empty kind intersection", nothing, None, &[]);
        add(
            "row 18 two chains",
            key_filter("text:avatar", order, KeyScope::Unscoped, two_chain_window()),
            None,
            &[KEY_INDEX, RESET_ARM, KEY_INDEX, RESET_ARM],
        );
        add(
            "row 24 block range",
            key_filter(
                "text:avatar",
                order,
                KeyScope::Unscoped,
                Some(window(&[(KEY_CHAIN, Some(5_000), Some(15_000))])),
            ),
            None,
            &both,
        );
        let scoped = [EMITTER_KEY_INDEX, RESET_ARM];
        for scope in [
            KeyScope::Contract(LARGE),
            KeyScope::Resolver(KEY_CHAIN, LARGE),
        ] {
            let scope_of = |emitter| match scope {
                KeyScope::Contract(_) => KeyScope::Contract(emitter),
                _ => KeyScope::Resolver(KEY_CHAIN, emitter),
            };
            let at = |key: &str, emitter| key_filter(key, order, scope_of(emitter), key_window());
            let label = match scope {
                KeyScope::Contract(_) => "contract_address",
                _ => "resolver",
            };
            add(
                &format!("G-3 rare, small without it, {label}"),
                at("text:links", SMALL_NONE),
                None,
                &scoped,
            );
            add(
                &format!("G-4 rare, small with a few, {label}"),
                at("text:links", SMALL_FEW),
                None,
                &scoped,
            );
            add(
                &format!("G-5 rare, large, {label}"),
                at("text:links", LARGE),
                None,
                &scoped,
            );
            let mut rare_written = at("text:links", LARGE);
            kinds(&mut rare_written, &["RecordChanged"]);
            add(
                &format!("G-5 rare writes only, large, {label}"),
                rare_written,
                None,
                &[EMITTER_KEY_INDEX],
            );
            add(
                &format!("G-6 unknown, large, {label}"),
                at("text:nosuch", LARGE),
                None,
                &scoped,
            );
            add(
                &format!("G-7 common, small without it, {label}"),
                at("addr:60", SMALL_NONE),
                None,
                &scoped,
            );
            add(
                &format!("G-8 common, small with a few, {label}"),
                at("addr:60", SMALL_FEW),
                None,
                &scoped,
            );
            add(
                &format!("G-9 common, large, {label}"),
                at("text:avatar", LARGE),
                None,
                &scoped,
            );
            let mut bounded = at("text:avatar", LARGE);
            bounded.to_block = Some(5_000);
            add(
                &format!("G-10 common, large, deep to_block, {label}"),
                bounded,
                None,
                &scoped,
            );
            add(
                &format!("G-11 common, large, deep cursor, {label}"),
                at("text:avatar", LARGE),
                Some(cursor),
                &scoped,
            );
        }
        // Row 23, a record row with no block, is only readable without a block window, which
        // a served read always carries, so its plan is not pinned here. Its rows and order are
        // in `record_key_pages_match_the_plain_statement` ("no window" reads).
        add(
            "G-13 resolver outside the window",
            key_filter(
                "text:links",
                order,
                KeyScope::Resolver(KEY_OTHER_CHAIN, LARGE),
                key_window(),
            ),
            None,
            &[],
        );
    }
    shapes.push(KeyShape {
        name: "row 13 page of one".to_owned(),
        filter: key_filter(
            "text:avatar",
            HistoryOrder::Desc,
            KeyScope::Unscoped,
            key_window(),
        ),
        cursor: None,
        limit: 2,
        arms: vec![KEY_INDEX, RESET_ARM],
    });
    shapes
}

/// Every node of the page's own plan, not the per-row subqueries.
fn page_nodes<'a>(node: &'a Value, output: &mut Vec<&'a Value>) {
    if matches!(
        node["Parent Relationship"].as_str(),
        Some("SubPlan" | "InitPlan")
    ) {
        return;
    }
    output.push(node);
    if let Some(children) = node["Plans"].as_array() {
        for child in children {
            page_nodes(child, output);
        }
    }
}

/// The arm a scan serves: its index for a write arm, or [`RESET_ARM`] for a scan whose index
/// condition holds the reset kind and the chain, so it reads no `RecordChanged` entry.
fn arm_of(scan: &Value) -> String {
    let condition = scan["Index Cond"].as_str().unwrap_or_default();
    let index = scan["Index Name"].as_str().unwrap_or("none");
    if index != KEY_INDEX
        && index != EMITTER_KEY_INDEX
        && condition.contains("(event_kind = 'RecordVersionChanged'::text)")
        && condition.contains("chain_id")
    {
        RESET_ARM.to_owned()
    } else {
        index.to_owned()
    }
}

fn loops(node: &Value) -> f64 {
    node["Actual Loops"].as_f64().unwrap_or(1.0)
}

/// Plan every shape and report each one whose arms do not read their indexes by every
/// equality the read carries, or read more than a page.
async fn record_key_plan_failures(
    connection: &mut PgConnection,
    shapes: &[KeyShape],
    mode: PlanMode,
) -> Result<Vec<String>> {
    let mut failures = Vec::new();
    for shape in shapes {
        let name = &shape.name;
        let cursor = match shape.cursor {
            Some(identity) => Some(cursor_at(connection, identity).await?),
            None => None,
        };
        let keyset = match cursor.as_ref() {
            Some(cursor) => {
                Some(load_history_keyset(connection, &shape.filter, true, cursor, false).await?)
            }
            None => None,
        };
        let mut query = QueryBuilder::<Postgres>::new("");
        push_history_page_query(
            &mut query,
            &shape.filter,
            true,
            keyset.as_ref(),
            false,
            shape.limit,
        );
        let plan = explain_page(connection, query, mode).await?;
        let mut scans = Vec::new();
        resolver_page_scans(&plan[0]["Plan"], &mut scans);
        let mut read: Vec<String> = scans.iter().map(|scan| arm_of(scan)).collect();
        read.sort();
        let mut expected: Vec<String> = shape.arms.iter().map(|arm| (*arm).to_owned()).collect();
        expected.sort();
        let summary = scans
            .iter()
            .map(|scan| {
                format!(
                    "[{} {} {} cond {} filter {} rows {} removed {}]",
                    scan["Node Type"],
                    scan["Scan Direction"],
                    scan["Index Name"],
                    scan["Index Cond"],
                    scan["Filter"],
                    scan["Actual Rows"],
                    scan["Rows Removed by Filter"]
                )
            })
            .collect::<Vec<_>>()
            .join(" ");
        eprintln!("{name} {mode:?}: {summary}");
        if read != expected {
            failures.push(format!(
                "{name} {mode:?}: arms must read {expected:?}; read {summary}"
            ));
            continue;
        }
        let arm_count = shape.arms.len() as f64;
        let ceiling = shape.limit as f64 + 16.0;
        for scan in &scans {
            let index = scan["Index Name"].as_str().unwrap_or_default();
            let condition = scan["Index Cond"].as_str().unwrap_or_default();
            let filter = scan["Filter"].as_str().unwrap_or_default();
            if scan["Node Type"] != "Index Scan" {
                failures.push(format!(
                    "{name} {mode:?}: {index} must be an index scan; {summary}"
                ));
            }
            if !condition.contains("chain_id") {
                failures.push(format!(
                    "{name} {mode:?}: {index} must hold the chain in its index condition; {summary}"
                ));
            }
            let reset = arm_of(scan) == RESET_ARM;
            if !reset && !(condition.contains("\"left\"(") && condition.contains("record_key")) {
                failures.push(format!(
                    "{name} {mode:?}: {index} must hold the key prefix in its index condition; {summary}"
                ));
            }
            if index == EMITTER_KEY_INDEX && !condition.contains("emitting_address") {
                failures.push(format!("{name} {mode:?}: {index} must hold the emitter in its index condition; {summary}"));
            }
            if !reset && filter.contains("emitting_address") {
                failures.push(format!(
                    "{name} {mode:?}: {index} filters the emitter on the heap; {summary}"
                ));
            }
            let visited = (scan["Actual Rows"].as_f64().unwrap_or(0.0)
                + scan["Rows Removed by Filter"].as_f64().unwrap_or(0.0))
                * loops(scan);
            let ceiling = if reset { KEY_CHAIN_RESETS } else { ceiling };
            if visited > ceiling {
                failures.push(format!(
                    "{name} {mode:?}: {index} visited {visited} rows, over the ceiling {ceiling}; {summary}"
                ));
            }
        }
        let mut nodes = Vec::new();
        page_nodes(&plan[0]["Plan"], &mut nodes);
        for node in nodes {
            let node_type = node["Node Type"].as_str().unwrap_or_default();
            if node_type.contains("Bitmap") {
                failures.push(format!(
                    "{name} {mode:?}: the page must not read a bitmap; {summary}"
                ));
            }
            if node_type.ends_with("Sort") {
                let input: f64 = node["Plans"]
                    .as_array()
                    .map(|children| {
                        children
                            .iter()
                            .map(|child| {
                                child["Actual Rows"].as_f64().unwrap_or(0.0) * loops(child)
                            })
                            .sum()
                    })
                    .unwrap_or(0.0);
                if input > arm_count.max(1.0) * ceiling {
                    failures.push(format!(
                        "{name} {mode:?}: a {node_type} sorts {input} rows, over {} per arm",
                        ceiling
                    ));
                }
            }
        }
    }
    Ok(failures)
}

async fn key_database(name: &str, analyze: bool) -> Result<TestDatabase> {
    let fixture = if analyze {
        format!("{KEY_FIXTURE}{KEY_ANALYZE}")
    } else {
        KEY_FIXTURE.to_owned()
    };
    phase_database(name, &fixture).await
}

/// Plan every shape in `mode` and return each shape's failures against the bar.
async fn record_key_plan_report(name: &str, mode: PlanMode, analyze: bool) -> Result<Vec<String>> {
    let database = key_database(name, analyze).await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        record_key_plan_failures(&mut connection, &plan_shapes(), mode).await
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn record_key_pages_read_the_record_indexes() -> Result<()> {
    let failures =
        record_key_plan_report("history_record_key_plan", PlanMode::Unprepared, true).await?;
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

#[tokio::test]
async fn record_key_custom_plans_read_the_record_indexes() -> Result<()> {
    let failures =
        record_key_plan_report("history_record_key_custom", PlanMode::Custom, true).await?;
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

/// Test-only evidence, not a gate. Record-arm pages are never prepared (`load_history_page`
/// sends them unprepared), so no request reaches a generic plan of them. This records what a
/// generic plan would read: at the time of writing, the scoped oldest-first page from a deep
/// cursor read the key index with the emitter on the heap. Whoever prepares these pages finds
/// that here.
#[tokio::test]
async fn record_key_generic_plans_are_recorded_test_only() -> Result<()> {
    let failures =
        record_key_plan_report("history_record_key_generic", PlanMode::Generic, true).await?;
    // Written past the test harness's capture, so the run's log keeps the record.
    let mut log = std::io::stderr();
    for failure in &failures {
        writeln!(log, "generic plan, test-only: {failure}")?;
    }
    writeln!(
        log,
        "generic plan, test-only: {} shapes off the bar",
        failures.len()
    )?;
    Ok(())
}

/// Test-only evidence, not a gate. On a table with no statistics at all the planner can tie
/// the key index and the emitter-led index for a scoped arm. Served tables are analyzed. This
/// records what each shape reads before `ANALYZE`.
#[tokio::test]
async fn record_key_plans_before_analyze_are_recorded_test_only() -> Result<()> {
    let failures =
        record_key_plan_report("history_record_key_unanalyzed", PlanMode::Unprepared, false)
            .await?;
    let mut log = std::io::stderr();
    for failure in &failures {
        writeln!(log, "before ANALYZE, test-only: {failure}")?;
    }
    writeln!(
        log,
        "before ANALYZE, test-only: {} shapes off the bar",
        failures.len()
    )?;
    Ok(())
}

/// The rows the plain statement returns for `filter`, in its order, unpaged: `source.rs`'s
/// source with every condition on `ne`.
async fn key_plain_identities(
    pool: &PgPool,
    filter: &EventHistoryReadFilter,
) -> Result<Vec<String>> {
    let mut query = QueryBuilder::<Postgres>::new("SELECT ne.event_identity");
    source::push_history_source_for_filter(&mut query, filter, true, false, false);
    push_history_filters(&mut query, filter, true, ResolverSource::Condition);
    push_product_history_duplicate_filter(&mut query, filter, true);
    push_history_order(&mut query, filter.order);
    Ok(query.build_query_scalar().fetch_all(pool).await?)
}

/// Every key, kind set, scope and window the plan shapes read, plus reads without a window
/// and with the scope the arms leave to the plain statement.
fn equivalence_filters() -> Vec<(String, EventHistoryReadFilter)> {
    let mut filters = Vec::new();
    for order in [HistoryOrder::Desc, HistoryOrder::Asc] {
        let mut add = |name: String, filter| filters.push((format!("{name} {order:?}"), filter));
        for key in [
            "text:avatar",
            "addr:60",
            "text:links",
            "text:nosuch",
            "avatar",
            "text:Case, and space ",
        ] {
            add(
                format!("unscoped {key}"),
                key_filter(key, order, KeyScope::Unscoped, key_window()),
            );
        }
        for (label, kind_set) in [
            ("writes", &["RecordChanged"][..]),
            ("resets", &["RecordVersionChanged"][..]),
        ] {
            for key in ["text:avatar", "text:nosuch"] {
                let mut filter = key_filter(key, order, KeyScope::Unscoped, key_window());
                kinds(&mut filter, kind_set);
                add(format!("unscoped {key} {label} only"), filter);
            }
        }
        let mut deep = key_filter("text:avatar", order, KeyScope::Unscoped, key_window());
        deep.to_block = Some(5);
        add("unscoped to_block below every match".to_owned(), deep);
        let mut nothing = key_filter("text:avatar", order, KeyScope::Unscoped, key_window());
        nothing.match_no_events = true;
        add("unscoped empty kind intersection".to_owned(), nothing);
        add(
            "unscoped block range".to_owned(),
            key_filter(
                "text:avatar",
                order,
                KeyScope::Unscoped,
                Some(window(&[(KEY_CHAIN, Some(5_000), Some(15_000))])),
            ),
        );
        add(
            "unscoped two chains".to_owned(),
            key_filter("text:avatar", order, KeyScope::Unscoped, two_chain_window()),
        );
        add(
            "unscoped no window".to_owned(),
            key_filter("text:avatar", order, KeyScope::Unscoped, None),
        );
        for emitter in [LARGE, SMALL_FEW, SMALL_NONE] {
            for scope in [
                KeyScope::Contract(emitter),
                KeyScope::Resolver(KEY_CHAIN, emitter),
            ] {
                for key in ["text:avatar", "addr:60", "text:links", "text:nosuch"] {
                    add(
                        format!("{scope:?} {key}"),
                        key_filter(key, order, scope, key_window()),
                    );
                }
                let mut written = key_filter("text:links", order, scope, key_window());
                kinds(&mut written, &["RecordChanged"]);
                add(format!("{scope:?} text:links writes only"), written);
                let mut bounded = key_filter("text:avatar", order, scope, key_window());
                bounded.to_block = Some(5_000);
                add(format!("{scope:?} text:avatar to_block"), bounded);
                add(
                    format!("{scope:?} text:avatar no window"),
                    key_filter("text:avatar", order, scope, None),
                );
            }
        }
        add(
            "resolver outside the window".to_owned(),
            key_filter(
                "text:links",
                order,
                KeyScope::Resolver(KEY_OTHER_CHAIN, LARGE),
                key_window(),
            ),
        );
        add(
            "resolver on the second chain of two".to_owned(),
            key_filter(
                "text:avatar",
                order,
                KeyScope::Resolver(KEY_OTHER_CHAIN, LARGE),
                two_chain_window(),
            ),
        );
    }
    filters
}

/// Every page of `filter` of `page_size` rows, from the first.
async fn key_walk(
    pool: &PgPool,
    filter: &EventHistoryReadFilter,
    page_size: u64,
) -> Result<Vec<String>> {
    let mut paged = Vec::new();
    let mut cursor = None;
    loop {
        let (rows, next) = page_identities(pool, filter, cursor.as_ref(), page_size).await?;
        paged.extend(rows);
        match next {
            Some(next) => cursor = Some(next),
            None => return Ok(paged),
        }
    }
}

/// The pages of `filter` against `expected`: the unpaged read, and either every page of a
/// few sizes or, for a long read, the first pages and one page from a deep cursor.
async fn key_page_failures(
    pool: &PgPool,
    name: &str,
    filter: &EventHistoryReadFilter,
    expected: &[String],
) -> Result<Vec<String>> {
    let mut failures = Vec::new();
    let (unpaged, no_more) = page_identities(pool, filter, None, 10_000).await?;
    if no_more.is_some() || unpaged != expected {
        failures.push(format!(
            "{name}: the unpaged read differs from the plain statement"
        ));
    }
    if expected.len() <= 80 {
        for page_size in [1u64, 7] {
            if key_walk(pool, filter, page_size).await? != expected {
                failures.push(format!(
                    "{name}: pages of {page_size} differ from the plain statement"
                ));
            }
        }
        return Ok(failures);
    }
    let (first, _) = page_identities(pool, filter, None, 1).await?;
    if first != expected[..1] {
        failures.push(format!("{name}: the page of one differs"));
    }
    let mut paged = Vec::new();
    let mut cursor = None;
    for _ in 0..3 {
        let (rows, next) = page_identities(pool, filter, cursor.as_ref(), 20).await?;
        paged.extend(rows);
        cursor = next;
    }
    if paged != expected[..60] {
        failures.push(format!("{name}: the first pages of 20 differ"));
    }
    let middle = expected.len() / 2;
    let (_, deep) = page_identities(pool, filter, None, middle as u64).await?;
    let (rows, _) = page_identities(pool, filter, deep.as_ref(), 20).await?;
    let end = (middle + 20).min(expected.len());
    if rows != expected[middle..end] {
        failures.push(format!("{name}: the page after a deep cursor differs"));
    }
    Ok(failures)
}

#[tokio::test]
async fn record_key_pages_match_the_plain_statement() -> Result<()> {
    let database = key_database("history_record_key_equivalence", true).await?;
    let result = async {
        let pool = database.pool();
        let mut failures = Vec::new();
        let mut nonempty = 0;
        for (name, filter) in equivalence_filters() {
            let expected = key_plain_identities(pool, &filter).await?;
            if !expected.is_empty() {
                nonempty += 1;
            }
            failures.extend(key_page_failures(pool, &name, &filter, &expected).await?);
        }
        // The fixture's own guard: every key and scope above but the unknown and bare keys, the
        // empty intersection and the window outside the resolver's chain has rows.
        ensure!(nonempty >= 80, "only {nonempty} reads returned rows");
        ensure!(failures.is_empty(), "{}", failures.join("\n"));
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

/// The indexes make the reads cheap. Without them every page returns the same rows and
/// cursors, and the plan check fails for every shape that reads a write arm.
#[tokio::test]
async fn record_key_pages_without_the_record_indexes_match_and_fail_the_plan_check() -> Result<()> {
    let database = key_database("history_record_key_no_index", true).await?;
    let result = async {
        let pool = database.pool();
        let shapes = plan_shapes();
        let mut before = Vec::new();
        for shape in &shapes {
            let first = page_identities(pool, &shape.filter, None, 20).await?;
            let second = page_identities(pool, &shape.filter, first.1.as_ref(), 20).await?;
            let unpaged = page_identities(pool, &shape.filter, None, 10_000).await?;
            before.push((first, second, unpaged));
        }
        for index in RECORD_INDEXES {
            sqlx::query(&format!("DROP INDEX IF EXISTS {index}"))
                .execute(pool)
                .await?;
        }
        for (shape, expected) in shapes.iter().zip(&before) {
            let first = page_identities(pool, &shape.filter, None, 20).await?;
            let second = page_identities(pool, &shape.filter, first.1.as_ref(), 20).await?;
            let unpaged = page_identities(pool, &shape.filter, None, 10_000).await?;
            ensure!(
                (first, second, unpaged) == *expected,
                "{}: pages differ without the record indexes",
                shape.name
            );
        }
        let mut connection = pool.acquire().await?;
        let failures =
            record_key_plan_failures(&mut connection, &shapes, PlanMode::Unprepared).await?;
        for shape in shapes
            .iter()
            .filter(|shape| shape.arms.iter().any(|arm| *arm != RESET_ARM))
        {
            ensure!(
                failures
                    .iter()
                    .any(|failure| failure.starts_with(&format!("{} ", shape.name))),
                "{} passed the plan check without the record indexes",
                shape.name
            );
        }
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

/// `include=total_count` on an emitter-scoped read runs the capped count over the plain
/// source. It counts the scoped read's rows and reads no record arm.
#[tokio::test]
async fn record_key_counts_keep_the_plain_source() -> Result<()> {
    let database = key_database("history_record_key_count", true).await?;
    let result = async {
        let pool = database.pool();
        for scope in [
            KeyScope::Contract(LARGE),
            KeyScope::Resolver(KEY_CHAIN, LARGE),
            KeyScope::Contract(SMALL_FEW),
        ] {
            let filter = key_filter("text:avatar", HistoryOrder::Desc, scope, key_window());
            let expected = key_plain_identities(pool, &filter).await?.len() as i64;
            for (limit, want) in [(None, expected), (Some(101i64), expected.min(101))] {
                let mut query = QueryBuilder::<Postgres>::new("");
                push_history_count_query(&mut query, &filter, true, limit);
                ensure!(
                    !query.sql().contains(RECORD_ARM_SQL),
                    "{scope:?}: the count reads a record arm"
                );
                let total: i64 = query.build_query_scalar().fetch_one(pool).await?;
                ensure!(
                    total == want,
                    "{scope:?} limit {limit:?}: counted {total}, expected {want}"
                );
            }
        }
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

/// Only an unanchored, canonical product page with a key reads the record arms. Anchored,
/// diagnostics and keyless pages, and every statement other than the page, keep their
/// sources.
#[test]
fn record_arms_serve_only_unanchored_canonical_key_pages() {
    let page_sql = |filter: &EventHistoryReadFilter, canonical_only: bool, candidates: bool| {
        let mut query = QueryBuilder::<Postgres>::new("");
        push_history_page_query(
            &mut query,
            filter,
            canonical_only,
            None,
            candidates,
            KEY_LIMIT,
        );
        query.sql().to_owned()
    };
    let unscoped = key_filter(
        "text:avatar",
        HistoryOrder::Desc,
        KeyScope::Unscoped,
        key_window(),
    );
    let contract = key_filter(
        "text:avatar",
        HistoryOrder::Desc,
        KeyScope::Contract(LARGE),
        key_window(),
    );
    let resolver = key_filter(
        "text:avatar",
        HistoryOrder::Desc,
        KeyScope::Resolver(KEY_CHAIN, LARGE),
        key_window(),
    );
    for (name, filter) in [
        ("unscoped", &unscoped),
        ("contract", &contract),
        ("resolver", &resolver),
    ] {
        assert!(
            page_sql(filter, true, false).contains(RECORD_ARM_SQL),
            "{name}: an unanchored key page must read the record arms"
        );
        assert!(
            !page_sql(filter, false, false).contains(RECORD_ARM_SQL),
            "{name}: non-canonical"
        );
        assert!(
            !page_sql(filter, true, true).contains(RECORD_ARM_SQL),
            "{name}: candidates"
        );
        let mut diagnostics = filter.clone();
        diagnostics.diagnostics = true;
        assert!(
            !page_sql(&diagnostics, true, false).contains(RECORD_ARM_SQL),
            "{name}: diagnostics"
        );
        let mut keyless = filter.clone();
        keyless.record_key = None;
        assert!(
            !page_sql(&keyless, true, false).contains(RECORD_ARM_SQL),
            "{name}: no key"
        );
        let mut named = filter.clone();
        named.selectors = vec![HistorySelector::LogicalNames(vec!["ens:key".to_owned()])];
        assert!(
            !page_sql(&named, true, false).contains(RECORD_ARM_SQL),
            "{name}: name anchor"
        );
        let mut registration = filter.clone();
        registration.registration_id = Some(uuid::Uuid::from_u128(1));
        assert!(
            !page_sql(&registration, true, false).contains(RECORD_ARM_SQL),
            "{name}: registration anchor"
        );
        let mut count = QueryBuilder::<Postgres>::new("");
        push_history_count_query(&mut count, filter, true, Some(101));
        assert!(!count.sql().contains(RECORD_ARM_SQL), "{name}: count");
    }
    let mut no_window = unscoped.clone();
    no_window.block_window = None;
    assert!(
        !page_sql(&no_window, true, false).contains(RECORD_ARM_SQL),
        "a read with no window and no resolver names no chain for the arms"
    );
}

/// Keys the indexed prefix cannot tell apart on its own, all on the key chain: a key of about
/// 9 kB of hex, which no index entry could hold whole; two keys that share their first 256
/// characters, one of them also written by a small emitter; a 256-character key of four-byte
/// characters; and a 256-character key with a 257-character key that extends it. One reset.
const LONG_KEY_FIXTURE: &str = r#"
    INSERT INTO chain_lineage
        (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
    SELECT 'ethereum-sepolia', 'ethereum-sepolia-block-' || n, n, to_timestamp(n), 'canonical'
    FROM generate_series(1, 100) n;
    INSERT INTO normalized_events
        (event_identity, namespace, event_kind, source_family, manifest_version,
         chain_id, block_hash, block_number, transaction_hash, transaction_index,
         log_index, derivation_kind, canonicality_state, after_state, raw_fact_ref)
    SELECT identity_, 'ens', kind_, 'ens_v1_resolver_l1', 1,
           'ethereum-sepolia', 'ethereum-sepolia-block-' || n, n, 'tx-' || n, 0, 1,
           'ens_v1_unwrapped_authority', 'canonical',
           CASE WHEN key_ IS NULL THEN jsonb_build_object('node', 'node-key')
                ELSE jsonb_build_object('node', 'node-key', 'record_key', key_) END,
           jsonb_build_object('emitting_address', emitter_)
    FROM (VALUES
            ('long:k1', 10, 'RecordChanged',
             'text:' || (SELECT string_agg(md5(i::text), '' ORDER BY i)
                         FROM generate_series(1, 290) i),
             '0x00000000000000000000000000000000000000a7'),
            ('long:k2a', 20, 'RecordChanged', 'text:' || repeat('x', 300) || 'a',
             '0x00000000000000000000000000000000000000a7'),
            ('long:k2b', 21, 'RecordChanged', 'text:' || repeat('x', 300) || 'b',
             '0x00000000000000000000000000000000000000a7'),
            ('long:k2b-small', 22, 'RecordChanged', 'text:' || repeat('x', 300) || 'b',
             '0x00000000000000000000000000000000000000c7'),
            ('long:k3', 30, 'RecordChanged', 'text:' || repeat(chr(128512), 251),
             '0x00000000000000000000000000000000000000a7'),
            ('long:k4-256', 40, 'RecordChanged', 'text:' || repeat('y', 251),
             '0x00000000000000000000000000000000000000a7'),
            ('long:k4-257', 41, 'RecordChanged', 'text:' || repeat('y', 251) || 'z',
             '0x00000000000000000000000000000000000000a7'),
            ('long:reset', 50, 'RecordVersionChanged', NULL,
             '0x00000000000000000000000000000000000000a7'))
         keys(identity_, n, kind_, key_, emitter_);
    ANALYZE normalized_events;
    ANALYZE chain_lineage;
"#;

/// For each case, the stored key of `key_of`, read on each scope: the page, every page of one,
/// and the plain statement return exactly `rows`, newest first, and the write arm reads its
/// index by the key prefix. The reset arm is not pinned on this small fixture.
async fn assert_long_keys(name: &str, cases: &[(&str, KeyScope, &[&str])]) -> Result<()> {
    let database = phase_database(name, LONG_KEY_FIXTURE).await?;
    let result = async {
        let pool = database.pool();
        for (key_of, scope, rows) in cases {
            let key: String = sqlx::query_scalar(
                "SELECT after_state ->> 'record_key' FROM normalized_events
                 WHERE event_identity = $1",
            )
            .bind(*key_of)
            .fetch_one(pool)
            .await?;
            let filter = key_filter(
                &key,
                HistoryOrder::Desc,
                *scope,
                Some(window(&[(KEY_CHAIN, None, Some(100))])),
            );
            let expected: Vec<String> = rows.iter().map(|row| (*row).to_owned()).collect();
            let label = format!("{key_of} {scope:?}");
            ensure!(
                key_plain_identities(pool, &filter).await? == expected,
                "{label}: the plain statement returned other rows"
            );
            let (page, _) = page_identities(pool, &filter, None, 100).await?;
            ensure!(page == expected, "{label}: the page returned {page:?}");
            ensure!(
                key_walk(pool, &filter, 1).await? == expected,
                "{label}: pages of one differ"
            );
            let write_index = match scope {
                KeyScope::Unscoped => KEY_INDEX,
                _ => EMITTER_KEY_INDEX,
            };
            let mut query = QueryBuilder::<Postgres>::new("");
            push_history_page_query(&mut query, &filter, true, None, false, KEY_LIMIT);
            let mut connection = pool.acquire().await?;
            let plan = explain_page(&mut connection, query, PlanMode::Unprepared).await?;
            drop(connection);
            let mut scans = Vec::new();
            resolver_page_scans(&plan[0]["Plan"], &mut scans);
            ensure!(
                scans.iter().any(|scan| {
                    scan["Index Name"] == write_index
                        && scan["Index Cond"]
                            .as_str()
                            .is_some_and(|condition| condition.contains("\"left\"("))
                }),
                "{label}: the write arm must read {write_index} by the key prefix: {plan}"
            );
        }
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

const LONG_SCOPES: [KeyScope; 3] = [
    KeyScope::Unscoped,
    KeyScope::Contract(LARGE),
    KeyScope::Resolver(KEY_CHAIN, LARGE),
];

/// K1. A key of about 9 kB, longer than any index entry, is written with every index in place
/// and read back.
#[tokio::test]
async fn k1_a_key_longer_than_an_index_entry_is_written_and_read() -> Result<()> {
    let cases: Vec<(&str, KeyScope, &[&str])> = LONG_SCOPES
        .iter()
        .map(|scope| ("long:k1", *scope, &["long:reset", "long:k1"] as &[&str]))
        .collect();
    assert_long_keys("history_record_key_k1", &cases).await
}

/// K2. Two keys that share their first 256 characters are told apart, scoped and unscoped.
#[tokio::test]
async fn k2_keys_sharing_the_indexed_prefix_are_told_apart() -> Result<()> {
    let mut cases: Vec<(&str, KeyScope, &[&str])> = vec![
        (
            "long:k2a",
            KeyScope::Unscoped,
            &["long:reset", "long:k2a"] as &[&str],
        ),
        (
            "long:k2b",
            KeyScope::Unscoped,
            &["long:reset", "long:k2b-small", "long:k2b"] as &[&str],
        ),
        (
            "long:k2b",
            KeyScope::Contract(SMALL_FEW),
            &["long:k2b-small"] as &[&str],
        ),
    ];
    for scope in &LONG_SCOPES[1..] {
        cases.push(("long:k2a", *scope, &["long:reset", "long:k2a"] as &[&str]));
        cases.push(("long:k2b", *scope, &["long:reset", "long:k2b"] as &[&str]));
    }
    assert_long_keys("history_record_key_k2", &cases).await
}

/// K3. A 256-character key of four-byte characters is written and read back.
#[tokio::test]
async fn k3_a_key_of_four_byte_characters_is_written_and_read() -> Result<()> {
    let cases: Vec<(&str, KeyScope, &[&str])> = LONG_SCOPES
        .iter()
        .map(|scope| ("long:k3", *scope, &["long:reset", "long:k3"] as &[&str]))
        .collect();
    assert_long_keys("history_record_key_k3", &cases).await
}

/// K4. A key of exactly 256 characters and one of 257 that extends it share the whole indexed
/// prefix, and each returns only its own rows.
#[tokio::test]
async fn k4_keys_of_256_and_257_characters_read_only_their_own_rows() -> Result<()> {
    let mut cases: Vec<(&str, KeyScope, &[&str])> = Vec::new();
    for scope in LONG_SCOPES {
        cases.push((
            "long:k4-256",
            scope,
            &["long:reset", "long:k4-256"] as &[&str],
        ));
        cases.push((
            "long:k4-257",
            scope,
            &["long:reset", "long:k4-257"] as &[&str],
        ));
    }
    assert_long_keys("history_record_key_k4", &cases).await
}

/// The count cells: the key absent (with every kind and with `kind=RecordChanged`), rare and
/// common, on a small and a large emitter, under
/// `contract_address` and `resolver`, with and without `include=total_count`. With it, an
/// emitter-scoped read runs the capped count over the plain source. Each cell's count equals the
/// old statement's, the plain statement's rows counted, and the cell records its driver and how
/// many index entries it read against the cap. Without it no count runs.
#[tokio::test]
async fn record_key_count_cells_match_the_old_statement_and_record_their_driver() -> Result<()> {
    const CAP: i64 = 100;
    let database = key_database("history_record_key_count_cells", true).await?;
    let result = async {
        let pool = database.pool();
        for (key, writes_only) in [
            (None, false),
            (None, true),
            (Some("text:links"), false),
            (Some("text:avatar"), false),
        ] {
            for (size, emitter) in [("small", SMALL_FEW), ("large", LARGE)] {
                for scope in [
                    KeyScope::Contract(emitter),
                    KeyScope::Resolver(KEY_CHAIN, emitter),
                ] {
                    let cell = format!(
                        "key {key:?}{}, {size} emitter, {scope:?}",
                        if writes_only {
                            ", kind=RecordChanged"
                        } else {
                            ""
                        }
                    );
                    let mut filter = key_filter(
                        key.unwrap_or("text:avatar"),
                        HistoryOrder::Desc,
                        scope,
                        key_window(),
                    );
                    if key.is_none() {
                        filter.record_key = None;
                        filter.event_kinds = if writes_only {
                            vec!["RecordChanged".to_owned()]
                        } else {
                            Vec::new()
                        };
                    }
                    let rows = key_plain_identities(pool, &filter).await?;
                    let old = i64::try_from(rows.len())?;
                    ensure!(old > 0, "{cell}: the fixture gives no row");
                    for (limit, want) in [(None, old), (Some(CAP + 1), old.min(CAP + 1))] {
                        let mut count = QueryBuilder::<Postgres>::new("");
                        push_history_count_query(&mut count, &filter, true, limit);
                        let counted: i64 = count.build_query_scalar().fetch_one(pool).await?;
                        ensure!(
                            counted == want,
                            "{cell} limit {limit:?}: counted {counted}, the old statement {want}"
                        );
                    }
                    // Without `include=total_count` the page runs no count.
                    let page = load_history_page(
                        pool,
                        filter.clone(),
                        true,
                        None,
                        20,
                        HistorySummaryMode::None,
                        false,
                        None,
                    )
                    .await?;
                    ensure!(
                        page.summary.is_none(),
                        "{cell}: a count ran without total_count"
                    );
                    let paged: Vec<String> = page
                        .rows
                        .into_iter()
                        .map(|row| row.event_identity)
                        .collect();
                    ensure!(
                        paged == rows[..rows.len().min(20)],
                        "{cell}: the page differs from the old statement"
                    );
                    let mut capped = QueryBuilder::<Postgres>::new("");
                    push_history_count_query(&mut capped, &filter, true, Some(CAP + 1));
                    let mut connection = pool.acquire().await?;
                    let plan = explain_page(&mut connection, capped, PlanMode::Unprepared).await?;
                    drop(connection);
                    let mut nodes = Vec::new();
                    page_nodes(&plan[0]["Plan"], &mut nodes);
                    let mut driver = Vec::new();
                    let mut entries = 0.0;
                    for node in &nodes {
                        let node_type = node["Node Type"].as_str().unwrap_or_default();
                        ensure!(
                            !(node_type == "Seq Scan"
                                && node["Relation Name"] == "normalized_events"),
                            "{cell}: the count scans normalized_events sequentially"
                        );
                        let Some(index) = node["Index Name"].as_str() else {
                            continue;
                        };
                        if !index.starts_with("normalized_events_") {
                            continue;
                        }
                        let read = node["Actual Rows"].as_f64().unwrap_or(0.0) * loops(node);
                        entries += read;
                        driver.push(format!("{node_type} {index} ({read})"));
                    }
                    let stops = entries <= (CAP + 1) as f64 + 16.0;
                    // Written past the test harness's capture, so the run's log keeps each
                    // cell's driver.
                    writeln!(
                        std::io::stderr(),
                        "count cell {cell}: old statement {old} rows; driver {}; index entries \
                         read {entries}; stops at the cap of {CAP}: {stops}",
                        driver.join(", ")
                    )?;
                }
            }
        }
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}
