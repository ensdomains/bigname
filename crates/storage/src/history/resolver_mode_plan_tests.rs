//! A resolver request outside the arm shape keeps the plain `normalized_events` source and the
//! resolver condition for its page and its count, in custom and generic prepared plans alike.
use super::*;

const NAMED_RESOLVER: &str = "0x00000000000000000000000000000000000000e1";
const QUIET_RESOLVER: &str = "0x00000000000000000000000000000000000000e2";
const SELECTOR_ADDRESS: &str = "0x00000000000000000000000000000000000000ee";
const MODE_NAME: &str = "ens:mode";

/// One noise row per block. The name's 60 newest rows are emitted by the named resolver,
/// which also has 12,000 pointer changes for another name. The name's 6,000 older rows are
/// pointer changes between other resolvers. The quiet resolver emitted the chain's 60 newest
/// matching rows and has no pointer changes at all. So each read below fills its page from
/// the newest rows, while the resolver's pointer rows lie far below it or do not exist.
const MODE_FIXTURE: &str = r#"
    INSERT INTO chain_lineage
        (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
    SELECT 'ethereum-sepolia', 'ethereum-sepolia-block-' || n, n, to_timestamp(n), 'canonical'
    FROM generate_series(1, 20000) n;
    INSERT INTO name_surfaces
        (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
         namehash, labelhashes, normalizer_version, visibility_state, chain_id,
         block_hash, block_number, canonicality_state)
    SELECT name, 'ens', name, ARRAY[name], '\x00'::bytea, split_part(name, ':', 2),
           ARRAY[name], 'test', 'active', 'ethereum-sepolia',
           'ethereum-sepolia-block-1', 1, 'canonical'::canonicality_state
    FROM (VALUES ('ens:mode'), ('ens:other')) names(name);
    INSERT INTO normalized_events
        (event_identity, namespace, logical_name_id, event_kind, source_family,
         manifest_version, chain_id, block_hash, block_number, transaction_hash,
         transaction_index, log_index, derivation_kind, canonicality_state,
         after_state, before_state, raw_fact_ref)
    SELECT 'mode-noise:' || n, 'ens', NULL, 'RecordChanged', 'ens_v1_resolver_l1', 1,
           'ethereum-sepolia', 'ethereum-sepolia-block-' || n, n, 'tx-noise-' || n, 0, 0,
           'ens_v1_unwrapped_authority', 'canonical',
           jsonb_build_object('node', 'node-noise'), '{}'::jsonb,
           jsonb_build_object('emitting_address', '0x00000000000000000000000000000000000000f1')
    FROM generate_series(1, 20000) n;
    INSERT INTO normalized_events
        (event_identity, namespace, logical_name_id, event_kind, source_family,
         manifest_version, chain_id, block_hash, block_number, transaction_hash,
         transaction_index, log_index, derivation_kind, canonicality_state,
         after_state, before_state, raw_fact_ref)
    SELECT 'mode:named-emit:' || n, 'ens', 'ens:mode', 'RecordChanged', 'ens_v1_resolver_l1', 1,
           'ethereum-sepolia', 'ethereum-sepolia-block-' || n, n, 'tx-named-' || n, 0, 1,
           'ens_v1_unwrapped_authority', 'canonical',
           jsonb_build_object('node', 'node-mode'), '{}'::jsonb,
           jsonb_build_object('emitting_address', '0x00000000000000000000000000000000000000e1')
    FROM generate_series(19861, 19920) n;
    INSERT INTO normalized_events
        (event_identity, namespace, logical_name_id, event_kind, source_family,
         manifest_version, chain_id, block_hash, block_number, transaction_hash,
         transaction_index, log_index, derivation_kind, canonicality_state,
         after_state, before_state, raw_fact_ref)
    SELECT 'mode:quiet-emit:' || n, 'ens', NULL, 'RecordChanged', 'ens_v1_resolver_l1', 1,
           'ethereum-sepolia', 'ethereum-sepolia-block-' || n, n, 'tx-quiet-' || n, 0, 2,
           'ens_v1_unwrapped_authority', 'canonical',
           jsonb_build_object('node', 'node-quiet'), '{}'::jsonb,
           jsonb_build_object('emitting_address', '0x00000000000000000000000000000000000000e2')
    FROM generate_series(19941, 20000) n;
    INSERT INTO normalized_events
        (event_identity, namespace, logical_name_id, event_kind, source_family,
         manifest_version, chain_id, block_hash, block_number, transaction_hash,
         transaction_index, log_index, derivation_kind, canonicality_state,
         after_state, before_state, raw_fact_ref)
    SELECT 'mode:pointer:' || kind || ':' || n, 'ens',
           CASE kind WHEN 'name' THEN 'ens:mode' ELSE 'ens:other' END,
           'ResolverChanged', 'ens_v1_registry_l1', 1, 'ethereum-sepolia',
           'ethereum-sepolia-block-' || (3 * n + offset_), 3 * n + offset_,
           'tx-pointer-' || kind || '-' || n, 0, 1,
           'ens_v1_unwrapped_authority', 'canonical',
           jsonb_build_object('node', 'node-' || kind, 'resolver', after_),
           jsonb_build_object('resolver', before_), '{}'::jsonb
    FROM (VALUES
            ('set', 0, '0x00000000000000000000000000000000000000e1',
             '0x0000000000000000000000000000000000000099'),
            ('clear', 1, '0x0000000000000000000000000000000000000099',
             '0x00000000000000000000000000000000000000e1'),
            ('name', 2, '0x0000000000000000000000000000000000000099',
             '0x0000000000000000000000000000000000000098'))
         pointers(kind, offset_, after_, before_)
    CROSS JOIN generate_series(1, 6000) n;
    ANALYZE normalized_events;
    ANALYZE chain_lineage;
    ANALYZE name_surfaces;
"#;

/// The resolver condition `push_history_filters` adds when the source is not the arms. A read
/// outside the arm shape must carry it.
const RESOLVER_CONDITION: &str = "OR (ne.event_kind = 'ResolverChanged' \
     AND (lower(ne.after_state ->> 'resolver') = ";

/// Only the pointer-set arm tests a `candidate` row's new resolver for equality.
const ARM_SQL: &str = "lower(candidate.after_state ->> 'resolver') = ";

struct ModeCase {
    name: &'static str,
    filter: EventHistoryReadFilter,
    canonical_only: bool,
    include_candidates: bool,
    /// Whether the plain source reads `normalized_events` once as `ne`. A registration read
    /// keeps its own candidate source, aliased `candidate`.
    plain_scan: bool,
}

fn mode_filter(address: &str) -> EventHistoryReadFilter {
    let mut filter = resolver_filter(HistoryOrder::Desc, address);
    filter.block_window = Some(window(&[(RESOLVER_CHAIN, None, Some(20_000))]));
    filter
}

fn mode_cases() -> Vec<ModeCase> {
    let mut name = mode_filter(NAMED_RESOLVER);
    name.selectors = vec![HistorySelector::LogicalNames(vec![MODE_NAME.to_owned()])];
    // The selector an address read in `both` scope builds for a product page.
    let mut address = mode_filter(NAMED_RESOLVER);
    address.selectors = vec![HistorySelector::OrAccountParticipants {
        anchors: Box::new(HistorySelector::OrRootPermissionSubject {
            anchors: Box::new(HistorySelector::LogicalNames(vec![MODE_NAME.to_owned()])),
            subject: SELECTOR_ADDRESS.to_owned(),
            namespace: Some("ens".to_owned()),
        }),
        address: SELECTOR_ADDRESS.to_owned(),
        namespace: Some("ens".to_owned()),
        relations: None,
    }];
    let mut candidates = mode_filter(QUIET_RESOLVER);
    candidates.diagnostics = true;
    let registration_id = uuid::Uuid::from_u128(1);
    let mut registration = mode_filter(NAMED_RESOLVER);
    registration.registration_id = Some(registration_id);
    registration.selectors = vec![HistorySelector::ProductRegistration {
        logical_name_ids: vec![MODE_NAME.to_owned()],
        resource_ids: vec![registration_id],
    }];
    vec![
        ModeCase {
            name: "resolver plus name",
            filter: name,
            canonical_only: true,
            include_candidates: false,
            plain_scan: true,
        },
        ModeCase {
            name: "resolver plus address",
            filter: address,
            canonical_only: true,
            include_candidates: false,
            plain_scan: true,
        },
        ModeCase {
            name: "canonical_only=false",
            filter: mode_filter(QUIET_RESOLVER),
            canonical_only: false,
            include_candidates: false,
            plain_scan: true,
        },
        ModeCase {
            name: "include_candidates=true",
            filter: candidates,
            canonical_only: true,
            include_candidates: true,
            plain_scan: true,
        },
        ModeCase {
            name: "resolver plus registration",
            filter: registration,
            canonical_only: true,
            include_candidates: false,
            plain_scan: false,
        },
    ]
}

/// Rows visited by every scan of `normalized_events` in the plan, subplans included.
fn event_rows_visited(node: &Value) -> f64 {
    let own = if node["Relation Name"] == "normalized_events" {
        (node["Actual Rows"].as_f64().unwrap_or(0.0)
            + node["Rows Removed by Filter"].as_f64().unwrap_or(0.0)
            + node["Rows Removed by Index Recheck"]
                .as_f64()
                .unwrap_or(0.0))
            * node["Actual Loops"].as_f64().unwrap_or(1.0)
    } else {
        0.0
    };
    own + node["Plans"]
        .as_array()
        .map(|children| children.iter().map(event_rows_visited).sum())
        .unwrap_or(0.0)
}

#[tokio::test]
async fn resolver_reads_outside_the_arm_shape_keep_the_plain_source() -> Result<()> {
    let database = phase_database("history_resolver_mode_plan", MODE_FIXTURE).await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        let mut failures = Vec::new();
        for case in mode_cases() {
            for mode in [PlanMode::Custom, PlanMode::Generic] {
                for (statement, page_limit) in [("page", 2i64), ("page", 51), ("capped count", 101)]
                {
                    let label = format!("{} {mode:?} {statement} limit {page_limit}", case.name);
                    let mut query = QueryBuilder::<Postgres>::new("");
                    if statement == "page" {
                        push_history_page_query(
                            &mut query,
                            &case.filter,
                            case.canonical_only,
                            None,
                            case.include_candidates,
                            page_limit,
                        );
                    } else {
                        // The summary count of the same request, as `load_history_page` builds it.
                        push_history_count_query(
                            &mut query,
                            &case.filter,
                            case.canonical_only,
                            Some(page_limit),
                        );
                    }
                    let sql = query.sql().to_owned();
                    if sql.contains(ARM_SQL) {
                        failures.push(format!("{label}: the statement reads the resolver arms"));
                    }
                    if !sql.contains(RESOLVER_CONDITION) {
                        failures.push(format!(
                            "{label}: the statement lost the resolver condition"
                        ));
                    }
                    let plan = explain_page(&mut connection, query, mode).await?;
                    let mut scans = Vec::new();
                    resolver_page_scans(&plan[0]["Plan"], &mut scans);
                    let aliases: Vec<&Value> = scans.iter().map(|scan| &scan["Alias"]).collect();
                    let visited = event_rows_visited(&plan[0]["Plan"]);
                    eprintln!(
                        "{label}: page scans {aliases:?} {:?}, visited {visited}",
                        scans
                            .iter()
                            .map(|scan| scan["Index Name"].as_str().unwrap_or("-"))
                            .collect::<Vec<_>>()
                    );
                    if case.plain_scan && (scans.len() != 1 || scans[0]["Alias"] != "ne") {
                        failures.push(format!(
                            "{label}: the statement must scan normalized_events once as ne, \
                             scanned {aliases:?}"
                        ));
                    }
                }
            }
        }
        ensure!(failures.is_empty(), "{}", failures.join("\n"));
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

/// The control: a canonical, activated-only request with no selector still takes the arms,
/// for its page and its count.
#[tokio::test]
async fn resolver_reads_in_the_arm_shape_keep_the_arms() -> Result<()> {
    let database = phase_database("history_resolver_mode_arms", MODE_FIXTURE).await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        for mode in [PlanMode::Custom, PlanMode::Generic] {
            for statement in ["page", "capped count"] {
                let filter = mode_filter(QUIET_RESOLVER);
                let mut query = QueryBuilder::<Postgres>::new("");
                if statement == "page" {
                    push_history_page_query(&mut query, &filter, true, None, false, 2);
                } else {
                    push_history_count_query(&mut query, &filter, true, Some(101));
                }
                let sql = query.sql().to_owned();
                ensure!(
                    sql.contains(ARM_SQL) && !sql.contains(RESOLVER_CONDITION),
                    "{mode:?} {statement}: the arm shape must read the arms without the \
                     resolver condition"
                );
                let plan = explain_page(&mut connection, query, mode).await?;
                let mut scans = Vec::new();
                resolver_page_scans(&plan[0]["Plan"], &mut scans);
                ensure!(
                    // PostgreSQL numbers the repeated arm alias: candidate, candidate_1, ...
                    !scans.is_empty()
                        && scans.iter().all(|scan| {
                            scan["Alias"]
                                .as_str()
                                .is_some_and(|alias| alias.starts_with("candidate"))
                        }),
                    "{mode:?} {statement}: the arm shape must scan only the arms: {plan}"
                );
            }
        }
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}
