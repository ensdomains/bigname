//! Resolver page work stays bounded by the page where the plain statement's would: a request
//! that also filters by contract reads the plain statement, so the contract's own emitter index
//! serves it in page order, and a window below the resolver's newest rows bounds the arms' index
//! scans, so they neither read down to it nor sort the whole window.
use super::*;

const COST_RESOLVER: &str = "0x00000000000000000000000000000000000000a7";
const COST_CONTRACT: &str = "0x00000000000000000000000000000000000000c7";

/// Pointer changes that set the resolver, one per block. Another contract emitted the oldest
/// 20,000 and the newest 39,000. The requested contract emitted the 1,000 between them. So a
/// read that walks the resolver's pointer rows in block order passes a long run of the other
/// contract's rows in either order before it reaches one row of the requested contract. A read
/// of a window from block 10,000 to 10,100 either walks 49,900 newer or 9,999 older rows in
/// order, or reads the window's rows out of order and sorts all of them.
const CONTRACT_COST_FIXTURE: &str = r#"
    INSERT INTO chain_lineage
        (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
    SELECT 'ethereum-sepolia', 'ethereum-sepolia-block-' || n, n, to_timestamp(n), 'canonical'
    FROM generate_series(1, 60000) n;
    INSERT INTO normalized_events
        (event_identity, namespace, event_kind, source_family, manifest_version,
         chain_id, block_hash, block_number, transaction_hash, transaction_index,
         log_index, derivation_kind, canonicality_state, after_state, before_state, raw_fact_ref)
    SELECT 'contract-cost:' || n, 'ens', 'ResolverChanged', 'ens_v1_registry_l1', 1,
           'ethereum-sepolia', 'ethereum-sepolia-block-' || n, n, 'tx-contract-cost-' || n, 0, 0,
           'ens_v1_unwrapped_authority', 'canonical',
           jsonb_build_object('node', 'node-' || n,
                              'resolver', '0x00000000000000000000000000000000000000a7'),
           jsonb_build_object('resolver', '0x0000000000000000000000000000000000000099'),
           jsonb_build_object(
               'emitting_address',
               CASE WHEN n BETWEEN 20001 AND 21000
                    THEN '0x00000000000000000000000000000000000000c7'
                    ELSE '0x00000000000000000000000000000000000000d7' END)
    FROM generate_series(1, 60000) n;
    ANALYZE normalized_events;
    ANALYZE chain_lineage;
"#;

/// Rows read from `normalized_events` by every scan in the plan, subplans included.
fn rows_read(node: &Value) -> f64 {
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
        .map(|children| children.iter().map(rows_read).sum())
        .unwrap_or(0.0)
}

/// Rows fed into every sort in the plan, incremental sorts included.
fn sort_input(node: &Value) -> f64 {
    let children = node["Plans"].as_array();
    let own = if matches!(
        node["Node Type"].as_str(),
        Some("Sort" | "Incremental Sort")
    ) {
        children
            .map(|children| {
                children
                    .iter()
                    .map(|child| {
                        child["Actual Rows"].as_f64().unwrap_or(0.0)
                            * child["Actual Loops"].as_f64().unwrap_or(1.0)
                    })
                    .sum()
            })
            .unwrap_or(0.0)
    } else {
        0.0
    };
    own + children
        .map(|children| children.iter().map(sort_input).sum())
        .unwrap_or(0.0)
}

/// Plans each page of `filter` in both prepared modes and checks that it returns a full page
/// while reading and sorting rows in proportion to the page.
async fn page_work_failures(
    connection: &mut PgConnection,
    name: &str,
    base: &EventHistoryReadFilter,
) -> Result<Vec<String>> {
    let mut failures = Vec::new();
    for order in [HistoryOrder::Desc, HistoryOrder::Asc] {
        let mut filter = base.clone();
        filter.order = order;
        for page_size in [1i64, 50] {
            // A page reads one row past its size to know whether another page follows.
            let page_limit = page_size + 1;
            for mode in [PlanMode::Custom, PlanMode::Generic] {
                let label = format!("{name} {order:?} page of {page_size} {mode:?}");
                let mut query = QueryBuilder::<Postgres>::new("");
                push_history_page_query(&mut query, &filter, true, None, false, page_limit);
                let plan = explain_page(&mut *connection, query, mode).await?;
                let root = &plan[0]["Plan"];
                let read = rows_read(root);
                let sorted = sort_input(root);
                let returned = root["Actual Rows"].as_f64().unwrap_or(0.0);
                eprintln!("{label}: returned {returned}, read {read}, sorted {sorted}");
                if returned != page_limit as f64 {
                    failures.push(format!(
                        "{label}: returned {returned} rows, the page asks for {page_limit}"
                    ));
                }
                // Every run of rows the page must not read is far above these bounds.
                let read_bound = (10 * page_limit + 100) as f64;
                let sort_bound = (4 * page_limit + 20) as f64;
                if read > read_bound {
                    failures.push(format!("{label}: read {read} rows, more than {read_bound}"));
                }
                if sorted > sort_bound {
                    failures.push(format!(
                        "{label}: sorted {sorted} rows, more than {sort_bound}"
                    ));
                }
            }
        }
    }
    Ok(failures)
}

#[tokio::test]
async fn resolver_contract_pages_stop_after_the_page() -> Result<()> {
    let database = phase_database("history_resolver_contract_cost", CONTRACT_COST_FIXTURE).await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        let mut filter = resolver_filter(HistoryOrder::Desc, COST_RESOLVER);
        filter.event_kinds = vec!["ResolverChanged".to_owned()];
        filter.contract_address = Some(COST_CONTRACT.to_owned());
        let failures = page_work_failures(&mut connection, "contract", &filter).await?;
        ensure!(failures.is_empty(), "{}", failures.join("\n"));
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

/// A window over two chains, as an events request sends, whose range on the resolver's chain
/// lies far below its newest pointer rows and far above its oldest.
#[tokio::test]
async fn resolver_window_pages_start_at_the_window() -> Result<()> {
    let database = phase_database("history_resolver_window_cost", CONTRACT_COST_FIXTURE).await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        let mut filter = resolver_filter(HistoryOrder::Desc, COST_RESOLVER);
        filter.event_kinds = vec!["ResolverChanged".to_owned()];
        filter.block_window = Some(window(&[
            (RESOLVER_CHAIN, Some(10_000), Some(10_100)),
            ("other-chain", Some(1), Some(60_000)),
        ]));
        let failures = page_work_failures(&mut connection, "window", &filter).await?;
        ensure!(failures.is_empty(), "{}", failures.join("\n"));
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}
