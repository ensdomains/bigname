//! The registry overview's `counts.events` must equal the events feed's `total_count` for the
//! same contract, chain and published block.
use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::json;

use super::{
    ChainBlockRange, EventHistoryFilter, HistoryBlockWindow, HistorySummaryMode,
    load_event_history_page,
};

const CHAIN: &str = "ethereum-sepolia";
const EMITTER: &str = "0x00000000000000000000000000000000000000aa";
const AS_OF: i64 = 8;
const BASE: &str = "base-mainnet";
const BASENAMES: &str = "0x00000000000000000000000000000000000000cc";

#[tokio::test]
async fn contract_event_count_matches_feed_total_count() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("contract_event_count_parity").pool_max_connections(1),
    )
    .await?;
    let result = async {
        let pool = database.pool();
        let mut connection = pool.acquire().await?;
        sqlx::raw_sql("CREATE SCHEMA bigname_phase; SET search_path TO bigname_phase, public")
            .execute(&mut *connection)
            .await?;
        for baseline in [
            include_str!("../../schema/baseline/01_chain.sql"),
            include_str!("../../schema/baseline/02_raw_facts.sql"),
            include_str!("../../schema/baseline/03_identity.sql"),
            include_str!("../../schema/baseline/04_manifests.sql"),
            include_str!("../../schema/baseline/05_normalized_events.sql"),
        ] {
            sqlx::raw_sql(baseline).execute(&mut *connection).await?;
        }
        sqlx::raw_sql(
            "INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
             SELECT chain, chain || '-block-' || n, n, to_timestamp(n), 'canonical'
             FROM unnest(ARRAY['ethereum-sepolia', 'ethereum-mainnet', 'base-mainnet']) chain, generate_series(1, 9) n",
        )
        .execute(&mut *connection)
        .await?;
        let snapshot = json!({"state_derived": true, "registrar_surface_snapshot": true});
        let rows = [
            // Ordinary logs: counted by both.
            ("label-1", "RegistrationGranted", CHAIN, EMITTER, 1, json!({})),
            ("pointer-2", "SubregistryChanged", CHAIN, EMITTER, 2, json!({})),
            // Registrar-surface snapshots replay retained state, not another on-chain action.
            ("snapshot-grant-3", "RegistrationGranted", CHAIN, EMITTER, 3, snapshot.clone()),
            ("snapshot-expiry-3", "ExpiryChanged", CHAIN, EMITTER, 3, snapshot),
            // A registry read copy of an original resolver change.
            ("origin-4:ResolverChanged:registry-read:resource-1", "ResolverChanged", CHAIN, EMITTER, 4, json!({"node": "node-4"})),
            // Two copies of one handoff: the feed keeps one.
            ("origin-5:ResolverChanged:registry-fallback-handoff:resource-1", "ResolverChanged", CHAIN, EMITTER, 5, json!({"node": "node-5"})),
            ("origin-5:ResolverChanged:registry-fallback-handoff:resource-2", "ResolverChanged", CHAIN, EMITTER, 5, json!({"node": "node-5"})),
            // Outside the selection: above the published block, another emitter, another chain.
            ("label-9", "RegistrationGranted", CHAIN, EMITTER, 9, json!({})),
            ("other-emitter-1", "RegistrationGranted", CHAIN, "0x00000000000000000000000000000000000000bb", 1, json!({})),
            ("other-chain-1", "RegistrationGranted", "ethereum-mainnet", EMITTER, 1, json!({})),
            // A registry outside the ens namespace.
            ("basenames-1", "SubregistryChanged", BASE, BASENAMES, 1, json!({})),
            ("basenames-2", "ResolverChanged", BASE, BASENAMES, 2, json!({"node": "node-b2"})),
        ];
        for (identity, kind, chain, emitter, block, after_state) in rows {
            let namespace = if chain == BASE { "basenames" } else { "ens" };
            sqlx::query(
                "INSERT INTO normalized_events
                    (event_identity, namespace, event_kind, source_family, manifest_version,
                     chain_id, block_hash, block_number, transaction_hash, transaction_index,
                     log_index, derivation_kind, canonicality_state, raw_fact_ref, after_state)
                 VALUES ($1, $7, $2, 'ens_v2_registry_l1', 1, $3, $3 || '-block-' || $5, $5,
                         'tx-' || $1, 0, 0, 'ens_v2_registry_resource_surface', 'canonical',
                         jsonb_build_object('kind', 'raw_log', 'emitting_address', $4::text), $6)",
            )
            .bind(identity)
            .bind(kind)
            .bind(chain)
            .bind(emitter)
            .bind(block)
            .bind(after_state)
            .bind(namespace)
            .execute(&mut *connection)
            .await?;
        }
        drop(connection);

        let kinds: Vec<String> = [
            "ExpiryChanged",
            "RegistrationGranted",
            "ResolverChanged",
            "SubregistryChanged",
        ]
        .map(str::to_owned)
        .to_vec();
        let feed = load_event_history_page(
            pool,
            EventHistoryFilter {
                namespace: Some("ens".to_owned()),
                contract_address: Some(EMITTER.to_owned()),
                event_kinds: kinds.clone(),
                block_window: Some(HistoryBlockWindow {
                    ranges: vec![ChainBlockRange {
                        chain_id: CHAIN.to_owned(),
                        from_block: None,
                        to_block: Some(AS_OF),
                    }],
                }),
                publication_block_bounds: Some([(CHAIN.to_owned(), AS_OF)].into()),
                ..EventHistoryFilter::default()
            },
            true,
            None,
            1,
            HistorySummaryMode::Count,
            false,
        )
        .await?
        .summary
        .expect("count summary")
        .total_count;
        assert_eq!(feed, 3);
        let overview = super::count_contract_events(pool, CHAIN, EMITTER, &kinds, Some(AS_OF)).await?;
        assert_eq!(overview, feed, "overview counts.events differs from the feed total_count");

        let basenames_feed = load_event_history_page(
            pool,
            EventHistoryFilter {
                namespace: Some("basenames".to_owned()),
                contract_address: Some(BASENAMES.to_owned()),
                event_kinds: kinds.clone(),
                block_window: Some(HistoryBlockWindow {
                    ranges: vec![ChainBlockRange {
                        chain_id: BASE.to_owned(),
                        from_block: None,
                        to_block: Some(AS_OF),
                    }],
                }),
                ..EventHistoryFilter::default()
            },
            true,
            None,
            1,
            HistorySummaryMode::Count,
            false,
        )
        .await?
        .summary
        .expect("count summary")
        .total_count;
        assert_eq!(basenames_feed, 2);
        let basenames =
            super::count_contract_events(pool, BASE, BASENAMES, &kinds, Some(AS_OF)).await?;
        assert_eq!(basenames, basenames_feed, "a registry outside the ens namespace counted 0");
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}
