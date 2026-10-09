//! The production cache must distinguish an instance-less child from the same declared parent.
use super::*;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::types::time::OffsetDateTime;

const CHAIN: &str = "ethereum-sepolia";
const ETH: &str = "0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4";

#[tokio::test]
async fn registry_support_cache_keeps_both_instance_inputs_in_either_order() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("registry_support_cache").pool_max_connections(1),
    )
    .await?;
    let result = async {
        database.create_phase_schema().await?;
        for baseline in [
            include_str!("../../../../schema/baseline/01_chain.sql"),
            include_str!("../../../../schema/baseline/02_raw_facts.sql"),
            include_str!("../../../../schema/baseline/03_identity.sql"),
            include_str!("../../../../schema/baseline/04_manifests.sql"),
            include_str!("../../../../schema/baseline/05_normalized_events.sql"),
            include_str!("../../../../schema/baseline/06_projections.sql"),
            include_str!("../../../../schema/baseline/07_labels.sql"),
            include_str!("../../../../schema/baseline/08_heartbeats.sql"),
            include_str!("../../../../schema/baseline/09_divergence.sql"),
            include_str!("../../../../schema/baseline/10_phase_state.sql"),
        ] {
            sqlx::raw_sql(baseline).execute(database.pool()).await?;
        }
        sqlx::query("SELECT set_config('bigname.interpreter_content_hash', $1, false)")
            .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
            .execute(database.pool())
            .await?;
        let repository = bigname_manifests::load_repository(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia"),
        )?;
        bigname_manifests::sync_schema_v2_repository(database.pool(), &repository).await?;
        // Fixture setup gets the real declared instance. Production recognition below reads
        // only the retained normalized declarations and origin facts.
        let instance: Uuid = sqlx::query_scalar(
            "SELECT contract_instance_id FROM contract_instance_addresses
             WHERE chain_id = $1 AND address = $2 AND active_to_block_number IS NULL",
        )
        .bind(CHAIN)
        .bind(ETH)
        .fetch_one(database.pool())
        .await?;
        // The set a publication of the synced manifests records: each manifest's latest update.
        let admission_manifests: Option<String> = sqlx::query_scalar(
            "SELECT string_agg(source_manifest_id || ':' || event_id, ',')
             FROM (SELECT source_manifest_id, max(normalized_event_id) AS event_id
                   FROM normalized_events
                   WHERE chain_id = $1 AND event_kind = 'SourceManifestUpdated'
                   GROUP BY source_manifest_id) latest",
        )
        .bind(CHAIN)
        .fetch_one(database.pool())
        .await?;
        let publication = FamilyPublication {
            chain_id: CHAIN.into(),
            block_number: 11_820_505,
            block_hash: "fixture".into(),
            block_timestamp: OffsetDateTime::from_unix_timestamp(1_800_000_005)?,
            block_timestamp_json: json!("2027-01-15T08:00:05Z"),
            admission: None,
            admission_manifests,
        };
        let mut conn = database.pool().acquire().await?;
        let declarations = registry_support::Declarations::load(&mut conn, &publication).await?;
        for order in [[None, Some(instance)], [Some(instance), None]] {
            // Repeat the complete keys too: repeated requests reuse exactly their own result.
            let lookups = order
                .into_iter()
                .chain(order)
                .map(|id| (ETH.to_owned(), id))
                .collect();
            let supported = load_supported(&mut conn, &publication, &declarations, lookups).await?;
            assert_eq!(supported.len(), 2);
            assert!(supported.get(&(ETH.into(), None)).unwrap().is_none());
            let parent = supported
                .get(&(ETH.into(), Some(instance)))
                .unwrap()
                .as_ref()
                .unwrap();
            assert_eq!(parent.model, Model::Declared);
            assert_eq!(parent.namespace, "ens");
            assert_eq!(
                parent.root,
                crate::identity::ens_v2_registry_root_resource_id(CHAIN, instance)
            );
        }
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}
