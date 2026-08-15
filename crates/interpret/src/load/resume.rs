use sqlx::PgConnection;

use crate::{InterpretError, Result};

pub(super) async fn finish_restore(
    connection: &mut PgConnection,
    chain_id: &str,
    from_block: i64,
    restore: bigname_adapters::schema_v2::AdapterSessionRestore,
) -> Result<bigname_adapters::SchemaV2AdapterSession> {
    let timestamp = predecessor_timestamp(connection, chain_id, from_block).await?;
    Ok(restore.finish(timestamp))
}

pub(super) async fn predecessor_timestamp(
    connection: &mut PgConnection,
    chain_id: &str,
    from_block: i64,
) -> Result<Option<time::OffsetDateTime>> {
    let predecessor = from_block.saturating_sub(1);
    let rows: Vec<(i64, String, time::OffsetDateTime)> = sqlx::query_as(
        "SELECT block_number, block_hash, block_timestamp
         FROM chain_lineage
         WHERE chain_id = $1 AND block_number <= $2
           AND canonicality_state IN ('canonical', 'safe', 'finalized')
         ORDER BY block_number DESC, block_hash
         LIMIT 2",
    )
    .bind(chain_id)
    .bind(predecessor)
    .fetch_all(&mut *connection)
    .await
    .map_err(|error| {
        InterpretError::database("failed to load interpret resume predecessor", error)
    })?;
    super::require_unique_live_heights(
        chain_id,
        rows.iter()
            .map(|(number, hash, _)| (*number, hash.as_str())),
    )?;
    let Some((number, _, timestamp)) = rows.into_iter().next() else {
        return Ok(None);
    };
    if number != predecessor {
        return Err(InterpretError::data_integrity(format!(
            "interpret resume at block {from_block} for chain {chain_id} has no readable predecessor block {predecessor}"
        )));
    }
    Ok(Some(timestamp))
}

#[cfg(test)]
mod tests {
    use bigname_adapters::schema_v2::{
        ManifestInput, PriorEventInput, StateCacheCapacity, begin_schema_v2_adapter_restore,
    };
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};
    use serde_json::json;
    use time::{Duration, OffsetDateTime};

    use super::*;

    type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

    fn restore() -> bigname_adapters::schema_v2::AdapterSessionRestore {
        let mut restore = begin_schema_v2_adapter_restore(
            "resume-seed".to_owned(),
            vec![ManifestInput {
                manifest_id: 1,
                manifest_version: 1,
                namespace: "ens".to_owned(),
                source_family: "ens_v2_registry_l1".to_owned(),
                chain_id: "resume-seed".to_owned(),
                deployment_label: "fixture".to_owned(),
                normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
                payload_json: json!({"abi":{"events":[]}}).to_string(),
            }],
            Vec::new(),
            Vec::new(),
            StateCacheCapacity::Unlimited,
        )
        .expect("empty restore catalog");
        restore
            .apply_prior_events(vec![PriorEventInput {
                retained_state_key: "registration".to_owned(),
                chain_id: "resume-seed".to_owned(),
                namespace: "ens".to_owned(),
                logical_name_id: None,
                resource_id: None,
                event_kind: "RegistrationGranted".to_owned(),
                source_family: "ens_v2_registry_l1".to_owned(),
                manifest_version: 1,
                source_manifest_id: Some(1),
                state_scope: Some("0xregistry:-:0xtoken:-:LabelRegistered".to_owned()),
                block_timestamp: Some(OffsetDateTime::UNIX_EPOCH + Duration::SECOND),
                after_state: json!({
                    "source_event":"LabelRegistered",
                    "token_id":"0xtoken",
                    "raw_label_hex":"616c696365",
                    "expiry":2,
                }),
            }])
            .expect("retained ENSv2 token");
        restore
    }

    #[tokio::test]
    async fn canonical_predecessor_is_forwarded_to_adapter_restore() -> TestResult {
        let database =
            TestDatabase::create(TestDatabaseConfig::new("interpret_resume_seed")).await?;
        sqlx::raw_sql(include_str!("../../../../schema-v2/baseline/01_chain.sql"))
            .execute(database.pool())
            .await?;
        sqlx::raw_sql(
            "INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state) VALUES
             ('resume-seed', 'block-0', 0, to_timestamp(0), 'finalized'),
             ('resume-seed', 'block-1', 1, to_timestamp(1), 'canonical'),
             ('resume-seed', 'orphan-2', 2, to_timestamp(9), 'orphaned'),
             ('resume-seed', 'block-2', 2, to_timestamp(3), 'safe'),
             ('lineage-floor', 'block-100', 100, to_timestamp(100), 'canonical')",
        )
        .execute(database.pool())
        .await?;

        let mut connection = database.pool().acquire().await?;
        let actual = finish_restore(&mut connection, "resume-seed", 3, restore()).await?;
        let expected = restore().finish(Some(OffsetDateTime::UNIX_EPOCH + Duration::seconds(3)));
        assert_eq!(actual, expected);
        assert_ne!(actual, restore().finish(None));
        assert_eq!(
            predecessor_timestamp(&mut connection, "resume-seed", 0).await?,
            None
        );
        assert_eq!(
            predecessor_timestamp(&mut connection, "lineage-floor", 100).await?,
            None
        );
        assert!(
            predecessor_timestamp(&mut connection, "resume-seed", 4)
                .await
                .is_err()
        );
        drop(connection);
        database.cleanup().await?;
        Ok(())
    }
}
