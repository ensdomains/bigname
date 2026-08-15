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
        AddressAdmissionInput, BatchInput, ManifestInput, PriorEventInput, RawBlockInput,
        StateCacheCapacity, begin_schema_v2_adapter_restore, interpret_schema_v2_batch_incremental,
    };
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};
    use serde_json::{Value, json};
    use sqlx::types::Uuid;
    use time::{Duration, OffsetDateTime};

    use super::*;

    type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
    const ROOT: &str = "0x0000000000000000000000000000000000000042";
    const CHILD: &str = "0x0000000000000000000000000000000000000043";

    fn restore() -> bigname_adapters::schema_v2::AdapterSessionRestore {
        let mut restore = begin_schema_v2_adapter_restore(
            "resume-seed".to_owned(),
            vec![manifest()],
            Vec::new(),
            admissions(),
            StateCacheCapacity::Unlimited,
        )
        .expect("restore catalog");
        restore
            .apply_prior_events(vec![
                prior_event(
                    "parent-registration",
                    "RegistrationGranted",
                    ROOT,
                    Some("0xparent"),
                    None,
                    json!({
                        "source_event":"LabelRegistered",
                        "token_id":"0xparent",
                        "raw_label_hex":"737562",
                        "expiry":2,
                        "registry_contract_instance_id":Uuid::from_u128(1).to_string(),
                    }),
                ),
                prior_event(
                    "parent-subregistry",
                    "SubregistryChanged",
                    ROOT,
                    Some("0xparent"),
                    None,
                    json!({"token_id":"0xparent", "subregistry":CHILD}),
                ),
                prior_event(
                    "child-parent",
                    "ParentChanged",
                    CHILD,
                    None,
                    None,
                    json!({"parent":ROOT, "raw_label_hex":"737562"}),
                ),
                prior_event(
                    "child-registration",
                    "RegistrationGranted",
                    CHILD,
                    Some("0xchild"),
                    None,
                    json!({
                        "source_event":"LabelRegistered",
                        "token_id":"0xchild",
                        "raw_label_hex":"6c656166",
                        "expiry":100,
                        "registry_contract_instance_id":Uuid::from_u128(2).to_string(),
                    }),
                ),
                prior_event(
                    "child-resource",
                    "TokenResourceLinked",
                    CHILD,
                    Some("0xchild"),
                    Some(Uuid::from_u128(99)),
                    json!({
                        "token_id":"0xchild",
                        "upstream_resource":"0x99",
                    }),
                ),
            ])
            .expect("retained nested ENSv2 state");
        restore
    }

    fn manifest() -> ManifestInput {
        ManifestInput {
            manifest_id: 1,
            manifest_version: 1,
            namespace: "ens".to_owned(),
            source_family: "ens_v2_registry_l1".to_owned(),
            chain_id: "resume-seed".to_owned(),
            deployment_label: "fixture".to_owned(),
            normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
            payload_json: json!({"abi":{"events":[]}}).to_string(),
        }
    }

    fn admissions() -> Vec<AddressAdmissionInput> {
        vec![
            AddressAdmissionInput {
                address: ROOT.to_owned(),
                contract_instance_id: Uuid::from_u128(1),
                source_manifest_id: Some(1),
                role: Some("registry".to_owned()),
                discovery_edge_kind: None,
                discovery_from_contract_instance_id: None,
                discovery_observation_key: None,
                active_from_block: Some(0),
                active_to_block: None,
            },
            AddressAdmissionInput {
                address: CHILD.to_owned(),
                contract_instance_id: Uuid::from_u128(2),
                source_manifest_id: Some(1),
                role: Some("registry".to_owned()),
                discovery_edge_kind: Some("subregistry".to_owned()),
                discovery_from_contract_instance_id: Some(Uuid::from_u128(1)),
                discovery_observation_key: Some("fixture-subregistry".to_owned()),
                active_from_block: Some(0),
                active_to_block: None,
            },
        ]
    }

    fn prior_event(
        retained_state_key: &str,
        event_kind: &str,
        emitter: &str,
        token_id: Option<&str>,
        resource_id: Option<Uuid>,
        mut after_state: Value,
    ) -> PriorEventInput {
        if let Some(token_id) = token_id {
            after_state["token_id"] = Value::String(token_id.to_owned());
        }
        PriorEventInput {
            retained_state_key: retained_state_key.to_owned(),
            chain_id: "resume-seed".to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: None,
            resource_id,
            event_kind: event_kind.to_owned(),
            source_family: "ens_v2_registry_l1".to_owned(),
            manifest_version: 1,
            source_manifest_id: Some(1),
            state_scope: Some(format!(
                "{emitter}:-:{}:-:{event_kind}",
                token_id.unwrap_or("-")
            )),
            block_timestamp: Some(OffsetDateTime::UNIX_EPOCH + Duration::SECOND),
            after_state,
        }
    }

    async fn database(prefix: &str) -> TestResult<TestDatabase> {
        let database = TestDatabase::create(TestDatabaseConfig::new(prefix)).await?;
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
        Ok(database)
    }

    #[tokio::test]
    async fn canonical_predecessor_is_forwarded_to_adapter_restore() -> TestResult {
        let database = database("interpret_resume_seed").await?;
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

    #[tokio::test]
    async fn quiet_predecessor_expiry_retracts_the_name_during_cold_restore() -> TestResult {
        let database = database("interpret_quiet_expiry_seed").await?;
        let mut connection = database.pool().acquire().await?;
        // Retained topology ends at timestamp 1, the parent expires at 2, and the quiet readable
        // predecessor is timestamp 3. The seed must consume that historical name retraction.
        let seeded = finish_restore(&mut connection, "resume-seed", 3, restore()).await?;
        let unseeded = restore().finish(None);
        drop(connection);

        let batch = || BatchInput {
            chain_id: "resume-seed".to_owned(),
            manifests: vec![manifest()],
            discovery_rules: Vec::new(),
            admissions: admissions(),
            prior_events: Vec::new(),
            blocks: vec![RawBlockInput {
                chain_id: "resume-seed".to_owned(),
                block_hash: "block-3".to_owned(),
                block_number: 3,
                block_timestamp: OffsetDateTime::UNIX_EPOCH + Duration::seconds(3),
                canonicality_state: "canonical".to_owned(),
            }],
            raw_logs: Vec::new(),
        };
        let (seeded_output, _) = interpret_schema_v2_batch_incremental(batch(), Some(seeded))?;
        let (unseeded_output, _) = interpret_schema_v2_batch_incremental(batch(), Some(unseeded))?;
        let is_expiry = |event: &bigname_adapters::schema_v2::NormalizedEvent| {
            event
                .after_state
                .get("source_event")
                .and_then(Value::as_str)
                == Some("RegistryPathExpired")
        };
        assert!(
            seeded_output
                .normalized_events
                .iter()
                .all(|event| !is_expiry(event))
        );
        assert!(unseeded_output.normalized_events.iter().any(is_expiry));

        database.cleanup().await?;
        Ok(())
    }
}
