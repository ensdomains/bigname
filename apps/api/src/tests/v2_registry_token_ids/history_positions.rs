use super::*;

#[tokio::test]
async fn v2_history_tokens_follow_multiple_same_block_regenerations_and_batch_operator()
-> Result<()> {
    let (manifest, _, _) = fixture(false)?;
    let mut logs = registration(0, 0, 120);
    let operator: Address = "0x00000000000000000000000000000000000000ab".parse()?;
    for version in 0..3_u32 {
        let index = i64::from(version) * 4;
        let mut sequence = vec![
            raw(
                EACRolesChanged {
                    resource: token(0),
                    account: GRANTEE.parse()?,
                    oldRoleBitmap: U256::from(version),
                    newRoleBitmap: U256::from(version + 1),
                }
                .encode_log_data(),
                121,
                index,
            ),
            transfer(version, HOLDER.parse()?, Address::ZERO, 121, index + 1),
            raw(
                TokenRegenerated {
                    oldTokenId: token(version),
                    newTokenId: token(version + 1),
                }
                .encode_log_data(),
                121,
                index + 2,
            ),
            transfer(version + 1, Address::ZERO, HOLDER.parse()?, 121, index + 3),
        ];
        if version == 2 {
            for log in &mut sequence {
                log.transaction_index = 1;
                log.transaction_hash = "0xsecondtransaction".into();
            }
        }
        logs.extend(sequence);
    }
    logs.push(raw(
        TransferBatch {
            operator,
            from: HOLDER.parse()?,
            to: GRANTEE.parse()?,
            ids: vec![token(3)],
            values: vec![U256::from(1)],
        }
        .encode_log_data(),
        122,
        0,
    ));
    logs.push(raw(
        TransferSingle {
            operator,
            from: GRANTEE.parse()?,
            to: HOLDER.parse()?,
            id: token(3),
            value: U256::from(1),
        }
        .encode_log_data(),
        123,
        0,
    ));
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_blocks(&database, 120..=123).await?;
    seed_interpret(&database, &manifest, &logs).await?;
    let engine = bigname_interpret::Engine::new(database.pool.clone());
    for block in 120..=123 {
        engine
            .run_batch(bigname_interpret::BatchRequest {
                chain_id: CHAIN.into(),
                from_block: block,
                to_block: block,
                resume_current: None,
                mode: bigname_interpret::RunMode::Normal,
            })
            .await?;
    }
    routes::publish(&database, 123).await?;
    let retained:Vec<Value>=sqlx::query_scalar("SELECT after_state FROM normalized_events WHERE event_kind='TokenControlTransferred' AND block_number IN (122,123)").fetch_all(&database.pool).await?;
    assert_eq!(retained.len(), 2);
    assert!(
        retained
            .iter()
            .all(|after| after["operator"] == format!("{operator:#x}"))
    );
    for route in [
        format!("/v1/names/{NAME}/history?scope=both"),
        format!("/v1/names/{NAME}/history?scope=both&include=data,raw,child_registrations"),
        format!("/v1/events?name={NAME}"),
        format!("/v1/events?contract_address={REGISTRY}"),
        format!("/v1/addresses/{GRANTEE}/history?namespace=ens"),
    ] {
        let (status, body) = read_family_response(
            &database,
            &format!(
                "{route}{}&order=asc&page_size=200",
                if route.contains("include=") {
                    ""
                } else {
                    "&include=data,raw"
                }
            ),
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "{body:#}");
        let rows = body["data"].as_array().unwrap();
        let roles = rows
            .iter()
            .filter(|r| r["kind"] == "PermissionChanged" && r["block_number"] == 121)
            .collect::<Vec<_>>();
        assert_eq!(roles.len(), 3, "{body:#}");
        for (version, row) in roles.into_iter().enumerate() {
            assert_eq!(
                row["data"]["token_id"],
                token(version as u32).to_string(),
                "{row:#}"
            );
            assert_eq!(row["data"]["canonical_id"], token(0).to_string(), "{row:#}");
        }
        for block in [122, 123] {
            let transfer = rows
                .iter()
                .find(|r| r["kind"] == "TokenControlTransferred" && r["block_number"] == block)
                .unwrap();
            assert_eq!(transfer["data"]["token_id"], token(3).to_string());
            assert_eq!(transfer["data"]["operator"], format!("{operator:#x}"));
            assert_eq!(transfer["data"]["canonical_id"], token(0).to_string());
        }
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_history_historical_models_keep_direct_words_and_proven_permission_tokens() -> Result<()>
{
    for preaudit in [true, false] {
        let (mut manifest, _, logs) = fixture(preaudit)?;
        if !preaudit {
            // Retained June declaration copied exactly from main before the deployment rotation;
            // activation and the relocated fixture chain apply only to this disposable database.
            let repository = bigname_manifests::load_repository(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("src/tests/fixtures/ens-v2-token-postaudit"),
            )?;
            let mut old = repository.manifests()[0].manifest.clone();
            old.chain = CHAIN.into();
            manifest.manifest_version = old.manifest_version as i64;
            manifest.deployment_label = old.deployment_epoch.clone();
            manifest.payload_json = serde_json::to_string(&old)?;
        }
        let database = TestDatabase::new_migrated().await?;
        seed_v2_history_blocks(&database, 119..=134).await?;
        seed_interpret(&database, &manifest, &logs).await?;
        let engine = bigname_interpret::Engine::new(database.pool.clone());
        for block in 119..=134 {
            engine
                .run_batch(bigname_interpret::BatchRequest {
                    chain_id: CHAIN.into(),
                    from_block: block,
                    to_block: block,
                    resume_current: None,
                    mode: bigname_interpret::RunMode::Normal,
                })
                .await?;
        }
        routes::publish(&database, 134).await?;
        let (status, body) = read_family_response(
            &database,
            &format!(
                "/v1/names/{NAME}/history?scope=both&include=data,raw&order=asc&page_size=200"
            ),
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "{body:#}");
        let mut grants = 0;
        let mut permissions = 0;
        for row in body["data"].as_array().unwrap() {
            if row["kind"] == "PermissionChanged" {
                permissions += 1;
                if preaudit {
                    assert!(row["data"].get("token_id").is_none(), "{row:#}");
                } else {
                    let version = match row["block_number"].as_i64().unwrap() {
                        120 | 121 => 0,
                        123 | 124 => 2,
                        131 => 4,
                        134 => 5,
                        _ => panic!("{row:#}"),
                    };
                    assert_eq!(
                        row["data"]["token_id"],
                        token(version).to_string(),
                        "{row:#}"
                    );
                }
                assert_eq!(row["data"]["canonical_id"], token(0).to_string(), "{row:#}");
            }
            if row["kind"] == "RegistrationGranted" {
                grants += 1;
                let version = match row["block_number"].as_i64().unwrap() {
                    120 => 0,
                    123 => 2,
                    131 => 4,
                    134 => 5,
                    _ => panic!("{row:#}"),
                };
                assert_eq!(row["data"]["token_id"], token(version).to_string());
            }
        }
        assert!(grants >= 4 && permissions >= 4, "{body:#}");
        database.cleanup().await?;
    }
    Ok(())
}
