use super::*;

#[tokio::test]
async fn v2_history_token_ids_keep_old_versions_and_pre_regeneration_permission_tokens()
-> Result<()> {
    let database = routes::database_at(134).await?;
    let mut seen = std::collections::BTreeMap::new();
    for route in [
        format!("/v1/names/{NAME}/history?scope=both"),
        format!("/v1/names/{NAME}/history?scope=name"),
        format!("/v1/names/{NAME}/history?scope=registration"),
        format!("/v1/events?name={NAME}"),
        format!("/v1/events?contract_address={REGISTRY}"),
        format!("/v1/addresses/{GRANTEE}/history?namespace=ens"),
    ] {
        let (status, expanded) = read_family_response(
            &database,
            &format!("{route}&include=data,raw,total_count&page_size=200&order=asc"),
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "{route}: {expanded:#}");
        let rows = expanded["data"].as_array().context("history data")?;
        let mut token_count = 0;
        for row in rows {
            let block = row["block_number"].as_i64().unwrap();
            let expected = match row["kind"].as_str().unwrap() {
                "RegistrationGranted" => match block {
                    120 => Some(0),
                    123 => Some(2),
                    131 => Some(4),
                    134 => Some(5),
                    _ => None,
                },
                "TokenControlTransferred" if matches!(block, 125 | 126) => Some(3),
                "PermissionChanged" => match block {
                    120 | 121 => Some(0),
                    123 | 124 => Some(2),
                    131 => Some(4),
                    134 => Some(5),
                    _ => None,
                },
                _ => None,
            };
            assert_eq!(
                row["data"].get("token_id"),
                expected
                    .map(|version| json!(token(version).to_string()))
                    .as_ref(),
                "{route}: {row:#}"
            );
            if expected.is_some() {
                token_count += 1;
            }
            let id = row["id"].as_str().unwrap().to_owned();
            if let Some(previous) = seen.insert(id, row["data"].clone()) {
                assert_eq!(previous, row["data"], "same event across routes: {row:#}");
            }
        }
        assert!(token_count >= 2, "{route}: {expanded:#}");
        let (status, lean) = read_family_response(
            &database,
            &format!("{route}&include=raw,total_count&page_size=200&order=asc"),
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "{lean:#}");
        let lean_rows = lean["data"].as_array().unwrap();
        assert_eq!(
            rows.iter().map(|row| &row["id"]).collect::<Vec<_>>(),
            lean_rows.iter().map(|row| &row["id"]).collect::<Vec<_>>()
        );
        assert_eq!(expanded["page"], lean["page"]);
        assert!(lean_rows.iter().all(|row| row.get("data").is_none()));
    }
    database.cleanup().await
}

pub(super) async fn assert_registration_payment(database: &TestDatabase, paid: bool) -> Result<()> {
    let mut actions = std::collections::BTreeSet::new();
    let mut registered = 0;
    for route in [
        format!("/v1/names/{NAME}/history?scope=both"),
        format!("/v1/events?name={NAME}"),
        format!("/v1/addresses/{HOLDER}/history?namespace=ens"),
    ] {
        let (status, body) = read_family_response(
            database,
            &format!("{route}&include=data,raw,total_count&order=asc&page_size=200"),
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "{body:#}");
        let rows = body["data"].as_array().unwrap();
        for row in rows {
            assert_ne!(row["kind"], "RegistrarNameRegistered", "{row:#}");
            if row["kind"] == "RegistrationGranted" && row["block_number"] == 120 {
                registered += 1;
                if paid {
                    assert_eq!(row["data"]["base_cost"], U256::MAX.to_string(), "{row:#}");
                    assert_eq!(row["data"]["premium"], "0", "{row:#}");
                    assert_eq!(
                        row["data"]["referrer"],
                        format!("0x{}", "0".repeat(64)),
                        "{row:#}"
                    );
                    assert_eq!(
                        row["data"]["payment_token"],
                        json!({"chain_id":1,"address":GRANTEE}),
                        "{row:#}"
                    );
                    actions.insert(row["data"]["action_id"].as_str().unwrap().to_owned());
                } else {
                    assert!(row["data"].get("base_cost").is_none(), "{row:#}");
                }
            } else {
                assert!(row["data"].get("base_cost").is_none(), "{row:#}");
            }
        }
        // A page of one still obtains evidence later in the transaction without adding rows.
        let mut cursor = None::<String>;
        let mut paged = Vec::new();
        loop {
            let url = format!(
                "{route}&include=data,raw,total_count&order=asc&page_size=1{}",
                cursor
                    .as_ref()
                    .map(|c| format!("&cursor={c}"))
                    .unwrap_or_default()
            );
            let (status, page) = read_family_response(database, &url).await?;
            assert_eq!(status, StatusCode::OK, "{page:#}");
            paged.extend(page["data"].as_array().unwrap().iter().cloned());
            cursor = page["page"]["next_cursor"].as_str().map(str::to_owned);
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(paged, *rows, "{route}");
    }
    assert!(registered >= 3);
    if paid {
        assert_eq!(actions.len(), 1);
    }
    Ok(())
}

#[tokio::test]
async fn v2_history_payload_context_rejects_redo_on_all_collection_routes() -> Result<()> {
    use bigname_storage::history_anchor_read_test_hooks::{HistoryReadHookPoint, install};
    let database = routes::database_at(134).await?;
    for point in [
        HistoryReadHookPoint::AfterPage,
        HistoryReadHookPoint::AfterContext,
    ] {
        for finish in [false, true] {
            for route in [
                format!("/v1/names/{NAME}/history?include=data,raw"),
                format!("/v1/names/{NAME}/history?include=data,raw,child_registrations"),
                format!("/v1/events?name={NAME}&include=data,raw"),
                format!("/v1/events?contract_address={REGISTRY}&include=data,raw"),
                format!("/v1/addresses/{HOLDER}/history?include=data,raw"),
            ] {
                let (_guard, control) = install(&database.lookup_pool, point).await?;
                let state = database.app_state_with_public_namespaces(&["ens"]);
                let request_route = route.clone();
                let task = tokio::spawn(async move {
                    app_router(state)
                        .oneshot(
                            Request::builder()
                                .uri(request_route)
                                .body(Body::empty())
                                .unwrap(),
                        )
                        .await
                });
                tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    control.wait_until_reached(),
                )
                .await
                .context("history context hook")?;
                database
                    .simulate_interpret_redo_begin(CHAIN, "recompute_flags")
                    .await?;
                // Invalid replacement evidence must still return stale, not an internal error.
                let saved:Vec<(i64,Value)>=sqlx::query_as("SELECT normalized_event_id,after_state FROM normalized_events WHERE event_kind='TokenResourceLinked'").fetch_all(&database.pool).await?;
                if point == HistoryReadHookPoint::AfterPage {
                    sqlx::query("UPDATE normalized_events SET after_state=jsonb_set(after_state,'{current_token_id}','\"invalid\"') WHERE event_kind='TokenResourceLinked'").execute(&database.pool).await?;
                }
                if finish {
                    database.simulate_interpret_redo_finish(CHAIN).await?;
                }
                control.resume().await;
                let response = task.await??;
                let status = response.status();
                let body: Value = read_json(response).await?;
                assert_eq!(
                    status,
                    StatusCode::CONFLICT,
                    "{point:?} {finish} {route}: {body:#}"
                );
                assert_eq!(body["error"]["code"], "stale", "{body:#}");
                assert!(body.get("data").is_none());
                for (id, after) in saved {
                    sqlx::query(
                        "UPDATE normalized_events SET after_state=$1 WHERE normalized_event_id=$2",
                    )
                    .bind(after)
                    .bind(id)
                    .execute(&database.pool)
                    .await?;
                }
                if !finish {
                    database.simulate_interpret_redo_finish(CHAIN).await?;
                }
            }
        }
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_history_context_keeps_captured_bounds_during_healthy_publication_advance() -> Result<()>
{
    use bigname_storage::history_anchor_read_test_hooks::{HistoryReadHookPoint, install};
    for route in [
        format!("/v1/names/{NAME}/history?scope=both"),
        format!("/v1/events?name={NAME}"),
        format!("/v1/addresses/{HOLDER}/history?namespace=ens"),
    ] {
        let database = routes::database_at(131).await?;
        let (_guard, control) =
            install(&database.lookup_pool, HistoryReadHookPoint::AfterPage).await?;
        let state = database.app_state_with_public_namespaces(&["ens"]);
        let url = format!("{route}&include=data,raw&page_size=200");
        let task = tokio::spawn(async move {
            app_router(state)
                .oneshot(Request::builder().uri(url).body(Body::empty()).unwrap())
                .await
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            control.wait_until_reached(),
        )
        .await
        .context("history publication hook")?;
        let engine = bigname_interpret::Engine::new(database.pool.clone());
        for block in 132..=134 {
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
        control.resume().await;
        let response = task.await??;
        let status = response.status();
        let body: Value = read_json(response).await?;
        assert_eq!(status, StatusCode::OK, "{route}: {body:#}");
        for row in body["data"].as_array().unwrap() {
            assert!(row["block_number"].as_i64().unwrap() <= 131, "{row:#}");
            assert_ne!(row["data"]["token_id"], token(5).to_string(), "{row:#}");
        }
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn v2_history_batched_registrations_keep_separate_payments_and_omit_missing_evidence()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let registrar =
        compatibility::admit_family_from(&database, "sepolia", CHAIN, "ens_v2_registrar_l1", 990)
            .await?;
    let registrar_address = compatibility::role_address(&registrar, "registrar");
    let (manifest, _, _) = fixture(false)?;
    let mut logs = Vec::new();
    for (offset, label, price) in [(0, LABEL, U256::MAX), (5, "anotherpayment", U256::from(77))] {
        let label_hash = keccak256(label);
        let id = U256::from_be_bytes(*label_hash) & !U256::from(u32::MAX);
        logs.extend([
            raw(
                LabelRegistered {
                    tokenId: id,
                    labelHash: label_hash,
                    label: label.into(),
                    owner: HOLDER.parse()?,
                    expiry: 1_900_000_000,
                    sender: registrar_address,
                }
                .encode_log_data(),
                120,
                offset,
            ),
            raw(
                TransferSingle {
                    operator: registrar_address,
                    from: Address::ZERO,
                    to: HOLDER.parse()?,
                    id,
                    value: U256::from(1),
                }
                .encode_log_data(),
                120,
                offset + 1,
            ),
            raw(
                TokenResource {
                    tokenId: id,
                    resource: id,
                }
                .encode_log_data(),
                120,
                offset + 2,
            ),
            raw(
                EACRolesChanged {
                    resource: id,
                    account: HOLDER.parse()?,
                    oldRoleBitmap: U256::ZERO,
                    newRoleBitmap: U256::from(1),
                }
                .encode_log_data(),
                120,
                offset + 3,
            ),
        ]);
        let mut payment = raw(
            compatibility::payment_observation(false, label, id, price)?,
            120,
            offset + 4,
        );
        payment.emitting_address = format!("{registrar_address:#x}");
        logs.push(payment);
    }
    seed_v2_history_blocks(&database, 120..=120).await?;
    seed_interpret(&database, &manifest, &logs).await?;
    bigname_interpret::Engine::new(database.pool.clone())
        .run_batch(bigname_interpret::BatchRequest {
            chain_id: CHAIN.into(),
            from_block: 120,
            to_block: 120,
            resume_current: None,
            mode: bigname_interpret::RunMode::Normal,
        })
        .await?;
    routes::publish(&database, 120).await?;
    let route = format!(
        "/v1/addresses/{HOLDER}/history?namespace=ens&include=data,raw,total_count&order=asc&page_size=200"
    );
    let (status, body) = read_family_response(&database, &route).await?;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    let rows = body["data"].as_array().unwrap();
    let grants = rows
        .iter()
        .filter(|r| r["kind"] == "RegistrationGranted")
        .collect::<Vec<_>>();
    assert_eq!(grants.len(), 4, "{body:#}");
    let mut actions = std::collections::BTreeMap::<String, usize>::new();
    for row in grants {
        let expected = if row["name"] == NAME {
            U256::MAX.to_string()
        } else {
            "77".to_owned()
        };
        assert_eq!(row["data"]["base_cost"], expected, "{row:#}");
        *actions
            .entry(row["data"]["action_id"].as_str().unwrap().into())
            .or_default() += 1;
    }
    assert_eq!(actions.len(), 2);
    assert!(actions.values().all(|count| *count == 2));
    sqlx::query("DELETE FROM normalized_events WHERE event_kind='RegistrarNameRegistered'")
        .execute(&database.pool)
        .await?;
    let (status, missing) = read_family_response(&database, &route).await?;
    assert_eq!(status, StatusCode::OK, "{missing:#}");
    assert_eq!(body["page"], missing["page"]);
    let missing = missing["data"].as_array().unwrap();
    assert_eq!(
        rows.iter().map(|r| &r["id"]).collect::<Vec<_>>(),
        missing.iter().map(|r| &r["id"]).collect::<Vec<_>>()
    );
    assert!(missing.iter().all(|r| r["data"].get("base_cost").is_none()));
    database.cleanup().await
}
