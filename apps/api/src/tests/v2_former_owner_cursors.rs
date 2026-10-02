// Real collection cursors over projected grant/release inputs, including explicit unregisters.
const FORMER_CURSOR_HOLDER: &str = "0x0000000000000000000000000000000000000063";
const FORMER_CURSOR_REGISTRY: &str = "0x0000000000000000000000000000000000006300";
const FORMER_CURSOR_EXPIRY: i64 = 1_767_225_600; // 2026-01-01T00:00:00Z

async fn seed_former_cursor_names(database: &TestDatabase, dated: bool) -> Result<()> {
    const CHAIN: &str = "ethereum-mainnet";
    const FAMILY: &str = "ens_v2_registry_l1";
    upsert_phase_raw_blocks(
        &database.pool,
        &[raw_block(CHAIN, "0xformer-grant", None, 80, 1_735_689_600)],
    )
    .await?;
    let (_, registry) = declare_family_fixture_contract(
        &database.pool,
        "ens",
        CHAIN,
        FAMILY,
        "registry",
        FORMER_CURSOR_REGISTRY,
    )
    .await?;
    let names = [
        "dated-a.eth",
        "dated-b.eth",
        "dated-c.eth",
        "unregistered-a.eth",
        "unregistered-b.eth",
    ];
    for (index, name) in names.into_iter().enumerate() {
        let undated = index >= 3;
        if !dated && !undated {
            continue;
        }
        let resource = Uuid::from_u128(0x63000 + index as u128 * 10);
        let logical = seed_family_identity_inputs(
            &database.pool,
            "ens",
            name,
            CHAIN,
            80,
            "0xformer-grant",
            resource,
            Uuid::from_u128(resource.as_u128() + 1),
            Uuid::from_u128(resource.as_u128() + 2),
            "ens_v2",
        )
        .await?;
        let token = format!("0x{:064x}", index + 1);
        let release_block = 90 + index as i64;
        let released_at = FORMER_CURSOR_EXPIRY + index as i64;
        let expiry = if undated { 1_900_000_000 } else { released_at };
        let release_hash = format!("0xformer-release-{index}");
        upsert_phase_raw_blocks(
            &database.pool,
            &[raw_block(
                CHAIN,
                &release_hash,
                None,
                release_block,
                released_at,
            )],
        )
        .await?;
        let mut grant = history_event(
            &format!("former-grant-{index}"),
            Some(&logical),
            Some(resource),
            Some(CHAIN),
            Some(80),
            Some("0xformer-grant"),
            Some("0xformer-grant-tx"),
            Some(index as i64),
            CanonicalityState::Canonical,
        );
        grant.event_kind = "RegistrationGranted".into();
        grant.source_family = FAMILY.into();
        grant.raw_fact_ref = json!({"kind":"raw_log", "emitting_address":FORMER_CURSOR_REGISTRY});
        grant.before_state = json!({});
        grant.after_state = json!({"source_event":"LabelRegistered", "status":"registered",
            "registry_contract_instance_id":registry, "token_id":token,
            "registrant":FORMER_CURSOR_HOLDER, "owner":FORMER_CURSOR_HOLDER, "expiry":expiry});
        let mut release = history_event(
            &format!("former-release-{index}"),
            undated.then_some(logical.as_str()),
            Some(resource),
            Some(CHAIN),
            Some(release_block),
            Some(&release_hash),
            Some("0xformer-release-tx"),
            Some(0),
            CanonicalityState::Canonical,
        );
        release.event_kind = "RegistrationReleased".into();
        release.source_family = FAMILY.into();
        release.before_state = json!({});
        // These are the existing token_state_event/append_resource_expiration producer shapes,
        // also exercised by crates/project/tests/families_former_owner.rs.
        release.after_state = if undated {
            json!({"source_event":"LabelUnregistered", "registry_contract_instance_id":registry,
                "token_id":token, "sender":FORMER_CURSOR_HOLDER})
        } else {
            json!({"source_event":"RegistryPathExpired", "derived_from":"interpreter_state",
                "terminal_reason":"registry_name_binding_expired", "registry":FORMER_CURSOR_REGISTRY,
                "registry_contract_instance_id":registry, "token_id":token,
                "expiry":expiry, "status":"released", "released_at":released_at})
        };
        release.raw_fact_ref = if undated {
            json!({"kind":"raw_log", "emitting_address":FORMER_CURSOR_REGISTRY})
        } else {
            json!({"kind":"derived", "emitting_address":FORMER_CURSOR_REGISTRY})
        };
        bigname_storage::insert_normalized_event_fixtures(&database.pool, &[grant, release])
            .await?;
    }
    publish_v2_names_fixture(database).await
}

fn former_cursor_route() -> String {
    format!("/v1/addresses/{FORMER_CURSOR_HOLDER}/names?relation=former_owner&namespace=ens")
}

#[tokio::test]
async fn v2_former_owner_cursor_walks_explicit_unregisters_and_mixed_expiries() -> Result<()> {
    for dated in [false, true] {
        let database = TestDatabase::new_migrated().await?;
        seed_former_cursor_names(&database, dated).await?;
        for order in ["asc", "desc"] {
            let base = format!("{}&order={order}", former_cursor_route());
            let baseline = v2_names_payload(&database, &base).await?;
            let mut expected = if dated {
                // A null expiry is the smallest value.
                vec![
                    "unregistered-a.eth",
                    "unregistered-b.eth",
                    "dated-a.eth",
                    "dated-b.eth",
                    "dated-c.eth",
                ]
            } else {
                vec!["unregistered-a.eth", "unregistered-b.eth"]
            };
            if order == "desc" {
                expected.reverse();
            }
            assert_eq!(v2_names_listed(&baseline), expected);
            for row in baseline["data"].as_array().context("rows")? {
                assert_eq!(
                    row["lapsed_registration"]["owner"],
                    FORMER_CURSOR_HOLDER
                );
                if row["name"]
                    .as_str()
                    .context("name")?
                    .starts_with("unregistered-")
                {
                    assert!(row["expires_at"].is_null());
                    assert_eq!(row["lapsed_registration"]["release_kind"], "unregistered");
                    let released_at = if row["name"] == "unregistered-a.eth" {
                        "1767225603"
                    } else {
                        "1767225604"
                    };
                    assert_eq!(row["lapsed_registration"]["released_at"], released_at);
                }
            }
            let mut uri = format!("{base}&page_size=1");
            let mut walked = Vec::new();
            for _ in 0..expected.len() {
                let response = v2_names_response(&database, &uri).await?;
                let status = response.status();
                let page: Value = read_json(response).await?;
                assert_eq!(status, StatusCode::OK, "dated={dated} {order}: {page}");
                walked.extend(v2_names_listed(&page));
                match page["page"]["next_cursor"].as_str() {
                    Some(cursor) => uri = format!("{base}&page_size=1&cursor={cursor}"),
                    None => break,
                }
            }
            assert_eq!(walked, expected, "dated={dated} {order}");
        }
        database.cleanup().await?;
    }
    Ok(())
}

async fn former_cursor_binds_fractional_bound(field: &str, second: &str) -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_former_cursor_names(&database, true).await?;
    for order in ["asc", "desc"] {
        let base = format!(
            "{}&order={order}&{field}=2026-01-01T00:00:{second}",
            former_cursor_route()
        );
        let original = v2_names_payload(&database, &format!("{base}Z")).await?;
        let first = v2_names_payload(&database, &format!("{base}Z&page_size=1")).await?;
        let cursor = first["page"]["next_cursor"]
            .as_str()
            .context("issued cursor")?;
        let expected_rest =
            v2_names_payload(&database, &format!("{base}Z&cursor={cursor}")).await?;
        // UTC offset spelling and zero fractional digits represent the same instant.
        let equivalent = v2_names_payload(
            &database,
            &format!("{base}.000000000%2B00:00&cursor={cursor}"),
        )
        .await?;
        assert_eq!(equivalent["data"], expected_rest["data"]);
        for fraction in ["5", "000000001"] {
            let changed = format!("{base}.{fraction}Z");
            let changed_page = v2_names_payload(&database, &changed).await?;
            assert_ne!(
                v2_names_listed(&changed_page),
                v2_names_listed(&original),
                "{changed}"
            );
            let response =
                v2_names_response(&database, &format!("{changed}&cursor={cursor}")).await?;
            let status = response.status();
            let payload: Value = read_json(response).await?;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{changed}: {payload}");
            assert_eq!(
                payload["error"]["message"],
                "cursor must be a valid pagination cursor"
            );
            let first = v2_names_payload(&database, &format!("{changed}&page_size=1")).await?;
            let fractional_cursor = first["page"]["next_cursor"]
                .as_str()
                .context("fractional cursor")?;
            let canonical =
                v2_names_payload(&database, &format!("{changed}&cursor={fractional_cursor}"))
                    .await?;
            let equivalent = v2_names_payload(
                &database,
                &format!("{base}.{fraction:0<9}%2B00:00&cursor={fractional_cursor}"),
            )
            .await?;
            assert_eq!(equivalent["data"], canonical["data"]);
        }
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_former_owner_cursor_binds_fractional_lower_bound() -> Result<()> {
    former_cursor_binds_fractional_bound("expires_after", "00").await
}

#[tokio::test]
async fn v2_former_owner_cursor_binds_fractional_upper_bound() -> Result<()> {
    former_cursor_binds_fractional_bound("expires_before", "02").await
}

#[tokio::test]
async fn v2_former_owner_rejects_registration_dedupe() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_former_cursor_names(&database, true).await?;
    let base = former_cursor_route();
    let first = v2_names_payload(&database, &format!("{base}&page_size=1")).await?;
    let cursor = first["page"]["next_cursor"].as_str().context("cursor")?;
    let next = v2_names_payload(&database, &format!("{base}&cursor={cursor}")).await?;
    let explicit_name =
        v2_names_payload(&database, &format!("{base}&dedupe=name&cursor={cursor}")).await?;
    assert_eq!(next["data"], explicit_name["data"]);
    for suffix in [String::new(), format!("&cursor={cursor}")] {
        let response =
            v2_names_response(&database, &format!("{base}&dedupe=registration{suffix}")).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body: Value = read_json(response).await?;
        assert_eq!(
            body["error"]["message"],
            "dedupe=registration is not supported with relation=former_owner"
        );
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_former_owner_expiry_bounds_are_rejected_on_other_relations() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_former_cursor_names(&database, true).await?;
    for relation in ["resolves_to", "owner", "any"] {
        for bound in ["expires_after", "expires_before"] {
            let response = v2_names_response(&database, &format!(
                "/v1/addresses/{FORMER_CURSOR_HOLDER}/names?relation={relation}&{bound}=2026-01-01T00:00:00Z"
            )).await?;
            assert_eq!(
                response.status(),
                StatusCode::BAD_REQUEST,
                "{relation} {bound}"
            );
            let body: Value = read_json(response).await?;
            assert_eq!(
                body["error"]["message"],
                "expires_after and expires_before require relation=former_owner"
            );
        }
    }
    database.cleanup().await
}
