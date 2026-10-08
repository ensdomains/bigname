//! Public route parity across the publication clock, using normalized lifecycle evidence through
//! the real Project reducers. Raw Engine production/restart coverage lives in Interpret tests.
use super::*;

const E: i64 = 1_700_000_242;
const G: i64 = E + 90 * 86400;
const NAME: &str = "lifecycle.eth";

fn assert_schedule(row: &Value, status: &str, context: &str) {
    assert_eq!(row["status"], status, "{context}: {row}");
    assert_eq!(row["expires_at"], E.to_string(), "{context}: {row}");
    assert_eq!(row["grace_ends_at"], G.to_string(), "{context}: {row}");
    assert!(row.get("registration_status").is_none(), "{context}: {row}");
}

fn named<'a>(rows: &'a Value, name: &str) -> Option<&'a Value> {
    rows.as_array()?.iter().find(|row| row["name"] == name)
}

#[tokio::test]
async fn lifecycle_public_routes_use_one_schedule_at_expiry_and_grace_publications() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bounded_membership_blocks(&database, 240).await?;
    // Seed real publication clocks before fixture events: the fixture inserter creates missing
    // event blocks with a year-2000 placeholder, and canonical block timestamps are immutable.
    for (block, now) in [(241, E - 1), (242, E), (243, G), (244, G + 1)] {
        upsert_phase_raw_blocks(
            &database.pool,
            &[raw_block(
                FAMILY_CHAIN,
                &format!("0xhistory{block}"),
                Some(&format!("0xhistory{}", block - 1)),
                block,
                now,
            )],
        )
        .await?;
    }
    let (logical, resource) = seed_family_name(&database, NAME, 0x262_1000, "ens_v1").await?;
    seed_family_name(&database, "eth", 0x262_2000, "ens_v1").await?;
    insert_family_label_preimage(&database.pool, b"lifecycle").await?;
    let node = logical.strip_prefix("ens:").unwrap();
    let parent = bigname_lookup::ens_namehash_hex("eth")?;
    let mut release = family_event(
        "lifecycle-release",
        Some(&logical),
        Some(resource),
        "RegistrationReleased",
        "ens_v1_registrar_l1",
        244,
        0,
        json!({"authority_kind":"registrar", "expiry":E, "released_at":G+1}),
    );
    release.before_state = json!({"registrant":FAMILY_ALICE, "authority_kind":"registrar"});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[
        family_event("lifecycle-grant", Some(&logical), Some(resource), "RegistrationGranted", "ens_v1_registrar_l1", 210, 0,
            json!({"authority_kind":"registrar", "registrant":FAMILY_ALICE, "expiry":E})),
        family_event("lifecycle-node", Some(&logical), Some(resource), "SubregistryChanged", "ens_v1_registry_l1", 210, 1,
            json!({"source_event":"NewOwner", "node":parent, "child_node":node,
                "labelhash":child_labelhash("lifecycle"), "owner":FAMILY_ALICE,"owner_getter":FAMILY_ALICE,"emitter_role":"registry"})),
        family_event("lifecycle-registry-owner", Some(&logical), Some(resource), "AuthorityTransferred", "ens_v1_registry_l1", 210, 2,
            json!({"source_event":"Transfer", "node":node,"owner":FAMILY_ALICE,"owner_getter":FAMILY_ALICE,"emitter_role":"registry"})),
        family_event("lifecycle-resolver", Some(&logical), Some(resource), "ResolverChanged", "ens_v1_registry_l1", 210, 3,
            json!({"node":node,"resolver":FAMILY_RESOLVER})),
        release,
    ]).await?;
    sqlx::query(
        "UPDATE surface_bindings SET active_to=to_timestamp($2) WHERE surface_binding_id=$1",
    )
    .bind(Uuid::from_u128(0x262_1002))
    .bind((G + 1) as f64)
    .execute(&database.pool)
    .await?;
    declare_family_fixture_resolver(
        &database.pool,
        "ens",
        FAMILY_CHAIN,
        "ens_v1_resolver_l1",
        FAMILY_RESOLVER,
    )
    .await?;
    let mut registration_handle = None;
    // Each state has a new canonical block; the expiry and grace publications add no events.
    for (block, now, status) in [
        (241, E - 1, "active"),
        (242, E, "expired"),
        (243, G, "expired"),
        (244, G + 1, "released"),
    ] {
        upsert_phase_raw_blocks(
            &database.pool,
            &[raw_block(
                FAMILY_CHAIN,
                &format!("0xhistory{block}"),
                Some(&format!("0xhistory{}", block - 1)),
                block,
                now,
            )],
        )
        .await?;
        database.seed_snapshot_selector_chain_positions(&json!({
            "ethereum":{"chain_id":FAMILY_CHAIN,"block_number":block,
                "block_hash":format!("0xhistory{block}"),
                "timestamp":bigname_storage::UnixSeconds::from(OffsetDateTime::from_unix_timestamp(now)?).internal_string()}
        })).await?;
        publish_test_families_on(&database.pool, FAMILY_CHAIN, block).await?;
        let published: OffsetDateTime = sqlx::query_scalar(
            "SELECT block_timestamp FROM chain_lineage WHERE chain_id=$1 AND block_hash=$2",
        )
        .bind(FAMILY_CHAIN)
        .bind(format!("0xhistory{block}"))
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(published.unix_timestamp(), now);
        let detail = v2_names_payload(&database, &format!("/v1/names/{NAME}")).await?;
        assert_eq!(detail["data"]["read_status"], "ok", "{detail}");
        assert_schedule(&detail["data"], status, "detail");
        assert!(
            detail["data"]["registration_id"].is_string(),
            "stable lease handle: {detail}"
        );
        if let Some(handle) = &registration_handle {
            assert_eq!(
                &detail["data"]["registration_id"], handle,
                "passive time changed the public lease handle"
            );
        } else {
            registration_handle = Some(detail["data"]["registration_id"].clone());
        }
        for uri in [
            format!(
                "/v1/names?namespace=ens&expires_after={E}&expires_before={}",
                E + 1
            ),
            format!(
                "/v1/names?namespace=ens&grace_ends_after={G}&grace_ends_before={}",
                G + 1
            ),
            "/v1/search?q=lifecycle&match=prefix".to_owned(),
            "/v1/names/eth/subnames?include_expired=true".to_owned(),
        ] {
            let body = lifecycle_payload(&database, &uri).await?;
            let row = named(&body["data"], NAME).with_context(|| format!("{uri}: {body}"))?;
            assert_schedule(row, status, &uri);

            assert_eq!(body["meta"]["as_of"], detail["meta"]["as_of"], "{uri}");
        }
        let children =
            v2_names_payload(&database, "/v1/names/eth/subnames?include_expired=false").await?;
        assert_eq!(
            named(&children["data"], NAME).is_some(),
            status == "active",
            "{children}"
        );
        let address = v2_names_payload(
            &database,
            &format!("/v1/addresses/{FAMILY_ALICE}/names?relation=owner&namespace=ens"),
        )
        .await?;
        if status == "released" {
            assert!(named(&address["data"], NAME).is_none(), "{address}");
        } else {
            assert_schedule(
                named(&address["data"], NAME).context("owner row through grace")?,
                status,
                "address owner",
            );
        }
        for profile in ["detail", "feed"] {
            let lookup = v2_lookup_json(
                &database,
                json!({"profile":profile,"inputs":[{"name":NAME}]}),
            )
            .await?;
            assert_eq!(lookup["data"][0]["status"], "ok", "{lookup}");
            assert_eq!(lookup["data"][0]["record"]["read_status"], "ok", "{lookup}");
            assert_schedule(&lookup["data"][0]["record"], status, profile);
            assert_eq!(lookup["meta"]["as_of"], detail["meta"]["as_of"], "{lookup}");
        }
        let resolver =
            v2_names_payload(&database, &format!("/v1/resolvers/1/{FAMILY_RESOLVER}")).await?;
        if status == "released" {
            assert!(
                named(&resolver["data"]["bound_names"]["data"], NAME).is_none(),
                "{resolver}"
            );
        } else {
            assert_schedule(
                named(&resolver["data"]["bound_names"]["data"], NAME)
                    .context("resolver bound name")?,
                status,
                "nested resolver",
            );
        }
    }
    database.cleanup().await
}

async fn lifecycle_payload(database: &TestDatabase, uri: &str) -> Result<Value> {
    let (status, body) = read_family_response(database, uri).await?;
    anyhow::ensure!(status == StatusCode::OK, "{uri}: {status}: {body}");
    Ok(body)
}
