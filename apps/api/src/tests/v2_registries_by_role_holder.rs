//! `GET /v1/registries?role_holder=` on the registry root fixture:
//!
//! | Holder | Expected |
//! | --- | --- |
//! | each of `ROOT_HOLDERS` | alpha, with the powers `/v1/permissions?registry=` lists for it |
//! | `ROOT_HOLDERS[0]` in upper case | the same page |
//! | `REVOKED_HOLDER` | no row, as `/v1/permissions?registry=` lists none |
//! | `V2_PERMISSIONS_SUBJECT`, a per-name grant on alpha only | no row |
//! | the same after alpha's parent name lapses by path expiry | the same rows on both routes |
//! | `ROOT_HOLDERS[0]` once alpha's instance retired | still alpha; `/v1/permissions?registry=` is empty |
//! | `PAGER`, root grants at three addresses, one stored in upper case | lower-case rows in byte order, one per page |
use super::*;

const PAGER: &str = "0x00000000000000000000000000000000000003f0";

async fn registries_response(database: &TestDatabase, uri: &str) -> Result<(StatusCode, Value)> {
    let response = app_router(database.app_state_with_public_namespaces(&["ens"]))
        .oneshot(Request::builder().uri(uri).body(Body::empty())?)
        .await
        .context("registries request failed")?;
    let status = response.status();
    Ok((status, read_json(response).await?))
}

/// The page for `holder` with and without the chain filter, which must agree.
async fn registries_of(database: &TestDatabase, holder: &str) -> Result<Value> {
    let mut pages = Vec::new();
    for filter in ["", "&chain_id=1"] {
        let uri = format!("/v1/registries?role_holder={holder}{filter}");
        let (status, body) = registries_response(database, &uri).await?;
        assert_eq!(status, StatusCode::OK, "{uri}: {body}");
        assert_eq!(body["page"]["total_count"], Value::Null, "{body}");
        assert_eq!(body["page"]["has_more"], false, "{body}");
        pages.push(body["data"].clone());
    }
    assert_eq!(pages[0], pages[1], "{holder}");
    Ok(pages.remove(0))
}

/// The row `/v1/permissions?registry=1:alpha&address=` implies for `holder`: its root rows
/// counted, their powers unioned and sorted, or none when it lists no root row.
async fn row_from_permissions(database: &TestDatabase, holder: &str) -> Result<Value> {
    let page = v2_permissions_payload_for_database(
        database,
        &format!("/v1/permissions?registry=1:{ALPHA_REGISTRY}&address={holder}&page_size=200"),
    )
    .await?;
    assert_eq!(page["page"]["has_more"], false, "{page}");
    let rows: Vec<&Value> = page["data"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["grant_scope"]["kind"] == "root")
        .collect();
    if rows.is_empty() {
        return Ok(json!([]));
    }
    let powers = rows
        .iter()
        .flat_map(|row| row["powers"].as_array().unwrap().iter())
        .map(|power| power.as_str().unwrap().to_owned())
        .collect::<std::collections::BTreeSet<_>>();
    Ok(json!([{"chain_id":1, "address":ALPHA_REGISTRY, "grant_count":rows.len(), "powers":powers}]))
}

/// Every holder's page equals what `/v1/permissions?registry=` lists for it.
async fn assert_parity(database: &TestDatabase) -> Result<()> {
    let holders = ROOT_HOLDERS
        .iter()
        .copied()
        .chain([REVOKED_HOLDER, V2_PERMISSIONS_SUBJECT]);
    for holder in holders {
        let expected = row_from_permissions(database, holder).await?;
        assert_eq!(registries_of(database, holder).await?, expected, "{holder}");
    }
    Ok(())
}

#[tokio::test]
async fn registries_by_role_holder_match_the_registry_root_holders() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_root_holders(&database).await?;
    for holder in ROOT_HOLDERS {
        assert_eq!(
            row_from_permissions(&database, holder).await?,
            json!([{"chain_id":1, "address":ALPHA_REGISTRY, "grant_count":1,
                    "powers":["admin_registrar", "registrar"]}]),
            "{holder}"
        );
    }
    for holder in [REVOKED_HOLDER, V2_PERMISSIONS_SUBJECT] {
        assert_eq!(row_from_permissions(&database, holder).await?, json!([]));
    }
    assert_parity(&database).await?;
    let upper = ROOT_HOLDERS[0].to_ascii_uppercase().replacen("0X", "0x", 1);
    assert_eq!(
        registries_of(&database, &upper).await?,
        registries_of(&database, ROOT_HOLDERS[0]).await?
    );

    // alpha's parent name lapses by path expiry. The lapse drops the parent token's own
    // grants, not alpha's root grants: both routes still list the holders.
    let lapse = json!({"position":{"block_number":80, "transaction_index":0, "log_index":0,
        "event_identity":"registry-alpha-lapse"}, "source_event":"fixture"});
    sqlx::query(
        "INSERT INTO project_lifecycle_key_state (chain_id, resource_id, block_number,
             event_identity, last_active, last_path_expiry)
         VALUES ('ethereum-mainnet', $1, 80, 'registry-alpha-lapse', $2, $3)
         ON CONFLICT (chain_id, resource_id) DO UPDATE
         SET last_path_expiry = EXCLUDED.last_path_expiry, last_revival = NULL",
    )
    .bind(Uuid::from_u128(0xA000))
    .bind(
        json!({"kind":"RegistrationGranted", "position":{"block_number":59,
        "transaction_index":0, "log_index":0, "event_identity":"registry-alpha-grant"}}),
    )
    .bind(&lapse)
    .execute(&database.pool)
    .await?;
    assert_parity(&database).await?;
    assert_eq!(
        registries_of(&database, ROOT_HOLDERS[0])
            .await?
            .as_array()
            .unwrap()
            .len(),
        1
    );

    // Retiring alpha's instance before the publication empties the registry read, while the
    // stored grant is still listed here.
    sqlx::query(
        "UPDATE contract_instance_addresses SET active_to_block_number = 80, deactivated_at = now()
         WHERE address = $1",
    )
    .bind(ALPHA_REGISTRY)
    .execute(&database.pool)
    .await?;
    assert_eq!(
        row_from_permissions(&database, ROOT_HOLDERS[0]).await?,
        json!([])
    );
    assert_eq!(
        registries_of(&database, ROOT_HOLDERS[0]).await?,
        json!([{"chain_id":1, "address":ALPHA_REGISTRY, "grant_count":1,
                "powers":["admin_registrar", "registrar"]}])
    );
    database.cleanup().await
}

/// Root grants at three addresses, the second stored in upper case, page one at a time in
/// lower-case byte order, with and without the chain filter, and each cursor binds the holder
/// and the filter. A request without a holder, with a malformed holder, chain, page size or
/// cursor, a cursor from another list, or a parameter the route does not take is
/// `400 invalid_input`.
#[tokio::test]
async fn registries_by_role_holder_page_by_cursor_and_refuse_malformed_requests() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_root_holders(&database).await?;
    let registries = [
        "0x00000000000000000000000000000000000003a1",
        "0x00000000000000000000000000000000000003b2",
        "0x00000000000000000000000000000000000003c3",
    ];
    for (index, registry) in registries.iter().enumerate() {
        // As stored, 03B2 sorts before 03a1.
        let stored = if index == 1 {
            registry.to_ascii_uppercase().replacen("0X", "0x", 1)
        } else {
            (*registry).to_owned()
        };
        sqlx::query(
            "INSERT INTO project_grant (chain_id, resource_id, subject, scope, block_number,
                 event_identity, event_kind, scope_kind, scope_detail, effective_powers,
                 grant_source, revoked)
             VALUES ('ethereum-mainnet', $1, $2, 'root', 80, $3, 'RootPermissionChanged', 'root',
                 jsonb_build_object('kind', 'root', 'chain_id', 'ethereum-mainnet',
                     'registry_address', $4::text),
                 '[\"registrar\"]',
                 '{\"kind\": \"raw_log\", \"source_event\": \"EACRolesChanged\",
                   \"root_resource\": true}', false)",
        )
        .bind(Uuid::from_u128(0x3f0_000 + index as u128))
        .bind(PAGER)
        .bind(format!("registry-role-holder-pager:{index}"))
        .bind(stored)
        .execute(&database.pool)
        .await?;
    }
    // Two resolver grants, so the resolver list of the same holder has a cursor.
    for index in 0..2 {
        sqlx::query(
            "INSERT INTO project_grant (chain_id, resource_id, subject, scope, block_number,
                 event_identity, event_kind, scope_kind, effective_powers, grant_source, revoked)
             VALUES ('ethereum-mainnet', $1, $2, $3, 80, $4, 'PermissionChanged', 'resolver',
                 '[\"set_addr\"]',
                 '{\"kind\": \"raw_log\", \"source_event\": \"EACRolesChanged\",
                   \"root_resource\": true}', false)",
        )
        .bind(Uuid::from_u128(0x3f1_000 + index as u128))
        .bind(PAGER)
        .bind(format!(
            "resolver:ethereum-mainnet:0x00000000000000000000000000000000000003e{index}"
        ))
        .bind(format!("registry-role-holder-pager-resolver:{index}"))
        .execute(&database.pool)
        .await?;
    }
    let mut cursors = Vec::new();
    for filter in ["", "&chain_id=1"] {
        let base = format!("/v1/registries?role_holder={PAGER}{filter}&page_size=1");
        let mut route = base.clone();
        let mut walked = Vec::new();
        loop {
            let (status, body) = registries_response(&database, &route).await?;
            assert_eq!(status, StatusCode::OK, "{body}");
            walked.extend(body["data"].as_array().unwrap().iter().cloned());
            if body["page"]["has_more"] == false {
                assert!(body["page"]["next_cursor"].is_null(), "{body}");
                break;
            }
            let cursor = body["page"]["next_cursor"].as_str().unwrap().to_owned();
            route = format!("{base}&cursor={cursor}");
            cursors.push((filter, cursor));
        }
        assert_eq!(
            walked,
            registries
                .iter()
                .map(
                    |registry| json!({"chain_id":1, "address":registry, "grant_count":1,
                                       "powers":["registrar"]})
                )
                .collect::<Vec<_>>(),
            "{filter}"
        );
    }
    let (status, resolvers) = registries_response(
        &database,
        &format!("/v1/resolvers?role_holder={PAGER}&page_size=1"),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{resolvers}");
    let resolver_cursor = resolvers["page"]["next_cursor"]
        .as_str()
        .expect("a second resolver")
        .to_owned();
    let mut refused = vec![
        "/v1/registries".to_owned(),
        "/v1/registries?role_holder=".to_owned(),
        "/v1/registries?role_holder=0x12".to_owned(),
        format!("/v1/registries?role_holder={PAGER}&chain_id=5"),
        format!("/v1/registries?role_holder={PAGER}&chain_id=99999"),
        format!("/v1/registries?role_holder={PAGER}&chain_id=11155111"),
        format!("/v1/registries?role_holder={PAGER}&chain_id=ethereum-mainnet"),
        format!("/v1/registries?role_holder={PAGER}&page_size=0"),
        format!("/v1/registries?role_holder={PAGER}&namespace=ens"),
        format!("/v1/registries?role_holder={PAGER}&cursor=zz"),
        format!("/v1/registries?role_holder={PAGER}&cursor={resolver_cursor}"),
        format!(
            "/v1/registries?role_holder={}&cursor={}",
            ROOT_HOLDERS[0], cursors[0].1
        ),
    ];
    // A cursor from the unfiltered list does not continue the filtered one, nor the reverse.
    for (filter, cursor) in &cursors {
        let other = if filter.is_empty() { "&chain_id=1" } else { "" };
        refused.push(format!(
            "/v1/registries?role_holder={PAGER}{other}&cursor={cursor}"
        ));
    }
    for uri in refused {
        let (status, body) = registries_response(&database, &uri).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {body}");
        assert_eq!(body["error"]["code"], "invalid_input", "{uri}: {body}");
    }
    let (_, body) = registries_response(&database, "/v1/registries").await?;
    assert!(
        body["error"]["message"]
            .as_str()
            .is_some_and(|message| message.starts_with("role_holder is required")),
        "{body}"
    );
    database.cleanup().await
}
