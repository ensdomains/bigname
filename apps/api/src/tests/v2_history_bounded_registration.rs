// Name history continuations and the registration set, judged at the published block. A
// continuation does not look the name up again, and the registration resources a read follows come
// from bindings, wrap links and registrar grants at or below the bound, never from the
// registration Project selects for the name now.

async fn bounded_route_status(database: &TestDatabase, uri: &str) -> Result<(StatusCode, Value)> {
    let response = v2_history_response_for_database(database, uri).await?;
    let status = response.status();
    Ok((status, read_json(response).await?))
}

#[tokio::test]
async fn name_history_continuation_does_not_look_the_name_up_again() -> Result<()> {
    const NAME: &str = "continued.eth";
    let database = TestDatabase::new_migrated().await?;
    seed_bounded_membership_blocks(&database, 240).await?;
    let (logical_name_id, resource) = seed_bounded_name(
        &database,
        NAME,
        0xb0a_4000,
        "0x00000000000000000000000000000000000b0a04",
        bigname_storage::AddressNameRelation::EffectiveController,
        205,
    )
    .await?;
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            v2_history_event(
                "continued-grant",
                Some(&logical_name_id),
                Some(resource),
                "RegistrationGranted",
                205,
            ),
            v2_history_event(
                "continued-renew",
                Some(&logical_name_id),
                Some(resource),
                "RegistrationRenewed",
                210,
            ),
            v2_history_event(
                "continued-transfer",
                Some(&logical_name_id),
                Some(resource),
                "AuthorityTransferred",
                215,
            ),
        ],
    )
    .await?;
    publish_bounded_membership_at(&database, 240).await?;

    let mut cursors = Vec::new();
    for scope in ["name", "registration", "both"] {
        let route = format!("/v1/names/{NAME}/history?scope={scope}&page_size=1");
        let first = v2_history_payload_for_database(&database, &route).await?;
        assert_eq!(
            bounded_route_hashes(&first),
            vec!["0xtx215"],
            "{route}: {first}"
        );
        let cursor = first["page"]["next_cursor"]
            .as_str()
            .expect("second page")
            .to_owned();
        cursors.push((route, cursor));
    }

    // Project stops publishing the name. The first page reports it missing; a continuation
    // already bound to the name keeps paging through the evidence at the bound.
    sqlx::query("DELETE FROM bigname_phase.name_current WHERE logical_name_id = $1")
        .bind(&logical_name_id)
        .execute(&database.pool)
        .await?;
    for (route, cursor) in cursors {
        let (status, payload) = bounded_route_status(&database, &route).await?;
        assert_eq!(status, StatusCode::NOT_FOUND, "{route}: {payload}");
        let (status, payload) =
            bounded_route_status(&database, &format!("{route}&cursor={cursor}")).await?;
        assert_eq!(status, StatusCode::OK, "{route} continuation: {payload}");
        assert_eq!(
            bounded_route_hashes(&payload),
            vec!["0xtx210"],
            "{route}: {payload}"
        );
    }
    database.cleanup().await
}

// Project selects a lease as the name's registration whose only registrar grant lies above the
// bound. The lease's older rows must not join a read bound below that grant: until the grant is
// published, no binding, wrap link or grant ties the lease to the name.
#[tokio::test]
async fn registration_selected_above_the_bound_adds_no_lease_history() -> Result<()> {
    const NAME: &str = "selected-later.eth";
    let database = TestDatabase::new_migrated().await?;
    seed_bounded_membership_blocks(&database, 240).await?;
    let (logical_name_id, resource) = seed_bounded_name(
        &database,
        NAME,
        0xb0a_5000,
        "0x00000000000000000000000000000000000b0a05",
        bigname_storage::AddressNameRelation::EffectiveController,
        205,
    )
    .await?;
    let lease = Uuid::from_u128(0xb0a_5100);
    upsert_test_resources(
        &database.pool,
        &[address_name_resource(
            lease,
            None,
            "0xselected-lease-resource",
            78,
        )],
    )
    .await?;
    let namehash = bigname_lookup::ens_namehash_hex(NAME)?;
    let mut lease_grant = v2_history_event(
        "selected-lease-grant",
        None,
        Some(lease),
        "RegistrationGranted",
        241,
    );
    lease_grant.after_state["namehash"] = json!(&namehash);
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            v2_history_event(
                "selected-bound",
                Some(&logical_name_id),
                Some(resource),
                "AuthorityTransferred",
                205,
            ),
            v2_history_event(
                "selected-lease-transfer",
                None,
                Some(lease),
                "TokenControlTransferred",
                230,
            ),
            lease_grant,
        ],
    )
    .await?;
    sqlx::query(
        "UPDATE bigname_phase.name_current
         SET declared_summary = jsonb_set(
             declared_summary,
             '{registration}',
             COALESCE(declared_summary -> 'registration', '{}'::jsonb)
                 || jsonb_build_object('resource_id', $2::text)
         )
         WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .bind(lease.to_string())
    .execute(&database.pool)
    .await?;
    publish_bounded_membership_at(&database, 240).await?;

    assert_eq!(
        bigname_storage::load_bounded_registration_resource_ids(
            &database.pool,
            &logical_name_id,
            &bounded_at(240),
        )
        .await?,
        vec![resource],
    );
    for page_route in [
        format!("/v1/names/{NAME}/history?scope=registration&page_size=20"),
        format!("/v1/names/{NAME}/history?scope=both&page_size=20"),
    ] {
        let payload = v2_history_payload_for_database(&database, &page_route).await?;
        assert_eq!(
            bounded_route_hashes(&payload),
            vec!["0xtx205"],
            "{page_route}: {payload}"
        );
    }

    publish_bounded_membership_at(&database, 241).await?;
    let mut expected = vec![lease, resource];
    expected.sort_unstable();
    assert_eq!(
        bigname_storage::load_bounded_registration_resource_ids(
            &database.pool,
            &logical_name_id,
            &bounded_at(241),
        )
        .await?,
        expected,
    );
    let route = format!("/v1/names/{NAME}/history?scope=registration&page_size=1");
    let first = v2_history_payload_for_database(&database, &route).await?;
    assert_eq!(bounded_route_hashes(&first), vec!["0xtx241"], "{first}");
    let cursor = first["page"]["next_cursor"].as_str().expect("second page");
    let second =
        v2_history_payload_for_database(&database, &format!("{route}&cursor={cursor}")).await?;
    assert_eq!(bounded_route_hashes(&second), vec!["0xtx230"], "{second}");
    database.cleanup().await
}
