// A name registered through the BaseRegistrar and wrapped later: the grant says `registrar`, and
// the wrap's `AuthorityEpochChanged` moves the name's authority to `wrapper`. Project serves that
// as `registration.authority_kind`, so the diagnostics authority route used to replace the name's
// control section with an unsupported one. A wrapped name has an owner, so the route now serves
// the control section Project built, with the NameWrapper as the registry owner.
// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L264-L268 @ ens_v1@91c966f)
#[tokio::test]
async fn a_name_wrapped_after_registration_serves_its_control_on_the_authority_route() -> Result<()>
{
    const NAME: &str = "wrapped-after-grant.eth";
    let database = TestDatabase::new_migrated().await?;
    seed_bounded_membership_blocks(&database, 240).await?;
    let (logical_name_id, resource) = seed_bounded_name(
        &database,
        NAME,
        0xb0a_6900,
        BOUNDED_ADDRESS,
        bigname_storage::AddressNameRelation::Registrant,
        205,
    )
    .await?;
    // Project writes the rows below from the events. Open the fixture's binding before the
    // events' timestamps so they fall inside it.
    sqlx::query("DELETE FROM bigname_phase.address_names_current WHERE logical_name_id = $1")
        .bind(&logical_name_id)
        .execute(&database.pool)
        .await?;
    sqlx::query(
        "UPDATE bigname_phase.surface_bindings SET active_from = '2023-11-14T00:00:00Z'
         WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .execute(&database.pool)
    .await?;
    let mut registered = v2_history_event(
        &format!("{NAME}-registered"),
        Some(&logical_name_id),
        Some(resource),
        "RegistrationGranted",
        205,
    );
    registered.after_state["registrant"] = json!(BOUNDED_ADDRESS);
    let mut wrapped = v2_history_event(
        &format!("{NAME}-wrapped"),
        Some(&logical_name_id),
        Some(resource),
        "AuthorityEpochChanged",
        206,
    );
    wrapped.source_family = "ens_v1_wrapper_l1".to_owned();
    wrapped.after_state = json!({
        "source_event": "NameWrapped",
        "authority_kind": "wrapper",
        "authority_key": format!("wrapper:{NAME}"),
        "owner": BOUNDED_ADDRESS,
    });
    let mut registry_owner = v2_history_event(
        &format!("{NAME}-registry-owner"),
        Some(&logical_name_id),
        Some(resource),
        "AuthorityTransferred",
        206,
    );
    registry_owner.source_family = "ens_v1_registry_l1".to_owned();
    registry_owner.log_index = Some(1);
    registry_owner.after_state = json!({
        "owner": WRAPPER_CONTRACT,
        "owner_getter": WRAPPER_CONTRACT,
    });
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[registered, wrapped, registry_owner],
    )
    .await?;
    bigname_project::Engine::new(database.pool.clone())
        .run_batch(bigname_project::BatchRequest {
            chain_id: BOUNDED_CHAIN.to_owned(),
            target_block: 240,
            affected_from_block: 200,
            affected_to_block: 240,
            resume_current: None,
            mode: bigname_project::RunMode::Normal,
        })
        .await?;
    publish_bounded_membership_at(&database, 240).await?;
    // The shape the removed API rule matched: the grant says registrar, the served authority kind
    // says wrapper.
    let (grant_kind, served_kind): (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT (SELECT after_state ->> 'authority_kind' FROM normalized_events
                 WHERE event_identity = $2),
                current.declared_summary #>> '{registration,authority_kind}'
         FROM bigname_phase.name_current current WHERE current.logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .bind(format!("{NAME}-registered"))
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(
        (grant_kind.as_deref(), served_kind.as_deref()),
        (Some("registrar"), Some("wrapper"))
    );

    let payload = v2_get_json(&database, &format!("/v1/diagnostics/names/{NAME}/authority")).await?;
    let control = &payload["data"]["control"];
    assert_ne!(control["status"], "unsupported", "{control:#}");
    assert!(control.get("unsupported_reason").is_none(), "{control:#}");
    assert_eq!(control["registrant"], BOUNDED_ADDRESS, "{control:#}");
    assert_eq!(control["registry_owner"], WRAPPER_CONTRACT, "{control:#}");
    database.cleanup().await?;
    Ok(())
}
