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
    let (logical_name_id, resource) =
        seed_family_name(&database, NAME, 0xb0a_6900, "ens_v1").await?;
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
        "node": logical_name_id.trim_start_matches("ens:"),
        "owner": WRAPPER_CONTRACT,
        "owner_getter": WRAPPER_CONTRACT,
    });
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[registered, wrapped, registry_owner],
    )
    .await?;
    publish_test_families(&database, 240).await?;
    // The shape the removed API rule matched: the grant says registrar, the served authority kind
    // says wrapper.
    let grant_kind: Option<String> = sqlx::query_scalar(
        "SELECT after_state ->> 'authority_kind' FROM normalized_events WHERE event_identity = $1",
    )
    .bind(format!("{NAME}-registered"))
    .fetch_one(&database.pool)
    .await?;
    let composed =
        bigname_storage::families::name::load_family_name(&database.pool, &logical_name_id)
            .await?
            .context("wrapped family name")?;
    let served_kind = composed
        .declared_summary
        .pointer("/registration/authority_kind")
        .and_then(Value::as_str);
    assert_eq!(
        (grant_kind.as_deref(), served_kind),
        (Some("registrar"), Some("wrapper"))
    );

    let payload = v2_get_json(
        &database,
        &format!("/v1/diagnostics/names/{NAME}/authority"),
    )
    .await?;
    let control = &payload["data"]["control"];
    assert_ne!(control["status"], "unsupported", "{control:#}");
    assert!(control.get("unsupported_reason").is_none(), "{control:#}");
    assert_eq!(control["registrant"], BOUNDED_ADDRESS, "{control:#}");
    assert_eq!(control["registry_owner"], WRAPPER_CONTRACT, "{control:#}");
    database.cleanup().await?;
    Ok(())
}
