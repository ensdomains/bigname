// A relation the address held at the bound and lost after it. Project's current row for the relation is gone by then, so a read bound below the loss
// can admit the name's older events only if the historical event matcher reproduces the relation
// from the event that created it. The matcher knows three shapes: a `RegistrationGranted`
// registrant and a `TokenControlTransferred` recipient on a token-backed resource, and an
// `AuthorityTransferred` owner on a registry-only resource or an ENSv2 registry resource. Each
// test below loses one relation kind; the ignored ones are the known limitation listed under
// "Known limitation" in the history anchor section of docs/api-v2-routes.md: kinds the matcher
// does not reproduce, which stay on the current-row path bounded by the cited block.

const LOST_OTHER: &str = "0x00000000000000000000000000000000000b0aff";

/// Seed `name` with `relation` created by `created` at block 205 and a later name event at 210,
/// then lose the relation at 241: the address's current row is removed, as Project removes it
/// when it publishes 241. Returns the bounded read at 240, filtered to `filter` when given.
async fn lost_relation_history(
    name: &str,
    seed: u128,
    relation: bigname_storage::AddressNameRelation,
    filter: Option<bigname_storage::AddressNameRelation>,
    created: impl FnOnce(&str, Uuid) -> NormalizedEvent,
) -> Result<(Vec<String>, Vec<String>)> {
    let database = TestDatabase::new_migrated().await?;
    seed_bounded_membership_blocks(&database, 240).await?;
    let (logical_name_id, resource) =
        seed_bounded_name(&database, name, seed, BOUNDED_ADDRESS, relation, 205).await?;
    let mut later = v2_history_event(
        &format!("{name}-renewed"),
        Some(&logical_name_id),
        Some(resource),
        "RegistrationRenewed",
        210,
    );
    later.log_index = Some(1);
    let mut lost = v2_history_event(
        &format!("{name}-lost"),
        Some(&logical_name_id),
        Some(resource),
        "TokenControlTransferred",
        241,
    );
    lost.after_state = json!({ "to": LOST_OTHER });
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[created(&logical_name_id, resource), later, lost],
    )
    .await?;
    publish_bounded_membership_at(&database, 240).await?;
    let held = bounded_address_history_hashes(&database, filter, 240).await?;
    sqlx::query(
        "DELETE FROM bigname_phase.address_names_current
         WHERE address = $1 AND logical_name_id = $2",
    )
    .bind(BOUNDED_ADDRESS)
    .bind(&logical_name_id)
    .execute(&database.pool)
    .await?;
    let lost = bounded_address_history_hashes(&database, filter, 240).await?;
    database.cleanup().await?;
    Ok((held, lost))
}

fn assert_relation_survives_loss(held: &[String], lost: &[String]) {
    assert_eq!(
        held,
        ["0xtx210", "0xtx205"],
        "the relation held at 240 admits the name"
    );
    assert_eq!(
        lost, held,
        "losing the relation at 241 changed what a read bound at 240 admits"
    );
}

fn relation_event(
    kind: &str,
    after_state: Value,
    derivation_kind: &str,
) -> impl FnOnce(&str, Uuid) -> NormalizedEvent {
    let kind = kind.to_owned();
    let derivation_kind = derivation_kind.to_owned();
    move |logical_name_id: &str, resource: Uuid| {
        let mut event = v2_history_event(
            &format!("{logical_name_id}-{kind}"),
            Some(logical_name_id),
            Some(resource),
            &kind,
            205,
        );
        event.after_state = after_state;
        event.derivation_kind = derivation_kind;
        event
    }
}

const V1_DERIVATION: &str = "ens_v1_unwrapped_authority";
const V2_DERIVATION: &str = "ens_v2_registry_resource_surface";

#[tokio::test]
async fn lost_registrant_from_a_grant_keeps_its_bounded_history() -> Result<()> {
    let (held, lost) = lost_relation_history(
        "lost-grant-registrant.eth",
        0xb0a_6000,
        bigname_storage::AddressNameRelation::Registrant,
        Some(bigname_storage::AddressNameRelation::Registrant),
        relation_event(
            "RegistrationGranted",
            json!({"authority_kind": "registrar", "registrant": BOUNDED_ADDRESS}),
            V1_DERIVATION,
        ),
    )
    .await?;
    assert_relation_survives_loss(&held, &lost);
    Ok(())
}

#[tokio::test]
async fn lost_token_holder_from_a_transfer_keeps_its_bounded_history() -> Result<()> {
    let (held, lost) = lost_relation_history(
        "lost-transfer-holder.eth",
        0xb0a_6100,
        bigname_storage::AddressNameRelation::TokenHolder,
        Some(bigname_storage::AddressNameRelation::TokenHolder),
        relation_event(
            "TokenControlTransferred",
            json!({"to": BOUNDED_ADDRESS}),
            V1_DERIVATION,
        ),
    )
    .await?;
    assert_relation_survives_loss(&held, &lost);
    Ok(())
}

#[tokio::test]
async fn lost_ens_v2_controller_from_an_authority_transfer_keeps_its_bounded_history() -> Result<()>
{
    let (held, lost) = lost_relation_history(
        "lost-v2-controller.eth",
        0xb0a_6200,
        bigname_storage::AddressNameRelation::EffectiveController,
        Some(bigname_storage::AddressNameRelation::EffectiveController),
        relation_event(
            "AuthorityTransferred",
            json!({"owner": BOUNDED_ADDRESS}),
            V2_DERIVATION,
        ),
    )
    .await?;
    assert_relation_survives_loss(&held, &lost);
    Ok(())
}

#[tokio::test]
#[ignore = "docs/api-v2-routes.md history known limitation: an ENSv1 .eth registry controller \
            from AuthorityTransferred on a token-backed resource is not reproduced"]
async fn lost_ens_v1_controller_of_a_token_backed_name_keeps_its_bounded_history() -> Result<()> {
    let (held, lost) = lost_relation_history(
        "lost-v1-token-controller.eth",
        0xb0a_6300,
        bigname_storage::AddressNameRelation::EffectiveController,
        Some(bigname_storage::AddressNameRelation::EffectiveController),
        relation_event(
            "AuthorityTransferred",
            json!({"owner": BOUNDED_ADDRESS}),
            V1_DERIVATION,
        ),
    )
    .await?;
    assert_relation_survives_loss(&held, &lost);
    Ok(())
}

#[tokio::test]
#[ignore = "docs/api-v2-routes.md history known limitation: a token holder whose only evidence \
            is the grant is not reproduced"]
async fn lost_token_holder_from_a_grant_keeps_its_bounded_history() -> Result<()> {
    let (held, lost) = lost_relation_history(
        "lost-grant-holder.eth",
        0xb0a_6400,
        bigname_storage::AddressNameRelation::TokenHolder,
        Some(bigname_storage::AddressNameRelation::TokenHolder),
        relation_event(
            "RegistrationGranted",
            json!({"authority_kind": "registrar", "registrant": BOUNDED_ADDRESS}),
            V1_DERIVATION,
        ),
    )
    .await?;
    assert_relation_survives_loss(&held, &lost);
    Ok(())
}

#[tokio::test]
#[ignore = "docs/api-v2-routes.md history known limitation: an effective controller that fell \
            back to the token holder is not reproduced"]
async fn lost_controller_from_the_token_holder_fallback_keeps_its_bounded_history() -> Result<()> {
    let (held, lost) = lost_relation_history(
        "lost-fallback-controller.eth",
        0xb0a_6600,
        bigname_storage::AddressNameRelation::EffectiveController,
        Some(bigname_storage::AddressNameRelation::EffectiveController),
        relation_event(
            "TokenControlTransferred",
            json!({"to": BOUNDED_ADDRESS}),
            V1_DERIVATION,
        ),
    )
    .await?;
    assert_relation_survives_loss(&held, &lost);
    Ok(())
}

#[tokio::test]
#[ignore = "docs/api-v2-routes.md history known limitation: an ENSv2 PermissionChanged \
            controller is not reproduced"]
async fn lost_controller_from_a_permission_change_keeps_its_bounded_history() -> Result<()> {
    let (held, lost) = lost_relation_history(
        "lost-permission-controller.eth",
        0xb0a_6700,
        bigname_storage::AddressNameRelation::EffectiveController,
        Some(bigname_storage::AddressNameRelation::EffectiveController),
        relation_event(
            "PermissionChanged",
            json!({"subject": BOUNDED_ADDRESS, "resource_control": true}),
            V2_DERIVATION,
        ),
    )
    .await?;
    assert_relation_survives_loss(&held, &lost);
    Ok(())
}
