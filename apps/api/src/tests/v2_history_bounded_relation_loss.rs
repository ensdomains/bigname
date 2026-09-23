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

// A relation the address still holds, whose cited event moves above the bound without a change
// of holder. A token transfer from the holder to itself (a registrar `Transfer(A, A)`) is a valid
// upstream event that the adapters keep, and Project cites the latest registration event for the
// registrant, token holder and fallback controller rows, so the current row now cites a block
// above the bound. The holder at the bound is unchanged: a read bound at 240 must admit the same
// rows, report the same count and continue a cursor it issued before the self-transfer.
#[tokio::test]
async fn self_transfer_above_the_bound_keeps_the_held_token_holder() -> Result<()> {
    const NAME: &str = "self-transfer-holder.eth";
    let database = TestDatabase::new_migrated().await?;
    seed_bounded_membership_blocks(&database, 240).await?;
    let (logical_name_id, resource) = seed_bounded_name(
        &database,
        NAME,
        0xb0a_6800,
        BOUNDED_ADDRESS,
        bigname_storage::AddressNameRelation::TokenHolder,
        205,
    )
    .await?;
    let mut grant = v2_history_event(
        "self-transfer-grant",
        Some(&logical_name_id),
        Some(resource),
        "RegistrationGranted",
        205,
    );
    grant.after_state["registrant"] = json!(BOUNDED_ADDRESS);
    let mut renewed = v2_history_event(
        "self-transfer-renewed",
        Some(&logical_name_id),
        Some(resource),
        "RegistrationRenewed",
        210,
    );
    renewed.log_index = Some(1);
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[grant, renewed]).await?;
    cite_holder_event(&database, &logical_name_id, "self-transfer-grant").await?;
    publish_bounded_membership_at(&database, 240).await?;

    let route =
        format!("/v1/addresses/{BOUNDED_ADDRESS}/history?relation=owner&page_size=1&include=total_count");
    let first = v2_history_payload_for_database(&database, &route).await?;
    assert_eq!(bounded_route_hashes(&first), vec!["0xtx210"], "{first}");
    assert_eq!(first["page"]["total_count"], json!(2), "{first}");
    let cursor = first["page"]["next_cursor"]
        .as_str()
        .expect("second page")
        .to_owned();
    let held = bounded_address_history_hashes(
        &database,
        Some(bigname_storage::AddressNameRelation::TokenHolder),
        240,
    )
    .await?;
    assert_eq!(held, ["0xtx210", "0xtx205"]);

    // The holder transfers the token to itself at 241, and Project republishes the row citing
    // that transfer. The publication stays at 240.
    let mut self_transfer = v2_history_event(
        "self-transfer-241",
        Some(&logical_name_id),
        Some(resource),
        "TokenControlTransferred",
        241,
    );
    self_transfer.before_state = json!({ "from": BOUNDED_ADDRESS });
    self_transfer.after_state = json!({ "source_event": "Transfer", "to": BOUNDED_ADDRESS });
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[self_transfer]).await?;
    cite_holder_event(&database, &logical_name_id, "self-transfer-241").await?;

    let again = bounded_address_history_hashes(
        &database,
        Some(bigname_storage::AddressNameRelation::TokenHolder),
        240,
    )
    .await?;
    assert_eq!(
        again, held,
        "a self-transfer above the bound dropped the name the address held at 240"
    );
    let repeated = v2_history_payload_for_database(&database, &route).await?;
    assert_eq!(
        bounded_route_hashes(&repeated),
        bounded_route_hashes(&first),
        "{repeated}"
    );
    assert_eq!(repeated["page"]["total_count"], json!(2), "{repeated}");
    let (status, continued) =
        bounded_route_status(&database, &format!("{route}&cursor={cursor}")).await?;
    assert_eq!(status, StatusCode::OK, "continuation: {continued}");
    assert_eq!(bounded_route_hashes(&continued), vec!["0xtx205"], "{continued}");
    database.cleanup().await
}

/// Point the address's current row for `logical_name_id` at the event Project cites for it, the
/// event `event_identity`, as `provenance.normalized_event_id` and `chain_positions.block_number`.
async fn cite_holder_event(
    database: &TestDatabase,
    logical_name_id: &str,
    event_identity: &str,
) -> Result<()> {
    let updated = sqlx::query(
        "UPDATE bigname_phase.address_names_current anc
         SET provenance = anc.provenance
                 || jsonb_build_object('normalized_event_id', event.normalized_event_id),
             chain_positions = jsonb_set(
                 anc.chain_positions, '{block_number}', to_jsonb(event.block_number)
             )
         FROM normalized_events event
         WHERE anc.address = $1 AND anc.logical_name_id = $2 AND event.event_identity = $3",
    )
    .bind(BOUNDED_ADDRESS)
    .bind(logical_name_id)
    .bind(event_identity)
    .execute(&database.pool)
    .await?;
    assert_eq!(updated.rows_affected(), 1, "one current row cites {event_identity}");
    Ok(())
}

// The negative side of the rule above: a token-holder row cited above the bound stays out of a
// read bound at 240 when the address acquired the token after the bound, even when the cited event
// is a self-transfer, because a transfer to the address lies between the bound and it.
#[tokio::test]
async fn self_transfer_after_a_later_acquisition_admits_no_older_events() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bounded_membership_blocks(&database, 240).await?;
    let mut expected_absent = Vec::new();
    for (name, seed, acquired_by) in [
        ("acquired-then-self.eth", 0xb0a_6900_u128, "transfer"),
        ("acquired-by-transfer.eth", 0xb0a_6a00_u128, "cited"),
    ] {
        let (logical_name_id, resource) = seed_bounded_name(
            &database,
            name,
            seed,
            BOUNDED_ADDRESS,
            bigname_storage::AddressNameRelation::TokenHolder,
            205,
        )
        .await?;
        // The name's history at the bound names another holder.
        let mut grant = v2_history_event(
            &format!("{name}-grant"),
            Some(&logical_name_id),
            Some(resource),
            "RegistrationGranted",
            205,
        );
        grant.after_state["registrant"] = json!(LOST_OTHER);
        grant.transaction_hash = Some(format!("0x{name}-205"));
        let mut acquired = v2_history_event(
            &format!("{name}-acquired"),
            Some(&logical_name_id),
            Some(resource),
            "TokenControlTransferred",
            241,
        );
        acquired.before_state = json!({ "from": LOST_OTHER });
        acquired.after_state = json!({ "source_event": "Transfer", "to": BOUNDED_ADDRESS });
        let mut events = vec![grant, acquired];
        let cited = if acquired_by == "transfer" {
            let mut self_transfer = v2_history_event(
                &format!("{name}-self"),
                Some(&logical_name_id),
                Some(resource),
                "TokenControlTransferred",
                241,
            );
            self_transfer.log_index = Some(1);
            self_transfer.before_state = json!({ "from": BOUNDED_ADDRESS });
            self_transfer.after_state =
                json!({ "source_event": "Transfer", "to": BOUNDED_ADDRESS });
            events.push(self_transfer);
            format!("{name}-self")
        } else {
            format!("{name}-acquired")
        };
        bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
        cite_holder_event(&database, &logical_name_id, &cited).await?;
        expected_absent.push(format!("0x{name}-205"));
    }
    publish_bounded_membership_at(&database, 240).await?;
    let hashes = bounded_address_history_hashes(
        &database,
        Some(bigname_storage::AddressNameRelation::TokenHolder),
        240,
    )
    .await?;
    assert!(
        !hashes.iter().any(|hash| expected_absent.contains(hash)),
        "a holder acquired above the bound admitted older events: {hashes:?}"
    );
    database.cleanup().await
}
