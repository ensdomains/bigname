//! Address history keeps the acquisition event from the family relation fold. A controller
//! acquired after a pinned page's block cannot admit the resource's earlier history.
#[path = "families_support/mod.rs"]
mod support;
use anyhow::Result;
use bigname_project::families::{self, FamilyMode};
use bigname_storage::{AddressNameRelation, HistoryPageOptions, HistoryScope, HistorySummaryMode};
use serde_json::json;
use support::{CHAIN, Event, Fixture, uuid};

const OWNER: &str = "0x00000000000000000000000000000000000000a1";
const CONTROLLER: &str = "0x00000000000000000000000000000000000000b1";
const REGISTRAR: &str = "0x00000000000000000000000000000000000000e3";

async fn history(fixture: &Fixture, through: i64) -> Result<Vec<String>> {
    let options = HistoryPageOptions {
        publication_block_bounds: Some([(CHAIN.to_owned(), through)].into()),
        ..Default::default()
    };
    Ok(bigname_storage::load_address_history_page_for_relations(
        &fixture.pool,
        CONTROLLER,
        Some("ens"),
        Some(&[AddressNameRelation::EffectiveController]),
        HistoryScope::Both,
        true,
        None,
        50,
        HistorySummaryMode::Count,
        &options,
        false,
    )
    .await?
    .rows
    .into_iter()
    .map(|row| row.event_identity)
    .collect())
}

#[tokio::test]
async fn controller_history_uses_its_actual_event_and_undo_restores_the_previous_relation()
-> Result<()> {
    let fixture = Fixture::new("family_address_history", 5).await?;
    let name = format!("ens:0x{:064x}", 1);
    let resource = uuid(1);
    fixture
        .binding(&uuid(101), &name, &resource, "ens_v1", 0, 0, None)
        .await?;
    fixture
        .write(
            1,
            0,
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            Some(&name),
            Some(&resource),
            json!({"authority_kind":"registrar", "status":"registered", "registrant":OWNER,
               "expiry":2_000_000_000u64}),
            REGISTRAR,
        )
        .await?;
    fixture.apply(2, FamilyMode::Normal).await?;
    let event_id = fixture
        .event(
            Event::new(
                "controller:3",
                3,
                0,
                "PermissionChanged",
                "ens_v1_registrar_l1",
            )
            .name(&name)
            .resource(&resource)
            .after(json!({"subject":CONTROLLER,
             "scope":{"kind":"resource"}, "effective_powers":["resource_control"]})),
        )
        .await?;
    fixture.apply(3, FamilyMode::Normal).await?;
    let rows =
        bigname_storage::load_address_names_current(&fixture.pool, CONTROLLER, Some("ens"), None)
            .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].provenance["normalized_event_id"], json!(event_id));
    assert_eq!(rows[0].chain_positions["block_number"], json!(3));
    assert!(
        history(&fixture, 2).await?.is_empty(),
        "a later controller cannot admit older events"
    );
    let visible = history(&fixture, 3).await?;
    assert!(visible.contains(&"controller:3".to_owned()), "{visible:?}");
    assert!(
        visible.contains(&"RegistrationGranted:1:0".to_owned()),
        "{visible:?}"
    );
    families::undo_to(&fixture.pool, CHAIN, 2).await?;
    assert!(
        bigname_storage::load_address_names_current(&fixture.pool, CONTROLLER, None, None)
            .await?
            .is_empty()
    );
    fixture.apply(3, FamilyMode::Normal).await?;
    assert_eq!(history(&fixture, 3).await?, visible);
    fixture.assert_rebuild_equal(3).await?;
    assert_eq!(history(&fixture, 3).await?, visible);
    fixture.cleanup().await
}
