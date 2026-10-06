//! Address history keeps the acquisition event from the family relation fold. A controller
//! acquired after a pinned page's block cannot admit the resource's earlier history.
#[path = "families_support/mod.rs"]
mod support;
use anyhow::Result;
use bigname_project::families::{self, FamilyMode};
use bigname_storage::{AddressNameRelation, HistoryPageOptions, HistoryScope, HistorySummaryMode};
use serde_json::{Value, json};
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
    let catalogue_before = catalogue_facts(&fixture).await?;
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
    let catalogue_after = catalogue_facts(&fixture).await?;
    assert_ne!(
        catalogue_after, catalogue_before,
        "the later controller must change catalogue membership"
    );
    let anchor_before_images: Vec<Value> = sqlx::query_scalar(
        "SELECT before_image FROM project_family_undo WHERE chain_id=$1 AND block_number=3
         AND family='project_address_history_anchor' AND before_image IS NOT NULL",
    )
    .bind(CHAIN)
    .fetch_all(&fixture.pool)
    .await?;
    assert!(
        !anchor_before_images.is_empty(),
        "existing owner envelope changes in this block"
    );
    for image in anchor_before_images {
        assert!(
            catalogue_before[0].as_array().unwrap().contains(&image),
            "journal must retain the full pre-block row, not an intermediate envelope: {image}"
        );
    }
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
    assert_eq!(
        catalogue_facts(&fixture).await?,
        catalogue_before,
        "actual undo must restore masks, provenance, sources, edges and envelope bytes"
    );
    assert_catalogue_stamp(&fixture, 2).await?;
    assert!(
        bigname_storage::load_address_names_current(&fixture.pool, CONTROLLER, None, None)
            .await?
            .is_empty()
    );
    fixture.apply(3, FamilyMode::Normal).await?;
    assert_eq!(catalogue_facts(&fixture).await?, catalogue_after);
    assert_catalogue_stamp(&fixture, 3).await?;
    assert_eq!(history(&fixture, 3).await?, visible);
    fixture.assert_rebuild_equal(3).await?;
    assert_catalogue_stamp(&fixture, 3).await?;
    assert_eq!(history(&fixture, 3).await?, visible);
    fixture.cleanup().await
}

async fn catalogue_facts(fixture: &Fixture) -> Result<Vec<Value>> {
    let mut result = Vec::new();
    for table in [
        "project_address_history_anchor",
        "project_history_source",
        "project_history_source_edge",
    ] {
        result.push(sqlx::query_scalar(&format!(
            "SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY to_jsonb(row)::text),'[]'::jsonb) FROM {table} row",
        )).fetch_one(&fixture.pool).await?);
    }
    Ok(result)
}
async fn assert_catalogue_stamp(fixture: &Fixture, block: i64) -> Result<()> {
    let matches:bool=sqlx::query_scalar(
        "SELECT catalogue.block_number=$2 AND catalogue.block_number=family.current_block_number
           AND catalogue.block_hash=family.current_block_hash
           AND catalogue.publication_sequence=family.sequence
           AND catalogue.input_content_hash=family.input_content_hash AND catalogue.catalogue_version=1
         FROM project_history_catalogue_marker catalogue JOIN project_family_marker family USING(chain_id)
         WHERE catalogue.chain_id=$1",
    ).bind(CHAIN).bind(block).fetch_one(&fixture.pool).await?;
    assert!(
        matches,
        "catalogue and family publications must be the same generation"
    );
    Ok(())
}

async fn diagnostic_history(fixture: &Fixture) -> Result<Vec<String>> {
    let filter = bigname_storage::EventHistoryFilter {
        namespace: Some("ens".to_owned()),
        address: Some(bigname_storage::EventHistoryAddressFilter {
            address: CONTROLLER.to_owned(),
            relation: Some(AddressNameRelation::EffectiveController),
        }),
        ..Default::default()
    };
    Ok(bigname_storage::load_event_history_page(
        &fixture.pool,
        filter,
        true,
        None,
        50,
        HistorySummaryMode::Count,
        true,
    )
    .await?
    .rows
    .into_iter()
    .map(|row| row.event_identity)
    .collect())
}

#[tokio::test]
async fn raw_controller_audit_survives_revocation_and_family_reset() -> Result<()> {
    let fixture = Fixture::new("family_controller_audit", 5).await?;
    let name = format!("ens:0x{:064x}", 2);
    let resource = uuid(2);
    fixture
        .binding(&uuid(102), &name, &resource, "ens_v1", 0, 0, None)
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
    let granted = json!({"subject":CONTROLLER, "scope":{"kind":"resource"},
        "effective_powers":["resource_control"]});
    fixture
        .event(
            Event::new(
                "audit-control-grant",
                3,
                0,
                "PermissionChanged",
                "ens_v1_registrar_l1",
            )
            .name(&name)
            .resource(&resource)
            .after(granted.clone()),
        )
        .await?;
    fixture.apply(3, FamilyMode::Normal).await?;
    assert!(!history(&fixture, 3).await?.is_empty());
    let granted_catalogue = catalogue_facts(&fixture).await?;
    fixture
        .event(
            Event::new(
                "audit-control-revoke",
                4,
                0,
                "PermissionChanged",
                "ens_v1_registrar_l1",
            )
            .name(&name)
            .resource(&resource)
            .before(granted)
            .after(
                json!({"subject":CONTROLLER, "scope":{"kind":"resource"}, "effective_powers":[]}),
            ),
        )
        .await?;
    fixture.apply(4, FamilyMode::Normal).await?;
    let revoked_catalogue = catalogue_facts(&fixture).await?;
    assert_ne!(granted_catalogue, revoked_catalogue);
    let controller_current: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_address_history_anchor WHERE chain_id=$1
         AND address=$2 AND current_mask<>0",
    )
    .bind(CHAIN)
    .bind(CONTROLLER)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        controller_current, 0,
        "revocation removes every current address membership"
    );
    assert!(
        history(&fixture, 4).await?.is_empty(),
        "product history retains current-controller admission"
    );
    let retained = diagnostic_history(&fixture).await?;
    assert!(
        retained.contains(&"audit-control-grant".to_owned()),
        "{retained:?}"
    );
    assert!(
        retained.contains(&"audit-control-revoke".to_owned()),
        "{retained:?}"
    );
    assert!(
        retained.contains(&"RegistrationGranted:1:0".to_owned()),
        "{retained:?}"
    );

    families::undo_to(&fixture.pool, CHAIN, 3).await?;
    assert_eq!(catalogue_facts(&fixture).await?, granted_catalogue);
    assert_catalogue_stamp(&fixture, 3).await?;
    fixture.apply(4, FamilyMode::Normal).await?;
    assert_eq!(catalogue_facts(&fixture).await?, revoked_catalogue);
    assert_catalogue_stamp(&fixture, 4).await?;
    fixture.assert_rebuild_equal(4).await?;
    assert_eq!(catalogue_facts(&fixture).await?, revoked_catalogue);
    assert_catalogue_stamp(&fixture, 4).await?;

    let mut options = bigname_project::families::FamilyOptions::new(support::CONTENT_HASH);
    options.max_blocks_per_run = 0;
    fixture.apply_with(4, FamilyMode::Rebuild, &options).await?;
    assert!(fixture.rows("project_address_name_index").await?.is_empty());
    assert_eq!(
        diagnostic_history(&fixture).await?,
        retained,
        "raw evidence survives cleared families"
    );
    fixture.apply(4, FamilyMode::Normal).await?;
    assert_eq!(diagnostic_history(&fixture).await?, retained);
    assert!(history(&fixture, 4).await?.is_empty());
    fixture.cleanup().await
}
