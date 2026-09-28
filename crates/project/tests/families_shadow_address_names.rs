//! F13 address relations across the NameWrapper masks (TYR-36 step 7b slice 3): one unchanged
//! wrapper row read at two successive publications, with the mask crossing between them, the
//! pattern of `families_shadow_wrapper.rs`. The served `address_names_current` relations of the
//! holder change across the crossing, and at each publication every address's names page, read
//! through the served reader and through the F13 reader over the composed names
//! (`bigname_storage::families::records::load_family_address_names_page`, compared in
//! `compare_family_reads`), is the same in every served sort, dedupe mode and filter.
//!
//! - the effective controller: a `resource_control` PermissionChanged sets it only while the name
//!   is wrapped or emancipated and out of the `.eth` grace window (address_names.rs:228-252);
//! - the token holder and the registrant: past the wrapper expiry an emancipated name has no
//!   wrapper state and no owner (address_names.rs:425-439, name_current/build.sql:618-641).
//!
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843-L856 @ ens_v1@91c966f)
#[path = "families_shadow_support/mod.rs"]
mod shadow_support;
#[path = "families_support/mod.rs"]
mod support;

use anyhow::{Result, ensure};
use shadow_support::{
    publish_and_compare,
    wrapper::{
        CANNOT_UNWRAP, GRACE_PERIOD, HOLDER, IS_DOT_ETH, PARENT_CANNOT_CONTROL, timestamp, wrapped,
    },
};
use support::{CHAIN, Fixture, hash, uuid};

/// Give `resource` a token lineage, which the registrant and token holder relations require.
async fn with_token_lineage(fixture: &Fixture, resource: &str) -> Result<()> {
    let lineage = uuid(900);
    sqlx::query(
        "INSERT INTO token_lineages (token_lineage_id, chain_id, block_hash, block_number,
             provenance, canonicality_state)
         VALUES ($1::uuid, $2, $3, 0, '{}'::jsonb, 'canonical')",
    )
    .bind(&lineage)
    .bind(CHAIN)
    .bind(hash(0))
    .execute(&fixture.pool)
    .await?;
    sqlx::query("UPDATE resources SET token_lineage_id = $1::uuid WHERE resource_id = $2::uuid")
        .bind(&lineage)
        .bind(resource)
        .execute(&fixture.pool)
        .await?;
    Ok(())
}

/// The holder's served relations, in rank order.
async fn served_relations(fixture: &Fixture) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar(
        "SELECT relation::text FROM address_names_current WHERE address = $1
         ORDER BY CASE relation::text WHEN 'registrant' THEN 0 WHEN 'token_holder' THEN 1
                  ELSE 2 END",
    )
    .bind(HOLDER)
    .fetch_all(&fixture.pool)
    .await?)
}

/// Publish `target`, compare every name, then every address page with the F13 reader.
async fn publish_and_compare_addresses(fixture: &Fixture, target: i64) -> Result<Vec<String>> {
    let names = publish_and_compare(fixture, target).await?;
    shadow_support::assert_counts(&names, &[], &[]);
    let report = bigname_storage::families::records::compare_family_reads(
        &fixture.pool,
        CHAIN,
        Some((target, hash(target))),
        1,
    )
    .await?;
    eprintln!(
        "F13_SHADOW target={target} address_name_addresses={} address_name_pages={} \
         differences={:#?}",
        report.address_name_addresses, report.address_name_pages, report.differences
    );
    ensure!(report.current(), "the families did not follow {target}");
    ensure!(
        report.differences.is_empty(),
        "the family reads differ at {target}: {:#?}",
        report.differences
    );
    ensure!(
        report.listing_excused == 0,
        "nothing is excused in a fixture: {report:?}"
    );
    served_relations(fixture).await
}

/// The effective controller across grace entry: the wrapper expiry is one grace period after
/// block 14's timestamp, so block 14 sits at the strict grace bound and the holder's
/// `resource_control` grant makes it the controller; at block 15 the name is inside the grace
/// window, the grant reads as a revoke, and only the registrant and token holder remain.
#[tokio::test]
async fn the_effective_controller_crosses_grace_entry() -> Result<()> {
    let fixture = Fixture::new("families_shadow_address_names_grace", 20).await?;
    let fuses = PARENT_CANNOT_CONTROL | IS_DOT_ETH;
    let resource = wrapped(&fixture, fuses, timestamp(14) + GRACE_PERIOD).await?;
    with_token_lineage(&fixture, &resource).await?;
    assert_eq!(
        publish_and_compare_addresses(&fixture, 14).await?,
        ["registrant", "token_holder", "effective_controller"]
    );
    assert_eq!(
        publish_and_compare_addresses(&fixture, 15).await?,
        ["registrant", "token_holder"]
    );
    fixture.cleanup().await
}

/// The token holder and the registrant across the wrapper expiry: at block 14, the exact expiry
/// second, the emancipated name is still held (and inside its grace window, so no controller);
/// at block 15 the state and the owner lapse and the holder has no relation left.
#[tokio::test]
async fn the_token_holder_and_registrant_cross_the_expiry() -> Result<()> {
    let fixture = Fixture::new("families_shadow_address_names_expiry", 20).await?;
    let fuses = PARENT_CANNOT_CONTROL | IS_DOT_ETH;
    let resource = wrapped(&fixture, fuses, timestamp(14)).await?;
    with_token_lineage(&fixture, &resource).await?;
    assert_eq!(
        publish_and_compare_addresses(&fixture, 14).await?,
        ["registrant", "token_holder"]
    );
    assert_eq!(
        publish_and_compare_addresses(&fixture, 15).await?,
        Vec::<String>::new()
    );
    fixture.cleanup().await
}

/// A locked name keeps its token holder in and out of the grace window, and never has a
/// controller from a permission grant (locked is not wrapped or emancipated); with no token
/// lineage the effective controller is the folded controller alone, which the masked grant
/// leaves unset, so the name has no relation at all.
#[tokio::test]
async fn a_locked_name_keeps_its_holder_and_a_lineageless_name_has_none() -> Result<()> {
    let fixture = Fixture::new("families_shadow_address_names_locked", 20).await?;
    let fuses = PARENT_CANNOT_CONTROL | IS_DOT_ETH | CANNOT_UNWRAP;
    let resource = wrapped(&fixture, fuses, timestamp(14) + GRACE_PERIOD).await?;
    with_token_lineage(&fixture, &resource).await?;
    for target in [14, 15] {
        assert_eq!(
            publish_and_compare_addresses(&fixture, target).await?,
            ["registrant", "token_holder"],
            "block {target}"
        );
    }
    fixture.cleanup().await?;

    let fixture = Fixture::new("families_shadow_address_names_no_lineage", 20).await?;
    wrapped(
        &fixture,
        PARENT_CANNOT_CONTROL | IS_DOT_ETH,
        timestamp(14) + GRACE_PERIOD,
    )
    .await?;
    assert_eq!(
        publish_and_compare_addresses(&fixture, 14).await?,
        ["effective_controller"]
    );
    assert_eq!(
        publish_and_compare_addresses(&fixture, 15).await?,
        Vec::<String>::new()
    );
    fixture.cleanup().await
}
