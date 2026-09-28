//! Family address relations across NameWrapper grace and expiry boundaries, including
//! effective controllers, token holders, registrants and locked or lineageless names.
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843-L856 @ ens_v1@91c966f)
#[path = "families_read_support/mod.rs"]
mod read_support;
#[path = "families_support/mod.rs"]
mod support;

use anyhow::Result;
use read_support::{
    publish,
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

/// The effective controller across grace entry: the wrapper expiry is one grace period after
/// block 14's timestamp, so block 14 sits at the strict grace bound and the holder's
/// `resource_control` grant makes it the controller; at block 15 the name is inside the grace
/// window, the grant reads as a revoke, and only the registrant and token holder remain.
#[tokio::test]
async fn the_effective_controller_crosses_grace_entry() -> Result<()> {
    let fixture = Fixture::new("families_address_names_grace", 20).await?;
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
    let fixture = Fixture::new("families_address_names_expiry", 20).await?;
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
    let fixture = Fixture::new("families_address_names_locked", 20).await?;
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

    let fixture = Fixture::new("families_address_names_no_lineage", 20).await?;
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

async fn publish_and_compare_addresses(fixture: &Fixture, target: i64) -> Result<Vec<String>> {
    publish(fixture, target).await?;
    Ok(
        bigname_storage::load_address_names_current(&fixture.pool, HOLDER, None, None)
            .await?
            .into_iter()
            .map(|row| row.relation.as_str().to_owned())
            .collect(),
    )
}
