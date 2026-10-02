//! Family address relations across NameWrapper grace and expiry boundaries, including
//! effective controllers, token holders and locked or lineageless names, and the composed
//! `control.owner` and grace flag they agree with.
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843-L856 @ ens_v1@91c966f)
#[path = "families_read_support/mod.rs"]
mod read_support;
#[path = "families_support/mod.rs"]
mod support;

use anyhow::{Context, Result};
use read_support::{
    publish,
    wrapper::{
        CANNOT_UNWRAP, GRACE_PERIOD, HOLDER, IS_DOT_ETH, PARENT_CANNOT_CONTROL, name, timestamp,
        wrapped,
    },
};
use support::{CHAIN, Fixture, hash, uuid};

/// Give `resource` a token lineage, which makes its token holder the name's owner.
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
/// window, the grant reads as a revoke, and only the token holder remains. The composed row
/// recomposes at grace entry with no new event, and its grace flag, which omits the served
/// `manager`, flips with the relation.
#[tokio::test]
async fn the_effective_controller_crosses_grace_entry() -> Result<()> {
    let fixture = Fixture::new("families_address_names_grace", 20).await?;
    let fuses = PARENT_CANNOT_CONTROL | IS_DOT_ETH;
    let resource = wrapped(&fixture, fuses, timestamp(14) + GRACE_PERIOD).await?;
    with_token_lineage(&fixture, &resource).await?;
    assert_eq!(
        publish_and_compare_addresses(&fixture, 14).await?,
        ["token_holder", "effective_controller"]
    );
    assert_eq!(composed(&fixture).await?, (Some(HOLDER.to_owned()), false));
    assert_eq!(
        publish_and_compare_addresses(&fixture, 15).await?,
        ["token_holder"]
    );
    assert_eq!(composed(&fixture).await?, (Some(HOLDER.to_owned()), true));
    fixture.cleanup().await
}

/// The token holder across the wrapper expiry: at block 14, the exact expiry second, the
/// emancipated name is still held (and inside its grace window, so no controller); at block 15
/// the state and the owner lapse and the holder has no relation left.
#[tokio::test]
async fn the_token_holder_crosses_the_expiry() -> Result<()> {
    let fixture = Fixture::new("families_address_names_expiry", 20).await?;
    let fuses = PARENT_CANNOT_CONTROL | IS_DOT_ETH;
    let resource = wrapped(&fixture, fuses, timestamp(14)).await?;
    with_token_lineage(&fixture, &resource).await?;
    assert_eq!(
        publish_and_compare_addresses(&fixture, 14).await?,
        ["token_holder"]
    );
    assert_eq!(
        publish_and_compare_addresses(&fixture, 15).await?,
        Vec::<String>::new()
    );
    fixture.cleanup().await
}

/// A locked name keeps its token holder in and out of the grace window and, like a wrapped or
/// emancipated one, is managed by it outside grace: NameWrapper's `canModifyName` has no
/// wrapper-state condition
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L214-L222 @ ens_v1@91c966f).
/// With no token lineage the owner is still the registrant the wrap names, while the effective
/// controller is the folded controller alone, which the holder's grant sets outside grace and the
/// masked grant leaves unset inside it.
#[tokio::test]
async fn a_locked_name_is_managed_by_its_holder_and_a_lineageless_name_by_its_controller()
-> Result<()> {
    let fixture = Fixture::new("families_address_names_locked", 20).await?;
    let fuses = PARENT_CANNOT_CONTROL | IS_DOT_ETH | CANNOT_UNWRAP;
    let resource = wrapped(&fixture, fuses, timestamp(14) + GRACE_PERIOD).await?;
    with_token_lineage(&fixture, &resource).await?;
    assert_eq!(
        publish_and_compare_addresses(&fixture, 14).await?,
        ["token_holder", "effective_controller"]
    );
    assert_eq!(
        publish_and_compare_addresses(&fixture, 15).await?,
        ["token_holder"]
    );
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
        ["token_holder", "effective_controller"]
    );
    assert_eq!(
        publish_and_compare_addresses(&fixture, 15).await?,
        ["token_holder"]
    );
    fixture.cleanup().await
}

/// A wrapped name whose wrapper expiry is unknown serves no wrapper state, so neither its grace
/// nor its manager is known, but it still has its token holder as `owner`: the relation lists it
/// as the field serves it.
#[tokio::test]
async fn an_unknown_wrapper_mask_keeps_the_token_holder() -> Result<()> {
    let fixture = Fixture::new("families_address_names_unknown_mask", 20).await?;
    let resource = wrapped(&fixture, PARENT_CANNOT_CONTROL | IS_DOT_ETH, timestamp(14)).await?;
    with_token_lineage(&fixture, &resource).await?;
    sqlx::query(
        "UPDATE normalized_events SET after_state = after_state - 'expiry'
         WHERE resource_id = $1::uuid",
    )
    .bind(&resource)
    .execute(&fixture.pool)
    .await?;
    assert_eq!(
        publish_and_compare_addresses(&fixture, 14).await?,
        ["token_holder"]
    );
    assert_eq!(composed(&fixture).await?, (Some(HOLDER.to_owned()), false));
    fixture.cleanup().await
}

/// The composed `control.owner` and `wrapper_in_grace` of the wrapped name; the stored
/// `project_name_summary.owner` the registry-label filters read must be the same owner.
async fn composed(fixture: &Fixture) -> Result<(Option<String>, bool)> {
    let row = bigname_storage::families::name::load_family_name(&fixture.pool, &name(1))
        .await?
        .context("composed wrapped name")?;
    let summary = &row.declared_summary;
    let owner = summary
        .pointer("/control/owner")
        .and_then(|owner| owner.as_str())
        .map(str::to_owned);
    let stored: Option<String> = sqlx::query_scalar(
        "SELECT owner FROM project_name_summary WHERE chain_id = $1 AND logical_name_id = $2",
    )
    .bind(CHAIN)
    .bind(name(1))
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(stored, owner);
    Ok((
        owner,
        summary.get("wrapper_in_grace") == Some(&serde_json::Value::Bool(true)),
    ))
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
