//! Wrapper shadow reads (TYR-36 step 3, docs/glossary.md "Shadow read"): the F2b wrapper row of
//! a wrapped name, masked at the publication's block clock (D6), gives the restriction block and
//! the wrapper holder's permission row. Each case publishes with the production batch, follows it
//! with the families, and compares every served name and resource with its family read at the
//! exact expiry and grace boundaries: a wrapper expiry equal to the block timestamp, one second
//! before and one after, with `IS_DOT_ETH` clear and set.
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L214-L238 @ ens_v1@91c966f)
#[path = "families_shadow_support/mod.rs"]
mod shadow_support;
#[path = "families_support/mod.rs"]
mod support;

use anyhow::Result;
use serde_json::{Value, json};
use shadow_support::{
    publish_and_compare,
    wrapper::{CANNOT_UNWRAP, GRACE_PERIOD, IS_DOT_ETH, PARENT_CANNOT_CONTROL, timestamp, wrapped},
};
use support::Fixture;

const TARGET: i64 = 16;

async fn restrictions(fixture: &Fixture, resource: &str) -> Result<Option<Value>> {
    Ok(sqlx::query_scalar(
        "SELECT resource_restrictions FROM permissions_current_resource_summary
         WHERE resource_id = $1::uuid",
    )
    .bind(resource)
    .fetch_optional(&fixture.pool)
    .await?
    .flatten())
}

/// One wrapped name at an expiry `offset` seconds from the target block's timestamp: every
/// served name and resource equals its family read, and the served restriction block is the one
/// the boundary calls for.
async fn boundary(fuses: i64, offset: i64) -> Result<Option<Value>> {
    let fixture = Fixture::new(
        &format!(
            "families_shadow_wrapper_{}_{}",
            fuses,
            offset.rem_euclid(1_000_000_000)
        ),
        20,
    )
    .await?;
    let resource = wrapped(&fixture, fuses, timestamp(TARGET) + offset).await?;
    let report = publish_and_compare(&fixture, TARGET).await?;
    shadow_support::assert_counts(&report, &[], &[]);
    assert!(report.names == 1 && report.resources >= 1, "{report:?}");
    let served = restrictions(&fixture, &resource).await?;
    fixture.cleanup().await?;
    Ok(served)
}

fn block(state: &str, fuses: i64, offset: i64) -> Option<Value> {
    Some(
        json!({"kind": "ens_v1_wrapper", "wrapper_state": state, "fuses": fuses,
                "expiry_seconds": timestamp(TARGET) + offset}),
    )
}

/// Past its expiry an emancipated name has no wrapper state and no restriction block; at the
/// exact expiry second it is still emancipated.
#[tokio::test]
async fn an_emancipated_wrapper_masks_past_its_exact_expiry() -> Result<()> {
    let fuses = PARENT_CANNOT_CONTROL;
    assert_eq!(boundary(fuses, -1).await?, None);
    for offset in [0, 1] {
        assert_eq!(
            boundary(fuses, offset).await?,
            block("emancipated", fuses, offset)
        );
    }
    Ok(())
}

/// A locked `.eth` wrapper: its NameWrapper expiry already includes the registrar grace period,
/// so the mask falls at the expiry itself and nothing changes across the grace length after it.
#[tokio::test]
async fn a_locked_dot_eth_wrapper_masks_at_its_expiry_not_its_grace() -> Result<()> {
    let fuses = PARENT_CANNOT_CONTROL | IS_DOT_ETH | CANNOT_UNWRAP;
    assert_eq!(boundary(fuses, -1).await?, None);
    for offset in [0, 1, GRACE_PERIOD - 1, GRACE_PERIOD, GRACE_PERIOD + 1] {
        assert_eq!(
            boundary(fuses, offset).await?,
            block("locked", fuses, offset)
        );
    }
    Ok(())
}

/// A name its parent still controls keeps its `wrapped` state on both sides of the expiry; only
/// its fuses fall to zero past it (they are zero here throughout).
#[tokio::test]
async fn a_wrapped_name_under_its_parent_keeps_its_state_past_expiry() -> Result<()> {
    for offset in [-1, 0, 1] {
        assert_eq!(boundary(0, offset).await?, block("wrapped", 0, offset));
    }
    Ok(())
}
