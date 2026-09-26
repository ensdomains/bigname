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
    wrapper::{
        CANNOT_UNWRAP, GRACE_PERIOD, HOLDER, HOLDER_POWERS, IS_DOT_ETH, OPERATOR,
        PARENT_CANNOT_CONTROL, name, timestamp, wrapped,
    },
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

/// `(subject, relation, powers)` per served resource-scoped row of `resource`.
async fn rows(fixture: &Fixture, resource: &str) -> Result<Vec<(String, String, Value)>> {
    Ok(sqlx::query_as(
        "SELECT subject, grant_source ->> 'relation_kind', effective_powers
         FROM permissions_current WHERE resource_id = $1::uuid ORDER BY subject, 2",
    )
    .bind(resource)
    .fetch_all(&fixture.pool)
    .await?)
}

async fn registrant(fixture: &Fixture) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT declared_summary -> 'control' -> 'registrant' FROM name_current
         WHERE logical_name_id = $1",
    )
    .bind(name(1))
    .fetch_one(&fixture.pool)
    .await?)
}

/// Every holder power but `extend_expiry`: an emancipated name without CAN_EXTEND_EXPIRY.
fn emancipated_powers() -> Value {
    json!(
        HOLDER_POWERS
            .iter()
            .filter(|power| **power != "extend_expiry")
            .collect::<Vec<_>>()
    )
}

/// Item 7 of the TYR-36 step 3 review (Q4): one unchanged wrapper row read at two successive
/// publications with no event between them. With the wrapper expiry at block 14's timestamp,
/// block 14 still serves the emancipated state, the holder and operator rows and the
/// registrant; block 15 is past the expiry, so the state and owner lapse, the fuses fall to
/// zero, and no row, restriction block or control registrant is served. Every name and
/// resource equals its family read at both blocks.
#[tokio::test]
async fn one_wrapper_row_read_at_two_publications_across_its_expiry() -> Result<()> {
    let fixture = Fixture::new("families_shadow_wrapper_two_publications_expiry", 20).await?;
    let fuses = PARENT_CANNOT_CONTROL | IS_DOT_ETH;
    let resource = wrapped(&fixture, fuses, timestamp(14)).await?;
    let before = publish_and_compare(&fixture, 14).await?;
    shadow_support::assert_counts(&before, &[], &[]);
    assert_eq!(
        restrictions(&fixture, &resource).await?,
        Some(
            json!({"kind": "ens_v1_wrapper", "wrapper_state": "emancipated", "fuses": fuses,
                    "expiry_seconds": timestamp(14)})
        )
    );
    assert_eq!(
        rows(&fixture, &resource).await?,
        vec![
            (HOLDER.to_owned(), "holder".to_owned(), json!(["approve"])),
            (
                OPERATOR.to_owned(),
                "operator".to_owned(),
                json!(["approve"])
            ),
        ],
        "at the exact expiry the name is inside the grace window, so only approval is served"
    );
    assert_eq!(registrant(&fixture).await?, json!(HOLDER));
    let after = publish_and_compare(&fixture, 15).await?;
    shadow_support::assert_counts(&after, &[], &[]);
    assert_eq!(restrictions(&fixture, &resource).await?, None);
    assert_eq!(rows(&fixture, &resource).await?, vec![]);
    assert_eq!(registrant(&fixture).await?, Value::Null);
    fixture.cleanup().await
}

/// Item 7 of the TYR-36 step 3 review (Q4): the same unchanged row across grace entry. The
/// wrapper expiry is one grace period after block 14's timestamp, so block 14 sits exactly at
/// the grace bound, which is strict, and serves every emancipated power; block 15 is inside
/// the grace window and serves approval only, to the holder and to the operator the fan-out
/// copies it to. Every name and resource equals its family read at both blocks.
#[tokio::test]
async fn one_wrapper_row_read_at_two_publications_across_grace_entry() -> Result<()> {
    let fixture = Fixture::new("families_shadow_wrapper_two_publications_grace", 20).await?;
    let fuses = PARENT_CANNOT_CONTROL | IS_DOT_ETH;
    let expiry = timestamp(14) + GRACE_PERIOD;
    let resource = wrapped(&fixture, fuses, expiry).await?;
    let before = publish_and_compare(&fixture, 14).await?;
    shadow_support::assert_counts(&before, &[], &[]);
    assert_eq!(
        rows(&fixture, &resource).await?,
        vec![
            (HOLDER.to_owned(), "holder".to_owned(), emancipated_powers()),
            (
                OPERATOR.to_owned(),
                "operator".to_owned(),
                emancipated_powers()
            ),
        ]
    );
    let after = publish_and_compare(&fixture, 15).await?;
    shadow_support::assert_counts(&after, &[], &[]);
    assert_eq!(
        rows(&fixture, &resource).await?,
        vec![
            (HOLDER.to_owned(), "holder".to_owned(), json!(["approve"])),
            (
                OPERATOR.to_owned(),
                "operator".to_owned(),
                json!(["approve"])
            ),
        ]
    );
    assert_eq!(
        restrictions(&fixture, &resource).await?,
        Some(
            json!({"kind": "ens_v1_wrapper", "wrapper_state": "emancipated", "fuses": fuses,
                    "expiry_seconds": expiry})
        )
    );
    assert_eq!(registrant(&fixture).await?, json!(HOLDER));
    fixture.cleanup().await
}
