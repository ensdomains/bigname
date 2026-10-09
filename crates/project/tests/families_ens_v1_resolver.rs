//! F4 (`project_registry_pointer`) and the composed name row's `ens_v1_resolver`: the ENSv1
//! registry's resolver pointer for the name's node, `{chain_id, address}`, or null for a zero
//! pointer, a pointer event without a resolver, or a node with no pointer event.
mod families_support;

use anyhow::Result;
use bigname_project::families::{self, FamilyMode};
use families_support::{CHAIN, Event, Fixture};
use serde_json::{Value, json};

const R1: &str = "0x00000000000000000000000000000000000000a1";
const R2: &str = "0x00000000000000000000000000000000000000a2";
const ZERO: &str = "0x0000000000000000000000000000000000000000";

fn name(number: u64) -> String {
    format!("ens:0x{number:064x}")
}

/// The stored resolver of the node of name `number`, none without an F4 row.
async fn f4(fixture: &Fixture, number: u64) -> Result<Option<String>> {
    Ok(sqlx::query_scalar(
        "SELECT resolver_address FROM project_registry_pointer
         WHERE chain_id = $1 AND namespace = 'ens' AND node = $2",
    )
    .bind(CHAIN)
    .bind(format!("0x{number:064x}"))
    .fetch_optional(&fixture.pool)
    .await?)
}

/// The `ens_v1_resolver` of name `number`'s stored lookup row (`project_lookup_name.core`, the
/// composed row the lookup route serves), none when the key is absent.
async fn composed(fixture: &Fixture, number: u64) -> Result<Option<Value>> {
    let core: Option<Value> = sqlx::query_scalar(
        "SELECT core FROM project_lookup_name WHERE chain_id = $1 AND logical_name_id = $2",
    )
    .bind(CHAIN)
    .bind(name(number))
    .fetch_optional(&fixture.pool)
    .await?
    .flatten();
    let core = core.ok_or_else(|| anyhow::anyhow!("{} has no stored lookup row", name(number)))?;
    Ok(core["declared_summary"].get("ens_v1_resolver").cloned())
}

fn served(address: &str) -> Option<Value> {
    Some(json!({"chain_id": CHAIN, "address": address}))
}

/// A registry `NewResolver` for name `number`'s node, without a resolver when `resolver` is none.
async fn pointer_event(
    fixture: &Fixture,
    block: i64,
    log: i64,
    number: u64,
    resolver: Option<&str>,
) -> Result<()> {
    let node = format!("0x{number:064x}");
    let name = name(number);
    fixture.surface(&name, &node).await?;
    let mut after = json!({"source_event": "NewResolver", "node": node});
    if let Some(resolver) = resolver {
        after["resolver"] = json!(resolver);
    }
    let identity = format!("pointer-{block}-{log}");
    fixture
        .event(
            Event::new(
                &identity,
                block,
                log,
                "ResolverChanged",
                "ens_v1_registry_l1",
            )
            .name(&name)
            .after(after),
        )
        .await?;
    Ok(())
}

/// The unwrapped migration clears the pointer with `setRecord(node, GRAVEYARD, 0, 0)`
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/migration/UnlockedMigrationController.sol:L112-L117 @ ens_v2_sepolia_20261001@07e55a05),
/// which emits `NewResolver` only when the stored resolver changes
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L33-L41 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L174-L188 @ ens_v1@91c966f).
/// The unlocked wrapped migration calls `NameWrapper.setResolver(node, 0)`
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/migration/UnlockedMigrationController.sol:L146 @ ens_v2_sepolia_20261001@07e55a05),
/// a pass-through to the registry's `setResolver`, which always emits
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L666-L671 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89-L95 @ ens_v1@91c966f).
/// A locked name without `CANNOT_SET_RESOLVER` burned is cleared the same way, through
/// `NameWrapper.setResolver(node, 0)`
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/migration/LockedWrapperReceiver.sol:L137-L138 @ ens_v2_sepolia_20261001@07e55a05).
/// Every case reads null: a set pointer cleared (1), a node with none that the unwrapped path
/// leaves without a log (2), and one a wrapped path clears again (3).
#[tokio::test]
async fn a_migration_that_clears_the_resolver_reads_null() -> Result<()> {
    let fixture = Fixture::new("families_registry_pointer_migration", 2).await?;
    pointer_event(&fixture, 1, 0, 1, Some(R1)).await?;
    pointer_event(&fixture, 2, 0, 1, Some(ZERO)).await?;
    fixture.surface(&name(2), &format!("0x{:064x}", 2)).await?;
    pointer_event(&fixture, 2, 1, 3, Some(ZERO)).await?;
    fixture.apply(1, FamilyMode::Normal).await?;
    assert_eq!(composed(&fixture, 1).await?, served(R1));
    fixture.apply(2, FamilyMode::Normal).await?;
    assert_eq!(f4(&fixture, 1).await?.as_deref(), Some(ZERO));
    assert_eq!(f4(&fixture, 2).await?, None);
    assert_eq!(f4(&fixture, 3).await?.as_deref(), Some(ZERO));
    for number in [1, 2, 3] {
        assert_eq!(
            composed(&fixture, number).await?,
            Some(Value::Null),
            "{number}"
        );
    }
    fixture.assert_rebuild_equal(2).await?;
    fixture.cleanup().await
}

/// A locked name with `CANNOT_SET_RESOLVER` burned is migrated without a clear: the receiver
/// reads the ENSv1 pointer into the ENSv2 registration instead
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/migration/LockedWrapperReceiver.sol:L137-L144 @ ens_v2_sepolia_20261001@07e55a05),
/// so the registry keeps it. That branch emits no ENSv1 registry event, so the test models no
/// migration: it sets the pointer and checks that it stays.
#[tokio::test]
async fn a_locked_cannot_set_resolver_name_keeps_its_pointer() -> Result<()> {
    let fixture = Fixture::new("families_registry_pointer_locked", 2).await?;
    pointer_event(&fixture, 1, 0, 4, Some(R1)).await?;
    fixture.apply(2, FamilyMode::Normal).await?;
    assert_eq!(f4(&fixture, 4).await?.as_deref(), Some(R1));
    assert_eq!(composed(&fixture, 4).await?, served(R1));
    fixture.assert_rebuild_equal(2).await?;
    fixture.cleanup().await
}

/// A pointer event without a resolver is stored as the empty address and reads null.
#[tokio::test]
async fn an_event_without_a_resolver_reads_null() -> Result<()> {
    let fixture = Fixture::new("families_registry_pointer_empty", 2).await?;
    pointer_event(&fixture, 1, 0, 5, Some(R1)).await?;
    pointer_event(&fixture, 2, 0, 5, None).await?;
    fixture.apply(2, FamilyMode::Normal).await?;
    assert_eq!(f4(&fixture, 5).await?.as_deref(), Some(""));
    assert_eq!(composed(&fixture, 5).await?, Some(Value::Null));
    fixture.assert_rebuild_equal(2).await?;
    fixture.cleanup().await
}

/// Undoing the block that moved the pointer restores the previous F4 value, and the stored
/// summary follows it from the journal.
#[tokio::test]
async fn undo_restores_the_pointer() -> Result<()> {
    let fixture = Fixture::new("families_registry_pointer_undo", 2).await?;
    pointer_event(&fixture, 1, 0, 6, Some(R1)).await?;
    pointer_event(&fixture, 2, 0, 6, Some(R2)).await?;
    fixture.apply(1, FamilyMode::Normal).await?;
    assert_eq!(composed(&fixture, 6).await?, served(R1));
    fixture.apply(2, FamilyMode::Normal).await?;
    assert_eq!(composed(&fixture, 6).await?, served(R2));
    assert_eq!(families::undo_to(&fixture.pool, CHAIN, 1).await?, 1);
    assert_eq!(f4(&fixture, 6).await?.as_deref(), Some(R1));
    assert_eq!(composed(&fixture, 6).await?, served(R1));
    fixture.assert_undo_restores(2).await?;
    assert_eq!(composed(&fixture, 6).await?, served(R2));
    fixture.assert_rebuild_equal(2).await?;
    fixture.cleanup().await
}
