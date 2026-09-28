//! Shared event fixtures for the permanent family permission and address readers.
#![allow(dead_code)]
pub mod wrapper;
use crate::support::Fixture;
use anyhow::Result;
use bigname_project::families::FamilyMode;
pub async fn publish(fixture: &Fixture, target: i64) -> Result<()> {
    fixture.apply(target, FamilyMode::Normal).await?;
    Ok(())
}
