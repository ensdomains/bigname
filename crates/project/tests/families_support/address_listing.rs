//! The address listing oracle every publication a family test makes is checked against: the
//! stored rows (`project_address_listing`) equal what the address-names route composes at read
//! for every address of the chain (`assert_address_listing_matches_the_route`).
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Result, ensure};
use bigname_storage::families::records::assert_address_listing_matches_the_route;

use super::Fixture;

impl Fixture {
    /// The address listing of `chain` equals the route's rows at its family marker's block. A
    /// chain with no marker block stores no rows. A publication the readers refuse has no route
    /// rows to compare with: one written under this build's projection content hash is counted
    /// as skipped, and one written under another hash, which no reader of this build serves, is
    /// not checked.
    pub async fn assert_address_listing(&self, chain: &str) -> Result<()> {
        let marker: Option<(Option<i64>, Option<String>)> = sqlx::query_as(
            "SELECT current_block_number, input_content_hash FROM project_family_marker
             WHERE chain_id = $1",
        )
        .bind(chain)
        .fetch_optional(&self.pool)
        .await?;
        let (block, content_hash) = marker.unwrap_or_default();
        if block.is_none() {
            let stored: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM project_address_listing WHERE chain_id = $1",
            )
            .bind(chain)
            .fetch_one(&self.pool)
            .await?;
            ensure!(
                stored == 0,
                "{chain} has no marker block but stores {stored} address listing rows"
            );
            return Ok(());
        }
        let counter = if assert_address_listing_matches_the_route(&self.pool, chain).await? {
            &self.address_listing_checks.compared
        } else if content_hash.as_deref() == Some(super::CONTENT_HASH) {
            &self.address_listing_checks.skipped
        } else {
            return Ok(());
        };
        counter.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// The publications whose address listing was compared with the route so far.
    pub fn address_listing_compared(&self) -> usize {
        self.address_listing_checks.compared()
    }
}

/// The address listing checks of one fixture: publications compared, and publications the
/// readers refused.
#[derive(Debug, Default)]
pub(super) struct Checks {
    compared: AtomicUsize,
    skipped: AtomicUsize,
}

impl Checks {
    /// A fixture that checked a publication compared at least one: one whose every publication
    /// the readers refused proved nothing about the listing.
    pub(super) fn require(&self) -> Result<()> {
        let compared = self.compared.load(Ordering::Relaxed);
        let skipped = self.skipped.load(Ordering::Relaxed);
        ensure!(
            skipped == 0 || compared > 0,
            "{skipped} publications were refused by the readers and none was compared with the \
             address listing"
        );
        Ok(())
    }

    /// The publications compared so far.
    pub(super) fn compared(&self) -> usize {
        self.compared.load(Ordering::Relaxed)
    }
}
