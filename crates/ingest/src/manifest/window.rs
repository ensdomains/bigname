use sqlx::PgPool;

use super::{WatchFilter, announcements};
use crate::Result;

impl WatchFilter {
    /// Restricts a prepared watch plan to one fetch window without widening any interval.
    pub(crate) fn clipped(&self, from: i64, to: i64) -> Self {
        let mut filter = self.clone();
        filter.address_ranges.retain_mut(|range| {
            range.from_block = range.from_block.max(from);
            range.to_block = range.to_block.min(to);
            range.from_block <= range.to_block
        });
        filter.all_emitter_ranges.retain_mut(|range| {
            range.from_block = range.from_block.max(from);
            range.to_block = range.to_block.min(to);
            range.from_block <= range.to_block
        });
        filter.implementation_ranges.retain_mut(|range| {
            range.from_block = range.from_block.max(from);
            range.to_block = range.to_block.min(to);
            range.from_block <= range.to_block
        });
        filter
    }

    pub(crate) fn has_queries(&self) -> bool {
        !self.address_ranges.is_empty()
            || !self.all_emitter_ranges.is_empty()
            || !self.implementation_ranges.is_empty()
    }

    pub(crate) async fn supplement_creation_announcements(
        &mut self,
        pool: &PgPool,
        chain_id: &str,
        from: i64,
        to: i64,
    ) -> Result<()> {
        for topic in self.creation_topic0s() {
            let announcements = announcements::canonical(pool, chain_id, to, &topic).await?;
            self.admit_creation_announcements(&topic, announcements, from, to);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{AddressRange, AllEmitterRange, CreationWatch, ImplementationRange};

    #[test]
    fn clipping_preserves_topic_scopes_and_creation_watches() {
        let plan = WatchFilter {
            address_ranges: vec![
                AddressRange {
                    address: "0x01".into(),
                    from_block: 10,
                    to_block: 30,
                    topic0s: vec!["0xaa".into()],
                },
                AddressRange {
                    address: "0x02".into(),
                    from_block: 40,
                    to_block: 50,
                    topic0s: vec!["0xbb".into()],
                },
            ],
            all_emitter_ranges: vec![AllEmitterRange {
                from_block: 0,
                to_block: 100,
                topic0s: vec!["0xcc".into()],
            }],
            implementation_ranges: vec![ImplementationRange {
                from_block: 0,
                to_block: 100,
                topic0: "0xdd".into(),
                topic1s: vec!["0xee".into()],
            }],
            creation_watches: vec![CreationWatch {
                announcement_topic0: "0xcc".into(),
                scoped_topic0s: vec!["0xff".into()],
            }],
        };
        let mut filter = plan.clipped(20, 45);
        assert!(filter.includes("0x01", "0xaa", 20));
        assert!(!filter.includes("0x01", "0xaa", 31));
        assert!(!filter.includes("0x01", "0xbb", 40));
        assert!(filter.includes("0x02", "0xbb", 40));
        assert!(!filter.includes("0x02", "0xbb", 46));
        assert!(filter.includes_log("0x03", &["0xdd".into(), "0xee".into()], 25));
        assert!(!filter.includes_log("0x03", &["0xdd".into(), "0xff".into()], 25));
        assert!(
            filter
                .queries()
                .iter()
                .all(|query| query.from_block >= 20 && query.to_block <= 45)
        );
        filter.admit_creation_announcements("0xcc", [("0x04".into(), 25)], 20, 45);
        assert!(!filter.includes("0x04", "0xff", 24));
        assert!(filter.includes("0x04", "0xff", 25));
        assert!(
            !plan.includes("0x04", "0xff", 25),
            "per-window discoveries must not mutate the prepared plan"
        );
    }
}
