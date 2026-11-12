//! The leads stream of a rebuild's composition pass (docs/projections.md, "Address listing"): the
//! address listing's `registration_lead`, decided once at the pass block after every names chunk
//! committed. The names stream's workers write each name's listing rows and decide no lead, since
//! an (address, resource) group's rows can lie in several workers' chunks. Each partition is the
//! addresses with `hashtext(address) & (partitions - 1)` equal to it, so a group is never split,
//! and sets every row's lead by the rank the incremental path's lead step uses
//! (`derived::lead_rank`): in each group the smallest `(display_key, logical_name_id)` listed row
//! leads and no other row does. A row with no resource leads no group. A partition run again
//! changes nothing.
use sqlx::{Postgres, Transaction};

use super::steps::{Parts, Step, Steps};
use crate::{
    ProjectError, Result,
    families::{derived::lead_rank, tables::ADDRESS_LISTING},
};

pub(crate) const STEPS: Steps = &[("leads", Parts::Partitions)];

const LEADS: &str = concat!(
    "/* project:families.compose.leads */
    UPDATE project_address_listing listing
    SET registration_lead = ranked.lead
    FROM (
        SELECT members.address, members.logical_name_id, ",
    lead_rank!(),
    " AS lead
        FROM project_address_listing members
        WHERE members.chain_id = $1 AND members.resource_id IS NOT NULL
          AND (hashtext(members.address) & $3::int) = $2::int
    ) ranked
    WHERE listing.chain_id = $1 AND listing.address = ranked.address
      AND listing.logical_name_id = ranked.logical_name_id
      AND listing.registration_lead <> ranked.lead"
);

/// Run the partition `step` names. Returns the table it wrote and the rows it changed there.
pub(crate) async fn run(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    step: &Step,
) -> Result<(&'static str, u64)> {
    let rows = sqlx::query(LEADS)
        .bind(chain)
        .bind(i32::from(step.part))
        .bind(step.mask())
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to decide the address listing's leads", error)
        })?
        .rows_affected();
    Ok((ADDRESS_LISTING.name, rows))
}
