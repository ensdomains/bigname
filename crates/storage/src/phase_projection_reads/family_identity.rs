//! Batch lookup's identity and inventory views of the family publication.
use crate::IdentityNameRecordRow;
use anyhow::Result;

pub(super) async fn load(
    db: crate::ReadDb<'_>,
    ids: &[String],
    include_inventory: bool,
) -> Result<Vec<IdentityNameRecordRow>> {
    let mut snapshot = db.snapshot().await?;
    let out = crate::families::lookup::load_on(&mut snapshot, ids, include_inventory).await?;
    snapshot.close().await?;
    Ok(out)
}
