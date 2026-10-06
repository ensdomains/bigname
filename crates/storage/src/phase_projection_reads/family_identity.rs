//! Batch lookup's identity and inventory views of the family publication.
use crate::IdentityNameRecordRow;
use anyhow::Result;
use sqlx::{PgConnection, PgPool};

pub(super) async fn load(
    pool: &PgPool,
    ids: &[String],
    include_inventory: bool,
) -> Result<Vec<IdentityNameRecordRow>> {
    let mut snapshot = crate::families::read_snapshot(pool).await?;
    let out = crate::families::lookup::load_on(&mut snapshot, ids, include_inventory).await?;
    snapshot.commit().await?;
    Ok(out)
}

pub(crate) async fn load_on(
    conn: &mut PgConnection,
    ids: &[String],
    include_inventory: bool,
) -> Result<Vec<IdentityNameRecordRow>> {
    crate::families::lookup::load_on(conn, ids, include_inventory).await
}
