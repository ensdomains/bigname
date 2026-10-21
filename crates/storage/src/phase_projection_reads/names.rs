use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use sqlx::PgPool;

use crate::{IdentityNameRecordRow, NameCurrentRow};

pub async fn load_phase_identity_records_by_ids(
    db: impl Into<crate::ReadDb<'_>>,
    logical_name_ids: &[String],
) -> Result<Vec<IdentityNameRecordRow>> {
    load_phase_identity_records(db.into(), logical_name_ids, true).await
}

pub async fn load_phase_identity_name_feed_records_by_ids(
    db: impl Into<crate::ReadDb<'_>>,
    logical_name_ids: &[String],
) -> Result<Vec<IdentityNameRecordRow>> {
    load_phase_identity_records(db.into(), logical_name_ids, false).await
}

async fn load_phase_identity_records(
    db: crate::ReadDb<'_>,
    logical_name_ids: &[String],
    include_inventory: bool,
) -> Result<Vec<IdentityNameRecordRow>> {
    let requested = dedupe(logical_name_ids);
    if requested.is_empty() {
        return Ok(Vec::new());
    }

    super::family_identity::load(db, &requested, include_inventory).await
}

pub async fn load_phase_name_current_rows_by_ids(
    pool: &PgPool,
    logical_name_ids: &[String],
) -> Result<BTreeMap<String, NameCurrentRow>> {
    crate::families::name::load_family_names_by_logical_name_ids(pool, logical_name_ids).await
}

fn dedupe(values: &[String]) -> Vec<String> {
    values
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}
