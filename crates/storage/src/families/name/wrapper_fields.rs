//! Batched stored wrapper expiry shared by composition and Project publication.
use crate::{
    NameCurrentRow,
    name_current::wrapper_expiry::{self, WrapperExpiryKey},
};
use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::PgConnection;
use std::collections::BTreeMap;

/// Write the stored expiry of the name's NameWrapper entry beside the composed wrapper fields:
/// on a row that serves a wrapper state, and on a row whose emancipated or locked wrapper has
/// passed its expiry, which composition masks (`wrapper_masked`) instead because NameWrapper then
/// reports no owner and no fuses for it
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843-L856 @ ens_v1@91c966f).
/// Inside
/// `defer_wrapper_expiries` each such row gets a pending marker instead, which the response
/// resolves once for all the rows it serves.
pub(super) async fn attach_wrapper_expiries(
    conn: &mut PgConnection,
    rows: &mut BTreeMap<String, NameCurrentRow>,
) -> Result<()> {
    attach_expiries(
        conn,
        rows.values_mut().collect(),
        wrapper_expiry::deferred(),
    )
    .await
}

/// Publication must finish every expiry even if its caller happens to defer API reads.
pub(super) async fn attach_published_wrapper_expiries<'a>(
    conn: &mut PgConnection,
    rows: impl IntoIterator<Item = &'a mut NameCurrentRow>,
) -> Result<()> {
    attach_expiries(conn, rows.into_iter().collect(), false).await
}

async fn attach_expiries(
    conn: &mut PgConnection,
    mut rows: Vec<&mut NameCurrentRow>,
    deferred: bool,
) -> Result<()> {
    let mut wanted = BTreeMap::new();
    for row in rows.iter().filter(|row| wrapper_flagged(row)) {
        wanted.insert(wrapper_key(row)?, backed(row));
    }
    if wanted.is_empty() {
        return Ok(());
    }
    if deferred {
        for row in rows.iter_mut().filter(|row| wrapper_flagged(row)) {
            let marker = wrapper_expiry::pending_marker(&wrapper_key(row)?, backed(row));
            row.declared_summary[wrapper_expiry::WRAPPER_EXPIRY_PENDING_KEY] = marker;
        }
        return Ok(());
    }
    let served = wrapper_expiry::load_wrapper_expiries(&mut *conn, &wanted).await?;
    for row in rows.iter_mut().filter(|row| wrapper_flagged(row)) {
        if let Some(expiry) = served.get(&wrapper_key(row)?) {
            row.declared_summary[wrapper_expiry::WRAPPER_EXPIRY_KEY] = expiry.clone();
        }
    }
    Ok(())
}

fn wrapper_flagged(row: &NameCurrentRow) -> bool {
    backed(row) || row.declared_summary.get("wrapper_masked") == Some(&Value::Bool(true))
}

fn backed(row: &NameCurrentRow) -> bool {
    row.declared_summary
        .get("wrapper_state")
        .and_then(Value::as_str)
        .and_then(crate::public_name_fields::WrapperState::from_wire)
        .is_some_and(crate::public_name_fields::WrapperState::is_backed)
}

/// Composition reads the wrapper of the name's binding resource, which is the row's
/// `resource_id` whenever the row is bound.
fn wrapper_key(row: &NameCurrentRow) -> Result<WrapperExpiryKey> {
    Ok(WrapperExpiryKey {
        chain_id: row.provenance["chain_id"]
            .as_str()
            .context("composed name has no chain")?
            .to_owned(),
        resource_id: row.resource_id.with_context(|| {
            format!(
                "composed wrapper of {} has no resource",
                row.logical_name_id
            )
        })?,
    })
}
