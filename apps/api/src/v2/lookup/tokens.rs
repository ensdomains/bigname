use super::LookupResult;
use crate::v2::{V2Result, name_record::tokens};

/// One batch across forward results and every reverse result page, at the captured lookup head.
pub(super) async fn apply(
    pool: &sqlx::PgPool,
    results: &mut [Option<LookupResult>],
    snapshot: &bigname_storage::SelectedSnapshot,
) -> V2Result<()> {
    let records = results
        .iter_mut()
        .flatten()
        .flat_map(|result| {
            result
                .record
                .iter_mut()
                .chain(result.records.iter_mut().flatten())
        })
        .filter_map(|record| {
            tokens::token_registration(
                record.authority,
                record.registration_status,
                record.registration_id.as_deref(),
            )
            .map(|registration| (registration, record))
        })
        .collect::<Vec<_>>();
    let token_ids = tokens::load(pool, records.iter().map(|(id, _)| *id), snapshot).await?;
    for (registration, record) in records {
        record.token_id = token_ids.get(&registration).cloned();
    }
    Ok(())
}
