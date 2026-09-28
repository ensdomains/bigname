//! The record count of each listed name (`include=counts` and the role summary).
use std::collections::BTreeMap;

use bigname_storage::NameCurrentRow;

pub(super) async fn load_address_name_record_counts<'a>(
    pool: &sqlx::PgPool,
    names: impl Iterator<Item = &'a str>,
    name_rows: &BTreeMap<String, NameCurrentRow>,
) -> anyhow::Result<BTreeMap<String, u64>> {
    // The family inventory of each name's record-serving resource.
    let (logical_name_ids, rows): (Vec<String>, Vec<&NameCurrentRow>) = names
        .filter_map(|logical_name_id| {
            name_rows
                .get(logical_name_id)
                .map(|row| (logical_name_id.to_owned(), row))
        })
        .unzip();
    let counts = bigname_storage::families::records::load_family_record_counts(pool, &rows).await?;
    Ok(logical_name_ids
        .into_iter()
        .zip(counts)
        .filter_map(|(logical_name_id, count)| count.map(|count| (logical_name_id, count)))
        .collect())
}
