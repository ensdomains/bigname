//! The record count of each listed name (`include=counts` and the role summary).
use std::collections::BTreeMap;

use bigname_storage::NameCurrentRow;

pub(super) async fn load_address_name_record_counts<'a>(
    pool: &sqlx::PgPool,
    names: impl Iterator<Item = &'a str>,
    name_rows: &BTreeMap<String, NameCurrentRow>,
) -> anyhow::Result<BTreeMap<String, u64>> {
    if bigname_storage::publication_source::serve_from_families() {
        // The family inventory of each name's record-serving resource (TYR-36 step 7b).
        let (logical_name_ids, rows): (Vec<String>, Vec<&NameCurrentRow>) = names
            .filter_map(|logical_name_id| {
                name_rows
                    .get(logical_name_id)
                    .map(|row| (logical_name_id.to_owned(), row))
            })
            .unzip();
        let counts =
            bigname_storage::families::records::load_family_record_counts(pool, &rows).await?;
        return Ok(logical_name_ids
            .into_iter()
            .zip(counts)
            .filter_map(|(logical_name_id, count)| count.map(|count| (logical_name_id, count)))
            .collect());
    }
    let mut logical_name_ids = Vec::new();
    let mut keys = Vec::new();
    for logical_name_id in names {
        let Some(name_row) = name_rows.get(logical_name_id) else {
            continue;
        };
        let Some((resource_id, boundary)) =
            bigname_storage::resolution_record_inventory_lookup_key_any_chain(name_row)
        else {
            continue;
        };
        logical_name_ids.push(logical_name_id.to_owned());
        keys.push((resource_id, boundary));
    }

    let counts =
        bigname_storage::count_record_inventory_selectors_by_lookup_keys(pool, &keys).await?;
    Ok(logical_name_ids
        .into_iter()
        .zip(counts)
        .filter_map(|(logical_name_id, count)| count.map(|count| (logical_name_id, count)))
        .collect())
}
