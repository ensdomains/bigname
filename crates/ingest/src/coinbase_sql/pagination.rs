use anyhow::{Result, bail};

use super::{
    client::CoinbaseSqlClient,
    query::{CoinbaseSqlFilterPack, build_query},
    rows::CoinbaseLogRow,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CoinbaseSqlLogCursor {
    pub block_number: i64,
    pub transaction_index: i64,
    pub log_index: i64,
}

impl From<&CoinbaseLogRow> for CoinbaseSqlLogCursor {
    fn from(row: &CoinbaseLogRow) -> Self {
        Self {
            block_number: row.block_number,
            transaction_index: row.transaction_index,
            log_index: row.log_index,
        }
    }
}

pub(super) async fn fetch_all_pages(
    client: &CoinbaseSqlClient,
    pack: &CoinbaseSqlFilterPack,
    page_limit: usize,
    sql_limit: usize,
) -> Result<Vec<CoinbaseLogRow>> {
    let mut cursor = None;
    let mut rows = Vec::new();
    loop {
        let sql = build_query(pack, cursor, page_limit)?;
        if sql.len() > sql_limit {
            bail!("Coinbase SQL query exceeds its configured character limit");
        }
        let page = client.run_query(&sql).await?;
        let page_len = page.len();
        append_page(&mut rows, page)?;
        if page_len < page_limit {
            return Ok(rows);
        }
        let next = rows.last().map(CoinbaseSqlLogCursor::from);
        if next == cursor {
            bail!("Coinbase SQL full page did not advance its cursor");
        }
        cursor = next;
    }
}

fn append_page(rows: &mut Vec<CoinbaseLogRow>, page: Vec<CoinbaseLogRow>) -> Result<()> {
    for row in page {
        let cursor = CoinbaseSqlLogCursor::from(&row);
        if let Some(previous) = rows.last_mut() {
            let previous_cursor = CoinbaseSqlLogCursor::from(&*previous);
            if cursor == previous_cursor {
                let same_identity = previous.block_hash == row.block_hash
                    && previous.transaction_hash == row.transaction_hash
                    && previous.address == row.address
                    && previous.topics == row.topics;
                if same_identity && previous.decoded != row.decoded {
                    if row.decoded {
                        *previous = row;
                    }
                    continue;
                }
                if *previous == row {
                    continue;
                }
                bail!("Coinbase SQL returned conflicting rows at one log position");
            }
            if (
                cursor.block_number,
                cursor.transaction_index,
                cursor.log_index,
            ) < (
                previous_cursor.block_number,
                previous_cursor.transaction_index,
                previous_cursor.log_index,
            ) {
                bail!("Coinbase SQL rows are not ordered");
            }
        }
        rows.push(row);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(decoded: bool) -> CoinbaseLogRow {
        CoinbaseLogRow {
            block_number: 1,
            block_hash: format!("0x{}", "11".repeat(32)),
            transaction_hash: format!("0x{}", "22".repeat(32)),
            transaction_index: 2,
            log_index: 3,
            address: format!("0x{}", "33".repeat(20)),
            topics: vec![format!("0x{}", "44".repeat(32))],
            decoded,
        }
    }

    #[test]
    fn decoded_and_encoded_union_twins_count_once() -> Result<()> {
        let mut rows = Vec::new();
        append_page(&mut rows, vec![row(false), row(true)])?;
        assert_eq!(rows, vec![row(true)]);
        Ok(())
    }
}
