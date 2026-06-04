use std::collections::BTreeMap;

use anyhow::Result;

use super::*;

fn provider_log(block_hash: &str, block_number: i64) -> ProviderLog {
    ProviderLog {
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            .to_owned(),
        transaction_index: 0,
        log_index: 0,
        address: "0x0000000000000000000000000000000000000001".to_owned(),
        topics: Vec::new(),
        data: "0x".to_owned(),
    }
}

#[test]
fn sample_validation_resolves_only_returned_log_blocks() -> Result<()> {
    let logs_by_block = BTreeMap::from([
        (
            42,
            vec![provider_log(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                42,
            )],
        ),
        (
            45,
            vec![provider_log(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                45,
            )],
        ),
    ]);

    assert_eq!(
        coinbase_sql_sample_validation_block_numbers(
            BackfillBlockRange::new(40, 50)?,
            &logs_by_block
        ),
        vec![42, 45]
    );
    assert_eq!(
        coinbase_sql_sample_validation_block_numbers(
            BackfillBlockRange::new(40, 50)?,
            &BTreeMap::new()
        ),
        Vec::<i64>::new()
    );
    Ok(())
}
