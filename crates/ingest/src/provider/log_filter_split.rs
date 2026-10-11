//! Splits an `eth_getLogs` filter whose address or topic lists are longer than a provider
//! accepts, and merges the split answers back into one result.
//!
//! Some providers refuse a filter when one position (the address list, or the list of
//! values for one topic) carries more entries than they allow. Each list is cut into chunks
//! of at most the configured limit, and every combination of chunks is queried. A log has one
//! address and one value per topic, so every log the whole filter matches is matched by some
//! combination, and no combination matches a log the whole filter would not.

use std::collections::BTreeMap;

use anyhow::{Result, bail};
use tracing::debug;

use super::types::Log;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct FilterPart<'a> {
    pub addresses: &'a [String],
    pub topic0s: &'a [String],
    pub topic1s: &'a [String],
}

/// Every combination of at most `limit` addresses, `topic0`s and `topic1`s, in input order
/// with address chunks outermost. An empty list matches any value and stays one empty chunk.
pub(super) fn split_filter<'a>(
    addresses: &'a [String],
    topic0s: &'a [String],
    topic1s: &'a [String],
    limit: usize,
) -> Vec<FilterPart<'a>> {
    let address_chunks = chunks(addresses, limit);
    let topic0_chunks = chunks(topic0s, limit);
    let topic1_chunks = chunks(topic1s, limit);
    let mut parts =
        Vec::with_capacity(address_chunks.len() * topic0_chunks.len() * topic1_chunks.len());
    for addresses in &address_chunks {
        for topic0s in &topic0_chunks {
            for topic1s in &topic1_chunks {
                parts.push(FilterPart {
                    addresses,
                    topic0s,
                    topic1s,
                });
            }
        }
    }
    if parts.len() > 1 {
        debug!(
            component = "ingest_provider",
            address_entries = addresses.len(),
            address_queries = address_chunks.len(),
            topic0_entries = topic0s.len(),
            topic0_queries = topic0_chunks.len(),
            topic1_entries = topic1s.len(),
            topic1_queries = topic1_chunks.len(),
            queries = parts.len(),
            "splitting eth_getLogs filter over the position limit"
        );
    }
    parts
}

fn chunks(entries: &[String], limit: usize) -> Vec<&[String]> {
    if entries.is_empty() {
        vec![entries]
    } else {
        entries.chunks(limit).collect()
    }
}

/// Merges the answers of split queries in block and log-index order.
///
/// A log is identified by its block hash and log index. A repeat with the same content is
/// dropped. A repeat with different content is a contradiction in the provider's answers.
pub(super) fn merge_split_logs(parts: Vec<Vec<Log>>) -> Result<Vec<Log>> {
    let mut by_identity = BTreeMap::<(String, i64), Log>::new();
    for log in parts.into_iter().flatten() {
        let key = (log.block_hash.clone(), log.log_index);
        match by_identity.get(&key) {
            Some(previous) if *previous != log => bail!(
                "provider returned conflicting log identity {} {} across split log queries",
                key.0,
                key.1
            ),
            Some(_) => {}
            None => {
                by_identity.insert(key, log);
            }
        }
    }
    let mut logs = by_identity.into_values().collect::<Vec<_>>();
    logs.sort_by(|left, right| {
        (left.block_number, left.log_index, &left.block_hash).cmp(&(
            right.block_number,
            right.log_index,
            &right.block_hash,
        ))
    });
    Ok(logs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entries(count: usize) -> Vec<String> {
        (0..count).map(|index| format!("0x{index:040x}")).collect()
    }

    #[test]
    fn each_position_is_chunked_at_the_limit() {
        let one = entries(1);
        for (count, expected) in [(0, 1), (500, 1), (501, 2), (1_501, 4)] {
            let list = entries(count);
            for position in [0, 2] {
                let parts = if position == 0 {
                    split_filter(&list, &one, &[], 500)
                } else {
                    split_filter(&[], &one, &list, 500)
                };
                assert_eq!(parts.len(), expected, "{count} entries");
                let chunks = parts
                    .iter()
                    .map(|part| {
                        if position == 0 {
                            part.addresses
                        } else {
                            part.topic1s
                        }
                    })
                    .collect::<Vec<_>>();
                assert!(chunks.iter().all(|chunk| chunk.len() <= 500));
                assert_eq!(chunks.concat(), list);
                if count == 0 {
                    assert!(chunks[0].is_empty());
                }
            }
        }
        for (count, expected) in [(500, 1), (501, 2), (1_501, 4)] {
            let list = entries(count);
            let parts = split_filter(&[], &list, &[], 500);
            assert_eq!(parts.len(), expected, "{count} topic0s");
            assert!(parts.iter().all(|part| part.topic0s.len() <= 500));
            assert_eq!(
                parts
                    .iter()
                    .map(|part| part.topic0s)
                    .collect::<Vec<_>>()
                    .concat(),
                list
            );
        }
    }
}
