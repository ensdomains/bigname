use std::{fmt, thread, time::Duration};

use alloy_primitives::B256;
use anyhow::Result;
use reth_ethereum::provider::{BlockHashReader, BlockNumReader};
use tracing::{debug, info};

use super::EthereumRethProviderFactory;

const HEAD_REFRESH_MAX_ATTEMPTS: usize = 5;
const HEAD_REFRESH_RETRY_BACKOFF: Duration = Duration::from_millis(150);

#[derive(Debug, Eq, PartialEq)]
struct CanonicalHead {
    best_number: u64,
    block_hash: B256,
}

#[derive(Debug)]
struct MissingCanonicalBlockHash {
    best_number: u64,
}

impl fmt::Display for MissingCanonicalBlockHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Reth DB did not return canonical block hash for best number {}",
            self.best_number
        )
    }
}

impl std::error::Error for MissingCanonicalBlockHash {}

pub(super) fn fetch_canonical_head_with_retry(
    chain: &str,
    factory: &EthereumRethProviderFactory,
) -> Result<B256> {
    retry_canonical_head_read(chain, || fetch_canonical_head_once(factory), thread::sleep)
        .map(|head| head.block_hash)
}

fn fetch_canonical_head_once(factory: &EthereumRethProviderFactory) -> Result<CanonicalHead> {
    let chain_info = factory.chain_info()?;
    let block_hash = if chain_info.best_hash == B256::ZERO {
        factory
            .block_hash(chain_info.best_number)?
            .ok_or(MissingCanonicalBlockHash {
                best_number: chain_info.best_number,
            })?
    } else {
        chain_info.best_hash
    };

    Ok(CanonicalHead {
        best_number: chain_info.best_number,
        block_hash,
    })
}

fn retry_canonical_head_read(
    chain: &str,
    mut read: impl FnMut() -> Result<CanonicalHead>,
    mut backoff: impl FnMut(Duration),
) -> Result<CanonicalHead> {
    let mut attempt_count = 1usize;

    loop {
        match read() {
            Ok(head) => {
                if attempt_count > 1 {
                    info!(
                        service = "indexer",
                        command = "provider",
                        chain,
                        canonical_best_number = head.best_number,
                        attempt_count,
                        retry_count = attempt_count - 1,
                        "Reth DB head refresh recovered after canonical block hash retry"
                    );
                }
                return Ok(head);
            }
            Err(error) => {
                let Some(raced_best_number) = missing_canonical_block_hash_best_number(&error)
                else {
                    return Err(error);
                };
                if attempt_count >= HEAD_REFRESH_MAX_ATTEMPTS {
                    return Err(error);
                }

                debug!(
                    service = "indexer",
                    command = "provider",
                    chain,
                    raced_best_number,
                    attempt_count,
                    max_attempts = HEAD_REFRESH_MAX_ATTEMPTS,
                    backoff_ms = HEAD_REFRESH_RETRY_BACKOFF.as_millis() as u64,
                    "Reth DB canonical block hash was absent for the resolved best number; retrying head refresh"
                );
                backoff(HEAD_REFRESH_RETRY_BACKOFF);
                attempt_count += 1;
            }
        }
    }
}

fn missing_canonical_block_hash_best_number(error: &anyhow::Error) -> Option<u64> {
    error.chain().find_map(|cause| {
        cause
            .downcast_ref::<MissingCanonicalBlockHash>()
            .map(|missing| missing.best_number)
    })
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use reth_ethereum::provider::ProviderError;

    use super::*;

    #[test]
    fn classifier_accepts_only_typed_missing_canonical_hash_error() {
        let typed = anyhow::Error::new(MissingCanonicalBlockHash { best_number: 42 });
        assert_eq!(missing_canonical_block_hash_best_number(&typed), Some(42));

        let matching_text =
            anyhow::anyhow!("Reth DB did not return canonical block hash for best number 42");
        assert_eq!(
            missing_canonical_block_hash_best_number(&matching_text),
            None
        );

        let other_provider_error = anyhow::Error::new(ProviderError::StateForNumberNotFound(42));
        assert_eq!(
            missing_canonical_block_hash_best_number(&other_provider_error),
            None
        );
    }

    #[test]
    fn absent_then_present_retries_with_a_fresh_best_number() -> Result<()> {
        let resolved_hash = B256::repeat_byte(0x22);
        let mut outcomes = VecDeque::from([
            Err(anyhow::Error::new(MissingCanonicalBlockHash {
                best_number: 100,
            })),
            Ok(CanonicalHead {
                best_number: 101,
                block_hash: resolved_hash,
            }),
        ]);
        let mut read_count = 0usize;
        let mut backoffs = Vec::new();

        let head = retry_canonical_head_read(
            "ethereum-mainnet",
            || {
                read_count += 1;
                outcomes.pop_front().expect("test read outcome")
            },
            |duration| backoffs.push(duration),
        )?;

        assert_eq!(read_count, 2);
        assert!(outcomes.is_empty());
        assert_eq!(
            head,
            CanonicalHead {
                best_number: 101,
                block_hash: resolved_hash,
            }
        );
        assert_eq!(backoffs, vec![HEAD_REFRESH_RETRY_BACKOFF]);
        Ok(())
    }

    #[test]
    fn persistent_absence_exhausts_attempt_cap_and_preserves_error() {
        let mut read_count = 0usize;
        let mut backoffs = Vec::new();

        let error = retry_canonical_head_read(
            "ethereum-mainnet",
            || {
                let best_number = 200 + read_count as u64;
                read_count += 1;
                Err(anyhow::Error::new(MissingCanonicalBlockHash {
                    best_number,
                }))
            },
            |duration| backoffs.push(duration),
        )
        .expect_err("persistent missing canonical hash must fail");

        assert_eq!(read_count, HEAD_REFRESH_MAX_ATTEMPTS);
        assert_eq!(
            backoffs,
            vec![HEAD_REFRESH_RETRY_BACKOFF; HEAD_REFRESH_MAX_ATTEMPTS - 1]
        );
        assert_eq!(
            error.to_string(),
            "Reth DB did not return canonical block hash for best number 204"
        );
        assert_eq!(
            error
                .downcast_ref::<MissingCanonicalBlockHash>()
                .map(|missing| missing.best_number),
            Some(204)
        );
    }

    #[test]
    fn unrelated_error_does_not_retry() {
        let mut read_count = 0usize;
        let mut backoffs = Vec::new();

        let error = retry_canonical_head_read(
            "ethereum-mainnet",
            || {
                read_count += 1;
                Err(anyhow::Error::new(ProviderError::StateForNumberNotFound(
                    42,
                )))
            },
            |duration| backoffs.push(duration),
        )
        .expect_err("unrelated head-refresh error must fail immediately");

        assert_eq!(read_count, 1);
        assert!(backoffs.is_empty());
        assert!(matches!(
            error.downcast_ref::<ProviderError>(),
            Some(ProviderError::StateForNumberNotFound(42))
        ));
    }
}
