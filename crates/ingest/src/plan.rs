use crate::{
    IngestError, Result,
    engine::{BatchRequest, HeadMarkers, Marker, SourceDescriptor},
    provider::{Block, HeadSnapshot, ProviderKind, normalized_kind},
};

// Both Base sources deliberately include this block: the Coinbase bulk range
// ends here and RPC begins here, so warehouse lag cannot leave a gap. Stored
// rows use shared block-hash and log-position keys, making the overlap idempotent.
pub const BASE_COINBASE_SEAM_BLOCK: i64 = 48_428_000;

pub fn validate_request(request: &BatchRequest) -> Result<()> {
    if request.chain_id.trim().is_empty() || request.sources.is_empty() {
        return Err(IngestError::configuration(
            "ingest requires a chain and at least one source",
        ));
    }
    if request.chain_id == "ethereum-mainnet"
        && (request.sources.len() != 1
            || normalized_kind(&request.sources[0].kind) != ProviderKind::Reth)
    {
        return Err(IngestError::configuration(
            "ethereum-mainnet ingest requires one local Reth DB source",
        ));
    }
    if request.chain_id == "base-mainnet" {
        let coinbase = request
            .sources
            .iter()
            .find(|source| normalized_kind(&source.kind) == ProviderKind::Coinbase);
        let rpc = request
            .sources
            .iter()
            .find(|source| normalized_kind(&source.kind) == ProviderKind::Rpc);
        if request.sources.len() != 2 || coinbase.is_none() || rpc.is_none() {
            return Err(IngestError::configuration(
                "base-mainnet ingest requires one Coinbase SQL source and one RPC source",
            ));
        }
        let coinbase = coinbase.expect("checked Coinbase source");
        let rpc = rpc.expect("checked RPC source");
        if coinbase.start_block > BASE_COINBASE_SEAM_BLOCK {
            return Err(IngestError::configuration(format!(
                "base-mainnet Coinbase SQL source must start at or before seam block \
                 {BASE_COINBASE_SEAM_BLOCK}"
            )));
        }
        if rpc.start_block != BASE_COINBASE_SEAM_BLOCK {
            return Err(IngestError::configuration(format!(
                "base-mainnet RPC source must start at seam block \
                 {BASE_COINBASE_SEAM_BLOCK}"
            )));
        }
    }
    if let Some((from, to)) = request.redo_range
        && (from < 0 || from > to)
    {
        return Err(IngestError::configuration("ingest redo range is invalid"));
    }
    Ok(())
}

pub fn sort_sources(sources: &mut [SourceDescriptor]) {
    sources.sort_by_key(|source| match normalized_kind(&source.kind) {
        ProviderKind::Coinbase => 0,
        ProviderKind::Rpc => 1,
        ProviderKind::Reth => 2,
    });
}

pub fn primary_source(sources: &[SourceDescriptor]) -> Result<&SourceDescriptor> {
    sources
        .iter()
        .find(|source| normalized_kind(&source.kind) != ProviderKind::Coinbase)
        .ok_or_else(|| IngestError::configuration("ingest has no chain block provider"))
}

pub fn target_number(source: &SourceDescriptor, heads: &HeadSnapshot) -> i64 {
    if normalized_kind(&source.kind) == ProviderKind::Coinbase {
        BASE_COINBASE_SEAM_BLOCK
    } else {
        heads.latest.number
    }
}

pub fn redo_source_target(source: &SourceDescriptor, range_to: i64) -> i64 {
    if normalized_kind(&source.kind) == ProviderKind::Coinbase {
        range_to
            .min(BASE_COINBASE_SEAM_BLOCK)
            .max(source.start_block)
    } else {
        range_to.max(source.start_block)
    }
}

pub fn publishable_heads(current: &Marker, snapshot: &HeadSnapshot) -> HeadMarkers {
    let safe = snapshot
        .safe
        .as_ref()
        .map(|block| bounded_checkpoint(current, block));
    let finalized = safe.as_ref().and_then(|_| {
        snapshot
            .finalized
            .as_ref()
            .map(|block| bounded_checkpoint(current, block))
    });
    HeadMarkers {
        latest: current.clone(),
        safe,
        finalized,
    }
}

fn bounded_checkpoint(current: &Marker, checkpoint: &Block) -> Marker {
    if checkpoint.number <= current.number {
        marker_from_block(checkpoint)
    } else {
        current.clone()
    }
}

fn marker_from_block(block: &Block) -> Marker {
    Marker {
        number: block.number,
        hash: block.hash.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(chain_id: &str, sources: Vec<SourceDescriptor>) -> BatchRequest {
        BatchRequest {
            chain_id: chain_id.to_owned(),
            sources,
            cursors: Vec::new(),
            redo_range: None,
            resume_current: None,
        }
    }

    fn source(kind: &str, start_block: i64) -> SourceDescriptor {
        SourceDescriptor {
            key: kind.to_owned(),
            kind: kind.to_owned(),
            start_block,
            endpoint: "test".to_owned(),
        }
    }

    #[test]
    fn production_chain_source_contracts_are_exact() {
        assert!(
            validate_request(&request("ethereum-mainnet", vec![source("reth-db", 0)],)).is_ok()
        );
        assert!(validate_request(&request("ethereum-mainnet", vec![source("rpc", 0)],)).is_err());
        assert!(
            validate_request(&request(
                "base-mainnet",
                vec![
                    source("coinbase-sql", BASE_COINBASE_SEAM_BLOCK - 1),
                    source("rpc", BASE_COINBASE_SEAM_BLOCK),
                ],
            ))
            .is_ok()
        );
        assert!(
            validate_request(&request(
                "base-mainnet",
                vec![
                    source("coinbase-sql", BASE_COINBASE_SEAM_BLOCK - 1),
                    source("rpc", BASE_COINBASE_SEAM_BLOCK + 1),
                ],
            ))
            .is_err()
        );
    }

    #[test]
    fn published_checkpoints_are_bounded_by_stored_ingest_progress() {
        let snapshot = HeadSnapshot {
            latest: block(25),
            safe: Some(block(20)),
            finalized: Some(block(15)),
        };

        let below_finalized = publishable_heads(
            &Marker {
                number: 10,
                hash: "block-10".to_owned(),
            },
            &snapshot,
        );
        assert_eq!(below_finalized.safe, below_finalized.finalized);
        assert_eq!(
            below_finalized.finalized.map(|marker| marker.number),
            Some(10)
        );

        let between_checkpoints = publishable_heads(
            &Marker {
                number: 17,
                hash: "block-17".to_owned(),
            },
            &snapshot,
        );
        assert_eq!(
            between_checkpoints.safe.map(|marker| marker.number),
            Some(17)
        );
        assert_eq!(
            between_checkpoints.finalized.map(|marker| marker.number),
            Some(15)
        );
    }

    fn block(number: i64) -> Block {
        Block {
            hash: format!("block-{number}"),
            parent_hash: (number > 0).then(|| format!("block-{}", number - 1)),
            number,
            timestamp_unix_secs: number,
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
        }
    }
}
