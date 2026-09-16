use anyhow::{Result, bail};

use crate::provider::{Block, BlockBundle, HeadSnapshot, Log, ResolvedBlock, SelectedPayloads};

#[derive(Clone)]
pub struct RethDbProvider;

impl RethDbProvider {
    pub fn new(_chain: &str, _datadir: &str) -> Result<Self> {
        bail!("Reth DB support was not compiled; enable the reth-db feature")
    }

    pub async fn heads(&self) -> Result<HeadSnapshot> {
        bail!("Reth DB support was not compiled")
    }

    pub async fn earliest_available_block(&self) -> Result<i64> {
        bail!("Reth DB support was not compiled")
    }

    pub async fn resolve(&self, _numbers: &[i64]) -> Result<Vec<ResolvedBlock>> {
        bail!("Reth DB support was not compiled")
    }

    pub async fn headers(&self, _blocks: &[ResolvedBlock]) -> Result<Vec<Block>> {
        bail!("Reth DB support was not compiled")
    }

    pub async fn logs(
        &self,
        _blocks: &[ResolvedBlock],
        _addresses: &[String],
        _topics: &[String],
        _topic1s: &[String],
    ) -> Result<Vec<Log>> {
        bail!("Reth DB support was not compiled")
    }

    pub(crate) async fn transaction_payloads(
        &self,
        _blocks: &[ResolvedBlock],
        _logs: &[Log],
    ) -> Result<SelectedPayloads> {
        bail!("Reth DB support was not compiled")
    }

    pub async fn bundles(&self, _blocks: &[ResolvedBlock]) -> Result<Vec<BlockBundle>> {
        bail!("Reth DB support was not compiled")
    }
}
