use anyhow::Result;

use crate::{
    cli::{BackfillArgs, OpsCatchupArgs, RepairEnsV1TextRecordsArgs, RunArgs},
    provider::ProviderRegistry,
};

pub(crate) trait ProviderSourceArgs {
    fn provider_registry(&self) -> Result<ProviderRegistry>;
}

macro_rules! impl_provider_source_args {
    ($($args:ty),+ $(,)?) => {
        $(
            impl ProviderSourceArgs for $args {
                fn provider_registry(&self) -> Result<ProviderRegistry> {
                    ProviderRegistry::from_sources_with_code_fallbacks(
                        &self.chain_rpc_urls,
                        &self.chain_reth_db_sources,
                        &self.chain_rpc_code_fallback_urls,
                    )
                }
            }
        )+
    };
}

impl_provider_source_args!(
    RunArgs,
    BackfillArgs,
    OpsCatchupArgs,
    RepairEnsV1TextRecordsArgs,
);
