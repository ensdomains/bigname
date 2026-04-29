use bigname_execution::ChainRpcUrls;
use sqlx::PgPool;

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) phase: &'static str,
    pub(crate) pool: PgPool,
    pub(crate) chain_rpc_urls: ChainRpcUrls,
}
