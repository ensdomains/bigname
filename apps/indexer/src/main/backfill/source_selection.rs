use super::BackfillSourceKind;

pub(crate) fn selected_backfill_source(
    requested: BackfillSourceKind,
    chain: &str,
    coinbase_sql_configured: bool,
) -> BackfillSourceKind {
    match requested {
        BackfillSourceKind::Auto if is_base_chain(chain) && coinbase_sql_configured => {
            BackfillSourceKind::CoinbaseSql
        }
        BackfillSourceKind::Auto => BackfillSourceKind::HashPinned,
        source => source,
    }
}

pub(crate) fn is_base_chain(chain: &str) -> bool {
    matches!(chain, "base-mainnet" | "base" | "base-sepolia")
}
