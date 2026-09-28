/// ENSv1 reverse resolvers that answer `name()` without emitting a record event, so the claim can
/// only be learned by calling them. The reference indexer records the same for this deployment
/// (upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L311 @ ensnode@2017ae6)
/// (upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L316 @ ensnode@2017ae6).
/// This list selects which reverse claims get hydrated and therefore which
/// primary claim rows are hydrated, so it lives inside the interpreter content hash's watched
/// roots rather than in a serving crate.
pub(crate) const EVENT_SILENT_REVERSE_RESOLVER_ADDRESSES: &[&str] =
    &["0xa2c122be93b0074270ebee7f6b7292c7deb45047"];

// Manifest-admitted, non-proxy legacy public resolvers with text storage.
// (upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L71 @ ens_app_v3@7175858)
// (upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L88 @ ens_app_v3@7175858)
// (upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L105 @ ens_app_v3@7175858)
// (upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L121 @ ens_app_v3@7175858)
pub(crate) const TEXT_RESOLVERS: &[&str] = &[
    "0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41",
    "0xdaaf96c344f63131acadd0ea35170e7892d3dfba",
    "0x226159d592e2b063810a10ebf6dcbada94ed68b8",
    "0x5ffc014343cd971b7eb70732021e26c35b744cc4",
];
