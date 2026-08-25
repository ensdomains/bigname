#[cfg(feature = "reth-db")]
mod enabled;
#[cfg(not(feature = "reth-db"))]
mod unavailable;

/// Top-level storage directories opened by the Reth database provider.
pub const RETH_DB_OPENED_STORAGE_CHILDREN: [&str; 3] = ["db", "static_files", "rocksdb"];

#[cfg(feature = "reth-db")]
pub use enabled::RethDbProvider;
#[cfg(not(feature = "reth-db"))]
pub use unavailable::RethDbProvider;
