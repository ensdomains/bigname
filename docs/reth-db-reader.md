# Direct Reth reader

The reader dependency is Reth v2.5.0 and uses the built-in Mainnet or Sepolia
chain specification for the configured Ethereum chain. Other chains remain
unsupported. The dependency, reference pin, lockfile and Rust 1.98.0 build
toolchain move together. Sepolia uses Reth's genesis and hardfork schedule
(upstream: .refs/reth/crates/chainspec/src/spec.rs:L146 @ reth@189c0df3).

Use the reader with a matching v2.5.0 node. Its monitored read-only factory opens
MDBX and static files read-only and RocksDB as a secondary, and refreshes the
secondary and static-file indexes as committed MDBX state changes
(upstream: .refs/reth/crates/storage/provider/src/providers/database/builder.rs:L95 @ reth@189c0df3)
(upstream: .refs/reth/crates/storage/provider/src/providers/database/mod.rs:L292 @ reth@189c0df3).
The reader retains Reth's default read-transaction timeout so a slow read does
not indefinitely hold back the running writer's page reclamation
(upstream: .refs/reth/crates/storage/provider/src/providers/database/builder.rs:L74 @ reth@189c0df3).

Give the reader a writable, reader-owned wrapper directory containing the
node's `db`, `static_files`, and `rocksdb` storage children. The node data must
remain read-only to the reader except for the existing `db/mdbx.lck`, which
MDBX readers need to update. Reth also creates
`rocksdb-secondary-tmp-<pid>` beside the configured `rocksdb` directory, so a
fully read-only wrapper does not work
(upstream: .refs/reth/crates/storage/provider/src/providers/rocksdb/provider.rs:L415 @ reth@189c0df3).
When using containers, expose `mdbx.dat` read-only and the existing lock file
writable inside the reader's `db` directory; mount static files and RocksDB
read-only. Never substitute a copied lock file for the writer's actual lock.

Build and run the bounded operator sample:

```sh
cargo build -p bigname-ingest --features reth-db --example reth-db-smoke --locked
target/debug/examples/reth-db-smoke ethereum-sepolia /reader/reth 11550000,11550001
```

The final optional argument is a comma-separated list of topic0 hashes. The
sample accepts at most 32 explicit block numbers, reads through the ingestion
provider, and emits one JSON object with persisted heads, the optimistic
retention floor, headers, full transactions, receipts, logs, optional filtered
logs, and elapsed milliseconds. Binary fields are byte arrays. It rechecks the
selected canonical hashes before returning and fails on incomplete receipts.
This is a storage read: it does not run ingestion, change the source descriptor,
write PostgreSQL, migrate node storage, or repair the node.

Compare the selected canonical block hashes, transaction identities, receipts
and logs against independent RPC before switching intake. Include the saved
ingestion boundary in the sample. Compare genesis only when the node retains
its body and receipts; pruned block zero is not a reason to reject an otherwise
complete retained sample. The floor and the node's snapshot
height alone do not prove complete retained receipts. The persisted database
head can trail the node's in-memory RPC head.

The Reth upgrade also updates the Alloy decode dependencies fingerprinted by
the interpreter content hash. Treat the resulting hash change through the
existing replay rules; provider switching does not authorize dropping raw
facts or resetting an ingestion checkpoint.
