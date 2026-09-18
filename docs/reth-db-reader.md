# Direct Reth reader

The reader dependency is Reth v2.5.0 and uses the built-in Mainnet or Sepolia
chain specification for the configured Ethereum chain. Other chains remain
unsupported. The dependency, reference pin, lockfile and Rust 1.98.0 build
toolchain move together. Sepolia uses Reth's genesis and hardfork schedule
(upstream: .refs/reth/crates/chainspec/src/spec.rs:L146 @ reth@189c0df3).

Selecting a chain specification does not by itself check that the datadir holds
that chain: Reth's read-only open builds the factory around whatever
specification it is given
(upstream: .refs/reth/crates/storage/provider/src/providers/database/builder.rs:L95 @ reth@189c0df3)
and reads canonical block hashes from the stored headers, not from the
specification
(upstream: .refs/reth/crates/storage/provider/src/providers/database/mod.rs:L694 @ reth@189c0df3).
The reader therefore checks the datadir itself when it opens it: it compares the
stored canonical hash of block 0 with the selected specification's genesis hash
and refuses to open on a mismatch, with an error that names both hashes and the
configured chain, or when no block 0 header is stored
(`crates/ingest/src/provider/reth_db/enabled.rs`, `verify_stored_chain`). The
check reads one header and does not need the genesis body or receipts, which a
pruned node may no longer hold. It applies to every use of the reader: intake,
verification, the source-transport command and the bounded sample below. It
does not check the node's version or configuration, only which chain the
datadir holds.

Use the reader with a matching v2.5.0 node. Its monitored read-only factory opens
MDBX and static files read-only and RocksDB as a secondary, and refreshes the
secondary and static-file indexes as committed MDBX state changes
(upstream: .refs/reth/crates/storage/provider/src/providers/database/builder.rs:L95 @ reth@189c0df3)
(upstream: .refs/reth/crates/storage/provider/src/providers/database/mod.rs:L292 @ reth@189c0df3).
The reader retains Reth's default read-transaction timeout so a slow read does
not indefinitely hold back the running writer's page reclamation
(upstream: .refs/reth/crates/storage/provider/src/providers/database/builder.rs:L74 @ reth@189c0df3).

## Mount contract

Give the reader a writable, reader-owned wrapper directory containing the
node's `db`, `static_files`, and `rocksdb` storage children. The node data must
remain read-only to the reader except for the existing `db/mdbx.lck`, which
MDBX readers need to update; the reader refuses to open a datadir whose lock
file it cannot open for writing
(`crates/ingest/src/provider/reth_db/enabled.rs`, `validate_datadir`). Reth also
creates `rocksdb-secondary-tmp-<pid>` beside the configured `rocksdb` directory,
so a fully read-only wrapper does not work
(upstream: .refs/reth/crates/storage/provider/src/providers/rocksdb/provider.rs:L415 @ reth@189c0df3).
Reth removes that directory when the reader closes cleanly
(upstream: .refs/reth/crates/storage/provider/src/providers/rocksdb/provider.rs:L726 @ reth@189c0df3);
after a killed reader, delete leftover `rocksdb-secondary-tmp-*` directories
from the wrapper while no reader is running.
When using containers, expose `mdbx.dat` read-only and the existing lock file
writable inside the reader's `db` directory; mount static files and RocksDB
read-only. Never substitute a copied lock file for the writer's actual lock.

`docker-compose.reth-db.yml` implements this contract. A single read-only bind
of the whole datadir cannot work, for the two reasons above, so the overlay
mounts five things on the phase runner:

| Container path | Host source | Mode |
| --- | --- | --- |
| `RETH_DATA_DIR` | `RETH_READER_DIR`, a separate directory owned by the reader's user | read-write |
| `RETH_DATA_DIR/db` | the node's `db` | read-only |
| `RETH_DATA_DIR/static_files` | the node's `static_files` | read-only |
| `RETH_DATA_DIR/rocksdb` | the node's `rocksdb` | read-only |
| `RETH_DATA_DIR/db/mdbx.lck` | the node's `db/mdbx.lck` file | read-write |

The lock-file bind follows one file, not a path. If the node's `mdbx.lck` is
ever deleted and recreated, recreate the reader container so it binds the new
file. Docker creates the empty `db`, `static_files` and `rocksdb` mount points
inside `RETH_READER_DIR` on first start; nothing else belongs there.

The overlay also requires two settings that have no safe default:

- `RETH_READER_USER`, the numeric `uid:gid` the reader runs as. Reth creates
  the MDBX files with mode `0644`
  (upstream: .refs/reth/crates/storage/libmdbx-rs/src/environment.rs:L636 @ reth@189c0df3),
  so only the node's own user can write `mdbx.lck`. Run the reader as that
  user instead of widening the node's file modes. The image keeps `/app` and
  the manifests world-readable for this reason (`Dockerfile`). The same user
  must own `RETH_READER_DIR` and be able to write
  `BIGNAME_PHASE_RUNNER_WRITABLE_PATH`.
- `RETH_NODE_PID_NAMESPACE`, the PID namespace the reader joins:
  `container:<reth container name>` when the node runs in a container, or
  `host` when it runs directly on the host. The reason is explained under
  [Switching Sepolia from local RPC to direct Reth reads](deployment.md#switching-sepolia-from-local-rpc-to-direct-reth-reads).

The reader is a trusted peer of the node, not a process the node is isolated
from. It runs as the node's user, shares the node's PID namespace, and writes
the node's real MDBX lock file, which is coordination state the node depends on
(see below); a reader that damages that file damages the node. The read-only
binds are cooperative, not isolating: they stop a correct reader from writing
node data by mistake, but they do not confine a compromised reader, which on a
host whose process-access checks permit it can reach the node's own mount view,
writable mounts included, through `/proc/<node pid>/root`. Treat the reader's
image and configuration with the same care as the node's. A deployment that
needs the reader confined from the node must use a different design, such as
RPC intake or an enforced credential and security-policy boundary; this overlay
does not provide one.

The reader and the node must run on the same host, against the same local
filesystem. MDBX coordinates readers and the writer through the shared lock file
and process IDs, and refuses a database on a network filesystem
(upstream: .refs/reth/crates/storage/libmdbx-rs/mdbx-sys/libmdbx/mdbx.h:L2023 @ reth@189c0df3).
A copied or snapshotted datadir is a different database, not a live view of the
node.

Reads are memory-mapped: MDBX maps its data file
(upstream: .refs/reth/crates/storage/libmdbx-rs/mdbx-sys/libmdbx/mdbx.c:L20384 @ reth@189c0df3)
and Reth maps each static file
(upstream: .refs/reth/crates/storage/nippy-jar/src/lib.rs:L348 @ reth@189c0df3).
The pages the reader touches are file page cache, and the kernel charges page
cache to the container that populates it. Where the phase runner has a container
memory ceiling (`BIGNAME_PHASE_RUNNER_MEMORY_LIMIT`, added on `main` by
[PR #917](https://github.com/ensdomains/bigname/pull/917)), direct reads count
against that ceiling. Size the ceiling from a measured run of this reader; no
figure is recommended here.

## Bounded sample

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
write PostgreSQL, migrate node storage, or repair the node. It fails to open a
datadir that does not hold the named chain, with the same genesis-hash error as
intake.

Compare the selected canonical block hashes, transaction identities, receipts
and logs against independent RPC before switching intake. Expect one difference
that is not a fault: for a contract-creation transaction that failed, the sample
reports a null `contract_address`
(`crates/ingest/src/provider/reth_db/convert.rs`), while Reth's RPC reports
`contractAddress` as the address the creation would have used, whatever the
receipt status
(upstream: .refs/reth/crates/rpc/rpc-eth-types/src/receipt.rs:L31 @ reth@189c0df3).
The sample prints whole blocks, so any block holding a failed creation shows
this. Stored rows are unaffected: Ingest stores only transactions that emitted a
watched log, and a failed transaction emits none. Include the saved
ingestion boundary in the sample. Compare genesis only when the node retains
its body and receipts; pruned block zero is not a reason to reject an otherwise
complete retained sample. The floor and the node's snapshot
height alone do not prove complete retained receipts. The persisted database
head can trail the node's in-memory RPC head.

## Interpreter content hash

This build rotates the
[interpreter content hash](glossary.md#interpreter-content-hash). Two of its
inputs change: the Reth upgrade moves the seven fingerprinted Alloy crates
(`alloy-dyn-abi`, `alloy-primitives`, `alloy-sol-macro`,
`alloy-sol-macro-expander`, `alloy-sol-macro-input`, `alloy-sol-type-parser` and
`alloy-sol-types`, listed in `crates/content-hash/src/lockfile.rs`) from 1.5.7
to 1.7.3 in `Cargo.lock`, and the Rust 1.98 update edits
`crates/interpret/src/recompute.rs`, which is a hashed source file
(`crates/content-hash/src/compute.rs`). An existing deployment therefore needs
the full-history Interpret and Project redo that
[interpretation replay](storage.md#interpretation-replay) requires for any
rotation, finished before the matching API serves. Follow the runbook's
[planned migration and fingerprint boundary](runbooks/production-docker.md#planned-migration-and-fingerprint-boundary).
This applies to every chain the deployment indexes, including Mainnet
deployments that never use the Sepolia reader. Provider switching does not
authorize dropping raw facts or resetting an ingestion checkpoint.
