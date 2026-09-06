# Verified Lookup

Verified lookup is request-scoped schema-v2 behavior. The API reads declared
topology and phase projections, may call an admitted chain provider, compares
the answer with indexed state where the route requires it, and returns the
result without writing a reusable cache outcome or durable execution trace.

The only serving-path write is the guarded
[resolution divergence ledger](glossary.md#resolution-divergence-ledger). It is
an operational observation of a direct live/indexed disagreement, not a result
cache, projection, or source of truth.

## Read planes

The API keeps these meanings separate:

- `indexed` reads the phase projection and record inventory only;
- `verified` attempts the admitted schema-v2 lookup path for the requested
  selector or primary-name tuple; and
- `auto` uses a satisfying indexed answer and attempts verified lookup only for
  requested selectors that indexed state cannot satisfy.

Verified lookup never backfills `record_inventory_current` or
`primary_names_current`. Project owns those rows. A provider answer affects only
the current response and, for guarded direct comparisons, divergence-ledger
state.

## Snapshot and canonicality

Before lookup, the API selects a readable project publication and exact chain
positions from `bigname_phase`. Every admitted projection row must be at or
before that selection and must resolve through `bigname_phase.chain_lineage`.
The API revalidates the selected project generation before returning. A moved
head, mismatched hash, future publication, missing canonical lineage, or
interpreter-content-hash mismatch returns `409 stale`; it does not fall back to
an answer at another position.

Provider calls use the selected block identity rather than `latest`. Missing
provider configuration, unsupported topology, and unsupported selectors are
reported through the route's explicit unsupported or failed result shapes.

## Resolver-record lookup

ENS verified resolution uses the manifest-admitted Universal Resolver
entrypoint.[^ens-docs-univ][^v1-ur-deploy] The supported topology classes are:

- exact-surface direct resolution;
- exact-surface alias resolution with a declared non-empty alias path; and
- exact-surface wildcard-derived resolution with a declared wildcard source
  and matched labels; and
- [Universal Resolver ancestor
  discovery](glossary.md#universal-resolver-ancestor-discovery): Ethereum
  Mainnet exact-surface resolution with a null exact resolver and no
  alias, linked-subregistry, projected wildcard, or transport path, executed
  through the manifest-admitted Universal Resolver at the readable Ethereum
  head. This last route has no indexed comparison and retains the exact resolver
  as null in the API response. The entrypoint walks to the nearest nonzero
  ancestor and accepts it only when it implements ENSIP-10
  `(upstream: .refs/ens_v1/contracts/universalResolver/RegistryUtils.sol:L25-L38 @ ens_v1@91c966f)`
  `(upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L63-L88 @ ens_v1@91c966f)`.

Ancestor-selected non-alias paths, linked-subregistry ancestor selection,
transport-assisted ENS paths, and CCIP-participating ENS paths remain explicit
`unsupported` unless a retained route contract says otherwise.

Basenames verified resolution admits the exact-surface transport-assisted
direct path through the manifest-selected L1 Resolver. That contract may use
`OffchainLookup` and `resolveWithProof` for non-`base.eth` requests.[^bn-readme-l22][^bn-readme-l69][^bn-readme-l70][^bn-l1resolver-l154][^bn-l1resolver-l173][^bn-l1resolver-l191]
Other Basenames path classes remain explicit `unsupported`.

Requests identify records with normalized selector keys such as
`addr:<coin_type>`, `text:<key>`, and `contenthash`. Decimal coin types are
canonicalized to their unsigned 64-bit decimal spelling before deduplication;
out-of-range values are invalid input. This is an intentional narrowing of the
upstream resolver `uint256 coinType` surface and is recorded in
[`upstream.md`](upstream.md).[^v1-iaddressres-l14][^bn-addrresolver-l93]

Selector-local results use `success`, `not_found`, `unsupported`, or `failed`.
One unsupported selector does not discard successful answers for other
selectors in the same request.

For Universal Resolver ancestor discovery, `ResolverNotFound(bytes)` is a live
`not_found` with reason `resolver_not_found` only when its embedded DNS name
equals the request name. Other reverts fail execution, and `OffchainLookup`
stays unsupported because ENS verified record resolution does not follow CCIP-Read.
Every successfully decoded call for one name at one block must identify the
same effective resolver. A `ResolverNotFound` outcome cannot coexist with a
successfully decoded effective resolver; either inconsistency fails the request
closed. Ordinary selector-local failed or unsupported outcomes remain mixed per
key.
Provider results remain request-scoped and are not cached or copied into a
projection.

## CCIP-Read gateway transport

A gateway URL is chosen by the contract that reverts, not by bigname, so the
serving path treats it as untrusted input. Requests are bounded before the URL
reaches the network:

- **Scheme.** Only `http` and `https` are attempted. Anything else fails the
  gateway before a request is sent.
- **Redirects.** Not followed. A `3xx` is reported as an unsuccessful gateway
  status, so a URL that satisfied any origin check cannot bounce the request to
  a different host.
- **Response size.** The body read is capped at 1 MiB; a longer response fails
  that gateway rather than streaming into the request.
- **Fan-out and time.** Each gateway HTTP request has a 1000 ms connect and a
  1500 ms total timeout; those bound one request, not the lookup. For each
  CCIP-Read step the resolver's URL list is tried in order, at most four URLs,
  and a timed-out or unreachable URL falls through to the next; a resolution
  follows at most four steps, each followed by one JSON-RPC callback bounded by
  `BIGNAME_API_RPC_TIMEOUT_MS`. The `x-batch-gateway:true` form is followed
  for at most 8 inner requests, runs at most 4 of them at a time in request
  order, and lets their decoded responses total at most the same 1 MiB; a
  longer batch fails the record in band before any request is launched, and a
  batch whose responses pass that total fails it during the fan-out. A batch
  the Universal Resolver builds holds one lookup per call it was asked to
  make: one for a plain resolver call, one per entry of a `multicall()`
  (upstream: .refs/ens_v1/contracts/universalResolver/ResolverCaller.sol:L98-L122 @ ens_v1@91c966f;
  upstream: .refs/ens_v1/contracts/ccipRead/CCIPBatcher.sol:L42-L53 @ ens_v1@91c966f),
  and bigname asks for one record per call, so the cap sits well above what a
  legitimate batch carries. Across all
  of that, the gateway side of one CCIP-Read resolution shares a single 6 s
  budget: it is initialised once, every gateway request runs under whatever
  remains, and each completed request's elapsed time is subtracted, so later
  steps and URLs get only the remainder — four steps never get 6 s each. The
  budget is measured around the gateway requests only; the callback
  `eth_call`s between steps are bounded by `BIGNAME_API_RPC_TIMEOUT_MS` and do
  not draw on it, so a slow provider cannot starve a healthy gateway. When the
  budget runs out the record fails in band as `resolver_call_failed`, the same
  way a configured RPC timeout does, whatever phase the in-flight gateway
  request was in — a connection that had not completed when the budget expired
  fails in band too, so the connect-phase whole-request `500` applies only to a
  request that fails within its own per-request timeouts — instead of holding the request until the
  30 s `BIGNAME_API_REQUEST_TIMEOUT_MS` fails it as a whole. The worst case for
  one record is therefore 6 s of gateway time in total plus up to four
  callbacks at the RPC timeout.

**What is deliberately not enforced in process: destination host or IP.** The
gateway may resolve to any address the container can route to, including link
local and cluster internal ranges. Who chooses that destination differs by path.

For Basenames the URL set is a single gateway URL read from the L1 resolver's
contract-level `url` storage
(upstream: .refs/basenames/src/L1/L1Resolver.sol:L28-L29 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L1/L1Resolver.sol:L171-L173 @ basenames@1809bbc).
Only the contract owner can change it, through `setUrl` under `onlyOwner`, which
emits `UrlChanged`
(upstream: .refs/basenames/src/L1/L1Resolver.sol:L92-L100 @ basenames@1809bbc);
no name owner or caller can substitute a URL on this path, and bigname calls the
manifest-declared L1 resolver address directly and rejects an `OffchainLookup`
from any other sender. The URL is fixed only until that owner rotates it —
upstream ships an operator script for exactly that
(upstream: .refs/basenames/script/configure/SetL1ResolverUrl.s.sol:L13-L16 @ basenames@1809bbc)
— and bigname does not index `UrlChanged`, so a rotation is observed only
through the live revert. Egress policy for this path should pin the host the
contract currently returns, not assume it is immutable.

For the ENS primary-name path the reverse leg is two plain `eth_call`s —
registry `resolver(node)`, then `name(node)` on the reverse resolver — that never
follow CCIP-Read, so the reverse resolver itself cannot supply URLs. The forward
`addr:60` leg is different: it calls the Universal Resolver's `resolve(name,
data)` with CCIP-Read following enabled, and the Universal Resolver forwards the
target resolver's `OffchainLookup.urls` unchanged — directly when the resolver
supports ERC-7996
(upstream: .refs/ens_v1/contracts/ccipRead/CCIPReader.sol:L72-L94 @ ens_v1@91c966f),
or wrapped in a batch-gateway request otherwise
(upstream: .refs/ens_v1/contracts/ccipRead/CCIPBatcher.sol:L107-L126 @ ens_v1@91c966f),
which bigname unwraps and fetches itself. Because the Universal Resolver wraps
the calldata in ENSIP-10 `resolve(name, data)` for any resolver that advertises
`IExtendedResolver`
(upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L73-L88 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L330-L337 @ ens_v1@91c966f),
the URLs are chosen by whoever controls the forward name's resolver, or a
wildcard resolver on one of its ancestors. An address that controls its reverse
record — the address itself, an ENS operator it approved, a registrar
controller, or the owner of an `Ownable` contract at that address
(upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L40-L49 @ ens_v1@91c966f)
— chooses only which name the forward leg looks up; in practice that is enough,
because it can point the claim at a name whose resolver it also controls.
bigname applies no allowlist to the resolver address or the gateway host before
following; the only pre-fetch check is that the `OffchainLookup.sender` equals
the Universal Resolver. Egress restriction for that case belongs to network
policy around the API container, not to this client, and no such policy is
described in the deployment docs today. Treat it as a prerequisite before `/v2`
is admitted at the public edge, and see
[`production.md`](production.md) for the current edge posture.

## Primary-name lookup

The verified primary-name product path supports ENS on coin type `60`. It
performs a fresh reverse lookup at the selected Ethereum position; a projected
`primary_names_current` claim is not required. When a projected claim exists,
the route consults it before live execution so unsupported exact-name coverage
or an unverifiable selected [authority arm](glossary.md#authority-epoch) can
refuse the forward call. After the reverse leg, the same exact-name gate applies
to the live claim. An absent readable exact-name row admits the forward call.
The live reverse claim must already be byte-normalized, and the route accepts it
only when the forward address matches the requested address. A reverse claim
alone is not proof of a primary name.[^v1-aur-l217][^v1-aur-l263][^v1-aur-l269]

Invalid or non-normalized claims remain non-primary. A successful claim whose
publication has no matching canonical phase-lineage row makes snapshot-selected
lookup stale. Other namespace and coin-type tuples are explicit unsupported
unless the API contract admits them.

Primary-name lookup writes neither projections nor divergence observations.

## Divergence ledger

The lookup engine may call fixed-`search_path`, security-definer functions that
revalidate the selected lookup state and then create, refresh, or clear an
active resolution-divergence observation. The API role has `EXECUTE` on those
functions but no direct write privilege on the ledger table.

An observation records the logical name, resolver identity, request kind,
selected positions, and indexed/live comparison. Reorg handling clears active
observations whose recorded positions include an orphaned block. The ledger is
diagnostic evidence only; indexed projection reads and verified provider reads
do not consume it as an answer.

## Removed legacy artifacts

The old execution crate, `execution_traces`, `execution_steps`, and
`execution_cache_outcomes` have been deleted. There is no worker trace inspector,
persisted-execution explain route, legacy cache invalidator, or serving fallback
to those tables. Unsupported behavior must remain explicit rather than being
hidden behind a stale cached result.

---

[^ens-docs-univ]: <https://docs.ens.domains/resolvers/universal/> (official Universal Resolver proxy)
[^v1-ur-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/UniversalResolver.json:L2 @ ens_v1@91c966f)
[^v1-iaddressres-l14]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddressResolver.sol:L14 @ ens_v1@91c966f)
[^v1-aur-l217]: (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L217 @ ens_v1@91c966f)
[^v1-aur-l263]: (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L263 @ ens_v1@91c966f)
[^v1-aur-l269]: (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L269 @ ens_v1@91c966f)
[^bn-readme-l22]: (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc)
[^bn-readme-l69]: (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc)
[^bn-readme-l70]: (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)
[^bn-l1resolver-l154]: (upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc)
[^bn-l1resolver-l173]: (upstream: .refs/basenames/src/L1/L1Resolver.sol:L173 @ basenames@1809bbc)
[^bn-l1resolver-l191]: (upstream: .refs/basenames/src/L1/L1Resolver.sol:L191 @ basenames@1809bbc)
[^bn-addrresolver-l93]: (upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L93 @ basenames@1809bbc)
