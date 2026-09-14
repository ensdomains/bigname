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

Lookup captures the Project publication's block number, block hash, interpreter
content hash, and row generation (`xmin`) before calling providers. A publication
may be `completed` or `running` and trail the stored execution head by one block,
matching `PROJECT_PUBLICATION_LAG_TOLERANCE_BLOCKS`. Revalidation locks that
captured publication and its canonical lineage, rather than requiring Project to
have completed the execution head. Provider calls remain pinned to the captured
execution head; the indexed comparison retains its own projected position.
A changed publication row still refuses the response, including a same-height
republish or a phase-status transition during the provider call. This tolerance
does not eliminate concurrent-state `409` responses or relax route-level checks
that indexed and verified answers share one reported position.

Provider calls use the selected block identity rather than `latest`. Missing
provider configuration, unsupported topology, and unsupported selectors are
reported through the route's explicit unsupported or failed result shapes.

## Resolver-record lookup

ENS verified resolution uses the manifest-admitted Universal Resolver
entrypoint on the deployment profile's Ethereum L1: Mainnet under
`manifests/mainnet`,[^ens-docs-univ][^v1-ur-deploy] Sepolia under
`manifests/sepolia`, where the same proxy address is admitted by the archived
2026-06-29 Sepolia deployment.[^v2-sepolia-ur-proxy][^v1-sepolia-ur-deploy]
Both profiles apply the same route classes, gates, hash pinning, and provider
limits; the profile only selects the chain and therefore the
`BIGNAME_API_CHAIN_RPC_URLS` entry the API needs (`ethereum-mainnet=` or
`ethereum-sepolia=`). Every ENS class below is additionally gated by the selected
[authority arm](glossary.md#authority-epoch): the selected `ens_execution`
manifest's `verified_authority_arms` (`manifests.md` §
`verified_authority_arms`; absent means `["ens_v1"]`) must list the name's
projected arm, or the engine refuses before any provider call and the API
reports `exact_name_authority_not_verifiable` for every requested key. A row
whose authority selection records no arm (a registry-only serving row) is not
arm-scoped. The supported topology classes are:

- exact-surface direct resolution: a name bound through its selected
  `declared_registry_path` binding, on either arm, whose projected exact
  resolver is non-null. Project writes this topology for bound ENS names whose
  binding resource has a record inventory row, copying that row's record
  boundary (`projections.md` § Exact-name projection); a bound row without an
  inventory row has no topology and is `unsupported` with
  `verified_records_not_supported`. On the Mainnet and Sepolia profiles the
  arm gate admits only `ens_v1` names; the `sepolia-hackathon` profile admits
  `ens_v2` names too, and its UniversalResolverV2 resolves them by walking
  root→leaf through `getSubregistry`, keeping the deepest nonzero
  `getResolver`, and accepting an ancestor-selected resolver only when it
  implements ENSIP-10
  `(upstream: .refs/ens_v2/contracts/src/universalResolver/libraries/LibRegistry.sol:L21-L45 @ ens_v2@a971bd64)`
  `(upstream: .refs/ens_v2/contracts/src/universalResolver/UniversalResolverV2.sol:L73-L80 @ ens_v2@a971bd64)`
  `(upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L73-L88 @ ens_v1@91c966f)`.
  A declared ENSv1 mirror resolver stays the exact resolver in the topology:
  that is the address the Universal Resolver reports and the divergence
  comparison is keyed on;
- exact-surface alias resolution with a declared non-empty alias path; and
- exact-surface wildcard-derived resolution with a declared wildcard source
  and matched labels; and
- [Universal Resolver ancestor
  discovery](glossary.md#universal-resolver-ancestor-discovery): exact-surface
  resolution on the profile's Ethereum L1 (Mainnet or Sepolia) with a null
  exact resolver and no alias, linked-subregistry, projected wildcard, or
  transport path, executed through that chain's manifest-admitted Universal
  Resolver at its readable head. This last route has no indexed comparison and retains the exact resolver
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

## Primary-name lookup

The verified primary-name product path supports ENS on coin type `60`. It
performs a fresh reverse lookup at the selected Ethereum position; a projected
`primary_names_current` claim is not required. When a projected claim exists,
the route consults it before live execution so unsupported exact-name coverage
or an unverifiable selected [authority arm](glossary.md#authority-epoch) can
refuse the forward call. An arm is verifiable when the selected `ens_execution`
manifest lists it in `verified_authority_arms` (`manifests.md` §
`verified_authority_arms`; absent means `["ens_v1"]`, the Mainnet and Sepolia
declaration; the `sepolia-hackathon` manifest lists `ens_v2` as well because its
proxy is a UniversalResolverV2). After the reverse leg, the same exact-name
gate applies to the live claim. An absent readable exact-name row admits the
forward call.
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
selected positions, and indexed/live comparison. The indexed side is the same
evaluator the records route serves: an `unsupported` record inventory row
compares as `unsupported`, never as a retained entry value. Reorg handling clears active
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
[^v2-sepolia-ur-proxy]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/UpgradableUniversalResolverProxy.json:L2 @ ens_v2@a971bd64)
[^v1-sepolia-ur-deploy]: (upstream: .refs/ens_v1/deployments/sepolia/UniversalResolver.json:L2 @ ens_v1@91c966f)
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
