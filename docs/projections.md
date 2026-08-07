# Projections

[Projections](glossary.md#projection) are rebuildable read models over canonical
identity and [normalized events](glossary.md#normalized-event). Wire shapes live
in [`api-v2.md`](api-v2.md) and [`api-v2-routes.md`](api-v2-routes.md); identity
and event semantics live in [`architecture.md`](architecture.md); persistence
rules live in [`storage.md`](storage.md).

The schema-v2 Project phase is the only projection writer. Adapters and the API
never write projection rows.

## Live maintenance

For each chain, the phase runner publishes a provider head and advances or
redoes Interpret and Project through that exact head. A displaced readable
suffix stamps both downstream phases. Interpret redo also stamps Project for
the actual derived range, so a same-hash interpretation repair cannot leave an
older projection generation published.

Project includes existing current rows whose cited input is no longer readable,
allowing a winning fork to retract losing-fork output. It stages the affected
scope in connection-local tables and publishes the related projection rows and
phase state transactionally.

After event-derived publication, configured Ethereum
[hydration](glossary.md#hydration) may refresh:

- an existing ENS/60 reverse tuple whose current resolver is an admitted
  event-silent resolver; and
- supported ENSv1 `text:<key>` entries whose normalized event retained the key
  but not the value.[^ensnode-legacy-revresolver-l311][^ensnode-legacy-revresolver-l316][^ensnode-legacy-text-l356]

Hydration uses the exact number and hash from `chain_heads`, revalidates that
head in the publication transaction, and never calls provider `latest`.
Failed calls restore the event-derived baseline and keep Project retryable. It
does not write raw facts, identity rows, normalized events, reusable execution
outcomes, or durable traces.

## Rules

- Every row carries stable identity, provenance, manifest version, support, and
  chain-position or target-publication context.
- Exact-name reads resolve snapshot selection first, then join only rows
  admitted at those positions.
- A row published at an earlier target may serve a later selected head when the
  affected scope has not changed. Equal-height admission requires the selected
  hash to match.
- Readers fail closed when selected positions, canonical lineage, or the current
  Project generation cannot be proven. They do not patch a missing row from raw
  facts, adapter internals, provider data, or a newer projection.
- Resource-keyed projections consume an event only when its `resource_id`
  resolves to a readable identity row.
- Coverage and support are explicit. They are never inferred from row presence
  or a historical ingest range.
- Verified provider answers are request-scoped lookup output, not projection
  state.

## Families

| Projection | Primary key | Primary read |
| --- | --- | --- |
| `name_current` | `logical_name_id` | exact-name lookup and search |
| `address_names_current` | `(address, logical_name_id, relation)` | address-to-names and reverse lookup |
| `children_current` | parent/child identity plus class | direct and classified child collections |
| `permissions_current` | resource, subject, and scope | resource permissions and role summaries |
| `permissions_current_resource_summary` | `resource_id` | permission support and authority summary |
| `resolver_current` | chain and resolver address | resolver overview |
| `record_inventory_current` | resource plus record boundary key | indexed record inventory and values |
| `primary_names_current` | address, coin type, and namespace | declared primary-name claims |

`surface_bindings` remains identity history rather than a `_current`
projection. Exact-name reads select the active binding for the surface and
selected position.

## Exact-name projection

`name_current` assembles current registration, authority, control, resolver,
coverage, and display context for one logical name. Ordinary lifecycle changes
within the same authority anchor preserve `resource_id`; wrap, unwrap,
re-registration, or another authority-anchor change follows the identity rules
in [`architecture.md`](architecture.md#identity-strategy).

ENSv1 wrapper lifecycle and fuse effects are projected from canonical wrapper
facts. During registrar grace, the holder and lifecycle state remain visible,
while owner modification, transfer, and effective-controller membership stop at
grace start.[^v1-wrapper-grace-expiry][^v1-wrapper-grace-authority] Expired
wrapper state contributes no effective holder powers.[^v1-wrapper-expired]

For the ENSv2 post-audit Sepolia deployment profile, declared exact-name rows
come from the admitted registry and registrar families. Out-of-profile resolver,
reverse, primary-name, mainnet, and execution behavior does not become exact-name
truth.

For Basenames, exact-name truth comes from the admitted Base registry,
registrar, and resolver families. Base primary-claim intake and L1 compatibility
transport do not create alternate exact-name rows.[^bn-readme-l70][^v1-l2rev-base-deploy][^v1-l2rev-event]

## Address and child collections

Address-to-name collections use `address_names_current` membership and join
`name_current` for display, sort, and compact record fields. Relation vocabulary
is `registrant`, `token_holder`, and `effective_controller`. Surface is the
default unit; resource deduplication is explicit.

`children_current` stores direct and classified child relations. For registry
events that expose only a labelhash, Project composes the child name from a
verified label preimage when one exists and leaves every name column null when
none does — the labelhash and child node are proven, the label is not. Reads
name such a child by the [non-name form](glossary.md#non-name-form)
`[<labelhash-without-0x>].<parent-name>`, built from the parent's stored
spelling, and lower-case it for the normalized form. A preimage whose label
bytes are not valid UTF-8, or contain a NUL, is a third state: Project stores
the whole child name as raw bytes with no decoded form, and reads escape-encode
that whole string, parent portion included. Neither shape is an addressable
name. A preimage improves readability but does not create ownership or
exact-name authority. ENSv2 direct and linked
children derive from admitted graph events rather than token enumeration.[^v1-registry-l45][^v1-registry-l82][^v2-events-l49][^v2-events-l75]

## History

History routes read normalized events, not a current projection cache. Surface,
resource, and address scopes are filters over the same canonical event set.
Projection rows may supply readable names for result decoration, but the API
does not synthesize history from current state.

## Permissions

`permissions_current` is resource-anchored and preserves subject, scope,
effective powers, provenance, and chain positions. The companion resource
summary distinguishes authoritative empty enumeration from unsupported or
partial permission support.

For ENSv1 wrapper-backed resources, fuse state alone does not manufacture a
holder grant. A separately observed compatible holder grant is masked by the
current lifecycle and fuse rules. For ENSv2, permissions remain keyed by the
upstream resource linked to bigname `resource_id`, not by token ID.[^v2-iperm-l57][^v2-pr-l261][^v2-pr-l351]

Unknown or inconsistent summary vocabulary is a storage error. Product routes
fail closed rather than converting it into broader support.

## Resolver and records

`resolver_current` summarizes one resolver contract across readable bound names,
aliases, roles, record evidence, and normalized events. It is diagnostic and
does not replace exact-name topology.

`record_inventory_current` records the selectors observed for the current
resource and boundary, explicit gaps, unsupported families, and any retained
indexed values. Resolver-local events are accepted only under the manifest and
current-resolver rules documented in [`manifests.md`](manifests.md).

For ENSv1, an admitted current resolver may contribute supported address, text,
and contenthash inventory. An unlisted or unsupported resolver family stays
explicitly unsupported. For ENSv2, current-emitter version evidence may define a
boundary while the unadmitted resolver profile still publishes no record
values. Basenames record facts remain gated by the admitted Base resolver
profile.

`GET /v2/names/{name}/records` reads this inventory for `indexed` behavior.
`verified` and `auto` may use fresh schema-v2 lookup as described in
[`execution.md`](execution.md); they never read a legacy execution cache.

## Primary names

`primary_names_current` stores declared claim state only. Supported statuses are
`success`, `not_found`, `unsupported`, and `invalid_name`. A successful row keeps
the raw claim and whether its bytes already equal the normalized claim. Project
does not persist a verified-primary result or trace identity.

Current-head hydration for an admitted event-silent ENSv1 reverse resolver may
refresh an existing ENS/60 claim tuple at the exact published Ethereum head. It
does not create a normalized event or verified result. Provider failure restores
the event-derived row and keeps Project retryable.

Verified ENS/60 primary-name status is computed per request by schema-v2 lookup.
It requires the declared claim and a matching forward address; tuple presence
alone does not prove primary status.

## Reorg and redo

Canonicality change, manifest change, or interpreted-content replacement stamps
the affected Project range. Project rebuilds the affected scope in dependency
order and publishes one coherent generation. There is no worker invalidation
queue, apply cursor, replay-version fence, durable stage table, replay marker,
dead-letter queue, or cache invalidation side effect.

`phase-runner rewind` selects an exact stored readable ancestor, marks the
displaced suffix orphaned through normal head publication, and stamps downstream
redo. Historical API reads serve only when eligible projection materialization
exists for the selected positions; they never overwrite newer current rows or
fall forward to current state.

An interpreter content-hash rotation requires a full-history Interpret and
Project walk. Phase state and API admission refuse to mix output from different
compiled hashes.

## Index baseline

Indexes follow measured serving queries. Baseline access paths cover exact-name
identity, address relation membership and pagination, parent-child collections,
resource permissions, resolver identity, record-inventory boundaries, primary
claim tuples, normalized-event history, and phase lineage/head selection.
Adding a compact route may justify another measured index; it does not create a
new truth family.

## Ownership

- Interpret and adapters emit identity, discovery, and normalized events.
- Project reads canonical interpreted input and owns every projection write.
- The API reads projections and request-scoped lookup output.
- Storage exposes typed reads and phase publication boundaries; it does not
  grant adapters or API handlers a projection write shortcut.

---

[^bn-readme-l70]: (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)
[^v1-l2rev-base-deploy]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L2 @ ens_v1@91c966f)
[^v1-l2rev-event]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L98 @ ens_v1@91c966f)
[^v1-registry-l45]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L45 @ ens_v1@91c966f)
[^v1-registry-l82]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L82 @ ens_v1@91c966f)
[^v1-wrapper-grace-expiry]: (upstream: .refs/ens_v1/contracts/wrapper/README.md:L69 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L806 @ ens_v1@91c966f)
[^v1-wrapper-grace-authority]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L218 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L221 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L820 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L828 @ ens_v1@91c966f)
[^v1-wrapper-expired]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L852 @ ens_v1@91c966f)
[^v2-events-l49]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L56 @ ens_v2@ccaeb58)
[^v2-events-l75]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L88 @ ens_v2@ccaeb58)
[^v2-iperm-l57]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L71 @ ens_v2@ccaeb58)
[^v2-pr-l261]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L301 @ ens_v2@ccaeb58)
[^v2-pr-l351]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L360 @ ens_v2@ccaeb58)
[^ensnode-legacy-text-l356]: (upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L356 @ ensnode@2017ae6) (upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L364 @ ensnode@2017ae6)
[^ensnode-legacy-revresolver-l311]: (upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L311 @ ensnode@2017ae6)
[^ensnode-legacy-revresolver-l316]: (upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L316 @ ensnode@2017ae6)
