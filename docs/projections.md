# Projections

[Projections](glossary.md#projection) are rebuildable read models over canonical
identity and [normalized events](glossary.md#normalized-event). Wire shapes live
in [`api-v1.md`](api-v1.md) and [`api-v1-routes.md`](api-v1-routes.md); identity
and event semantics live in [`architecture.md`](architecture.md); persistence
rules live in [`storage.md`](storage.md).

The schema-v2 Project phase is the only projection writer. Adapters and the API
never write projection rows.

## Live maintenance

For each chain, the phase runner publishes a provider head and advances or
redoes Interpret and Project through that exact head. Interpret redo stamps
Project for the affected range, including a same-hash interpretation repair.
Project updates the [owned key families](glossary.md#owned-key-family) directly;
there is no second batch builder or serving-source switch.

A normal block commits its changed family keys, before-images and
[family marker](glossary.md#family-marker) together. Readers compose names,
records, permissions, resolver collections and primary claims from that
publication. They require a live marker with the current interpreter content
hash and readable lineage, and apply the route's selected-position and redo
fences. A reset rebuild remains unavailable while its marker is
`bootstrap_pending`. A failure leaves the last committed prefix intact and
fails the Project run; it does not publish partially reduced keys.

The reducers consume activated canonical normalized events in
[canonical event order](glossary.md#canonical-event-order), identity bindings,
and admitted manifests. Clears remain facts: a zero pointer, revoked approval
or empty record can replace an earlier value without deleting its key.
Name and resource changes update their own keys; reads combine the current
keys instead of republishing every name that shares a resolver. The compact
name summary and address indexes are derived within publication as described
[below](#owned-key-families). Historical child-registration membership is
retained separately for [history](#child-registration-events).

Reorg repair restores journalled before-images and replays the replacement
branch. A content-hash change or unavailable older journal requires a rebuild
from retained canonical interpreted input. Each block or rebuild range
revalidates its predecessor, input revision, phase redo state and publication
context before committing. The [reorg rules](#reorg-and-redo) and
[publication mechanics](#owned-key-families) describe resumption and limits.

Every Project SQL statement starts with a
[statement identifier](glossary.md#statement-identifier). The current family
metrics report run duration, block-transaction duration, marker lag and
duplicate anomalies; see the [monitoring runbook](runbooks/pipeline-monitoring.md).

### Follow-only hydration

Configured Ethereum Mainnet follow blocks may refresh an existing ENS/60
reverse tuple on an admitted event-silent resolver, and supported ENSv1
`text:<key>` entries whose normalized event retained the key but not the
value.[^ensnode-legacy-revresolver-l311][^ensnode-legacy-revresolver-l316][^ensnode-legacy-text-l356]
This [hydration](glossary.md#hydration) is a Project-owned overlay on the
event-derived baseline. It writes no raw facts, normalized events, verified
results, reusable execution outcomes or traces.

A short preparation transaction previews the block with the normal reducers,
including resolver pointers, classification and records, and closes before RPC.
Calls use the exact block number and hash being published, never provider
`latest`. The publication transaction revalidates the predecessor, input
revision, canonical block and each result's selected identity before accepting
it. Overlay changes are journalled with the ordinary family rows. Failed calls
remove the overlay and the block still publishes; a later follow block retries.
Missing required RPC configuration refuses an eligible configured follow run.
Replay, rebuild and rebuild ranges make no hydration calls. Undo can restore a
previous overlay; reset rebuild starts from the baseline, and later follow
blocks repair missing values.

Text hydration is restricted to supported inventory entries on the four
manifest-admitted legacy public resolvers `0x4976fb03…`, `0xDaaF96c3…`,
`0x226159d5…`, and `0x5FfC0143…`, whose admitted text profiles are recorded by
the pinned ENS app metadata
(upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L71 @ ens_app_v3@7175858)
(upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L88 @ ens_app_v3@7175858)
(upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L105 @ ens_app_v3@7175858)
(upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L121 @ ens_app_v3@7175858).
The transaction checks the record event position, partition version, namehash
and classification again. `project_node_record_value.hydrated_value` retains
the outcome, value, block hash and selectors; `hydrated_at_block` retains the
height. The event columns remain unchanged. Successful empty reads are
`not_found`; failure or lost admission exposes the baseline. A canonical result
survives head advancement while its selectors remain valid. Inventory readers
reject mismatched or orphaned overlays immediately, before another write.
Reverse claims use the bounded refresh policy under [Primary names](#primary-names).

## Rules

- Project alone owns family rows, derived indexes, child-registration membership,
  the marker, undo journal and repair record. API and adapters cannot write them.
- Non-maintenance values must be contract-defined literals or derived from
  activated canonical normalized events; readable identity, discovery and
  authority inputs; admitted manifests and verified label preimages; the
  selected block and its lineage timestamp; another chain's readable position
  aligned to that timestamp; or family state folded from those inputs. The
  documented hydration overlays additionally use hash-pinned provider results.
  Operational timestamps and attempt counters are not protocol facts or history.
- A reducer reads the same transaction's prior key state and changes. A rebuild
  range exposes earlier blocks' pending writes to later blocks before publishing
  them together. Read composition uses one admitted database snapshot for all
  participating family inputs, rather than mixing independently fetched rows.
- Exact-name reads resolve snapshot selection first. A publication may trail the
  selected head only within the route's admitted lag; equal-height hashes must
  match. Readers fail closed when publication, canonical lineage, input content
  or selected positions cannot be proven. They do not repair missing family
  state from raw facts, adapter internals or provider answers.
- Resource-keyed reads require a readable resource identity. Record attribution
  may reach node-keyed events without a resource or logical name, but only
  through the selected pointer and the declaration, namespace and node checks
  in [Resolver and records](#resolver-and-records).
- Candidate normalized events and candidate identity/discovery effects do not
  enter product state. An independently admitted event remains activated when a
  migration correlation references it; the association alone adds no authority.
  An independently admitted `registry_announcement` still drives the watch plan.
  After an activated parent transition, child reachability may use a readable
  canonical migration association only with the active ordinary announcement,
  matching topology and nonempty evidence contained in the parent boundary.
- Coverage and support are explicit, never inferred from row presence or a
  historical ingest range. Verified provider answers remain request-scoped.

## Families

| Read model | Published inputs | Read behavior |
| --- | --- | --- |
| Exact names | Name/binding, lifecycle, wrapper, registry and pointer families | Compose selected authority, control, registration, resolver and topology |
| Address-to-names | Address candidate indexes and current name/permission families | Admit current `registrant`, `token_holder` and `effective_controller` relations |
| Address-to-records | Node/record-ID inverse indexes and current inventory | Admit `resolves_to` through the same indexed-record evaluator |
| Children and labels | Child-edge candidates, parent subregistries and name summary | Filter current reachability, authority, expiry and readable display |
| Permissions | Grants, resource admin aggregates and account approvals | Compose masked powers, operators, restrictions and coverage |
| Resolvers | Classification, pointers, aliases, links and grants | Classify the resolver and page current collections |
| Record inventory | Current pointer, classification, partitions, values and links | Compose selectors, values, boundary and provenance |
| Primary claims | Reverse tuples, node claims and normalization | Compose declared claim and any valid hydration overlay |
| Child registration history | `child_registration_events` | Retain direct-child registration membership by parent and event identity |

The [owned key family inventory](glossary.md#owned-key-family) maps each family
to its tables and reducer. `surface_bindings` remains identity history.
Exact-name reads select the logical name's
[authority epoch](glossary.md#authority-epoch), then fields from that epoch's
binding and resources. A released ENSv1 lease without revived custody selects
its lease, wrapper or registry-only binding as a
[released v1 authority](glossary.md#released-v1-authority) tombstone.
A name without migration proof follows the chain, including root names
([ADR 0007](adrs/0007-follow-the-chain-ens-authority.md)): a current ENSv2
binding selects ENSv2; a released or expired ENSv2 registration without a later
live reservation remains a [released v2 authority](glossary.md#released-v2-authority)
tombstone; otherwise a live ENSv1 binding selects ENSv1, and a name with no open
binding follows its history.

## Exact-name projection

The composed exact-name row assembles current registration, authority, control, resolver,
coverage, and display context for one logical name. Ordinary lifecycle changes
within the same authority anchor preserve `resource_id`; wrap, unwrap,
re-registration, or another authority-anchor change follows the identity rules
in [`architecture.md`](architecture.md#identity-model).
For ENSv2, a selected binding's non-terminal lifecycle remains the exact-name
registration until it becomes terminal, even if another lifecycle has a later
grant or reservation event.
After it becomes terminal, composition prefers another surviving lifecycle;
if all lifecycles are terminal, it prefers the selected binding's terminal
event over a later terminal event from another lifecycle, then prefers the
later canonical event position, including the emission ordinal within one log.
`resource_id` identifies the current control or registration resource. The nullable
`serving_resource_id` identifies a separate, event-derived resolver and record-serving
[serving resource](glossary.md#serving-resource) when no control binding is open. It is not a binding, registration,
address relation, or permission authority. Resolver and record readers use
`COALESCE(serving_resource_id, resource_id)`; control, relation, and permission readers use only
`resource_id`. `provenance.read_reachability.basis` names how the serving resource was
selected: `retained_registry_resolver_pointer` for an ownerless ENSv1 or Basenames registry
name (registry owner proven zero, row supported and unregistered), or
`root_registry_resolver_pointer` for an ENSv2 TLD whose root-registry token has a
current nonzero resolver pointer but no registration (typically a reservation: owner zero with
the pointer set in the same block), so no surface binding and no selected authority. That TLD
row keeps `current_authority_not_projected`: the pointer is followed from the name-linked
root-registry `ResolverChanged` to its token resource, the latest pointer on that resource
wins (a state-derived expiry clear names the resource but no logical name), and a
`RegistrationReleased` on the resource at or after the pointer withdraws it. A reservation does
not withdraw it, and the row's lifecycle summary still reports the reservation. The resource-keyed families retain unnamed releases and resolver clears, so
those facts also withdraw the pointer when the name is composed. The root
registry stores the pointer per token, for reservations too, and returns it while the label is
unexpired; a finite reservation expiry withdraws through the interpreter's derived expiry
release and pointer clear, an infinite one never does.
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L150-L155 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @ ens_v2@a971bd64)
Its projection provenance stores the [source family](glossary.md#source-family)
of the event that selected the current resolver pointer. Resolver binding
summaries use that stored event provenance rather than a prior resolver row's
classification.

`provenance.authority_selection` records the selected
[authority epoch](glossary.md#authority-epoch) arm and three facts the API reads
beside it. `registry_generation` is present only on the `ens_v1` arm: `old`
when the name's ENS node has an ownership record in the 2017 registry and none
in the current registry, `current` otherwise, and always `current` for the
root ([registry generation](glossary.md#registry-generation)).
`registry_handoff_block_number` is the block of the node's first
current-registry ownership record, whatever the arm, and is absent before one
exists and for the root. Both read activated, canonical registry ownership
events by node rather than by name: a `NewOwner` counts for its child node and
a `Transfer` for its own node, and the `emitter_role` of the event tells the two
registries apart. A same-transaction registration that reconciliation marked
`registry_migrated` needs no separate reading: reconciliation keeps the
transaction's last current-registry ownership write, which already counts.
Transient removal only drops ownership writes before the last eligible one,
and a redundant write cannot remove that evidence: a same-owner `setOwner`
that derives no rows does not move the last eligible ownership position, and a
redundant `reclaim` calls `setSubnodeOwner`, whose `NewOwner` always derives
another `SubregistryChanged`
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L175 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L74-L84 @ ens_v1@91c966f).
A produced fixture where the registration's provisional owner writes the
record and later hands the token and the record on shows transient removal
dropping the earlier write's `AuthorityTransferred` while the later write's
stays.
`ownerless_registry` is `true` exactly when the row is the supported,
unregistered ownerless registry profile; the selected arm is kept, but the API
serves no `authority` for such a row and no public `authority` filter matches
it.

`declared_summary.topology` is the lookup engine's routing input
(`architecture.md` § `verified_queries`, `execution.md` § Resolver-record
lookup). The snapshot reader composes it in a fixed order, selecting the first
applicable shape: alias paths, observed wildcard paths, ownerless
ENS registry pointers, then exact-surface direct ENS names, then Basenames
transport. The direct shape covers an ENS name bound through its selected
`declared_registry_path` binding on either [authority arm](glossary.md#authority-epoch):
one `registry_path` hop for the binding, one `resolver_path` hop for the
projected exact resolver (a declared ENSv1 mirror resolver stays the mirror
address), empty `subregistry_path`, null wildcard, alias, and transport detail,
and `version_boundaries` copied from the binding resource's
composed inventory
`record_version_boundary`. A bound name whose exact
resolver is null keeps no topology so the Universal Resolver discovery route
classifies it from the absent shape, and a bound name whose binding resource has
no inventory row is skipped because the engine requires the copied boundary to
equal the inventory row's. Whether a verified read may then execute for the
name's arm is decided per deployment profile by the `ens_execution` manifest's
`verified_authority_arms`, not by the topology.

When a retained direct-registry authority first becomes name-addressable, its
[`state-derived normalized event`](glossary.md#state-derived-normalized-event)
of kind `SurfaceBound` carries the observed registry owner. The exact-name
control summary exposes that owner, its registration authority context identifies
the registry-only anchor, and the effective-controller address relation includes
the owner; `control.status` remains null unless another selected authority event supplies it.

ENSv1 wrapper lifecycle and fuse effects are projected from canonical wrapper
facts. During registrar grace, the holder and lifecycle state remain visible,
while owner modification, transfer, and effective-controller membership stop at
grace start.[^v1-wrapper-grace-expiry][^v1-wrapper-grace-authority] Expired
wrapper fuses are projected as zero, matching NameWrapper `getData`; an expired
emancipated or locked position also contributes no lifecycle value or effective
holder powers because that read clears its owner.[^v1-wrapper-expired]



ENSv1 BaseRegistrar lifecycle rows (`RegistrationGranted`, `RegistrationRenewed`,
`ExpiryChanged`, `RegistrationReleased`, and the registrar's `TokenControlTransferred`) can carry
no `logical_name_id`, because the registrar's own events identify a lease by labelhash only
(upstream: .refs/ens_v1/contracts/ethregistrar/IBaseRegistrar.sol:L10-L20 @ ens_v1@91c966f).
The family lifecycle fold associates such a row with its name, by exact identity and never by label or
time. Only these `ens_v1_registrar_l1` rows are named this way. A row of any other source family
that carries a resource but no name, for example an ENSv1 registry row written before the label
was known, keeps no name: it stays out of the name's `created_at` and provenance lists, as
before registrar rows were joined by resource identity.

- **Through the lease's own binding.** A name-less row whose `resource_id` has a binding
  candidate, open or closed, to a surface with the row's namehash is associated with that name. This
  is what a controller event that names the lease later, or a
  [registrar surface snapshot](glossary.md#registrar-surface-snapshot), makes possible.
  A row on the same resource with a different namehash is not attached.
- **Through a wrap.** A wrapped `.eth` name is bound to its NameWrapper resource, so the lease is
  reached from the selected wrapper binding's `SurfaceBound` row by one of two rules:
  1. *A named grant in the wrap's own transaction.* When a controller event creates the lease
     (today's mainnet manifest), that event follows `NameWrapped` in the registration
     transaction, so the wrap could not record the lease and
     `wrapped_registrar_resource_id` is `null`. The grant carries the name, and sharing the
     wrap's transaction identifies it.
  2. *The recorded link.* When the BaseRegistrar's own event creates the lease, or the name is
     wrapped in a later transaction, the lease exists before `NameWrapped` and the wrap records
     its `resource_id` in `wrapped_registrar_resource_id`. The registrar rows are name-less, so
     there is nothing else to match on; the link plus equality of the wrap's node and the row's
     namehash identifies them. These rows are also associated by the lifecycle fold, from any `NameWrapped`
     binding row of the name that recorded the lease, so the statement that collects each
     name's authority events joins events to names by a plain equality on the name and never
     searches the rows that carry no name. The registrar `Transfer` into the NameWrapper in the
     wrap's own transaction is not named this way.

  Both rules stay because both shapes exist in stored events: rule 1 alone cannot see name-less
  registrar rows, and rule 2 alone would drop the registrar lease, and with it `registered_at`
  and the registrar expiry, from every name registered through the NameWrapper under a manifest
  where the controller event grants the lease.


  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L264-L268 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289-L305 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/deployments/mainnet/WrappedETHRegistrarController.json:L656 @ ens_v1@91c966f)

The served registrant of a wrapped name follows the chain: the `NameWrapped` owner, then each
later NameWrapper transfer. The registrar `Transfer` that moves the token into the NameWrapper
during a later wrap is left out of the registrant fold: it is custody moving to the wrapper
contract, not a change of holder, and the person who holds the name afterwards is the
`NameWrapped` owner recorded next in the same transaction.
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L264-L268 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L894-L903 @ ens_v1@91c966f)

`declared_summary.registration.resource_id` names the registration of an ENSv1 name by its
BaseRegistrar lease: the selected registration's own resource, or the lease the current wrapper
binding leads to by the two rules above. It is the lease whether the name was wrapped at
registration or later, and stays the same through unwrap and rewrap. It is `null` when the name
has no registrar lease (a wrapped subname) and for ENSv2 registrations; the API then uses the
bound resource.

The family lifecycle inputs retain these registrar and wrapper associations.
Read composition uses the same identity links after incremental publication,
undo and rebuild, including a retained lease whose binding is now closed.

A registry-only binding reads the events of the binding it replaced only up to the position where
it opened; nothing later on that resource can decide control. The lease the name kept is the one
exception. After a registrar token is transferred without `reclaim` the name still has its
BaseRegistrar lease, and that lease goes on being renewed, and in the end lapses, under the
registry-only binding. So `RegistrationRenewed`, `ExpiryChanged` and `RegistrationReleased` rows
of the `ens_v1_registrar_l1` family on exactly that lease's resource still reach the
registration after the handoff. A renewal updates its expiry and `latest_event_kind`;
`registered_at`, the registrant, the selected binding and every `control` field other than the
repeated expiry stay as they were. `renew` writes only the lease's expiry, and the registrar
writes the registry owner only when registering and in `reclaim`, so a token transfer alone
leaves the registry owner unchanged.

That lease's token can be transferred again, still without `reclaim`. Such a
`TokenControlTransferred` row of the `ens_v1_registrar_l1` family on exactly that lease's
resource, positioned after the binding opened, reaches the registrant and nothing else: the
registration's `registrant`, the repeated `control.registrant` and
`provenance.registrant_event_id` follow the token to its new holder, while the registry owner,
the owner served, the selected binding, its `registry_only` authority kind, the resolver,
`latest_event_kind` and the lease's `resource_id`, `registered_at` and expiry stay as they were.
The transfer is read into the registrant-naming rows only; it never enters the event stream the
control folds read, so a registrar token transfer cannot decide control while the registry-only
binding is the authority. A later renewal and the lease's release then behave as they do without
the transfer.
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L157-L169 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L148-L150 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L175 @ ens_v1@91c966f)

The registry-only binding stands for one lease at a time, and that lease can change. After the
retained lease has been released, a controller can grant the name again with `registerOnly`,
which mints a new token and writes the expiry without touching the registry: the registry-only
binding stays the name's only open one and the successor lease gets no binding of its own. The
binding then stands for the successor lease. A grant qualifies when it is a
`RegistrationGranted` of the `ens_v1_registrar_l1` family and registrar authority kind that
carries the name and the surface's namehash on another resource, positioned after the binding
opened and after a `RegistrationReleased` of the lease the binding replaced; the latest
qualifying grant is the name's lease, so a further release and `registerOnly` move it again,
and that lease's grant, renewals, expiry changes and release reach the registration the same
way the retained lease's did. The registration takes the successor
lease's `resource_id`, `registered_at`, expiry and registrant, and is `active` again with no
`released_at`; the selected binding, its `registry_only` authority kind, the registry owner and
every other `control` field stay as they were. Rows of any other kind or source family, and
lease rows on any other resource even when they carry the name (an earlier lease of the same
name, granted before the binding opened; a grant observed before the lease the binding stands
for was released; or a resource the name was never bound to), stay outside the window. A grant by
`register` writes the registry owner in its own transaction, so it opens a binding of its own
and is selected the way any re-registration is.
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L118-L152 @ ens_v1@91c966f)

A registry-only binding can follow another registry-only binding of the same name and arm: a
registry `Transfer` opens one, a `reclaim` closes it without opening a lease binding, and a later
registry `Transfer` opens the next. A registry owner change writes no registrar state, so the
later binding stands for the same lease as the one it replaced. When a binding's predecessor is
a registry-only binding that stands for a lease, the binding takes that predecessor's whole
handoff (the binding it replaced and its position, the wrapped registrar lease and node that
binding recorded, and the lease with its position) instead of taking the registry-only
predecessor itself as its lease. The registration then keeps the lease's `resource_id`,
`registered_at`, expiry and registrant through any number of such bindings. A registrar grant
that moves a registry-only binding's lease (the successor lease above) moves it for every later
binding that carried that handoff over, including one opened later in the grant's own block,
which Project builds before it reaches the block's grants.
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L60-L69 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L175 @ ens_v1@91c966f)

The release of the retained lease releases the name like any other lapse. On chain the registry
keeps the owner and resolver it held after an ordinary lapse too; what makes a lapsed `.eth`
name available again is the registrar, whose `ownerOf` reverts once the lease is past its expiry
and whose `available` is true once it is past grace. ENSv2 draws the same line inside the
registry: it checks expiry on every read and returns no resolver and no subregistry for an
expired label. The handed-off name is therefore not the one exception. It becomes a
[released v1 authority](glossary.md#released-v1-authority) tombstone: `status` `released` with
the lease's `released_at`, and no current owner, manager, registrant, authority, control,
resolver or records; the address listing drops it and a name-filtered permissions request
selects nothing, as for any released name. The tombstone selects the registry-only binding,
which stands for the released lease the way the closed NameWrapper binding stands for a wrapped
one. It fires only for the lease that binding stands for (the lease it replaced or, once that
was released, the successor lease `registerOnly` granted under it: the one lease whose lifecycle
rows the window admits past the binding's position), released by a registrar row that arrived
after the binding opened, and only when that binding is the name's only open one. A successor
lease needs no binding of its own for this. A release of an earlier lease carrying the name,
whether it came before the name was registered again or is observed after the handoff, does not
release the live registration, and neither does the replaced lease's release once a successor
lease is the name's. A release at the very position where a registry-only binding opened is the
release that handed the name over itself; that revived registry-only custody is unchanged.
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L71-L76 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L100-L103 @ ens_v1@91c966f)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L249-L257 @ ens_v2@a971bd64)

A lease registered through the NameWrapper that lapses past grace is released like any other:
the registrar's `ownerOf` reverts and the name is available again. Its registrar resource never
had a binding, so the [released v1 authority](glossary.md#released-v1-authority) tombstone
selects the closed NameWrapper binding that stands for the lease, found by the same two rules as
above: the recorded `wrapped_registrar_resource_id`, or a named grant in the wrap's transaction.
The rule starts from a registrar `RegistrationReleased`, so it does not fire when only the
NameWrapper's own expiry has passed and the registrar lease, renewed on the BaseRegistrar
directly, is still live; that name is not released. In that state the NameWrapper reports no
owner for a name whose `PARENT_CANNOT_CONTROL` fuse is burned, which every wrapped `.eth`
second-level name has, so `registration.registrant` and `control.registrant` are `null` from the
first block whose timestamp is past the NameWrapper expiry, together with the already cleared
`wrapper_state` and token-holder relation. The `registrant` address-to-name relation reads the
same field and is dropped with it. A renewal through the NameWrapper moves its expiry and
restores all of them.
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843-L856 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1013 @ ens_v1@91c966f)

A tombstone keeps `registration.expiry`, the lapsed lease's own expiry, and adds
`registration.lapsed_registration = {registrant, authority_kind, authority_key, released_at}`:
the holder selected by the registrant fold at the release, and the authority the released
lease binding's resource had before its closing epoch (the NameWrapper for a lease that lapsed
while wrapped). The registrar's `RegistrationReleased` row names the BaseRegistrar token owner
it ended. For an unwrapped lease that is the holder, and the fold reads it. For a wrapped lease
it is the NameWrapper contract, so the fold skips the release and the holder is the NameWrapper
token owner at the release: the `NameWrapped` owner, then each later NameWrapper transfer. The
fold recognizes the wrapped lease by the same two rules as above, the recorded
`wrapped_registrar_resource_id` or a named grant in the wrap's transaction, so the NameWrapper
contract is never served as the lapsed holder under either manifest shape. An unwrapped lease
becomes a tombstone only when no ENSv1 registry owner can take the node over at the release,
for example after `registerOnly`, which does not write the registry; its `authority_kind` is
`registrar`. `released_at` is the timestamp of the block at which the adapter settled the
release, the first block whose timestamp is after the lease's expiry plus the 90-day grace
period. The API serves `authority_kind` as `lapsed_registration.held_through`, only for the
values `registrar` and `wrapper`, and does not serve `authority_key`.
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L118-L127 @ ens_v1@91c966f) `registration.registrant`, `authority_kind` and
`authority_key` stay `null`, so nothing that reads current state (address-to-name relations,
permissions, counts) sees the lapsed holder. No other row carries the block.
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L71-L76 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L101-L104 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L157-L169 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L265 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L297 @ ens_v1@91c966f)



Declared ENSv2 exact-name rows come from the admitted root and registry
families, with registrar events adding history and renewal expiry. A name whose
selected ENSv2 registration carries no authority refusal is supported without a
registrar event; see
[architecture](architecture.md#ensv1ensv2-current-authority). Resolver,
reverse, primary-name, and execution behavior outside those admitted sources does
not become exact-name truth.

Within the selected ENSv2 registration lifecycle, `control.registry_owner`
follows the latest canonical ownership event, including
`TokenControlTransferred.to`. This represents the registry token's owner;
role-only permission changes do not transfer it.[^owner-v2] Lifecycle and resource
association still bound the eligible events. ENSv1 and Basenames retain their
separate registry-owner and registrar-holder meanings.[^owner-v1][^owner-bn]

For Basenames, exact-name truth comes from the admitted Base registry,
registrar, and resolver families. Base primary-claim intake and L1 compatibility
transport do not create alternate exact-name rows.[^bn-readme-l70][^v1-l2rev-base-deploy][^v1-l2rev-event]

## Address and child collections

Address-to-name collections start from the current family address indexes and
compose each candidate's selected name, lifecycle and permission relations in
the admitted publication snapshot. Relation vocabulary is `registrant`,
`token_holder`, `effective_controller`, and `role_holder`. Surface is the default unit;
resource deduplication is explicit. These ordinary listings describe current
relations. For a node an ENSv1 registry `NewOwner` created, the address index
also holds, as `effective_controller` under the node's `<namespace>:<node>` id,
the node's registry owner facts and the owner each such `NewOwner` reported,
whether or not a surface names the node. A candidate with no
[name surface](glossary.md#surface-name-surface) composes no name row; the read
lists it only when the child relation below lists it under its parent and
serves the requested address as its owner, with the child relation's name and
the node's registry-only resource, and with no surface binding.

A fourth relation, `role_holder`, lists the holders of an ENSv2 registry role
on a name's selected registration resource. `PermissionedRegistry` keeps
per-account roles on each registration's token resource, and a holder can act
on the name within them without owning the token, for example change its
resolver or subregistry.
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L142-L155 @ ens_v2@a971bd64)
Any role counts; the `was_reserved` marker alone does not, because it
authorizes nothing.
(upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L47-L48 @ ens_v2@a971bd64)
A role holder is not an `effective_controller`. The read takes the holders
from the served permission rows of the resource (the F8 grants with registry
scope after the read-time masks `GET /v1/permissions` applies), not from the
address index: the candidate names are the names bound to a resource on which
the address has such a grant, and the composed name keeps the holder only
while that resource is its selected resource. Only the requested address's
grants are read, so the membership read does not process other holders' grants
on the same registration. A request whose explicit relation set excludes
`role_holder` skips both role-candidate discovery and role-grant loading;
unfiltered reads and sets including `role_holder` retain them. This does not
bound the request's ordinary ownership enumeration or other work. A role held on the registry root
reaches every name in the registry, and an ENSv2 registry operator approved
with `setApprovalForAll` is not a permission row, so neither adds names to an
address's collection. Reverse lookup does not serve this relation.
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L575-L592 @ ens_v2@a971bd64)

Raw unbounded diagnostic address history separately includes retained
controller and permission evidence, including former controllers, as documented
in [the audit route contract](api-v1-routes.md).

The node and record-ID inverse indexes supply candidates for `resolves_to`.
The reader composes each candidate's current record inventory and admits it only
when the forward indexed evaluator answers `success` with a nonzero 20-byte
address (`0x` and 40 hex digits, stored lowercase). This includes a name with a
serving resource but no control authority: its binding and control-resource
fields remain null, while its record resource is required. Zero, cleared and
other-length values do not match. Decimal coin types whose value is 20 bytes
can contribute; `coin_type=evm` narrows the read to EVM types.

The ENSIP-19 default address (`addr:2147483648`) contributes only when the
current resolver declares `ensip19_default_address`. The reader uses the same
exact-entry precedence and scoped coin-60 zero-address absence rule as
`bigname_domain::resolver_read::evaluate_indexed_record`. An exact success wins;
a retained exact absence blocks the default only under the documented rule.
The indexes accelerate candidate selection and do not authorize answers on
their own. Undo, pointer changes, version resets and classification changes are
visible through current family composition. `resolves_to` is a record relation;
`relation=any` does not include it.

Child readers compose direct and classified relations from family candidates. For registry
events from ENSv1, the reader first filters the relation by the parent's
ENSv1→ENSv2 migration path: `unwrapped`, `unlocked_wrapped`, and
`emancipated_child` parents retain no ENSv1 children, while `locked_wrapped` and
`locked_child` parents retain only a [migratable child](glossary.md#migratable-child)
through their [migration registry](glossary.md#migration-registry-wrapperregistry).
An unknown activated path is a data-integrity failure. Child authority
selection then keeps only the arm the child's own authority selects; cross-era
recency never chooses the arm. A released ENSv2 child is a released v2
authority tombstone and publishes no relation on either arm; a child publishes
its ENSv1 relation only when its own selected arm is ENSv1 and the relation
survived that filter. Any entry the child has had in the parent's migration
registry, released or not, makes it non-migratable, so a released child of a
locked parent publishes no relation while its ENSv1 wrapper binding is open. A surviving locked-path row cites the matched association's stable
logical-edge and correlation identities plus its source manifest; its row-level
manifest version therefore accounts for the association that authorized the
migration registry. Its `normalized_event_ids`, `event_identities`,
`raw_fact_refs`, and `manifest_versions` arrays are independent evidence sets,
not positionally aligned tuples; an input contributes only the identifiers it
actually owns.
Reachability is per parent relation, not transitive: hiding a parent-to-child relation does not itself hide that child's children.
An ENSv1 or Basenames registry child's owner is its node's current registry
owner, from the node's latest owner-setting registry event, not the owner the
edge's `NewOwner` reported: a later registry `Transfer` moves it. The owner is
the registry's owner getter view, so an owner the registry reads as zero is
zero. An unmasked 2017 registry owner word serves its low 20 bytes as the
child's display owner, as the fallback registry's typed read returns it
([architecture](architecture.md)); it still names no control owner. A child with no
owner, or whose name summary records a zero-owner transfer, publishes a relation
only while it has a serving resource.
For registry
events that expose only a labelhash, The reader composes the child name from a
verified label preimage when one exists and its normalization verdict is true,
and leaves the name columns null when none does — the labelhash and child node
are proven, the label is not. Reads name such a child by the [non-name
form](glossary.md#non-name-form)
`[<labelhash-without-0x>].<parent-name>`, built from the parent's stored
spelling, and returns those same stored bytes in both name fields. A preimage whose label
bytes are not valid UTF-8, or contain a NUL, is a third state: Composition retains
the whole child name as raw bytes with no decoded form, and reads escape-encode
that whole string, parent portion included. A preimage whose bytes decode but
fail the verdict is a fourth state: the text is a valid string but not a name
for the proven node — serving it would attach a spelling that re-hashes to a
different node — and escaping it would serve the same misleading text, so
Composition keeps the raw label bytes, withholds the decoded text and both name
columns, and the placeholder serves. None of these shapes is an addressable
name. A preimage improves readability but does not create ownership or
exact-name authority. ENSv2 direct and linked
children derive from admitted graph events rather than token enumeration, and
join the child's own active surface, so none of the name-less shapes arises
there.[^v1-registry-l45][^v1-registry-l82][^v2-events-l49][^v2-events-l75]

Chain-observed label preimages are shared across namespaces. Child readers join
verified preimages to the proven labelhash in the selected family snapshot;
label evidence improves display without introducing authority.

## History

History routes read normalized events, not a current projection cache. Product
surface, resource, and address scopes filter the same canonical,
consumer-visible event set: ordinary rows and
`consumer_visibility=activated` rows only. Candidate rows and
`migration_event_associations` remain available to diagnostics. An association
never removes or duplicates the independently admitted ordinary event it
references. One V1 registry resolver log can have a registry-resource row for
reads and a distinct control-resource row so both resource links survive
replay. Product history returns the control-resource row once and suppresses the
additional row carrying the registry resource link; raw diagnostics returns both normalized rows. Without
a distinct control resource, the sole registry-resource row remains
product-visible. Consumer visibility is applied before candidate evidence can
contribute an address anchor and again when rows are selected. Name and resource
anchors are constructed from readable bindings before row selection. Product
duplicate suppression then runs before cursor validation, summary calculation,
type filtering, keyset pagination, page-size limiting, or cursor construction,
so neither candidate admission nor the extra resource link can broaden, shorten,
or reorder a product page. Projection rows may supply readable names for result
decoration, but the API does not synthesize history from current state.
Name history's `include=child_registrations` selects its extra rows through
the historical [`child_registration_events`](#child-registration-events)
membership; the rows it returns are still normalized events.
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89-L94 @ ens_v1@91c966f)

### Child registration events

`child_registration_events` lists, for each name, the normalized events that
are registrations of its direct children. It is historical membership, not a
current child list: one row per parent logical name and event identity, with
the event's chain position. Name history reads it for
[`include=child_registrations`](api-v1-routes.md#direct-child-registrations-includechild_registrations);
event payloads and public event IDs still come from `normalized_events`.

Project derives each row from one block event and the event's own name
surface. The surface's current `visibility_state` is the one input that can
change without a new event: a `recompute-flags` run can flip it, and the
Project redo that run stamps rebuilds the affected rows (see
[`deployment.md`](deployment.md)). The rules:

- The event is an activated, readable canonical `RegistrationGranted` (or
  `LabelRegistered`) row with a chain position and a logical name, and it is
  not a state-derived registrar surface snapshot, the one registration row
  product history already suppresses as a duplicate.
- The event's name surface is `active` and has at least two labels.
- The parent is the name one label up: its identity is the event's namespace
  and the namehash of the surface's label hashes without the first one. The
  parent's own surface is not consulted, so the row does not depend on when or
  whether the parent was surfaced. Serving requires the row's chain to equal
  the requested name's surface chain.
- Rows whose parent is `eth` or `base.eth` are not stored. Name history refuses
  the option for those two names, and every second-level registrar grant would
  otherwise be copied here.

The event's logical name is the one Interpret attributed when the event
happened, so the rule never borrows eligibility from current child or exact-name composition, the parent's current subregistry, or a current contract
address range. Rows therefore survive a child's release, the parent unlinking
or replacing its subregistry, and a registry moving under another parent: the
registry's earlier grants keep their earlier parent and its later grants get
the new one.

The family block writes memberships in its own publication transaction and journals
before-images under the existing `(parent_logical_name_id, event_identity)` key.
Release or a later subregistry change does not remove a membership. Undo removes
a dropped block's new memberships or restores their previous images; replay derives
the replacement block's memberships from its events. A full rebuild clears the
chain and replays retained events, including the existing surface visibility rule.
Rebuild ranges read their memberships together, then fold them through the same
block reducer. The history table and its parent-history index remain in use.

History cursors retain stable positions and follow the
[history walk](glossary.md#history-walk) contract across replacement
publications. Product history checks the reached pointer-chain family markers
in the same snapshot as resolver classification. Missing or rebuilding
publication maps to `409 stale`; it does not force every history page to retain
one exact family generation. Raw unbounded diagnostics remain available during
Interpret and Project redo through retained input evidence.

## Permissions

The composed permission set is resource-anchored and preserves subject, scope,
effective powers, provenance, and chain positions. The companion resource
summary distinguishes authoritative empty enumeration from unsupported or
partial permission support. Current non-wrapper summaries are partial because
registrar token and account approvals, resolver operators and delegates, and
ENSv2 registry operators are not indexed. NameWrapper summaries are partial for
a narrower reason described below: holders, operators, and per-token delegates
are rows, while parent control of a non-emancipated wrapped subname and resolver
operators/delegates are not. For a grant on an ENSv2 record-ID resolver, whose
resource is the keccak of a setter argument rather than a name
(upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L307-L338 @ ens_v2@a971bd64),
`scope_detail` also keeps the selector the interpreter decoded from that
argument (`resource_selector`) so reads can say which record the grant is
about — recognized by the selector's hash being the resource itself
(upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L336-L337 @ ens_v2@a971bd64),
which the node-keyed generation's named-resource selectors never satisfy:
`NamedTextResource` hashes the key alone and `NamedAddrResource` carries no
hash at all
(upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L144-L153 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L168-L172 @ ens_v2_sepolia_20260629@ccaeb58); the interpreter reads the argument under the union of the old and new
role bitmaps, and reads keep only the readings the row's effective powers
still hold. A grant whose argument was never observed keeps a plain scope.

`project_account_approval` separately folds `AccountPermissionChanged`
events from the [`standard_approval`
derivation](glossary.md#standard-approval-derivation) by chain, authority kind, authority contract,
owner, subject, and relation. It retains both active and revoked latest states;
`approved=true` carries `registry_control` for a registry and `wrapper_control`
for a NameWrapper, while `approved=false` carries no effective powers. Project
retains account approvals once per account key; readers join registry and
NameWrapper operators as described below. Name composition carries the latest
[registry-owner binding](glossary.md#registry-owner-binding) onto the resource
selected for an ENSv1 or Basenames name. Registry-family owner observations are
first ranked by logical name or emitting resource to suppress detached history,
then mapped onto that selected resource and ranked again by output resource.
The separate resource that retains registry observations is bypassed by that
mapping. When the name has no eligible selected resource, or the event has
no logical name, the observation stays on its emitting resource. This remapping
never crosses onto an ENSv2 resource. A latest zero owner or an admitted registry-
or registrar-family `SurfaceUnbound` transition clears the binding. A registrar-
family `SurfaceBound` carries the registry owner and emitter-derived registry
contract remembered at transition time, not the registrar token owner, so the new
current authority receives the binding without attribution to the registrar
emitter; wrapper-family authority transitions remain outside this rule.

The serving read combines direct grant rows with effective
registry-operator rows; it does not persist account approvals once per
resource. An account row is effective only when it is approved, has
`authority_kind=registry` and `relation_kind=operator`, and its chain,
`authority_contract`, and owner equal the resource summary's binding chain,
`registry_contract`, and `registry_owner`. Both the account row and binding
must point to current canonical, safe, or finalized chain lineage, and the
resource summary must pass its ordinary current-lineage filter. The join uses
the emitter-derived registry contract address because one admitted address on
one chain identifies one contract instance across manifest epochs; the account
row retains `authority_contract_instance_id` as the corresponding admitted
instance evidence. A binding move to a different registry address therefore
isolates registry-contract generations and makes the old approval inapplicable. A retained
revocation (`approved=false`), a cleared binding, or orphaned account or binding
evidence is served as absence.

(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L108-L118 @ ens_v1@91c966f)
(upstream: .refs/basenames/src/L2/Registry.sol:L46-L52 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L2/Registry.sol:L148-L158 @ basenames@1809bbc)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L42-L50 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L78-L103 @ ens_v1@91c966f)
(upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L70-L84 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L575-L592 @ ens_v2@a971bd64) Known
owner-derived rows remain available, but neither those rows nor a zero-row
summary is an authoritative permission enumeration. The current family reader
reports partial coverage for the documented absent surfaces; an empty row set
alone cannot establish full coverage.

When a registrar `Transfer` changes ENSv1 or Basenames authority between a
registrar resource and a registry-only resource, `resource_control` and
`resolver_control` for any selected nonzero resolver are revoked on the retiring
registry-only resource or granted to its owner when it becomes active. An unchanged
authority emits no additional registry-only balancing rows; ordinary token-holder permission rows remain unchanged.
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L86-L95 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L175 @ ens_v1@91c966f)
(upstream: .refs/basenames/src/L2/Registry.sol:L46-L52 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L2/Registry.sol:L132-L134 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L321-L329 @ basenames@1809bbc)

A registrar-token transfer can leave the original account as registry owner.
When that boundary selects a registry-only resource, the adapter carries
`registry_owner` on its `AuthorityEpochChanged` observation only if retained
registry getter evidence is nonzero and matches the selected authority owner.
The same retained evidence supplies the registry contract. This is retained
registry state, not an owner-change argument from the registrar `Transfer` log.
Missing, zero, inconsistent or unmasked owner evidence contributes no value;
a selected wrapper or registrar authority does not qualify. Project consumes
this value through its existing owner fold without changing resource selection.
A later registrar token transfer that leaves the registry-only authority selected
emits no new epoch, so the retained `registry_owner` stays the served owner; that
transfer can update the registrar holder while leaving registry control unchanged. The Basenames registrar behaves
the same way: its token transfer is the inherited ERC-721 ownership write, and it
writes the registry owner only from `reclaim` and registration.
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L175 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L17-L20 @ ens_v1@91c966f)
(upstream: .refs/basenames/src/L2/Registry.sol:L49-L52 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L24 @ basenames@1809bbc)
(upstream: .refs/basenames/lib/solady/src/tokens/ERC721.sol:L744-L745 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L327-L329 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L421-L423 @ basenames@1809bbc)

This transfer-only observation does not apply to release. A release that restores
a retained direct-registry authority carries that authority's owner. A genuinely
ownerless release epoch still clears served registry ownership. Event kinds,
identity anchors,
canonicality and bindings are unchanged. The payload change alters the generated
interpreter content hash. Existing events need full-history Interpret redo,
then stamped Project redo, before deploying the matching API as required by
[the deployment contract](deployment.md). A Project-only rebuild cannot supply
the missing event value.

The owner fold reads an epoch's owner only when the epoch states one. An
`AuthorityEpochChanged` whose after-state carries no `owner`, `registry_owner` or
`owner_word_unmasked` field, as a registrar grant's or token transfer's, says nothing about the
registry owner and leaves the fold as the earlier facts set it; an explicit null owner, as a
release's, still clears. A registrar-authority `SurfaceBound` records as its bound owner the
registry owner the registrar adapter read from retained registry state (`owner_getter`), and the
fold reads that bound owner for every admitted, non-state-derived registrar binding of the
selected resource. So a registry `Transfer` that opened a registry-only binding, whose
`AuthorityTransferred` sits on the registry-only resource and is no longer admitted once a
registrar token transfer binds the lease again, still decides the owner served.

For an ENSv1 or Basenames name whose selected binding is not a NameWrapper authority, one rule
decides the served registry owner: the node's newest registry `NewOwner` or `Transfer` in
canonical order, from `project_registry_owner_event` and whatever resource the adapter anchored
it on, wins over every older owner fact. Only a registry write sets `owner(node)`. Every other
owner fact of such a name either restates one (a binding's bound owner, a registrar transfer's
retained registry owner, a registry-only or boundary epoch's owner, `NameUnwrapped`'s raw
controller argument) or clears it (a release's explicit null owner), so a newer fact overrides
the newest write only when it is such a clear. This covers a zero-equivalent write while the
lease stays selected, which the adapter anchors on the registry's read-anchor resource and the
admission does not hold, whatever came between it and the older fact. A NameWrapper-selected
name keeps the fold, because the owner it serves is the wrapped token's holder while the
registry names the NameWrapper: its NameWrapper epochs and every admitted NameWrapper
`TokenControlTransferred` (a `TransferSingle` or `TransferBatch` of the wrapped token) set it,
and a registrar ERC721 transfer does not, since the NameWrapper holds that token while the name
is wrapped. Past its NameWrapper expiry, a wrapped name whose `PARENT_CANNOT_CONTROL` fuse is
burned (an emancipated or locked name, such as an emancipated subname) has no owner in the
NameWrapper, so `control.registry_owner` is `null` there, like `control.registrant`. A wrapped name
whose expiry passes without that fuse keeps its token holder as the owner. ENSv2 names keep their
own fold.
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843-L856 @ ens_v1@91c966f)

A registry owner write is read as the registry getter's view, `owner(node)`: the event's
`owner_getter`, which is zero for a literal zero and, on a registry whose getter maps its own
address to zero, for that address (`owner_getter_reason` `registry_self`), such as a `reclaim` or
an `unwrapETH2LD` to the registry itself; none when the owner word is unmasked; and the reported
`registry_owner` or `owner` only for a payload written before the getter was recorded. The raw
owner an epoch restates never decides the owner of a name the rule above covers.

A registry record the admitted Graveyard holds is burned and names no owner. The Graveyard
claims a lapsed `.eth` name and clears a subname of a name it holds by making itself the node's
registry owner, so the registry adapter marks a current-registry `NewOwner` or `Transfer` naming
the Graveyard of the migration manifest (same chain and namespace, at or after its declared
start block) with `owner_getter_reason` `graveyard`. Its owner word and getter stay as the chain
wrote them. Every served read then gives that node no owner: the control owner is none, as
for an unmasked word; the `owner_required` fallback serves none instead of failing; and the
subnames route serves no owner, unlike an unmasked word, whose low 20 bytes stay the display
owner, so a cleared subname with no name row is not listed. The record
decides even when the name is NameWrapper-selected: a subname wrapped without
`PARENT_CANNOT_CONTROL` keeps a live, transferable NameWrapper token after the Graveyard clears its
record, and the owner fold lets the Graveyard-held write win over every later NameWrapper token
transfer, single or batch, so no owner is served again until a newer registry write. The address
relations read the same write as naming no controller: the `AuthorityTransferred`, and the
`resource_control` grant or registry-only binding its own log restates it with, set the zero
controller, which is never listed, so a cleared subname or claimed name with a surface is not
listed under the Graveyard in `GET /v1/addresses/{address}/names`. The Graveyard's address is not
masked: its other relations, such as a live token sent to it, are unchanged. This is
not the zero owner of `registry_self`, and it does not make the registry ownerless. Known gap
(TYR-100): any other registry write that moves a wrapped subname away from the NameWrapper, such
as its unwrapped parent's owner calling `setSubnodeOwner`, keeps the NameWrapper binding
selected, so the stale token's holder is still served as owner; only the Graveyard's write is
handled here. The same gap has a Graveyard variant: after the Graveyard clears the record of a
subname wrapped without `PARENT_CANNOT_CONTROL`, a holder who sends the surviving NameWrapper
token to the Graveyard makes it the holder of that still-selected binding, and its holder grant
lists the Graveyard as the subname's `manager` in `GET /v1/addresses/{address}/names`, although
the served owner stays null. Only the
admitted Graveyard counts; the Graveyards of superseded Sepolia deployments are not declared
and their records are served as the chain holds them. A registrant that sends a live `.eth`
token to the Graveyard keeps the lease running, since the name cannot be registered again
before its expiry and grace period, so that registration is served as the chain holds it, the
Graveyard as registrant, until it lapses.
(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/migration/Graveyard.sol:L142-L172 @ ens_v2_sepolia_20260916@366de741)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L101-L104 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L347-L374 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L281-L303 @ ens_v1@91c966f)

An `active` ENSv1 or Basenames registration whose authority is its registrar lease or its
registry record always serves a registry owner, because the registry answers `owner(node)` for
every node. The test is the registration's status and authority kind; it does not look at a
wrapper, so a wrapped name whose registration authority is the registrar is included. When the fold finds no owner fact, or its latest fact cleared the owner, the
served owner is the node's latest registry `AuthorityTransferred` kept in
`project_registry_owner_event` (none when its owner word is unmasked or the admitted Graveyard
holds the record), or the zero address when
the registry holds no record of the node. A node with a registry record but no owner the
families kept is a data-integrity failure: Project does not publish the block, like any other
integrity failure. A rebuild range composes its names at the range's last block, so there the
check applies to what that block serves, not to each block inside the range.
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L60-L84 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L123-L131 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L175 @ ens_v1@91c966f)

When a state-derived ENSv2 path-expiry release remains the resource's terminal
lifecycle event and retires effective permission rows, the resource summary
keeps the selected registration-authority event's provenance unchanged.
Separate `expiry_retirement_*` fields identify the release event, its source
manifest and source family, its manifest version, and its
block/transaction/log position. A later ENSv2 grant or reservation removes
these fields. A later `RegistrationRenewed` removes them when
`revived_from_expiry=true` and a preceding state-derived path-expiry release
belongs to the same `resource_id`. Whether the release named a surface does not
participate: a same-resource renewal revives retained grants even while another
token remains the current holder of that name. Unregistering an owned entry and
later registering it use a new versioned resource, so a renewal on that new
resource cannot match the old resource's release or grants. Registering a
non-expired owner-zero reservation does not enter the owner-burn branch, so
neither version counter advances. ENSv2 constructs the permission resource from
`eacVersionId`, so the registration reuses the reservation's resource ID.
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L29-L34
@ ens_v2@a971bd64) (upstream:
.refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L428-L471 @
ens_v2@a971bd64) (upstream:
.refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L632-L645 @
ens_v2@a971bd64)
This is the
resource-lifecycle form of the [ENSv2 expired-role projection
narrowing](upstream.md#known-divergences). The retirement citation therefore
explains why the rows are absent without
rewriting which event established authority.

For ENSv1 NameWrapper resources the interpreter emits the holder grant itself.
`NameWrapped` grants the wrapped owner `resource_control`, `set_resolver`,
`set_ttl`, `create_subnames`, `transfer`, `unwrap`, `burn_fuses`, `approve`,
`extend_subname_expiry`, and `extend_expiry` on the resource scope and
`resolver_control` on the linked resolver; `TransferSingle` and `TransferBatch`
revoke that set from the previous holder and grant it to the new one; a burn to
the zero address revokes it and records the burn, so the `NameUnwrapped` that
follows every burn other than the un-admitted upgrade path emits no second
revocation, while an upgraded name still leaves no live holder row. Fuse state alone still manufactures
no grant; Project masks each row with the current lifecycle and
[expiry-effective](glossary.md#expiry-effective-namewrapper-fuse-word) fuse
word: `resource_control` clears on a `locked` position, `burn_fuses` is present
only while `PARENT_CANNOT_CONTROL` is burnt (until then `_canFusesBeBurned`
rejects every owner-controlled burn and the holder cannot burn that
parent-controlled bit), and `extend_expiry` is present only while
`CAN_EXTEND_EXPIRY` is burnt.
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L421-L437 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L443-L470 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1058-L1068 @ ens_v1@91c966f)
Returned permission rows join the same wrapper lifecycle and fuse summary as
exact-name reads.
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L878-L902 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L483-L509 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L269-L278 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L283-L299 @ ens_v1@91c966f)

The per-token delegate comes from NameWrapper `Approval`: the approved address
receives `extend_subname_expiry` on the resource scope, an approval to the zero
address revokes it, and the interpreter tracks the current delegate so it can
revoke the row without an event when a transfer clears the approval
(`CANNOT_APPROVE` unburnt, evaluated on the expiry-cleared fuse word) or a burn
clears it unconditionally. The transfer revocation is emitted even when the
delegate is the transfer recipient, so a restored interpreter that rebuilt the
delegate from those rows replays identically, and it is emitted before the
holder rows of the same log: Project folds permission rows by
`(resource, subject, scope)` and keeps the newest in canonical event order, so when the recipient is the delegate its holder grant
(the later row) wins over the empty token-approval revocation. An approval that
survives a transfer because `CANNOT_APPROVE` is burnt keeps its row, and when
that retained delegate is the outgoing holder the interpreter re-emits its
token-approval grant after the holder revocation, because `getApproved` still
names it and `canExtendSubnames` still admits it; without the re-emission the
empty holder revocation would be its newest row and the fold would drop it.
Every token-approval row of a name and subject, whether from an `Approval` log
or from the event-less clear on transfer or burn, shares one
[interpreter state key](glossary.md#interpreter-state-key), so a resumed
interpreter that keeps only the newest row per key restores the clear that
followed a grant and replays the same rows as the first pass.
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L108-L121 @ ens_v1@91c966f) The delegate's only power is the
`getApproved` branch of `canExtendSubnames`; transfers and `approve` itself
accept only the holder and its operators.
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L109-L136 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L228-L238 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L837-L840 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L37-L47 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L137-L150 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L275 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L375-L378 @ ens_v1@91c966f)

Owner-wide operators come from NameWrapper `ApprovalForAll`, normalized like
registry operators into `project_account_approval` with
`authority_kind=wrapper` and `wrapper_control`. For these operators,
the reader joins wrapper holder rows to approved account rows whose owner is
that holder and whose authority contract is the same NameWrapper. It composes
one operator row per scope with the holder's masked powers and
`grant_source.relation_kind=operator`. An operator who is also the token
delegate keeps the operator set. The join uses one publication snapshot, so
approval changes need no persisted per-resource fan-out. `canModifyName` and
the ERC-1155-fuse approval and transfer checks authorize an operator as the holder.
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L214-L222 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L105-L117 @ ens_v1@91c966f)

The resource summary classifies a NameWrapper resource `unsupported` with
`wrapper_parent_and_resolver_delegation_not_projected`, which readers map to
partial coverage: the parent of a non-emancipated wrapped subname can still
replace its owner, fuses, and expiry through `setSubnodeOwner`,
`setSubnodeRecord`, and `setChildFuses`, and resolver operator/delegate
approvals are not enumerated as rows.
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L517 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L565 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L596 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L78-L103 @ ens_v1@91c966f)

The summary also carries `resource_restrictions`, the
[resource restrictions](glossary.md#resource-restrictions) block the API
serves as `restrictions`. For a NameWrapper resource whose wrapper fields
would be served it is `{kind: ens_v1_wrapper, wrapper_state, fuses,
expiry_seconds}` with the expiry-effective fuse word at the target timestamp,
and it is omitted once the newest wrapper lifecycle evidence on the resource is
a close rather than an open: opens are the `NameWrapped` token transfer and any
resource-scope holder grant, closes are the `NameUnwrapped`
`AuthorityEpochChanged`/`SurfaceUnbound` rows and any resource-scope holder
revocation without a following grant, which covers the `.eth` 2LD unwrap whose
epoch row lands on the reactivated registrar resource and the un-admitted
`upgrade()` burn that emits no `NameUnwrapped`; for an ENSv2 registry resource it is
`{kind: ens_v2_registry, locked_roles}`, where `locked_roles` lists
`unregister`, `renew`, `set_subregistry`, `set_resolver`, and `transfer` whose
admin role (`can_transfer_admin` for `transfer`) no current row on the resource
or its registry root carries, because only a held admin role can grant or
revoke that role and the registration cannot re-grant an admin role; it is
`NULL` for every other resource. The registry root is read from the resource
identity table, and current resource and registry-root admin aggregates are
read in the same family snapshot. A root permission change therefore affects
`locked_roles` without rewriting every registration.
(upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L418-L424 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L453-L455 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L560-L572 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L24-L45 @ ens_v2@a971bd64)
For ENSv2, permissions remain keyed by the
upstream resource linked to bigname `resource_id`, not by token ID.[^v2-iperm-l57][^v2-pr-l261][^v2-pr-l351]

Unknown or inconsistent typed summary combinations are a storage error. A
persisted unsupported reason that a reader does not recognize maps to partial
unknown support rather than wrapper support or an internal server error.

## Resolver and records

### Records shared through resolver links

For the [record-ID resolver generation](architecture.md), Project selects the
latest canonical link for each materialized name and emitting resolver, falling
back to the resolver's zero-node link only when the exact link is absent or zero.
It then ranks updates within the selected record ID by selector. Relinking does
not discard values written before the link, and an explicit empty value cannot
fall back to a previous link or to a different record. The latest link selection
and value event both contribute provenance. No unknown name acquires a serving
row solely because its resolver emitted a link.

Name history retains shared-record writes after an exact link, default link, or
resolver pointer changes. Project reconstructs the effective exact/default record
selection within each retained resolver-pointer interval. Each selected record
contributes writes before that selection ends, including writes made before the
link; writes made only after the name stopped selecting that record are excluded.
Current record values still use only the latest selection. Historical attribution
and its selecting link IDs remain in `provenance.attributed_event_ids`, so removing
a link or write during redo rebuilds its former consumers. Link IDs are rebuild
dependencies; they do not add a new public history event type. Exact links and
the zero-node fallback select persistent record storage.
(upstream: .refs/ens_v2_sepolia_20260903/contracts/src/resolver/PermissionedResolver.sol:L363 @ ens_v2_sepolia_20260903@5da83f6a)
(upstream: .refs/ens_v2_sepolia_20260903/contracts/src/resolver/PermissionedResolver.sol:L381 @ ens_v2_sepolia_20260903@5da83f6a)

Link, pointer and shared-record updates change their own family keys. Current
inventory composition joins the selected keys in one publication snapshot;
redo restores their before-images and replays replacement input. Historical
attribution reads retained normalized pointer and link intervals, so a retired
current link does not erase the name's earlier history.

Resolver overview reads `project_resolver_classification`. Bound names, aliases,
links and permissions are separate collections composed from current families;
there are no stored sampled section summaries, digests or summary-version
carry-forward rows. Record links apply only to the admitted record-ID resolver
generation. Each `Linked` overwrites a node's record ID, and the zero-node link
can supply the default for an unlinked name.
(upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IRecordResolver.sol:L32-L38 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L97-L100 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L363-L367 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L379-L386 @ ens_v2@a971bd64)

The resolver classification's `unsupported_reason` for an ENSv2 resolver (and the
`coverage.unsupported_reason` copied onto its record inventory) uses a closed
vocabulary: `resolver_not_declared` when an exact `public_resolver_v2`
declaration is required and absent (also the ENSv1 and Basenames reason for an
undeclared address); `resolver_implementation_unknown` when a discovered proxy
has no canonical `Upgraded` observation — neither an ERC-1967 `Upgraded` log
nor a factory announcement — so its implementation is unknown;
`resolver_implementation_not_declared` when the latest observation names an
implementation outside the active manifest's `resolver_implementations`; and
`resolver_binding_enumeration_not_projected` on the binding summary of a
supported resolver whose family does not project binding enumeration.


The composed record inventory records the selectors observed under a resource's
latest retained linked resolver event whose name has a readable canonical
surface at the selected publication, with fallback to an earlier linked event when a
later event's name lacks such a surface. A selected zero-address resolver
suppresses inventory rather than falling back to an older nonzero event. It
remains resource-keyed when a registry-only name loses control: an event-linked nonzero registry
resolver may keep that resource reachable through `serving_resource_id` while the
control resource and binding stay null. This evidence is derived entirely from normalized events;
Project and API serving perform no live registry or resolver read. It
also records the selected resolver's record boundary, explicit gaps,
unsupported families, and any retained indexed values.
`provenance.record_event_ids` lists the current write of every selected record
key in every family, ABI writes included even though ABI records are neither
selectors nor entries, followed by the selecting link ids that
`provenance.record_link_event_ids` also lists. The records and lookup routes
read a name's ABI content types back from those ids
([`api-v1-routes.md`](api-v1-routes.md), `GET /v1/names/{name}/records`), so
limiting that list to the served families would silently drop them. Those
routes use `abi_observation_classification` captured with the inventory's
source family and role in the same family snapshot. They do not reclassify the
resolver from a newer live publication after inventory capture. Selected ABI
event IDs must still resolve to canonical, retained evidence at the captured
positions; missing evidence is `abi_observations_stale`. A concurrent family
reset cannot turn that captured admitted classification into an unsupported
answer. The record event need
not carry that resource: The reader normally joins its `logical_name_id` and
emitting resolver to the pointer without restricting either event's source
family. An `ens_v1_resolver_l1` event whose `logical_name_id` is null may join
when the selected pointer's source family is `ens_v1_registry_l1`,
`ens_v1_registrar_l1`, or `ens_v1_wrapper_l1`, and only through the same chain,
the surface namehash equal to its retained node, and the pointer address equal
to its emitting resolver. A selected `ens_v2_registry_l1` or `ens_v2_root_l1`
pointer may also join when its target resolver's final classification is
supported `ens_v1_resolver_l1` from an applicable exact declaration and the
classifying manifest's namespace matches the pointer's namespace. The family reader applies the same guarded exception. Every `RecordChanged` or
`RecordVersionChanged` event that joins without a logical name of its own is
listed in the row's `provenance.attributed_event_ids`, whether or not it is
the current value for its record key. `registration`- and `both`-scope name
history list the same node-keyed writes by evaluating this attribution from the
pointer evidence at or below the read's published block (`docs/storage.md`); a
Project test checks that the two agree at the current publication. Retracted events leave the readable attribution set. Attribution spans every resolver
pointer the resource has selected, not only the current one: each pointer
attributes the node-keyed writes on its resolver at chain positions before the
pointer that superseded it, and the latest pointer is open-ended. A write is
therefore attributed exactly when it was visible to the name at some point,
because resolver storage persists and ENSv1 reads it at read time; a write on a
resolver the name never selected, or made only after the name left that resolver
for good, stays unattributed. Value selection does not widen with it: records,
versions, resets, and `unsupported_reason`s are still selected only through the
latest non-zero pointer. When the selected pointer is a clear, the registration
has no pointer to serve records through; historical attribution remains available
independently: the boundary anchors on the clearing `ResolverChanged`, `support_status`
is `unsupported` with `resolver_pointer_cleared`, there are no selectors, no
entries, and no `resolver_address`, and `provenance.record_serving` is `false` so
every record-serving read excludes the row and a cleared name answers exactly as
it does with no row at all. Only history reads it, for the
`attributed_event_ids` it carries. A `basenames_base_resolver` event
with no logical-name attribution may join only when the selected pointer is
`basenames_base_registry`, with the same chain, node-to-namehash, and resolver
emitter match. Basenames keeps the current resolver by node, permits its
registrar controller and reverse registrar to write independently of the node
owner, and stores text by record version, node, and key.
(upstream: .refs/basenames/src/L2/Registry.sol:L173-L180 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L2/L2Resolver.sol:L193-L199 @ basenames@1809bbc)
(upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/ResolverBase.sol:L7-L24 @ basenames@1809bbc)
(upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/profiles/TextResolver.sol:L7-L36 @ basenames@1809bbc)
Pointer position is
not a write-time lower bound: selecting a resolver exposes its retained
pre-pointer writes, switching away hides them,
and switching back restores them. The latest `RecordVersionChanged` from that
resolver remains the boundary, and records must be strictly later than it. This
follows the registry resolver lookup
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L137 @ ens_v1@91c966f)
and the resolver's version-, node-, and key-scoped text storage
(upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L28 @ ens_v1@91c966f).
When the first active ENSv1 [name surface](glossary.md#surface-name-surface) is
materialized, Interpret links the latest replayed nonzero registry resolver to
the current registry-only authority resource. If getter-visible registry
ownership is explicitly zero, Interpret instead links that resolver to the
retained registry [serving resource](glossary.md#serving-resource) without
creating control. A latest zero-address
resolver selection suppresses this materialization pointer rather than reviving
an older nonzero selection. The original raw-derived normalized row remains
immutable; the linked pointer is an additive
[state-derived normalized event](glossary.md#state-derived-normalized-event) at
the raw event that first materializes the active surface. A wrapper-provided
surface links the retained registry read resource without binding that dormant
registry resource while wrapper control remains current. Same-transaction
registration reconciliation leaves that registry-read pointer on the dormant
registry resource rather than retargeting it to registrar control. Record
attribution remains node-keyed and provider-free. If a registrar registration
makes the registrar resource current before the retained registry-only
authority can be materialized, the same observation still marks that retained
authority's surface known. A later registrar release can therefore restore the
existing registry resource and its direct-registry owner instead of losing the
known name.
An old-registry resolver selection stops being eligible when either a
current-registry `NewOwner` or `Transfer` creates that node's current-registry
record. The ownership observation persists the
[registry fallback handoff](glossary.md#registry-fallback-handoff) across replay;
if the old pointer was already linked, later linked zero-resolver events
retract it from every registry, registrar, or wrapper resource to which it was
linked, including a resource from an authority epoch that ended before the
handoff. An old-registry `Transfer` cannot clear a resolver
selected from the current registry.
The root resolver is the frozen exception: current-registry ownership does not
retract its old-registry pointer or suppress later old-registry root updates.
(upstream: .refs/ens_subgraph/src/ensRegistry.ts:L243-L248 @ ens_subgraph@723f1b6a)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L18-L24 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L60-L68 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L82 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L48-L54 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L150-L172 @ ens_v1@91c966f)
A resource-less record event cannot create a binding, and name and record reads
expose the inventory only when the name's current readable control resource or
`serving_resource_id` selects it. Resolver-local events are accepted only under the manifest and
current-resolver rules documented in
[`manifests.md`](manifests.md).

The resolver classification also carries effective manifest-declared
[`read_features`](manifests.md#required-fields). A supported inventory copies
`ensip19_default_address` into `provenance.read_rules` with source key
`addr:2147483648`. `selectors` and `entries` remain exact `RecordChanged`
observations: Project does not fabricate target coin types or rewrite the
default entry. ENSv1 `ContenthashChanged` normalized state uses
`contenthash_hex` with `value_retained=false`. ENSv1 and Basenames
`AddressChanged` normalized state uses decimal `coin_type`,
`address_bytes_hex`, and `value_retained=false`, except that coin type 60 with
an exactly 20-byte payload preserves the scalar `value` envelope used by the
legacy `AddrChanged` event.
(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L22-L24 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L47-L70 @ ens_v1@91c966f)
(upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L43-L66 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L76-L82 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L108-L110 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L116-L121 @ basenames@1809bbc)
Project reconstructs a retained contenthash entry as
`value={"encoding":"hex","bytes":"0x..."}` and retains an address entry as
scalar `value="0x..."`. An empty `contenthash_hex` or `address_bytes_hex`
payload becomes an exact `not_found` entry with `value` omitted. The nested
`value.bytes` address compatibility shape receives the same empty-value
classification.

Project classifies an exact 20-byte-zero `addr:60` as `not_found`, with `value` omitted, behind an
ENSv1 registry, registrar, or wrapper resolver pointer, or a Basenames registry resolver pointer.
This covers current scalar and retained nested `value.bytes` envelopes; other origins, types,
nonempty lengths, and nonzero values remain stored successes. Project keeps the entry and selector, changes
no raw facts or normalized events, and records selected nonempty exact absences in
`provenance.exact_nonempty_not_found_record_keys`, a sorted, deduplicated array
omitted when empty. Only the scoped zero20 predicate adds `addr:60`.
Rust and SQL block default derivation only for a matching exact `addr:60`
`not_found` entry. Orphan markers are ignored and exact successes still win.
Empty or missing exact values retain permitted fallback. This private marker
adds no read rule or authority; non-authoritative coverage remains unsupported.
Marker-producing Project, both readers, and rebuilt rows must reach one
maintainer-selected publication boundary before affected reads become public.
(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L36-L40 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L81-L84 @ ens_v1@91c966f)
(upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L93-L99 @ basenames@1809bbc)

Rows produced under an earlier [interpreter content
hash](glossary.md#interpreter-content-hash) may retain the nested `value` object
until the [re-derivation boundary](glossary.md#re-derivation-boundary) completes.
They are not serving-eligible with the matching API during that interval;
shared readers nevertheless normalize both the nested bytes object and scalar
address forms. This hash rotation requires a complete retained-range Interpret
re-walk and Project rebuild before publication, with no manifest change.

For coin type 60, the multicoin `AddressChanged` payload takes precedence over
its immediately adjacent compatibility `AddrChanged` sibling in the same
transaction, so an empty multicoin clear remains empty instead of becoming a
retained zero-address value. The paired logs share one effective ordering
position; any later independent write in that transaction still wins.
(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L47-L65 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L68-L85 @ ens_v1@91c966f)
Removing the feature, changing to an unflagged resolver, or
rotating a proxy to an unflagged implementation removes the rule on the same
scoped rebuild. Full and incremental rebuilds select the feature from the same
current resolver classification.

A pointer from `ens_v2_registry_l1` or `ens_v2_root_l1` whose target is a
supported
[ENSv1 mirror resolver](glossary.md#ensv1-mirror-resolver-ensv1_mirror_resolver)
(`classification.role = ensv1_mirror_resolver`, declared under
[`manifests.md`](manifests.md#ensv1-mirror-resolver-declarations)) is not
attributed on the mirror's own address, because the mirror stores no records.
Project models the call the mirror makes instead. The mirror finds the resolver
with `RegistryUtils.findResolver` over the ENSv1 registry its declaration names:
the walk reads the DNS-encoded name toward the root and selects the nearest
node, the exact node first, whose registry resolver is nonzero; the root node is
never consulted. It then keeps that resolver only when it was found at the
queried node or supports `IExtendedResolver`, and otherwise answers with no
resolver. It does not continue the walk past a rejected ancestor to look for a
farther one. A kept resolver is called with the caller's calldata: an immediate
resolver at the queried node receives the queried node's getter call and
answers from its own storage for that node, while an `IExtendedResolver`
receives `resolve(name, data)` and answers by its own logic.
(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/resolver/ENSV1Resolver.sol:L40-L43 @ ens_v2_sepolia_20260916@366de741)
(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/libraries/LibResolution.sol:L39-L48 @ ens_v2_sepolia_20260916@366de741)
(upstream: .refs/ens_v1/contracts/universalResolver/RegistryUtils.sol:L25-L38 @ ens_v1@91c966f)
(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/resolver/AbstractMirrorResolver.sol:L66-L74 @ ens_v2_sepolia_20260916@366de741)
(upstream: .refs/ens_v1/contracts/universalResolver/ResolverCaller.sol:L66-L70 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/universalResolver/ResolverCaller.sol:L88-L96 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/universalResolver/ResolverCaller.sol:L108-L127 @ ens_v1@91c966f)

The family inventory reader reproduces this from current pointer keys and
classification at the selected publication. The registry's
resolver per node is the latest canonical `ens_v1_registry_l1`,
`ens_v1_registrar_l1`, or `ens_v1_wrapper_l1` `ResolverChanged` that addresses
that namehash, clears included and whether or not the event is linked to a
logical name or resource (the registry sets resolvers for nodes nobody owns in
ENSv1, whose [pre-surface](glossary.md#pre-surface) pointer keeps both null).
The addressed name is read in the adapters' shared ENSv1 node order:
`after_state.child_node`, then `after_state.namehash`, then `after_state.node`.
A state-derived pointer for a newly linked child keeps the `NewOwner`
observation, whose `node` is the parent and whose `child_node` is the child, so
it is the child's pointer, not the parent's. Pointer reduction and the history reader use this same node identity. The consulted
nodes are the queried name's surface and each proper ancestor surface below the
root, matched by label suffix in the same namespace; the nearest consulted node
with a nonzero resolver is selected.
When the selected resolver is a supported, same-namespace `ens_v1_resolver_l1`
declaration that is not itself a mirror and the selection is the exact node,
the mirrored resource is re-pointed at that resolver for the queried node and
the ordinary node-keyed attribution above computes its `selectors`, `entries`,
`unsupported_families`, `last_change`, record version boundary,
`provenance.record_event_ids`, `provenance.attributed_event_ids`,
`provenance.read_rules`, and `exact_nonempty_not_found_record_keys` exactly as
for an ENSv1 name served by that resolver. An ancestor selection is never
derived through, so the queried name never serves the ancestor's records or the
ancestor resolver's storage for the queried node.
`provenance.resolver_address` and
`provenance.resolver_pointer_event_id` stay the name's own mirror pointer, and
`provenance.mirror = {resolver_address, mirrored_source_family:
"ens_v1_resolver_l1", mirrored_registry_source_family: "ens_v1_registry_l1",
mirrored_registry_address, queried_node, mirrored_node, mirrored_name,
ancestor_depth, forwarding, mirrored_resolver_address, mirrored_resource_id?,
mirrored_pointer_event_id, mirrored_pointer_source_family}` records the walk
(`mirrored_resource_id` only when the selected pointer event carries one):
`mirrored_node` and `mirrored_name` are the selected registry node and its raw
name, `ancestor_depth` is `0` for the exact node and otherwise the number of
leading labels the walk stripped, and `forwarding` is `direct_call` or
`extended_resolve` per the selected resolver's declared read features. That
mode is inferred from the declaration alone. It does not claim that a call
happened or that the selected resolver passed the mirror's check; an unsupported
row's `mirrored_unsupported_reason` records a rejection.

Otherwise the row is `unsupported` with `mirrored_resolver_not_projected`, no
selectors, no entries, no record, link or attributed event ids, and no read
rules. Either no consulted node has a nonzero resolver
(`provenance.mirror` then carries only the registry fields and `queried_node`),
or the selected resolver cannot be derived through:
`provenance.mirror.mirrored_unsupported_reason` is
`resolver_classification_missing` (unclassified, or declared in another
namespace), the resolver's own unsupported reason, `mirrored_resolver_is_mirror`,
`mirrored_resolver_not_ensv1`, `ensip10_extended_resolver` (an ancestor whose
answer for a descendant is resolver-defined), or `ancestor_resolver_not_extended`
(an ancestor the mirror rejects because it does not support
`IExtendedResolver`), alongside the selected node. The classification reasons
come first, so an ancestor reason only marks an ancestor that would otherwise be
eligible. The row keeps the name's own mirror pointer as its boundary and
`provenance.resolver_address`, and the selected ancestor's address and pointer
event stay in `provenance.mirror`. This marker is internal composed
provenance only: the API's `include=inventory` and name-record diagnostics do
not serialize it, `data.resolver` stays the mirror, and an explicit live
fallback follows the ordinary verified-execution contract. A
mirror whose own classification is unsupported or belongs to another namespace
keeps the ordinary resolver reason or `resolver_classification_missing`. The
composition reads consulted surfaces, registry-node pointers, classification
and node-record values from the same admitted family snapshot. Reading an
ancestor adds no authority or exact-name row for it. A changed pointer,
classification or record is visible on the next composition, without a
persistent subscriber rebuild or temporary-table dependency expansion.
Address-to-record reads consume the same supported mirrored inventory as
name-side reads.

For ENSv1, an admitted current resolver may contribute supported address, text,
and contenthash inventory. An unlisted or unsupported resolver family stays
explicitly unsupported. For ENSv2, current-emitter version evidence may define a
boundary while the unadmitted resolver profile still publishes no record
values. Basenames record facts remain gated by the admitted Base resolver
profile. Readers enforce this on the inventory row itself: the records route,
name detail, batch lookup, the
`resolves_to` reader, and the divergence-ledger comparison take
values only from a `supported` row. Entries retained on an `unsupported` row are
diagnostics for operators, never answers
([api-v1-routes.md](api-v1-routes.md#get-v1namesnamerecords)).

`GET /v1/names/{name}/records` reads this inventory for `indexed` behavior,
and for every source to derive its default key set and `include=inventory`
container. `verified` and `auto` may use fresh schema-v2 lookup as described in
[`execution.md`](execution.md); they never read a legacy execution cache.

## Primary names

The reverse families retain declared claim state plus internal rolling
reverse-name polling selection state. Supported claim statuses are `success`,
`not_found`, `unsupported`, and `invalid_name`. A successful row keeps the raw
claim and whether its bytes already equal the normalized claim. The internal
selection columns are not claim fields and readers never select them. Project
does not persist a verified-primary result or trace identity.

For a retained `ReverseClaimed` tuple, Project joins its `reverse_node` to
node-keyed `NameChanged` records on the current registry resolver. No forward
name surface or resource attribution is needed. Resolver records written before
the tuple or before a resolver pointer change remain eligible when that resolver
is current. Changing the reverse node's owner alone does not clear its stored
name (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L170 @ ens_v1@91c966fe).
The reverse registrar emits the tuple before assigning the registry
resolver, then writes the name through that resolver
(upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L83 @ ens_v1@91c966fe)
(upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L129 @ ens_v1@91c966fe).

Project selects the last canonical name write or record-version reset for that
node and resolver at the projection head, ordered by block, transaction, log,
and emission ordinal. A version reset or blank name yields `not_found`;
Names retained only as bytes yield `unsupported`. Changing away from a resolver stops
using its name; changing back exposes that resolver's retained current-version
name. This follows the resolver's version-keyed storage and reset behavior
(upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L17 @ ens_v1@91c966fe)
(upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L28 @ ens_v1@91c966fe)
(upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L21 @ ens_v1@91c966fe).
Name, version and resolver changes update their family keys; the claim reader
selects their current state. Explicit `NameForAddrChanged` tuple claims retain
their existing event path. These are declared claims; forward verification
remains request-scoped.

Configured mainnet follow blocks prepare reverse hydration
before opening the publication transaction. A short preparation transaction uses
the normal pointer and reverse reducers to include the new block's candidates,
then closes before the hash-pinned RPC calls. The publication transaction checks
the predecessor, input revision and block hash again, reduces the events, and
accepts an answer only for the same selected reverse node and resolver. The
result and its baseline enter F12's owned row set and are journalled with the
family marker, including refresh work on empty blocks. Failed calls retract the
overlay and publish the block; a later follow block retries through the bounded
rolling selection. Successful not-found is distinct from failure. Attempt cohorts
use the monotonically increasing publication generation. The reader also binds
the overlay to its selected node/resolver and readable block hash.

Replay and rebuild perform no hydration RPC. Undo restores the previous overlay
with its row, and new or changed selectors use event-derived claims until a later
follow block refreshes them. Rebuild ranges retain their existing behavior.

Each follow block refreshes eligible tuples changed by the block, then at most
250 additional eligible tuples. Rolling selection orders never-attempted tuples
first, then the least recently attempted group; within a group it uses the
oldest successful hydration height and stable tuple identity. Attempts use the
publication generation as a durable ordering value. A failed group advances in
the rotation and exposes the event-derived baseline, so it does not repeatedly
starve older waiting groups. These counters never make a failed result readable.
A completed same-head run performs no extra hydration tick. Subsequent follow
blocks refresh eligible tuples; replay remains provider-free.

The reader accepts an overlay only while its baseline reverse node and resolver
still match the current claim and its hydration block remains readable. A
selector change after replay immediately exposes the current event-derived
claim, even if undo restored an older overlay. Successful empty responses mean
`not_found`; an invalid name remains `invalid_name`; failed calls add no claim.

Verified ENS/60 primary-name status is computed per request by schema-v2 lookup.
It does not require a projected declared claim: the route performs a fresh
reverse lookup, requires that live claim to be byte-normalized, and accepts it
only when the forward address matches. A projected claim, when present, remains
an indexed candidate and an input to the pre-forward authority gate; tuple
presence alone does not prove primary status.

## Reorg and redo

Canonicality change, manifest change or interpreted-content replacement stamps
the affected Project range. Project undoes family publications to a trusted
base and replays activated canonical input. The family journal includes child
history, current keys, derived indexes and hydration overlays. The durable
repair record tracks the attempt and its `undoing`, `replaying`, `rebuilding`
or `complete` state. The old pre-delete resolver, expiry-root and child-history
handoffs are removed: replay starts from the restored per-key state, rather
than trying to rediscover affected names from deleted aggregate serving rows.

A reset rebuild clears chain-local family state and derives it again from
retained interpreted input. Readers refuse the rebuilding marker. A malformed
undo journal is a data-integrity failure, not permission to silently discard
it; an operator can choose a rebuild after investigating. The family marker
and repair record are the restart boundary, independent of a process's last
in-memory batch. There is no reusable execution cache or invalidation worker.

`phase-runner rewind` selects an exact stored readable ancestor, marks the
displaced suffix orphaned through normal head publication, and stamps downstream
redo. Before changing heads or lineage, it refuses to orphan the retained end
of an unfinished operator Ingest redo. Complete the reported covering Ingest
repair with configured sources before retrying; required Ingest work retains
its [Live recovery path](chain-intake.md#redo-and-rewind).
Historical API reads serve only when eligible projection materialization
exists for the selected positions; they never overwrite newer current rows or
fall forward to current state.

An interpreter content-hash rotation requires a full-history Interpret and
Project walk. Phase state and API admission refuse to mix output from different
compiled hashes.

## Owned key families

Project keeps per-key current state for the facts readers compose, grouped by
owned family: name identity and binding candidates,
registration and lease state, wrapper state, registry ownership, resolver
classification, the registry-node and resource resolver pointers, node and
record-id records with resolver links, grants and account approvals, aliases,
child edges, reverse tuples and claims, and the address associations. Each row
belongs to one key and holds what the latest events of that key left, clears
included: a zero pointer, record id `0`, a revoked grant or an inactive alias
stays a row. A row goes only when nothing remains for its key. Each grant also
carries `registration_position`, the the position of the resource's latest `RegistrationGranted` or
`RegistrationReserved` before the grant, earlier events of the grant's own block
included, so the publishing steps can tell which registration of the resource a
grant was written under. Permission composition uses that position with the current resource lifecycle
when deciding whether the grant remains effective.

The Universal Resolver proxy family (`project_universal_resolver_proxy`) keeps,
per declared `ens_execution` proxy, the implementation its latest `Upgraded`
installed and how Interpret classified it against the manifest. The composed
name reader follows the client-facing proxy's chain through these rows once per
batch to decide whether the publication is past the
[Universal Resolver cutover](glossary.md#universal-resolver-cutover), which
moves the served expiry of reserved `.eth` names and withholds resolution from
`.eth` names ENSv1 decides without a live ENSv2 entry (`docs/api-v1.md` §
Expiry and grace). Schema-migration
`20260929200000_project_universal_resolver_proxy.sql` adds the table; an empty
table reads as not cut over, and the content-hash rotation that ships with it
rebuilds the families.

F5 keeps two independently owned pointer keys. `project_resource_pointer` keeps
one resource's latest pointer, including unnamed changes, for root, alias and
wildcard composition. `project_named_resource_pointer` keeps the latest named
`ResolverChanged` per `(chain_id, resource_id, logical_name_id)`, clears included.
Only events carrying both keys write it; unnamed changes and changes naming
another name leave it alone. A release does not delete a pointer fact: the
composed reader still applies its binding and reachability rules. Both reducers
use the same canonical event order and before-image journal as every family.

The composed name reader fetches named pointers by exact resource/name pairs.
Bound-name discovery uses `project_named_resource_pointer_resolver_idx` to find
retained named pointer keys at the requested resolver, alongside the existing
resource and registry-node pointer paths. It does not scan the resolver's
`ResolverChanged` history. This bounds that input to retained pointer keys, not
to page size: the candidate walk and sort can still visit the resolver's retained
keys on each batch. Candidate enumeration and sorting can exceed page size; realistic-scale
latency remains a rollout acceptance check.

Schema-migration `20260929140000_named_resource_pointer.sql` adds the named key
table to an existing phase schema and atomically clears all family rows, the
marker, journal and repair record when the table was absent, including
`project_name_summary` when it exists.
The next family
run rebuilds from canonical interpreted input; fenced reads remain stale until
the new publication is live. Reapplying the migration preserves an existing
publication. Fresh initialization installs the same table from the baseline.
The reducer source participates in the shared content fingerprint, so an older
family build cannot be served under the new binary's hash.

Registration and lease state also keeps every registration, renewal, release,
reservation, expiry change and token transfer of a lease or ENSv2 triple as a
row of its own, never pruned. Each row keeps the name the adapter emitted,
which nothing rewrites, beside the name the ENSv1 registrar and wrapper linking
gives it. The address-to-name and address-to-record index rows are not kept
state: after every block and every undo they are derived again for the keys the
block touched. A block that changes an ENSv1 registry node's owner row or one
of its child edges touches the node's `<namespace>:<node>` name id, with or
without a surface, so the index rows of a registry child follow its registry
owner. Resolver classification classifies a resolver at the block that
changed its candidates, the pointers that name it, its proxy upgrades, a
discovery edge, address or declaration of it, or the [active manifest
set](glossary.md#active-manifest-set-family-block), with the
manifests active at that block.

One family is not event-keyed: the [name summary](glossary.md#name-summary)
(`project_name_summary`) holds, per name, the fields the child and label lists
filter, sort and count by inside one statement: the selected authority arm,
whether the name has a serving resource, the registration status, the expiry
and registration times, and whether the latest registry Transfer attributed to
the name names the zero owner, attributed by the child-read contract (by the name the Transfer carries, else the latest named registry event of
any kind of its resource and family, read from the readable interpreted events,
else an active surface at its node), and the owner the name row serves
(`control.owner`, else `control.registry_owner`, lower-cased), which the
registry labels' `owner` and `exclude_owner` filters read. Every name with a
surface has a row. The selected arm remains available when an unreadable token
lineage withholds the composed name row: child relations still use that selection,
while optional name fields remain absent. A list cannot compose those at read for every child of a parent, so
the name row is composed at read except for this summary, which is
stored. After a block writes its other family rows, and on a block that writes
none, the family step composes the summary again, with the composed name
reader's own selection, for every name the block touched: the names, nodes and
resources of every row its journal names, each resource widened to the names
whose candidates, key states, association targets, lifecycle events, wrapper
row, owner events or pointer read it, every name a registry event carries on a
resource that a registry event of a block since the family marker's carries
(such an event can move the resource's unnamed Transfers to another name, and a
rebuild range composes once for all its blocks), every name whose surface
appeared since the family marker's block, and every name whose stored
`recompose_at` the block's time has reached. A block that changes a Universal
Resolver proxy row also recomposes every name with an ENSv2 reservation, since
the cutover moves the expiry those names serve. Finite lifecycle expiry and
the summary expiry used for ordering are exact numeric Unix seconds, including
the full finite ENSv2 `uint64` range. Family readers, expiry indexes, retained
keys and undo journals preserve the integer without calendar conversion or
floating-point rounding. Contract-specific absent-expiry classification occurs
with the selected registration context; a large finite value stays finite. The
API renders the integer as a decimal string (see
[the timestamp contract](api-v1.md#timestamp-format-and-absent-expiry)).
`recompose_at` is the first second
at which the name's composition can change with no fact changing: a binding
interval opening or closing, or a NameWrapper expiry or grace boundary. It is
stored in Unix seconds, since a NameWrapper expiry can lie past the last
instant a timestamp holds, and kept for a name that composes no row. A summary that
changed is journalled and written like any other family row, so an undo
restores it from the journal and composes nothing; a rebuild composes every
surfaced name.

A stored summary is therefore refreshed only when a block touches the name or
its scheduled boundary passes, and the work list is deliberately no wider.
Inputs that change in place without either, such as a normalizer recompute of
a surface's visibility or a lineage readability flip, are covered because a
recompute only happens with a code change that rotates the interpreter
fingerprint, which rebuilds the families. A reorg goes through undo, which
restores the summaries from the journal.

### Publication and resumption

ProjectPhase invokes the family runner directly. One run applies or undoes at
most 256 blocks by default (`--project-families-max-blocks`, or
`BIGNAME_PHASE_RUNNER_PROJECT_FAMILIES_MAX_BLOCKS`). A run with remaining work
records its committed progress and continues in normal mode. The operator redo
command uses the same machinery, preserving its attempt and frozen replay
target across bounded continuations. `recompute-flags` belongs to Interpret;
its ordinary downstream Project redo uses this path.

The batch prelude checks the phase lock, heartbeat and storage capacity. Its
write estimate derives from the preceding family's reported row count and is
not a storage reservation. Long runs retain durable marker progress between
blocks or ranges. Cancellation rolls back uncommitted work; a commit already
in progress may finish. Restart uses the committed marker and repair record.

Before work, Project captures the Interpret/Project input token with a bounded
read. Each publication transaction locks the planned family marker and repair
record, checks the expected generation and repair attempt, and requires the
canonical predecessor and target block. It rechecks the interpreter content
hash, redo state and family input revision. A mismatched revision or overlapping
Interpret repair fails the run. Active manifests come from the run's admitted
manifest input and are selected at each block; the marker records that set.

The transaction journals each changed row's before-image and the prior marker,
writes the changed keys and derived state, and advances the marker sequence.
Conflicting duplicate deliveries of one event identity keep the first in
canonical order and increment the anomaly metric. The normalized input table
already enforces unique event identities.

A family failure leaves the committed prefix in place and fails Project.
The supervised runner retries with backoff, including family data-integrity
failures; a one-shot redo returns the failure and leaves repair unfinished for
an explicit rerun. The failure is not converted into a successful publication.

Verified lookup captures topology, inventory, participating publications and
real manifest provenance in one snapshot. After RPC, the guarded ledger writer
locks and checks that captured state and overlapping Interpret/Project redo
state through its transaction. A family advance, reset, changed manifest or
new overlapping repair refuses both insert and clear; an ordinary Project
progress/status update alone does not. See
[verified lookup storage](storage.md#verified-lookup-storage).

### Bounded rebuild ranges

A rebuild (a first build, or a rebuild after a content hash change, a redo
below the kept journal or an orphaned lineage) applies its work blocks at or
below a switch point several to a transaction, in [rebuild
ranges](glossary.md#rebuild-range). The switch point is the chain's safe block
minus 5, read from `chain_heads` once per run, or 256 blocks below the target
when no safe block is published. Work blocks above it, and the target, which
completes the rebuild, go one to a transaction as above, so a reorg near the
head still undoes single blocks. The switch follows the safe block, not the
finalized one, by the product owner's ruling; a safe block is not final, and a
reorg whose fork point lies inside a range undoes that whole range and replays
from its predecessor. The first range after the rebuild's reset holds one work
block. After each range commits, the next asks for twice the blocks that range
actually applied, which after an event-cap cut is fewer than it asked for; a
later run that resumes the rebuild starts by asking for its whole remaining
budget. A request is capped at 1,024 work blocks and at what the run's budget
has left, since each block counts against the budget: under the default budget
of 256 blocks a run the budget is the binding cap, and 1,024 binds only when a
run's budget is larger. A range also ends at 4,096 events, counted per block
before duplicates are dropped: it ends before the block that would take it past
the cap and always holds its first block, so a first block over the cap is a
range of its own. A range opens with its first block as a single block does,
passing the fences above once and reading that block's lineage row, then reads
the remaining blocks' lineage rows in one statement and their event counts in
another. For the blocks it keeps it reads the events, surface bindings and
resolver activations in one statement each, grouped back by block: events are
ordered and taken once per `event_identity` within their own block, and each
block takes its own active manifest set. Before folding it loads, once per
table, the rows the blocks' events and prefetched bindings and activations
name; a row a reducer finds only while folding is loaded then. It folds the
blocks one at a time through the same reducers, each block seeing the families
as the blocks before it left them. The eight reads that go to a family table
mid-fold also see the rows the range changed and has not written yet: the name
candidates, registry-only candidates, retained grants and predecessor releases
of F1's binding candidates, F3's stored classifications, the lease candidates
and unnamed registrar rows of name decoding, and the lifecycle rows of F13's
registrant fold. The classification read adds the resolvers the range stored;
one the range removed still comes from the table, but with no candidate left it
gets no row, as block by block. A block classifies resolvers and reads a name's
current binding at its own height. The range then journals, for every row it
changed, the row as it was before the range and the prior marker, under its
last block, writes once, advances the marker to its last block still in
`bootstrap_pending`, prunes and commits: one generation. An undo takes the
whole range back at once, so undo to a block inside a range stops on the
range's predecessor. The family tables, and the marker apart from its
generation, equal applying the same blocks one by one
(`crates/project/tests/families_range.rs`, and every test's rebuild comparison,
which rebuilds both ways); the undo rows and the generation count differ by
design. Redo replay and live follow stay one block to a transaction.

### Ordering and undo

Events fold by block, transaction, log, semantic event priority, and the
[emission ordinal](glossary.md#emission-ordinal) within an adapter write batch.
The stable event identity breaks remaining ties; generated database IDs do not
determine protocol event order. Incremental, undo/replay and rebuild use the
same reducers and ordering.

Undo rows are kept back to the lowest of: 256 blocks below the family marker,
the chain's finalized block, its safe block, and the block an active repair
still has to undo to or replay from. Without a finalized and a safe block
nothing is pruned. A rebuild range's undo rows are one journal entry under the
range's last block.

A Project redo undoes the families from their journal down to the block before
the redo range and replays them to the frozen replay target. A marker left on a block
that is no longer readable is undone the same way. A redo below the kept
journal, a redo attempt the families never saw, or an explicit reset rebuild clears the
families and rebuilds them from the blocks that carry events or surface
bindings or start or stop a resolver activation, so a rebuild visits every
block the normal path writes a binding candidate in. So do families whose marker records a content hash
other than the running binary's, so a partially completed older build cannot be served by the new binary. An undo journal the families refuse, such as one whose prior
markers form a cycle, does not trigger a rebuild: every run that needs it
fails with a data-integrity error and changes nothing, and the Project run
fails and is retried, until an operator runs a rebuild or a redo below the kept
journal. That is deliberate: a
malformed journal is a defect to look at, not state to rebuild over silently.
The repair record describes the latest of these: its
attempt, reason, trusted base, replay target, state (`undoing`, `replaying`,
`rebuilding` or `complete`) and, once done, the marker, generation and input
content hash it completed with. Each transition commits with the work it
describes: the reset commits with the rebuild's intent, the last undo with the
move to replaying, and the final replayed or rebuilt block with the
completion. A run that stops between blocks is resumed by the next. Each
start of a Project redo moves the Project row to a new redo attempt, so a
rebuild records the attempt it began under. When the runner reruns an
interrupted redo with the same range while the Project row still records the
running binary's interpreter content hash (no other hash and no manifest or
authority invalidation marker in between), keeping its saved progress, a rebuild the attempt just before it left is
carried over to the new attempt and resumes from the family marker, provided
its input revision is unchanged and the families were written under this
binary's content hash. Otherwise, including a rerun over another range, a
skipped attempt, an invalidation or a moved input revision, the redo rebuilds
from the start. A
redo retried after it completed is recognised only while the marker, its
generation and the content hash still match. A rebuild refreshes the planner
statistics of the family tables after 1, 2, 4, 8, ... generations (single
blocks or rebuild ranges) committed since its reset, counted across runs from
the generation the reset recorded. Because a range is one generation, a
rebuild in ranges refreshes after more blocks than one block by block. With
the default 256-block budget, no event-cap cuts and work still below the
range switch, the first run commits nine generations (1, 2, 4, ..., 128 and 1
blocks) and each later run one, so the fifth refresh (after 16 generations)
lands after 2,048 work blocks and the eighth (after 128) after 30,720.

### Validation boundary

Permanent tests exercise actual normalized inputs, family publication, endpoint
composition and undo/replay. The end-to-end corpus compares all registered
family tables and exact name/subname endpoint state between incremental and
rebuild runs. Hydration has separate follow and replay tests, since live
provider observations are not deterministic rebuild inputs. This source/test
acceptance does not replace real-scale latency and operational rollout gates.

## Index baseline

Indexes follow measured serving queries. Baseline access paths cover exact-name
identity, address relation membership and pagination, parent-child collections,
resource permissions, resolver identity, record-inventory boundaries, primary
claim tuples, normalized-event history, and phase lineage/head selection.
Adding a compact route may justify another measured index; it does not create a
new truth family.

## Ownership

- Interpret owns identity, discovery, normalized input, admission and its
  diagnostic evidence. Adapters supply protocol interpretation behavior.
- Project owns the family reducers, derived indexes, name summary, historical
  child-registration membership, hydration overlays, marker, journal and repair
  record. All production schema-v2 projection readers use this publication.
- API reads admitted projections, normalized history and request-scoped lookup
  output. Raw diagnostics retain their separate unbounded audit contract.
- Storage exposes typed snapshot reads and guarded publication boundaries. It
  grants no adapter or API projection-write shortcut. GraphQL remains a separate
  compatibility contract.


---

[^bn-readme-l70]: (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)
[^v1-l2rev-base-deploy]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L2 @ ens_v1@91c966f)
[^v1-l2rev-event]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L98 @ ens_v1@91c966f)
[^v1-registry-l45]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L45 @ ens_v1@91c966f)
[^v1-registry-l82]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L82 @ ens_v1@91c966f)
[^v1-wrapper-grace-expiry]: (upstream: .refs/ens_v1/contracts/wrapper/README.md:L69 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L806 @ ens_v1@91c966f)
[^v1-wrapper-grace-authority]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L218 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L221 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L820 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L828 @ ens_v1@91c966f)
[^v1-wrapper-expired]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L852 @ ens_v1@91c966f)
[^v2-events-l49]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L56 @ ens_v2@a971bd64)
[^v2-events-l75]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L88 @ ens_v2@a971bd64)
[^v2-iperm-l57]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L83 @ ens_v2@a971bd64)
[^v2-pr-l261]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L304 @ ens_v2@a971bd64)
[^v2-pr-l351]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L363 @ ens_v2@a971bd64)
[^ensnode-legacy-text-l356]: (upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L356 @ ensnode@2017ae6) (upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L364 @ ensnode@2017ae6)
[^ensnode-legacy-revresolver-l311]: (upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L311 @ ensnode@2017ae6)
[^ensnode-legacy-revresolver-l316]: (upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L316 @ ensnode@2017ae6)

[^owner-v2]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L482 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L531 @ ens_v2@a971bd64)
[^owner-v1]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L123 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L71 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172 @ ens_v1@91c966f)
[^owner-bn]: (upstream: .refs/basenames/src/L2/Registry.sol:L165 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L285 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L321 @ basenames@1809bbc)
