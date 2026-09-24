# ADR 0007: Follow The Chain For ENSv1 And ENSv2 Name Authority

Status: Accepted
Date: 2026-09-24

## Context

During the ENSv1→ENSv2 migration one `.eth` name can have facts on both
protocol arms: an ENSv1 registration or registry entry, and an ENSv2 registry
entry. bigname must pick one arm for every current field of the name (its
[authority epoch](../glossary.md#authority-epoch)).

Until this ADR, bigname picked an arm only when it had seen an admitted
[authority proof](../glossary.md#authority-proof) (an activated
`MigrationApplied` boundary or a positive ENSv2 child registration), a
qualifying ENSv2 release, or only one arm. Otherwise a name with evidence on both
arms was refused: its profile was served identity-only with the reason
`independent_ens_deployments_overlap` on Sepolia or
`conflicting_current_ens_authority` on Mainnet. The rule treated choosing an arm
without a proof as inventing an authority boundary. Only the four exact
[shared ENS infrastructure](../glossary.md#shared-ens-infrastructure) names were
excepted.

On Sepolia that refused 652 names on 2026-09-23. None of them was live on both
arms at once. In 649 of them one arm held only history: 466 names live on ENSv1
whose ENSv2 label had been granted and released again, and 183 names whose
ENSv1 registration had ended before a fresh ENSv2 registration. The code refused
those because it counted any authority event ever seen on an arm, which was
wider than the documented rule about current candidates. The other three had an
open ENSv1 binding (a registry owner left over after expiry, or an ENSv1
registry owner change after a wrapped name moved to ENSv2) next to a current
ENSv2 registration, with no migration proof bigname could see.

The ENSv2 contracts answer this question themselves, without a proof:

- The ENSv2 `.eth` registrar checks only the ENSv2 registry. `register` requires
  the label to be available on ENSv2, which means its ENSv2 entry has expired and
  its ENSv2 grace period has passed. It never reads ENSv1.
  (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registrar/ETHRegistrar.sol:L142 @ ens_v2_sepolia_20260916@366de741)
  (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registrar/ETHRegistrar.sol:L245-L257 @ ens_v2_sepolia_20260916@366de741)
  (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registrar/ETHRegistrar.sol:L284-L292 @ ens_v2_sepolia_20260916@366de741)
- The ENSv2 registry refuses to overwrite a registered label, and a reserved
  label can become registered only through a caller holding
  `ROLE_REGISTER_RESERVED`.
  (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L466 @ ens_v2_sepolia_20260916@366de741)
  (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L471 @ ens_v2_sepolia_20260916@366de741)
- What keeps a live ENSv1 name from being registered afresh on ENSv2 is the
  premigration script, not the contracts: it writes each live ENSv1 name into
  the ENSv2 registry as a reservation with owner zero and `ENSV1Resolver` as its
  resolver.
  (upstream: .refs/ens_v2_sepolia_20260916/contracts/docs/premigration.md:L3-L8 @ ens_v2_sepolia_20260916@366de741)
  (upstream: .refs/ens_v2_sepolia_20260916/contracts/docs/premigration.md:L149-L150 @ ens_v2_sepolia_20260916@366de741)
  (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registrar/BatchRegistrar.sol:L64-L65 @ ens_v2_sepolia_20260916@366de741)
- The ENSv2 Universal Resolver has one read path and it starts from the ENSv2
  root registry. It walks ENSv2 registries label by label, taking each entry's
  resolver, and never reads the ENSv1 registry.
  (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/UniversalResolverV2.sol:L56-L63 @ ens_v2_sepolia_20260916@366de741)
  (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/libraries/LibResolution.sol:L58-L85 @ ens_v2_sepolia_20260916@366de741)
  The registry returns an unexpired entry's resolver whether the entry is
  registered or reserved.
  (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L283-L286 @ ens_v2_sepolia_20260916@366de741)
  So a registered ENSv2 entry answers from its own ENSv2 resolver, whatever
  ENSv1 holds. A reserved entry answers through `ENSV1Resolver`, which looks the
  name up in the ENSv1 registry.
  (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/resolver/ENSV1Resolver.sol:L40-L43 @ ens_v2_sepolia_20260916@366de741)

The proof the refusal waited for is therefore a product of the migration
scripts, and the chain does not need it to decide who holds a name.

## Decision

bigname follows the chain. For an ordinary ENS name:

- **An ENSv2 registration decides.** When the name has an ENSv2 binding open at
  the target block, which only a registered ENSv2 entry creates, bigname selects
  ENSv2. This holds whatever ENSv1 holds and without a migration proof. The
  authority epoch starts at that ENSv2 binding, and no proof fields are
  published.
- **An ENSv2 reservation defers to ENSv1.** A reservation opens no binding and is
  not ENSv2 authority, as before. A name with a live ENSv1 registration and only
  a reservation is served from ENSv1. A name whose ENSv1 registration has ended
  and that has only a reservation has nothing current: it is served as the
  released ENSv1 registration, or as `current_authority_not_projected` when no
  released registration qualifies.
- **No ENSv2 entry means ENSv1 decides.** Without an open ENSv2 binding the
  name's open ENSv1 binding is selected. A name with no open binding on either
  arm follows its ENSv1 history when it has any, and its ENSv2 history
  otherwise.

The earlier decisions keep their precedence and their meaning: an activated
ENSv1→ENSv2 migration proof or a positive ENSv2 child registration selects
ENSv2 from the proof's position, a qualifying ENSv2 release keeps the released
ENSv2 tombstone, and the four shared ENS infrastructure names select a current
ENSv2 arm without publishing an authority epoch when ENSv1 evidence exists.

No name is refused for holding facts on both arms any more. Project no longer
produces `independent_ens_deployments_overlap` or
`conflicting_current_ens_authority`.

An ENSv1 lease that is still live under a name ENSv2 now holds keeps the
visibility it has after a proven migration: its events stay in name history,
and its permission rows stay in the permission views. It supplies no current
field of the name.

The dual-current generation halts keep their post-proof scope. They still stop
a publication when a name with an activated ENSv1→ENSv2 migration proof keeps a
current ENSv1 binding, or when a child whose ENSv2 authority is proven keeps an
ENSv1 relation asserted after that authority began. A name selected by the
ENSv2 registration rule has no proof, so its live ENSv1 lease never reaches
either halt.

## Upstream anchors

- ENSv2 `.eth` registration checks only ENSv2 state: `ETHRegistrar.sol` L142,
  L245-L257 and L284-L292, cited above.
- The registry refuses to overwrite a registered label and needs
  `ROLE_REGISTER_RESERVED` for a reserved one: `PermissionedRegistry.sol` L466
  and L471, cited above.
- Premigration reservations carry `ENSV1Resolver`: `premigration.md` L3-L8 and
  L149-L150, and `BatchRegistrar.sol` L64-L65, cited above.
- The Universal Resolver reads only ENSv2 registries, and `ENSV1Resolver`
  forwards to the ENSv1 registry: `UniversalResolverV2.sol` L56-L63,
  `LibResolution.sol` L58-L85, `PermissionedRegistry.sol` L283-L286 and
  `ENSV1Resolver.sol` L40-L43, cited above.

One case differs from the Universal Resolver. When a `.eth` label has no live
ENSv2 entry, the resolver walk keeps the nearest ancestor's resolver, and the
deployment registers `eth` in the root registry without one, so the Universal
Resolver finds no resolver and the lookup fails.
(upstream: .refs/ens_v2_sepolia_20260916/contracts/deploy/01_ETHRegistry.ts:L36-L48 @ ens_v2_sepolia_20260916@366de741)
(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/AbstractNormalizedUniversalResolver.sol:L406-L408 @ ens_v2_sepolia_20260916@366de741)
bigname instead lets ENSv1 decide such a name, which is what the ENSv1 registry
itself records. The premigration reservation normally covers every live ENSv1
name, so this case arises only for a live ENSv1 name whose reservation was
used and released, or that was registered on ENSv1 after the premigration
snapshot. The difference is listed in
[`upstream.md`](../upstream.md#ensv1-authority-without-an-ensv2-entry).

## Consequences

- Names that were identity-only with `independent_ens_deployments_overlap` or
  `conflicting_current_ens_authority` are served. A name with a current ENSv2
  registration is served from ENSv2; any other such name is served from ENSv1,
  including as a released ENSv1 registration. On Sepolia this covers all 652
  names refused on 2026-09-23.
- A name registered on ENSv1 after the premigration snapshot and then registered
  on ENSv2 is served from ENSv2, which is also what the Universal Resolver
  returns.
- The API keeps treating the two retired reasons as unsupported reasons that
  reduce a name to its identity fields. It only sees them on projection rows an
  earlier interpreter derived before the required redo.
- Selecting an arm now reads only the arms' current bindings, and falls back to
  authority-event history only for a name with no open binding. Cross-era
  recency still never chooses an arm.
- The change rotates the
  [interpreter content hash](../glossary.md#interpreter-content-hash), so every
  deployment needs a complete retained-range Project redo before the matching API
  is served.

## Rollout

Doc-first. This ADR and the contract docs change first; the Project authority
selection follows in the same pull request. The rule applies to every ENS
[deployment profile](../glossary.md#deployment-profile). Mainnet has no ENSv2
deployment yet, so nothing changes there until ENSv2 facts appear, and then the
same rule applies.

## Alternatives considered

**Keep the refusal and only narrow it to names current on both arms.** This
clears the names where one arm holds only history, but still refuses a name
with a live ENSv1 binding next to a current ENSv2 registration. The chain
resolves that name from ENSv2, so the refusal would keep hiding a name the
Universal Resolver answers.

**Keep the refusal as it was.** Every name touched by both eras would stay
identity-only until bigname sees a migration boundary it can prove, even though
the chain has already decided who holds it.

## References

- [`architecture.md` § ENSv1→ENSv2 current authority](../architecture.md#ensv1ensv2-current-authority)
- [`manifests.md` § ENSv1 (`sepolia` deployment profile)](../manifests.md)
- [`consumer-capabilities.md`](../consumer-capabilities.md)
- [`api-v2-routes.md`](../api-v2-routes.md)
- [`upstream.md` § Known divergences](../upstream.md#known-divergences)
- Linear TYR-13
