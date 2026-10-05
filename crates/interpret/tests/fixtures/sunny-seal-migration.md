# Recorded unwrapped migration

`sunny-seal-migration.json` contains the Sepolia transaction
`0xf74cdf176735299f0101d9ea60b2cf0f91da420891f987ab17c11809a5e16253`
at block `11849653`, transaction index `58`, and its complete 19-log sequence
(indices 80–98). The six earlier raw logs cover the numeric registrar grant,
resolver, token transfer, ENSv2 reservation and resolver, and registry owner.
Their five transaction envelopes and the selected blocks' actual canonical
headers are retained, including predecessor headers required by the Engine.

Nine normalized events preserve the indexer's retained registrar authority and
ENSv2 reservation state immediately before the failed migration. The registrar
lease exists, but its earlier events are unnamed and its retained grant has
`surface_known: false`. The captured state had no committed ENSv1 binding.
Database allocation IDs and observation timestamps are omitted; raw chain
bytes, event identities, manifest IDs, resources, lineage and state payloads
are preserved. This is a bounded reconstruction, not a complete historical
database export.

The test installs the checked-in manifests through normal synchronization,
preserving the four manifest IDs referenced by the recorded state. Earlier
raw observations materialize identity prerequisites, then the recorded tails
and absence of bindings establish the captured snapshot. A fresh Engine
interprets the complete transaction through the production adapter and writer.
The warmed comparison advances over the preceding header with no selected
raw logs before continuing with the same Engine. Project reads the committed
result. Negative cases deliberately omit individual proof logs or retain only
the ordinary incoming transfer; those subsets are not claimed to be complete
on-chain transactions.

After the recorded migration and redo, the restore test adds an explicitly
generated next-block renewal of the same lease through the admitted wrapped
registrar controller. It includes the encoded call, EOA payment, BaseRegistrar
renewal and following controller renewal; the unwrapped token emits no wrapper
log. Its header, transaction and logs are test data, not part of this capture. The
renewal must retain the Graveyard holder and leave current ENSv2 authority
unchanged after a fresh Engine restore and Project publication.

A separate generated boundary case advances past the old lease's expiry and
grace period and uses an authorized BaseRegistrar `registerOnly` burn, mint
and numeric grant. It verifies the new lease gets its own binding and a later
ordinary renewal preserves that binding. None of those generated future
transactions or timestamps are claimed as observed Sepolia activity.
