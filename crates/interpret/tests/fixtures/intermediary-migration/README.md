# Executed local intermediary migration

`chain.json` preserves the complete captured local chain for
`audit-extra-transfer.eth`: 51 headers, 50 transactions and receipts, and 60
logs. Its SHA-256 is
`72193ba5b0e7c35a867e3ae4ea046a5a8da0367ccc8951e82d44a3c436a0b54d`.
The corpus was produced by actual Anvil execution using ENSv1 at
`91c966febd7b55494269df830fc6775f040b927b` and the October ENSv2 deployment
artifacts at `07e55a056f5b6a9c90119f501bdd05714e67dddd`.
The manifests preserve the event definitions with the actual local deployment
addresses and start blocks. `deployment.json` identifies those addresses;
`MigrationBatcher.sol` is the approved intermediary used in the execution.

After numeric ENSv1 registration and an ENSv2 reservation, the owner approves
the intermediary. Its `migrate` call transfers the registrar token from the
owner to itself and then transfers it into the migration controller. Transaction
`0x4e823057919a1134ed73393e0b26592d138d2c97e52570dede40282ba3e4194f`
at block 50 emits:

| Log | Effect |
| --- | --- |
| 0 | Registrar token: owner → intermediary |
| 1 | Registrar token: intermediary → controller |
| 2 | ENSv1 registry reclaim to controller |
| 3 | ENSv1 registry owner → Graveyard |
| 4 | Registrar token: controller → Graveyard |
| 5–8 | ENSv2 registration, initial mint, resource link and role grant |

The controller accepts the BaseRegistrar callback, validates the label and token,
then performs reclaim, registry cleanup, registrar cleanup and ENSv2 injection.
(upstream: .refs/ens_v2_sepolia_20261001/contracts/src/migration/UnlockedMigrationController.sol:L92-L120 @ ens_v2_sepolia_20261001@07e55a05)

The test derives all state from the captured raw facts through the real Engine
and writer, then publishes with Project. It checks exact-capture processing,
restoration, continuing batches, combined processing and redo, including the
ordinary log-0 authority history and its nonempty cleanup interval.

Two controls are deliberately generated from this corpus. The timing control
adds `block_number * 12 seconds` to the supplied block timestamp while retaining
the receipt bytes; it is not another executed or cryptographically valid chain.
The ordinary-prefix control selects only log 0 from the final transaction to
compare its complete normalized facts and binding fields. That partial selection
is not claimed to be a complete transaction. Both controls remain separate from
the unchanged captured JSON.
