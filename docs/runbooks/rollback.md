# Rollback Runbook

Run the checked-in rollback smoke gate from the rollback checkout:

```sh
scripts/rollback-smoke
```

For CI or a fully cached local environment:

```sh
scripts/rollback-smoke --no-network
```

The gate checks local pinned upstream refs, builds the API binary, and verifies
`/healthz`.

`--no-network` makes Cargo offline and requires the health endpoint to be
loopback. It does not skip PostgreSQL or runtime checks.

Before changing deployed binaries:

1. drain traffic that could cross incompatible projection publication
   boundaries;
2. run the rollback smoke gate against the exact rollback checkout;
3. confirm the rollback image uses the intended manifest tree and migration
   set; and
4. keep the public edge on the maintainer-approved policy for that binary.

For the current C2 state, the binary has no v1 REST routes and the pre-C3 edge
does not expose `/v2`. Do not improvise an edge flip as part of rollback; the
C3 public-edge change remains separately maintainer-gated.

Do not proceed when health cannot prove the expected database and phase-runner
state. Apply any rollback migration plan separately; the deleted worker
migration command is not part of this gate.
