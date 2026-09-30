# PostgreSQL-native operational restore exercise

Supervisor explicitly adopted native-replacement-proposal.md after the exhaustive-equivalence attempt stopped.
Its drafts and Pro receipts are preserved separately as failed attempts; they are not evidence of a completed proof.
This replacement uses existing fixture helpers and PostgreSQL-native transfer with selected application checks.
Scope ceilings remain driver 1150, shell 230, checkpoint SQL 650, roles 80, contract 110 and accessor 10 gross lines.
All actual additions plus deletions count, including ignored files. No seventh helper, dependency or manifest change.

## Inputs and ordinary H0

Use base 045739d08ad4c27211f09f0141375e987df84ae8 and the counted overlay, rechecked before execution.
Use the existing Sepolia Anvil wrapper, chain ID 11155111 and genesis timestamp 1750000000; record its actual endpoint.
Use archived ENSv2 artifacts at pin a971bd6449154045e2b26ff13d0e56027452f407, without recompiling replacements.
Archive authority: (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/.deployment.json:L4 @ ens_v2@a971bd64).
Retain metadata and seven input artifact hashes. Seven-creation forensic reconstruction is not a replacement requirement.
Use the existing four-family Sepolia generator, preserving versions/statuses and zero resolver/subregistry selection.
One drpc-kind source points to owned Anvil with ethereum_head seed basis and start block zero.
Retain one compiled profile-specific binary and deployment profile across H0, restoration and H1.
Register restore640.eth ordinarily to a nonzero fixture account with unexpired duration and ordinary renewal permission.
Require successful registration, expected on-chain owner/expiry and the supported active projection with matching registrant.
Require canonical registration evidence, healthy ordinary progress and actual provider-trusted quick_synced Verify at F0.
F0 must be an observed finalized marker covering H0 setup. No independent-source verification is claimed.
Use ordinary run, never seeded replay, phase-hash adjustment or SQL-written indexing output.
Require explicit bounded command/readiness/progress/shutdown deadlines and the recorded poll interval.

## Native backup and restore procedure

Cleanly stop and wait for the H0 writer and close its writing connections before baseline queries and dump.
Record original database identity, size, selected H0 records, complete SQLx ledger and native schema output.
Create an unfiltered custom-format pg_dump archive; retain digest, bytes, contents listing, exit and stderr.
Create a distinct destination database from template0 in the owned disposable PostgreSQL cluster.
Declare cluster roles and database-level privileges/extension availability separately from restored application objects.
Use the same existing PostgreSQL image for server/client tools and record actual versions and required options.
Restore the complete archive with pg_restore --single-transaction --exit-on-error and ownership/ACL restoration enabled.
Do not use --clean, --no-owner, --no-acl or object/data filters. Do not initialize or migrate the destination.
Do not repair application grants or indexed state after restoration.
Before any restored writer, require native restore success, matching independent native schema output,
matching complete migration ledger, and matching selected H0 name/registration/phase records.
Use an explicit identical native restrict key for comparison-only schema output; never execute those files.
Any unexplained selected-state/schema/ledger mismatch fails the exercise; no ad hoc normalizer.

## Ordinary H1 and timing

Only after restored-H0 checks pass, submit one genuinely newer ordinary registry renewal.
Require a successful new canonical transaction, greater expiry, unchanged owner and retained H0 registration history.
Start the same sealed release/profile against the restored database with its correctly paired direct Verify reader.
Require ordinary intake/publication of that transaction, expected renewal evidence and updated supported projection.
Require actual resumed frozen-F0 Verify and healthy required phase progress without accepted pending errors or redo.
Record dump duration, restore duration, and restart-to-healthy-H1 duration separately, with database bytes and fixture/head context.
Record host platform, CPU count and PostgreSQL/image versions; timing is descriptive for this fixture and host.
Cleanly stop/wait for the writer; release the existing Anvil owner and verify the owned job has no remaining descendants.
Report harness cleanup honestly: its Drop does not return an independently captured Anvil exit status.
Clean only owned database/container/storage resources, and retain command/process exits and cleanup results.
A timeout or failed native command aborts the exercise; killing docker exec alone does not establish stopped server work.

## Privileges, evidence and limitations

The Verify login is directly authenticated, without elevated attributes, memberships, CREATE or relation/column/sequence writes.
Writer and reader must resolve to the same database name/OID/cluster; required pg_control_system grants are declared prerequisites.
Retain source/overlay, original Pro and replacement decision, artifact/profile/binary identities, commands, logs and query results.
Keep credentials in protected inputs outside the nonsecret seal; no passwords in recorded commands or environment dumps.
Seal closed evidence as sorted path/size/digest entries, with the manifest digest recorded separately.
The backup is whole-database and unfiltered; application validation is SELECTED, not exhaustive equality certification.
This replacement explicitly relinquishes independent every-row, sequence, object, dependency and ACL equivalence.
It has no custom exhaustive census, generic comparator or mutation-validator framework.
Success establishes one native restore exercise with the stated checks and real resumed indexing, not closure of issue #640.
No production RTO, Mainnet bootstrap/catch-up capacity, schedule/retention adoption, fresh migration admission,
HTTP/hydration execution, cross-release rollback or terminal-failure recovery is established.
Document these changed claims at normal subsequent review gates; do not label this as the former proof passing.
Candidate/static review comes first; builds and runtime require a separate explicit serial reservation and fresh resource census.
Callback delivery 01a07938-8a45-78b2-85c3-b4451b1ee8c4 on completion/failure/block/decision/resources with evidence and next action.
At lowest applicable non-Spark Codex usage <=30% remaining, checkpoint/stop local work and await Tate; never switch/reset accounts.
