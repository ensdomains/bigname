-- Versioning for the watched surface: any transaction that mutates
-- discovery_edges (insert, reactivation, window update, or deactivation) OR
-- the manifest-declared watched surface (manifest entries, seeded addresses,
-- declared start blocks, rollout status) must bump the owning chain's epoch
-- row in the same transaction. Promotion's in-process verified coverage
-- frontier stores the epoch it verified under and re-verifies when the epoch
-- moves, so watch-set growth behind the frontier is never trusted stale —
-- including mutations from other processes (backfill CLI, manifest sync).
CREATE TABLE public.discovery_admission_epochs (
    chain_id text PRIMARY KEY,
    epoch bigint NOT NULL
);
