-- Phase 7 projection foundation: persist the exact-tuple declared claimed-name source
-- separately from claim provenance so later API readback can publish claimed_primary_name.name
-- without widening the claim-local provenance surface.

ALTER TABLE primary_names_current
  ADD COLUMN normalized_claim_name TEXT,
  ADD CONSTRAINT primary_names_current_normalized_claim_name_check
    CHECK (
      normalized_claim_name IS NULL
      OR BTRIM(normalized_claim_name) <> ''
    );
