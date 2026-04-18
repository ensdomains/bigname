-- Phase 7 projection foundation: extend primary_names_current from tuple presence
-- into declared claim-state storage without introducing verified truth.

ALTER TABLE primary_names_current
  ADD COLUMN claim_status TEXT NOT NULL DEFAULT 'unsupported',
  ADD COLUMN raw_claim_name TEXT,
  ADD COLUMN claim_provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD CONSTRAINT primary_names_current_claim_status_check
    CHECK (claim_status IN ('success', 'not_found', 'unsupported', 'invalid_name')),
  ADD CONSTRAINT primary_names_current_raw_claim_name_check
    CHECK (
      (claim_status = 'invalid_name' AND raw_claim_name IS NOT NULL AND BTRIM(raw_claim_name) <> '')
      OR
      (claim_status <> 'invalid_name' AND raw_claim_name IS NULL)
    ),
  ADD CONSTRAINT primary_names_current_claim_provenance_object_check
    CHECK (jsonb_typeof(claim_provenance) = 'object');
