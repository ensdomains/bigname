-- Phase 10 worker audit: persisted manifest drift and proxy alert observations.
--
-- These observations are operational audit state only. They do not represent
-- manifest truth, discovery admission, watch-plan inputs, projections, or
-- adapter-owned normalized events.

CREATE TABLE manifest_alert_observations (
  manifest_alert_observation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  observation_identity TEXT NOT NULL,
  observation_kind TEXT NOT NULL,
  lifecycle_status TEXT NOT NULL DEFAULT 'active',
  namespace TEXT NOT NULL,
  source_family TEXT NOT NULL,
  manifest_version BIGINT NOT NULL CHECK (manifest_version > 0),
  source_manifest_id BIGINT REFERENCES manifest_versions (manifest_id) ON DELETE SET NULL,
  chain_id TEXT NOT NULL,
  contract_instance_id UUID REFERENCES contract_instances (contract_instance_id),
  proxy_contract_instance_id UUID REFERENCES contract_instances (contract_instance_id),
  expected_implementation_contract_instance_id UUID REFERENCES contract_instances (contract_instance_id),
  observed_implementation_contract_instance_id UUID REFERENCES contract_instances (contract_instance_id),
  discovery_edge_id BIGINT REFERENCES discovery_edges (discovery_edge_id) ON DELETE SET NULL,
  expected_code_hash TEXT,
  observed_code_hash TEXT,
  observed_code_byte_length BIGINT CHECK (observed_code_byte_length IS NULL OR observed_code_byte_length >= 0),
  observed_block_number BIGINT CHECK (observed_block_number IS NULL OR observed_block_number >= 0),
  observed_block_hash TEXT,
  observed_canonicality_state canonicality_state,
  raw_fact_ref JSONB NOT NULL DEFAULT '{}'::JSONB,
  expected_material JSONB NOT NULL DEFAULT '{}'::JSONB,
  observed_material JSONB NOT NULL DEFAULT '{}'::JSONB,
  watch_plan_metadata JSONB NOT NULL DEFAULT '{}'::JSONB,
  alert_metadata JSONB NOT NULL DEFAULT '{}'::JSONB,
  remediation_status TEXT,
  remediation_metadata JSONB,
  first_observed_at TIMESTAMPTZ NOT NULL,
  last_observed_at TIMESTAMPTZ NOT NULL,
  remediated_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (observation_identity),
  CHECK (observation_kind IN ('manifest_drift', 'proxy_implementation_drift')),
  CHECK (lifecycle_status IN ('active', 'acknowledged', 'remediated', 'dismissed')),
  CHECK (jsonb_typeof(raw_fact_ref) = 'object'),
  CHECK (jsonb_typeof(expected_material) = 'object'),
  CHECK (jsonb_typeof(observed_material) = 'object'),
  CHECK (jsonb_typeof(watch_plan_metadata) = 'object'),
  CHECK (jsonb_typeof(alert_metadata) = 'object'),
  CHECK (remediation_metadata IS NULL OR jsonb_typeof(remediation_metadata) = 'object'),
  CHECK ((observed_block_hash IS NULL) = (observed_block_number IS NULL)),
  CHECK (last_observed_at >= first_observed_at),
  CHECK (remediated_at IS NULL OR remediated_at >= first_observed_at),
  CHECK (
    observation_kind <> 'manifest_drift'
    OR (
      contract_instance_id IS NOT NULL
      AND proxy_contract_instance_id IS NULL
      AND expected_code_hash IS NOT NULL
      AND observed_code_hash IS NOT NULL
      AND observed_canonicality_state IS NOT NULL
    )
  ),
  CHECK (
    observation_kind <> 'proxy_implementation_drift'
    OR (
      contract_instance_id IS NOT NULL
      AND proxy_contract_instance_id IS NOT NULL
      AND contract_instance_id = proxy_contract_instance_id
    )
  )
);

CREATE INDEX manifest_alert_observations_lookup_idx
  ON manifest_alert_observations (
    observation_kind,
    lifecycle_status,
    namespace,
    source_family,
    manifest_version,
    manifest_alert_observation_id DESC
  );

CREATE INDEX manifest_alert_observations_manifest_idx
  ON manifest_alert_observations (source_manifest_id, observation_kind, manifest_alert_observation_id DESC)
  WHERE source_manifest_id IS NOT NULL;

CREATE INDEX manifest_alert_observations_contract_idx
  ON manifest_alert_observations (contract_instance_id, observation_kind, manifest_alert_observation_id DESC)
  WHERE contract_instance_id IS NOT NULL;

CREATE INDEX manifest_alert_observations_proxy_idx
  ON manifest_alert_observations (proxy_contract_instance_id, manifest_alert_observation_id DESC)
  WHERE proxy_contract_instance_id IS NOT NULL;
