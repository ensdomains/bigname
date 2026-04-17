-- Phase 2 execution trace storage foundation: durable skeleton persistence for verified traces.

CREATE TABLE execution_traces (
  execution_trace_id UUID PRIMARY KEY,
  request_type TEXT NOT NULL,
  request_key TEXT NOT NULL,
  namespace TEXT NOT NULL,
  chain_context JSONB NOT NULL DEFAULT '{}'::JSONB,
  manifest_context JSONB NOT NULL DEFAULT '{}'::JSONB,
  contracts_called JSONB NOT NULL DEFAULT '[]'::JSONB,
  gateway_digests JSONB NOT NULL DEFAULT '[]'::JSONB,
  final_payload JSONB,
  failure_payload JSONB,
  request_metadata JSONB NOT NULL DEFAULT '{}'::JSONB,
  finished_at TIMESTAMPTZ NOT NULL,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (jsonb_typeof(chain_context) = 'object' AND chain_context <> '{}'::JSONB),
  CHECK (jsonb_typeof(manifest_context) = 'object' AND manifest_context <> '{}'::JSONB),
  CHECK (jsonb_typeof(contracts_called) = 'array'),
  CHECK (jsonb_typeof(gateway_digests) = 'array'),
  CHECK (jsonb_typeof(request_metadata) = 'object'),
  CHECK (final_payload IS NOT NULL OR failure_payload IS NOT NULL)
);

CREATE TABLE execution_steps (
  execution_trace_id UUID NOT NULL REFERENCES execution_traces (execution_trace_id) ON DELETE CASCADE,
  step_index BIGINT NOT NULL CHECK (step_index >= 0),
  step_kind TEXT NOT NULL,
  input_digest TEXT,
  output_digest TEXT,
  latency_ms BIGINT CHECK (latency_ms IS NULL OR latency_ms >= 0),
  canonicality_dependency JSONB NOT NULL DEFAULT '{}'::JSONB,
  step_payload JSONB NOT NULL DEFAULT '{}'::JSONB,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (execution_trace_id, step_index),
  CHECK (
    jsonb_typeof(canonicality_dependency) = 'object'
    AND canonicality_dependency <> '{}'::JSONB
  ),
  CHECK (jsonb_typeof(step_payload) = 'object')
);
