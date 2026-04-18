-- Phase 7 projection foundation: primary-name bootstrap tuple presence keyed by address,
-- coin_type, and namespace.

CREATE TABLE primary_names_current (
  address TEXT NOT NULL,
  coin_type TEXT NOT NULL,
  namespace TEXT NOT NULL,
  PRIMARY KEY (address, coin_type, namespace),
  CHECK (address <> ''),
  CHECK (coin_type <> ''),
  CHECK (namespace <> '')
);
