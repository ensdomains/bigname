CREATE TABLE IF NOT EXISTS label_preimages (
    labelhash text PRIMARY KEY,
    raw_label bytea NOT NULL,
    decoded_label text,
    normalizer_version text NOT NULL,
    normalized_under_version boolean NOT NULL,
    normalization_error text,
    source_kind text NOT NULL,
    source_priority integer NOT NULL,
    provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
    observed_at timestamptz NOT NULL DEFAULT now(),
    inserted_at timestamptz NOT NULL DEFAULT now(),
    CHECK (btrim(labelhash) <> ''),
    CHECK (octet_length(raw_label) > 0),
    CONSTRAINT label_preimages_decoded_label_matches_raw_check CHECK (
        decoded_label IS NULL
        OR convert_to(decoded_label, 'UTF8') = raw_label
    ),
    CHECK (btrim(normalizer_version) <> ''),
    CONSTRAINT label_preimages_normalization_coherence_check CHECK (
        (normalized_under_version AND normalization_error IS NULL)
        OR (
            NOT normalized_under_version
            AND normalization_error IS NOT NULL
            AND btrim(normalization_error) <> ''
        )
    ),
    CHECK (btrim(source_kind) <> ''),
    CHECK (source_priority >= 0),
    CHECK (jsonb_typeof(provenance) = 'object')
);

CREATE INDEX IF NOT EXISTS label_preimages_normalization_idx
    ON label_preimages (
        normalizer_version,
        normalized_under_version,
        labelhash
    );

CREATE TABLE IF NOT EXISTS ens_names (
    hash text PRIMARY KEY,
    name text NOT NULL,
    CHECK (btrim(hash) <> ''),
    CHECK (name <> '')
);

COMMENT ON TABLE label_preimages IS
    'This table maps label hashes to verified raw labels. Consumers use the bounded labelhash primary key; unbounded raw label bytes are deliberately absent from btree indexes.';
COMMENT ON COLUMN label_preimages.labelhash IS
    'This value is the label hash.';
COMMENT ON COLUMN label_preimages.raw_label IS
    'This value is the verbatim chain label bytes. Lookup uses the labelhash primary key because these attacker-controlled bytes are unbounded.';
COMMENT ON COLUMN label_preimages.decoded_label IS
    'This value is the PostgreSQL-representable UTF-8 decoding of the raw bytes when one exists.';
COMMENT ON COLUMN label_preimages.normalizer_version IS
    'This value identifies the tested normalization rules.';
COMMENT ON COLUMN label_preimages.normalized_under_version IS
    'This flag states whether the raw label passes normalization.';
COMMENT ON COLUMN label_preimages.normalization_error IS
    'This value explains a failed normalization test.';
COMMENT ON COLUMN label_preimages.source_kind IS
    'This value identifies the label source kind.';
COMMENT ON COLUMN label_preimages.source_priority IS
    'This value ranks competing label sources.';
COMMENT ON COLUMN label_preimages.provenance IS
    'This object identifies the label source.';
COMMENT ON COLUMN label_preimages.observed_at IS
    'This time records the stored observation.';
COMMENT ON COLUMN label_preimages.inserted_at IS
    'This time records row creation.';

COMMENT ON TABLE ens_names IS
    'This table stores the imported ENS rainbow data. Import traversal uses the bounded hash primary key; unbounded names are deliberately absent from btree indexes.';
COMMENT ON COLUMN ens_names.hash IS
    'This value is the imported label hash.';
COMMENT ON COLUMN ens_names.name IS
    'This value is the imported label. Lookup and import traversal use the hash primary key because label text is unbounded.';
