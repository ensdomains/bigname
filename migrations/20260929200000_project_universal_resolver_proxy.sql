-- Add the Project-owned Universal Resolver proxy family (project_universal_resolver_proxy) to
-- existing schema-v2 databases. It holds, per declared ens_execution Universal Resolver proxy,
-- the implementation its latest Upgraded installed, from which the composed name reader decides
-- whether a block resolves through ENSv2 (docs/manifests.md, `universal_resolver_implementations`).
-- Table only: no existing row changes. The change that adds the family also rotates the
-- interpreter content hash, so the families are rebuilt under the new build before they serve;
-- until then the empty table reads as "not cut over", which is what the families served before.
-- An empty schema-migration database has no phase baseline yet, so this schema-migration is a
-- no-op there and phase-runner init-schema installs the same table. The guard names the family
-- marker, the table every family publication writes, not name_current, which
-- 20260929160000_remove_served_projections.sql drops.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.project_family_marker') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_universal_resolver_proxy (
    chain_id text NOT NULL,
    proxy_address text NOT NULL,
    proxy_role text,
    implementation text NOT NULL,
    implementation_kind text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    PRIMARY KEY (chain_id, proxy_address),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL)),
    CHECK (implementation_kind IN ('admitted_universal_resolver', 'universal_resolver_proxy', 'other'))
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_universal_resolver_proxy IS
    'Project-owned Universal Resolver proxy state: per declared ens_execution proxy, the implementation its latest Upgraded event installed, in canonical event order. A block resolves through ENSv2 (the Universal Resolver cutover) while the chain of implementations from the client-facing universal_resolver proxy, through declared proxies, ends at an admitted UniversalResolverV2 implementation; a proxy with no row has no known implementation, since its constructor sets the first one without an event. The composed name reader reads every row of the chain at the family publication.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_universal_resolver_proxy.chain_id IS
    'This value is the chain whose events wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_universal_resolver_proxy.proxy_address IS
    'This value is the lower-cased address of the proxy that emitted Upgraded.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_universal_resolver_proxy.proxy_role IS
    'This value is the manifest role of that proxy: universal_resolver for the client-facing proxy, universal_resolver_managed for the intermediate one.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_universal_resolver_proxy.implementation IS
    'This value is the lower-cased implementation the latest Upgraded installed.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_universal_resolver_proxy.implementation_kind IS
    'This value is how the manifest classifies that implementation: admitted_universal_resolver (listed in universal_resolver_implementations), universal_resolver_proxy (another declared Universal Resolver proxy), or other.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_universal_resolver_proxy.block_number IS
    'This value is the block number of the latest Upgraded of the proxy.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_universal_resolver_proxy.transaction_index IS
    'This value is the transaction index of that event; null with log_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_universal_resolver_proxy.log_index IS
    'This value is the log index of that event; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_universal_resolver_proxy.event_identity IS
    'This value is that event identity, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_universal_resolver_proxy.normalized_event_id IS
    'This value names that event in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
END
$migration$;
