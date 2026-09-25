-- Existing schema-v2 databases gain the owned key family comments (TYR-36
-- step 2) that cite the adapter lines behind the binding pairing and the
-- pinned registry lines behind the F5 TLD-expiry clear, in place of the plain
-- statements migrations 101400 and 101500 wrote, and state the numeric ranges
-- the F2b wrapper fuses and expiry keep, in place of the 100100 statements.
-- Comments only; no column, index or row changes. An empty schema-migration
-- database has no phase baseline yet, so this migration is a no-op there and
-- phase-runner init-schema installs the same comments.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_current') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.event_identity IS
    'This value is the event identity of the binding''s position: the position of the SurfaceBound that opened it (the block''s SurfaceBound of the same name and resource at the transaction and log index of the binding''s provenance), else the binding''s own block and provenance index with the identity binding:<surface_binding_id>. It is the final tiebreak of the canonical event order, compared as bytes; two bindings one event opened are ordered by surface_binding_id. The adapter materializes one raw log''s events and bindings together (adapters schema_v2/session.rs:490 and :512) and stamps each log-sourced binding with that log''s provenance (schema_v2/identity.rs:229 and :329); a block-boundary binding and its SurfaceBound come from one block with no transaction or log (identity/boundary.rs:137). The families assume, as an adapter precondition, that an identity binding:<surface_binding_id> means the adapter''s reconcile dropped the SurfaceBound (schema_v2/protocol/v1/reconcile_support.rs:42-43), not that the SurfaceBound sits at another position; the cited lines show that a binding and its SurfaceBound share provenance, not that every binding has an opener. If the precondition fails, the family positions the binding at its own block and provenance index under the identity binding:<surface_binding_id>, with no error and no anomaly count.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resource_pointer.resolver_address IS
    'This value is the lower-cased resolver of the latest ResolverChanged on the resource, named or not, clears included. At an ENSv2 root-registry TLD expiry the interpreter emits the resolver clear with no logical name (adapters schema_v2/protocol/v2_registry/expiry.rs); this row keeps that clear, where the served pointer read takes named ResolverChanged only (builders/linked_records.rs, project_record_pointer_latest) and never sees it, so the served inventory keeps a row the name no longer reaches. The pinned registry returns the zero address from getResolver once the token has expired (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258, L628-L630 @ ens_v2@a971bd64), which this row matches.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_wrapper_state.fuses IS
    'This value is the fuses of the latest PermissionScopeChanged when a JSON number whose value is an integer from 0 to 9223372036854775807, the range the served modifiers read before casting to bigint (builders/permissions.rs modifiers, address_names.rs scope_modifiers); null otherwise. A non-integral spelling such as 1.0 fails that served cast and the Project batch, so it never reaches a served row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_wrapper_state.expiry_seconds IS
    'This value is the latest wrapper expiry when a JSON number whose value is from 0 to 18446744073709551615, compared by value as the served numeric read does (address_names.rs wrapper_expiries, children.rs latest_wrapper_expiries), so 1.0 and 1.5 count as those numbers; null otherwise.'
$ddl$;
END
$migration$;
