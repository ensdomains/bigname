-- Existing schema-v2 databases gain the owned key family comments (TYR-36
-- step 2) that state where a family differs from the served build, so the
-- steps that read the families can key on them. Comments only; no column,
-- index or row changes. An empty schema-migration database has no phase
-- baseline yet, so this migration is a no-op there and phase-runner
-- init-schema installs the same comments.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_current') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_family_undo IS
    'Project-owned undo record of the owned key families: per applied block, the image each family row had before the block first changed it, plus the prior marker under family marker. Undoing a block restores these images. Rows are kept back to the lowest of 256 blocks below the marker, the finalized block, the safe block and an active repair''s floor; with no finalized or safe head nothing is pruned, so the journal grows by every block until the heads appear and is then pruned in one delete. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.registry_only IS
    'This value is true once an AuthorityEpochChanged registry_only was seen on this name and resource, in the binding''s block or later. An epoch at an earlier block than the binding does not set it, where the served REGISTRY_ONLY_HANDOFFS (name_authority/stage.rs:127-134) takes an epoch on the name and resource at any position.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.surface_binding_id IS
    'This value identifies the surface binding. It orders candidates only after the whole position: two bindings of one name at the same position with no transaction or log (synthesised) order by event_identity and then this id, where the served selection orders equal (block, transaction, log) by surface_binding_id descending without the identity.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_node_state.owner_event_kind IS
    'This value is the kind of the registry event that last set the owner group: AuthorityTransferred or SubregistryChanged, both of which report the owner (name_authority/stage.rs:200-261). Either overwrites the group, so a SubregistryChanged after an AuthorityTransferred whose getter was zero replaces the owner; the served ownerless verdict, which reads AuthorityTransferred only, cannot be recovered from this row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_classification.unsupported_reason IS
    'This value is the reason when unsupported: resolver_not_declared, resolver_implementation_unknown, resolver_implementation_not_declared, or resolver_manifest_not_active for a resolver with candidates but no active manifest of its family, which the served build leaves out (one such row per resolver).'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_classification.pointer_families IS
    'This value maps each resolver family to the number of F4 and F5 pointer rows pointing at the resolver now, standing for the priority 2 name pointers. It approximates the served candidates: an unnamed ENSv2 pointer row counts here though the served build has no candidate for it, so a resolver with an ENSv1 event proposal and such a pointer can classify under ens_v2_resolver_l1 here and ens_v1_resolver_l1 served.'
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_registry_pointer IS
    'Project-owned ENSv1 registry-node resolver pointer of family F4: the latest ResolverChanged per node, clears included, from the ENSv1 registry, registrar and wrapper families only (record_inventory/mirror.rs:100). A ResolverChanged of another family with no resource, such as a Basenames reverse node pointer, lands in neither F4 nor F5, where the served reverse-claim resolver (builders/primary_names.rs:103-113) reads the latest ResolverChanged at the node from any family. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_resolver_link IS
    'Project-owned resolver links of family F7: per resolver and node, the latest ResolverRecordLinked; record id 0 is an explicit clear. A link whose payload carries no resolver is kept, where the served links.sql requires the payload resolver to be present and equal to the emitter. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_family_marker.admission_manifests IS
    'This value is the key of the active manifest set the last block classified under: manifest_id:event_id of the latest SourceManifestUpdated event of every manifest the chain reads, at or below the block or with no block. A family run reads the manifest updates once, so an update written during a run applies from the next run; a block that sees another key classifies every stored resolver again. An update with no block applies to every block, so it is not tied to the block it was written at.'
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_resolver_classification IS
    'Project-owned resolver classification of family F3, pinned to the block that last classified it: resolver_current without its sampled sections, from the candidate accumulators the row keeps and the discovery edges, declarations and manifests active at that block. A resolver is classified again when an event names it, a pointer moves to or from it, a resolver edge, its address or a declaration of it starts or stops, and when the active manifest set changes. Edge and address activity also honours deactivated_at, a wall-clock time as in the served build, so a classification can differ from a later rebuild once an edge is deactivated. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_classification.admission_manifests IS
    'This value is the key of the active manifest set the classification was made under (project_family_marker.admission_manifests).'
$ddl$;
END
$migration$;
