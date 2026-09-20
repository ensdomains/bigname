#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [ "${1:-}" != "--database-ready" ]; then
    if [ -n "${BIGNAME_TEST_DATABASE_URL:-}" ]; then
        export SCHEMA_V2_EXTERNAL_DATABASE=1
    fi
    exec "$ROOT/scripts/test-db" -- "$ROOT/schema-v2/apply-check.sh" --database-ready
fi

container="${BIGNAME_TEST_POSTGRES_CONTAINER:-bigname-test-postgres}"
image="${BIGNAME_TEST_POSTGRES_IMAGE:-postgres:16-alpine}"
database="${BIGNAME_TEST_POSTGRES_DB:-bigname}"
user="${BIGNAME_TEST_POSTGRES_USER:-bigname}"

psql_mode=""
if [ "${SCHEMA_V2_EXTERNAL_DATABASE:-0}" = "1" ] \
    && command -v psql >/dev/null 2>&1; then
    psql_mode="host"
elif [ "${SCHEMA_V2_EXTERNAL_DATABASE:-0}" = "1" ] \
    && command -v docker >/dev/null 2>&1 \
    && [ -n "${BIGNAME_DATABASE_URL:-}" ]; then
    psql_mode="client-container"
elif command -v docker >/dev/null 2>&1 \
    && docker inspect "$container" >/dev/null 2>&1 \
    && [ "$(docker inspect --format '{{.State.Running}}' "$container")" = "true" ]; then
    psql_mode="database-container"
elif command -v psql >/dev/null 2>&1; then
    psql_mode="host"
elif command -v docker >/dev/null 2>&1 && [ -n "${BIGNAME_DATABASE_URL:-}" ]; then
    psql_mode="client-container"
else
    printf '%s\n' "psql or Docker is required for the schema check" >&2
    exit 1
fi

# Every batch after set-up runs on a connection authenticated as a per-run
# login role that owns the scratch schema and holds no privilege on
# bigname_phase, so whatever a schema-migration reaches for outside the rewritten
# text -- an identifier assembled inside EXECUTE, a search-path-relative name
# in a DO body -- fails on the production schema instead of changing it
# unobserved. A separate login, not SET ROLE on the owner's session: RESET
# ROLE would hand a schema-migration the owner back. Only role set-up and teardown
# use the owner's connection.
run_psql_as_owner() {
    case "$psql_mode" in
        database-container)
            docker exec -i "$container" \
                psql -X -q -v ON_ERROR_STOP=1 -U "$user" -d "$database"
            ;;
        host)
            psql -X -q -v ON_ERROR_STOP=1 "${BIGNAME_DATABASE_URL:?}"
            ;;
        client-container)
            docker run --rm --network host -i "$image" \
                psql -X -q -v ON_ERROR_STOP=1 "$BIGNAME_DATABASE_URL"
            ;;
    esac
}
run_psql() {
    case "$psql_mode" in
        database-container)
            docker exec -i "$container" \
                psql -X -q -v ON_ERROR_STOP=1 -U "$apply_check_role" -d "$database"
            ;;
        host)
            psql -X -q -v ON_ERROR_STOP=1 "$apply_check_url"
            ;;
        client-container)
            docker run --rm --network host -i "$image" \
                psql -X -q -v ON_ERROR_STOP=1 "$apply_check_url"
            ;;
    esac
}

render_phase_migration() {
    local migration_file="$1"
    sed "s/bigname_phase/$scratch_schema/g" "$migration_file"
}
# Strip `--` comments the way PostgreSQL reads them: not inside a single-quoted
# string ('' escapes), a double-quoted identifier, or a $$ body, across lines.
# `quote` carries the open quoting from one line to the next; a file that ends
# inside a quote is unparsable and the caller treats it as such.
sql_comment_stripper='
    function strip_sql_comments(line,    out, i, c, n) {
        out = ""; n = length(line); i = 1
        while (i <= n) {
            c = substr(line, i, 1)
            if (quote == "") {
                if (substr(line, i, 2) == "--") { break }
                if (substr(line, i, 2) == "$$") { quote = "$$"; out = out "$$"; i += 2; continue }
                if (c == "\047" || c == "\"") { quote = c }
            } else if (quote == "$$") {
                if (substr(line, i, 2) == "$$") { quote = ""; out = out "$$"; i += 2; continue }
            } else if (c == quote) {
                if (quote == "\047" && substr(line, i + 1, 1) == "\047") { out = out "\047\047"; i += 2; continue }
                quote = ""
            }
            out = out c; i++
        }
        return out
    }
'
# Inventory membership requires the whole quoted or bare literal bigname_phase token
# after stripping -- line comments. Search-path-relative phase SQL would be silently
# excluded. Today only the two public-schema service-loop files are outside the inventory;
# block comments are not stripped, so a token-only mention is included and fails loud.
phase_migration_uses_production_schema() {
    awk "$sql_comment_stripper"'
        { line = strip_sql_comments($0) }
        line ~ /(^|[^[:alnum:]_])"?bigname_phase"?([^[:alnum:]_]|$)/ { found = 1 }
        END { exit !found }
    ' "$1"
}
# A schema-migration outside the inventory is never applied here, so a phase schema-migration
# written against the connection's search path would change bigname_phase
# unlisted and untested. Since the legacy public schema was dropped, every such
# file must consist of statements the check can read, each naming the object it
# creates, alters, drops, or writes with its schema (for CREATE INDEX, the
# table), quoted or not; the check prints what carries no schema qualifier and
# any statement it cannot read.
legacy_public_schema_drop="20260806120000_drop_legacy_public_schema.sql"
# migration-inventory.txt lists every schema-migration file, in order, up to
# the documented head, each with the SHA-384 of its bytes -- the checksum sqlx
# records when it applies the file and rejects on a later mismatch. sqlx
# applies any version a database has not recorded, whatever its position, so
# a file named to sort anywhere below the head would run on an initialized
# database while looking historical or already frozen, and an edit to an
# applied file breaks every initialized database; the directory must
# therefore equal the inventory exactly, bytes included, and a new
# schema-migration lands by joining the inventory and advancing the head in
# the same change.
migration_inventory="$ROOT/schema-v2/migration-inventory.txt"
# A post-cutoff schema-migration that names no bigname_phase object is never
# applied by this check, so the rule for one is closed rather than parsed: it
# may consist only of `DROP INDEX|SEQUENCE|VIEW|MATERIALIZED VIEW|FUNCTION|
# PROCEDURE` statements (with CONCURRENTLY, IF EXISTS, RESTRICT) whose every
# target is `schema.name`, written with plain identifiers and no strings,
# quoted identifiers, dollar quoting, block comments, or other lexical forms.
# CONCURRENTLY is accepted only in a file whose first bytes are sqlx's
# `-- no-transaction` marker and only over one index, since PostgreSQL
# refuses it inside a transaction block and with more than one target.
# CASCADE is refused: PostgreSQL would drop whatever depends on the target, so
# a bigname_phase view, trigger, or foreign key hanging off a public object
# would go with it unlisted, while RESTRICT (the default) makes such a
# dependency fail the real schema-migration loudly. DROP TABLE is refused
# outright: a table in another schema can be an inheritance child or a
# partition of a bigname_phase table, and RESTRICT does not guard that link --
# the drop succeeds and takes the rows visible through the phase parent.
# Anything else -- any DDL that creates or alters, any DML, any expression,
# any routine call -- must name `bigname_phase` and thereby join the
# inventory, where it is applied and observed. A search-path-relative name
# cannot be written under this rule, whatever statement shape carries it.
migration_is_closed_form_drop() {
    awk "$sql_comment_stripper"'
        # sqlx runs a file outside a transaction only when its bytes start
        # with this marker; DROP INDEX CONCURRENTLY fails inside one.
        NR == 1 && index($0, "-- no-transaction") == 1 { no_transaction = 1 }
        { text = text " " strip_sql_comments($0) }
        END {
            if (quote != "") { print " [unterminated quote at end of file]"; exit 1 }
            if (text ~ /["\047$]/ || text ~ /\/\*/) { print " [quoted identifier, string, dollar quoting, or block comment]"; exit 1 }
            gsub(/[[:space:]]+/, " ", text)
            n = split(text, statements, ";")
            for (i = 1; i <= n; i++) {
                s = statements[i]; sub(/^ +/, "", s); sub(/ +$/, "", s)
                if (s == "") continue
                u = toupper(s)
                if (match(u, /^DROP TABLE /)) {
                    bad = bad " [DROP TABLE may detach a child or partition of a bigname_phase table: " substr(s, 1, 40) "]"
                    continue
                }
                if (!match(u, /^DROP (INDEX( CONCURRENTLY)?|SEQUENCE|VIEW|MATERIALIZED VIEW|FUNCTION|PROCEDURE)( IF EXISTS)? /)) {
                    bad = bad " [not a closed-form drop: " substr(s, 1, 40) "]"
                    continue
                }
                routine = (u ~ /^DROP (FUNCTION|PROCEDURE) /)
                rest = substr(u, RSTART + RLENGTH)
                if (rest ~ / CASCADE$/) {
                    bad = bad " [CASCADE may drop a dependent bigname_phase object: " substr(s, 1, 40) "]"
                    continue
                }
                sub(/ RESTRICT$/, "", rest)
                # A routine target carries its argument signature; commas inside
                # its parentheses separate arguments, not targets.
                t = 0; depth = 0; target = ""
                for (k = 1; k <= length(rest); k++) {
                    c = substr(rest, k, 1)
                    if (c == "(") depth++
                    else if (c == ")") depth--
                    if (depth < 0) break
                    if (c == "," && depth == 0) { targets[++t] = target; target = "" } else target = target c
                }
                targets[++t] = target
                if (depth != 0) { bad = bad " [unbalanced parentheses: " substr(s, 1, 40) "]"; continue }
                # PostgreSQL runs DROP INDEX CONCURRENTLY only as a top-level
                # statement and only over one index; either form would pass
                # here and fail the real run.
                if (u ~ /^DROP INDEX CONCURRENTLY /) {
                    if (!no_transaction) { bad = bad " [CONCURRENTLY in a file sqlx runs in a transaction: " substr(s, 1, 40) "]"; continue }
                    if (t > 1) { bad = bad " [CONCURRENTLY takes one index: " substr(s, 1, 40) "]"; continue }
                }
                # Each target is `schema.name`, and for a routine optionally a
                # signature `(type, type(10,2), ...)` of plain type names; any
                # other token before, inside, or after the signature is refused
                # rather than dropped, since sqlx would still run the file.
                for (j = 1; j <= t; j++) {
                    target = targets[j]; gsub(/^ +| +$/, "", target)
                    signature = ""
                    if (routine && match(target, /\(/)) {
                        signature = substr(target, RSTART + 1)
                        target = substr(target, 1, RSTART - 1); sub(/ +$/, "", target)
                        # The signature runs to the closing parenthesis of the one
                        # it opened: a typmod may nest, a second list may not.
                        depth = 1; ok = (signature ~ /\)$/)
                        signature = substr(signature, 1, length(signature) - 1)
                        for (k = 1; k <= length(signature) && ok; k++) {
                            c = substr(signature, k, 1)
                            if (c == "(") depth++
                            else if (c == ")") depth--
                            if (depth < 1) ok = 0
                        }
                        # Each argument is a type name: words, optionally
                        # schema-qualified, with a numeric typmod and array
                        # brackets; an empty or other argument is refused.
                        a = 0; arg = ""; sdepth = 0
                        for (k = 1; k <= length(signature) && ok; k++) {
                            c = substr(signature, k, 1)
                            if (c == "(") sdepth++
                            else if (c == ")") sdepth--
                            if (c == "," && sdepth == 0) { args[++a] = arg; arg = "" } else arg = arg c
                        }
                        if (a > 0 || arg != "") args[++a] = arg
                        for (k = 1; k <= a && ok; k++) {
                            arg = args[k]; gsub(/^ +| +$/, "", arg)
                            if (arg !~ /^[A-Z_][A-Z0-9_]*(\.[A-Z_][A-Z0-9_]*)?( [A-Z_][A-Z0-9_]*(\.[A-Z_][A-Z0-9_]*)?)*(\([0-9]+(, ?[0-9]+)*\))?(\[\])*$/) ok = 0
                        }
                        if (!ok) bad = bad " [signature: " signature "]"
                    }
                    if (target !~ /^[A-Z_][A-Z0-9_]*\.[A-Z_][A-Z0-9_]*$/) {
                        bad = bad " " (target == "" ? "[empty target]" : target)
                    }
                }
            }
            if (bad != "") { print bad; exit 1 }
            exit 0
        }
    ' "$1"
}
migration_uses_unicode_escape() {
    grep -qiE "U&[\"']" "$1"
}
# The frozen artifact is the baseline plus the inventoried schema-migrations
# through the documented head. schema-v2/frozen-schema.txt is that artifact's
# catalog -- the baseline's extension declarations, then every relation with
# its storage options, row-security flags, replica identity and partitioning,
# every column with its storage, statistics target, privileges and the
# schema, provider and locale of its collation, constraint, index, view,
# routine with its full argument list and both body forms, trigger with its firing state,
# sequence with its full range, type and domain with their privileges,
# comment, policy, rule, extended
# statistics object, the schema's own privileges, every collation the schema
# holds and every cast to or from one of its types, with the schema name
# normalized -- built
# here into its own schema and compared line for line, so a change to a
# baseline file or a schema-migration that moves the schema without moving the
# frozen catalog fails, whatever its name. Regenerate deliberately with
# SCHEMA_V2_APPLY_CHECK_WRITE_FINGERPRINT=1 in the change that moves the schema.
frozen_schema_catalog_sql="$(cat <<'CATALOG_SQL'
SELECT line FROM (
    SELECT 0 AS section, c.relname AS a, '' AS b,
           format('relation %s kind=%s persistence=%s acl=%s options=%s rls=%s force_rls=%s replica_identity=%s partition_key=%s partition_bound=%s inherits=%s',
                  c.relname, c.relkind, c.relpersistence,
                  COALESCE(replace(array_to_string(c.relacl, ','), current_user, 'owner'), 'default'),
                  COALESCE((SELECT string_agg(o, ',' ORDER BY o) FROM unnest(c.reloptions) o), '-'),
                  c.relrowsecurity, c.relforcerowsecurity, c.relreplident,
                  COALESCE(pg_get_partkeydef(c.oid), '-'),
                  COALESCE(pg_get_expr(c.relpartbound, c.oid), '-'),
                  COALESCE((SELECT string_agg(p.relname, ',' ORDER BY i.inhseqno)
                            FROM pg_inherits i JOIN pg_class p ON p.oid = i.inhparent
                            WHERE i.inhrelid = c.oid), '-')) AS line
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = current_schema() AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')
    UNION ALL
    SELECT 1, c.relname, a.attname,
           format('column %s.%s %s %s default=%s identity=%s generated=%s collation=%s storage=%s compression=%s statistics=%s acl=%s options=%s existing_rows=%s',
                  c.relname, a.attname,
                  format_type(a.atttypid, a.atttypmod),
                  CASE WHEN a.attnotnull THEN 'not null' ELSE 'null' END,
                  COALESCE(pg_get_expr(d.adbin, d.adrelid), '-'),
                  COALESCE(NULLIF(a.attidentity, ''), '-'),
                  COALESCE(NULLIF(a.attgenerated, ''), '-'),
                  -- The referenced collation by schema and definition, so a
                  -- schema-local collation shadowing a pg_catalog name, or a
                  -- provider or locale change behind the same name, differs.
                  -- The database default collation is the one definition that
                  -- is the deployment's (storage.md), so it is named alone.
                  COALESCE((SELECT CASE WHEN col.oid = 'default'::regcollation THEN 'pg_catalog.default'
                                        ELSE format('%I.%I provider=%s collate=%s ctype=%s locale=%s deterministic=%s',
                                          cn.nspname, col.collname, col.collprovider,
                                          COALESCE(NULLIF(col.collcollate, ''), '-'),
                                          COALESCE(NULLIF(col.collctype, ''), '-'),
                                          COALESCE(to_jsonb(col) ->> 'colllocale', to_jsonb(col) ->> 'colliculocale', '-'),
                                          col.collisdeterministic) END
                            FROM pg_collation col JOIN pg_namespace cn ON cn.oid = col.collnamespace
                            WHERE col.oid = a.attcollation AND a.attcollation <> 0), '-'),
                  a.attstorage, COALESCE(NULLIF(a.attcompression, ''), '-'),
                  CASE WHEN a.attstattarget IS NULL OR a.attstattarget < 0 THEN 'default'
                       ELSE a.attstattarget::text END,
                  COALESCE(replace(array_to_string(a.attacl, ','), current_user, 'owner'), 'default'),
                  COALESCE((SELECT string_agg(o, ',' ORDER BY o) FROM unnest(a.attoptions) o), '-'),
                  -- A column added with a default keeps that value for the rows that
                  -- predate it. Printed only when it is not the current default: then
                  -- old and new rows read different values, which a fresh database
                  -- never does.
                  CASE WHEN a.atthasmissing
                        AND a.attmissingval::text IS DISTINCT FROM
                            pg_temp.frozen_catalog_default(pg_get_expr(d.adbin, d.adrelid),
                                                           format_type(a.atttypid, a.atttypmod) LIKE '%[]')
                       THEN a.attmissingval::text ELSE 'default' END)
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
    LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
    WHERE n.nspname = current_schema() AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
    UNION ALL
    SELECT 2, c.relname, con.conname,
           format('constraint %s.%s %s', c.relname, con.conname, pg_get_constraintdef(con.oid))
    FROM pg_constraint con
    JOIN pg_class c ON c.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = current_schema()
    UNION ALL
    SELECT 3, c.relname, i.relname,
           format('index %s.%s %s valid=%s', c.relname, i.relname, pg_get_indexdef(x.indexrelid), x.indisvalid)
    FROM pg_index x
    JOIN pg_class i ON i.oid = x.indexrelid
    JOIN pg_class c ON c.oid = x.indrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = current_schema()
    UNION ALL
    SELECT 4, c.relname, '', format('view %s %s', c.relname, pg_get_viewdef(c.oid, true))
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = current_schema() AND c.relkind IN ('v', 'm')
    UNION ALL
    SELECT 5, p.proname, pg_get_function_identity_arguments(p.oid),
           format('routine %s(%s) returns %s kind=%s lang=%s volatile=%s strict=%s leakproof=%s parallel=%s secdef=%s cost=%s rows=%s config=%s acl=%s body=%s sqlbody=%s',
                  p.proname, pg_get_function_arguments(p.oid),
                  pg_get_function_result(p.oid), p.prokind,
                  (SELECT l.lanname FROM pg_language l WHERE l.oid = p.prolang),
                  p.provolatile, p.proisstrict, p.proleakproof, p.proparallel, p.prosecdef,
                  p.procost, p.prorows,
                  COALESCE(array_to_string(p.proconfig, ';'), '-'),
                  COALESCE(replace(array_to_string(p.proacl, ','), current_user, 'owner'), 'default'),
                  md5(replace(p.prosrc, current_schema(), 'bigname_phase')),
                  -- A SQL-standard body (BEGIN ATOMIC) is stored parsed, with
                  -- prosrc empty; only its printed form tells two apart.
                  md5(replace(COALESCE(pg_get_function_sqlbody(p.oid), ''), current_schema(), 'bigname_phase')))
    FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = current_schema()
    UNION ALL
    SELECT 6, c.relname, t.tgname,
           format('trigger %s.%s %s enabled=%s', c.relname, t.tgname, pg_get_triggerdef(t.oid), t.tgenabled)
    FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = current_schema() AND NOT t.tgisinternal
    UNION ALL
    SELECT 7, s.sequencename, '',
           format('sequence %s %s start=%s increment=%s min=%s max=%s cache=%s cycle=%s owned_by=%s',
                  s.sequencename, s.data_type, s.start_value, s.increment_by,
                  s.min_value, s.max_value, s.cache_size, s.cycle,
                  COALESCE((SELECT format('%s.%s', c.relname, a.attname)
                            FROM pg_depend dep
                            JOIN pg_class c ON c.oid = dep.refobjid
                            JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = dep.refobjsubid
                            WHERE dep.classid = 'pg_class'::regclass
                              AND dep.objid = format('%I.%I', s.schemaname, s.sequencename)::regclass
                              AND dep.refclassid = 'pg_class'::regclass
                              AND dep.deptype IN ('a', 'i')), '-'))
    FROM pg_sequences s WHERE s.schemaname = current_schema()
    UNION ALL
    SELECT 8, t.typname, '',
           format('type %s %s %s base=%s %s default=%s check=%s attributes=%s acl=%s range=%s', t.typname, t.typtype,
                  COALESCE((SELECT string_agg(e.enumlabel, ',' ORDER BY e.enumsortorder)
                            FROM pg_enum e WHERE e.enumtypid = t.oid), '-'),
                  CASE WHEN t.typtype = 'd' THEN format_type(t.typbasetype, t.typtypmod) ELSE '-' END,
                  CASE WHEN t.typtype = 'd' AND t.typnotnull THEN 'not null' ELSE 'null' END,
                  COALESCE(t.typdefault, '-'),
                  COALESCE((SELECT string_agg(format('%s %s', con.conname, pg_get_constraintdef(con.oid)), ',' ORDER BY con.conname)
                            FROM pg_constraint con WHERE con.contypid = t.oid), '-'),
                  COALESCE((SELECT string_agg(format('%s %s', a.attname, format_type(a.atttypid, a.atttypmod)), ',' ORDER BY a.attnum)
                            FROM pg_attribute a WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped), '-'),
                  COALESCE(replace(array_to_string(t.typacl, ','), current_user, 'owner'), 'default'),
                  -- A range type's subtype, operator class, collation, canonical and
                  -- difference functions, and multirange type: two ranges of one name
                  -- print the same without them.
                  COALESCE((SELECT format('%s opclass=%s collation=%s canonical=%s subdiff=%s multirange=%s',
                                          format_type(r.rngsubtype, NULL),
                                          (SELECT opc.opcname FROM pg_opclass opc WHERE opc.oid = r.rngsubopc),
                                          COALESCE((SELECT col.collname FROM pg_collation col WHERE col.oid = r.rngcollation), '-'),
                                          COALESCE(NULLIF(r.rngcanonical::regproc::text, '-'), '-'),
                                          COALESCE(NULLIF(r.rngsubdiff::regproc::text, '-'), '-'),
                                          format_type(r.rngmultitypid, NULL))
                            FROM pg_range r WHERE r.rngtypid = t.oid), '-'))
    FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE n.nspname = current_schema() AND t.typtype IN ('e', 'd', 'c', 'r')
      AND NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.reltype = t.oid AND c.relkind <> 'c')
    UNION ALL
    SELECT 9, COALESCE(c.relname, p.proname, t.typname, con.conname), COALESCE(a.attname, ''),
           format('comment %s %s %s', COALESCE(c.relname, p.proname, t.typname, con.conname),
                  COALESCE(a.attname, '-'), d.description)
    FROM pg_description d
    LEFT JOIN pg_class c ON d.classoid = 'pg_class'::regclass AND c.oid = d.objoid
    LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = d.objsubid AND d.objsubid > 0
    LEFT JOIN pg_proc p ON d.classoid = 'pg_proc'::regclass AND p.oid = d.objoid
    LEFT JOIN pg_type t ON d.classoid = 'pg_type'::regclass AND t.oid = d.objoid
    LEFT JOIN pg_constraint con ON d.classoid = 'pg_constraint'::regclass AND con.oid = d.objoid
    JOIN pg_namespace n ON n.oid = COALESCE(c.relnamespace, p.pronamespace, t.typnamespace, con.connamespace)
    WHERE n.nspname = current_schema()
    UNION ALL
    SELECT 10, c.relname, pol.polname,
           format('policy %s.%s permissive=%s command=%s roles=%s using=%s check=%s',
                  c.relname, pol.polname, pol.polpermissive, pol.polcmd,
                  COALESCE((SELECT string_agg(label, ',' ORDER BY label)
                            FROM (SELECT CASE WHEN r = 0 THEN 'public'
                                              WHEN pg_get_userbyid(r) = current_user THEN 'owner'
                                              ELSE pg_get_userbyid(r) END AS label
                                  FROM unnest(pol.polroles) r) roles), '-'),
                  COALESCE(pg_get_expr(pol.polqual, pol.polrelid), '-'),
                  COALESCE(pg_get_expr(pol.polwithcheck, pol.polrelid), '-'))
    FROM pg_policy pol JOIN pg_class c ON c.oid = pol.polrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = current_schema()
    UNION ALL
    SELECT 11, c.relname, r.rulename,
           format('rule %s.%s %s', c.relname, r.rulename, pg_get_ruledef(r.oid, true))
    FROM pg_rewrite r JOIN pg_class c ON c.oid = r.ev_class
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = current_schema() AND r.rulename <> '_RETURN'
    UNION ALL
    SELECT 12, s.stxname, '',
           format('statistics %s %s', s.stxname, pg_get_statisticsobjdef(s.oid))
    FROM pg_statistic_ext s JOIN pg_namespace n ON n.oid = s.stxnamespace
    WHERE n.nspname = current_schema()
    UNION ALL
    -- Default privileges the baseline sets for the schema, and the ones it
    -- sets for the owning role in every schema (defaclnamespace 0), which
    -- shape objects created later without moving any existing ACL.
    SELECT 13, '', '',
           format('schema acl=%s default_acl=%s owner_default_acl=%s',
                  COALESCE(replace(array_to_string(n.nspacl, ','), current_user, 'owner'), 'default'),
                  COALESCE((SELECT string_agg(format('%s:%s', da.defaclobjtype,
                                                     replace(array_to_string(da.defaclacl, ','), current_user, 'owner')),
                                              ';' ORDER BY da.defaclobjtype)
                            FROM pg_default_acl da WHERE da.defaclnamespace = n.oid), '-'),
                  COALESCE((SELECT string_agg(format('%s:%s', da.defaclobjtype,
                                                     replace(array_to_string(da.defaclacl, ','), current_user, 'owner')),
                                              ';' ORDER BY da.defaclobjtype)
                            FROM pg_default_acl da WHERE da.defaclnamespace = 0 AND da.defaclrole = n.nspowner), '-'))
    FROM pg_namespace n WHERE n.nspname = current_schema()
    UNION ALL
    SELECT 14, col.collname, '',
           format('collation %s provider=%s collate=%s ctype=%s locale=%s deterministic=%s',
                  col.collname, col.collprovider,
                  COALESCE(NULLIF(col.collcollate, ''), '-'),
                  COALESCE(NULLIF(col.collctype, ''), '-'),
                  COALESCE(to_jsonb(col) ->> 'colllocale', to_jsonb(col) ->> 'colliculocale', '-'),
                  col.collisdeterministic)
    FROM pg_collation col JOIN pg_namespace n ON n.oid = col.collnamespace
    WHERE n.nspname = current_schema()
    UNION ALL
    -- A cast is catalogued by its types, not a schema: every cast whose source
    -- or target type lives here is listed, since an implicit or assignment
    -- cast changes how production SQL resolves operators and functions.
    SELECT 15, format_type(ca.castsource, NULL), format_type(ca.casttarget, NULL),
           format('cast %s -> %s context=%s method=%s function=%s',
                  format_type(ca.castsource, NULL), format_type(ca.casttarget, NULL),
                  ca.castcontext, ca.castmethod,
                  COALESCE((SELECT format('%s(%s)', p.proname, pg_get_function_identity_arguments(p.oid))
                            FROM pg_proc p WHERE p.oid = ca.castfunc), '-'))
    FROM pg_cast ca
    JOIN pg_type st ON st.oid = ca.castsource
    JOIN pg_type tt ON tt.oid = ca.casttarget
    JOIN pg_namespace sn ON sn.oid = st.typnamespace
    JOIN pg_namespace tn ON tn.oid = tt.typnamespace
    WHERE current_schema() IN (sn.nspname, tn.nspname)
    UNION ALL
    -- An operator the schema holds changes how expressions resolve even when
    -- its function already existed.
    SELECT 16, o.oprname, format('%s,%s', format_type(o.oprleft, NULL), format_type(o.oprright, NULL)),
           format('operator %s (%s, %s) returns %s function=%s commutator=%s negator=%s restrict=%s join=%s hashes=%s merges=%s',
                  o.oprname, format_type(o.oprleft, NULL), format_type(o.oprright, NULL),
                  format_type(o.oprresult, NULL), o.oprcode::regproc,
                  COALESCE(NULLIF(o.oprcom::regoperator::text, '0'), '-'),
                  COALESCE(NULLIF(o.oprnegate::regoperator::text, '0'), '-'),
                  COALESCE(NULLIF(o.oprrest::regproc::text, '-'), '-'),
                  COALESCE(NULLIF(o.oprjoin::regproc::text, '-'), '-'),
                  o.oprcanhash, o.oprcanmerge)
    FROM pg_operator o JOIN pg_namespace n ON n.oid = o.oprnamespace
    WHERE n.nspname = current_schema()
) catalog
ORDER BY section, a, b, line;
CATALOG_SQL
)"
frozen_schema_catalog="$ROOT/schema-v2/frozen-schema.txt"
# A fresh database gets the baseline alone (the schema-migrations are no-ops
# before it exists); an initialized one gets the baseline it was born with plus
# every schema-migration since. Both must be the same artifact, so the catalog
# is taken twice -- after the baseline, and again after the schema-migrations
# -- and the two must agree before either is compared with the frozen file.
# The catalog of one schema, the schema name normalized to bigname_phase.
frozen_schema_catalog() {
    local schema="$1"
    # An extension lives outside the schema (its objects usually in public), so
    # the declarations the baseline carries head the catalog as written, one
    # per line with whitespace collapsed: a new or changed CREATE EXTENSION is
    # a schema change like any other.
    printf '%s\n' "$baseline_extension_statements" \
        | sed -E 's/[[:space:]]+/ /g; s/ *; *$//; s/^ //' | sort | sed 's/^/extension /'
    {
        printf '\\pset format unaligned\n\\pset tuples_only on\n'
        printf 'SET search_path TO "%s";\n' "$schema"
        # Evaluates a column default expression as the column's type and prints
        # it as a one-element array, which is how pg_attribute.attmissingval
        # prints, so the two compare.
        cat <<'SQL'
CREATE FUNCTION pg_temp.frozen_catalog_default(expression text, is_array boolean) RETURNS text
LANGUAGE plpgsql STABLE AS $$
DECLARE evaluated text;
BEGIN
    IF expression IS NULL THEN RETURN NULL; END IF;
    -- An array value is stored as the one element of a text-typed array, and
    -- PostgreSQL flattens ARRAY[ARRAY[...]] instead.
    IF is_array THEN
        EXECUTE format('SELECT ARRAY[(%s)::text]::text', expression) INTO evaluated;
    ELSE
        EXECUTE format('SELECT ARRAY[(%s)]::text', expression) INTO evaluated;
    END IF;
    RETURN evaluated;
END $$;
SQL
        printf '%s\n' "$frozen_schema_catalog_sql"
    } | run_psql | sed "s/$schema/bigname_phase/g"
}
assert_frozen_schema_fingerprint() {
    local observed after_baseline migration_file
    observed="$(mktemp "${TMPDIR:-/tmp}/schema-v2-frozen-catalog.XXXXXX")"
    after_baseline="$(mktemp "${TMPDIR:-/tmp}/schema-v2-baseline-catalog.XXXXXX")"
    (
        scratch_schema="$frozen_schema"
        apply_baseline
        frozen_schema_catalog "$frozen_schema" > "$after_baseline"
        for migration_file in "$ROOT"/migrations/*.sql; do
            if phase_migration_uses_production_schema "$migration_file"; then
                render_phase_migration "$migration_file" | run_psql
            fi
        done
        frozen_schema_catalog "$frozen_schema" > "$observed"
    )
    if [ ! -s "$observed" ] || [ ! -s "$after_baseline" ]; then
        printf '%s\n' "the frozen artifact produced an empty catalog" >&2
        exit 1
    fi
    if ! diff -u "$after_baseline" "$observed" >&2; then
        printf '%s\n' \
            "the baseline alone (a fresh database) and the baseline plus every schema-migration (an initialized one) are different artifacts (diff above: - baseline, + migrated); a baseline edit needs the matching schema-migration, and a schema-migration needs the matching baseline edit" >&2
        rm -f -- "$observed" "$after_baseline"
        exit 1
    fi
    assert_predecessor_baseline_transition "$after_baseline" "$observed"
    rm -f -- "$after_baseline"
    if [ "${SCHEMA_V2_APPLY_CHECK_WRITE_FINGERPRINT:-0}" = 1 ]; then
        cp "$observed" "$frozen_schema_catalog"
        printf '%s\n' "wrote $(basename "$frozen_schema_catalog") ($(wc -l < "$observed" | tr -d ' ') lines)"
    elif ! diff -u "$frozen_schema_catalog" "$observed" >&2; then
        printf '%s\n' \
            "the frozen artifact's catalog differs from $(basename "$frozen_schema_catalog") (diff above); a schema change lands with SCHEMA_V2_APPLY_CHECK_WRITE_FINGERPRINT=1 regenerating it in the same change, under an ADR 0007 carve-out or amendment" >&2
        rm -f -- "$observed"
        exit 1
    fi
    rm -f -- "$observed"
}
# The comparison above proves every schema-migration is a no-op on the current
# baseline, which a baseline edit with no schema-migration also satisfies: the
# object is in both catalogs before any schema-migration runs. What an
# initialized database actually does is start from an earlier baseline, so
# the previous commit's baseline (the same point the inventory comparison
# reads) plus every current schema-migration must be the current baseline.
# The predecessor baseline is applied on its own before the schema-migrations
# and must differ from the current one exactly when the baseline files do,
# which proves this path reads the previous files and not the working tree.
assert_predecessor_baseline_transition() {
    local after_baseline="$1" observed="$2"
    local base status predecessor_dir predecessor_catalog migrated_catalog file
    base="$(prior_ref)" && status=0 || status=$?
    case "$status" in
        0) ;;
        2) return 0 ;;
        *) exit 1 ;;
    esac
    if ! git -C "$ROOT" cat-file -e "$base:schema-v2/baseline" 2>/dev/null; then
        printf '%s\n' "note: $base has no baseline, predecessor transition not compared" >&2
        return 0
    fi
    predecessor_dir="$(mktemp -d "${TMPDIR:-/tmp}/schema-v2-predecessor-baseline.XXXXXX")"
    while IFS= read -r file; do
        git -C "$ROOT" show "$base:schema-v2/baseline/$file" > "$predecessor_dir/$file"
    done < <(git -C "$ROOT" ls-tree --name-only "$base:schema-v2/baseline")
    predecessor_catalog="$(mktemp "${TMPDIR:-/tmp}/schema-v2-predecessor-catalog.XXXXXX")"
    migrated_catalog="$(mktemp "${TMPDIR:-/tmp}/schema-v2-predecessor-migrated.XXXXXX")"
    (
        scratch_schema="$predecessor_schema"
        # The catalog heads with the baseline's extension declarations; here
        # they are the predecessor's, so a declaration added to the baseline
        # alone shows up too.
        baseline_extension_statements="$(baseline_extension_statements_of "$predecessor_dir")" || exit 1
        apply_baseline "$predecessor_dir"
        frozen_schema_catalog "$predecessor_schema" > "$predecessor_catalog"
        for migration_file in "$ROOT"/migrations/*.sql; do
            if phase_migration_uses_production_schema "$migration_file"; then
                render_phase_migration "$migration_file" | run_psql
            fi
        done
        frozen_schema_catalog "$predecessor_schema" > "$migrated_catalog"
    ) || exit 1
    if git -C "$ROOT" diff --quiet "$base" -- schema-v2/baseline; then
        if ! diff -u "$after_baseline" "$predecessor_catalog" >&2; then
            printf '%s\n' "the baseline files are unchanged since $base but the predecessor baseline produced another catalog (diff above)" >&2
            exit 1
        fi
    elif diff -q "$after_baseline" "$predecessor_catalog" >/dev/null; then
        printf '%s\n' "the baseline files changed since $base but the predecessor baseline produced the same catalog; the predecessor path is not reading the previous files" >&2
        exit 1
    fi
    if ! diff -u "$migrated_catalog" "$observed" >&2; then
        printf '%s\n' \
            "the previous baseline ($base) plus every schema-migration and the current baseline are different artifacts (diff above: - previous baseline migrated, + current baseline); a baseline edit lands with the schema-migration that makes the same change on an initialized database" >&2
        rm -rf -- "$predecessor_dir" "$predecessor_catalog" "$migrated_catalog"
        exit 1
    fi
    rm -rf -- "$predecessor_dir" "$predecessor_catalog" "$migrated_catalog"
}
# The catalog proves on every run that it sees what it claims to: each change
# below is planted in the frozen schema inside a transaction that is rolled
# back, and the catalog taken inside that transaction must differ from the
# one taken without it. A planted change the catalog cannot see would pass
# the freeze unnoticed, which is how the earlier catalog gaps were found.
frozen_schema_catalog_within() {
    local schema="$1" planted_sql="$2"
    {
        printf '\\pset format unaligned\n\\pset tuples_only on\n'
        printf 'SET search_path TO "%s";\n' "$schema"
        printf 'BEGIN;\n%s\n' "$planted_sql"
        cat <<'SQL'
CREATE FUNCTION pg_temp.frozen_catalog_default(expression text, is_array boolean) RETURNS text
LANGUAGE plpgsql STABLE AS $$
DECLARE evaluated text;
BEGIN
    IF expression IS NULL THEN RETURN NULL; END IF;
    IF is_array THEN
        EXECUTE format('SELECT ARRAY[(%s)::text]::text', expression) INTO evaluated;
    ELSE
        EXECUTE format('SELECT ARRAY[(%s)]::text', expression) INTO evaluated;
    END IF;
    RETURN evaluated;
END $$;
SQL
        printf '%s\n' "$frozen_schema_catalog_sql"
        printf 'ROLLBACK;\n'
    } | run_psql | sed "s/$schema/bigname_phase/g"
}
assert_frozen_catalog_sees_planted_changes() {
    local planted reason planted_catalog
    local -a planted_changes=(
        # A schema-local collation named like the catalog one, and a text
        # column moved onto it: same name, another definition and namespace.
        'schema-local collation:CREATE COLLATION "C" FROM pg_catalog."C"; ALTER TABLE service_heartbeats ALTER COLUMN phase_name TYPE text COLLATE "C";'
        # A collation the schema holds that no column uses yet.
        'unused collation:CREATE COLLATION phase_c FROM pg_catalog."C";'
        # An implicit INOUT cast on a phase type: no function, no relation,
        # only a pg_cast row, and it changes operator resolution.
        'implicit cast:CREATE CAST (canonicality_state AS text) WITH INOUT AS IMPLICIT;'
        'assignment cast:CREATE CAST (text AS canonicality_state) WITH INOUT AS ASSIGNMENT;'
        # A type privilege: no relation, column or routine row moves.
        'type privilege:REVOKE USAGE ON TYPE canonicality_state FROM PUBLIC;'
        # An operator over an existing function: no routine row moves.
        'operator:CREATE OPERATOR === (LEFTARG = text, RIGHTARG = text, FUNCTION = pg_catalog.texteq);'
        # A default privilege for the owning role in every schema: no
        # existing ACL moves.
        'role-global default privilege:ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO PUBLIC;'
    )
    planted_catalog="$(mktemp "${TMPDIR:-/tmp}/schema-v2-planted-catalog.XXXXXX")"
    for planted in "${planted_changes[@]}"; do
        reason="${planted%%:*}"
        frozen_schema_catalog_within "$frozen_schema" "${planted#*:}" > "$planted_catalog"
        if [ ! -s "$planted_catalog" ]; then
            printf '%s\n' "the planted catalog ($reason) came back empty" >&2
            rm -f -- "$planted_catalog"
            exit 1
        fi
        if diff -q "$frozen_schema_catalog" "$planted_catalog" >/dev/null; then
            printf '%s\n' "the frozen catalog does not see a planted change ($reason)" >&2
            rm -f -- "$planted_catalog"
            exit 1
        fi
        refusal_assertions_passed=$((refusal_assertions_passed + 1))
    done
    # Two definitions under one name that print the same without the
    # column added for them: SQL-standard bodies leave prosrc empty for both,
    # and range types share the generic type row. The catalogs taken with
    # each must differ.
    local other_catalog pair reason
    other_catalog="$(mktemp "${TMPDIR:-/tmp}/schema-v2-planted-catalog.XXXXXX")"
    for pair in \
        'SQL-standard routine bodies:CREATE FUNCTION planted_atomic(x integer) RETURNS boolean LANGUAGE sql IMMUTABLE BEGIN ATOMIC SELECT x > 1; END;:CREATE FUNCTION planted_atomic(x integer) RETURNS boolean LANGUAGE sql IMMUTABLE BEGIN ATOMIC SELECT x > 2; END;' \
        'range subtypes:CREATE TYPE planted_range AS RANGE (SUBTYPE = bigint);:CREATE TYPE planted_range AS RANGE (SUBTYPE = integer);'
    do
        reason="${pair%%:*}"; pair="${pair#*:}"
        frozen_schema_catalog_within "$frozen_schema" "${pair%%:CREATE *}" > "$planted_catalog"
        frozen_schema_catalog_within "$frozen_schema" "CREATE ${pair#*:CREATE }" > "$other_catalog"
        if [ ! -s "$planted_catalog" ] || diff -q "$planted_catalog" "$other_catalog" >/dev/null; then
            printf '%s\n' "the frozen catalog does not tell two $reason apart" >&2
            rm -f -- "$planted_catalog" "$other_catalog"
            exit 1
        fi
        refusal_assertions_passed=$((refusal_assertions_passed + 1))
    done
    rm -f -- "$planted_catalog" "$other_catalog"
}
# The fresh artifact above has no rows, so a schema-migration whose DDL runs
# only when a table holds data leaves it unchanged there. The scratch schema
# has by now been populated by every predecessor-shape and behavior proof, and
# the proofs leave parts of it at older shapes; applying the whole inventoried
# sequence once more is what sqlx does on an initialized database at deploy
# (every file is required to be idempotent once applied), and the result must
# be the frozen artifact too, rows and all.
assert_exercised_schema_matches_frozen() {
    local exercised migration_file
    exercised="$(mktemp "${TMPDIR:-/tmp}/schema-v2-exercised-catalog.XXXXXX")"
    for migration_file in "$ROOT"/migrations/*.sql; do
        if phase_migration_uses_production_schema "$migration_file"; then
            emit_phase_migration "$migration_file" exercised | run_psql
        fi
    done
    frozen_schema_catalog "$scratch_schema" > "$exercised"
    if ! diff -u "$frozen_schema_catalog" "$exercised" >&2; then
        printf '%s\n' \
            "the exercised scratch schema (populated, every schema-migration applied) differs from $(basename "$frozen_schema_catalog") (diff above: - frozen, + exercised); a schema-migration whose effect depends on the rows it finds is not the frozen artifact on an initialized database" >&2
        rm -f -- "$exercised"
        exit 1
    fi
    rm -f -- "$exercised"
}
# The documented head is checked separately from the catalog: the head names
# the artifact, the catalog is the artifact; a merge that brings a newer file moves the
# artifact without moving the contract. Both documents name the head once as
# `migrations/<file>.sql`; each must be the newest file, and they must agree.
assert_documented_head_is_newest_migration() {
    local newest documented doc
    newest="$(ls "$ROOT"/migrations/*.sql | sort | tail -n 1 | xargs basename)"
    for doc in docs/adrs/0007-v1-schema-freeze.md docs/storage.md; do
        documented="$(grep -oE 'migrations/[0-9]{14}_[a-z0-9_]+\.sql' "$ROOT/$doc" | head -n 1 | sed 's#^migrations/##')"
        if [ -z "$documented" ]; then
            printf '%s\n' "$doc names no schema-migration head" >&2
            exit 1
        fi
        if [ "$documented" != "$newest" ]; then
            printf '%s\n' \
                "$doc names the schema-migration head $documented, but the newest file in migrations/ is $newest; advance the frozen head in ADR 0007 and storage.md in the change that lands the schema-migration" >&2
            exit 1
        fi
    done
}
# The inventory and the catalog are editable in the same change, so a file
# inserted below the head could join both. What tells an insertion from frozen
# history is the previous inventory: on a pull request the base branch's, on a
# push the parent commit's. Every entry that was not there before must sort
# after the head that was, and nothing that was there may go.
# Each inventory line is `<sha384>  <file>`, as sha384sum prints it.
current_migration_inventory() {
    (cd "$ROOT/migrations" && sha384sum -- *.sql | sort -k2)
}
assert_no_migration_below_prior_head() {
    local prior prior_head line entry checksum status
    prior="$(prior_migration_inventory)" && status=0 || status=$?
    case "$status" in
        0) ;;
        2) return 0 ;;
        *) exit 1 ;;
    esac
    prior_head="$(printf '%s\n' "$prior" | tail -n 1 | awk '{print $2}')"
    [ -n "$prior_head" ] || return 0
    local prior_checksum
    while IFS= read -r line; do
        [ -n "$line" ] || continue
        checksum="${line%% *}"; entry="${line##* }"
        prior_checksum="$(printf '%s\n' "$prior" | awk -v entry="$entry" '$2 == entry { print $1 }')"
        if [ -n "$prior_checksum" ]; then
            # "-" is an inventory written before checksums were recorded.
            if [ "$prior_checksum" != "-" ] && [ "$prior_checksum" != "$checksum" ]; then
                printf '%s\n' \
                    "$entry is in the previous inventory with different bytes; a listed schema-migration is immutable, sqlx rejects the edit on every initialized database (checksum now $checksum)" >&2
                exit 1
            fi
            continue
        fi
        if ! [[ "$entry" > "$prior_head" ]]; then
            printf '%s\n' \
                "$entry is new but sorts at or below the previous head $prior_head; sqlx would apply it to an initialized database while the freeze recorded nothing" >&2
            exit 1
        fi
    done < "$migration_inventory"
    local current_entries
    current_entries="$(awk '{print $2}' "$migration_inventory")"
    while IFS= read -r line; do
        [ -n "$line" ] || continue
        entry="${line##* }"
        # grep -q closes its input on the first match; under pipefail the
        # upstream writer's SIGPIPE would fail the pipeline, so no pipe here.
        if ! grep -qxF -- "$entry" <<< "$current_entries"; then
            printf '%s\n' "$entry was in the previous inventory and is gone; frozen history is immutable" >&2
            exit 1
        fi
    done <<< "$prior"
}
# The previous inventory on status 0; status 2 (with a note) when no history is
# reachable, which only a checkout without git or without a base can produce;
# status 1 when the base must be there and is not. The caller runs this in a
# command substitution, so the fatal case is a status, not an exit, and the
# caller tells it from the optional one instead of folding both into success.
# SCHEMA_V2_PRIOR_INVENTORY_REF names the comparison point explicitly.
# prior_ref prints the base commit under the same statuses; the predecessor
# baseline comparison reads the same point.
prior_ref() {
    local base
    if ! git -C "$ROOT" rev-parse --git-dir >/dev/null 2>&1; then
        printf '%s\n' "note: no git history, previous inventory not compared" >&2
        return 2
    fi
    if [ -n "${SCHEMA_V2_PRIOR_INVENTORY_REF:-}" ]; then
        base="$SCHEMA_V2_PRIOR_INVENTORY_REF"
    elif [ -n "${GITHUB_BASE_REF:-}" ]; then
        # In CI the base is not optional: a guard that skips on a failed fetch
        # is a guard a flaky network switches off.
        if ! git -C "$ROOT" fetch -q --depth=1 origin "$GITHUB_BASE_REF"; then
            printf '%s\n' "could not fetch the base branch $GITHUB_BASE_REF for the previous inventory" >&2
            return 1
        fi
        base="FETCH_HEAD"
    elif git -C "$ROOT" rev-parse --verify -q origin/main >/dev/null 2>&1 \
        && ! git -C "$ROOT" merge-base --is-ancestor HEAD origin/main 2>/dev/null; then
        base="origin/main"
    else
        git -C "$ROOT" fetch -q --deepen=1 origin 2>/dev/null || true
        base="HEAD~1"
    fi
    if ! git -C "$ROOT" rev-parse --verify -q "$base^{commit}" >/dev/null 2>&1; then
        if [ -n "${GITHUB_ACTIONS:-}" ]; then
            printf '%s\n' "the previous commit $base is not available for the previous inventory" >&2
            return 1
        fi
        printf '%s\n' "note: $base is not available, previous inventory not compared" >&2
        return 2
    fi
    printf '%s\n' "$base"
}
prior_migration_inventory() {
    local base status
    base="$(prior_ref)" && status=0 || status=$?
    [ "$status" = 0 ] || return "$status"
    if ! git -C "$ROOT" cat-file -e "$base:schema-v2/migration-inventory.txt" 2>/dev/null; then
        printf '%s\n' "note: $base has no migration inventory, previous inventory not compared" >&2
        return 2
    fi
    # A line with no checksum is an inventory written before checksums were recorded.
    git -C "$ROOT" show "$base:schema-v2/migration-inventory.txt" | awk 'NF == 1 { print "-", $1; next } { print }'
}
assert_uninventoried_migrations_are_schema_qualified() {
    local migration_file migration_basename reason noncanonical
    # The rule proves itself on every run: each planted form must be refused
    # with its reason, and the closed form must be accepted.
    local -a refused=(
        'DROP INDEX IF EXISTS public.old_idx, name_current_lookup_idx RESTRICT;'
        'DROP VIEW public.bridge CASCADE;'
        'DROP FUNCTION IF EXISTS public.fn(integer, text) cascade;'
        'DROP TABLE public.phase_child RESTRICT;'
        'drop table if exists public.a, public.b;'
        'DROP INDEX CONCURRENTLY IF EXISTS "public"."ok_idx";'
        'DROP VIEW "phase.audit";'
        'DROP SEQUENCE U&"bigname\005Fphase".chain_phase_state_seq;'
        'CREATE INDEX x ON public.t (a);'
        'UPDATE public.t SET a = 1;'
        'ALTER TABLE public.shadow INHERIT chain_phase_state;'
        'CREATE TABLE public.shadow () INHERITS (public.audit, chain_phase_state);'
        'WITH chosen AS (SELECT 1) UPDATE chain_phase_state SET a = 1;'
        'DROP FUNCTION IF EXISTS public.fn(integer, text), g(integer);'
        'DROP FUNCTION IF EXISTS public.fn(integer, text;'
        'CREATE TABLE public.audit AS SELECT * FROM ONLY chain_phase_state;'
        'CREATE TABLE public.audit AS SELECT * FROM public.safe, chain_phase_state;'
        'CREATE TABLE pg_temp.audit AS SELECT write_resolution_divergence(1);'
        'CREATE TABLE public.audit AS SELECT "write_resolution_divergence"(1);'
        'INSERT INTO public.audit VALUES (nextval('"'"'reverse_hydration_attempt_ordinal_seq'"'"'));'
        'COMMENT ON TABLE public.audit IS $msg$text -- literal$msg$; UPDATE chain_phase_state SET a = 1;'
        'DROP VIEW public.a /* -- */; UPDATE chain_phase_state SET a = 1;'
        'CREATE FUNCTION public.touch() RETURNS int LANGUAGE sql AS '"'"'SELECT 1'"'"';'
        'DROP VIEW public.a; DROP VIEW b;'
        'DROP FUNCTION public.fn(integer) GARBAGE;'
        'DROP FUNCTION IF EXISTS public.fn(integer) RESTRICT GARBAGE;'
        'DROP FUNCTION public.fn(integer) public.g(integer);'
        'DROP FUNCTION public.fn(integer),;'
        'DROP FUNCTION public.fn(integer) (text);'
        'DROP INDEX public.old_idx(integer);'
        'DROP FUNCTION public.fn(integer,);'
        'DROP FUNCTION public.fn(.);'
        'DROP FUNCTION public.fn(,integer);'
        'DROP FUNCTION public.fn(integer, numeric(10,));'
        'DROP FUNCTION public.fn(text[)];'
        'DROP FUNCTION public.fn(integer text.);'
        'DROP INDEX CONCURRENTLY IF EXISTS public.old_idx;'
        $'-- no-transaction\nDROP INDEX CONCURRENTLY public.a, public.b;'
        $'-- no-transaction\nDROP INDEX CONCURRENTLY IF EXISTS public.a, public.b RESTRICT;'
        $'-- a comment\n-- no-transaction\nDROP INDEX CONCURRENTLY public.a;'
    )
    for reason in "${refused[@]}"; do
        if printf '%s\n' "$reason" | migration_is_closed_form_drop /dev/stdin >/dev/null; then
            printf '%s\n' "closed-form check accepted a form it must refuse: $reason" >&2
            exit 1
        fi
    done
    if ! printf '%s\n' 'COMMENT ON TABLE U&"bigname\005Fphase".chain_phase_state IS '"'"'bigname_phase'"'"';' \
        | migration_uses_unicode_escape /dev/stdin \
        || ! printf '%s\n' "COMMENT ON TABLE u&'bigname_phase'.t IS '';" | migration_uses_unicode_escape /dev/stdin; then
        printf '%s\n' "unicode-escape check missed a planted U& form" >&2
        exit 1
    fi
    if printf '%s\n' 'DROP TABLE IF EXISTS bigname_phase.audit_u_and_v;' | migration_uses_unicode_escape /dev/stdin; then
        printf '%s\n' "unicode-escape check refused a plain identifier" >&2
        exit 1
    fi
    if ! printf -- '-- no-transaction\nDROP INDEX CONCURRENTLY IF EXISTS\n    public.old_idx;\nDROP VIEW IF EXISTS public.a, public.b;\nDROP MATERIALIZED VIEW public.mv RESTRICT;\nDROP SEQUENCE IF EXISTS public.s;\nDROP FUNCTION IF EXISTS public.fn(integer, text), public.g(numeric(10,2)), public.h(), public.k(double precision, character varying(10), text[], public.kind, timestamp with time zone);\nDROP PROCEDURE public.p(integer, text) RESTRICT;\n' \
        | migration_is_closed_form_drop /dev/stdin >/dev/null; then
        printf '%s\n' "closed-form check refused the closed form" >&2
        exit 1
    fi
    if ! diff -u "$migration_inventory" <(current_migration_inventory) >&2; then
        printf '%s\n' \
            "migrations/ differs from $(basename "$migration_inventory") (see the diff above): a schema-migration lands by joining the inventory and advancing the documented head in the same change, cannot be named to sort below the head, and once listed its bytes are immutable (sqlx checks the same checksum on every initialized database)" >&2
        exit 1
    fi
    for migration_file in "$ROOT"/migrations/*.sql; do
        migration_basename="$(basename "$migration_file")"
        [[ "$migration_basename" > "$legacy_public_schema_drop" ]] || continue
        # PostgreSQL folds an unquoted BIGNAME_PHASE to the production schema, but
        # the inventory and the scratch-schema rewrite match the lowercase literal
        # only, so any other spelling anywhere -- even beside a lowercase one --
        # would reach production unlisted and unrewritten: refuse it.
        # `grep -q` would close the pipe on its first hit and, under pipefail,
        # turn the producer's SIGPIPE into a failed test; read every match.
        noncanonical="$(grep -oi 'bigname_phase' "$migration_file" | grep -v '^bigname_phase$' || true)"
        if [ -n "$noncanonical" ]; then
            printf '%s\n' \
                "$migration_basename spells the phase schema other than bigname_phase; PostgreSQL folds it to the production schema but this check would neither inventory nor rewrite it" >&2
            exit 1
        fi
        # A Unicode-escaped identifier or string (U&"..." / U&'...') can spell
        # bigname_phase without containing the literal, so neither the
        # inventory match nor the scratch-schema rewrite would see it; there is
        # no schema-migration that needs the form, so refuse it outright.
        if migration_uses_unicode_escape "$migration_file"; then
            printf '%s\n' \
                "$migration_basename uses a Unicode-escaped identifier or string (U&), which this check can neither inventory nor rewrite" >&2
            exit 1
        fi
        if phase_migration_uses_production_schema "$migration_file"; then
            continue
        fi
        if ! reason="$(migration_is_closed_form_drop "$migration_file")"; then
            printf '%s\n' \
                "$migration_basename names no bigname_phase object, so it may only drop schema-qualified objects; it contains:$reason. Name bigname_phase to have it inventoried and applied, or qualify every drop target" >&2
            exit 1
        fi
    done
}
report_timing() {
    local elapsed=$((SECONDS - timing_started - ${2:-0}))
    if [ "${SCHEMA_V2_APPLY_CHECK_TIMING:-0}" = 1 ]; then
        printf 'schema-v2 timing: %s=%ss\n' "$1" "$elapsed"
    fi
    timing_started=$SECONDS
}
emit_phase_migration() {
    local migration_file="$1"
    local context="$2"
    if [ ! -f "$migration_file" ]; then
        printf '%s\n' "schema-migration path does not exist: $migration_file" >&2
        exit 1
    fi
    printf '%s|%s\n' "$context" "$(basename "$migration_file")" \
        >> "$migration_application_log"
    render_phase_migration "$migration_file"
}
# Leave an invalid, not-ready index under a scratch index's name the way an
# interrupted concurrent build does, with supported DDL only: a direct UPDATE
# of pg_index needs a superuser, which the documented external-database path
# does not have. A second session holds a writer's lock on the table inside an
# open transaction; the concurrent build creates its catalog entry, then waits
# for that transaction and is stopped by its own statement_timeout, which
# leaves the entry with indisvalid and indisready both false. The holder is
# then terminated (its own role may), so nothing outlives the helper.
build_invalid_index() {
    local index_name="$1" table_name="$2" flags attempt holder_pid
    printf 'SET search_path TO "%s";\nDROP INDEX %s;\n' "$scratch_schema" "$index_name" \
        | run_psql >/dev/null
    printf 'BEGIN;\nLOCK TABLE "%s".%s IN ROW EXCLUSIVE MODE;\nSELECT pg_sleep(120) AS apply_check_lock_holder;\nROLLBACK;\n' \
        "$scratch_schema" "$table_name" | run_psql >/dev/null 2>&1 &
    holder_pid=$!
    for attempt in $(seq 1 60); do
        if [ "$(
            printf '\\pset tuples_only on\n\\pset format unaligned\nSELECT count(*) FROM pg_stat_activity WHERE usename = current_user AND pid <> pg_backend_pid() AND state = %s AND query LIKE %s;\n' \
                "'active'" "'%apply_check_lock_holder%'" | run_psql
        )" = 1 ]; then
            break
        fi
        sleep 0.5
    done
    printf 'SET search_path TO "%s";\nSET statement_timeout = %s;\nCREATE INDEX CONCURRENTLY %s ON %s (chain_id);\n' \
        "$scratch_schema" "'3s'" "$index_name" "$table_name" | run_psql >/dev/null 2>&1 || true
    printf 'SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = current_user AND pid <> pg_backend_pid() AND query LIKE %s;\n' \
        "'%apply_check_lock_holder%'" | run_psql >/dev/null
    wait "$holder_pid" 2>/dev/null || true
    flags="$(
        printf '\\pset tuples_only on\n\\pset format unaligned\nSELECT indisvalid OR indisready FROM pg_index WHERE indexrelid = to_regclass(%s);\n' \
            "'$scratch_schema.$index_name'" | run_psql
    )"
    if [ "$flags" != f ]; then
        printf '%s\n' \
            "could not leave an invalid index under $index_name on $scratch_schema.$table_name (flags: '$flags')" >&2
        exit 1
    fi
}
# Run a probe with a scratch index replaced by an invalid one of the same
# name, then put the reviewed index back, whatever the probe's outcome.
with_index_invalidated() {
    local index_name="$1" table_name="$2" status definition
    shift 2
    definition="$(
        printf '\\pset tuples_only on\n\\pset format unaligned\nSET search_path TO "%s";\nSELECT pg_get_indexdef(%s::regclass);\n' \
            "$scratch_schema" "'$scratch_schema.$index_name'" | run_psql
    )"
    build_invalid_index "$index_name" "$table_name"
    "$@" && status=0 || status=$?
    printf 'SET search_path TO "%s";\nDROP INDEX %s;\n%s;\n' \
        "$scratch_schema" "$index_name" "$definition" | run_psql >/dev/null
    return "$status"
}
assert_migration_context_count() {
    local migration_file="$1"
    local context="$2"
    local expected_count="$3"
    local migration_basename
    local observed_count
    migration_basename="$(basename "$migration_file")"
    observed_count="$(
        awk -F '|' -v context="$context" -v migration="$migration_basename" \
            '$1 == context && $2 == migration { count += 1 } END { print count + 0 }' \
            "$migration_application_log"
    )"
    if [ "$observed_count" -ne "$expected_count" ]; then
        printf '%s\n' \
            "$migration_basename in $context: expected $expected_count, observed $observed_count successful applications" >&2
        exit 1
    fi
}
assert_reviewed_phase_migrations_applied() {
    local migration_file
    local migration_basename
    local skip_entry
    local skip_basename
    local skip_reason
    local skip_count=0
    local -a expected_migrations=()
    local -a without_predecessor_shape_proof=()
    local -A expected_lookup=()
    local -A skipped_lookup=()
    while IFS= read -r migration_file; do
        migration_basename="$(basename "$migration_file")"
        expected_migrations+=("$migration_basename")
        expected_lookup["$migration_basename"]=1
    done < <(
        for migration_file in "$ROOT"/migrations/*.sql; do
            if phase_migration_uses_production_schema "$migration_file"; then
                printf '%s\n' "$migration_file"
            fi
        done | sort
    )
    for skip_entry in "${intentional_phase_migration_skips[@]}"; do
        skip_basename="${skip_entry%%|*}"
        skip_reason="${skip_entry#*|}"
        if [ "$skip_basename" = "$skip_entry" ] || [ -z "$skip_reason" ]; then
            printf '%s\n' \
                "intentional schema-migration skip lacks a one-line reason: $skip_entry" >&2
            exit 1
        fi
        if [ -z "${expected_lookup[$skip_basename]:-}" ]; then
            printf '%s\n' \
                "intentional schema-migration skip names an unreviewed or missing file: $skip_basename" >&2
            exit 1
        fi
        if [ -n "${skipped_lookup[$skip_basename]:-}" ]; then
            printf '%s\n' \
                "duplicate intentional schema-migration skip: $skip_basename" >&2
            exit 1
        fi
        skipped_lookup["$skip_basename"]="$skip_reason"
        skip_count=$((skip_count + 1))
    done
    for migration_basename in "${expected_migrations[@]}"; do
        if [ -n "${skipped_lookup[$migration_basename]:-}" ]; then
            continue
        fi
        if ! awk -F '|' -v migration="$migration_basename" \
            '$1 == "empty-schema" && $2 == migration { found = 1 } END { exit !found }' \
            "$migration_application_log"
        then
            printf '%s\n' \
                "reviewed phase schema-migration was not applied on empty-schema path: $migration_basename" >&2
            exit 1
        fi
        if ! awk -F '|' -v migration="$migration_basename" \
            '($1 == "baseline-first" || $1 == "preceding-shape" || $1 == "specialized") && $2 == migration { found = 1 } END { exit !found }' \
            "$migration_application_log"
        then
            printf '%s\n' \
                "reviewed phase schema-migration was not applied on an initialized-schema path: $migration_basename" >&2
            exit 1
        fi
        if awk -F '|' -v migration="$migration_basename" \
            '($1 == "preceding-shape" || $1 == "specialized") && $2 == migration { found = 1 } END { exit !found }' \
            "$migration_application_log"; then
            predecessor_shape_proof_count=$((predecessor_shape_proof_count + 1))
        else
            without_predecessor_shape_proof+=("$migration_basename")
        fi
    done
    if [ "$predecessor_shape_proof_count" -ne "$expected_predecessor_shape_proof_count" ]; then
        printf '%s\n' "exact-predecessor-shape proof count: expected $expected_predecessor_shape_proof_count, observed $predecessor_shape_proof_count" >&2
        exit 1
    fi
    if [ "${SCHEMA_V2_APPLY_CHECK_VERBOSE:-0}" = 1 ]; then
        printf 'phase schema-migrations without exact-predecessor-shape proof (%s):\n' "${#without_predecessor_shape_proof[@]}" >&2
        printf '  %s\n' "${without_predecessor_shape_proof[@]}" >&2
    fi
    expected_reviewed_phase_migration_count="${#expected_migrations[@]}"
    unique_successful_migration_count="$(
        cut -d '|' -f 2 "$migration_application_log" | sort -u | wc -l \
            | tr -d ' '
    )"
    total_successful_migration_applications="$(wc -l < "$migration_application_log")"
    intentional_phase_migration_skip_count="$skip_count"
}
# Emit SQL that fails unless the session search_path is exactly this text.
assert_search_path_sql() {
    printf '%s\n' \
        "DO \$\$ BEGIN" \
        "    IF current_setting('search_path') <> '$1' THEN" \
        "        RAISE EXCEPTION 'search_path is %, expected $1', current_setting('search_path');" \
        "    END IF;" \
        "END \$\$;"
}
# Emit SQL that fails unless quote_all_identifiers is exactly this text.
assert_quote_all_identifiers_sql() {
    printf '%s\n' \
        "DO \$\$ BEGIN" \
        "    IF current_setting('quote_all_identifiers') <> '$1' THEN" \
        "        RAISE EXCEPTION 'quote_all_identifiers is %, expected $1', current_setting('quote_all_identifiers');" \
        "    END IF;" \
        "END \$\$;"
}
# Emit SQL that runs one check of printed catalog text, a schema-migration or
# a whole ops/ installer, for a caller that has quote_all_identifiers on.
# PostgreSQL then prints every identifier in pg_get_indexdef and
# pg_get_constraintdef quoted, so a check that compares or searches the
# printed text must turn the setting off while it reads the definitions, or it
# refuses healthy indexes and misses healthy constraints. It must also leave
# the caller's setting as it found it: in the session, inside one transaction
# where the setting is only transaction-local, and after that transaction
# commits. Nothing here is recorded as a schema-migration application.
emit_quote_all_identifiers_probe() {
    local checked_file="$1"
    local transaction_mode="$2"
    printf 'SET quote_all_identifiers = on;\n'
    render_phase_migration "$checked_file"
    assert_quote_all_identifiers_sql on
    if [ "$transaction_mode" = in-transaction ]; then
        # As sqlx applies a schema-migration. The installers cannot run here:
        # CREATE INDEX CONCURRENTLY refuses a transaction block.
        printf 'BEGIN;\n'
        render_phase_migration "$checked_file"
        assert_quote_all_identifiers_sql on
        printf 'COMMIT;\n'
        assert_quote_all_identifiers_sql on
        printf 'RESET quote_all_identifiers;\nBEGIN;\nSET LOCAL quote_all_identifiers = on;\n'
        render_phase_migration "$checked_file"
        assert_quote_all_identifiers_sql on
        printf 'COMMIT;\n'
        assert_quote_all_identifiers_sql off
    fi
    printf 'RESET quote_all_identifiers;\n'
}
# Print the first PostgreSQL error message in psql's stderr, whole. A RAISE
# that quotes a printed index definition spanning several lines, as the ENSv1
# lookahead due-probe index does, prints those lines after the ERROR: line and
# before the HINT:, DETAIL: or CONTEXT: line that follows the message.
psql_error_message() {
    awk '
        !started && /^ERROR:[[:space:]]*/ {
            started = 1
            sub(/^ERROR:[[:space:]]*/, "")
            print
            next
        }
        started && /^(ERROR|DETAIL|HINT|CONTEXT|QUERY|STATEMENT|WHERE|LOCATION|LINE [0-9]+|psql):/ { exit }
        started { print }
    '
}
assert_migration_refusal() {
    local label="$1"
    local migration_file="$2"
    local exact_message="$3"
    local setup_sql
    local refusal_stderr
    local observed_error
    local refusal_started=$SECONDS
    setup_sql="$(cat)"
    if refusal_stderr="$({
        printf 'BEGIN;\n'
        printf 'SET LOCAL search_path TO "%s";\n' "$scratch_schema"
        printf '%s\n' "$setup_sql"
        render_phase_migration "$migration_file"
        printf 'ROLLBACK;\n'
    } | run_psql 2>&1 >/dev/null)"; then
        printf '%s\n' "$label: migration unexpectedly succeeded" >&2
        exit 1
    fi
    observed_error="$(printf '%s\n' "$refusal_stderr" | psql_error_message)"
    if [ "$observed_error" != "$exact_message" ]; then
        printf '%s\n' \
            "$label: expected PostgreSQL error: $exact_message" \
            "$label: observed PostgreSQL error: $observed_error" \
            "$label: complete stderr:" \
            "$refusal_stderr" >&2
        exit 1
    fi
    refusal_assertions_passed=$((refusal_assertions_passed + 1))
    refusal_probe_seconds=$((refusal_probe_seconds + SECONDS - refusal_started))
}
# Run a concurrent index installer from ops/ against the scratch schema and
# require it to stop with exactly this error. CREATE INDEX CONCURRENTLY cannot
# run inside a transaction, so the caller commits its setup and undoes it.
assert_index_install_refusal() {
    local label="$1"
    local install_file="$2"
    local exact_message="$3"
    local refusal_stderr
    local observed_error
    local refusal_started=$SECONDS
    if refusal_stderr="$(
        render_phase_migration "$install_file" | run_psql 2>&1 >/dev/null
    )"; then
        printf '%s\n' "$label: index installer unexpectedly succeeded" >&2
        exit 1
    fi
    observed_error="$(printf '%s\n' "$refusal_stderr" | psql_error_message)"
    if [ "$observed_error" != "$exact_message" ]; then
        printf '%s\n' \
            "$label: expected PostgreSQL error: $exact_message" \
            "$label: observed PostgreSQL error: $observed_error" \
            "$label: complete stderr:" \
            "$refusal_stderr" >&2
        exit 1
    fi
    refusal_assertions_passed=$((refusal_assertions_passed + 1))
    refusal_probe_seconds=$((refusal_probe_seconds + SECONDS - refusal_started))
}
# Prove one concurrent index installer from ops/ against the scratch schema:
# from the shape without the index it builds the fresh-baseline definition and
# passes its own validity and definition checks, a rerun is a no-op, an invalid
# index, a valid index with other keys, or a table under the same name makes it
# fail, and the documented drop-and-rerun recovery works. So does a valid index
# that differs from the reviewed one only by the schema name and a dot inside
# each JSON key literal, which a check that strips the schema name from the
# printed definition would accept.
# The optional fifth argument names the indexed table (default discovery_edges)
# and the sixth the columns of the wrong-keys stand-in index.
assert_concurrent_index_installer() {
    local label="$1"
    local index_name="$2"
    local install_file="$3"
    local readme_path="$4"
    local table_name="${5:-discovery_edges}"
    local wrong_key_columns="${6:-active_from_block_number, chain_id}"
    local reviewed_definition
    local schema_in_literal_definition
    local matches_baseline_sql="DO \$\$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_index, expected_installed_index expected
        WHERE indexrelid = '$index_name'::regclass
          AND indisvalid AND indisready
          AND pg_get_indexdef(indexrelid) = expected.definition
    ) THEN
        RAISE EXCEPTION '$index_name prebuild differs from the baseline';
    END IF;
END \$\$;"
    {
        printf 'SET search_path TO "%s";\n' "$scratch_schema"
        printf '%s\n' \
            "CREATE TABLE expected_installed_index AS" \
            "SELECT pg_get_indexdef(indexrelid) AS definition" \
            "FROM pg_index WHERE indexrelid = '$index_name'::regclass;" \
            "DROP INDEX $index_name;"
        render_phase_migration "$install_file"
        render_phase_migration "$install_file"
        # The installer reads definitions under its own search_path and must
        # leave the session's as it found it.
        assert_search_path_sql "$scratch_schema"
        printf '%s\n' "$matches_baseline_sql"
        # A caller with quote_all_identifiers on must get the same answer for
        # the healthy index, and keep its setting.
        emit_quote_all_identifiers_probe "$install_file" outside-transaction
        printf '%s\n' "$matches_baseline_sql"
    } | run_psql >/dev/null
    # The installer names the fresh-baseline definition as the expected one,
    # printed as it reads it: with search_path set to pg_catalog, so the table
    # and the enum type both carry the schema name. Read it before the index is
    # replaced below.
    reviewed_definition="$(
        {
            printf '%s\n' \
                '\pset tuples_only on' \
                '\pset format unaligned' \
                "SET search_path TO pg_catalog;" \
                "SELECT pg_get_indexdef('$scratch_schema.$index_name'::regclass);"
        } | run_psql
    )"
    # An interrupted concurrent build leaves an invalid index under this name.
    build_invalid_index "$index_name" "$table_name"
    assert_index_install_refusal "$label-invalid-prebuild" "$install_file" \
        "$index_name is missing from $scratch_schema.$table_name or is not valid and ready; follow the recovery steps in $readme_path before retrying"
    # A wrong manual prebuild leaves a valid index with other keys under this
    # name.
    {
        printf 'SET search_path TO "%s";\n' "$scratch_schema"
        printf '%s\n' \
            "DROP INDEX $index_name;" \
            "CREATE INDEX $index_name" \
            "    ON $table_name ($wrong_key_columns);"
    } | run_psql >/dev/null
    assert_index_install_refusal "$label-wrong-keys-prebuild" "$install_file" \
        "$index_name exists but does not have the reviewed definition; found \"CREATE INDEX $index_name ON $scratch_schema.$table_name USING btree ($wrong_key_columns)\", expected \"$reviewed_definition\"; follow the recovery steps in $readme_path before retrying"
    # A valid, ready index on the right table whose JSON key literals start with
    # the schema name and a dot indexes other values, so it must be refused too.
    # Removing the schema name from the printed definition would hide that.
    schema_in_literal_definition="${reviewed_definition//->> \'/->> \'$scratch_schema.}"
    if [ "$schema_in_literal_definition" = "$reviewed_definition" ]; then
        # A path literal: the first path element gets the schema name instead.
        schema_in_literal_definition="${reviewed_definition//#>> \'\{/#>> \'\{$scratch_schema.}"
    fi
    if [ "$schema_in_literal_definition" = "$reviewed_definition" ]; then
        printf '%s\n' "$label: reviewed definition has no JSON key literal to alter" >&2
        exit 1
    fi
    {
        printf 'SET search_path TO "%s";\n' "$scratch_schema"
        printf '%s\n' "DROP INDEX $index_name;" "$schema_in_literal_definition;"
    } | run_psql >/dev/null
    assert_index_install_refusal "$label-schema-name-in-literal-prebuild" "$install_file" \
        "$index_name exists but does not have the reviewed definition; found \"$schema_in_literal_definition\", expected \"$reviewed_definition\"; follow the recovery steps in $readme_path before retrying"
    # IF NOT EXISTS also skips a table under this name.
    {
        printf 'SET search_path TO "%s";\n' "$scratch_schema"
        printf '%s\n' "DROP INDEX $index_name;" "CREATE TABLE $index_name ();"
    } | run_psql >/dev/null
    assert_index_install_refusal "$label-table-under-name" "$install_file" \
        "$scratch_schema.$index_name is a table, not an index, so the index was never built; remove or rename that relation, then follow $readme_path before retrying"
    {
        printf 'SET search_path TO "%s";\n' "$scratch_schema"
        printf '%s\n' "DROP TABLE $index_name;"
        render_phase_migration "$install_file"
        printf '%s\n' "$matches_baseline_sql" "DROP TABLE expected_installed_index;"
    } | run_psql >/dev/null
}
# The discovery index validity check must accept the reviewed definitions
# however they were built. sqlx runs schema-migrations without the phase schema
# on search_path, which makes PostgreSQL print the enum type in the predicate
# with its schema name unless the check controls search_path itself, so prove
# both session settings. The check must also leave the session search_path as
# it found it.
assert_discovery_index_definitions_accepted() {
    local context="$1"
    {
        printf 'SET search_path TO "%s";\n' "$scratch_schema"
        emit_phase_migration "$discovery_index_validity_migration" "$context"
        assert_search_path_sql "$scratch_schema"
        printf 'SET search_path TO public;\n'
        emit_phase_migration "$discovery_index_validity_migration" "$context"
        assert_search_path_sql public
        # Inside one transaction, as sqlx applies a schema-migration, a later
        # statement must see the search_path the transaction already had.
        printf 'BEGIN;\nSET LOCAL search_path TO "%s", public;\n' "$scratch_schema"
        render_phase_migration "$discovery_index_validity_migration"
        assert_search_path_sql "$scratch_schema, public"
        printf 'COMMIT;\n'
        assert_search_path_sql public
        emit_quote_all_identifiers_probe "$discovery_index_validity_migration" in-transaction
    } | run_psql
}
assert_unconfigured_settlement_constraint() {
    local provenance="$1"
    local false_error

    {
        printf 'SET search_path TO "%s";\n' "$scratch_schema"
        printf '%s\n' \
            "INSERT INTO chain_phase_state (" \
            "    chain_id, phase_name, settled_while_unconfigured" \
            ") VALUES (" \
            "    'phase-settlement-${provenance}-true', 'ingest', TRUE" \
            ");"
    } | run_psql

    if false_error="$({
        printf 'SET search_path TO "%s";\n' "$scratch_schema"
        printf '%s\n' \
            "INSERT INTO chain_phase_state (" \
            "    chain_id, phase_name, settled_while_unconfigured" \
            ") VALUES (" \
            "    'phase-settlement-${provenance}-false', 'project', FALSE" \
            ");"
    } | run_psql 2>&1)"; then
        printf '%s\n' \
            "$provenance settlement constraint accepted a FALSE marker" >&2
        exit 1
    fi
    if [[ "$false_error" != *chain_phase_state_unconfigured_settlement_check* ]]; then
        printf '%s\n' \
            "$provenance FALSE marker failed without the named settlement constraint" >&2
        printf '%s\n' "$false_error" >&2
        exit 1
    fi
    printf '%s\n' \
        "$provenance settlement constraint accepts non-Verify TRUE and rejects FALSE"
}

wait_for_schema_v2_race_session() {
    local application_name="$1"
    local status
    local attempt

    for ((attempt = 1; attempt <= 100; attempt += 1)); do
        status="$(
            {
                printf 'SET search_path TO "%s";\n' "$scratch_schema"
                printf '%s\n' \
                    "SELECT CASE WHEN EXISTS (" \
                    "    SELECT 1" \
                    "    FROM pg_stat_activity" \
                    "    WHERE application_name = '$application_name'" \
                    "      AND wait_event = 'PgSleep'" \
                    ") THEN 'schema_v2_race_ready'" \
                    "ELSE 'schema_v2_race_waiting' END;"
            } | run_psql_as_owner
        )"
        if [[ "$status" == *schema_v2_race_ready* ]]; then
            return 0
        fi
        sleep 0.05
    done

    return 1
}

scratch_schema="schema_v2_apply_check_${PPID}_$$"
apply_check_role="${scratch_schema}_role"
if [[ ! "$scratch_schema" =~ ^[a-z0-9_]+$ ]]; then
    printf '%s\n' "invalid scratch schema name" >&2
    exit 1
fi
apply_check_role_password="$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')"
# The owner's URL with its userinfo replaced by the given login and password.
# libpq also takes credentials as query parameters, which override the
# userinfo, and percent-decodes a parameter's name before reading it, so
# `%75ser=` is `user=`; those keys are dropped after decoding and every other
# connection option is kept as written.
login_url_from() {
    local owner_url="$1" login="$2" password="$3"
    local url_scheme url_rest url_authority url_path url_query
    url_scheme="${owner_url%%://*}://"
    url_rest="${owner_url#*://}"
    # The authority ends at the first of / ? # -- /dbname is optional in libpq's
    # grammar, so a query may follow the host directly.
    url_authority="$(printf '%s' "$url_rest" | sed -E 's#[/?\#].*$##')"
    url_path="${url_rest#"$url_authority"}"
    url_path="${url_path%%#*}"
    url_query=""
    if [[ "$url_path" == *\?* ]]; then
        url_query="${url_path#*\?}"
        url_path="${url_path%%\?*}"
        url_query="$(printf '%s' "$url_query" | tr '&' '\n' | awk '
            BEGIN { for (i = 0; i < 256; i++) byte[sprintf("%02x", i)] = i }
            function decoded(text,    out, i, c, hex) {
                out = ""; i = 1
                while (i <= length(text)) {
                    c = substr(text, i, 1); hex = tolower(substr(text, i + 1, 2))
                    if (c == "%" && hex ~ /^[0-9a-f][0-9a-f]$/) { out = out sprintf("%c", byte[hex]); i += 3 }
                    else { out = out c; i++ }
                }
                return out
            }
            { key = $0; sub(/=.*/, "", key); key = decoded(key) }
            key == "user" || key == "password" || key == "passfile" { next }
            { print }' | paste -sd '&' -)"
    fi
    printf '%s\n' "${url_scheme}${login}:${password}@${url_authority##*@}${url_path}${url_query:+?$url_query}"
}
# The rewrite proves itself on every run: each planted query form must lose
# its credential keys and keep the other options, encoded or not.
assert_login_url_drops_credentials() {
    local planted rewritten
    for planted in \
        'postgresql://owner:secret@db.example:5432/bigname?user=owner&password=secret&passfile=/x&sslmode=require' \
        'postgresql://owner:secret@db.example/bigname?%75ser=owner&%70assword=secret&sslmode=require' \
        'postgresql://owner:secret@db.example?PASSFILE=/x&%70%61%73%73%66%69%6C%65=/x&sslmode=require&user=owner' \
        'postgres://db.example/bigname?application_name=a%3Db&user=owner&sslmode=require'; do
        rewritten="$(login_url_from "$planted" login pw)"
        case "$rewritten" in
            *login:pw@db.example*sslmode=require*) ;;
            *) printf '%s\n' "the login URL rewrite lost the host or an option: $rewritten" >&2; exit 1 ;;
        esac
        if [[ "$rewritten" == *owner* ]] || [[ "$rewritten" == *secret* ]] || [[ "$rewritten" == *passfile* ]] \
            || [[ "$rewritten" == *%75ser* ]] || [[ "$rewritten" == *%70assword* ]] || [[ "$rewritten" == *%70%61* ]]; then
            printf '%s\n' "the login URL rewrite kept a credential key: $rewritten" >&2
            exit 1
        fi
    done
    # A key libpq does not decode to a credential stays: it is not ours to drop.
    case "$(login_url_from 'postgresql://owner:secret@db.example/bigname?PASSFILE=/x' login pw)" in
        *PASSFILE=/x*) ;;
        *) printf '%s\n' "the login URL rewrite dropped a non-credential key" >&2; exit 1 ;;
    esac
}
assert_login_url_drops_credentials
# The same URL over another database: libpq's optional /dbname is replaced or
# added, and the query is kept.
url_with_database() {
    local owner_url="$1" database="$2"
    local url_scheme url_rest url_authority url_path url_query
    url_scheme="${owner_url%%://*}://"
    url_rest="${owner_url#*://}"
    url_authority="$(printf '%s' "$url_rest" | sed -E 's#[/?\#].*$##')"
    url_path="${url_rest#"$url_authority"}"
    url_path="${url_path%%#*}"
    url_query=""
    if [[ "$url_path" == *\?* ]]; then
        url_query="?${url_path#*\?}"
    fi
    printf '%s\n' "${url_scheme}${url_authority}/${database}${url_query}"
}
[ "$(url_with_database 'postgresql://owner:secret@db.example:5432/bigname?sslmode=require' scratch_db)" = 'postgresql://owner:secret@db.example:5432/scratch_db?sslmode=require' ] || { printf '%s\n' "the database rewrite lost the query or authority" >&2; exit 1; }
[ "$(url_with_database 'postgresql://owner:secret@db.example?sslmode=require' scratch_db)" = 'postgresql://owner:secret@db.example/scratch_db?sslmode=require' ] || { printf '%s\n' "the database rewrite did not add a path" >&2; exit 1; }
# On an external server the configured user holds CREATEDB (the Rust tests
# create their databases the same way; docs/development.md) but not
# necessarily CREATE on the database the URL names, which CREATE SCHEMA needs.
# The check therefore runs in a database of its own, owned by that user, and
# drops it on exit; the URL that named the server is kept for the drop.
apply_check_database=""
apply_check_server_url="${BIGNAME_DATABASE_URL:-}"
if [ "${SCHEMA_V2_EXTERNAL_DATABASE:-0}" = 1 ] && [ -n "${BIGNAME_DATABASE_URL:-}" ]; then
    apply_check_database="${scratch_schema}_db"
    printf 'CREATE DATABASE "%s";\n' "$apply_check_database" | run_psql_as_owner
    BIGNAME_DATABASE_URL="$(url_with_database "$BIGNAME_DATABASE_URL" "$apply_check_database")"
    export BIGNAME_DATABASE_URL
fi
apply_check_url=""
if [ -n "${BIGNAME_DATABASE_URL:-}" ]; then
    apply_check_url="$(login_url_from "$BIGNAME_DATABASE_URL" "$apply_check_role" "$apply_check_role_password")"
fi
migration_application_log="$(
    mktemp "${TMPDIR:-/tmp}/schema-v2-migration-applications.XXXXXX"
)"
# Future entries must use basename|one-line reason.
intentional_phase_migration_skips=()
refusal_assertions_passed=0
expected_refusal_assertions=216
predecessor_shape_proof_count=0
expected_predecessor_shape_proof_count=40
refusal_probe_seconds=0
timing_started=$SECONDS

cleanup() {
    if [ -n "${schema_v2_race_pid:-}" ] \
        && kill -0 "$schema_v2_race_pid" >/dev/null 2>&1; then
        kill "$schema_v2_race_pid" >/dev/null 2>&1 || true
        wait "$schema_v2_race_pid" >/dev/null 2>&1 || true
    fi
    if [ -n "${schema_v2_race_log:-}" ]; then
        rm -f -- "$schema_v2_race_log"
    fi
    if [ -n "${migration_application_log:-}" ]; then
        rm -f -- "$migration_application_log"
    fi
    {
        printf 'DROP SCHEMA IF EXISTS "%s" CASCADE;\n' "$scratch_schema"
        printf 'DROP SCHEMA IF EXISTS "%s" CASCADE;\n' "${scratch_schema}_frozen"
        printf 'DROP SCHEMA IF EXISTS "%s" CASCADE;\n' "${scratch_schema}_predecessor"
        printf 'DROP OWNED BY "%s";\n' "$apply_check_role"
        printf 'DROP ROLE IF EXISTS "%s";\n' "$apply_check_role"
    } | run_psql_as_owner >/dev/null 2>&1 || true
    if [ -n "${apply_check_database:-}" ]; then
        printf 'DROP DATABASE IF EXISTS "%s" WITH (FORCE);\n' "$apply_check_database" \
            | BIGNAME_DATABASE_URL="$apply_check_server_url" run_psql_as_owner >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

# The owner keeps the race probe (another session's wait event is not visible
# to an ordinary login) and installs the extensions the baseline declares,
# which the login could not (it never holds CREATE on the database). Only the
# two reviewed statements are forwarded, matched whole: btree_gist for the
# baseline's exclusion constraints and pgcrypto for public.digest, each
# `CREATE EXTENSION IF NOT EXISTS <name> WITH SCHEMA public;` as a statement of its
# own. Anything else in a CREATE EXTENSION statement -- a third extension, another
# schema, a second statement after the semicolon -- fails here rather than
# running on the owner's connection; a new extension is a schema change and
# joins this list under an ADR 0007 carve-out or amendment. A real
# init-schema on an empty database has only the baseline to install them, so
# both must still be declared there.
# The statements of an SQL file, one per line with whitespace collapsed and
# the trailing semicolon kept: line and (nested) block comments removed, the
# splitter blind inside single quotes, quoted identifiers and dollar quoting,
# so a statement inside a comment or a routine body is not a statement.
sql_statement_splitter='
    BEGIN { quote = ""; depth = 0; stmt = ""; escaped = 0 }
    {
        line = $0 "\n"; n = length(line); i = 1
        while (i <= n) {
            c = substr(line, i, 1); two = substr(line, i, 2)
            if (depth > 0) {
                if (two == "/*") { depth++; i += 2; continue }
                if (two == "*/") { depth--; i += 2; continue }
                i++; continue
            }
            if (quote == "") {
                if (two == "--") break
                if (two == "/*") { depth = 1; i += 2; continue }
                if (c == "$" && match(substr(line, i), /^\$[A-Za-z_][A-Za-z0-9_]*\$|^\$\$/)) {
                    quote = substr(line, i, RLENGTH); stmt = stmt quote; i += RLENGTH; continue
                }
                if (c == "\047" || c == "\"") {
                    quote = c
                    # An escape-string literal (E prefix): a backslash escapes
                    # the next character, so an escaped quote does not end it.
                    escaped = (c == "\047" && i > 1 && substr(line, i - 1, 1) ~ /[Ee]/ \
                               && (i == 2 || substr(line, i - 2, 1) !~ /[A-Za-z0-9_]/))
                }
                else if (c == ";") { emit(stmt ";"); stmt = ""; i++; continue }
            } else if (length(quote) > 1) {
                if (substr(line, i, length(quote)) == quote) { stmt = stmt quote; i += length(quote); quote = ""; continue }
            } else if (escaped && c == "\\") {
                stmt = stmt substr(line, i, 2); i += 2; continue
            } else if (c == quote) {
                if (quote == "\047" && substr(line, i + 1, 1) == "\047") { stmt = stmt "\047\047"; i += 2; continue }
                quote = ""; escaped = 0
            }
            stmt = stmt c; i++
        }
    }
    function emit(text) {
        gsub(/[[:space:]]+/, " ", text); sub(/^ /, "", text); sub(/ ;$/, ";", text)
        if (text != ";") print text
    }
    END {
        if (depth > 0) { print "[unterminated block comment]"; exit 1 }
        if (quote != "") { print "[unterminated quote]"; exit 1 }
        sub(/[[:space:]]+$/, "", stmt)
        if (stmt != "") emit(stmt)
    }
'
sql_statements() {
    awk "$sql_statement_splitter" "$@"
}
# The statements a baseline directory would execute to install extensions:
# every CREATE EXTENSION the splitter finds as a statement of its own, so a
# declaration a comment or a routine body has swallowed is not one.
baseline_extension_statements_of() {
    local statements
    if ! statements="$(sql_statements "$1"/*.sql)"; then
        printf '%s\n' "schema-v2/baseline cannot be split into statements: $(printf '%s\n' "$statements" | tail -n 1)" >&2
        return 1
    fi
    printf '%s\n' "$statements" | grep -iE '^CREATE EXTENSION ' || true
}
# Exactly the two reviewed statements, both present: anything else that would
# reach the owner's connection, or a prerequisite the runtime baseline no
# longer installs, fails here with the reason.
check_baseline_extensions() {
    local statements="$1" required_extension extension_statement
    for required_extension in btree_gist pgcrypto; do
        if ! printf '%s\n' "$statements" | grep -qxF "CREATE EXTENSION IF NOT EXISTS $required_extension WITH SCHEMA public;"; then
            printf '%s\n' "schema-v2/baseline no longer declares CREATE EXTENSION IF NOT EXISTS $required_extension WITH SCHEMA public; as a statement of its own; init-schema on an empty database needs it" >&2
            return 1
        fi
    done
    while IFS= read -r extension_statement; do
        [ -n "$extension_statement" ] || continue
        case "$extension_statement" in
            "CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public;" \
            | "CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;") ;;
            *)
                printf '%s\n' \
                    "schema-v2/baseline carries a CREATE EXTENSION statement this check does not know: $extension_statement" \
                    "only the two reviewed btree_gist and pgcrypto statements run on the owner's connection; a new extension is a schema change under ADR 0007 and joins the reviewed list here" >&2
                return 1
                ;;
        esac
    done <<< "$statements"
}
# The rule proves itself on every run against planted copies of the baseline:
# a declaration inside a block comment, a line comment, or a DO body is not
# installed by init-schema and must not pass; a third extension, another
# schema, or a split statement must not reach the owner.
assert_baseline_extension_rule_holds() {
    local planted planted_dir reason statements
    local -a refused=(
        'block comment:/* CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public; */'
        'nested block comment:/* outer /* inner */ CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public; */'
        'line comment:-- CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public;'
        'do body:DO $$ BEGIN EXECUTE '"'"'CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public'"'"'; END $$;'
        'string:SELECT '"'"'CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public;'"'"';'
        'third extension:CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public; CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA public;'
        'other schema:CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA bigname_phase;'
        'split line:CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public'
        'escape string:SELECT E'"'"'it\\'"'"'s not a statement; CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public;'"'"';'
        'unterminated string:SELECT '"'"'open; CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public;'
        'unterminated block comment:/* open; CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public;'
    )
    planted_dir="$(mktemp -d "${TMPDIR:-/tmp}/schema-v2-extension-rule.XXXXXX")"
    for planted in "${refused[@]}"; do
        reason="${planted%%:*}"
        rm -f "$planted_dir"/*.sql
        printf '%s\n' "${planted#*:}" > "$planted_dir/01_planted.sql"
        # The other required statement stays intact so only the planted form decides.
        printf 'CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;\n' > "$planted_dir/02_other.sql"
        if statements="$(baseline_extension_statements_of "$planted_dir" 2>/dev/null)" \
            && check_baseline_extensions "$statements" 2>/dev/null; then
            printf '%s\n' "extension rule accepted a baseline it must refuse ($reason)" >&2
            exit 1
        fi
        refusal_assertions_passed=$((refusal_assertions_passed + 1))
    done
    rm -f "$planted_dir"/*.sql
    printf '/* leading */ CREATE   EXTENSION IF NOT EXISTS\n  btree_gist WITH SCHEMA public; -- trailing\nSELECT E'"'"'a\\'"'"'b'"'"', '"'"'c'"'"''"'"'d'"'"', $q$e'"'"'f$q$;\nCREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;\n' > "$planted_dir/01_ok.sql"
    if ! statements="$(baseline_extension_statements_of "$planted_dir")" || ! check_baseline_extensions "$statements"; then
        printf '%s\n' "extension rule refused the two reviewed statements written across lines and beside comments" >&2
        exit 1
    fi
    rm -rf "$planted_dir"
}
assert_baseline_extension_rule_holds
baseline_extension_statements="$(baseline_extension_statements_of "$ROOT/schema-v2/baseline")" || exit 1
check_baseline_extensions "$baseline_extension_statements" || exit 1
# The login is provisioned by the owner without any database-level grant,
# which the documented external-database user (CREATEDB and CREATEROLE, not
# necessarily the database's owner) may not be able to make: the owner takes
# membership of the role it just created and creates both scratch schemas
# owned by the login. The login connects on PUBLIC's default CONNECT and never
# holds CREATE on the database, so an assembled CREATE SCHEMA bigname_phase
# fails where the production schema does not yet exist.
frozen_schema="${scratch_schema}_frozen"
predecessor_schema="${scratch_schema}_predecessor"
{
    printf "CREATE ROLE \"%s\" LOGIN PASSWORD '%s';\n" \
        "$apply_check_role" "$apply_check_role_password"
    printf 'GRANT "%s" TO CURRENT_USER;\n' "$apply_check_role"
    printf '%s\n' "$baseline_extension_statements"
    printf 'CREATE SCHEMA "%s" AUTHORIZATION "%s";\n' "$scratch_schema" "$apply_check_role"
    printf 'CREATE SCHEMA "%s" AUTHORIZATION "%s";\n' "$frozen_schema" "$apply_check_role"
    printf 'CREATE SCHEMA "%s" AUTHORIZATION "%s";\n' "$predecessor_schema" "$apply_check_role"
} | run_psql_as_owner
# Prove the role boundary on every run: an identifier the rewrite cannot see,
# assembled inside EXECUTE, must fail on the production schema whether or not
# that schema exists in this database, while the same statement against the
# rewritten literal succeeds in the scratch schema.
assert_dynamic_production_name_is_refused() {
    local probe_stderr statement
    local prelude
    for prelude in "" "RESET ROLE;" "RESET SESSION AUTHORIZATION;" "SET SESSION AUTHORIZATION DEFAULT;"; do
    for statement in \
        "CREATE TABLE bigname_' || 'phase.apply_check_probe (a int)" \
        "INSERT INTO bigname_' || 'phase.chain_phase_state (chain_id, phase_name) VALUES (''probe'', ''ingest'')" \
        "CREATE SCHEMA bigname_' || 'phase_apply_check_probe"
    do
        if probe_stderr="$({
            printf 'SET search_path TO "%s";\n' "$scratch_schema"
            [ -z "$prelude" ] || printf '%s\n' "$prelude"
            printf "DO \$\$ BEGIN EXECUTE '%s'; END \$\$;\n" "$statement"
        } | run_psql 2>&1 >/dev/null)"; then
            printf '%s\n' "dynamic production schema name was not refused: $statement" >&2
            exit 1
        fi
        case "$probe_stderr" in
            *"permission denied for schema bigname_phase"* \
                | *'schema "bigname_phase" does not exist'* \
                | *'relation "bigname_phase.'*'does not exist'* \
                | *"permission denied for database"*) ;;
            *)
                printf '%s\n' "dynamic production schema name failed for another reason:" "$probe_stderr" >&2
                exit 1
                ;;
        esac
    done
    done
    # The owner is not reachable from the login role at all.
    local owner_login
    owner_login="$(printf 'SELECT current_user AS owner_login \\gset\n\\echo :owner_login\n' | run_psql_as_owner)"
    if [ -z "$owner_login" ]; then
        printf '%s\n' "could not read the owner login" >&2
        exit 1
    fi
    if printf 'SET ROLE "%s";\n' "$owner_login" | run_psql >/dev/null 2>&1 \
        || printf 'SET SESSION AUTHORIZATION "%s";\n' "$owner_login" | run_psql >/dev/null 2>&1; then
        printf '%s\n' "the conformance login could assume the owner" >&2
        exit 1
    fi
    if ! {
        printf 'SET search_path TO "%s";\n' "$scratch_schema"
        printf 'BEGIN;\n'
        printf "DO \$\$ BEGIN EXECUTE 'CREATE TABLE bigname_phase.apply_check_probe (a int)'; END \$\$;\n" \
            | sed "s/bigname_phase/$scratch_schema/g"
        printf 'ROLLBACK;\n'
    } | run_psql >/dev/null 2>&1; then
        printf '%s\n' "rewritten dynamic scratch schema name was refused" >&2
        exit 1
    fi
    refusal_assertions_passed=$((refusal_assertions_passed + 1))
}
assert_dynamic_production_name_is_refused

apply_baseline() {
    local sql_file
    for sql_file in "${1:-$ROOT/schema-v2/baseline}"/*.sql; do
        {
            printf 'SET client_min_messages TO warning;\n'
            printf 'SET search_path TO "%s";\n' "$scratch_schema"
            cat "$sql_file"
        } | run_psql
    done
}

# A schema-migration database can exist before phase-runner installs the phase
# baseline. Every reviewed phase schema-migration must be a no-op on that empty path.
for migration_file in \
    "$ROOT/migrations/20260809120000_manifest_authority_attestation_audit.sql" \
    "$ROOT/migrations/20260810120000_remove_l2_migration_remnants.sql" \
    "$ROOT/migrations/20260811120000_ens_v2_migration_slice_1.sql" \
    "$ROOT/migrations/20260811120100_ens_v2_migration_slice_1_validate.sql" \
    "$ROOT/migrations/20260811120200_ens_v2_migration_slice_1_constraints.sql" \
    "$ROOT/migrations/20260813120000_reverse_hydration_attempt_state.sql" \
    "$ROOT/migrations/20260813120100_reverse_hydration_attempt_state_validate.sql" \
    "$ROOT/migrations/20260814120000_project_redo_resolver_evidence.sql" \
    "$ROOT/migrations/20260814122000_verify_unconfigured_settlement.sql" \
    "$ROOT/migrations/20260814122100_verify_unconfigured_settlement_validate.sql" \
    "$ROOT/migrations/20260814123000_ingest_redo_source_boundary_markers.sql" \
    "$ROOT/migrations/20260814124000_redo_attempt_generation.sql" \
    "$ROOT/migrations/20260814125000_ingest_redo_manifest_authority.sql" \
    "$ROOT/migrations/20260814130000_surface_binding_authority_arm.sql" \
    "$ROOT/migrations/20260814131000_project_generation_failure_audit.sql" \
    "$ROOT/migrations/20260814132000_project_generation_failure_child_authority.sql" \
    "$ROOT/migrations/20260820140000_raw_block_preimage_derivation.sql" \
    "$ROOT/migrations/20260820140100_raw_block_preimage_derivation_validate.sql" \
    "$ROOT/migrations/20260820140200_raw_block_preimage_derivation_swap.sql" \
    "$ROOT/migrations/20260825041728_redo_attempt_generation_comment.sql" \
    "$ROOT/migrations/20260826120000_interpret_decode_skip_audit.sql" \
    "$ROOT/migrations/20260826120100_manifest_applied_change_count.sql" \
    "$ROOT/migrations/20260827120000_normalized_events_ens_v1_record_node_resolver_idx.sql" \
    "$ROOT/migrations/20260827130000_normalized_events_v1_after_node_scope_idx.sql" \
    "$ROOT/migrations/20260827130100_normalized_events_v1_after_child_scope_idx.sql" \
    "$ROOT/migrations/20260827130200_normalized_events_v1_before_node_scope_idx.sql" \
    "$ROOT/migrations/20260827130300_normalized_events_v1_before_child_scope_idx.sql" \
    "$ROOT/migrations/20260827130400_normalized_events_v2_subregistry_pointer_scope_idx.sql" \
    "$ROOT/migrations/20260828120000_name_current_serving_resource.sql" \
    "$ROOT/migrations/20260831120000_retire_direct_divergences_for_null_resolver.sql" \
    "$ROOT/migrations/20260831140000_discovery_watch_admissions.sql" \
    "$ROOT/migrations/20260831150000_normalized_events_v2_expiry_scope_idx.sql" \
    "$ROOT/migrations/20260902120000_normalized_events_basenames_record_node_resolver_idx.sql" \
    "$ROOT/migrations/20260902140000_project_redo_expiry_roots.sql" \
    "$ROOT/migrations/20260902150000_project_redo_expiry_resources.sql" \
    "$ROOT/migrations/20260902160000_registry_operator_account_permissions.sql" \
    "$ROOT/migrations/20260902160100_registry_operator_account_permissions_validate.sql" \
    "$ROOT/migrations/20260902160200_registry_operator_account_permissions_swap.sql" \
    "$ROOT/migrations/20260904120000_project_redo_child_registration_history.sql" \
    "$ROOT/migrations/20260906120000_exact_zero_addr60_default_derivation.sql" \
    "$ROOT/migrations/20260909120000_resolver_record_id_events.sql" \
    "$ROOT/migrations/20260909120100_resolver_record_id_events_validate.sql" \
    "$ROOT/migrations/20260909120200_resolver_record_id_events_swap.sql" \
    "$ROOT/migrations/20260911120000_normalized_events_emitter_history_idx.sql" \
    "$ROOT/migrations/20260911120100_address_records_current.sql" \
    "$ROOT/migrations/20260911120200_name_current_registration_expiry_idx.sql" \
    "$ROOT/migrations/20260913120000_unsupported_inventory_serves_no_record_values.sql" \
    "$ROOT/migrations/20260913130000_permissions_resource_restrictions.sql" \
    "$ROOT/migrations/20260913130100_account_permission_state_wrapper_operators.sql" \
    "$ROOT/migrations/20260914120000_lookup_publication_revalidation.sql" \
    "$ROOT/migrations/20260914120100_address_records_current_comments.sql" \
    "$ROOT/migrations/20260915120000_address_records_optional_authority.sql" \
    "$ROOT/migrations/20260916120000_surface_bindings_name_history_idx.sql" \
    "$ROOT/migrations/20260917120000_discovery_edges_observation_history_idx.sql" \
    "$ROOT/migrations/20260917130000_discovery_edges_reopen_idx.sql" \
    "$ROOT/migrations/20260917131000_project_scoped_history_indexes.sql" \
    "$ROOT/migrations/20260917140000_resolver_creation_self_edge.sql" \
    "$ROOT/migrations/20260917141000_discovery_self_edge_check_name.sql" \
    "$ROOT/migrations/20260917150000_normalized_events_v1_lookahead_indexes.sql" \
    "$ROOT/migrations/20260917160000_discovery_edges_index_validity_check.sql" \
    "$ROOT/migrations/20260917161000_project_scoped_history_index_validity_check.sql" \
    "$ROOT/migrations/20260918120000_normalized_events_resolver_history_idx.sql"
do
    emit_phase_migration "$migration_file" empty-schema | run_psql
done

# SQLx migrations run before phase-runner installs a fresh schema-v2 baseline.
# They must leave that namespace empty so init-schema can still accept it.
# Comment-only upgrades must tolerate that same pre-initialization path.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    emit_phase_migration "$ROOT/migrations/20260814121000_phase_heartbeat_liveness_comment.sql" empty-schema
    cat <<'SQL'
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_class relation
        JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname = current_schema()
        UNION ALL
        SELECT 1
        FROM pg_proc function
        JOIN pg_namespace namespace ON namespace.oid = function.pronamespace
        WHERE namespace.nspname = current_schema()
        UNION ALL SELECT 1 FROM pg_type type_row JOIN pg_namespace namespace ON namespace.oid = type_row.typnamespace WHERE namespace.nspname = current_schema()
    ) THEN
        RAISE EXCEPTION
            'schema-migrations created objects before fresh schema initialization';
    END IF;
END
$$;
SQL
} | run_psql
report_timing empty-schema

apply_baseline
apply_baseline
report_timing baseline-install
# The production functions intentionally bind their SECURITY DEFINER lookups
# to bigname_phase. Prove that contract before rebinding only this scratch
# schema's copies so the remainder of this isolated harness can exercise them.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
DO $$
DECLARE
    unsafe_function_count bigint;
BEGIN
    SELECT count(*)
    INTO unsafe_function_count
    FROM pg_proc procedure
    JOIN pg_namespace namespace
      ON namespace.oid = procedure.pronamespace
    WHERE namespace.nspname = current_schema()
      AND procedure.proname IN (
          'revalidate_resolution_lookup_state',
          'write_resolution_divergence'
      )
      AND procedure.proconfig @>
          ARRAY['search_path=pg_catalog, bigname_phase, pg_temp']::text[];

    IF unsafe_function_count <> 2 THEN
        RAISE EXCEPTION
            'lookup SECURITY DEFINER functions lack the fixed production search path';
    END IF;
END
$$;
SQL
    printf \
        'ALTER FUNCTION "%s".revalidate_resolution_lookup_state(text, bigint, text, jsonb, jsonb, uuid, text, text) SET search_path = pg_catalog, "%s", pg_temp;\n' \
        "$scratch_schema" "$scratch_schema"
    printf \
        'ALTER FUNCTION "%s".write_resolution_divergence(uuid, text, text, text, bigint, text, jsonb, text, text, text, text, jsonb, jsonb, boolean) SET search_path = pg_catalog, "%s", pg_temp;\n' \
        "$scratch_schema" "$scratch_schema"
} | run_psql
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
BEGIN;
INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
    manifest_version, chain_id, derivation_kind)
SELECT 'fresh-record-id-' || kind, 'schema-v2-check', kind, 'ens_v2_resolver_l1',
    1, 'schema-v2-check', 'ens_v2_resolver'
FROM unnest(ARRAY['ResolverRecordLinked', 'ResolverPermissionArgument']) AS kinds(kind);
ROLLBACK;
SQL
} | run_psql


{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
DO $$
DECLARE
    invalid_indexes text;
BEGIN
    SELECT string_agg(required.index_name, ', ' ORDER BY required.index_name)
    INTO invalid_indexes
    FROM (
        VALUES
            (
                'normalized_events_pointer_after_resolver_history_idx',
                '%chain_id%lower%after_state%resolver%block_number%block_hash%INCLUDE%normalized_event_id%',
                'ResolverChanged',
                NULL,
                false
            ),
            (
                'normalized_events_pointer_before_resolver_history_idx',
                '%chain_id%lower%before_state%resolver%block_number%block_hash%INCLUDE%normalized_event_id%',
                'ResolverChanged',
                NULL,
                false
            )
    ) AS required(
        index_name, definition_pattern, event_kind, scope_pattern, resource_required
    )
    WHERE NOT EXISTS (
        SELECT 1
        FROM pg_class index_relation
        JOIN pg_namespace namespace
          ON namespace.oid = index_relation.relnamespace
        JOIN pg_index index_state
          ON index_state.indexrelid = index_relation.oid
        WHERE namespace.nspname = current_schema()
          AND index_relation.relname = required.index_name
          AND index_state.indisvalid
          AND index_state.indisready
          AND index_state.indislive
          AND pg_get_indexdef(index_relation.oid) LIKE required.definition_pattern
          AND pg_get_expr(index_state.indpred, index_state.indrelid, true)
              LIKE format('%%event_kind%%%s%%', required.event_kind)
          AND pg_get_expr(index_state.indpred, index_state.indrelid, true)
              LIKE '%consumer_visibility%activated%'
          AND pg_get_expr(index_state.indpred, index_state.indrelid, true)
              LIKE '%canonicality_state%canonical%safe%finalized%'
          AND (
              required.scope_pattern IS NULL
              OR pg_get_expr(index_state.indpred, index_state.indrelid, true)
                  LIKE required.scope_pattern
          )
          AND (
              NOT required.resource_required
              OR pg_get_expr(index_state.indpred, index_state.indrelid, true)
                  LIKE '%resource_id%IS NOT NULL%'
          )
    );

    IF invalid_indexes IS NOT NULL THEN
        RAISE EXCEPTION
            'resolver history indexes do not match the baseline: %',
            invalid_indexes;
    END IF;
END
$$;
SQL
} | run_psql

# TestDatabase installs the current baseline before SQLx applies reviewed
# migrations. The same reviewed files must be idempotent on that baseline-first
# path, including the validation and metadata-swap steps.
for migration_file in \
    "$ROOT/migrations/20260809120000_manifest_authority_attestation_audit.sql" \
    "$ROOT/migrations/20260811120000_ens_v2_migration_slice_1.sql" \
    "$ROOT/migrations/20260811120100_ens_v2_migration_slice_1_validate.sql" \
    "$ROOT/migrations/20260811120200_ens_v2_migration_slice_1_constraints.sql" \
    "$ROOT/migrations/20260813120000_reverse_hydration_attempt_state.sql" \
    "$ROOT/migrations/20260813120000_reverse_hydration_attempt_state.sql" \
    "$ROOT/migrations/20260813120100_reverse_hydration_attempt_state_validate.sql" \
    "$ROOT/migrations/20260813120100_reverse_hydration_attempt_state_validate.sql" \
    "$ROOT/migrations/20260814120000_project_redo_resolver_evidence.sql" \
    "$ROOT/migrations/20260814130000_surface_binding_authority_arm.sql" \
    "$ROOT/migrations/20260814130000_surface_binding_authority_arm.sql" \
    "$ROOT/migrations/20260814131000_project_generation_failure_audit.sql" \
    "$ROOT/migrations/20260814131000_project_generation_failure_audit.sql" \
    "$ROOT/migrations/20260814132000_project_generation_failure_child_authority.sql" \
    "$ROOT/migrations/20260814132000_project_generation_failure_child_authority.sql" \
    "$ROOT/migrations/20260820140000_raw_block_preimage_derivation.sql" \
    "$ROOT/migrations/20260820140000_raw_block_preimage_derivation.sql" \
    "$ROOT/migrations/20260820140100_raw_block_preimage_derivation_validate.sql" \
    "$ROOT/migrations/20260820140100_raw_block_preimage_derivation_validate.sql" \
    "$ROOT/migrations/20260820140200_raw_block_preimage_derivation_swap.sql" \
    "$ROOT/migrations/20260820140200_raw_block_preimage_derivation_swap.sql" \
    "$ROOT/migrations/20260825041728_redo_attempt_generation_comment.sql" \
    "$ROOT/migrations/20260825041728_redo_attempt_generation_comment.sql" \
    "$ROOT/migrations/20260826120000_interpret_decode_skip_audit.sql" \
    "$ROOT/migrations/20260826120000_interpret_decode_skip_audit.sql" \
    "$ROOT/migrations/20260826120100_manifest_applied_change_count.sql" \
    "$ROOT/migrations/20260827120000_normalized_events_ens_v1_record_node_resolver_idx.sql" \
    "$ROOT/migrations/20260827130000_normalized_events_v1_after_node_scope_idx.sql" \
    "$ROOT/migrations/20260827130100_normalized_events_v1_after_child_scope_idx.sql" \
    "$ROOT/migrations/20260827130200_normalized_events_v1_before_node_scope_idx.sql" \
    "$ROOT/migrations/20260827130300_normalized_events_v1_before_child_scope_idx.sql" \
    "$ROOT/migrations/20260827130400_normalized_events_v2_subregistry_pointer_scope_idx.sql" \
    "$ROOT/migrations/20260828120000_name_current_serving_resource.sql" \
    "$ROOT/migrations/20260831120000_retire_direct_divergences_for_null_resolver.sql" \
    "$ROOT/migrations/20260831120000_retire_direct_divergences_for_null_resolver.sql" \
    "$ROOT/migrations/20260831140000_discovery_watch_admissions.sql" \
    "$ROOT/migrations/20260831140000_discovery_watch_admissions.sql" \
    "$ROOT/migrations/20260831150000_normalized_events_v2_expiry_scope_idx.sql" \
    "$ROOT/migrations/20260902120000_normalized_events_basenames_record_node_resolver_idx.sql" \
    "$ROOT/migrations/20260902140000_project_redo_expiry_roots.sql" \
    "$ROOT/migrations/20260902140000_project_redo_expiry_roots.sql" \
    "$ROOT/migrations/20260902150000_project_redo_expiry_resources.sql" \
    "$ROOT/migrations/20260902150000_project_redo_expiry_resources.sql" \
    "$ROOT/migrations/20260902160000_registry_operator_account_permissions.sql" \
    "$ROOT/migrations/20260902160000_registry_operator_account_permissions.sql" \
    "$ROOT/migrations/20260902160100_registry_operator_account_permissions_validate.sql" \
    "$ROOT/migrations/20260902160100_registry_operator_account_permissions_validate.sql" \
    "$ROOT/migrations/20260902160200_registry_operator_account_permissions_swap.sql" \
    "$ROOT/migrations/20260902160200_registry_operator_account_permissions_swap.sql" \
    "$ROOT/migrations/20260904120000_project_redo_child_registration_history.sql" \
    "$ROOT/migrations/20260904120000_project_redo_child_registration_history.sql" \
    "$ROOT/migrations/20260909120000_resolver_record_id_events.sql" \
    "$ROOT/migrations/20260909120000_resolver_record_id_events.sql" \
    "$ROOT/migrations/20260909120100_resolver_record_id_events_validate.sql" \
    "$ROOT/migrations/20260909120100_resolver_record_id_events_validate.sql" \
    "$ROOT/migrations/20260909120200_resolver_record_id_events_swap.sql" \
    "$ROOT/migrations/20260909120200_resolver_record_id_events_swap.sql" \
    "$ROOT/migrations/20260911120000_normalized_events_emitter_history_idx.sql" \
    "$ROOT/migrations/20260911120000_normalized_events_emitter_history_idx.sql" \
    "$ROOT/migrations/20260911120100_address_records_current.sql" \
    "$ROOT/migrations/20260911120100_address_records_current.sql" \
    "$ROOT/migrations/20260911120200_name_current_registration_expiry_idx.sql" \
    "$ROOT/migrations/20260911120200_name_current_registration_expiry_idx.sql" \
    "$ROOT/migrations/20260913120000_unsupported_inventory_serves_no_record_values.sql" \
    "$ROOT/migrations/20260913120000_unsupported_inventory_serves_no_record_values.sql" \
    "$ROOT/migrations/20260913130000_permissions_resource_restrictions.sql" \
    "$ROOT/migrations/20260913130000_permissions_resource_restrictions.sql" \
    "$ROOT/migrations/20260913130100_account_permission_state_wrapper_operators.sql" \
    "$ROOT/migrations/20260913130100_account_permission_state_wrapper_operators.sql" \
    "$ROOT/migrations/20260914120000_lookup_publication_revalidation.sql" \
    "$ROOT/migrations/20260914120000_lookup_publication_revalidation.sql" \
    "$ROOT/migrations/20260914120100_address_records_current_comments.sql" \
    "$ROOT/migrations/20260914120100_address_records_current_comments.sql"
do
    emit_phase_migration "$migration_file" baseline-first | run_psql
done
report_timing baseline-first
# Recreate the additive historical binding index from its preceding schema shape.
# Compare the resulting catalog definition to the fresh baseline, then prove reruns.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
CREATE TEMP TABLE expected_binding_history_index AS
SELECT pg_get_indexdef(indexrelid) AS definition
FROM pg_index
WHERE indexrelid = 'surface_bindings_chain_name_history_idx'::regclass;
DROP INDEX surface_bindings_chain_name_history_idx;
SQL
    emit_phase_migration "$ROOT/migrations/20260916120000_surface_bindings_name_history_idx.sql" preceding-shape
    emit_phase_migration "$ROOT/migrations/20260916120000_surface_bindings_name_history_idx.sql" baseline-first
    cat <<'SQL'
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_index, expected_binding_history_index expected
        WHERE indexrelid = 'surface_bindings_chain_name_history_idx'::regclass
          AND indisvalid AND indisready AND indpred IS NULL
          AND pg_get_indexdef(indexrelid) = expected.definition
    ) THEN
        RAISE EXCEPTION 'historical binding index upgrade differs from the baseline';
    END IF;
END $$;
SQL
} | run_psql
# The address-record table shipped before its column comments. The additive
# comment schema-migration must restore all current comments without rewriting that schema-migration.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
CREATE TEMP TABLE expected_address_record_comments AS
SELECT objsubid, description
FROM pg_description
WHERE classoid = 'pg_class'::regclass
  AND objoid = 'address_records_current'::regclass;
DO $$
DECLARE
    column_name text;
BEGIN
    COMMENT ON TABLE address_records_current IS NULL;
    FOR column_name IN
        SELECT attname FROM pg_attribute
        WHERE attrelid = 'address_records_current'::regclass
          AND attnum > 0 AND NOT attisdropped
    LOOP
        EXECUTE format('COMMENT ON COLUMN address_records_current.%I IS NULL', column_name);
    END LOOP;
END
$$;
SQL
    emit_phase_migration \
        "$ROOT/migrations/20260914120100_address_records_current_comments.sql" \
        preceding-shape
    cat <<'SQL'
DO $$
BEGIN
    IF EXISTS (
        SELECT objsubid, description FROM expected_address_record_comments
        EXCEPT
        SELECT objsubid, description FROM pg_description
        WHERE classoid = 'pg_class'::regclass
          AND objoid = 'address_records_current'::regclass
    ) THEN
        RAISE EXCEPTION 'address-record comment migration did not restore baseline comments';
    END IF;
END
$$;
DROP TABLE expected_address_record_comments;
SQL
} | run_psql
# The reverse index previously required authority identity even when a name had
# a readable serving resource. Prove that exact predecessor upgrades and that
# repeat application preserves the required record-resource identity.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
ALTER TABLE address_records_current
    ALTER COLUMN surface_binding_id SET NOT NULL,
    ALTER COLUMN resource_id SET NOT NULL,
    ALTER COLUMN binding_kind SET NOT NULL;
SQL
    emit_phase_migration "$ROOT/migrations/20260915120000_address_records_optional_authority.sql" preceding-shape
    emit_phase_migration "$ROOT/migrations/20260915120000_address_records_optional_authority.sql" baseline-first
    emit_phase_migration "$ROOT/migrations/20260915120000_address_records_optional_authority.sql" baseline-first
    cat <<'SQL'
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_attribute
        WHERE attrelid = 'address_records_current'::regclass
          AND attname IN ('surface_binding_id', 'resource_id', 'binding_kind')
          AND attnotnull
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_attribute
        WHERE attrelid = 'address_records_current'::regclass
          AND attname = 'record_resource_id' AND attnotnull
    ) THEN
        RAISE EXCEPTION 'reverse-index upgrade did not preserve optional authority and required serving identity';
    END IF;
END
$$;
SQL
} | run_psql
assert_migration_context_count "$ROOT/migrations/20260915120000_address_records_optional_authority.sql" empty-schema 1
assert_migration_context_count "$ROOT/migrations/20260915120000_address_records_optional_authority.sql" preceding-shape 1
assert_migration_context_count "$ROOT/migrations/20260915120000_address_records_optional_authority.sql" baseline-first 2
# Both discovery indexes are still the ones the fresh baseline built.
discovery_index_validity_migration="$ROOT/migrations/20260917160000_discovery_edges_index_validity_check.sql"
assert_discovery_index_definitions_accepted baseline-first
# Recreate the additive discovery observation-history index from its preceding
# schema shape. Compare the resulting catalog definition to the fresh baseline,
# then prove a rerun leaves it unchanged.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
CREATE TEMP TABLE expected_discovery_history_index AS
SELECT pg_get_indexdef(indexrelid) AS definition
FROM pg_index
WHERE indexrelid = 'discovery_edges_observation_history_idx'::regclass;
DROP INDEX discovery_edges_observation_history_idx;
SQL
    emit_phase_migration "$ROOT/migrations/20260917120000_discovery_edges_observation_history_idx.sql" preceding-shape
    emit_phase_migration "$ROOT/migrations/20260917120000_discovery_edges_observation_history_idx.sql" baseline-first
    cat <<'SQL'
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_index, expected_discovery_history_index expected
        WHERE indexrelid = 'discovery_edges_observation_history_idx'::regclass
          AND indisvalid AND indisready AND indpred IS NOT NULL
          AND pg_get_indexdef(indexrelid) = expected.definition
    ) THEN
        RAISE EXCEPTION 'discovery observation-history index upgrade differs from the baseline';
    END IF;
END $$;
DROP TABLE expected_discovery_history_index;
SQL
} | run_psql
# The observation-history index is now the one its schema-migration built.
assert_discovery_index_definitions_accepted preceding-shape
# The live prebuild in ops/discovery-history-index/install.sql must build the
# baseline definition, refuse an invalid index, and recover as its README says.
assert_concurrent_index_installer discovery-history \
    discovery_edges_observation_history_idx \
    "$ROOT/ops/discovery-history-index/install.sql" \
    ops/discovery-history-index/README.md
# Recreate the additive discovery reopen index from its preceding schema shape.
# Compare the resulting catalog definition to the fresh baseline, then prove a
# rerun leaves it unchanged.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
CREATE TEMP TABLE expected_discovery_reopen_index AS
SELECT pg_get_indexdef(indexrelid) AS definition
FROM pg_index
WHERE indexrelid = 'discovery_edges_reopen_idx'::regclass;
DROP INDEX discovery_edges_reopen_idx;
SQL
    emit_phase_migration "$ROOT/migrations/20260917130000_discovery_edges_reopen_idx.sql" preceding-shape
    emit_phase_migration "$ROOT/migrations/20260917130000_discovery_edges_reopen_idx.sql" baseline-first
    cat <<'SQL'
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_index, expected_discovery_reopen_index expected
        WHERE indexrelid = 'discovery_edges_reopen_idx'::regclass
          AND indisvalid AND indisready AND indpred IS NULL
          AND pg_get_indexdef(indexrelid) = expected.definition
    ) THEN
        RAISE EXCEPTION 'discovery reopen index upgrade differs from the baseline';
    END IF;
END $$;
DROP TABLE expected_discovery_reopen_index;
SQL
} | run_psql
# The reopen index is now the one its schema-migration built.
assert_discovery_index_definitions_accepted preceding-shape
# The live prebuild in ops/discovery-reopen-index/install.sql must build the
# baseline definition, refuse an invalid index, and recover as its README says.
assert_concurrent_index_installer discovery-reopen \
    discovery_edges_reopen_idx \
    "$ROOT/ops/discovery-reopen-index/install.sql" \
    ops/discovery-reopen-index/README.md
# The two index schema-migrations above adopt an existing index by name alone.
# Both indexes are now the ones the ops/ installers built. The validity check
# passes on that shape too and changes nothing when rerun.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    emit_phase_migration "$discovery_index_validity_migration" preceding-shape
    emit_phase_migration "$discovery_index_validity_migration" baseline-first
    cat <<'SQL'
DO $$
BEGIN
    IF (
        SELECT count(*) FROM pg_index
        WHERE indexrelid IN (
                  'discovery_edges_observation_history_idx'::regclass,
                  'discovery_edges_reopen_idx'::regclass
              )
          AND indisvalid AND indisready
    ) <> 2 THEN
        RAISE EXCEPTION 'discovery index validity check changed an index';
    END IF;
END $$;
SQL
} | run_psql
assert_migration_context_count "$discovery_index_validity_migration" preceding-shape 5
assert_migration_context_count "$discovery_index_validity_migration" baseline-first 3
# An interrupted concurrent build leaves an invalid index under the right name.
# Mark each scratch index invalid in turn and require the schema-migration to
# fail rather than record success.
for discovery_index_name in \
    discovery_edges_observation_history_idx \
    discovery_edges_reopen_idx
do
    with_index_invalidated "$discovery_index_name" discovery_edges \
        assert_migration_refusal "invalid-$discovery_index_name" \
        "$discovery_index_validity_migration" \
        "$discovery_index_name exists but is not a valid and ready index on $scratch_schema.discovery_edges; follow the recovery steps in ops/discovery-history-index/README.md or ops/discovery-reopen-index/README.md, then run the schema-migrations again" <<SQL
SQL
    # The earlier files build the index whenever the table exists, so a missing
    # index here was dropped or never installed, and nothing would rebuild it.
    assert_migration_refusal "missing-$discovery_index_name" \
        "$discovery_index_validity_migration" \
        "$discovery_index_name does not exist although $scratch_schema.discovery_edges does; build it with the matching install.sql as ops/discovery-history-index/README.md or ops/discovery-reopen-index/README.md describes, then run the schema-migrations again" <<SQL
DROP INDEX $discovery_index_name;
SQL
    # CREATE INDEX IF NOT EXISTS also skips a table or view under the name.
    assert_migration_refusal "table-named-$discovery_index_name" \
        "$discovery_index_validity_migration" \
        "$scratch_schema.$discovery_index_name is a table, not an index, so the index was never built; remove or rename that relation, build the index with the matching install.sql as ops/discovery-history-index/README.md or ops/discovery-reopen-index/README.md describes, then run the schema-migrations again" <<SQL
DROP INDEX $discovery_index_name;
CREATE TABLE $discovery_index_name ();
SQL
    assert_migration_refusal "view-named-$discovery_index_name" \
        "$discovery_index_validity_migration" \
        "$scratch_schema.$discovery_index_name is a view, not an index, so the index was never built; remove or rename that relation, build the index with the matching install.sql as ops/discovery-history-index/README.md or ops/discovery-reopen-index/README.md describes, then run the schema-migrations again" <<SQL
DROP INDEX $discovery_index_name;
CREATE VIEW $discovery_index_name AS SELECT 1 AS occupied;
SQL
done
# A wrong manual prebuild leaves a valid index under the right name that the
# intended queries cannot use. Give each index the other one's column order,
# then the right columns with the wrong predicate, and require the
# schema-migration to fail and name both definitions.
discovery_history_index_columns="chain_id, from_contract_instance_id, edge_kind, (provenance ->> 'observation_key'), active_from_block_number"
discovery_reopen_index_columns="chain_id, from_contract_instance_id, edge_kind, active_from_block_number, (provenance ->> 'observation_key')"
discovery_history_index_printed_columns="chain_id, from_contract_instance_id, edge_kind, ((provenance ->> 'observation_key'::text)), active_from_block_number"
discovery_reopen_index_printed_columns="chain_id, from_contract_instance_id, edge_kind, active_from_block_number, ((provenance ->> 'observation_key'::text))"
discovery_index_printed_predicate="WHERE (canonicality_state <> 'orphaned'::$scratch_schema.canonicality_state)"
discovery_history_index_reviewed="CREATE INDEX discovery_edges_observation_history_idx ON $scratch_schema.discovery_edges USING btree ($discovery_history_index_printed_columns) $discovery_index_printed_predicate"
discovery_reopen_index_reviewed="CREATE INDEX discovery_edges_reopen_idx ON $scratch_schema.discovery_edges USING btree ($discovery_reopen_index_printed_columns)"
discovery_index_recovery="follow the recovery steps in ops/discovery-history-index/README.md or ops/discovery-reopen-index/README.md, then run the schema-migrations again"
assert_migration_refusal wrong-column-order-discovery_edges_observation_history_idx \
    "$discovery_index_validity_migration" \
    "discovery_edges_observation_history_idx exists but does not have the reviewed definition; found \"CREATE INDEX discovery_edges_observation_history_idx ON $scratch_schema.discovery_edges USING btree ($discovery_reopen_index_printed_columns) $discovery_index_printed_predicate\", expected \"$discovery_history_index_reviewed\"; $discovery_index_recovery" <<SQL
DROP INDEX discovery_edges_observation_history_idx;
CREATE INDEX discovery_edges_observation_history_idx
    ON discovery_edges ($discovery_reopen_index_columns)
    WHERE canonicality_state <> 'orphaned';
SQL
assert_migration_refusal wrong-predicate-discovery_edges_observation_history_idx \
    "$discovery_index_validity_migration" \
    "discovery_edges_observation_history_idx exists but does not have the reviewed definition; found \"CREATE INDEX discovery_edges_observation_history_idx ON $scratch_schema.discovery_edges USING btree ($discovery_history_index_printed_columns) WHERE (deactivated_at IS NULL)\", expected \"$discovery_history_index_reviewed\"; $discovery_index_recovery" <<SQL
DROP INDEX discovery_edges_observation_history_idx;
CREATE INDEX discovery_edges_observation_history_idx
    ON discovery_edges ($discovery_history_index_columns)
    WHERE deactivated_at IS NULL;
SQL
assert_migration_refusal wrong-column-order-discovery_edges_reopen_idx \
    "$discovery_index_validity_migration" \
    "discovery_edges_reopen_idx exists but does not have the reviewed definition; found \"CREATE INDEX discovery_edges_reopen_idx ON $scratch_schema.discovery_edges USING btree ($discovery_history_index_printed_columns)\", expected \"$discovery_reopen_index_reviewed\"; $discovery_index_recovery" <<SQL
DROP INDEX discovery_edges_reopen_idx;
CREATE INDEX discovery_edges_reopen_idx
    ON discovery_edges ($discovery_history_index_columns);
SQL
assert_migration_refusal wrong-predicate-discovery_edges_reopen_idx \
    "$discovery_index_validity_migration" \
    "discovery_edges_reopen_idx exists but does not have the reviewed definition; found \"CREATE INDEX discovery_edges_reopen_idx ON $scratch_schema.discovery_edges USING btree ($discovery_reopen_index_printed_columns) $discovery_index_printed_predicate\", expected \"$discovery_reopen_index_reviewed\"; $discovery_index_recovery" <<SQL
DROP INDEX discovery_edges_reopen_idx;
CREATE INDEX discovery_edges_reopen_idx
    ON discovery_edges ($discovery_reopen_index_columns)
    WHERE canonicality_state <> 'orphaned';
SQL
# A valid, ready index on the right table that differs only by the schema name
# and a dot inside the JSON key literal indexes provenance ->>
# '<schema>.observation_key', which is NULL for every real row. A check that
# strips the schema name from the printed definition would accept it.
for discovery_index_name in \
    discovery_edges_observation_history_idx \
    discovery_edges_reopen_idx
do
    case "$discovery_index_name" in
        discovery_edges_observation_history_idx)
            discovery_index_reviewed="$discovery_history_index_reviewed" ;;
        *)
            discovery_index_reviewed="$discovery_reopen_index_reviewed" ;;
    esac
    discovery_index_found="${discovery_index_reviewed//\'observation_key\'/\'$scratch_schema.observation_key\'}"
    assert_migration_refusal "schema-name-in-literal-$discovery_index_name" \
        "$discovery_index_validity_migration" \
        "$discovery_index_name exists but does not have the reviewed definition; found \"$discovery_index_found\", expected \"$discovery_index_reviewed\"; $discovery_index_recovery" <<SQL
DROP INDEX $discovery_index_name;
$discovery_index_found;
SQL
done
# Recreate all eight additive project-scoped history indexes from their
# preceding schema shape. Compare every resulting catalog definition to the
# fresh baseline, then prove a rerun leaves them unchanged.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
CREATE TEMP TABLE expected_project_scoped_history_indexes AS
SELECT index_class.relname AS index_name,
       pg_get_indexdef(pg_index.indexrelid) AS definition
FROM pg_index
JOIN pg_class index_class ON index_class.oid = pg_index.indexrelid
WHERE pg_index.indrelid = 'normalized_events'::regclass
  AND index_class.relname IN (
      'normalized_events_project_name_node_idx',
      'normalized_events_project_name_child_idx',
      'normalized_events_project_name_after_target_idx',
      'normalized_events_project_name_before_target_idx',
      'normalized_events_project_primary_after_idx',
      'normalized_events_project_primary_before_idx',
      'normalized_events_project_primary_after_source_idx',
      'normalized_events_project_primary_before_source_idx'
  );
DO $$
BEGIN
    IF (SELECT count(*) FROM expected_project_scoped_history_indexes) <> 8 THEN
        RAISE EXCEPTION 'fresh baseline does not define all eight project-scoped history indexes';
    END IF;
END $$;
DROP INDEX
    normalized_events_project_name_node_idx,
    normalized_events_project_name_child_idx,
    normalized_events_project_name_after_target_idx,
    normalized_events_project_name_before_target_idx,
    normalized_events_project_primary_after_idx,
    normalized_events_project_primary_before_idx,
    normalized_events_project_primary_after_source_idx,
    normalized_events_project_primary_before_source_idx;
SQL
    emit_phase_migration "$ROOT/migrations/20260917131000_project_scoped_history_indexes.sql" preceding-shape
    emit_phase_migration "$ROOT/migrations/20260917131000_project_scoped_history_indexes.sql" baseline-first
    cat <<'SQL'
DO $$
BEGIN
    IF (
        SELECT count(*)
        FROM expected_project_scoped_history_indexes expected
        JOIN pg_class index_class ON index_class.relname = expected.index_name
        JOIN pg_index ON pg_index.indexrelid = index_class.oid
        WHERE pg_index.indrelid = 'normalized_events'::regclass
          AND pg_index.indisvalid AND pg_index.indisready
          AND pg_index.indpred IS NOT NULL
          AND pg_get_indexdef(pg_index.indexrelid) = expected.definition
    ) <> 8 THEN
        RAISE EXCEPTION 'project-scoped history index upgrade differs from the baseline';
    END IF;
END $$;
DROP TABLE expected_project_scoped_history_indexes;
SQL
} | run_psql
# Upgrade the discovery self-edge rule from its preceding shape: an unnamed
# CHECK that allowed only registry announcements to point at themselves.
# 20260917140000 is already applied on a live database, so it stays as it was
# and always replaces the rule. 20260917141000 starts from that exact result:
# it must leave the fresh baseline's name, definition and validity, keep exactly
# one self-edge CHECK, and replace nothing on the first or the second apply.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
CREATE FUNCTION pg_temp.self_edge_checks()
RETURNS TABLE (constraint_oid oid, conname name, definition text, convalidated boolean)
LANGUAGE sql STABLE AS $$
    SELECT oid, conname, pg_get_constraintdef(oid), convalidated
    FROM pg_constraint
    WHERE conrelid = 'discovery_edges'::regclass AND contype = 'c'
      AND pg_get_constraintdef(oid) LIKE
          '%from_contract_instance_id <> to_contract_instance_id%'
$$;
CREATE FUNCTION pg_temp.assert_self_edge_check_matches_baseline(step text)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF (SELECT count(*) FROM pg_temp.self_edge_checks()) <> 1 THEN
        RAISE EXCEPTION '%: discovery_edges does not carry exactly one self-edge CHECK', step;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_temp.self_edge_checks() observed
        JOIN expected_discovery_self_edge_check expected
          ON expected.conname = observed.conname
         AND expected.definition = observed.definition
         AND expected.convalidated = observed.convalidated
        WHERE observed.convalidated
    ) THEN
        RAISE EXCEPTION '%: discovery self-edge CHECK differs from the baseline', step;
    END IF;
END $$;
CREATE FUNCTION pg_temp.assert_self_edge_check_kept(step text)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_temp.self_edge_checks() observed
        JOIN kept_discovery_self_edge_check kept USING (constraint_oid)
    ) THEN
        RAISE EXCEPTION '%: the discovery self-edge CHECK was replaced', step;
    END IF;
END $$;
CREATE TEMP TABLE expected_discovery_self_edge_check AS
SELECT conname, definition, convalidated
FROM pg_temp.self_edge_checks()
WHERE conname = 'discovery_edges_self_edge_check';
CREATE TEMP TABLE expected_discovery_sibling_checks AS
SELECT conname, pg_get_constraintdef(oid) AS definition
FROM pg_constraint
WHERE conrelid = 'discovery_edges'::regclass AND contype = 'c'
  AND conname ~ '^discovery_edges_check[0-9]*$';
DO $$
BEGIN
    IF (SELECT count(*) FROM expected_discovery_self_edge_check) <> 1 THEN
        RAISE EXCEPTION 'fresh baseline does not name discovery_edges_self_edge_check';
    END IF;
    IF (SELECT count(*) FROM expected_discovery_sibling_checks) <> 4 THEN
        RAISE EXCEPTION 'fresh baseline does not pin discovery_edges_check1 to discovery_edges_check4';
    END IF;
END $$;
ALTER TABLE discovery_edges
    DROP CONSTRAINT discovery_edges_self_edge_check,
    ADD CHECK (
        edge_kind = 'registry_announcement'
        OR from_contract_instance_id <> to_contract_instance_id
    );
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_temp.self_edge_checks()
        WHERE conname = 'discovery_edges_check'
    ) THEN
        RAISE EXCEPTION 'preceding self-edge CHECK did not take its original generated name';
    END IF;
END $$;
SQL
    emit_phase_migration "$ROOT/migrations/20260917140000_resolver_creation_self_edge.sql" preceding-shape
    cat <<'SQL'
SELECT pg_temp.assert_self_edge_check_matches_baseline('after 20260917140000');
CREATE TEMP TABLE kept_discovery_self_edge_check AS
SELECT constraint_oid FROM pg_temp.self_edge_checks();
SQL
    emit_phase_migration "$ROOT/migrations/20260917141000_discovery_self_edge_check_name.sql" preceding-shape
    cat <<'SQL'
SELECT pg_temp.assert_self_edge_check_matches_baseline('first 20260917141000 apply');
SELECT pg_temp.assert_self_edge_check_kept('first 20260917141000 apply');
SQL
    emit_phase_migration "$ROOT/migrations/20260917141000_discovery_self_edge_check_name.sql" baseline-first
    cat <<'SQL'
SELECT pg_temp.assert_self_edge_check_matches_baseline('second 20260917141000 apply');
SELECT pg_temp.assert_self_edge_check_kept('second 20260917141000 apply');
DO $$
BEGIN
    IF EXISTS (
        (SELECT conname, pg_get_constraintdef(oid)
         FROM pg_constraint
         WHERE conrelid = 'discovery_edges'::regclass AND contype = 'c'
           AND conname ~ '^discovery_edges_check[0-9]*$'
         EXCEPT SELECT conname, definition FROM expected_discovery_sibling_checks)
        UNION ALL
        (SELECT conname, definition FROM expected_discovery_sibling_checks
         EXCEPT
         SELECT conname, pg_get_constraintdef(oid)
         FROM pg_constraint
         WHERE conrelid = 'discovery_edges'::regclass AND contype = 'c'
           AND conname ~ '^discovery_edges_check[0-9]*$')
    ) THEN
        RAISE EXCEPTION 'upgraded discovery_edges sibling CHECK names differ from the baseline';
    END IF;
END $$;
SQL
    # A caller with quote_all_identifiers on must take the same no-op path.
    # PostgreSQL then prints every identifier in pg_get_constraintdef quoted,
    # so the file finds the self-edge rule only if it turns the setting off
    # while it reads the definitions; otherwise it adds a second rule under
    # the taken name and fails. The rule must keep its name, text, validity
    # and OID, and the caller must keep its setting.
    emit_quote_all_identifiers_probe "$ROOT/migrations/20260917141000_discovery_self_edge_check_name.sql" in-transaction
    cat <<'SQL'
SELECT pg_temp.assert_self_edge_check_matches_baseline('quoted-identifier no-op apply');
SELECT pg_temp.assert_self_edge_check_kept('quoted-identifier no-op apply');

-- A baseline installed after 20260917140000 ran as a no-op could hold the
-- wanted rule under its generated name. That is a rename, not a replacement.
ALTER TABLE discovery_edges
    RENAME CONSTRAINT discovery_edges_self_edge_check TO discovery_edges_check;
SQL
    emit_phase_migration "$ROOT/migrations/20260917141000_discovery_self_edge_check_name.sql" specialized
    cat <<'SQL'
SELECT pg_temp.assert_self_edge_check_matches_baseline('generated-name apply');
SELECT pg_temp.assert_self_edge_check_kept('generated-name apply');

-- The rename path must find the generated-name rule under quoting too.
ALTER TABLE discovery_edges
    RENAME CONSTRAINT discovery_edges_self_edge_check TO discovery_edges_check;
SET quote_all_identifiers = on;
SQL
    render_phase_migration "$ROOT/migrations/20260917141000_discovery_self_edge_check_name.sql"
    assert_quote_all_identifiers_sql on
    cat <<'SQL'
RESET quote_all_identifiers;
SELECT pg_temp.assert_self_edge_check_matches_baseline('quoted-identifier generated-name apply');
SELECT pg_temp.assert_self_edge_check_kept('quoted-identifier generated-name apply');

-- A rule with different text, next to a stray second one, is replaced.
ALTER TABLE discovery_edges
    DROP CONSTRAINT discovery_edges_self_edge_check,
    ADD CHECK (
        edge_kind = 'registry_announcement'
        OR from_contract_instance_id <> to_contract_instance_id
    ),
    ADD CONSTRAINT discovery_edges_stray_self_edge_check CHECK (
        edge_kind <> 'proxy_implementation'
        OR from_contract_instance_id <> to_contract_instance_id
    );
SQL
    emit_phase_migration "$ROOT/migrations/20260917141000_discovery_self_edge_check_name.sql" specialized
    cat <<'SQL'
SELECT pg_temp.assert_self_edge_check_matches_baseline('different-text apply');
DROP TABLE expected_discovery_self_edge_check;
DROP TABLE expected_discovery_sibling_checks;
DROP TABLE kept_discovery_self_edge_check;
SQL
} | run_psql
# 20260917131000 adopts an existing relation by name alone, so the later
# 20260917161000_project_scoped_history_index_validity_check.sql must accept
# the eight indexes that file just rebuilt. sqlx runs schema-migrations without
# the phase schema on search_path, which makes PostgreSQL print the enum type
# in each predicate with its schema name unless the check controls search_path
# itself, so prove both session settings. The check must also leave the
# search_path as it found it, in the session and inside one transaction.
project_history_validity_migration="$ROOT/migrations/20260917161000_project_scoped_history_index_validity_check.sql"
project_history_install="$ROOT/ops/project-scoped-history/install.sql"
project_history_readme=ops/project-scoped-history/README.md
project_history_index_names=(
    normalized_events_project_name_node_idx
    normalized_events_project_name_child_idx
    normalized_events_project_name_after_target_idx
    normalized_events_project_name_before_target_idx
    normalized_events_project_primary_after_idx
    normalized_events_project_primary_before_idx
    normalized_events_project_primary_after_source_idx
    normalized_events_project_primary_before_source_idx
)
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    emit_phase_migration "$project_history_validity_migration" baseline-first
    assert_search_path_sql "$scratch_schema"
    printf 'SET search_path TO public;\n'
    emit_phase_migration "$project_history_validity_migration" baseline-first
    assert_search_path_sql public
    printf 'BEGIN;\nSET LOCAL search_path TO "%s", public;\n' "$scratch_schema"
    render_phase_migration "$project_history_validity_migration"
    assert_search_path_sql "$scratch_schema, public"
    printf 'COMMIT;\n'
    assert_search_path_sql public
    emit_quote_all_identifiers_probe "$project_history_validity_migration" in-transaction
} | run_psql
# The live prebuild in ops/project-scoped-history/install.sql builds all eight
# indexes in one file. For each in turn it must build the baseline definition,
# refuse an invalid index, a valid index with other keys, a valid index whose
# JSON key literals start with the schema name, and a table under the name,
# and recover as its README says.
for project_history_index_name in "${project_history_index_names[@]}"; do
    assert_concurrent_index_installer "project-history-$project_history_index_name" \
        "$project_history_index_name" \
        "$project_history_install" \
        "$project_history_readme" \
        normalized_events \
        "block_number, chain_id"
done
# Require the installer's first HINT line to be exactly this text. The installer
# names the DROP INDEX CONCURRENTLY recovery there, beside the refusal itself.
assert_index_install_hint() {
    local label="$1"
    local install_file="$2"
    local exact_hint="$3"
    local observed_hint
    observed_hint="$(
        { render_phase_migration "$install_file" | run_psql 2>&1 >/dev/null || true; } \
            | sed -n 's/^HINT:[[:space:]]*//p' \
            | sed -n '1p'
    )"
    if [ "$observed_hint" != "$exact_hint" ]; then
        printf '%s\n' \
            "$label: expected PostgreSQL hint: $exact_hint" \
            "$label: observed PostgreSQL hint: $observed_hint" >&2
        exit 1
    fi
    refusal_assertions_passed=$((refusal_assertions_passed + 1))
}
# The installer refuses before it builds anything. With the last index invalid
# and the first one absent, it must stop on the invalid one, tell the operator
# how to drop it, and leave the first one unbuilt.
project_history_first_index="${project_history_index_names[0]}"
project_history_last_index="${project_history_index_names[7]}"
printf 'SET search_path TO "%s";\nDROP INDEX %s;\n' "$scratch_schema" "$project_history_first_index" | run_psql >/dev/null
build_invalid_index "$project_history_last_index" normalized_events
assert_index_install_refusal project-history-refuses-before-building \
    "$project_history_install" \
    "$project_history_last_index is missing from $scratch_schema.normalized_events or is not valid and ready; follow the recovery steps in $project_history_readme before retrying"
assert_index_install_hint project-history-invalid-index-hint \
    "$project_history_install" \
    "An interrupted concurrent build leaves an invalid index. Confirm in pg_stat_progress_create_index that no build is still running, run DROP INDEX CONCURRENTLY $scratch_schema.$project_history_last_index, then rerun this script."
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<SQL
DO \$\$
BEGIN
    IF to_regclass('$project_history_first_index') IS NOT NULL THEN
        RAISE EXCEPTION 'project-scoped history installer built an index before refusing an invalid one';
    END IF;
END \$\$;
DROP INDEX CONCURRENTLY $project_history_last_index;
SQL
    render_phase_migration "$project_history_install"
    # An index on another table under the name is not the index either.
    printf '%s\n' \
        "DROP INDEX $project_history_first_index;" \
        "CREATE INDEX $project_history_first_index ON discovery_edges (chain_id);"
} | run_psql >/dev/null
assert_index_install_refusal project-history-index-on-another-table \
    "$project_history_install" \
    "$project_history_first_index is missing from $scratch_schema.normalized_events or is not valid and ready; follow the recovery steps in $project_history_readme before retrying"
assert_index_install_hint project-history-index-on-another-table-hint \
    "$project_history_install" \
    "An index on $scratch_schema.discovery_edges holds this name. Rename or remove it, then rerun this script."
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' "DROP INDEX $project_history_first_index;"
    render_phase_migration "$project_history_install"
} | run_psql >/dev/null
# All eight indexes are now the ones the installer built. The validity check
# passes on that shape under both search_path settings and changes nothing.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    emit_phase_migration "$project_history_validity_migration" preceding-shape
    printf 'SET search_path TO public;\n'
    emit_phase_migration "$project_history_validity_migration" baseline-first
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
DO $$
BEGIN
    IF (
        SELECT count(*)
        FROM pg_index
        JOIN pg_class index_class ON index_class.oid = pg_index.indexrelid
        WHERE pg_index.indrelid = 'normalized_events'::regclass
          AND index_class.relname LIKE 'normalized\_events\_project\_%\_idx'
          AND pg_index.indisvalid AND pg_index.indisready
    ) <> 8 THEN
        RAISE EXCEPTION 'project-scoped history index validity check changed an index';
    END IF;
END $$;
SQL
} | run_psql
assert_migration_context_count "$project_history_validity_migration" empty-schema 1
assert_migration_context_count "$project_history_validity_migration" preceding-shape 1
assert_migration_context_count "$project_history_validity_migration" baseline-first 3
# Put each index in turn into every shape CREATE INDEX IF NOT EXISTS skips,
# inside a transaction that rolls back, and require the schema-migration to
# fail rather than record success. The expected definition it names must be how
# the fresh-baseline index prints.
project_history_recovery="follow the recovery steps in $project_history_readme, then run the schema-migrations again"
project_history_rebuild="build the index with ops/project-scoped-history/install.sql as $project_history_readme describes, then run the schema-migrations again"
for project_history_index_name in "${project_history_index_names[@]}"; do
    with_index_invalidated "$project_history_index_name" normalized_events \
        assert_migration_refusal "invalid-$project_history_index_name" \
        "$project_history_validity_migration" \
        "$project_history_index_name exists but is not a valid and ready index on $scratch_schema.normalized_events; $project_history_recovery" <<SQL
SQL
    # The earlier file builds the index whenever the table exists, so a missing
    # index here was dropped or never installed, and nothing would rebuild it.
    assert_migration_refusal "missing-$project_history_index_name" \
        "$project_history_validity_migration" \
        "$project_history_index_name does not exist although $scratch_schema.normalized_events does; build it with ops/project-scoped-history/install.sql as $project_history_readme describes, then run the schema-migrations again" <<SQL
DROP INDEX $project_history_index_name;
SQL
    assert_migration_refusal "table-named-$project_history_index_name" \
        "$project_history_validity_migration" \
        "$scratch_schema.$project_history_index_name is a table, not an index, so the index was never built; remove or rename that relation, $project_history_rebuild" <<SQL
DROP INDEX $project_history_index_name;
CREATE TABLE $project_history_index_name ();
SQL
    assert_migration_refusal "view-named-$project_history_index_name" \
        "$project_history_validity_migration" \
        "$scratch_schema.$project_history_index_name is a view, not an index, so the index was never built; remove or rename that relation, $project_history_rebuild" <<SQL
DROP INDEX $project_history_index_name;
CREATE VIEW $project_history_index_name AS SELECT 1 AS occupied;
SQL
    assert_migration_refusal "other-table-$project_history_index_name" \
        "$project_history_validity_migration" \
        "$project_history_index_name exists but is not a valid and ready index on $scratch_schema.normalized_events; $project_history_recovery" <<SQL
DROP INDEX $project_history_index_name;
CREATE INDEX $project_history_index_name ON discovery_edges (chain_id);
SQL
    project_history_reviewed_definition="$(
        {
            printf 'SET search_path TO "%s";\n' "$scratch_schema"
            printf '%s\n' \
                '\pset tuples_only on' \
                '\pset format unaligned' \
                "SET search_path TO pg_catalog;" \
                "SELECT pg_get_indexdef('$scratch_schema.$project_history_index_name'::regclass);"
        } | run_psql
    )"
    assert_migration_refusal "wrong-keys-$project_history_index_name" \
        "$project_history_validity_migration" \
        "$project_history_index_name exists but does not have the reviewed definition; found \"CREATE INDEX $project_history_index_name ON $scratch_schema.normalized_events USING btree (block_number, chain_id)\", expected \"$project_history_reviewed_definition\"; $project_history_recovery" <<SQL
DROP INDEX $project_history_index_name;
CREATE INDEX $project_history_index_name ON normalized_events (block_number, chain_id);
SQL
    # The right keys are not enough: the included column and the predicate are
    # part of the reviewed definition too.
    project_history_found_definition="${project_history_reviewed_definition/ INCLUDE (normalized_event_id)/}"
    assert_migration_refusal "no-included-column-$project_history_index_name" \
        "$project_history_validity_migration" \
        "$project_history_index_name exists but does not have the reviewed definition; found \"$project_history_found_definition\", expected \"$project_history_reviewed_definition\"; $project_history_recovery" <<SQL
DROP INDEX $project_history_index_name;
$project_history_found_definition;
SQL
    project_history_found_definition="${project_history_reviewed_definition/\'safe\'::$scratch_schema.canonicality_state, /}"
    assert_migration_refusal "wrong-predicate-$project_history_index_name" \
        "$project_history_validity_migration" \
        "$project_history_index_name exists but does not have the reviewed definition; found \"$project_history_found_definition\", expected \"$project_history_reviewed_definition\"; $project_history_recovery" <<SQL
DROP INDEX $project_history_index_name;
$project_history_found_definition;
SQL
    # Nor is a printed definition that matches once the schema name is removed:
    # with the schema name and a dot at the start of each JSON key literal, in
    # the keys and in the predicate, the index is valid, ready, and on the right
    # table, but it indexes after_state ->> '<schema>.node' and the like, which
    # is NULL for every real row.
    project_history_found_definition="${project_history_reviewed_definition//->> \'/->> \'$scratch_schema.}"
    if [ "$project_history_found_definition" = "$project_history_reviewed_definition" ]; then
        printf '%s\n' "$project_history_index_name: reviewed definition has no JSON key literal to alter" >&2
        exit 1
    fi
    assert_migration_refusal "schema-name-in-literal-$project_history_index_name" \
        "$project_history_validity_migration" \
        "$project_history_index_name exists but does not have the reviewed definition; found \"$project_history_found_definition\", expected \"$project_history_reviewed_definition\"; $project_history_recovery" <<SQL
DROP INDEX $project_history_index_name;
$project_history_found_definition;
SQL
done
# Recreate both ENSv1 lookahead loader indexes from their preceding schema
# shape. Compare each resulting catalog definition to the fresh baseline, then
# prove a rerun leaves them unchanged.
v1_lookahead_migration="$ROOT/migrations/20260917150000_normalized_events_v1_lookahead_indexes.sql"
v1_lookahead_install="$ROOT/ops/v1-lookahead-indexes/install.sql"
v1_lookahead_readme=ops/v1-lookahead-indexes/README.md
v1_lookahead_index_names=(
    normalized_events_v1_due_probe_idx
    normalized_events_v1_direct_node_probe_idx
)
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
CREATE TEMP TABLE expected_v1_lookahead_indexes AS
SELECT index_class.relname AS index_name,
       pg_get_indexdef(pg_index.indexrelid) AS definition
FROM pg_index
JOIN pg_class index_class ON index_class.oid = pg_index.indexrelid
WHERE pg_index.indrelid = 'normalized_events'::regclass
  AND index_class.relname IN (
      'normalized_events_v1_due_probe_idx',
      'normalized_events_v1_direct_node_probe_idx'
  );
DO $$
BEGIN
    IF (SELECT count(*) FROM expected_v1_lookahead_indexes) <> 2 THEN
        RAISE EXCEPTION 'fresh baseline does not define both ENSv1 lookahead indexes';
    END IF;
END $$;
DROP INDEX
    normalized_events_v1_due_probe_idx,
    normalized_events_v1_direct_node_probe_idx;
SQL
    emit_phase_migration "$v1_lookahead_migration" preceding-shape
    emit_phase_migration "$v1_lookahead_migration" baseline-first
    cat <<'SQL'
DO $$
BEGIN
    IF (
        SELECT count(*)
        FROM expected_v1_lookahead_indexes expected
        JOIN pg_class index_class ON index_class.relname = expected.index_name
        JOIN pg_index ON pg_index.indexrelid = index_class.oid
        WHERE pg_index.indrelid = 'normalized_events'::regclass
          AND pg_index.indisvalid AND pg_index.indisready
          AND pg_index.indpred IS NOT NULL
          AND pg_get_indexdef(pg_index.indexrelid) = expected.definition
    ) <> 2 THEN
        RAISE EXCEPTION 'ENSv1 lookahead index upgrade differs from the baseline';
    END IF;
END $$;
DROP TABLE expected_v1_lookahead_indexes;
SQL
} | run_psql
# The schema-migration adopts an existing relation by name alone, so its check
# must accept the two indexes it just rebuilt. sqlx runs schema-migrations
# without the phase schema on search_path, which makes PostgreSQL print the
# enum type in each predicate with its schema name unless the check controls
# search_path itself, so prove both session settings. The check must also
# leave the search_path as it found it, in the session and inside one
# transaction, and give a caller with quote_all_identifiers on the same answer
# while keeping that setting.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    emit_phase_migration "$v1_lookahead_migration" baseline-first
    assert_search_path_sql "$scratch_schema"
    printf 'SET search_path TO public;\n'
    emit_phase_migration "$v1_lookahead_migration" baseline-first
    assert_search_path_sql public
    printf 'BEGIN;\nSET LOCAL search_path TO "%s", public;\n' "$scratch_schema"
    render_phase_migration "$v1_lookahead_migration"
    assert_search_path_sql "$scratch_schema, public"
    printf 'COMMIT;\n'
    assert_search_path_sql public
    emit_quote_all_identifiers_probe "$v1_lookahead_migration" in-transaction
} | run_psql
assert_migration_context_count "$v1_lookahead_migration" empty-schema 1
assert_migration_context_count "$v1_lookahead_migration" preceding-shape 1
assert_migration_context_count "$v1_lookahead_migration" baseline-first 3
# The live prebuild in ops/v1-lookahead-indexes/install.sql builds both indexes
# in one file. For each in turn it must build the baseline definition, refuse
# an invalid index, a valid index with other keys, a valid index whose JSON
# key literals start with the schema name, and a table under the name, and
# recover as its README says.
for v1_lookahead_index_name in "${v1_lookahead_index_names[@]}"; do
    assert_concurrent_index_installer "v1-lookahead-$v1_lookahead_index_name" \
        "$v1_lookahead_index_name" \
        "$v1_lookahead_install" \
        "$v1_lookahead_readme" \
        normalized_events \
        "block_number, chain_id"
done
# The installer refuses before it builds anything. With the second index
# invalid and the first one absent, it must stop on the invalid one, tell the
# operator how to drop it, and leave the first one unbuilt.
v1_lookahead_first_index="${v1_lookahead_index_names[0]}"
v1_lookahead_last_index="${v1_lookahead_index_names[1]}"
printf 'SET search_path TO "%s";\nDROP INDEX %s;\n' "$scratch_schema" "$v1_lookahead_first_index" | run_psql >/dev/null
build_invalid_index "$v1_lookahead_last_index" normalized_events
assert_index_install_refusal v1-lookahead-refuses-before-building \
    "$v1_lookahead_install" \
    "$v1_lookahead_last_index is missing from $scratch_schema.normalized_events or is not valid and ready; follow the recovery steps in $v1_lookahead_readme before retrying"
assert_index_install_hint v1-lookahead-invalid-index-hint \
    "$v1_lookahead_install" \
    "An interrupted concurrent build leaves an invalid index. Confirm in pg_stat_progress_create_index that no build is still running, run DROP INDEX CONCURRENTLY $scratch_schema.$v1_lookahead_last_index, then rerun this script."
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<SQL
DO \$\$
BEGIN
    IF to_regclass('$v1_lookahead_first_index') IS NOT NULL THEN
        RAISE EXCEPTION 'ENSv1 lookahead installer built an index before refusing an invalid one';
    END IF;
END \$\$;
DROP INDEX CONCURRENTLY $v1_lookahead_last_index;
SQL
    render_phase_migration "$v1_lookahead_install"
    # An index on another table under the name is not the index either.
    printf '%s\n' \
        "DROP INDEX $v1_lookahead_first_index;" \
        "CREATE INDEX $v1_lookahead_first_index ON discovery_edges (chain_id);"
} | run_psql >/dev/null
assert_index_install_refusal v1-lookahead-index-on-another-table \
    "$v1_lookahead_install" \
    "$v1_lookahead_first_index is missing from $scratch_schema.normalized_events or is not valid and ready; follow the recovery steps in $v1_lookahead_readme before retrying"
assert_index_install_hint v1-lookahead-index-on-another-table-hint \
    "$v1_lookahead_install" \
    "An index on $scratch_schema.discovery_edges holds this name. Rename or remove it, then rerun this script."
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' "DROP INDEX $v1_lookahead_first_index;"
    render_phase_migration "$v1_lookahead_install"
} | run_psql >/dev/null
# Put each index in turn into every shape CREATE INDEX IF NOT EXISTS skips,
# inside a transaction that rolls back, and require the schema-migration to
# fail rather than record success. The expected definition it names must be how
# the fresh-baseline index prints, read under search_path pg_catalog.
v1_lookahead_recovery="follow the recovery steps in $v1_lookahead_readme, then run the schema-migrations again"
for v1_lookahead_index_name in "${v1_lookahead_index_names[@]}"; do
    with_index_invalidated "$v1_lookahead_index_name" normalized_events \
        assert_migration_refusal "invalid-$v1_lookahead_index_name" \
        "$v1_lookahead_migration" \
        "$v1_lookahead_index_name exists but is not a valid and ready index on $scratch_schema.normalized_events; $v1_lookahead_recovery" <<SQL
SQL
    # CREATE INDEX IF NOT EXISTS also skips a table or view under the name.
    assert_migration_refusal "table-named-$v1_lookahead_index_name" \
        "$v1_lookahead_migration" \
        "$scratch_schema.$v1_lookahead_index_name is a table, not an index, so the index was never built; remove or rename that relation, then run the schema-migrations again" <<SQL
DROP INDEX $v1_lookahead_index_name;
CREATE TABLE $v1_lookahead_index_name ();
SQL
    assert_migration_refusal "view-named-$v1_lookahead_index_name" \
        "$v1_lookahead_migration" \
        "$scratch_schema.$v1_lookahead_index_name is a view, not an index, so the index was never built; remove or rename that relation, then run the schema-migrations again" <<SQL
DROP INDEX $v1_lookahead_index_name;
CREATE VIEW $v1_lookahead_index_name AS SELECT 1 AS occupied;
SQL
    assert_migration_refusal "other-table-$v1_lookahead_index_name" \
        "$v1_lookahead_migration" \
        "$v1_lookahead_index_name exists but is not a valid and ready index on $scratch_schema.normalized_events; $v1_lookahead_recovery" <<SQL
DROP INDEX $v1_lookahead_index_name;
CREATE INDEX $v1_lookahead_index_name ON discovery_edges (chain_id);
SQL
    # A wrong manual prebuild leaves a valid index with other keys under the
    # name. The expected text is how the fresh-baseline index prints with
    # search_path set to pg_catalog: the table and the enum type both carry the
    # schema name, and the due-probe CASE expression keeps its several lines.
    v1_lookahead_reviewed_definition="$(
        {
            printf 'SET search_path TO "%s";\n' "$scratch_schema"
            printf '%s\n' \
                '\pset tuples_only on' \
                '\pset format unaligned' \
                "SET search_path TO pg_catalog;" \
                "SELECT pg_get_indexdef('$scratch_schema.$v1_lookahead_index_name'::regclass);"
        } | run_psql
    )"
    assert_migration_refusal "wrong-keys-$v1_lookahead_index_name" \
        "$v1_lookahead_migration" \
        "$v1_lookahead_index_name exists but does not have the reviewed definition; found \"CREATE INDEX $v1_lookahead_index_name ON $scratch_schema.normalized_events USING btree (block_number, chain_id)\", expected \"$v1_lookahead_reviewed_definition\"; $v1_lookahead_recovery" <<SQL
DROP INDEX $v1_lookahead_index_name;
CREATE INDEX $v1_lookahead_index_name ON normalized_events (block_number, chain_id);
SQL
    # The right keys are not enough: the predicate is part of the reviewed
    # definition too.
    v1_lookahead_found_definition="${v1_lookahead_reviewed_definition/\'safe\'::$scratch_schema.canonicality_state, /}"
    if [ "$v1_lookahead_found_definition" = "$v1_lookahead_reviewed_definition" ]; then
        printf '%s\n' "$v1_lookahead_index_name: reviewed definition has no safe canonicality state to remove" >&2
        exit 1
    fi
    assert_migration_refusal "wrong-predicate-$v1_lookahead_index_name" \
        "$v1_lookahead_migration" \
        "$v1_lookahead_index_name exists but does not have the reviewed definition; found \"$v1_lookahead_found_definition\", expected \"$v1_lookahead_reviewed_definition\"; $v1_lookahead_recovery" <<SQL
DROP INDEX $v1_lookahead_index_name;
$v1_lookahead_found_definition;
SQL
    # Nor is a printed definition that matches once the schema name is removed:
    # with the schema name and a dot at the start of each JSON key literal, the
    # index is valid, ready, and on the right table, but it indexes
    # after_state ->> '<schema>.expiry' and the like, which is NULL for every
    # real row.
    v1_lookahead_found_definition="${v1_lookahead_reviewed_definition//->> \'/->> \'$scratch_schema.}"
    if [ "$v1_lookahead_found_definition" = "$v1_lookahead_reviewed_definition" ]; then
        printf '%s\n' "$v1_lookahead_index_name: reviewed definition has no JSON key literal to alter" >&2
        exit 1
    fi
    assert_migration_refusal "schema-name-in-literal-$v1_lookahead_index_name" \
        "$v1_lookahead_migration" \
        "$v1_lookahead_index_name exists but does not have the reviewed definition; found \"$v1_lookahead_found_definition\", expected \"$v1_lookahead_reviewed_definition\"; $v1_lookahead_recovery" <<SQL
DROP INDEX $v1_lookahead_index_name;
$v1_lookahead_found_definition;
SQL
done
# Exercise reverse_hydration_attempt_state_upgrade from the exact predecessor
# shape, then validate the additive tuple invariant independently. Both files
# must remain idempotent after the upgrade completes.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
ALTER TABLE primary_names_current
    DROP CONSTRAINT primary_names_current_reverse_hydration_attempt_check,
    DROP COLUMN reverse_hydration_attempted_block_number,
    DROP COLUMN reverse_hydration_attempted_block_hash,
    DROP COLUMN reverse_hydration_attempt_ordinal;
DROP SEQUENCE reverse_hydration_attempt_ordinal_seq;
SQL
    emit_phase_migration \
        "$ROOT/migrations/20260813120000_reverse_hydration_attempt_state.sql" \
        preceding-shape
    cat <<'SQL'
DO $$
BEGIN
    IF to_regclass(
        current_schema() || '.reverse_hydration_attempt_ordinal_seq'
    ) IS NULL THEN
        RAISE EXCEPTION
            'reverse_hydration_attempt_state_upgrade did not restore the sequence';
    END IF;
    IF (
        SELECT count(*)
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'primary_names_current'
          AND (
              (column_name = 'reverse_hydration_attempted_block_number'
               AND data_type = 'bigint' AND is_nullable = 'YES')
              OR (column_name = 'reverse_hydration_attempted_block_hash'
                  AND data_type = 'text' AND is_nullable = 'YES')
              OR (column_name = 'reverse_hydration_attempt_ordinal'
                  AND data_type = 'bigint' AND is_nullable = 'YES')
          )
    ) <> 3 THEN
        RAISE EXCEPTION
            'reverse_hydration_attempt_state_upgrade did not restore all three columns';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'primary_names_current'::regclass
          AND conname = 'primary_names_current_reverse_hydration_attempt_check'
          AND NOT convalidated
          AND pg_get_expr(conbin, conrelid) =
              '(((reverse_hydration_attempted_block_number IS NULL) AND (reverse_hydration_attempted_block_hash IS NULL) AND (reverse_hydration_attempt_ordinal IS NULL)) OR ((reverse_hydration_attempted_block_number IS NOT NULL) AND (reverse_hydration_attempted_block_number >= 0) AND (reverse_hydration_attempted_block_hash IS NOT NULL) AND (btrim(reverse_hydration_attempted_block_hash) <> ''''::text) AND (reverse_hydration_attempt_ordinal IS NOT NULL) AND (reverse_hydration_attempt_ordinal > 0)))'
    ) THEN
        RAISE EXCEPTION
            'reverse_hydration_attempt_state_upgrade constraint is missing, already validated, or has the wrong tuple predicate';
    END IF;
    IF col_description(
        'primary_names_current'::regclass,
        (SELECT attnum FROM pg_attribute
         WHERE attrelid = 'primary_names_current'::regclass
           AND attname = 'reverse_hydration_attempted_block_number'
           AND NOT attisdropped)
    ) IS DISTINCT FROM 'This internal reverse-name polling selection value identifies the head height of the latest attempt. Readers never use it as serving data.'
       OR col_description(
        'primary_names_current'::regclass,
        (SELECT attnum FROM pg_attribute
         WHERE attrelid = 'primary_names_current'::regclass
           AND attname = 'reverse_hydration_attempted_block_hash'
           AND NOT attisdropped)
    ) IS DISTINCT FROM 'This internal reverse-name polling selection value identifies the head hash of the latest attempt. Readers never use it as serving data.'
       OR col_description(
        'primary_names_current'::regclass,
        (SELECT attnum FROM pg_attribute
         WHERE attrelid = 'primary_names_current'::regclass
           AND attname = 'reverse_hydration_attempt_ordinal'
           AND NOT attisdropped)
    ) IS DISTINCT FROM 'This internal value orders reverse-name polling attempts for fair rolling selection. It never records or validates a provider result.'
       OR obj_description(
        'reverse_hydration_attempt_ordinal_seq'::regclass, 'pg_class'
    ) IS DISTINCT FROM 'This sequence assigns durable order to reverse-name polling batches; its values are not serving data.'
    THEN
        RAISE EXCEPTION
            'reverse_hydration_attempt_state_upgrade is missing an expected primary_names_current column or reverse_hydration_attempt_ordinal_seq comment';
    END IF;
END
$$;
SQL
} | run_psql
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    emit_phase_migration \
        "$ROOT/migrations/20260813120100_reverse_hydration_attempt_state_validate.sql" \
        preceding-shape
    cat <<'SQL'
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'primary_names_current'::regclass
          AND conname = 'primary_names_current_reverse_hydration_attempt_check'
          AND convalidated
    ) THEN
        RAISE EXCEPTION
            'reverse_hydration_attempt_state_upgrade constraint was not validated';
    END IF;
END
$$;
SQL
} | run_psql
emit_phase_migration \
    "$ROOT/migrations/20260813120000_reverse_hydration_attempt_state.sql" \
    preceding-shape | run_psql
emit_phase_migration \
    "$ROOT/migrations/20260813120100_reverse_hydration_attempt_state_validate.sql" \
    preceding-shape | run_psql
for migration_file in \
    "$ROOT/migrations/20260813120000_reverse_hydration_attempt_state.sql" \
    "$ROOT/migrations/20260813120100_reverse_hydration_attempt_state_validate.sql"
do
    assert_migration_context_count "$migration_file" empty-schema 1
    assert_migration_context_count "$migration_file" baseline-first 2
    assert_migration_context_count "$migration_file" preceding-shape 2
done
report_timing reverse-hydration
# Exercise the service-loop upgrade's exact predecessor constraint discovery.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    render_phase_migration \
        "$ROOT/migrations/20260721130000_service_loop_heartbeats.sql"
} | run_psql
assert_migration_refusal \
    service-loop-scope-constraint \
    "$ROOT/migrations/20260721140000_service_loop_phase_heartbeats.sql" \
    'service_loop_heartbeats scope constraint was not found' <<'SQL'
DO $$
DECLARE
    scope_constraint text;
BEGIN
    SELECT constraint_row.conname
    INTO scope_constraint
    FROM pg_constraint AS constraint_row
    WHERE constraint_row.conrelid = 'service_loop_heartbeats'::regclass
      AND constraint_row.contype = 'c'
      AND pg_get_constraintdef(constraint_row.oid) LIKE '%scope_kind%'
    ORDER BY constraint_row.conname
    LIMIT 1;
    EXECUTE format(
        'ALTER TABLE service_loop_heartbeats DROP CONSTRAINT %I',
        scope_constraint
    );
END
$$;
SQL
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' 'DROP TABLE service_loop_heartbeats;'
} | run_psql

# Exercise the authority-arm upgrade from its exact preceding empty binding
# shape. Applying the reviewed file twice must retain one required column, its
# closed check, and the chain/name/arm exclusion domain.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
ALTER TABLE surface_bindings
    DROP CONSTRAINT surface_bindings_no_overlap,
    DROP CONSTRAINT surface_bindings_authority_arm_check,
    DROP COLUMN authority_arm;
ALTER TABLE surface_bindings
    ADD CONSTRAINT surface_bindings_no_overlap
    EXCLUDE USING gist (
        logical_name_id WITH =,
        tstzrange(active_from, COALESCE(active_to, 'infinity'::timestamptz), '[)') WITH &&
    )
    WHERE (canonicality_state IN ('canonical', 'safe', 'finalized'));

INSERT INTO manifest_versions (
    manifest_version, namespace, source_family, chain_id, deployment_label,
    rollout_status, normalizer_version, file_path, manifest_payload
) VALUES (
    1, 'schema-v2-check', 'authority-reset-sentinel', 'authority-reset',
    'authority-reset', 'draft', 'test', 'authority-reset-sentinel.toml', '{}'::jsonb
);
INSERT INTO chain_lineage (
    chain_id, block_hash, block_number, block_timestamp, canonicality_state
) VALUES (
    'authority-reset', '0x01', 1, to_timestamp(1), 'canonical'
);
INSERT INTO name_surfaces (
    logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
    namehash, labelhashes, normalizer_version, visibility_state,
    chain_id, block_hash, block_number, canonicality_state
) VALUES (
    'schema-v2-check:0xreset', 'schema-v2-check', 'reset', ARRAY['reset'],
    '\x'::bytea, '0xreset', ARRAY['0xreset'], 'test', 'active',
    'authority-reset', '0x01', 1, 'canonical'
);
INSERT INTO resources (
    resource_id, chain_id, block_hash, block_number, canonicality_state
) VALUES (
    '00000000-0000-0000-0000-000000000001',
    'authority-reset', '0x01', 1, 'canonical'
);
INSERT INTO normalized_events (
    event_identity, namespace, event_kind, source_family, manifest_version,
    chain_id, derivation_kind, canonicality_state
) VALUES (
    'authority-reset-normalized-sentinel', 'schema-v2-check',
    'SourceManifestUpdated', 'authority-reset-sentinel', 1,
    'authority-reset', 'manifest_sync', 'canonical'
);
SQL
} | run_psql
assert_migration_refusal \
    authority-arm-offline-reset \
    "$ROOT/migrations/20260814130000_surface_binding_authority_arm.sql" \
    'surface binding authority-arm upgrade requires the reviewed offline binding reset before migration apply' <<'SQL'
INSERT INTO surface_bindings (
    surface_binding_id, logical_name_id, resource_id, binding_kind,
    active_from, chain_id, block_hash, block_number, canonicality_state
) VALUES (
    '00000000-0000-0000-0000-000000000002',
    'schema-v2-check:0xreset',
    '00000000-0000-0000-0000-000000000001',
    'declared_registry_path', to_timestamp(1),
    'authority-reset', '0x01', 1, 'canonical'
);
SQL
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
INSERT INTO surface_bindings (
    surface_binding_id, logical_name_id, resource_id, binding_kind,
    active_from, chain_id, block_hash, block_number, canonicality_state
) VALUES (
    '00000000-0000-0000-0000-000000000002',
    'schema-v2-check:0xreset',
    '00000000-0000-0000-0000-000000000001',
    'declared_registry_path', to_timestamp(1),
    'authority-reset', '0x01', 1, 'canonical'
);
TRUNCATE TABLE
    name_current,
    address_names_current,
    address_records_current,
    surface_bindings
    CONTINUE IDENTITY RESTRICT;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM surface_bindings) THEN
        RAISE EXCEPTION 'targeted binding reset left historical bindings';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM manifest_versions
        WHERE file_path = 'authority-reset-sentinel.toml'
    ) OR NOT EXISTS (
        SELECT 1 FROM normalized_events
        WHERE event_identity = 'authority-reset-normalized-sentinel'
    ) THEN
        RAISE EXCEPTION 'targeted binding reset removed preserved identity metadata';
    END IF;
END
$$;
SQL
    emit_phase_migration \
        "$ROOT/migrations/20260814130000_surface_binding_authority_arm.sql" \
        preceding-shape
    emit_phase_migration \
        "$ROOT/migrations/20260814130000_surface_binding_authority_arm.sql" \
        preceding-shape
    cat <<'SQL'
DO $$
DECLARE
    exclusion_definition text;
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'surface_bindings'
          AND column_name = 'authority_arm'
          AND is_nullable = 'NO'
    ) THEN
        RAISE EXCEPTION 'authority-arm upgrade did not add the required column';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'surface_bindings'::regclass
          AND conname = 'surface_bindings_authority_arm_check'
    ) THEN
        RAISE EXCEPTION 'authority-arm upgrade did not add its closed check';
    END IF;
    SELECT pg_get_constraintdef(oid)
    INTO exclusion_definition
    FROM pg_constraint
    WHERE conrelid = 'surface_bindings'::regclass
      AND conname = 'surface_bindings_no_overlap';
    IF exclusion_definition NOT LIKE '%chain_id WITH =%'
        OR exclusion_definition NOT LIKE '%logical_name_id WITH =%'
        OR exclusion_definition NOT LIKE '%authority_arm WITH =%'
    THEN
        RAISE EXCEPTION
            'authority-arm exclusion has the wrong domain: %', exclusion_definition;
    END IF;
END
$$;
SQL
} | run_psql

# Exercise the initialized pre-change schema branch: normalized events and
# unrelated data already exist, while the new redo handoff does not.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
INSERT INTO normalized_events (
    event_identity, namespace, event_kind, source_family,
    manifest_version, chain_id, derivation_kind, canonicality_state
) VALUES (
    'redo-handoff-upgrade-sentinel', 'schema-v2-check',
    'SourceManifestUpdated', 'schema-check', 1, 'schema-v2-check',
    'manifest_sync', 'finalized'
);
DROP TABLE project_redo_resolver_evidence;
SQL
    emit_phase_migration \
        "$ROOT/migrations/20260814120000_project_redo_resolver_evidence.sql" \
        preceding-shape
    cat <<'SQL'
DO $$
DECLARE
    constraint_count bigint;
    index_is_ready boolean;
BEGIN
    IF to_regclass(current_schema() || '.project_redo_resolver_evidence') IS NULL THEN
        RAISE EXCEPTION 'initialized-schema upgrade did not create redo handoff';
    END IF;

    SELECT count(*)
    INTO constraint_count
    FROM pg_constraint constraint_row
    WHERE constraint_row.conrelid = 'project_redo_resolver_evidence'::regclass
      AND constraint_row.contype IN ('p', 'c');
    IF constraint_count <> 4 THEN
        RAISE EXCEPTION
            'initialized-schema redo handoff has % required constraints, expected 4',
            constraint_count;
    END IF;

    SELECT index_state.indisvalid
       AND index_state.indisready
       AND index_state.indislive
    INTO index_is_ready
    FROM pg_class index_relation
    JOIN pg_namespace namespace
      ON namespace.oid = index_relation.relnamespace
    JOIN pg_index index_state
      ON index_state.indexrelid = index_relation.oid
    WHERE namespace.nspname = current_schema()
      AND index_relation.relname = 'project_redo_resolver_evidence_range_idx';
    IF index_is_ready IS DISTINCT FROM true THEN
        RAISE EXCEPTION 'initialized-schema redo handoff index is not ready';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM normalized_events
        WHERE event_identity = 'redo-handoff-upgrade-sentinel'
    ) THEN
        RAISE EXCEPTION 'initialized-schema upgrade changed existing normalized data';
    END IF;
END
$$;
DELETE FROM normalized_events
WHERE event_identity = 'redo-handoff-upgrade-sentinel';
SQL
} | run_psql

# Exercise the initialized pre-change schema branch for the bounded path-expiry
# handoff. Existing normalized events must survive while the schema-migrations
# add the table and then extend its scope to permission resources.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
INSERT INTO normalized_events (
    event_identity, namespace, event_kind, source_family,
    manifest_version, chain_id, derivation_kind, canonicality_state
) VALUES (
    'expiry-redo-handoff-upgrade-sentinel', 'schema-v2-check',
    'SourceManifestUpdated', 'schema-check', 1, 'schema-v2-check',
    'manifest_sync', 'finalized'
);
DROP TABLE project_redo_expiry_roots;
SQL
    emit_phase_migration \
        "$ROOT/migrations/20260902140000_project_redo_expiry_roots.sql" \
        preceding-shape
    emit_phase_migration \
        "$ROOT/migrations/20260902150000_project_redo_expiry_resources.sql" \
        preceding-shape
    emit_phase_migration \
        "$ROOT/migrations/20260902150000_project_redo_expiry_resources.sql" \
        preceding-shape
    cat <<'SQL'
DO $$
DECLARE
    constraint_count bigint;
    index_is_ready boolean;
    logical_name_is_nullable boolean;
    resource_id_is_nullable boolean;
BEGIN
    IF to_regclass(current_schema() || '.project_redo_expiry_roots') IS NULL THEN
        RAISE EXCEPTION
            'initialized-schema upgrade did not create path-expiry handoff';
    END IF;

    SELECT count(*)
    INTO constraint_count
    FROM pg_constraint constraint_row
    WHERE constraint_row.conrelid = 'project_redo_expiry_roots'::regclass
      AND constraint_row.contype IN ('p', 'c');
    IF constraint_count <> 4 THEN
        RAISE EXCEPTION
            'initialized-schema path-expiry handoff has % required constraints, expected 4',
            constraint_count;
    END IF;

    SELECT is_nullable = 'YES'
    INTO logical_name_is_nullable
    FROM information_schema.columns
    WHERE table_schema = current_schema()
      AND table_name = 'project_redo_expiry_roots'
      AND column_name = 'logical_name_id';
    IF logical_name_is_nullable IS DISTINCT FROM true THEN
        RAISE EXCEPTION
            'initialized-schema path-expiry logical name is not nullable';
    END IF;

    SELECT is_nullable = 'YES'
    INTO resource_id_is_nullable
    FROM information_schema.columns
    WHERE table_schema = current_schema()
      AND table_name = 'project_redo_expiry_roots'
      AND column_name = 'resource_id';
    IF resource_id_is_nullable IS DISTINCT FROM true THEN
        RAISE EXCEPTION
            'initialized-schema path-expiry resource is absent or not nullable';
    END IF;

    SELECT index_state.indisvalid
       AND index_state.indisready
       AND index_state.indislive
    INTO index_is_ready
    FROM pg_class index_relation
    JOIN pg_namespace namespace
      ON namespace.oid = index_relation.relnamespace
    JOIN pg_index index_state
      ON index_state.indexrelid = index_relation.oid
    WHERE namespace.nspname = current_schema()
      AND index_relation.relname = 'project_redo_expiry_roots_range_idx';
    IF index_is_ready IS DISTINCT FROM true THEN
        RAISE EXCEPTION
            'initialized-schema path-expiry handoff index is not ready';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM normalized_events
        WHERE event_identity = 'expiry-redo-handoff-upgrade-sentinel'
    ) THEN
        RAISE EXCEPTION
            'initialized-schema path-expiry handoff changed normalized data';
    END IF;
END
$$;
DELETE FROM normalized_events
WHERE event_identity = 'expiry-redo-handoff-upgrade-sentinel';
SQL
} | run_psql

# Exercise and verify the in-place comment upgrade on an initialized schema.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' \
        "COMMENT ON COLUMN service_heartbeats.heartbeat_at IS" \
        "    'This time records the latest completed work unit.';"
} | run_psql
for ignored in 1 2; do
    emit_phase_migration \
        "$ROOT/migrations/20260814121000_phase_heartbeat_liveness_comment.sql" \
        preceding-shape \
        | run_psql
done
heartbeat_comment_check="$({
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' \
        "SELECT CASE WHEN col_description('service_heartbeats'::regclass," \
        "    (SELECT attnum FROM pg_attribute" \
        "     WHERE attrelid = 'service_heartbeats'::regclass" \
        "       AND attname = 'heartbeat_at')) =" \
        "    'This time records runner liveness, including refreshes during storage-capacity waits.'" \
        "THEN 'heartbeat_liveness_comment_exact'" \
        "ELSE 'heartbeat_liveness_comment_wrong' END;"
} | run_psql)"
if [[ "$heartbeat_comment_check" != *heartbeat_liveness_comment_exact* ]]; then
    printf '%s\n' "heartbeat liveness comment upgrade was not applied" >&2
    exit 1
fi

# Exercise the initialized-schema unconfigured-settlement upgrade from its preceding
# shape. The existing row must stay NULL, and both additive migrations must be
# idempotent after the constraint has been validated.
assert_unconfigured_settlement_constraint baseline
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' \
        "INSERT INTO chain_phase_state (chain_id, phase_name)" \
        "VALUES ('verify-settlement-upgrade-check', 'verify');" \
        "ALTER TABLE chain_phase_state" \
        "    DROP CONSTRAINT chain_phase_state_unconfigured_settlement_check," \
        "    DROP COLUMN settled_while_unconfigured;"
} | run_psql
for ignored in 1 2; do
    for migration_file in \
        "$ROOT/migrations/20260814122000_verify_unconfigured_settlement.sql" \
        "$ROOT/migrations/20260814122100_verify_unconfigured_settlement_validate.sql"
    do
        emit_phase_migration "$migration_file" preceding-shape | run_psql
    done
done
assert_unconfigured_settlement_constraint migration
verify_settlement_upgrade_check="$({
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
SELECT CASE WHEN
    (SELECT settled_while_unconfigured IS NULL
     FROM chain_phase_state
     WHERE chain_id = 'verify-settlement-upgrade-check'
       AND phase_name = 'verify')
    AND (SELECT settled_while_unconfigured IS TRUE
         FROM chain_phase_state
         WHERE chain_id = 'phase-settlement-migration-true'
           AND phase_name = 'ingest')
    AND EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'chain_phase_state'
          AND column_name = 'settled_while_unconfigured'
          AND is_nullable = 'YES'
          AND column_default IS NULL
    )
    AND col_description(
        'chain_phase_state'::regclass,
        (SELECT attnum
         FROM pg_attribute
         WHERE attrelid = 'chain_phase_state'::regclass
           AND attname = 'settled_while_unconfigured'
           AND NOT attisdropped)
    ) = 'True only when startup settled an active phase row for a chain absent from runtime configuration; NULL identifies ordinary phase state.'
    AND EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'chain_phase_state'::regclass
          AND conname = 'chain_phase_state_unconfigured_settlement_check'
          AND convalidated
    )
THEN 'verify_settlement_upgrade_exact'
ELSE 'verify_settlement_upgrade_wrong' END;
SQL
} | run_psql)"
if [[ "$verify_settlement_upgrade_check" != *verify_settlement_upgrade_exact* ]]; then
    printf '%s\n' "Verify settlement provenance upgrade was not applied exactly" >&2
    exit 1
fi

# Exercise the initialized-schema Ingest redo boundary-marker upgrade from its
# preceding shape, then verify baseline and schema-migration parity.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' \
        "ALTER TABLE chain_phase_state" \
        "    DROP CONSTRAINT chain_phase_state_ingest_redo_source_boundaries_check," \
        "    DROP COLUMN redo_source_boundary_markers;"
} | run_psql
for ignored in 1 2; do
    emit_phase_migration \
        "$ROOT/migrations/20260814123000_ingest_redo_source_boundary_markers.sql" \
        preceding-shape \
        | run_psql
done
redo_source_boundary_upgrade_check="$({
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
SELECT CASE WHEN
    EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'chain_phase_state'
          AND column_name = 'redo_source_boundary_markers'
          AND data_type = 'jsonb'
          AND is_nullable = 'YES'
          AND column_default IS NULL
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'chain_phase_state'::regclass
          AND conname = 'chain_phase_state_ingest_redo_source_boundaries_check'
          AND convalidated
          AND pg_get_constraintdef(oid) LIKE '%phase_name = ''ingest''%'
          AND pg_get_constraintdef(oid) LIKE '%redo_in_progress%'
          AND pg_get_constraintdef(oid) LIKE '%jsonb_typeof%'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'chain_phase_state'::regclass
          AND conname <> 'chain_phase_state_ingest_redo_source_boundaries_check'
          AND convalidated
          AND pg_get_constraintdef(oid) LIKE '%redo_previous_phase_status%'
          AND pg_get_constraintdef(oid) LIKE '%redo_from_block_number%'
    )
    AND col_description(
        'chain_phase_state'::regclass,
        (SELECT attnum
         FROM pg_attribute
         WHERE attrelid = 'chain_phase_state'::regclass
           AND attname = 'redo_source_boundary_markers'
           AND NOT attisdropped)
    ) = 'This object maps each Ingest source key to a block number and hash returned by a boundary load during the active redo.'
THEN 'redo_source_boundary_upgrade_ok'
ELSE 'redo_source_boundary_upgrade_wrong' END;
SQL
} | run_psql)"
if [[ "$redo_source_boundary_upgrade_check" != *redo_source_boundary_upgrade_ok* ]]; then
    printf '%s\n' "Ingest redo source-boundary upgrade was not applied" >&2
    exit 1
fi

# Exercise the initialized-schema redo-attempt generation upgrade from its
# preceding shape, then verify baseline and schema-migration parity.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' \
        "ALTER TABLE chain_phase_state" \
        "    DROP CONSTRAINT chain_phase_state_redo_attempt_generation_check," \
        "    DROP COLUMN redo_attempt_generation;"
} | run_psql
for ignored in 1 2; do
    emit_phase_migration \
        "$ROOT/migrations/20260814124000_redo_attempt_generation.sql" \
        preceding-shape \
        | run_psql
    emit_phase_migration \
        "$ROOT/migrations/20260825041728_redo_attempt_generation_comment.sql" \
        preceding-shape \
        | run_psql
    emit_phase_migration \
        "$ROOT/migrations/20260831140000_discovery_watch_admissions.sql" \
        baseline-first \
        | run_psql
done
redo_attempt_generation_upgrade_check="$({
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
SELECT CASE WHEN
    EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'chain_phase_state'
          AND column_name = 'redo_attempt_generation'
          AND data_type = 'bigint'
          AND is_nullable = 'NO'
          AND column_default = '0'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'chain_phase_state'::regclass
          AND conname = 'chain_phase_state_redo_attempt_generation_check'
          AND convalidated
          AND pg_get_constraintdef(oid) LIKE '%redo_attempt_generation >= 0%'
    )
    AND col_description(
        'chain_phase_state'::regclass,
        (SELECT attnum
         FROM pg_attribute
         WHERE attrelid = 'chain_phase_state'::regclass
           AND attname = 'redo_attempt_generation'
           AND NOT attisdropped)
    ) = 'This nonnegative, row-local counter increments when an explicit redo begins, when the phase runner installs or extends required downstream redo, and when the shared required-Ingest installer records genuinely new manifest or discovery demand. Repeated observation of unchanged semantic demand is suppressed before installation and does not advance it.'
THEN 'redo_attempt_generation_upgrade_ok'
ELSE 'redo_attempt_generation_upgrade_wrong' END;
SQL
} | run_psql)"
if [[ "$redo_attempt_generation_upgrade_check" != *redo_attempt_generation_upgrade_ok* ]]; then
    printf '%s\n' "Redo attempt generation upgrade was not applied" >&2
    exit 1
fi

# Exercise the initialized-schema Ingest redo manifest-fingerprint upgrade from
# its preceding shape, then verify baseline and schema-migration parity.
baseline_redo_manifest_authority_constraint="$({
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
SELECT pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid = 'chain_phase_state'::regclass
  AND conname = 'chain_phase_state_ingest_redo_manifest_authority_check'
  AND convalidated;
SQL
} | run_psql)"
if [[ -z "$baseline_redo_manifest_authority_constraint" ]]; then
    printf '%s\n' "Baseline is missing the Ingest redo manifest-fingerprint constraint" >&2
    exit 1
fi
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' \
        "ALTER TABLE chain_phase_state" \
        "    DROP CONSTRAINT chain_phase_state_ingest_redo_manifest_authority_check," \
        "    DROP COLUMN redo_manifest_authority_fingerprint;"
} | run_psql
for ignored in 1 2; do
    emit_phase_migration \
        "$ROOT/migrations/20260814125000_ingest_redo_manifest_authority.sql" \
        preceding-shape \
        | run_psql
done
migration_redo_manifest_authority_constraint="$({
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
SELECT pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid = 'chain_phase_state'::regclass
  AND conname = 'chain_phase_state_ingest_redo_manifest_authority_check'
  AND convalidated;
SQL
} | run_psql)"
if [[ "$migration_redo_manifest_authority_constraint" != "$baseline_redo_manifest_authority_constraint" ]]; then
    printf '%s\n' "Baseline and schema-migration Ingest redo evidence constraints differ" >&2
    exit 1
fi
redo_manifest_authority_upgrade_check="$({
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
SELECT CASE WHEN
    EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'chain_phase_state'
          AND column_name = 'redo_manifest_authority_fingerprint'
          AND data_type = 'text'
          AND is_nullable = 'YES'
          AND column_default IS NULL
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'chain_phase_state'::regclass
          AND conname = 'chain_phase_state_ingest_redo_manifest_authority_check'
          AND convalidated
          AND pg_get_constraintdef(oid) LIKE '%phase_name = ''ingest''%'
          AND pg_get_constraintdef(oid) LIKE '%redo_in_progress%'
          AND pg_get_constraintdef(oid) LIKE '%redo_manifest_authority_fingerprint ~%'
    )
    AND col_description(
        'chain_phase_state'::regclass,
        (SELECT attnum
         FROM pg_attribute
         WHERE attrelid = 'chain_phase_state'::regclass
           AND attname = 'redo_manifest_authority_fingerprint'
           AND NOT attisdropped)
    ) = 'For an active Ingest redo, this value binds resumable numeric and per-source boundary evidence to the chain''s active manifest rows, excluding normalizer_version.'
THEN 'redo_manifest_authority_upgrade_ok'
ELSE 'redo_manifest_authority_upgrade_wrong' END;
SQL
} | run_psql)"
if [[ "$redo_manifest_authority_upgrade_check" != *redo_manifest_authority_upgrade_ok* ]]; then
    printf '%s\n' "Ingest redo manifest-fingerprint upgrade was not applied" >&2
    exit 1
fi

# Exercise the initialized-schema upgrade against the preceding closed
# vocabularies. Rewrite only the qualified schema name so the checked-in
# schema-migration runs against this isolated scratch namespace.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
ALTER TABLE surface_bindings
    DROP CONSTRAINT surface_bindings_binding_kind_check,
    ADD CONSTRAINT surface_bindings_binding_kind_check
        CHECK (
            binding_kind IN (
                'declared_registry_path',
                'linked_subregistry_path',
                'resolver_alias_path',
                'observed_wildcard_path',
                'migration_rebind',
                'observed_only'
            )
        );
ALTER TABLE name_current
    DROP CONSTRAINT name_current_binding_kind_check,
    ADD CONSTRAINT name_current_binding_kind_check
        CHECK (
            binding_kind IS NULL
            OR binding_kind IN (
                'declared_registry_path',
                'linked_subregistry_path',
                'resolver_alias_path',
                'observed_wildcard_path',
                'migration_rebind',
                'observed_only'
            )
        );
ALTER TABLE address_names_current
    DROP CONSTRAINT address_names_current_binding_kind_check,
    ADD CONSTRAINT address_names_current_binding_kind_check
        CHECK (
            binding_kind IN (
                'declared_registry_path',
                'linked_subregistry_path',
                'resolver_alias_path',
                'observed_wildcard_path',
                'migration_rebind',
                'observed_only'
            )
        );
ALTER TABLE permissions_current
    DROP CONSTRAINT permissions_current_scope_kind_check,
    ADD CONSTRAINT permissions_current_scope_kind_check
        CHECK (
            scope_kind IN (
                'root',
                'registry',
                'resource',
                'resolver',
                'record_manager',
                'migration_derived',
                'transport_derived'
            )
        );
INSERT INTO normalized_events (
    event_identity, namespace, event_kind, source_family, manifest_version,
    chain_id, derivation_kind, after_state
) VALUES (
    'remove-l2-safe-event', 'schema-v2-check', 'PermissionChanged',
    'schema-check', 1, 'schema-v2-check', 'ens_v2_permissions',
    '{"scope":{"kind":"resource"}}'::jsonb
);
INSERT INTO surface_bindings (
    surface_binding_id, logical_name_id, resource_id, binding_kind,
    authority_arm, active_from, chain_id, block_hash, block_number,
    canonicality_state
) VALUES
    ('00000000-0000-0000-0000-000000000030', 'schema-v2-check:0xreset',
     '00000000-0000-0000-0000-000000000001', 'declared_registry_path',
     'ens_v1', to_timestamp(2), 'authority-reset', '0x01', 1, 'canonical'),
    ('00000000-0000-0000-0000-000000000031', 'schema-v2-check:0xreset',
     '00000000-0000-0000-0000-000000000001', 'declared_registry_path',
     'ens_v2', to_timestamp(2), 'authority-reset', '0x01', 1, 'canonical');
INSERT INTO name_current (
    logical_name_id, namespace, raw_name, namehash, surface_binding_id,
    resource_id, binding_kind, support_status, manifest_version
) VALUES (
    'schema-v2-check:0xreset', 'schema-v2-check', 'reset', '0xreset',
    '00000000-0000-0000-0000-000000000031',
    '00000000-0000-0000-0000-000000000001',
    'declared_registry_path', 'supported', 1
);
INSERT INTO address_names_current (
    address, logical_name_id, relation, namespace, raw_name, namehash,
    surface_binding_id, resource_id, binding_kind, support_status,
    manifest_version
) VALUES (
    'remove-l2-address', 'schema-v2-check:0xreset', 'registrant',
    'schema-v2-check', 'reset', '0xreset',
    '00000000-0000-0000-0000-000000000031',
    '00000000-0000-0000-0000-000000000001',
    'declared_registry_path', 'supported', 1
);
INSERT INTO permissions_current (
    resource_id, subject, scope, scope_kind, manifest_version
) VALUES (
    '00000000-0000-0000-0000-000000000001',
    'remove-l2-subject', 'remove-l2-scope', 'resource', 1
);
SQL
} | run_psql

remove_l2_migration="$ROOT/migrations/20260810120000_remove_l2_migration_remnants.sql"
assert_migration_refusal remove-l2-normalized-events "$remove_l2_migration" \
    'cannot remove permission scopes: normalized events still use removed values' <<'SQL'
UPDATE normalized_events SET after_state = '{"scope":{"kind":"migration_derived"}}'
WHERE event_identity = 'remove-l2-safe-event';
SQL
assert_migration_refusal remove-l2-surface-bindings "$remove_l2_migration" \
    'cannot remove migration_rebind: surface bindings still use it' <<'SQL'
UPDATE surface_bindings SET binding_kind = 'migration_rebind'
WHERE surface_binding_id = '00000000-0000-0000-0000-000000000030';
SQL
assert_migration_refusal remove-l2-name-current "$remove_l2_migration" \
    'cannot remove migration_rebind: current names still use it' <<'SQL'
ALTER TABLE name_current ALTER CONSTRAINT
    name_current_surface_binding_id_logical_name_id_resource_i_fkey
    DEFERRABLE INITIALLY DEFERRED;
UPDATE name_current SET binding_kind = 'migration_rebind'
WHERE logical_name_id = 'schema-v2-check:0xreset';
SQL
assert_migration_refusal remove-l2-address-names-current "$remove_l2_migration" \
    'cannot remove migration_rebind: current address-name rows still use it' <<'SQL'
ALTER TABLE address_names_current ALTER CONSTRAINT
    address_names_current_surface_binding_id_logical_name_id_r_fkey
    DEFERRABLE INITIALLY DEFERRED;
UPDATE address_names_current SET binding_kind = 'migration_rebind'
WHERE address = 'remove-l2-address';
SQL
assert_migration_refusal remove-l2-permissions-current "$remove_l2_migration" \
    'cannot remove permission scopes: current rows still use removed values' <<'SQL'
UPDATE permissions_current SET scope_kind = 'migration_derived'
WHERE subject = 'remove-l2-subject';
SQL

{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' \
        "DELETE FROM address_names_current WHERE address = 'remove-l2-address';" \
        "DELETE FROM name_current WHERE logical_name_id = 'schema-v2-check:0xreset';" \
        "DELETE FROM permissions_current WHERE subject = 'remove-l2-subject';" \
        "DELETE FROM surface_bindings WHERE surface_binding_id IN (" \
        "    '00000000-0000-0000-0000-000000000030'," \
        "    '00000000-0000-0000-0000-000000000031'" \
        ");" \
        "DELETE FROM normalized_events WHERE event_identity = 'remove-l2-safe-event';"
    emit_phase_migration "$remove_l2_migration" preceding-shape
    cat <<'SQL'
DO $$
DECLARE
    removed_vocabulary_count bigint;
BEGIN
    SELECT count(*)
    INTO removed_vocabulary_count
    FROM pg_constraint constraint_row
    WHERE constraint_row.conname IN (
        'surface_bindings_binding_kind_check',
        'name_current_binding_kind_check',
        'address_names_current_binding_kind_check',
        'permissions_current_scope_kind_check'
    )
      AND constraint_row.conrelid IN (
          'surface_bindings'::regclass,
          'name_current'::regclass,
          'address_names_current'::regclass,
          'permissions_current'::regclass
      )
      AND pg_get_constraintdef(constraint_row.oid) ~
          '(migration_rebind|migration_derived|transport_derived)';

    IF removed_vocabulary_count <> 0 THEN
        RAISE EXCEPTION
            'initialized-schema upgrade retained removed vocabulary in % constraints',
            removed_vocabulary_count;
    END IF;
END
$$;
SQL
} | run_psql

# Exercise the ENSv1-to-ENSv2 vocabulary upgrade from the exact preceding
# normalized-event shape. This catches deployed-schema drift independently of
# the idempotent fresh-install baseline check above.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
DROP TABLE migration_candidate_discovery_effects;
DROP TABLE migration_candidate_identity_effects;
DROP TABLE migration_discovery_associations;
DROP TABLE migration_event_associations;
ALTER TABLE normalized_events
    DROP CONSTRAINT normalized_events_event_kind_check,
    DROP CONSTRAINT normalized_events_derivation_kind_check,
    DROP CONSTRAINT normalized_events_migration_correlation_ids_check,
    DROP CONSTRAINT normalized_events_consumer_visibility_check,
    DROP CONSTRAINT normalized_events_candidate_correlation_check,
    DROP COLUMN migration_correlation_ids,
    DROP COLUMN consumer_visibility,
    ADD CONSTRAINT normalized_events_event_kind_check
        CHECK (
            event_kind IN (
                'AliasChanged',
                'AuthorityEpochChanged',
                'AuthorityTransferred',
                'ExpiryChanged',
                'ParentChanged',
                'PermissionChanged',
                'PermissionScopeChanged',
                'PreimageObserved',
                'RecordChanged',
                'RecordVersionChanged',
                'RegistrarNameRegistered',
                'RegistrationGranted',
                'RegistrationReleased',
                'RegistrationRenewed',
                'RegistrationReserved',
                'RegistryCreated',
                'ResolverChanged',
                'ReverseChanged',
                'RootPermissionChanged',
                'SourceManifestUpdated',
                'SubregistryChanged',
                'SurfaceBound',
                'SurfaceUnbound',
                'TokenControlTransferred',
                'TokenRegenerated',
                'TokenResourceLinked',
                'Upgraded'
            )
        ),
    ADD CONSTRAINT normalized_events_derivation_kind_check
        CHECK (
            derivation_kind IN (
                'ens_v1_reverse_claim',
                'ens_v1_unwrapped_authority',
                'ens_v2_permissions',
                'ens_v2_registrar',
                'ens_v2_registry_resource_surface',
                'ens_v2_resolver',
                'manifest_sync',
                'proxy_upgrade',
                'raw_log_preimage_observation'
            )
        );
DROP FUNCTION migration_correlation_ids_valid(text[]);
DROP TABLE account_permission_state_current;
DROP INDEX permissions_current_resource_registry_binding_idx;
ALTER TABLE permissions_current_resource_summary
    DROP CONSTRAINT permissions_current_resource_summary_registry_binding_check,
    DROP COLUMN registry_owner,
    DROP COLUMN registry_contract,
    DROP COLUMN registry_binding_provenance,
    DROP COLUMN registry_binding_chain_positions;
SQL
    for migration_file in \
        "$ROOT/migrations/20260811120000_ens_v2_migration_slice_1.sql" \
        "$ROOT/migrations/20260811120100_ens_v2_migration_slice_1_validate.sql" \
        "$ROOT/migrations/20260811120200_ens_v2_migration_slice_1_constraints.sql" \
        "$ROOT/migrations/20260820140000_raw_block_preimage_derivation.sql" \
        "$ROOT/migrations/20260820140100_raw_block_preimage_derivation_validate.sql" \
        "$ROOT/migrations/20260820140200_raw_block_preimage_derivation_swap.sql" \
        "$ROOT/migrations/20260902160000_registry_operator_account_permissions.sql" \
        "$ROOT/migrations/20260902160100_registry_operator_account_permissions_validate.sql" \
        "$ROOT/migrations/20260902160200_registry_operator_account_permissions_swap.sql"
    do
        emit_phase_migration "$migration_file" preceding-shape
    done
} | run_psql

# Dropping consumer_visibility above took the four resolver-history indexes
# whose predicates name it with it, which is the shape of a database that took
# slice 1 in place: #415 added them to the baseline with no schema-migration.
# The file that carries them now must build the two kept indexes to the
# fresh-baseline definition from that shape, change nothing when rerun, and
# refuse a kept index under the right name that is invalid, is another
# definition, or is a table. A database initialized from the #415 baseline
# holds all four; from that shape the file must drop the two retired
# `permission_*` indexes, which have no reader, and refuse a retired name
# held by a table.
resolver_history_index_migration="$ROOT/migrations/20260918120000_normalized_events_resolver_history_idx.sql"
resolver_history_index_names="normalized_events_pointer_after_resolver_history_idx normalized_events_pointer_before_resolver_history_idx"
resolver_history_retired_names="normalized_events_permission_after_resolver_history_idx normalized_events_permission_before_resolver_history_idx"
resolver_history_retired_sql='
CREATE INDEX normalized_events_permission_after_resolver_history_idx
    ON normalized_events (chain_id, lower(after_state #>> '"'"'{scope,resolver_address}'"'"'), block_number, block_hash)
    INCLUDE (resource_id)
    WHERE event_kind = '"'"'PermissionChanged'"'"' AND consumer_visibility = '"'"'activated'"'"'
      AND canonicality_state IN ('"'"'canonical'"'"', '"'"'safe'"'"', '"'"'finalized'"'"')
      AND after_state #>> '"'"'{scope,kind}'"'"' = '"'"'resolver'"'"' AND resource_id IS NOT NULL;
CREATE INDEX normalized_events_permission_before_resolver_history_idx
    ON normalized_events (chain_id, lower(before_state #>> '"'"'{scope,resolver_address}'"'"'), block_number, block_hash)
    INCLUDE (resource_id)
    WHERE event_kind = '"'"'PermissionChanged'"'"' AND consumer_visibility = '"'"'activated'"'"'
      AND canonicality_state IN ('"'"'canonical'"'"', '"'"'safe'"'"', '"'"'finalized'"'"')
      AND before_state #>> '"'"'{scope,kind}'"'"' = '"'"'resolver'"'"' AND resource_id IS NOT NULL;
'
assert_resolver_history_index_state() {
    local reason="$1"
    {
        printf 'SET search_path TO "%s";\n' "$scratch_schema"
        cat <<SQL
DO \$\$
DECLARE built integer; retired integer;
BEGIN
    SELECT count(*) INTO built FROM pg_index
    WHERE indrelid = 'normalized_events'::regclass
      AND indexrelid::regclass::text LIKE 'normalized_events_pointer_%_resolver_history_idx'
      AND indisvalid AND indisready;
    SELECT count(*) INTO retired FROM pg_class
    WHERE relnamespace = current_schema()::regnamespace
      AND relname LIKE 'normalized_events_permission_%_resolver_history_idx';
    IF built <> 2 OR retired <> 0 THEN
        RAISE EXCEPTION '$reason: % of 2 kept resolver-history indexes built, % retired ones present', built, retired;
    END IF;
END \$\$;
SQL
    } | run_psql
}
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_class
        WHERE relnamespace = current_schema()::regnamespace
          AND relname LIKE 'normalized_events_%_resolver_history_idx'
    ) THEN
        RAISE EXCEPTION 'the slice-1 predecessor shape still carries a resolver-history index';
    END IF;
END $$;
SQL
    emit_phase_migration "$resolver_history_index_migration" preceding-shape
    emit_phase_migration "$resolver_history_index_migration" baseline-first
} | run_psql
assert_resolver_history_index_state "from the slice-1 shape"
assert_migration_context_count "$resolver_history_index_migration" preceding-shape 1
assert_migration_context_count "$resolver_history_index_migration" baseline-first 1
# The #415 shape: the two kept indexes present, the two retired ones too.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' "$resolver_history_retired_sql"
    emit_phase_migration "$resolver_history_index_migration" preceding-shape
} | run_psql
assert_resolver_history_index_state "from the #415 shape"
assert_migration_context_count "$resolver_history_index_migration" preceding-shape 2
for resolver_history_retired_name in $resolver_history_retired_names; do
    assert_migration_refusal "table-named-$resolver_history_retired_name" \
        "$resolver_history_index_migration" \
        "$scratch_schema.$resolver_history_retired_name is not an index (relkind r), so it cannot be the retired index; remove or rename that relation, then run the schema-migrations again" <<SQL
CREATE TABLE $resolver_history_retired_name ();
SQL
    assert_migration_refusal "other-table-$resolver_history_retired_name" \
        "$resolver_history_index_migration" \
        "$scratch_schema.$resolver_history_retired_name is an index on another table, so it cannot be the retired index; remove or rename that index, then run the schema-migrations again" <<SQL
CREATE INDEX $resolver_history_retired_name ON discovery_edges (chain_id);
SQL
    # The definition the file expects for a retired name is how the #415 index
    # prints under search_path pg_catalog; read it from a copy built and
    # dropped here.
    resolver_history_retired_reviewed="$(
        {
            printf '\\pset tuples_only on\n\\pset format unaligned\n'
            printf 'SET search_path TO "%s";\n' "$scratch_schema"
            printf '%s\n' "$resolver_history_retired_sql"
            printf 'SET search_path TO pg_catalog;\n'
            printf "SELECT pg_get_indexdef('%s.%s'::regclass);\n" "$scratch_schema" "$resolver_history_retired_name"
            printf 'DROP INDEX "%s".normalized_events_permission_after_resolver_history_idx, "%s".normalized_events_permission_before_resolver_history_idx;\n' "$scratch_schema" "$scratch_schema"
        } | run_psql
    )"
    assert_migration_refusal "other-definition-$resolver_history_retired_name" \
        "$resolver_history_index_migration" \
        "$scratch_schema.$resolver_history_retired_name is not the retired index; found \"CREATE INDEX $resolver_history_retired_name ON $scratch_schema.normalized_events USING btree (chain_id, block_number)\", expected \"$resolver_history_retired_reviewed\"; remove or rename that index, then run the schema-migrations again" <<SQL
CREATE INDEX $resolver_history_retired_name ON normalized_events (chain_id, block_number);
SQL
done
# The definitions the file expects are the fresh baseline's, as printed with
# search_path set to pg_catalog; the frozen catalog comparison at the end
# proves the built indexes match the baseline. The schema-migration's own
# check must accept all four under either session search_path and with
# quote_all_identifiers on, and leave both settings as it found them.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    emit_phase_migration "$resolver_history_index_migration" baseline-first
    assert_search_path_sql "$scratch_schema"
    printf 'SET search_path TO public;\n'
    emit_phase_migration "$resolver_history_index_migration" baseline-first
    assert_search_path_sql public
    printf 'BEGIN;\nSET LOCAL search_path TO "%s", public;\n' "$scratch_schema"
    render_phase_migration "$resolver_history_index_migration"
    assert_search_path_sql "$scratch_schema, public"
    printf 'COMMIT;\n'
    assert_search_path_sql public
    emit_quote_all_identifiers_probe "$resolver_history_index_migration" in-transaction
} | run_psql
assert_migration_context_count "$resolver_history_index_migration" empty-schema 1
assert_migration_context_count "$resolver_history_index_migration" baseline-first 3
# The live prebuild in ops/resolver-history-indexes/install.sql builds both
# kept indexes and drops both retired ones in one file. For each kept index in
# turn it must build the baseline definition, refuse an invalid index, a valid
# index with other keys, a valid index whose JSON key literals start with the
# schema name, and a table under the name, and recover as its README says. It
# must drop the retired pair from the #415 shape and refuse a retired name a
# table holds.
resolver_history_install="$ROOT/ops/resolver-history-indexes/install.sql"
resolver_history_readme=ops/resolver-history-indexes/README.md
for resolver_history_index_name in $resolver_history_index_names; do
    assert_concurrent_index_installer "resolver-history-$resolver_history_index_name" \
        "$resolver_history_index_name" \
        "$resolver_history_install" \
        "$resolver_history_readme" \
        normalized_events \
        "block_number, chain_id"
done
# The kept predicates name consumer_visibility, which slice 1 adds; the
# installer names that prerequisite on a namespace from before slice 1, and
# names the table on one that has no phase schema, instead of failing inside
# the first build. Renaming keeps every dependent, so the shape is put back.
printf 'SET search_path TO "%s";\nALTER TABLE normalized_events RENAME COLUMN consumer_visibility TO consumer_visibility_absent;\n' "$scratch_schema" | run_psql >/dev/null
assert_index_install_refusal resolver-history-before-slice-1 \
    "$resolver_history_install" \
    "$scratch_schema.normalized_events has no consumer_visibility column, which both kept predicates name; apply the schema-migrations through 20260811120000_ens_v2_migration_slice_1.sql first, as docs/runbooks/production-docker.md step 3 describes, then rerun this script"
printf 'SET search_path TO "%s";\nALTER TABLE normalized_events RENAME COLUMN consumer_visibility_absent TO consumer_visibility;\nALTER TABLE normalized_events RENAME TO normalized_events_absent;\n' "$scratch_schema" | run_psql >/dev/null
assert_index_install_refusal resolver-history-no-phase-table \
    "$resolver_history_install" \
    "$scratch_schema.normalized_events does not exist; this script is for an initialized namespace, and a fresh one takes these indexes from the baseline"
printf 'SET search_path TO "%s";\nALTER TABLE normalized_events_absent RENAME TO normalized_events;\n' "$scratch_schema" | run_psql >/dev/null
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' "$resolver_history_retired_sql"
    render_phase_migration "$resolver_history_install"
} | run_psql >/dev/null
assert_resolver_history_index_state "installer from the #415 shape"
for resolver_history_retired_name in $resolver_history_retired_names; do
    printf 'SET search_path TO "%s";\nCREATE TABLE %s ();\n' "$scratch_schema" "$resolver_history_retired_name" | run_psql >/dev/null
    assert_index_install_refusal "resolver-history-table-named-$resolver_history_retired_name" \
        "$resolver_history_install" \
        "$scratch_schema.$resolver_history_retired_name is not an index (relkind r), so it cannot be the retired index; remove or rename that relation, then rerun this script"
    printf 'SET search_path TO "%s";\nDROP TABLE %s;\nCREATE INDEX %s ON discovery_edges (chain_id);\n' "$scratch_schema" "$resolver_history_retired_name" "$resolver_history_retired_name" | run_psql >/dev/null
    assert_index_install_refusal "resolver-history-other-table-$resolver_history_retired_name" \
        "$resolver_history_install" \
        "$scratch_schema.$resolver_history_retired_name is an index on another table, so it cannot be the retired index; remove or rename that index, then rerun this script"
    printf 'SET search_path TO "%s";\nDROP INDEX %s;\nCREATE INDEX %s ON normalized_events (chain_id, block_number);\n' "$scratch_schema" "$resolver_history_retired_name" "$resolver_history_retired_name" | run_psql >/dev/null
    assert_index_install_refusal "resolver-history-other-definition-$resolver_history_retired_name" \
        "$resolver_history_install" \
        "$scratch_schema.$resolver_history_retired_name is not the retired index; found \"CREATE INDEX $resolver_history_retired_name ON $scratch_schema.normalized_events USING btree (chain_id, block_number)\", expected \"$(
            {
                printf '\\pset tuples_only on\n\\pset format unaligned\n'
                printf 'SET search_path TO "%s";\n' "$scratch_schema"
                printf 'DROP INDEX %s;\n' "$resolver_history_retired_name"
                printf '%s\n' "$resolver_history_retired_sql"
                printf 'SET search_path TO pg_catalog;\n'
                printf "SELECT pg_get_indexdef('%s.%s'::regclass);\n" "$scratch_schema" "$resolver_history_retired_name"
                printf 'DROP INDEX "%s".normalized_events_permission_after_resolver_history_idx, "%s".normalized_events_permission_before_resolver_history_idx;\n' "$scratch_schema" "$scratch_schema"
                printf 'SET search_path TO "%s";\nCREATE INDEX %s ON normalized_events (chain_id, block_number);\n' "$scratch_schema" "$resolver_history_retired_name"
            } | run_psql
        )\"; remove or rename that index, then rerun this script"
    printf 'SET search_path TO "%s";\nDROP INDEX %s;\n' "$scratch_schema" "$resolver_history_retired_name" | run_psql >/dev/null
done
# The installer refuses before it builds anything. With the last index invalid
# and the first one absent, it must stop on the invalid one, tell the operator
# how to drop it, and leave the first one unbuilt.
resolver_history_first_index="${resolver_history_index_names%% *}"
resolver_history_last_index="${resolver_history_index_names##* }"
printf 'SET search_path TO "%s";\nDROP INDEX %s;\n' "$scratch_schema" "$resolver_history_first_index" | run_psql >/dev/null
build_invalid_index "$resolver_history_last_index" normalized_events
assert_index_install_refusal resolver-history-refuses-before-building \
    "$resolver_history_install" \
    "$resolver_history_last_index is missing from $scratch_schema.normalized_events or is not valid and ready; follow the recovery steps in $resolver_history_readme before retrying"
assert_index_install_hint resolver-history-invalid-index-hint \
    "$resolver_history_install" \
    "An interrupted concurrent build leaves an invalid index. Confirm in pg_stat_progress_create_index that no build is still running, run DROP INDEX CONCURRENTLY $scratch_schema.$resolver_history_last_index, then rerun this script."
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<SQL
DO \$\$
BEGIN
    IF to_regclass('$resolver_history_first_index') IS NOT NULL THEN
        RAISE EXCEPTION 'resolver-history installer built an index before refusing an invalid one';
    END IF;
END \$\$;
DROP INDEX CONCURRENTLY $resolver_history_last_index;
SQL
    render_phase_migration "$resolver_history_install"
    # An index on another table under the name is not the index either.
    printf '%s\n' \
        "DROP INDEX $resolver_history_first_index;" \
        "CREATE INDEX $resolver_history_first_index ON discovery_edges (chain_id);"
} | run_psql >/dev/null
assert_index_install_refusal resolver-history-index-on-another-table \
    "$resolver_history_install" \
    "$resolver_history_first_index is missing from $scratch_schema.normalized_events or is not valid and ready; follow the recovery steps in $resolver_history_readme before retrying"
assert_index_install_hint resolver-history-index-on-another-table-hint \
    "$resolver_history_install" \
    "An index on $scratch_schema.discovery_edges holds this name. Rename or remove it, then rerun this script."
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' "DROP INDEX $resolver_history_first_index;"
    render_phase_migration "$resolver_history_install"
} | run_psql >/dev/null
# Put each index in turn into every shape the schema-migration must refuse
# rather than adopt: invalid, another definition, a table under the name. The
# expected definition it names must be how the fresh-baseline index prints,
# read under search_path pg_catalog.
resolver_history_recovery="follow the recovery steps in $resolver_history_readme, then run the schema-migrations again"
for resolver_history_index_name in $resolver_history_index_names; do
    resolver_history_reviewed="$(
        {
            printf '\\pset tuples_only on\n\\pset format unaligned\n'
            printf 'SET search_path TO pg_catalog;\n'
            printf "SELECT pg_get_indexdef('%s.%s'::regclass);\n" "$scratch_schema" "$resolver_history_index_name"
        } | run_psql
    )"
    with_index_invalidated "$resolver_history_index_name" normalized_events \
        assert_migration_refusal "invalid-$resolver_history_index_name" \
        "$resolver_history_index_migration" \
        "$resolver_history_index_name exists but is not a valid and ready index on $scratch_schema.normalized_events; $resolver_history_recovery" <<SQL
SQL
    assert_migration_refusal "wrong-definition-$resolver_history_index_name" \
        "$resolver_history_index_migration" \
        "$resolver_history_index_name exists but does not have the reviewed definition; found \"CREATE INDEX $resolver_history_index_name ON $scratch_schema.normalized_events USING btree (chain_id, block_number)\", expected \"$resolver_history_reviewed\"; $resolver_history_recovery" <<SQL
DROP INDEX $resolver_history_index_name;
CREATE INDEX $resolver_history_index_name ON normalized_events (chain_id, block_number);
SQL
    assert_migration_refusal "table-named-$resolver_history_index_name" \
        "$resolver_history_index_migration" \
        "$scratch_schema.$resolver_history_index_name is a table, not an index, so the index was never built; remove or rename that relation, then follow $resolver_history_readme and run the schema-migrations again" <<SQL
DROP INDEX $resolver_history_index_name;
CREATE TABLE $resolver_history_index_name ();
SQL
done

# The preceding vocabulary reconstruction ends with the exact 20260902 constraint.
record_id_add="$ROOT/migrations/20260909120000_resolver_record_id_events.sql"
record_id_validate="$ROOT/migrations/20260909120100_resolver_record_id_events_validate.sql"
record_id_swap="$ROOT/migrations/20260909120200_resolver_record_id_events_swap.sql"
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
    manifest_version, chain_id, derivation_kind, after_state)
VALUES ('record-id-predecessor', 'schema-v2-check', 'RecordChanged', 'ens_v2_resolver_l1',
    1, 'schema-v2-check', 'ens_v2_resolver', '{"retained":true}');
CREATE TEMP TABLE record_id_predecessor AS
    SELECT * FROM normalized_events WHERE event_identity = 'record-id-predecessor';
SQL
    emit_phase_migration "$record_id_add" preceding-shape
    emit_phase_migration "$record_id_swap" preceding-shape
    cat <<'SQL'
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
        WHERE conrelid = 'normalized_events'::regclass
          AND conname = 'normalized_events_event_kind_check' AND convalidated)
    OR NOT EXISTS (SELECT 1 FROM pg_constraint
        WHERE conrelid = 'normalized_events'::regclass
          AND conname = 'normalized_events_event_kind_check_record_id' AND NOT convalidated)
    THEN RAISE EXCEPTION 'unvalidated record-ID replacement removed prior protection'; END IF;
END $$;
SQL
    emit_phase_migration "$record_id_validate" preceding-shape
    emit_phase_migration "$record_id_swap" preceding-shape
    cat <<'SQL'
DO $$
BEGIN
    IF EXISTS (SELECT * FROM record_id_predecessor EXCEPT
        SELECT * FROM normalized_events WHERE event_identity = 'record-id-predecessor')
    THEN RAISE EXCEPTION 'record-ID upgrade changed existing facts'; END IF;
END $$;
DELETE FROM normalized_events WHERE event_identity = 'record-id-predecessor';
SQL
} | run_psql
for migration_file in "$record_id_add" "$record_id_validate" "$record_id_swap"; do
    emit_phase_migration "$migration_file" preceding-shape | run_psql
    assert_migration_context_count "$migration_file" empty-schema 1
    assert_migration_context_count "$migration_file" baseline-first 2
    expected_record_id_applications=2
    if [ "$migration_file" = "$record_id_swap" ]; then expected_record_id_applications=3; fi
    assert_migration_context_count "$migration_file" preceding-shape "$expected_record_id_applications"
done

# Function migrations are rendered into this isolated schema. Their fixed search
# paths must follow that rendering, including when replacing baseline functions.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
DO $$
BEGIN
    IF (
        SELECT count(*) FROM pg_proc procedure
        JOIN pg_namespace namespace ON namespace.oid = procedure.pronamespace
        WHERE namespace.nspname = current_schema()
          AND procedure.proname IN (
              'revalidate_resolution_lookup_state', 'write_resolution_divergence'
          )
          AND procedure.proconfig @> ARRAY[
              'search_path=pg_catalog, ' || current_schema() || ', pg_temp'
          ]::text[]
    ) <> 2 THEN
        RAISE EXCEPTION 'lookup function migrations lack the fixed scratch search path';
    END IF;
END
$$;
SQL
} | run_psql

{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
DO $$
DECLARE
    missing_tables text;
    unexpected_tables text;
    uncommented_tables text;
    uncommented_columns text;
    forbidden_tables text;
    forbidden_columns text;
    forbidden_projection_publication_tables text;
    raw_table_without_hash_key text;
    missing_behavioral_constraints text;
    unbounded_btree_columns text;
BEGIN
    WITH expected(table_name) AS (
        VALUES
            ('account_permission_state_current'),
            ('address_names_current'),
            ('address_records_current'),
            ('chain_heads'),
            ('chain_header_audit'),
            ('chain_lineage'),
            ('chain_phase_state'),
            ('children_current'),
            ('contract_instance_addresses'),
            ('contract_instances'),
            ('discovery_watch_admissions'),
            ('discovery_edges'),
            ('ens_names'),
            ('ingest_cursors'),
            ('interpret_decode_skips'),
            ('label_preimages'),
            ('manifest_contract_instances'),
            ('manifest_discovery_rules'),
            ('manifest_authority_attestations'),
            ('manifest_versions'),
            ('migration_candidate_discovery_effects'),
            ('migration_candidate_identity_effects'),
            ('migration_discovery_associations'),
            ('migration_event_associations'),
            ('name_current'),
            ('name_surfaces'),
            ('normalized_events'),
            ('project_generation_failures'),
            ('project_redo_child_registration_history'),
            ('project_redo_expiry_roots'),
            ('project_redo_resolver_evidence'),
            ('permissions_current'),
            ('permissions_current_resource_summary'),
            ('primary_names_current'),
            ('raw_logs'),
            ('raw_receipts'),
            ('raw_transactions'),
            ('record_inventory_current'),
            ('resolution_divergences'),
            ('resolver_current'),
            ('resources'),
            ('service_heartbeats'),
            ('surface_bindings'),
            ('token_lineages')
    )
    SELECT string_agg(expected.table_name, ', ' ORDER BY expected.table_name)
    INTO missing_tables
    FROM expected
    LEFT JOIN information_schema.tables actual
      ON actual.table_schema = current_schema()
     AND actual.table_name = expected.table_name
     AND actual.table_type = 'BASE TABLE'
    WHERE actual.table_name IS NULL;

    IF missing_tables IS NOT NULL THEN
        RAISE EXCEPTION 'missing schema-v2 tables: %', missing_tables;
    END IF;

    IF to_regclass('account_permission_state_current_active_subject_idx') IS NULL
       OR to_regclass('account_permission_state_current_applicability_idx') IS NULL
       OR to_regclass('permissions_current_resource_registry_binding_idx') IS NULL
    THEN
        RAISE EXCEPTION 'registry-operator projection indexes are incomplete';
    END IF;

    WITH expected(table_name) AS (
        VALUES
            ('account_permission_state_current'),
            ('address_names_current'),
            ('address_records_current'),
            ('chain_heads'),
            ('chain_header_audit'),
            ('chain_lineage'),
            ('chain_phase_state'),
            ('children_current'),
            ('contract_instance_addresses'),
            ('contract_instances'),
            ('discovery_watch_admissions'),
            ('discovery_edges'),
            ('ens_names'),
            ('ingest_cursors'),
            ('interpret_decode_skips'),
            ('label_preimages'),
            ('manifest_contract_instances'),
            ('manifest_discovery_rules'),
            ('manifest_authority_attestations'),
            ('manifest_versions'),
            ('migration_candidate_discovery_effects'),
            ('migration_candidate_identity_effects'),
            ('migration_discovery_associations'),
            ('migration_event_associations'),
            ('name_current'),
            ('name_surfaces'),
            ('normalized_events'),
            ('project_generation_failures'),
            ('project_redo_child_registration_history'),
            ('project_redo_expiry_roots'),
            ('project_redo_resolver_evidence'),
            ('permissions_current'),
            ('permissions_current_resource_summary'),
            ('primary_names_current'),
            ('raw_logs'),
            ('raw_receipts'),
            ('raw_transactions'),
            ('record_inventory_current'),
            ('resolution_divergences'),
            ('resolver_current'),
            ('resources'),
            ('service_heartbeats'),
            ('surface_bindings'),
            ('token_lineages')
    )
    SELECT string_agg(actual.table_name, ', ' ORDER BY actual.table_name)
    INTO unexpected_tables
    FROM information_schema.tables actual
    LEFT JOIN expected
      ON expected.table_name = actual.table_name
    WHERE actual.table_schema = current_schema()
      AND actual.table_type = 'BASE TABLE'
      AND expected.table_name IS NULL;

    IF unexpected_tables IS NOT NULL THEN
        RAISE EXCEPTION 'unexpected schema-v2 tables: %', unexpected_tables;
    END IF;

    -- Add exact exceptions only after maintainer authorization. An entry here is
    -- a carve-out under docs/adrs/0007-v1-schema-freeze.md: a table that trips the
    -- forbidden-name policy and was authorized anyway. If a schema change fails
    -- above with a forbidden-table error, that ADR is where the exception is
    -- argued, not this list.
    -- `project_generation_failures` is the contracted name of the append-only
    -- projection-generation failure audit (docs/storage.md, table ownership and
    -- "Projection publication"); it is not retention-generation state.
    WITH maintainer_authorized_allowlist(table_name) AS (
        VALUES ('project_generation_failures')
    )
    SELECT string_agg(actual.table_name, ', ' ORDER BY actual.table_name)
    INTO forbidden_tables
    FROM information_schema.tables actual
    LEFT JOIN maintainer_authorized_allowlist
      ON maintainer_authorized_allowlist.table_name = actual.table_name
    WHERE actual.table_schema = current_schema()
      AND actual.table_type = 'BASE TABLE'
      AND maintainer_authorized_allowlist.table_name IS NULL
      AND (
          actual.table_name ~
              '(coverage|backfill|lease|generation|revision)'
          OR actual.table_name ~
              '(checkpoint|frontier|queue|dead_letter|watermark)'
          OR actual.table_name ~
              '(code_hash|execution_trace|execution_step)'
          OR actual.table_name ~ '(outcome_cache|raw_call|startup)'
          OR actual.table_name ~
              '(fence|epoch|journal|promotion|reconciliation|rederive)'
          OR actual.table_name ~
              '(drift|alert|replay_version|dead_letter|watermark|staging)'
          OR actual.table_name = 'manifest_capability_flags'
      );

    IF forbidden_tables IS NOT NULL THEN
        RAISE EXCEPTION 'forbidden schema-v2 tables: %', forbidden_tables;
    END IF;

    -- Add exact exceptions only after maintainer authorization.
    WITH maintainer_authorized_allowlist(table_name, column_name) AS (
        VALUES
            ('chain_heads', 'lineage_orphaning_epoch'),
            ('discovery_watch_admissions', 'lineage_orphaning_epoch'),
            ('manifest_authority_attestations', 'generation_token'),
            ('chain_phase_state', 'redo_attempt_generation')
    )
    SELECT string_agg(
        format('%I.%I', actual.table_name, actual.column_name),
        ', '
        ORDER BY actual.table_name, actual.ordinal_position
    )
    INTO forbidden_columns
    FROM information_schema.columns actual
    LEFT JOIN maintainer_authorized_allowlist
      ON maintainer_authorized_allowlist.table_name = actual.table_name
     AND maintainer_authorized_allowlist.column_name = actual.column_name
    WHERE actual.table_schema = current_schema()
      AND maintainer_authorized_allowlist.column_name IS NULL
      AND (
          actual.column_name ~
              '(coverage|exhaustiveness|generation|revision)'
          OR actual.column_name ~ '(supersed|repair|capability)'
          OR actual.column_name ~
              '(fence|epoch|journal|promotion|reconciliation|rederive)'
          OR actual.column_name ~
              '(drift|alert|replay_version|dead_letter|watermark|staging)'
          OR actual.column_name = 'code_hash'
      );

    IF forbidden_columns IS NOT NULL THEN
        RAISE EXCEPTION 'forbidden schema-v2 columns: %', forbidden_columns;
    END IF;

    SELECT string_agg(table_name, ', ' ORDER BY table_name)
    INTO forbidden_projection_publication_tables
    FROM information_schema.tables
    WHERE table_schema = current_schema()
      AND table_type = 'BASE TABLE'
      AND table_name ~ '(_staging|_publication)$';

    IF forbidden_projection_publication_tables IS NOT NULL THEN
        RAISE EXCEPTION
            'schema-v2 contains forbidden projection publication tables: %',
            forbidden_projection_publication_tables;
    END IF;

    SELECT string_agg(class.relname, ', ' ORDER BY class.relname)
    INTO uncommented_tables
    FROM pg_class class
    JOIN pg_namespace namespace
      ON namespace.oid = class.relnamespace
    WHERE namespace.nspname = current_schema()
      AND class.relkind = 'r'
      AND obj_description(class.oid, 'pg_class') IS NULL;

    IF uncommented_tables IS NOT NULL THEN
        RAISE EXCEPTION 'schema-v2 tables without comments: %', uncommented_tables;
    END IF;

    SELECT string_agg(
        format('%I.%I', class.relname, attribute.attname),
        ', '
        ORDER BY class.relname, attribute.attnum
    )
    INTO uncommented_columns
    FROM pg_class class
    JOIN pg_namespace namespace
      ON namespace.oid = class.relnamespace
    JOIN pg_attribute attribute
      ON attribute.attrelid = class.oid
    WHERE namespace.nspname = current_schema()
      AND class.relkind = 'r'
      AND attribute.attnum > 0
      AND NOT attribute.attisdropped
      AND col_description(class.oid, attribute.attnum) IS NULL;

    IF uncommented_columns IS NOT NULL THEN
        RAISE EXCEPTION 'schema-v2 columns without comments: %', uncommented_columns;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'normalized_events'
          AND column_name ~ '(repair|supersed|authority|revision|generation)'
    ) THEN
        RAISE EXCEPTION 'normalized_events contains repair apparatus';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name IN (
              'name_current',
              'address_names_current',
              'address_records_current',
              'permissions_current',
              'account_permission_state_current',
              'permissions_current_resource_summary',
              'record_inventory_current',
              'resolver_current',
              'children_current',
              'primary_names_current'
          )
          AND column_name IN (
              'coverage',
              'exhaustiveness',
              'enumeration_basis',
              'explicit_gaps'
          )
    ) THEN
        RAISE EXCEPTION 'a current projection contains exhaustiveness accounting';
    END IF;

    WITH required(table_name) AS (
        VALUES
            ('chain_header_audit'),
            ('raw_logs'),
            ('raw_receipts'),
            ('raw_transactions')
    )
    SELECT string_agg(required.table_name, ', ' ORDER BY required.table_name)
    INTO raw_table_without_hash_key
    FROM required
    WHERE NOT EXISTS (
        SELECT 1
        FROM pg_constraint constraint_row
        JOIN pg_class class
          ON class.oid = constraint_row.conrelid
        JOIN pg_namespace namespace
          ON namespace.oid = class.relnamespace
        WHERE namespace.nspname = current_schema()
          AND class.relname = required.table_name
          AND constraint_row.contype = 'p'
          AND EXISTS (
              SELECT 1
              FROM unnest(constraint_row.conkey) AS key_column(attnum)
              JOIN pg_attribute attribute
                ON attribute.attrelid = class.oid
               AND attribute.attnum = key_column.attnum
              WHERE attribute.attname = 'block_hash'
          )
    );

    IF raw_table_without_hash_key IS NOT NULL THEN
        RAISE EXCEPTION
            'raw tables without block_hash primary keys: %',
            raw_table_without_hash_key;
    END IF;

    WITH invariant_checks(invariant_name, is_present) AS (
        SELECT
            'canonical surface bindings cannot overlap',
            EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'surface_bindings'::regclass
                  AND conname = 'surface_bindings_no_overlap'
                  AND contype = 'x'
            )
        UNION ALL
        SELECT
            'one address row is active per contract instance',
            EXISTS (
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = current_schema()
                  AND indexname =
                      'contract_instance_addresses_active_instance_idx'
                  AND indexdef LIKE 'CREATE UNIQUE INDEX%'
                  AND indexdef LIKE '%(contract_instance_id)%'
                  AND indexdef LIKE '%deactivated_at IS NULL%'
            )
        UNION ALL
        SELECT
            'manifest contract declarations cascade on manifest deletion',
            EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'manifest_contract_instances'::regclass
                  AND conname =
                      'manifest_contract_instances_manifest_fkey'
                  AND contype = 'f'
                  AND confdeltype = 'c'
            )
        UNION ALL
        SELECT
            'manifest discovery rules cascade on manifest deletion',
            EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'manifest_discovery_rules'::regclass
                  AND conname = 'manifest_discovery_rules_manifest_fkey'
                  AND contype = 'f'
                  AND confdeltype = 'c'
            )
        UNION ALL
        SELECT
            'migration event associations retain missing normalized-event parents',
            NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'migration_event_associations'::regclass
                  AND confrelid = 'normalized_events'::regclass
                  AND contype = 'f'
            )
        UNION ALL
        SELECT
            'receipt transaction positions match their transactions',
            EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'raw_receipts'::regclass
                  AND conname = 'raw_receipts_transaction_position_fkey'
                  AND contype = 'f'
            )
        UNION ALL
        SELECT
            'log transaction positions match their transactions',
            EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'raw_logs'::regclass
                  AND conname = 'raw_logs_transaction_position_fkey'
                  AND contype = 'f'
            )
        UNION ALL
        SELECT
            'chain lineage canonicality transitions are constrained',
            EXISTS (
                SELECT 1
                FROM pg_trigger
                WHERE tgrelid = 'chain_lineage'::regclass
                  AND tgname =
                      'chain_lineage_enforce_canonicality_transition'
                  AND NOT tgisinternal
            )
        UNION ALL
        SELECT
            'chain heads carry a nonnegative lineage orphaning epoch',
            EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'chain_heads'
                  AND column_name = 'lineage_orphaning_epoch'
                  AND data_type = 'bigint'
                  AND is_nullable = 'NO'
                  AND column_default IN ('0', '0::bigint')
            )
            AND EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'chain_heads'::regclass
                  AND conname = 'chain_heads_lineage_orphaning_epoch_check'
                  AND contype = 'c'
            )
        UNION ALL
        SELECT
            'divergence writes guard the compared record inventory row',
            EXISTS (
                SELECT 1
                FROM pg_proc procedure
                JOIN pg_namespace namespace
                  ON namespace.oid = procedure.pronamespace
                WHERE namespace.nspname = current_schema()
                  AND procedure.proname = 'write_resolution_divergence'
                  AND pg_get_function_identity_arguments(procedure.oid) =
                      'compared_resource_id uuid, compared_boundary_key text, compared_row_xmin text, requested_authoritative_chain_id text, requested_authoritative_block_number bigint, requested_authoritative_block_hash text, compared_execution_authority jsonb, requested_logical_name_id text, requested_resolver_chain_id text, requested_resolver_address text, requested_record_key text, compared_positions jsonb, live_answer jsonb, used_ccip_read boolean'
            )
        UNION ALL
        SELECT
            format('%s carries its closed vocabulary', required.table_name),
            EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = to_regclass(required.table_name)
                  AND conname = required.constraint_name
                  AND contype = 'c'
            )
        FROM (
            VALUES
                (
                    'contract_instances',
                    'contract_instances_contract_kind_check'
                ),
                (
                    'discovery_edges',
                    'discovery_edges_edge_kind_check'
                ),
                (
                    'manifest_discovery_rules',
                    'manifest_discovery_rules_edge_kind_check'
                ),
                (
                    'surface_bindings',
                    'surface_bindings_binding_kind_check'
                ),
                ('name_current', 'name_current_binding_kind_check'),
                (
                    'address_names_current',
                    'address_names_current_binding_kind_check'
                ),
                (
                    'normalized_events',
                    'normalized_events_event_kind_check'
                ),
                (
                    'normalized_events',
                    'normalized_events_derivation_kind_check'
                ),
                (
                    'normalized_events',
                    'normalized_events_migration_correlation_ids_check'
                ),
                (
                    'normalized_events',
                    'normalized_events_consumer_visibility_check'
                ),
                (
                    'normalized_events',
                    'normalized_events_candidate_correlation_check'
                ),
                (
                    'permissions_current',
                    'permissions_current_scope_kind_check'
                ),
                (
                    'address_names_current',
                    'address_names_current_relation_check'
                )
        ) AS required(table_name, constraint_name)
        UNION ALL
        SELECT
            format('%s filters readable history', required.index_name),
            EXISTS (
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = current_schema()
                  AND indexname = required.index_name
                  AND indexdef LIKE '% WHERE %'
                  AND indexdef LIKE '%canonicality_state%'
                  AND indexdef LIKE '%''canonical''%'
                  AND indexdef LIKE '%''safe''%'
                  AND indexdef LIKE '%''finalized''%'
            )
        FROM (
            VALUES
                ('surface_bindings_name_idx'),
                ('surface_bindings_resource_idx'),
                ('normalized_events_name_history_idx'),
                ('normalized_events_resource_history_idx'),
                ('normalized_events_subregistry_registration_history_idx')
        ) AS required(index_name)
        UNION ALL
        SELECT
            format('%s has the reviewed delta-driven definition', required.index_name),
            EXISTS (
                SELECT 1
                FROM pg_class AS index_relation
                JOIN pg_namespace AS namespace
                  ON namespace.oid = index_relation.relnamespace
                JOIN pg_index AS index_state
                  ON index_state.indexrelid = index_relation.oid
                JOIN pg_class AS table_relation
                  ON table_relation.oid = index_state.indrelid
                JOIN pg_am AS access_method
                  ON access_method.oid = index_relation.relam
                WHERE namespace.nspname = current_schema()
                  AND index_relation.relname = required.index_name
                  AND table_relation.relname = required.table_name
                  AND access_method.amname = 'btree'
                  AND index_state.indisvalid
                  AND index_state.indisready
                  AND index_state.indislive
                  AND index_state.indnatts = index_state.indnkeyatts
                  AND index_state.indnkeyatts =
                      cardinality(required.key_patterns)
                  AND NOT EXISTS (
                      SELECT 1
                      FROM generate_subscripts(required.key_patterns, 1)
                          AS key(ordinal)
                      WHERE pg_get_indexdef(
                                index_relation.oid,
                                key.ordinal,
                                true
                            ) NOT LIKE required.key_patterns[key.ordinal]
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM generate_subscripts(required.key_patterns, 1)
                          AS key(ordinal)
                      WHERE pg_index_column_has_property(
                                index_relation.oid,
                                key.ordinal,
                                'desc'
                            ) IS DISTINCT FROM (
                                required.index_name IN (
                                    'normalized_events_resolver_alias_history_idx',
                                    'normalized_events_resolver_upgrade_history_idx',
                                    'normalized_events_subregistry_registration_history_idx'
                                )
                                AND key.ordinal IN (3, 4)
                            )
                  )
                  AND CASE
                      WHEN required.predicate_patterns IS NULL
                          THEN index_state.indpred IS NULL
                      ELSE index_state.indpred IS NOT NULL
                           AND NOT EXISTS (
                               SELECT 1
                               FROM generate_subscripts(
                                   required.predicate_patterns,
                                   1
                               ) AS predicate(ordinal)
                               WHERE pg_get_expr(
                                         index_state.indpred,
                                         index_state.indrelid,
                                         true
                                     ) NOT LIKE
                                     required.predicate_patterns[predicate.ordinal]
                           )
                  END
            )
        FROM (
            VALUES
                (
                    'normalized_events_chain_block_number_idx',
                    'normalized_events',
                    ARRAY['chain_id', 'block_number'],
                    NULL::text[]
                ),
                (
                    'normalized_events_resolver_alias_history_idx',
                    'normalized_events',
                    ARRAY[
                        'chain_id',
                        '%lower%COALESCE%after_state%resolver%before_state%resolver%raw_fact_ref%emitting_address%',
                        'block_number',
                        'normalized_event_id'
                    ],
                    ARRAY[
                        '%event_kind%AliasChanged%',
                        '%canonicality_state%canonical%safe%finalized%'
                    ]
                ),
                (
                    'normalized_events_resolver_upgrade_history_idx',
                    'normalized_events',
                    ARRAY[
                        'chain_id',
                        '%lower%after_state%proxy_address%',
                        'block_number',
                        'normalized_event_id'
                    ],
                    ARRAY[
                        '%event_kind%Upgraded%',
                        '%canonicality_state%canonical%safe%finalized%'
                    ]
                ),
                (
                    'normalized_events_subregistry_registration_history_idx',
                    'normalized_events',
                    ARRAY[
                        'chain_id',
                        '%after_state%registry_contract_instance_id%',
                        'block_number',
                        'normalized_event_id',
                        'logical_name_id'
                    ],
                    ARRAY[
                        '%event_kind%RegistrationGranted%RegistrationReserved%RegistrationRenewed%RegistrationReleased%',
                        '%source_family%ens_v2_root_l1%ens_v2_registry_l1%',
                        '%canonicality_state%canonical%safe%finalized%',
                        '%logical_name_id%IS NOT NULL%',
                        '%after_state%registry_contract_instance_id%IS NOT NULL%'
                    ]
                ),
                (
                    'name_surfaces_chain_block_number_idx',
                    'name_surfaces',
                    ARRAY['chain_id', 'block_number'],
                    NULL
                ),
                (
                    'surface_bindings_chain_block_number_idx',
                    'surface_bindings',
                    ARRAY['chain_id', 'block_number'],
                    NULL
                ),
                (
                    'resources_chain_block_number_idx',
                    'resources',
                    ARRAY['chain_id', 'block_number'],
                    NULL
                ),
                (
                    'children_current_labelhash_idx',
                    'children_current',
                    ARRAY[
                        'namespace',
                        'lower(labelhash)',
                        'parent_logical_name_id',
                        'child_logical_name_id'
                    ],
                    NULL
                ),
                (
                    'name_current_resolver_idx',
                    'name_current',
                    ARRAY[
                        '%declared_summary%resolver%chain_id%',
                        '%lower%declared_summary%resolver%address%',
                        'logical_name_id'
                    ],
                    ARRAY['%declared_summary%resolver%address%IS NOT NULL%']
                ),
                (
                    'permissions_current_resolver_scope_idx',
                    'permissions_current',
                    ARRAY[
                        '%scope_detail%chain_id%',
                        '%lower%scope_detail%resolver_address%',
                        'resource_id'
                    ],
                    ARRAY[
                        '%scope_kind%resolver%',
                        '%scope_detail%resolver_address%IS NOT NULL%'
                    ]
                ),
                (
                    'record_inventory_current_resolver_idx',
                    'record_inventory_current',
                    ARRAY[
                        '%provenance%chain_id%',
                        '%lower%provenance%resolver_address%',
                        'resource_id'
                    ],
                    ARRAY['%provenance%resolver_address%IS NOT NULL%']
                ),
                (
                    'primary_names_current_reverse_node_idx',
                    'primary_names_current',
                    ARRAY[
                        '%claim_provenance%chain_id%',
                        '%lower%claim_provenance%reverse_node%',
                        'address',
                        'coin_type',
                        'namespace'
                    ],
                    ARRAY['%claim_provenance%reverse_node%IS NOT NULL%']
                ),
                (
                    'permissions_current_resource_wrapper_expiry_idx',
                    'permissions_current_resource_summary',
                    ARRAY[
                        '%provenance%chain_id%',
                        '%provenance%wrapper_expiry_boundary%expiry_seconds%numeric%',
                        'resource_id'
                    ],
                    ARRAY['%provenance%wrapper_expiry_boundary%']
                )
        ) AS required(
            index_name,
            table_name,
            key_patterns,
            predicate_patterns
        )
        UNION ALL
        SELECT
            'normalized event state compaction has its expression index',
            EXISTS (
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = current_schema()
                  AND indexname =
                      'normalized_events_interpreter_state_history_idx'
                  AND indexdef LIKE '%raw_fact_ref%interpreter_state_key%'
                  AND indexdef LIKE '%digest%sha256%'
                  AND indexdef LIKE '%canonicality_state%'
            )
        UNION ALL
        SELECT
            'name surface visibility requires an explicit decision',
            NOT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'name_surfaces'
                  AND column_name = 'visibility_state'
                  AND column_default IS NOT NULL
            )
        UNION ALL
        SELECT
            'manifest deployment label has an unambiguous storage name',
            EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'manifest_versions'
                  AND column_name = 'deployment_label'
            )
            AND NOT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'manifest_versions'
                  AND column_name = 'deployment_id'
            )
        UNION ALL
        SELECT
            'record inventory has no primary-key duplicate index',
            NOT EXISTS (
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = current_schema()
                  AND indexname =
                      'record_inventory_current_resource_idx'
            )
        UNION ALL
        SELECT
            'children raw labels preserve bytes with optional exact text',
            EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'children_current'
                  AND column_name = 'raw_label'
                  AND data_type = 'bytea'
                  AND is_nullable = 'YES'
            )
            AND EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'children_current'
                  AND column_name = 'decoded_label'
                  AND data_type = 'text'
                  AND is_nullable = 'YES'
            )
            AND EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'children_current'::regclass
                  AND conname =
                      'children_current_decoded_label_matches_raw_check'
                  AND contype = 'c'
            )
            AND EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'children_current'::regclass
                  AND conname =
                      'children_current_decoded_label_requires_raw_check'
                  AND contype = 'c'
            )
            AND EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'children_current'
                  AND column_name = 'labelhash'
                  AND is_nullable = 'NO'
            )
        UNION ALL
        SELECT
            'children raw names preserve bytes with optional exact text',
            EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'children_current'
                  AND column_name = 'raw_name'
                  AND data_type = 'bytea'
                  AND is_nullable = 'YES'
            )
            AND EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'children_current'
                  AND column_name = 'decoded_name'
                  AND data_type = 'text'
                  AND is_nullable = 'YES'
            )
            AND EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'children_current'::regclass
                  AND conname =
                      'children_current_decoded_name_matches_raw_check'
                  AND contype = 'c'
            )
            AND EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'children_current'::regclass
                  AND conname =
                      'children_current_decoded_name_requires_raw_check'
                  AND contype = 'c'
            )
    )
    SELECT string_agg(invariant_name, ', ' ORDER BY invariant_name)
    INTO missing_behavioral_constraints
    FROM invariant_checks
    WHERE NOT is_present;

    IF missing_behavioral_constraints IS NOT NULL THEN
        RAISE EXCEPTION
            'missing schema-v2 behavioral constraints: %',
            missing_behavioral_constraints;
    END IF;

    WITH unbounded_input(table_name, column_name) AS (
        VALUES
            ('label_preimages', 'raw_label'),
            ('label_preimages', 'decoded_label'),
            ('ens_names', 'name'),
            ('name_surfaces', 'raw_name'),
            ('name_surfaces', 'raw_labels'),
            ('name_surfaces', 'dns_encoded_name'),
            ('name_surfaces', 'normalization_errors'),
            ('normalized_events', 'before_state'),
            ('normalized_events', 'after_state'),
            ('name_current', 'raw_name'),
            ('children_current', 'raw_name'),
            ('children_current', 'decoded_name'),
            ('children_current', 'raw_label'),
            ('children_current', 'decoded_label'),
            ('record_inventory_current', 'selectors'),
            ('record_inventory_current', 'unsupported_families'),
            ('record_inventory_current', 'last_change'),
            ('record_inventory_current', 'entries'),
            ('address_names_current', 'raw_name'),
            ('primary_names_current', 'raw_claim_name'),
            ('resolution_divergences', 'request_kind')
    )
    SELECT string_agg(
        format('%I.%I in %I', input.table_name, input.column_name, index.oid::regclass),
        ', '
        ORDER BY input.table_name, input.column_name, index.relname
    )
    INTO unbounded_btree_columns
    FROM unbounded_input input
    JOIN pg_namespace namespace
      ON namespace.nspname = current_schema()
    JOIN pg_class relation
      ON relation.relnamespace = namespace.oid
     AND relation.relname = input.table_name
    JOIN pg_attribute attribute
      ON attribute.attrelid = relation.oid
     AND attribute.attname = input.column_name
    JOIN pg_index indexed
      ON indexed.indrelid = relation.oid
     AND attribute.attnum = ANY(indexed.indkey::smallint[])
    JOIN pg_class index ON index.oid = indexed.indexrelid
    JOIN pg_am access_method ON access_method.oid = index.relam
    WHERE access_method.amname = 'btree';

    IF unbounded_btree_columns IS NOT NULL THEN
        RAISE EXCEPTION
            'unbounded externally controlled columns remain in btree indexes: %',
            unbounded_btree_columns;
    END IF;
END
$$;

CREATE FUNCTION assert_exact_zero_migration_behavior() RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    zero_case text;
    zero_step integer;
    zero_expected jsonb;
    zero_live jsonb;
    zero_row jsonb;
    zero_written jsonb;
    zero_cleared jsonb;
    manifest_key bigint;
    transition_case record;
    violated_constraint text;
    accepted_projection_identity_mismatches text[] := ARRAY[]::text[];
    accepted_cross_chain_relationships text[] := ARRAY[]::text[];
    accepted_head_invariants text[] := ARRAY[]::text[];
    accepted_projection_relationship_mismatches text[] := ARRAY[]::text[];
    accepted_manifest_mismatches text[] := ARRAY[]::text[];
    accepted_divergence_canonicality text[] := ARRAY[]::text[];
    divergence_guard_xmin text;
    divergence_execution_authority jsonb;
    divergence_write_status text;
    divergence_count bigint;
    oversized_text text;
    oversized_bytes bytea;
BEGIN
    SELECT string_agg(md5(chunk::text), '' ORDER BY chunk)
    INTO oversized_text
    FROM generate_series(1, 96) AS chunks(chunk);
    oversized_bytes := convert_to(oversized_text, 'UTF8');
    IF octet_length(oversized_bytes) <= 2704 THEN
        RAISE EXCEPTION 'oversized-input probe did not exceed the btree row limit';
    END IF;

    INSERT INTO chain_lineage (
        chain_id,
        block_hash,
        block_number,
        block_timestamp,
        canonicality_state
    )
    VALUES
        (
            'schema-v2-check',
            'block-0',
            0,
            '2026-01-01 00:00:00+00',
            'canonical'
        ),
        (
            'schema-v2-check',
            'block-1',
            1,
            '2026-01-01 00:00:01+00',
            'canonical'
        ),
        (
            'schema-v2-other',
            'block-other-0',
            0,
            '2026-01-01 00:00:00+00',
            'canonical'
        ),
        (
            'schema-v2-other',
            'orphaned-block-1',
            1,
            '2026-01-01 00:00:01+00',
            'orphaned'
        );

    FOR transition_case IN
        SELECT *
        FROM (
            VALUES
                ('observed', 'safe'),
                ('observed', 'finalized'),
                ('canonical', 'observed'),
                ('canonical', 'finalized'),
                ('safe', 'observed'),
                ('safe', 'canonical'),
                ('orphaned', 'observed'),
                ('orphaned', 'safe'),
                ('orphaned', 'finalized'),
                ('finalized', 'observed'),
                ('finalized', 'canonical'),
                ('finalized', 'safe'),
                ('finalized', 'orphaned')
        ) AS illegal(from_state, to_state)
    LOOP
        INSERT INTO chain_lineage (
            chain_id,
            block_hash,
            block_number,
            block_timestamp,
            canonicality_state
        )
        VALUES (
            format(
                'schema-v2-illegal-%s-%s',
                transition_case.from_state,
                transition_case.to_state
            ),
            'transition-block',
            0,
            '2026-01-01 00:00:00+00',
            transition_case.from_state::canonicality_state
        );

        BEGIN
            UPDATE chain_lineage
            SET canonicality_state =
                transition_case.to_state::canonicality_state
            WHERE chain_id = format(
                'schema-v2-illegal-%s-%s',
                transition_case.from_state,
                transition_case.to_state
            )
              AND block_hash = 'transition-block';
            RAISE EXCEPTION
                'chain_lineage accepted illegal canonicality transition % -> %',
                transition_case.from_state,
                transition_case.to_state;
        EXCEPTION
            WHEN check_violation THEN
                IF SQLERRM <> format(
                    'illegal chain lineage canonicality transition: %s -> %s',
                    transition_case.from_state,
                    transition_case.to_state
                ) THEN
                    RAISE;
                END IF;
        END;

        DELETE FROM chain_lineage
        WHERE chain_id = format(
            'schema-v2-illegal-%s-%s',
            transition_case.from_state,
            transition_case.to_state
        )
          AND block_hash = 'transition-block';
    END LOOP;

    FOR transition_case IN
        SELECT *
        FROM (
            VALUES
                ('observed', 'canonical'),
                ('observed', 'orphaned'),
                ('canonical', 'safe'),
                ('canonical', 'orphaned'),
                ('safe', 'finalized'),
                ('safe', 'orphaned'),
                ('orphaned', 'canonical')
        ) AS legal(from_state, to_state)
    LOOP
        INSERT INTO chain_lineage (
            chain_id,
            block_hash,
            block_number,
            block_timestamp,
            canonicality_state
        )
        VALUES (
            format(
                'schema-v2-legal-%s-%s',
                transition_case.from_state,
                transition_case.to_state
            ),
            'transition-block',
            0,
            '2026-01-01 00:00:00+00',
            transition_case.from_state::canonicality_state
        );

        UPDATE chain_lineage
        SET canonicality_state =
            transition_case.to_state::canonicality_state
        WHERE chain_id = format(
            'schema-v2-legal-%s-%s',
            transition_case.from_state,
            transition_case.to_state
        )
          AND block_hash = 'transition-block';

        IF NOT EXISTS (
            SELECT 1
            FROM chain_lineage
            WHERE chain_id = format(
                'schema-v2-legal-%s-%s',
                transition_case.from_state,
                transition_case.to_state
            )
              AND block_hash = 'transition-block'
              AND canonicality_state =
                  transition_case.to_state::canonicality_state
        ) THEN
            RAISE EXCEPTION
                'chain_lineage did not apply legal canonicality transition % -> %',
                transition_case.from_state,
                transition_case.to_state;
        END IF;

        DELETE FROM chain_lineage
        WHERE chain_id = format(
            'schema-v2-legal-%s-%s',
            transition_case.from_state,
            transition_case.to_state
        )
          AND block_hash = 'transition-block';
    END LOOP;

    INSERT INTO chain_lineage (
        chain_id,
        block_hash,
        block_number,
        block_timestamp,
        canonicality_state,
        first_observed_at,
        canonicality_updated_at
    )
    VALUES (
        'schema-v2-transition-timestamp',
        'transition-block',
        0,
        '2026-01-01 00:00:00+00',
        'observed',
        '2026-01-01 00:00:00+00',
        '2026-01-01 00:00:00+00'
    );

    UPDATE chain_lineage
    SET canonicality_state = 'canonical'
    WHERE chain_id = 'schema-v2-transition-timestamp'
      AND block_hash = 'transition-block';

    IF (
        SELECT canonicality_updated_at
        FROM chain_lineage
        WHERE chain_id = 'schema-v2-transition-timestamp'
          AND block_hash = 'transition-block'
    ) <= TIMESTAMPTZ '2026-01-01 00:00:00+00'
    THEN
        RAISE EXCEPTION
            'chain_lineage did not timestamp its canonicality transition';
    END IF;

    DELETE FROM chain_lineage
    WHERE chain_id = 'schema-v2-transition-timestamp'
      AND block_hash = 'transition-block';

    INSERT INTO chain_lineage (
        chain_id,
        block_hash,
        parent_hash,
        block_number,
        block_timestamp,
        canonicality_state
    )
    VALUES
        (
            'schema-v2-checkpoint-jump',
            'jump-block-0',
            NULL,
            0,
            '2026-01-01 00:00:00+00',
            'observed'
        ),
        (
            'schema-v2-checkpoint-jump',
            'jump-block-1',
            'jump-block-0',
            1,
            '2026-01-01 00:00:01+00',
            'observed'
        ),
        (
            'schema-v2-checkpoint-jump',
            'jump-block-2',
            'jump-block-1',
            2,
            '2026-01-01 00:00:02+00',
            'observed'
        ),
        (
            'schema-v2-checkpoint-jump',
            'jump-block-3',
            'jump-block-2',
            3,
            '2026-01-01 00:00:03+00',
            'observed'
        );

    UPDATE chain_lineage
    SET canonicality_state = 'canonical'
    WHERE chain_id = 'schema-v2-checkpoint-jump';

    UPDATE chain_lineage
    SET canonicality_state = 'safe'
    WHERE chain_id = 'schema-v2-checkpoint-jump'
      AND block_number <= 2;

    UPDATE chain_lineage
    SET canonicality_state = 'finalized'
    WHERE chain_id = 'schema-v2-checkpoint-jump'
      AND block_number <= 1;

    INSERT INTO chain_heads (
        chain_id,
        latest_block_hash,
        latest_block_number,
        safe_block_hash,
        safe_block_number,
        finalized_block_hash,
        finalized_block_number
    )
    VALUES (
        'schema-v2-checkpoint-jump',
        'jump-block-3',
        3,
        'jump-block-2',
        2,
        'jump-block-1',
        1
    );

    IF (
        SELECT lineage_orphaning_epoch
        FROM chain_heads
        WHERE chain_id = 'schema-v2-checkpoint-jump'
    ) <> 0 THEN
        RAISE EXCEPTION 'chain head orphaning epoch did not start at zero';
    END IF;

    BEGIN
        UPDATE chain_heads
        SET lineage_orphaning_epoch = -1
        WHERE chain_id = 'schema-v2-checkpoint-jump';
        RAISE EXCEPTION 'chain head accepted a negative orphaning epoch';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "chain_heads_lineage_orphaning_epoch_check"%'
            THEN
                RAISE;
            END IF;
    END;

    IF (
        SELECT array_agg(
            canonicality_state::text
            ORDER BY block_number
        )
        FROM chain_lineage
        WHERE chain_id = 'schema-v2-checkpoint-jump'
    ) <> ARRAY['finalized', 'finalized', 'safe', 'canonical']
    THEN
        RAISE EXCEPTION
            'ordered checkpoint jump did not preserve adjacent canonicality transitions';
    END IF;

    BEGIN
        INSERT INTO chain_heads (
            chain_id,
            latest_block_hash,
            latest_block_number
        )
        VALUES (
            'schema-v2-other',
            'orphaned-block-1',
            1
        );
        accepted_head_invariants :=
            array_append(accepted_head_invariants, 'orphaned head');
        DELETE FROM chain_heads
        WHERE chain_id = 'schema-v2-other';
    EXCEPTION
        WHEN check_violation OR foreign_key_violation THEN
            IF SQLERRM <>
                'latest head must reference a canonical chain block'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO chain_lineage (
            chain_id,
            block_hash,
            block_number,
            block_timestamp,
            canonicality_state
        )
        VALUES (
            'schema-v2-check',
            'competing-block-0',
            0,
            '2026-01-01 00:00:00+00',
            'canonical'
        );
        accepted_head_invariants :=
            array_append(
                accepted_head_invariants,
                'multiple canonical blocks at one height'
            );
        DELETE FROM chain_lineage
        WHERE chain_id = 'schema-v2-check'
          AND block_hash = 'competing-block-0';
    EXCEPTION
        WHEN unique_violation THEN
            IF SQLERRM NOT LIKE
                '%chain_lineage_readable_height_idx%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO chain_heads (
        chain_id,
        latest_block_hash,
        latest_block_number
    )
    VALUES (
        'schema-v2-check',
        'block-0',
        0
    );

    BEGIN
        UPDATE chain_lineage
        SET canonicality_state = 'orphaned'
        WHERE chain_id = 'schema-v2-check'
          AND block_hash = 'block-0';
        accepted_head_invariants :=
            array_append(
                accepted_head_invariants,
                'head demoted to orphaned'
            );
        UPDATE chain_lineage
        SET canonicality_state = 'canonical'
        WHERE chain_id = 'schema-v2-check'
          AND block_hash = 'block-0';
    EXCEPTION
        WHEN check_violation OR foreign_key_violation THEN
            IF SQLERRM <>
                'a chain head still references this block state'
            THEN
                RAISE;
            END IF;
    END;

    IF cardinality(accepted_head_invariants) > 0 THEN
        RAISE EXCEPTION
            'chain tables accepted invalid head state: %',
            array_to_string(accepted_head_invariants, ', ');
    END IF;

    INSERT INTO chain_phase_state (
        chain_id,
        phase_name,
        phase_status,
        verification_level
    )
    VALUES (
        'schema-v2-check',
        'verify',
        'idle',
        'quick_synced'
    );

    UPDATE chain_phase_state
    SET verification_level = 'cross_checked'
    WHERE chain_id = 'schema-v2-check'
      AND phase_name = 'verify';

    UPDATE chain_phase_state
    SET verification_level = 'node_checked'
    WHERE chain_id = 'schema-v2-check'
      AND phase_name = 'verify';

    INSERT INTO chain_phase_state (
        chain_id,
        phase_name
    )
    VALUES (
        'schema-v2-redo-failure',
        'interpret'
    );

    UPDATE chain_phase_state
    SET phase_status = 'running',
        redo_in_progress = true,
        redo_mode = 'redo',
        redo_previous_phase_status = 'idle',
        redo_from_block_number = 0,
        redo_to_block_number = 1,
        last_error = 'deterministic redo failure',
        started_at = now()
    WHERE chain_id = 'schema-v2-redo-failure'
      AND phase_name = 'interpret';

    BEGIN
        INSERT INTO chain_phase_state (
            chain_id,
            phase_name,
            phase_status,
            last_error,
            started_at
        )
        VALUES (
            'schema-v2-invalid-running-error',
            'interpret',
            'running',
            'invalid normal-run error',
            now()
        );
        RAISE EXCEPTION
            'a normal running phase accepted last_error';
    EXCEPTION
        WHEN check_violation THEN
            NULL;
    END;

    DELETE FROM chain_phase_state
    WHERE chain_id = 'schema-v2-redo-failure'
      AND phase_name = 'interpret';

    BEGIN
        INSERT INTO chain_phase_state (
            chain_id,
            phase_name,
            phase_status,
            verification_level
        )
        VALUES (
            'schema-v2-check',
            'ingest',
            'idle',
            'quick_synced'
        );
        RAISE EXCEPTION
            'a non-verify phase accepted a verification level';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "chain_phase_state_verification_phase_check"%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO chain_phase_state (
        chain_id,
        phase_name,
        phase_status,
        started_at
    )
    VALUES (
        'schema-v2-check',
        'ingest',
        'paused',
        now()
    );

    DELETE FROM chain_phase_state
    WHERE chain_id = 'schema-v2-check'
      AND phase_name = 'ingest';

    INSERT INTO raw_transactions (
        chain_id,
        block_hash,
        block_number,
        transaction_hash,
        transaction_index,
        from_address
    )
    VALUES
        ('schema-v2-check', 'block-0', 0, 'tx-0', 0, 'address-0'),
        ('schema-v2-check', 'block-0', 0, 'tx-1', 1, 'address-1');

    BEGIN
        INSERT INTO raw_receipts (
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index
        )
        VALUES ('schema-v2-check', 'block-0', 0, 'tx-0', 1);
        RAISE EXCEPTION
            'raw_receipts accepted a mismatched transaction position';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "raw_receipts_transaction_position_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO raw_logs (
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            log_index,
            emitting_address
        )
        VALUES (
            'schema-v2-check',
            'block-0',
            0,
            'tx-0',
            1,
            0,
            'address-0'
        );
        RAISE EXCEPTION
            'raw_logs accepted a mismatched transaction position';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "raw_logs_transaction_position_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO contract_instances (
        contract_instance_id,
        chain_id,
        contract_kind
    )
    VALUES (
        '00000000-0000-0000-0000-000000000001',
        'schema-v2-check',
        'root'
    );

    INSERT INTO contract_instances (
        contract_instance_id,
        chain_id,
        contract_kind
    )
    VALUES (
        '00000000-0000-0000-0000-000000000002',
        'schema-v2-check',
        'contract'
    );

    INSERT INTO contract_instances (
        contract_instance_id,
        chain_id,
        contract_kind
    )
    VALUES (
        '00000000-0000-0000-0000-000000000003',
        'schema-v2-other',
        'root'
    );

    BEGIN
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address
        )
        VALUES (
            '00000000-0000-0000-0000-000000000002',
            'schema-v2-other',
            'cross-chain-address'
        );
        RAISE EXCEPTION
            'a contract address accepted a different instance chain';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "contract_instance_addresses_instance_chain_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO discovery_edges (
            chain_id,
            edge_kind,
            from_contract_instance_id,
            to_contract_instance_id,
            discovery_source,
            admission_basis
        )
        VALUES (
            'schema-v2-other',
            'proxy_implementation',
            '00000000-0000-0000-0000-000000000001',
            '00000000-0000-0000-0000-000000000002',
            'schema-v2-check',
            'schema-v2-check'
        );
        RAISE EXCEPTION
            'a discovery edge accepted different endpoint chains';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "discovery_edges_from_instance_chain_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO discovery_edges (
        chain_id,
        edge_kind,
        from_contract_instance_id,
        to_contract_instance_id,
        discovery_source,
        admission_basis
    )
    VALUES (
        'schema-v2-check',
        'registry_announcement',
        '00000000-0000-0000-0000-000000000001',
        '00000000-0000-0000-0000-000000000001',
        'RegistryCreated',
        'schema-v2-check'
    );

    BEGIN
        INSERT INTO discovery_edges (
            chain_id,
            edge_kind,
            from_contract_instance_id,
            to_contract_instance_id,
            discovery_source,
            admission_basis
        )
        VALUES (
            'schema-v2-check',
            'resolver',
            '00000000-0000-0000-0000-000000000001',
            '00000000-0000-0000-0000-000000000001',
            'schema-v2-check',
            'schema-v2-check'
        );
        RAISE EXCEPTION
            'a non-announcement discovery edge accepted equal endpoints';
    EXCEPTION
        WHEN check_violation THEN
            NULL;
    END;

    INSERT INTO contract_instance_addresses (
        contract_instance_id,
        chain_id,
        address,
        active_from_block_number,
        active_to_block_number,
        deactivated_at
    )
    VALUES (
        '00000000-0000-0000-0000-000000000002',
        'schema-v2-check',
        'historical-address-0',
        0,
        100,
        now()
    );

    BEGIN
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address,
            active_from_block_number,
            active_to_block_number,
            deactivated_at
        )
        VALUES (
            '00000000-0000-0000-0000-000000000002',
            'schema-v2-check',
            'historical-address-1',
            50,
            150,
            now()
        );
        RAISE EXCEPTION
            'one contract instance accepted overlapping address ranges';
    EXCEPTION
        WHEN exclusion_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "contract_instance_addresses_no_overlap"%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO contract_instance_addresses (
        contract_instance_id,
        chain_id,
        address,
        active_from_block_number
    )
    VALUES (
        '00000000-0000-0000-0000-000000000001',
        'schema-v2-check',
        'contract-address-0',
        0
    );

    BEGIN
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address
        )
        VALUES (
            '00000000-0000-0000-0000-000000000001',
            'schema-v2-check',
            'contract-address-1'
        );
        RAISE EXCEPTION
            'one contract instance accepted two active addresses';
    EXCEPTION
        WHEN unique_violation OR exclusion_violation THEN
            IF SQLERRM NOT LIKE
                '%contract_instance_addresses_active_instance_idx%'
                AND SQLERRM NOT LIKE
                    '%constraint "contract_instance_addresses_no_overlap"%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO token_lineages (
        token_lineage_id,
        chain_id,
        block_hash,
        block_number
    )
    VALUES
        (
            '00000000-0000-0000-0000-000000000031',
            'schema-v2-check',
            'block-0',
            0
        ),
        (
            '00000000-0000-0000-0000-000000000032',
            'schema-v2-check',
            'block-0',
            0
        ),
        (
            '00000000-0000-0000-0000-000000000033',
            'schema-v2-check',
            'block-0',
            0
        );

    INSERT INTO resources (
        resource_id,
        token_lineage_id,
        chain_id,
        block_hash,
        block_number
    )
    VALUES
        (
            '00000000-0000-0000-0000-000000000011',
            '00000000-0000-0000-0000-000000000031',
            'schema-v2-check',
            'block-0',
            0
        ),
        (
            '00000000-0000-0000-0000-000000000012',
            '00000000-0000-0000-0000-000000000032',
            'schema-v2-check',
            'block-0',
            0
        ),
        (
            '00000000-0000-0000-0000-000000000013',
            NULL,
            'schema-v2-other',
            'block-other-0',
            0
        );

    INSERT INTO name_surfaces (
        logical_name_id,
        namespace,
        raw_name,
        raw_labels,
        dns_encoded_name,
        namehash,
        labelhashes,
        normalizer_version,
        visibility_state,
        chain_id,
        block_hash,
        block_number,
        canonicality_state
    )
    VALUES (
        'schema-v2-check:namehash-0',
        'schema-v2-check',
        'Name',
        ARRAY['Name'],
        '\x044e616d6500',
        'namehash-0',
        ARRAY['labelhash-0'],
        'check',
        'active',
        'schema-v2-check',
        'block-0',
        0,
        'canonical'
    );

    INSERT INTO name_surfaces (
        logical_name_id,
        namespace,
        raw_name,
        raw_labels,
        dns_encoded_name,
        namehash,
        labelhashes,
        normalizer_version,
        visibility_state,
        chain_id,
        block_hash,
        block_number,
        canonicality_state
    )
    VALUES (
        'schema-v2-check:namehash-2',
        'schema-v2-check',
        'Other',
        ARRAY['Other'],
        '\x054f7468657200',
        'namehash-2',
        ARRAY['labelhash-2'],
        'check',
        'active',
        'schema-v2-check',
        'block-0',
        0,
        'canonical'
    );

    BEGIN
        INSERT INTO name_surfaces (
            logical_name_id,
            namespace,
            raw_name,
            raw_labels,
            dns_encoded_name,
            namehash,
            labelhashes,
            normalizer_version,
            chain_id,
            block_hash,
            block_number,
            canonicality_state
        )
        VALUES (
            'schema-v2-check:namehash-missing-visibility',
            'schema-v2-check',
            'Missing',
            ARRAY['Missing'],
            '\x074d697373696e6700',
            'namehash-missing-visibility',
            ARRAY['labelhash-missing-visibility'],
            'check',
            'schema-v2-check',
            'block-0',
            0,
            'canonical'
        );
        RAISE EXCEPTION
            'name_surfaces accepted an omitted visibility decision';
    EXCEPTION
        WHEN not_null_violation THEN
            IF SQLERRM NOT LIKE
                '%null value in column "visibility_state"%'
            THEN
                RAISE EXCEPTION
                    'name_surfaces omitted visibility failed with unexpected message: %',
                    SQLERRM;
            END IF;
    END;

    INSERT INTO name_surfaces (
        logical_name_id,
        namespace,
        raw_name,
        raw_labels,
        dns_encoded_name,
        namehash,
        labelhashes,
        normalizer_version,
        visibility_state,
        normalization_errors,
        deactivation_reason,
        deactivated_at,
        chain_id,
        block_hash,
        block_number,
        canonicality_state
    )
    VALUES (
        'schema-v2-check:namehash-shadow',
        'schema-v2-check',
        'Shadow',
        ARRAY['Shadow'],
        '\x06536861646f7700',
        'namehash-shadow',
        ARRAY['labelhash-shadow'],
        'check',
        'shadow',
        '["normalization failed"]'::jsonb,
        'normalization failed',
        '2026-01-01 00:00:01+00',
        'schema-v2-check',
        'block-0',
        0,
        'canonical'
    );

    BEGIN
        INSERT INTO name_surfaces (
            logical_name_id,
            namespace,
            raw_name,
            raw_labels,
            dns_encoded_name,
            namehash,
            labelhashes,
            normalizer_version,
            visibility_state,
            normalization_errors,
            chain_id,
            block_hash,
            block_number,
            canonicality_state
        )
        VALUES (
            'schema-v2-check:namehash-invalid-active',
            'schema-v2-check',
            'Invalid Active',
            ARRAY['Invalid Active'],
            '\x0e496e76616c69642041637469766500',
            'namehash-invalid-active',
            ARRAY['labelhash-invalid-active'],
            'check',
            'active',
            '["unexpected"]'::jsonb,
            'schema-v2-check',
            'block-0',
            0,
            'canonical'
        );
        RAISE EXCEPTION
            'name_surfaces accepted active normalization errors';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "name_surfaces_visibility_coherence_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO name_surfaces (
            logical_name_id,
            namespace,
            raw_name,
            raw_labels,
            dns_encoded_name,
            namehash,
            labelhashes,
            normalizer_version,
            visibility_state,
            chain_id,
            block_hash,
            block_number,
            canonicality_state
        )
        VALUES (
            'schema-v2-check:namehash-invalid-shadow',
            'schema-v2-check',
            'Invalid Shadow',
            ARRAY['Invalid Shadow'],
            '\x0e496e76616c696420536861646f7700',
            'namehash-invalid-shadow',
            ARRAY['labelhash-invalid-shadow'],
            'check',
            'shadow',
            'schema-v2-check',
            'block-0',
            0,
            'canonical'
        );
        RAISE EXCEPTION
            'name_surfaces accepted an incomplete shadow state';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "name_surfaces_visibility_coherence_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO name_surfaces (
            logical_name_id,
            namespace,
            raw_name,
            raw_labels,
            dns_encoded_name,
            namehash,
            labelhashes,
            normalizer_version,
            visibility_state,
            chain_id,
            block_hash,
            block_number,
            canonicality_state
        )
        VALUES (
            'schema-v2-check:not-the-namehash',
            'schema-v2-check',
            'Other',
            ARRAY['Other'],
            '\x054f7468657200',
            'namehash-1',
            ARRAY['labelhash-1'],
            'check',
            'active',
            'schema-v2-check',
            'block-0',
            0,
            'canonical'
        );
        RAISE EXCEPTION
            'name_surfaces accepted a logical ID that is not namespace:namehash';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "name_surfaces_logical_identity_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO resources (
            resource_id,
            token_lineage_id,
            chain_id,
            block_hash,
            block_number
        )
        VALUES (
            '00000000-0000-0000-0000-000000000014',
            '00000000-0000-0000-0000-000000000033',
            'schema-v2-other',
            'block-other-0',
            0
        );
        accepted_cross_chain_relationships :=
            array_append(
                accepted_cross_chain_relationships,
                'resources.token_lineage_id'
            );
        DELETE FROM resources
        WHERE resource_id = '00000000-0000-0000-0000-000000000014';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "resources_chain_id_token_lineage_id_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO surface_bindings (
            surface_binding_id,
            logical_name_id,
            resource_id,
            binding_kind,
            authority_arm,
            active_from,
            chain_id,
            block_hash,
            block_number
        )
        VALUES (
            '00000000-0000-0000-0000-000000000023',
            'schema-v2-check:namehash-0',
            '00000000-0000-0000-0000-000000000013',
            'declared_registry_path',
            'ens_v1',
            '2026-01-01 00:00:00+00',
            'schema-v2-other',
            'block-other-0',
            0
        );
        accepted_cross_chain_relationships :=
            array_append(
                accepted_cross_chain_relationships,
                'surface_bindings.logical_name_id'
            );
        DELETE FROM surface_bindings
        WHERE surface_binding_id =
            '00000000-0000-0000-0000-000000000023';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "surface_bindings_chain_id_logical_name_id_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO surface_bindings (
            surface_binding_id,
            logical_name_id,
            resource_id,
            binding_kind,
            authority_arm,
            active_from,
            chain_id,
            block_hash,
            block_number
        )
        VALUES (
            '00000000-0000-0000-0000-000000000024',
            'schema-v2-check:namehash-0',
            '00000000-0000-0000-0000-000000000013',
            'declared_registry_path',
            'ens_v1',
            '2026-01-01 00:00:00+00',
            'schema-v2-check',
            'block-0',
            0
        );
        accepted_cross_chain_relationships :=
            array_append(
                accepted_cross_chain_relationships,
                'surface_bindings.resource_id'
            );
        DELETE FROM surface_bindings
        WHERE surface_binding_id =
            '00000000-0000-0000-0000-000000000024';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "surface_bindings_chain_id_resource_id_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            logical_name_id,
            event_kind,
            source_family,
            manifest_version,
            chain_id,
            derivation_kind
        )
        VALUES (
            'cross-chain-name-event',
            'schema-v2-check',
            'schema-v2-check:namehash-0',
            'ResolverChanged',
            'check',
            1,
            'schema-v2-other',
            'ens_v2_resolver'
        );
        accepted_cross_chain_relationships :=
            array_append(
                accepted_cross_chain_relationships,
                'normalized_events.logical_name_id'
            );
        DELETE FROM normalized_events
        WHERE event_identity = 'cross-chain-name-event';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "normalized_events_chain_id_logical_name_id_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            resource_id,
            event_kind,
            source_family,
            manifest_version,
            chain_id,
            derivation_kind
        )
        VALUES (
            'cross-chain-resource-event',
            'schema-v2-check',
            '00000000-0000-0000-0000-000000000011',
            'ResolverChanged',
            'check',
            1,
            'schema-v2-other',
            'ens_v2_resolver'
        );
        accepted_cross_chain_relationships :=
            array_append(
                accepted_cross_chain_relationships,
                'normalized_events.resource_id'
            );
        DELETE FROM normalized_events
        WHERE event_identity = 'cross-chain-resource-event';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "normalized_events_chain_id_resource_id_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    IF cardinality(accepted_cross_chain_relationships) > 0 THEN
        RAISE EXCEPTION
            'identity tables accepted cross-chain relationships: %',
            array_to_string(accepted_cross_chain_relationships, ', ');
    END IF;

    INSERT INTO record_inventory_current (
        resource_id,
        record_version_boundary_key,
        record_version_boundary,
        selectors,
        unsupported_families,
        entries,
        support_status,
        provenance,
        chain_positions,
        canonicality_summary,
        manifest_version
    ) VALUES (
        '00000000-0000-0000-0000-000000000011',
        'schema-v2-lookup-guard',
        '{"kind": "schema-v2-lookup-guard"}'::jsonb,
        '[]'::jsonb,
        '[]'::jsonb,
        '[]'::jsonb,
        'supported',
        '{}'::jsonb,
        '{}'::jsonb,
        '{}'::jsonb,
        1
    );

    INSERT INTO name_current (
        logical_name_id,
        namespace,
        raw_name,
        namehash,
        declared_summary,
        support_status,
        manifest_version
    ) VALUES (
        'schema-v2-check:namehash-0',
        'schema-v2-check',
        'Name',
        'namehash-0',
        '{
            "topology": {
                "resolver_path": [
                    {
                        "chain_id": "schema-v2-check",
                        "address": "resolver-address-guard"
                    }
                ],
                "version_boundaries": {
                    "record_version_boundary": {
                        "kind": "schema-v2-lookup-guard"
                    }
                }
            }
        }'::jsonb,
        'supported',
        1
    );

    INSERT INTO chain_phase_state (
        chain_id,
        phase_name,
        phase_status,
        current_block_number,
        current_block_hash,
        target_block_number,
        target_block_hash,
        input_content_hash,
        started_at,
        finished_at
    ) VALUES (
        'schema-v2-check',
        'project',
        'completed',
        0,
        'block-0',
        0,
        'block-0',
        'schema-v2-check-content',
        now(),
        now()
    );

    INSERT INTO manifest_versions (
        manifest_version,
        namespace,
        source_family,
        chain_id,
        deployment_label,
        rollout_status,
        normalizer_version,
        file_path,
        manifest_payload
    ) VALUES (
        1,
        'schema-v2-check',
        'schema-v2-lookup-guard',
        'schema-v2-check',
        'schema-v2-lookup-guard',
        'active',
        'check',
        'schema-v2-lookup-guard.toml',
        '{}'::jsonb
    ) RETURNING manifest_id INTO manifest_key;

    INSERT INTO manifest_contract_instances (
        manifest_id,
        chain_id,
        declaration_kind,
        declaration_name,
        contract_instance_id,
        declared_address,
        role,
        proxy_kind
    ) VALUES (
        manifest_key,
        'schema-v2-check',
        'contract',
        'schema-v2-lookup-guard',
        '00000000-0000-0000-0000-000000000002',
        'resolver-address-guard',
        'lookup-guard',
        'none'
    );

    SELECT jsonb_build_object(
        'project_row_xmin', phase.xmin::text,
        'logical_name_id', name.logical_name_id,
        'name_row_xmin', name.xmin::text,
        'manifest_authorities', jsonb_build_array(jsonb_build_object(
            'declared_address', declaration.declared_address,
            'manifest_id', manifest.manifest_id::text,
            'manifest_row_xmin', manifest.xmin::text,
            'declaration_id',
                declaration.manifest_contract_instance_id::text,
            'declaration_row_xmin', declaration.xmin::text
        ))
    )
    INTO divergence_execution_authority
    FROM chain_phase_state AS phase
    CROSS JOIN name_current AS name
    JOIN manifest_versions AS manifest
      ON manifest.manifest_id = manifest_key
    JOIN manifest_contract_instances AS declaration
      ON declaration.manifest_id = manifest.manifest_id
    WHERE phase.chain_id = 'schema-v2-check'
      AND phase.phase_name = 'project'
      AND name.logical_name_id = 'schema-v2-check:namehash-0';

    SELECT xmin::text
    INTO divergence_guard_xmin
    FROM record_inventory_current
    WHERE resource_id = '00000000-0000-0000-0000-000000000011'
      AND record_version_boundary_key = 'schema-v2-lookup-guard';

    SELECT write_resolution_divergence(
        '00000000-0000-0000-0000-000000000011',
        'schema-v2-lookup-guard',
        divergence_guard_xmin,
        'schema-v2-check',
        0,
        'block-0',
        divergence_execution_authority,
        'schema-v2-check:namehash-0',
        'schema-v2-check',
        'resolver-address-guard',
        'addr:60',
        '{
            "resolver": {
                "chain_id": "schema-v2-check",
                "block_hash": "block-0",
                "block_number": 0,
                "timestamp": "2026-01-01T00:00:00Z"
            }
        }'::jsonb,
        '{"status": "success", "value": "0x01"}'::jsonb,
        false
    ) INTO divergence_write_status;

    IF divergence_write_status <> 'written'
        OR NOT EXISTS (
            SELECT 1
            FROM resolution_divergences
            WHERE resolver_address = 'resolver-address-guard'
              AND indexed_result = '{"status": "not_found"}'::jsonb
              AND live_result =
                  '{"status": "success", "value": "0x01"}'::jsonb
              AND observed_positions -> 'resolver' ->> 'block_hash' =
                  'block-0'
        )
    THEN
        RAISE EXCEPTION
            'guarded divergence write did not store both answers and anchor';
    END IF;

    SELECT write_resolution_divergence(
        '00000000-0000-0000-0000-000000000011',
        'schema-v2-lookup-guard',
        divergence_guard_xmin,
        'schema-v2-check',
        0,
        'block-0',
        divergence_execution_authority,
        'schema-v2-check:namehash-0',
        'schema-v2-check',
        'resolver-address-guard',
        'text:' || oversized_text,
        '{
            "resolver": {
                "chain_id": "schema-v2-check",
                "block_hash": "block-0",
                "block_number": 0,
                "timestamp": "2026-01-01T00:00:00Z"
            }
        }'::jsonb,
        '{"status": "success", "value": "oversized"}'::jsonb,
        false
    ) INTO divergence_write_status;

    IF divergence_write_status <> 'written'
        OR NOT EXISTS (
            SELECT 1
            FROM resolution_divergences
            WHERE resolver_address = 'resolver-address-guard'
              AND request_kind_hash =
                  public.digest('text:' || oversized_text, 'sha256')
              AND request_kind = 'text:' || oversized_text
        )
    THEN
        RAISE EXCEPTION
            'oversized divergence request was not findable by its bounded digest';
    END IF;

    SELECT count(*)
    INTO divergence_count
    FROM resolution_divergences;

    SELECT write_resolution_divergence(
        '00000000-0000-0000-0000-000000000011',
        'schema-v2-lookup-guard',
        divergence_guard_xmin,
        'schema-v2-check',
        0,
        'block-0',
        divergence_execution_authority,
        'schema-v2-check:namehash-0',
        'schema-v2-check',
        'resolver-address-guard',
        'addr:60',
        '{
            "resolver": {
                "chain_id": "schema-v2-check",
                "block_hash": "orphaned-agreement",
                "block_number": 1,
                "timestamp": "2026-01-01T00:00:01Z"
            }
        }'::jsonb,
        '{"status": "not_found"}'::jsonb,
        false
    ) INTO divergence_write_status;

    IF divergence_write_status <> 'guard_rejected'
        OR NOT EXISTS (
            SELECT 1
            FROM resolution_divergences
            WHERE resolver_address = 'resolver-address-guard'
              AND request_kind = 'addr:60'
              AND cleared_at IS NULL
        )
    THEN
        RAISE EXCEPTION
            'orphaned agreement cleared an older active divergence';
    END IF;

    SELECT write_resolution_divergence(
        '00000000-0000-0000-0000-000000000011',
        'schema-v2-lookup-guard',
        divergence_guard_xmin || '-stale',
        'schema-v2-check',
        0,
        'block-0',
        divergence_execution_authority,
        'schema-v2-check:namehash-0',
        'schema-v2-check',
        'resolver-address-guard',
        'text:url',
        '{
            "resolver": {
                "chain_id": "schema-v2-check",
                "block_hash": "block-0",
                "block_number": 0,
                "timestamp": "2026-01-01T00:00:00Z"
            }
        }'::jsonb,
        '{"status": "success", "value": "https://example.test"}'::jsonb,
        false
    ) INTO divergence_write_status;

    IF divergence_write_status <> 'guard_rejected'
        OR (SELECT count(*) FROM resolution_divergences) <> divergence_count
    THEN
        RAISE EXCEPTION
            'row-unchanged guard accepted a stale projection comparison';
    END IF;

    SELECT write_resolution_divergence(
        '00000000-0000-0000-0000-000000000011',
        'schema-v2-lookup-guard',
        divergence_guard_xmin,
        'schema-v2-check',
        0,
        'block-0',
        divergence_execution_authority,
        'schema-v2-check:namehash-0',
        'schema-v2-check',
        'resolver-address-ccip-guard',
        'text:ccip',
        '{
            "resolver": {
                "chain_id": "schema-v2-check",
                "block_hash": "block-0",
                "block_number": 0,
                "timestamp": "2026-01-01T00:00:00Z"
            }
        }'::jsonb,
        '{"status": "success", "value": "ccip"}'::jsonb,
        true
    ) INTO divergence_write_status;

    IF divergence_write_status <> 'ccip_skipped'
        OR (SELECT count(*) FROM resolution_divergences) <> divergence_count
    THEN
        RAISE EXCEPTION 'CCIP result reached the divergence ledger';
    END IF;

    -- These are SQL evaluator fixtures, separate from the Project component proof.
    FOR zero_case IN SELECT unnest(ARRAY['marked', 'missing', 'empty']) LOOP
        DELETE FROM resolution_divergences
        WHERE logical_name_id = 'schema-v2-check:namehash-0' AND request_kind = 'addr:60';
        UPDATE record_inventory_current
        SET entries = '[{"record_key":"addr:2147483648","record_family":"addr",
                "selector_key":"2147483648","status":"success",
                "value":"0x2222222222222222222222222222222222222222"}]'::jsonb
            || CASE WHEN zero_case = 'missing' THEN '[]'::jsonb ELSE
                '[{"record_key":"addr:60","record_family":"addr",
                  "selector_key":"60","status":"not_found"}]'::jsonb END,
            provenance = '{"read_rules":[{"kind":"ensip19_default_address",
                "source_record_key":"addr:2147483648"}]}'::jsonb
                || CASE WHEN zero_case = 'marked' THEN
                    '{"exact_nonempty_not_found_record_keys":["addr:60"]}'::jsonb
                    ELSE '{}'::jsonb END
        WHERE resource_id = '00000000-0000-0000-0000-000000000011';
        SELECT xmin::text INTO STRICT divergence_guard_xmin FROM record_inventory_current
        WHERE resource_id = '00000000-0000-0000-0000-000000000011';
        zero_expected := CASE WHEN zero_case = 'marked' THEN '{"status":"not_found"}'::jsonb
            ELSE '{"status":"success","value":"0x2222222222222222222222222222222222222222"}'::jsonb END;
        FOR zero_step IN 1..3 LOOP
            zero_live := CASE WHEN zero_step = 1 THEN
                '{"status":"success","value":"0x1111111111111111111111111111111111111111"}'::jsonb
                ELSE zero_expected END;
            SELECT write_resolution_divergence(
                '00000000-0000-0000-0000-000000000011', 'schema-v2-lookup-guard',
                divergence_guard_xmin, 'schema-v2-check', 0, 'block-0',
                divergence_execution_authority, 'schema-v2-check:namehash-0',
                'schema-v2-check', 'resolver-address-guard', 'addr:60',
                '{"resolver":{"chain_id":"schema-v2-check","block_hash":"block-0",
                  "block_number":0,"timestamp":"2026-01-01T00:00:00Z"}}'::jsonb,
                zero_live, false
            ) INTO divergence_write_status;
            IF divergence_write_status <> (ARRAY['written', 'cleared', 'agreement'])[zero_step] THEN
                RAISE EXCEPTION 'exact-zero writer action mismatch: %, %, %', zero_case, zero_step, divergence_write_status;
            END IF;
            SELECT to_jsonb(ledger) INTO STRICT zero_row FROM resolution_divergences ledger
            WHERE logical_name_id = 'schema-v2-check:namehash-0' AND request_kind = 'addr:60';
            IF zero_row->'indexed_result' <> zero_expected
                OR (zero_row->>'cleared_at' IS NULL) <> (zero_step = 1) THEN
                RAISE EXCEPTION 'exact-zero durable answer or active state mismatch: %, %', zero_case, zero_step;
            END IF;
            IF zero_step = 1 THEN
                zero_written := zero_row;
            ELSIF zero_step = 2 THEN
                IF zero_row - 'cleared_at' <> zero_written - 'cleared_at' THEN
                    RAISE EXCEPTION 'exact-zero clear replaced or mutated the retained row';
                END IF;
                zero_cleared := zero_row;
            ELSIF zero_row <> zero_cleared THEN
                RAISE EXCEPTION 'exact-zero agreement mutated the retained cleared row';
            END IF;
        END LOOP;
    END LOOP;

    DELETE FROM name_current
    WHERE logical_name_id = 'schema-v2-check:namehash-0';

    INSERT INTO resolution_divergences (
        logical_name_id,
        resolver_chain_id,
        resolver_address,
        request_kind,
        observed_positions,
        indexed_result,
        live_result
    )
    VALUES (
        'schema-v2-check:namehash-0',
        'schema-v2-check',
        'resolver-address-0',
        'addr',
        '{
            "resolver": {
                "chain_id": "schema-v2-check",
                "block_hash": "block-0",
                "block_number": 0,
                "timestamp": "2026-01-01T00:00:00Z"
            }
        }'::jsonb,
        '{"value": "indexed"}'::jsonb,
        '{"value": "live"}'::jsonb
    );

    BEGIN
        UPDATE chain_lineage
        SET block_timestamp = '2026-01-01 00:00:02+00'
        WHERE chain_id = 'schema-v2-check'
          AND block_hash = 'block-0';
        RAISE EXCEPTION
            'chain_lineage accepted a block timestamp identity change';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM <> 'chain lineage block identity is immutable' THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO resolution_divergences (
            logical_name_id,
            resolver_chain_id,
            resolver_address,
            request_kind,
            observed_positions,
            indexed_result,
            live_result,
            first_observed_at,
            last_observed_at,
            cleared_at
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check',
            'resolver-address-time-check',
            'addr',
            '{
                "resolver": {
                    "chain_id": "schema-v2-check",
                    "block_hash": "block-1",
                    "block_number": 1,
                    "timestamp": "2026-01-01T00:00:01Z"
                }
            }'::jsonb,
            '{"value": "indexed"}'::jsonb,
            '{"value": "live"}'::jsonb,
            '2026-01-01 00:00:00+00',
            '2026-01-03 00:00:00+00',
            '2026-01-02 00:00:00+00'
        );
        RAISE EXCEPTION
            'resolution_divergences accepted clearing before last observation';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "resolution_divergences_clearing_time_check"%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO resolution_divergences (
        logical_name_id,
        resolver_chain_id,
        resolver_address,
        request_kind,
        observed_positions,
        indexed_result,
        live_result
    )
    VALUES (
        'schema-v2-check:namehash-0',
        'schema-v2-other',
        'resolver-address-reorg-check',
        'addr',
        '{
            "resolver": {
                "chain_id": "schema-v2-other",
                "block_hash": "block-other-0",
                "block_number": 0,
                "timestamp": "2026-01-01T00:00:00Z"
            }
        }'::jsonb,
        '{"value": "indexed"}'::jsonb,
        '{"value": "live"}'::jsonb
    );

    UPDATE chain_lineage
    SET canonicality_state = 'orphaned'
    WHERE chain_id = 'schema-v2-other'
      AND block_hash = 'block-other-0';

    IF EXISTS (
        SELECT 1
        FROM resolution_divergences
        WHERE resolver_address = 'resolver-address-reorg-check'
          AND cleared_at IS NULL
    ) THEN
        accepted_divergence_canonicality :=
            array_append(
                accepted_divergence_canonicality,
                'active row survived an orphaned observed block'
            );
    END IF;

    UPDATE chain_lineage
    SET canonicality_state = 'canonical'
    WHERE chain_id = 'schema-v2-other'
      AND block_hash = 'block-other-0';

    DELETE FROM resolution_divergences
    WHERE resolver_address = 'resolver-address-reorg-check';

    BEGIN
        INSERT INTO resolution_divergences (
            logical_name_id,
            resolver_chain_id,
            resolver_address,
            request_kind,
            observed_positions,
            indexed_result,
            live_result
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-other',
            'resolver-address-orphan-check',
            'addr',
            '{
                "resolver": {
                    "chain_id": "schema-v2-other",
                    "block_hash": "orphaned-block-1",
                    "block_number": 1,
                    "timestamp": "2026-01-01T00:00:01Z"
                }
            }'::jsonb,
            '{"value": "indexed"}'::jsonb,
            '{"value": "live"}'::jsonb
        );
        accepted_divergence_canonicality :=
            array_append(
                accepted_divergence_canonicality,
                'active row accepted an orphaned observed block'
            );
        DELETE FROM resolution_divergences
        WHERE resolver_address = 'resolver-address-orphan-check';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM <>
                'active resolution difference position resolver is not canonical'
            THEN
                RAISE;
            END IF;
    END;

    IF cardinality(accepted_divergence_canonicality) > 0 THEN
        RAISE EXCEPTION
            'resolution differences accepted noncanonical positions: %',
            array_to_string(accepted_divergence_canonicality, ', ');
    END IF;

    BEGIN
        INSERT INTO resolution_divergences (
            logical_name_id,
            resolver_chain_id,
            resolver_address,
            request_kind,
            observed_positions,
            indexed_result,
            live_result
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check',
            'resolver-address-0',
            'addr',
            '{
                "resolver": {
                    "chain_id": "schema-v2-check",
                    "block_hash": "block-1",
                    "block_number": 1,
                    "timestamp": "2026-01-01T00:00:01Z"
                }
            }'::jsonb,
            '{"value": "indexed"}'::jsonb,
            '{"value": "live"}'::jsonb
        );
        RAISE EXCEPTION
            'one request accepted two active divergence rows';
    EXCEPTION
        WHEN unique_violation THEN
            IF SQLERRM NOT LIKE
                '%resolution_divergences_one_active_request_idx%'
            THEN
                RAISE;
            END IF;
    END;

    UPDATE resolution_divergences
    SET cleared_at = now()
    WHERE logical_name_id = 'schema-v2-check:namehash-0'
      AND resolver_chain_id = 'schema-v2-check'
      AND resolver_address = 'resolver-address-0'
      AND request_kind = 'addr';

    INSERT INTO resolution_divergences (
        logical_name_id,
        resolver_chain_id,
        resolver_address,
        request_kind,
        observed_positions,
        indexed_result,
        live_result
    )
    VALUES (
        'schema-v2-check:namehash-0',
        'schema-v2-check',
        'resolver-address-0',
        'addr',
        '{
            "resolver": {
                "chain_id": "schema-v2-check",
                "block_hash": "block-1",
                "block_number": 1,
                "timestamp": "2026-01-01T00:00:01Z"
            }
        }'::jsonb,
        '{"value": "indexed"}'::jsonb,
        '{"value": "live"}'::jsonb
    );

    INSERT INTO surface_bindings (
        surface_binding_id,
        logical_name_id,
        resource_id,
        binding_kind,
        authority_arm,
        active_from,
        chain_id,
        block_hash,
        block_number,
        canonicality_state
    )
    VALUES (
        '00000000-0000-0000-0000-000000000021',
        'schema-v2-check:namehash-0',
        '00000000-0000-0000-0000-000000000011',
        'declared_registry_path',
        'ens_v1',
        '2026-01-01 00:00:00+00',
        'schema-v2-check',
        'block-0',
        0,
        'canonical'
    );

    INSERT INTO surface_bindings (
        surface_binding_id,
        logical_name_id,
        resource_id,
        binding_kind,
        authority_arm,
        active_from,
        chain_id,
        block_hash,
        block_number,
        canonicality_state
    )
    VALUES (
        '00000000-0000-0000-0000-000000000025',
        'schema-v2-check:namehash-2',
        '00000000-0000-0000-0000-000000000012',
        'declared_registry_path',
        'ens_v1',
        '2026-01-01 00:00:00+00',
        'schema-v2-check',
        'block-0',
        0,
        'canonical'
    );

    BEGIN
        INSERT INTO name_current (
            logical_name_id,
            namespace,
            raw_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            support_status,
            manifest_version
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check',
            'Name',
            'namehash-0',
            '00000000-0000-0000-0000-000000000025',
            '00000000-0000-0000-0000-000000000012',
            '00000000-0000-0000-0000-000000000032',
            'declared_registry_path',
            'supported',
            1
        );
        accepted_projection_relationship_mismatches :=
            array_append(
                accepted_projection_relationship_mismatches,
                'name_current binding'
            );
        DELETE FROM name_current
        WHERE logical_name_id = 'schema-v2-check:namehash-0';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "name_current_surface_binding_id_logical_name_id_resource_i_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO address_names_current (
            address,
            logical_name_id,
            relation,
            namespace,
            raw_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            support_status,
            manifest_version
        )
        VALUES (
            'address-binding-mismatch',
            'schema-v2-check:namehash-0',
            'registrant',
            'schema-v2-check',
            'Name',
            'namehash-0',
            '00000000-0000-0000-0000-000000000025',
            '00000000-0000-0000-0000-000000000012',
            '00000000-0000-0000-0000-000000000032',
            'declared_registry_path',
            'supported',
            1
        );
        accepted_projection_relationship_mismatches :=
            array_append(
                accepted_projection_relationship_mismatches,
                'address_names_current binding'
            );
        DELETE FROM address_names_current
        WHERE address = 'address-binding-mismatch';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "address_names_current_surface_binding_id_logical_name_id_r_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO name_current (
            logical_name_id,
            namespace,
            raw_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            support_status,
            manifest_version
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check',
            'Name',
            'namehash-0',
            '00000000-0000-0000-0000-000000000021',
            '00000000-0000-0000-0000-000000000011',
            '00000000-0000-0000-0000-000000000032',
            'declared_registry_path',
            'supported',
            1
        );
        accepted_projection_relationship_mismatches :=
            array_append(
                accepted_projection_relationship_mismatches,
                'name_current token lineage'
            );
        DELETE FROM name_current
        WHERE logical_name_id = 'schema-v2-check:namehash-0';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "name_current_resource_id_token_lineage_id_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO address_names_current (
            address,
            logical_name_id,
            relation,
            namespace,
            raw_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            support_status,
            manifest_version
        )
        VALUES (
            'address-token-mismatch',
            'schema-v2-check:namehash-0',
            'registrant',
            'schema-v2-check',
            'Name',
            'namehash-0',
            '00000000-0000-0000-0000-000000000021',
            '00000000-0000-0000-0000-000000000011',
            '00000000-0000-0000-0000-000000000032',
            'declared_registry_path',
            'supported',
            1
        );
        accepted_projection_relationship_mismatches :=
            array_append(
                accepted_projection_relationship_mismatches,
                'address_names_current token lineage'
            );
        DELETE FROM address_names_current
        WHERE address = 'address-token-mismatch';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "address_names_current_resource_id_token_lineage_id_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    IF cardinality(accepted_projection_relationship_mismatches) > 0 THEN
        RAISE EXCEPTION
            'projection tables accepted mismatched relationships: %',
            array_to_string(
                accepted_projection_relationship_mismatches,
                ', '
            );
    END IF;

    BEGIN
        INSERT INTO name_current (
            logical_name_id,
            namespace,
            raw_name,
            namehash,
            support_status,
            manifest_version
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'wrong-namespace',
            'Name',
            'wrong-namehash',
            'supported',
            1
        );
        accepted_projection_identity_mismatches :=
            array_append(
                accepted_projection_identity_mismatches,
                'name_current'
            );
        DELETE FROM name_current
        WHERE logical_name_id = 'schema-v2-check:namehash-0';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "name_current_logical_identity_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO children_current (
            parent_logical_name_id,
            child_logical_name_id,
            namespace,
            raw_name,
            raw_label,
            namehash,
            labelhash,
            manifest_version
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check:child-namehash',
            'wrong-namespace',
            convert_to('Child.Name', 'UTF8'),
            convert_to('Child', 'UTF8'),
            'wrong-namehash',
            'wrong-labelhash',
            1
        );
        accepted_projection_identity_mismatches :=
            array_append(
                accepted_projection_identity_mismatches,
                'children_current'
            );
        DELETE FROM children_current
        WHERE parent_logical_name_id = 'schema-v2-check:namehash-0'
          AND child_logical_name_id = 'schema-v2-check:child-namehash';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "children_current_logical_identity_check"%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO children_current (
        parent_logical_name_id,
        child_logical_name_id,
        namespace,
        raw_name,
        decoded_name,
        raw_label,
        decoded_label,
        namehash,
        labelhash,
        manifest_version
    )
    VALUES (
        'schema-v2-check:namehash-0',
        'schema-v2-check:child-clean',
        'schema-v2-check',
        convert_to('Clean.Name', 'UTF8'),
        'Clean.Name',
        convert_to('Clean', 'UTF8'),
        'Clean',
        'child-clean',
        'labelhash-child-clean',
        1
    ), (
        'schema-v2-check:namehash-0',
        'schema-v2-check:child-hostile',
        'schema-v2-check',
        decode('ff002e4e616d65', 'hex'),
        NULL,
        decode('ff00', 'hex'),
        NULL,
        'child-hostile',
        'labelhash-child-hostile',
        1
    );

    INSERT INTO children_current (
        parent_logical_name_id,
        child_logical_name_id,
        namespace,
        raw_name,
        decoded_name,
        raw_label,
        decoded_label,
        namehash,
        labelhash,
        manifest_version
    )
    VALUES (
        'schema-v2-check:namehash-0',
        'schema-v2-check:child-topology-only',
        'schema-v2-check',
        NULL,
        NULL,
        NULL,
        NULL,
        'child-topology-only',
        'labelhash-topology-only',
        1
    );

    BEGIN
        INSERT INTO children_current (
            parent_logical_name_id,
            child_logical_name_id,
            namespace,
            raw_name,
            decoded_name,
            raw_label,
            decoded_label,
            namehash,
            labelhash,
            manifest_version
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check:child-decoded-label-without-raw',
            'schema-v2-check',
            NULL,
            NULL,
            NULL,
            'Synthesized',
            'child-decoded-label-without-raw',
            'labelhash-decoded-label-without-raw',
            1
        );
        RAISE EXCEPTION
            'children_current accepted a decoded label without raw bytes';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "children_current_decoded_label_requires_raw_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO children_current (
            parent_logical_name_id,
            child_logical_name_id,
            namespace,
            raw_name,
            decoded_name,
            raw_label,
            decoded_label,
            namehash,
            labelhash,
            manifest_version
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check:child-decoded-name-without-raw',
            'schema-v2-check',
            NULL,
            'Synthesized.Name',
            NULL,
            NULL,
            'child-decoded-name-without-raw',
            'labelhash-decoded-name-without-raw',
            1
        );
        RAISE EXCEPTION
            'children_current accepted a decoded name without raw bytes';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "children_current_decoded_name_requires_raw_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO children_current (
            parent_logical_name_id,
            child_logical_name_id,
            namespace,
            raw_name,
            decoded_name,
            raw_label,
            decoded_label,
            namehash,
            labelhash,
            manifest_version
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check:child-decoding-drift',
            'schema-v2-check',
            convert_to('Raw.Name', 'UTF8'),
            'Raw.Name',
            convert_to('raw', 'UTF8'),
            'different',
            'child-decoding-drift',
            'labelhash-child-decoding-drift',
            1
        );
        RAISE EXCEPTION
            'children_current accepted decoded text that differs from raw bytes';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "children_current_decoded_label_matches_raw_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO children_current (
            parent_logical_name_id,
            child_logical_name_id,
            namespace,
            raw_name,
            decoded_name,
            raw_label,
            decoded_label,
            namehash,
            labelhash,
            manifest_version
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check:child-name-decoding-drift',
            'schema-v2-check',
            convert_to('Raw.Name', 'UTF8'),
            'Different.Name',
            convert_to('Raw', 'UTF8'),
            'Raw',
            'child-name-decoding-drift',
            'labelhash-child-name-decoding-drift',
            1
        );
        RAISE EXCEPTION
            'children_current accepted decoded name text that differs from raw bytes';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "children_current_decoded_name_matches_raw_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO address_names_current (
            address,
            logical_name_id,
            relation,
            namespace,
            raw_name,
            namehash,
            surface_binding_id,
            resource_id,
            binding_kind,
            support_status,
            manifest_version
        )
        VALUES (
            'address-0',
            'schema-v2-check:namehash-0',
            'registrant',
            'wrong-namespace',
            'Name',
            'wrong-namehash',
            '00000000-0000-0000-0000-000000000021',
            '00000000-0000-0000-0000-000000000011',
            'declared_registry_path',
            'supported',
            1
        );
        accepted_projection_identity_mismatches :=
            array_append(
                accepted_projection_identity_mismatches,
                'address_names_current'
            );
        DELETE FROM address_names_current
        WHERE address = 'address-0'
          AND logical_name_id = 'schema-v2-check:namehash-0'
          AND relation = 'registrant';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "address_names_current_logical_identity_check"%'
            THEN
                RAISE;
            END IF;
    END;

    IF cardinality(accepted_projection_identity_mismatches) > 0 THEN
        RAISE EXCEPTION
            'projection tables accepted mismatched logical IDs: %',
            array_to_string(
                accepted_projection_identity_mismatches,
                ', '
            );
    END IF;

    BEGIN
        INSERT INTO contract_instances (
            contract_instance_id,
            chain_id,
            contract_kind
        )
        VALUES (
            '00000000-0000-0000-0000-000000000099',
            'schema-v2-check',
            'registry'
        );
        RAISE EXCEPTION
            'contract_instances accepted an unknown contract kind';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "contract_instances_contract_kind_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO discovery_edges (
            chain_id,
            edge_kind,
            from_contract_instance_id,
            to_contract_instance_id,
            discovery_source,
            admission_basis
        )
        VALUES (
            'schema-v2-check',
            'proxy',
            '00000000-0000-0000-0000-000000000001',
            '00000000-0000-0000-0000-000000000002',
            'schema-check',
            'schema-check'
        );
        RAISE EXCEPTION
            'discovery_edges accepted an unknown edge kind';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "discovery_edges_edge_kind_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO surface_bindings (
            surface_binding_id,
            logical_name_id,
            resource_id,
            binding_kind,
            authority_arm,
            active_from,
            chain_id,
            block_hash,
            block_number
        )
        VALUES (
            '00000000-0000-0000-0000-000000000029',
            'schema-v2-check:namehash-0',
            '00000000-0000-0000-0000-000000000011',
            'declared',
            'ens_v1',
            '2025-01-01 00:00:00+00',
            'schema-v2-check',
            'block-0',
            0
        );
        RAISE EXCEPTION
            'surface_bindings accepted an unknown binding kind';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "surface_bindings_binding_kind_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            event_kind,
            source_family,
            manifest_version,
            chain_id,
            derivation_kind
        )
        VALUES (
            'invalid-event-kind',
            'schema-v2-check',
            'Check',
            'schema-check',
            1,
            'schema-v2-check',
            'ens_v2_resolver'
        );
        RAISE EXCEPTION
            'normalized_events accepted an unknown event kind';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "normalized_events_event_kind_check"%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
        manifest_version, chain_id, derivation_kind)
    SELECT 'valid-record-id-' || kind, 'schema-v2-check', kind, 'ens_v2_resolver_l1',
        1, 'schema-v2-check', 'ens_v2_resolver'
    FROM unnest(ARRAY['ResolverRecordLinked', 'ResolverPermissionArgument']) AS kinds(kind);
    DELETE FROM normalized_events WHERE event_identity IN (
        'valid-record-id-ResolverRecordLinked', 'valid-record-id-ResolverPermissionArgument');

    INSERT INTO normalized_events (
        event_identity,
        namespace,
        event_kind,
        source_family,
        manifest_version,
        chain_id,
        derivation_kind,
        migration_correlation_ids,
        consumer_visibility
    )
    SELECT
        'valid-migration-event-kind-' || event_kind,
        'schema-v2-check',
        event_kind,
        'ens_v2_migration_l1',
        1,
        'schema-v2-check',
        'ens_v2_migration',
        ARRAY['migration-correlation-check'],
        'candidate'
    FROM unnest(
        ARRAY['ContractDiscovered', 'MigrationApplied']
    ) AS admitted(event_kind);

    INSERT INTO normalized_events (
        event_identity,
        namespace,
        event_kind,
        source_family,
        manifest_version,
        chain_id,
        derivation_kind
    )
    SELECT
        'valid-derivation-kind-' || derivation_kind,
        'schema-v2-check',
        'ResolverChanged',
        'schema-check',
        1,
        'schema-v2-check',
        derivation_kind
    FROM unnest(
        ARRAY[
            'ens_v1_reverse_claim',
            'ens_v1_unwrapped_authority',
            'ens_v2_migration',
            'ens_v2_permissions',
            'ens_v2_registrar',
            'ens_v2_registry_resource_surface',
            'ens_v2_resolver',
            'manifest_sync',
            'proxy_upgrade',
            'raw_block_preimage_observation',
            'raw_log_preimage_observation',
            'standard_approval'
        ]
    ) AS admitted(derivation_kind);

    INSERT INTO normalized_events (
        event_identity, namespace, event_kind, source_family,
        manifest_version, chain_id, derivation_kind
    ) VALUES (
        'valid-account-permission-changed', 'schema-v2-check',
        'AccountPermissionChanged', 'ens_v1_registry_l1', 1,
        'schema-v2-check', 'standard_approval'
    );

    FOR transition_case IN
        SELECT * FROM (VALUES
            ('0x00000000000000000000000000000000000000AA',
             '0x00000000000000000000000000000000000000bb', true, '["registry_control"]'::jsonb),
            ('', '0x00000000000000000000000000000000000000bb', true, '["registry_control"]'::jsonb),
            ('0x00000000000000000000000000000000000000aa',
             '0x00000000000000000000000000000000000000bb', true, '[]'::jsonb),
            ('0x00000000000000000000000000000000000000aa',
             '0x00000000000000000000000000000000000000bb', false, '["registry_control"]'::jsonb)
        ) AS invalid(authority_contract, owner, approved, powers)
    LOOP
        BEGIN
            INSERT INTO account_permission_state_current (
                chain_id, authority_kind, authority_contract, authority_contract_instance_id,
                owner, subject, relation_kind, approved, effective_powers, grant_source,
                inheritance_path, transfer_behavior, provenance, chain_positions,
                canonicality_summary, manifest_version
            ) VALUES (
                'schema-v2-check', 'registry', transition_case.authority_contract,
                '00000000-0000-0000-0000-000000000099', transition_case.owner,
                '0x00000000000000000000000000000000000000cc', 'operator',
                transition_case.approved, transition_case.powers, '{}', '[]', '{}', '{}', '{}', '{}', 1
            );
            RAISE EXCEPTION 'account permission state accepted an invalid row';
        EXCEPTION WHEN check_violation THEN NULL;
        END;
    END LOOP;

    BEGIN
        INSERT INTO permissions_current_resource_summary (
            resource_id, registry_owner, registry_contract,
            registry_binding_chain_positions, support_status,
            unsupported_reason, provenance, chain_positions,
            canonicality_summary, manifest_version
        ) VALUES (
            '00000000-0000-0000-0000-000000000011',
            '0x00000000000000000000000000000000000000aa',
            '0x00000000000000000000000000000000000000bb',
            '{}', 'unsupported', 'registry-binding-probe', '{}', '{}', '{}', 1
        );
        RAISE EXCEPTION 'permission resource summary accepted a partial registry binding';
    EXCEPTION WHEN check_violation THEN
        GET STACKED DIAGNOSTICS violated_constraint = CONSTRAINT_NAME;
        IF violated_constraint <>
            'permissions_current_resource_summary_registry_binding_check'
        THEN
            RAISE EXCEPTION
                'partial registry binding failed through unexpected constraint %',
                violated_constraint;
        END IF;
    END;

    BEGIN
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            event_kind,
            source_family,
            manifest_version,
            chain_id,
            derivation_kind
        )
        VALUES (
            'invalid-derivation-kind',
            'schema-v2-check',
            'ResolverChanged',
            'schema-check',
            1,
            'schema-v2-check',
            'check'
        );
        RAISE EXCEPTION
            'normalized_events accepted an unknown derivation kind';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "normalized_events_derivation_kind_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO permissions_current (
            resource_id,
            subject,
            scope,
            scope_kind,
            manifest_version
        )
        VALUES (
            '00000000-0000-0000-0000-000000000011',
            'schema-check-subject',
            'schema-check-scope',
            'check',
            1
        );
        RAISE EXCEPTION
            'permissions_current accepted an unknown scope kind';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "permissions_current_scope_kind_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO name_current (
            logical_name_id,
            namespace,
            raw_name,
            namehash,
            surface_binding_id,
            resource_id,
            binding_kind,
            support_status,
            manifest_version
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check',
            'Name',
            'namehash-0',
            '00000000-0000-0000-0000-000000000021',
            '00000000-0000-0000-0000-000000000011',
            'declared',
            'supported',
            1
        );
        RAISE EXCEPTION
            'name_current accepted an unknown binding kind';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "name_current_binding_kind_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO address_names_current (
            address,
            logical_name_id,
            relation,
            namespace,
            raw_name,
            namehash,
            surface_binding_id,
            resource_id,
            binding_kind,
            support_status,
            manifest_version
        )
        VALUES (
            'invalid-relation',
            'schema-v2-check:namehash-0',
            'owner',
            'schema-v2-check',
            'Name',
            'namehash-0',
            '00000000-0000-0000-0000-000000000021',
            '00000000-0000-0000-0000-000000000011',
            'declared_registry_path',
            'supported',
            1
        );
        RAISE EXCEPTION
            'address_names_current accepted an unknown relation';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "address_names_current_relation_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO address_names_current (
            address,
            logical_name_id,
            relation,
            namespace,
            raw_name,
            namehash,
            surface_binding_id,
            resource_id,
            binding_kind,
            support_status,
            manifest_version
        )
        VALUES (
            'invalid-binding-kind',
            'schema-v2-check:namehash-0',
            'registrant',
            'schema-v2-check',
            'Name',
            'namehash-0',
            '00000000-0000-0000-0000-000000000021',
            '00000000-0000-0000-0000-000000000011',
            'declared',
            'supported',
            1
        );
        RAISE EXCEPTION
            'address_names_current accepted an unknown binding kind';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "address_names_current_binding_kind_check"%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO label_preimages (
        labelhash,
        raw_label,
        decoded_label,
        normalizer_version,
        normalized_under_version,
        normalization_error,
        source_kind,
        source_priority
    )
    VALUES
        (
            'labelhash-normalized',
            convert_to('normalized', 'UTF8'),
            'normalized',
            'check',
            true,
            NULL,
            'schema-check',
            0
        ),
        (
            'labelhash-rejected',
            decode('ff00', 'hex'),
            NULL,
            'check',
            false,
            'normalization failed',
            'schema-check',
            0
        );

    FOR transition_case IN
        SELECT *
        FROM (
            VALUES
                (
                    'labelhash-invalid-success',
                    true,
                    'unexpected error'::text
                ),
                (
                    'labelhash-invalid-failure',
                    false,
                    NULL::text
                )
        ) AS invalid(labelhash, normalized, normalization_error)
    LOOP
        BEGIN
            INSERT INTO label_preimages (
                labelhash,
                raw_label,
                decoded_label,
                normalizer_version,
                normalized_under_version,
                normalization_error,
                source_kind,
                source_priority
            )
            VALUES (
                transition_case.labelhash,
                convert_to('invalid', 'UTF8'),
                'invalid',
                'check',
                transition_case.normalized,
                transition_case.normalization_error,
                'schema-check',
                0
            );
            RAISE EXCEPTION
                'label_preimages accepted incoherent normalization state';
        EXCEPTION
            WHEN check_violation THEN
                IF SQLERRM NOT LIKE
                    '%constraint "label_preimages_normalization_coherence_check"%'
                THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    BEGIN
        INSERT INTO label_preimages (
            labelhash,
            raw_label,
            decoded_label,
            normalizer_version,
            normalized_under_version,
            normalization_error,
            source_kind,
            source_priority
        )
        VALUES (
            'labelhash-decoding-drift',
            convert_to('raw', 'UTF8'),
            'different',
            'check',
            false,
            'normalization failed',
            'schema-check',
            0
        );
        RAISE EXCEPTION
            'label_preimages accepted decoded text that differs from raw bytes';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "label_preimages_decoded_label_matches_raw_check"%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO label_preimages (
        labelhash,
        raw_label,
        decoded_label,
        normalizer_version,
        normalized_under_version,
        normalization_error,
        source_kind,
        source_priority
    )
    VALUES (
        'labelhash-oversized',
        oversized_bytes,
        oversized_text,
        'check',
        false,
        'normalization failed',
        'schema-check',
        0
    );

    INSERT INTO normalized_events (
        event_identity,
        namespace,
        event_kind,
        source_family,
        manifest_version,
        chain_id,
        block_number,
        block_hash,
        raw_fact_ref,
        derivation_kind,
        canonicality_state
    )
    VALUES (
        'event-oversized-interpreter-state-key',
        'schema-v2-check',
        'RecordChanged',
        'schema-check',
        1,
        'schema-v2-check',
        0,
        'block-0',
        jsonb_build_object(
            'interpreter_state_key', oversized_text,
            'state_scope', oversized_text
        ),
        'raw_log_preimage_observation',
        'canonical'
    );

    IF NOT EXISTS (
        SELECT 1
        FROM normalized_events
        WHERE chain_id = 'schema-v2-check'
          AND public.digest(
              COALESCE(
                  raw_fact_ref ->> 'interpreter_state_key',
                  event_identity
              ),
              'sha256'
          ) = public.digest(oversized_text, 'sha256')
          AND raw_fact_ref ->> 'interpreter_state_key' = oversized_text
    ) THEN
        RAISE EXCEPTION
            'oversized interpreter state key was not findable by its bounded digest';
    END IF;

    INSERT INTO ens_names (hash, name)
    VALUES ('hash-oversized', oversized_text);

    INSERT INTO name_surfaces (
        logical_name_id,
        namespace,
        raw_name,
        raw_labels,
        dns_encoded_name,
        namehash,
        labelhashes,
        normalizer_version,
        visibility_state,
        chain_id,
        block_hash,
        block_number,
        canonicality_state
    )
    VALUES (
        'schema-v2-check:namehash-oversized',
        'schema-v2-check',
        oversized_text,
        ARRAY[oversized_text],
        decode('', 'hex'),
        'namehash-oversized',
        ARRAY['labelhash-oversized'],
        'check',
        'active',
        'schema-v2-check',
        'block-0',
        0,
        'canonical'
    );

    INSERT INTO surface_bindings (
        surface_binding_id,
        logical_name_id,
        resource_id,
        binding_kind,
        authority_arm,
        active_from,
        chain_id,
        block_hash,
        block_number,
        canonicality_state
    )
    VALUES (
        '00000000-0000-0000-0000-000000000029',
        'schema-v2-check:namehash-oversized',
        '00000000-0000-0000-0000-000000000011',
        'declared_registry_path',
        'ens_v1',
        '2026-01-01 00:00:00+00',
        'schema-v2-check',
        'block-0',
        0,
        'canonical'
    );

    INSERT INTO name_current (
        logical_name_id,
        namespace,
        raw_name,
        namehash,
        surface_binding_id,
        resource_id,
        binding_kind,
        support_status,
        manifest_version
    )
    VALUES (
        'schema-v2-check:namehash-oversized',
        'schema-v2-check',
        oversized_text,
        'namehash-oversized',
        '00000000-0000-0000-0000-000000000029',
        '00000000-0000-0000-0000-000000000011',
        'declared_registry_path',
        'supported',
        1
    );

    INSERT INTO children_current (
        parent_logical_name_id,
        child_logical_name_id,
        namespace,
        raw_name,
        decoded_name,
        raw_label,
        decoded_label,
        namehash,
        labelhash,
        manifest_version
    )
    VALUES (
        'schema-v2-check:namehash-0',
        'schema-v2-check:namehash-oversized-child',
        'schema-v2-check',
        oversized_bytes,
        oversized_text,
        oversized_bytes,
        oversized_text,
        'namehash-oversized-child',
        'labelhash-oversized-child',
        1
    );

    INSERT INTO address_names_current (
        address,
        logical_name_id,
        relation,
        namespace,
        raw_name,
        namehash,
        surface_binding_id,
        resource_id,
        binding_kind,
        support_status,
        manifest_version
    )
    VALUES (
        'address-oversized',
        'schema-v2-check:namehash-oversized',
        'registrant',
        'schema-v2-check',
        oversized_text,
        'namehash-oversized',
        '00000000-0000-0000-0000-000000000029',
        '00000000-0000-0000-0000-000000000011',
        'declared_registry_path',
        'supported',
        1
    );

    INSERT INTO primary_names_current (
        address,
        coin_type,
        namespace,
        claim_status,
        raw_claim_name,
        claim_name_is_normalized
    )
    VALUES (
        'primary-oversized',
        '60',
        'schema-v2-check',
        'success',
        oversized_text,
        false
    );

    IF NOT EXISTS (
        SELECT 1 FROM label_preimages
        WHERE labelhash = 'labelhash-oversized' AND raw_label = oversized_bytes
    ) OR NOT EXISTS (
        SELECT 1 FROM ens_names
        WHERE hash = 'hash-oversized' AND name = oversized_text
    ) OR NOT EXISTS (
        SELECT 1 FROM name_surfaces
        WHERE namespace = 'schema-v2-check'
          AND visibility_state = 'active'
          AND namehash = 'namehash-oversized'
          AND raw_name = oversized_text
    ) OR NOT EXISTS (
        SELECT 1 FROM name_current
        WHERE namespace = 'schema-v2-check'
          AND namehash = 'namehash-oversized'
          AND logical_name_id = 'schema-v2-check:namehash-oversized'
          AND raw_name = oversized_text
    ) OR NOT EXISTS (
        SELECT 1 FROM children_current
        WHERE parent_logical_name_id = 'schema-v2-check:namehash-0'
          AND surface_class = 'declared'
          AND namehash = 'namehash-oversized-child'
          AND child_logical_name_id = 'schema-v2-check:namehash-oversized-child'
          AND raw_name = oversized_bytes
          AND raw_label = oversized_bytes
    ) OR NOT EXISTS (
        SELECT 1 FROM address_names_current
        WHERE lower(address) = 'address-oversized'
          AND relation = 'registrant'
          AND namespace = 'schema-v2-check'
          AND namehash = 'namehash-oversized'
          AND logical_name_id = 'schema-v2-check:namehash-oversized'
          AND raw_name = oversized_text
    ) OR NOT EXISTS (
        SELECT 1 FROM primary_names_current
        WHERE namespace = 'schema-v2-check'
          AND coin_type = '60'
          AND address = 'primary-oversized'
          AND claim_status = 'success'
          AND raw_claim_name = oversized_text
    ) THEN
        RAISE EXCEPTION
            'oversized input was not findable through every bounded lookup path';
    END IF;

    INSERT INTO primary_names_current (
        address,
        coin_type,
        namespace,
        claim_status,
        raw_claim_name,
        claim_name_is_normalized,
        unsupported_reason
    )
    VALUES
        (
            'primary-success',
            '60',
            'schema-v2-check',
            'success',
            'name.eth',
            true,
            NULL
        ),
        (
            'primary-not-found',
            '60',
            'schema-v2-check',
            'not_found',
            NULL,
            false,
            NULL
        ),
        (
            'primary-unsupported',
            '60',
            'schema-v2-check',
            'unsupported',
            NULL,
            false,
            'coin type unsupported'
        ),
        (
            'primary-invalid-name',
            '60',
            'schema-v2-check',
            'invalid_name',
            'invalid name',
            false,
            NULL
        );

    FOR transition_case IN
        SELECT *
        FROM (
            VALUES
                (
                    'primary-missing-name',
                    'success',
                    NULL::text,
                    false,
                    NULL::text,
                    'primary_names_current_claim_name_check'
                ),
                (
                    'primary-unexpected-name',
                    'not_found',
                    'name.eth',
                    false,
                    NULL::text,
                    'primary_names_current_claim_name_check'
                ),
                (
                    'primary-invalid-normalized',
                    'invalid_name',
                    'invalid name',
                    true,
                    NULL::text,
                    'primary_names_current_normalized_claim_check'
                ),
                (
                    'primary-missing-reason',
                    'unsupported',
                    NULL::text,
                    false,
                    NULL::text,
                    'primary_names_current_unsupported_reason_coherence_check'
                ),
                (
                    'primary-unexpected-reason',
                    'success',
                    'name.eth',
                    true,
                    'unexpected reason',
                    'primary_names_current_unsupported_reason_coherence_check'
                )
        ) AS invalid(
            address,
            claim_status,
            raw_claim_name,
            claim_name_is_normalized,
            unsupported_reason,
            constraint_name
        )
    LOOP
        BEGIN
            INSERT INTO primary_names_current (
                address,
                coin_type,
                namespace,
                claim_status,
                raw_claim_name,
                claim_name_is_normalized,
                unsupported_reason
            )
            VALUES (
                transition_case.address,
                '60',
                'schema-v2-check',
                transition_case.claim_status,
                transition_case.raw_claim_name,
                transition_case.claim_name_is_normalized,
                transition_case.unsupported_reason
            );
            RAISE EXCEPTION
                'primary_names_current accepted incoherent claim state';
        EXCEPTION
            WHEN check_violation THEN
                IF SQLERRM NOT LIKE format(
                    '%%constraint "%s"%%',
                    transition_case.constraint_name
                ) THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    INSERT INTO ingest_cursors (
        chain_id,
        source_key,
        source_kind,
        seed_basis,
        start_block_number,
        next_block_number,
        target_block_number,
        last_processed_block_number,
        last_processed_block_hash
    )
    VALUES (
        'schema-v2-check',
        'valid-source',
        'logs',
        'base_seam',
        10,
        11,
        12,
        10,
        'processed-block-10'
    );

    FOR transition_case IN
        SELECT *
        FROM (
            VALUES
                (
                    'invalid-next',
                    10::bigint,
                    9::bigint,
                    NULL::bigint,
                    NULL::bigint,
                    NULL::text,
                    'ingest_cursors_next_block_order_check'
                ),
                (
                    'invalid-target',
                    10::bigint,
                    10::bigint,
                    9::bigint,
                    NULL::bigint,
                    NULL::text,
                    'ingest_cursors_target_block_order_check'
                ),
                (
                    'invalid-last-pair',
                    10::bigint,
                    11::bigint,
                    NULL::bigint,
                    10::bigint,
                    NULL::text,
                    'ingest_cursors_last_processed_pair_check'
                ),
                (
                    'invalid-last-order',
                    10::bigint,
                    11::bigint,
                    NULL::bigint,
                    11::bigint,
                    'processed-block-11',
                    'ingest_cursors_last_processed_order_check'
                )
        ) AS invalid(
            source_key,
            start_block_number,
            next_block_number,
            target_block_number,
            last_processed_block_number,
            last_processed_block_hash,
            constraint_name
        )
    LOOP
        BEGIN
            INSERT INTO ingest_cursors (
                chain_id,
                source_key,
                source_kind,
                seed_basis,
                start_block_number,
                next_block_number,
                target_block_number,
                last_processed_block_number,
                last_processed_block_hash
            )
            VALUES (
                'schema-v2-check',
                transition_case.source_key,
                'logs',
                'base_seam',
                transition_case.start_block_number,
                transition_case.next_block_number,
                transition_case.target_block_number,
                transition_case.last_processed_block_number,
                transition_case.last_processed_block_hash
            );
            RAISE EXCEPTION
                'ingest_cursors accepted invalid ordering';
        EXCEPTION
            WHEN check_violation THEN
                IF SQLERRM NOT LIKE format(
                    '%%constraint "%s"%%',
                    transition_case.constraint_name
                ) THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    INSERT INTO service_heartbeats (
        service_name,
        instance_id,
        chain_id,
        phase_name,
        started_at,
        heartbeat_at
    )
    VALUES (
        'indexer',
        'instance-valid',
        'schema-v2-check',
        'project',
        '2026-01-01 00:00:00+00',
        '2026-01-01 00:00:01+00'
    );

    FOR transition_case IN
        SELECT *
        FROM (
            VALUES
                (
                    '',
                    'instance-invalid-service',
                    'schema-v2-check',
                    'project',
                    '2026-01-01 00:00:00+00'::timestamptz,
                    '2026-01-01 00:00:01+00'::timestamptz,
                    'service_heartbeats_service_name_check'
                ),
                (
                    'indexer',
                    '',
                    'schema-v2-check',
                    'project',
                    '2026-01-01 00:00:00+00'::timestamptz,
                    '2026-01-01 00:00:01+00'::timestamptz,
                    'service_heartbeats_instance_id_check'
                ),
                (
                    'indexer',
                    'instance-invalid-chain',
                    '',
                    'project',
                    '2026-01-01 00:00:00+00'::timestamptz,
                    '2026-01-01 00:00:01+00'::timestamptz,
                    'service_heartbeats_chain_id_check'
                ),
                (
                    'indexer',
                    'instance-invalid-phase',
                    'schema-v2-check',
                    'publish',
                    '2026-01-01 00:00:00+00'::timestamptz,
                    '2026-01-01 00:00:01+00'::timestamptz,
                    'service_heartbeats_phase_name_check'
                ),
                (
                    'indexer',
                    'instance-invalid-time',
                    'schema-v2-check',
                    'project',
                    '2026-01-01 00:00:01+00'::timestamptz,
                    '2026-01-01 00:00:00+00'::timestamptz,
                    'service_heartbeats_time_order_check'
                )
        ) AS invalid(
            service_name,
            instance_id,
            chain_id,
            phase_name,
            started_at,
            heartbeat_at,
            constraint_name
        )
    LOOP
        BEGIN
            INSERT INTO service_heartbeats (
                service_name,
                instance_id,
                chain_id,
                phase_name,
                started_at,
                heartbeat_at
            )
            VALUES (
                transition_case.service_name,
                transition_case.instance_id,
                transition_case.chain_id,
                transition_case.phase_name,
                transition_case.started_at,
                transition_case.heartbeat_at
            );
            RAISE EXCEPTION
                'service_heartbeats accepted an invalid row shape';
        EXCEPTION
            WHEN check_violation THEN
                IF SQLERRM NOT LIKE format(
                    '%%constraint "%s"%%',
                    transition_case.constraint_name
                ) THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    BEGIN
        INSERT INTO surface_bindings (
            surface_binding_id,
            logical_name_id,
            resource_id,
            binding_kind,
            authority_arm,
            active_from,
            chain_id,
            block_hash,
            block_number,
            canonicality_state
        )
        VALUES (
            '00000000-0000-0000-0000-000000000022',
            'schema-v2-check:namehash-0',
            '00000000-0000-0000-0000-000000000012',
            'declared_registry_path',
            'ens_v1',
            '2026-01-01 00:00:00+00',
            'schema-v2-check',
            'block-0',
            0,
            'canonical'
        );
        RAISE EXCEPTION
            'surface_bindings accepted overlapping canonical ranges';
    EXCEPTION
        WHEN exclusion_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "surface_bindings_no_overlap"%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO manifest_versions (
        manifest_version,
        namespace,
        source_family,
        chain_id,
        deployment_label,
        rollout_status,
        normalizer_version,
        file_path,
        manifest_payload
    )
    VALUES (
        1,
        'schema-v2-check',
        'schema-v2-check',
        'schema-v2-check',
        'schema-v2-check',
        'draft',
        'check',
        'schema-v2-check.toml',
        '{}'::jsonb
    )
    RETURNING manifest_id INTO manifest_key;

    BEGIN
        INSERT INTO manifest_discovery_rules (
            manifest_id,
            edge_kind,
            admission
        )
        VALUES (manifest_key, 'announced', 'declared');
        RAISE EXCEPTION
            'manifest_discovery_rules accepted an unknown edge kind';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "manifest_discovery_rules_edge_kind_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO manifest_contract_instances (
            manifest_id,
            chain_id,
            declaration_kind,
            declaration_name,
            contract_instance_id,
            declared_address,
            proxy_kind
        )
        VALUES (
            manifest_key,
            'schema-v2-check',
            'root',
            'cross-chain-root',
            '00000000-0000-0000-0000-000000000003',
            'cross-chain-address',
            'none'
        );
        accepted_manifest_mismatches :=
            array_append(
                accepted_manifest_mismatches,
                'manifest contract chain'
            );
        DELETE FROM manifest_contract_instances
        WHERE manifest_id = manifest_key
          AND declaration_name = 'cross-chain-root';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "manifest_contract_instances_chain_id_contract_instance_id_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            event_kind,
            source_family,
            manifest_version,
            source_manifest_id,
            chain_id,
            derivation_kind
        )
        VALUES (
            'mismatched-manifest-event',
            'wrong-namespace',
            'ResolverChanged',
            'wrong-source-family',
            99,
            manifest_key,
            'schema-v2-other',
            'ens_v2_resolver'
        );
        accepted_manifest_mismatches :=
            array_append(
                accepted_manifest_mismatches,
                'normalized event manifest'
            );
        DELETE FROM normalized_events
        WHERE event_identity = 'mismatched-manifest-event';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "normalized_events_source_manifest_id_namespace_source_fami_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    IF cardinality(accepted_manifest_mismatches) > 0 THEN
        RAISE EXCEPTION
            'manifest tables accepted contradictory provenance: %',
            array_to_string(accepted_manifest_mismatches, ', ');
    END IF;

    INSERT INTO manifest_contract_instances (
        manifest_id,
        chain_id,
        declaration_kind,
        declaration_name,
        contract_instance_id,
        declared_address,
        proxy_kind
    )
    VALUES (
        manifest_key,
        'schema-v2-check',
        'root',
        'root',
        '00000000-0000-0000-0000-000000000001',
        'contract-address-0',
        'none'
    );

    INSERT INTO manifest_discovery_rules (
        manifest_id,
        edge_kind,
        admission
    )
    VALUES (manifest_key, 'resolver', 'declared');

    UPDATE contract_instance_addresses
    SET source_manifest_id = manifest_key
    WHERE contract_instance_id =
        '00000000-0000-0000-0000-000000000001'
      AND deactivated_at IS NULL;

    INSERT INTO discovery_edges (
        chain_id,
        edge_kind,
        from_contract_instance_id,
        to_contract_instance_id,
        discovery_source,
        admission_basis,
        source_manifest_id
    )
    VALUES (
        'schema-v2-check',
        'proxy_implementation',
        '00000000-0000-0000-0000-000000000001',
        '00000000-0000-0000-0000-000000000002',
        'schema-v2-check',
        'schema-v2-check',
        manifest_key
    );

    INSERT INTO normalized_events (
        event_identity,
        namespace,
        event_kind,
        source_family,
        manifest_version,
        source_manifest_id,
        chain_id,
        derivation_kind
    )
    VALUES (
        'manifest-delete-check',
        'schema-v2-check',
        'ResolverChanged',
        'schema-v2-check',
        1,
        manifest_key,
        'schema-v2-check',
        'ens_v2_resolver'
    );

    DELETE FROM manifest_versions
    WHERE manifest_id = manifest_key;

    IF EXISTS (
        SELECT 1
        FROM manifest_contract_instances
        WHERE manifest_id = manifest_key
    ) OR EXISTS (
        SELECT 1
        FROM manifest_discovery_rules
        WHERE manifest_id = manifest_key
    ) THEN
        RAISE EXCEPTION
            'manifest deletion did not remove child declarations';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM contract_instance_addresses
        WHERE source_manifest_id = manifest_key
    ) OR EXISTS (
        SELECT 1
        FROM discovery_edges
        WHERE source_manifest_id = manifest_key
    ) OR EXISTS (
        SELECT 1
        FROM normalized_events
        WHERE source_manifest_id = manifest_key
    ) THEN
        RAISE EXCEPTION
            'manifest deletion did not clear retained provenance links';
    END IF;
END
$$;
SQL
} | run_psql

# The independent predecessor body and ACL are copied from public #855 f95200b3.
# The exact-zero result body hash is pinned from baseline 9f417401; the newer
# unsupported-inventory schema-migration is proved separately against the current baseline.
zero_default_migration="$ROOT/migrations/20260906120000_exact_zero_addr60_default_derivation.sql"
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
CREATE FUNCTION exact_zero_writer_metadata() RETURNS jsonb LANGUAGE sql AS $$
    SELECT jsonb_build_object(
        'definition', replace(pg_get_functiondef(p.oid), current_schema(), '<phase>'),
        'arguments', pg_get_function_identity_arguments(p.oid),
        'return_type', pg_get_function_result(p.oid), 'owner', p.proowner,
        'security_definer', p.prosecdef,
        'config', replace(to_jsonb(p.proconfig)::text, current_schema(), '<phase>')::jsonb,
        'acl', (SELECT jsonb_agg(jsonb_build_array(a.grantor, a.grantee,
                    a.privilege_type, a.is_grantable) ORDER BY a.grantor, a.grantee,
                    a.privilege_type, a.is_grantable)
                FROM aclexplode(COALESCE(p.proacl, acldefault('f', p.proowner))) a)
    ) FROM pg_proc p
    WHERE p.oid = 'write_resolution_divergence(uuid,text,text,text,bigint,text,jsonb,text,text,text,text,jsonb,jsonb,boolean)'::regprocedure;
$$;
CREATE TEMP TABLE exact_zero_expected_writer AS SELECT exact_zero_writer_metadata() AS metadata;
DO $$
BEGIN
    IF (SELECT metadata->>'security_definer' <> 'true'
        OR metadata->>'return_type' <> 'text'
        OR metadata->'config' <> '["search_path=pg_catalog, <phase>, pg_temp"]'::jsonb
        FROM exact_zero_expected_writer)
        OR EXISTS (SELECT 1 FROM pg_proc p,
            LATERAL aclexplode(COALESCE(p.proacl, acldefault('f', p.proowner))) a
            WHERE p.oid = 'write_resolution_divergence(uuid,text,text,text,bigint,text,jsonb,text,text,text,text,jsonb,jsonb,boolean)'::regprocedure
              AND a.grantee = 0 AND a.privilege_type = 'EXECUTE') THEN
        RAISE EXCEPTION 'fresh exact-zero writer privilege envelope is invalid';
    END IF;
END
$$;
BEGIN;
SELECT assert_exact_zero_migration_behavior();
ROLLBACK;
SQL
    for pass in 1 2; do
        emit_phase_migration "$zero_default_migration" baseline-first
        cat <<'SQL'
DO $$
BEGIN
    IF (SELECT md5(prosrc) FROM pg_proc
        WHERE oid = 'write_resolution_divergence(uuid,text,text,text,bigint,text,jsonb,text,text,text,text,jsonb,jsonb,boolean)'::regprocedure)
            IS DISTINCT FROM '23d078f731ff9405584f1587c0113045'
        OR exact_zero_writer_metadata() - 'definition'
            <> (SELECT metadata - 'definition' FROM exact_zero_expected_writer) THEN
        RAISE EXCEPTION 'exact-zero function definition, signature or privilege metadata diverged';
    END IF;
END
$$;
BEGIN;
SELECT assert_exact_zero_migration_behavior();
ROLLBACK;
SQL
    done
    cat <<'SQL'
CREATE OR REPLACE FUNCTION write_resolution_divergence(
    compared_resource_id uuid,
    compared_boundary_key text,
    compared_row_xmin text,
    requested_authoritative_chain_id text,
    requested_authoritative_block_number bigint,
    requested_authoritative_block_hash text,
    compared_execution_authority jsonb,
    requested_logical_name_id text,
    requested_resolver_chain_id text,
    requested_resolver_address text,
    requested_record_key text,
    compared_positions jsonb,
    live_answer jsonb,
    used_ccip_read boolean
)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, bigname_phase, pg_temp
AS $$
DECLARE
    guard_status text;
    resolver_path jsonb;
    compared_entries jsonb;
    compared_provenance jsonb;
    compared_support_status text;
    selector_family text;
    selector_key text;
    indexed_entry jsonb;
    default_entry jsonb;
    indexed_status text;
    indexed_value jsonb;
    indexed_answer jsonb;
BEGIN
    IF used_ccip_read THEN
        RETURN 'ccip_skipped';
    END IF;

    IF compared_execution_authority ->> 'logical_name_id'
        IS DISTINCT FROM requested_logical_name_id
    THEN
        RETURN 'guard_rejected';
    END IF;

    guard_status := revalidate_resolution_lookup_state(
        requested_authoritative_chain_id,
        requested_authoritative_block_number,
        requested_authoritative_block_hash,
        compared_positions,
        compared_execution_authority,
        compared_resource_id,
        compared_boundary_key,
        compared_row_xmin
    );

    IF guard_status <> 'unchanged' THEN
        RETURN 'guard_rejected';
    END IF;

    CASE
        WHEN requested_record_key = 'avatar' THEN
            selector_family := 'avatar';
            selector_key := NULL;
        WHEN requested_record_key = 'contenthash' THEN
            selector_family := 'contenthash';
            selector_key := NULL;
        WHEN requested_record_key LIKE 'text:%'
            AND length(substr(requested_record_key, 6)) > 0
        THEN
            selector_family := 'text';
            selector_key := substr(requested_record_key, 6);
        WHEN requested_record_key ~ '^addr:(0|[1-9][0-9]*)$' THEN
            BEGIN
                selector_key := substr(requested_record_key, 6);
                IF selector_key::numeric > 18446744073709551615::numeric THEN
                    RETURN 'guard_rejected';
                END IF;
                selector_family := 'addr';
            EXCEPTION
                WHEN data_exception THEN
                    RETURN 'guard_rejected';
            END;
        ELSE
            RETURN 'guard_rejected';
    END CASE;

    SELECT inventory.entries,
           inventory.provenance,
           inventory.support_status,
           name.declared_summary #> '{topology,resolver_path}'
    INTO compared_entries, compared_provenance, compared_support_status, resolver_path
    FROM record_inventory_current AS inventory
    JOIN name_current AS name
      ON name.logical_name_id = requested_logical_name_id
     AND name.support_status = 'supported'
     AND name.declared_summary
            #> '{topology,version_boundaries,record_version_boundary}' =
         inventory.record_version_boundary
    WHERE inventory.resource_id = compared_resource_id
      AND inventory.record_version_boundary_key = compared_boundary_key
      AND inventory.xmin::text = compared_row_xmin
    FOR SHARE OF inventory, name;

    IF NOT FOUND
        OR jsonb_typeof(resolver_path) IS DISTINCT FROM 'array'
        OR jsonb_array_length(resolver_path) = 0
        OR resolver_path -> (jsonb_array_length(resolver_path) - 1)
                ->> 'chain_id' <> requested_resolver_chain_id
        OR lower(
            resolver_path -> (jsonb_array_length(resolver_path) - 1)
                ->> 'address'
        ) <> lower(requested_resolver_address)
    THEN
        RETURN 'guard_rejected';
    END IF;

    SELECT candidate.entry
    INTO indexed_entry
    FROM jsonb_array_elements(compared_entries)
        WITH ORDINALITY AS candidate(entry, ordinal)
    WHERE candidate.entry ->> 'record_key' = requested_record_key
       OR (
            candidate.entry ->> 'record_family' = selector_family
            AND (candidate.entry ->> 'selector_key')
                IS NOT DISTINCT FROM selector_key
       )
       OR (
            requested_record_key = 'avatar'
            AND candidate.entry ->> 'record_key' = 'text:avatar'
       )
    ORDER BY CASE
        WHEN candidate.entry ->> 'record_key' = 'text:avatar'
            AND requested_record_key = 'avatar'
        THEN 1
        ELSE 0
    END,
    candidate.ordinal
    LIMIT 1;

    IF (indexed_entry IS NULL OR indexed_entry ->> 'status' = 'not_found')
       AND selector_family = 'addr'
       AND (
           selector_key = '60'
           OR selector_key::numeric BETWEEN 2147483649::numeric AND 4294967295::numeric
       )
       AND EXISTS (
           SELECT 1
           FROM jsonb_array_elements(COALESCE(
               compared_provenance -> 'read_rules', '[]'::jsonb
           )) rule
           WHERE rule ->> 'kind' = 'ensip19_default_address'
             AND rule ->> 'source_record_key' = 'addr:2147483648'
       )
    THEN
        IF compared_support_status <> 'supported' THEN
            indexed_entry := jsonb_build_object('status', 'unsupported');
        ELSE
            SELECT candidate.entry
            INTO default_entry
            FROM jsonb_array_elements(compared_entries)
                WITH ORDINALITY AS candidate(entry, ordinal)
            WHERE candidate.entry ->> 'record_key' = 'addr:2147483648'
               OR (
                    candidate.entry ->> 'record_family' = 'addr'
                    AND candidate.entry ->> 'selector_key' = '2147483648'
               )
            ORDER BY candidate.ordinal
            LIMIT 1;

            IF default_entry IS NULL THEN
                indexed_entry := jsonb_build_object('status', 'not_found');
            ELSIF default_entry ->> 'status' IN ('success', 'not_found') THEN
                -- Match the requested getter's verified decode. addr(bytes32) converts
                -- the coin-60 bytes to address(0); multicoin addr(bytes32,uint256)
                -- preserves non-empty bytes, including 20 zero bytes.
                -- (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L36-L40 @ ens_v1@91c966f)
                -- (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L685-L697 @ ens_v2_sepolia_20260629@ccaeb58)
                IF selector_key = '60'
                   AND default_entry ->> 'status' = 'success'
                   AND lower(COALESCE(
                       default_entry #>> '{value,value}',
                       default_entry #>> '{value,bytes}',
                       default_entry ->> 'value'
                   )) = '0x0000000000000000000000000000000000000000'
                THEN
                    indexed_entry := jsonb_build_object('status', 'not_found');
                ELSE
                    indexed_entry := default_entry;
                END IF;
            ELSE
                indexed_entry := jsonb_build_object('status', 'unsupported');
            END IF;
        END IF;
    ELSIF (indexed_entry IS NULL OR indexed_entry ->> 'status' = 'not_found')
          AND compared_support_status <> 'supported'
    THEN
        indexed_entry := jsonb_build_object('status', 'unsupported');
    END IF;

    IF indexed_entry IS NULL THEN
        indexed_answer := jsonb_build_object('status', 'not_found');
    ELSE
        indexed_status := CASE COALESCE(
            indexed_entry ->> 'status',
            'unsupported'
        )
            WHEN 'failed' THEN 'execution_failed'
            ELSE COALESCE(indexed_entry ->> 'status', 'unsupported')
        END;
        indexed_answer := jsonb_build_object('status', indexed_status);
        IF indexed_status = 'success' THEN
            indexed_value := COALESCE(
                indexed_entry #> '{value,value}',
                indexed_entry #> '{value,bytes}',
                indexed_entry -> 'value'
            );
            IF jsonb_typeof(indexed_value) = 'string' THEN
                indexed_answer := indexed_answer || jsonb_build_object(
                    'value',
                    CASE
                        WHEN selector_family = 'addr'
                            THEN lower(indexed_value #>> '{}')
                        ELSE indexed_value #>> '{}'
                    END
                );
            ELSE
                indexed_answer := jsonb_build_object('status', 'unsupported');
            END IF;
        END IF;
    END IF;

    IF indexed_answer = live_answer THEN
        UPDATE resolution_divergences
        SET cleared_at = GREATEST(statement_timestamp(), last_observed_at)
        WHERE logical_name_id = requested_logical_name_id
          AND resolver_chain_id = requested_resolver_chain_id
          AND lower(resolver_address) = lower(requested_resolver_address)
          AND request_kind_hash =
              public.digest(requested_record_key, 'sha256')
          AND request_kind = requested_record_key
          AND cleared_at IS NULL;
        IF FOUND THEN
            RETURN 'cleared';
        END IF;
        RETURN 'agreement';
    END IF;

    UPDATE resolution_divergences
    SET cleared_at = GREATEST(statement_timestamp(), last_observed_at)
    WHERE logical_name_id = requested_logical_name_id
      AND resolver_chain_id = requested_resolver_chain_id
      AND lower(resolver_address) = lower(requested_resolver_address)
      AND request_kind_hash =
          public.digest(requested_record_key, 'sha256')
      AND request_kind = requested_record_key
      AND observed_positions <> compared_positions
      AND cleared_at IS NULL;

    IF EXISTS (
        SELECT 1
        FROM resolution_divergences
        WHERE logical_name_id = requested_logical_name_id
          AND resolver_chain_id = requested_resolver_chain_id
          AND lower(resolver_address) = lower(requested_resolver_address)
          AND request_kind_hash =
              public.digest(requested_record_key, 'sha256')
          AND request_kind <> requested_record_key
    ) THEN
        RAISE EXCEPTION 'resolution divergence request-key hash collision'
            USING ERRCODE = '23514';
    END IF;

    INSERT INTO resolution_divergences (
        logical_name_id,
        resolver_chain_id,
        resolver_address,
        request_kind,
        observed_positions,
        indexed_result,
        live_result
    ) VALUES (
        requested_logical_name_id,
        requested_resolver_chain_id,
        lower(requested_resolver_address),
        requested_record_key,
        compared_positions,
        indexed_answer,
        live_answer
    )
    ON CONFLICT ON CONSTRAINT resolution_divergences_pkey DO UPDATE
    SET indexed_result = EXCLUDED.indexed_result,
        live_result = EXCLUDED.live_result,
        last_observed_at = GREATEST(
            resolution_divergences.last_observed_at,
            statement_timestamp()
        ),
        cleared_at = NULL
    WHERE resolution_divergences.request_kind = EXCLUDED.request_kind;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'resolution divergence request-key hash collision'
            USING ERRCODE = '23514';
    END IF;

    RETURN 'written';
END
$$;

REVOKE ALL ON FUNCTION write_resolution_divergence(
    uuid, text, text, text, bigint, text, jsonb, text, text, text,
    text, jsonb, jsonb, boolean
) FROM PUBLIC;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_proc
        WHERE oid = 'write_resolution_divergence(uuid,text,text,text,bigint,text,jsonb,text,text,text,text,jsonb,jsonb,boolean)'::regprocedure
          AND md5(prosrc) = '6a4678aa84cd76e410d7883daed9f8fa'
          AND proconfig = ARRAY['search_path=pg_catalog, bigname_phase, pg_temp'])
        OR exact_zero_writer_metadata() - 'definition' - 'config'
            <> (SELECT metadata - 'definition' - 'config' FROM exact_zero_expected_writer)
        OR exact_zero_writer_metadata() = (SELECT metadata FROM exact_zero_expected_writer) THEN
        RAISE EXCEPTION 'independent f952 predecessor was not installed';
    END IF;
END
$$;
SQL
    for pass in 1 2; do
        emit_phase_migration "$zero_default_migration" preceding-shape
        cat <<'SQL'
DO $$
BEGIN
    IF (SELECT md5(prosrc) FROM pg_proc
        WHERE oid = 'write_resolution_divergence(uuid,text,text,text,bigint,text,jsonb,text,text,text,text,jsonb,jsonb,boolean)'::regprocedure)
            IS DISTINCT FROM '23d078f731ff9405584f1587c0113045'
        OR exact_zero_writer_metadata() - 'definition'
            <> (SELECT metadata - 'definition' FROM exact_zero_expected_writer) THEN
        RAISE EXCEPTION 'exact-zero function definition, signature or privilege metadata diverged';
    END IF;
END
$$;
BEGIN;
SELECT assert_exact_zero_migration_behavior();
ROLLBACK;
SQL
    done
    emit_phase_migration \
        "$ROOT/migrations/20260913120000_unsupported_inventory_serves_no_record_values.sql" \
        preceding-shape
    cat <<'SQL'
DO $$
BEGIN
    IF exact_zero_writer_metadata() <> (SELECT metadata FROM exact_zero_expected_writer) THEN
        RAISE EXCEPTION 'unsupported-inventory writer upgrade diverged from the current baseline';
    END IF;
END
$$;
BEGIN;
SELECT assert_exact_zero_migration_behavior();
ROLLBACK;
DROP TABLE exact_zero_expected_writer;
DROP FUNCTION exact_zero_writer_metadata();
DROP FUNCTION assert_exact_zero_migration_behavior();
SQL
} | run_psql
assert_migration_context_count "$zero_default_migration" empty-schema 1
assert_migration_context_count "$zero_default_migration" baseline-first 2
assert_migration_context_count "$zero_default_migration" preceding-shape 2
report_timing exact-zero-default

{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
INSERT INTO chain_lineage (
    chain_id,
    block_hash,
    block_number,
    block_timestamp,
    canonicality_state
)
VALUES (
    'schema-v2-race',
    'race-block-0',
    0,
    '2026-01-01 00:00:00+00',
    'canonical'
);

INSERT INTO name_surfaces (
    logical_name_id,
    namespace,
    raw_name,
    raw_labels,
    dns_encoded_name,
    namehash,
    labelhashes,
    normalizer_version,
    visibility_state,
    chain_id,
    block_hash,
    block_number,
    canonicality_state
)
VALUES (
    'schema-v2-race:race-namehash',
    'schema-v2-race',
    'race',
    ARRAY['race'],
    decode('00', 'hex'),
    'race-namehash',
    ARRAY['race-labelhash'],
    'schema-v2-check',
    'active',
    'schema-v2-race',
    'race-block-0',
    0,
    'canonical'
);
SQL
} | run_psql

schema_v2_race_log="$(mktemp)"
schema_v2_race_application="schema_v2_race_${PPID}_$$"

{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf "SET application_name TO '%s';\n" \
        "$schema_v2_race_application"
    cat <<'SQL'
BEGIN;

INSERT INTO resolution_divergences (
    logical_name_id,
    resolver_chain_id,
    resolver_address,
    request_kind,
    observed_positions,
    indexed_result,
    live_result
)
VALUES (
    'schema-v2-race:race-namehash',
    'schema-v2-race',
    'resolver-address-race',
    'addr',
    '{
        "resolver": {
            "chain_id": "schema-v2-race",
            "block_hash": "race-block-0",
            "block_number": 0,
            "timestamp": "2026-01-01T00:00:00Z"
        }
    }'::jsonb,
    '{"value": "indexed"}'::jsonb,
    '{"value": "live"}'::jsonb
);

SELECT pg_sleep(5);
COMMIT;
SQL
} | run_psql >"$schema_v2_race_log" 2>&1 &
schema_v2_race_pid=$!

if ! wait_for_schema_v2_race_session "$schema_v2_race_application"; then
    printf '%s\n' \
        "concurrent resolution-difference insert did not become ready" >&2
    cat "$schema_v2_race_log" >&2
    exit 1
fi

{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
UPDATE chain_lineage
SET canonicality_state = 'orphaned'
WHERE chain_id = 'schema-v2-race'
  AND block_hash = 'race-block-0';
SQL
} | run_psql

if ! wait "$schema_v2_race_pid"; then
    cat "$schema_v2_race_log" >&2
    exit 1
fi
schema_v2_race_pid=""

{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM resolution_divergences
        WHERE resolver_address = 'resolver-address-race'
          AND cleared_at IS NULL
    ) THEN
        RAISE EXCEPTION
            'concurrent reorg left an active resolution difference';
    END IF;
END
$$;
SQL
} | run_psql

rm -f -- "$schema_v2_race_log"
schema_v2_race_log=""

{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
INSERT INTO chain_lineage (
    chain_id,
    block_hash,
    block_number,
    block_timestamp,
    canonicality_state
)
VALUES (
    'schema-v2-head-race',
    'head-race-block-0',
    0,
    '2026-01-01 00:00:00+00',
    'canonical'
);
SQL
} | run_psql

schema_v2_race_log="$(mktemp)"
schema_v2_race_application="schema_v2_head_race_${PPID}_$$"

{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf "SET application_name TO '%s';\n" \
        "$schema_v2_race_application"
    cat <<'SQL'
BEGIN;

INSERT INTO chain_heads (
    chain_id,
    latest_block_hash,
    latest_block_number
)
VALUES (
    'schema-v2-head-race',
    'head-race-block-0',
    0
);

SELECT pg_sleep(5);
COMMIT;
SQL
} | run_psql >"$schema_v2_race_log" 2>&1 &
schema_v2_race_pid=$!

if ! wait_for_schema_v2_race_session "$schema_v2_race_application"; then
    printf '%s\n' "concurrent chain-head insert did not become ready" >&2
    cat "$schema_v2_race_log" >&2
    exit 1
fi

schema_v2_head_update_output=""
if ! schema_v2_head_update_output="$(
    {
        printf 'SET search_path TO "%s";\n' "$scratch_schema"
        cat <<'SQL'
UPDATE chain_lineage
SET canonicality_state = 'orphaned'
WHERE chain_id = 'schema-v2-head-race'
  AND block_hash = 'head-race-block-0';
SQL
    } | run_psql 2>&1
)"; then
    if [[ "$schema_v2_head_update_output" != \
        *"a chain head still references this block state"* ]]; then
        printf '%s\n' "$schema_v2_head_update_output" >&2
        exit 1
    fi
fi

if ! wait "$schema_v2_race_pid"; then
    cat "$schema_v2_race_log" >&2
    exit 1
fi
schema_v2_race_pid=""

{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM chain_heads AS head
        JOIN chain_lineage AS lineage
          ON lineage.chain_id = head.chain_id
         AND lineage.block_hash = head.latest_block_hash
         AND lineage.block_number = head.latest_block_number
        WHERE lineage.canonicality_state
            NOT IN ('canonical', 'safe', 'finalized')
    ) THEN
        RAISE EXCEPTION
            'concurrent reorg left a head on a noncanonical block';
    END IF;
END
$$;
SQL
} | run_psql

report_timing specialized-predecessor "$refusal_probe_seconds"
if [ "${SCHEMA_V2_APPLY_CHECK_TIMING:-0}" = 1 ]; then printf 'schema-v2 timing: refusal-probes=%ss\n' "$refusal_probe_seconds"; fi
assert_uninventoried_migrations_are_schema_qualified
assert_documented_head_is_newest_migration
assert_no_migration_below_prior_head
assert_frozen_schema_fingerprint
assert_frozen_catalog_sees_planted_changes
assert_exercised_schema_matches_frozen
assert_reviewed_phase_migrations_applied
if [ "$refusal_assertions_passed" -ne "$expected_refusal_assertions" ]; then
    printf '%s\n' \
        "refusal assertions: $refusal_assertions_passed/$expected_refusal_assertions" >&2
    exit 1
fi
report_timing final-assertions
printf '%s\n' \
    "schema-v2 baseline applied twice and passed structural and behavior checks"
printf '%s\n' \
    "schema-migration coverage: expected reviewed phase schema-migrations=$expected_reviewed_phase_migration_count; applied on maintained paths=$unique_successful_migration_count; exact-predecessor-shape proofs=$predecessor_shape_proof_count/$expected_reviewed_phase_migration_count; without exact-predecessor-shape proof=$((expected_reviewed_phase_migration_count - predecessor_shape_proof_count)); total successful applications=$total_successful_migration_applications; intentional skips=$intentional_phase_migration_skip_count; refusal assertions=$refusal_assertions_passed/$expected_refusal_assertions"
