//! The history of the composition pass tests (families_compose.rs, families_compose_builder.rs):
//! `rebuild_performance/seed.sql` for 60 names with the ENSv2 path history of `ens_v2_path.rs`,
//! and on top ENSv1 registry children a NewOwner creates with no name surface, record values by
//! node and by record id, unnamed registrar transfers of a name with no surface in two blocks, two
//! names bound to one resource, and an unnamed resource pointer whose node moves. Also a probe
//! that records what each committed chunk of a pass left.
use anyhow::Result;
use serde_json::json;
use sqlx::raw_sql;

use super::{CHAIN, Fixture, ens_v2_path, uuid};

pub const SEED: &str = include_str!("../rebuild_performance/seed.sql");
pub const R1: &str = "0x00000000000000000000000000000000000000a1";
pub const ALICE: &str = "0x00000000000000000000000000000000000000aa";
pub const CAROL: &str = "0x00000000000000000000000000000000000000cc";
/// The namehash of a registrar name no surface names, transferred unnamed at blocks 160 and 180.
pub const TRANSFERRED: &str = "0x00000000000000000000000000000000000000000000000000000000000f01d0";
/// The nodes an unnamed ENSv2 resource pointer names at block 165, then at block 185. Neither
/// has a surface, and only the interpreted events still hold the first.
pub const MOVED_FROM: &str = "0x000000000000000000000000000000000000000000000000000000000000a0a0";
pub const MOVED_TO: &str = "0x000000000000000000000000000000000000000000000000000000000000b0b0";

/// ENSv1 registry children with no name surface, as `families_benchmark.rs` writes them: one per
/// twentieth name, created by a NewOwner under a seed name, every second one transferred 30
/// blocks later.
pub const REGISTRY_CHILDREN: &str = "
INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state)
SELECT md5('c' || i)::uuid, '__CHAIN__', '0x' || lpad(to_hex(1 + (i % 270)), 64, '0'),
       1 + (i % 270), 'canonical'::canonicality_state
FROM generate_series(1, __NAMES__ / 20) i;
INSERT INTO normalized_events (event_identity, namespace, resource_id, event_kind, source_family,
    manifest_version, chain_id, block_number, block_hash, transaction_hash, transaction_index,
    log_index, derivation_kind, canonicality_state, after_state, raw_fact_ref)
SELECT 'seed:child:' || kind || ':' || i, 'ens', md5('c' || i)::uuid, kind,
       'ens_v1_registry_l1', 1, '__CHAIN__', block, '0x' || lpad(to_hex(block), 64, '0'),
       '0x' || md5('ct' || i || kind), 0, 20, 'ens_v1_unwrapped_authority',
       'canonical'::canonicality_state,
       CASE kind
           WHEN 'SubregistryChanged' THEN jsonb_build_object('source_event', 'NewOwner',
               'node', '0x' || md5('n' || i) || md5('m' || i),
               'child_node', '0x' || md5('cn' || i) || md5('cm' || i),
               'labelhash', '0x' || md5('cl' || i) || md5('ck' || i),
               'owner', '0x' || substr(md5('co' || i), 1, 40),
               'owner_getter', '0x' || substr(md5('co' || i), 1, 40), 'emitter_role', 'registry')
           ELSE jsonb_build_object('source_event', 'Transfer',
               'node', '0x' || md5('cn' || i) || md5('cm' || i),
               'owner', '0x' || substr(md5('cp' || i), 1, 40),
               'owner_getter', '0x' || substr(md5('cp' || i), 1, 40), 'emitter_role', 'registry')
       END,
       '{\"emitting_address\":\"0x00000000000000000000000000000000000000a3\"}'
FROM generate_series(1, __NAMES__ / 20) i
CROSS JOIN LATERAL (VALUES ('SubregistryChanged', 1 + (i % 270)),
    ('AuthorityTransferred', 31 + (i % 270))) kinds(kind, block)
WHERE kind = 'SubregistryChanged' OR i % 2 = 0;
";

pub fn node(n: u64) -> String {
    format!("0x{n:064x}")
}

/// The history described at the top, on canonical blocks 0 to 300.
pub async fn seeded(prefix: &str) -> Result<Fixture> {
    let fixture = Fixture::new(prefix, 0).await?;
    for script in [SEED, REGISTRY_CHILDREN] {
        raw_sql(
            &script
                .replace("__NAMES__", "60")
                .replace("__CHAIN__", CHAIN)
                .replace("1700000000", "1800000000"),
        )
        .execute(&fixture.pool)
        .await?;
    }
    ens_v2_path::history(&fixture).await?;
    let addr = |node: String, value: &str| {
        json!({"node": node, "resolver": R1, "record_key": "addr:60", "record_family": "addr",
               "selector_key": "60", "value": value, "source_event": "AddressChanged"})
    };
    fixture
        .write(
            150,
            1,
            "RecordChanged",
            "ens_v1_resolver_l1",
            None,
            None,
            addr(node(1), ALICE),
            R1,
        )
        .await?;
    fixture
        .write(
            151,
            1,
            "RecordChanged",
            "ens_v2_resolver_l1",
            None,
            None,
            json!({"resolver": R1, "resolver_record_id": "7",
                   "storage_model": "resolver_record_id", "record_key": "addr:60",
                   "record_family": "addr", "selector_key": "60", "value": CAROL,
                   "source_event": "AddressUpdated"}),
            R1,
        )
        .await?;
    let lease = uuid(901);
    for (block, to) in [(160, ALICE), (180, CAROL)] {
        fixture
            .write(
                block,
                7,
                "TokenControlTransferred",
                "ens_v1_registrar_l1",
                None,
                Some(&lease),
                json!({"namehash": TRANSFERRED, "to": to, "source_event": "Transfer",
                       "authority_kind": "registrar"}),
                R1,
            )
            .await?;
    }
    // Two names bound to one resource serve their records from one inventory.
    let shared = uuid(903);
    let (first, second) = (format!("ens:{}", node(901)), format!("ens:{}", node(902)));
    fixture
        .binding(&uuid(904), &first, &shared, "ens_v1", 140, 0, None)
        .await?;
    fixture
        .binding(&uuid(905), &second, &shared, "ens_v1", 140, 0, None)
        .await?;
    fixture
        .write(
            140,
            1,
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            Some(&first),
            Some(&shared),
            json!({"authority_kind": "registrar", "status": "registered", "registrant": ALICE,
                   "expiry": 2_000_000_000u64}),
            R1,
        )
        .await?;
    fixture
        .write(
            140,
            2,
            "ResolverChanged",
            "ens_v1_registry_l1",
            Some(&first),
            Some(&shared),
            json!({"node": node(901), "resolver": R1}),
            R1,
        )
        .await?;
    // An ENSv2 registry's pointer, so no ENSv1 registry node pointer keeps the node it left.
    let pointer = uuid(902);
    for (block, at) in [(165, MOVED_FROM), (185, MOVED_TO)] {
        fixture
            .write(
                block,
                7,
                "ResolverChanged",
                "ens_v2_registry_l1",
                None,
                Some(&pointer),
                json!({"node": at, "resolver": R1}),
                R1,
            )
            .await?;
    }
    Ok(fixture)
}

/// Install the probe: every chunk of a pass that commits logs, inside its own transaction, the
/// stream it composed, the stream and cursor it left, the record's chunk count and the session's
/// `work_mem` (`compose_probe_log`). The catalogue build's `done` partition copies the build's
/// work table (`compose_probe_work`) and the name work list (`compose_probe_names`), and the pass's beginning logs its work list's size
/// (`compose_probe_list`). Nothing the families read sees the probe.
pub async fn install_probe(fixture: &Fixture) -> Result<()> {
    raw_sql(
        "CREATE TABLE compose_probe_log (seen bigserial PRIMARY KEY, stream smallint,
             next_stream smallint, cursor text, chunks bigint, work_mem text,
             at timestamptz NOT NULL);
         CREATE TABLE compose_probe_work (LIKE project_compose_history_work);
         CREATE TABLE compose_probe_names (LIKE project_compose_name_work);
         CREATE TABLE compose_probe_list (seen bigserial PRIMARY KEY, names bigint NOT NULL);
         CREATE FUNCTION compose_probe() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             INSERT INTO compose_probe_log (stream, next_stream, cursor, chunks, work_mem, at)
             VALUES (OLD.compose_stream, NEW.compose_stream, NEW.compose_cursor,
                     NEW.compose_chunks, current_setting('work_mem'), clock_timestamp());
             IF OLD.compose_stream = 3 AND NEW.compose_cursor IS NOT NULL THEN
                 IF NEW.compose_cursor::jsonb ->> 'step' = 'done' THEN
                     DELETE FROM compose_probe_work;
                     INSERT INTO compose_probe_work SELECT * FROM project_compose_history_work;
                     DELETE FROM compose_probe_names;
                     INSERT INTO compose_probe_names SELECT * FROM project_compose_name_work;
                 END IF;
             END IF;
             RETURN NEW;
         END $$;
         CREATE TRIGGER compose_probe AFTER UPDATE ON project_repair_record
         FOR EACH ROW WHEN (NEW.compose_chunks > OLD.compose_chunks)
         EXECUTE FUNCTION compose_probe();
         CREATE FUNCTION compose_probe_list() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             INSERT INTO compose_probe_list (names) SELECT count(*) FROM project_compose_name_work;
             RETURN NULL;
         END $$;
         CREATE TRIGGER compose_probe_list AFTER INSERT ON project_compose_name_work
         FOR EACH STATEMENT EXECUTE FUNCTION compose_probe_list();",
    )
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

/// Forget what the probe saw.
pub async fn reset_probe(fixture: &Fixture) -> Result<()> {
    raw_sql(
        "TRUNCATE compose_probe_log, compose_probe_work, compose_probe_names, compose_probe_list",
    )
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

/// The committed chunks the probe logged, in commit order: the stream each composed and the
/// cursor it left.
pub async fn probe_chunks(fixture: &Fixture) -> Result<Vec<(Option<i16>, Option<String>)>> {
    Ok(
        sqlx::query_as("SELECT stream, cursor FROM compose_probe_log ORDER BY seen")
            .fetch_all(&fixture.pool)
            .await?,
    )
}

/// The size of the work list the last pass began with.
pub async fn probe_list(fixture: &Fixture) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT coalesce((SELECT names FROM compose_probe_list ORDER BY seen DESC LIMIT 1), -1)",
    )
    .fetch_one(&fixture.pool)
    .await?)
}

/// The catalogue build's partitions in the order it commits them under `spans` block spans and
/// `partitions` key partitions, as (step, part).
pub fn build_order(spans: i64, partitions: i64) -> Vec<(String, i64)> {
    let mut order = vec![("keys".to_owned(), 0)];
    order.extend((0..spans).map(|part| ("spans".to_owned(), part)));
    for step in [
        "edges",
        "sources",
        "edge_bounds",
        "name_anchors",
        "resource_anchors",
    ] {
        order.extend((0..partitions).map(|part| (step.to_owned(), part)));
    }
    order.push(("done".to_owned(), 0));
    order
}

/// The (step, part) of a catalogue build cursor, with its partition counts.
pub fn build_step(cursor: &str) -> Option<((String, i64), (i64, i64))> {
    let cursor: serde_json::Value = serde_json::from_str(cursor).ok()?;
    Some((
        (
            cursor["step"].as_str()?.to_owned(),
            cursor["part"].as_i64()?,
        ),
        (cursor["spans"].as_i64()?, cursor["partitions"].as_i64()?),
    ))
}

/// The build partitions the probe logged, in commit order.
pub async fn probe_build(fixture: &Fixture) -> Result<Vec<(String, i64)>> {
    Ok(probe_chunks(fixture)
        .await?
        .into_iter()
        .filter(|(stream, _)| *stream == Some(3))
        .filter_map(|(_, cursor)| build_step(cursor.as_deref()?).map(|(step, _)| step))
        .collect())
}
