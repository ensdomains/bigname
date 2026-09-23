use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::{Postgres, Transaction};

// Twenty linked mirror pointers: mirror resource `m-i` points at name `chain-i` and at
// `chain-(i+1)`. Starting from `chain-1`, each include pass can reach exactly one new key,
// alternating a resource hop (`chain-i` -> `m-i`) and a name hop (`m-i` -> `chain-(i+1)`),
// so the closure needs 39 growing passes (m-1, chain-2, m-2, ..., chain-20, m-20) plus
// one converging pass: 40 iterations.
const CHAIN: i64 = 20;
const CHAIN_FIXTURE: &str = r#"
INSERT INTO name_surfaces
SELECT 'chain-'||i,'ens','chain-node-'||i,'bench',10,'block','canonical',ARRAY['chain-'||i,'test']
FROM generate_series(1, $1) i;
INSERT INTO normalized_events
SELECT 5000+i,'chain-v1-'||i,'bench','ens',NULL,NULL,'ResolverChanged','ens_v1_registry_l1',1,1,10,
       'block',0,0,'canonical','activated',jsonb_build_object('node','chain-node-'||i,'resolver','0xshared'),'{}','{}'
FROM generate_series(1, $1) i;
INSERT INTO normalized_events
SELECT 6000+i,'chain-own-'||i,'bench','ens',md5('m-'||i)::uuid,'chain-'||i,'ResolverChanged','ens_v2_registry_l1',1,1,10,
       'block',0,0,'canonical','activated',jsonb_build_object('node','chain-node-'||i,'resolver','0xMIRROR'),'{}','{}'
FROM generate_series(1, $1) i;
INSERT INTO normalized_events
SELECT 7000+i,'chain-next-'||i,'bench','ens',md5('m-'||i)::uuid,'chain-'||(i+1),'ResolverChanged','ens_v2_registry_l1',1,1,10,
       'block',0,0,'canonical','activated',jsonb_build_object('node','chain-node-'||(i+1),'resolver','0xMIRROR'),'{}','{}'
FROM generate_series(1, $1 - 1) i;
"#;

async fn relation_locks(tx: &mut Transaction<'_, Postgres>) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM pg_locks WHERE pid = pg_backend_pid() AND locktype = 'relation'",
    )
    .fetch_one(&mut **tx)
    .await?)
}

async fn scope_size(tx: &mut Transaction<'_, Postgres>) -> Result<(i64, i64)> {
    Ok(sqlx::query_as(
        "SELECT (SELECT count(*) FROM project_scope_names),
                (SELECT count(*) FROM project_scope_resources)",
    )
    .fetch_one(&mut **tx)
    .await?)
}

// PostgreSQL holds the lock of every relation created or dropped until the transaction
// ends. The mirror work tables are created once per publication and truncated between
// passes, so the relation locks a publication holds must not grow with the hop count.
#[tokio::test]
async fn mirror_relation_locks_stay_constant_across_closure_hops() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("mirror_scope_locks")).await?;
    let mut tx = database.pool().begin().await?;
    sqlx::raw_sql(include_str!("../../testdata/sql/scope/mirror_fixture.sql"))
        .execute(&mut *tx)
        .await?;
    for statement in CHAIN_FIXTURE
        .split(';')
        .filter(|sql| !sql.trim().is_empty())
    {
        sqlx::query(statement).bind(CHAIN).execute(&mut *tx).await?;
    }
    sqlx::query("ANALYZE normalized_events")
        .execute(&mut *tx)
        .await?;
    sqlx::query("ANALYZE name_surfaces")
        .execute(&mut *tx)
        .await?;
    sqlx::query("TRUNCATE project_scope_names, project_scope_resources, project_changed_events")
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO project_scope_names VALUES('chain-1')")
        .execute(&mut *tx)
        .await?;

    let before = relation_locks(&mut tx).await?;
    let mut strategy = super::stage(&mut tx, "bench", 10).await?;
    let mut after_first = None;
    let mut hops = Vec::new();
    let mut iterations = 0;
    loop {
        let size = scope_size(&mut tx).await?;
        super::include(&mut tx, "bench", 10, &mut strategy).await?;
        iterations += 1;
        if after_first.is_none() {
            after_first = Some(relation_locks(&mut tx).await?);
        }
        let grown = scope_size(&mut tx).await?;
        if grown == size {
            break;
        }
        hops.push(if grown.0 > size.0 { "name" } else { "resource" });
        assert_eq!(
            (grown.0 - size.0) + (grown.1 - size.1),
            1,
            "pass {iterations} must add exactly one key"
        );
        assert!(iterations <= 2 * CHAIN + 1, "closure did not converge");
    }
    let after = relation_locks(&mut tx).await?;
    super::finish(&mut tx, strategy).await?;

    assert_eq!(iterations, 2 * CHAIN, "iterations");
    assert!(iterations >= 30);
    for (index, hop) in hops.iter().enumerate() {
        let expected = if index % 2 == 0 { "resource" } else { "name" };
        assert_eq!(*hop, expected, "hop {index}");
    }
    let one_time = after_first.expect("at least one pass") - before;
    eprintln!(
        "relation locks: before {before}, after the first pass {}, after {iterations} passes {after}",
        before + one_time
    );
    assert!(
        after - before <= one_time,
        "relation locks grew with the hop count: before {before}, after the first of \
         {iterations} passes {}, after the loop {after}",
        before + one_time
    );
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}
