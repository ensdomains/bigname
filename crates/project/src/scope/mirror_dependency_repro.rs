//! Separate read dependencies from affected publication keys; retain the replay control.
use super::*;

async fn trace(
    changed_ancestor: bool,
    evidence_only: bool,
    late_ancestor: bool,
) -> Result<Vec<Scope>> {
    let database = TestDatabase::create(TestDatabaseConfig::new("mirror_dependency_repro")).await?;
    let mut tx = database.pool().begin().await?;
    sqlx::raw_sql(include_str!("mirror_fixture.sql"))
        .execute(&mut *tx)
        .await?;
    sqlx::raw_sql(
        "TRUNCATE normalized_events, name_surfaces, project_changed_events,
             project_scope_names, project_scope_resources;
         INSERT INTO name_surfaces VALUES
             ('left.eth','ens','left','bench',10,'block','canonical',ARRAY['left','eth']),
             ('right.eth','ens','right','bench',10,'block','canonical',ARRAY['right','eth']),
             ('eth','ens','eth','bench',10,'block','canonical',ARRAY['eth']);
         INSERT INTO normalized_events VALUES
             (1,'left-pointer','bench','ens',md5('left')::uuid,'left.eth','ResolverChanged','ens_v2_registry_l1',1,1,10,'block',0,0,'canonical','activated','{\"resolver\":\"0xmirror\"}','{}','{}'),
             (2,'right-pointer','bench','ens',md5('right')::uuid,'right.eth','ResolverChanged','ens_v2_registry_l1',1,1,10,'block',0,0,'canonical','activated','{\"resolver\":\"0xmirror\"}','{}','{}'),
             (3,'ancestor-pointer','bench','ens',md5('ancestor')::uuid,'eth','ResolverChanged','ens_v1_registry_l1',1,1,10,'block',0,0,'canonical','activated','{\"node\":\"eth\",\"resolver\":\"0xshared\"}','{}','{}');
         ANALYZE normalized_events; ANALYZE name_surfaces; ANALYZE project_changed_events",
    ).execute(&mut *tx).await?;
    if changed_ancestor {
        sqlx::query("INSERT INTO project_changed_events SELECT * FROM normalized_events WHERE normalized_event_id=3")
            .execute(&mut *tx).await?;
    } else {
        sqlx::query("INSERT INTO project_scope_resources VALUES(md5('left')::uuid)")
            .execute(&mut *tx)
            .await?;
    }
    let mut strategy = super::super::stage(&mut tx, "bench", 10).await?;
    super::super::use_evidence_inputs(&mut strategy, evidence_only);
    let mut result = vec![named_scope(&mut tx).await?];
    for pass in 0..4 {
        if late_ancestor && pass == 1 {
            sqlx::query("INSERT INTO project_scope_names VALUES('eth') ON CONFLICT DO NOTHING")
                .execute(&mut *tx)
                .await?;
        }
        super::super::include(&mut tx, "bench", 10, &mut strategy).await?;
        let next = named_scope(&mut tx).await?;
        eprintln!(
            "mirror dependency changed_ancestor={changed_ancestor} pass={pass} names={:?} resources={:?}",
            next.0, next.1
        );
        let done = result.last() == Some(&next);
        result.push(next);
        if done && !(late_ancestor && pass == 0) {
            break;
        }
    }
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(result)
}

async fn named_scope(tx: &mut Transaction<'_, Postgres>) -> Result<Scope> {
    let names = scope(tx).await?.0;
    let resources = sqlx::query_scalar("SELECT CASE resource_id WHEN md5('left')::uuid THEN 'left' WHEN md5('right')::uuid THEN 'right' ELSE 'ancestor' END FROM project_scope_resources ORDER BY 1")
        .fetch_all(&mut **tx).await?;
    Ok((names, resources))
}

#[tokio::test]
async fn characterize_unchanged_ancestor_becoming_reverse_invalidation_seed() -> Result<()> {
    let steps = trace(false, false, false).await?;
    assert!(steps[0].0.is_empty());
    assert_eq!(steps[0].1.len(), 1);
    assert!(steps[1].0.contains(&"eth".into()));
    assert!(!steps[1].1.contains(&"right".into()));
    assert!(steps[2].1.contains(&"right".into()));
    assert_eq!(steps.last().unwrap().1.len(), 3);
    Ok(())
}

#[tokio::test]
async fn changed_common_ancestor_requires_both_subscribers() -> Result<()> {
    let steps = trace(true, true, false).await?;
    assert!(steps[0].0.is_empty() && steps[0].1.is_empty());
    assert!(steps[1].1.contains(&"left".into()));
    assert!(steps[1].1.contains(&"right".into()));
    Ok(())
}

#[tokio::test]
async fn proposed_unchanged_common_ancestor_must_not_invalidate_sibling() -> Result<()> {
    let steps = trace(false, true, false).await?;
    assert!(
        !steps.last().unwrap().1.contains(&"right".into()),
        "an unchanged ancestor read by left.eth became a reverse invalidation seed for right.eth"
    );
    Ok(())
}

#[tokio::test]
async fn evidence_ancestor_later_changed_by_another_operator_still_invalidates_sibling()
-> Result<()> {
    let steps = trace(false, true, true).await?;
    assert_eq!(steps[1].1, vec!["left"]);
    assert!(!steps[1].0.contains(&"eth".into()));
    assert!(steps.last().unwrap().1.contains(&"left".into()));
    assert!(steps.last().unwrap().1.contains(&"right".into()));
    Ok(())
}
