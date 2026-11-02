//! An ENSv1 name whose selected resource carries no pointer of its own serves the resolver its
//! ENSv1 registry record holds, and that resolver's records. ENSv1 clients read the resolver from
//! the registry at read time
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L137-L141 @ ens_v1@91c966f).
//!
//! The cases run raw logs through Engine on the checked-in Sepolia profile, then reshape the
//! stored events before Project runs, as Interpret wrote them for legacy `.eth` names:
//!
//! - the lease selected, and the registry's only `NewResolver` on the registry-only resource
//!   (`misattribute`), or the reverse, the registry-only resource selected after a BaseRegistrar
//!   transfer without `reclaim` and the only `NewResolver` on the lease.
//! - the registry-read copy of a `NewResolver` on the registry-only resource dropped
//!   (`uncopied`), and the resolver Interpret carries onto the newly active resource at such a
//!   transfer dropped (`uncarried`), as no copy was written for those names.
//! - the `NewResolver` left without a name (`unnamed`), as Interpret writes it before it knows
//!   the name.
use super::super::v2_continuing_lease::SECOND_OWNER;
use super::*;

/// Premigration's `register` with no resolver: the ENSv1 lease and the ownerless reservation with
/// the ENSv1 mirror as its ENSv2 resolver. The lease has no pointer of its own.
fn bare() -> Result<Logs> {
    let token = U256::from_be_bytes(keccak256(label().as_bytes()).0) >> 32 << 32;
    let mut out = registered_with(None, OWNER, LEASE, Vec::new())?;
    out.push((
        NEW_REGISTRY,
        ResolverUpdated {
            tokenId: token,
            resolver: MIRROR.parse()?,
            sender: OWNER.parse()?,
        }
        .encode_log_data(),
    ));
    Ok(out)
}

/// A BaseRegistrar token transfer with no `reclaim`, which leaves the registry owner as it was.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L175 @ ens_v1@91c966f)
fn transferred(from: &str, to: &str) -> Result<Logs> {
    Ok(vec![(
        BASE_REGISTRAR,
        erc721::Transfer {
            from: from.parse()?,
            to: to.parse()?,
            tokenId: U256::from_be_bytes(keccak256(label().as_bytes()).0),
        }
        .encode_log_data(),
    )])
}

/// Drops every `ResolverChanged` Interpret wrote at `block`, the resolver it carries onto the
/// resource a transfer makes active.
async fn uncarried(pool: &PgPool, block: i64) -> Result<()> {
    sqlx::query(
        "DELETE FROM bigname_phase.normalized_events
         WHERE chain_id = $1 AND block_number = $2 AND event_kind = 'ResolverChanged'",
    )
    .bind(CHAIN)
    .bind(block)
    .execute(pool)
    .await?;
    Ok(())
}

/// Drops the registry-read copy Interpret writes on the registry-only resource for a registry
/// `NewResolver` at `block`, as it writes none while it does not know that resource's name.
async fn uncopied(pool: &PgPool, block: i64) -> Result<()> {
    sqlx::query(
        "DELETE FROM bigname_phase.normalized_events
         WHERE chain_id = $1 AND block_number = $2 AND event_kind = 'ResolverChanged'
           AND event_identity LIKE '%:ResolverChanged:registry-read:%'",
    )
    .bind(CHAIN)
    .bind(block)
    .execute(pool)
    .await?;
    Ok(())
}

/// Clears the name of every registry `ResolverChanged` of `block`.
async fn unnamed(pool: &PgPool, block: i64) -> Result<()> {
    let cleared: Vec<String> = sqlx::query_scalar(
        "UPDATE bigname_phase.normalized_events SET logical_name_id = NULL
         WHERE chain_id = $1 AND block_number = $2 AND event_kind = 'ResolverChanged'
           AND source_family = 'ens_v1_registry_l1'
         RETURNING event_identity",
    )
    .bind(CHAIN)
    .bind(block)
    .fetch_all(pool)
    .await?;
    anyhow::ensure!(!cleared.is_empty(), "block {block} has no registry pointer");
    Ok(())
}

/// What a reshaping applies to one block, by its offset after the base.
#[derive(Clone, Copy)]
enum Reshape {
    Misattribute(i64),
    Uncopied(i64),
    Uncarried(i64),
    Unnamed(i64),
}

async fn reshape(pool: &PgPool, base: i64, steps: &[Reshape]) -> Result<()> {
    for step in steps {
        match *step {
            Reshape::Misattribute(offset) => misattribute(pool, base + offset).await?,
            Reshape::Uncopied(offset) => uncopied(pool, base + offset).await?,
            Reshape::Uncarried(offset) => uncarried(pool, base + offset).await?,
            Reshape::Unnamed(offset) => unnamed(pool, base + offset).await?,
        }
    }
    Ok(())
}

/// `base` and `after` interpreted, reshaped, and the families rebuilt at `HEAD`.
async fn published_from(base: Logs, after: Vec<Logs>, steps: &[Reshape]) -> Result<TestDatabase> {
    let database = TestDatabase::new_migrated().await?;
    let first = interpret_from(&database, base, after).await?;
    reshape(&database.pool, first, steps).await?;
    project(&database.pool, HEAD, FamilyMode::Rebuild).await?;
    Ok(database)
}

/// The reshaped fixture and the same logs as Interpret wrote them, both rebuilt.
async fn both_from(
    base: impl Fn() -> Result<Logs>,
    after: impl Fn() -> Result<Vec<Logs>>,
    steps: &[Reshape],
) -> Result<(Value, Value)> {
    let database = published_from(base()?, after()?, steps).await?;
    let wrong = served(&database).await?;
    database.cleanup().await?;
    let reference = published_from(base()?, after()?, &[]).await?;
    let right = served(&reference).await?;
    reference.cleanup().await?;
    Ok((wrong, right))
}

/// The reshaped fixture and the same logs as Interpret wrote them, each published one normal
/// Project run per block and read after the runs to each block of `reads` (0 is the base).
async fn stepwise_from(
    base: impl Fn() -> Result<Logs>,
    after: impl Fn() -> Result<Vec<Logs>>,
    steps: &[Reshape],
    reads: &[i64],
) -> Result<Vec<(Value, Value)>> {
    let mut sides = Vec::new();
    for steps in [steps, &[]] {
        let database = TestDatabase::new_migrated().await?;
        let first = interpret_from(&database, base()?, after()?).await?;
        reshape(&database.pool, first, steps).await?;
        let mut views = Vec::new();
        let mut next = first;
        for read in reads {
            while next <= first + read {
                project(&database.pool, next, FamilyMode::Normal).await?;
                next += 1;
            }
            views.push(served(&database).await?);
        }
        database.cleanup().await?;
        sides.push(views);
    }
    let right = sides.pop().expect("reference");
    Ok(sides
        .pop()
        .expect("reshaped")
        .into_iter()
        .zip(right)
        .collect())
}

fn logical_name() -> String {
    format!("ens:{:#x}", lease_node())
}

async fn composed(database: &TestDatabase) -> Result<bigname_storage::NameCurrentRow> {
    bigname_storage::families::name::load_family_name(&database.pool, &logical_name())
        .await?
        .context("the name has a composed row")
}

/// Which resource the name selects, its binding candidates in order, whether the selected
/// resource has a pointer event of the name, and the resource the node's registry pointer
/// carries.
async fn shape(database: &TestDatabase) -> Result<Value> {
    let pool = &database.pool;
    let row = composed(database).await?;
    let selected = row.provenance["authority_selection"]["resource_id"].clone();
    let candidates: Vec<(String, bool)> = sqlx::query_as(
        "SELECT resource_id::text, registry_only
         FROM bigname_phase.project_binding_candidate
         WHERE chain_id = $1 AND logical_name_id = $2
         ORDER BY block_number, transaction_index NULLS FIRST, log_index NULLS FIRST,
                  surface_binding_id",
    )
    .bind(CHAIN)
    .bind(logical_name())
    .fetch_all(pool)
    .await?;
    let own: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM bigname_phase.normalized_events
         WHERE chain_id = $1 AND event_kind = 'ResolverChanged' AND logical_name_id = $2
           AND resource_id::text = $3",
    )
    .bind(CHAIN)
    .bind(logical_name())
    .bind(selected.as_str())
    .fetch_one(pool)
    .await?;
    let registry: Option<Option<String>> = sqlx::query_scalar(
        "SELECT resource_id::text FROM bigname_phase.project_registry_pointer
         WHERE chain_id = $1 AND namespace = 'ens' AND node = $2",
    )
    .bind(CHAIN)
    .bind(format!("{:#x}", lease_node()))
    .fetch_optional(pool)
    .await?;
    let candidates: Vec<Value> = candidates
        .iter()
        .map(|(resource, registry_only)| {
            json!({"resource": resource, "registry_only": registry_only})
        })
        .collect();
    Ok(json!({
        "selected": selected,
        "candidates": candidates,
        "own_pointers_on_selected": own,
        "registry_pointer_resource": registry.flatten(),
    }))
}

/// The fixture has two or more candidates, the selected one is (or is not) the registry-only
/// candidate, it has no pointer event of the name, and the node's registry pointer sits on
/// another candidate's resource.
fn assert_orientation(shape: &Value, selects_registry_only: bool) {
    let candidates = shape["candidates"].as_array().expect("candidates");
    assert!(candidates.len() >= 2, "{shape:#}");
    let selected = candidates
        .iter()
        .find(|candidate| candidate["resource"] == shape["selected"])
        .unwrap_or_else(|| panic!("the selected resource is a candidate: {shape:#}"));
    assert_eq!(
        selected["registry_only"],
        json!(selects_registry_only),
        "{shape:#}"
    );
    assert_eq!(shape["own_pointers_on_selected"], json!(0), "{shape:#}");
    let pointer = candidates
        .iter()
        .find(|candidate| candidate["resource"] == shape["registry_pointer_resource"])
        .unwrap_or_else(|| panic!("the registry pointer is on a candidate: {shape:#}"));
    assert_eq!(
        pointer["registry_only"],
        json!(!selects_registry_only),
        "{shape:#}"
    );
}

/// The summary row the lists and the bound-name listing read.
async fn summary_resolver(database: &TestDatabase) -> Result<Option<String>> {
    Ok(sqlx::query_scalar(
        "SELECT resolver_address FROM bigname_phase.project_name_summary
         WHERE chain_id = $1 AND logical_name_id = $2",
    )
    .bind(CHAIN)
    .bind(logical_name())
    .fetch_one(&database.pool)
    .await?)
}

/// The resolver fields of every mode name `address`.
fn assert_resolver_everywhere(view: &Value, address: &str) {
    for field in [
        "resolver",
        "ens_v1_resolver",
        "lookup_resolver",
        "keyed_resolver",
    ] {
        assert_eq!(view[field], resolver(address), "{field}: {view:#}");
    }
}

/// The lease written by a direct registry `setResolver` while it is the active resource, then the
/// token moved without `reclaim`, so the name selects its registry-only resource.
fn written_then_transferred() -> Result<Vec<Logs>> {
    Ok(vec![
        pointer(lease_node(), NEXT)?,
        next_records(lease_node())?,
        transferred(OWNER, SECOND_OWNER)?,
    ])
}

/// R1: the name selects its registry-only resource (the token moved without `reclaim`), and the
/// registry's only `NewResolver` is a named write on the lease. Every mode serves `NEXT` and its
/// records, and the stored summary row names it.
#[tokio::test]
async fn r1_a_pointer_on_the_lease_serves_a_selected_registry_only_resource() -> Result<()> {
    let database = published_from(
        bare()?,
        written_then_transferred()?,
        &[Reshape::Uncopied(1), Reshape::Uncarried(3)],
    )
    .await?;
    let shape = shape(&database).await?;
    let view = served(&database).await?;
    let summary = summary_resolver(&database).await?;
    database.cleanup().await?;
    eprintln!("r1 shape: {shape:#}");
    assert_orientation(&shape, true);
    assert_serves_next(&view);
    assert_eq!(summary.as_deref(), Some(NEXT));
    Ok(())
}

/// R2: R1 with the lease's `NewResolver` left without a name.
#[tokio::test]
async fn r2_an_unnamed_pointer_on_the_lease_serves_the_registry_resolver() -> Result<()> {
    let database = published_from(
        bare()?,
        written_then_transferred()?,
        &[Reshape::Uncopied(1), Reshape::Unnamed(1), Reshape::Uncarried(3)],
    )
    .await?;
    let shape = shape(&database).await?;
    let view = served(&database).await?;
    database.cleanup().await?;
    assert_orientation(&shape, true);
    assert_serves_next(&view);
    Ok(())
}

/// O3: the third class. The lease selected, and its own `NewResolver` left without a name, as
/// Interpret writes it before it knows the name. The pointer is not admitted on the name's own
/// resource, so the name reads the registry pointer.
#[tokio::test]
async fn o3_an_unnamed_pointer_on_the_selected_lease_serves_the_registry_resolver() -> Result<()> {
    let database = published_from(
        bare()?,
        vec![pointer(lease_node(), NEXT)?, next_records(lease_node())?],
        &[Reshape::Unnamed(1)],
    )
    .await?;
    let view = served(&database).await?;
    database.cleanup().await?;
    assert_serves_next(&view);
    Ok(())
}

fn next_after() -> Result<Vec<Logs>> {
    Ok(vec![
        pointer(lease_node(), NEXT)?,
        next_records(lease_node())?,
    ])
}

/// R3b: the lease selected with no pointer of its own, and the registry's only `NewResolver` on
/// the registry-only resource. Every mode serves `NEXT` and its records, as the same logs on the
/// lease do.
#[tokio::test]
async fn r3b_a_lease_without_a_pointer_serves_the_registry_only_pointer() -> Result<()> {
    let (wrong, right) = both_from(bare, next_after, &[Reshape::Misattribute(1)]).await?;
    assert_serves_next(&wrong);
    assert_eq!(wrong, right);
    Ok(())
}

/// R4: the lease's own latest pointer is a clear, and the registry then points the node at `NEXT`
/// through the registry-only resource. Every mode and the stored summary row serve `NEXT`.
#[tokio::test]
async fn r4_a_cleared_own_pointer_reads_the_later_registry_pointer() -> Result<()> {
    let after = || -> Result<Vec<Logs>> {
        Ok(vec![
            pointer(lease_node(), ZERO)?,
            pointer(lease_node(), NEXT)?,
            next_records(lease_node())?,
        ])
    };
    let database = published_from(premigrated()?, after()?, &[Reshape::Misattribute(2)]).await?;
    let wrong = served(&database).await?;
    let summary = summary_resolver(&database).await?;
    database.cleanup().await?;
    assert_serves_next(&wrong);
    assert_eq!(summary.as_deref(), Some(NEXT));
    let reference = published_from(premigrated()?, after()?, &[]).await?;
    let right = served(&reference).await?;
    reference.cleanup().await?;
    assert_eq!(wrong, right);
    Ok(())
}

/// R5: no pointer of its own, and the registry's pointer set and then cleared through the
/// registry-only resource. The name composes the clear: no resolver, the clear as its latest
/// event, the registry family, and a null `ens_v1.resolver`.
#[tokio::test]
async fn r5_a_cleared_registry_pointer_composes_the_clear() -> Result<()> {
    let database = published_from(
        bare()?,
        vec![pointer(lease_node(), NEXT)?, pointer(lease_node(), ZERO)?],
        &[Reshape::Misattribute(1), Reshape::Misattribute(2)],
    )
    .await?;
    let row = composed(&database).await?;
    let view = served(&database).await?;
    database.cleanup().await?;
    let summary = &row.declared_summary;
    assert!(summary["resolver"]["address"].is_null(), "{summary:#}");
    assert_eq!(
        summary["resolver"]["latest_event_kind"], "ResolverChanged",
        "{summary:#}"
    );
    assert!(summary["ens_v1_resolver"].is_null(), "{summary:#}");
    assert_eq!(
        row.provenance["resolver_pointer_source_family"], "ens_v1_registry_l1",
        "{:#}",
        row.provenance
    );
    for field in [
        "resolver",
        "ens_v1_resolver",
        "lookup_resolver",
        "keyed_resolver",
    ] {
        assert!(view[field].is_null(), "{field}: {view:#}");
    }
    Ok(())
}

/// R6: a node with no registry pointer composes no resolver and no latest event.
#[tokio::test]
async fn r6_a_node_without_a_registry_pointer_composes_nothing() -> Result<()> {
    let database = published_from(bare()?, Vec::new(), &[]).await?;
    let row = composed(&database).await?;
    database.cleanup().await?;
    let summary = &row.declared_summary;
    assert!(summary["resolver"]["address"].is_null(), "{summary:#}");
    assert!(
        summary["resolver"]["latest_event_kind"].is_null(),
        "{summary:#}"
    );
    assert!(summary["ens_v1_resolver"].is_null(), "{summary:#}");
    assert!(
        row.provenance.get("resolver_pointer_source_family").is_none(),
        "{:#}",
        row.provenance
    );
    Ok(())
}

/// R11: `NEXT`'s records written before and after the registry-only pointer in its block are
/// served through the registry pointer, as the same logs on the lease serve them.
#[tokio::test]
async fn r11_records_around_the_pointer_are_served_through_the_registry_pointer() -> Result<()> {
    for before in [true, false] {
        let after = move || -> Result<Vec<Logs>> {
            let parts = if before {
                vec![next_records(lease_node()), pointer(lease_node(), NEXT)]
            } else {
                vec![pointer(lease_node(), NEXT), next_records(lease_node())]
            };
            Ok(vec![logs(parts)?])
        };
        let (wrong, right) = both_from(bare, after, &[Reshape::Misattribute(1)]).await?;
        assert_serves_next(&wrong);
        assert_eq!(wrong, right, "records before the pointer: {before}");
    }
    Ok(())
}

/// R12: `relation=resolves_to` lists the name under the address `NEXT` holds for the node, with
/// the pointer on either candidate's resource, on the address-names route in `ens` and in every
/// namespace and on lookup, each read in full and walked as an address with more candidate names
/// than the exact cap.
#[tokio::test]
async fn r12_resolves_to_lists_the_name_through_the_registry_pointer() -> Result<()> {
    for (case, after, steps) in [
        (
            "lease selected",
            next_after()?,
            vec![Reshape::Misattribute(1)],
        ),
        (
            "registry-only selected",
            written_then_transferred()?,
            vec![Reshape::Uncopied(1), Reshape::Uncarried(3)],
        ),
    ] {
        let database = published_from(bare()?, after, &steps).await?;
        if case == "registry-only selected" {
            assert_orientation(&shape(&database).await?, true);
        }
        let mut reads = Vec::new();
        for bounded in [false, true] {
            reads.push((
                bounded,
                resolving_everywhere(&database, NEXT_HOLDER, bounded).await?,
            ));
        }
        database.cleanup().await?;
        for (bounded, (found, paths)) in reads {
            assert_eq!(
                found,
                vec![vec![json!(NAME)]; 3],
                "{case}, bounded {bounded}"
            );
            assert_eq!(
                paths.contains(&"exact"),
                !bounded,
                "{case}, bounded {bounded}: {paths:?}"
            );
        }
    }
    Ok(())
}

/// N1: the widened candidate set reaches a name's other resources, but the name is listed only
/// through the resource it selects. `FIRST` holds the owner's address for the node, written while
/// the lease was active, and the lease keeps that pointer. The token then moves without
/// `reclaim`, and the registry owner points the node at `NEXT` while the registry-only resource is
/// active, so the selected resource admits a pointer of its own, `NEXT`. The name is listed under
/// `NEXT`'s holder and not under the owner, read in full and walked.
#[tokio::test]
async fn n1_resolves_to_skips_a_name_whose_selected_pointer_is_its_own() -> Result<()> {
    let database = published_from(
        premigrated()?,
        vec![
            transferred(OWNER, SECOND_OWNER)?,
            pointer(lease_node(), NEXT)?,
            next_records(lease_node())?,
        ],
        &[],
    )
    .await?;
    let shape = shape(&database).await?;
    let mut reads = Vec::new();
    for bounded in [false, true] {
        reads.push((
            bounded,
            resolving_everywhere(&database, NEXT_HOLDER, bounded).await?.0,
            resolving_everywhere(&database, OWNER, bounded).await?.0,
        ));
    }
    let view = served(&database).await?;
    database.cleanup().await?;
    eprintln!("n1 shape: {shape:#}");
    assert!(
        shape["own_pointers_on_selected"]
            .as_i64()
            .is_some_and(|own| own > 0),
        "{shape:#}"
    );
    assert_serves_next(&view);
    for (bounded, next_holder, owner) in reads {
        assert_eq!(
            next_holder,
            vec![vec![json!(NAME)]; 3],
            "bounded {bounded}"
        );
        assert_eq!(owner, vec![Vec::<Value>::new(); 3], "bounded {bounded}");
    }
    Ok(())
}

/// R13: the registry moves the node from `NEXT` to `FIRST` and then clears it, every write on the
/// registry-only resource, one normal Project run per block. The name follows each write, as the
/// same logs on the lease do.
#[tokio::test]
async fn r13_later_registry_writes_recompose_the_name() -> Result<()> {
    let after = || -> Result<Vec<Logs>> {
        Ok(vec![
            pointer(lease_node(), NEXT)?,
            next_records(lease_node())?,
            pointer(lease_node(), FIRST)?,
            pointer(lease_node(), ZERO)?,
        ])
    };
    let steps = stepwise_from(
        bare,
        after,
        &[
            Reshape::Misattribute(1),
            Reshape::Misattribute(3),
            Reshape::Misattribute(4),
        ],
        &[2, 3, 4],
    )
    .await?;
    assert_serves_next(&steps[0].0);
    assert_resolver_everywhere(&steps[1].0, FIRST);
    for field in [
        "resolver",
        "ens_v1_resolver",
        "lookup_resolver",
        "keyed_resolver",
    ] {
        assert!(steps[2].0[field].is_null(), "{field}: {:#}", steps[2].0);
    }
    for (wrong, right) in steps {
        assert_eq!(wrong, right);
    }
    Ok(())
}

/// R14: a full rebuild serves and stores what the incremental runs published, for R1's shape.
#[tokio::test]
async fn r14_a_rebuild_matches_the_incremental_runs() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let first = interpret_from(&database, bare()?, written_then_transferred()?).await?;
    reshape(
        &database.pool,
        first,
        &[Reshape::Uncopied(1), Reshape::Uncarried(3)],
    )
    .await?;
    for block in first..=HEAD {
        project(&database.pool, block, FamilyMode::Normal).await?;
    }
    assert_orientation(&shape(&database).await?, true);
    let incremental = served(&database).await?;
    let rows = stored(&database).await?;
    assert_serves_next(&incremental);
    project(&database.pool, HEAD, FamilyMode::Rebuild).await?;
    assert_eq!(served(&database).await?, incremental);
    assert_eq!(stored(&database).await?, rows);
    database.cleanup().await
}

/// R15: the stored rows carry the registry resolver, not a read-time fill: the summary row, and
/// the lookup inventory and records the name's record resource stores.
#[tokio::test]
async fn r15_the_stored_rows_serve_the_registry_resolver() -> Result<()> {
    let database = published_from(bare()?, next_after()?, &[Reshape::Misattribute(1)]).await?;
    let rows = stored(&database).await?;
    let summary = summary_resolver(&database).await?;
    database.cleanup().await?;
    assert_eq!(summary.as_deref(), Some(NEXT), "{rows:#}");
    assert!(rows["inventory"].is_array(), "{rows:#}");
    let records = rows["records"].as_array().cloned().unwrap_or_default();
    assert!(
        records
            .iter()
            .any(|record| record["record_key"] == json!("addr:61")),
        "{rows:#}"
    );
    Ok(())
}

/// S2: below a name served through its registry pointer, a registry child with no pointer of its
/// own serves no resolver and no `ens_v1.resolver`: ENSv1 has no wildcard, and the parent's
/// pointer is the parent node's.
#[tokio::test]
async fn s2_a_child_without_a_pointer_does_not_take_the_parents_registry_pointer() -> Result<()> {
    let database = published_from(
        bare()?,
        vec![pointer(lease_node(), NEXT)?, child()?],
        &[Reshape::Misattribute(1)],
    )
    .await?;
    let parent = served(&database).await?;
    let view = served_name(&database, &child_name()).await?;
    database.cleanup().await?;
    assert_resolver_everywhere(&parent, NEXT);
    for field in [
        "resolver",
        "ens_v1_resolver",
        "lookup_resolver",
        "keyed_resolver",
    ] {
        assert!(view[field].is_null(), "{field}: {view:#}");
    }
    Ok(())
}

/// O1: the orientation the stored tables show. The name has a registry-only candidate and a lease
/// candidate, the registry's pointer is on the registry-only candidate's resource and not on the
/// lease's, and the name selects the lease, the later candidate. The token moves away without
/// `reclaim`, the registry owner writes the pointer while the registry-only resource is active,
/// and the token comes back. Every mode and the stored summary row serve `NEXT`.
#[tokio::test]
async fn o1_a_selected_lease_serves_the_registry_only_candidates_pointer() -> Result<()> {
    let after = vec![
        transferred(OWNER, SECOND_OWNER)?,
        pointer(lease_node(), NEXT)?,
        next_records(lease_node())?,
        transferred(SECOND_OWNER, OWNER)?,
    ];
    let database = published_from(
        bare()?,
        after,
        &[Reshape::Uncarried(1), Reshape::Uncarried(4)],
    )
    .await?;
    let shape = shape(&database).await?;
    let view = served(&database).await?;
    let summary = summary_resolver(&database).await?;
    database.cleanup().await?;
    eprintln!("o1 shape: {shape:#}");
    assert_orientation(&shape, false);
    assert_serves_next(&view);
    assert_eq!(summary.as_deref(), Some(NEXT));
    Ok(())
}

/// O2: the orientation the scope describes. The registry's pointer is set in the registration
/// transaction, on the lease, and the token then moves without `reclaim`, so the name selects its
/// registry-only candidate. Every mode and the stored summary row serve `FIRST`, and
/// `relation=resolves_to` lists the name under the address `FIRST` holds.
#[tokio::test]
async fn o2_a_selected_registry_only_resource_serves_the_leases_pointer() -> Result<()> {
    let database = published_from(
        premigrated()?,
        vec![transferred(OWNER, SECOND_OWNER)?],
        &[Reshape::Uncopied(0), Reshape::Uncarried(1)],
    )
    .await?;
    let shape = shape(&database).await?;
    let view = served(&database).await?;
    let summary = summary_resolver(&database).await?;
    let found = resolving_names(&database, OWNER).await?;
    database.cleanup().await?;
    eprintln!("o2 shape: {shape:#}");
    assert_orientation(&shape, true);
    assert_serves_first(&view);
    assert_eq!(summary.as_deref(), Some(FIRST));
    assert_eq!(found, [json!(NAME)]);
    Ok(())
}
