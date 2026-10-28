//! Which inventories a resolver's classification change puts in the lookup work
//! (`project:families.lookup.inventory_work`).
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use uuid::Uuid;

use super::create;
use crate::families::guard_tests::{CHAIN, database, hash};

const RESOLVER: &str = "0x00000000000000000000000000000000000000c7";

fn image(kind: &str, status: &str, reason: Option<&str>, manifest: Option<i64>) -> Value {
    json!({"classification": {"kind": kind}, "support_status": status,
           "unsupported_reason": reason, "manifest_id": manifest})
}

// An inventory stores the reason of an unsupported resolver (storage
// `families/records/serving.rs`, `family_pointer_eligibility`) and reads its classification
// object before support (`families/records/mirror.rs`, `classify`). It reads `manifest_id`
// only while the resolver is supported: the guarded partition (`families/records/rows.rs`) and
// the declaration namespace. So every change but one recomposes the inventories that depend on
// the resolver: a `manifest_id` change between two unsupported rows recomposes none.
#[tokio::test]
async fn a_classification_change_recomposes_the_inventories_that_read_its_fields() -> Result<()> {
    let unsupported = image("a", "unsupported", Some("resolver_not_declared"), Some(1));
    let supported = image("a", "supported", None, Some(1));
    let cases: [(&str, Option<Value>, Option<Value>, bool); 12] = [
        (
            "unsupported re-filed with a new reason, manifest and classification",
            Some(image(
                "b",
                "unsupported",
                Some("resolver_manifest_not_active"),
                None,
            )),
            Some(unsupported.clone()),
            true,
        ),
        (
            "unsupported, reason only",
            Some(image(
                "a",
                "unsupported",
                Some("resolver_implementation_unknown"),
                Some(1),
            )),
            Some(unsupported.clone()),
            true,
        ),
        // `classify` and `is_mirror_pointer` read `role` and `source_family` before support.
        (
            "unsupported, classification only",
            Some(image(
                "b",
                "unsupported",
                Some("resolver_not_declared"),
                Some(1),
            )),
            Some(unsupported.clone()),
            true,
        ),
        (
            "unsupported, manifest only",
            Some(image(
                "a",
                "unsupported",
                Some("resolver_not_declared"),
                Some(2),
            )),
            Some(unsupported.clone()),
            false,
        ),
        (
            "unsupported row inserted",
            None,
            Some(unsupported.clone()),
            true,
        ),
        (
            "unsupported row deleted",
            Some(unsupported.clone()),
            None,
            true,
        ),
        (
            "supported row inserted",
            None,
            Some(supported.clone()),
            true,
        ),
        ("supported row deleted", Some(supported.clone()), None, true),
        (
            "support withdrawn",
            Some(supported.clone()),
            Some(unsupported.clone()),
            true,
        ),
        (
            "supported, manifest re-stamped",
            Some(image("a", "supported", None, Some(2))),
            Some(supported.clone()),
            true,
        ),
        (
            "supported, classification changed",
            Some(image("b", "supported", None, Some(1))),
            Some(supported.clone()),
            true,
        ),
        (
            "supported, nothing changed, no manifest",
            Some(image("a", "supported", None, None)),
            Some(image("a", "supported", None, None)),
            false,
        ),
    ];
    let (database, pool) = database().await?;
    let resource = Uuid::from_u128(0x41);
    let mut wrong = Vec::new();
    for (case, before, current, recomposes) in cases {
        let mut transaction = pool.begin().await?;
        create(&mut transaction).await?;
        sqlx::query(
            "INSERT INTO project_lookup_dependency (chain_id, resource_id, kind, key1, key2, key3)
             VALUES ($1, $2, 'classification', $3, '', '')",
        )
        .bind(CHAIN)
        .bind(resource)
        .bind(RESOLVER)
        .execute(&mut *transaction)
        .await?;
        if let Some(row) = current {
            sqlx::query(
                "INSERT INTO project_resolver_classification (chain_id, resolver_address,
                     block_number, event_identity, classification, support_status,
                     unsupported_reason, manifest_id)
                 VALUES ($1, $2, 5, 'classified', $3 -> 'classification',
                         $3 ->> 'support_status', $3 ->> 'unsupported_reason',
                         ($3 ->> 'manifest_id')::bigint)",
            )
            .bind(CHAIN)
            .bind(RESOLVER)
            .bind(row)
            .execute(&mut *transaction)
            .await?;
        }
        sqlx::query(
            "INSERT INTO project_family_undo (chain_id, block_number, block_hash, family, key,
                 before_image)
             VALUES ($1, 5, $2, 'project_resolver_classification', $3, $4)",
        )
        .bind(CHAIN)
        .bind(hash(5))
        .bind(json!([CHAIN, RESOLVER]).to_string())
        .bind(before)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(include_str!("inventory_work.sql"))
            .bind(CHAIN)
            .bind(5_i64)
            .bind(4_i64)
            .execute(&mut *transaction)
            .await
            .with_context(|| format!("inventory work, case {case}"))?;
        let work: Vec<Uuid> =
            sqlx::query_scalar("SELECT resource_id FROM pg_temp.bigname_lookup_inventory_work")
                .fetch_all(&mut *transaction)
                .await?;
        let expected = if recomposes { vec![resource] } else { vec![] };
        if work != expected {
            wrong.push(format!("{case}: {work:?}"));
        }
        transaction.rollback().await?;
    }
    database.cleanup().await?;
    ensure!(wrong.is_empty(), "wrong inventory work: {wrong:#?}");
    Ok(())
}
