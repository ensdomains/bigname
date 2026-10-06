//! Gate 1 diagnostic: compare the factored batch qualifier with the original two-read path.
//! The caller owns a disposable fixture transaction; this function only reads it.
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

pub async fn verify(conn: &mut PgConnection, resources: &[Uuid]) -> Result<Value> {
    let batch = super::super::basenames_context::qualified_pointers(conn, resources).await;
    let mut old = std::collections::BTreeMap::new();
    let original:Result<()>=async {
        for resource in resources {
            let Some(pointer)=super::super::topology::load_family_wildcard_source_on(conn,"base-mainnet",*resource).await? else {continue};
            // Exact original topology.rs boundary lookup, before the shared context factor.
            let Some(event)=sqlx::query("SELECT normalized_event_id,block_hash FROM bigname_phase.normalized_events WHERE event_identity=$1")
                .bind(pointer.boundary_position["event_identity"].as_str()).fetch_optional(&mut *conn).await? else {continue};
            let event_id:i64=event.try_get("normalized_event_id")?;
            let block_hash:String=event.try_get("block_hash")?;
            old.insert(*resource,(pointer,event_id,block_hash));
        }
        Ok(())
    }.await;
    match (original, batch) {
        (Err(old), Err(new)) => Ok(
            json!({"both_reject":true,"original_error":old.to_string(),"batch_error":new.to_string()}),
        ),
        (Ok(()), Ok(batch)) => {
            ensure!(
                old.len() == batch.len(),
                "pointer qualifier cardinality differs"
            );
            for (resource, (pointer, event_id, block_hash)) in &old {
                let candidate = batch
                    .get(resource)
                    .ok_or_else(|| anyhow::anyhow!("missing batch resource {resource}"))?;
                ensure!(
                    candidate.pointer == *pointer
                        && candidate.event_id == *event_id
                        && candidate.block_hash == *block_hash,
                    "pointer qualifier differs for {resource}"
                );
            }
            Ok(
                json!({"both_reject":false,"qualified":old.len(),"resources":old.keys().collect::<Vec<_>>()}),
            )
        }
        (old, batch) => anyhow::bail!(
            "pointer qualifier error parity differs: old_error={:?},batch_error={:?}",
            old.err(),
            batch.err()
        ),
    }
}
