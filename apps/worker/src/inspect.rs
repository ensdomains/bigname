use anyhow::Result;
use bigname_storage::{
    CanonicalityInspection, CanonicalityInspectionStatus, CanonicalityState, DatabaseConfig,
    RawFactAuditCounts,
};
use clap::{Args, Subcommand};
use serde_json::{Value, json};

#[derive(Args, Debug)]
pub(crate) struct InspectArgs {
    #[command(subcommand)]
    pub(crate) command: InspectCommand,
}

#[derive(Subcommand, Debug)]
pub(crate) enum InspectCommand {
    #[command(about = "Inspect canonicality and block-scoped audit counts for one block hash")]
    Canonicality(InspectCanonicalityArgs),
}

#[derive(Args, Debug)]
pub(crate) struct InspectCanonicalityArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) chain_id: String,
    #[arg(long)]
    pub(crate) block_hash: String,
}

pub(crate) async fn inspect_command(args: InspectArgs) -> Result<()> {
    match args.command {
        InspectCommand::Canonicality(args) => inspect_canonicality(args).await,
    }
}

async fn inspect_canonicality(args: InspectCanonicalityArgs) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let inspection =
        bigname_storage::inspect_block_canonicality(&pool, &args.chain_id, &args.block_hash)
            .await?;

    println!("{}", render_canonicality_inspection(&inspection));
    Ok(())
}

fn render_canonicality_inspection(inspection: &CanonicalityInspection) -> Value {
    json!({
        "chain_id": inspection.chain_id.as_str(),
        "block_hash": inspection.block_hash.as_str(),
        "status": canonicality_inspection_status_label(inspection.status),
        "lineage_canonicality": inspection.lineage_state.map(canonicality_state_label),
        "parent_hash": inspection.parent_hash.as_deref(),
        "block_number": inspection.block_number,
        "raw_fact_counts": render_raw_fact_counts(&inspection.raw_fact_counts),
        "normalized_event_count": inspection.normalized_event_count,
        "states": {
            "observed": inspection.status == CanonicalityInspectionStatus::Observed,
            "canonical": inspection.status == CanonicalityInspectionStatus::Canonical,
            "safe": inspection.status == CanonicalityInspectionStatus::Safe,
            "finalized": inspection.status == CanonicalityInspectionStatus::Finalized,
            "missing": inspection.status == CanonicalityInspectionStatus::Missing,
            "orphaned": inspection.status == CanonicalityInspectionStatus::Orphaned,
        }
    })
}

fn render_raw_fact_counts(counts: &RawFactAuditCounts) -> Value {
    json!({
        "raw_blocks": counts.raw_block_count,
        "raw_code_hashes": counts.raw_code_hash_count,
        "raw_transactions": counts.raw_transaction_count,
        "raw_receipts": counts.raw_receipt_count,
        "raw_logs": counts.raw_log_count,
        "raw_call_snapshots": counts.raw_call_snapshot_count,
        "total": counts.total(),
    })
}

const fn canonicality_inspection_status_label(
    status: CanonicalityInspectionStatus,
) -> &'static str {
    match status {
        CanonicalityInspectionStatus::Missing => "missing",
        CanonicalityInspectionStatus::Observed => "observed",
        CanonicalityInspectionStatus::Canonical => "canonical",
        CanonicalityInspectionStatus::Safe => "safe",
        CanonicalityInspectionStatus::Finalized => "finalized",
        CanonicalityInspectionStatus::Orphaned => "orphaned",
    }
}

const fn canonicality_state_label(state: CanonicalityState) -> &'static str {
    match state {
        CanonicalityState::Observed => "observed",
        CanonicalityState::Canonical => "canonical",
        CanonicalityState::Safe => "safe",
        CanonicalityState::Finalized => "finalized",
        CanonicalityState::Orphaned => "orphaned",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_canonicality_inspection_json() {
        let rendered = render_canonicality_inspection(&CanonicalityInspection {
            chain_id: "eth-mainnet".to_owned(),
            block_hash: "0xabc".to_owned(),
            status: CanonicalityInspectionStatus::Safe,
            lineage_state: Some(CanonicalityState::Safe),
            parent_hash: Some("0xparent".to_owned()),
            block_number: Some(123),
            raw_fact_counts: RawFactAuditCounts {
                raw_block_count: 1,
                raw_code_hash_count: 2,
                raw_transaction_count: 3,
                raw_receipt_count: 4,
                raw_log_count: 5,
                raw_call_snapshot_count: 6,
            },
            normalized_event_count: 7,
        });

        assert_eq!(rendered["chain_id"], "eth-mainnet");
        assert_eq!(rendered["block_hash"], "0xabc");
        assert_eq!(rendered["status"], "safe");
        assert_eq!(rendered["lineage_canonicality"], "safe");
        assert_eq!(rendered["parent_hash"], "0xparent");
        assert_eq!(rendered["block_number"], 123);
        assert_eq!(rendered["raw_fact_counts"]["raw_blocks"], 1);
        assert_eq!(rendered["raw_fact_counts"]["raw_code_hashes"], 2);
        assert_eq!(rendered["raw_fact_counts"]["raw_transactions"], 3);
        assert_eq!(rendered["raw_fact_counts"]["raw_receipts"], 4);
        assert_eq!(rendered["raw_fact_counts"]["raw_logs"], 5);
        assert_eq!(rendered["raw_fact_counts"]["raw_call_snapshots"], 6);
        assert_eq!(rendered["raw_fact_counts"]["total"], 21);
        assert_eq!(rendered["normalized_event_count"], 7);
        assert_eq!(rendered["states"]["safe"], true);
        assert_eq!(rendered["states"]["canonical"], false);
    }

    #[test]
    fn renders_missing_lineage_as_nulls() {
        let rendered = render_canonicality_inspection(&CanonicalityInspection {
            chain_id: "eth-mainnet".to_owned(),
            block_hash: "0xmissing".to_owned(),
            status: CanonicalityInspectionStatus::Missing,
            lineage_state: None,
            parent_hash: None,
            block_number: None,
            raw_fact_counts: RawFactAuditCounts::default(),
            normalized_event_count: 0,
        });

        assert_eq!(rendered["status"], "missing");
        assert!(rendered["lineage_canonicality"].is_null());
        assert!(rendered["parent_hash"].is_null());
        assert!(rendered["block_number"].is_null());
        assert_eq!(rendered["raw_fact_counts"]["total"], 0);
        assert_eq!(rendered["states"]["missing"], true);
        assert_eq!(rendered["states"]["orphaned"], false);
    }
}
