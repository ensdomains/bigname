//! Private, bounded-memory keyed snapshots. No row contents reach logs.
use super::*;
use std::{
    fs,
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
};

pub(in crate::engine) struct Snapshot {
    directory: PathBuf,
    preserve: bool,
}
impl Drop for Snapshot {
    fn drop(&mut self) {
        if !self.preserve {
            let _ = fs::remove_dir_all(&self.directory);
        }
    }
}
impl Snapshot {
    pub(in crate::engine) async fn capture(
        tx: &mut Transaction<'_, Postgres>,
        root: &Path,
    ) -> Result<Self> {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let directory = root.join(format!("project-contract-{}-{nonce}", std::process::id()));
        let mut options = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            options.mode(0o700);
        }
        options.create(&directory)?;
        let result = Self {
            directory,
            preserve: false,
        };
        for (table, keys) in TABLES {
            let mut out = writer(&result.directory.join(table))?;
            sqlx::query(&format!("DECLARE contract_rows NO SCROLL CURSOR FOR SELECT jsonb_build_array(k,v)::text FROM (SELECT jsonb_build_array({keys})::text k,to_jsonb(row) v FROM {table} row) s ORDER BY k COLLATE \"C\""))
                .execute(&mut **tx).await?;
            loop {
                let rows: Vec<String> = sqlx::query_scalar("FETCH FORWARD 128 FROM contract_rows")
                    .fetch_all(&mut **tx)
                    .await?;
                if rows.is_empty() {
                    break;
                }
                for row in rows {
                    writeln!(out, "{row}")?;
                }
            }
            out.flush()?;
            sqlx::query("CLOSE contract_rows")
                .execute(&mut **tx)
                .await?;
        }
        Ok(result)
    }
}
fn writer(path: &Path) -> Result<BufWriter<fs::File>> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    Ok(BufWriter::new(options.open(path)?))
}
struct Rows {
    input: BufReader<fs::File>,
    next: Option<(String, Value)>,
    previous: Option<String>,
}
impl Rows {
    fn open(path: &Path) -> Result<Self> {
        let mut r = Self {
            input: BufReader::new(fs::File::open(path)?),
            next: None,
            previous: None,
        };
        r.advance()?;
        Ok(r)
    }
    fn advance(&mut self) -> Result<()> {
        let mut line = String::new();
        self.next = if self.input.read_line(&mut line)? == 0 {
            None
        } else {
            let (key, row): (String, Value) = serde_json::from_str(&line)?;
            ensure!(
                self.previous.as_ref().is_none_or(|p| p < &key),
                "duplicate or unsorted projection key"
            );
            self.previous = Some(key.clone());
            Some((key, row))
        };
        Ok(())
    }
    fn at(&self, key: &str) -> Option<&Value> {
        self.next.as_ref().filter(|(k, _)| k == key).map(|(_, v)| v)
    }
}

/// The three private snapshots one comparison reads: the old output, the candidate output and
/// the reference (old algorithm) output for the same target.
pub(in crate::engine) struct Snapshots<'a> {
    pub(in crate::engine) baseline: &'a mut Snapshot,
    pub(in crate::engine) candidate: &'a mut Snapshot,
    pub(in crate::engine) reference: &'a mut Snapshot,
}

/// What a candidate row must satisfy to differ from the baseline: the independently captured
/// mandatory scopes, the legacy publication scope, the target metadata and the previous marker
/// that retained rows are validated against.
pub(in crate::engine) struct Expectations<'a> {
    pub(in crate::engine) mandatory: &'a Scopes,
    pub(in crate::engine) old_scope: &'a Scopes,
    pub(in crate::engine) target: &'a Target,
    pub(in crate::engine) previous: i64,
}

pub(in crate::engine) async fn compare(
    tx: &mut Transaction<'_, Postgres>,
    snapshots: Snapshots<'_>,
    expectations: Expectations<'_>,
) -> Result<()> {
    let Snapshots {
        baseline,
        candidate,
        reference,
    } = snapshots;
    let Expectations {
        mandatory,
        old_scope,
        target,
        previous,
    } = expectations;
    let mut audit_report = writer(&candidate.directory.join("independent-scopes.json"))?;
    serde_json::to_writer(
        &mut audit_report,
        &json!({"algorithm":"literal_full_pairs_directional_outputs_original_nonmirror_operators","mandatory":mandatory.evidence(),"legacy_publication":old_scope.evidence()}),
    )?;
    audit_report.flush()?;
    let mut report = writer(&candidate.directory.join("differences.jsonl"))?;
    let mut failed = 0_u64;
    let mut canonical = BTreeSet::new();
    for (table, _) in TABLES {
        let mut b = Rows::open(&baseline.directory.join(table))?;
        let mut c = Rows::open(&candidate.directory.join(table))?;
        let mut r = Rows::open(&reference.directory.join(table))?;
        let (mut exact, mut retained) = (0_u64, 0_u64);
        while let Some(key) = [&b.next, &c.next, &r.next]
            .into_iter()
            .filter_map(|v| v.as_ref().map(|(k, _)| k))
            .min()
            .cloned()
        {
            let verdict = compare_row(
                table,
                b.at(&key),
                c.at(&key),
                r.at(&key),
                mandatory,
                old_scope,
                target,
            );
            let verdict = match verdict {
                Ok(Verdict::Retained) => {
                    validate_retained(tx, table, b.at(&key).unwrap(), previous, &mut canonical)
                        .await
                        .map(|()| Verdict::Retained)
                }
                other => other,
            };
            let label = match &verdict {
                Ok(Verdict::Exact) => {
                    exact += 1;
                    None
                }
                Ok(Verdict::Retained) => {
                    retained += 1;
                    Some("approved_old_symmetric_mirror_scope_only")
                }
                Err(_) => {
                    failed += 1;
                    Some("rejected")
                }
            };
            if let Some(label) = label {
                writeln!(
                    report,
                    "{}",
                    json!({"table":table,"key":key,"result":label,
                    "reason":verdict.as_ref().err().map(|e|e.to_string()),"baseline":b.at(&key),"candidate":c.at(&key),"reference":r.at(&key)})
                )?;
            }
            for rows in [&mut b, &mut c, &mut r] {
                if rows.at(&key).is_some() {
                    rows.advance()?;
                }
            }
        }
        eprintln!(
            "SEPOLIA_CONTRACT_COMPARISON table={table} exact={exact} retained={retained} cumulative_rejected={failed}"
        );
    }
    report.flush()?;
    // Keep explicit raw three-way differences private for review, including on success.
    // A successful caller can delete these after archiving the aggregate receipt.
    candidate.preserve = true;
    if failed > 0 {
        baseline.preserve = true;
        reference.preserve = true;
    } else {
        // Keep only the explicit exception report (with raw B/C/R) and independent
        // scope evidence. Successful full-row streams are removed, as in strict mode.
        for (table, _) in TABLES {
            fs::remove_file(candidate.directory.join(table))?;
        }
    }
    ensure!(
        failed == 0,
        "contract comparison rejected {failed} rows; private evidence directory {}",
        candidate.directory.display()
    );
    eprintln!(
        "SEPOLIA_CONTRACT_COMPARISON result=contract_accepted exact_reference_equal=not_claimed differences=private"
    );
    Ok(())
}

async fn validate_retained(
    tx: &mut Transaction<'_, Postgres>,
    table: &str,
    row: &Value,
    previous: i64,
    cache: &mut BTreeSet<(i64, String)>,
) -> Result<()> {
    let context = match table {
        "name_current" => &row["chain_positions"]["ethereum-sepolia"],
        "primary_names_current" => &row["claim_provenance"],
        _ => &row["chain_positions"],
    };
    let number_key = if table == "name_current" {
        "block_number"
    } else {
        "target_block_number"
    };
    let hash_key = if table == "name_current" {
        "block_hash"
    } else {
        "target_block_hash"
    };
    let number = context[number_key]
        .as_i64()
        .ok_or_else(|| anyhow::anyhow!("missing retained target number"))?;
    let hash = context[hash_key]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("missing retained target hash"))?;
    ensure!(
        number <= previous,
        "retained projection has future target context"
    );
    if table == "primary_names_current" {
        ensure!(
            context["chain_id"] == "ethereum-sepolia",
            "foreign primary context"
        );
    } else {
        ensure!(
            row["canonicality_summary"]["target_block_number"] == json!(number)
                && row["canonicality_summary"]["target_block_hash"] == json!(hash),
            "inconsistent retained target context"
        );
    }
    if !cache.contains(&(number, hash.into())) || table == "name_current" {
        let timestamp:Option<Value>=sqlx::query_scalar("SELECT to_jsonb(block_timestamp) FROM chain_lineage WHERE chain_id='ethereum-sepolia' AND block_number=$1 AND block_hash=$2 AND canonicality_state IN ('canonical','safe','finalized')")
            .bind(number).bind(hash).fetch_optional(&mut **tx).await?;
        ensure!(
            timestamp.is_some(),
            "retained target is not readable canonical history"
        );
        if table == "name_current" {
            ensure!(
                Some(&context["timestamp"]) == timestamp.as_ref(),
                "retained target timestamp does not match lineage"
            );
        }
        cache.insert((number, hash.into()));
    }
    Ok(())
}
