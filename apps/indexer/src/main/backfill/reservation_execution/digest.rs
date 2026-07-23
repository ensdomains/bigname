use std::io::{self, Write};

use alloy_primitives::{Keccak256, hex};
use anyhow::{Context, Result};
use serde::Serialize;

use bigname_adapters::StartupAdapterProgress;
use bigname_manifests::WatchedBackfillTarget;

const DIGEST_PROGRESS_ITEMS: usize = 1_000;

pub(super) fn keccak256_json_digest<T>(value: &T) -> Result<String>
where
    T: Serialize + ?Sized,
{
    let mut writer = Keccak256Writer::default();
    serde_json::to_writer(&mut writer, value).context("failed to serialize JSON digest input")?;
    Ok(format!("keccak256:{}", hex_string(&writer.finalize())))
}

pub(super) async fn keccak256_selected_targets_digest_with_progress(
    pool: &sqlx::PgPool,
    items: &[WatchedBackfillTarget],
    excluded_source_family: Option<&str>,
    progress: &mut dyn StartupAdapterProgress,
) -> Result<String> {
    let mut writer = Keccak256Writer::default();
    writer.write_all(b"[")?;
    let mut count = 0usize;
    for item in items {
        if excluded_source_family.is_some_and(|family| item.source_family == family) {
            continue;
        }
        if count > 0 {
            writer.write_all(b",")?;
        }
        serde_json::to_writer(&mut writer, item)
            .context("failed to serialize JSON digest array item")?;
        count += 1;
        if count.is_multiple_of(DIGEST_PROGRESS_ITEMS) {
            progress.record(pool).await?;
        }
    }
    writer.write_all(b"]")?;
    if count > 0 && !count.is_multiple_of(DIGEST_PROGRESS_ITEMS) {
        progress.record(pool).await?;
    }
    Ok(format!("keccak256:{}", hex_string(&writer.finalize())))
}

pub(super) async fn keccak256_json_value_digest_with_progress(
    pool: &sqlx::PgPool,
    value: &serde_json::Value,
    progress: &mut dyn StartupAdapterProgress,
) -> Result<String> {
    let Some(fields) = value.as_object() else {
        return keccak256_json_digest(value);
    };
    let mut writer = Keccak256Writer::default();
    writer.write_all(b"{")?;
    for (field_index, (key, value)) in fields.iter().enumerate() {
        if field_index > 0 {
            writer.write_all(b",")?;
        }
        serde_json::to_writer(&mut writer, key).context("failed to serialize JSON digest key")?;
        writer.write_all(b":")?;
        if let Some(items) = value.as_array() {
            writer.write_all(b"[")?;
            for (item_index, item) in items.iter().enumerate() {
                if item_index > 0 {
                    writer.write_all(b",")?;
                }
                serde_json::to_writer(&mut writer, item)
                    .context("failed to serialize JSON digest array item")?;
                if (item_index + 1).is_multiple_of(DIGEST_PROGRESS_ITEMS) {
                    progress.record(pool).await?;
                }
            }
            writer.write_all(b"]")?;
            if !items.is_empty() && !items.len().is_multiple_of(DIGEST_PROGRESS_ITEMS) {
                progress.record(pool).await?;
            }
        } else {
            serde_json::to_writer(&mut writer, value)
                .context("failed to serialize JSON digest value")?;
        }
    }
    writer.write_all(b"}")?;
    Ok(format!("keccak256:{}", hex_string(&writer.finalize())))
}

#[derive(Default)]
struct Keccak256Writer {
    hasher: Keccak256,
}

impl Keccak256Writer {
    fn finalize(self) -> [u8; 32] {
        self.hasher.finalize().0
    }
}

impl Write for Keccak256Writer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.hasher.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn hex_string(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}
