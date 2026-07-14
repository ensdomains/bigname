use std::io::{self, Write};

use alloy_primitives::{Keccak256, hex};
use anyhow::{Context, Result};
use serde::Serialize;

pub(super) fn keccak256_json_digest<T>(value: &T) -> Result<String>
where
    T: Serialize + ?Sized,
{
    let mut writer = Keccak256Writer::default();
    serde_json::to_writer(&mut writer, value).context("failed to serialize JSON digest input")?;
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
