//! A block's journal, keys and rows bound in pieces. PostgreSQL refuses a jsonb array whose
//! elements take more than 268,435,455 bytes in their stored form, and a block that changes many
//! names would bind more than that as one array. A statement that binds such an array runs once
//! per piece instead, in order, on the block's transaction.
use std::io;

use serde_json::Value;

/// The JSON text of one bound array, at most. An element longer by itself is bound alone. The
/// stored form can take several times the text: `a_full_piece_of_the_densest_rows_is_accepted`
/// binds a full piece of the densest rows, one-digit numbers under one-letter keys and in arrays.
pub(crate) const BIND_BYTES: usize = 16 * 1024 * 1024;

fn budget() -> usize {
    #[cfg(test)]
    if let Some(bytes) = super::bind_log::budget() {
        return bytes;
    }
    BIND_BYTES
}

/// `values` in order, in arrays of at most `BIND_BYTES` of JSON text each. Each value moves into
/// its piece, so the pieces never hold a second copy.
pub(crate) fn chunks(values: Vec<Value>) -> impl Iterator<Item = Vec<Value>> + Send {
    let budget = budget();
    let mut values = values
        .into_iter()
        .map(|value| {
            let bytes = text_bytes(&value);
            (value, bytes)
        })
        .peekable();
    std::iter::from_fn(move || {
        let mut chunk = Vec::new();
        // The opening bracket, then each element with the comma or bracket after it.
        let mut bytes = 1;
        while let Some(&(_, size)) = values.peek() {
            if !chunk.is_empty() && bytes + size + 1 > budget {
                break;
            }
            bytes += size + 1;
            chunk.extend(values.next().map(|(value, _)| value));
        }
        (!chunk.is_empty()).then_some(chunk)
    })
}

/// The length of `value` as compact JSON, which is what a bound jsonb value carries.
fn text_bytes(value: &Value) -> usize {
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value).expect("a JSON value serializes");
    counter.0
}

struct Counter(usize);

impl io::Write for Counter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0 += buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
