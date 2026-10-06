//! Unicode-scalar tokens shared by the identity writer and search query selector.
use std::collections::BTreeSet;

pub const CONTAINS: i16 = 1;
pub const PREFIX: i16 = 2;

/// Each distinct token once per document; byte values preserve exact Unicode spelling.
pub fn postings(name: &str) -> BTreeSet<(i16, i16, Vec<u8>)> {
    let chars: Vec<char> = name.chars().collect();
    let mut tokens = BTreeSet::new();
    for length in 1..=3_usize.min(chars.len()) {
        for window in chars.windows(length) {
            let text: String = window.iter().collect();
            tokens.insert((CONTAINS, length as i16, text.into_bytes()));
        }
        let prefix: String = chars[..length].iter().collect();
        tokens.insert((PREFIX, length as i16, prefix.into_bytes()));
    }
    tokens
}

/// Necessary terms, in stable probe order. One complete short range is sufficient for an
/// exact recheck; no caller may treat a truncated range as a complete set of matches.
pub fn required(query: &str, prefix: bool) -> Vec<(i16, String)> {
    let chars: Vec<char> = query.chars().collect();
    if chars.is_empty() {
        return Vec::new();
    }
    if chars.len() <= 3 {
        return vec![(if prefix { PREFIX } else { CONTAINS }, query.to_owned())];
    }
    let mut terms = Vec::new();
    let mut seen = BTreeSet::new();
    if prefix {
        let first: String = chars[..3].iter().collect();
        seen.insert((PREFIX, first.clone()));
        terms.push((PREFIX, first));
    }
    for window in chars.windows(3) {
        let token: String = window.iter().collect();
        if seen.insert((CONTAINS, token.clone())) {
            terms.push((CONTAINS, token));
        }
    }
    terms
}
