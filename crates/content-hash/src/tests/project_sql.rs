//! Every `.sql` file under `crates/project/src` is a hash input, whatever code loads it. SQL
//! that only tests load (fixtures, reference oracles) belongs in `crates/project/testdata/sql/`,
//! so that editing it does not rotate the interpreter content hash.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path, PathBuf},
};

use super::workspace_root;

const PROJECT_SOURCE_ROOT: &str = "crates/project/src";

#[derive(Default)]
struct References {
    production: BTreeSet<String>,
    test: BTreeSet<String>,
}

#[test]
fn project_sql_under_the_hashed_root_is_loaded_by_production_code() {
    let workspace_root = workspace_root();
    let (references, unsupported) = project_sql_references(&workspace_root);
    let mut sql_files = Vec::new();
    collect_files(
        &workspace_root.join(PROJECT_SOURCE_ROOT),
        "sql",
        &mut sql_files,
    );
    assert!(
        !sql_files.is_empty(),
        "expected production SQL under {PROJECT_SOURCE_ROOT}"
    );

    let mut violations = Vec::new();
    for path in sql_files {
        let key = relative_key(&workspace_root, &path);
        let entry = references.get(&key);
        if entry.is_some_and(|entry| !entry.production.is_empty()) {
            continue;
        }
        let loaders = entry
            .map(|entry| entry.test.iter().cloned().collect::<Vec<_>>().join(", "))
            .unwrap_or_default();
        violations.push(if loaders.is_empty() {
            format!("{key} (not loaded by any Rust source)")
        } else {
            format!("{key} (loaded only by test code: {loaders})")
        });
    }
    assert!(
        violations.is_empty(),
        "SQL under {PROJECT_SOURCE_ROOT} is a content-hash input; move test-only SQL to \
         crates/project/testdata/sql/:\n{}",
        violations.join("\n")
    );
    // Only a production loader can satisfy the rule above, so a production include_str! this
    // scanner cannot read must fail here with its own message rather than surface later as a
    // file "not loaded by any Rust source". An unreadable spelling in test code cannot hide a
    // violation, because test loaders never count.
    let production_unsupported = unsupported
        .iter()
        .filter(|(_, test)| !test)
        .map(|(message, _)| message.as_str())
        .collect::<Vec<_>>();
    assert!(
        production_unsupported.is_empty(),
        "{}",
        production_unsupported.join("\n")
    );
}

#[test]
fn include_scanner_separates_cfg_test_items_from_production_code() {
    let source = r#####"
        const A: &str = include_str!("production.sql");
        const RAW: &str = include_str!(r"raw_production.sql");
        const SPACED: &str = std::include_str! ( "spaced_production.sql" );
        fn build(x: bool) -> u8 {
            #[cfg(test)]
            if x {
                let _ = include_str!("gated_if.sql");
                let _ = include_str!(r#"raw_gated.sql"#);
                return 1;
            }
            let _ = "include_str!(\"in_string.sql\")";
            let _ = my_include_str!("other_macro.sql");
            #[cfg(test)]
            let y = if x { include_str!("gated_let.sql") } else { "}" };
            let _ = '{';
            let _ = r#"{ include_str!("in_raw_string.sql") "#;
            // include_str!("in_comment.sql") {
            let _: &'static str = include_str!(
                "after_lifetime.sql"
            );
            0
        }
        struct S {
            #[cfg(test)]
            gated: [u8; 2],
            open: u8,
        }
        #[cfg(test)]
        #[path = "x.rs"]
        mod tests;
        const B: &str = include_str!("after_module.sql");
    "#####;
    let found = include_sites(source);
    assert_eq!(
        found,
        vec![
            ("production.sql".to_owned(), false),
            ("raw_production.sql".to_owned(), false),
            ("spaced_production.sql".to_owned(), false),
            ("gated_if.sql".to_owned(), true),
            ("raw_gated.sql".to_owned(), true),
            ("gated_let.sql".to_owned(), true),
            ("after_lifetime.sql".to_owned(), false),
            ("after_module.sql".to_owned(), false),
        ]
    );
}

#[test]
fn include_scanner_accepts_every_literal_spelling() {
    let source = r#####"
        const C1: &str = include_str!(/* note */ "block_comment_production.sql");
        const C2: &str = include_str! // note
            ("line_comment_production.sql");
        const K: &str = include_str!["bracket_production.sql"];
        const BR: &str = include_str!{"brace_production.sql"};
        const H: &str = include_str!("hex_production\x2esql");
        const U: &str = include_str!("unicode_production\u{2e}sql");
        const N: &str = include_str!("continued_\
                                      production.sql");
        #[cfg(test)]
        fn gated_forms() {
            let _ = include_str!(/* note */ "block_comment_gated.sql");
            let _ = include_str!["bracket_gated.sql"];
            let _ = include_str!{"brace_gated.sql"};
            let _ = include_str!("hex_gated\x2esql");
            let _ = include_str!("unicode_gated\u{2e}sql");
            let _ = include_str!("continued_\
                                  gated.sql");
        }
        fn build() -> usize {
            #[cfg(test)]
            let a = include_str!{"brace_gate_first.sql"}.len() + include_str!("brace_gate_second.sql").len();
            let p = include_str!("after_brace_gate.sql");
            0
        }
    "#####;
    assert_eq!(
        include_sites(source),
        vec![
            ("block_comment_production.sql".to_owned(), false),
            ("line_comment_production.sql".to_owned(), false),
            ("bracket_production.sql".to_owned(), false),
            ("brace_production.sql".to_owned(), false),
            ("hex_production.sql".to_owned(), false),
            ("unicode_production.sql".to_owned(), false),
            ("continued_production.sql".to_owned(), false),
            ("block_comment_gated.sql".to_owned(), true),
            ("bracket_gated.sql".to_owned(), true),
            ("brace_gated.sql".to_owned(), true),
            ("hex_gated.sql".to_owned(), true),
            ("unicode_gated.sql".to_owned(), true),
            ("continued_gated.sql".to_owned(), true),
            ("brace_gate_first.sql".to_owned(), true),
            ("brace_gate_second.sql".to_owned(), true),
            ("after_brace_gate.sql".to_owned(), false),
        ]
    );
}

#[test]
fn include_scanner_reports_spellings_it_cannot_read() {
    let source = r#####"
        const A: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/computed.sql"));
        const B: &str = include_str!("bad_escape\q.sql");
        const C: &str = include_str! "no_delimiter.sql";
        const D: &str = include_str!(PATH);
        const E: &str = include_str!("two" "literals.sql");
        #[cfg(test)]
        const F: &str = include_str!(concat!("gated", ".sql"));
        const G: &str = include_str!("still_read.sql");
        const H: bool = include_str != 0;
    "#####;
    let scan = scan_includes(source);
    assert_eq!(scan.sites, vec![("still_read.sql".to_owned(), false)]);
    assert_eq!(
        scan.unsupported,
        vec![
            UnsupportedSite {
                line: 2,
                gated: false
            },
            UnsupportedSite {
                line: 3,
                gated: false
            },
            UnsupportedSite {
                line: 4,
                gated: false
            },
            UnsupportedSite {
                line: 5,
                gated: false
            },
            UnsupportedSite {
                line: 6,
                gated: false
            },
            UnsupportedSite {
                line: 8,
                gated: true
            },
        ]
    );
    let message = unsupported_message("crates/project/src/scope/example.rs", 2);
    for expected in [
        "crates/project/src/scope/example.rs:2:",
        "include_str!(\"path\")",
        "include_str![\"path\"]",
        "include_str!{\"path\"}",
        "concat!",
        "use a plain literal path",
    ] {
        assert!(message.contains(expected), "{message} lacks {expected}");
    }
}

/// Maps each `.sql` file under the Project source root to the Rust files that load it with
/// `include_str!`, split into production loaders and test-only loaders. A loader is test-only
/// when its file is a `#[cfg(test)]` module (the content hash's own scanner decides which) or
/// the `include_str!` sits inside an item or statement marked `#[cfg(test)]`. Also returns every
/// `include_str!` invocation the scanner cannot read, with whether it is in test code.
fn project_sql_references(
    workspace_root: &Path,
) -> (BTreeMap<String, References>, Vec<(String, bool)>) {
    let cfg_test_modules =
        crate::source_paths::cfg_test_sources(workspace_root, &[PROJECT_SOURCE_ROOT])
            .expect("cfg(test) module scan must succeed");
    let mut rust_files = Vec::new();
    collect_files(
        &workspace_root.join(PROJECT_SOURCE_ROOT),
        "rs",
        &mut rust_files,
    );
    collect_files(
        &workspace_root.join("crates/project/tests"),
        "rs",
        &mut rust_files,
    );

    let mut references: BTreeMap<String, References> = BTreeMap::new();
    let mut unsupported = Vec::new();
    for path in rust_files {
        let key = relative_key(workspace_root, &path);
        let test_file =
            cfg_test_modules.contains(&key) || !key.starts_with(&format!("{PROJECT_SOURCE_ROOT}/"));
        let source = fs::read_to_string(&path).expect("Project source must be readable");
        let scan = scan_includes(&source);
        for site in scan.unsupported {
            unsupported.push((
                unsupported_message(&key, site.line),
                test_file || site.gated,
            ));
        }
        for (literal, gated) in scan.sites {
            if !literal.ends_with(".sql") {
                continue;
            }
            let target = normalize(&path.parent().expect("file has a parent").join(&literal));
            let target_key = relative_key(workspace_root, &target);
            let entry = references.entry(target_key).or_default();
            if test_file || gated {
                entry.test.insert(key.clone());
            } else {
                entry.production.insert(key.clone());
            }
        }
    }
    (references, unsupported)
}

fn unsupported_message(file: &str, line: usize) -> String {
    format!(
        "{file}:{line}: include_str! spelling this guard cannot read. Write \
         include_str!(\"path\"), include_str![\"path\"] or include_str!{{\"path\"}} with one \
         plain or raw string literal as the path (whitespace and // or /* */ comments between \
         the tokens are fine). concat! and other computed paths are not supported: use a plain \
         literal path."
    )
}

#[derive(Debug, PartialEq)]
struct UnsupportedSite {
    line: usize,
    gated: bool,
}

#[derive(Debug, Default)]
struct IncludeScan {
    sites: Vec<(String, bool)>,
    unsupported: Vec<UnsupportedSite>,
}

fn include_sites(source: &str) -> Vec<(String, bool)> {
    scan_includes(source).sites
}

/// Returns each `include_str!` path literal in `source`, and whether it sits inside an item or
/// statement marked `#[cfg(test)]`, plus every `include_str!` invocation whose argument is not a
/// single string literal. The scan skips comments, string literals, and character literals, and
/// tracks delimiter depth: a marked region ends at the `;` or `,` that ends it at its own depth,
/// at the `}` that closes its block (unless `else` follows), or when its enclosing block closes.
/// The braces of a brace-delimited macro call never end a region.
fn scan_includes(source: &str) -> IncludeScan {
    let chars: Vec<char> = source.chars().collect();
    let mut scan = IncludeScan::default();
    let mut depth = 0usize;
    let mut gate: Option<usize> = None;
    let mut macro_braces = BTreeSet::new();
    let mut braces: Vec<bool> = Vec::new();
    let mut index = 0;
    while index < chars.len() {
        let c = chars[index];
        let next = chars.get(index + 1).copied();
        match c {
            '/' if next == Some('/') => {
                while index < chars.len() && chars[index] != '\n' {
                    index += 1;
                }
                continue;
            }
            '/' if next == Some('*') => {
                index = skip_block_comment(&chars, index);
                continue;
            }
            'i' if !is_ident_char(previous(&chars, index)) => {
                if let Some(invocation) = include_invocation(&chars, index) {
                    match invocation.argument {
                        Some(literal) => scan.sites.push((literal, gate.is_some())),
                        None => scan.unsupported.push(UnsupportedSite {
                            line: chars[..index].iter().filter(|c| **c == '\n').count() + 1,
                            gated: gate.is_some(),
                        }),
                    }
                    if let Some(open) = invocation.brace {
                        macro_braces.insert(open);
                    }
                    // Resume after the `!` so the argument's delimiters and literal are scanned
                    // like any other tokens.
                    index = invocation.after_bang;
                    continue;
                }
            }
            'r' | 'b' if !is_ident_char(previous(&chars, index)) => {
                if let Some((_, end)) = read_prefixed_string(&chars, index) {
                    index = end;
                    continue;
                }
            }
            '"' => {
                index = read_string(&chars, index).end;
                continue;
            }
            '\'' => {
                index = skip_char_or_lifetime(&chars, index);
                continue;
            }
            '#' if next == Some('[') => {
                let end = matching_bracket(&chars, index + 1);
                let attribute: String = chars[index..end]
                    .iter()
                    .filter(|c| !c.is_whitespace())
                    .collect();
                if attribute == "#[cfg(test)]" && gate.is_none() {
                    gate = Some(depth);
                }
                index = end;
                continue;
            }
            '{' => {
                depth += 1;
                braces.push(macro_braces.contains(&index) || follows_macro_bang(&chars, index));
            }
            '(' | '[' => depth += 1,
            '}' | ')' | ']' => {
                depth = depth.saturating_sub(1);
                let macro_brace = c == '}' && braces.pop().unwrap_or(false);
                if let Some(start) = gate {
                    let enclosing_closed = depth < start;
                    let block_ended = depth == start
                        && c == '}'
                        && !macro_brace
                        && !next_word_is(&chars, index + 1, "else");
                    if enclosing_closed || block_ended {
                        gate = None;
                    }
                }
            }
            ';' | ',' if gate == Some(depth) => gate = None,
            _ => {}
        }
        index += 1;
    }
    scan
}

struct Invocation {
    /// The path literal, or `None` when the argument is not a single string literal.
    argument: Option<String>,
    after_bang: usize,
    /// Index of the opening `{` of a brace-delimited call.
    brace: Option<usize>,
}

/// Recognizes an `include_str!` invocation starting at `start`: the macro name, `!`, an opening
/// `(`, `[` or `{`, one ordinary or raw string literal, an optional trailing comma, and the
/// matching closing delimiter, with whitespace and non-doc comments allowed between them.
/// Returns `None` when `start` does not begin an invocation, and an invocation without an
/// argument when the name and `!` are there but the rest cannot be read.
fn include_invocation(chars: &[char], start: usize) -> Option<Invocation> {
    const NAME: &str = "include_str";
    let mut index = start;
    for expected in NAME.chars() {
        if chars.get(index) != Some(&expected) {
            return None;
        }
        index += 1;
    }
    if chars.get(index).copied().is_some_and(is_ident_char) {
        return None;
    }
    index = skip_trivia(chars, index);
    if chars.get(index) != Some(&'!') || chars.get(index + 1) == Some(&'=') {
        return None;
    }
    let after_bang = index + 1;
    let unreadable = Invocation {
        argument: None,
        after_bang,
        brace: None,
    };
    index = skip_trivia(chars, after_bang);
    let open = index;
    let close = match chars.get(open) {
        Some('(') => ')',
        Some('[') => ']',
        Some('{') => '}',
        _ => return Some(unreadable),
    };
    let brace = (close == '}').then_some(open);
    let unreadable = Invocation {
        brace,
        ..unreadable
    };
    index = skip_trivia(chars, open + 1);
    let (literal, end) = match chars.get(index) {
        Some('"') => {
            let string = read_string(chars, index);
            match string.decoded {
                Some(literal) => (literal, string.end),
                None => return Some(unreadable),
            }
        }
        Some('r') => match read_prefixed_string(chars, index) {
            Some(raw) => raw,
            None => return Some(unreadable),
        },
        _ => return Some(unreadable),
    };
    index = skip_trivia(chars, end);
    if chars.get(index) == Some(&',') {
        index = skip_trivia(chars, index + 1);
    }
    if chars.get(index) != Some(&close) {
        return Some(unreadable);
    }
    Some(Invocation {
        argument: Some(literal),
        after_bang,
        brace,
    })
}

/// Skips whitespace, `//` line comments and `/* */` block comments, but not doc comments.
fn skip_trivia(chars: &[char], mut index: usize) -> usize {
    loop {
        while chars.get(index).is_some_and(|c| c.is_whitespace()) {
            index += 1;
        }
        let at = |offset: usize| chars.get(index + offset).copied();
        if at(0) == Some('/') && at(1) == Some('/') {
            let doc = at(2) == Some('!') || (at(2) == Some('/') && at(3) != Some('/'));
            if doc {
                return index;
            }
            while index < chars.len() && chars[index] != '\n' {
                index += 1;
            }
        } else if at(0) == Some('/') && at(1) == Some('*') {
            let doc = at(2) == Some('!')
                || (at(2) == Some('*') && !matches!(at(3), Some('*') | Some('/')));
            if doc {
                return index;
            }
            index = skip_block_comment(chars, index);
        } else {
            return index;
        }
    }
}

/// True when the `{` at `index` directly follows `name!`, which makes it a macro delimiter.
fn follows_macro_bang(chars: &[char], index: usize) -> bool {
    let mut before = index;
    while before > 0 && chars[before - 1].is_whitespace() {
        before -= 1;
    }
    before >= 2 && chars[before - 1] == '!' && is_ident_char(chars[before - 2])
}

fn previous(chars: &[char], index: usize) -> char {
    if index == 0 { ' ' } else { chars[index - 1] }
}

fn is_ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

fn skip_block_comment(chars: &[char], start: usize) -> usize {
    let mut nesting = 0;
    let mut index = start;
    while index + 1 < chars.len() {
        if chars[index] == '/' && chars[index + 1] == '*' {
            nesting += 1;
            index += 2;
        } else if chars[index] == '*' && chars[index + 1] == '/' {
            nesting -= 1;
            index += 2;
            if nesting == 0 {
                return index;
            }
        } else {
            index += 1;
        }
    }
    chars.len()
}

/// Reads `r"…"`, `r#"…"#`, `b"…"`, `br#"…"#` and `b'…'`, returning the contents (verbatim for a
/// raw string, empty for a byte character) and the index after the literal; returns `None` for
/// an identifier.
fn read_prefixed_string(chars: &[char], start: usize) -> Option<(String, usize)> {
    let mut index = start;
    if chars[index] == 'b' {
        index += 1;
        match chars.get(index) {
            Some('"') => {
                let string = read_string(chars, index);
                return Some((string.decoded.unwrap_or_default(), string.end));
            }
            Some('\'') => return Some((String::new(), skip_char_or_lifetime(chars, index))),
            Some('r') => {}
            _ => return None,
        }
    }
    index += 1;
    let mut hashes = 0;
    while chars.get(index) == Some(&'#') {
        hashes += 1;
        index += 1;
    }
    if chars.get(index) != Some(&'"') {
        return None;
    }
    index += 1;
    let contents_start = index;
    while index < chars.len() {
        if chars[index] == '"'
            && (0..hashes).all(|offset| chars.get(index + 1 + offset) == Some(&'#'))
        {
            let literal = chars[contents_start..index].iter().collect();
            return Some((literal, index + 1 + hashes));
        }
        index += 1;
    }
    Some((chars[contents_start..].iter().collect(), chars.len()))
}

struct StringLiteral {
    /// The value with escapes decoded, or `None` when an escape is not a valid string escape.
    decoded: Option<String>,
    end: usize,
}

/// Reads the ordinary string literal whose opening quote is at `start`, decoding `\"`, `\\`,
/// `\'`, `\n`, `\r`, `\t`, `\0`, `\xNN` (up to `\x7F`), `\u{…}` and the backslash-newline
/// continuation, which drops the newline and the whitespace after it.
fn read_string(chars: &[char], start: usize) -> StringLiteral {
    let mut literal = String::new();
    let mut valid = true;
    let mut index = start + 1;
    while index < chars.len() {
        match chars[index] {
            '"' => {
                return StringLiteral {
                    decoded: valid.then_some(literal),
                    end: index + 1,
                };
            }
            '\\' => {
                let (decoded, next) = read_escape(chars, index);
                match decoded {
                    Some(Some(c)) => literal.push(c),
                    Some(None) => {}
                    None => valid = false,
                }
                index = next;
            }
            c => {
                literal.push(c);
                index += 1;
            }
        }
    }
    StringLiteral {
        decoded: None,
        end: chars.len(),
    }
}

/// Decodes the escape whose backslash is at `start`. Returns `Some(Some(c))` for a character,
/// `Some(None)` for a line continuation, `None` for an invalid escape, and the index after it.
/// An invalid escape never consumes the closing quote.
fn read_escape(chars: &[char], start: usize) -> (Option<Option<char>>, usize) {
    let simple = |c: char| (Some(Some(c)), start + 2);
    match chars.get(start + 1) {
        Some('"') => simple('"'),
        Some('\\') => simple('\\'),
        Some('\'') => simple('\''),
        Some('n') => simple('\n'),
        Some('r') => simple('\r'),
        Some('t') => simple('\t'),
        Some('0') => simple('\0'),
        Some('x') => {
            let digits: String = chars.iter().skip(start + 2).take(2).collect();
            match u8::from_str_radix(&digits, 16) {
                Ok(value) if digits.len() == 2 && value <= 0x7f => {
                    (Some(Some(char::from(value))), start + 4)
                }
                _ => (None, start + 2),
            }
        }
        Some('u') if chars.get(start + 2) == Some(&'{') => {
            let mut index = start + 3;
            let mut digits = String::new();
            while let Some(&c) = chars.get(index) {
                if c == '}' || c == '"' {
                    break;
                }
                if c != '_' {
                    digits.push(c);
                }
                index += 1;
            }
            let decoded = (chars.get(index) == Some(&'}') && (1..=6).contains(&digits.len()))
                .then(|| u32::from_str_radix(&digits, 16).ok())
                .flatten()
                .and_then(char::from_u32);
            match decoded {
                Some(c) => (Some(Some(c)), index + 1),
                None => (None, index),
            }
        }
        Some('\n') | Some('\r') => {
            let mut index = start + 1;
            while chars.get(index).is_some_and(|c| c.is_whitespace()) {
                index += 1;
            }
            (Some(None), index)
        }
        Some('u') | Some(_) => (None, start + 2),
        None => (None, start + 1),
    }
}

fn skip_char_or_lifetime(chars: &[char], start: usize) -> usize {
    match chars.get(start + 1) {
        Some('\\') => {
            let mut index = start + 2;
            while index < chars.len() && chars[index] != '\'' {
                index += 1;
            }
            index + 1
        }
        Some(_) if chars.get(start + 2) == Some(&'\'') => start + 3,
        _ => start + 1,
    }
}

fn matching_bracket(chars: &[char], open: usize) -> usize {
    let mut nesting = 0;
    let mut index = open;
    while index < chars.len() {
        match chars[index] {
            '[' => nesting += 1,
            ']' => {
                nesting -= 1;
                if nesting == 0 {
                    return index + 1;
                }
            }
            '"' => {
                index = read_string(chars, index).end;
                continue;
            }
            _ => {}
        }
        index += 1;
    }
    chars.len()
}

fn next_word_is(chars: &[char], start: usize, word: &str) -> bool {
    let rest: String = chars[start..]
        .iter()
        .skip_while(|c| c.is_whitespace())
        .take(word.len() + 1)
        .collect();
    rest.starts_with(word) && !rest[word.len()..].starts_with(is_ident_char)
}

fn collect_files(directory: &Path, extension: &str, files: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(directory) else {
        return;
    };
    let mut entries = entries
        .collect::<Result<Vec<_>, _>>()
        .expect("directory entries must be readable");
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect_files(&path, extension, files);
        } else if path.extension().is_some_and(|found| found == extension) {
            files.push(path);
        }
    }
}

fn normalize(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::ParentDir if normalized.file_name().is_some_and(|name| name != "..") => {
                normalized.pop();
            }
            Component::CurDir => {}
            other => normalized.push(other),
        }
    }
    normalized
}

fn relative_key(workspace_root: &Path, path: &Path) -> String {
    let root = normalize(workspace_root);
    normalize(path)
        .strip_prefix(&root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}
