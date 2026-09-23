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
    let references = project_sql_references(&workspace_root);
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

/// Maps each `.sql` file under the Project source root to the Rust files that load it with
/// `include_str!`, split into production loaders and test-only loaders. A loader is test-only
/// when its file is a `#[cfg(test)]` module (the content hash's own scanner decides which) or
/// the `include_str!` sits inside an item or statement marked `#[cfg(test)]`.
fn project_sql_references(workspace_root: &Path) -> BTreeMap<String, References> {
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
    for path in rust_files {
        let key = relative_key(workspace_root, &path);
        let test_file =
            cfg_test_modules.contains(&key) || !key.starts_with(&format!("{PROJECT_SOURCE_ROOT}/"));
        let source = fs::read_to_string(&path).expect("Project source must be readable");
        for (literal, gated) in include_sites(&source) {
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
    references
}

/// Returns each `include_str!` path literal in `source`, and whether it sits inside an item or
/// statement marked `#[cfg(test)]`. The scan skips comments, string literals, and character
/// literals, and tracks delimiter depth: a marked region ends at the `;` or `,` that ends it at
/// its own depth, at the `}` that closes its block (unless `else` follows), or when its
/// enclosing block closes.
fn include_sites(source: &str) -> Vec<(String, bool)> {
    let chars: Vec<char> = source.chars().collect();
    let mut sites = Vec::new();
    let mut depth = 0usize;
    let mut gate: Option<usize> = None;
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
                if let Some((literal, after_bang)) = include_argument(&chars, index) {
                    sites.push((literal, gate.is_some()));
                    // Resume after the `!` so the argument's delimiters and literal are scanned
                    // like any other tokens.
                    index = after_bang;
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
                index = read_string(&chars, index).1;
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
            '{' | '(' | '[' => depth += 1,
            '}' | ')' | ']' => {
                depth = depth.saturating_sub(1);
                if let Some(start) = gate {
                    let enclosing_closed = depth < start;
                    let block_ended =
                        depth == start && c == '}' && !next_word_is(&chars, index + 1, "else");
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
    sites
}

/// Recognizes an `include_str!` invocation starting at `start`: the macro name, `!`, `(`, and
/// an ordinary or raw string literal, with optional whitespace between them. Returns the literal
/// and the index just after the `!`.
fn include_argument(chars: &[char], start: usize) -> Option<(String, usize)> {
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
    index = skip_whitespace(chars, index);
    if chars.get(index) != Some(&'!') {
        return None;
    }
    let after_bang = index + 1;
    index = skip_whitespace(chars, after_bang);
    if chars.get(index) != Some(&'(') {
        return None;
    }
    index = skip_whitespace(chars, index + 1);
    let literal = match chars.get(index) {
        Some('"') => read_string(chars, index).0,
        Some('r') => read_prefixed_string(chars, index)?.0,
        _ => return None,
    };
    Some((literal, after_bang))
}

fn skip_whitespace(chars: &[char], mut index: usize) -> usize {
    while chars.get(index).is_some_and(|c| c.is_whitespace()) {
        index += 1;
    }
    index
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

/// Reads `r"…"`, `r#"…"#`, `b"…"`, `br#"…"#` and `b'…'`, returning the contents (empty for a
/// byte character) and the index after the literal; returns `None` for an identifier.
fn read_prefixed_string(chars: &[char], start: usize) -> Option<(String, usize)> {
    let mut index = start;
    if chars[index] == 'b' {
        index += 1;
        match chars.get(index) {
            Some('"') => return Some(read_string(chars, index)),
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

fn read_string(chars: &[char], start: usize) -> (String, usize) {
    let mut literal = String::new();
    let mut index = start + 1;
    while index < chars.len() {
        match chars[index] {
            '\\' => {
                if let Some(escaped) = chars.get(index + 1) {
                    literal.push(*escaped);
                }
                index += 2;
            }
            '"' => return (literal, index + 1),
            c => {
                literal.push(c);
                index += 1;
            }
        }
    }
    (literal, chars.len())
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
                index = read_string(chars, index).1;
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
