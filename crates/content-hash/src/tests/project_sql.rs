//! Every `.sql` file under `crates/project/src` is a hash input, whatever code loads it. SQL
//! that only tests load (fixtures, reference oracles) belongs in `crates/project/testdata/sql/`,
//! so that editing it does not rotate the interpreter content hash.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path, PathBuf},
};

use proc_macro2::{TokenStream, TokenTree};
use syn::{
    Arm, Attribute, Expr, Field, FieldValue, ImplItem, Item, LitStr, Local, Macro, Meta, StmtMacro,
    Token, TraitItem, Variant,
    parse::{ParseStream, Parser},
    visit::{self, Visit},
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
    let root = workspace_root();
    let mut sql_files = Vec::new();
    collect_files(&root.join(PROJECT_SOURCE_ROOT), "sql", &mut sql_files);
    assert!(
        !sql_files.is_empty(),
        "expected production SQL under {PROJECT_SOURCE_ROOT}"
    );
    let failures = inventory_failures(&root);
    assert!(
        failures.is_empty(),
        "SQL under {PROJECT_SOURCE_ROOT} is a content-hash input; every file there needs a \
         production loader, and test-only SQL belongs in crates/project/testdata/sql/:\n{}",
        failures.join("\n")
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
        const D: &str = include_str!(PATH);
        const E: &str = include_str!("two" "literals.sql");
        const F: &str = include_str!(b"bytes.sql");
        const G: &str = std::include_str!(c"c_string.sql");
        #[cfg(test)]
        const H: &str = include_str!(concat!("gated", ".sql"));
        const I: &str = include_str!("still_read.sql");
        const J: bool = include_str != 0;
        const K: &str = my::include_str!(concat!("other", ".sql"));
    "#####;
    let scan = scan_includes(source).expect("fixture must parse as Rust");
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

#[test]
fn inventory_reports_an_unreadable_production_spelling_before_the_loader_it_hides() {
    let tree = super::SampleTree::empty();
    tree.write(
        "crates/project/src/scope/mirror.rs",
        "pub fn stage() -> &'static str {\n    include_str!(concat!(\"mirror\", \".sql\"))\n}\n",
    );
    tree.write("crates/project/src/scope/mirror.sql", "SELECT 1;\n");
    tree.write("crates/project/src/scope/broken.rs", "fn broken( {\n");
    let failures = inventory_failures(tree.path());
    assert_eq!(failures.len(), 3, "{failures:#?}");
    assert!(
        failures[0].starts_with("crates/project/src/scope/broken.rs: does not parse as Rust"),
        "{failures:#?}"
    );
    assert_eq!(
        failures[1],
        unsupported_message("crates/project/src/scope/mirror.rs", 2)
    );
    assert_eq!(
        failures[2],
        "crates/project/src/scope/mirror.sql (not loaded by any Rust source)"
    );
}

#[test]
fn current_tree_keeps_computed_includes_in_test_code() {
    let root = workspace_root();
    let inventory = project_sql_inventory(&root);
    assert!(
        inventory.unparsable.is_empty(),
        "{:#?}",
        inventory.unparsable
    );
    // Project tests load migrations through include_str!(concat!(env!(...), ...)), which the
    // guard cannot read; they must all be recognized as test code.
    assert!(
        !inventory.unsupported.is_empty(),
        "expected the computed migration includes in Project tests to be reported"
    );
    for (message, test) in &inventory.unsupported {
        eprintln!("unsupported (test code: {test}): {message}");
        assert!(test, "{message}");
    }
    let mut sql_files = Vec::new();
    collect_files(&root.join(PROJECT_SOURCE_ROOT), "sql", &mut sql_files);
    let loaded = sql_files
        .iter()
        .filter(|path| {
            inventory
                .references
                .get(&relative_key(&root, path))
                .is_some_and(|entry| !entry.production.is_empty())
        })
        .count();
    eprintln!(
        "production SQL files with a production loader: {loaded}/{}",
        sql_files.len()
    );
    assert_eq!(loaded, sql_files.len());
}

/// What the guard learned about one tree: each `.sql` path's loaders, every `include_str!` it
/// could not read (message, and whether it is in test code), and every Rust file that does not
/// parse.
#[derive(Default)]
struct Inventory {
    references: BTreeMap<String, References>,
    unsupported: Vec<(String, bool)>,
    unparsable: Vec<String>,
}

/// Every reason the tree under `root` breaks the rule, in one list: Rust files that do not
/// parse, then production `include_str!` spellings the guard cannot read, then SQL under the
/// hashed root without a production loader. Reporting them together keeps an unreadable
/// production spelling from being masked by the missing loader it causes.
fn inventory_failures(root: &Path) -> Vec<String> {
    let inventory = project_sql_inventory(root);
    let mut failures = inventory.unparsable;
    failures.extend(
        inventory
            .unsupported
            .into_iter()
            .filter(|(_, test)| !test)
            .map(|(message, _)| message),
    );
    let mut sql_files = Vec::new();
    collect_files(&root.join(PROJECT_SOURCE_ROOT), "sql", &mut sql_files);
    for path in sql_files {
        let key = relative_key(root, &path);
        let entry = inventory.references.get(&key);
        if entry.is_some_and(|entry| !entry.production.is_empty()) {
            continue;
        }
        let loaders = entry
            .map(|entry| entry.test.iter().cloned().collect::<Vec<_>>().join(", "))
            .unwrap_or_default();
        failures.push(if loaders.is_empty() {
            format!("{key} (not loaded by any Rust source)")
        } else {
            format!("{key} (loaded only by test code: {loaders})")
        });
    }
    failures
}

/// Maps each `.sql` file under the Project source root to the Rust files that load it with
/// `include_str!`, split into production loaders and test-only loaders. A loader is test-only
/// when its file is a `#[cfg(test)]` module (the content hash's own scanner decides which), is
/// under `crates/project/tests`, or the `include_str!` sits inside an item, statement,
/// expression, or other node marked `#[cfg(test)]`.
fn project_sql_inventory(root: &Path) -> Inventory {
    let cfg_test_modules = crate::source_paths::cfg_test_sources(root, &[PROJECT_SOURCE_ROOT])
        .expect("cfg(test) module scan must succeed");
    let mut rust_files = Vec::new();
    collect_files(&root.join(PROJECT_SOURCE_ROOT), "rs", &mut rust_files);
    collect_files(&root.join("crates/project/tests"), "rs", &mut rust_files);

    let mut inventory = Inventory::default();
    for path in rust_files {
        let key = relative_key(root, &path);
        let test_file =
            cfg_test_modules.contains(&key) || !key.starts_with(&format!("{PROJECT_SOURCE_ROOT}/"));
        let source = fs::read_to_string(&path).expect("Project source must be readable");
        let scan = match scan_includes(&source) {
            Ok(scan) => scan,
            Err(error) => {
                inventory
                    .unparsable
                    .push(format!("{key}: does not parse as Rust: {error}"));
                continue;
            }
        };
        for site in scan.unsupported {
            inventory.unsupported.push((
                unsupported_message(&key, site.line),
                test_file || site.gated,
            ));
        }
        for (literal, gated) in scan.sites {
            if !literal.ends_with(".sql") {
                continue;
            }
            let target = normalize(&path.parent().expect("file has a parent").join(&literal));
            let target_key = relative_key(root, &target);
            let entry = inventory.references.entry(target_key).or_default();
            if test_file || gated {
                entry.test.insert(key.clone());
            } else {
                entry.production.insert(key.clone());
            }
        }
    }
    inventory
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
    scan_includes(source)
        .expect("fixture must parse as Rust")
        .sites
}

/// Parses `source` as a Rust file and returns each `include_str!` path, decoded as the compiler
/// decodes it, with whether it sits under a `#[cfg(test)]` attribute, plus every `include_str!`
/// whose argument is not exactly one string literal. Includes nested in the bodies of other
/// macros are found at token level.
fn scan_includes(source: &str) -> syn::Result<IncludeScan> {
    // The compiler reads CRLF line endings as LF before it tokenizes.
    let file = syn::parse_file(&source.replace("\r\n", "\n"))?;
    let mut visitor = IncludeVisitor::default();
    visitor.visit_file(&file);
    Ok(visitor.scan)
}

#[derive(Default)]
struct IncludeVisitor {
    gated: usize,
    scan: IncludeScan,
}

impl IncludeVisitor {
    fn within(&mut self, attrs: &[Attribute], visit: impl FnOnce(&mut Self)) {
        let gated = attrs.iter().any(is_cfg_test);
        self.gated += usize::from(gated);
        visit(self);
        self.gated -= usize::from(gated);
    }

    fn record(&mut self, line: usize, tokens: TokenStream) {
        let gated = self.gated > 0;
        match include_path(tokens) {
            Some(path) => self.scan.sites.push((path, gated)),
            None => self.scan.unsupported.push(UnsupportedSite { line, gated }),
        }
    }

    /// Finds `include_str ! (…)` (optionally `std::`, `core::`, or `::`-qualified) inside the
    /// token body of another macro, and recurses into every group.
    fn scan_tokens(&mut self, tokens: TokenStream) {
        let tokens = tokens.into_iter().collect::<Vec<_>>();
        for (index, token) in tokens.iter().enumerate() {
            match token {
                TokenTree::Ident(ident) if ident == "include_str" => {
                    let bang = matches!(
                        tokens.get(index + 1),
                        Some(TokenTree::Punct(punct)) if punct.as_char() == '!'
                    );
                    if let (true, Some(TokenTree::Group(group))) = (bang, tokens.get(index + 2))
                        && builtin_qualifier(&tokens[..index])
                    {
                        self.record(ident.span().start().line, group.stream());
                    }
                }
                TokenTree::Group(group) => self.scan_tokens(group.stream()),
                _ => {}
            }
        }
    }
}

impl<'ast> Visit<'ast> for IncludeVisitor {
    fn visit_item(&mut self, node: &'ast Item) {
        self.within(item_attrs(node), |this| visit::visit_item(this, node));
    }

    fn visit_impl_item(&mut self, node: &'ast ImplItem) {
        let attrs: &[Attribute] = match node {
            ImplItem::Const(item) => &item.attrs,
            ImplItem::Fn(item) => &item.attrs,
            ImplItem::Type(item) => &item.attrs,
            ImplItem::Macro(item) => &item.attrs,
            _ => &[],
        };
        self.within(attrs, |this| visit::visit_impl_item(this, node));
    }

    fn visit_trait_item(&mut self, node: &'ast TraitItem) {
        let attrs: &[Attribute] = match node {
            TraitItem::Const(item) => &item.attrs,
            TraitItem::Fn(item) => &item.attrs,
            TraitItem::Type(item) => &item.attrs,
            TraitItem::Macro(item) => &item.attrs,
            _ => &[],
        };
        self.within(attrs, |this| visit::visit_trait_item(this, node));
    }

    fn visit_local(&mut self, node: &'ast Local) {
        self.within(&node.attrs, |this| visit::visit_local(this, node));
    }

    fn visit_stmt_macro(&mut self, node: &'ast StmtMacro) {
        self.within(&node.attrs, |this| visit::visit_stmt_macro(this, node));
    }

    fn visit_expr(&mut self, node: &'ast Expr) {
        self.within(expr_attrs(node), |this| visit::visit_expr(this, node));
    }

    fn visit_arm(&mut self, node: &'ast Arm) {
        self.within(&node.attrs, |this| visit::visit_arm(this, node));
    }

    fn visit_field(&mut self, node: &'ast Field) {
        self.within(&node.attrs, |this| visit::visit_field(this, node));
    }

    fn visit_field_value(&mut self, node: &'ast FieldValue) {
        self.within(&node.attrs, |this| visit::visit_field_value(this, node));
    }

    fn visit_variant(&mut self, node: &'ast Variant) {
        self.within(&node.attrs, |this| visit::visit_variant(this, node));
    }

    fn visit_macro(&mut self, node: &'ast Macro) {
        if is_include_str(&node.path) {
            let line = node
                .path
                .segments
                .last()
                .map_or(0, |segment| segment.ident.span().start().line);
            self.record(line, node.tokens.clone());
        } else {
            self.scan_tokens(node.tokens.clone());
        }
    }
}

/// Decodes a macro body that is exactly one string literal, optionally followed by a comma.
fn include_path(tokens: TokenStream) -> Option<String> {
    let parser = |input: ParseStream| {
        let path: LitStr = input.parse()?;
        if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
        }
        Ok(path)
    };
    parser.parse2(tokens).ok().map(|path| path.value())
}

/// `include_str`, `std::include_str`, `core::include_str`, each optionally with a leading `::`.
fn is_include_str(path: &syn::Path) -> bool {
    let segments = path
        .segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .collect::<Vec<_>>();
    match segments.as_slice() {
        [name] => name == "include_str",
        [krate, name] => (krate == "std" || krate == "core") && name == "include_str",
        _ => false,
    }
}

/// True when the tokens before an `include_str` ident leave it unqualified or qualified by
/// `std::`/`core::` (with an optional leading `::`).
fn builtin_qualifier(before: &[TokenTree]) -> bool {
    let is_colon = |token: Option<&TokenTree>| matches!(token, Some(TokenTree::Punct(punct)) if punct.as_char() == ':');
    let count = before.len();
    if !(count >= 2 && is_colon(before.get(count - 1)) && is_colon(before.get(count - 2))) {
        return true;
    }
    matches!(
        count.checked_sub(3).and_then(|index| before.get(index)),
        Some(TokenTree::Ident(krate)) if krate == "std" || krate == "core"
    )
}

fn is_cfg_test(attr: &Attribute) -> bool {
    match &attr.meta {
        Meta::List(list) => list.path.is_ident("cfg") && list.tokens.to_string() == "test",
        _ => false,
    }
}

fn item_attrs(item: &Item) -> &[Attribute] {
    match item {
        Item::Const(item) => &item.attrs,
        Item::Enum(item) => &item.attrs,
        Item::ExternCrate(item) => &item.attrs,
        Item::Fn(item) => &item.attrs,
        Item::ForeignMod(item) => &item.attrs,
        Item::Impl(item) => &item.attrs,
        Item::Macro(item) => &item.attrs,
        Item::Mod(item) => &item.attrs,
        Item::Static(item) => &item.attrs,
        Item::Struct(item) => &item.attrs,
        Item::Trait(item) => &item.attrs,
        Item::TraitAlias(item) => &item.attrs,
        Item::Type(item) => &item.attrs,
        Item::Union(item) => &item.attrs,
        Item::Use(item) => &item.attrs,
        _ => &[],
    }
}

fn expr_attrs(expr: &Expr) -> &[Attribute] {
    match expr {
        Expr::Array(expr) => &expr.attrs,
        Expr::Assign(expr) => &expr.attrs,
        Expr::Async(expr) => &expr.attrs,
        Expr::Await(expr) => &expr.attrs,
        Expr::Binary(expr) => &expr.attrs,
        Expr::Block(expr) => &expr.attrs,
        Expr::Break(expr) => &expr.attrs,
        Expr::Call(expr) => &expr.attrs,
        Expr::Cast(expr) => &expr.attrs,
        Expr::Closure(expr) => &expr.attrs,
        Expr::Const(expr) => &expr.attrs,
        Expr::Continue(expr) => &expr.attrs,
        Expr::Field(expr) => &expr.attrs,
        Expr::ForLoop(expr) => &expr.attrs,
        Expr::Group(expr) => &expr.attrs,
        Expr::If(expr) => &expr.attrs,
        Expr::Index(expr) => &expr.attrs,
        Expr::Infer(expr) => &expr.attrs,
        Expr::Let(expr) => &expr.attrs,
        Expr::Lit(expr) => &expr.attrs,
        Expr::Loop(expr) => &expr.attrs,
        Expr::Macro(expr) => &expr.attrs,
        Expr::Match(expr) => &expr.attrs,
        Expr::MethodCall(expr) => &expr.attrs,
        Expr::Paren(expr) => &expr.attrs,
        Expr::Path(expr) => &expr.attrs,
        Expr::Range(expr) => &expr.attrs,
        Expr::RawAddr(expr) => &expr.attrs,
        Expr::Reference(expr) => &expr.attrs,
        Expr::Repeat(expr) => &expr.attrs,
        Expr::Return(expr) => &expr.attrs,
        Expr::Struct(expr) => &expr.attrs,
        Expr::Try(expr) => &expr.attrs,
        Expr::TryBlock(expr) => &expr.attrs,
        Expr::Tuple(expr) => &expr.attrs,
        Expr::Unary(expr) => &expr.attrs,
        Expr::Unsafe(expr) => &expr.attrs,
        Expr::While(expr) => &expr.attrs,
        Expr::Yield(expr) => &expr.attrs,
        _ => &[],
    }
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
