//! Shared PostgreSQL connection policy.

use sqlx::postgres::PgConnectOptions;

use crate::CURRENT_PROJECTION_REPLAY_VERSION;

/// PostgreSQL session setting read by the
/// [projection replay-version fence](../../../docs/glossary.md#projection-replay-version-fence).
pub const PROJECTION_REPLAY_VERSION_SETTING: &str = "bigname.projection_replay_version";

/// Stamp PostgreSQL connection options with this binary's projection replay version.
///
/// Every workspace connection constructor must pass its options through this helper before
/// connecting. Keeping the stamp on the startup packet also covers dedicated connections that do
/// not come from a service pool.
///
pub fn stamp_projection_replay_version(options: PgConnectOptions) -> PgConnectOptions {
    options.options([(
        PROJECTION_REPLAY_VERSION_SETTING,
        CURRENT_PROJECTION_REPLAY_VERSION.to_string(),
    )])
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        fs,
        path::{Path, PathBuf},
    };

    use super::*;
    use syn::{
        Block, Expr, ExprCall, ExprMethodCall, Pat, Stmt,
        visit::{self, Visit},
    };

    #[test]
    fn projection_replay_version_stamp_preserves_existing_startup_options() {
        let options = PgConnectOptions::new().options([("statement_timeout", "250ms")]);
        let stamped = stamp_projection_replay_version(options);
        let startup_options = stamped
            .get_options()
            .expect("stamped options must include PostgreSQL startup settings");

        assert!(startup_options.contains("-c statement_timeout=250ms"));
        assert!(startup_options.contains(&format!(
            "-c {PROJECTION_REPLAY_VERSION_SETTING}={CURRENT_PROJECTION_REPLAY_VERSION}"
        )));
    }

    #[test]
    fn workspace_connection_constructors_use_shared_replay_version_stamp() {
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .expect("storage crate must live under the workspace crates directory");
        let this_file = workspace_root.join("crates/storage/src/connection.rs");
        let mut source_files = Vec::new();
        for directory in ["apps", "crates", "tests"] {
            collect_rust_source_files(&workspace_root.join(directory), &mut source_files);
        }

        let mut missing_stamps = Vec::new();
        for path in source_files {
            if path == this_file {
                continue;
            }
            let source = fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
            missing_stamps.extend(find_unstamped_connection_constructors(&path, &source));
        }

        assert!(
            missing_stamps.is_empty(),
            "PostgreSQL connection constructors bypass the shared projection replay-version \
             stamp:\n{}",
            missing_stamps.join("\n")
        );
    }

    #[test]
    fn constructor_audit_rejects_an_unrelated_nearby_stamp() {
        let source = r#"
            use sqlx::{PgPool, postgres::PgPoolOptions};

            async fn connect(options_a: PgConnectOptions, options_b: PgConnectOptions) {
                let options_a =
                    bigname_storage::stamp_projection_replay_version(options_a);
                PgPoolOptions::new().connect_with(options_a).await.unwrap();
                PgPoolOptions::new().connect_with(options_b).await.unwrap();
            }
        "#;

        let missing = find_unstamped_connection_constructors(Path::new("nearby_stamp.rs"), source);
        assert_eq!(missing.len(), 1, "{missing:#?}");
    }

    fn find_unstamped_connection_constructors(path: &Path, source: &str) -> Vec<String> {
        let is_postgres_source = source.contains("PgPoolOptions")
            || source.contains("PgPool")
            || source.contains("PgConnection");
        if !is_postgres_source {
            return Vec::new();
        }

        let syntax = syn::parse_file(source)
            .unwrap_or_else(|error| panic!("failed to parse {}: {error}", path.display()));
        let mut audit = ConnectionConstructorAudit {
            path,
            scopes: Vec::new(),
            missing_stamps: Vec::new(),
        };
        audit.visit_file(&syntax);
        audit.missing_stamps
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum OptionsStamp {
        Stamped,
        ExplicitTestOptOut,
        Unknown,
    }

    struct ConnectionConstructorAudit<'a> {
        path: &'a Path,
        scopes: Vec<HashMap<String, OptionsStamp>>,
        missing_stamps: Vec<String>,
    }

    impl ConnectionConstructorAudit<'_> {
        fn check_options(&mut self, options: &Expr, line: usize) {
            match self.options_stamp(options) {
                OptionsStamp::Stamped => {}
                OptionsStamp::ExplicitTestOptOut
                    if self
                        .path
                        .ends_with(Path::new("crates/test-support/src/lib.rs")) => {}
                OptionsStamp::ExplicitTestOptOut => self.missing_stamps.push(format!(
                    "{}:{line} uses the test-only unstamped options marker outside test-support",
                    self.path.display()
                )),
                OptionsStamp::Unknown => self.missing_stamps.push(format!(
                    "{}:{line} passes options that are not derived from the shared projection \
                     replay-version stamp",
                    self.path.display()
                )),
            }
        }

        fn options_stamp(&self, expression: &Expr) -> OptionsStamp {
            match expression {
                Expr::Call(call) => match called_function_name(call).as_deref() {
                    Some("stamp_projection_replay_version") => OptionsStamp::Stamped,
                    Some("explicitly_unstamped_projection_replay_version_options_for_test") => {
                        OptionsStamp::ExplicitTestOptOut
                    }
                    _ => OptionsStamp::Unknown,
                },
                Expr::Path(path) => path
                    .path
                    .get_ident()
                    .and_then(|identifier| self.lookup(identifier.to_string().as_str()))
                    .unwrap_or(OptionsStamp::Unknown),
                Expr::Reference(reference) => self.options_stamp(&reference.expr),
                Expr::Paren(paren) => self.options_stamp(&paren.expr),
                Expr::Group(group) => self.options_stamp(&group.expr),
                Expr::Try(try_expression) => self.options_stamp(&try_expression.expr),
                Expr::Await(await_expression) => self.options_stamp(&await_expression.base),
                Expr::MethodCall(call) => self.options_stamp(&call.receiver),
                Expr::Block(block) => self.block_stamp(&block.block),
                Expr::If(if_expression) => {
                    let Some((_, else_expression)) = &if_expression.else_branch else {
                        return OptionsStamp::Unknown;
                    };
                    combine_options_stamps([
                        self.block_stamp(&if_expression.then_branch),
                        self.options_stamp(else_expression),
                    ])
                }
                Expr::Match(match_expression) => combine_options_stamps(
                    match_expression
                        .arms
                        .iter()
                        .map(|arm| self.options_stamp(&arm.body)),
                ),
                _ => OptionsStamp::Unknown,
            }
        }

        fn block_stamp(&self, block: &Block) -> OptionsStamp {
            match block.stmts.last() {
                Some(Stmt::Expr(expression, None)) => self.options_stamp(expression),
                _ => OptionsStamp::Unknown,
            }
        }

        fn lookup(&self, name: &str) -> Option<OptionsStamp> {
            self.scopes
                .iter()
                .rev()
                .find_map(|scope| scope.get(name).copied())
        }

        fn bind_local(&mut self, pattern: &Pat, stamp: OptionsStamp) {
            let Some(name) = local_binding_name(pattern) else {
                return;
            };
            self.scopes
                .last_mut()
                .expect("a local binding must belong to a block")
                .insert(name, stamp);
        }

        fn report_direct_url_constructor(&mut self, line: usize) {
            self.missing_stamps.push(format!(
                "{}:{line} uses a URL connection constructor instead of stamped options",
                self.path.display()
            ));
        }
    }

    impl<'ast> Visit<'ast> for ConnectionConstructorAudit<'_> {
        fn visit_block(&mut self, block: &'ast Block) {
            self.scopes.push(HashMap::new());
            for statement in &block.stmts {
                if let Stmt::Local(local) = statement {
                    let stamp = if let Some(initializer) = &local.init {
                        self.visit_expr(&initializer.expr);
                        if let Some((_, diverging_expression)) = &initializer.diverge {
                            self.visit_expr(diverging_expression);
                        }
                        self.options_stamp(&initializer.expr)
                    } else {
                        OptionsStamp::Unknown
                    };
                    self.bind_local(&local.pat, stamp);
                } else {
                    visit::visit_stmt(self, statement);
                }
            }
            self.scopes.pop();
        }

        fn visit_expr_method_call(&mut self, call: &'ast ExprMethodCall) {
            let method = call.method.to_string();
            let line = call.method.span().start().line;
            match method.as_str() {
                "connect_with" | "connect_lazy_with" => {
                    if let Some(options) = call.args.first() {
                        self.check_options(options, line);
                    } else {
                        self.missing_stamps.push(format!(
                            "{}:{line} connection constructor has no options argument",
                            self.path.display()
                        ));
                    }
                }
                "connect" | "connect_lazy" => self.report_direct_url_constructor(line),
                _ => {}
            }
            visit::visit_expr_method_call(self, call);
        }

        fn visit_expr_call(&mut self, call: &'ast ExprCall) {
            let Some(path) = called_function_path(call) else {
                visit::visit_expr_call(self, call);
                return;
            };
            let Some(function) = path.segments.last() else {
                visit::visit_expr_call(self, call);
                return;
            };
            let is_postgres_constructor = path.segments.iter().any(|segment| {
                segment.ident == "PgConnection"
                    || segment.ident == "PgPool"
                    || segment.ident == "PgPoolOptions"
            });
            if is_postgres_constructor {
                let line = function.ident.span().start().line;
                match function.ident.to_string().as_str() {
                    "connect_with" | "connect_lazy_with" => {
                        if let Some(options) = call.args.first() {
                            self.check_options(options, line);
                        } else {
                            self.missing_stamps.push(format!(
                                "{}:{line} connection constructor has no options argument",
                                self.path.display()
                            ));
                        }
                    }
                    "connect" | "connect_lazy" => self.report_direct_url_constructor(line),
                    _ => {}
                }
            }
            visit::visit_expr_call(self, call);
        }
    }

    fn called_function_path(call: &ExprCall) -> Option<&syn::Path> {
        match call.func.as_ref() {
            Expr::Path(path) => Some(&path.path),
            _ => None,
        }
    }

    fn called_function_name(call: &ExprCall) -> Option<String> {
        called_function_path(call)?
            .segments
            .last()
            .map(|segment| segment.ident.to_string())
    }

    fn local_binding_name(pattern: &Pat) -> Option<String> {
        match pattern {
            Pat::Ident(identifier) => Some(identifier.ident.to_string()),
            Pat::Type(typed) => local_binding_name(&typed.pat),
            _ => None,
        }
    }

    fn combine_options_stamps(stamps: impl IntoIterator<Item = OptionsStamp>) -> OptionsStamp {
        let mut combined = OptionsStamp::Stamped;
        let mut saw_stamp = false;
        for stamp in stamps {
            saw_stamp = true;
            match stamp {
                OptionsStamp::Unknown => return OptionsStamp::Unknown,
                OptionsStamp::ExplicitTestOptOut => {
                    combined = OptionsStamp::ExplicitTestOptOut;
                }
                OptionsStamp::Stamped => {}
            }
        }
        if saw_stamp {
            combined
        } else {
            OptionsStamp::Unknown
        }
    }

    fn collect_rust_source_files(directory: &Path, files: &mut Vec<PathBuf>) {
        let entries = fs::read_dir(directory)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", directory.display()));
        for entry in entries {
            let path = entry
                .unwrap_or_else(|error| {
                    panic!(
                        "failed to read an entry under {}: {error}",
                        directory.display()
                    )
                })
                .path();
            if path.is_dir() {
                if path.file_name().is_some_and(|name| name == "target") {
                    continue;
                }
                collect_rust_source_files(&path, files);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                files.push(path);
            }
        }
    }
}
