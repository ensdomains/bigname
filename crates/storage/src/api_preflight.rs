use anyhow::{Context, Result};
use sqlx::{PgPool, Row};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ApiLookupDdlKind {
    Relation,
    Function,
}

impl ApiLookupDdlKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Relation => "relation",
            Self::Function => "function",
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ApiLookupDdlObject {
    pub kind: ApiLookupDdlKind,
    pub identity: String,
}

pub async fn load_missing_api_lookup_ddl(pool: &PgPool) -> Result<Vec<ApiLookupDdlObject>> {
    let rows = sqlx::query(
        r#"
        WITH required(kind, identity) AS (
            VALUES
                ('relation', 'bigname_phase.chain_heads'),
                ('relation', 'bigname_phase.chain_lineage'),
                ('relation', 'bigname_phase.chain_phase_state'),
                ('relation', 'bigname_phase.name_current'),
                ('relation', 'bigname_phase.name_surfaces'),
                ('relation', 'bigname_phase.resources'),
                ('relation', 'bigname_phase.surface_bindings'),
                ('relation', 'bigname_phase.token_lineages'),
                ('relation', 'bigname_phase.record_inventory_current'),
                ('relation', 'bigname_phase.manifest_versions'),
                ('relation', 'bigname_phase.manifest_contract_instances'),
                ('relation', 'bigname_phase.resolution_divergences'),
                (
                    'function',
                    'bigname_phase.revalidate_resolution_lookup_state(text,bigint,text,jsonb,jsonb,uuid,text,text)'
                ),
                (
                    'function',
                    'bigname_phase.write_resolution_divergence(uuid,text,text,text,bigint,text,jsonb,text,text,text,text,jsonb,jsonb,boolean)'
                )
        )
        SELECT kind, identity
        FROM required
        WHERE CASE kind
            WHEN 'relation' THEN to_regclass(identity) IS NULL
            WHEN 'function' THEN to_regprocedure(identity) IS NULL
        END
        ORDER BY kind, identity
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to inspect required API lookup DDL")?;

    rows.into_iter()
        .map(|row| {
            let kind = match row.try_get::<&str, _>("kind")? {
                "relation" => ApiLookupDdlKind::Relation,
                "function" => ApiLookupDdlKind::Function,
                unexpected => {
                    return Err(anyhow::anyhow!(
                        "unexpected API lookup DDL kind {unexpected}"
                    ));
                }
            };
            Ok(ApiLookupDdlObject {
                kind,
                identity: row.try_get("identity")?,
            })
        })
        .collect()
}
