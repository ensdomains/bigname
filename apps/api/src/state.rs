use sqlx::PgPool;

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) phase: &'static str,
    pub(crate) pool: PgPool,
}
