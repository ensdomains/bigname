use std::{future::Future, pin::Pin};

use anyhow::Result;
use sqlx::PgPool;

pub(crate) type StartupAdapterProgressFuture<'a> =
    Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

pub(crate) trait StartupAdapterProgress: Send {
    fn record<'a>(&'a mut self, pool: &'a PgPool) -> StartupAdapterProgressFuture<'a>;
}
