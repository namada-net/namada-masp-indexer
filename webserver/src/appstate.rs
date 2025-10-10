use std::env;
use std::time::Duration;

use anyhow::Context;
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::deadpool::{Object, Pool};

pub type AsyncDbPool = Pool<AsyncPgConnection>;

#[derive(Clone)]
pub struct AppState {
    db: AsyncDbPool,
}

impl AppState {
    pub async fn new(db_url: String) -> anyhow::Result<Self> {
        let max_pool_size = env::var("DATABASE_POOL_SIZE")
            .unwrap_or_else(|_| 8.to_string())
            .parse::<usize>()
            .unwrap_or(8_usize);

        let max_conn_retries = env::var("DATABASE_MAX_CONN_RETRIES")
            .unwrap_or_else(|_| 5.to_string())
            .parse::<u32>()
            .unwrap_or(5);

        let pool = tryhard::retry_fn(|| async {
            let config = AsyncDieselConnectionManager::<
                diesel_async::AsyncPgConnection,
            >::new(db_url.clone());
            Pool::builder(config).max_size(max_pool_size).build()
        })
        .retries(max_conn_retries)
        .exponential_backoff(Duration::from_millis(100))
        .max_delay(Duration::from_secs(5))
        .await?;

        Ok(Self { db: pool })
    }

    pub async fn get_db_connection(
        &self,
    ) -> anyhow::Result<Object<AsyncPgConnection>> {
        self.db.get().await.context(
            "Failed to get db connection handle from pool of db connections",
        )
    }
}
