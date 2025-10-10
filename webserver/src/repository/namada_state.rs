use anyhow::Context;
use diesel::dsl::max;
use diesel::{OptionalExtension, QueryDsl, SelectableHelper};
use diesel_async::RunQueryDsl;
use orm::block_index::BlockIndex;
use orm::schema::{block_index, chain_state};
use shared::height::BlockHeight;
use xorf::BinaryFuse16;

use crate::appstate::AppState;

#[derive(Clone)]
pub struct NamadaStateRepository {
    pub(crate) app_state: AppState,
}

pub trait NamadaStateRepositoryTrait {
    fn new(app_state: AppState) -> Self;
    async fn get_latest_height(&self) -> anyhow::Result<Option<BlockHeight>>;
    async fn get_block_index(
        &self,
    ) -> anyhow::Result<Option<(i32, BinaryFuse16)>>;
}

impl NamadaStateRepositoryTrait for NamadaStateRepository {
    fn new(app_state: AppState) -> Self {
        Self { app_state }
    }

    async fn get_latest_height(&self) -> anyhow::Result<Option<BlockHeight>> {
        let mut conn = self
            .app_state
            .get_db_connection()
            .await
            .context("Failed to get DB connection")?;

        let max_height: Option<i32> = chain_state::table
            .select(max(chain_state::dsl::block_height))
            .first(&mut conn)
            .await
            .context("Query failed to get latest block height")?;

        Ok(max_height.map(BlockHeight::from))
    }

    async fn get_block_index(
        &self,
    ) -> anyhow::Result<Option<(i32, BinaryFuse16)>> {
        let mut conn = self
            .app_state
            .get_db_connection()
            .await
            .context("Failed to get DB connection")?;

        let Some(index) = block_index::table
            .select(BlockIndex::as_select())
            .first::<BlockIndex>(&mut conn)
            .await
            .optional()
            .context("Query failed to get latest block index")?
        else {
            return Ok(None);
        };

        let deserialized_filter = tokio::task::spawn_blocking(move || {
            bincode::deserialize::<BinaryFuse16>(&index.serialized_data)
        })
        .await
        .context("Task for block index deserialization panicked")?
        .context("Failed to deserialize block index data")?;

        Ok(Some((index.block_height, deserialized_filter)))
    }
}
