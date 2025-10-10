use anyhow::Context;
use diesel::{
    ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper,
};
use diesel_async::RunQueryDsl;
use orm::schema::{chain_state, tx};
use orm::tx::TxDb;

use crate::appstate::AppState;

#[derive(Clone)]
pub struct TxRepository {
    pub(crate) app_state: AppState,
}

pub trait TxRepositoryTrait {
    fn new(app_state: AppState) -> Self;
    async fn get_txs(
        &self,
        from_block_height: i32,
        to_block_height: i32,
    ) -> anyhow::Result<Vec<TxDb>>;
}

impl TxRepositoryTrait for TxRepository {
    fn new(app_state: AppState) -> Self {
        Self { app_state }
    }

    async fn get_txs(
        &self,
        from_block_height: i32,
        to_block_height: i32,
    ) -> anyhow::Result<Vec<TxDb>> {
        let mut conn = self.app_state.get_db_connection().await?;

        let latest_block_height: i32 = chain_state::table
            .select(chain_state::dsl::block_height)
            .get_result(&mut conn)
            .await
            .optional()
            .with_context(|| {
                "Failed to get the latest block height from the database"
            })?
            .unwrap_or_default();

        if latest_block_height < to_block_height {
            anyhow::bail!(
                "Height range {from_block_height} -- {to_block_height} of the \
                 requested txs exceeds latest block height \
                 ({latest_block_height})"
            );
        }

        let transactions = tx::table
            .filter(
                tx::dsl::block_height
                    .between(from_block_height, to_block_height),
            )
            .order_by((
                tx::dsl::block_height.asc(),
                tx::dsl::block_index.asc(),
                tx::dsl::masp_tx_index.asc(),
            ))
            .select(TxDb::as_select())
            .get_results(&mut conn)
            .await
            .with_context(|| {
                format!(
                    "Failed to get transations from the database in the \
                     height range {from_block_height}-{to_block_height}"
                )
            })?;

        Ok(transactions)
    }
}
