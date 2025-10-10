use anyhow::Context;
use diesel::{
    ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper,
};
use diesel_async::RunQueryDsl;
use orm::schema::{notes_index, witness};
use orm::witness::WitnessDb;

use crate::appstate::AppState;

#[derive(Clone)]
pub struct WitnessMapRepository {
    pub(crate) app_state: AppState,
}

pub trait WitnessMapRepositoryTrait {
    fn new(app_state: AppState) -> Self;
    async fn get_witnesses(
        &self,
        block_height: i32,
    ) -> anyhow::Result<(Vec<WitnessDb>, i32)>;
}

impl WitnessMapRepositoryTrait for WitnessMapRepository {
    fn new(app_state: AppState) -> Self {
        Self { app_state }
    }

    async fn get_witnesses(
        &self,
        block_height: i32,
    ) -> anyhow::Result<(Vec<WitnessDb>, i32)> {
        let mut conn = self
            .app_state
            .get_db_connection()
            .await
            .context("Failed to get DB connection")?;

        let Some(closest_height) = notes_index::table
            .order(notes_index::dsl::block_height.desc())
            .filter(notes_index::dsl::block_height.le(block_height))
            .select(notes_index::dsl::block_height)
            .first::<i32>(&mut conn)
            .await
            .optional()
            .context("Query failed to find closest block height")?
        else {
            return Ok((Vec::new(), block_height));
        };

        let witnesses = witness::table
            .filter(witness::dsl::block_height.eq(closest_height))
            .select(WitnessDb::as_select())
            .get_results(&mut conn)
            .await
            .with_context(|| {
                format!("Query failed for witnesses at height {closest_height}")
            })?;

        Ok((witnesses, closest_height))
    }
}
