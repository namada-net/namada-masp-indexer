use anyhow::Context;
use diesel::{
    ExpressionMethods, OptionalExtension, QueryDsl, RunQueryDsl,
    SelectableHelper,
};
use orm::schema::{notes_index, witness};
use orm::witness::WitnessDb;
use shared::error::ContextDbInteractError;

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
        let conn = self.app_state.get_db_connection().await.context(
            "Failed to retrieve connection from the pool of database \
             connections",
        )?;

        conn.interact(move |conn| {
            conn.build_transaction().read_only().run(|conn| {
                let Some(closest_height) = notes_index::table
                    .order(notes_index::dsl::block_height.desc())
                    .filter(notes_index::dsl::block_height.le(block_height))
                    .select(notes_index::dsl::block_height)
                    .first(conn)
                    .optional()
                    .with_context(|| {
                        format!(
                            "Failed to fetch height from the db closest to \
                             the provided height {block_height}"
                        )
                    })?
                else {
                    return anyhow::Ok((vec![], block_height));
                };

                let witnesses = witness::table
                    .filter(witness::dsl::block_height.eq(closest_height))
                    .select(WitnessDb::as_select())
                    .get_results::<WitnessDb>(conn)
                    .with_context(|| {
                        format!(
                            "Failed to fetch witnesses from the db at height \
                             {closest_height} (the closest to the provided \
                             height {block_height})"
                        )
                    })?;

                anyhow::Ok((witnesses, closest_height))
            })
        })
        .await
        .context_db_interact_error()?
    }
}
