use anyhow::Context;
use diesel::{
    ExpressionMethods, OptionalExtension, QueryDsl, RunQueryDsl,
    SelectableHelper,
};
use orm::schema::{commitment_tree, notes_index};
use orm::tree::TreeDb;
use shared::error::ContextDbInteractError;

use crate::appstate::AppState;

#[derive(Clone)]
pub struct TreeRepository {
    pub(crate) app_state: AppState,
}

pub trait TreeRepositoryTrait {
    fn new(app_state: AppState) -> Self;
    async fn get_at_height(
        &self,
        block_height: i32,
    ) -> anyhow::Result<Option<TreeDb>>;
}

impl TreeRepositoryTrait for TreeRepository {
    fn new(app_state: AppState) -> Self {
        Self { app_state }
    }

    async fn get_at_height(
        &self,
        block_height: i32,
    ) -> anyhow::Result<Option<TreeDb>> {
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
                    .first::<i32>(conn)
                    .optional()
                    .with_context(|| {
                        format!(
                            "Failed to fetch height from the db closest to \
                             the provided height {block_height}"
                        )
                    })?
                else {
                    return anyhow::Ok(None);
                };

                let tree = commitment_tree::table
                    .filter(
                        commitment_tree::dsl::block_height.eq(closest_height),
                    )
                    .select(TreeDb::as_select())
                    .first(conn)
                    .with_context(|| {
                        format!(
                            "Failed to fetch commitment tree from the db at \
                             height {closest_height} (the closest to the \
                             provided height {block_height})"
                        )
                    })?;

                anyhow::Ok(Some(tree))
            })
        })
        .await
        .context_db_interact_error()?
    }
}
