use anyhow::Context;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use diesel_async::RunQueryDsl;
use orm::notes_index::NotesIndexDb;
use orm::schema::notes_index;

use crate::appstate::AppState;

#[derive(Clone)]
pub struct NotesIndexRepository {
    pub(crate) app_state: AppState,
}

pub trait NotesIndexRepositoryTrait {
    fn new(app_state: AppState) -> Self;
    async fn get_notes_index(
        &self,
        block_height: i32,
    ) -> anyhow::Result<Vec<NotesIndexDb>>;
}

impl NotesIndexRepositoryTrait for NotesIndexRepository {
    fn new(app_state: AppState) -> Self {
        Self { app_state }
    }

    async fn get_notes_index(
        &self,
        block_height: i32,
    ) -> anyhow::Result<Vec<NotesIndexDb>> {
        let mut conn = self.app_state.get_db_connection().await?;

        let notes = notes_index::table
            .filter(notes_index::dsl::block_height.le(block_height))
            .select(NotesIndexDb::as_select())
            .get_results(&mut conn)
            .await
            .with_context(|| {
                format!(
                    "Failed to retrieve note indices up to block height \
                     {block_height} from db"
                )
            })?;

        Ok(notes)
    }
}
