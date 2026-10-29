//! Network engine implementation is assigned to the isolated engine worktree.
use crate::{
    LoadError,
    model::{RunConfig, RunData},
};
pub async fn run(_config: RunConfig) -> Result<RunData, LoadError> {
    Err(LoadError::Config("engine implementation pending".into()))
}
