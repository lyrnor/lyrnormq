//! Exact metric implementation is assigned to the isolated metrics worktree.
use crate::{LoadError, model::RunData};
use serde::Serialize;
#[derive(Debug, Serialize)]
pub struct Report {
    pub success: bool,
}
pub fn summarize(_data: &RunData) -> Result<Report, LoadError> {
    Err(LoadError::Config("metrics implementation pending".into()))
}
