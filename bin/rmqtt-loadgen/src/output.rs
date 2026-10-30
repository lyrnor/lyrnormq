//! Report and optional raw sample writers; call only after the async load runtime stops.
use crate::{
    LoadError,
    metrics::Report,
    model::{RunConfig, RunData},
};
use serde::Serialize;
use std::{
    fs::File,
    io::{BufWriter, Write},
    time::{SystemTime, UNIX_EPOCH},
};
#[derive(Serialize)]
pub struct Environment {
    tool_version: &'static str,
    build_profile: &'static str,
    git_revision: &'static str,
    code_tree_dirty: &'static str,
    os: &'static str,
    architecture: &'static str,
    available_parallelism: Option<usize>,
    started_unix_ms: Option<u128>,
    arguments: Vec<String>,
    resource_sampling: &'static str,
}
pub fn environment(started: SystemTime) -> Environment {
    Environment {
        tool_version: env!("CARGO_PKG_VERSION"),
        build_profile: if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        git_revision: option_env!("LOADGEN_GIT_REVISION").unwrap_or("unknown"),
        code_tree_dirty: option_env!("LOADGEN_CODE_DIRTY").unwrap_or("unknown"),
        os: std::env::consts::OS,
        architecture: std::env::consts::ARCH,
        available_parallelism: std::thread::available_parallelism().ok().map(usize::from),
        started_unix_ms: started
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|d| d.as_millis()),
        arguments: std::env::args().collect(),
        resource_sampling: "not implicit; attach external process CPU/RSS samples using run timestamps",
    }
}
fn json_error(e: serde_json::Error) -> LoadError {
    LoadError::InvalidData(format!("JSON output: {e}"))
}
pub fn write_raw(file: File, data: &RunData) -> Result<(), LoadError> {
    let mut writer = BufWriter::new(file);
    for pair in &data.pairs {
        for sample in &pair.samples {
            serde_json::to_writer(&mut writer, sample).map_err(json_error)?;
            writer.write_all(b"\n")?;
        }
    }
    writer.flush()?;
    Ok(())
}
pub fn write_report(
    file: &mut File,
    report: &Report,
    environment: &Environment,
) -> Result<(), LoadError> {
    #[derive(Serialize)]
    struct Output<'a> {
        #[serde(flatten)]
        report: &'a Report,
        environment: &'a Environment,
    }
    serde_json::to_writer_pretty(
        &mut *file,
        &Output {
            report,
            environment,
        },
    )
    .map_err(json_error)?;
    file.write_all(b"\n")?;
    file.flush()?;
    Ok(())
}
pub fn write_failure(
    file: &mut File,
    phase: &str,
    error: &LoadError,
    config: &RunConfig,
    environment: &Environment,
) -> Result<(), LoadError> {
    serde_json::to_writer_pretty(&mut *file,&serde_json::json!({"schema_version":1,"success":false,"phase":phase,"error":error.to_string(),"config":config,"environment":environment})).map_err(json_error)?;
    file.write_all(b"\n")?;
    file.flush()?;
    Ok(())
}
