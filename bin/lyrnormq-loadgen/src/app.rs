//! Synchronous CLI boundary: filesystem IO and sorting never run on load workers.
use crate::{LoadError, config, engine, metrics, output};
use std::{fs::OpenOptions, io::Write, time::SystemTime};

pub fn execute(args: impl IntoIterator<Item = String>) -> Result<bool, LoadError> {
    let Some(options) = config::parse(args)? else {
        println!("{}", config::USAGE);
        return Ok(true);
    };
    let started = SystemTime::now();
    // Reserve both outputs before opening network connections. Existing files are never replaced.
    let mut report_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&options.output)?;
    let raw_file = match options
        .raw
        .as_ref()
        .map(|path| OpenOptions::new().write(true).create_new(true).open(path))
        .transpose()
    {
        Ok(file) => file,
        Err(error) => {
            let error = LoadError::Io(error);
            output::write_failure(
                &mut report_file,
                "output_reservation",
                &error,
                &options.config,
                &output::environment(started),
            )?;
            return Err(error);
        }
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let error = LoadError::Io(error);
            output::write_failure(
                &mut report_file,
                "runtime_initialization",
                &error,
                &options.config,
                &output::environment(started),
            )?;
            return Err(error);
        }
    };
    eprintln!(
        "lyrnormq-loadgen: {} pairs / {} TCP connections; QoS{}; {} bytes; {}",
        options.config.pairs,
        options.config.pairs * 2,
        options.config.qos,
        options.config.payload_bytes,
        if options.config.rate == 0 {
            "closed-loop"
        } else {
            "fixed absolute schedule"
        }
    );
    let result = runtime.block_on(engine::run(options.config.clone()));
    drop(runtime);
    let environment = output::environment(started);
    match result {
        Ok(data) => {
            if let Some(file) = raw_file {
                output::write_raw(file, &data)?;
            }
            match metrics::summarize(&data) {
                Ok(report) => {
                    let success = report.success;
                    output::write_report(&mut report_file, &report, &environment)?;
                    println!("report: {} (success={success})", options.output.display());
                    Ok(success)
                }
                Err(error) => {
                    output::write_failure(
                        &mut report_file,
                        "instrumentation_validation",
                        &error,
                        &options.config,
                        &environment,
                    )?;
                    Err(error)
                }
            }
        }
        Err(error) => {
            output::write_failure(
                &mut report_file,
                "setup_or_engine",
                &error,
                &options.config,
                &environment,
            )?;
            report_file.flush()?;
            Err(error)
        }
    }
}
