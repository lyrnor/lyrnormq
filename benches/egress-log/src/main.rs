//! Direct synchronous Journal benchmark; the production engine is unmodified.
mod config;
mod metrics;
mod run;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Some(config) = config::Config::parse(std::env::args().skip(1))? else {
        return Ok(());
    };
    let result = run::run(config)?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
