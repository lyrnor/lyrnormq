use std::process::ExitCode;
fn main() -> ExitCode {
    match rmqtt_loadgen::app::execute(std::env::args().skip(1)) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(2),
        Err(error) => {
            eprintln!("rmqtt-loadgen: {error}");
            ExitCode::from(1)
        }
    }
}
