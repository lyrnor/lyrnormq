//! Process-wide subscriber and bounded off-runtime stderr writer.
use std::{env, error::Error, fmt};
use tracing_appender::non_blocking::{NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::{filter::Targets, layer::SubscriberExt, util::SubscriberInitExt};

const DEFAULT_FILTER: &str = "warn,rmqtt=info";
const BUFFERED_LINES: usize = 8192;

pub enum InitError {
    Environment,
    Filter,
    Format,
    Subscriber,
}
impl fmt::Debug for InitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl fmt::Display for InitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Environment => "logging environment variables must be Unicode",
            Self::Filter => {
                "RUST_LOG requires level or target=level directives (maximum 4096 bytes)"
            }
            Self::Format => "RMQTT_LOG_FORMAT must be text or json",
            Self::Subscriber => "could not initialize logging subscriber",
        })
    }
}
impl Error for InitError {}

fn variable(name: &str) -> Result<Option<String>, InitError> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(InitError::Environment),
    }
}

fn configuration(filter: Option<&str>, format: Option<&str>) -> Result<(Targets, bool), InitError> {
    let filter = filter.unwrap_or(DEFAULT_FILTER);
    if filter.is_empty() || filter.len() > 4096 {
        return Err(InitError::Filter);
    }
    let filter = filter.parse().map_err(|_| InitError::Filter)?;
    let json = match format.unwrap_or("text") {
        "text" => false,
        "json" => true,
        _ => return Err(InitError::Format),
    };
    Ok((filter, json))
}

/// Keep the guard outside Tokio's runtime so normal exit flushes off async workers.
/// Overflow deliberately drops logs rather than blocking MQTT tasks.
pub fn init() -> Result<WorkerGuard, InitError> {
    let (filter, json) = configuration(
        variable("RUST_LOG")?.as_deref(),
        variable("RMQTT_LOG_FORMAT")?.as_deref(),
    )?;
    let (writer, guard) = NonBlockingBuilder::default()
        .buffered_lines_limit(BUFFERED_LINES)
        .lossy(true)
        .thread_name("rmqtt-log")
        .finish(std::io::stderr());
    let subscriber = tracing_subscriber::registry().with(filter);
    let layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_target(true)
        .with_writer(writer);
    let initialized = if json {
        subscriber.with(layer.json()).try_init()
    } else {
        subscriber.with(layer).try_init()
    };
    initialized.map_err(|_| InitError::Subscriber)?;
    Ok(guard)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_and_target_filters_have_predictable_levels() {
        let (filter, json) = configuration(None, None).unwrap();
        assert!(!json);
        assert!(filter.would_enable("rmqtt", &tracing::Level::INFO));
        assert!(!filter.would_enable("dependency", &tracing::Level::INFO));
        let (filter, json) = configuration(Some("warn,rmqtt=debug"), Some("json")).unwrap();
        assert!(json);
        assert!(filter.would_enable("rmqtt::admin", &tracing::Level::DEBUG));
    }

    #[test]
    fn invalid_settings_fail_without_echoing_supplied_values() {
        for filter in ["", "rmqtt=not-a-level", &"x".repeat(4097)] {
            let error = configuration(Some(filter), None).unwrap_err();
            assert!(matches!(error, InitError::Filter));
            assert!(!error.to_string().contains("not-a-level"));
        }
        assert!(matches!(
            configuration(None, Some("secret-format")),
            Err(InitError::Format)
        ));
        assert!(!InitError::Format.to_string().contains("secret-format"));
    }
}
