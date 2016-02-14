use log::LogLevelFilter;
pub fn get_level_by_string(lv: &str) -> LogLevelFilter {
    match &*lv.to_string().to_lowercase() {
        "trace" => LogLevelFilter::Trace,
        "debug" => LogLevelFilter::Debug,
        "info" => LogLevelFilter::Info,
        "warn" => LogLevelFilter::Warn,
        "error" => LogLevelFilter::Error,
        "off" => LogLevelFilter::Off,
        _ => LogLevelFilter::Info,
    }
}
