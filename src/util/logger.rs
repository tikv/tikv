use log::LogLevelFilter;
pub fn get_level_by_string(lv: &str) -> LogLevelFilter {
    #![allow(match_same_arms)]
    match &*lv.to_owned().to_lowercase() {
        "trace" => LogLevelFilter::Trace,
        "debug" => LogLevelFilter::Debug,
        "info" => LogLevelFilter::Info,
        "warn" => LogLevelFilter::Warn,
        "error" => LogLevelFilter::Error,
        "off" => LogLevelFilter::Off,
        _ => LogLevelFilter::Info,
    }
}
