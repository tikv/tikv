pub mod server;
#[macro_use]
pub mod setup;
pub mod signal_handler;

/// Returns the tikv version information.
pub fn tikv_version_info() -> String {
    let fallback = "Unknown (env var does not exist when building)";
    format!(
        "Release Version:   {}\
         \nGit Commit Hash:   {}\
         \nGit Commit Branch: {}\
         \nUTC Build Time:    {}\
         \nRust Version:      {}",
        env!("CARGO_PKG_VERSION"),
        option_env!("TIKV_BUILD_GIT_HASH").unwrap_or(fallback),
        option_env!("TIKV_BUILD_GIT_BRANCH").unwrap_or(fallback),
        option_env!("TIKV_BUILD_TIME").unwrap_or(fallback),
        option_env!("TIKV_BUILD_RUSTC_VERSION").unwrap_or(fallback),
    )
}

/// Prints the tikv version information to the standard output.
#[allow(dead_code)]
pub fn log_tikv_info() {
    info!("Welcome to TiKV");
    for line in tikv_version_info().lines() {
        info!("{}", line);
    }
}
