#[cfg(unix)]
pub(crate) mod profiling;
#[macro_use]
pub(crate) mod setup;
pub(crate) mod signal_handler;

/// Returns the tikv version information.
pub fn tikv_version_info() -> String {
    let fallback = "Unknown (env var does not exist when building)";
    format!(
        "\nRelease Version:   {}\
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
pub fn print_tikv_info() {
    info!("Welcome to TiKV. {}", tikv_version_info());
}
