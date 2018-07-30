#[cfg(unix)]
pub(crate) mod profiling;
#[macro_use]
pub(crate) mod setup;
pub(crate) mod signal_handler;

/// Returns the tikv version information.
pub fn tikv_version_info() -> String {
    format!(
        "\nRelease Version:   {}\
         \nGit Commit Hash:   {}\
         \nGit Commit Branch: {}\
         \nUTC Build Time:    {}\
         \nRust Version:      {}",
        env!("CARGO_PKG_VERSION"),
        env!("TIKV_BUILD_TIME"),
        env!("TIKV_BUILD_GIT_HASH"),
        env!("TIKV_BUILD_GIT_BRANCH"),
        env!("TIKV_BUILD_RUSTC_VERSION"),
    )
}

/// Prints the tikv version information to the standard output.
pub fn print_tikv_info() {
    info!("Welcome to TiKV. {}", tikv_version_info());
}
