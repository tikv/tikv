#[cfg(unix)]
pub(crate) mod profiling;
#[macro_use]
pub(crate) mod setup;
pub(crate) mod signal_handler;

use build_info::build_info;

/// Returns the tikv version information.
pub fn tikv_version_info() -> String {
    let (hash, branch, date, rustc) = build_info!();
    format!(
        "\nRelease Version:   {}\
         \nGit Commit Hash:   {}\
         \nGit Commit Branch: {}\
         \nUTC Build Time:    {}\
         \nRust Version:      {}",
        env!("CARGO_PKG_VERSION"),
        hash,
        branch,
        date,
        rustc,
    )
}

/// Prints the tikv version information to the standard output.
pub fn print_tikv_info() {
    info!("Welcome to TiKV. {}", tikv_version_info());
}
