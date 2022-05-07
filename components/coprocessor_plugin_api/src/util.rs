// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::{allocator::HostAllocatorPtr, plugin_api::CoprocessorPlugin};

/// Name of the exported constructor with signature [`PluginConstructorSignature`] for the plugin.
pub static PLUGIN_CONSTRUCTOR_SYMBOL: &[u8] = b"_plugin_create";
/// Name of the exported function with signature [`PluginGetBuildInfoSignature`] to get build
/// information about the plugin.
pub static PLUGIN_GET_BUILD_INFO_SYMBOL: &[u8] = b"_plugin_get_build_info";
/// Name of the exported function with signature [`PluginGetPluginInfoSignature`] to get some
/// information about the plugin.
pub static PLUGIN_GET_PLUGIN_INFO_SYMBOL: &[u8] = b"_plugin_get_plugin_info";

/// Type signature of the exported function with symbol [`PLUGIN_CONSTRUCTOR_SYMBOL`].
pub type PluginConstructorSignature =
    unsafe fn(host_allocator: HostAllocatorPtr) -> *mut dyn CoprocessorPlugin;

/// Type signature of the exported function with symbol [`PLUGIN_GET_BUILD_INFO_SYMBOL`].
pub type PluginGetBuildInfoSignature = extern "C" fn() -> BuildInfo;

/// Type signature of the exported function with symbol [`PLUGIN_GET_PLUGIN_INFO_SYMBOL`].
pub type PluginGetPluginInfoSignature = extern "C" fn() -> PluginInfo;

/// Automatically collected build information about the plugin that is exposed from the library.
///
/// Will be automatically created when using [`declare_plugin!(...)`](declare_plugin) and will be
/// used by TiKV when a plugin is loaded to determine whether there are compilation mismatches.
#[repr(C)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildInfo {
    /// Version of the [`coprocessor_plugin_api`](crate) crate that was used to compile this plugin.
    pub api_version: &'static str,
    /// Target triple for which platform this plugin was compiled.
    pub target: &'static str,
    /// Version of the Rust compiler that was used for compilation.
    pub rustc: &'static str,
}

impl BuildInfo {
    pub const fn get() -> Self {
        Self {
            api_version: env!("API_VERSION"),
            target: env!("TARGET"),
            rustc: env!("RUSTC_VERSION"),
        }
    }
}

/// Information about the plugin, like its name and version.
#[repr(C)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginInfo {
    /// The name of the plugin.
    pub name: &'static str,
    /// The version string of the plugin. Should follow semantic versioning.
    pub version: &'static str,
}

/// Declare a plugin for the library so that it can be loaded by TiKV.
///
/// The macro has three different versions:
/// * `declare_plugin!(plugin_name, plugin_version, plugin_ctor)` which gives you full control.
/// * `declare_plugin!(plugin_name, plugin_ctor)` automatically fetches the version from `Cargo.toml`.
/// * `declare_plugin!(plugin_ctor)` automatically fetches plugin name and version from `Cargo.toml`.
///
/// The types of `plugin_name` and `plugin_version` have to be `&'static str` literals.
///
/// # Notes
/// This works by automatically generating an `extern "C"` function with a
/// pre-defined signature and symbol name. Therefore you will only be able to
/// declare one plugin per library.
///
/// Further, it sets the `#[global_allocator]` of the plugin to use the hosts
/// allocator. This makes passing owned data between TiKV and plugin easier
/// but at the cost of not being able to use a custom allocator.
#[macro_export]
macro_rules! declare_plugin {
    ($plugin_ctor:expr) => {
        declare_plugin!(
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION"),
            $plugin_ctor
        );
    };
    ($plugin_name:expr, $plugin_ctor:expr) => {
        declare_plugin!($plugin_name, env!("CARGO_PKG_VERSION"), $plugin_ctor);
    };
    ($plugin_name:expr, $plugin_version:expr, $plugin_ctor:expr) => {
        #[cfg(not(test))]
        #[global_allocator]
        static HOST_ALLOCATOR: $crate::allocator::HostAllocator =
            $crate::allocator::HostAllocator::new();

        #[no_mangle]
        pub unsafe extern "C" fn _plugin_get_build_info() -> $crate::util::BuildInfo {
            $crate::util::BuildInfo::get()
        }

        #[no_mangle]
        pub unsafe extern "C" fn _plugin_get_plugin_info() -> $crate::util::PluginInfo {
            $crate::util::PluginInfo {
                name: $plugin_name,
                version: $plugin_version,
            }
        }

        #[no_mangle]
        pub unsafe extern "C" fn _plugin_create(
            host_allocator: $crate::allocator::HostAllocatorPtr,
        ) -> *mut $crate::CoprocessorPlugin {
            #[cfg(not(test))]
            HOST_ALLOCATOR.set_allocator(host_allocator);

            let boxed: Box<dyn $crate::CoprocessorPlugin> = Box::new($plugin_ctor);
            Box::into_raw(boxed)
        }
    };
}

/// Transforms the name of a package into the name of the compiled library.
///
/// The result of the function can be used to correctly locate build artifacts of `dylib` on
/// different platforms.
///
/// The name of the `dylib` is
/// * `lib<pkgname>.so` on Linux
/// * `lib<pkgname>.dylib` on MaxOS
/// * `lib<pkgname>.dll` on Windows
///
/// See also <https://doc.rust-lang.org/reference/linkage.html>
///
/// *Note: Depending on artifacts of other crates will be easier with
/// [this RFC](https://github.com/rust-lang/cargo/issues/9096).*
pub fn pkgname_to_libname(pkgname: &str) -> String {
    let pkgname = pkgname.to_string().replace('-', "_");
    if cfg!(target_os = "windows") {
        format!("{}.dll", pkgname)
    } else if cfg!(target_os = "macos") {
        format!("lib{}.dylib", pkgname)
    } else {
        format!("lib{}.so", pkgname)
    }
}
