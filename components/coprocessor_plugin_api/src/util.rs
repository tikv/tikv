// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::allocator::HostAllocatorPtr;
use super::plugin_api::CoprocessorPlugin;

/// Name of the exported constructor function for the plugin in the `dylib`.
pub static PLUGIN_CONSTRUCTOR_SYMBOL: &[u8] = b"_plugin_create";
/// Name of the exported function to get build information about the plugin.
pub static PLUGIN_GET_BUILD_INFO_SYMBOL: &[u8] = b"_plugin_get_build_info";

/// Type signature of the exported constructor function for the plugin in the `dylib`.
/// See also [`PLUGIN_CONSTRUCTOR_SYMBOL`].
pub type PluginConstructorSignature =
    unsafe fn(host_allocator: HostAllocatorPtr) -> *mut dyn CoprocessorPlugin;
/// Type signature of the exported function to get build information about the plugin.
/// See also [`PLUGIN_GET_BUILD_INFO_SYMBOL`].
pub type PluginGetBuildInfo = extern "C" fn() -> BuildInfo;

/// Build information about the plugin.
///
/// Will be automatically created when using [`declare_plugin!(...)`](declare_plugin).
#[repr(C)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildInfo {
    pub api_version: &'static str,
    pub target: &'static str,
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

/// Declare a plugin for the library so that it can be loaded by TiKV.
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
        #[cfg(not(test))]
        #[global_allocator]
        static HOST_ALLOCATOR: $crate::allocator::HostAllocator =
            $crate::allocator::HostAllocator::new();

        #[cfg(not(test))]
        #[no_mangle]
        pub unsafe extern "C" fn _plugin_get_build_info() -> $crate::BuildInfo {
            $crate::BuildInfo::get()
        }

        #[no_mangle]
        pub unsafe extern "C" fn _plugin_create(
            host_allocator: $crate::allocator::HostAllocatorPtr,
        ) -> *mut $crate::CoprocessorPlugin {
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
    let pkgname = pkgname.to_string().replace("-", "_");
    if cfg!(target_os = "windows") {
        format!("{}.dll", pkgname)
    } else if cfg!(target_os = "macos") {
        format!("lib{}.dylib", pkgname)
    } else {
        format!("lib{}.so", pkgname)
    }
}
