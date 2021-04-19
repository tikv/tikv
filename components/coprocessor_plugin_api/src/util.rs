// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::allocator::HostAllocatorPtr;
use super::plugin_api::CoprocessorPlugin;

/// Name of the exported constructor function for the plugin in the `dylib`.
pub const PLUGIN_CONSTRUCTOR_SYMBOL: &[u8] = b"_plugin_create";
/// Type signature of the exported constructor function for the plugin in the `dylib`.
pub type PluginConstructorSignature =
    unsafe fn(host_allocator: HostAllocatorPtr) -> *mut dyn CoprocessorPlugin;

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
        #[global_allocator]
        static HOST_ALLOCATOR: $crate::allocator::HostAllocator =
            $crate::allocator::HostAllocator::new();

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
