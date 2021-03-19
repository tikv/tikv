// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

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
    ($plugin_type:ty) => {
        #[global_allocator]
        static HOST_ALLOCATOR: HostAllocator = HostAllocator::new();

        #[no_mangle]
        pub unsafe extern "C" fn _plugin_create(
            host_allocator: $crate::allocator::HostAllocatorPtr,
        ) -> *mut $crate::CoprocessorPlugin {
            HOST_ALLOCATOR.set_allocator(host_allocator);

            let object = <$plugin_type>::create();
            let boxed: Box<dyn $crate::CoprocessorPlugin> = Box::new(object);
            Box::into_raw(boxed)
        }
    };
}
