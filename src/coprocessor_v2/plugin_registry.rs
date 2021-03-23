// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use coprocessor_plugin_api::{allocator::HostAllocatorPtr, *};
use libloading::{Error as DylibError, Library, Symbol};
use std::collections::BTreeMap;
use std::ffi::OsStr;

#[derive(Default)]
pub struct PluginRegistry {
    /// Plugins that are currently loaded.
    /// Provides a mapping from the plugin's name to the actual instance.
    loaded_plugins: BTreeMap<String, LoadedPlugin>,
}

#[allow(dead_code)]
impl PluginRegistry {
    /// Creates a new `PluginRegistry`.
    pub fn new() -> Self {
        PluginRegistry::default()
    }

    /// Finds a plugin by its name. The plugin must have been loaded before with [`load_plugin()`].
    ///
    /// Plugins are indexed by the name that is returned by [`CoprocessorPlugin::name()`].
    pub fn get_plugin(&self, plugin_name: &str) -> Option<&impl CoprocessorPlugin> {
        self.loaded_plugins.get(plugin_name)
    }

    /// Loads a [`CoprocessorPlugin`] from a `dylib`.
    ///
    /// After this function has successfully finished, the plugin is registered with the
    /// [`PluginRegistry`] and can later be obtained by calling [`get_plugin()`] with the proper
    /// name.
    ///
    /// Returns the name of the loaded plugin.
    pub fn load_plugin<P: AsRef<OsStr>>(
        &mut self,
        filename: P,
    ) -> Result<&'static str, DylibError> {
        let lib = unsafe { Library::new(filename)? };
        let plugin = unsafe { LoadedPlugin::new(lib)? };
        let plugin_name = plugin.name();

        self.loaded_plugins.insert(plugin_name.to_string(), plugin);
        Ok(plugin_name)
    }
}

/// A wrapper around a loaded raw coprocessor plugin library.
struct LoadedPlugin {
    /// Pointer to a [`CoprocessorPlugin`] in the loaded `lib`.
    plugin: Box<dyn CoprocessorPlugin>,
}

impl LoadedPlugin {
    /// Creates a new `LoadedPlugin` by loading a `dylib` from a file into memory.
    ///
    /// The function instantiates the plugin by calling `_plugin_create()` to obtain a
    /// [`CoprocessorPlugin`].
    ///
    /// # Safety
    ///
    /// The library **must** contain a function with name [`PLUGIN_CONSTRUCTOR_SYMBOL`] and the
    /// signature of [`PluginConstructorSignature`]. Otherwise, behavior is undefined.
    /// See also [`libloading::Library::get()`] for more information on what restrictions apply to
    /// [`PLUGIN_CONSTRUCTOR_SYMBOL`].
    pub unsafe fn new(lib: Library) -> Result<Self, DylibError> {
        let constructor: Symbol<PluginConstructorSignature> = lib.get(PLUGIN_CONSTRUCTOR_SYMBOL)?;

        let host_allocator = HostAllocatorPtr {
            alloc_fn: std::alloc::alloc,
            dealloc_fn: std::alloc::dealloc,
        };

        let boxed_raw_plugin = constructor(host_allocator);
        let plugin = Box::from_raw(boxed_raw_plugin);

        // Leak library so that we will never drop it
        std::mem::forget(lib);

        Ok(LoadedPlugin { plugin })
    }
}

impl CoprocessorPlugin for LoadedPlugin {
    fn name(&self) -> &'static str {
        self.plugin.name()
    }

    fn on_raw_coprocessor_request(
        &self,
        region: &Region,
        request: &RawRequest,
        storage: &dyn RawStorage,
    ) -> Result<RawResponse, Box<dyn std::error::Error>> {
        self.plugin
            .on_raw_coprocessor_request(region, request, storage)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coprocessor_plugin_api::pkgname_to_libname;

    #[test]
    fn load_plugin() {
        let lib = unsafe { Library::new(pkgname_to_libname("example-plugin")).unwrap() };
        let loaded_plugin = unsafe { LoadedPlugin::new(lib).unwrap() };
        let plugin_name = loaded_plugin.name();

        assert_eq!(plugin_name, "example-plugin");
    }

    #[test]
    fn plugin_registry_load_and_get_plugin() {
        let mut plugin_registry = PluginRegistry::default();
        let plugin_name = plugin_registry
            .load_plugin(pkgname_to_libname("example-plugin"))
            .unwrap();
        let plugin = plugin_registry.get_plugin(plugin_name).unwrap();

        assert_eq!(plugin.name(), "example-plugin");
    }
}
