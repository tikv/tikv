// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use coprocessor_plugin_api::{
    allocator::HostAllocatorPtr, CoprocessorPlugin, PluginConstructorSignature,
    PLUGIN_CONSTRUCTOR_NAME,
};
use libloading::{Error as DylibError, Library, Symbol};
use std::collections::BTreeMap;
use std::ffi::OsStr;

#[derive(Default)]
pub struct PluginRegistry {
    /// Plugins that are currently loaded.
    /// Provides a mapping from the plugin's name to the actual instance.
    loaded_plugins: BTreeMap<String, LoadedPlugin>,
}

impl PluginRegistry {
    /// Creates a new `PluginRegistry`.
    pub fn new() -> Self {
        PluginRegistry::default()
    }

    /// Finds a plugin by its name. The plugin must have been loaded before with [`load_plugin()`].
    ///
    /// Plugins are indexed by the name that is returned by [`CoprocessorPlugin::name()`].
    pub fn get_plugin(&self, plugin_name: &str) -> Option<&dyn CoprocessorPlugin> {
        self.loaded_plugins.get(plugin_name).map(|p| p.plugin())
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
        let plugin_name = plugin.plugin().name();

        self.loaded_plugins.insert(plugin_name.to_string(), plugin);
        Ok(plugin_name)
    }
}

/// A wrapper around a loaded raw coprocessor plugin library.
struct LoadedPlugin {
    /// Pointer to a [`CoprocessorPlugin`] in the loaded `lib`.
    plugin: Box<dyn CoprocessorPlugin>,
    /// Underlying library.
    lib: Library,
}

impl LoadedPlugin {
    /// Creates a new `LoadedPlugin` by loading a `dylib` from a file into memory.
    ///
    /// The function instantiates the plugin by calling `_plugin_create()` to obtain a
    /// [`CoprocessorPlugin`].
    ///
    /// # Safety
    ///
    /// The library **must** contain a function with name [`PLUGIN_CONSTRUCTOR_NAME`] and the
    /// signature of [`PluginConstructorSignature`]. Otherwise, behavior is undefined.
    /// See also [`libloading::Library::get()`] for more information on what restrictions apply to
    /// [`PLUGIN_CONSTRUCTOR_NAME`].
    pub unsafe fn new(lib: Library) -> Result<Self, DylibError> {
        let constructor: Symbol<PluginConstructorSignature> = lib.get(PLUGIN_CONSTRUCTOR_NAME)?;

        let host_allocator = HostAllocatorPtr {
            alloc_fn: std::alloc::alloc,
            dealloc_fn: std::alloc::dealloc,
        };

        let boxed_raw_plugin = constructor(host_allocator);
        let plugin = Box::from_raw(boxed_raw_plugin);

        Ok(LoadedPlugin {
            plugin,
            lib,
        })
    }

    pub fn plugin(&self) -> &dyn CoprocessorPlugin {
        self.plugin.as_ref()
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
        let plugin_name = loaded_plugin.plugin().name();

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
