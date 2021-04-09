// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use coprocessor_plugin_api::{allocator::HostAllocatorPtr, *};
use libloading::{Error as DylibError, Library, Symbol};
use notify::{DebouncedEvent, RecursiveMode, Watcher};
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, RwLock};
use std::thread;
use std::time::Duration;

/// Manages loading and unloading of coprocessor plugins.
pub struct PluginRegistry {
    inner: Arc<RwLock<PluginRegistryInner>>,
    /// Active file system watcher for hot-reloading.
    /// If dropped, the corresponding hot-reloading thread will stop.
    fs_watcher: notify::RecommendedWatcher,
}

#[allow(dead_code)]
impl PluginRegistry {
    /// Creates a new `PluginRegistry`.
    pub fn new() -> Self {
        let registry = Arc::new(RwLock::new(PluginRegistryInner::default()));

        // Create a file system watcher and background thread for hot-reloading.
        let (tx, rx) = mpsc::channel();
        let fs_watcher = notify::watcher(tx, Duration::from_secs(3)).unwrap();

        let hot_reload_registry = registry.clone();
        thread::spawn(move || {
            loop {
                match rx.recv() {
                    Ok(DebouncedEvent::Create(file)) => {
                        if is_library_file(&file) {
                            let r = hot_reload_registry.write().unwrap().load_plugin(&file);
                            if let Err(err) = r {
                                warn!("failed to load coprocessor plugin. Maybe not compiled correctly as a TiKV plugin?"; "plugin_path" => file.display(), "error" => ?err);
                            }
                        }
                    }
                    Ok(DebouncedEvent::Remove(file)) => {
                        if is_library_file(&file) {
                            hot_reload_registry
                                .write()
                                .unwrap()
                                .unload_plugin_by_path(&file);
                        }
                    }
                    Ok(_) => (),
                    Err(_) => break, // Stop when watcher is dropped.
                }
            }
        });

        PluginRegistry {
            inner: registry,
            fs_watcher,
        }
    }

    /// Hot-reloads plugins from a given directory.
    ///
    /// All plugins that are already present in the directory will be loaded.
    /// A background thread is spawned to watch file system events. If the library file of a loaded
    /// plugin is deleted, the corresponding plugin is automatically unloaded; if a new library file
    /// is placed into the directory, it will be automatically loaded into TiKV's coprocessor plugin
    /// system.
    ///
    /// A file will only be loaded if it has the proper file ending of dynamic link libraries for
    /// the current platform (`.so` for Linux, `.dylib` for MacOS, `.dll` for Windows).
    pub fn start_hot_reloading(
        &mut self,
        plugin_directory: impl Into<PathBuf>,
    ) -> notify::Result<()> {
        let plugin_directory = plugin_directory.into();

        // Register a new directory for hot-reloading.
        self.fs_watcher
            .watch(&plugin_directory, RecursiveMode::NonRecursive)?;

        // Load plugins that are already in the directory.
        for entry in std::fs::read_dir(&plugin_directory)? {
            if let Ok(file) = entry.map(|f| f.path()) {
                if is_library_file(&file) {
                    let r = self.load_plugin(&file);
                    if let Err(err) = r {
                        warn!("failed to load coprocessor plugin. Maybe not compiled correctly as a TiKV plugin?"; "plugin_path" => file.display(), "error" => ?err);
                    }
                }
            }
        }

        Ok(())
    }

    /// Finds a plugin by its name. The plugin must have been loaded before with [`load_plugin()`].
    ///
    /// Plugins are indexed by the name that is returned by [`CoprocessorPlugin::name()`].
    pub fn get_plugin(&self, plugin_name: &str) -> Option<Arc<impl CoprocessorPlugin>> {
        self.inner.read().unwrap().get_plugin(plugin_name)
    }

    /// Returns the names of the currently loaded plugins.
    /// The order of plugin names is arbitrary.
    pub fn loaded_plugin_names(&self) -> Vec<String> {
        // Collect names into vector so we can release the `RwLockReadGuard` before we return.
        self.inner
            .read()
            .unwrap()
            .loaded_plugin_names()
            .cloned()
            .collect()
    }

    /// Loads a [`CoprocessorPlugin`] from a `dylib`.
    ///
    /// After this function has successfully finished, the plugin is registered with the
    /// [`PluginRegistry`] and can later be obtained by calling [`get_plugin()`] with the proper
    /// name.
    ///
    /// Returns the name of the loaded plugin.
    pub fn load_plugin<P: AsRef<OsStr>>(&self, filename: P) -> Result<&'static str, DylibError> {
        self.inner.write().unwrap().load_plugin(filename)
    }

    /// Unloads a plugin with the given `plugin_name`.
    pub fn unload_plugin_by_name(&self, plugin_name: &str) {
        self.inner
            .write()
            .unwrap()
            .unload_plugin_by_name(plugin_name)
    }

    /// Unloads a plugin based on the given file path.
    ///
    /// The plugin whose library path is equal to the given `plugin_file` path is unloaded.
    ///
    /// Note that equality between paths here means equality of the representation, so `"lib"` is
    /// not the same as `"./lib"`; in other words: [`unload_plugin_by_path()`] requires the _exact_
    /// same path as was passed to [`load_plugin()`].
    pub fn unload_plugin_by_path<P: AsRef<OsStr>>(&self, plugin_path: P) {
        self.inner
            .write()
            .unwrap()
            .unload_plugin_by_path(plugin_path)
    }
}

impl Default for PluginRegistry {
    fn default() -> Self {
        PluginRegistry::new()
    }
}

#[derive(Default)]
struct PluginRegistryInner {
    /// Plugins that are currently loaded.
    /// Provides a mapping from the plugin's name to the actual instance.
    loaded_plugins: HashMap<String, Arc<LoadedPlugin>>,
}

impl PluginRegistryInner {
    #[inline]
    pub fn get_plugin(&self, plugin_name: &str) -> Option<Arc<impl CoprocessorPlugin>> {
        self.loaded_plugins.get(plugin_name).cloned()
    }

    #[inline]
    pub fn loaded_plugin_names(&self) -> impl Iterator<Item = &String> {
        self.loaded_plugins.keys()
    }

    #[inline]
    pub fn load_plugin<P: AsRef<OsStr>>(
        &mut self,
        filename: P,
    ) -> Result<&'static str, DylibError> {
        let plugin = unsafe { LoadedPlugin::new(&filename)? };
        let plugin_name = plugin.name();

        self.loaded_plugins
            .insert(plugin_name.to_string(), Arc::new(plugin));
        Ok(plugin_name)
    }

    #[inline]
    pub fn unload_plugin_by_name(&mut self, plugin_name: &str) {
        self.loaded_plugins.remove(plugin_name);
    }

    #[inline]
    pub fn unload_plugin_by_path<P: AsRef<OsStr>>(&mut self, plugin_path: P) {
        if let Some(plugin_name) = self
            .loaded_plugins
            .iter()
            .find(|(_plugin_name, plugin)| plugin.file_name() == plugin_path.as_ref())
            .map(|(plugin_name, _)| plugin_name.clone())
        {
            self.unload_plugin_by_name(&plugin_name)
        }
    }
}

/// A wrapper around a loaded raw coprocessor plugin library.
struct LoadedPlugin {
    /// Pointer to a [`CoprocessorPlugin`] in the loaded `lib`.
    plugin: Box<dyn CoprocessorPlugin>,
    /// The path of the library file for this plugin.
    file_path: OsString,
}

impl LoadedPlugin {
    /// Creates a new `LoadedPlugin` by loading a `dylib` from a file into memory.
    ///
    /// The `file_path` argument may be any of:
    /// * A simple filename of a library if the library is in any of the platform-specific locations
    ///   from where libraries are usually loaded, e.g. the current directory or in
    ///   `LD_LIBRARY_PATH` on unix systems.
    /// * Absolute path to the library
    /// * Relative (to the current working directory) path to the library
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
    pub unsafe fn new<P: AsRef<OsStr>>(file_path: P) -> Result<Self, DylibError> {
        let lib = Library::new(&file_path)?;
        let constructor: Symbol<PluginConstructorSignature> = lib.get(PLUGIN_CONSTRUCTOR_SYMBOL)?;

        let host_allocator = HostAllocatorPtr {
            alloc_fn: std::alloc::alloc,
            dealloc_fn: std::alloc::dealloc,
        };

        let boxed_raw_plugin = constructor(host_allocator);
        let plugin = Box::from_raw(boxed_raw_plugin);

        // Leak library so that we will never drop it
        std::mem::forget(lib);

        Ok(LoadedPlugin {
            plugin,
            file_path: file_path.as_ref().to_os_string(),
        })
    }

    /// Returns the file name of the library file where this plugin was loaded from.
    pub fn file_name(&self) -> &OsStr {
        &self.file_path
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
    ) -> Result<RawResponse, PluginError> {
        self.plugin
            .on_raw_coprocessor_request(region, request, storage)
    }
}

#[cfg(target_os = "linux")]
fn is_library_file<P: AsRef<Path>>(path: P) -> bool {
    path.as_ref().extension() == Some(OsStr::new("so"))
}

#[cfg(target_os = "macos")]
fn is_library_file<P: AsRef<Path>>(path: P) -> bool {
    path.as_ref().extension() == Some(OsStr::new("dylib"))
}

#[cfg(target_os = "windows")]
fn is_library_file<P: AsRef<Path>>(path: P) -> bool {
    path.as_ref().extension() == Some(OsStr::new("dll"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use coprocessor_plugin_api::pkgname_to_libname;

    #[test]
    fn load_plugin() {
        let library_path = pkgname_to_libname("example-plugin");
        let loaded_plugin = unsafe { LoadedPlugin::new(&library_path).unwrap() };

        assert_eq!(loaded_plugin.name(), "example-plugin");
        assert_eq!(loaded_plugin.file_name().to_str().unwrap(), library_path);
    }

    #[test]
    fn registry_load_and_get_plugin() {
        let registry = PluginRegistry::default();
        let library_path = pkgname_to_libname("example-plugin");
        let plugin_name = registry.load_plugin(&library_path).unwrap();

        let plugin = registry.get_plugin(plugin_name).unwrap();

        assert_eq!(plugin.name(), "example-plugin");
        assert_eq!(registry.loaded_plugin_names(), vec!["example-plugin"]);
    }

    #[test]
    fn registry_unload_plugin_by_name() {
        let registry = PluginRegistry::default();
        let library_path = pkgname_to_libname("example-plugin");
        let plugin_name = registry.load_plugin(&library_path).unwrap();

        assert!(registry.get_plugin(plugin_name).is_some());

        registry.unload_plugin_by_name(plugin_name);

        assert!(registry.get_plugin(plugin_name).is_none());
        assert_eq!(registry.loaded_plugin_names().len(), 0);
    }

    #[test]
    fn plugin_registry_unload_plugin_by_path() {
        let registry = PluginRegistry::default();
        let library_path = pkgname_to_libname("example-plugin");
        let plugin_name = registry.load_plugin(&library_path).unwrap();

        assert!(registry.get_plugin(plugin_name).is_some());

        registry.unload_plugin_by_path(library_path);

        assert!(registry.get_plugin(plugin_name).is_none());
        assert_eq!(registry.loaded_plugin_names().len(), 0);
    }

    // TODO: test hot-reloading. Wait for PR #9802 to see how we deal with .so files in the end.
}
