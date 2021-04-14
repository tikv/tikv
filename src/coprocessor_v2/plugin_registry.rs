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
    fs_watcher: Option<notify::RecommendedWatcher>,
}

#[allow(dead_code)]
impl PluginRegistry {
    /// Creates a new `PluginRegistry`.
    pub fn new() -> Self {
        let registry = Arc::new(RwLock::new(PluginRegistryInner::default()));

        PluginRegistry {
            inner: registry,
            fs_watcher: None,
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

        // If this is the first call to `start_hot_reloading()`, create a new file system watcher
        // and background thread for loading plugins. For later invocations, the same watcher and
        // thread will be used.
        if self.fs_watcher.is_none() {
            let (tx, rx) = mpsc::channel();
            let fs_watcher = notify::watcher(tx, Duration::from_secs(3)).unwrap();

            let hot_reload_registry = self.inner.clone();
            thread::spawn(move || {
                // Simple helper functions for loading/unloading plugins.
                let maybe_load = |file: &PathBuf| {
                    let mut hot_reload_registry = hot_reload_registry.write().unwrap();
                    if is_library_file(&file) {
                        // Ignore errors.
                        hot_reload_registry.load_plugin(file).ok();
                    }
                };
                let unload = |file: &PathBuf| {
                    let mut hot_reload_registry = hot_reload_registry.write().unwrap();
                    if let Some(plugin) = hot_reload_registry.get_plugin_by_path(file) {
                        hot_reload_registry.unload_plugin(plugin.name());
                    }
                };
                let rename = |old_path: &PathBuf, new_path: &PathBuf| {
                    let mut hot_reload_registry = hot_reload_registry.write().unwrap();
                    if let Some(plugin) = hot_reload_registry.get_plugin_by_path(old_path) {
                        hot_reload_registry.update_plugin_path(plugin.name(), new_path);
                    }
                };

                loop {
                    match rx.recv() {
                        Ok(DebouncedEvent::Create(file)) => {
                            maybe_load(&file);
                        }
                        Ok(DebouncedEvent::Remove(file)) => {
                            unload(&file);
                        }
                        Ok(DebouncedEvent::Write(file)) => {
                            unload(&file);
                            maybe_load(&file);
                        }
                        Ok(DebouncedEvent::Rename(old_file, new_file)) => {
                            rename(&old_file, &new_file);
                        }
                        Ok(_) => (),
                        Err(_) => break, // Stop when watcher is dropped.
                    }
                }
            });

            self.fs_watcher = Some(fs_watcher);
        }

        // Register a new directory for hot-reloading.
        if let Some(watcher) = &mut self.fs_watcher {
            watcher.watch(&plugin_directory, RecursiveMode::NonRecursive)?;
        } else {
            // `self.fs_watcher` will never be `None` at this point.
            unreachable!();
        }

        // Load plugins that are already in the directory.
        self.load_plugins_from_dir(&plugin_directory)?;
        Ok(())
    }

    /// Finds a plugin by its name. The plugin must have been loaded before with [`load_plugin()`].
    ///
    /// Plugins are indexed by the name that is returned by [`CoprocessorPlugin::name()`].
    pub fn get_plugin(&self, plugin_name: &str) -> Option<Arc<impl CoprocessorPlugin>> {
        self.inner.read().unwrap().get_plugin(plugin_name)
    }

    /// finds a plugin by its associated file path, similar to [`get_plugin()`].
    ///
    /// The given path has to be exactly the same as the one the plugin with loaded with, e.g.
    /// `"./coprocessors/plugin1.so"` would be *different* from `"coprocessors/plugin1.so"`
    /// (note the leading `./`). The same applies when the associated path was changed with
    /// [`update_plugin_path()`].
    pub fn get_plugin_by_path<P: AsRef<OsStr>>(
        &self,
        plugin_path: P,
    ) -> Option<Arc<impl CoprocessorPlugin>> {
        self.inner.read().unwrap().get_plugin_by_path(plugin_path)
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
    pub fn load_plugin<P: AsRef<OsStr>>(&self, file_name: P) -> Result<&'static str, DylibError> {
        self.inner.write().unwrap().load_plugin(file_name)
    }

    /// Attempts to load all plugins from a given directory.
    ///
    /// Returns a list of the names of all successfully loaded plugins.
    /// If a file could not be successfully loaded as a plugin, it will be discarded.
    ///
    /// The plugins have to follow the system's naming convention in order to be loaded, e.g. `.so`
    /// for Linux, `.dylib` for MacOS and `.dll` for Windows.
    pub fn load_plugins_from_dir(
        &self,
        dir_name: impl Into<PathBuf>,
    ) -> std::io::Result<Vec<&'static str>> {
        let dir_name = dir_name.into();
        let mut loaded_plugins = Vec::new();

        for entry in std::fs::read_dir(&dir_name)? {
            if let Ok(file) = entry.map(|f| f.path()) {
                if is_library_file(&file) {
                    // Ignore errors.
                    let r = self.load_plugin(&file).ok();
                    if let Some(plugin_name) = r {
                        loaded_plugins.push(plugin_name);
                    }
                }
            }
        }
        Ok(loaded_plugins)
    }

    /// Unloads a plugin with the given `plugin_name`.
    pub fn unload_plugin(&self, plugin_name: &str) {
        self.inner.write().unwrap().unload_plugin(plugin_name)
    }

    /// Updates the associated file path for plugin.
    ///
    /// This function should be used to maintain consistent state when the underlying file of a
    /// plugin was renamed or moved.
    pub fn update_plugin_path<P: AsRef<OsStr>>(&self, plugin_name: &str, new_path: P) {
        self.inner
            .write()
            .unwrap()
            .update_plugin_path(plugin_name, new_path)
    }

    /// Returns the associated file path for the plugin for the given `plugin_name`.
    pub fn get_path_for_plugin(&self, plugin_name: &str) -> Option<OsString> {
        self.inner
            .read()
            .unwrap()
            .get_path_for_plugin(plugin_name)
            .map(|s| s.to_os_string())
    }
}

#[derive(Default)]
struct PluginRegistryInner {
    /// Plugins that are currently loaded.
    /// Provides a mapping from the plugin's name to the actual instance.
    loaded_plugins: HashMap<String, (OsString, Arc<LoadedPlugin>)>,
}

impl PluginRegistryInner {
    #[inline]
    pub fn get_plugin(&self, plugin_name: &str) -> Option<Arc<impl CoprocessorPlugin>> {
        self.loaded_plugins
            .get(plugin_name)
            .map(|(_path, plugin)| plugin.clone())
    }

    #[inline]
    pub fn get_plugin_by_path<P: AsRef<OsStr>>(
        &self,
        plugin_path: P,
    ) -> Option<Arc<impl CoprocessorPlugin>> {
        self.loaded_plugins
            .iter()
            .find(|(_, (path, _))| path == plugin_path.as_ref())
            .map(|(_, (_, plugin))| plugin.clone())
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
        let plugin = unsafe { LoadedPlugin::new(&filename) };
        if let Err(err) = &plugin {
            let filename = filename.as_ref().to_string_lossy();
            warn!("failed to load coprocessor plugin. Maybe not compiled correctly as a TiKV plugin?"; "plugin_path" => ?filename, "error" => ?err);
        }
        let plugin = plugin?;

        let plugin_name = plugin.name();

        self.loaded_plugins.insert(
            plugin_name.to_string(),
            (filename.as_ref().to_os_string(), Arc::new(plugin)),
        );
        Ok(plugin_name)
    }

    #[inline]
    pub fn unload_plugin(&mut self, plugin_name: &str) {
        self.loaded_plugins.remove(plugin_name);
    }

    #[inline]
    pub fn update_plugin_path<P: AsRef<OsStr>>(&mut self, plugin_name: &str, new_path: P) {
        if let Some((plugin_path, _)) = self.loaded_plugins.get_mut(plugin_name) {
            *plugin_path = new_path.as_ref().to_os_string();
        }
    }

    #[inline]
    pub fn get_path_for_plugin(&self, plugin_name: &str) -> Option<&OsStr> {
        self.loaded_plugins
            .get(plugin_name)
            .map(|(path, _plugin)| path.as_os_str())
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

//#[cfg(test)]
//mod tests {
//    use super::*;
//    use coprocessor_plugin_api::pkgname_to_libname;
//
//    #[test]
//    fn load_plugin() {
//        let library_path = pkgname_to_libname("example-plugin");
//        let loaded_plugin = unsafe { LoadedPlugin::new(&library_path).unwrap() };
//
//        assert_eq!(loaded_plugin.name(), "example-plugin");
//    }
//
//    #[test]
//    fn registry_load_and_get_plugin() {
//        let registry = PluginRegistry::new();
//        let library_path = pkgname_to_libname("example-plugin");
//        let plugin_name = registry.load_plugin(&library_path).unwrap();
//
//        let plugin = registry.get_plugin(plugin_name).unwrap();
//
//        assert_eq!(plugin.name(), "example-plugin");
//        assert_eq!(registry.loaded_plugin_names(), vec!["example-plugin"]);
//        assert_eq!(
//            registry
//                .get_path_for_plugin("example-plugin")
//                .unwrap()
//                .to_string_lossy(),
//            library_path
//        );
//    }
//
//    #[test]
//    fn update_plugin_path() {
//        let registry = PluginRegistry::new();
//        let library_path = pkgname_to_libname("example-plugin");
//        let library_path_2 = pkgname_to_libname("example-plugin-2");
//
//        let plugin_name = registry.load_plugin(&library_path).unwrap();
//
//        assert_eq!(
//            registry
//                .get_path_for_plugin(plugin_name)
//                .unwrap()
//                .to_str()
//                .unwrap(),
//            &library_path
//        );
//
//        registry.update_plugin_path(plugin_name, &library_path_2);
//
//        assert_eq!(
//            registry
//                .get_path_for_plugin(plugin_name)
//                .unwrap()
//                .to_str()
//                .unwrap(),
//            &library_path_2
//        );
//    }
//
//    #[test]
//    fn registry_unload_plugin() {
//        let registry = PluginRegistry::new();
//        let library_path = pkgname_to_libname("example-plugin");
//
//        let plugin_name = registry.load_plugin(&library_path).unwrap();
//
//        assert!(registry.get_plugin(plugin_name).is_some());
//
//        registry.unload_plugin(plugin_name);
//
//        assert!(registry.get_plugin(plugin_name).is_none());
//        assert_eq!(registry.loaded_plugin_names().len(), 0);
//    }
//
//    #[test]
//    fn plugin_registry_hot_reloading() {
//        let build_dir = std::env::current_exe()
//            .map(|p| p.as_path().parent().unwrap().to_owned())
//            .unwrap();
//        let coprocessor_dir = build_dir.join("coprocessors");
//        let library_path = coprocessor_dir.join(pkgname_to_libname("example-plugin"));
//        let library_path_2 = coprocessor_dir.join(pkgname_to_libname("example-plugin-2"));
//        let plugin_name = "example-plugin";
//
//        std::fs::remove_dir_all(&coprocessor_dir).unwrap();
//        std::fs::create_dir_all(&coprocessor_dir).unwrap();
//
//        let mut registry = PluginRegistry::new();
//        registry.start_hot_reloading(&coprocessor_dir).unwrap();
//
//        // trigger loading
//        std::fs::copy(
//            build_dir.join(&pkgname_to_libname("example-plugin")),
//            &library_path,
//        )
//        .unwrap();
//        std::thread::sleep(Duration::from_secs(4));
//
//        assert!(registry.get_plugin(plugin_name).is_some());
//        assert_eq!(
//            &PathBuf::from(registry.get_path_for_plugin(plugin_name).unwrap()),
//            &library_path
//        );
//
//        // trigger rename
//        std::fs::rename(&library_path, &library_path_2).unwrap();
//        std::thread::sleep(Duration::from_secs(4));
//
//        assert!(registry.get_plugin(plugin_name).is_some());
//        assert_eq!(
//            &PathBuf::from(registry.get_path_for_plugin(plugin_name).unwrap()),
//            &library_path_2
//        );
//
//        // trigger unloading
//        std::fs::remove_file(&library_path_2).unwrap();
//        std::thread::sleep(Duration::from_secs(4));
//
//        assert!(registry.get_plugin(plugin_name).is_none());
//    }
//}
