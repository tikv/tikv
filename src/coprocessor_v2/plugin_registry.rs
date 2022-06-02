// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, HashSet},
    ffi::{OsStr, OsString},
    ops::Range,
    path::{Path, PathBuf},
    sync::{mpsc, Arc, RwLock},
    thread,
    time::Duration,
};

use coprocessor_plugin_api::{allocator::HostAllocatorPtr, util::*, *};
use libloading::{Error as DylibError, Library, Symbol};
use notify::{DebouncedEvent, RecursiveMode, Watcher};
use semver::Version;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PluginLoadingError {
    #[error("failed to load library")]
    Dylib(#[from] DylibError),

    #[error("plugin version string does not follow SemVer standard")]
    VersionParseError(#[from] semver::SemVerError),

    #[error(
        "version mismatch of rustc: plugin was compiled with {plugin_rustc}, but TiKV with {tikv_rustc}"
    )]
    CompilerMismatch {
        plugin_rustc: String,
        tikv_rustc: String,
    },

    #[error(
        "target mismatch: plugin was compiled for {plugin_target}, but TiKV for {tikv_target}"
    )]
    TargetMismatch {
        plugin_target: String,
        tikv_target: String,
    },

    #[error(
        "coprocessor_plugin_api mismatch: plugin was compiled with {plugin_api}, but TiKV with {tikv_api}"
    )]
    ApiMismatch {
        plugin_api: String,
        tikv_api: String,
    },

    #[error("unloaded plugin `{path:?}` cannot be reloaded")]
    ReloadError { path: OsString },
}

/// Helper function for error handling.
fn err_on_mismatch(
    plugin_build_info: &BuildInfo,
    tikv_build_info: &BuildInfo,
) -> Result<(), PluginLoadingError> {
    if plugin_build_info.api_version != tikv_build_info.api_version {
        Err(PluginLoadingError::ApiMismatch {
            plugin_api: plugin_build_info.api_version.to_string(),
            tikv_api: tikv_build_info.api_version.to_string(),
        })
    } else if plugin_build_info.rustc != tikv_build_info.rustc {
        Err(PluginLoadingError::CompilerMismatch {
            plugin_rustc: plugin_build_info.rustc.to_string(),
            tikv_rustc: tikv_build_info.rustc.to_string(),
        })
    } else if plugin_build_info.target != tikv_build_info.target {
        Err(PluginLoadingError::TargetMismatch {
            plugin_target: plugin_build_info.target.to_string(),
            tikv_target: tikv_build_info.target.to_string(),
        })
    } else {
        Ok(())
    }
}

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
    /// the current platform (`.so` for Linux, `.dylib` for macOS, `.dll` for Windows).
    pub fn start_hot_reloading(
        &mut self,
        plugin_directory: impl Into<PathBuf>,
    ) -> notify::Result<()> {
        let plugin_directory = plugin_directory.into();

        // Create plugin directory if it doesn't exist.
        std::fs::create_dir_all(&plugin_directory)?;

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
                            // We cannot do much when the file is deleted. See issue #10854
                            warn!("a loaded coprocessor plugin is removed. Be aware that original plugin is still running"; "plugin_path" => ?file);
                        }
                        Ok(DebouncedEvent::Rename(old_file, new_file)) => {
                            // If the file is renamed with a different parent directory, we will receive a `Remove` instead.
                            debug_assert!(old_file.parent() == new_file.parent());
                            rename(&old_file, &new_file);
                        }
                        Ok(DebouncedEvent::Write(file)) => {
                            // We cannot do much when the file is deleted. See issue #10854
                            warn!("another process is overwriting a coprocessor plugin while the plugin is loaded. Be aware that original plugin is still running"; "plugin_path" => ?file);
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
    pub fn get_plugin(&self, plugin_name: &str) -> Option<Arc<LoadedPlugin>> {
        self.inner.read().unwrap().get_plugin(plugin_name)
    }

    /// finds a plugin by its associated file path, similar to [`get_plugin()`].
    ///
    /// The given path has to be exactly the same as the one the plugin with loaded with, e.g.
    /// `"./coprocessors/plugin1.so"` would be *different* from `"coprocessors/plugin1.so"`
    /// (note the leading `./`). The same applies when the associated path was changed with
    /// [`update_plugin_path()`].
    pub fn get_plugin_by_path<P: AsRef<OsStr>>(&self, plugin_path: P) -> Option<Arc<LoadedPlugin>> {
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
    pub fn load_plugin<P: AsRef<OsStr>>(&self, file_name: P) -> Result<String, PluginLoadingError> {
        self.inner.write().unwrap().load_plugin(file_name)
    }

    /// Attempts to load all plugins from a given directory.
    ///
    /// Returns a list of the names of all successfully loaded plugins.
    /// If a file could not be successfully loaded as a plugin, it will be discarded.
    ///
    /// The plugins have to follow the system's naming convention in order to be loaded, e.g. `.so`
    /// for Linux, `.dylib` for macOS and `.dll` for Windows.
    pub fn load_plugins_from_dir(
        &self,
        dir_name: impl Into<PathBuf>,
    ) -> std::io::Result<Vec<String>> {
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

    /// Original paths that plugins loaded from.
    /// Files in this list should not be loaded again.
    library_paths: HashSet<OsString>,
}

impl PluginRegistryInner {
    #[inline]
    pub fn get_plugin(&self, plugin_name: &str) -> Option<Arc<LoadedPlugin>> {
        self.loaded_plugins
            .get(plugin_name)
            .map(|(_path, plugin)| plugin.clone())
    }

    #[inline]
    pub fn get_plugin_by_path<P: AsRef<OsStr>>(&self, plugin_path: P) -> Option<Arc<LoadedPlugin>> {
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
    ) -> Result<String, PluginLoadingError> {
        if self.library_paths.contains(filename.as_ref()) {
            let err = Err(PluginLoadingError::ReloadError {
                path: filename.as_ref().to_owned(),
            });
            let filename = filename.as_ref().to_string_lossy();
            error!("Unloaded plugin should not load again!"; "plugin_path" => ?filename, "error" => ?err);
            return err;
        }
        let plugin = unsafe { LoadedPlugin::new(&filename) };
        if let Err(err) = &plugin {
            let filename = filename.as_ref().to_string_lossy();
            warn!("failed to load coprocessor plugin. Maybe not compiled correctly as a TiKV plugin?"; "plugin_path" => ?filename, "error" => ?err);
        }
        let plugin = plugin?;
        // plugin successfully loaded, add path to library_paths.
        self.library_paths.insert(filename.as_ref().to_owned());

        let plugin_name = plugin.name().to_string();

        self.loaded_plugins.insert(
            plugin_name.clone(),
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
pub struct LoadedPlugin {
    /// The name of the plugin.
    name: String,
    /// The version of the plugin.
    version: Version,
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
    pub unsafe fn new<P: AsRef<OsStr>>(file_path: P) -> Result<Self, PluginLoadingError> {
        let lib = Library::new(&file_path)?;

        let get_build_info: Symbol<'_, PluginGetBuildInfoSignature> =
            lib.get(PLUGIN_GET_BUILD_INFO_SYMBOL)?;
        let get_plugin_info: Symbol<'_, PluginGetPluginInfoSignature> =
            lib.get(PLUGIN_GET_PLUGIN_INFO_SYMBOL)?;
        let plugin_constructor: Symbol<'_, PluginConstructorSignature> =
            lib.get(PLUGIN_CONSTRUCTOR_SYMBOL)?;

        // It's important to check the ABI before calling the constructor.
        let plugin_build_info = get_build_info();
        let tikv_build_info = BuildInfo::get();
        err_on_mismatch(&plugin_build_info, &tikv_build_info)?;

        let info = get_plugin_info();
        let name = info.name.to_string();
        let version = Version::parse(info.version)?;

        let host_allocator = HostAllocatorPtr {
            alloc_fn: std::alloc::alloc,
            dealloc_fn: std::alloc::dealloc,
        };

        let boxed_raw_plugin = plugin_constructor(host_allocator);
        let plugin = Box::from_raw(boxed_raw_plugin);

        // Leak library so that we will never drop it
        std::mem::forget(lib);

        Ok(LoadedPlugin {
            name,
            version,
            plugin,
        })
    }

    /// Returns the name of the plugin.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the version of the plugin.
    pub fn version(&self) -> &Version {
        &self.version
    }
}

impl CoprocessorPlugin for LoadedPlugin {
    fn on_raw_coprocessor_request(
        &self,
        ranges: Vec<Range<Key>>,
        request: RawRequest,
        storage: &dyn RawStorage,
    ) -> PluginResult<RawResponse> {
        self.plugin
            .on_raw_coprocessor_request(ranges, request, storage)
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
    use coprocessor_plugin_api::util::pkgname_to_libname;

    use super::*;

    fn initialize_library() -> PathBuf {
        let mut path = std::env::current_exe().unwrap();
        path.set_file_name(pkgname_to_libname("example-plugin"));
        path
    }

    #[test]
    fn load_plugin() {
        let library_path = initialize_library();

        let loaded_plugin = unsafe { LoadedPlugin::new(&library_path).unwrap() };

        assert_eq!(loaded_plugin.name(), "example_plugin");
        assert_eq!(loaded_plugin.version(), &Version::parse("0.1.0").unwrap());
    }

    #[test]
    fn registry_load_and_get_plugin() {
        let library_path = initialize_library();

        let registry = PluginRegistry::new();
        let plugin_name = registry.load_plugin(&library_path).unwrap();

        let plugin = registry.get_plugin(&plugin_name).unwrap();

        assert_eq!(plugin.name(), "example_plugin");
        assert_eq!(registry.loaded_plugin_names(), vec!["example_plugin"]);
        assert_eq!(
            registry.get_path_for_plugin("example_plugin").unwrap(),
            library_path.as_os_str()
        );
    }

    #[test]
    fn update_plugin_path() {
        let library_path = initialize_library();

        let library_path_2 = library_path
            .parent()
            .unwrap()
            .join(pkgname_to_libname("example-plugin-2"));

        let registry = PluginRegistry::new();
        let plugin_name = registry.load_plugin(&library_path).unwrap();

        assert_eq!(
            registry.get_path_for_plugin(&plugin_name).unwrap(),
            library_path.as_os_str()
        );

        registry.update_plugin_path(&plugin_name, &library_path_2);

        assert_eq!(
            registry.get_path_for_plugin(&plugin_name).unwrap(),
            library_path_2.into_os_string()
        );
    }

    #[test]
    fn registry_unload_plugin() {
        let library_path = initialize_library();

        let registry = PluginRegistry::new();

        let plugin_name = registry.load_plugin(&library_path).unwrap();

        assert!(registry.get_plugin(&plugin_name).is_some());

        registry.unload_plugin(&plugin_name);

        assert!(registry.get_plugin(&plugin_name).is_none());
        assert_eq!(registry.loaded_plugin_names().len(), 0);
    }

    #[test]
    fn plugin_registry_hot_reloading() {
        let original_library_path = initialize_library();

        let coprocessor_dir = std::env::temp_dir().join("coprocessors");
        let library_path = coprocessor_dir.join(pkgname_to_libname("example-plugin"));
        let library_path_2 = coprocessor_dir.join(pkgname_to_libname("example-plugin-2"));
        let plugin_name = "example_plugin";

        // Make the coprocessor directory is empty.
        std::fs::create_dir_all(&coprocessor_dir).unwrap();
        std::fs::remove_dir_all(&coprocessor_dir).unwrap();

        let mut registry = PluginRegistry::new();
        registry.start_hot_reloading(&coprocessor_dir).unwrap();

        // trigger loading
        std::fs::copy(&original_library_path, &library_path).unwrap();
        // fs watcher detects changes in every 3 seconds, therefore, wait 4 seconds so as to make sure the watcher is triggered.
        std::thread::sleep(Duration::from_secs(4));

        assert!(registry.get_plugin(plugin_name).is_some());
        assert_eq!(
            &PathBuf::from(registry.get_path_for_plugin(plugin_name).unwrap()),
            &library_path
        );

        // trigger rename
        std::fs::rename(&library_path, &library_path_2).unwrap();
        // fs watcher detects changes in every 3 seconds, therefore, wait 4 seconds so as to make sure the watcher is triggered.
        std::thread::sleep(Duration::from_secs(4));

        assert!(registry.get_plugin(plugin_name).is_some());
        assert_eq!(
            &PathBuf::from(registry.get_path_for_plugin(plugin_name).unwrap()),
            &library_path_2
        );

        std::fs::remove_file(&library_path_2).unwrap();
        // fs watcher detects changes in every 3 seconds, therefore, wait 4 seconds so as to make sure the watcher is triggered.
        std::thread::sleep(Duration::from_secs(4));

        // plugin will not be unloadad
        assert!(registry.get_plugin(plugin_name).is_some());
    }
}
