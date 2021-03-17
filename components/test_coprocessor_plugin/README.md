This directory contains example plugins for TiKV's coprocessor, primarily used for testing.

Example plugins should have the following lines in their `Cargo.toml`

```toml
[lib]
crate-type = ["cdylib"]
```

When a crate specifies an example plugin as a dependency, a `cdylib` will be built in the `target/<profile>` directory.
The function `coprocessor_plugin_api::pkgname_to_libname()` can be used to resolve the package name
to a proper library name. 
